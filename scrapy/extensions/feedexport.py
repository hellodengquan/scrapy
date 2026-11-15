"""
Feed Exports extension

See documentation in docs/topics/feed-exports.rst
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import shutil
import sys
import warnings
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path, PureWindowsPath
from tempfile import NamedTemporaryFile
from typing import IO, TYPE_CHECKING, Any, Protocol, Self, TypeAlias, cast
from urllib.parse import unquote, urlparse
from uuid import uuid4

from twisted.internet.defer import Deferred, DeferredList, DeferredLock
from w3lib.url import file_uri_to_path
from zope.interface import Interface

from scrapy import Spider, signals
from scrapy.exceptions import NotConfigured, ScrapyDeprecationWarning
from scrapy.extensions.postprocessing import PostProcessingManager
from scrapy.utils._ftp import ftp_store_file
from scrapy.utils.asyncio import is_asyncio_available, run_in_thread
from scrapy.utils.boto import _get_max_pool_connections, is_aioboto3_available
from scrapy.utils.conf import feed_complete_default_values_from_settings
from scrapy.utils.defer import (
    _maybe_coroutine,
    deferred_from_coro,
    ensure_awaitable,
    maybe_deferred_to_future,
)
from scrapy.utils.misc import build_from_crawler, load_object
from scrapy.utils.python import without_none_values

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from _typeshed import OpenBinaryMode

    from scrapy.crawler import Crawler
    from scrapy.exporters import BaseItemExporter
    from scrapy.settings import BaseSettings, Settings


logger = logging.getLogger(__name__)


# Printf-style placeholders (e.g. %(time)s) used to build feed URIs. Any other
# percent character in a URI (e.g. percent-encoding such as %20 or %23) must be
# treated as a literal rather than as the start of a placeholder.
_FEED_URI_PLACEHOLDER_RE = re.compile(
    r"%\([^)]+\)[-+ #0]*(?:\d+|\*)?(?:\.(?:\d+|\*))?[diouxXeEfFgGcrsa]"
)


def apply_uri_params(uri_template: str, uri_params: dict[str, Any]) -> str:
    """Return *uri_template* with its ``%(...)s`` placeholders replaced using
    *uri_params*, leaving any other percent character untouched.

    This allows feed URIs to contain percent-encoded characters (e.g. ``%20``
    in a path with spaces or ``%23`` in FTP credentials) without them being
    misinterpreted as printf-style formatting directives.
    """
    parts: list[str] = []
    last = 0
    for match in _FEED_URI_PLACEHOLDER_RE.finditer(uri_template):
        parts.append(uri_template[last : match.start()].replace("%", "%%"))
        parts.append(match.group(0))
        last = match.end()
    parts.append(uri_template[last:].replace("%", "%%"))
    return "".join(parts) % uri_params


UriParamsCallableT: TypeAlias = Callable[
    [dict[str, Any], Spider], dict[str, Any] | None
]

_ItemProcessor: TypeAlias = Callable[[Any], Iterable[Any]]


_FEED_MODES: frozenset[str] = frozenset({"append", "create", "overwrite"})


def _get_mode(storage: Any, feed_options: dict[str, Any] | None, legacy: str) -> str:
    """Return the mode of *storage*, based on the *mode* feed option, or on the
    deprecated *overwrite* feed option, or, if neither is set, on *legacy*,
    which must be the mode that the storage used before the *mode* feed option
    existed."""
    feed_options = feed_options or {}
    mode = feed_options.get("mode")
    if mode is None:
        overwrite = feed_options.get("overwrite")
        mode = legacy if overwrite is None else "overwrite" if overwrite else "append"
    _check_mode(mode, storage)
    return mode


def _check_mode(mode: str, storage: Any, uri: str | None = None) -> None:
    if mode not in _FEED_MODES:
        raise ValueError(
            f"Invalid feed mode: {mode!r}. Supported modes: "
            f"{', '.join(sorted(_FEED_MODES))}."
        )
    supported: frozenset[str] | None = getattr(storage, "supported_modes", None)
    if supported is not None and mode not in supported:
        suffix = f" (feed URI: {uri})" if uri else ""
        raise ValueError(
            f"{type(storage).__name__} does not support the {mode!r} feed "
            f"mode{suffix}. Supported modes: {', '.join(sorted(supported))}."
        )


class ItemFilter:
    """
    This will be used by FeedExporter to decide if an item should be allowed
    to be exported to a particular feed.

    :param feed_options: feed specific options passed from FeedExporter
    :type feed_options: dict
    """

    feed_options: dict[str, Any] | None
    item_classes: tuple[type, ...]

    def __init__(self, feed_options: dict[str, Any] | None) -> None:
        self.feed_options = feed_options
        if feed_options is not None:
            self.item_classes = tuple(
                load_object(item_class)
                for item_class in feed_options.get("item_classes") or ()
            )
        else:
            self.item_classes = ()

    def accepts(self, item: Any) -> bool:
        """
        Return ``True`` if `item` should be exported or ``False`` otherwise.

        :param item: scraped item which user wants to check if is acceptable
        :type item: :ref:`Scrapy items <topics-items>`
        :return: `True` if accepted, `False` otherwise
        :rtype: bool
        """
        if self.item_classes:
            return isinstance(item, self.item_classes)
        return True  # accept all items by default


class _IFeedStorage(Interface):  # type: ignore[misc]  # pragma: no cover
    # pylint: disable=no-self-argument

    def __init__(uri, *, feed_options=None): ...  # type: ignore[no-untyped-def]  # pylint: disable=super-init-not-called

    def open(spider): ...  # type: ignore[no-untyped-def]

    def store(file): ...  # type: ignore[no-untyped-def]


class FeedStorageProtocol(Protocol):
    """Protocol that all Feed Storages must follow.

    :param uri: Feed URI.

    :param feed_options: :ref:`Feed options <feed-options>` of the feed.

    A feed storage may define a ``from_crawler(cls, crawler, uri, *,
    feed_options=None)`` class method instead of ``__init__``, to also get the
    :class:`~scrapy.crawler.Crawler` object.
    """

    def open(self, spider: Spider) -> IO[bytes] | Coroutine[Any, Any, IO[bytes]]:
        """Open the storage for *spider* and return the file-like object that
        item exporters write into.

        .. versionchanged:: 2.20.0
           This method may now be a coroutine function (``async def``).
        """

    def store(
        self, file: IO[bytes]
    ) -> Coroutine[Any, Any, None] | Deferred[None] | None:
        """Store *file*, the file-like object returned by :meth:`open`, and
        close it.

        .. versionchanged:: 2.20.0
           This method may now be a coroutine function (``async def``).
        """


class BlockingFeedStorage(ABC):
    """Base class for feed storages that store feeds using blocking code."""

    def open(self, spider: Spider) -> IO[bytes]:
        path = spider.crawler.settings["FEED_TEMPDIR"]
        if path and not Path(path).is_dir():
            raise OSError("Not a Directory: " + str(path))

        return NamedTemporaryFile(prefix="feed-", dir=path)

    def store(self, file: IO[bytes]) -> Deferred[None]:
        return deferred_from_coro(run_in_thread(self._store_in_thread, file))

    @abstractmethod
    def _store_in_thread(self, file: IO[bytes]) -> None:
        """Store *file* and close it.

        This method runs in a separate thread, so it may use blocking code.
        """
        raise NotImplementedError


class StdoutFeedStorage:
    """:ref:`Standard output <topics-feed-storage-stdout>` storage backend."""

    # The mode is irrelevant here: writing to a stream cannot destroy data.
    supported_modes: frozenset[str] = _FEED_MODES

    def __init__(
        self,
        uri: str,
        _stdout: IO[bytes] | None = None,
        *,
        feed_options: dict[str, Any] | None = None,
    ):
        if not _stdout:
            _stdout = sys.stdout.buffer
        self._stdout: IO[bytes] = _stdout

    def open(self, spider: Spider) -> IO[bytes]:
        return self._stdout

    def store(self, file: IO[bytes]) -> Deferred[None] | None:
        pass


_WRITE_MODES: dict[str, OpenBinaryMode] = {
    "append": "ab",
    "create": "xb",
    "overwrite": "wb",
}


def _uri_to_local_path(uri: str) -> str:
    return file_uri_to_path(uri) if uri.startswith("file:") else uri


def _completion_marker(staging_path: str) -> Path:
    return Path(f"{staging_path}.complete")


def _seal_staging_file(staging_path: str) -> None:
    """Mark the staged file at *staging_path* as complete, so that a later
    run may publish it but never publishes a half-written one."""
    _completion_marker(staging_path).touch()


def _is_staging_complete(staging_path: str) -> bool:
    return Path(staging_path).exists() and _completion_marker(staging_path).exists()


def _discard_staging_file(staging_path: str) -> None:
    Path(staging_path).unlink(missing_ok=True)
    _completion_marker(staging_path).unlink(missing_ok=True)


def _commit_staging_file(
    staging_path: str, final_path: str, write_mode: OpenBinaryMode
) -> None:
    """Publish the complete feed file at *staging_path* to *final_path*.

    In the ``create`` and ``overwrite`` modes the staging file is moved, so
    *final_path* only ever appears as a complete file. In the ``append`` mode
    the staged data is appended to *final_path* instead, which cannot be
    atomic. If anything fails, the staging file is left in place so that the
    commit can be retried later.
    """
    staging = Path(staging_path)
    final = Path(final_path)
    if write_mode == "ab":
        with staging.open("rb") as source, final.open("ab") as target:
            shutil.copyfileobj(source, target)
        staging.unlink()
    elif write_mode == "xb" and final.exists():
        raise FileExistsError(final_path)
    else:
        staging.replace(final)
    _completion_marker(staging_path).unlink(missing_ok=True)


class FileFeedStorage:
    """:ref:`Local filesystem <topics-feed-storage-fs>` storage backend.

    *uri* may be a ``file://`` URI or a plain path. Missing parent directories
    are created when the feed is opened.

    When the *atomic* feed option is enabled, items are written to a staging
    file next to the target, and the target only receives the file once the
    batch is complete. The staging file path is then available as
    :attr:`staging_path`.
    """

    supported_modes: frozenset[str] = _FEED_MODES

    def __init__(self, uri: str, *, feed_options: dict[str, Any] | None = None):
        self.path: str = _uri_to_local_path(uri)
        self.write_mode: OpenBinaryMode = _WRITE_MODES[
            _get_mode(self, feed_options, "append")
        ]
        self.staging_path: str | None = None
        if (feed_options or {}).get("atomic"):
            self.staging_path = f"{self.path}.part-{uuid4().hex}"

    def open(self, spider: Spider) -> IO[bytes]:
        dirname = Path(self.path).parent
        if dirname and not dirname.exists():
            dirname.mkdir(parents=True)
        if self.staging_path is None:
            return Path(self.path).open(self.write_mode)
        if self.write_mode == "xb" and Path(self.path).exists():
            # Report the conflict before any item is written, as the
            # non-atomic create mode does, instead of on commit.
            raise FileExistsError(self.path)
        return Path(self.staging_path).open("wb")

    def store(self, file: IO[bytes]) -> Deferred[None] | None:
        file.close()
        if self.staging_path is not None:
            _seal_staging_file(self.staging_path)
            _commit_staging_file(self.staging_path, self.path, self.write_mode)
        return None


class S3FeedStorage(BlockingFeedStorage):
    """:ref:`Amazon S3 <topics-feed-storage-s3>` storage backend."""

    supported_modes: frozenset[str] = frozenset({"create", "overwrite"})

    def __init__(
        self,
        uri: str,
        access_key: str | None = None,
        secret_key: str | None = None,
        acl: str | None = None,
        endpoint_url: str | None = None,
        *,
        feed_options: dict[str, Any] | None = None,
        session_token: str | None = None,
        region_name: str | None = None,
        max_pool_connections: int | None = None,
        upload_timeout: float | None = None,
    ):
        try:
            import boto3.session  # noqa: PLC0415
        except ImportError:
            raise NotConfigured("missing boto3 library") from None
        from botocore.config import Config  # noqa: PLC0415

        u = urlparse(uri)
        assert u.hostname
        self.bucketname: str = u.hostname
        self.access_key: str | None = u.username or access_key
        self.secret_key: str | None = u.password or secret_key
        self.session_token: str | None = session_token
        self.keyname: str = u.path[1:]  # remove first "/"
        self.acl: str | None = acl
        self.endpoint_url: str | None = endpoint_url
        self.region_name: str | None = region_name
        self.max_pool_connections: int | None = max_pool_connections
        self.upload_timeout: float | None = upload_timeout

        config_kwargs: dict[str, Any] = {}
        if max_pool_connections is not None:
            config_kwargs["max_pool_connections"] = max_pool_connections
        if upload_timeout is not None:
            config_kwargs["read_timeout"] = upload_timeout

        self._client_kwargs: dict[str, Any] = {
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "aws_session_token": self.session_token,
            "endpoint_url": self.endpoint_url,
            "region_name": self.region_name,
            "config": Config(**config_kwargs),
        }

        # Synchronous boto3 client, used when asyncio support or aioboto3 is not
        # available (its calls are then run in a thread).
        boto3_session = boto3.session.Session()
        self.s3_client = boto3_session.client("s3", **self._client_kwargs)

        self._mode: str = _get_mode(self, feed_options, "overwrite")

    @classmethod
    def from_crawler(
        cls,
        crawler: Crawler,
        uri: str,
        *,
        feed_options: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            uri,
            access_key=crawler.settings.getsecret("AWS_ACCESS_KEY_ID"),
            secret_key=crawler.settings.getsecret("AWS_SECRET_ACCESS_KEY"),
            session_token=crawler.settings.getsecret("AWS_SESSION_TOKEN"),
            acl=crawler.settings["FEED_STORAGE_S3_ACL"] or None,
            endpoint_url=crawler.settings["AWS_ENDPOINT_URL"] or None,
            region_name=crawler.settings["AWS_REGION_NAME"] or None,
            max_pool_connections=_get_max_pool_connections(crawler.settings),
            upload_timeout=crawler.settings.getfloat("UPLOAD_TIMEOUT") or None,
            feed_options=feed_options,
        )

    def store(self, file: IO[bytes]) -> Deferred[None]:
        if is_asyncio_available() and is_aioboto3_available():
            return deferred_from_coro(self._store_async(file))
        return super().store(file)

    async def _store_async(self, file: IO[bytes]) -> None:
        import aioboto3  # noqa: PLC0415

        file.seek(0)
        extra_args: dict[str, Any] = {"ACL": self.acl} if self.acl else {}
        session = aioboto3.Session()
        try:
            async with session.client("s3", **self._client_kwargs) as client:
                if self._mode == "create":
                    with self._raise_if_exists():
                        await client.put_object(
                            Bucket=self.bucketname,
                            Key=self.keyname,
                            Body=file,
                            IfNoneMatch="*",
                            **extra_args,
                        )
                else:
                    await client.upload_fileobj(
                        Fileobj=file,
                        Bucket=self.bucketname,
                        Key=self.keyname,
                        ExtraArgs=extra_args,
                    )
        finally:
            file.close()

    @contextlib.contextmanager
    def _raise_if_exists(self) -> Iterator[None]:
        from botocore.exceptions import ClientError  # noqa: PLC0415

        try:
            yield
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "PreconditionFailed":
                raise FileExistsError(
                    f"s3://{self.bucketname}/{self.keyname} already exists"
                ) from error
            raise

    def _store_in_thread(self, file: IO[bytes]) -> None:
        file.seek(0)
        extra_args: dict[str, Any] = {"ACL": self.acl} if self.acl else {}
        try:
            if self._mode == "create":
                # upload_fileobj() does not allow IfNoneMatch
                # (https://github.com/boto/boto3/issues/4366).
                with self._raise_if_exists():
                    self.s3_client.put_object(
                        Bucket=self.bucketname,
                        Key=self.keyname,
                        Body=file,
                        IfNoneMatch="*",
                        **extra_args,
                    )
            elif extra_args:
                self.s3_client.upload_fileobj(
                    Bucket=self.bucketname,
                    Key=self.keyname,
                    Fileobj=file,
                    ExtraArgs=extra_args,
                )
            else:
                self.s3_client.upload_fileobj(
                    Bucket=self.bucketname,
                    Key=self.keyname,
                    Fileobj=file,
                )
        finally:
            file.close()


class GCSFeedStorage(BlockingFeedStorage):
    """:ref:`GCS <topics-feed-storage-gcs>` storage backend."""

    supported_modes: frozenset[str] = _FEED_MODES

    def __init__(
        self,
        uri: str,
        project_id: str | None,
        acl: str | None,
        *,
        feed_options: dict[str, Any] | None = None,
        timeout: float | None = None,
    ):
        self.project_id: str | None = project_id
        self.acl: str | None = acl
        self.timeout: float | None = timeout
        u = urlparse(uri)
        assert u.hostname
        self.bucket_name: str = u.hostname
        self.blob_name: str = u.path[1:]  # remove first "/"

        self._mode: str = _get_mode(self, feed_options, "overwrite")

    @classmethod
    def from_crawler(
        cls,
        crawler: Crawler,
        uri: str,
        *,
        feed_options: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            uri,
            crawler.settings["GCS_PROJECT_ID"],
            crawler.settings["FEED_STORAGE_GCS_ACL"] or None,
            feed_options=feed_options,
            timeout=crawler.settings.getfloat("UPLOAD_TIMEOUT") or None,
        )

    def _store_in_thread(self, file: IO[bytes]) -> None:
        from google.api_core.exceptions import PreconditionFailed  # noqa: PLC0415
        from google.cloud.storage import Client  # noqa: PLC0415

        file.seek(0)
        try:
            client = Client(project=self.project_id)
            bucket = client.bucket(self.bucket_name)
            kwargs: dict[str, Any] = (
                {} if self.timeout is None else {"timeout": self.timeout}
            )
            blob = (
                bucket.get_blob(self.blob_name, **kwargs)
                if self._mode == "append"
                else None
            )
            if blob is None:
                upload_kwargs = (
                    {**kwargs, "if_generation_match": 0}
                    if self._mode == "create"
                    else kwargs
                )
                try:
                    bucket.blob(self.blob_name).upload_from_file(
                        file, predefined_acl=self.acl, **upload_kwargs
                    )
                except PreconditionFailed as error:
                    raise FileExistsError(
                        f"gs://{self.bucket_name}/{self.blob_name} already exists"
                    ) from error
                return
            # Appending uploads the new data as a separate object and composes
            # it with the existing one, leaving the data already stored
            # untouched. Composition resolves its sources by name, so it always
            # appends to the latest version of the blob.
            part = bucket.blob(f"{self.blob_name}.{uuid4().hex}.part")
            part.upload_from_file(file, predefined_acl=self.acl, **kwargs)
            try:
                blob.compose([blob, part], client=client, **kwargs)
            finally:
                part.delete(client=client, **kwargs)
            if self.acl:
                blob.acl.save_predefined(self.acl, client=client, **kwargs)
        finally:
            file.close()


class FTPFeedStorage(BlockingFeedStorage):
    """:ref:`FTP <feed-storage-ftp>` storage backend, which also handles
    :ref:`FTPS <feed-storage-ftps>` when *uri* uses the ``ftps`` scheme."""

    supported_modes: frozenset[str] = _FEED_MODES

    def __init__(
        self,
        uri: str,
        use_active_mode: bool = False,
        *,
        feed_options: dict[str, Any] | None = None,
        timeout: float | None = None,
    ):
        u = urlparse(uri)
        if not u.hostname:
            raise ValueError(f"Got a storage URI without a hostname: {uri}")
        self.host: str = u.hostname
        self.port: int = int(u.port or "21")
        self.username: str = u.username or ""
        self.password: str = unquote(u.password or "")
        self.path: str = u.path
        self.tls: bool = u.scheme == "ftps"
        self.use_active_mode: bool = use_active_mode
        self.timeout: float | None = timeout
        self._mode: str = _get_mode(self, feed_options, "overwrite")

    @property
    def overwrite(self) -> bool:
        warnings.warn(
            "FTPFeedStorage.overwrite is deprecated, use the mode feed option instead.",
            ScrapyDeprecationWarning,
            stacklevel=2,
        )
        return self._mode != "append"

    @classmethod
    def from_crawler(
        cls,
        crawler: Crawler,
        uri: str,
        *,
        feed_options: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            uri,
            use_active_mode=crawler.settings.getbool("FEED_STORAGE_FTP_ACTIVE"),
            feed_options=feed_options,
            timeout=crawler.settings.getfloat("UPLOAD_TIMEOUT") or None,
        )

    def _store_in_thread(self, file: IO[bytes]) -> None:
        ftp_store_file(
            path=self.path,
            file=file,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            use_active_mode=self.use_active_mode,
            mode=self._mode,
            tls=self.tls,
            timeout=self.timeout,
        )


class _BatchRecord:
    """The information needed to finish exporting a batch of an atomic feed
    whose commit did not complete during the run that created it."""

    __slots__ = (
        "batch_id",
        "itemcount",
        "items",
        "slot",
        "staging_path",
        "storage_name",
        "uri",
    )

    def __init__(
        self,
        batch_id: int,
        uri: str,
        staging_path: str,
        storage_name: str,
        *,
        items: list[Any] | None = None,
        itemcount: int = 0,
        slot: FeedSlot | None = None,
    ):
        self.batch_id: int = batch_id
        # The resolved feed URI of the batch, i.e. its identity: a retry
        # commits to the same target even if the template uses batch_time.
        self.uri: str = uri
        self.staging_path: str = staging_path
        self.storage_name: str = storage_name
        # The items exported into the batch, or None when they are not
        # tracked (see FeedSlot._pending_items).
        self.items: list[Any] | None = items
        # The number of items exported into the batch, which decides whether
        # closing it stores it at all (see FeedExporter._close_slot()).
        self.itemcount: int = itemcount
        # The slot of the batch while the run that created it is active,
        # whose values replace the saved ones when the state is saved.
        self.slot: FeedSlot | None = slot

    def to_state(self) -> dict[str, Any]:
        items = self.slot._pending_items if self.slot is not None else self.items
        itemcount = self.slot.itemcount if self.slot is not None else self.itemcount
        return {
            "batch_id": self.batch_id,
            "uri": self.uri,
            "staging": self.staging_path,
            "storage_name": self.storage_name,
            "itemcount": itemcount,
            "items": list(items or ()),
        }

    @classmethod
    def from_state(cls, state: dict[str, Any]) -> Self:
        return cls(
            state["batch_id"],
            state["uri"],
            state["staging"],
            state["storage_name"],
            items=list(state["items"]),
            itemcount=state["itemcount"],
        )


class FeedSlot:
    def __init__(
        self,
        storage: FeedStorageProtocol,
        uri: str,
        format: str,  # noqa: A002
        store_empty: bool,
        batch_id: int,
        uri_template: str,
        filter: ItemFilter,  # noqa: A002
        feed_options: dict[str, Any],
        spider: Spider,
        exporters: dict[str, type[BaseItemExporter]],
        settings: BaseSettings,
        crawler: Crawler,
    ):
        self.file: IO[bytes] | None = None
        self.exporter: BaseItemExporter | None = None
        self.storage: FeedStorageProtocol = storage
        # feed params
        self.batch_id: int = batch_id
        self.format: str = format
        self.store_empty: bool = store_empty
        self.uri_template: str = uri_template
        self.uri: str = uri
        self.filter: ItemFilter = filter
        # exporter params
        self.feed_options: dict[str, Any] = feed_options
        self.spider: Spider = spider
        self.exporters: dict[str, type[BaseItemExporter]] = exporters
        self.settings: BaseSettings = settings
        self.crawler: Crawler = crawler
        # flags
        self.itemcount: int = 0
        self._skipped: bool = False
        self._exporting: bool = False
        self._fileloaded: bool = False
        # Whether closing this slot has been scheduled, so that a batch is not
        # stored twice when its close is scheduled from several places.
        self._close_scheduled: bool = False
        # Outcome of the commit of this slot to its target: "staging" while
        # its data has not been stored yet, "committed" once the target holds
        # the complete batch, and "failed" when the commit failed.
        self.commit_state: str = "staging"
        # Items of this batch, kept only for atomic batched feeds, so that a
        # resumed crawl can export them again if their staging data is lost.
        self._pending_items: list[Any] | None = (
            []
            if feed_options.get("batch_item_count")
            and getattr(storage, "staging_path", None) is not None
            else None
        )

    async def start_exporting(self) -> None:
        if not self._fileloaded:
            self.file = await _maybe_coroutine(self.storage.open, self.spider)
            if "postprocessing" in self.feed_options:
                self.file = cast(
                    "IO[bytes]",
                    PostProcessingManager(
                        self.feed_options["postprocessing"],
                        self.file,
                        self.feed_options,
                    ),
                )
            self.exporter = self._get_exporter(
                file=self.file,
                format_=self.feed_options["format"],
                fields_to_export=self.feed_options["fields"],
                encoding=self.feed_options["encoding"],
                indent=self.feed_options["indent"],
                **self.feed_options["item_export_kwargs"],
            )
            self._fileloaded = True

        if not self._exporting:
            assert self.exporter
            await _maybe_coroutine(self.exporter.start_exporting)
            self._exporting = True

    def _get_exporter(
        self, file: IO[bytes], format_: str, *args: Any, **kwargs: Any
    ) -> BaseItemExporter:
        return build_from_crawler(
            self.exporters[format_], self.crawler, file, *args, **kwargs
        )

    async def finish_exporting(self) -> None:
        if self._exporting:  # pragma: no branch
            assert self.exporter
            await _maybe_coroutine(self.exporter.finish_exporting)
            self._exporting = False


class FeedExporter:
    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        exporter = cls(crawler)
        crawler.signals.connect(exporter.open_spider, signals.spider_opened)
        crawler.signals.connect(exporter._on_state_loaded, signals.spider_state_loaded)
        crawler.signals.connect(exporter._on_engine_started, signals.engine_started)
        crawler.signals.connect(exporter.close_spider, signals.spider_closed)
        crawler.signals.connect(exporter._on_state_saving, signals.spider_state_saving)
        crawler.signals.connect(exporter.item_scraped, signals.item_scraped)
        return exporter

    def __init__(self, crawler: Crawler):
        self.crawler: Crawler = crawler
        self.settings: Settings = crawler.settings
        self.feeds: dict[str, dict[str, Any]] = {}
        self.slots: list[FeedSlot] = []
        self.filters: dict[str, ItemFilter] = {}
        self.processors: dict[str, _ItemProcessor | None] = {}
        self._pending_close_tasks: list[asyncio.Task[None] | Deferred[None]] = []
        # Batches of atomic feeds that are not committed yet, per feed
        # template, in batch order. They are saved to the spider state so
        # that a resumed crawl can finish their export.
        self._batch_records: dict[str, list[_BatchRecord]] = {}
        # Item export may await user-defined code (e.g. an async def
        # export_item() method), while item_scraped signals are sent
        # concurrently (see the CONCURRENT_ITEMS setting). This lock prevents
        # overlapping item export calls, which item exporters, being stateful,
        # cannot handle.
        self._export_lock = DeferredLock()

        if not self.settings["FEEDS"] and not self.settings["FEED_URI"]:
            raise NotConfigured

        # Begin: Backward compatibility for FEED_URI and FEED_FORMAT settings
        if self.settings["FEED_URI"]:
            warnings.warn(
                "The `FEED_URI` and `FEED_FORMAT` settings have been deprecated in favor of "
                "the `FEEDS` setting. Please see the `FEEDS` setting docs for more details",
                category=ScrapyDeprecationWarning,
                stacklevel=2,
            )
            uri = self.settings["FEED_URI"]
            # handle pathlib.Path objects
            uri = str(uri.absolute()) if isinstance(uri, Path) else str(uri)
            feed_options = {"format": self.settings["FEED_FORMAT"]}
            self.feeds[uri] = feed_complete_default_values_from_settings(
                feed_options, self.settings, uri
            )
            self.filters[uri] = self._load_filter(feed_options)
            self.processors[uri] = self._load_processor(feed_options)
        # End: Backward compatibility for FEED_URI and FEED_FORMAT settings

        # 'FEEDS' setting takes precedence over 'FEED_URI'
        for settings_uri, feed_options in self.settings.getdict("FEEDS").items():
            # handle pathlib.Path objects
            uri = (
                str(settings_uri.absolute())
                if isinstance(settings_uri, Path)
                else str(settings_uri)
            )
            self.feeds[uri] = feed_complete_default_values_from_settings(
                feed_options, self.settings, uri
            )
            self.filters[uri] = self._load_filter(feed_options)
            self.processors[uri] = self._load_processor(feed_options)

        self.storages: dict[str, type[FeedStorageProtocol]] = self._load_components(
            "FEED_STORAGES"
        )
        self.exporters: dict[str, type[BaseItemExporter]] = self._load_components(
            "FEED_EXPORTERS"
        )
        if any(
            feed_options.get("mode") is None for feed_options in self.feeds.values()
        ):
            warnings.warn(
                "The default value of the FEED_MODE setting will change from "
                "None to 'create' in a future Scrapy version, i.e. Scrapy will "
                "stop writing feeds whose target already exists. Explicitly "
                "set FEED_MODE, or the mode feed option of every feed, to "
                "silence this warning.",
                category=ScrapyDeprecationWarning,
                stacklevel=2,
            )

        for uri, feed_options in self.feeds.items():
            if not self._storage_supported(uri, feed_options):
                raise NotConfigured
            if not self._settings_are_valid():
                raise NotConfigured
            if not self._exporter_supported(feed_options["format"]):
                raise NotConfigured
            if feed_options.get("atomic") and not self._storage_is_atomic(
                uri, feed_options
            ):
                storage_cls = self.storages.get(
                    urlparse(uri).scheme, self.storages["file"]
                )
                logger.warning(
                    f"The atomic feed option of the {uri} feed is only "
                    f"supported for local file system feeds; it is ignored by "
                    f"{getattr(storage_cls, '__name__', storage_cls)}."
                )

    def open_spider(self, spider: Spider) -> None:
        self._slots_initialized = False
        for uri, feed_options in self.feeds.items():
            uri_params = self._get_uri_params(spider, feed_options["uri_params"])
            try:
                resolved_uri = apply_uri_params(uri, uri_params)
            except KeyError as exc:
                logger.error(
                    f"Feed {uri!r} could not be opened: it contains a "
                    f"placeholder for {exc}, which is not a spider "
                    f"attribute and was not provided by the uri_params "
                    f"function."
                )
                continue
            slot = self._start_new_batch(
                batch_id=1,
                uri=resolved_uri,
                feed_options=feed_options,
                spider=spider,
                uri_template=uri,
            )
            self.slots.append(slot)
            self._track_batch(slot)

    async def _on_state_loaded(self, state: dict[str, Any]) -> None:
        """Apply the state saved by the previous run to the initial slots.

        Called via the spider_state_loaded signal after SpiderState has
        populated spider.state. Safe to mutate slots here because FeedSlot
        opens its file lazily; no I/O has occurred yet.
        """
        saved_records = {
            uri_template: [_BatchRecord.from_state(record) for record in records]
            for uri_template, records in state.get("feed_atomic_batches", {}).items()
        }
        slot_templates = {slot.uri_template for slot in self.slots}
        # Keep the batches of the feeds that have no slot in this run, so that
        # their export can still be retried if the feed is added back.
        for uri_template, records in saved_records.items():
            if uri_template not in slot_templates:
                self._batch_records[uri_template] = records
        feed_batch_ids: dict[str, int] = state.get("feed_batch_ids", {})
        spider = self.crawler.spider
        assert spider is not None
        for index, slot in enumerate(self.slots):
            records = saved_records.pop(slot.uri_template, [])
            # The record of the initial batch does not apply anymore: the
            # saved state or the recovery below replaces it.
            self._batch_records.pop(slot.uri_template, None)
            saved_id = feed_batch_ids.get(slot.uri_template, 0)
            if records and getattr(slot.storage, "staging_path", None) is not None:
                self.slots[index] = await self._recover_atomic_feed(
                    slot, records, saved_id, spider
                )
                continue
            if records:
                # The feed is no longer atomic, so its staged data cannot be
                # published, but leaving it behind is safer than deleting it.
                logger.warning(
                    f"Ignoring the unsaved batches of {slot.uri_template} from "
                    f"the previous run: they were written to staging files, "
                    f"which are only recovered when the atomic feed option is "
                    f"enabled.",
                    extra={"spider": spider},
                )
            if saved_id:
                uri_params = self._get_uri_params(
                    spider, self.feeds[slot.uri_template]["uri_params"]
                )
                uri_params["batch_id"] = saved_id
                slot.batch_id = saved_id
                slot.uri = apply_uri_params(slot.uri_template, uri_params)
                slot.storage = self._get_storage(
                    slot.uri, self.feeds[slot.uri_template]
                )
            self._track_batch(slot)
        self._slots_initialized = True
        await self.crawler.signals.send_catch_log_async(
            signals.feed_slots_initialized, slots=self.slots
        )

    async def _recover_atomic_feed(
        self,
        slot: FeedSlot,
        records: list[_BatchRecord],
        next_batch_id: int,
        spider: Spider,
    ) -> FeedSlot:
        """Finish the export of the batches that the previous run left
        uncommitted, and return the slot that this run continues with.

        *next_batch_id* is the batch that the previous run saved for this
        feed: the batch that was in progress, or the following one if it was
        stored.
        """
        feed_options = self.feeds[slot.uri_template]
        write_mode = _WRITE_MODES[_get_mode(slot.storage, feed_options, "append")]
        if next_batch_id < 1:
            next_batch_id = max(record.batch_id for record in records)
        closed_records = [
            record for record in records if record.batch_id < next_batch_id
        ]
        for record in closed_records:
            await self._recover_closed_batch(
                slot.uri_template, record, feed_options, write_mode, spider
            )
        open_record = next(
            (record for record in records if record.batch_id == next_batch_id), None
        )
        if open_record is None:
            # The batch in progress reached its target or was not stored, so
            # this run starts a new batch.
            self._set_slot_batch(slot, next_batch_id, spider)
            return slot
        return await self._recover_open_batch(
            slot, open_record, feed_options, write_mode, spider
        )

    async def _recover_closed_batch(
        self,
        uri_template: str,
        record: _BatchRecord,
        feed_options: dict[str, Any],
        write_mode: OpenBinaryMode,
        spider: Spider,
    ) -> None:
        """Publish a batch that was complete but not committed when the
        previous run saved its state, or export its items again if the
        previous run did not finish writing it."""
        if _is_staging_complete(record.staging_path):
            if self._publish_record(record, write_mode, spider) == "failed":
                self._keep_record(uri_template, record)
            return
        if not Path(record.staging_path).exists():  # noqa: ASYNC240
            if Path(_uri_to_local_path(record.uri)).exists():  # noqa: ASYNC240
                # The batch reached its target already.
                return
            if not record.items:
                # Its staged data is gone and it cannot be written again.
                return
            # The staged data was discarded after a failed re-export, so
            # write the batch again from its saved items.
        else:
            # The previous run stopped while writing the batch, so the staged
            # data is incomplete and must not be published.
            _discard_staging_file(record.staging_path)
            if not record.items:
                logger.warning(
                    f"Cannot recover the batch of {record.uri} from the "
                    f"previous run: its staged data was incomplete and its "
                    f"items were not saved.",
                    extra={"spider": spider},
                )
                return
        retry_record = await self._export_record_again(
            uri_template, record, feed_options, spider
        )
        if retry_record is not None:
            self._keep_record(uri_template, retry_record)

    async def _recover_open_batch(
        self,
        slot: FeedSlot,
        record: _BatchRecord,
        feed_options: dict[str, Any],
        write_mode: OpenBinaryMode,
        spider: Spider,
    ) -> FeedSlot:
        """Publish the data of *record* or export its items again as needed,
        and point *slot* at the batch that this run continues with."""
        if not feed_options["batch_item_count"]:
            # A feed without batches writes its whole output into a single
            # target, and the resumed run writes it again from scratch, so the
            # staged data of the previous run cannot be recovered.
            _discard_staging_file(record.staging_path)
            logger.warning(
                f"Cannot recover the feed {record.uri} from the previous run: "
                f"a feed without batches is written again from scratch when "
                f"the crawl is resumed.",
                extra={"spider": spider},
            )
            self._set_slot_batch(slot, record.batch_id, spider)
            return slot
        uri_template = slot.uri_template
        next_batch_id = record.batch_id + 1
        if _is_staging_complete(record.staging_path):
            # The batch was written completely, but its commit did not finish.
            if self._publish_record(record, write_mode, spider) == "failed":
                self._keep_record(uri_template, record)
            self._set_slot_batch(slot, next_batch_id, spider)
            return slot
        if Path(record.staging_path).exists():  # noqa: ASYNC240
            # The previous run stopped while writing the batch, so the staged
            # data is incomplete and must not be published.
            _discard_staging_file(record.staging_path)
            if not record.items:
                if (
                    feed_options["store_empty"]
                    and record.batch_id == 1
                    # The empty first batch may have been stored before the
                    # crawl was interrupted; keeping its identity could then
                    # conflict with its own target.
                    and Path(_uri_to_local_path(record.uri)).exists()  # noqa: ASYNC240
                ):
                    self._set_slot_batch(slot, next_batch_id, spider)
                    return slot
                # Nothing was exported into the batch, so it can be reused.
                self._set_slot_batch(slot, record.batch_id, spider)
                return slot
            return await self._resume_batch(slot, record, spider)
        # The staged data is gone: the batch either reached its target after
        # the state was saved, or its data was lost.
        if Path(_uri_to_local_path(record.uri)).exists():  # noqa: ASYNC240
            self._set_slot_batch(slot, next_batch_id, spider)
            return slot
        if not record.items:
            self._set_slot_batch(slot, record.batch_id, spider)
            return slot
        return await self._resume_batch(slot, record, spider)

    async def _resume_batch(
        self, slot: FeedSlot, record: _BatchRecord, spider: Spider
    ) -> FeedSlot:
        """Export the items of *record* again into a batch with its identity,
        and return the slot that the following items go into."""
        slot.batch_id = record.batch_id
        slot.uri = record.uri
        slot.storage = self._get_storage(slot.uri, self.feeds[slot.uri_template])
        items = record.items or []
        # The items of the previous run are already part of the batch, and the
        # items of this run extend it.
        slot._pending_items = list(items)
        self._track_batch(slot)
        try:
            await self._export_items(slot, items)
        except FileExistsError:
            # The target appeared after the previous run saved its state, so
            # this batch cannot be written again in the create mode.
            staging_path = cast("str", getattr(slot.storage, "staging_path", None))
            _discard_staging_file(staging_path)
            logger.error(
                f"Not writing {slot.uri} because it already exists and the "
                f"feed mode is 'create'; the recovered items of the batch are "
                f"lost.",
                extra={"spider": spider},
            )
            assert self.crawler.stats
            self.crawler.stats.inc_value(f"feedexport/conflicts/{record.storage_name}")
            self._untrack_batch(slot)
            self._set_slot_batch(slot, record.batch_id + 1, spider)
            return slot
        except Exception:
            # Keep the record so that a later run can export the items again,
            # and let the items of this run go into a fresh batch of the same
            # feed.
            logger.exception(
                f"Error exporting the recovered items of {record.uri}; they "
                f"will be exported again when the crawl is resumed again.",
                extra={"spider": spider},
            )
            self._reset_slot(slot)
            self._keep_record(slot.uri_template, record)
            self._set_slot_batch(slot, record.batch_id + 1, spider)
            return slot
        slot.itemcount = len(items)
        batch_item_count = self.feeds[slot.uri_template]["batch_item_count"]
        if slot.itemcount < batch_item_count:
            return slot
        # The batch is full again, so this run continues with the next one.
        next_slot = self._next_batch_slot(slot, spider)
        self._schedule_slot_close(slot, spider)
        self._track_batch(next_slot)
        return next_slot

    @staticmethod
    def _close_slot_file(slot: FeedSlot) -> None:
        """Close the file of *slot*, if any, ignoring errors."""
        if slot.file is not None:  # pragma: no branch
            with contextlib.suppress(Exception):
                slot.file.close()
            slot.file = None

    def _reset_slot(self, slot: FeedSlot) -> None:
        """Close the partly written staging file of *slot* and reset its
        export state, so that it can export a new batch."""
        staging_path = cast("str", getattr(slot.storage, "staging_path", None))
        self._close_slot_file(slot)
        _discard_staging_file(staging_path)
        slot.exporter = None
        slot._exporting = False
        slot._fileloaded = False
        slot._skipped = False
        slot.itemcount = 0
        self._untrack_batch(slot)
        slot._pending_items = []

    def _next_batch_slot(self, slot: FeedSlot, spider: Spider) -> FeedSlot:
        feed_options = self.feeds[slot.uri_template]
        uri_params = self._get_uri_params(spider, feed_options["uri_params"], slot)
        return self._start_new_batch(
            batch_id=slot.batch_id + 1,
            uri=apply_uri_params(slot.uri_template, uri_params),
            feed_options=feed_options,
            spider=spider,
            uri_template=slot.uri_template,
        )

    def _set_slot_batch(self, slot: FeedSlot, batch_id: int, spider: Spider) -> None:
        """Make *slot* export the batch *batch_id* of its feed, starting from
        a clean staging file, and track it."""
        feed_options = self.feeds[slot.uri_template]
        uri_params = self._get_uri_params(spider, feed_options["uri_params"])
        uri_params["batch_id"] = batch_id
        slot.batch_id = batch_id
        slot.uri = apply_uri_params(slot.uri_template, uri_params)
        slot.storage = self._get_storage(slot.uri, feed_options)
        self._track_batch(slot)

    async def _export_record_again(
        self,
        uri_template: str,
        record: _BatchRecord,
        feed_options: dict[str, Any],
        spider: Spider,
    ) -> _BatchRecord | None:
        """Export the items of the complete batch *record* into a new file
        with the identity of *record*, and publish it.

        Return a record to retry on a later run if the commit fails, or None.
        """
        slot = self._start_new_batch(
            batch_id=record.batch_id,
            uri=record.uri,
            feed_options=feed_options,
            spider=spider,
            uri_template=uri_template,
        )
        items = record.items or []
        staging_path = cast("str", getattr(slot.storage, "staging_path", None))
        try:
            await self._export_items(slot, items)
        except Exception:
            # Close the partly written file and keep the record so that a
            # later run can export the items again.
            self._close_slot_file(slot)
            _discard_staging_file(staging_path)
            logger.exception(
                f"Error exporting the recovered items of {record.uri}; they "
                f"will be exported again when the crawl is resumed again.",
                extra={"spider": spider},
            )
            assert self.crawler.stats
            self.crawler.stats.inc_value(
                f"feedexport/failed_count/{record.storage_name}"
            )
            return record
        slot.itemcount = len(items)
        await slot.finish_exporting()
        assert self.crawler.stats
        self.crawler.stats.inc_value(
            f"feedexport/item_count/{record.storage_name}", slot.itemcount
        )
        try:
            await ensure_awaitable(slot.storage.store(self._get_file(slot)))
        except FileExistsError:
            _discard_staging_file(staging_path)
            self.crawler.stats.inc_value(f"feedexport/conflicts/{record.storage_name}")
            logger.error(
                f"Discarding the staged data of {record.uri}: the target "
                f"already exists and the feed mode is 'create'.",
                extra={"spider": spider},
            )
            return None
        except Exception:
            logger.exception(
                f"Error storing the recovered feed {record.uri}; it will be "
                f"retried when the crawl is resumed again.",
                extra={"spider": spider},
            )
            self.crawler.stats.inc_value(
                f"feedexport/failed_count/{record.storage_name}"
            )
            return _BatchRecord(
                record.batch_id,
                record.uri,
                staging_path,
                record.storage_name,
                itemcount=slot.itemcount,
                items=items,
            )
        self.crawler.stats.inc_value(f"feedexport/success_count/{record.storage_name}")
        logger.info(f"Stored the recovered feed {record.uri}", extra={"spider": spider})
        return None

    async def _export_items(self, slot: FeedSlot, items: list[Any]) -> None:
        await slot.start_exporting()
        assert slot.exporter
        for item in items:
            await _maybe_coroutine(slot.exporter.export_item, item)

    def _publish_record(
        self, record: _BatchRecord, write_mode: OpenBinaryMode, spider: Spider
    ) -> str:
        """Publish the complete staged data of *record* to its target.

        Return ``"committed"``, ``"failed"`` if the commit may be retried on a
        later run, or ``"conflict"`` if the target already exists in the
        ``create`` mode, in which case the staged data is discarded.
        """
        storage_name = record.storage_name
        assert self.crawler.stats
        try:
            _commit_staging_file(
                record.staging_path, _uri_to_local_path(record.uri), write_mode
            )
        except FileExistsError:
            _discard_staging_file(record.staging_path)
            self.crawler.stats.inc_value(f"feedexport/conflicts/{storage_name}")
            logger.error(
                f"Discarding the staged data of {record.uri} "
                f"({record.staging_path}): the target already exists and the "
                f"feed mode is 'create'.",
                extra={"spider": spider},
            )
            return "conflict"
        except Exception:
            logger.exception(
                f"Error committing the staged data of {record.uri} "
                f"({record.staging_path}); it will be retried when the crawl "
                f"is resumed again.",
                extra={"spider": spider},
            )
            self.crawler.stats.inc_value(f"feedexport/failed_count/{storage_name}")
            return "failed"
        self.crawler.stats.inc_value(f"feedexport/success_count/{storage_name}")
        logger.info(f"Stored the recovered feed {record.uri}", extra={"spider": spider})
        return "committed"

    def _track_batch(self, slot: FeedSlot) -> None:
        """Keep what is needed to finish exporting the batch of *slot* if this
        run ends before its commit does."""
        staging_path = getattr(slot.storage, "staging_path", None)
        if staging_path is None:
            return
        records = self._batch_records.setdefault(slot.uri_template, [])
        if any(record.staging_path == staging_path for record in records):
            return  # pragma: no cover
        records.append(
            _BatchRecord(
                slot.batch_id,
                slot.uri,
                staging_path,
                self._storage_name(slot.uri),
                itemcount=slot.itemcount,
                items=slot._pending_items,
                slot=slot,
            )
        )

    def _keep_record(self, uri_template: str, record: _BatchRecord) -> None:
        self._batch_records.setdefault(uri_template, []).append(record)

    def _untrack_batch(self, slot: FeedSlot) -> None:
        """Forget about the batch of *slot*, which cannot be recovered."""
        staging_path = getattr(slot.storage, "staging_path", None)
        records = self._batch_records.get(slot.uri_template)
        if staging_path is None or not records:
            return
        remaining = [r for r in records if r.staging_path != staging_path]
        if remaining:
            self._batch_records[slot.uri_template] = remaining
        else:
            del self._batch_records[slot.uri_template]

    def _storage_name(self, uri: str) -> str:
        cls = self.storages.get(urlparse(uri).scheme, self.storages["file"])
        return getattr(cls, "__name__", str(cls))

    async def _on_engine_started(self) -> None:
        if not self._slots_initialized:
            self._slots_initialized = True
            await self.crawler.signals.send_catch_log_async(
                signals.feed_slots_initialized, slots=self.slots
            )

    async def _on_state_saving(self, state: dict[str, Any]) -> None:
        """Persist the current batch ID for each feed into spider.state.

        Called via the spider_state_saving signal before SpiderState writes
        spider.state to disk, so the next run can resume from the right batch.
        Also persists, for atomic feeds, the batches whose commit is still
        pending, so that the next run can finish exporting them.
        """
        # The spider_closed handlers run concurrently, so commits that
        # close_spider() started may still be running. Waiting for them keeps
        # the saved batch IDs and the saved staged data in line with the
        # batches that were actually stored.
        await self._wait_for_pending_closes()
        feed_batch_ids = state.setdefault("feed_batch_ids", {})
        for slot in self.slots:
            batch_id = slot.batch_id
            if (
                getattr(slot.storage, "staging_path", None) is not None
                and slot.commit_state == "committed"
            ):
                # The batch was stored, so the next run starts with the
                # following one instead of writing it again. Only atomic
                # feeds need this: other feeds keep the historical behavior.
                batch_id += 1
            feed_batch_ids[slot.uri_template] = batch_id
        records = {
            uri_template: [
                record.to_state()
                for record in sorted(batch_records, key=lambda r: r.batch_id)
            ]
            for uri_template, batch_records in self._batch_records.items()
            if batch_records
        }
        if records:
            state["feed_atomic_batches"] = records
        else:
            state.pop("feed_atomic_batches", None)

    def _skip(self, slot: FeedSlot) -> None:
        """Report that the target of *slot* already exists, and mark *slot* so
        that nothing is written to it."""
        slot._skipped = True
        logger.error(
            f"Not writing {slot.uri} because it already exists and the feed "
            "mode is 'create'; its items are lost. To write it instead, remove "
            "the target, or set the mode feed option or the FEED_MODE setting "
            "to 'overwrite' or 'append' (-O implies 'overwrite').",
            extra={"spider": slot.spider},
        )
        assert self.crawler.stats
        self.crawler.stats.inc_value(
            f"feedexport/conflicts/{type(slot.storage).__name__}"
        )

    async def close_spider(self, spider: Spider) -> None:
        await self._settle_slots(spider)

        # Send FEED_EXPORTER_CLOSED signal
        await self.crawler.signals.send_catch_log_async(signals.feed_exporter_closed)

    async def _settle_slots(self, spider: Spider) -> None:
        """Close every slot that is not closed yet and wait for all the
        closes, including those started when a batch filled up."""
        for slot in self.slots:
            self._schedule_slot_close(slot, spider)
        await self._wait_for_pending_closes()

    async def _wait_for_pending_closes(self) -> None:
        if self._pending_close_tasks:  # pragma: no branch
            if is_asyncio_available():
                await asyncio.wait(
                    cast("list[asyncio.Task[None]]", list(self._pending_close_tasks))
                )
            else:
                await DeferredList(
                    cast("list[Deferred[None]]", list(self._pending_close_tasks))
                )

    def _schedule_slot_close(
        self, slot: FeedSlot, spider: Spider
    ) -> asyncio.Task[None] | Deferred[None] | None:
        """Start closing the slot without waiting for it to finish, keeping
        track of the pending work so that it can be awaited in
        :meth:`_settle_slots` if it hasn't finished by then.

        Closing the same slot more than once has no effect, so that the
        concurrent handlers of :signal:`spider_closed` do not store a batch
        twice.
        """
        if slot._close_scheduled:  # pragma: no cover
            return None
        slot._close_scheduled = True
        aw: asyncio.Task[None] | Deferred[None]
        coro = self._close_slot(slot, spider)
        if is_asyncio_available():
            aw = asyncio.create_task(coro)
            self._pending_close_tasks.append(aw)
            aw.add_done_callback(self._pending_close_tasks.remove)
        else:
            aw = deferred_from_coro(coro)
            self._pending_close_tasks.append(aw)
            aw.addBoth(self._untrack_pending_close_task, aw)
        return aw

    def _untrack_pending_close_task(self, result: Any, aw: Deferred[None]) -> Any:
        self._pending_close_tasks.remove(aw)
        return result

    @staticmethod
    def _get_file(slot_: FeedSlot) -> IO[bytes]:
        assert slot_.file
        if isinstance(slot_.file, PostProcessingManager):
            slot_.file.close()
            return slot_.file.file
        return slot_.file

    async def _close_slot(self, slot: FeedSlot, spider: Spider) -> None:
        if slot._skipped:
            self._untrack_batch(slot)
            return

        if not slot.itemcount and not (slot.store_empty and slot.batch_id == 1):
            # In this case, the file is not stored, so no processing is required.
            self._untrack_batch(slot)
            return

        logmsg = f"{slot.format} feed ({slot.itemcount} items) in: {slot.uri}"
        slot_type = type(slot.storage).__name__
        staging_path = getattr(slot.storage, "staging_path", None)
        try:
            await self._finish_exporting(slot)
        except Exception:
            if staging_path is None:
                raise
            # For atomic feeds the export must not abort the close, so that
            # the staged file, which lacks a completion marker, is left for a
            # later run to replace.
            slot.commit_state = "failed"
            self._close_slot_file(slot)
            logger.exception("Error finishing %s", logmsg, extra={"spider": spider})
            self.crawler.stats.inc_value(f"feedexport/failed_count/{slot_type}")
        else:
            self.crawler.stats.inc_value(
                f"feedexport/item_count/{slot_type}", slot.itemcount
            )
            try:
                await ensure_awaitable(slot.storage.store(self._get_file(slot)))
            except FileExistsError:
                slot.commit_state = "failed"
                # The staged data cannot be published, so there is nothing to
                # recover.
                if staging_path is not None:
                    _discard_staging_file(staging_path)
                self._skip(slot)
                self.crawler.stats.inc_value(f"feedexport/failed_count/{slot_type}")
                self._untrack_batch(slot)
            except Exception:
                slot.commit_state = "failed"
                if staging_path is not None:
                    # The staged file holds the complete batch, and its record
                    # keeps the batch identity, so a later run can publish it.
                    logmsg += f" (staged in: {staging_path})"
                logger.exception(
                    "Error storing %s",
                    logmsg,
                    extra={"spider": spider},
                )
                self.crawler.stats.inc_value(f"feedexport/failed_count/{slot_type}")
            else:
                slot.commit_state = "committed"
                logger.info("Stored %s", logmsg, extra={"spider": spider})
                self.crawler.stats.inc_value(f"feedexport/success_count/{slot_type}")
                self._untrack_batch(slot)

        await self.crawler.signals.send_catch_log_async(
            signals.feed_slot_closed, slot=slot
        )

    async def _finish_exporting(self, slot: FeedSlot) -> None:
        if slot.itemcount:
            # Normal case
            await slot.finish_exporting()
        else:
            # Need to store the empty file
            await slot.start_exporting()
            await slot.finish_exporting()

    def _start_new_batch(
        self,
        batch_id: int,
        uri: str,
        feed_options: dict[str, Any],
        spider: Spider,
        uri_template: str,
    ) -> FeedSlot:
        """
        Redirect the output data stream to a new file.
        Execute multiple times if FEED_EXPORT_BATCH_ITEM_COUNT setting or FEEDS.batch_item_count is specified
        :param batch_id: sequence number of current batch
        :param uri: uri of the new batch to start
        :param feed_options: dict with parameters of feed
        :param spider: user spider
        :param uri_template: template of uri which contains %(batch_time)s or %(batch_id)d to create new uri
        """
        storage = self._get_storage(uri, feed_options)
        return FeedSlot(
            storage=storage,
            uri=uri,
            format=feed_options["format"],
            store_empty=feed_options["store_empty"],
            batch_id=batch_id,
            uri_template=uri_template,
            filter=self.filters[uri_template],
            feed_options=feed_options,
            spider=spider,
            exporters=self.exporters,
            settings=self.settings,
            crawler=self.crawler,
        )

    async def item_scraped(self, item: Any, spider: Spider) -> None:
        if not self._export_lock.locked:
            # Acquiring a free lock fires the returned Deferred right away
            self._export_lock.acquire()
        else:
            await maybe_deferred_to_future(self._export_lock.acquire())
        try:
            await self._export_item(item, spider)
        finally:
            self._export_lock.release()

    async def _export_item(self, item: Any, spider: Spider) -> None:
        slots = []
        for slot in self.slots:
            if not slot.filter.accepts(item):
                slots.append(
                    slot
                )  # if slot doesn't accept item, continue with next slot
                continue

            processor = self.processors[slot.uri_template]
            for exported_item in processor(item) if processor else (item,):
                if not slot._skipped:
                    try:
                        await slot.start_exporting()
                    except FileExistsError:
                        self._skip(slot)
                if not slot._skipped:
                    assert slot.exporter
                    try:
                        await _maybe_coroutine(slot.exporter.export_item, exported_item)
                    except Exception as e:
                        e.add_note(f"Item: {exported_item!r}")
                        raise
                    if slot._pending_items is not None:
                        # Keep the item so that a resumed crawl can export it
                        # again if the batch never reaches its target.
                        slot._pending_items.append(exported_item)
                # Skipped items are counted, so that the following files of
                # this feed still cover the same items as those of any other
                # feed.
                slot.itemcount += 1
                # create new slot for each slot with itemcount == FEED_EXPORT_BATCH_ITEM_COUNT and close the old one
                if (
                    self.feeds[slot.uri_template]["batch_item_count"]
                    and slot.itemcount
                    >= self.feeds[slot.uri_template]["batch_item_count"]
                ):
                    self._schedule_slot_close(slot, spider)
                    slot = self._next_batch_slot(slot, spider)  # noqa: PLW2901
                    self._track_batch(slot)
            slots.append(slot)
        self.slots = slots

    def _load_components(self, setting_prefix: str) -> dict[str, Any]:
        conf = without_none_values(
            cast("dict[str, str]", self.settings.getwithbase(setting_prefix))
        )
        d = {}
        for k, v in conf.items():
            with contextlib.suppress(NotConfigured):
                d[k] = load_object(v)
        return d

    def _exporter_supported(self, format_: str) -> bool:
        if format_ in self.exporters:
            return True
        if format_:
            logger.error("Unknown feed format: %(format)s", {"format": format_})
        else:
            logger.error(
                "Feed format not set and it could not be inferred from the "
                "feed URI; set it explicitly with the 'format' key"
            )
        return False

    def _settings_are_valid(self) -> bool:
        """
        If FEED_EXPORT_BATCH_ITEM_COUNT setting or FEEDS.batch_item_count is specified uri has to contain
        %(batch_time)s or %(batch_id)d to distinguish different files of partial output
        """
        for uri_template, values in self.feeds.items():
            if values["batch_item_count"] and not re.search(
                r"%\(batch_time\)s|%\(batch_id\)", uri_template
            ):
                logger.error(
                    "%%(batch_time)s or %%(batch_id)d must be in the feed URI (%s) if FEED_EXPORT_BATCH_ITEM_COUNT "
                    "setting or FEEDS.batch_item_count is specified and greater than 0. For more info see: "
                    "https://docs.scrapy.org/en/latest/topics/feed-exports.html#feed-export-batch-item-count",
                    uri_template,
                )
                return False
        return True

    def _storage_supported(self, uri: str, feed_options: dict[str, Any]) -> bool:
        scheme = urlparse(uri).scheme
        if scheme in self.storages or PureWindowsPath(uri).drive:
            try:
                storage = self._get_storage(uri, feed_options)
                self._check_storage_mode(uri, storage, feed_options)
                return True
            except NotConfigured as e:
                logger.error(
                    "Disabled feed storage scheme: %(scheme)s. Reason: %(reason)s",
                    {"scheme": scheme, "reason": str(e)},
                )
        else:
            logger.error("Unknown feed storage scheme: %(scheme)s", {"scheme": scheme})
        return False

    @staticmethod
    def _check_storage_mode(
        uri: str, storage: FeedStorageProtocol, feed_options: dict[str, Any]
    ) -> None:
        mode = feed_options.get("mode")
        if mode is None:
            # Every storage keeps its own historical mode.
            return
        if getattr(storage, "supported_modes", None) is None:
            logger.warning(
                f"{type(storage).__name__} does not declare which feed modes it "
                f"supports, so the mode feed option of the {uri} feed ({mode!r}) "
                "may be ignored."
            )
            return
        _check_mode(mode, storage, uri)

    def _storage_is_atomic(self, uri: str, feed_options: dict[str, Any]) -> bool:
        """Return whether the storage of *uri* supports the atomic feed
        option.

        Only local file feeds are atomic, including custom storages derived
        from :class:`FileFeedStorage`.
        """
        cls = self.storages.get(urlparse(uri).scheme, self.storages["file"])
        return isinstance(cls, type) and issubclass(cls, FileFeedStorage)

    def _get_storage(
        self, uri: str, feed_options: dict[str, Any]
    ) -> FeedStorageProtocol:
        """Build a storage object for the specified *uri* with the specified
        *feed_options*."""
        cls = self.storages.get(urlparse(uri).scheme, self.storages["file"])
        return build_from_crawler(cls, self.crawler, uri, feed_options=feed_options)

    def _get_uri_params(
        self,
        spider: Spider,
        uri_params_function: str | UriParamsCallableT | None,
        slot: FeedSlot | None = None,
    ) -> dict[str, Any]:
        params = {k: getattr(spider, k) for k in dir(spider)}
        utc_now = datetime.now(tz=UTC)
        params["time"] = utc_now.replace(microsecond=0).isoformat().replace(":", "-")
        params["batch_time"] = utc_now.isoformat().replace(":", "-")
        params["batch_id"] = slot.batch_id + 1 if slot is not None else 1
        uripar_function: UriParamsCallableT = (
            load_object(uri_params_function)
            if uri_params_function
            else lambda params, _: params
        )
        new_params = uripar_function(params, spider)
        return new_params if new_params is not None else params

    def _load_filter(self, feed_options: dict[str, Any]) -> ItemFilter:
        # load the item filter if declared else load the default filter class
        item_filter_class: type[ItemFilter] = load_object(
            feed_options.get("item_filter", ItemFilter)
        )
        return item_filter_class(feed_options)

    def _load_processor(self, feed_options: dict[str, Any]) -> _ItemProcessor | None:
        item_processor = feed_options.get("item_processor")
        return load_object(item_processor) if item_processor else None


def __getattr__(name: str) -> Any:  # pragma: no cover
    if name == "IFeedStorage":
        warnings.warn(
            "scrapy.extensions.feedexport.IFeedStorage is deprecated.",
            ScrapyDeprecationWarning,
            stacklevel=2,
        )
        return _IFeedStorage
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
