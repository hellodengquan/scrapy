from __future__ import annotations

import pickle
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any
from unittest import mock
from urllib.parse import urljoin

import pytest
from w3lib.url import file_uri_to_path

import scrapy
import scrapy.exporters
from scrapy import signals
from scrapy.extensions.feedexport import (
    FeedExporter,
    FileFeedStorage,
    _commit_staging_file,
    _seal_staging_file,
)
from scrapy.utils.misc import build_from_crawler
from scrapy.utils.test import get_crawler
from tests.utils.decorators import coroutine_test
from tests.utils.feedexport import PARSERS, crawl_items, unique_path

if TYPE_CHECKING:
    from tests.mockserver.http import MockServer


def build_url(path: str | Path) -> str:
    path_str = str(path)
    if path_str[0] != "/":
        path_str = "/" + path_str
    return urljoin("file:", path_str)


def feed_dir(settings: dict[str, Any]) -> Path:
    """Return the directory of the pending feed file given by a URI template that
    ends in a file name."""
    [uri] = settings["FEEDS"]
    return Path(file_uri_to_path(uri)).parent


def batch_files_of(dir_path: Path) -> list[str]:
    """Return the names of the committed feed files in *dir_path*."""
    return sorted(
        file.name
        for file in dir_path.iterdir()
        if file.is_file()
        and not file.name.endswith(".complete")
        and "part-" not in file.name
    )


def batch_files(settings: dict[str, Any]) -> list[Path]:
    """Return the committed feed files of *settings*, in file name order."""
    dir_path = feed_dir(settings)
    if not dir_path.exists():
        return []
    return [
        file
        for file in sorted(dir_path.iterdir())
        if file.is_file()
        and not file.name.endswith(".complete")
        and "part-" not in file.name
    ]


def atomic_settings(path: str | Path, **options: Any) -> dict[str, Any]:
    return {
        "FEEDS": {
            build_url(path): {"format": "jsonlines", "atomic": True, **options},
        },
        "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
    }


async def crawl_items_listening(
    mockserver: MockServer,
    items: list[Any],
    settings: dict[str, Any],
    signal: object,
) -> tuple[Any, list[dict[str, Any]]]:
    """Crawl *items* with *settings*, recording the keyword arguments that
    every *signal* handler receives."""
    received: list[dict[str, Any]] = []

    class TestSpider(scrapy.Spider):
        name = "testspider"
        start_urls = [mockserver.url("/")]

        def parse(self, response):
            yield from items

    crawler = get_crawler(TestSpider, settings)

    async def record(sender, **kwargs):
        received.append(kwargs)

    crawler.signals.connect(record, signal)
    await crawler.crawl_async()
    return crawler, received


def fail_commit(dir_path: Path, *, seal: bool = True, once: bool = False) -> Any:
    """Patch feed commits into *dir_path* to fail.

    With *seal* the commit of a sealed staged file fails, as when moving it to
    its target fails. Without it, the export itself fails before the staged
    file is marked complete, as when the crawl is interrupted while writing.

    With *once*, only the first commit into *dir_path* fails.
    """
    original_commit = _commit_staging_file
    original_store = FileFeedStorage.store
    failed = False

    def should_fail(path: str) -> bool:
        nonlocal failed
        if Path(path).resolve().parent != dir_path.resolve():
            return False
        if failed and once:
            return False
        failed = True
        return True

    def commit(staging_path: str, final_path: str, write_mode: Any) -> None:
        if not should_fail(final_path):
            return original_commit(staging_path, final_path, write_mode)
        raise OSError("Cannot commit")

    def store(self: FileFeedStorage, file: IO[bytes]) -> None:
        file.close()
        if not should_fail(self.path):
            original_store(self, file)
            return
        raise OSError("Cannot store")

    if seal:
        return mock.patch("scrapy.extensions.feedexport._commit_staging_file", commit)
    return mock.patch.object(FileFeedStorage, "store", store)


class TestVisibility:
    @coroutine_test
    async def test_open_batch_not_visible(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """While a batch is open, its target does not exist; a full batch
        appears at its target as soon as it is committed."""
        dir_path = unique_path(tmp_path)
        target = Path(dir_path, "1.jsonl")
        next_target = Path(dir_path, "2.jsonl")
        seen: list[tuple[bool, bool]] = []

        class TestSpider(scrapy.Spider):
            name = "testspider"
            start_urls = [mockserver.url("/")]

            def parse(self, response):
                yield {"foo": "bar1"}
                yield {"foo": "bar2"}
                yield scrapy.Request(
                    mockserver.url("/"), callback=self.parse2, dont_filter=True
                )

            def parse2(self, response):
                # The first batch filled up, so it was committed; the second
                # one is open and must not be visible yet.
                seen.append((target.exists(), next_target.exists()))
                yield {"foo": "bar3"}

        settings = atomic_settings(Path(dir_path) / "%(batch_id)d.jsonl")
        await get_crawler(TestSpider, settings).crawl_async()

        assert seen == [(True, False)]
        assert PARSERS["jsonlines"](target.read_bytes()) == [
            {"foo": "bar1"},
            {"foo": "bar2"},
        ]

    @coroutine_test
    async def test_no_staging_files_left(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        settings = atomic_settings(unique_path(tmp_path) / "%(batch_id)d.feed")
        await crawl_items(mockserver, [{"foo": 1}, {"foo": 2}, {"foo": 3}], settings)
        assert [p.name for p in batch_files(settings)] == ["1.feed", "2.feed"]

    @pytest.mark.parametrize("fmt", ["json", "jsonlines", "csv", "xml"])
    @coroutine_test
    async def test_formats_and_templates(
        self, mockserver: MockServer, tmp_path: Path, fmt: str
    ) -> None:
        """Existing formats and URI templates keep working."""
        path = unique_path(tmp_path) / "%(batch_time)s_%(batch_id)02d.feed"
        settings = {
            "FEEDS": {build_url(path): {"format": fmt, "atomic": True}},
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
        }
        crawler = await crawl_items(mockserver, [{"foo": "bar"}], settings)
        assert crawler.stats
        assert crawler.stats.get_value("feedexport/success_count/FileFeedStorage") == 1
        [file] = batch_files(settings)
        assert PARSERS[fmt](file.read_bytes()) == [{"foo": "bar"}]

    @coroutine_test
    async def test_committed_bytes_match_non_atomic(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        items = [{"foo": "bar1"}, {"foo": "bar2"}, {"foo": "bar3"}]
        path = unique_path(tmp_path) / "%(batch_id)d.feed"
        with_atomic = atomic_settings(path)
        await crawl_items(mockserver, items, with_atomic)
        without_atomic = {
            "FEEDS": {build_url(path): {"format": "jsonlines"}},
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
        }
        await crawl_items(mockserver, items, without_atomic)
        assert [p.read_bytes() for p in batch_files(with_atomic)] == [
            p.read_bytes() for p in batch_files(without_atomic)
        ]


class TestDisabled:
    @coroutine_test
    async def test_default_is_not_atomic(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        dir_path = unique_path(tmp_path)
        target = Path(dir_path, "1.jsonl")
        seen: list[bool] = []

        class TestSpider(scrapy.Spider):
            name = "testspider"
            start_urls = [mockserver.url("/")]

            def parse(self, response):
                yield {"foo": "bar1"}
                yield scrapy.Request(
                    mockserver.url("/"), callback=self.parse2, dont_filter=True
                )

            def parse2(self, response):
                seen.append(target.exists())

        settings = {
            "FEEDS": {build_url(dir_path / "%(batch_id)d.jsonl"): {}},
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
        }
        await get_crawler(TestSpider, settings).crawl_async()
        assert seen == [True]

    @pytest.mark.parametrize(
        ("setting", "option", "atomic"),
        [
            (False, False, False),
            (True, True, True),
            (True, False, False),
            (False, True, True),
        ],
        ids=["off", "on", "setting-on", "option-on"],
    )
    @coroutine_test
    async def test_setting_and_option(
        self,
        mockserver: MockServer,
        tmp_path: Path,
        setting: bool,
        option: bool,
        atomic: bool,
    ) -> None:
        """The setting enables it for every feed, and the feed option
        overrides the setting."""
        dir_path = unique_path(tmp_path)
        target = Path(dir_path, "1.jsonl")
        settings: dict[str, Any] = {
            "FEEDS": {
                build_url(dir_path / "%(batch_id)d.jsonl"): {
                    "format": "jsonlines",
                    "atomic": option,
                }
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
            "FEED_EXPORT_ATOMIC": setting,
        }
        crawler = await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        seen: list[str | None] = []

        def on_slot_closed(slot):
            seen.append(slot.storage.staging_path)

        crawler.signals.connect(on_slot_closed, signals.feed_slot_closed)
        assert target.exists()
        assert PARSERS["jsonlines"](target.read_bytes()) == [{"foo": "bar1"}]
        assert [p.name for p in Path(dir_path).iterdir()] == ["1.jsonl"]

    @coroutine_test
    async def test_conflict_with_existing_target(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        path = unique_path(tmp_path) / "1.jsonl"
        path.parent.mkdir()
        path.write_bytes(b"existing")
        settings = {
            "FEEDS": {
                build_url(path): {
                    "format": "jsonlines",
                    "atomic": True,
                    "mode": "create",
                }
            }
        }
        crawler = await crawl_items(mockserver, [{"foo": "bar"}], settings)
        assert path.read_bytes() == b"existing"
        assert crawler.stats
        assert crawler.stats.get_value("feedexport/conflicts/FileFeedStorage") == 1
        assert sorted(p.name for p in path.parent.iterdir()) == ["1.jsonl"]

    @coroutine_test
    async def test_conflict_at_commit(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A target that appears while the batch is written is reported as a
        conflict, and the staged data is discarded."""
        dir_path = unique_path(tmp_path)
        target = Path(dir_path, "1.feed")
        settings = {
            "FEEDS": {
                build_url(Path(dir_path) / "%(batch_id)d.feed"): {
                    "format": "jsonlines",
                    "atomic": True,
                    "mode": "create",
                }
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
        }
        original_commit = _commit_staging_file

        def commit(staging_path: str, final_path: str, write_mode: Any) -> None:
            target.write_bytes(b"existing")
            return original_commit(staging_path, final_path, write_mode)

        with mock.patch("scrapy.extensions.feedexport._commit_staging_file", commit):
            crawler = await crawl_items(mockserver, [{"foo": "bar"}] * 2, settings)
        assert target.read_bytes() == b"existing"
        assert crawler.stats
        assert crawler.stats.get_value("feedexport/conflicts/FileFeedStorage") == 1
        assert sorted(batch_files_of(dir_path)) == ["1.feed"]

    @coroutine_test
    async def test_other_storage_ignores_option(
        self, mockserver: MockServer, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        settings = {
            "FEEDS": {"stdout:": {"format": "jsonlines", "atomic": True}},
        }
        crawler = get_crawler(settings_dict=settings)
        with caplog.at_level("WARNING"):
            build_from_crawler(FeedExporter, crawler)
        assert "only supported for local file system feeds" in caplog.text

    def test_append_mode(self, tmp_path: Path) -> None:
        path = tmp_path / "feed.jsonl"
        path.write_bytes(b'{"foo": "old"}\n')
        storage = FileFeedStorage(
            build_url(path), feed_options={"atomic": True, "mode": "append"}
        )
        assert storage.staging_path
        with Path(storage.staging_path).open("wb") as file:
            file.write(b'{"foo": "new"}\n')
        storage.store(file)
        assert path.read_bytes() == b'{"foo": "old"}\n{"foo": "new"}\n'
        assert sorted(p.name for p in tmp_path.iterdir()) == ["feed.jsonl"]

    def test_plain_path(self, tmp_path: Path) -> None:
        """Plain paths, not only file:// URIs, support the option."""
        path = tmp_path / "feed.jsonl"
        storage = FileFeedStorage(str(path), feed_options={"atomic": True})
        assert storage.staging_path
        assert storage.staging_path.startswith(f"{path}.part-")

    def test_custom_file_storage(self, tmp_path: Path) -> None:
        """A custom storage derived from FileFeedStorage supports the option."""

        class CustomFileFeedStorage(FileFeedStorage):
            pass

        path = tmp_path / "%(batch_id)d.feed"
        settings: dict[str, Any] = {
            "FEEDS": {build_url(path): {"format": "jsonlines", "atomic": True}},
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
            "FEED_STORAGES": {"file": CustomFileFeedStorage},
        }
        crawler = get_crawler(settings_dict=settings)
        exporter = build_from_crawler(FeedExporter, crawler)
        spider = scrapy.Spider.from_crawler(crawler, "default")
        exporter.open_spider(spider)
        storage = exporter.slots[0].storage
        assert isinstance(storage, FileFeedStorage)
        assert storage.staging_path


class TestOutcomes:
    @coroutine_test
    async def test_commit_state(self, mockserver: MockServer, tmp_path: Path) -> None:
        _, received = await crawl_items_listening(
            mockserver,
            [{"foo": 1}, {"foo": 2}, {"foo": 3}, {"foo": 4}],
            atomic_settings(unique_path(tmp_path) / "%(batch_id)d.feed"),
            signals.feed_slot_closed,
        )
        assert [kw["slot"].commit_state for kw in received] == [
            "committed",
            "committed",
        ]

    @coroutine_test
    async def test_failed_commit_is_distinguishable(
        self, mockserver: MockServer, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A failed commit reports failure, keeps the staged data, and names
        it so that it can be recovered."""
        dir_path = unique_path(tmp_path)
        target = Path(dir_path, "1.jsonl")
        with fail_commit(target.parent), caplog.at_level("ERROR"):
            crawler, received = await crawl_items_listening(
                mockserver,
                [{"foo": 1}, {"foo": 2}],
                atomic_settings(Path(dir_path) / "%(batch_id)d.jsonl"),
                signals.feed_slot_closed,
            )
        assert [kw["slot"].commit_state for kw in received] == ["failed"]
        assert not target.exists()
        files = sorted(p.name for p in Path(dir_path).iterdir())
        [staging] = [
            name for name in files if "part-" in name and not name.endswith(".complete")
        ]
        assert str(Path(dir_path, staging)) in caplog.text
        assert crawler.stats
        assert crawler.stats.get_value("feedexport/failed_count/FileFeedStorage") == 1
        assert (
            "feedexport/success_count/FileFeedStorage" not in crawler.stats.get_stats()
        )

    @coroutine_test
    async def test_multiple_targets_partial_failure(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A failure of one target does not affect the data committed by
        another one."""
        dir_a = unique_path(tmp_path)
        dir_b = unique_path(tmp_path)
        settings: dict[str, Any] = {
            "FEEDS": {
                build_url(dir_a / "%(batch_id)d.jsonl"): {
                    "format": "jsonlines",
                    "atomic": True,
                },
                build_url(dir_b / "%(batch_id)d.jsonl"): {
                    "format": "jsonlines",
                    "atomic": True,
                },
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
        }
        with fail_commit(dir_a, once=True):
            crawler = await crawl_items(
                mockserver,
                [{"foo": "bar1"}, {"foo": "bar2"}, {"foo": "bar3"}],
                settings,
            )
        assert (Path(dir_b) / "1.jsonl").read_bytes() == (
            b'{"foo": "bar1"}\n{"foo": "bar2"}\n'
        )
        assert (Path(dir_b) / "2.jsonl").read_bytes() == b'{"foo": "bar3"}\n'
        assert not (Path(dir_a) / "1.jsonl").exists()
        assert crawler.stats
        assert crawler.stats.get_value("feedexport/success_count/FileFeedStorage") == 3
        assert crawler.stats.get_value("feedexport/failed_count/FileFeedStorage") == 1

    @pytest.mark.parametrize(
        ("store_empty", "expected"), [(True, 1), (False, 0)], ids=["store", "skip"]
    )
    @coroutine_test
    async def test_empty_feed(
        self, mockserver: MockServer, tmp_path: Path, store_empty: bool, expected: int
    ) -> None:
        """An empty feed follows the store_empty rules."""
        settings = {
            **atomic_settings(unique_path(tmp_path) / "%(batch_id)d.feed"),
            "FEED_STORE_EMPTY": store_empty,
        }
        await crawl_items(mockserver, [], settings)
        assert len(batch_files(settings)) == expected

    @coroutine_test
    async def test_short_last_batch(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """The last batch may hold fewer items, as without atomic commits."""
        settings = atomic_settings(unique_path(tmp_path) / "%(batch_id)d.feed")
        await crawl_items(mockserver, [{"foo": 1}, {"foo": 2}, {"foo": 3}], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [
            [{"foo": 1}, {"foo": 2}],
            [{"foo": 3}],
        ]


class TestRecovery:
    @coroutine_test
    async def test_failed_closed_batch_and_committed_open_batch(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A closed batch that failed to commit does not disturb the batch
        that was in progress and committed before the state was saved."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            **atomic_settings(Path(dir_path) / "%(batch_id)d.feed"),
            "JOBDIR": str(tmp_path / "job"),
        }
        original_commit = _commit_staging_file

        def commit(staging_path: str, final_path: str, write_mode: Any) -> None:
            # Fail the first batch only; the last, open batch commits.
            if Path(final_path).name == "1.feed":
                raise OSError("Cannot commit")
            return original_commit(staging_path, final_path, write_mode)

        with mock.patch("scrapy.extensions.feedexport._commit_staging_file", commit):
            await crawl_items(
                mockserver,
                [{"foo": "bar1"}, {"foo": "bar2"}, {"foo": "bar3"}],
                settings,
            )
        assert batch_files_of(dir_path) == ["2.feed"]
        await crawl_items(mockserver, [{"foo": "bar4"}], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [
            [{"foo": "bar1"}, {"foo": "bar2"}],
            [{"foo": "bar3"}],
            [{"foo": "bar4"}],
        ]

    @pytest.mark.parametrize("seal", [True, False], ids=["staged", "interrupted"])
    @coroutine_test
    async def test_failed_commit_retried(
        self, mockserver: MockServer, tmp_path: Path, seal: bool
    ) -> None:
        """A batch whose commit failed is published on resume under the same
        identity, whether its staged data was complete or interrupted."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            **atomic_settings(Path(dir_path) / "%(batch_time)s_%(batch_id)02d.jsonl"),
            "JOBDIR": str(tmp_path / "job"),
        }
        with fail_commit(Path(dir_path)):
            await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        template = next(iter(settings["FEEDS"]))
        [record] = state["feed_atomic_batches"][template]
        target = Path(file_uri_to_path(record["uri"]))
        assert not target.exists()

        await crawl_items(mockserver, [{"foo": "bar3"}], settings)
        assert PARSERS["jsonlines"](target.read_bytes()) == [
            {"foo": "bar1"},
            {"foo": "bar2"},
        ]
        # The retried batch kept the identity, and so the batch_time, of the
        # original, and the new items went into the following batch.
        names = sorted(p.name for p in Path(dir_path).iterdir())
        assert target.name in names
        [new_batch] = [name for name in names if name != target.name]
        assert new_batch.split("_")[0] != target.name.split("_")[0]
        assert PARSERS["jsonlines"](Path(dir_path, new_batch).read_bytes()) == [
            {"foo": "bar3"}
        ]

    @coroutine_test
    async def test_resume_with_full_batch(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """Re-exporting a batch that fills up during recovery continues with
        the following batch right away."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            **atomic_settings(Path(dir_path) / "%(batch_id)d.feed"),
            "JOBDIR": str(tmp_path / "job"),
        }
        # Fail while writing batch 1, so its items must be exported again.
        with fail_commit(Path(dir_path), seal=False):
            await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        await crawl_items(mockserver, [{"foo": "bar3"}, {"foo": "bar4"}], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [
            [{"foo": "bar1"}, {"foo": "bar2"}],
            [{"foo": "bar3"}, {"foo": "bar4"}],
        ]

    @coroutine_test
    async def test_resume_export_error_keeps_items(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """If re-exporting saved items fails, they stay in the state so that a
        later run can export them again."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            **atomic_settings(Path(dir_path) / "%(batch_id)d.feed"),
            "JOBDIR": str(tmp_path / "job"),
        }
        # Fail while writing batch 1, so its items must be exported again.
        with fail_commit(Path(dir_path), seal=False, once=True):
            await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)

        # Fail the recovery only: the exporter of the recovered batch is
        # writing into a staging file, which the resumed run's own batch does
        # not exist yet, and failing on a seeded item keeps this run working.
        original_export_item = scrapy.exporters.JsonLinesItemExporter.export_item
        recovering = False

        def start_exporting(self):
            nonlocal recovering
            recovering = "1.feed" in getattr(self.file, "name", "")

        def export_item(self, item):
            if recovering and item == {"foo": "bar1"}:
                raise RuntimeError("Cannot export")
            original_export_item(self, item)

        with (
            mock.patch.object(
                scrapy.exporters.JsonLinesItemExporter,
                "start_exporting",
                start_exporting,
            ),
            mock.patch.object(
                scrapy.exporters.JsonLinesItemExporter, "export_item", export_item
            ),
        ):
            await crawl_items(mockserver, [{"foo": "bar4"}], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        template = next(iter(settings["FEEDS"]))
        assert state["feed_atomic_batches"][template][0]["items"] == [
            {"foo": "bar1"},
            {"foo": "bar2"},
        ]

        await crawl_items(mockserver, [{"foo": "bar5"}], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [
            [{"foo": "bar1"}, {"foo": "bar2"}],
            [{"foo": "bar4"}],
            [{"foo": "bar5"}],
        ]

    @coroutine_test
    async def test_resume_continues_after_committed_batches(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """Resuming keeps committed batches untouched and continues with the
        next ones, without exporting their items again."""
        settings: dict[str, Any] = {
            **atomic_settings(unique_path(tmp_path) / "%(batch_id)d.feed"),
            "JOBDIR": str(tmp_path / "job"),
        }
        await crawl_items(
            mockserver, [{"foo": "bar1"}, {"foo": "bar2"}, {"foo": "bar3"}], settings
        )
        await crawl_items(mockserver, [{"foo": "bar4"}], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [
            [{"foo": "bar1"}, {"foo": "bar2"}],
            [{"foo": "bar3"}],
            [{"foo": "bar4"}],
        ]

    @coroutine_test
    async def test_full_batch_at_close(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A batch whose items are exactly the batch size at close is stored
        once, and the next run does not write it again."""
        settings: dict[str, Any] = {
            **atomic_settings(unique_path(tmp_path) / "%(batch_id)d.feed"),
            "JOBDIR": str(tmp_path / "job"),
        }
        await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        assert [p.read_bytes() for p in batch_files(settings)] == [
            b'{"foo": "bar1"}\n{"foo": "bar2"}\n'
        ]
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        assert state["feed_batch_ids"][next(iter(settings["FEEDS"]))] == 2
        await crawl_items(mockserver, [{"foo": "bar3"}], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [
            [{"foo": "bar1"}, {"foo": "bar2"}],
            [{"foo": "bar3"}],
        ]

    @coroutine_test
    async def test_unbatched_feed_recovered(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A feed without batches writes its single target from scratch when
        the crawl is resumed."""
        settings: dict[str, Any] = {
            "FEEDS": {
                build_url(unique_path(tmp_path) / "feed.feed"): {
                    "format": "jsonlines",
                    "atomic": True,
                }
            },
            "JOBDIR": str(tmp_path / "job"),
        }
        with fail_commit(feed_dir(settings), once=True):
            await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        assert state["feed_atomic_batches"]
        await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        assert [p.read_bytes() for p in batch_files(settings)] == [b'{"foo": "bar2"}\n']

    @coroutine_test
    async def test_missing_batch_id(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """State without a saved batch ID falls back to the highest pending
        batch."""
        settings: dict[str, Any] = {
            **atomic_settings(unique_path(tmp_path) / "%(batch_id)d.feed"),
            "JOBDIR": str(tmp_path / "job"),
        }
        with fail_commit(feed_dir(settings), once=True):
            await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        state_path = tmp_path / "job" / "spider.state"
        state = pickle.loads(state_path.read_bytes())
        state["feed_batch_ids"] = {}
        state_path.write_bytes(pickle.dumps(state, protocol=4))
        await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [
            [{"foo": "bar1"}],
            [{"foo": "bar2"}],
        ]

    @coroutine_test
    async def test_atomic_disabled_on_resume(
        self,
        mockserver: MockServer,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Batches staged by an atomic run are kept but not published when the
        resumed run does not enable the option."""
        settings: dict[str, Any] = {
            **atomic_settings(unique_path(tmp_path) / "%(batch_id)d.feed"),
            "JOBDIR": str(tmp_path / "job"),
        }
        with fail_commit(feed_dir(settings), once=True), caplog.at_level("ERROR"):
            await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        dir_path = feed_dir(settings)
        staged = [p for p in dir_path.iterdir() if "part-" in p.name]
        assert staged
        settings["FEEDS"][next(iter(settings["FEEDS"]))]["atomic"] = False
        await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        # The staged batch was left in place, and the resumed run wrote its
        # items to the target.
        assert sorted(p.name for p in dir_path.iterdir()) == sorted(
            ["1.feed", *(p.name for p in staged)]
        )
        assert (dir_path / "1.feed").read_bytes() == b'{"foo": "bar2"}\n'
        assert "were written to staging files" in caplog.text

    @coroutine_test
    async def test_resume_with_open_batch(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A crawl closed while its last batch was still open exports those
        items again, without losing or duplicating them."""
        settings: dict[str, Any] = {
            **atomic_settings(unique_path(tmp_path) / "%(batch_id)d.feed"),
            "JOBDIR": str(tmp_path / "job"),
        }
        await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        assert batch_files(settings)[0].read_bytes() == b'{"foo": "bar1"}\n'
        await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [
            [{"foo": "bar1"}],
            [{"foo": "bar2"}],
        ]

    @coroutine_test
    async def test_stale_state_ignored(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A state that mentions a batch that reached its target after it was
        saved does not cause the batch to be written again."""
        settings: dict[str, Any] = {
            **atomic_settings(unique_path(tmp_path) / "%(batch_id)d.feed"),
            "JOBDIR": str(tmp_path / "job"),
        }
        with fail_commit(feed_dir(settings)):
            await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        state_path = tmp_path / "job" / "spider.state"
        template = next(iter(settings["FEEDS"]))
        state = pickle.loads(state_path.read_bytes())
        [record] = state["feed_atomic_batches"][template]
        # Simulate a commit that finished after the state was saved: the
        # target holds the batch and the staged data is gone.
        target = Path(file_uri_to_path(record["uri"]))
        Path(record["staging"]).replace(target)
        Path(f"{record['staging']}.complete").unlink()
        await crawl_items(mockserver, [{"foo": "bar3"}], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [
            [{"foo": "bar1"}, {"foo": "bar2"}],
            [{"foo": "bar3"}],
        ]

    @coroutine_test
    async def test_retry_keeps_identity_on_later_runs(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A commit that keeps failing is retried on every resume, always
        against the same target."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            **atomic_settings(Path(dir_path) / "%(batch_time)s_%(batch_id)02d.jsonl"),
            "JOBDIR": str(tmp_path / "job"),
        }
        state_path = tmp_path / "job" / "spider.state"
        template = next(iter(settings["FEEDS"]))
        with fail_commit(Path(dir_path)):
            await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        first = pickle.loads(state_path.read_bytes())
        with fail_commit(Path(dir_path)):
            await crawl_items(mockserver, [{"foo": "bar3"}], settings)
        second = pickle.loads(state_path.read_bytes())
        first_record = first["feed_atomic_batches"][template][0]
        first_batch_records = [
            record
            for record in second["feed_atomic_batches"][template]
            if record["batch_id"] == first_record["batch_id"]
        ]
        [record] = first_batch_records
        assert record["uri"] == first_record["uri"]
        target = Path(file_uri_to_path(record["uri"]))
        assert not target.exists()
        # Only the two batches of the failed commits were staged, both with
        # the batch identity of the first failed commit.
        # The staged file of the first failed commit was replaced by the
        # retry, which kept the batch identity, and so the target name.
        assert Path(record["staging"]).exists()
        assert Path(record["staging"]).name.startswith(target.name)


def rewrite_state(
    tmp_path: Path,
    template: str,
    edit: dict[str, Any],
    *,
    batch_id: int,
) -> None:
    """Update the saved record of batch *batch_id* of *template* in
    :file:`spider.state` with the values of *edit*."""
    state_path = tmp_path / "job" / "spider.state"
    state = pickle.loads(state_path.read_bytes())
    records = state["feed_atomic_batches"][template]
    record = next(record for record in records if record["batch_id"] == batch_id)
    record.update(edit)
    state_path.write_bytes(pickle.dumps(state, protocol=4))


class TestRecoveryBranches:
    """The recovery paths that only happen when the saved state and the files
    on disk disagree, e.g. because the crawl was interrupted between them."""

    @staticmethod
    def _settings(tmp_path: Path, *, store_empty: bool = True) -> dict[str, Any]:
        return {
            **atomic_settings(unique_path(tmp_path) / "%(batch_id)d.feed"),
            "FEED_STORE_EMPTY": store_empty,
            "JOBDIR": str(tmp_path / "job"),
        }

    @coroutine_test
    async def test_closed_batch_without_staging_and_items(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A closed batch whose staged data and items are both gone is skipped,
        and the crawl continues with the batch after it."""
        settings = self._settings(tmp_path)
        template = next(iter(settings["FEEDS"]))
        with fail_commit(feed_dir(settings)):
            await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        record = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        [saved] = record["feed_atomic_batches"][template]
        Path(saved["staging"]).unlink()
        Path(f"{saved['staging']}.complete").unlink()
        rewrite_state(tmp_path, template, {"items": []}, batch_id=1)
        await crawl_items(mockserver, [{"foo": "bar3"}], settings)
        assert [p.read_bytes() for p in batch_files(settings)] == [b'{"foo": "bar3"}\n']

    @coroutine_test
    async def test_closed_batch_without_items_warns(
        self,
        mockserver: MockServer,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An interrupted closed batch whose items were not saved cannot be
        recovered, and says so."""
        settings = self._settings(tmp_path)
        template = next(iter(settings["FEEDS"]))
        with fail_commit(feed_dir(settings), seal=False):
            await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        rewrite_state(tmp_path, template, {"items": []}, batch_id=1)
        with caplog.at_level("WARNING"):
            await crawl_items(mockserver, [{"foo": "bar3"}], settings)
        assert "its items were not saved" in caplog.text
        assert [p.read_bytes() for p in batch_files(settings)] == [b'{"foo": "bar3"}\n']

    @coroutine_test
    async def test_closed_batch_retry_commit_fails_again(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A committed closed batch whose retried commit fails is kept in the
        state for the following run."""
        settings = self._settings(tmp_path)
        template = next(iter(settings["FEEDS"]))
        with fail_commit(feed_dir(settings)):
            await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        with fail_commit(feed_dir(settings)):
            await crawl_items(mockserver, [{"foo": "bar3"}], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        assert [
            record["batch_id"] for record in state["feed_atomic_batches"][template]
        ] == [1, 2]
        assert not batch_files(settings)

    @coroutine_test
    async def test_open_batch_last_target_exists(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """An interrupted open batch whose target already exists continues with
        the following batch instead of writing it again."""
        settings = self._settings(tmp_path)
        template = next(iter(settings["FEEDS"]))
        with fail_commit(feed_dir(settings), seal=False):
            await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        [saved] = state["feed_atomic_batches"][template]
        # The batch reached its target after the state was saved, so its staged
        # data is gone and only the target is left behind.
        Path(saved["staging"]).unlink()
        Path(f"{saved['staging']}.complete").unlink(missing_ok=True)
        Path(file_uri_to_path(saved["uri"])).write_bytes(b'{"foo": "bar1"}\n')
        await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [[{"foo": "bar1"}], [{"foo": "bar2"}]]

    @coroutine_test
    async def test_open_batch_without_staging_or_items(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """An open batch whose staged data and items are both gone is written
        again in the same batch, so its number is not skipped."""
        settings = self._settings(tmp_path)
        template = next(iter(settings["FEEDS"]))
        with fail_commit(feed_dir(settings), seal=False):
            await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        [saved] = state["feed_atomic_batches"][template]
        Path(saved["staging"]).unlink()
        Path(f"{saved['staging']}.complete").unlink(missing_ok=True)
        rewrite_state(tmp_path, template, {"items": []}, batch_id=1)
        await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        assert [p.name for p in batch_files(settings)] == ["1.feed"]
        assert PARSERS["jsonlines"](batch_files(settings)[0].read_bytes()) == [
            {"foo": "bar2"}
        ]

    @coroutine_test
    async def test_empty_first_batch_kept_identity_after_store(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """An interrupted empty first batch that did reach its target is not
        written again under its own identity, which would conflict with it."""
        settings = {
            **atomic_settings(unique_path(tmp_path) / "%(batch_id)d.feed"),
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
            "FEED_STORE_EMPTY": True,
            "JOBDIR": str(tmp_path / "job"),
        }
        template = next(iter(settings["FEEDS"]))
        # Fail while writing the empty first batch, so it is left uncommitted.
        with fail_commit(feed_dir(settings), seal=False):
            await crawl_items(mockserver, [], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        [saved] = state["feed_atomic_batches"][template]
        # The empty batch was stored before the state was saved, so its target
        # exists while its staged data is still incomplete.
        Path(file_uri_to_path(saved["uri"])).write_bytes(b"")
        await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [[], [{"foo": "bar1"}]]

    @coroutine_test
    async def test_open_batch_resume_export_error(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """If exporting the saved items of an interrupted open batch fails,
        the record is kept and this run's items go into the next batch."""
        settings = self._settings(tmp_path)
        template = next(iter(settings["FEEDS"]))
        with fail_commit(feed_dir(settings), seal=False):
            await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        original_export_item = scrapy.exporters.JsonLinesItemExporter.export_item

        def export_item(self, item):
            if item == {"foo": "bar1"}:
                raise RuntimeError("Cannot export")
            original_export_item(self, item)

        with mock.patch.object(
            scrapy.exporters.JsonLinesItemExporter, "export_item", export_item
        ):
            await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        assert [
            record["batch_id"] for record in state["feed_atomic_batches"][template]
        ] == [1]
        assert [p.read_bytes() for p in batch_files(settings)] == [b'{"foo": "bar2"}\n']

    @coroutine_test
    async def test_open_batch_resume_conflict(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A target that appears before an interrupted open batch is written
        again is reported as a conflict, and the items of the run go into the
        batch after the conflicting one."""
        settings = {
            **atomic_settings(
                unique_path(tmp_path) / "%(batch_id)d.feed", mode="create"
            ),
            "JOBDIR": str(tmp_path / "job"),
        }
        template = next(iter(settings["FEEDS"]))
        with fail_commit(feed_dir(settings), seal=False):
            await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        [saved] = state["feed_atomic_batches"][template]
        # The target appeared after the previous run saved its state, so this
        # run cannot write the batch again.
        target = Path(file_uri_to_path(saved["uri"]))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"existing")
        crawler = await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        assert target.read_bytes() == b"existing"
        assert crawler.stats
        assert crawler.stats.get_value("feedexport/conflicts/FileFeedStorage") == 1
        assert [p.name for p in batch_files(settings)] == ["1.feed", "2.feed"]
        assert PARSERS["jsonlines"](
            Path(feed_dir(settings), "2.feed").read_bytes()
        ) == [{"foo": "bar2"}]

    @coroutine_test
    async def test_unbatched_feed_missing_batches(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """The pending batches of a feed that is not part of this run are kept
        in the state, so that a later run can still recover them."""
        settings = self._settings(tmp_path)
        template = next(iter(settings["FEEDS"]))
        with fail_commit(feed_dir(settings)):
            await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        state_path = tmp_path / "job" / "spider.state"
        state = pickle.loads(state_path.read_bytes())
        absent_template = "file:///nonexistent/%(batch_id)d.feed"
        state["feed_atomic_batches"][absent_template] = state[
            "feed_atomic_batches"
        ].pop(template)
        state_path.write_bytes(pickle.dumps(state, protocol=4))
        await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        state = pickle.loads(state_path.read_bytes())
        # The feed of this run has nothing pending: its batch was committed.
        assert sorted(state["feed_atomic_batches"]) == [absent_template]

    @coroutine_test
    async def test_open_batch_resume_fills_again(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """Re-exporting the saved items of an interrupted open batch can fill
        it up again, e.g. when the batch size was lowered between runs, so the
        crawl continues with the next batch right away."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            "FEEDS": {
                build_url(Path(dir_path) / "%(batch_id)d.feed"): {
                    "format": "jsonlines",
                    "atomic": True,
                }
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 3,
            "JOBDIR": str(tmp_path / "job"),
        }
        # Interrupt the crawl while the first batch holds two of its three
        # items, so it is saved as the batch in progress.
        with fail_commit(dir_path, seal=False):
            await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        settings["FEED_EXPORT_BATCH_ITEM_COUNT"] = 2
        await crawl_items(mockserver, [{"foo": "bar3"}], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [[{"foo": "bar1"}, {"foo": "bar2"}], [{"foo": "bar3"}]]

    @coroutine_test
    async def test_closed_batch_retry_conflict(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """If the target of a retried closed batch appears before the retry
        commits it, the conflict is reported and the staged data discarded."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            **atomic_settings(Path(dir_path) / "%(batch_id)d.feed", mode="create"),
            "JOBDIR": str(tmp_path / "job"),
        }
        template = next(iter(settings["FEEDS"]))
        with fail_commit(dir_path):
            await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        [saved] = state["feed_atomic_batches"][template]
        target = Path(file_uri_to_path(saved["uri"]))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"existing")
        crawler = await crawl_items(mockserver, [{"foo": "bar3"}], settings)
        assert target.read_bytes() == b"existing"
        assert crawler.stats
        assert crawler.stats.get_value("feedexport/conflicts/FileFeedStorage") == 1
        assert not Path(saved["staging"]).exists()
        assert Path(feed_dir(settings), "2.feed").exists()

    @coroutine_test
    async def test_closed_batch_retry_store_fails(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """If storing a retried closed batch fails, its record is kept with the
        staged path of the retry so that a later run can publish it."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            **atomic_settings(Path(dir_path) / "%(batch_id)d.feed"),
            "JOBDIR": str(tmp_path / "job"),
        }
        template = next(iter(settings["FEEDS"]))
        # Fail while writing the batch, so it must be exported again.
        with fail_commit(dir_path, seal=False):
            await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        # Fail the commit of the retry, so the batch stays pending.
        with fail_commit(dir_path):
            await crawl_items(mockserver, [{"foo": "bar3"}], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        [record] = [
            record
            for record in state["feed_atomic_batches"][template]
            if record["batch_id"] == 1
        ]
        assert record["items"] == [{"foo": "bar1"}, {"foo": "bar2"}]
        assert Path(record["staging"]).exists()

    @coroutine_test
    async def test_open_batch_complete_commit_fails(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """An open batch whose staged data is complete but whose commit fails
        is kept in the state, and the crawl continues with the next batch."""
        settings = self._settings(tmp_path)
        template = next(iter(settings["FEEDS"]))
        with fail_commit(feed_dir(settings), seal=False):
            await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        [saved] = state["feed_atomic_batches"][template]
        # The previous run wrote the batch completely, but its commit did not
        # run.
        _seal_staging_file(saved["staging"])
        with fail_commit(feed_dir(settings), once=True):
            await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        assert [
            record["batch_id"] for record in state["feed_atomic_batches"][template]
        ] == [1]
        assert [p.read_bytes() for p in batch_files(settings)] == [b'{"foo": "bar2"}\n']

    @coroutine_test
    async def test_open_batch_incomplete_without_items_reuses_identity(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """An interrupted open batch that exported no items is written again
        under the same number, so no number is skipped."""
        settings = self._settings(tmp_path)
        template = next(iter(settings["FEEDS"]))
        with fail_commit(feed_dir(settings), seal=False):
            await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        # The staged data is incomplete and the items were not saved, as when
        # the state is written before the items reach it.
        rewrite_state(tmp_path, template, {"items": []}, batch_id=1)
        await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        assert [p.name for p in batch_files(settings)] == ["1.feed"]
        assert PARSERS["jsonlines"](batch_files(settings)[0].read_bytes()) == [
            {"foo": "bar2"}
        ]

    @coroutine_test
    async def test_open_batch_saved_items_without_staging(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """An open batch whose staged data is gone but whose items were saved
        is exported again from them."""
        settings = self._settings(tmp_path)
        template = next(iter(settings["FEEDS"]))
        with fail_commit(feed_dir(settings), seal=False):
            await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        [saved] = state["feed_atomic_batches"][template]
        Path(saved["staging"]).unlink()
        Path(f"{saved['staging']}.complete").unlink(missing_ok=True)
        await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [[{"foo": "bar1"}, {"foo": "bar2"}]]

    @coroutine_test
    async def test_open_batch_resume_below_batch_size(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A re-exported open batch that stays below the batch size keeps
        receiving the items of this run."""
        settings = self._settings(tmp_path)
        with fail_commit(feed_dir(settings), seal=False):
            await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        await crawl_items(mockserver, [], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [[{"foo": "bar1"}]]

    @coroutine_test
    async def test_closed_batch_retry_export_conflict(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """If the target appears while the items of a closed batch are exported
        again, the retry reports a conflict and keeps nothing pending."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            **atomic_settings(Path(dir_path) / "%(batch_id)d.feed", mode="create"),
            "JOBDIR": str(tmp_path / "job"),
        }
        template = next(iter(settings["FEEDS"]))
        # Interrupt the write, so the batch must be exported again.
        with fail_commit(dir_path, seal=False):
            await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        [saved] = state["feed_atomic_batches"][template]
        target = Path(file_uri_to_path(saved["uri"]))
        target.parent.mkdir(parents=True, exist_ok=True)
        # The target appears after the retry opened its staging file.
        original_commit = _commit_staging_file

        def commit(staging_path: str, final_path: str, write_mode: Any) -> None:
            target.write_bytes(b"existing")
            return original_commit(staging_path, final_path, write_mode)

        with mock.patch("scrapy.extensions.feedexport._commit_staging_file", commit):
            crawler = await crawl_items(mockserver, [{"foo": "bar3"}], settings)
        assert target.read_bytes() == b"existing"
        assert crawler.stats
        assert crawler.stats.get_value("feedexport/conflicts/FileFeedStorage") == 1
        state = pickle.loads((tmp_path / "job" / "spider.state").read_bytes())
        assert template not in state.get("feed_atomic_batches", {})

    @coroutine_test
    async def test_finish_exporting_failure_keeps_staged_data(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """If finishing the export of a batch fails, the incomplete staged data
        is left behind and no target is written."""

        class BrokenExporter(scrapy.exporters.JsonLinesItemExporter):
            def finish_exporting(self):
                raise RuntimeError("Cannot finish")

        settings = {
            **atomic_settings(unique_path(tmp_path) / "%(batch_id)d.feed"),
            "FEED_EXPORTERS": {"jsonlines": BrokenExporter},
        }
        crawler, received = await crawl_items_listening(
            mockserver,
            [{"foo": "bar1"}, {"foo": "bar2"}],
            settings,
            signals.feed_slot_closed,
        )
        assert [kw["slot"].commit_state for kw in received] == ["failed"]
        dir_path = feed_dir(settings)
        assert not batch_files(settings)
        staged = [
            p
            for p in dir_path.iterdir()
            if "part-" in p.name and not p.name.endswith(".complete")
        ]
        assert staged
        assert crawler.stats
        assert crawler.stats.get_value("feedexport/failed_count/FileFeedStorage") == 1

    @coroutine_test
    async def test_slot_closed_once(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A slot that fills up during the crawl is not stored again when the
        spider closes, so its target is not committed twice."""
        settings = atomic_settings(unique_path(tmp_path) / "%(batch_id)d.feed")
        crawler, received = await crawl_items_listening(
            mockserver,
            [{"foo": "bar1"}, {"foo": "bar2"}, {"foo": "bar3"}],
            settings,
            signals.feed_slot_closed,
        )
        assert [kw["slot"].commit_state for kw in received] == ["committed"] * 2
        assert crawler.stats
        assert crawler.stats.get_value("feedexport/success_count/FileFeedStorage") == 2

    @coroutine_test
    async def test_non_atomic_finish_failure_is_not_stored(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A failure while finishing the export of a feed without atomic
        commits propagates, as before, and no success is recorded."""

        class BrokenExporter(scrapy.exporters.JsonLinesItemExporter):
            def finish_exporting(self):
                raise RuntimeError("Cannot finish")

        settings = {
            "FEEDS": {
                build_url(unique_path(tmp_path) / "feed.feed"): {"format": "jsonlines"}
            },
            "FEED_EXPORTERS": {"jsonlines": BrokenExporter},
        }
        opened: list[IO[bytes]] = []
        original_open = FileFeedStorage.open

        def open_(self, spider):
            file = original_open(self, spider)
            opened.append(file)
            return file

        with mock.patch.object(FileFeedStorage, "open", open_):
            crawler = await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        # Without an atomic commit the failed export leaves its file open, so
        # close it here to keep the test self-contained.
        for file in opened:
            file.close()
        assert crawler.stats
        assert (
            "feedexport/success_count/FileFeedStorage" not in crawler.stats.get_stats()
        )

    @coroutine_test
    async def test_resumed_feed_retried_on_every_run(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A batch that keeps failing to commit is retried on each resume, and
        the items of each run still reach their targets without duplicates."""
        settings = self._settings(tmp_path)
        with fail_commit(feed_dir(settings), seal=False):
            await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        with fail_commit(feed_dir(settings)):
            await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        await crawl_items(mockserver, [{"foo": "bar3"}], settings)
        assert [
            PARSERS["jsonlines"](p.read_bytes()) for p in batch_files(settings)
        ] == [[{"foo": "bar1"}, {"foo": "bar2"}], [{"foo": "bar3"}]]
