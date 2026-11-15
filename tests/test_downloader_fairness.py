"""Tests for fair sharing of the global transfer budget between downloader
slots when some slots are slow, backing off or full.
"""

from __future__ import annotations

import asyncio
from email.utils import formatdate
from time import monotonic, time
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs, urlparse

import pytest

from scrapy import Request, Spider
from scrapy.core.downloader import Downloader
from scrapy.core.downloader.handlers.base import BaseDownloadHandler
from scrapy.exceptions import DownloadCancelledError
from scrapy.http import Response
from scrapy.utils.asyncio import sleep
from scrapy.utils.defer import maybe_deferred_to_future
from scrapy.utils.test import get_crawler
from tests.spiders import MockServerSpider
from tests.utils.decorators import coroutine_test

if TYPE_CHECKING:
    from scrapy.crawler import Crawler
    from tests.mockserver.http import MockServer

# Windows asyncio timers can fire up to one tick early.
_TOLERANCE = 0.05


def _query(url: str) -> dict[str, list[str]]:
    return parse_qs(urlparse(url).query)


class ScriptedHandler(BaseDownloadHandler):
    """Download handler whose behavior is fully described by the query string
    of each request URL, so tests run against the real downloader without a
    network and with fixed delays.

    ``delay`` makes the download take that many seconds. ``status`` and
    ``retry_after`` produce a response with that status and Retry-After header.
    Every start and finish is recorded with the request's download slot.
    """

    def __init__(self, crawler: Crawler) -> None:
        super().__init__(crawler)
        self.events: list[tuple[str, str, float]] = []

    async def download_request(self, request: Request) -> Response:
        query = _query(request.url)
        slot = request.meta["download_slot"]
        delay = float(query.get("delay", ["0"])[0])
        status = int(query.get("status", ["200"])[0])
        self.events.append(("start", slot, monotonic()))
        if delay:
            await sleep(delay)
        headers: dict[str, str] = {}
        if "retry_after" in query:
            headers["Retry-After"] = query["retry_after"][0]
        self.events.append(("finish", slot, monotonic()))
        return Response(request.url, status=status, headers=headers)


def _downloader(**settings: Any) -> Downloader:
    crawler = get_crawler(DefaultSpider, settings_dict=settings)
    crawler.spider = crawler._create_spider()
    return Downloader(crawler)


class DefaultSpider(Spider):
    name = "fairness"
    start_urls: list[str] = []


def _request(slot: str, name: str, **query: Any) -> Request:
    params = "&".join(f"{key}={value}" for key, value in query.items())
    url = (
        f"http://{slot}.example/{name}?{params}"
        if params
        else (f"http://{slot}.example/{name}")
    )
    return Request(url, meta={"download_slot": slot}, dont_filter=True)


# ---------------------------------------------------------------------------
# Retry-After parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("2", 2.0),
        ("0", 0.0),
        (" 3.5 ", 3.5),
    ],
)
def test_retry_after_delay_seconds(header: str, expected: float) -> None:
    response = Response("http://slow.example/", headers={"Retry-After": header})
    assert Downloader._retry_after_delay(response) == pytest.approx(expected)


def test_retry_after_delay_http_date() -> None:
    future = formatdate(time() + 10, usegmt=True)
    response = Response("http://slow.example/", headers={"Retry-After": future})
    assert Downloader._retry_after_delay(response) == pytest.approx(10, abs=1)


@pytest.mark.parametrize(
    "header", [None, "", "not-a-date", "Wed, 21 Oct 2015 07:28:00"]
)
def test_retry_after_delay_invalid(header: str | None) -> None:
    response = Response(
        "http://slow.example/",
        headers={} if header is None else {"Retry-After": header},
    )
    assert Downloader._retry_after_delay(response) is None


# ---------------------------------------------------------------------------
# Slot backoff
# ---------------------------------------------------------------------------


@coroutine_test
async def test_backoff_uses_retry_after() -> None:
    downloader = _downloader(
        DOWNLOAD_HANDLERS={"http": ScriptedHandler},
        RETRY_ENABLED=False,
    )
    try:
        start = monotonic()
        await maybe_deferred_to_future(
            downloader.fetch(_request("slow", "first", status=429, retry_after=0.3))
        )
        slot = downloader.slots["slow"]
        assert slot.backoff_until == pytest.approx(start + 0.3, abs=_TOLERANCE)
        assert slot.backoff_attempts == 1

        # A second request waits for the backoff to end before it is sent.
        await maybe_deferred_to_future(
            downloader.fetch(_request("slow", "second", delay=0.05))
        )
        handler = cast("ScriptedHandler", downloader.handlers._handlers["http"])
        first_start = next(
            event_time
            for event, event_slot, event_time in handler.events
            if event == "start" and event_slot == "slow"
        )
        second_start = next(
            event_time
            for event, event_slot, event_time in handler.events[1:]
            if event == "start" and event_slot == "slow"
        )
        assert second_start - first_start >= 0.3 - _TOLERANCE
    finally:
        downloader.close()
        await downloader.handlers._close()


@coroutine_test
async def test_backoff_exponential_without_retry_after() -> None:
    downloader = _downloader(
        DOWNLOAD_HANDLERS={"http": ScriptedHandler},
        RETRY_ENABLED=False,
    )
    try:
        for expected_attempts in range(1, 4):
            await maybe_deferred_to_future(
                downloader.fetch(
                    _request("slow", f"fail-{expected_attempts}", status=503)
                )
            )
            slot = downloader.slots["slow"]
            assert slot.backoff_attempts == expected_attempts
            expected_delay = min(60.0, 1.0 * 2 ** (expected_attempts - 1))
            assert slot.backoff_until - monotonic() == pytest.approx(
                expected_delay, abs=_TOLERANCE
            )
    finally:
        downloader.close()
        await downloader.handlers._close()


@coroutine_test
async def test_success_resets_backoff_attempts() -> None:
    downloader = _downloader(
        DOWNLOAD_HANDLERS={"http": ScriptedHandler},
        RETRY_ENABLED=False,
    )
    try:
        await maybe_deferred_to_future(
            downloader.fetch(_request("slow", "fail", status=429, retry_after=0))
        )
        slot = downloader.slots["slow"]
        assert slot.backoff_attempts == 1
        await maybe_deferred_to_future(downloader.fetch(_request("slow", "ok")))
        assert slot.backoff_attempts == 0
    finally:
        downloader.close()
        await downloader.handlers._close()


@coroutine_test
async def test_backoff_does_not_block_other_slots() -> None:
    downloader = _downloader(
        CONCURRENT_REQUESTS=2,
        DOWNLOAD_HANDLERS={"http": ScriptedHandler},
        RETRY_ENABLED=False,
    )
    try:
        # Fill the global budget with the slow slot backing off.
        slow_fetches = [
            maybe_deferred_to_future(
                downloader.fetch(
                    _request("slow", f"slow-{index}", status=429, retry_after=1)
                )
            )
            for index in range(2)
        ]
        # Let both slow downloads finish and enter their backoff.
        await sleep(0.1)
        start = monotonic()
        await maybe_deferred_to_future(
            downloader.fetch(_request("fast", "fast", delay=0.05))
        )
        # The other slot is served immediately, it does not wait for the slow
        # slot's backoff.
        assert monotonic() - start < 0.3
        for fetch in slow_fetches:
            await fetch
    finally:
        downloader.close()
        await downloader.handlers._close()


# ---------------------------------------------------------------------------
# Global transfer cap and fair dispatch
# ---------------------------------------------------------------------------


@coroutine_test
async def test_global_transfer_cap_counts_only_transferring() -> None:
    downloader = _downloader(
        CONCURRENT_REQUESTS=2,
        DOWNLOAD_HANDLERS={"http": ScriptedHandler},
    )
    try:
        fetches = [
            maybe_deferred_to_future(
                downloader.fetch(_request(f"slot-{index}", "r", delay=0.2))
            )
            for index in range(4)
        ]
        await sleep(0.05)
        assert downloader._transferring_total == 2
        # Queued requests do not take a global transfer place.
        assert sum(len(slot.queue) for slot in downloader.slots.values()) == 2
        for fetch in fetches:
            await fetch
        assert downloader._transferring_total == 0
    finally:
        downloader.close()
        await downloader.handlers._close()


@coroutine_test
async def test_slots_take_turns_at_global_budget() -> None:
    downloader = _downloader(
        CONCURRENT_REQUESTS=1,
        CONCURRENT_REQUESTS_PER_DOMAIN=4,
        DOWNLOAD_HANDLERS={"http": ScriptedHandler},
    )
    try:
        # Enqueue every request without awaiting it, so that they all queue up
        # before any of them gets the single global transfer place. The slot
        # sweep then serves them round-robin instead of letting one slot take
        # the place each time its previous download finishes.
        fetches = [
            maybe_deferred_to_future(
                downloader.fetch(_request(slot, f"r-{slot}", delay=0.05))
            )
            for slot in ("a", "b", "c", "a", "b", "c")
        ]
        handler = cast("ScriptedHandler", downloader.handlers._handlers["http"])
        for _ in range(100):
            if len(handler.events) >= 12:
                break
            await sleep(0.05)
        for fetch in fetches:
            await fetch
        served = [slot for event, slot, _ in handler.events if event == "start"]
        assert served == ["a", "b", "c", "a", "b", "c"]
    finally:
        downloader.close()
        await downloader.handlers._close()


# ---------------------------------------------------------------------------
# Cancellation while a slot is backed off
# ---------------------------------------------------------------------------


@coroutine_test
async def test_fast_stop_while_slot_backed_off() -> None:
    downloader = _downloader(
        CONCURRENT_REQUESTS=1,
        DOWNLOAD_HANDLERS={"http": ScriptedHandler},
        RETRY_ENABLED=False,
    )
    try:
        await maybe_deferred_to_future(
            downloader.fetch(_request("slow", "fail", status=429, retry_after=5))
        )
        # A request queued behind the backoff, and a download in flight on
        # another slot.
        queued = maybe_deferred_to_future(downloader.fetch(_request("slow", "queued")))
        in_flight = maybe_deferred_to_future(
            downloader.fetch(_request("fast", "in-flight", delay=5))
        )
        await sleep(0.1)
        dropped = await downloader.stop()
        assert dropped == 2
        assert downloader._transferring_total == 0
        assert all(not slot.transferring for slot in downloader.slots.values())
        assert all(not slot.queue for slot in downloader.slots.values())
        # The queued request is dropped with a downloader cancellation, and the
        # in-flight download is cancelled at the task level.
        with pytest.raises(DownloadCancelledError):
            await queued
        with pytest.raises((asyncio.CancelledError, DownloadCancelledError)):
            await in_flight
    finally:
        downloader.close()
        await downloader.handlers._close()


# ---------------------------------------------------------------------------
# End-to-end crawl against the mock server
# ---------------------------------------------------------------------------


class BackoffSpider(MockServerSpider):
    """Crawl two logical domains sharing the mock server. The slow domain
    answers 429 with a Retry-After header while the fast domain keeps serving
    200 responses.
    """

    name = "backoff"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.events: list[tuple[str, float, int]] = []
        self.start_time = monotonic()

    async def start(self):
        assert self.mockserver
        base = self.mockserver.url("/rate-limit")
        for index in range(3):
            yield Request(
                f"{base}?delay=0.05&status=429&retry_after=0.3&n={index}",
                meta={"download_slot": "slow.example"},
                dont_filter=True,
                callback=self.parse,
                errback=self.record_failure,
            )
        for index in range(3):
            yield Request(
                f"{base}?delay=0.05&n={index}",
                meta={"download_slot": "fast.example"},
                dont_filter=True,
                callback=self.parse,
            )

    def parse(self, response: Response) -> Any:
        slot = response.meta["download_slot"]
        self.events.append((slot, monotonic() - self.start_time, response.status))

    def record_failure(self, failure: Any) -> None:
        request = failure.request
        slot = request.meta["download_slot"]
        # The Retry middleware gives up after RETRY_TIMES and reports the last
        # response's status through the failure.
        status = getattr(failure.value, "response", None)
        code = status.status if status is not None else 0
        self.events.append((slot, monotonic() - self.start_time, code))


@coroutine_test
async def test_backoff_crawl_keeps_other_domain_progressing(
    mockserver: MockServer,
) -> None:
    crawler = get_crawler(
        BackoffSpider,
        settings_dict={
            "CONCURRENT_REQUESTS": 2,
            "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
            "DOWNLOAD_DELAY": 0,
            "DOWNLOAD_DELAY_JITTER": 0,
            "RETRY_TIMES": 0,
        },
    )
    await crawler.crawl_async(mockserver=mockserver)
    spider = crawler.spider
    assert isinstance(spider, BackoffSpider)

    slow = [event for event in spider.events if event[0] == "slow.example"]
    fast = [event for event in spider.events if event[0] == "fast.example"]
    assert len(slow) == 3
    assert len(fast) == 3
    # Every fast response arrives before the slow slot's first backoff ends,
    # i.e. the backing-off domain never stalls the other one.
    assert max(event[1] for event in fast) < min(event[1] for event in slow) + 0.3
    assert crawler.stats.get_value("downloader/backoff_count", 0) >= 1
    assert crawler.stats.get_value("spider_exceptions", 0) == 0


@coroutine_test
async def test_backoff_crawl_is_deterministic(mockserver: MockServer) -> None:
    """Running the same fixed-delay crawl twice produces the same slot order."""

    async def run() -> list[str]:
        crawler = get_crawler(
            BackoffSpider,
            settings_dict={
                "CONCURRENT_REQUESTS": 2,
                "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
                "DOWNLOAD_DELAY": 0,
                "DOWNLOAD_DELAY_JITTER": 0,
                "RETRY_TIMES": 0,
            },
        )
        await crawler.crawl_async(mockserver=mockserver)
        spider = crawler.spider
        assert isinstance(spider, BackoffSpider)
        return [event[0] for event in spider.events]

    first = await run()
    second = await run()
    assert first == second


def test_backoff_signal_connected_per_slot() -> None:
    """The backoff state lives on the slot, so two slots never share it."""
    crawler = get_crawler(DefaultSpider)
    crawler.spider = crawler._create_spider()
    downloader = Downloader(crawler)
    try:
        first_request = Request("http://a.example/")
        second_request = Request("http://b.example/")
        _, first_slot = downloader._get_slot(first_request)
        _, second_slot = downloader._get_slot(second_request)
        first_slot.backoff_until = monotonic() + 10
        first_slot.backoff_attempts = 2
        assert second_slot.backoff_until == 0.0
        assert second_slot.backoff_attempts == 0
    finally:
        downloader.close()
