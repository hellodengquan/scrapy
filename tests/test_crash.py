from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

from scrapy.pqueues import _path_safe
from tests.utils.cmdline import proc

if TYPE_CHECKING:
    from pathlib import Path


# Dies without any cleanup once kill_at requests have reached the scheduler.
SPIDER = """
import os

from scrapy import Request, Spider, signals


class KillSpider(Spider):
    name = "kill"
    custom_settings = {
        "SCHEDULER_DISK_QUEUE": "scrapy.squeues.PickleFifoSQLiteQueue",
        "SCHEDULER_START_DISK_QUEUE": "scrapy.squeues.PickleFifoSQLiteQueue",
        "CONCURRENT_REQUESTS": 1,
        "JOBDIR_SYNC_EVERY": 1,
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.scheduled = 0
        crawler.signals.connect(spider.request_scheduled, signals.request_scheduled)
        return spider

    def request_scheduled(self, request, spider):
        self.scheduled += 1
        if self.scheduled == int(getattr(self, "kill_at", 0)):
            os._exit(1)

    async def start(self):
        if hasattr(self, "resume"):
            return
        for i in range(20):
            yield Request(f"data:,{i}", priority=i % 3)

    def parse(self, response):
        self.logger.info(f"Parsed {response.url}")
"""


def run(tmp_path: Path, *args: str) -> str:
    spider = tmp_path / "spider.py"
    spider.write_text(SPIDER, encoding="utf-8")
    jobdir = tmp_path / "job"
    _, _, log = proc(
        "runspider", str(spider), "-s", f"JOBDIR={jobdir}", *args, cwd=tmp_path
    )
    return log


def _active_json(tmp_path: Path) -> Any:
    path = tmp_path / "job" / "requests.queue" / "active.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_resume_after_kill(tmp_path: Path) -> None:
    # The delay keeps all but the first request or two in the queue when the
    # spider dies, while start requests keep being scheduled eagerly.
    run(tmp_path, "-a", "kill_at=10", "-s", "DOWNLOAD_DELAY=5")
    [(slot, priorities)] = _active_json(tmp_path).items()
    slot_dir = tmp_path / "job" / "requests.queue" / _path_safe(slot)
    assert set(priorities) == {
        int(path.name.rstrip("s")) for path in slot_dir.iterdir()
    }

    log = run(tmp_path, "-a", "resume=1")
    match = re.search(r"Resuming crawl \((\d+) requests scheduled\)", log)
    assert match, log
    pending = int(match.group(1))
    assert pending > 0
    assert log.count("Parsed data:,") == pending
    assert "Spider closed (finished)" in log
    assert _active_json(tmp_path) == {}


def test_seen_requests_survive_kill(tmp_path: Path) -> None:
    dupefilter = "DUPEFILTER_CLASS=scrapy.dupefilters.DiskDupeFilter"
    log = run(tmp_path, "-a", "kill_at=10", "-s", "DOWNLOAD_DELAY=5", "-s", dupefilter)
    # Without resume, the spider sends all its start requests again, and only
    # those that did not reach the scheduler before the kill must get through.
    log += run(tmp_path, "-s", dupefilter)
    parsed = re.findall(r"Parsed (data:,\d+)", log)
    assert len(parsed) == len(set(parsed))
    # The request being downloaded when the spider dies is lost.
    assert len(parsed) >= 19


ATOMIC_SPIDER = """
import os

from scrapy import Request, Spider, signals


class AtomicSpider(Spider):
    name = "atomic"
    custom_settings = {
        "FEEDS": {
            __import__("pathlib").Path(__file__).parent / "out" / "%(batch_id)d.jsonl": {
                "format": "jsonlines",
                "atomic": True,
            },
        },
        "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
        "JOBDIR_SYNC_EVERY": 1,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scraped = 0

    def item_scraped(self, item, response, spider):
        self.scraped += 1
        # Die right after the second item, i.e. after the first batch is full
        # but before its close has finished.
        if self.scraped == int(getattr(self, "kill_at", 0)):
            os._exit(1)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.item_scraped, signals.item_scraped)
        return spider

    async def start(self):
        if hasattr(self, "resume"):
            return
        for i in range(4):
            yield Request(f"data:,{i}")

    def parse(self, response):
        yield {"value": response.url}
"""


def run_atomic(tmp_path: Path, *args: str) -> str:
    spider = tmp_path / "atomic.py"
    spider.write_text(ATOMIC_SPIDER, encoding="utf-8")
    _, _, log = proc(
        "runspider",
        str(spider),
        "-s",
        f"JOBDIR={tmp_path / 'job'}",
        "--loglevel",
        "INFO",
        *args,
        cwd=tmp_path,
    )
    return log


def test_atomic_feed_incomplete_file_not_visible(tmp_path: Path) -> None:
    """A killed crawl never leaves an incomplete batch at its target.

    The staged file remains in place, because the killed crawl had no chance
    to save the state that a resumed crawl needs to recover it.
    """
    out = tmp_path / "out"
    run_atomic(tmp_path, "-a", "kill_at=2")
    assert not (tmp_path / "job" / "spider.state").exists()
    targets = [p for p in out.iterdir() if p.suffix == ".jsonl"]
    assert targets == []
    staged = [p for p in out.iterdir() if "part-" in p.name]
    assert staged
    # The staged data was not marked complete, so no later run publishes it.
    assert not any(p.with_name(f"{p.name}.complete").exists() for p in staged)

    run_atomic(tmp_path, "-a", "resume=1")
    # Every line at a target is a complete, parseable item: the incomplete
    # staged data of the killed run was never published.
    for file in out.iterdir():
        if file.suffix != ".jsonl":
            continue
        for line in file.read_text(encoding="utf-8").splitlines():
            assert json.loads(line).keys() == {"value"}
