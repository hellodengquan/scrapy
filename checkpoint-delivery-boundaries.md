# Checkpoint delivery boundaries in Scrapy's resumable crawls

This report analyses how four independently persisted stages interact when a
crawl is resumed from `JOBDIR`:

1. the scheduler queue (`requests.queue/`, `active.json` plus the per-priority
   disk queues),
2. the duplicate filter (`requests.seen` or `requests.seen.db`),
3. the download/retry path (a response that becomes a new queued request),
4. item landing (the item pipeline and `FeedExporter` writing `FEEDS`).

Each stage is durable on a different trigger, and no stage is transactional
with respect to any other. The report derives the delivery guarantee each
stage actually provides, locates the windows where pages are silently skipped
or items are duplicated, and then specifies an idempotent SQLite landing
design. It closes with what changing the request fingerprint cannot fix and how
any such change would alter previous behaviour and the resume flow.

Everything below is reproducible locally with the commands in
[Reproduction](#reproduction). Findings are marked *verified* when they were
observed with a real Scrapy 2.20.0 crawl against a local HTTP server, or *derived*
when they follow from the code paths without a dedicated run.

---

## 1. Ground truth: what each stage commits, and when

### 1.1 Scheduler queue

`scrapy/core/scheduler.py`:

- `enqueue_request()` (`:424`) consults the dupefilter first (`:438`), then
  calls `_dqpush()` (`:442`).
- `next_request()` (`:452`) pops from the memory or disk priority queue and
  **never consults the dupefilter**. The dupefilter is purely an enqueue-time
  gate; this asymmetry drives Window B.
- `_dqpush()` calls `self.dqs.push(request)` (`:486`) and, in a `finally`
  block, `_sync_dqs_state()` (`:505`).
- `_sync_dqs_state()` (`:528`) only writes anything when `JOBDIR_SYNC_EVERY`
  is non-zero **and** the priority queue exposes a truthy `changed`
  attribute; it then writes `active.json` every `_sync_every` changes.
- `_write_dqs_state()` (`:591`) writes `active.json.tmp` and `os.replace()`s
  it over `active.json`, so the *index file* is atomic.

The key structural fact: **`active.json` is an index of which priority
sub-directories to open, not a description of queue contents.** On resume,
`_dq()` (`:553`) builds the priority queue from `startprios=state`, and
`ScrapyPriorityQueue.init_prios()` (`scrapy/pqueues.py:180`) only creates a
downstream queue for priorities listed in that state. A priority directory
that exists on disk but is absent from `active.json` is never opened.

The durability of the rows inside those directories depends entirely on the
queue class:

| Queue class | Where the data lives | Survives `os._exit()`? |
| --- | --- | --- |
| `PickleLifoDiskQueue` / `PickleFifoDiskQueue` | append-only chunk files plus `info.json` | **No.** `info.json` is written only in `close()` (queuelib `FifoDiskQueue.close`), and the LIFO queue only rewrites its 4-byte size header on `close()`. |
| `PickleLifoSQLiteQueue` / `PickleFifoSQLiteQueue` | SQLite table `queue` | **Yes**, for every committed `INSERT`/`DELETE` (each `with self._db as conn` block commits). |

*Verified.* Pushing five items, popping two, then exiting without `close()`
gives:

```
fifo   reopen -> []                      (all five lost)
lifo   reopen -> []                      (all five lost, file even removed)
sqlite reopen -> ['item-2','item-3','item-4']   (pops durable)
```

Therefore: with the default `SCHEDULER_DISK_QUEUE` / `SCHEDULER_START_DISK_QUEUE`
(`PickleLifoDiskQueue` / `PickleFifoDiskQueue`) the *only* durable scheduler
artifact is whatever `active.json` happened to contain. The requests themselves
are effectively **zero-or-one**: a hard kill loses the whole queue body.

### 1.2 Duplicate filter

`scrapy/dupefilters.py`:

- `RFPDupeFilter.request_seen()` (`:140`) adds the fingerprint to an in-memory
  set, then appends it to a buffered `a+b` file handle (`:146`). Nothing flushes
  it. `close()` (`:158`) closes the handle, which flushes.
- `DiskDupeFilter.request_seen()` (`:228`) does `INSERT OR IGNORE`
  (`:230`), then commits only every `JOBDIR_SYNC_EVERY` insert (`:233`),
  or on `close()` (`:241`). The database runs `journal_mode=WAL` with
  `synchronous=NORMAL` (`:213-214`), so committed rows survive process death
  but do **not** survive an OS crash / power loss.

So the dupefilter is **at-most-once deduplication with a lag**: a request is
recorded as "seen" strictly *before* it is enqueued, but the record may or may
not be durable depending on the sync setting, and the write is not atomic with
the queue push.

### 1.3 Download and retry

`get_retry_request()` (`scrapy/downloadermiddlewares/retry.py:38`) builds a new
`Request` with `meta["retry_times"] += 1` and `meta["skip_dupefilter_once"] =
True` (`:112-114`). That new request is yielded from `process_response`, travels
back through the engine, and is enqueued like any other request.

`Scheduler.enqueue_request()` pops `skip_dupefilter_once` (`:434`), so the
retry request bypasses *this run's* dupefilter check — but its fingerprint is
also **not inserted** into the dupefilter, because the `if` short-circuits
before `df.request_seen()` (`:438`). The retry request is therefore invisible to
the dupefilter forever.

On resume, a persisted retry request is reloaded from the disk queue and
re-downloaded normally. That is not because its fingerprint is missing; it is
because `Scheduler.next_request()` (`:452`) returns rows straight from
`next_priority()`/`pop()` without consulting the dupefilter at all. The
dupefilter is a *gate at enqueue time only*. This asymmetry is the root of
Window B: a request that is re-created by the spider must pass the gate again,
while a request that was already queued never does.

### 1.4 Item landing

Items reach persistence through `ITEM_PIPELINES` / `ITEM_PROCESSOR` and through
`FeedExporter`, both driven by the `item_scraped` signal
(`scrapy/core/scraper.py:639`). `FeedExporter.item_scraped()`
(`scrapy/extensions/feedexport.py:964`) is an **async signal handler that the
scraper awaits inline**; it serialises into the feed file under a
`DeferredLock` (`:693`) but does not `flush()` or `fsync()`.

`FeedSlot.finish_exporting()` (`:661`) only stops the exporter; the file is
handed to `storage.store()` at slot close (`:912`), which is where
`FileFeedStorage.store()` (`:273`) actually `close()`s the handle.

There is **no checkpoint for landed items at all**. The `spider.state`
extension (`scrapy/extensions/spiderstate.py`) saves a spawn-time
`feed_batch_ids` map (`feedexport.py:818`) but no per-item progress marker.

---

## 2. Delivery guarantees, stage by stage

Reading the four stages together yields the following, where "at-least-once"
means the stage can re-deliver work and "at-most-once" means it can drop work.

| Stage | Guarantee | Why |
| --- | --- | --- |
| Scheduler enqueue, default disk queues | **at-most-once** (in practice zero-or-one) | rows are lost on hard kill; `active.json` survives only if a sync happened |
| Scheduler enqueue, SQLite disk queues + `JOBDIR_SYNC_EVERY>=1` | **at-least-once** for the request, **at-most-once** for the index | rows commit on every push; `active.json` lags and may omit a directory |
| Scheduler dequeue → download | **at-most-once** | `next_request()` removes the row before the download starts; a kill mid-download loses it |
| Dupefilter | **at-most-once** dedupe, with a durability lag | fingerprint is recorded before enqueue; not committed until `close()` or every N inserts |
| Download → retry → enqueue | **at-most-once** at the build→enqueue step; **at-least-once** once the retry is committed to a SQLite queue | a retry built but not yet enqueued is lost; a committed retry resumes and re-downloads |
| Item pipeline (`process_item`) | **no guarantee** | no checkpoint; a kill during processing loses the item |
| Feed export | **no guarantee** | no flush/fsync: a kill loses buffered items, and a resumed run can append the same items again |

The single sentence to remember: **Scrapy's resumable crawl is at-most-once
for pages and at-least-once for items, and only the SQLite queue classes make
even the page side survive a hard kill at all.**

---

## 3. Interruption timings and the windows they open

Each case below is a hard kill (`os._exit(137)`) at a precise hook, followed by
a second run on the same `JOBDIR`. The harness is in
[Reproduction](#reproduction).

### 3.1 Graceful pause vs. hard kill

*Verified.* A SIGINT (first Ctrl-C) drains in-flight work and runs the whole
close sequence: the scheduler `close()` (`scheduler.py:413`) writes
`active.json`, the dupefilter `close()` commits, `FeedExporter.close_spider()`
(`feedexport.py:844`) awaits `_close_slot()` and stores the file. Across four
different SIGINT delays, the resumed run lost **zero** pages and produced
**zero** duplicate rows:

```
SIGDELAY=1.2  run2 parsed ['4','5','6']  feed [0..6]  dups []
SIGDELAY=1.5  run2 parsed ['5','6']      feed [0..6]  dups []
SIGDELAY=1.8  run2 parsed ['6']          feed [0..6]  dups []
SIGDELAY=2.4  run2 parsed ['6']          feed [0..6]  dups []
```

This is the important negative result: **there is no page-loss or
item-duplication bug in the graceful path.** Every window below requires an
unclean termination (SIGKILL, `os._exit`, crash, OOM, container eviction), a
second Ctrl-C ("fast" shutdown, `crawler.py:871`), or a third Ctrl-C ("kill"
shutdown, `crawler.py:878`, which is a race).

*Verified* for the three stop modes on one server and one job directory, with
six requests at priorities 0-2 and a 1.5 s download delay:

```
stop        run1 fetched   resume fetched        feed rows        outcome
graceful    [0, 2]         [5, 1, 4, 3]          [0,2,5,1,4,3]    exact
fast (2x)   [0]            [5, 1, 4, 3]          [0,5,1,4,3]      page 2 lost (in flight)
```

The fast stop drops the in-flight download (Window A) and nothing is duplicated;
pages are simply missing from the feed with no error. It is a deterministic
consequence of `_fast_stop_downloader()` (`engine.py:904`) discarding the
requests still held by the downloader.

The third Ctrl-C (`crawler.py:878`) is a **race**, not a deterministic state:
whether the process exits before the scheduler's `close()` writes the final
`active.json` depends on how far graceful shutdown has progressed. Two runs of
the same scenario produced different results:

```
run A: active.json [-1, 0]   resume [1, 4, 3]   page 5 never resumed   (Window C)
run B: active.json [-2,-1,0] resume [5, 1, 4, 3] complete
```

So the force kill can convert a graceful stop into Window C, but does not
reliably do so. Window A, by contrast, is deterministic for the fast stop.

### 3.2 Window A — dequeue before download (lost page)

This window is independent of the missing-index window (§3.4): it happens even
when `active.json` is fully up to date, because `next_request()` has already
removed the row from the SQLite queue.

*Verified.* Six start requests are enqueued at priorities 0, 1, 2, 0, 1, 2;
the crawl downloads with `CONCURRENT_REQUESTS=1` and is killed inside
`process_request` for the 3rd download. At that point the queue still holds
three rows and the index lists both priorities:

```
run1 rc=137 parsed ['0']
active.json: {"127.0.0.1": [-1, 0]}
  .../-1s rows 2
  .../0s rows 1
run2 parsed [1, 4, 3]        (DiskDupeFilter)
feed pages [1, 4, 3]         (page 2 is nowhere: 0 was in the lost feed buffer,
                             2 was the in-flight request)
```

Page 2 was dequeued and was being downloaded when the process died. Its queue
row is gone, no other queue row references it, and no `start()` request
re-creates it. Result: **silent page loss with no error stat**, even with SQLite
queues, a synchronised index, and a fully durable dupefilter.

With the default `RFPDupeFilter` the resumed run re-derives pages 0-5 from
`start()` (since the fingerprints were lost), so the page is recovered — at the
cost of re-downloading everything and duplicating the feed rows (§3.7).

### 3.3 Window B — fingerprint committed, request not yet enqueued (lost page)

This is the most dangerous window because it cannot be detected at all.

Sequence inside `Scheduler.enqueue_request()`:

```
line 438: self.df.request_seen(request)   # inserts + (maybe) commits fp
line 442: self._dqpush(request)           # pushes to disk queue
```

A kill between the two leaves a durable fingerprint with no request. On resume,
the spider's `start()` re-yields and the callback re-creates the same URL — but
`request_seen()` returns `True`, the request is dropped, and
`dupefilter/filtered` increments. Nothing re-queues it.

*Verified* with a `DiskDupeFilter` subclass that exits after `request_seen()`
has committed a fingerprint and before the scheduler pushes the request
(`dupe_diskdf_1`). Run 1 fetched page 0; the callback yielded page 1, whose
fingerprint was committed, and the process died. Run 2 on the same job
directory:

```
run2 parsed []   filtered 1   feed rows 0
```

Not even page 0 — the seed URL — came back, because its fingerprint had been
committed when it was first enqueued. **A single unclean kill between the two
lines can therefore lose the crawl's entry point.**

Note the asymmetry with `RFPDupeFilter`: because its `requests.seen` handle is
buffered and never flushed until `close()`, the *unclean* kill usually loses
the fingerprint too, so `start()` can re-fetch the page (verified in
`chain_kill_after_dequeue_rfp`: all 12 pages were re-downloaded, but no
duplicate feed rows appeared, because the first run's buffered feed rows were
lost as well — see Window D). So `DiskDupeFilter` turns "duplicate work" into
"lost work" — a strictly worse failure mode for a resumable crawl.

### 3.4 Window C — committed queue row missing from `active.json` (ghost row)

*Verified deterministically* with `ScrapyPriorityQueue` directly:

```
push two priority-0 requests, close()     -> dirs ['0']
active.json written as []                 -> (simulating a kill before index sync)
resume                                    -> len 0, pop None
push one new priority-0 request           -> len 3
pop all                                   -> ['/a', '/b', '/c']
```

So a row that is durable in SQLite but *not indexed* is invisible at resume
start and only resurfaces if the same priority directory is reopened later.
When it resurfaces it is a stale request that the current run believes is done;
it re-downloads and re-emits its items.

*Verified end-to-end* (`ghost_dup`), killing in the `finally` of `_dqpush`
before `_sync_dqs_state()` writes:

```
run1 rc=137  active.json ABSENT  (one start-request row committed in 0s/)
run2 parsed [0, 0, 1]  feed [0, 0, 1]  dups [0]
```

The same file landing twice is the item-duplication window.

### 3.5 Window D — unflushed feed buffer (lost items, not lost pages)

*Verified.* The item is handed to `JsonLinesItemExporter.export_item()`
(`exporters.py:195`), which writes to a buffered file object. Kill
immediately after `item_scraped` for the 4th item, with SQLite queues and the
disk dupefilter:

```
run1 rc=137 parsed ['0','1','2','3']
run2 parsed ['4'..'11']
feed rows [4..11]        # 0..3 are gone
```

The pages 0-3 were fetched and processed; only their feed rows vanished. With
the default (in-memory, buffered) dupefilter the resumed run re-fetches 0-3 and
the feed is complete but has duplicate work (see §3.7).

The buffering is size-dependent, which makes this window nondeterministic:

*Verified* with 20 KB items:

```
big=0 (small) queue=lifo   feed_lines_after_kill=0   resume -> 7 pages, dups []
big=0 (small) queue=sqlite feed_lines_after_kill=0   resume -> 8 pages, dups [4]
big=1 (20KB)  queue=lifo   feed_lines_after_kill=2   resume feed [0,2,0,2,1,4,3,6,5] dups [0,2]
big=1 (20KB)  queue=sqlite feed_lines_after_kill=2   resume feed [0,1,4,0,5,6,1,2,3,4] dups [0,1,4]
```

Small items stay in the `BufferedWriter` and are lost; large items overflow it
and survive, which *creates* duplicates on resume. Neither outcome is a
guarantee.

### 3.6 Window E — batch feed ID reset (overwrite destroys, append duplicates)

*Verified.* `FeedExporter` records the current `batch_id` into `spider.state`
only via `spider_state_saving` (`feedexport.py:818`), which fires on graceful
`spider_closed`. On an unclean kill `spider.state` is never written, so
`_on_state_loaded()` (`:782`) sees no `feed_batch_ids` and the resumed run
starts again at `batch_id=1`.

With `FEED_MODE=overwrite` and `FEED_EXPORT_BATCH_ITEM_COUNT=3`:

```
run1 rc=137  feed-1.jsonl before resume: [0, 1, 2]
run2 parsed ['6'..'11']
feed-1.jsonl: [6, 7, 8]      # 0,1,2 silently destroyed
feed-2.jsonl: [9, 10, 11]
```

With `FEED_MODE=append` and the default (buffered) dupefilter the same reset
produces the opposite defect. Killing after item 6 and resuming:

```
run1 rc=137  feed-1.jsonl: [0,1,2,3,4,5]   (first batch closed and flushed)
run2 parsed [6,0,7,1,8,2,9,3,10,4,11,5,6]  # start() re-yields 0..2; the
                                           # fingerprints were never flushed
feed-1..5: [0,1,2,6,0,7] [1,8,2] [9,3,10] [4,11,5] [6]
            16 rows for 12 pages; pages 0,1,2,6 land twice, while 3,4,5 (which
            were only ever in the killed run's unflushed buffer) land once
```

The resumed run's first batch reuses `batch_id=1`, so it appends into the same
`feed-1.jsonl` and then continues numbering the following batches from there.

### 3.7 Window F — dupefilter lag turns loss into duplication

*Verified.* Same 12-page chain, kill on the 4th dequeue:

```
default dupefilter : run2 parsed all 12 pages, feed 12 rows, dups []
DiskDupeFilter     : run2 parsed none,      feed 0 rows, lost 12
```

The default dupefilter is "recovering" only because it forgot what it had seen,
so `start()` re-fetches everything from scratch. After a graceful stop it is
exact; after an unclean stop it is at-least-once (re-fetch), and whether that
shows up as duplicate feed rows depends on whether the first run's rows had
already left the OS buffer (Window D). `DiskDupeFilter` is the reverse: exact
dedupe state, but the lost pages are unrecoverable because `start()` is the
spider's only re-entry point and everything it yields is filtered.

Neither setting is a guarantee. One loses pages; the other re-downloads them and
may duplicate items.

### 3.8 Window G — retry request is not durable

*Verified.* With `RETRY_TIMES=5` and a server returning 500 five times, killing
on the 3rd download leaves no queued retry; the resumed run starts over from
the seed and succeeds. Killing immediately after `RetryMiddleware` built the
retry request but before the engine enqueued it yields the same result
(`retry_kill_retry_built`, `retry_kill_retry_built_diskdf`: page not in the
feed, run 2 re-downloads it from the seed).

A retry that was *built* but not yet enqueued is lost; the next run's `start()`
must rebuild it, which it does only because the spider re-yields the seed. For
a retry whose parent page was already scraped and whose callback is the only
source of that request, a kill after `process_response` returns but before
`enqueue_request()` commits loses the retry permanently, and the fingerprint of
the original request is already in the dupefilter — the same loss mode as
Window B.

Conversely, a retry that *did* reach the SQLite queue is durable and resumes
correctly. `skip_dupefilter_once` is popped at enqueue time so the persisted
request carries no bypass flag, but that does not matter: `next_request()`
never consults the dupefilter, so the queued retry is returned and re-downloaded
(*verified* with `ScrapyPriorityQueue`: two rows survive, retry pops with
`retry_times=1`).

### 3.9 Summary of windows

| Window | Trigger | Page loss | Item duplication |
| --- | --- | --- | --- |
| A dequeue→download | hard kill mid-download | yes | no |
| B fingerprint before enqueue | hard kill between `request_seen` and `push` | **yes, undetectable** | no |
| C unindexed queue row | hard kill before index sync | no | **yes** when the directory reopens |
| D feed buffer | hard kill before flush | no | yes (large items) / items lost (small) |
| E batch ID reset | hard kill before `spider.state` | no | overwrite: items destroyed; append: duplicates |
| F dupefilter lag | hard kill, default dupefilter | no | yes (re-fetch) |
| G retry not durable | hard kill after `process_response` | yes, if no re-seed | no |

The only windows that lose data *without any chance of later recovery by
re-running the spider* are **A** and **B**. Window A loses the in-flight
request from the queue; Window B additionally makes the re-created request
invisible to the dupefilter, so even a spider that re-derives its whole
frontier from `start()` cannot get it back. **The landing ledger in §4 is what
turns both from unrecoverable into "re-fetch and dedupe".**

---

## 4. An idempotent SQLite landing design

The goal: make "an item is in the database" the single durable fact, so that
re-fetching a page (which the design below deliberately allows) cannot create
duplicates, and so that a resumed crawl can prove which pages have landed.

### 4.1 Two tables

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = FULL;     -- "NORMAL" is not enough for power loss
PRAGMA busy_timeout = 5000;

CREATE TABLE IF NOT EXISTS item (
    item_key BLOB PRIMARY KEY,        -- SHA-256 of the canonical item JSON
    payload  BLOB NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS page (
    page_key BLOB PRIMARY KEY,        -- SHA-256 of the canonical response URL
    landed_at TEXT NOT NULL
) WITHOUT ROWID;
```

`item` is keyed by the item itself, not by `(page, item)`. That matters: an item
may legitimately appear on two different pages (a shared author, a re-listed
product), and `(page_key, item_key)` would let both copies in. Keying on the
item alone makes the landing truly idempotent across pages and across
re-fetches.

The `page` table is deliberately **not** a foreign-key parent of `item`. The
pipeline writes items before the middleware writes the marker, so an FK would
fail; and the marker must be able to exist without items (a page with no
extracted items still counts as done).

`page_key` and `item_key` must be derived from *logical* identity, not from
`Request.id` or from the response body: the same logical page can come back
with a different body (a rotating ad, a session token), and that must not
produce a second item row. Canonicalise the URL with
`w3lib.url.canonicalize_url()` before hashing, and normalise the item (sorted
keys, strip volatile fields) before hashing.

### 4.2 Writing items and the page marker

Two hooks are needed, because no single hook sees both the response and the
post-pipeline item:

- an **item pipeline** (`process_item(item)`) writes each item that survived
  the whole pipeline. It cannot see the response, but it does not need to: the
  row is keyed by `item_key` alone.
- a **spider middleware** (`process_spider_output(response, result)`) writes
  the page marker once the callback's output iterable is exhausted. It sees the
  response and the output boundary, which the pipeline never does.

Both connections use `isolation_level=None`, which makes every `execute()`
autocommit. This is essential: a single `BEGIN … commit()` in `close_spider()`
loses everything on an unclean kill, which defeats the purpose.

```python
# pipeline: one durable row per item that survived the whole ITEM_PIPELINES chain
class SqliteLandingPipeline:
    def open_spider(self):
        self.db = connect()                 # PRAGMAs + DDL from 4.1

    def process_item(self, item):
        self.db.execute(
            "INSERT OR IGNORE INTO item VALUES (?,?)",
            (sha256_bytes(json.dumps(dict(item), sort_keys=True)), to_json(item)),
        )
        return item

    def close_spider(self):
        self.db.close()


# spider middleware: the response-finished marker
class PageMarkerMiddleware:
    def __init__(self):
        self.db = connect()

    async def process_spider_output(self, response, result):
        if response is None:                # errback output; no page
            async for out in result:
                yield out
            return
        async for out in result:            # passes items and requests on
            yield out
        # The callback's output is exhausted, so every item it produced has
        # already gone through the item pipeline.
        self.db.execute(
            "INSERT OR IGNORE INTO page VALUES (?, datetime('now'))",
            (sha256_bytes(response.url.encode()),),
        )
```

Two implementation details that are easy to get wrong and were confirmed while
validating this design:

- **The middleware must not define `open_spider`.** Spider middleware has no
  such hook, so a connection opened there raises `AttributeError` on the first
  response. Open the connection in `__init__` or `from_crawler`.
- **The pipeline must not batch the writes.** A single
  `BEGIN … commit()` in `close_spider()` (the natural reading of "one
  transaction") loses every item on an unclean kill. Each `execute()` must be
  autocommitted, which is what `isolation_level=None` gives you.

Why this split, and not one hook:

- **Items must not be written before the pipeline.** `process_spider_output`
  runs *upstream* of the item pipeline: the scraper sends each item yielded by
  the output chain to `start_itemproc_async()` (`scraper.py:548`), so writing
  items in the middleware would persist items that a later pipeline stage drops
  with `DropItem`. The pipeline hook only sees items that survived.
- **The page marker must be written after the items.** `page` is written only
  after the output iterable is exhausted. A kill while the callback is still
  yielding leaves no marker, so the page is fetched again on resume; because
  item writes are idempotent, that re-fetch cannot duplicate a row.
- **`item_scraped` is not usable for the marker.** It carries the response
  (`signals.py:52`) but fires once per item, with no signal that a response's
  items are complete, so two pages emitting zero and three items are
  indistinguishable from three and zero.

The two writes share a database but are separate autocommitted statements, so
they are not atomic with each other. That is deliberate and harmless: items are
idempotent, so the worst case is one extra fetch, never a lost or duplicated
row.

*Validated.* With this design and `dont_filter=True` on frontier requests,
a run killed after four responses left `item=4, page=4` in the database; a
resumed run with the **default** (and therefore stale) `RFPDupeFilter`
re-fetched only the pages that had not landed, ending at `item=7, page=7`; a
third run re-fetched nothing. Every item payload appeared exactly once across
all three runs.

An earlier variant without `dont_filter=True` stalled instead: the resumed
run kept being filtered at the seed, so `item`/`page` never advanced past the
count from the first run.

### 4.3 The frontier that makes resume idempotent

`page` is the progress table: a page with a row has landed. The spider reads
its seed frontier from it instead of re-deriving it blindly:

```python
async def start(self):
    for url in self._seed_urls():
        if not self._db.has_page(canonical_key(url)):
            # dont_filter is required: the landing table, not the dupefilter,
            # decides. A fingerprint committed by the killed run would
            # otherwise veto a page that never landed (Window B).
            yield Request(url, callback=self.parse_page, dont_filter=True)
```

`dont_filter=True` here is not a shortcut; it is the whole mechanism. Without
it the frontier only skips pages whose fingerprints the *previous* run already
committed, and the exact pages that were lost in Window A/B come back
filtered — the design would reproduce the bug it is meant to fix.

This is what closes Window B for seeded URLs: a fingerprint left in
`requests.seen` without a page row does **not** stop the seed request from
being re-issued, because the check is against `page`, and the request bypasses
the dupefilter entirely.

For pages discovered *from* a landed page, the links are re-extracted when that
page is re-fetched, so the frontier reconstructs itself. The design therefore
does not need to persist a separate "discovered but not visited" set.

Crucially, the design **does not depend on the dupefilter at all**. It tolerates
re-fetching (landing is idempotent) and it tolerates a stale `requests.seen`
(the `page` table is authoritative). That is the property a resumed crawl
needs, and it is precisely the property the fingerprint cannot provide. The
cost is that duplication prevention moves from the dupefilter to `item_key`,
which is the only place where it can be correct.

### 4.4 Making the feed safe too

Feed exports cannot be made transactional through a custom pipeline. The
practical options, in decreasing order of robustness:

- land into SQLite and treat the feed as a *view* regenerated from the database
  after the crawl (the feed is then disposable);
- keep `FEED_MODE=append` and disable `FEED_EXPORT_BATCH_ITEM_COUNT`, which is
  what makes Window E destructive (a reset `batch_id` then appends to a
  per-run URI instead of overwriting `feed-1`);
- add a `feed_slot_closed` handler that records the last landed `item_key`, so
  a resumed run can skip rows the previous run already wrote.

`FEED_MODE=create` on a resumed crawl is always wrong: the target from the
previous run exists, so every item of the resumed run is discarded with a
logged error (`feedexport.py:828`). `overwrite` is only safe if the feed is
regenerated in full from the database.

### 4.5 Fsync and the WAL

`PRAGMA synchronous=NORMAL` is what `DiskDupeFilter` uses today
(`dupefilters.py:214`). It protects against process death but **not** against
power loss or a kernel panic, because it does not fsync the WAL on every
commit. For a landing table whose entire purpose is durability, use
`synchronous=FULL` on the landing connection, or accept `NORMAL` and document
that the guarantee is "process-crash safe, not host-crash safe".

---

## 5. What changing the request fingerprint cannot fix

It is tempting to widen the fingerprint (include more headers, the body, a
timestamp, `Request.id`) so that re-created requests are not filtered. That
fixes nothing in the windows above, and it breaks deduplication in general:

1. **Window A / Window G are not fingerprint problems.** The queue simply does
   not contain the request after a kill. No fingerprint change can re-materialise
   a row that was never written or was dequeued and lost.
2. **Window B is not helped, and can only be "fixed" by disabling dedupe.**
   The fingerprint is written *before* the request is enqueued (`scheduler.py:438`
   vs `:442`). Widening it changes *which* requests collide, not the fact that a
   crash between the two lines leaves a record with no request. The only
   fingerprint change that would let a re-created request through is one that
   differs between attempts (a timestamp, `retry_times`, `Request.id`); that
   makes every re-created request non-matching, which is the same as turning
   deduplication off. The defect is the ordering, not the hash.
3. **Window C cannot be reached by the fingerprint at all.** The row is in the
   queue and is never loaded because `active.json` omits its directory. The
   dupefilter is never consulted for it.
4. **Window D and E live after the dupefilter.** Items are emitted from a
   fetched page; the fingerprint has already done its job. Duplicating or
   destroying feed rows is a landing problem.
5. **`Request.id` is not a stable cross-run identity.** *Verified*: `copy()` and
   `replace()` mint a new `id` and set `parent_id`; a freshly constructed
   `Request` for the same URL gets a *different* `id`. Using `id` in the
   fingerprint would disable deduplication for every retry, redirect and
   re-created request, and would make `requests.seen` grow without bound.
6. **Widening the fingerprint cannot distinguish "this page landed" from "this
   page was fetched".** That is the actual missing bit of state. It requires a
   landing ledger (the `page` table in §4), not a different hash.

The fingerprint is a *deduplication key for requests*. The failures in §3 are
about *what happened after a request was dequeued*, and no change to the key can
record an outcome.

---

## 6. Impact on old behaviour and on the resume flow

Adopting the §4 design changes several behaviours users can observe today.

### 6.1 Resume no longer requires the dupefilter

Today a resumed crawl relies on `requests.seen` to avoid re-scraping pages that
already landed. Under the new design the landing table is the authority:

- a page with a `page` row is skipped at seed time;
- a page without one is re-fetched, and its landing is idempotent.

Consequence: **a missing or reset `DUPEFILTER_CLASS` no longer causes duplicate
items**, because the dedupe key has moved to `item_key` in the landing table.
It *does* mean more downloads than today when `requests.seen` is the only
surviving artifact, since the frontier re-fetches every page with no `page` row.
Operators who currently rely on "drop `requests.seen` to force a full re-crawl"
keep that behaviour, but a re-crawl now produces zero duplicate rows instead of
a duplicated feed.

This flips the meaning of the dupefilter in a resumed crawl: today it is the
thing that prevents re-work; under the design it is only an *in-run* optimiser
(avoid downloading the same URL twice within one process), and correctness no
longer depends on it surviving. That also means a custom
`REQUEST_FINGERPRINTER_CLASS` can no longer cause silent page loss through
Window B — it can at most cause an extra download.

### 6.2 Feeds change shape

- With `FEED_MODE=overwrite` and batch feeds, a resumed run today *silently
  destroys* the first run's batches (Window E, verified). Under the design the
  feed becomes a derived artifact, so either batches are rewritten from the
  database (complete, no loss) or the resumed run appends only unseen
  `item_key`s.
- Existing dashboards that count `feedexport/item_count` will see counts change
  once duplicate rows are suppressed.

### 6.3 `JOBDIR_SYNC_EVERY` becomes less load-bearing

The setting currently trades durability against write cost for *two* artifacts
(`active.json` and `requests.seen.db`). Under the design:

- the landing DB commits per page, so its durability does not depend on
  `JOBDIR_SYNC_EVERY`;
- `active.json` still does. The ghost-row window (§3.4) is only closed if the
  index is written in the same transaction as the queue rows, which the current
  `queuelib` API does not expose.

So `JOBDIR_SYNC_EVERY=1` remains necessary for the SQLite scheduler queues, and
the design should document that **`active.json` is still the weakest link for
pages** even when landing is perfect.

### 6.4 Graceful stops are unchanged

The whole design is a no-op for the graceful path: it already loses nothing
(§3.1). A resumed crawl that stops cleanly keeps its current byte-for-byte
behaviour for feeds and for `requests.seen`.

### 6.5 Migration path

1. Add the landing pipeline and the page-marker middleware; leave `FEEDS`
   untouched. From this point items are deduplicated by `item_key`, so a
   re-fetch is harmless even though it still happens.
2. Make `start()` read the frontier from SQLite and mark its requests
   `dont_filter=True`. This is the only change that affects the resume flow, and
   it must be opt-in per spider (a new spider attribute or setting), because a
   spider that hard-codes its seeds in `start()` cannot use it.
3. Switch batch feeds off (or add the `feed_slot_closed` ledger), and set
   `FEED_MODE` explicitly.
4. Only then consider tightening `DUPEFILTER_CLASS` (for example to
   `DiskDupeFilter`), because from that point a stale fingerprint is a
   performance issue rather than a correctness one.

Each step is independently safe, which matters for existing deployments: step 1
changes no observable behaviour except that duplicate item payloads stop
appearing, and step 2 is the only one that changes what the resumed run
downloads.

### 6.6 What the design still cannot fix

- **Window C** fully: a row committed to a SQLite queue but never indexed in
  `active.json` still needs either a scan of `requests.queue/*/` at resume or a
  queue API that writes index and rows atomically. The design narrows this to
  "SQLite queues only, and only the crash-before-index instant", but does not
  eliminate it.
- **Window A**: the request popped for download is gone from the queue. Only a
  per-request "in flight" ledger (write on dequeue, clear after landing) can
  recover it; the landing design can *detect* the loss (a page in the frontier
  with no `page` row) but not prevent the extra fetch.

---

## 7. Reproduction

All commands were run on macOS with Python 3.13 and a source install of this
working tree (`Scrapy 2.20.0`, `queuelib 1.10.0`). The harness lives outside the
repository; the source tree was not modified.

```bash
python3 -m venv /tmp/scrapy-checkpoint-env
/tmp/scrapy-checkpoint-env/bin/pip install -e .
```

The harness has five pieces:

- `server.py` — a threaded HTTP server serving `/page/<n>` (with links to
  `n+1` and `n+2`), `/flaky` (500 N times, then 200), and `/big/<n>`.
- `spiders/ckpt.py` — a spider that dies from `request_scheduled`,
  `response_received`, or `item_scraped` after the Nth event.
- `spiders/chain.py` — a linear chain that dies on the Nth dequeue, response,
  or item.
- `spiders/probe.py` — downloader middlewares that count `process_request` and
  `process_response` so the kill falls exactly on a desired boundary.
- `spiders/dupe_kill.py`, `spiders/dupes.py` — dupefilter subclasses that die
  right after a fingerprint is inserted (Window B).

Representative invocations, each followed by a second run on the same
`JOBDIR`:

```bash
# Window A: kill with requests still queued
python windowA2.py

# Window B: kill after the fingerprint insert, before the queue push
python dupe_case.py

# Window D: kill after item_scraped, before the feed flush
python run_case.py chain   # case chain_kill_after_item

# Window E: batch feeds with overwrite + unclean kill
python overwrite_batch.py

# Window C: durable queue row missing from active.json
python orphan_controlled.py

# stop-mode comparison (1x / 2x SIGINT)
python stopkinds.py

# graceful baseline
SIGDELAY=1.5 python sigint_case.py

# §4 design: kill mid-crawl, then resume with a stale dupefilter
python design/run.py && python design/idem.py
```

The exact `run.log`, job directory and feed from each case are written under
`/tmp/ckpt-harness/work/<case>/`.

The `design/` directory holds a runnable implementation of §4 —
`design/land.py` (the item pipeline), `design/mid.py` (the page-marker
middleware), `design/spider.py` (a spider whose `start()` reads the frontier
from the `page` table) and `design/run.py`/`design/idem.py` (the drivers). It
was exercised against the same kill hook, and its observed row counts are the
ones quoted in §4.2 and §4.3.

### Determinism notes

- The feed-buffer window (§3.5) is **not deterministic**: whether an item is in
  the OS buffer at kill time depends on its serialized size. The small/large
  contrast above is the reliable signal.
- `os._exit(137)` is used rather than SIGKILL so the kill point is inside the
  crawler process at a known hook; it skips all cleanup exactly as a crash
  would.
- All `JOBDIR` values are distinct per case, per the documentation's warning
  that a job directory must not be shared.

---

## 8. Conclusions

1. **Graceful pause/resume is correct.** No page loss and no item duplication
   was observed across four SIGINT timings. Every defect below needs an unclean
   stop.
2. **Pages are at-most-once** for every scheduler configuration. The default
   non-SQLite queues lose the whole queue body on a hard kill; the SQLite
   queues keep the rows but drop the one already dequeued for download
   (Window A).
3. **Items are at-least-once** whenever the resumed run re-fetches, because
   nothing records which pages already landed.
4. **The most serious windows are A and B**, because neither is recoverable by
   re-running the spider. B is worse: it is silent (only `dupefilter/filtered`
   moves) and its fingerprint permanently blocks the re-created request.
5. **Window C is close behind** — `active.json` is an index, not a manifest,
   and SQLite rows outside the index neither load at resume nor disappear.
6. **A stable item key plus a page-level landing marker** makes item delivery
   idempotent and makes a page ledger, rather than the dupefilter, the source of
   truth on resume. The design in §4 was implemented and observed to keep
   exactly one row per item across a kill and a stale-dupefilter resume.
7. **No fingerprint change helps.** The defect is that the system never records
   outcomes, not that it identifies requests too loosely.
