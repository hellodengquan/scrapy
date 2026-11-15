# 异步采集任务的退出时机诊断

诊断对象：本仓库当前工作树（Scrapy 2.20.0，分支 `task/19`，提交 `e63a574`）。
所有复现只访问两个本机端点：`tests/mockserver`（部分早期用例）与
`/tmp/scrapy-idle-diag/scripts/server.py` 里的本地 aiohttp 站点
（`127.0.0.1`，临时端口）。没有访问任何外部站点。

## 0. 结论摘要

| 场景 | 结果 | 归属 |
| --- | --- | --- |
| 起始页仍在产出，条目在管线里慢速处理 | 安全，正常 `finished` | 框架正确 |
| 起始页仍在产出，同时下载重试未结束 | 安全，重试被排程并完成 | 框架正确 |
| `spider_idle` 处理器**同步**调用 `engine.crawl()` | 安全 | 框架正确 |
| `spider_idle` 处理器里 `create_task` 后再 `crawl()` | 调用方未等待任务，抛 `RuntimeError` | 调用方错误 |
| `spider_idle` 处理器任务里 `await Request`（先 `await sleep(0)`） | 同上 | 调用方错误 |
| `spider_idle` 处理器任务里 `await Request`（立即 await） | 安全（碰巧） | 时序侥幸 |
| 一轮 `DontCloseSpider` + 任务在下一个心跳周期内入队 | 安全（宽限 ≈ 一个心跳周期，生产 5s） | 框架行为，但非"等任务"语义 |
| 显式 `graceful`/`fast` 关闭，条目/下载在飞行中 | 安全（`fast` 丢弃下载，符合文档） | 框架正确 |
| **关闭已开始，仍在飞行的回调之后 yield 新请求** | **静默丢失**（无日志、无 `request_dropped`、无 `request_left_downloader`，stats 只剩 `enqueued > dequeued`） | **框架缺陷** |
| **关闭已开始，重试在 drain 之后才产生** | 同上，重试请求被丢弃 | **框架缺陷** |
| idle 处理器任务抢在关闭前入队（`finished` 路径） | 请求被接受后静默搁置 | 调用方未等待 + 框架无提示 |
| **无 reactor 模式 `fast` 关闭 + 回调 `await Request`** | **关闭永久卡死**：`crawl_async()` 不返回，`spider_closed`/`engine_stopped` 不发送，feed 不落盘 | **框架缺陷（更严重）** |

一句话：**空闲判断本身是对的**（`spider_is_idle()` 的五个条件覆盖了调度器、
下载器、slot、scraper 和 `start()`），真正的问题是**关闭的 drain 判据与空闲判据
不一致**——`_Slot._maybe_fire_closing()` 只看 `inprogress`，不看
`has_pending_requests()`。因此"关闭时仍在跑的回调之后产出的请求"会进入调度器，
却再也无人取出。

---

## 1. 实际代码：状态机与判据

### 1.1 空闲判据（`scrapy/core/engine.py:623`）

```python
def spider_is_idle(self) -> bool:
    if self._slot is None:
        raise RuntimeError("Engine slot not assigned")
    if not self.scraper.slot.is_idle():
        return False
    if self._slot.inprogress:  # the downloader may not have these requests yet
        return False
    if self.downloader.active:  # downloader has pending requests
        return False
    if self._start is not None:  # not all start requests are handled
        return False
    return not self._slot.scheduler.has_pending_requests()
```

`inprogress` 的生命周期（实测，`s22_invariants.py`）：

- 入队：`_start_scheduled_request()` 在 `_download()` **之前**
  `add_request()`（`engine.py:565`）。这正是 `#8315`/`#8355` 修复的内容，
  防止"已从调度器取出但还没进下载器"被误判空闲。
- 出队：`d2.addBoth(partial(self._remove_request, ...))`，即在
  `_handle_downloader_output` → `scraper.enqueue_scrape()` → 回调 →
  管线**全部结束之后**（`engine.py:554`）。
- `download_async()` 里是 `try: await _download(...) finally: remove_request`
  （`engine.py:715`、`721`）。

实测在管线里挂住时 `inprogress=1`、`scraper_is_idle=False`，
所以"已抓条目等待落地"期间不会误判空闲——这一点框架是正确的（场景 1、8）。

### 1.2 关闭判据（`scrapy/core/engine.py:170`）

```python
def _maybe_fire_closing(self) -> None:
    if self.closing is not None and not self.inprogress:
        assert not self.closing.called
        self.nextcall.cancel()
        if self.heartbeat.running:
            self.heartbeat.stop()
        self.closing.callback(None)
```

注意两点：

1. 判据只有 `inprogress`，**没有** `scheduler.has_pending_requests()`。
2. 一旦成立就 `nextcall.cancel()` 并停掉 heartbeat，也就是**永远不再取调度器**。

于是出现窗口：某个请求的回调在 `inprogress` 仍包含自己时 `yield` 了新请求
（`engine.crawl()` 接受它，因为 `_drained()` 看的是 `closing.called`，
此刻还是 `False`）；回调与管线随后结束，`remove_request()` 触发
`_maybe_fire_closing()`，此时 `inprogress` 已空、调度器里却有刚入队的请求。
判据不看调度器，直接放行关闭，那个请求就留在调度器里没人取。

实测追踪（`s18_mechanism.py`，一次 graceful 关闭）：

```
_maybe_fire_closing: inprogress=1 closing=True scheduler_pending=False
after fire: closing_called=False nextcall._call=None
callback yields request
signal:request_scheduled ok?key=late     <-- 被接受
CallLaterOnce.schedule(delay=0) was_none=True   <-- 调度已取消，重新登记成功
signal:item_scraped
_maybe_fire_closing: inprogress=0 closing=True scheduler_pending=True   <-- 判据不查调度器
after fire: closing_called=True
signal:spider_closed reason=shutdown
scheduler enqueued=2 dequeued=1          <-- 一个请求留在调度器里，无人处理
```

`s18` 里 `CallLaterOnce.schedule()` 在 slot 关闭后其实**成功**登记了一个新回调；
但 `close_spider_async()` 随后直接 `await self._slot.close()` →
`_close_downloader()` → `scheduler.close()`，`_start_scheduled_requests()`
再也不会被调用，所以新登记的回调没有意义。这正是缺陷的机械原因。

### 1.3 复现的分层：请求已交给框架 vs 仍由用户异步逻辑持有

这一点决定了责任归属，实测三组对照：

| 写法 | 请求何时进入框架 | 结果 |
| --- | --- | --- |
| `engine.crawl(req)` 同步调用 | 当步即入调度器、`inprogress` 计入 | 安全 |
| `asyncio.create_task(... engine.crawl(req) ...)` | 取决于任务何时被调度 | 常抛 `RuntimeError`，或不安全 |
| `await Request(...)`（独立任务里） | await 那一刻 `add_request` | 立即 await 安全；先 `await sleep(0)` 则不安全 |

`Request.__await__`（`scrapy/http/request/__init__.py:452`）要求
`_active_crawler` 已被设置。`_active_crawler_scope` 只在回调、管线、
`Spider.start()` 和异步信号处理期间有效（`engine.py:460`、
`engine.py:738`、`scraper.py:296`）。`asyncio` 任务会**复制**当前上下文，
所以从回调里 created 的任务能拿到 `_active_crawler` 的值并成功发出请求；
但 `_active_crawler_scope` 在回调结束时就把值复原了，任务里的这个副本
是"过期的"。它能让请求发出去，却不带来任何针对关闭的保护——
是否来得及被登记，纯看时序。

---

## 2. 交错场景：哪些安全，哪些是调用方问题，哪些证明框架提前关闭

全部用固定事件顺序（不是靠墙钟竞争），复现脚本见
`/tmp/scrapy-idle-diag/scripts/final_suite.py`（结果为逐行 `enq/deq/dl/items/reason`）。

### 2.1 安全：起始页仍在产出，或条目仍在管线

```
1 baseline slow item                          enq=1 deq=1 dl=1 items=1 reason=finished
2 start suspended while item in pipeline      enq=2 deq=2 dl=2 items=2 reason=finished
3 start suspended while retry pending         enq=3 deq=3 dl=3 items=2 reason=finished
```

场景 2 里 `start()` 在 `await sleep(0.5)` 期间，第一个条目在管线里慢速处理；
`_start is not None` 让空闲判据保持 False，因此不会提前关闭。
场景 3 里第一次 503 触发重试，重试被排进调度器、重新下载、拿到 200；
`retry/count=1`，`items=2`。

### 2.2 安全：`spider_idle` 处理器同步加的请求，以及 `DontCloseSpider` 换来的下一轮

```
4 idle handler crawl() synchronously          enq=2 deq=2 dl=2 items=2 reason=finished
7 DontCloseSpider + immediate crawl task      enq=2 deq=2 dl=2 items=2 reason=finished
```

`_spider_idle()` 在 handler 全部返回后**重新检查** `spider_is_idle()`
（`engine.py:882`）；同步 `crawl()` 加的请求让它跳过关闭。

场景 7 是任务，只是它先 `await sleep(0)` 再 `crawl()`；它能成功**只因为**
handler 抛了 `DontCloseSpider`，`_spider_idle()` 提前 `return`，把关闭判定
推到了下一轮。下一轮由 heartbeat 唤醒（生产 `_SLOT_HEARTBEAT_INTERVAL = 5.0s`）。
`s52_dontclose_delay.py` 把心跳缩到 0.1s 后测得这个宽限**恰好一个心跳周期**：

```
delay=  0ms  idle_calls=2 enq=2 deq=2 items=2 reason=finished
delay= 50ms  idle_calls=2 enq=2 deq=2 items=2 reason=finished
delay= 90ms  idle_calls=2 enq=2 deq=2 items=2 reason=finished
delay=110ms  idle_calls=2 enq=1 deq=1 items=1 reason=finished   task: RuntimeError: No open spider to crawl
delay=300ms  idle_calls=2 enq=1 deq=1 items=1 reason=finished   task: RuntimeError: No open spider to crawl
```

所以 **`DontCloseSpider` 给的是"下一轮再来一次"，不是"直到某个任务完成"**；
生产里这个宽限约 5 秒，随心跳周期变化，不随任务长度变化。
超期时调用方会拿到 `RuntimeError`。这个异常**不会**进入 scrapy 日志、
`spider_error` 信号或统计（`s54_task_error_visibility.py` 实测：
`spider_error signals: none`、`asyncio log lines: none`、`asyncio` 循环的
`exception_handler` 也没被调用）；只有 `RuntimeError` 留在 task 上，
调用方自己 await/记日志时才可见。

### 2.3 调用方未等待任务：`create_task` 后再 `crawl()`

```
5 idle handler task crawls after 0ms          enq=1 deq=1 dl=1 items=1 reason=finished caller-error=True
5 idle handler task crawls after 50ms         enq=1 deq=1 dl=1 items=1 reason=finished caller-error=True
6 idle handler inline await (yield_first=True)  enq=1 deq=1 dl=1 items=1 reason=finished caller-error=True
```

错误文本（`s23_idle_task_errors.py`）：

- 任务在关闭判定之前运行：`RuntimeError: Cannot crawl <GET ...>: the spider is closing`
- 任务在关闭之后运行：`RuntimeError: No open spider to crawl: <GET ...>`

两者都是 `engine.crawl()`/`download_async()` 主动拒绝，属于**调用方没有等待**
——处理器（同步函数）返回后任务还没被调度，框架没有义务等它。这与
`docs/topics/signals.rst:350` 的说明一致：在 idle 里排请求"不保证"能阻止关闭。

一个重要的时序侥幸：

```
6 idle handler inline await (yield_first=False) enq=1 deq=1 dl=2 items=1 reason=finished caller-error=False
```

同一个写法，只因为任务里没有先 `await sleep(0)`，`await Request` 在关闭判定
之前完成了 `add_request`，就被算作 idle 不成立而获救。**这不是契约，
只是当前实现里的调度顺序。** 把 `await sleep(0)` 去掉就变得"能用"，
加回来就报错——这正说明可依赖的是"任务是否被等待"，而不是"请求是否已交给框架"。

反过来说，`await Request` 一旦真的进了 `download_async()`，它会被正确计入
`inprogress` 与 `downloader.active`（`s45_bg_tracked.py`：后台任务下载 0.6s 的
页面期间，`spider_idle` 直到下载完成、`inprogress=0` 才触发）。
**问题只出现在"请求还留在用户任务里、尚未进入引擎"的那段窗口。**

### 2.4 能证明框架提前关闭：drain 窗口

```
10 close(graceful) then callback yields request  enq=2 deq=1 dl=1 items=1 reason=shutdown SILENT LOSS
10 close(fast) then callback yields request      enq=2 deq=1 dl=1 items=1 reason=shutdown SILENT LOSS
11 close(graceful) while retry pending           enq=2 deq=1 dl=1 items=0 reason=shutdown retry/count=1
```

事件序列（`s14_drain_loss.py`、`s15_silent.py`）：

```
spider_opened engine_started
request_scheduled first ; request_reached_downloader first ; response_received first
stop_async(graceful)
callback: suspend          <- 回调已经在用户代码里挂住
callback: yield item+request
item_scraped               <- 条目落地
scheduler/enqueued=2 deq=1 dl=1   <- 第二个请求：无 dropped、无 left_downloader、无日志
```

机制说明：`engine.crawl()` 只在 `_drained()` 为真时拒绝请求，而
`_drained()` 检查的是 `closing.called`（`engine.py:636`）。`_Slot.close()`
先设 `self.closing = Deferred()` 再调 `_maybe_fire_closing()`；后者看到
`inprogress` 尚非空，只把 `closing` 留在那里，`closing.called` 仍是 `False`，
所以 `crawl()` 接受新请求并把它放进调度器。

随后，回调那条 `_handle_downloader_output` 链走到
`d2.addBoth(partial(self._remove_request, ...))`（`engine.py:576`），
`remove_request()` 再调 `_maybe_fire_closing()`；这一次 `inprogress` 已空、
`scheduler` 里却刚好有那个新请求。判据不看调度器，于是
`nextcall.cancel()` + `heartbeat.stop()` + `closing.callback(None)`，
关闭继续走到 `_close_downloader()` / `scheduler.close()`，调度器里的请求
再也不会被 `next_request()` 取出。

`s18_mechanism.py` 的追踪明确显示这一顺序：
`inprogress=1 →（callback 产出并 crawl）→ inprogress=0 scheduler_pending=True → 关闭`。

这是**框架提前关闭**而不是调用方错误，因为调用方走的是公开路径
（回调正常 `yield Request`），且在 `engine.crawl()` 看来"蜘蛛尚未关闭"。

`s15_silent.py` 把 scrapy 日志开到 DEBUG，结果：**没有任何一行包含 `late`，
没有任何 WARNING/ERROR，`downloader/request_dropped_count` 为 None，
`dupefilter/filtered` 为 None。** 唯一痕迹是 `scheduler/enqueued=2,
scheduler/dequeued=1`。漏采是静默的。

在 JOBDIR 下（`s14_drain_loss.py` 第二次运行）这个请求会被写进
`requests.queue`，并在 resuming 之后被送出——但那依赖重启，
且 `dupefilter` 记下了它的指纹；见第 3 节。

### 2.5 正常结束 vs 显式关闭的差别

```
8 close(graceful) while item in pipeline       enq=1 deq=1 dl=1 items=1 reason=shutdown
8 close(fast) while item in pipeline           enq=1 deq=1 dl=1 items=1 reason=shutdown
9 close(graceful) while page downloading       enq=1 deq=1 dl=1 items=1 reason=shutdown
9 close(fast) while page downloading           enq=1 deq=1 dl=1 items=0 reason=shutdown
```

- **正常结束**（`finished`）：关闭在 `spider_is_idle()` 为真之后才发起，
  该判据已经包含 `scheduler.has_pending_requests()`。因为普通回调在
  `inprogress` 里，`inprogress` 不空就不会判定 idle，所以**普通回调
  `yield Request` 不会在 finished 路径上被搁置**（`s55_finished_no_loss.py`
  的 26 请求链：`enq=26 deq=26 items=26`）。
- **显式关闭**：`close_spider_async()` 自己发起，用 `_Slot.closing` +
  `inprogress` 作为 drain 判据，**不查调度器**。因此在飞行回调节点产出的请求
  会被搁置——这是 2.4 的框架缺陷。

补充一个 `finished` 路径上的变体（`s46_finished_window.py`）：如果
`spider_idle` 处理器用 `create_task` 入队，且任务在 `close_spider_async()`
真正开始前先跑了一步，那么 `engine.crawl()` 会**接受**这个请求（打印
`request_scheduled`），随后关闭照旧发生：

```
spider_idle
idle handler: create task
request_scheduled late            <-- 被接受
task: crawl accepted
spider_closed reason=finished
enq 2 deq 1 dl 1 items 1 reason finished   <-- 再次静默搁置
```

这一条的责任在调用方（任务没被等待，`docs/topics/signals.rst:350` 已声明
"不保证"），但框架仍然**接受并登记**了一个最终不会执行的请求，
没有任何 dropped 信号。与 2.4 的区别是：2.4 走的是公开的
`yield Request` 路径，不依赖任何调用方违规。

`fast` 关闭按文档（`docs/news.rst:899`）就是丢弃下载中的请求：
`downloader/request_dropped_count=1`、`items=0`，这与预期一致。
注意这只覆盖"已进入下载器"的请求；`graceful` 的 drain 判据不含调度器，
所以"已入调度器、未进下载器"的请求在**两种** mode 下都可能被搁置
（上表 10/11 行的 graceful 与 fast 结果相同）。

### 2.6 附加发现：无 reactor 模式下 `fast` + `await Request` 永久卡死

```
12 close(graceful) during awaited inline request  ... reason=shutdown crawl_async=returned@0.38s state=STOPPED
12 close(fast) during awaited inline request      ... reason=None crawl_async=returned@4.00s state=SPIDER_CLOSING
```

`fast` 那行显示的 `returned@4.00s` **不是正常返回**：它是 4s 看门狗超时后
`asyncio.wait_for()` 取消正在等待 `crawl_async()` 的外层任务的结果。用
`s42_fast_precise.py` 把 `crawl_async()` 与 `stop_async()` 各自拆成 task
并看 8 秒，`crawl_done=False` 一直保持到观测结束，`stop_async` 也被同一取消
卷走（`stop_async raised CancelledError`），`spider_closed`/`engine_stopped`
从未发送、`finish_reason` 为 `None`。`s47_c12_repeat.py` 在单进程里连跑 8 次，
8/8 都是 `state=SPIDER_CLOSING reason=None spider_closed=False`。

```
0.141 stop_async(fast) called
0.141 callback raised CancelledError
0.503 … 4.017  crawl_done=False state=SPIDER_CLOSING
4.017 HANG CONFIRMED: crawl_async never returned
```

相关信号：`spider_closed`、`engine_stopped` 从未发送；`_closewait` 永不 fire；
已经发生 `item_scraped`，但 feed 文件**始终为空**
（`s43_feed_hang.py`：`spider_closed after +2s: False`，`feed bytes: ''`）。
`runner.join()` 也不返回（`s44_join_check.py`：`join TIMEOUT`）。

根因在 `scrapy/core/scraper.py:349`：

```python
async def _wait_for_processing(self, result, request, queue_dfd):
    try:
        await self._scrape(result, request)
    except Exception as exc:
        queue_dfd.errback(exc)
    else:
        queue_dfd.callback(None)
```

`asyncio.CancelledError` 是 `BaseException` 的子类，**不是 `Exception`**，
所以被 fast stop 取消时 `queue_dfd` 既不被 `errback` 也不被 `callback`。
于是 `enqueue_scrape()` 的 `yield dfd` 永久挂起，`finally` 里的
`slot.finish_response(request)` / `_check_if_closing()` 永不执行，
`inprogress` 里永远留着最初那个请求，`_Slot.closing` 永不 fire。

对照验证（`s37_wedge_fix.py`）：把 `_wait_for_processing` 改成
"也接住 `asyncio.CancelledError` 并 `errback` 同一个异常"，同样的场景
0.4s 内返回、`reason=shutdown`。这证实了根因。

范围（`s36_wedge_scope.py`）：

- 已排程请求的 fast stop：0.14s 正常返回、`reason=shutdown`；
- **回调里 `await Request` 的 fast stop：永久卡死**；
- 管线里 `await engine.download_async()` 的 fast stop：4.02s 返回
  （未卡死，但那 4s 是 `wait_for` 的超时；这条不构成对照证据，
  因为该场景的 `start()` 只 `yield` 了一个 item，没有任何已排程请求）。

只有在 **reactorless + fast** 时复现。默认的 Twisted asyncio reactor 下
同一用例 `spider_closed reason=shutdown` 正常（`s34_reactor_fast.py`）。
生产可达路径：`AsyncCrawlerProcess` + `TWISTED_REACTOR_ENABLED=False`
（`docs/topics/practices.rst:245`），Ctrl-C 第二次触发
`_schedule_reactorless_shutdown(mode="fast")`（`crawler.py:1309` 起）。

这一条与"漏采"不同：**它不是漏条目，而是进程不退出**（更严重，需要独立修复）。

---

## 3. 修正退出判断：对去重与重试的影响

### 3.1 候选修正

把 `_maybe_fire_closing()` 与 `spider_is_idle()` 对齐，同时保留 drain 能力：

```python
def _maybe_fire_closing(self):
    if (self.closing is not None and not self.inprogress
            and not self.scheduler.has_pending_requests()):
        ...
```

并在 `_start_scheduled_requests()` 里允许 `SPIDER_CLOSING` + `graceful` 期间继续取调度器。
（诊断脚本 `s19_fix2.py` 用 monkeypatch 实现对齐判据+无界 drain，
`s50_true_bounded.py` 实现有界 drain；均未改仓库。）

### 3.2 效果（实测）

| | 基线 | 对齐判据后 |
| --- | --- | --- |
| 关闭窗口内产出的请求 | `enq=2 deq=1 dl=1 items=1` | `enq=2 deq=2 dl=2 items=2` |
| 关闭时未完成的重试 | `enq=2 deq=1 dl=1 items=0 retry/count=1` | `enq=2 deq=2 dl=2 items=1`（重试完成，`server hits r=2`） |
| finished 路径上的 idle-task 抢跑 | `enq=2 deq=1 items=1` | `enq=2 deq=2 items=2` |

即修正能同时救回普通请求、重试和 finished 路径上的抢跑。重试的
`skip_dupefilter_once` 在 `Scheduler.enqueue_request()`（`scheduler.py:424`）
里被消费，不影响后续去重；`dupefilter/filtered` 保持 None。

**但表里的"对齐判据后"是"无界 drain"，不能直接采用**：它把关闭等价成
"抽干调度器"，遇到持续产出新请求的回调会永不退出（3.3）。"有界 drain"
才是可用方案，代价是关闭开始后新产出的请求被 `request_dropped` 丢弃
（可见、可观测），而不是静默搁置。

`dupefilter` 的持久化细节值得单独说明。基线（`s39_retry_dedup_resume.py`、
`s51_resume_check.py`）：一次 graceful 关闭把重试请求留在调度器，JOBDIR 把它
写进 `requests.queue/.../q00000`；`requests.seen` 里已有该指纹。
重启后靠 `skip_dupefilter_once` 才让它重新出队：
run1 `enq=2 deq=1 retry/count=1`，run2 `enq=4 deq=5 retry/count=3
downloader/request_count=5 hits={'r': 2}`。也就是说
**JOBDIR 只能在重启后补救，不能免除"关闭时重试被搁置"这一事实**；
不重启就是漏采。`retry/count` 在两次运行之间是累计的，这也是判断
"重试是否真的跑完"的好指标。

### 3.3 代价：会永远不退出吗

关键区分：**"调度器为空"这个判据**在两类请求空间上行为完全不同：

- **无界链**（每个回调都产出新 URL）：`s20_fix_livelock.py` 里基线
  0.19s 结束（`items=12`）；"抽干调度器"式修正 2s 超时仍在跑
  （`items=184`，`reason=None`）。**这就是"为防漏采而永不退出"的具体形态。**
- **有界但很长的链**：基线会丢尾部；抽干式修正会把整条链跑完（长时间）。

本质：把关闭判据从 `inprogress` 扩展成 `inprogress ∨ scheduler_pending`
等价于"关闭 = 抽干调度器"。只要回调持续新增请求，抽干永不完成。

可接受的两种语义：

1. **有界 drain**（`s50_true_bounded.py`，实测）：关闭开始时快照
   `len(scheduler)` 作为预算，只允许取出这么多，之后新产出的请求
   立即走 `request_dropped`。实测（两次运行，`items` 数值随调度顺序
   在 11–12 之间浮动，取决于关闭时刻已排程的条数）：
   ```
   baseline:
     chain (baseline)   returned@0.19s  enq=13 deq=12 items=12 dropped=[]
     late  (baseline)   returned@0.41s  enq=2  deq=1  items=1  dropped=[]
   bounded:
     chain (bounded)    returned@0.16s  enq=11 deq=11 items=11 dropped=['request_dropped 11']
     late  (bounded)    returned@0.41s  enq=1  deq=1  items=1  dropped=['request_dropped late']
   ```
   关闭时间没有被无界链拖长（≈0.17s vs 0.19s），而普通漏采请求变成了
   **可见的 `request_dropped`**，调用方可以在回调里检查
   `engine.state`/`engine.running` 并自行持久化。
   预算用完后 `crawl()` 直接拒绝，请求不进调度器，所以
   `enq == deq == items + dropped` 是可核对的。
2. **只在 `finished`/idle 路径上要求"调度器为空"**，`fast`/显式 close
   保持现状但补上可观测性（`request_dropped` 或至少一次 WARNING）。

### 3.4 条目是否提交

- 修正后 `item_scraped_count` 与 `items` 都随请求完成而增加，
  说明条目仍经管线提交（`s19_fix2.py`: `items=2`）。
- `fast` 关闭仍可能丢条目（`items=0`），那是 `mode="fast"` 的既定语义。
- 卡死场景（2.6）里条目可能已 `item_scraped` 但 **feed 未 flush**
  （`s43_feed_hang.py`），因为 `spider_closed` 没发送。
  这不能靠"改关闭判据"解决，属于 `_wait_for_processing` 的取消传播缺陷。

---

## 4. 给调用方的实用结论

1. 不要在 `spider_idle` 处理器里用 `create_task` 后直接 `engine.crawl()`；
   处理器返回前必须完成入队，或者至少不要依赖"下一轮一定还会来"。
2. 回调里要发请求就 `yield`，不要丢进后台任务；后台任务里的
   `await Request` 在关闭时会被拒绝或静默迟到。
3. `await Request` 只在回调/管线/`start()` 的作用域内有效；
   在独立任务里即使 `_active_crawler` 被继承，也不受关闭保护
   （请求一旦真正进入引擎就受保护，见 2.3 的 `s45_bg_tracked.py`）。
4. 依赖 JOBDIR 重启来补漏采时，注意 `requests.seen` 已记指纹，
   需要 `skip_dupefilter_once`（重试路径自带）。
5. 无 reactor + fast stop 目前不要与 `await Request` 同时使用。
6. 用 `scheduler/enqueued` 与 `scheduler/dequeued` 是否相等来体检漏采：
   两者不等说明有请求被搁置在调度器里。

---

## 5. 复现清单

脚本都在 `/tmp/scrapy-idle-diag/scripts/`，用
`/tmp/scrapy-diag-venv/bin/python -u <script>` 运行（venv 从本仓库 editable 安装）。

| 脚本 | 作用 |
| --- | --- |
| `s1_states.py` | 单请求的状态轨迹（inprogress/dl/sched/scrape/items） |
| `s22_invariants.py` | 证明 `inprogress` 覆盖回调+管线全过程 |
| `s18_mechanism.py` | 关闭判据只看 `inprogress` 的机械追踪 |
| `s14_drain_loss.py` | drain 窗口静默丢失 + JOBDIR 恢复 |
| `s15_silent.py` | 证实丢失没有任何日志/信号/统计痕迹 |
| `s19_fix2.py` / `s50_true_bounded.py` | 对齐判据修正 vs 有界 drain |
| `s20_fix_livelock.py` | 无界链在"抽干调度器"式修正下永不退出 |
| `s49_bounded_widfth.py` | 对齐判据在 finished 路径上的 idle-task 抢跑（`enq=2 deq=2 items=2`） |
| `s46_finished_window.py` | finished 路径上的静默搁置 |
| `s52_dontclose_delay.py` | 量化 `DontCloseSpider` 的宽限 = 一个心跳周期 |
| `s54_task_error_visibility.py` | 迟到的 `engine.crawl()` 失败不会进 scrapy 日志/spider_error |
| `s41_fast_repeat.py` / `s42_fast_precise.py` / `s47_c12_repeat.py` | reactorless fast-stop 关闭卡死（8/8 复现） |
| `s37_wedge_fix.py` | 用 `_wait_for_processing` 补丁证实卡死根因 |
| `s43_feed_hang.py` / `s44_join_check.py` | 卡死时 feed 不落盘、join 不返回 |
| `final_suite.py` | 汇总 13 个场景、20 种变体的一行结论 |
| `s55_finished_no_loss.py` | 普通回调 `yield Request` 在 finished 路径上 `enq=deq=items=26` |

未修改仓库任何文件；`crawler-idle-race.md` 是本任务唯一新增文件。
诊断用的 venv 在 `/tmp/scrapy-diag-venv`，脚本在 `/tmp/scrapy-idle-diag/`，
两者都不在仓库内，可随时删除。
仓库里唯一被诊断过程写出的文件是 `tests/keys/localhost.crt` 和
`tests/keys/localhost.key`（`tests/keys/__init__.py` 的 `generate_keys()`，
`.gitignore` 已忽略），因为 mock server 的 HTTPS 端口需要它们；
它们未进入 git 状态。
