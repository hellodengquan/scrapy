# Scrapy 异步采集任务退出时机诊断（crawler-idle-race）

诊断对象：本仓库 Scrapy 2.20.0（`scrapy/core/engine.py`、`scraper.py`、`scheduler.py`、`utils/signal.py`、`utils/defer.py`）。
方法：阅读代码 + 仅访问 127.0.0.1 的 mock server（`ThreadingHTTPServer`，端点 `/page/N`、`/slow?d=`、`/flaky`）逐项复现，用信号（`item_scraped`、`spider_idle`、`spider_closed`）与服务端命中计数还原事件顺序。复现脚本共 10 个场景（S1–S9、S6 分 a/b），下文时间线均为实际运行输出。

## 1. 代码路径：从待抓页面到条目落地的状态变化

一个请求在框架内的完整生命周期，及其在各阶段被"空闲判断"看到的计数器：

1. **入队**：`ExecutionEngine.crawl()` → `_schedule_request()`（engine.py:656）→ 发 `request_scheduled` 信号 → `scheduler.enqueue_request()`（scheduler.py:424）。**去重在此发生**：`df.request_seen(request)` 一旦记录即不可撤销，返回 `False` 则发 `request_dropped`，请求就此消失。入队后 `slot.nextcall.schedule()`。
2. **出队下载**：`_start_scheduled_requests()`（engine.py:526）——**仅当引擎处于 `RUNNING` 且未暂停**才工作——`scheduler.next_request()` 取出请求，先 `slot.add_request()`（计入 `inprogress`，engine.py:565）再 `_download()` → `downloader.fetch()`（计入 `downloader.active`）。RetryMiddleware 是下载中间件，**重试发生在 fetch 链内部**：原请求在整个重试期间一直占着 `inprogress` 和 `downloader.active`，重试产生的新请求由 `_handle_downloader_output()` 经 `crawl()` 重新入队（engine.py:611-613）。
3. **刮削**：响应经 `_handle_downloader_output` → `scraper.enqueue_scrape()`（scraper.py:260）进入 scraper 槽位（`queue`/`active`），回调输出的 `Request` 回到第 1 步，`Item` 进 `start_itemproc_async()`（`itemproc_size += 1`，scraper.py:594），管道完成后 `item_scraped` 信号发出、`itemproc_size -= 1`（scraper.py:646）。
4. **出账**：请求 Deferred 链末端 `_remove_request()` 将其移出 `inprogress`（engine.py:552），并再次 `nextcall.schedule()`。
5. **空闲判定**：`spider_is_idle()`（engine.py:623-634）要求五项同时为空：

   ```python
   scraper.slot.is_idle()            # 刮削队列、活动响应、itemproc_size
   not self._slot.inprogress         # 引擎已接手的请求
   not self.downloader.active        # 下载器中的请求
   self._start is None               # Spider.start() 已耗尽
   not scheduler.has_pending_requests()
   ```

   为真且 `close_if_idle` 时进入 `_spider_idle()`（engine.py:862）：**同步**发送 `spider_idle` 信号；无 `DontCloseSpider` 且**复查** `spider_is_idle()` 仍为真，才 `close_spider_async()`。
6. **关闭**：`close_spider_async()`（engine.py:938）转入 `SPIDER_CLOSING` → 取消 start 请求消费协程 → `slot.close()` 等 `inprogress` 排空 → 关下载器 → `scraper.close_spider_async()` 等 `itemproc_size` 归零（scraper.py:228-244）→ `scheduler.close(reason)` → `spider_closed` → `engine_stopped`。

**关键不变量**：只要请求已交给框架（入队、在下载、在刮削、条目在管道），上述五个计数器至少一个非零，空闲判定不可能通过。漏采只能发生在"请求仍由用户异步逻辑持有、框架看不到"或"关闭流程主动丢弃已入队请求"两种情形。

## 2. 复现结果

### 2.1 请求已交给框架：安全

**S1 — `async def start()` 慢速产生起始请求**（首个 yield 前 sleep 0.5s，每个 yield 间 sleep 0.4s）：

```
[  0.666] start(): yielding request for /page/0
[  1.479] start(): yielding request for /page/2
[  1.885] signal: spider_idle fired          ← 只在 start() 耗尽后触发一次
[  1.886] signal: spider_closed (finished)
```

`spider_is_idle()` 中 `self._start is not None` 守卫（engine.py:632）生效：起始页面仍在产生时引擎不会空闲。3 个页面全部抓取，`finish_reason=finished`。

**S2 — 下载重试未结束**（`/flaky` 前两次 500、第三次 200，`RETRY_TIMES=2`，每次命中 sleep 0.3s）：

```
[  0.161] server: hit #1 /flaky   (500)
[  0.477] server: hit #2 /flaky   (500)
[  0.790] server: hit #3 /flaky   (200)
[  1.099] signal: item_scraped
[  1.099] signal: spider_idle fired   ← 重试窗口内从未触发
```

重试期间原请求始终在 `inprogress`/`downloader.active` 中，空闲判定不会通过。条目正常落地。

**S3 — 慢条目处理**（管道 `process_item` sleep 0.8s，其余全部瞬时完成）：

```
[  0.170] pipeline: processing started
[  0.971] pipeline: processing done
[  0.971] signal: item_scraped
[  0.972] signal: spider_idle fired (items_in_progress=0)
[  0.973] signal: spider_closed (finished)
```

`itemproc_size` 计入 `scraper.slot.is_idle()`（scraper.py:107-108），条目等待落地期间引擎不空闲；`item_scraped` 严格先于 `spider_closed`。

**S8 — `start()` 直接产出条目**（2 个 item + 慢管道）：两个条目全部落地。附带发现：最后一个条目从管道出来时（0.950s）没有任何路径再触发 `nextcall`，引擎直到 5s 心跳（`_SLOT_HEARTBEAT_INTERVAL`，engine.py:189）才察觉空闲并关闭（`spider_closed` 在 5.150s）。**不是漏采，是关闭延迟**——`start_itemproc_async()` 的 `finally` 只调 `_check_if_closing()`，不调度下一轮空闲检查。

**S9 — 回调内 `await Request(...)`**（inline-requests 模式，内联请求 sleep 0.6s）：`download_async()` 在挂起前同步把请求加入 `inprogress`（engine.py:715），引擎不空闲，条目落地。**"已交给框架"与"用户持有"的分界线就是 `download_async()`/`crawl()` 是否已被调用**——调用之前的异步等待框架不可见。

### 2.2 请求仍由用户异步逻辑持有：调用方未等待任务

**S4 — 回调里 `asyncio.create_task()` 分离任务，任务稍后再 `engine.crawl()`**：

```
[  0.156] parse: spawning detached asyncio task
[  0.157] signal: spider_closed (finished)   ← 引擎无事可等，立即关闭
（任务 0.5s 后才醒，reactor 已停，/page/1 从未请求）
RESULT: CONFIRMED-LOSS
```

框架的五个计数器都看不到用户任务，空闲判定如实反映"框架没有待办"。**这属于调用方未等待自己的任务**，不是框架提前关闭。正确的写法是把请求 `yield` 出去、在回调里 `await Request(...)`（S9）、或在 `spider_idle` 处理器里补请求并 raise `DontCloseSpider`。

两个加重此陷阱的框架细节：

- `scrapy/utils/defer.py:677` 的 `_schedule_coro()` 用 `asyncio.create_task()` 且**不保存强引用**，事件循环对任务只持弱引用，任务可能被 GC 回收（本次复现的 stop 触发任务就因此被静默回收过一次，加 `TASKS.add(...)` 后才触发）。用户模仿该模式会踩同样的坑。
- `SignalManager.connect()` 经 pydispatch 默认**弱引用**处理器：把局部 lambda/闭包连到 `spider_idle` 而不保存引用，处理器会被 GC 静默摘除，与"框架不触发"表现相同。

### 2.3 框架自身造成的漏采

**S5 — `async def` 的 `spider_idle` 处理器补充请求**：

```
[  0.153] parse: http://127.0.0.1:.../page/0
RuntimeWarning: coroutine 'on_idle' was never awaited   (engine.py:536, self._spider_idle())
[  0.153] signal: spider_closed (finished)
RESULT: CONFIRMED-LOSS（/page/extra 从未请求）
```

`_spider_idle()` 用**同步** `send_catch_log()`（engine.py:871）；`utils/signal.py:56` 只对返回 `Deferred` 的处理器报错，**协程返回值被静默丢弃**——处理器体一行都没执行（日志里连 "ENTERED" 都没有），`DontCloseSpider` 更不可能生效。对照 S6：同步处理器无论是否 raise `DontCloseSpider` 都安全（不 raise 时 `_spider_idle()` 在信号后复查 `spider_is_idle()`，调度器里已有新请求，复查不通过）。也就是说：**`spider_idle` 是唯一不支持异步处理器的核心信号之一，且失败方式是完全静默的**。这是可证明的框架缺陷：用户按其他信号的异步惯例写处理器，请求无声丢失，关闭原因还是正常的 `finished`。

**S7a — 显式 graceful 关闭恰逢重试窗口**（`/flaky` 第一次命中 sleep 0.3s 期间 `stop_async()`）：

```
[  0.171] server: hit #1 /flaky
[  0.370] triggering stop_async(mode=graceful)
[  0.485] signal: spider_closed (shutdown)
scheduler/enqueued: 2   scheduler/dequeued: 1   ← 重试请求已入队，从未出队
server hits: {'/flaky': 1}                        ← 重试从未发出
RESULT: CONFIRMED-LOSS
```

机制：500 响应在关闭期间返回，RetryMiddleware 产出的重试请求经 `crawl()` 成功入队（`crawl()` 只检查 `_drained()`，此刻 `slot.closing` 尚未 fire，入队被接受），但 `_start_scheduled_requests()` 在引擎离开 `RUNNING` 后直接返回（engine.py:527），该请求永远不被发送，最终随 `scheduler.close()` 丢弃（无 `JOBDIR` 时内存队列直接销毁）。**graceful 语义只承诺"等下载器里已有的请求完成"，不覆盖重试中间件即将产生的新请求**。

**S7b — 显式 graceful 关闭恰逢在途响应**（响应回调同时产出条目和跟进请求）：

```
[  0.250] triggering stop_async(mode=graceful)
[  0.565] parse_a: yielding item + follow-up request /page/b
[  0.566] signal: item_scraped {'from': 'a'}     ← 条目安全落地
[  0.567] signal: spider_closed (shutdown)
scheduler/enqueued: 2   scheduler/dequeued: 1   ← /page/b 入队后被丢弃
```

关闭流程对**条目**是安全的：`slot.close()` 等 `inprogress` 排空后，`scraper.close_spider_async()` 还会等 `itemproc_size` 归零才发 `spider_closed`。但对**关闭期间新发现的请求**（跟进链接、重试）一律入队即弃——`enqueue` 与 `dequeue` 的计数差（2 vs 1）就是丢失证据，且无任何日志或统计项直接报告丢弃数量。

**S7c — fast 关闭**：在途下载被 `downloader.stop()` 直接丢弃（0.352s 关闭，1.0s 的下载未完成），条目不落地。这是 fast 模式的声明式语义，符合预期。

### 2.4 场景分类汇总

| 场景 | 事件顺序结论 | 归类 |
|---|---|---|
| S1 起始请求慢速产生 | idle 判定被 `_start` 守卫挡住 | 安全 |
| S2 重试进行中 | 原请求持续占 `inprogress` | 安全 |
| S3/S8 条目在管道中 | `itemproc_size` 计入 idle 判定，关闭流程等管道排空 | 安全（S8 有关闭延迟，≤5s 心跳） |
| S9 回调内 await 请求 | `download_async` 同步计入 `inprogress` | 安全 |
| S6 同步 idle 处理器补请求 | 信号后复查 `spider_is_idle()` 兜底 | 安全 |
| S4 分离任务持有请求 | 框架计数器看不到用户任务 | **调用方未等待任务** |
| S5 异步 idle 处理器 | 协程被同步信号发送静默丢弃 | **框架缺陷**（静默漏采） |
| S7a/S7b graceful 关闭 | 在途条目安全；关闭期间入队的请求（重试、跟进）被丢弃 | **框架提前关闭派生工作**（设计如此但无观测、无文档强调） |
| S7c fast 关闭 | 在途下载被丢弃 | 符合声明语义 |

## 3. 修复方案评估（对去重与重试完成的影响）

**方案 A：让 `spider_idle` 支持异步处理器**（修 S5）。把 `_spider_idle()` 改为调度协程、用 `send_catch_log_async` 发送，完成后复查 `spider_is_idle()` 再决定关闭，并加 in-flight 标志防止重入。
- 对去重：**无影响**。异步处理器补的请求仍只经 `enqueue_request()` 入队一次，`request_seen` 只记录一次。
- 对重试：**无影响**，不触碰下载路径。
- 风险：处理器 await 期间引擎其他路径（如显式 `stop_async()`）可能已开始关闭，复查时必须重新校验状态而非假定仍空闲。最低成本的做法是至少把"返回协程"纳入 `send_catch_log` 现有的 Deferred 报错分支，把静默失败变成显式错误。

**方案 B：graceful 关闭时排空调度器**（修 S7a/S7b）。让 `SPIDER_CLOSING` 状态下 `_start_scheduled_requests()` 继续发送已入队请求，直到调度器为空再完成 `slot.close()`。
- 对重试：重试请求得以发出并完成，S7a 的条目可落地。
- 对去重：**语义不变但有一个既有约束**——`request_seen` 在入队时已记录，排空只是发送，不会重复计数；但若进一步设想"关闭时把在途请求重新入队保存"类方案，则会被 dupefilter 立即以重复为由丢弃（除非 `dont_filter`），这正是当前设计只丢弃、不重新入队的原因。
- **永不退出风险（本方案的核心陷阱）**：被排空请求的回调可能源源不断产生新请求（无限分页、日历归档页），关闭将永远无法完成。必须加界限：只排空关闭发起时已入队的请求（对队列做快照），或设排空上限/超时，超时后回落到当前的丢弃行为。不加界限的"排空"就是用永不退出换不漏采，不可接受。

**方案 C：不改关闭语义，补齐可观测性与持久化**。
- 丢弃发生时记日志/统计（如 `scheduler/dropped_on_close`），让 S7a/S7b 的丢失可被发现而非静默。
- 文档明确：graceful 关闭只等在途请求与在管道条目，**不**等调度器余量；需要断点续采的场景用 `JOBDIR`——磁盘队列在 `scheduler.close()` 时落盘（scheduler.py:413-422），dupefilter 的 `requests.seen` 同步持久化，恢复时既不漏也不重。S7a 在 `JOBDIR` 下重试请求会留到下次运行完成。
- 用新加的 `requests_in_progress` / `items_in_progress` 属性（engine.py:290、306）给调用方提供等待依据，覆盖 S4 类场景：调用方在断言"我的派生任务都已交给框架"之前不触发关闭。

**方案 D（应否决）：以"事件循环还有未完成任务"作为退出条件**。心跳 `LoopingCall`、reactor 自身任务使该条件永远为真，任务永不退出；且框架无法区分用户任务与内部任务。S4 类问题只能由调用方负责 `await`，框架能做的是方案 C 的可观测性。

## 4. 结论

1. 框架内部的空闲判定（五计数器 + 信号后复查）对**已交给框架的工作**是自洽的：起始请求慢速产生、下载重试、慢条目管道、回调内联请求，正常结束路径均不漏采（S1/S2/S3/S8/S9）。
2. 已抓取条目等待落地期间**不会**被关闭流程截断：graceful 关闭在 `scraper.close_spider_async()` 处等 `itemproc_size` 归零，`item_scraped` 严格先于 `spider_closed`（S3/S7b）。
3. 可证明的框架侧漏采有两处：**异步 `spider_idle` 处理器被同步信号发送静默丢弃**（S5，建议方案 A 或至少显式报错）；**graceful 关闭丢弃关闭期间入队的重试/跟进请求**（S7a/S7b，建议方案 C 的观测与文档，或带界限的方案 B）。
4. 用户异步逻辑持有的请求（S4）不属于框架缺陷；但 `_schedule_coro()` 不持强引用、信号处理器默认弱引用这两个细节会把同类错误放大为静默失败，值得在文档中警示。
5. 任何"为防漏采"的退出判断修正都必须保留退出保证：对排空设界限、拒绝以事件循环任务存量为退出条件，否则会把漏采问题换成永不退出问题。

## 附：复现方法

复现脚本位于 `/tmp/idle-race/`（`harness.py` 提供 127.0.0.1 mock server 与事件日志，`s1`–`s9` 各场景独立运行），运行环境为仓库源码 editable 安装（Scrapy 2.20.0、Twisted 26.10、Python 3.14、asyncio reactor）。所有场景仅访问本机回环地址；每条结论上方的时间线均为对应脚本的真实输出。
