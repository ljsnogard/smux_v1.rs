# listener 的关闭模型：去掉令牌参数 + `close_async`（20261015-0353）

本文记录 `SmuxListener` 关闭模型的重新设计：**去掉 `K`（取消令牌）类型参数**，把「单次
等待的取消」与「listener 整体的关闭」彻底分开，并补上显式的 `close_async`。

---

## 1. 问题：`K` 把三件事混在一起

重构前的 `SmuxListener<Cfg, A, N, U, K = NonCancellableToken>` 里，`K` 在一个类型参数上
同时承担了：

1. **单次等待的取消**——`next_async` 的等待体与这个令牌竞速；
2. **listener 整体的关闭**——同一个令牌被透传进槽位的 accept 竞速、建环、在途握手、以及
   投递背压的 park；
3. **`NextAsync` 的类型参数化**——`NextWait_<'f, Item, Err, M>` 的 `M` 就是它，好让
   「每种令牌类型对应一个不同的 `NextAsync` 类型」。

后果是连锁的：`K` 顺着 `Shared_` / `Slot_` / 四个内部 async 函数一路传染；公开 API 上
多出 `new_with_cancel_` 与 `shutdown_token()`；调用方必须在**构造时**就决定「这个 listener
将来怎么被关闭」——而这恰恰是它无权替调用方决定的事。

正确的切分是：**每一次 `next_async` 都是可取消的**（调用方在每次等待上施加自己的令牌），
而**关闭**是 listener 自己的生命周期事件，由 `close_async` 或 drop 表达。

## 2. 新模型

```rust
pub struct SmuxListener<Cfg, A, N, U> { … }        // K 消失；new_ 仍是 4 个参数

impl<Cfg, A, N, U> SmuxListener<Cfg, A, N, U> {
    pub fn new_(inbound: A, cfg: Cfg, neg: N, scope: U) -> Self;
    pub async fn close_async(&mut self);            // 新增：优雅停止
}

impl TrAsyncIterator for SmuxListener<…> {
    type NextAsync<'f> = NextWait_<'f, Item, Err, ()>;   // 令牌由每次等待自己带进来
}

pub enum SmuxListenerError<E> {
    Cancelled,   // 本次等待被调用方的令牌中止；listener 仍可用
    Closed,      // 新增：listener 已关闭，不会再有连接
    Accept(E),
}
```

三项语义在实施前逐条确认过：

| 选择 | 结论 | 理由 |
| --- | --- | --- |
| 在途握手 | **直接取消**，然后等任务退出 | 否则一个不配合的对端能把 `close_async` 无限拖住 |
| 关闭时队列里的残留连接 | **丢弃**，此后 `next_async` 一律 `Closed` | 关闭语义干净，调用方不必再写「取到空才算完」 |
| 关闭的触发方 | 只有 `close_async(&mut self)` | 需要并发停止时，调用方 `select` 里 drop 掉 `next_async` 的 future 就释放了借用，紧接着调它即可；不为此再养一个可克隆句柄 |

`Cancelled` 与 `Closed` 必须分开：前者是「有人在这次等待上按了取消」，后者是「不会再有
东西」这个事实到达了。旧设计把两者都报成 `Cancelled`，调用方无法区分「我自己的超时」
与「listener 停了」。

## 3. 内部实现

### 3.1 `CloseSignal_`：可触发、可克隆、多等待者

listener 内部有好几处「等一个可能永远不来的事件」（accept、在途握手、投递背压），关闭时
必须能同时叫醒它们。`abs_cancel` 只提供两个极端（`CancelledToken` 永远已取消、
`NonCancellableToken` 永不取消），都不适用，于是自带一份：

- [`AtomicBool`] 让 `is_cancelled()` 能**同步**回答（各处循环第一句检查的就是它）；
- `futures::channel::oneshot` + `futures::future::Shared` 提供**多等待者**的一次性通知。

用 `Shared` 而不是自己拿 `Mutex<Vec<Waker>>` 写一份，关键在**清理**：等待体被丢弃时
`Shared` 会自己从登记表里摘除，而手写版本很容易只加不减——一个长时间运行的 listener
（每次 accept 循环迭代都注册一次）会因此在几小时里把内存吃光。

顺带得到一个行为改进：`SmuxListener` 被 **drop** 时触发端（`oneshot::Sender`）随之消失，
`Shared` 对所有等待者返回 `Err(Canceled)`，于是所有任务自己就退出——旧设计用
`NonCancellableToken` 时它们会一直挂到作用域消亡。

### 3.2 `TaskSet_`：从「查询」改成「等待」

旧设计用一张 `Vec<Live_>`（每条任务一个 `done_` 标记）回答「还有几个任务活着」，靠调用方
顺手清理。它只能查询，而 `close_async` 需要等待。于是换成：

- `tasks_: AtomicUsize`（槽位 + 在途握手的总数）、`slots_: AtomicUsize`（其中槽位数，即
  `max_inflight` 要限的额度）；
- `idle_: Mutex<Option<Waker>>`：计数归零时唤醒 `close_async`；
- 任务退出时的记账放在 `Drop`（[`TaskGuard_`]）里，因此 panic 也算退出。

首次 poll 完成「登记 → 复检」，不存在「最后一个任务在我登记之前就退出」的丢唤醒窗口。

### 3.3 收尾的两步：显式关闭写端 + 排空

`SmuxListener` 自己持有一份投递端（补槽位时要克隆给新槽位），因此**写端不会自行关闭**。
`close_async` 于是在任务全部退出后显式调用新加的 `Sender_::close_()`，再 `pop` 到终结，
把残留连接丢掉。

同时删掉了 `QueueMsg_::Stopped` 哨兵：关闭由 `close_async` 自己收尾，不再需要「槽位退出时
往队列里投一条停止消息」这条旁路。

## 4. 验收

三套后端各加一条
`{tokio,smol,compio}_next_async_cancellable_and_close_async_`，在真实 socket 上依次断言：

1. 用 `CancelledToken` 施加取消 → `Err(Cancelled)`；
2. 紧接着仍能正常交付一条连接（证明取消没有弄坏 listener）；
3. `close_async` 能打断正卡在 accept 上的槽位并在有限时间内返回，返回后
   `active_slots_() == 0`；
4. 其后 `next_async` 一律 `Err(Closed)`，且 `close_async` 幂等。
