# 子流状态原子化与收尾协议（T1 落地）

日期：2026-10-05 04:19
性质：**跨模块技术决策 + 落地记录**（`flow_ctrl` / `connection::owner_` /
`connection::mux_connection::registry_` / `connection::session_`）。

**取代关系**：本文是 `outlook-concurrency-20261002-2322.md` §12 的 **T1**
（`RxClosed` 收尾 / 半 RESET）与 **T2**（每条子流共享状态合并）的落地记录，并**推翻**
T2 原先「明确不做」的结论；同时推翻 `flow-ctrl-20261005-0115.md` §3.3 的「先不动」
裁决。凡与那两处冲突，以本文与当前代码为准。

---

## 1. 两个问题（人类指出）

1. **关闭路径过早干涉注册表数据**：`drop(tx)` / `drop(rx)` 一发生，收尾路径就改写身份
   或本地表项，导致 `MuxCore` 无法正确执行 smux 协议——要么发不出关闭通告，要么排不空
   已提交的字节。
2. **`ChannelOwner_` 与注册资料的寿命**：审查堆分配时发现，状态（`ChannelOwner_`）
   与注册记录（`ChanCtx_`）是**两个可独立死亡的对象**，寿命互不约束。

人类裁决（本轮原话，按原意记录）：

> 1. Q1 选 A：T1 与生命周期改造**一次过做完**。
> 2. 要改的应该是 `registry_.rs` 里的 `ChanCtx_`，以及 `owner_.rs` 里的
>    `ChannelState_`。它应该不只是现在 `ChannelOwner_` 里那么一点点的数据，它应该
>    保存流控相关的上下文数据都，这样做流控的时候只需要原子性地访问数据就不需要额外
>    加锁了，尤其是 `ChannelState_` 一大堆 bool 状态，都应该像 `AtomicFlags` 那样合并
>    在一个原子的 Flag 里面解决。
> 3. 先把这两个明显不合理的设计改完，后面什么锁的几乎都是伪问题。

---

## 2. 结论速览

| 事项 | 结论 |
| --- | --- |
| 每条子流状态的形态 | **一个可克隆的共享节点**：`Shared<ChannelState_, A>`；成员是**一个 `AtomicFlags<usize>` + 全原子窗口**，无锁 |
| 状态与身份的寿命 | 登记身份时（`reserve_channel_` / `reserve_inbound_`）建立，`attach_owner_` **删除** |
| 流控的并发契约 | 字段**单写者**；唯一跨任务多字段读（发送窗口的 `(r0, w0)`）打成一个 `u64` |
| 收尾判据 | 发送方向 = `FIN` 已发（蕴含接收环已排空）或对端 `RESET`；接收方向 = 应用丢 `rx` 或对端 `FIN`。**应用丢 `tx` 不算完成** |
| 释放身份 | 由 `claim_release_` 一次 CAS 判定并认领；两个方向都协议收尾后才进宽限态 |
| `FIN` / `RESET` | **正交**：`FIN` = 我不再发（对端关接收环写端）；`RESET` = 我不再收（对端停发送、**不**关接收环写端） |
| `drop(rx)` 之后到达的数据 | 解复用循环按 `app_rx_closed_` **静默丢弃**；写进已关闭的环得到的 `Closing` 也不按传输错误处理 |
| 剩下的锁 | 只有注册表那把（身份表增删查，全冷路径） |

---

## 3. 因果链（为什么这样改）

### 3.1 旧判据把「意图」当成「完成」

旧 `ChannelState_::is_done_`：

```text
tx_done = app_tx_closed_ || local_fin_sent_ || peer_reset_
rx_done = app_rx_closed_ || local_rx_closed_ || peer_fin_
```

`app_tx_closed_` 只是「应用不再写」的**意图**：发送环里可能还有已提交、尚未上网的字节，
`FIN` 也还没发。把它当完成，注册表身份就会先于排空被释放，而释放会把本地表项摘掉、
把 owner 丢掉——在途数据既没人搬、也没人记（实测：载荷只到 65024 / 65536）。

同时 `WriteEvent_::RxClosed` 无条件 `table.remove(&pair)` 并立刻发 `CLOSE(RESET)`：
若该 pair 已在 `pending_fin` 里，`finalize_entry_` 因查不到表项而「不可判定」，
退化成每轮重试一次的**僵尸条目**。

### 3.2 寿命分离让「提前释放」无法被结构性阻止

旧形状里 `ChannelOwner_` 是应用侧 `Shared::new` 出来的**三次分配**（热状态锁 + 两个去重
位），再经 `attach_owner_` 挂进注册表 `ChanCtx_.owner_`。它被四处各持一份克隆：注册表、
两个循环的本地表、应用半边。于是：

- 注册表条目可以先进宽限态，而 owner 还活着；
- 循环本地表可以先摘条目，而注册表条目还在。

没有任何一处能回答「这条子流还活着吗」。**把状态做成随身份记录一起建立、一起消亡的
同一份记录**，这个二选一才消失。

### 3.3 「同寿命」为什么要求原子状态

节点经 `Shared` 被多个任务以 `&self` 持有，`&mut` 借出不再可用；因此状态成员必须是
**内部可变**的。原子的另一个好处是它顺手消掉了锁：`is_done_` 这类多条件读只需一次
载入，`claim_release_` 这类「判定 + 置位」只需一次 CAS。人类第 3 条判断由此成立——
**状态原子化之后，关于锁的讨论基本都是伪问题**。

---

## 4. 落地形状

### 4.1 `owner_.rs`：`ChannelState_` = 一个字 + 原子窗口

```rust
pub(crate) struct ChannelState_ {
    flags_: AtomicFlags<usize>,   // 全部 bool / 枚举 / 两处去重位
    flow_: FlowCtrl,              // 收发双向窗口（方法全取 &self）
    establish_: Establish_,       // 持久通知通道（建流等待者）
    base_: Instant,               // active_ 的基准
    active_millis_: AtomicU64,
}
pub(crate) type ChannelOwner_<A> = Shared<ChannelState_, A>;
```

状态字位表（见源码常量）：`APP_TX_CLOSED` / `APP_RX_CLOSED` / `LOCAL_FIN_SENT` /
`LOCAL_RESET_SENT` / `PEER_FIN` / `PEER_RESET` / `RELEASED` / `PEER_OPENED` /
`STATE_READY` / `TX_QUEUED` / `RX_CONSUMED` / `establish_outcome_(2 位)`。

两个关键方法：

```rust
is_done_      = (LOCAL_FIN_SENT || PEER_RESET) && (APP_RX_CLOSED || PEER_FIN)
claim_release_ = CAS(expect = is_done_ && !RELEASED, desire = s | RELEASED)
```

`AtomicFlags` 没有 `fetch_or`，所以置位/清位都是 CAS 循环（`atomex` 的
`try_spin_compare_exchange_weak`），与 `buffex::RingState` 同一手法。

### 4.2 `flow_ctrl`：`&self` + 单写者契约（**公开 API 变更，已获批准**）

`FlowCtrl` / `SendWindow` / `RecvWindow` 的公开方法由 `&mut self` 改为 `&self`，
`send_window_mut` / `recv_window_mut` 删除；新增 `pub(crate)` 的
`FlowCtrl::new_empty_` 与 `install_(policy, ring_capacity)`（身份先于环容量出现，
窗口参数只能后装）。

并发契约按**字段写者**分：

| 字段 | 写者 | 读者 | 表达 |
| --- | --- | --- | --- |
| 接收窗口全部字段 | 解复用循环（建流期应用线程装一次，早于任何帧） | 解复用循环 | 逐字段原子即可 |
| `SendWindow::sent_` | 复用循环 | 复用循环 | 逐字段原子即可 |
| `SendWindow::reported_*` | 解复用循环 | **复用循环** | **唯一需要打包**：`(r0 低 32 位, w0)` 进一个 `u64` |
| `SendWindow::peer_epoch_base_` | 解复用循环 | 解复用循环 | 逐字段原子即可 |

`available()` 用低 32 位回绕减法（真实在途量 ≤ `max_window` < 2³²）；`on_report` 只会让
可用额度**变大或不变**，因此「读到旧快照 ⇒ 少批」是安全方向、不会越权。这条正是旧
`park` 条件里 `try_with_` 想表达的语义，现在退化成一次普通载入。

### 4.3 `registry_.rs`：状态随身份建立

```rust
struct ChanCtx_<A> { state_: ChannelOwner_<A>, inbound_: Inbound_ }
```

- `reserve_channel_` / `reserve_inbound_` **返回**状态句柄；`attach_owner_` 删除；
- `take_pending_inbound_` 返回 `(remote_dock, state)`，被动方句柄从 `income_async`
  起就交给 `ChannelHandle`；
- `install_channel_` 只做「建环 + `state.install_` + 发 `Attach`」；
- `inbound_` 仍留在注册表锁下（冷路径），锁序恒为 **注册表 → 无**（状态不再有锁）；
- `mark_failed_` 顺手删掉了 audit §5.4 记的死代码 `Vec<Waker>`：状态无锁之后，通知
  建流等待者可以在注册表锁内直接做完。

### 4.4 `FIN` / `RESET` 正交（协议可见）

| 帧 | 发送方声明 | 接收方动作 |
| --- | --- | --- |
| `CLOSE(FIN)` | 我不再发送 | 关自己的**接收**环写端：读完缓存即 EOF |
| `CLOSE(RESET)` | 我不再接收 | 停并放弃自己的**发送**方向，**不**关接收环写端 |

由此：

- `RxClosed`（`drop(rx)`）**不再摘写侧表项**，只置 `app_rx_closed_` + 发 `RESET`；
  发送方向的排空与 `FIN` 仍由 `pending_fin` / `TxClosed` 承担；
- 收到对端 `RESET` 时**允许直接放弃**本端发送环里尚未上网的字节（对方已声明不看）——
  这是唯一一处「已提交字节可以不送达」的例外，已写进 README §5 与
  `connection/mod.rs` §7.0；
- `maybe_release_` 成为**唯一**的身份释放入口（两个循环都可能调，CAS 保证只放行一个），
  它在 `release_channel_` 之后才投 `ReadEvent_::Release` 摘读侧表项。

### 4.5 `drop(rx)` 之后到达的数据

时间窗：应用丢掉 `rx`（关接收环消费端）→ 对端在收到我们 `RESET` 之前发出的帧仍会到达。
两种旧写法都会**误杀整条连接**（写进已关闭的环 → `ProducerError::Closing` → 判传输错误；
或读侧表项已摘 → 判协议违例）。现在：

1. `ChannelTx::drop` / `ChannelRx::drop` **同步**在状态字上置位（无锁、不依赖事件何时被
   处理）；
2. 解复用循环在写进接收环**之前**查 `is_app_rx_closed_`，命中即静默丢弃；
3. 仍撞上关闭窗口时，写失败若 tag 为 `WriteErrTag::Closing` 也按丢弃处理，不终止连接。

---

## 5. 验收

| 项 | 结果 |
| --- | --- |
| `cargo test --lib` | 170 passed（含 owner_ 的「协议层完成才释放」与窗口原子化单测） |
| `cargo test --test inmem_mux` | 20 passed（**新增 `mux_recv_dropped_dual_`**：丢 `rx` 后同连接另一条子流照常双向往返） |
| `cargo test --test smoke_tokio` | 4 passed；`flow_ctrl_socket_tokio_` 现在**写完立刻丢两半**仍逐字节收满 64 KiB（T1 的直接验收） |
| `cargo test --test smoke_compio` | 4 passed（同上） |
| `cargo test --test thread_safety` | 2 passed |
| `cargo test --doc` | 4 passed |
| `cargo clippy --all-targets -- -D warnings`（缺省 / compio） | 零告警 |
| `cargo check --all-targets` | 通过 |
| `cargo doc --no-deps` | 仍只有 3 条**既存**告警（与本轮无关） |

`layered_rpc` 挂死是 HEAD 既有现象（`flow-ctrl-20261005-0115.md` §6），未纳入本轮回归。

---

## 6. 遗留

1. **`drop(rx)` 后到达数据的静默丢弃分支**：机制已就位并有连接存活回归用例，但「帧恰好
   落在丢弃分支」依赖调度，未强制命中；要强制命中需要在解复用循环注入探针或做确定性
   调度，属后续加固。
2. `Establish_` 的 `flume::bounded(1)` 仍是每条子流一次全局分配（audit §3.1 #7）；
   `NotifySlot_` 化另开一轮，配丢唤醒压力用例。
3. `Arc<dyn Allocator>` 降到每连接一份（audit §5.5-A）未做。
4. audit §12-T4 的**计数分配器实测**仍未做：本轮把每条子流的 owner 分配从 3 次降到
   1 次（注册表节点里的 `Shared<ChannelState_>`），需要用 T4 的用例把数字钉住。
5. `K_MAX_FRAME_HEADER` 的注释与真实最坏值不符（audit §7），本轮未动。
