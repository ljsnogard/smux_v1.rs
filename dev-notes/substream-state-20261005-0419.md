# 子流状态节点与收尾协议（T1 落地）

日期：2026-10-05 04:19
性质：**跨模块技术决策 + 落地记录**（`flow_ctrl` / `connection::owner_` /
`connection::mux_connection::registry_` / `connection::session_`）。

**修订：2026-10-05（同日）**——第一版把两个窗口做成**逐字段原子**；人类指出那是过度
设计：窗口的并发面只有「整块状态的读改写」，应该把内部状态**打包放进一把零分配自旋锁**
（`SpinningMutexOwned`），对外仍保持 `&self` 外观。已按后者返工，本文记录**最终形状**；
逐字段原子版本只在 git 历史里（提交 `47d80f0`）。**带锁与不带锁的分界是**：状态字
（`AtomicFlags`）保持无锁，两个窗口内部各一把锁。

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
>    保存流控相关的上下文数据都，尤其是 `ChannelState_` 一大堆 bool 状态，都应该像
>    `AtomicFlags` 那样合并在一个原子的 Flag 里面解决。
> 3. 先把这两个明显不合理的设计改完，后面什么锁的几乎都是伪问题。
> 4. （返工裁决）**不是 `FlowCtrl` 放进锁**，而是 `FlowCtrl` 继续保持所有 API `&self`
>    的外观，**具体在 `SendWindow` 和 `RecvWindow` 内部设 lock**。

---

## 2. 结论速览

| 事项 | 结论 |
| --- | --- |
| 每条子流状态的形态 | **一个可克隆的共享节点** `Shared<ChannelState_, A>`：**一个无锁 `AtomicFlags<usize>` 状态字** + 两个窗口（各自内部一把零分配自旋锁） |
| 状态字为什么仍无锁 | `ChannelTx`/`ChannelRx` 的 `Drop` 与 `try_*` 去重位在**同步路径**上（不能取阻塞锁），`claim_release_` 需要「判定 + 置位」一次 CAS |
| 窗口为什么用锁而不是逐字段原子 | 并发面是「整块状态的读改写」：`install_` 改 4 个字段、`report()` 改 2 个、`on_report()` 读改写 4 个。逐字段原子下这些都不是原子的，只能靠「单写者 + 协议顺序」论证；一把锁让它们天然原子，并删掉 `(r0, w0)` 打包与低 32 位回绕技巧 |
| 锁的选型 | `atomic_sync::mutex::preemptive::SpinningMutexOwned`（**零内部堆分配**）。`cooperative` 版本每条实例一个 `Arc<RwCore>`，等于给每条子流添一次全局分配，违反分配纪律 |
| 状态与身份的寿命 | 登记身份时（`reserve_channel_` / `reserve_inbound_`）建立，`attach_owner_` **删除**；owner 分配 3 次 → 1 次 |
| 收尾判据 | 发送方向 = `FIN` 已发（蕴含接收环已排空）或对端 `RESET`；接收方向 = 应用丢 `rx` 或对端 `FIN`。**应用丢 `tx` 不算完成** |
| 释放身份 | 由 `claim_release_` 一次 CAS 判定并认领；两个方向都协议收尾后才进宽限态 |
| `FIN` / `RESET` | **正交**：`FIN` = 我不再发（对端关接收环写端）；`RESET` = 我不再收（对端停发送、**不**关接收环写端） |
| `drop(rx)` 之后到达的数据 | 解复用循环按 `app_rx_closed_` **静默丢弃**；写进已关闭的环得到的 `Closing` 也不按传输错误处理 |
| 剩下的锁 | 注册表那把（身份表增删查，冷路径）+ 每条子流两把零分配自旋锁（窗口内部） |

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

### 3.3 「同寿命」要求内部可变，但**不等于**逐字段原子

节点经 `Shared` 被多个任务以 `&self` 持有，`&mut` 借出不再可用，因此状态成员必须
**内部可变**。这里有两种实现方式，第一版选了前一种，被人类纠正：

| 方式 | 适用面 | 结论 |
| --- | --- | --- |
| 逐字段原子 | 单写者、单字段读改写的**标志位** | 对**状态字**合适：`is_done_` 一次载入、`claim_release_` 一次 CAS，天然无锁，且 `Drop`/同步去重位不能取阻塞锁 |
| 整块一把锁 | 多字段一起读改写的**窗口** | 对 `SendWindow`/`RecvWindow` 合适：一次操作 = 一个临界区，`install_`/`report`/`on_report` 不再靠「协议顺序排除并发读者」论证 |

人类第 3 条「锁几乎都是伪问题」的正确解读是：**把该锁的地方锁成一小块、该无锁的地方
保持无锁**，而不是「全部原子化所以不需要锁」。前者让状态简单、后者会逼出
`(r0, w0)` 打包与低 32 位回绕这类只为绕开锁而存在的技巧。

---

## 4. 落地形状

### 4.1 `owner_.rs`：`ChannelState_` = 一个无锁状态字 + 两个带锁窗口

```rust
pub(crate) struct ChannelState_ {
    flags_: AtomicFlags<usize>,   // 全部 bool / 枚举 / 两处去重位（无锁）
    flow_: FlowCtrl,              // 收发双向窗口（方法全取 &self；锁在窗口内部）
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

### 4.2 `flow_ctrl`：公开方法全 `&self`，锁下沉到窗口内部（**公开 API 变更，已获批准**）

公开方法由 `&mut self` 改为 `&self`，`send_window_mut` / `recv_window_mut` 删除；
新增 `pub(crate)` 的 `FlowCtrl::new_empty_` 与 `install_(policy, ring_capacity)`
（身份先于环容量出现，窗口参数只能后装）。

**并发不在 `FlowCtrl` 这一层**：`FlowCtrl` 只是两个字段的透传；两个窗口各自把
**内部状态整块**放进一把零分配自旋锁：

```rust
struct SendInner { sent_, reported_, peer_epoch_base_, max_ }        // 全是普通值
pub struct SendWindow { inner_: SpinningMutexOwned<SendInner> }
struct RecvInner { capacity_, received_, consumed_, reported_,
                   activity_since_report_, thresholds_, epoch_base_, epoch_limit_ }
pub struct RecvWindow { inner_: SpinningMutexOwned<RecvInner> }
```

为什么是锁而不是逐字段原子：

| 操作 | 改动的字段数 | 逐字段原子 | 一把锁 |
| --- | --- | --- | --- |
| `install_` | 4（容量 + 两阈值 + epoch 规格） | 非原子，靠「早于任何消费者」论证 | 一段临界区 |
| `report()` | 2（快照 + 变动量） | 非原子 | 一段临界区 |
| `on_report()` | 4（epoch 起点 + 绝对快照 + 窗口 + 有效性） | 非原子；还必须把 `(r0, w0)` 打包、用低 32 位回绕算在途量 | 一段临界区，打包与回绕**整段删除** |
| `on_data()` | 2（已收 + 变动量） | 非原子 | 一段临界区 |

锁的选型：`atomic_sync::mutex::preemptive::SpinningMutexOwned`（`AtomicUsize` +
`UnsafeCell<T>`，**零内部堆分配**）。`cooperative` 版本每条实例持一个 `Arc<RwCore>`，
等于给每条子流添一次**全局**分配，与本仓分配纪律冲突（选型对照见 §3.3 与
§4.2）。

三条实现纪律：

1. **一次操作 = 一个临界区**：`should_report` + `report` 这类「判定 + 产出」不暴露成
   两步给循环用，而是合成 `RecvWindow::take_report_` / `recheck_`（`ChannelState_`
   的两个便捷入口直接调它们）；
2. **`poll` 里不能等锁**：复用循环 park 的就绪条件用
   `SendWindow::available_try_`（`try_lock` 失败按「没额度」处理），与旧实现
   `try_with_` 的契约一致；
3. **临界区里不得 `await`、不得回调进 `ChannelState_`**：窗口方法都是同步的。

生产路径上这两把锁**不会真的自旋**：两个内侧循环投在同一个本地作用域（同线程），
应用线程只碰状态字、不碰窗口；建流期那一次 `install_` / `on_report` 也早于对端可能
发出的任何帧。

### 4.3 `registry_.rs`：状态随身份建立

```rust
struct ChanCtx_<A> { state_: ChannelOwner_<A>, inbound_: Inbound_ }
```

- `reserve_channel_` / `reserve_inbound_` **返回**状态句柄；`attach_owner_` 删除；
- `take_pending_inbound_` 返回 `(remote_dock, state)`，被动方句柄从 `income_async`
  起就交给 `ChannelHandle`；
- `install_channel_` 只做「建环 + `state.install_` + 发 `Attach`」；
- `inbound_` 仍留在注册表锁下（冷路径）；锁序恒为 **注册表 → 窗口锁**，绝不反向，
  且两把锁都不跨 `await`；
- `mark_failed_` 顺手删掉了 audit §5.4 记的死代码 `Vec<Waker>`：通知建流等待者只需
  一次 `notify_establish_`（状态字 + 持久通知通道），可以在注册表锁内直接做完。

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
| `cargo test --lib` | 170 passed（含 owner_ 的「协议层完成才释放」与窗口单测） |
| `cargo test --test inmem_mux` | 20 passed（**新增 `mux_recv_dropped_dual_`**：丢 `rx` 后同连接另一条子流照常双向往返） |
| `cargo test --test smoke_tokio` | 4 passed；`flow_ctrl_socket_tokio_` 现在**写完立刻丢两半**仍逐字节收满 64 KiB（T1 的直接验收） |
| `cargo test --test smoke_compio` | 4 passed（同上） |
| `cargo test --test thread_safety` | 2 passed（`Shared<ChannelState_>` 仍是 `Send + Sync`，含窗口自旋锁） |
| `cargo test --doc` | 4 passed |
| `cargo clippy --all-targets -- -D warnings`（缺省 / compio） | 零告警 |
| `cargo check --all-targets` | 通过 |
| `cargo doc --no-deps` | 仍只有 3 条**既存**告警（与本轮无关） |

以上是**最终（带锁窗口）**版本的数字；第一版（逐字段原子）只有 `lib 170 / inmem 20`
与本文相同，其余未变。`layered_rpc` 挂死是 HEAD 既有现象
（`flow-ctrl-20261005-0115.md` §6），未纳入本轮回归。

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
