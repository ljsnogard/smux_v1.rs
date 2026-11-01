# 连接内部交付矩阵：报文 → 处理对象 → 当前交付方式（20261026-1845）

- 分支：`feat/mpmc-chan`
- 基线提交：`3df1b70`（`Attach` 已走内联 inbox、事件通道容量 256）
- 状态：**现状梳理，不是设计定稿**。下一会话据此重新检阅整体设计。
- 相关：`session_` / `signal_` / `slot_` / `registry_` / `owner_` / `channel_handle` / `timer_` / `ctl_`

> 本文只回答三件事：**从传输收到什么报文**、**要交给哪些对象**、**目前用什么方式交**。
> 末尾列出已知的重复路径与待决问题；在交付矩阵定稿前，不再新增队列/通道/槽。

---

## 0. 连接级任务与输入

| 任务 | 入口 | 输入 | 输出 |
| --- | --- | --- | --- |
| 解复用 `demux` | `session_::demux_loop_async_` | `C::ConnRx`（socket 读半边，自带缓冲）、`ReadEvent_` 通道、`demux_inbox`、`hint_r`、取消令牌 | 接收环写端、`ChannelState_` 位、`reserve_inbound_`、`Control` / `TxReady` 事件、`hint_w` |
| 复用 `mux` | `session_::mux_loop_async_` | `C::ConnTx`（socket 写半边）、`WriteEvent_` 通道、`mux_inbox`、`hint_w`、取消令牌 | `ConnTx` 字节、`ChannelState_` 位、`release_channel_`、`hint_r` |
| 计时 `timer` | `timer_::timer_loop_async_` | 注册表扫描、`timer_wake` 位、`delay`、取消令牌 | `Control(PULSE/CLOSE/REJECT)`、`APP_TX_CLOSED` + `hint_w`、`LocalAbort` 事件、`mark_released_` + `hint_r` |
| 控制面 `ctl` | `ctl_::ctl_loop_async_` | **目前只有取消令牌** | **无**（骨架，尚未承担 Listener / 拆流） |

四个循环都是 `spawn_local` + `detach`，只持有 `MuxShared_`（注册表 + 时钟 + 指标），不持有
`MuxCore`；取消令牌数组长度为 4。

---

## 1. 从 socket 收到的帧

解析在 `session_::demux_loop_async_` 内逐字节完成；拿到 `FrameHeader` 后按 `kind` **同步**
分派。`pair = (local_dock, remote_dock)`。

| 帧 | 处理对象 | 当前交付方式 | 备注 |
| --- | --- | --- | --- |
| `Data` | 读表 `ReadEntry_ { owner_, writer_ }` | **直接**：`owner_.recv_on_data_` 流控记账 → 可能 `events_tx.try_send_durable_event_(Control(WINDOW_UPDATE))` → `write_into_ring_(&mut writer_, ...)` 写接收环 | 表里没有：宽限态静默丢，否则协议错误 |
| `Open`（已有表项） | `owner_` | **直接**：`send_on_report_`、`set_peer_opened_`、`wake_establish_` | 本端发起、对端回 `OPEN` |
| `Open`（无表项） | 注册表 + listener | **直接**：`reserve_inbound_` 登记身份、`Inbound_::Pending`、`notify_inbound_` | 应用 `income_async` 后由 `accept_async` 建环 |
| `Accept` / `Reject` | `owner_` | **直接**：`set_establish_outcome_` + `wake_establish_` | 建流等待者被唤醒 |
| `Close` | `owner_` + `writer_` | **直接**：`set_peer_fin_`/`set_peer_reset_`、`writer_.close()`、`notify_*_progress_`、`raise_write_hint_(HINT_W_PEER_CLOSED)` | 拆流记账由写循环扫描本地表完成 |
| `WindowUpdate` / `Pulse` | `owner_`（表里或注册表兜底） | **直接**：`send_on_report_`、`touch_`/`mark_data_`；再 `try_send_event_(TxReady)` 唤醒写循环 | 可重建；无表项时从注册表取 owner 只记活动 |
| `Datagram` | `TgRecvCtx_` | **直接**：`ctx.deliver_frame_` → 整帧写 telegraph 接收环 + `TgLenQueue_` | 中心循环不理解数据报内部结构 |

**结论**：入向帧本身不经过任何队列；解析完就在读循环栈上直接改 `ChannelState_` / 写环 /
注册表。只有「需要另一个任务做的事」才投递。

---

## 2. 非报文输入（应用 / 计时 → 循环）

| 产生方 | 交给谁 | 当前交付方式 | 每子流次数 |
| --- | --- | --- | --- |
| `ChannelTx::write*` | 写循环 | 发送环 + `WriteEvent_::TxReady`（可重建；满时丢 + `Overflow_` 标志） | 多次 |
| `ChannelTx::seal_tx_` | 写循环 | `APP_TX_CLOSED` 位 + `HINT_W_TX_CLOSED` | 1 |
| `ChannelRx::read*` | 读循环 | 接收环 + `ReadEvent_::RxConsumed`（可重建） | 多次 |
| `ChannelRx::seal_rx_` | 写循环 | `APP_RX_CLOSED` 位 + `HINT_W_RX_CLOSED` | 1 |
| `accept_async` / `install_channel_` | 写 / 读循环 | `mux_inbox` / `demux_inbox`（`InlineQueue_`，容量 64）；满时由 `accept_async` 补投 | 1 / 1 |
| `accept_async` 的 OPEN / ACCEPT | 写循环 | `WriteEvent_::Control`（durable） | 1 |
| `reject_async` | 写循环 | `WriteEvent_::Control(REJECT)` | ≤1 |
| `Telegraph::split` | 写 / 读循环 | `WriteEvent_::TgAttach` / `ReadEvent_::TgAttach` | 1 / 1 |
| `TelegraphSender::send` | 写循环 | `TgReady` 事件 + 长度队列 | 多次 |
| `TelegraphSender::drop` | 写循环 | `TgTxClosed` 事件 | 1 |
| 会话句柄 `Drop` | 注册表（任一核心执行者） | `SessionMailbox_`（mpmc 256 + `VecDeque` overflow） | 1/身份 |
| timer `Pulse` / `Close` / `Reject` | 写循环 | `WriteEvent_::Control` | 多次 / ≤2 |
| timer `Abort` | 写 / 读循环 | `WriteEvent_::LocalAbort` + `mark_released_` + `HINT_R_RELEASE` | 1 |
| `maybe_release_` | 读循环 | `HINT_R_RELEASE` | 1 |

---

## 3. 当前所有「队列 / 槽」

| 结构 | 类型 | 容量 | 分配 | 满时语义 | 承载 |
| --- | --- | --- | --- | --- | --- |
| `WriteEvent_` | `mpmc_chan` | 256 | 1 次/连接（约 16 KiB） | 可重建：丢 + 全扫；不可重建：`Overflow_::durable_` 无界 `VecDeque` | Control / TxReady / LocalAbort / TgAttach / TgReady / TgTxClosed |
| `ReadEvent_` | `mpmc_chan` | 256 | 1 次/连接 | 同上 | RxConsumed / TgAttach |
| `SessionMailbox_` | `mpmc_chan` + `Arc<MailboxOverflow_>` | 256 | 1 + 1 次 | `VecDeque` 无界 | 身份释放 |
| `mux_inbox` | `InlineQueue_<_, 64>` | 64 | 内联在 `RegistryRoot_` | 建流方 `push_async` 等待（`Vec<Waker>`） | `WriteAttach_` |
| `demux_inbox` | 同上 | 64 | 同上 | 同上 | `ReadAttach_` |
| `RegistryShared_.flags_` | `AtomicU64` 位域 | 1 字 | 随 root 一次 | 不适用（置位合并） | state / fail / timer / write hint / read hint |
| `NotifySlot_` | 原子位 + 单 waker | 1 | 内联 / `Shared` | 不适用（持久位） | establish / tx_progress / rx_progress / listener / TgLenQueue |
| `Vec<Waker>`（inbox 背压） | 自旋锁 `Vec` | 等待者数 | **背压时才分配** | 不适用 | inbox 空位等待 |
| `pending_attach_` | `Option`（`ChannelHandle`） | 2 | 栈内 | 不适用 | inbox 满时的补投 |
| `pending_fin` | `BTreeSet`（注入分配器） | 活跃子流数 | 节点分配 | 不适用 | 欠一条 FIN |
| `TgLenQueue_` | `[AtomicUsize; 64]` | 64 | 内联 | `OutboxFull` | telegraph 报文长度 |
| 发送 / 接收环 | `buffex::ring` | 调用方定 | 1 次/子流 | 写满 / 读空（背压） | 数据面 |

另有三个**连接级全局分配**（已知例外）：

1. 外层 `CooperativeRwLockOwned` 内部用全局 `Arc` 持有同步核心（`atomic_sync` 尚未支持
   `allocator_api`）；
2. `SessionMailbox_` 的 mpmc `Arc` 与 `MailboxOverflow_` 的 `Arc`；
3. 三个循环 + ctl 各一次的 `spawn_local` 装箱（后端内部）。

---

## 4. 已知重复路径与待决问题

### 4.1 `Attach` 有两条潜在入口

- 当前：`install_channel_` 直接写 `mux_inbox` / `demux_inbox`；
- 计划：`ctl` 循环原本要“与建流、拆流共用”，但骨架尚未接任何队列。

**必须先定**：`Attach` 走数据循环 inbox（当前），还是先进 `ctl_rx` 再由 ctl 转发？
两者只保留一个。`ctl` 若转发，会增加一跳、双缓冲、队头阻塞与半提交处理。

### 4.2 两个无界溢出暂存

- `signal_::Overflow_::durable_`（`SpinningMutexOwned<VecDeque<E>>`）：`Control` / `LocalAbort`
  / `TgAttach` 等满时进入；
- `signal_::MailboxOverflow_::queue_`：身份释放满时进入。

两者都与「减少运行时堆分配」直接冲突，且容量随负载增长。方向是：
`Control` 走专用小通道 + 背压，身份释放改用 `dropped_` 位 + ctl 扫描。

### 4.3 `Vec<Waker>` 是背压实现，不是数据队列

inbox 满时等待空位的生产者可能是 N 个（并发建流），`NotifySlot_` 的单 waker 不够用，
所以用 `Vec<Waker>` 广播。可选收敛：

- 并进 `SlotRing_` 的同一把 `SpinningMutexOwned`，减少一把锁；
- 或改用 `mpmc_chan` 的 SlabList 等待队列（代价：多一次通道分配）。

### 4.4 `HintSlot_` 是死代码

`slot_::HintSlot_` 已被 `RegistryShared_` 的位域取代，没有任何外部引用；模块级
`#![allow(dead_code)]` 正在掩盖它。应在定矩阵后删除（连同两个测试），并去掉 allow。

### 4.5 `TgAttach` 仍走事件通道

`WriteEvent_::TgAttach` / `ReadEvent_::TgAttach` 仍携带环半部，和 channel `Attach` 同形，
却还在 mpmc 通道里。要么同样迁到 inbox，要么明确 telegraph 的容量上界更小、可以留在
事件通道。

### 4.6 `LocalAbort` 仍走事件通道

timer 的 `Abort` 每子流一次，目前投 `WriteEvent_::LocalAbort`。它可以像
`PEER_FIN/PEER_RESET` 一样位化（`AbortCode_` 已在状态字 bit 16-17），由写循环扫描。

### 4.7 `Control` 把可重建与不可重建混在一起

`OPEN/ACCEPT/REJECT/CLOSE` 不可重建；`WINDOW_UPDATE/PULSE` 只是最新窗口快照。它们共用
`WriteEvent_::Control` 与同一个 durable 暂存。方向：

- 一次性帧：per-owner 定容槽（`InlineQueue_<ControlFrame_, 2>`）或专用小通道；
- 可重建帧：由 `FlowCtrl::recv_window()` + 时钟按需重建，不排队。

### 4.8 `ctl` 目前是空循环

第 4 个循环已经 spawn、有独立令牌，但不消费任何输入。在定矩阵前不要给它加队列；先在
文档里明确它到底承担哪些事务（Listener 入向、宽限、等排空、释放）。

---

## 5. 建议的检阅顺序（下一会话）

1. **先定“一条路径只有一种交付方式”**：数据 → 环；一次性 move 资源 → 每消费者 inbox；
   一次性状态 → owner 位 + 合并提示；可重建提示 → 位域/快照；需要排队的控制帧 → 一条
   专用通道或 per-owner 槽；身份释放 → 邮箱或 `dropped_` 位。
2. **再定 `Attach` 的唯一入口**（inbox vs ctl），据此决定 `ctl` 是否要 `ctl_rx`。
3. **消掉两个无界 `VecDeque`**（4.2）；这是与“减少堆分配”最直接的冲突。
4. **清死代码**（4.4）与重复路径（4.5 / 4.6）。
5. 最后才考虑容量与分配器归属（`EVENT_CHAN_CAP`、arena 的 `new_owned_in`、邮箱的
   `Shared<_, A>`）。

> 在此之前，不再新增队列、通道、槽或 `Vec`。

---

## 6. 文档状态

| 文档 | 状态 |
| --- | --- |
| `connection-delivery-20261026-1845.md`（本文） | **现行**：交付矩阵底稿 |
| `event-backpressure-20261026-0514.md` | **现行**：1a/1b/2a/2b/2d 的实施记录；阶段表已更新 |
| `archive/mpmc-chan-20261026-0046.md` | **已归档**：事件分类/容量/取消令牌均已被本文与 `event-backpressure` 取代；仅保留 flume→mpmc 的历史因果与实测 |
| `no-vec-audit-20261024-0008.md` | **历史**：`Vec` 审计的原则仍有效，行号基线 `d2f848e` 已过时 |
| `no-stage-20261015-1955.md` | **历史**：去掉连接级 stage 环的决策，仍是当前架构由来 |
| `flush-contract-20261015-2340.md` | **历史**：写侧收尾契约，仍有效 |
| `channel-close-20261017-0108.md` | **历史**：子流可等待收尾，仍有效 |
| `listener-queue-20261015-0330.md` | **历史**：`smux_v1_listener` 交付队列，仍有效 |
| `listener-close-20261015-0353.md` | **历史**：listener 关闭模型，仍有效 |
| `dock-macro-20261017-2254.md` | **历史**：dock 宏化，仍有效 |
| `uds-bench-20261015-2240.md` | **历史**：UDS 基准项目，仍有效 |
