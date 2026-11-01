# 事件队列的背压化与一次性通知内联（20261026-0514）

- 分支：`feat/mpmc-chan`（延续同名试验）
- 状态：**阶段 0 / 1a / 1b / 2a / 2b / 2d 已实施；2c 与阶段 3/4 待检阅**
- 现状速查：`connection-delivery-20261026-1845.md`（报文 → 处理对象 → 交付方式）
- 相关：`signal_` / `slot_` / `session_` / `owner_` / `timer_` / `mux_connection`
- 前序：`archive/mpmc-chan-20261026-0046.md`（把 `flume` 换成 `mpmc_chan`，暴露了共享定容队列的
  容量问题）

## 0. 结论先行

1. **共享事件队列的容量必然被并发子流数打满**，因为一次性通知的在途量是
   `子流数 × 常数`，而容量是固定值。继续给「满」打补丁（无界 `VecDeque` 暂存、
   全扫标志）只是把问题从「丢」换成「无界堆增长 + 自旋锁」。
2. 解决办法不是加大容量，而是**把容量按子流分摊**：每子流的一次性通知在协议状态机
   下有**常数上界**，内联进该子流的身份节点即可，永远不会因别的子流而饱和。
3. **不可重建的通知不再与高频提示混在一条队列里**：拆流 / 释放类通知退化为
   「owner 状态位 + 连接级合并提示」；`Attach`（携带环半部）走一条**专用小容量通道**；
   长事务（排空、释放宽限）交给**每连接一个 detach 的控制面循环**。
4. 全程守住「减少运行时动态堆分配」：删除两条 `1024 × 64 B ≈ 128 KiB/连接` 的固定
   缓冲与满时 `VecDeque`，改为每连接约 12 KiB 的小通道 + 节点内联槽（节点本来就按身份
   各分配一次）。

## 1. 高优先级、每子流不重复的事件（清单元数据）

判据：丢失会造成协议违约、环/身份资源泄漏，或等待者永久挂起。

### 1.1 送复用循环（`WriteEvent_`）

| 事件 | 产生点 | 每子流次数 | 负载 | 已存在的事实位 |
| --- | --- | --- | --- | --- |
| `Attach` | `install_channel_` | 1 | `BufferedRx` + owner | 无 |
| `Control(OPEN)` | `send_open_` | 1 | 开场载荷 + 窗口 | 无 |
| `Control(ACCEPT)` | `mux_accept_async_` | 1 | 窗口 | `ESTABLISH_SETTLED` |
| `Control(REJECT)` | reject / abort | ≤1 | 理由载荷 | `abort_` / notice |
| `Control(CLOSE FIN/RESET)` | `finalize_entry_` / timer / `RxClosed` | ≤2 | 无 | `APP_*` / `PEER_*` / `abort_` |
| `TxClosed` | `ChannelTx::seal_tx_` | 1 | dock 对 | `APP_TX_CLOSED` |
| `RxClosed` | `ChannelRx::seal_rx_` | 1 | dock 对 | `APP_RX_CLOSED` |
| `LocalAbort` | timer `Abort` | 1 | dock 对 | `AbortCode_` |
| `PeerClosed` | 读循环收 `CLOSE` | 1 | dock 对 + `reset_` | `PEER_FIN` / `PEER_RESET` |
| `TgAttach` | `Telegraph::split` | 1 | `BufferedRx` + `TgOwner` | 无 |
| `TgTxClosed` | `TelegraphSender::drop` | 1 | local dock | 无（新增位） |

### 1.2 送解复用循环（`ReadEvent_`）

| 事件 | 产生点 | 每子流次数 | 负载 | 已存在的事实位 |
| --- | --- | --- | --- | --- |
| `Attach` | `install_channel_` | 1 | `BufferedTx` + owner | 无 |
| `Release` | `maybe_release_` / timer | 1 | dock 对 | `RELEASED` |
| `TgAttach` | `Telegraph::split` | 1 | `BufferedTx` + `TgOwner` | 无 |

### 1.3 会话身份释放（`SessionEvent_`，多消费者）

`UnbindDock` / `ReleaseListener` / `ReleaseTelegraph` / `UnreserveChannel` /
`ReleaseChannel`，各 1/身份，由 `Drop` 同步投递，必须由某个核心执行者落实。

### 1.4 明确排除（会重复、可重建）

`TxReady` / `TgReady` / `RxConsumed` / `WINDOW_UPDATE` / `PULSE`：信息量只是
「可能变了」或「最新窗口快照」，由状态位 / `FlowCtrl::recv_window()` + 时钟重建。

**关键观察**：除 `Attach` / `TgAttach` 与 OPEN/ACCEPT/REJECT 这类携带一次性载荷的
条目外，其余都已有对应的状态位——它们本来只是「叫醒循环去看位」，不需要作为独立消息
排队。

## 2. 为什么加大容量解决不了

```text
一次性通知在途量 ≈ 子流数 × (Attach + OPEN + ACCEPT/REJECT + CLOSE×2 + 拆流通知)
```

容量是编译期常量，左侧随子流数线性增长 ⇒ 必然打满。`EVENT_CHAN_CAP` 从 256 调到
1024 只是把打满的时刻推迟（前序笔记 §6.1 已实测：256 在 256 子流跨线程用例里直接
超时）。

**容量应该按「每子流上界」分摊，而不是按「并发子流数」预留。**

## 3. 决策记录（本轮讨论）

| 议题 | 决定 |
| --- | --- |
| 拆流/释放类通知的落点 | **保留一条小容量专用通道**（满时背压），不再走 1024 混合队列 |
| Attach 的承载 | **不另建 attach 通道**；新增一个「Listener 与建流、拆流共用」的连接级循环，Listener 以 `local_dock` 为 key 单独建立 registry |
| 长操作执行者 | **每连接一个 detach executor**（不是每操作 spawn 一个任务） |
| 编码前置 | 两处 `Cargo.toml` 的未提交变更保持原样 |

### 3.1 为什么是「一个控制面循环」而不是「每操作一个任务」

`C::ConnTx` 与两个环半部是**独占移交**给数据循环的（`session_` 模块文档「半部为什么是
移交」）。独立任务不能替写循环搬数据或发帧，除非把传输写权改成共享（引入新锁/新缓冲，
与「减少分配」相悖）。

因此控制面循环的边界是：

- **能做的**：等待 owner 上的进展位、调注册表（异步锁、可取消）、决定「什么时候可以
  发 FIN/RESET/RELEASE」、分发 `Attach` 给两个数据循环；
- **不能做的**：读写 `tx_stage`、持有环半部、替写循环成帧。

这同时消除了写循环事件处理里的长 `await`（`Control` 整帧写出、`finalize_entry_`
等额度、`maybe_release_` 等注册表锁）。

## 4. v2 架构

### 4.1 四个循环

```text
demux  ── 数据面（收）
mux    ── 数据面（发）
timer  ── 保活 / 空闲拆流
ctl    ── 身份与生命周期：Attach 分发、OPEN/ACCEPT/REJECT 投递、
          Listener 入向、Release/finalize 长事务
```

`ctl` 是第四个取消令牌，`MuxCore` 的收尾判据从 3 个令牌变为 4 个。

### 4.2 Listener 的独立 registry

在 `ChannelRegistry_` 内部新增一张**按 `local_dock` 分组**的入向表（逻辑独立、共用同一
把写锁，避免第二把锁与锁序问题）：

```rust
// local_dock -> (remote_dock -> ChannelOwner_)
by_local_: BTreeMap<Dock, BTreeMap<Dock, ChannelOwner_<A>>>,
```

`Inbound_` 状态从 `ChanSlot_` 移入这张表，由 `ctl` 统一登记 / 取走 / 清理。
`income_async` 仍 park 在 `LsnRec_.notify_` 上。

### 4.3 三条投递路径

| 通路 | 承载 | 容量 | 满时 |
| --- | --- | --- | --- |
| `ctl_rx`（mpmc 定容） | `Attach`、会话释放、`Wake` | 64 | `send_async` 背压（可取消） |
| `mux_inbox` / `demux_inbox`（内联 SPSC） | `ctl` → 数据循环的 Attach | 64 | `ctl` 等消费者取 |
| `hint_w` / `hint_r`（`HintSlot_`） | 拆流/释放/水位提示 | 1（位集） | **不会饱和**（实现时并入 `RegistryShared_` 位域，见 §6.4） |
| 每子流 `InlineQueue_` | OPEN / ACCEPT / REJECT | 2 | 生产者等待（上界内不发生） |

`CLOSE(FIN/RESET)`、`WINDOW_UPDATE`、`PULSE` **不占队列**：由 owner 位与窗口快照重建。

### 4.4 背压点

- `ctl_rx`：建流 / 拆流 / 释放的 `send_async`，满则等 `ctl` 取走；
- `mux_inbox` / `demux_inbox`：`ctl` 等数据循环取走；
- `InlineQueue_`：槽满时 `push_async` 等待；
- `Drop` 路径不 `await`：只置 owner 位 + `raise_` 提示，`ctl` 醒来后按位扫描兜底。

## 5. 分配账

| 项 | 现在 | v2 |
| --- | --- | --- |
| 两条事件通道 | 2 × 64 KiB + 2 次大分配 | 2 × 64 × ~100 B ≈ 12.8 KiB + 2 次小分配 |
| `Overflow_::durable_` | 满时 `VecDeque` 无界增长 | 删除 |
| 每子流控制帧 | 共享队列条目 | 节点内 2 × 64 B（不新增分配） |
| 每子流一次性通知 | 共享队列条目 | owner 位（0 字节） |
| 提示 | 共享队列条目 | 每方向一个 `NotifySlot_`（0 堆） |
| 长操作 | 写循环内 `await` | 每连接 1 个 detach 任务（1 次 spawn） |

净效果：每连接少约 **115 KiB** 固定堆分配；每子流身份节点增大约 **200 B**（不新增分配
次数）；`durable_` 的无界增长消失。

## 6. 阶段计划与进度

| 阶段 | 内容 | 状态 |
| --- | --- | --- |
| 0 | `InlineQueue_` / `HintSlot_` 原语 + 单元测试 | **已完成**（`slot_.rs`，15 个测试全过） |
| 1a | `TxClosed` / `RxClosed` 位化 + 写循环扫描；删对应 durable 事件 | **已完成**（见 §6.2） |
| 1b | `PeerClosed` / `Release` 位化 + 读循环扫描 | **已完成**（见 §6.3） |
| 2a | 第 4 个 `ctl` 循环骨架 + 第 4 个取消令牌 | **已完成**（见 §6.5） |
| 2b | `Attach` 改走内联 inbox；补投背压；删事件 `Attach` | **已完成**（见 §6.5） |
| 2c | Listener registry（按 `local_dock`）+ ctl 承担入向 / 拆流事务 | **待检阅**（先定交付矩阵，见 `connection-delivery-20261026-1845.md` §4） |
| 2d | `EVENT_CHAN_CAP` 1024 → 256 | **已完成**（见 §6.5） |
| 3 | 控制帧入 `InlineQueue_`；CLOSE/窗口/PULSE 改重建；删除无界 durable 暂存 | 待做 |
| 4 | `DrainExecutor_` 长事务（finalize / release） | 待做 |

### 6.2 阶段 1a 实现记录

改动面：

- `ChannelRegistry_` 新增 `write_hint_: Shared<HintSlot_, A>`（每连接一次分配，与
  `timer_wake_` 同源）与 `raise_write_hint_` / `take_write_hint_` / `poll_write_hint_`；
  **§6.4 已把这组 `Shared` 合并为 `RegistryShared_` 的位域**；
- `ChannelTx::seal_tx_` / `ChannelRx::seal_rx_`：保留 `set_app_*` 置位，把原来的
  `try_send_durable_event_(TxClosed / RxClosed)` 换成一次 `raise_write_hint_`；
- timer 的 `TimerAction_::Finalize`：取 owner 并置 `APP_TX_CLOSED`，再 `raise`；
- 写循环每轮顶部 `take_write_hint_`：`HINT_W_TX_CLOSED` 做一次
  `scan_tx_closed_`（把 `APP_TX_CLOSED && !LOCAL_FIN_SENT` 的条目登记进
  `pending_fin`，由既有 2.5 步推进）；`HINT_W_RX_CLOSED` 调 `flush_rx_closed_`
  （认领 → 写 `CLOSE(RESET)` → 置「已上线」→ 唤醒 → 上报 → `maybe_release_`）；
- 写循环的 park 点同时 `poll_write_hint_`，被唤醒后回顶部扫描（不清 `last_ready`）；
- 删除 `WriteEvent_::TxClosed` / `RxClosed` 与 `handle_write_event_` 的对应分支。

**竞态修复（重要）**：合并提示只保证「有一次唤醒」。若应用在 `accept_async` 返回后
立刻 `drop(tx/rx)`，`seal_*` 可能在写循环处理 `Attach` **之前**就发出提示；提示被
顶部消费时，本地表里还没有这一对，扫描就会漏掉。修法是在
`handle_write_event_::Attach` 插入表项后检查一次状态字：`APP_TX_CLOSED` 直接登记
`pending_fin`，`APP_RX_CLOSED` 重新 `raise` 一次（此时表里已有该对）。这条与旧事件
通道的 FIFO 顺序保证等价，但不依赖队列。

验证（全部通过）：

- `cargo test -p smux_v1 --lib`：343；
- `inmem_mux`：20（compio / tokio / smol 三后端各一遍）；
- `smoke_*`：5（三后端，真实 socket，compio 14.5 s、tokio 11.4 s、smol 11.2 s）；
- `cross_thread_*`：1（三后端）；
- `keepalive`：5（tokio）；
- `cargo test --doc`：12；
- `cargo clippy -p smux_v1 --lib`：无新告警；
- `format_use_imports.py --check`：全部合规。

### 6.3 阶段 1b 实现记录

改动面：

- `WriteEvent_::PeerClosed` 删除：读循环收到 `CLOSE` 后只置 `PEER_FIN` / `PEER_RESET`
  并 `raise_write_hint_(HINT_W_PEER_CLOSED)`；写循环 `scan_peer_closed_` 按写表扫描，
  `RESET` 摘表 + 放弃 `pending_fin`，`FIN` 只做一次 `maybe_release_`（表项上的
  `peer_fin_handled_` 去重）；
- `ReadEvent_::Release` 删除：`maybe_release_` 与 timer `Abort` 改
  `raise_read_hint_(HINT_R_RELEASE)`；读循环 `scan_released_` 按 `RELEASED` 位摘表并
  `writer_.close()`；`is_released_` 从 `#[cfg(test)]` 放开为正式接口；
- 读循环的 park 同时 `poll_read_hint_`（`ReadWake_::Hint(u64)`），主循环与顶部都处理；
- `ChannelRegistry_` 新增 `read_hint_`（与 `write_hint_` 同源，每连接一次分配）。

**修复 1（优雅关闭 30 s 超时）**：`finalize_entry_` 原本发完 `FIN` 就 `table.remove`，
而 `scan_peer_closed_` 只扫写表 —— 对端随后的 `FIN` 到达时表项已不在，`maybe_release_`
永远不会被调用，身份不释放，只能等 `max_channel_timeout`。修法：`FIN` 发出后**保留**
写表项（`fin_sent_ = true`），只有 `is_released_()` 为真时才摘；`drain_one_` 对
`fin_sent_` 条目直接跳过，避免每轮对已关闭的环做 `try_read`。

**修复 2（空闲超时拆流读不到 EOF）**：timer 的 `Abort` 直接调 `release_channel_`，
不走 `claim_release_` 的「两方向都收尾」判据，因此 `RELEASED` 从未置位；读循环的
`scan_released_` 按位摘表，于是读表项一直留着、`writer_` 不关闭，应用读端拿不到 EOF。
修法：新增 `ChannelState_::mark_released_()`，timer 在该路径上补置位（幂等 CAS）。

**修复 3（`Attach` 先于对端 `CLOSE` 的竞态）**：读循环可能比写循环更早处理 `CLOSE`，
此时写表还没有该对；hint 被消费后扫描扫不到。修法：`WriteEvent_::Attach` 插入表项前
读一次 `PEER_FIN` / `PEER_RESET`，若有则插入后重新 `raise`（与阶段 1a 的
`APP_TX_CLOSED` / `APP_RX_CLOSED` 检查同构）。

验证（全部通过）：

- 默认 compio `--all-targets`：343 lib + inmem 20 + smoke 5 + cross_thread 1 + keepalive 5；
- tokio `--all-targets`：同上（inmem 30.2 s、smoke 26.1 s、另有 60 s 的 target）；
- smol `--all-targets`：同上（inmem 30.1 s、smoke 26.5 s）；
- `cargo test --doc`：12；
- `cargo clippy -p smux_v1 --lib`：无新告警；
- `format_use_imports.py --check`：合规。

> 计时数字比 1a 前的全量慢（inmem 15 s → 30 s、smoke 14 s → 18/26 s）。这些用例的
> 时长主要由连接收尾的等待路径决定（单测串行跑时 15 s / 30 s 是 `max_channel_timeout`
> 量级），**尚不能断定是本次改动引入的回归**；阶段 2 开始前应先用 `HEAD` 的 worktree
> 跑同一组单测做基线对比。

### 6.4 锁与锁外状态：arena 等待队列 + 位域（2026-10-26 下午）

这一轮不是事件队列的改动，而是顺着「锁外状态为什么是六个 `Shared`」这个问题做的
基础设施收敛。

1. **RwLock 换 `cooperative_arena`**：`atomic_sync` 的两套协作式锁 API 一致，后者把
   等待节点放进内部 `Arena<WaitNode>`（侵入式链表），不再每个 waiter 一个
   `Arc<WaitNode>`。长期运行（大量建流 / 拆流）下少一类 `Arc` 分配与释放，降低碎片。
2. **注册表锁外状态合并**：`timer_wake_` / `write_hint_` / `read_hint_` / `fail_kind_`
   / `state_` / `close_since_millis_` 原本各是一个 `Shared`（每连接 6 次分配、6 个原子）。
   现在压成一个 [`RegistryShared_`](../smux_v1/src/connection/mux_connection/registry_.rs)：
   - 一个 `AtomicU64` 位域：`state`(2) / `fail_kind`(2) / `timer_wake`(1) /
     `write_hint`(24) / `read_hint`(24)；
   - 一个 `AtomicU64` 存关闭起点（时间戳无法压进小位域）；
   - 三路 waker 槽（`Option<Waker>` 无法压进原子）。
   它与锁内的表一起放进 `RegistryRoot_`，注册表克隆只做**一次**引用计数
   （原 `inner_` + 6 个 `Shared` = 7 次分配 → 1 次）。
3. **取消令牌合并**：`cancelled_` 与「通知 pending」原本是两个 `AtomicBool` + 两次
   `Shared` 分配。等待方每次 poll 都先查取消位，通知位对取消语义是冗余的；合并成
   「一个 `AtomicBool` + 一个 waker 槽 + 一次分配」。3 个循环令牌从 6 次分配降到 3 次。
4. **中止代码并入状态字**：`ChannelState_.abort_`（`AtomicU8`）并入 `flags_` 的
   bit 16-17，每子流少一个原子。

分配账（每连接）：注册表 7 → 1；取消令牌 6 → 3；邮箱 2（未变）。每子流：`abort_`
原子 -1。

**性能观察（值得记录）**：合并后 `inmem_mux` 从 15 s / 30 s 降到 **1.0 s**
（compio / smol），tokio 从 30 s 降到 15 s；`smoke_*` 从 18/26 s 降到 16/11 s。
说明此前那 15 s / 30 s 的等待**并非全部**来自测试驱动方式——锁外状态分散在多个
`Shared`/原子时，唤醒与状态可见性存在跨克隆的竞态窗口，合并到同一原子域后消失。
之前 §6.3 里「待基线对比」的条目可以据此结案。

验证：三后端 `--all-targets` 全通过；`--doc` 12；`cargo clippy --lib` 无新告警；
`format_use_imports.py --check` 合规。

### 6.5 阶段 2a/2b：控制面循环骨架 + `Attach` 迁移（2026-10-26 晚）

**2a：第 4 个循环。** 取消令牌数组从 3 扩到 4，`MuxConnection::new_with_rt_` 多
`spawn_local` 一个 `ctl_loop_async_`（`ctl_.rs`，当前只等取消），`MuxCore` 的收尾判据
随之覆盖 4 个令牌。

**2b：`Attach` 改走内联 inbox。** `WriteAttach_` / `ReadAttach_`（dock 对 + 身份节点 +
环半部）不再进事件通道，而是放进 `RegistryRoot_` 的两条 `InlineQueue_`（容量 64）。
- `install_channel_` 先 `try_push`；满则把 `Attach` 带回 `ChannelHandle::pending_attach_`，
  由 `accept_async` 的异步阶段补投（真正的背压）；
- 写 / 读循环每轮顶部 drain inbox，park 时同时 poll inbox；
- 循环退出时守卫 drain 未取走的 `Attach` 并 `close()` 环半部，然后关闭 inbox；
- `WriteEvent_::Attach` / `ReadEvent_::Attach` 删除。

**修复 1（256 并发建流的死锁）**：`InlineQueue_` 的「等空位」原来用 `NotifySlot_`，
而它是**单等待者**槽；`cross_thread_*` 的 256 个并发建流任务里只有一个能留下 waker，
其余永久睡死（20 s 看门狗）。改为 `Vec<Waker>` 广播（`free_waiters_`），并在登记后
复检 `len_hint_ < N`，避免「腾位与登记之间」丢唤醒。取消也必须自己登记 waker
（`cancelled` future 与空位一起等），否则「取消后不唤醒」会卡住补投方。

**修复 2（流控超时）**：`RxConsumed` 仍在事件通道，`Attach` 已改走 inbox，两者不再共享
FIFO。若 `RxConsumed` 先被取出（此时读表还没有该对），事件被丢弃、`RX_CONSUMED` 位却
留着。修法：每插入一条新的读表项，就调一次 `rescan_read_` 按位重算水位。`smoke_compio`
的 `flow_ctrl_socket_` 从 20 s 超时回到 0.3 s。

验证：compio / tokio / smol `--all-targets` 全通过（343 lib + 全部集成；inmem 仍约 1 s
（compio/smol）/ 15 s（tokio），smoke 11–16 s）。

**2d：容量下调。** `Attach` 移出后 `EVENT_CHAN_CAP` 从 1024 降到 256（两条通道从
128 KiB 降到 32 KiB/连接）。256 条并发子流的 `cross_thread_*` 与三后端真实 socket
`smoke_*` 全部通过。

剩余：ctl 尚未承担 Listener registry 与拆流事务（2c）。

### 6.1 阶段 0 的接口

`slot_.rs` 提供：

- `InlineQueue_<T, N>`：`try_push_` / `try_pop_` / `poll_pop_` / `push_async_` / `close_`；
- `HintSlot_`：`raise_` / `take_` / `poll_wait_`；
- 测试覆盖 FIFO、满时拒绝、异步入队 park、取消、关闭唤醒、提示合并与唤醒。

过渡期在模块头用 `#![allow(dead_code)]` 抑制「尚未接入」的警告。**现状**：
`InlineQueue_` 已接入（`Attach` inbox）；`HintSlot_` 已被 §6.4 的位域取代、没有任何
外部引用；`#![allow(dead_code)]` 与 `HintSlot_` 应在交付矩阵定稿后一起删除。

## 7. 风险与待确认

1. **顺序保证**：`Attach` 必须先于同子流的帧处理。`ctl` 分发 `Attach` 与 `OPEN` 的
   先后由 `ctl` 串行化保证；读循环每轮先排空 `demux_inbox`。
2. **`Release` 先于 `Attach`**：处理 attach 时先查 `is_released_()`，已释放则直接关环、
   不插表。
3. **全扫成本**：写循环本来就全表扫描，新增的位检查同阶；读循环只在 `hint_r` 置位时扫。
4. **`chan-flume` feature**：阶段 3 后失去意义，去留需要单独决定（Cargo feature 属于
   对外约定）。
5. **`ctl` 与 `SessionMailbox_` 的关系**：`Drop` 不能 `await`，邮箱仍保留为兜底；
   是否给身份节点加 `dropped_` 位以彻底去掉溢出暂存，留到阶段 4 评估。
