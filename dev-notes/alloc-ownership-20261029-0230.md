# 临时分配归属：Global / 专用池 / Scope 的三分（20261029-0230）

- **分支**：`feat/sw-scope-r1`（R1）、`feat/sw-scope-r2`（R2）、`feat/sw-scope-r3`（R3）、`feat/sw-scope-r4`（R4）、`feat/sw-scope-r5`（R5）、`feat/sw-scope-r6`（R6）、`feat/sw-scope-r7`（R7）、`feat/sw-scope-r8`（R8）、`feat/sw-scope-r9`（R9）、`feat/sw-scope-r10`（R10）、`feat/sw-scope-r11`（R11）、`feat/sw-scope-r12`（R12）、`feat/sw-scope-r13`（R13）、`feat/sw-scope-r14`（R14）、`feat/sw-scope-r15`（R15）、`feat/sw-scope-r16`（R16）、`feat/sw-scope-r17`（R17）
- **状态**：**现行（R1 Scope 基座；R2 建流期 message；R3 open/reject + tg_live；R4 retired 列表；R5 删除 `PendingFin_`；R6 main 发 FIN；R7 main 发 RESET；R8 身份释放迁 main；R9 timer 收缩 + 控制帧全走公共 TX；R10 Close 请求化；R11 `RxConsumed` 位化；R12 `TakeInbound`/`OpenTelegraph` 请求化；R13 `TxReady` 位化；R15 `TgTxClosed` 位化；R16 `TgAttach` 走 inbox、删读事件通道；R17 事件通道整体删除）**
- **相关模块**：`connection::scope_`、`connection::config_`、`connection::main_loop`、
  `connection::session_`、`connection::mux_connection::registry_`

## 1. 这份文档要解决什么

`TrConnAllocCfg` 的每一项原本按“分配用途”划分配置面，但实际落地时出现了两类偏差：

1. 临时对象借用了长期结构池（例如 pending message ring 用 `RegistryAlloc`）；
2. 临时对象完全没有分配器，直接落全局堆（`Vec` / `Box` / `Arc` / `VecDeque` /
   `BTreeSet`）。

本文件按**被分配对象的真实生命周期**，而不是“容器生命周期”重新分类，作为后续
P3.3c / P4 / P5 的清理依据。

## 2. 三分原则

| 归属 | 适用对象 | 机制 | 不适用对象 |
| --- | --- | --- | --- |
| **Scope** | 生命周期短于子流 / Telegraph、创建与销毁都在同一任务或同一循环内、不需要 `Allocator` 接口 | `sw_scope::Scope` + `Retain` / `Owning`；域显式 `close` | 需要 `Send + 'static` 长期持有、需要 `Allocator` / `AllocatorClone` 的第三方结构 |
| **专用池（GAT）** | 长寿命、按类型或类型家族复用、需要 `Allocator` / `AllocatorClone` | `TrConnAllocCfg` 关联类型 | 每请求 / 每帧 / 每扫描的临时对象 |
| **Global（例外）** | 仅限握手期一次性、事件通道满时 `Drop` 路径等无法 `await` 的少数点 | 显式注释 + 分配断言 | 数据面稳态、每请求路径 |

`Scope` 不是 `Allocator`：B+ 树、`mpmc_chan`、`buffex` ring 仍然只能走 GAT；
`Scope` 的页仍由注入的池分配器提供，因此“用 Scope”不等于“完全不碰 Global”。

## 3. 逐点归属表

### 3.1 已迁出 Global 的临时分配（Scope / 注入池 / 栈）

| 分配点 | 对象 | 生命周期 | 当前归属 | 备注 |
| --- | --- | --- | --- | --- |
| `Tcs_::new_` | `TcsInner_<T>` | 一次请求 | 连接级 `Scope` | 跨 API 线程 / main 循环，不能放每循环域 |
| `main_loop` 的公共 RX 整帧暂存 | `[u8]` | 循环 | 连接级 `Scope`（暂留） | 一次性分配；没有锁竞争收益 |
| `mux_loop` 的搬出暂存 | `[u8]` | 循环 | 连接级 `Scope`（暂留） | 一次性分配；没有锁竞争收益 |
| `demux_loop` 的控制帧载荷暂存 | `[u8]` | 循环 | 连接级 `Scope`（暂留） | 一次性分配；没有锁竞争收益 |
| `main_loop` 的建流期 message 缓冲（R2） | `[u8]` | 连接（按请求复用） | 连接级 `Scope` | 一次分配、每请求复用；`read_msg_outgoing_into_` 写入切片 |
| `read_message_async` 的搬运块（R2） | `[u8; 1024]` | 一次读取 | 栈 | 分块直接写 sink，不再分配整条 `Vec` |
| `open_channel_async` / `reject_async` 的 message / reason（R3） | `[u8]` | 一次请求 | 连接级 `Scope` | 容量 `cap + 1`，读满即判超长；不再走全局 `Vec` |
| `demux` 的 `tg_live`（R3） | `DockSet_`（2 KiB B 树） | 每轮复用 | `C::LocalTableAlloc` | 不再用全局 `BTreeSet`；随循环归还 |
| `demux` 的 `gone` 列表（R3） | `Vec<Dock>` | 每轮复用 | `C::LocalTableAlloc` | 同上，容量跨轮复用 |
| `reap_retired_` 的 5 个列表（R4） | `Vec<Dock>` / `Vec<(Dock, Dock)>` | 一次扫描 | `C::RegistryAlloc` | 空列表不分配；非空时用连接结构分配器，不再走全局 |

### 3.2 仍直接走 Global 的临时分配（待清）

| 分配点 | 对象 | 生命周期 | 建议 |
| --- | --- | --- | --- |
| 事件通道 / 邮箱 | `Box<MpmcBuf>`、`Arc<Overflow_>`、`VecDeque` | 连接 / 事件 | P5 删除；不要复活 `EventAlloc` |

> R2 已清掉 `main_loop` 的 `read_msg_outgoing_` 与 `read_message_async` 的全局
> `Vec`，并删除 `ControlFrame_.payload_`。R3 清掉 open / reject 的 message / reason
> 与 demux 的 `BTreeSet`。R4 把 `reap_retired_` 的临时列表从全局 `Vec` 换成连接
> 结构分配器（绑定 + 监听阶段全局 11 → 4，注入 33 → 40）。非空 welcome 仍受
> `accept_async` 的 `TrBuffWrite` 形状限制（P3.3c 待补）。

### 3.3 临时对象借用长期池（待决策）

| 分配点 | 当前分配器 | 问题 | 方向 |
| --- | --- | --- | --- |
| pending message ring（每 pending channel 一对） | `RegistryAlloc`（`inner.alloc_`） | 每条 pending channel 的临时 ring 借结构池；`pending_msg_allocator` 是死方法 | 接上覆盖入口，或新增 `MsgAlloc` / 专用池（公开 trait 变更，需拍板） |
| 请求队列 / `Attach` inbox 的等待 chunk | `MpmcAlloc` / `AttachAlloc` | 容器长期、等待 chunk 是临时对象，但 `mpmc_chan` 要求 `AllocatorClone` | 暂保留；后续可合并 `AttachAlloc` 到 `MpmcAlloc` |
| `LocalTableAlloc` 上的 `PendingFin_` | `LocalTableAlloc` | `PendingFin_` 是子流级临时索引，不是长期结构 | P4 删除 `PendingFin_` 后自然消失 |

### 3.4 应继续保留在 GAT 的长期对象

| 对象 | GAT | 理由 |
| --- | --- | --- |
| 注册表根、四本 B+ 树、`ControlStateInner_`、`VecDeque` | `RegistryAlloc` | 连接级；B+ 树 / 容器要求 `Allocator` |
| 身份节点（`ChannelOwner_` / `DockBinding_` 等） | `ChanOwnerAlloc` | 子流 / 连接级；可做类型池 |
| 请求队列、`Attach` inbox | `MpmcAlloc` / `AttachAlloc` | `mpmc_chan` 要求 `AllocatorClone` |
| 五本本地表 | `LocalTableAlloc` | B+ 树要求 `Allocator` |
| `MuxCore`、取消令牌 | `CoreAlloc` | 连接级 `Shared` / 令牌 |
| 公共子流 ring 与 `PublicChannelInner_` | `PublicAlloc` | ring 要求 `Allocator` |

## 4. 第 1 轮实施记录

1. `connection::scope_` 增加每循环域：
   - `LoopScopeCfg`：页 4 KiB；
   - `LoopScope_`：持有 `Scope`，`Drop` 时显式 `close`；
   - `new_loop_scope_(alloc)`：在 crate 级 root 下建域。
2. 每循环域的页分配器入口（`ChannelRegistry_::loop_scope_alloc_`）仍未接线：R2 的
   main 建流期 message 缓冲只用连接级 Scope 的**一次**分配 + 每请求复用，不需要
   新域；入口留给 R3 的 `reap_retired_` / `tg_live` 等每轮临时对象。
3. **一次性 scratch 不迁**：先按原计划把 `main_loop` / `mux_loop` / `demux_loop` 的
   三块 `[u8]` 暂存迁到各自的 `LoopScope_`，实测（两连接）绑定 + 监听阶段从
   全局 11 / 注入 33 升到全局 17 / 注入 60（注入字节约 +1.06 MiB）。
   一次性分配没有锁竞争收益，新增的域实体 / 池块纯属固定成本，因此回退到连接级
   Scope；每循环基座保留给 R3 的每轮重复分配。
4. `LoopScope_` / `new_loop_scope_` / `loop_scope_alloc_` 当前只由单测使用，生产
   路径暂记 `#[allow(dead_code, reason = "R3 接线")]`；R3 接入 `reap_retired_` /
   `tg_live` 后删除该 allow。
5. 新增单测 `loop_scope_drop_closes_domain_`：守卫析构后 `try_sharing()` 返回
   `None`。
6. 验收：`cargo test -p smux_v1 --lib` **362 passed**（原 361 + 新增 1）；
   `alloc_count` 恢复原基线（全局 11 / 注入 33）。

## 4.1 第 2 轮实施记录（建流期 message 路径）

1. `ControlFrame_` 删除 `payload_: Vec<u8>` 与 `payload_()`，改为 `Copy` 的纯帧头
   结构；`OPEN` / `ACCEPT` / `REJECT` 的载荷由 `write_control_frame_async_` 的
   独立 `&[u8]` 参数携带（T23）。
2. `ChannelRegistry_::read_msg_outgoing_into_` 取代 `read_msg_outgoing_`：把
   outgoing 载荷写进 `main_loop` 的固定缓冲，不再返回 `Vec<u8>`；`dst.len()` 就是
   上限，超长返回 `ReadOutgoingErr_::TooLarge`（映射为 `FrameTooLarge`）。
3. `main_loop` 启动时从连接级 Scope 分配一块 `pending_msg_capacity` 大小的 message
   缓冲，一次分配、每请求复用；`main_step_` / `handle_request_` 增加该缓冲参数。
4. `read_message_async` 改为 1 KiB 栈上分块，逐块 `enqueue_frame_` 写 sink，不再
   先分配整条 `Vec<u8>`。
5. 新增单测 `read_outgoing_into_buffer_rejects_overlong`（正常读回 + 超长拒绝）与
   集成测试 `mux_message_inmem_dual_`（`OPEN` message 非空、welcome 当前为空）。
6. 验收：lib **365 passed**；`inmem_mux` **21 passed**（compio / tokio / smol）；
   `alloc_count` 基线不变（全局 11 / 注入 33）；`smoke_compio` 5 passed。
7. 已知未做：`accept_async` 的 welcome 仍是空载荷（其参数是 `TrBuffWrite`，无法读出
   本地 welcome 源；需要 API 形状决策），留 P3.3c。

## 4.2 第 3 轮实施记录（open / reject / tg_live）

1. `util_::read_available_into_vec_` 换成 `read_available_into_slice_`：调用方给
   `&mut [u8]`，函数只负责填满并返回长度，不再自己分配。
2. `open_channel_async` / `reject_async` 从连接级 Scope 分配 `cap + 1` 字节缓冲；
   读到 `cap + 1` 即判超长（`FrameTooLarge`），避免先读完整条再拒绝。
3. `demux` 的 `tg_live` 从全局 `BTreeSet<Dock>` 换成
   `DockSet_<C::LocalTableAlloc, Dock>`；`gone` 列表从全局 `Vec` 换成
   `Vec<Dock, C::LocalTableAlloc>`，两者都跨轮复用、随循环归还。
4. `session_` 删除 `BTreeSet` 导入。
5. 验收：lib **365 passed**；`inmem_mux` **21 passed**（compio / tokio / smol）；
   `smoke_compio` 5 passed；`alloc_count` 基线不变（全局 11 / 注入 33）。

## 4.3 第 4 轮实施记录（retired 扫描列表）

1. `reap_retired_` 的 5 个临时列表（listener / telegraph / unreserve / reject /
   unbind）改用 `C::RegistryAlloc` 的 `Vec::new_in`；空列表不分配，非空时也不再落
   全局堆。
2. 返回类型改为 `Vec<(Dock, Dock), C::RegistryAlloc>`，main 侧迭代方式不变。
3. 分配基线变化：绑定 + 监听阶段全局 **11 → 4**、注入 **33 → 40**（同一批分配换了
   归属，字节数基本不变）；`alloc_count` 断言更新为全局 ≤8 / 注入 =40。
4. 验收：lib **365 passed**；`inmem_mux` **21 passed**；`alloc_count` 1 passed；
   clippy 全目标干净。

## 4.4 第 5 轮实施记录（删除 `PendingFin_`）

1. 删除 `PendingFin_`（`LocalTableAlloc` 上的 B 树）及其全部参数 / 扫描函数
   `scan_tx_closed_`：半关闭的 `FIN` 重试不再维护独立集合。
2. `drain_once_` 每轮扫本地写表时，对
   `APP_TX_CLOSED && !LOCAL_FIN_SENT` 的条目直接调用 `finalize_entry_`；环里还有数据 /
   额度不足 / 段借不出来时返回 `false`，下一轮自然重试，不会饿死后面的条目。
3. 删除对应的 `tx_closed_handled_` loop-probe 计数；`finalize_entry_` / `scan_peer_closed_`
   / `scan_write_released_` 去掉 `pending_fin` 参数。
4. 验收：lib **365 passed**；`inmem_mux` **21 passed**；`keepalive_compio` 5；
   `smoke_compio` 5；`cross_thread_compio` 1；`alloc_count` 1（建流阶段全局 1→0，
   拆流阶段注入 6→4，绑定 + 监听基线不变）。
5. **未做**：`FIN` 仍由复用循环直接写 `ConnTx`，`TX_DRAINED` 位与「main 发 FIN / T1」
   尚未迁移；公开 `close_async` 的「FIN 已上线」语义仍依赖现有直接写路径。

## 4.5 第 6 轮实施记录（main 发 FIN / `TX_DRAINED`）

1. `ChannelState_` 增加 `TX_DRAINED`（bit 19）与 `FIN_ENQUEUED`（bit 20）。
2. `drain_once_` 不再直接发 FIN：发送环明确排空后置 `TX_DRAINED` 并 raise
   `HINT_MAIN_TX_DRAINED`；`finalize_entry_` / `flush_entry_` 删除。
3. main 新增 `flush_tx_drained_`：扫描 `TX_DRAINED` 子流，把 `CLOSE(FIN)` 写进公共
   TX ring，置 `FIN_ENQUEUED` 并 raise `HINT_W_FIN_ENQUEUED`。
4. mux 在公共 TX ring 刷空后调用 `scan_fin_enqueued_`：置 `LOCAL_FIN_SENT`、关发送环
   消费端、唤醒 `close_async` 等待者，并走 `maybe_release_`。若 ring 未空则把
   `HINT_W_FIN_ENQUEUED` 留给下一轮。
5. `MuxLoopShared_` 增加 `ControlState_` 句柄（写循环可 raise `HINT_MAIN_*`），构造器
   与 `Clone` 同步。
6. 修正 RESET 越序：`flush_rx_closed_` 在 `APP_TX_CLOSED && !LOCAL_FIN_SENT` 时先不发
   RESET，等 FIN 确认上线后再发；否则对端先看到 RESET、后看到 FIN，接收方向会少一次
   `peer_fin` 释放触发点（`mux_graceful_close_dual_` 曾因此超时）。
7. 验收：lib **365**；`inmem_mux` **21**（compio / tokio / smol）；`keepalive_compio` 5；
   `smoke_compio` 5；`cross_thread_compio` 1；`alloc_count` 1（绑定 + 监听基线不变；
   拆流阶段注入 4→14、全局仍 0，新增来自 FIN 扫描 / 公共 TX 路径，后续可再收）；
   clippy / doc / format 干净。
8. **T1 仍未完全完成**：mux 不再发 FIN，但仍负责 `CLOSE(RESET)`（`flush_rx_closed_`）
   与 `maybe_release_` 本地表动作；把 RESET 也迁到 main 需要另一次收尾状态机迁移。

## 4.6 第 7 轮实施记录（main 发 RESET）

1. `ChannelState_` 增加 `RESET_ENQUEUED`；新增 `HINT_MAIN_RESET` 与
   `HINT_W_RESET_ENQUEUED`。
2. `ChannelRx::seal_rx_` 不再 raise 写循环提示，改为 raise `HINT_MAIN_RESET`；
   `handle_write_attach_` 补查 `APP_RX_CLOSED` 时同样 raise main。
3. `scan_reset_pending_` 从注册表扫描 `APP_RX_CLOSED && !LOCAL_RESET_FLUSHED &&
   !RESET_ENQUEUED`；若本端发送方向已封口，则等到 `LOCAL_FIN_SENT` 之后才返回，
   保证 RESET 排在 FIN 之后。
4. main `flush_reset_` 把 `CLOSE(RESET)` 写进公共 TX ring 并置 `RESET_ENQUEUED`；
   mux 在公共 TX 刷空后 `scan_reset_enqueued_` 置 `LOCAL_RESET_FLUSHED`、唤醒
   `ChannelRx::close_async` 并尝试释放身份。
5. `scan_fin_enqueued_` 在 FIN 上线后 raise `HINT_MAIN_RESET`，唤醒此前被 FIN
   顺序挡住的 RESET 扫描。
6. 删除 `flush_rx_closed_`、`HINT_W_RX_CLOSED`、`control_close_via_`；mux 不再直接
   写 FIN / RESET。
7. 验收：lib **365**；`inmem_mux` **21**（compio / tokio / smol）；`keepalive_compio` 5；
   `smoke_compio` 5；`cross_thread_compio` 1；`alloc_count` 1；clippy / doc / format
   干净。
8. **T1 剩余**：mux 仍通过 `maybe_release_` 做本地表释放，并仍处理 timer 的
   `WriteEvent_::Control`（PULSE 等）；这两块留 R8。

## 4.7 第 8 轮实施记录（身份释放迁到 main）

1. `ControlState_` 新增 `HINT_MAIN_RELEASE`（bit 3）：写循环完成本地动作后只 raise 它，
   不再自己取注册表写锁做身份释放。
2. `ChannelRegistry_::release_done_channels_`：在**同一把写锁**内遍历 `bindings_`，
   对每条 `Channel` 记录尝试 [`ChannelState_::claim_release_`] 的一次性 CAS，成功者
   当场转成 `WaitClose` 宽限态；返回 `(释放列表, 活跃子流是否归零)`。认领与转宽限态
   同锁完成——分两次取锁时「CAS 成功、释放失败」会把子流永久卡在活跃表里
   （`claim_release_` 不会再成功，也没有别的路径替它转宽限态）。
3. 释放扫描**跳过**「应用已关读、但 `LOCAL_RESET_FLUSHED` 尚未置位」的子流：`RESET`
   帧由 main 写进公共 TX ring，写循环刷空后才置位；提前释放会让
   `scan_reset_pending_` 再也看不到该子流，`CLOSE(RESET)` 永久丢失。
4. `main_loop::flush_releasable_` 承接原 `maybe_release_` 的全部副作用：
   `HINT_R_RELEASE` / `HINT_W_RELEASED` 唤醒两侧循环、`notify_both_progress_` 唤醒
   应用等待者、`Fin` 的 `on_channel_closed` **恰好一次**上报，以及
   `became_idle && is_closing` 时的连接收尾（`cancel_loops_`）。
5. 删除 `session_::maybe_release_` 与只被它使用的 `lock_or_fail_` 宏；四个扫描
   （`scan_peer_closed_` / `scan_write_released_` / `scan_fin_enqueued_` /
   `scan_reset_enqueued_`）降为**同步函数**，只做本地表 / 环半部动作 + 置位 + raise
   提示。四个扫描里「表项已摘、owner 仍在注册表」的兜底路径由 main 的注册表全表扫描
   自然覆盖，不再需要按 pair 反查 `channel_owner_`。
6. `mux_loop_async_` 去掉读事件通道参数（`read_events`）：写循环不再向读循环投事件，
   启动处（`mux_connection::conn_`）同步少传一份 `EventSender_`。
7. 新增两条单测：`step_releases_done_channel_`（main 释放、二次 `claim_release_` 必须
   失败）、`step_waits_for_reset_before_release_`（欠 RESET 时两轮之间不得提前释放）。
8. 分配归属：释放扫描的返回列表用 `C::RegistryAlloc`，空列表不分配；只有**确实有
   释放**时才产生一次 `Vec` 分配。实测拆流阶段注入 **14 → 24**（8 条子流各一批），
   全局仍为 **0**；绑定 + 监听（全局 4 / 注入 40）、建流（全局 0）与稳态搬运
   （注入 0）不变。`tests/alloc_count.rs` 的拆流阶段只打印、不设上限断言（次数随
   「一批释放几条」变化，不是结构性不变量），已在注释里写明原因。
9. **T1 仍未完全完成**：mux 仍处理 timer 的 `WriteEvent_::Control`（`PULSE`、空闲
   拆流的 `FIN` / `RESET`、建流超时的 `REJECT`），也仍在每轮调用
   `drain_session_events_`（`SessionMailbox_` 的投递不带唤醒，需要和事件通道一起在
   R9/P5 清理）。timer 控制帧走 main 属于 R9 的 timer 收缩（T4）。
10. 验收：lib **367**（原 365 + 新增 2）；`inmem_mux` **21**（compio / tokio / smol）；
    `keepalive_compio` 5；`smoke_compio` 5；`cross_thread_compio` 1；`alloc_count` 1；
    doc 12；clippy 干净。

## 4.8 第 9 轮实施记录（timer 收缩 + 控制帧全走公共 TX）

1. **`timer_loop` 收缩为纯计时**：只读 [`TimerSignal_`]、`delay`、到期
   `raise_main_hint_(HINT_MAIN_TIMER)`；不再 `timer_scan_`、不再组帧、不再发协议动作。
   `TimerSignal_` 从 `timer_.rs` 的 dead code 挪进 `ControlState_`（main / timer 各持一句柄）。
2. **定时决策迁 `main_loop::tick_`**：`TimerAction_`（`Pulse` / `Abort` /
   `RejectEstablish` / `Finalize`）与连接级关闭动作都由 main 执行；产生的控制帧进
   **公共 TX ring**（FIFO：`CLOSE(FIN)` 必在 `CLOSE(RESET)` 之前）。公共 TX 满时
   不可重建的帧留在 `MainTickState_` 的定容队列里，下一轮补写（不 park、不丢）。
3. **窗口通告回程归 main**：`demux_loop` 只做 `recheck_due_`（记账 + 判定）并 raise
   `HINT_MAIN_WINDOW_DIRTY`；`main_loop` 先确认公共 TX 有空间，再用**栈上定长缓冲**
   调 `take_window_reports_` 取快照并组 `WINDOW_UPDATE`（零分配；空间不足不取，快照
   留在窗口里）。
4. **未裁决放弃改 retired 位**：`ChannelState_` 新增 `LOCAL_OPEN_SENT`；
   `abort_pending_` / `reject_after_open_` 只置 `HANDLE_RETIRED` + raise
   `HINT_MAIN_RETIRED`，由 `reap_retired_` 按「对端是否知道这条子流」决定撤销预留还是
   回 `REJECT` + 宽限。`accept_prepare_` 的 ring 建失败不再先投 `ReleaseChannel`
   （否则身份变 `WaitClose`，`REJECT` 永远发不出去——`mux_ring_rejected_dual_` 实测撞到）。
5. **删除 `WriteEvent_::Control` / `write_control_blocking_`**：控制帧不再经事件通道、
   不再由复用循环直写 `ConnTx`。
6. **删除会话释放邮箱**：`SessionEvent_` / `SessionMailbox_` / `MailboxOverflow_` /
   `post_session_event_` / `drain_session_events_` 全部删除。`MuxCore::notify_timer_`
   改为 raise `HINT_MAIN_TIMER`（timer 不再被唤醒重算）。至此 `mux_loop` / `demux_loop`
   都不再调用任何身份表写入口，`mux_loop` 也不再处理控制帧。
7. 顺带清理死代码：`RegistryShared_::poll_timer_`、`poll_timer_wake_`、
   `ChannelState_::recv_recheck_`、`RecvWindow::recheck_`、`timer_exit_*` probe；
   `TimerSignal_::clear_` 收敛为 `#[cfg(test)]`。
8. 验收：lib **367**；`inmem_mux` **21**（compio / tokio / smol）；`keepalive_compio` 5；
   `smoke_compio` 5；`cross_thread_compio` 1；`alloc_count` 1（稳态搬运注入恢复 **0**，
   窗口通告改用栈上定长缓冲后不再每批分配）；doc 12；clippy / import 格式干净。
9. **T1 完全完成**：`mux_loop` 只做「数据成帧 + 公共 chunk 搬运 + 本地表动作 + 置位」。
   **T4 完成**（timer 只计时）。剩余（R10）：事件通道本身（`TxReady` / `TgReady` /
   `RxConsumed` 仍排队）、`TakeInbound` / `Close` / `OpenTelegraph` 请求化、telegraph
   仍由 API 线程写身份表。

## 4.9 第 10 轮实施记录（`Close` 请求化 + 删 `ctl_.rs`）

1. **`Close` 走请求 + TCS**：`MainLoopRequest_::Close { reply }` 与
   `MainLoopReply_::Closed(bool)`；`MainTickState_` 增加 `close_reply_`（跨轮保存
   等待者）。`main_loop` 在 `tick_` 的连接级关闭动作里**先 `set_` 答复、再
   `cancel_loops_()`**（Q8）：活跃子流归零 ⇒ `Closed(true)`；整体超时 ⇒ `Closed(false)`。
   连接级失败在 `main_step_` 开头直接答 `Closed(false)` 并取消。
2. **`close_async` 改请求 + 完成源**：删掉 `wait_for_finalize_`（它等的是取消令牌，
   分不清「收敛」与「被取消」）；调用方只等一个 TCS 结果，取消时放弃结果并补一次强制
   收尾。连带删除 `MuxCore::opts_` / `MuxCore::enter_waiting_close_` /
   `ChannelRegistry_::is_forced_close_`（都已无使用者）。
3. **补一处实测缺陷**：`flush_releasable_` 在「最后一条子流释放 ⇒ 活跃归零」时直接
   `cancel_loops_`，**没有完成 `Close` 的 TCS** → `mux_graceful_close_dual_` 挂起。
   现在它先 `set_` `Closed(true)` 再取消。
4. 删除 `ctl_.rs`（未编译的死文件）与 `MainLoopRequest_::SendControl`（R9 起控制帧全由
   main 自组，无生产者）。
5. **T8 位化尝试后回退**（记录，不落地）：把 `WriteEvent_::TxReady` /
   `ReadEvent_::RxConsumed` 改成 `HINT_W_TX_READY` / `HINT_R_RX_CONSUMED` 合并提示、
   并删掉两个事件变体后，`mux_closed_ring_spin_dual_`（`Q` 的环在窗口回补后不再被排空）、
   `mux_drop_flush_dual_`、`flow_ctrl_socket_compio_` 三个用例失败；回退到事件通道。
   结论：**下轮位化前先把「窗口回补 → mux 重试」这条因果钉成单测**（当前它同时依赖
   `RxConsumed` 事件与 `TxReady` 事件两条通道，位化时任何一条的唤醒语义变化都会让
   `Q` 永久卡在 0 额度）。
6. 验收：lib **367**；`inmem_mux` **21**（compio / tokio / smol）；`keepalive_compio` 5；
   `smoke_compio` 5；`cross_thread_compio` 1；`alloc_count` 1；doc 12；clippy /
   import 格式干净。

## 4.10 第 11 轮实施记录（`RxConsumed` 位化 + park 丢位修复）

1. **`ReadEvent_::RxConsumed` 位化**：`ChannelRx::notify_consumed_` 去重位成功后只
   raise `HINT_R_RX_CONSUMED`；`demux_loop` 在顶部按全表调用 `rescan_read_`
   （清位 → 读积压 → `recv_recheck_due_` → raise `HINT_MAIN_WINDOW_DIRTY`）。
   `handle_read_event_` 因此只剩 `TgAttach`（签名收窄为 `(event, tg_table)`）。
2. **修复一处真实的丢唤醒缺陷**：`demux_loop` 的 park 里 `ReadWake_::Hint(bits)` 只处理
   `HINT_R_RELEASE`，而 `poll_read_hint_` 是「**取出并返回**」语义——park 取走却不处理
   的位直接丢失。位化 `RxConsumed` 后这等于「应用读了数据、水位永不重算、对端收不到
   `WINDOW_UPDATE`、发送方额度永远不回来」，`mux_closed_ring_spin_dual_` 与
   `flow_ctrl_socket_compio_` 立刻暴露。现在未处理的位**回放到顶部**，新增提示位不会
   再踩这个坑。
3. **`TxReady` 位化尝试后回退**（记录，不落地）：把 `WriteEvent_::TxReady` 也改成
   `HINT_W_TX_READY`、并删掉 `last_ready` / `last_ready_has_segment_`（park 不再等
   具体环）之后，`mux_drop_flush_dual_` 失败：应用写满 14 KiB 后**立即** drop，
   `raise_write_hint_` 之后 mux 没有下一轮（它停在某个等待源上，write hint 打不断）。
   回退到事件通道。结论：**下一轮位化前先查清 mux 在 ConnTx 背压 / 公共 chunk park
   时的唤醒源**（write hint 只覆盖「park 等提示」这一种形态）。
4. 验收：lib **367**；`inmem_mux` **21**（compio / tokio / smol）；`keepalive_compio` 5；
   `smoke_compio` 5；`cross_thread_compio` 1；`alloc_count` 1；doc 12；clippy /
   import 格式干净。

## 4.11 第 12 轮实施记录（`TakeInbound` / `OpenTelegraph` 请求化）

1. **`TakeInbound` 请求**：`MainLoopRequest_::TakeInbound { local_dock, reply }` +
   `MainLoopReply_::Inbound(Option<(Dock, owner, msg_)>)`。`income_async` 改为
   「提交请求 + 等 TCS + 无请求时等 listener 通知槽」，不再直接调注册表。
2. **放弃答复的回滚**：`take_pending_inbound_` 会把表槽推进到 `HandedOut` 并取走
   app 侧 message ring 两半。`set_` 在 `Abandoned` 上会 drop 结果（那对 ring 等于丢失、
   对端会一直等 `ACCEPT`），因此 main 在 `set_` **之前**检查 `is_abandoned_`，并新增
   `ChannelRegistry_::restore_pending_inbound_` 把状态与两半原样放回。
3. **`OpenTelegraph` 请求**：`MainLoopRequest_::OpenTelegraph { local_dock, reader_,
   writer_, reply }` + `MainLoopReply_::Telegraph(owner)`。main 先 `reserve_telegraph_`
   登记身份，再投两条 `TgAttach`（带环半部、不可位化），最后答复。
   `MuxShared_` 因此多持一份 `w_events_` / `r_events_` 克隆（只服务 telegraph，
   R13 给它建 attach inbox 后删除）；`MuxCore` 的 `r_events_` / `reserve_telegraph_` /
   `take_pending_inbound_` 转发方法删除。
4. **I3 收口**：`income_async` 与 `open_telegraph_async` 都不再直接写身份表；除循环与
   main 之外，身份表写入口只剩 `MuxCore` 的少量转发（`release_channel_` 等）。
5. 验收：lib **367**；`inmem_mux` **21**（compio / tokio / smol）；`keepalive_compio` 5；
   `smoke_compio` 5；`cross_thread_compio` 1；`alloc_count` 1；doc 12；clippy /
   import 格式干净。

## 4.12 第 13 轮实施记录（`TxReady` 位化）

1. `WriteEvent_::TxReady` 删除，改 `HINT_W_TX_READY`：`ChannelTx` 的写入路径在
   `mark_tx_queued_` 第一次成功时 raise 合并提示（去重位保证「不重复唤醒」，位本身
   保证「不丢」）；`demux_loop` 收到 `WINDOW_UPDATE` / `PULSE` 时也 raise 它（窗口回补
   必须能叫醒写循环重试）。
2. `demux_loop` 去掉 `events_tx` 参数（TxReady 是它最后一个写事件使用者）；写循环
   的 `handle_write_event_` 只剩 telegraph 三项。
3. **保留 `last_ready` 判据**（`last_ready` 现在恒为 `None`，该分支不再触发）：
   R11 曾把它连同 park 的环等待一起删掉并在 `mux_drop_flush_dual_` 上失败；本轮
   只删事件、不动 park 结构，全部用例通过。等 telegraph 位化、`WriteEvent_` 整体删除
   时再一并清掉这段死判据。
4. 验收：lib **367**；`inmem_mux` **21**（compio / tokio / smol）；`keepalive_compio` 5；
   `smoke_compio` 5；`cross_thread_compio` 1；`alloc_count` 1；doc 12；clippy /
   import 格式干净。

## 4.13 第 14 轮记录（`Tg*` 位化尝试后回退）

1. **尝试**：把 `WriteEvent_::TgReady` / `WriteEvent_::TgTxClosed` 改成
   `HINT_W_TG_READY` / `HINT_W_TG_TX_CLOSED`（`TgRec_` 加 `tx_closed_` 位，
   `scan_tg_tx_closed_` 扫本地 tg 表）。
2. **结果**：`mux_telegraph_inmem_dual_` 挂起。诊断输出（`--nocapture` + 临时
   `eprintln`）显示：应用三次 `tg_ready raise` 之后，写循环只跑了**一轮**
   （`tg round local=7`），随后不再有 `local=11` 的轮次——即卡在
   `flush_tg_endpoint_(7)` 内。
3. **定位**：`flush_tg_endpoint_` → `send_one_datagram_` → `write_frame_to_stage_`
   → `enqueue_frame_` 在传输写半边满时 **park**。旧实现里 `TgReady` 事件除了叫醒
   写循环，还保证「应用每次提交都会再来一条唤醒」；位化后这条唤醒变成合并提示，
   一旦写循环停在 `enqueue_frame_` 的背压上，提示位既不能打断该等待、也不会留下
   「稍后重试」的队列元素，报文从此不动。
4. **回退**：两处 `Tg*` 事件保留，`TgRec_` 的 `tx_closed_` 位一并撤销；工作区回到
   R13 的状态。
5. **下一轮的前置**：先把「写循环在 `enqueue_frame_` 背压时如何被唤醒」钉住——要么
   给传输写半边的排空方加一条 `raise_write_hint`，要么让 tg / 数据帧的 flush 在
   `enqueue_frame_` 前用「非阻塞写 + 保留队头」代替 park。没有这条之前，`Tg*` 位化
   会重复出现同一挂起。

## 4.14 第 15 轮实施记录（`TgTxClosed` 位化）

1. `WriteEvent_::TgTxClosed` 删除，改 `HINT_W_TG_TX_CLOSED`：`TgRec_` 增加
   `tx_closed_` 位，`Sender::drop` 只置位 + raise；写循环在 hints 处理里
   `scan_tg_tx_closed_` 扫本地 tg 表摘除并 `out_().clear_()`。
2. **`TgReady` 保持事件**（位化尝试继续失败，见 §4.13）：它是「每次提交一条唤醒」
   的一次性语义，改成合并提示后 `mux_telegraph_inmem_dual_` 仍挂起；加「排空后仍有
   未发报文就重新 raise」的自保持也没解决，说明阻塞点不在重试次数。已回退。
3. 验收：lib **367**；`inmem_mux` **21**（compio / tokio / smol）；`keepalive_compio` 5；
   `smoke_compio` 5；`cross_thread_compio` 1；`alloc_count` 1；doc 12；clippy /
   import 格式干净。

## 4.15 第 16 轮实施记录（`TgAttach` 走 attach inbox，删除读事件通道）

1. `WriteAttach_` / `ReadAttach_` 由 struct 改为**枚举**：`Channel` 变体沿用原字段，
   新增 `Telegraph` 变体（键只有 `local_dock`）。channel 与 telegraph 因此共用同一条
   attach inbox——不需要再加两对通道与八个类型别名。
2. `MuxLoopShared` 的 `OpenTelegraph` 处理改投 inbox（满时用
   `push_*_attach_async_` 背压等待），不再投 `TgAttach` 事件。
3. 删除 `WriteEvent_::TgAttach` / `ReadEvent_::TgAttach`；`ReadEvent_` 变空后**整条
   读事件通道删除**：`DemuxEventsGuard_`、`drain_read_events_`、`handle_read_event_`、
   `park_read_wake_` 的 `events` 参数与 `ReadWake_::Event` / `Rescan` 变体、
   `CoreBundle_::r_receiver_`、`MuxShared_` 的 `r_events_` 全部移除。
4. `WriteEvent_` 去掉 `C` 泛型（只剩 `TgReady { local_dock }`）。
5. **保留 `MuxLoopShared_::w_events_` 作为纯保活句柄**：核心析构只发起优雅关闭，循环
   要继续把已提交字节发完；少了这份生产端克隆，事件通道在核心析构时即关闭，写循环
   会在 `EventTake_::Closed` 处立刻退出——`mux_drop_flush_dual_` 实测复现（删掉后
   稳定失败，恢复后通过）。R17 删整条通道时这条保活语义要由「核心析构 → raise
   `HINT_MAIN_TIMER` → main tick → cancel」接替。
6. 验收：lib **367**；`inmem_mux` **21**（compio / tokio / smol）；`keepalive_compio` 5；
   `smoke_compio` 5；`cross_thread_compio` 1；`alloc_count` 1；doc 12；clippy /
   import 格式干净。

## 4.16 第 17 轮实施记录（事件通道整体删除）

1. `TgReady` 改走 **attach inbox 的零 payload 信号**（`WriteAttach_::TgReady`）：
   `Sender::send_async` 只 `try_push` 一条标记，inbox 满时丢弃（可重建，写循环每轮
   都会扫所有端点）；`handle_write_attach_` 收到标记后当场排空该端点。
   **为什么不用合并提示**：R14/R15 两次把它改成 `HINT_W_TG_READY` 都让
   `mux_telegraph_inmem_dual_` 挂起——合并位只保证「醒一次」，而队列语义保证「写环
   刚才满、随后有空」时还能被再叫醒一次。这是本轮唯一改变结论的地方。
2. 删除 `WriteEvent_` / `ReadEvent_` 两个枚举与**整条事件通道**：
   `EventSender_` / `EventReceiver_` / `Overflow_` / `EventTake_` /
   `TrEventReceiver_` / `event_channel_` / `EVENT_CHAN_CAP` / `EventTx_` / `EventRx_`，
   以及 `MuxEventsGuard_`、`mux_loop` 的 `events` 参数、`MuxShared_::w_events_`、
   `MuxCore::w_events_`、`CoreBundle_::w_receiver_`。
3. **保活语义转移**：R16 发现 `MuxShared_::w_events_` 是「核心析构后循环仍继续排空」
   的隐性保障；通道删除后改由「`MuxCore::Drop` → raise `HINT_MAIN_TIMER` → main tick
   → `cancel_loops_`」接替，`mux_drop_flush_dual_` 通过。
4. 删除 13 个只测事件通道的单测（lib 367 → **354**）与四个通道相关 probe。
5. 验收：lib **354**；`inmem_mux` **21**（compio / tokio / smol）；`keepalive_compio` 5；
   `smoke_compio` 5；`cross_thread_compio` 1；`alloc_count` 1；doc 12；clippy /
   import 格式干净。

## 5. 后续轮次与待拍板

| 轮次 | 内容 |
| --- | --- |
| R2（已完成） | `main_loop` 读 outgoing 用固定 Scope 缓冲；删除 `ControlFrame_.payload_`；`read_message_async` 栈上分块；补 `OPEN` message 集成测试 |
| R3（已完成） | open / reject 的 message / reason 用 Scope 有界读取；`tg_live` / `gone` 去 Global |
| R4（已完成） | `reap_retired_` 的 5 个列表改走连接结构分配器 |
| R5（已完成） | 删除 `PendingFin_`；FIN 重试先折进 `drain_once_` 的表扫描 |
| R6（已完成） | `TX_DRAINED` / `FIN_ENQUEUED`；main 发 `CLOSE(FIN)`；mux 在公共 TX 刷空后确认上线 |
| R7（已完成） | 把 `CLOSE(RESET)` 迁到 main；删除 `flush_rx_closed_` / `HINT_W_RX_CLOSED` / `control_close_via_` |
| R8（已完成） | 身份释放（`claim_release_` / `release_channel_` / 关闭上报 / 连接收尾）迁到 `main_loop::flush_releasable_`；mux 只置位 + raise `HINT_MAIN_RELEASE` |
| R9（已完成） | timer 收缩为纯计时；`PULSE` / 空闲拆流 `FIN`+`RESET` / 建流超时 `REJECT` / `WINDOW_UPDATE` 全部由 main 写公共 TX；删会话邮箱；`mux` 不再写身份表、不再处理控制帧 |
| R10（已完成） | `Close` 请求 + TCS（Q8：先答 TCS 再取消）；删 `wait_for_finalize_` / `ctl_.rs` / `SendControl`；T8 位化尝试后回退 |
| R11（已完成） | `RxConsumed` 位化（`HINT_R_RX_CONSUMED`）；修 demux park 丢位；`TxReady` 位化尝试后回退 |
| R12（已完成） | `TakeInbound` / `OpenTelegraph` 请求化；`income_async` / `open_telegraph_async` 不再直接写身份表 |
| R13（已完成） | `TxReady` 位化（`HINT_W_TX_READY`）；`demux` 不再持写事件生产端 |
| R14（未落地） | `Tg*` 位化尝试后回退：写循环在 `enqueue_frame_` 背压时无法被合并提示唤醒（见 §4.13） |
| R15（已完成） | `TgTxClosed` 位化（`TgRec_::tx_closed_` + `scan_tg_tx_closed_`）；`TgReady` 仍走事件通道 |
| R16（已完成） | `TgAttach` 走 attach inbox（`WriteAttach_`/`ReadAttach_` 改枚举）；删除整条读事件通道 |
| R17（已完成） | `TgReady` 走 inbox 信号；删除 `WriteEvent_`/`ReadEvent_` 与整条事件通道；T8 完成 |
| R10（配置收敛） | `pending_msg_allocator` 接线或删除；评估 `AttachAlloc` 并入 `MpmcAlloc`；池粒度定稿 |

待拍板：

1. **池粒度**：按具体类型、按 size class、还是按生命周期域？当前 `RegistryAlloc` /
   `LocalTableAlloc` 都服务多种类型，若坚持“单一数据类型”，需要进一步拆。
2. **`pending_msg_allocator`**：接上（无需公开 API 变更）还是新增 `MsgAlloc`
   （公开 trait 变更，需团队同意）。
3. **每循环域的页分配器**：当前用 `RegistryAlloc`；是否引入独立的 `TempAlloc` /
   region allocator，让循环域的页也脱离结构池。
4. **API 侧临时缓冲**：open / reject / `read_message_async` 是走连接级 Scope，还是
   改成流式读写；后者收益更大但改动面也更大。
5. **非空 welcome**：`accept_async` 的 `welcome: &mut W`（`TrBuffWrite`）是 sink，
   不能作为本地 welcome 的源；需要决定改签名（公开 API）还是另加入口，P3.3c 再定。
6. **事件通道**：其满时兜底可能在任意线程 `Drop` 中发生，不能 `await`；在 P5 删除前
   不要试图把它迁入 Scope。
