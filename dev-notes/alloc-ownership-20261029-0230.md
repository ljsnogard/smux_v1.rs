# 临时分配归属：Global / 专用池 / Scope 的三分（20261029-0230）

- **分支**：`feat/sw-scope-r1`（R1）、`feat/sw-scope-r2`（R2）、`feat/sw-scope-r3`（R3）、`feat/sw-scope-r4`（R4）
- **状态**：**现行（R1：每循环 Scope 基座；R2：建流期 message 路径；R3：open/reject 与 tg_live；R4：retired 扫描列表）**
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

## 5. 后续轮次与待拍板

| 轮次 | 内容 |
| --- | --- |
| R2（已完成） | `main_loop` 读 outgoing 用固定 Scope 缓冲；删除 `ControlFrame_.payload_`；`read_message_async` 栈上分块；补 `OPEN` message 集成测试 |
| R3（已完成） | open / reject 的 message / reason 用 Scope 有界读取；`tg_live` / `gone` 去 Global |
| R4（已完成） | `reap_retired_` 的 5 个列表改走连接结构分配器 |
| R5（P4 尾） | `PendingFin_` → `TX_DRAINED`；T1 让 mux 只搬数据 |
| R6（P5） | 删除事件通道 / 邮箱 / `signal_.rs` 大部分；timer 收缩；全局分配基线重定 |
| R7（配置收敛） | `pending_msg_allocator` 接线或删除；评估 `AttachAlloc` 并入 `MpmcAlloc`；池粒度定稿 |

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
