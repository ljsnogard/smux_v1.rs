# 临时分配归属：Global / 专用池 / Scope 的三分（20261029-0230）

- **分支**：`feat/sw-scope-r1`（从 `feat/sw-scope` @ `1a9ba52` 开出）
- **状态**：**现行（第 1 轮已实施：每循环 Scope 基座；一次性 scratch 经测量后暂留连接级域）**
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

### 3.1 已经迁入 Scope

| 分配点 | 对象 | 生命周期 | 当前归属 | 备注 |
| --- | --- | --- | --- | --- |
| `Tcs_::new_` | `TcsInner_<T>` | 一次请求 | 连接级 `Scope` | 跨 API 线程 / main 循环，不能放每循环域 |
| `main_loop` 的公共 RX 整帧暂存 | `[u8]` | 循环 | 连接级 `Scope`（暂留） | 一次性分配；R2 起按重复分配迁移 |
| `mux_loop` 的搬出暂存 | `[u8]` | 循环 | 连接级 `Scope`（暂留） | 一次性分配；R2 起按重复分配迁移 |
| `demux_loop` 的控制帧载荷暂存 | `[u8]` | 循环 | 连接级 `Scope`（暂留） | 一次性分配；R2 起按重复分配迁移 |

### 3.2 仍直接走 Global 的临时分配（待清）

| 分配点 | 对象 | 生命周期 | 建议 |
| --- | --- | --- | --- |
| `open_channel_async` / `reject_async` 的 `read_available_into_vec_` | `Vec<u8>` | 一次请求 | 有界读取 / 连接 Scope / 流式写 ring |
| `main_loop` 的 `read_msg_outgoing_` | `Vec<u8>` | 一次请求 | main 的 `LoopScope_` 缓冲 + 切片 |
| `read_message_async` | `Vec<u8>` | 一次读取 | 直接分块写 sink；或每循环 Scope |
| `ControlFrame_.payload_` | `Vec<u8>` | 一帧 | 删除字段，改切片（T23） |
| `reap_retired_` 的 5 个列表 | `Vec<Dock>` / `Vec<(Dock, Dock)>` | 一次扫描 | 固定内联 / 回调 / `DockSet_` |
| `demux` 的 `tg_live` | `BTreeSet<Dock>` | 每轮复用 | `DockSet_<C::LocalTableAlloc>` 或 demux Scope |
| 事件通道 / 邮箱 | `Box<MpmcBuf>`、`Arc<Overflow_>`、`VecDeque` | 连接 / 事件 | P5 删除；不要复活 `EventAlloc` |
| `util_` 的握手读取 | `Vec<u8>` | 握手 | 一次性冷路径，可保留为例外 |

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
2. 每循环域的页分配器入口（`ChannelRegistry_::loop_scope_alloc_`）留到 R2：只有
   循环真正开始做**重复性**临时分配时才需要取它；当前不接线，避免死代码和空转。
3. **一次性 scratch 不迁**：先按原计划把 `main_loop` / `mux_loop` / `demux_loop` 的
   三块 `[u8]` 暂存迁到各自的 `LoopScope_`，实测（两连接）绑定 + 监听阶段从
   全局 11 / 注入 33 升到全局 17 / 注入 60（注入字节约 +1.06 MiB）。
   一次性分配没有锁竞争收益，新增的域实体 / 池块纯属固定成本，因此回退到连接级
   Scope；每循环基座保留给 R2 的每请求 / 每轮重复分配。
4. `LoopScope_` / `new_loop_scope_` 当前只由单测使用，生产路径暂记
   `#[allow(dead_code, reason = "R2 接线")]`；R2 接入 main 的 message 缓冲后删除该
   allow。
5. 新增单测 `loop_scope_drop_closes_domain_`：守卫析构后 `try_sharing()` 返回
   `None`。
6. 验收：`cargo test -p smux_v1 --lib` **362 passed**（原 361 + 新增 1）；
   `alloc_count` 恢复原基线（全局 11 / 注入 33）。

## 5. 后续轮次与待拍板

| 轮次 | 内容 |
| --- | --- |
| R2（P3.3c） | open / reject / `read_message_async` 改为有界或流式；main 读 outgoing 用每循环 Scope；删除 `ControlFrame_.payload_`；补超长错误语义 |
| R3（P4 尾） | `PendingFin_` → `TX_DRAINED`；`reap_retired_` 列表与 `tg_live` 去 Global；T1 让 mux 只搬数据 |
| R4（P5） | 删除事件通道 / 邮箱 / `signal_.rs` 大部分；timer 收缩；全局分配基线重定 |
| R5（配置收敛） | `pending_msg_allocator` 接线或删除；评估 `AttachAlloc` 并入 `MpmcAlloc`；池粒度定稿 |

待拍板：

1. **池粒度**：按具体类型、按 size class、还是按生命周期域？当前 `RegistryAlloc` /
   `LocalTableAlloc` 都服务多种类型，若坚持“单一数据类型”，需要进一步拆。
2. **`pending_msg_allocator`**：接上（无需公开 API 变更）还是新增 `MsgAlloc`
   （公开 trait 变更，需团队同意）。
3. **每循环域的页分配器**：当前用 `RegistryAlloc`；是否引入独立的 `TempAlloc` /
   region allocator，让循环域的页也脱离结构池。
4. **API 侧临时缓冲**：open / reject / `read_message_async` 是走连接级 Scope，还是
   改成流式读写；后者收益更大但改动面也更大。
5. **事件通道**：其满时兜底可能在任意线程 `Drop` 中发生，不能 `await`；在 P5 删除前
   不要试图把它迁入 Scope。
