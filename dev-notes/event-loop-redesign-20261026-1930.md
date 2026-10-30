# 事件循环重构方案：三业务循环 + 计时服务 + 公共报文子流 + 主循环请求通道（20261026-1930）

- 分支：`feat/mpmc-chan`
- 基线提交：`abae4ee`
- 工作区提醒：`smux_v1/src/connection/slot_.rs` 有未提交改动；按项目纪律，实施前应先把
  该文件定稿或暂存，再开始本文的重构。本文引用的是该文件的当前形状。
- 状态：**设计稿，已通过评审（20261026）；实施中（P0 已完成，见 §17）**。
  Q1–Q16 均按 §15.2 的推荐值采纳；公开 API 变更已获团队同意。
- 相关：`session_` / `signal_` / `slot_` / `owner_` / `registry_` / `timer_` / `ctl_` /
  `channel_half` / `channel_handle` / `channel_listener` / `telegraph`
- 前序：
  - [`connection-delivery-20261026-1845.md`](connection-delivery-20261026-1845.md)（交付矩阵底稿）
  - [`event-backpressure-20261026-0514.md`](event-backpressure-20261026-0514.md)（位化与内联队列）
  - [`no-vec-audit-20261024-0008.md`](no-vec-audit-20261024-0008.md)（`Vec` 审计原则）

> 本文回答五件事：**循环怎么分**、**公共报文子流长什么样**、**建流期 message 放哪**、
> **请求如何异步答复**、**哪些堆容器 / 索引结构必须替换**。所有容量都是连接建立时一次性
> 分配的常备簿记；算法执行过程中不再产生 `Vec` / `VecDeque` / 无界暂存 / 按需
> `Vec<Waker>`。

---

## 0. 结论摘要

1. **三个业务循环 + 一个计时服务**：`demux_loop`（收）、`mux_loop`（发）、
   `main_loop`（控制面）；`timer_loop` **保留**为独立计时服务，负责把“到期”注入
   `main_loop`（`HINT_MAIN_TIMER`），不直接做协议决策。`ctl_` 删除。取消令牌数组保持
   4 个（0 demux / 1 mux / 2 timer / 3 main）。
2. **公共报文子流**：连接内部的第 0 号子流，**不进入身份表、不占 dock、不对用户公开**。
   它不发明新的 buffer 类型：内部就是两条普通 ring（与用户子流同一种
   `BufferedTx` / `BufferedRx`），ring 内用 **chunked 内部协议**
   `[u16 大端长度][该长度的字节]` 把“一个完整控制帧”排成 FIFO。`main_loop` 写 TX
   ring，`mux_loop` 按长度把帧搬进 `ConnTx`；`demux_loop` 把入向控制帧写进 RX ring，
   `main_loop` 读长度、读整帧并处理。`mux_loop` 每轮**先**搬公共子流的 chunk。
3. **建流期 message ring**：每个**未完成建流**的 channel（Listener 产出的入向请求 +
   发起方尚未发 `OPEN`、尚未 `accept` 的子流）自带一对小容量 ring，容量 / 分配器由
   `TrConnCfg`（MuxConfig）决定。`OPEN` message / `ACCEPT` welcome / `REJECT` reason
   都走它，替代 `CtrlCell_` 与 `Vec<u8>`；Listener 本身不共享一条 ring，而是每个
   pending 请求一对。
4. **请求通道**：所有非荷载 API 请求（bind / listen / take-inbound / reserve / open /
   accept / reject / close / telegraph）统一成 `MainRequest_`，经一条定容、内联、可
   背压的队列交给 `main_loop`。每个请求自带一个 **`Tcs_`（TaskCompletionSource）**；
   `main_loop` 把结果 `set_` 进去，调用方 `.await` 取回。
5. **单写者 + 只读初审**：身份表只由 `main_loop` 写；`demux_loop` 对身份表**只读**
   （做帧级与路由级合法性初审），`mux_loop` 不访问身份表；公共子流 TX ring 只由
   `main_loop` 写，RX ring 只由 `demux_loop` 写；`mux_loop` / `demux_loop` 各自独占
   自己的本地表。跨线程的 `Drop` 只置身份节点上的原子位并 `raise` 一个合并提示，
   不再投递任何堆队列。
6. **消失的容器**：`WriteEvent_` / `ReadEvent_` 两条 mpmc 通道、`Overflow_::durable_`
   的无界 `VecDeque`、`SessionMailbox_` 及其溢出 `VecDeque`、`ControlFrame_::payload_`
   的 `Vec`、`ChannelHandle::message_` 的 `Vec`、`read_available_into_vec_`、
   `CtrlCell_` 的定长载荷缓冲、`InlineQueue_::free_waiters_` 的 `Vec<Waker>`、
   `HintSlot_` 死代码。
7. **保留的堆**（连接常备簿记，需在评审时确认）：注册表 / 本地表的 arena 树（§18）、
   `Shared` 根对象、循环 `spawn_local`、每个请求一个 `Tcs_` 的 `Box`（后续接入快放
   快收池）、每个 pending channel 的两条小 message ring。数据面热路径与 `Drop` 路径
   **零分配**。
8. **索引迁移**：registry / dock 相关 `BTreeMap` / `BTreeSet` 改为 `bplus_tree_cmp` 的
   `BPlusTree` / `BTree`；`SlabList` **2 KiB chunk**（本 crate 默认，见 §18）降低长期
   碎片与固定开销。范围查询直接用
   `range` / `range_mut` / `iter_mut`（§18）。任务清单见 §19。

---

## 1. 现状：要消掉的队列与分配

`connection-delivery-20261026-1845.md` §3 已列出交付矩阵。把“为什么必须改”压缩成
六条：

| # | 现状 | 问题 | 本方案落点 |
| --- | --- | --- | --- |
| 1 | `WriteEvent_` / `ReadEvent_` 各 256 容量，混装 `Control` / `TxReady` / `LocalAbort` / `Tg*` | 容量按“并发子流数 × 常数”被吃满；可重建与不可重建混在一起，满时只能走 `Overflow_` | 删除两条通道；一次性控制帧走公共子流 ring 的 chunk，可重建状态只置位 |
| 2 | `Overflow_::durable_`：`SpinningMutexOwned<VecDeque<E>>` | 无界堆增长，与“减少分配”正面冲突 | 删除；公共子流 ring 是定容背压 |
| 3 | `SessionMailbox_`：mpmc 256 + `MailboxOverflow_` 无界 `VecDeque` | `Drop` 路径唯一还能无界增长的地方 | 删除；身份释放位化到身份节点 + `HINT_MAIN_RETIRED` |
| 4 | `ControlFrame_::payload_: Vec<u8>`、`ChannelHandle::message_: Vec<u8>`、`read_available_into_vec_` | 每次 OPEN / ACCEPT / REJECT 一次临时堆分配；`message_` 还要跨 open→accept 保存 | `OPEN` message / `ACCEPT` welcome / `REJECT` reason 走建流期 message ring；控制帧头在公共子流 ring 内组装；`message_` 删除。**注：`CtrlCell_` 在基线提交中已不存在**（前一阶段已删除），这里只保留历史对比 |
| 5 | `InlineQueue_::free_waiters_: SpinningMutexOwned<Vec<Waker>>` | 背压等待者按需堆增长 | 换成定容 `WakerSlots_<N>`；超限返回 `Busy` |
| 6 | `mux_loop` / `demux_loop` 内直接处理建流、窗口、拆流、释放，并 `await` 注册表锁 / 等排空 | 数据循环被业务阻塞；一条路径多种交付方式 | 数据循环只搬字节 + 合法性初审 + 置位；协议决策全部归 `main_loop` |

另外两处结构性问题：

- `ctl_loop_async_` 已经是第 4 个循环，但只等取消（`ctl_.rs` 全文 35 行）；
- `timer_loop_async_` 的 `Pulse` / `Abort` / `Finalize` / `RejectEstablish` 仍要经
  `WriteEvent_::Control`，是“业务逻辑分散在多个循环”的典型；本方案保留计时循环，
  但把它收缩为纯计时服务。

---

## 2. 目标与非目标

### 2.1 目标

- 数据面（`mux` / `demux`）只做四件事：读/写传输、把帧搬进/搬出环、帧级/路由级
  合法性初审（`demux`）、按位执行本地收尾。
- 所有“需要决定、需要答复、需要等对端”的控制面逻辑集中到 `main_loop`。
- 一次性控制帧**不丢、不乱序、不无界堆积**；可重建提示**不排队**。
- 建流期 message 有**每 pending channel 的固定上界**，不因并发建流数共享容量、不再用
  `Vec` 缓存。
- 除连接常备簿记与 `Tcs_` 外，算法执行过程中没有临时堆容器。
- 所有等待都可取消（`gen_mcf2` 的取消令牌链保持）。

### 2.2 非目标

- 不改 `abs_smux` 的 trait 签名与帧的线上格式。
- 不引入新的公开配置项 / 公开错误变体，除非 §15 的待拍板项获得确认。
- 不在本方案里实现 TCS 池；只定义池的接口形状与接入点。
- 不改握手、流控策略、metrics 的既有语义。

---

## 3. 硬不变量

后续所有实现与评审都以这八条为准；任何一条被破坏，方案即失效。

- **I1 热路径零分配**：`demux` 收数据帧、`mux` 发数据帧、`main_loop` 处理控制帧与
  定时 tick，均不得调用任何会分配的连接级容器。索引树的 chunk 分配只允许发生在
  attach / detach 冷路径（`SlabList` 在 chunk 内复用槽位，不逐条分配）。
- **I2 所有队列定容**：容量在连接建立时确定；满时语义必须显式（背压、丢弃可重建、
  或返回错误），不允许“先放溢出暂存以后再说”。
- **I3 单写者 + 只读初审**：
  - 身份表 `bindings_` / `docks_` / `remote_index_` / `wait_close_expiry_` **只由
    `main_loop` 写**；
  - `demux_loop` 对身份表**只读**，用于帧级 / 路由级合法性初审与宽限态判定；
    `mux_loop` 不访问身份表；
  - 公共子流 TX ring 只由 `main_loop` 写；
  - 公共子流 RX ring 只由 `demux_loop` 写；
  - `mux_loop` 的 `WriteTable_`、`demux_loop` 的 `ReadTable_` 各自独占。
- **I4 一次性 vs 可重建**：
  - 一次性：`OPEN` / `ACCEPT` / `REJECT` / `CLOSE(FIN|RESET)` → 公共子流 ring 的
    chunk，FIFO、不可丢；
  - 可重建：`WINDOW_UPDATE` / `PULSE` → owner 状态位 + 快照，`main_loop` 按需重建，
    不占 ring；
  - 高频提示：`TxReady` / `RxConsumed` / `TgReady` → 原子位 + 合并唤醒，不排队。
- **I5 `Drop` 纪律**：不取锁、不阻塞、不 `await`、不分配；只允许一次原子 `store` /
  `fetch_or` 和一次 `NotifySlot_::notify_`。
- **I6 数据面不组控制帧、不做协议决策**：`mux_loop` 不构造控制帧头、不决定控制帧的
  `kind` / `flags` / 窗口（`Data` / `Datagram` 数据帧的成帧仍是它的搬运职责）；
  `demux_loop` **要**做帧级与路由级合法性初审（查本地读表、只读身份表、宽限态），
  但不解释“建流 / 拆流 / 释放”的业务决策，也不写身份表。唯一的窗口快路径见 §8.2。
- **I7 控制优先**：`mux_loop` 每轮先检查公共子流 TX ring 的 chunk；只要有一个 chunk
  可搬，就先搬它，再发用户数据。
- **I8 可取消**：所有公开异步方法继续用 `gen_may_cancel_future` 封装；`main_loop` 的
  请求处理也必须让调用方的取消令牌能生效（取消后协议动作仍需完成，见 §7.3）。
- **I9 建流期 message 有界**：每个 pending channel 的每个方向至多一条 message，容量
  与分配器来自 `TrConnCfg`（§5.10）；`Listener` 不为所有入向请求共享一条 ring；任何
  路径都不得用 `Vec<u8>` 缓存 message。
- **I10 建流期 ring 的所有权**：app 侧两半随 `ChannelHandle`，main 侧两半放进身份表槽
  `ChanSlot_.msg_main_`；建流裁决或 `Drop` 时两侧关闭，身份释放后不保留。
- **I11 索引使用 arena 树**：身份表与 dock 相关表改用 `bplus_tree_cmp` 的
  `BPlusTree` / `BTree`，分配走 `SlabList` 的 **2 KiB chunk**（本 crate 默认）；查询路径不得把整表复制到
  `Vec`，不得为范围扫描临时分配容器。范围需求直接用 `range` / `range_mut` / `iter_mut`，不复制整表
  （§18）。

---

## 4. 目标架构

```text
                 API futures（bind / listen / open / accept / reject / close）
                         │  MainRequest_ + Tcs_（定容队列 + 背压）
                         ▼
┌──────────────────────── main_loop ────────────────────────┐
│  请求队列 ──► 身份表 / 协议状态机 ──► 组帧（公共 TX ring） │
│  公共 RX ring（chunked）──► 处理 OPEN/ACCEPT/REJECT/CLOSE  │
│  retired 位扫描（Drop 的释放请求）                          │
│  关闭推进（finalize / wait-close / graceful close）         │
└───────────┬────────────────────────────────┬──────────────┘
            │ 公共 RX ring（demux 写，main 读）│ 公共 TX ring（main 写，mux 读）
            ▼                                ▼
      demux_loop                          mux_loop
            │                                │
        C::ConnRx                        C::ConnTx
            │                                │
      用户子流接收环 / telegraph 接收环   用户子流发送环 / telegraph 发送环

                timer_loop ── HINT_MAIN_TIMER ──► main_loop
```

### 4.1 三个业务循环 + 计时服务

| 循环 | 只做 | 不做 |
| --- | --- | --- |
| `demux_loop` | 读 `ConnRx`、解析帧头、**帧级 / 路由级合法性初审**（查本地读表、只读身份表、宽限态）、把 DATA 写进子流接收环、把 DATAGRAM 写进 telegraph 接收环、把控制帧写进公共 RX ring 的 chunk、把 `WINDOW_UPDATE` / `PULSE` 的窗口快照写进 owner | 不写身份表、不建流、不释放身份、不组控制帧、不做协议决策 |
| `mux_loop` | 先搬公共 TX ring 的 chunk；再轮转用户子流发送环与 telegraph 发送环；执行 owner 位指示的本地收尾（关环 / 摘表）；排空后置 `TX_DRAINED` | 不组控制帧、不发 FIN/RESET/REJECT、不写身份表、不决定宽限期 |
| `main_loop` | 消费请求队列、消费公共 RX ring 的整帧、写身份表、组所有控制帧、推进保活 / 超时 / 优雅关闭、答复 TCS | 不搬用户数据、不持有 `ConnTx` / `ConnRx`、不持有用户子流的环半部（只持公共子流 TX 写端 / RX 读端） |
| `timer_loop` | 读 `main_loop` 发布的 `next_deadline`，`delay` 到期后 `raise HINT_MAIN_TIMER`；自身不做身份表扫描 | 不消费请求、不写身份表、不组帧、不直接发协议动作 |

### 4.2 所有权

| 对象 | 谁持有 | 说明 |
| --- | --- | --- |
| `ConnTx` | `mux_loop` 独占 | 保持现状（“半部为什么是移交”） |
| `ConnRx` | `demux_loop` 独占 | 同上 |
| 发送环读端 `BufferedRx` | `mux_loop` 本地表 | 经 `mux_inbox` 移交 |
| 接收环写端 `BufferedTx` | `demux_loop` 本地表 | 经 `demux_inbox` 移交 |
| 身份表 / dock 池 / 宽限索引 | `main_loop` 独占写；`demux_loop` 只读 | 保留读写锁：写者唯一、读临界区极短；底层是 `bplus_tree_cmp` 的 arena 树（§18） |
| 公共子流 | `Shared<PublicChannel_>`，三个业务循环持唤醒槽；ring 半部按方向分持 | 常备一次分配 |
| 请求队列 / TCS | `ControlState_` 内联 | 业务循环不直接访问，只有 API 与 main |
| 建流期 message ring（app 侧） | `ChannelHandle`；入向在 `TakeInbound` 前暂存 `ChanSlot_.msg_app_` | 每个 pending handle 一对；Drop 时随句柄释放 |
| 建流期 message ring（main 侧） | `ChanSlot_.msg_main_` | main 独占表槽，零锁；裁决 / retired 时关闭 |
| `MuxCore` | 应用面句柄 | 循环**不**持有；`Drop` 只置关闭位 + raise |

---

## 5. 公共报文子流

### 5.1 定位

- 它是**传输层通道**，不是身份：没有 `local_dock` / `remote_dock`，不进入
  `bindings_`，不参与 `max_channel_count` / `max_dock_chan_count` / dock 复用。
- 它承载的帧**保留原帧的 dock 字段**；公共子流只是“把这些帧排成一条高优先 FIFO”。
- **它不发明新的 buffer 类型**：内部就是两条普通 ring（与用户子流同一种
  `BufferedTx` / `BufferedRx`，由 `new_buffered_channel` 建出）。TX ring 由
  `main_loop` 写、`mux_loop` 读；RX ring 由 `demux_loop` 写、`main_loop` 读。
- ring 内的**内部协议**与 `mptp_rpc` 的 `abs_mptp::messaging::chunked` 同形：
  `[u16 大端长度][该长度的字节]`；长度前缀只属于 ring 内部，**不会写到 `ConnTx`**。
- 对用户不可见：`abs_smux` 的任何 trait 都不出现它；它也不占用协议里的 dock 空间。

### 5.2 chunked 内部协议

```text
public-ring = *chunk
chunk       = chunk-size(2 字节大端 u16) chunk-data
chunk-data  = 一个完整 smux 帧（帧头 + 载荷）
```

- 每个 chunk 承载**一个控制帧**；`chunk-size = 帧总长`，必须同时满足
  `<= K_MAX_CHUNK_PAYLOAD`（`u16::MAX`）与 `<= max_packet_size`。
- 因此公共子流的**单个控制帧**上界是 `min(max_packet_size, 65535)`。默认 4 KiB 远
  低于它；若协商出 >64 KiB 的 `max_packet_size`，控制帧仍受 65535 限制，超过则
  `FrameTooLarge`（数据帧不受这条影响）。
- 公共子流是持续的，**没有终止块**：`0x0000` 不产生；读到 0 按协议错误处理。结束由
  ring 半部关闭表示。
- 与 `ChunkedEncode` / `ChunkedReader` 的关系：复用同一套“长度前缀先于内容、读侧按
  块边界精确搬 N 字节”的用法，但**不引入对 `abs_mptp` 的依赖**，也不要求“未知长度体”
  的流式分块——每个 chunk 就是一条完整的 smux 帧。实现上只需要：
  - 读侧：`Demand::exactly(2)` 读长度，再按 `len` 分批搬（`Demand::exactly` /
    `no_more_than`）；
  - 写侧：动笔前先确认 ring 总空闲 `>= 2 + len`，再同步写长度与内容，避免“前缀已写、
    内容没跟上”的半 chunk。
- 为什么不用“帧槽数组 / 每个槽定长 `max_packet_size`”：那是另一套 buffer 类型，且
  控制帧大小差异很大；ring 本身已经是字节流容器，chunked 只加 2 字节前缀，内存与代码
  都更省。

### 5.3 建环与容量

```rust
// 连接建立时，用与用户子流相同的入口建两条普通 ring。
let cap = K_PUBLIC_RING_MIN.max(2usize * (2usize + max_packet_size));
let (public_tx_w, public_tx_r) = new_buffered_channel(Owned::new_uninit_slice(cap, alloc))?;
let (public_rx_w, public_rx_r) = new_buffered_channel(Owned::new_uninit_slice(cap, alloc))?;
```

- `cap >= 2 + max_packet_size` 是**最低要求**：保证一个最大控制帧的 chunk 能完整写进
  空 ring，写侧不会写到一半才发现放不下。
- 建议 `cap = 2 * (2 + max_packet_size)`：允许“一个 chunk 正在被读、一个 chunk 正在
  被写”，把控制帧的往返延迟与用户数据的发送解耦。默认 4 KiB 帧 ⇒ 每条 ring 约 8 KiB，
  两条 16 KiB。
- TX / RX 各一条；环半部按方向交给 `main_loop` / `mux_loop` / `demux_loop`，
  `PublicChannel_` 只持有**四路唤醒槽**（`tx_ready` main→mux、`tx_space` mux→main、
  `rx_ready` demux→main、`rx_space` main→demux）、关闭位与 `cap` 等元数据，不持有
  环半部（避免同一半部被多个循环同时持有）。
- 四路唤醒都是单等待者，用 `NotifySlot_` 的持久位即可，不需要 `Vec<Waker>`。
- 建环失败按连接建立失败处理（容量非法或分配失败）。

### 5.4 写侧：`main_loop` 与 `demux_loop`

两个写者写的是不同的 ring（main→TX，demux→RX），但用同一个同步函数。把这组读写
函数收进 `PublicChunkIo_`（一组关联函数 / 零大小辅助，不持有 buffer）：它只依赖
`TrBuffRead` / `TrBuffWrite` / `TrProducerState`，**不新增 buffer 类型**，也不依赖
`abs_mptp`。

```rust
/// 把「2 字节大端长度 + 帧字节」写进 ring。
/// 返回 WouldBlock 表示 ring 当前空闲不足 2 + len（调用方保留请求/帧，park 等空间）。
fn try_write_chunk_<W: TrBuffWrite<u8> + TrProducerState>(
    ring: &mut W,
    header: &[u8],
    payload: &[u8],
) -> Result<(), ChunkWriteErr_> {
    let len = header.len() + payload.len();
    if len == 0 || len > K_MAX_CHUNK_PAYLOAD || len > max_packet_size_ {
        return Err(ChunkWriteErr_::TooLarge);
    }
    // 1. 先看总空闲，不足就整体推迟——绝不写半 chunk。
    let free = ring.producer_state().map(|(free, _)| free).unwrap_or(0);
    if free < 2 + len {
        return Err(ChunkWriteErr_::WouldBlock);
    }
    // 2. 同步写：写端独占，总空闲已够，循环必然完成。
    write_all_sync_(ring, &(len as u16).to_be_bytes())?;
    write_all_sync_(ring, header)?;
    write_all_sync_(ring, payload)?;
    Ok(())
}
```

- `write_all_sync_` 用 `try_write(Demand::at_least(1))` 借段、复制、drop 段；不 `await`。
  总空闲已判够，因此不会在中途 `WouldBlock`。
- `main_loop` 调用它写 TX；返回 `WouldBlock` 时把当前请求/帧留在 `pending`，park 等
  `mux_loop` 消费出空间。请求 TCS 暂不完成。
- `demux_loop` 调用它写 RX；返回 `WouldBlock` 时 park 等 `main_loop` 消费。写之前它
  已经暂停读 `ConnRx`，所以不会丢控制帧。
- 长度前缀与内容之间不 park、不切到别的 chunk；同一写者串行，因此 ring 中不会出现
  “前缀 A、内容 B”的错配。
- 正常路径不写半 chunk。连接收尾时写者可能在“前缀已写、内容未写”处中断；此时
  main/demux 都在退出，ring 随后被 drop，不需要恢复。

### 5.5 读侧：`mux_loop`

`mux_loop` 维护一个本地状态 `public_chunk_left: usize`：

```text
0. 若 public_chunk_left == 0：
   - 从 TX ring 读 2 字节大端长度；不足 2 字节则本轮结束（park 等 ring 可读）。
   - len == 0 / len > max_packet_size ⇒ 协议错误。
   - public_chunk_left = len。
1. 把 public_chunk_left 字节从 TX ring 搬进 ConnTx：
   - 前缀已经消费，不写进 ConnTx；只搬 payload（完整 smux 帧）。
   - ConnTx 空间不足则 park 等可写；不插入用户数据帧。
2. public_chunk_left 减到 0 后回到 0 读下一个长度。
```

- 一旦开始一个 chunk（`public_chunk_left > 0`），必须把它搬完才能发用户数据；否则
  smux 帧会在 `ConnTx` 上交错。
- 因为 chunk 前缀在 ring 内、payload 是完整帧，`mux_loop` **不需要**重新组帧，也不需要
  知道帧的 `kind` / dock；它只按字节搬。
- 读侧不需要一次性借出整帧：可以按 `ConnTx` 当前可写量分段搬，`public_chunk_left`
  记录剩余；这正好利用了 ring 的流式特性。

### 5.6 读侧：`main_loop`

`main_loop` 维护 `public_rx_left: Option<usize>` 和一个**常备一帧暂存**：

```text
0. 若 public_rx_left 为 None：
   - 从 RX ring 读 2 字节长度；不足 2 字节则 park。
   - len == 0 / len > max_packet_size ⇒ 协议错误。
   - public_rx_left = Some(len)。
1. 读恰好 len 字节到常备帧暂存（`Owned<[u8]>`，连接一次分配，容量 max_packet_size）；
   不足则 park（ring 可读）。
2. 交给 `handle_control_frame_`；完成后 public_rx_left = None，回到 0。
```

- 帧暂存是**解析需要**，不是队列：同一时刻最多一帧，连接建立时一次分配。
- 也可以在 ring 段足够连续时直接借段解析；常备暂存是无回绕、跨 `await` 安全的统一
  路径，先按它实现。
- 读取的整帧是**原始 smux 帧字节**（帧头 + 载荷），包含全部合法性审查所需信息；
  具体审查分工见 §8.2。

### 5.7 优先级与公平性

`mux_loop` 的主循环每轮按下面的顺序尝试：

```text
loop {
    if public_chunk_left > 0 {
        move_public_payload_to_conn_tx();   // 必须搬完当前 chunk
        continue;
    }
    if let Some(len) = public_tx.try_read_u16() {
        public_chunk_left = len;
        continue;
    }
    if let Some(seg) = user_channels.next_writable() {
        write_data_frame(seg);
        continue;                           // 每发一段用户数据后回到顶部
    }
    if let Some(tg) = telegraphers.next_ready() {
        write_datagram(tg);
        continue;
    }
    break;
}
```

- 公共 chunk **始终插队到用户数据之前**；一旦开始搬一个 chunk，用户数据等待它搬完。
- 控制帧是低频事件（建流 / 拆流 / 拒绝），不会把用户数据饿死；如果将来出现控制帧
  风暴，再加“每 N 条公共帧后强制一条用户数据”的公平性配额，本方案不预先引入。

### 5.8 背压

| 位置 | 生产者 | 消费者 | 满时 |
| --- | --- | --- | --- |
| 公共 TX ring | `main_loop` | `mux_loop` | `main_loop` 保留 `pending_request`；park 等 ring 空间；**不写半 chunk** |
| 公共 RX ring | `demux_loop` | `main_loop` | `demux_loop` 暂停读 `ConnRx`；park 等 ring 空间；**不写半 chunk** |
| 请求队列 | API futures | `main_loop` | `push_async_` 等空位；等待者超过 `K_QUEUE_WAITERS` 返回 `Busy` |
| `mux_inbox` / `demux_inbox` | `main_loop` | 数据循环 | `main_loop` 保留待投递 attach，park 等 inbox 空位 |

**不允许**在任何一处用“无界暂存 / 溢出 `VecDeque` / 丢弃不可重建帧”兜底。

### 5.9 生命周期

- 连接建立时创建 `PublicChannel_`（两条 ring + 唤醒槽 + 关闭位）。
- `main_loop` 退出前：关闭 TX 写端（唤醒 `mux_loop` 读端）与 RX 读端（唤醒
  `demux_loop` 写端）；未消费的字节随 ring 半部释放。
- `mux_loop` / `demux_loop` 退出时：关闭各自持有的环半部；`mux_loop` 仍按既有
  `TxSealGuard_` 封口 `ConnTx`。
- 公共子流不参与身份释放、不进入宽限期、不产生 metrics 的子流级事件；它承载的帧
  仍按原 dock 上报 `on_frame` / `on_transport_bytes`（写侧在 `main_loop` 组帧时上报，
  读侧在 `demux_loop` 收帧时上报）。

### 5.10 建流期 message ring（Listener / pending channel）

#### 为什么需要

`open` 的 message、`accept` 的 welcome、`reject` 的 reason 都要在“调用方缓冲”与“帧”之间
搬运。现状用 `ControlFrame_::payload_: Vec<u8>` / `ChannelHandle::message_: Vec<u8>` 缓存；
这是每次建流 / 拒绝一次临时堆分配。改为：**每个未完成建流的 channel 自带一对小 ring**。

- 发起方（本端尚未发 `OPEN`、尚未 `accept`）：outgoing ring 存 `OPEN` 的 message；
  incoming ring 存对端 `ACCEPT` 的 welcome（若带）。
- 响应方（Listener 产出的入向 handle）：incoming ring 存对端 `OPEN` 的 message；outgoing
  ring 存 `ACCEPT` 的 welcome / `REJECT` 的 reason。
- **Listener 本身不共享一条 ring**：它为每个入向待决请求分配一对。共享一条会把“容量按
  子流分摊”重新变回“并发请求争同一块容量”，正是前面要消掉的问题；Listener 只负责把
  配置（容量 / 分配器）带进来。

#### 形状

```rust
/// 应用侧持有的两半：写 outgoing、读 incoming。
pub(crate) struct PendingMsgApp_ {
    tx_: BufferedTx, // app 写 -> main 读
    rx_: BufferedRx, // main 写 -> app 读
}

/// 主循环持有的两半。
pub(crate) struct PendingMsgMain_ {
    tx_: BufferedRx, // 读 app 写的 outgoing
    rx_: BufferedTx, // 写 incoming 给 app
}
```

ring 是单向的，所以“收发 message”实际是**每个 pending channel 一对**（outgoing /
incoming），不是一条双向 ring。每个方向仍是普通 ring（与用户子流同一种
`BufferedTx` / `BufferedRx`），**不新增 buffer 类型**；ring 内格式复用 §5.2 的 chunk：

```text
pending-message = [u16 大端长度][message bytes]
```

一个 pending channel 的每个方向**至多一条消息**（`OPEN` message / welcome / reason），
因此不需要队列，只需要“一条消息”的 ring。`0x0000` 表示空 message。

#### 配置（由 MuxConfig 决定）

在本项目的 `TrConnCfg`（即 MuxConfig 的落地 trait；它已经负责 `Alloc` / 运行时值等
资源项）上增加两个**带默认实现**的方法：

```rust
/// 每个方向的 message 载荷上限（字节）。0 表示只允许空 message。
fn pending_msg_capacity(&self) -> usize {
    K_PENDING_MSG_CAP_DEFAULT // 512
}

/// 建这些 ring 的分配器；默认复用连接内部结构的分配器。
fn pending_msg_allocator(&self) -> Self::Alloc {
    self.allocator()
}
```

- 实际 ring 容量 =
  `max(2, 2 + min(pending_msg_capacity(), max_packet_size.saturating_sub(K_MAX_FRAME_HEADER), K_MAX_CHUNK_PAYLOAD))`；
  2 字节是长度前缀，`max(2, ...)` 保证 `cap = 0` 时仍能写空的 `0x0000`。
- 两个方法都有默认实现，因此现有 `TrConnCfg` 实现不必改；如果必须使用**不同分配器类型**
  （而不是同一个 `Self::Alloc` 的另一个实例），需要新增关联类型 `MsgAlloc`，那是公开
  API 的破坏性变更（Q10）。
- 若团队希望把这两项放进上游 `abs_smux::conf::TrMuxConfig`，那是另一处公开 API 变更；
  本方案放在 `TrConnCfg`，因为它已经统一管理分配器与运行时资源。
- 若需要 **Listener 级**不同容量，需要给 `listen_async` 加参数或加一个 per-listener
  配置方法；本方案先使用连接级配置，避免再增公开 API。
- 容量在连接建立时校验：0 表示只允许空 message；因为 ring 至少有 2 字节，
  `PendingMsgApp_` 永远存在，不需要 `Option`。

#### 分配与生命周期

- 建连时把 `pending_msg_capacity()` 与 `pending_msg_allocator()` 各取一次，放进
  `MuxShared_` / `ControlState_`；main 建 ring 时直接用这份快照，不再逐请求取配置。
- `main_loop` 在 `ReserveChannel` / `reserve_inbound_` 时为该身份建一对 ring：
  两次 `new_buffered_channel(Owned::new_uninit_slice(ring_cap, msg_alloc))`，其中
  `ring_cap = max(2, 2 + cap)`。
- 发起方：app 侧两半随 `ReserveChannel` 的 reply 返回并放进 `ChannelHandle`；main 侧
  两半放进 `ChanSlot_.msg_main_`。
- 响应方：两对半都先放进 `ChanSlot_`（`msg_app_` / `msg_main_`）；`TakeInbound` 时把
  app 侧 move 给 `ChannelHandle`，main 侧留在槽里。身份表槽由 main 独占，零锁。
- 建流裁决（`accept` / `reject`）时：app 侧两半随 `ChannelHandle` 保留到 handle 释放；
  main 侧两半在裁决后关闭 / 丢弃。`Drop` 未裁决时只置 `RETIRED`；app 侧两半随句柄
  释放，main 侧在 retired 清扫时关闭。
- 建流完成后这对 ring 不再使用；正常 channel 的收发环不受影响。
- 每个 pending channel 两次 `new_buffered_channel`（每次一次 buffer 分配 + 一次 ring
  block 分配）；这是冷路径，后续可池化。建环失败按身份登记失败处理：发起方返回
  请求错误，响应方回 `REJECT`，不让连接因此失败。

#### 读写规则

- **app 写 outgoing**：检查 `len <= cap`（空 message 恒允许），把 `[u16 len][bytes]`
  写进 `BufferedTx`（可 `write_async`；空 message 写 `0x0000`）。
  - 发起方：只在 `open_channel_async` 时写 `OPEN` message；`accept_async` **不再写**
    outgoing，main 读的是 open 时留下的那条 chunk。
  - 响应方：`accept_async` 写 welcome（空也写 `0x0000`），`reject_async` 写 reason；
    然后提交 `Accept` / `Reject` 请求。
- **main 读 outgoing**：`try_read` 2 字节长度 + len 字节；不足则把请求留在 `pending`。
  因为 app 先写完再提交请求，main 看到请求时数据已就绪（ring 自身的 Release/Acquire
  与请求队列的同步共同保证）。
- **main 写 incoming**：用与 §5.4 同一个 `[u16 len][bytes]` 写入口（`header = &[]`、
  `payload = message`，写前按 `cap` 检查；这里不写 smux 帧头）；空间不足则保留帧 /
  pending。一个方向至多一条，正常不会满。
- **app 读 incoming**：`read_async` 先读 2 字节长度，再读 len 字节到调用方缓冲；
  这条消息没有 smux 帧头，长度就是 message 本身。
- **超长 message**：
  - `open` 的 message：`MessageTooLong`，撤销本地预留，不发 `OPEN`；
  - `accept` 的 welcome：`MessageTooLong`，回 `REJECT` 释放身份（不让对端悬着）；
  - `reject` 的 reason：响应方仍发一条**空 reason 的 `REJECT`**（协议上必须答复），再
    返回 `MessageTooLong`；发起方尚未发 `OPEN` 时只撤销预留，不发帧；
  - incoming message：响应方回 `REJECT`，发起方按建流失败处理；都不让整条连接失败。
  具体策略见 §9.3 / Q11。

#### 与 TCS 的关系

`CtrlCell_` 的载荷职责被这对 ring 取代；请求只带 `Tcs_`（完成源）。`CtrlCell_` 删除，
`Tcs_` 仍按 §7 的 `Shared` / Box 方式分配。

---

## 6. main_loop

### 6.1 输入

```rust
enum MainWake_<A> {
    Request(MainRequest_<A>),
    RxChunk,               // 公共 RX ring 可读（可能只读到长度，正在等整帧）
    TxSpace,               // 公共 TX ring 出现空间（pending_request 可继续）
    Hint(u64),             // timer / retired / window-dirty / app-closed ...
    Cancelled,
}
```

`main_loop` 的 park 点同时 poll 下列 future，任一就绪即返回：

- `requests_.poll_pop_(cx)`；
- `public_rx_.poll_readable_(cx)`（有 2 字节长度，或有当前 chunk 的剩余字节）；
- `public_tx_.poll_writable_(cx)`（仅当有 `pending_request` / 待写 chunk 时）；
- `ControlState_.poll_main_hint_(cx)`（**定时到期也走这里**：`HINT_MAIN_TIMER`）；
- `cancel_.cancellation()`。

`NotifySlot_` 的持久位保证“先 notify、后 park”不丢；请求队列用 `InlineQueue_` 的
`poll_pop_`（已有持久 `ready_`）。

### 6.2 请求事件

```rust
pub(crate) enum MainRequest_<A: AllocatorClone + Send + Sync> {
    /// 独占绑定一个 dock（显式或自动分配）。
    Bind {
        local_dock: Dock,
        reply: Tcs_<MainReply_<A>, A>,
    },

    /// 登记 listener 身份。
    Listen {
        local_dock: Dock,
        reserve: usize,
        reply: Tcs_<MainReply_<A>, A>,
    },

    /// 为一条子流登记身份（open 的第一步）。
    ReserveChannel {
        local_dock: Dock,
        remote_dock: Dock,
        reply: Tcs_<MainReply_<A>, A>,
    },

    /// 从 listener 的入向表里取一个待决请求（`income_async` 的一步）。
    ///
    /// 没有待决请求时返回 `MainReply_::Inbound(None)`；调用方在 `LsnOwner_` 的
    /// 通知槽上等下一次提示后重试。`inbound_` 状态仍在身份表槽里（`ChanSlot_`），
    /// 不搬进 listener 节点——节点是非泛型的，放不下 `ChannelOwner_<A>`。
    TakeInbound {
        local_dock: Dock,
        reply: Tcs_<MainReply_<A>, A>,
    },

    /// 最终裁决：建环已完成，环半部与 message 都已就绪。
    ///
    /// `role_` 区分发起方 / 响应方：
    /// - 发起方：从 pending outgoing ring 读 `OPEN` message，发 `OPEN`（带真实窗口）；
    /// - 响应方：从 pending outgoing ring 读 welcome，发 `OPEN`（响应方窗口）+ `ACCEPT`。
    ///
    /// `OPEN` 仍在 `accept` 时发出（不提前），因此 message 由 pending ring 保存到
    /// `accept`，不需要 `ChannelHandle::message_` 的 `Vec`。
    Accept {
        local_dock: Dock,
        remote_dock: Dock,
        owner: ChannelOwner_<A>,
        role_: EstablishRole_,
        attach: AttachPair_<A>,
        initial_window: Credit,
        reply: Tcs_<MainReply_<A>, A>,
    },

    /// 拒绝入向建流；reason 已写进 pending outgoing ring。
    Reject {
        local_dock: Dock,
        remote_dock: Dock,
        owner: ChannelOwner_<A>,
        reply: Tcs_<MainReply_<A>, A>,
    },

    /// 开一条 telegraph 端点。
    OpenTelegraph {
        local_dock: Dock,
        attach: TgAttachPair_<A>,
        reply: Tcs_<MainReply_<A>, A>,
    },

    /// 优雅关闭；`reply` 完成时返回“是否与对端收敛”。
    Close {
        reply: Tcs_<MainReply_<A>, A>,
    },
}
```

`MainReply_` 是统一结果，避免请求枚举里出现大量不同的 `Tcs_<T>`：

```rust
pub(crate) enum MainReply_<A: AllocatorClone + Send + Sync> {
    Unit,
    Dock(Dock),
    /// 新建的 pending channel：owner（窗口 / 状态）+ 该 channel 的 app 侧
    /// message ring 两半（`OPEN` message 的写端、incoming message 的读端）。
    Channel {
        owner: ChannelOwner_<A>,
        msg_: PendingMsgApp_,
    },
    Listener(LsnOwner_<A>),
    Telegraph(TgOwner_<A>),
    /// 从 listener 取出的入向 pending channel（remote + owner + app 侧 message ring）。
    Inbound(Option<(Dock, ChannelOwner_<A>, PendingMsgApp_)>),
    Closed(bool),
}
```

`Tcs_` 的结果类型统一为 `Result<MainReply_<A>, MuxError>`；API 侧用
`into_bind_()` / `into_listen_()` / ... 做一次穷举转换，不额外分配。

**被放弃的 reply 必须显式清理**：`ReserveChannel` / `TakeInbound` 的 reply 携带
`PendingMsgApp_`（两条 ring 半部）。main 在 `set_` 之前先查 `Tcs_::is_abandoned_()`：

- `ReserveChannel` 已放弃 → 撤销刚登记的身份，关闭该 pending 的 main 侧 ring；
- `TakeInbound` 已放弃 → 把 `PendingMsgApp_` 放回 `ChanSlot_.msg_app_`，把
  `inbound_` 保持为 `Pending`，下次 `income_async` 还能取到；不能把请求丢掉。

`set_` 在 `Abandoned` 上会 drop 结果，因此**必须**在 set 前判，不能依赖 set 的返回值。

**为什么载荷不直接放进请求枚举**：`open` 的 `message: &mut R`、`accept` 的
`welcome: &mut W`、`reject` 的 `reason: &mut R` 都是泛型 / 带生命周期的借用，不能
跨 `spawn_local` 边界进入 `main_loop`。因此由 API future 在提交请求前把载荷写进该
pending channel 的 **outgoing message ring**（§5.10）；请求只带 `owner` / `Tcs_`，
main 从 `ChanSlot_.msg_main_` 读。

**为什么 app 侧 ring 半部随 reply 返回**：`PendingMsgApp_` 里的 `BufferedTx` /
`BufferedRx` 是具体类型（ring 的泛型已在建环时擦除），可以 move 进 `MainReply_`；
`ChannelHandle` 直接持有它们，main 侧两半放在身份表槽里。

**为什么环半部可以进请求**：`BufferedTx` / `BufferedRx` 是具体类型（泛型 `B` 已在
API future 内被擦除），可以 move 进 `AttachPair_`；`main_loop` 只负责把它们投进
`mux_inbox` / `demux_inbox`，不触碰泛型。

```rust
/// 一次 accept 要投递给两个数据循环的环半部。
pub(crate) struct AttachPair_<A: AllocatorClone + Send + Sync> {
    write_: Option<WriteAttach_<A>>, // 发送环读端 -> mux_loop
    read_: Option<ReadAttach_<A>>,   // 接收环写端 -> demux_loop
}

/// telegraph 的对应物（键只有 local_dock）。
pub(crate) struct TgAttachPair_<A: AllocatorClone + Send + Sync> {
    write_: Option<TgWriteAttach_<A>>,
    read_: Option<TgReadAttach_<A>>,
}
```

### 6.3 主循环骨架

```rust
pub(crate) async fn main_loop_async_<C, K>(shared: MuxShared_<C>, cancel: K)
where C: TrConnCfg, K: TrCancellationToken
{
    let mut pending: Option<MainRequest_<C::Alloc>> = None;
    // 公共 RX ring 的 chunked 读状态：已读长度、还差多少字节；`rx_scratch` 是
    // 连接常备的一帧暂存（一次分配）。
    let mut public_rx_left: Option<usize> = None;
    let mut rx_scratch: Owned<[u8], C::Alloc> = Owned::new_slice(
        shared.max_packet_size_(),
        |_idx, slot| { slot.write(0u8); },
        shared.control_().allocator_().clone(),
    );
    // 关闭状态：`closing` 表示正在推进；`close_reply` 只在 `close_async` 的请求路径上
    // 有值（`MuxCore::drop` 没有调用方在等）。
    let mut closing = false;
    let mut close_reply: Option<Tcs_<MainReply_<C::Alloc>, C::Alloc>> = None;

    loop {
        if cancel.is_cancelled() { return drain_and_exit_(&shared); }

        // 1. 连接级失败 / 关闭请求优先。
        if shared.control_().status_().is_failed() {
            fail_all_pending_(&shared, &mut pending);
            return;
        }
        if shared.control_().take_close_requested_() {
            closing = true;
        }

        // 2. 合并提示：timer / retired / window-dirty / app-closed。
        //
        //    `timer_loop` 只在到期时 raise `HINT_MAIN_TIMER`；`main_loop` 自己不构造
        //    `delay`，因此 park 的等待源保持统一（请求 / ring / hint / cancel）。
        let hints = shared.control_().take_main_hint_();
        if hints & HINT_MAIN_TIMER != 0 {
            tick_(&shared, &mut closing, &mut close_reply);
        }
        if hints & HINT_MAIN_RETIRED != 0 { reap_retired_(&shared); }
        if hints & HINT_MAIN_WINDOW_DIRTY != 0 { drain_window_dirty_(&shared); }
        if hints & HINT_MAIN_TX_CLOSED != 0 { advance_tx_closed_(&shared); }

        // 3. 公共 RX ring：按 chunked 读长度 + 整帧；每轮最多 K_MAIN_FRAME_BATCH 条。
        //    `drain_one_public_frame_` 内部维护 `public_rx_left`，读到半帧就返回 Pending。
        for _ in 0..K_MAIN_FRAME_BATCH {
            match drain_one_public_frame_(&shared, &mut public_rx_left, &mut rx_scratch) {
                Some(frame) => handle_control_frame_(&shared, frame),
                None => break,
            }
        }

        // 4. 请求：每轮最多 K_MAIN_REQ_BATCH 条；需要 TX ring 空间的先保留。
        for _ in 0..K_MAIN_REQ_BATCH {
            if pending.is_none() {
                pending = shared.requests_().try_pop_();
            }
            let Some(req) = pending.take() else { break };
            match try_handle_request_(&shared, req) {
                Handled_(r) => { /* TCS 已 set */ }
                NeedTxSpace_(r) => { pending = Some(r); break; }
            }
        }

        // 5. 优雅关闭推进：给未收尾的子流置位 + raise hint。
        if closing { advance_close_(&shared, &mut close_reply); }

        // 6. park。
        match park_main_(&shared, &cancel).await { ... }
    }
}
```

要点：

- **所有处理都是同步的**：取身份表、组帧、set TCS 都不 `await`。唯一的 `await` 在
  park 点（等请求 / 帧 / 槽 / 定时器 / 取消）。这保证 `main_loop` 不会被某个子流
  的长事务阻塞。
- `tick_` 里不调用任何会 park 的 API；它只做 O(n) 扫描 + 组帧尝试。组帧需要的 TX ring
  空间拿不到时，置 `pending_tick_frames` 位，下一轮重试（可重建的 PULSE 可以直接丢，
  下一 tick 再来；不可重建的 CLOSE 必须保留在 `closing` 状态里）。
- `Close` 请求的 `reply` 在进入 `closing` 时存进 `main_loop` 的内联
  `close_reply: Option<Tcs_<...>>`；收敛或超时时先 `set_` 它，再 `cancel_loops_()`。
  `MuxCore::drop` 走的是同一条 `closing` 路径，但没有 `close_reply`。
- 每轮批处理上限防止“请求/帧源源不断 ⇒ 定时器与关闭推进饿死”。

### 6.4 `timer_loop` 保留为计时服务

**保留 `timer_loop`**（第 4 个循环 / 第 4 个取消令牌），但把它从“决策 + 投事件”收缩
成**纯计时服务**：

1. **决策部分**（同步）留在 `main_loop::tick_`：`timer_scan_` 的语义原样迁入：
   - 每条子流的 `PULSE` 职责（`pulse_millis = max_channel_timeout / 2`）；
   - 空闲超时 `Abort`；
   - 建流未裁决超时 `RejectEstablish`；
   - 优雅关闭时 `Finalize`；
   - `WaitClose` 墓碑回收（`reap_wait_close_`）；
   - 连接级关闭动作（`K_CLOSE_IDLE` / `K_CLOSE_TIMEOUT`）。
   身份表由 `main_loop` 独占，所以决策必须在 main 里；timer 不做扫描。
2. **等待部分**留在 `timer_loop`：它读 `main_loop` 发布的 `next_deadline`，`delay` 到期
   后 `raise_main_hint_(HINT_MAIN_TIMER)`。这就是“定时器注入信号”。

```rust
struct TimerSignal_ {
    /// 下一次必须醒来的连接内毫秒；`0` = 当前没有期限。
    next_millis_: AtomicU64,

    /// main -> timer：期限变了，重算 delay。
    timer_wake_: NotifySlot_,
}
```

- `main_loop` 每次 `tick_` 结束时写 `next_millis_` 并 `timer_wake_.notify_()`；连接
  建立后先发布一次初始期限（例如 `now + pulse_millis`），避免 timer 无事可等；
- `timer_loop` 的 park 同时 poll `timer_wake_`、`delay(next - now)`、取消令牌；醒来后
  复读 `next_millis_`，期限变了就重算，没到就继续等；
- 到期只 `raise_main_hint_(HINT_MAIN_TIMER)`，不投事件、不访问身份表、不组帧；
- `main_loop` 的 park 把 `HINT_MAIN_TIMER` 当作普通 hint 处理。

**为什么保留而不是并入 main**：`main_loop` 需要“偶尔的定时注入”（关闭超时、重试、
宽限回收），而让它自己持有 `C::Rt::delay` 会把一个额外的等待源塞进 park 点，且
`delay` 的 future 需要每次重建。保留一个只做计时的循环，main 的 park 仍然只有
“请求 / ring / hint / cancel”四类来源；timer 与 main 的耦合只是一个 `NotifySlot_`、
一个 `AtomicU64` 和 main hint 的一个位。`TimerSignal_` 放在 `ControlState_` 里，
`MuxShared_` 同时交给 main 与 timer。

**备选（不采用）**：让 `timer_loop` 继续执行 `timer_scan_` 并把 `TimerAction_` 投回
main。这要求 timer 读身份表、并且重新引入一条“timer → main”的事件通道；它与
“main 单写身份表 + 无堆事件通道”直接冲突。因此本方案只让 timer 做计时，决策留在
main。

**风险**：多一个 `spawn_local` 与取消令牌；timer 只做 `delay`，开销很小。若将来
`main_loop` 能方便地 poll `C::Rt::delay`，可以把 timer 收缩掉，但本方案不预设。

### 6.5 入向控制帧处理

`main_loop` 从公共 RX ring 按 chunked 协议读到**整帧字节**（帧头 + 载荷），用
`FrameHeader` 的同步解析函数解码（`frame_parser_` 目前只有异步读头；新增一个纯函数
`FrameHeader::decode_sync_(&[u8]) -> Result<(FrameHeader, usize), MuxError>`，不分配）。
这些帧在写入 ring 之前已经由 `demux_loop` 做过帧级 / 路由级合法性初审（§8.2）；main
只做协议状态决策。处理表：

| 帧 | 处理 | 产生的动作 |
| --- | --- | --- |
| `OPEN`（已有表项） | 更新 owner 的 `PEER_OPENED` + 发送窗口快照；把 message 写入该 pending channel 的 incoming ring（空 message 写 `0x0000`）；`wake_establish_` | 无帧 |
| `OPEN`（无表项） | 查 listener；配额检查；`reserve_inbound_` 建身份槽 + message ring，把 `inbound_ = Pending(report)` 写进表槽；把 message 写入 incoming ring（空写 `0x0000`）；`notify_inbound(local)` | 失败 / message 超长时组 `REJECT` |
| `ACCEPT` | 把 welcome 写入该 pending channel 的 incoming ring（空写 `0x0000`）；更新 `ESTABLISH_OUTCOME`；`wake_establish_` | 无帧 |
| `REJECT` | 把 reason 写入 incoming ring（空写 `0x0000`）；更新 `ESTABLISH_OUTCOME`；`wake_establish_` | 无帧 |
| `CLOSE(FIN)` | 置 `PEER_FIN`；raise `HINT_D_CLOSE` / `HINT_W_PEER_CLOSED` | 无帧；由 demux 关接收环、mux 摘写表 |
| `CLOSE(RESET)` | 置 `PEER_RESET`；raise `HINT_W_PEER_CLOSED` | 无帧；mux 摘写表；释放时再由 main raise `HINT_D_RELEASE` |
| `PULSE` / `WINDOW_UPDATE` | **不经过公共 RX**（§8.2） | — |
| 其他 / 解析失败 | `mark_failed_(Protocol)`；取消四个循环 | 失败 |

`CLOSE` 的处理带来一个顺序差异：现状 `demux` 收到 `CLOSE(FIN)` 会立即 `writer_.close()`；
新方案由 `main_loop` 置位，`demux` 下一轮扫描时关闭。这只多一轮调度，语义不变。合法性
初审仍由 `demux` 在写 ring 之前完成（§8.2）；main 只负责状态推进与提示。

### 6.6 请求处理

| 请求 | 同步处理 | 帧 | TCS 结果 |
| --- | --- | --- | --- |
| `Bind` | 检查关闭 / dock 保留值 / 独占绑定；自动分配走 `take_free_dock_` | 无 | `Dock` |
| `Listen` | 登记 listener 身份；建立 `LsnRec_` | 无 | `Listener` |
| `ReserveChannel` | 查重 / 配额；创建 `ChannelOwner_`；按配置建一对 message ring；main 侧放进表槽，app 侧随 reply 返回 | 无 | `Channel { owner, msg_ }` |
| `TakeInbound` | 按 `local_dock` 扫描表槽里的 `Inbound_::Pending`，取出并转 `HandedOut`；把该 pending 的 app 侧 message ring 一起返回 | 无 | `Inbound(Option<(Dock, Owner, msg_)>)` |
| `Accept`（发起方） | 从 outgoing ring 读 `OPEN` message；发 `OPEN`（带真实接收窗口）；登记“等对端 `OPEN` / `ACCEPT`”；投递 `AttachPair_` | `OPEN` | `Unit` |
| `Accept`（响应方） | 从 outgoing ring 读 welcome；发自己的 `OPEN`（真实窗口）+ `ACCEPT`（welcome）；投递 `AttachPair_` | `OPEN` / `ACCEPT` | `Unit` |
| `Reject` | 从 outgoing ring 读 reason；组 `REJECT`；释放身份 / 进宽限 | `REJECT` | `Unit` |
| `OpenTelegraph` | 登记 telegraph 身份；投递 attach | 无 | `Telegraph` |
| `Close` | 进入 `WaitingClose`；推进 finalize；全部释放或超时后 `cancel_loops_` | `CLOSE` 若干 | `Closed(bool)` |

**`Accept` 的帧顺序**：发起方 `accept` 发 `OPEN`（带 `OPEN` message 与真实接收窗口）；
响应方 `accept` 发自己的 `OPEN`（真实窗口）→ `ACCEPT`（welcome）。所有帧都进公共 TX
ring，FIFO 保证顺序。message / welcome 都从该 pending channel 的 outgoing ring 读，
`main_loop` 不缓存 `Vec`。

**`Accept` 的 `AttachPair_` 投递**：`main_loop` 先 `try_push_` 到 `write_inbox_` /
`read_inbox_`；若任一满，把整个 `AttachPair_` 留在 `pending_attach`（`main_loop` 的
内联 `Option`），park 等 inbox 空位。此时 `ACCEPT` 帧是否已经发出去？

- 推荐顺序：**先投 attach，再发 ACCEPT**。这样对端收到 ACCEPT 时，本端两个数据循环
  已经（或即将）有本地表项；避免“ACCEPT 先到、对端开始发数据、本端 demux 还没有读表
  项”的窗口。代价是 `Accept` 请求在 inbox 满时暂停，ACK 帧延后；这是正确背压。
- 如果 `ACCEPT` 已经发出但 attach 投递失败（连接收尾），必须关闭环半部并让对端看到
  `CLOSE(RESET)`；`main_loop` 的失败路径负责。

**`Close` 的推进**：

1. `Close` 请求 / `MuxCore::drop` 的关闭位 → `main_loop` 进入 `WaitingClose`；
2. `tick_` 遍历活跃子流，对每条已裁决子流置 `APP_TX_CLOSED`（或
   `GRACEFUL_FIN_REQUESTED`）并 `raise_write_hint_(HINT_W_TX_CLOSED)`；
3. `mux_loop` 排空该子流发送环后置 `TX_DRAINED` + `raise_main_hint_(HINT_MAIN_TX_DRAINED)`；
4. `main_loop` 对 `TX_DRAINED` 且 `!LOCAL_FIN_SENT` 的子流组 `CLOSE(FIN)` 进公共 TX，
   置 `LOCAL_FIN_SENT`；
5. 对端 `FIN` 到达后走 §6.5；`claim_release_()` 成功时释放身份；
6. 活跃子流归零或整体超时 → **先** `set_` `Close` 的 TCS（`Closed(是否收敛)`），**再**
   `cancel_loops_()`；`main_loop` 自己随后从 park 醒来返回。先完成 TCS 再取消，保证
   调用方拿到的是“收尾结果”而不是“被取消”。

### 6.7 retired / 身份释放

`Drop` 不投队列，只置位：

| `Drop` 对象 | 置位 | 提示 |
| --- | --- | --- |
| `DockBinding` | `DockCtx_.unbind_requested_` | `HINT_MAIN_RETIRED` |
| `ChannelListener` | `LsnRec_.retired_` | `HINT_MAIN_RETIRED` |
| `Telegraph` | `TgRec_.retired_` | `HINT_MAIN_RETIRED` |
| `ChannelHandle`（未裁决） | `ChannelState_.RETIRED`；app 侧 msg ring 随句柄释放 | `HINT_MAIN_RETIRED` |
| `ChannelTx` | `APP_TX_CLOSED` | `HINT_W_TX_CLOSED` + `HINT_MAIN_TX_CLOSED` |
| `ChannelRx` | `APP_RX_CLOSED` | `HINT_MAIN_RX_CLOSED` |

`main_loop` 在 `HINT_MAIN_RETIRED` 时遍历身份表（`bindings_` / `docks_`），对置位的
节点执行与现状 `SessionEvent_` 相同的落实动作（`UnbindDock` / `ReleaseListener` /
`ReleaseTelegraph` / `UnreserveChannel` / `ReleaseChannel`）。遍历是 O(n)，但只在
提示置位时发生；`Drop` 是冷路径，且同一轮会合并多个释放。释放 / 撤销 / 解绑任何
身份时，`ChanSlot_` 里的 `msg_app_` / `msg_main_` 必须一并 close/drop，不能让 pending
message ring 随表槽泄漏。

`ChannelHandle` 的 `RETIRED` 由 `main_loop` 按角色落实：发起方在 `accept` 之前
**尚未发过 `OPEN`**，因此走 `UnreserveChannel`（对端不知道这条子流，不必回 `REJECT`）；
响应方已经收到对端 `OPEN`，走 `REJECT` + 宽限。若发起方已经在 `accept` 中发出 `OPEN`
之后才取消，则与 `reject_after_open_` 同路径，回 `REJECT`。

为了让 `DockBinding` 能置位而不取锁，**不把整个 `DockCtx_` 放进 `Shared`**（`Shared`
不提供 `DerefMut`，main 还要改计数）。做法是：`DockCtx_` 仍按值放在 `docks_` 树里，
只内嵌一个 `unbind_requested_: Shared<AtomicBool, A>`；`DockBinding` 持同一句柄，
`Drop` 只 `store(true)` + raise。main 在 `HINT_MAIN_RETIRED` 时 `docks_.get_mut(dock)`
读标志并改计数。这是每个绑定一次的小分配；换来 `Drop` 的零锁、零队列。

---

## 7. TCS（TaskCompletionSource）

### 7.1 形状

```rust
/// 一次请求的完成源。句柄可克隆：调用方持一份等待，事件队列持一份 set。
pub(crate) struct Tcs_<T, A: AllocatorClone + Send + Sync> {
    inner_: Shared<TcsInner_<T>, A>,
}

struct TcsInner_<T> {
    /// `Pending` / `Done(T)` / `Abandoned`。
    state_: SpinningMutexOwned<TcsState_<T>>,

    /// 等待者（单 waiter：每个请求只有一个 API future 在等）。
    waker_: SpinningMutexOwned<Option<Waker>>,
}

enum TcsState_<T> {
    Pending,
    Done(T),
    Abandoned,
}
```

- `Tcs_::new_(alloc)`：一次 `Shared::new`（`Box` 等价），**这是本方案允许的堆分配**。
- `Tcs_::set_(value) -> bool`：锁内把 `Pending` 换成 `Done(value)`，取 waker，锁外
  `wake()`；若已是 `Abandoned`，直接 `drop(value)` 并返回 `false`。
- `Tcs_::poll_wait_(cx) -> Poll<Option<T>>`：`Done` 时取走值；`Abandoned` 返回
  `None`；`Pending` 时登记 waker。
- `Tcs_::abandon_()`：等待 future 被 drop 时调用；`Pending -> Abandoned`，不影响
  `main_loop` 继续完成协议动作。
- `Tcs_::is_abandoned_()`：`main_loop` 在组帧 / 投递 attach 前检查；只影响“是否还
  需要答复”，不影响协议动作本身。

### 7.2 取消

公开异步方法用 `gen_may_cancel_future` 包装，取消令牌到达时：

1. 等待 future 返回 `Cancelled`；
2. future 的 `Drop` 调 `Tcs_::abandon_()`；
3. 已入队的请求**不会**被撤回——`main_loop` 仍会完成它（否则对端可能永久悬在
   `wait_establish_` 上），只是不再 set 结果。

这与现状 `mux_accept_async_` 的取消语义一致：取消后仍要回 `REJECT` / 释放身份。
不同之处是取消不再靠“future 被 drop 时在 API 线程上做协议动作”，而是由 `main_loop`
统一完成。

### 7.3 结果的所有权

`Tcs_` 的 `T = Result<MainReply_<A>, MuxError>`；`MainReply_` 里的 `Shared` 句柄会
随结果 move 给调用方。若调用方已 `abandon_`，`set_` 会 drop 结果，句柄计数正常回落。

### 7.4 控制载荷：建流期 message ring

**不再有 `CtrlCell_`。** `open` 的 message、`accept` 的 welcome、`reject` 的 reason
都走 §5.10 的 per-pending message ring；请求只带 `Tcs_`，不再带定长载荷块。

- API future 在建流期把载荷写进 outgoing ring（`[u16 len][bytes]`），再提交请求；
- `main_loop` 从 `ChanSlot_.msg_main_` 的 outgoing 读端搬进控制帧；
- incoming message 由 `main_loop` 写进 incoming ring，应用通过
  `TrChannelHandle::read_message_async` 读走（公开 API 变更，见 §9.3 / Q12）。

这使“控制载荷”的内存完全落在每 pending channel 的小 ring 里：容量由
`TrConnCfg::pending_msg_capacity` 决定，分配器由 `pending_msg_allocator` 决定；不再有
“每个请求 4 KiB 的 `CtrlCell_`”。

### 7.5 快放快收池（后续）

现在每次请求一次 `Shared::new`；后续把 `Tcs_` 的 `Shared` 换成池化句柄：

```rust
pub(crate) trait TcsCache_ {
    fn acquire_(&self) -> TcsHandle_;
    fn release_(&self, handle: TcsHandle_);
}
```

池的形态：固定块大小的 free list（`InlineQueue_<NonNull<Block>, N>` + 一块连续 slab），
`acquire` / `release` 都是 O(1)、无堆增长。本方案只要求：

- 所有 TCS 的分配 / 释放都经 `Tcs_::new_` / `Drop` 两个点，便于以后替换；
- pending message ring 的 `new_buffered_channel` 是另一个可池化点（两块小 ring +
  ring block），但它是冷路径，先在 P6 评估。

### 7.6 定容等待者：`WakerSlots_`

`InlineQueue_` 的 `free_waiters_: Vec<Waker>` 是多生产者背压的唯一按需堆容器。替换为：

```rust
/// 定容的等待者槽。`N` 与队列容量同阶（默认 16）。
struct WakerSlots_<const N: usize> {
    slots_: SpinningMutexOwned<[Option<Waker>; N]>,
}

impl<const N: usize> WakerSlots_<N> {
    /// 已存在同一 waker 时不重复登记；没有空槽时返回 false（调用方报 Busy）。
    fn register_(&self, cx: &mut Context<'_>) -> bool;

    /// 取走全部等待者，锁外逐个 wake。
    fn wake_all_(&self);
}
```

`InlineQueue_::push_async_` 在 `register_` 返回 `false` 时返回
`PushErr_::Busy(item)`；API 侧把它翻译成 `BindError::Busy` / `HandleError::Busy`
（待拍板 Q3）。这样等待者集合有确定上界，且不再随着并发建流数增长。

---

## 8. 数据面循环

### 8.1 `mux_loop`

每轮：

```text
0. 若 cancel：退出（先 drain 自己的本地表，关闭环半部，封口 ConnTx）。
1. 处理 mux_inbox：插入新 attach。
2. 取 main hint（TX_CLOSED / PEER_CLOSED / RELEASED / ABORT / WINDOW）。
3. 公共子流优先搬 chunk（§5.7）。
4. 轮转用户子流，每条发一段数据帧（保留现有 drain_one_ 的额度 / 环段逻辑）。
5. telegraph 轮转。
6. 收尾：
   - APP_TX_CLOSED 且发送环已空（且没有待发额度）⇒ 置 TX_DRAINED，raise main hint；
   - PEER_RESET ⇒ 摘写表、关发送环；
   - RELEASED / ABORT ⇒ 摘写表、关发送环。
7. park：公共 TX ring 有 chunk / ConnTx 可写 / 用户环可读 / hint / inbox / cancel。
```

`mux_loop` 不再：

- 构造任何**控制**帧头（`Data` / `Datagram` 数据帧的帧头仍由它写，这是数据面成帧）；
- 发 `FIN` / `RESET` / `REJECT` / `PULSE`；
- 调 `maybe_release_` / `release_channel_` / `unreserve_channel_`；
- 维护 `PendingFin_`（`BTreeSet`）——用 owner 的 `TX_DRAINED` 位 + 本地表扫描替代。

> **落地状态（R8）**：`FIN`（R6）/ `RESET`（R7）/ 身份释放（R8）已全部移出
> `mux_loop`——释放由 `main_loop::flush_releasable_` 在 `HINT_MAIN_RELEASE` 上执行，
> 写循环只做本地表 / 半部动作、置位与 raise 提示。**尚未完成**的是 `PULSE` 等 timer
> 控制帧（仍走 `WriteEvent_::Control` → `write_control_blocking_`），留 R9/T4。

**数据帧成帧**保留在 `mux_loop`：它从发送环取段、写 `FrameHeader`（`Data`）、复制载荷。
这是“把子流荷载报文通过 Tx 发送出去”，属于数据搬运；不涉及协议决策。

### 8.2 `demux_loop`

每轮：

```text
0. 若 cancel：退出（关闭本地读表里的接收环写端、telegraph 接收环）。
1. 处理 demux_inbox：插入新 attach。
2. 取 main hint（RELEASED / PEER_FIN / ABORT / TG_RELEASE）。
3. 读 ConnRx，先做**帧级合法性**（帧头解析、长度上限、kind/flags/字段组合、窗口字段）：
   - DATA：查本地读表；命中则 owner.recv_on_data_ + 写接收环；
     未命中则查宽限态（只读）；不在宽限态则 mark_failed_(Protocol) + 取消四个循环；
   - DATAGRAM：按 local_dock 投进 telegraph 接收上下文（整帧）；
   - OPEN/ACCEPT/REJECT/CLOSE：做**路由级合法性初审**（只读身份表 + 宽限态），
     合法则把原始整帧写进公共 RX ring 的 chunk；非法则协议错误；
   - PULSE / WINDOW_UPDATE：**快路径**，见下。
4. park：ConnRx 可读 / 公共 RX ring 可写 / hint / inbox / cancel。
```

**`demux_loop` 必须承担合法性审查**。原因不是“它想管业务”，而是**审查所需的信息在它
手里**：

- 帧头与载荷的解析上下文（读了哪些字段、长度是否与 `max_packet_size` 一致、字段组合
  是否合法）只有读循环有；
- `DATA` 帧的路由（本地读表、宽限态）只有读循环的本地表有；
- 控制帧的“这个 pair 现在是否存在 / 是否在宽限态 / 是否有对应 owner”需要读身份表；
  等整帧交给 `main_loop` 再查，就把“帧到达那一刻”的上下文丢了——宽限态可能正好在
  这期间被回收。

因此 `demux_loop` 的合法性审查分两层，**只读、不写身份表**：

1. **帧级**：`frame_parser_` 的头解析 + 长度上限 + `kind` / `flags` / 窗口字段组合；
2. **路由级**：按帧种类查本地读表 / 身份表只读视图（`has_channel_` /
   `is_wait_close_` / `channel_owner_ro_`），据此决定“搬进环 / 静默丢弃 / 协议错误”。

| 帧 | demux 初审 | 结论 |
| --- | --- | --- |
| `DATA` | 查本地读表 | 命中 → 写接收环；未命中但宽限态 → 静默丢弃；否则协议错误 |
| `DATAGRAM` | 按 `local_dock` 找 telegraph 接收上下文 | 无接收方 → 整帧丢弃（地址语义，不报错） |
| `OPEN` | 解析 + 窗口字段 + 查身份表是已知 owner 还是入向 | 两种都合法 → 写 RX ring 交 main；字段非法 → 协议错误 |
| `ACCEPT` / `REJECT` | 解析 + 查 owner | 有 owner → 写 RX ring；无 owner → 视为在途，静默丢弃 |
| `CLOSE` | 解析 flags + 查表项 / 宽限态 | 有表项或宽限态 → 写 RX ring；未知 → 视为在途，静默丢弃 |
| `PULSE` / `WINDOW_UPDATE` | 解析窗口 + 查 owner | 有 owner → 快路径 apply；无 owner → 静默丢弃 |
| 其他 / 字段非法 | 帧级解析失败 | 协议错误 |

“协议错误”= `mark_failed_(Protocol)` + 取消四个循环，不把帧交给 main。合法的一次性
控制帧 → 把**原始整帧**写进公共 RX ring 的 chunk，交给 main 做协议状态决策。

> `ACCEPT` / `REJECT` / `CLOSE` 的“无 owner → 静默丢弃”不是放宽合法性，而是协议允许的
> 在途帧：本端可能刚拆流、对端还没收到 `CLOSE`。这与现状 `demux` 的 `if let Some(entry)`
> 行为一致；真正的未知 `DATA` 仍按协议错误处理。

身份表仍是“`main_loop` 唯一写、`demux_loop` 只读”：注册表保留协作式读写锁，写者只有
main，demux 用只读方法取读锁。取锁是协作式异步获取（可能让出执行权）；临界区只做一次
查表，不 `await`、不跨帧；控制帧是冷路径，读锁竞争可以接受。

**快路径例外（I6 的唯一例外）**：`PULSE` / `WINDOW_UPDATE` 携带的是窗口快照，属于
可重建状态（I4）。`demux_loop` 直接：

1. `owner.send_on_report_(report)` 更新发送窗口；
2. `PULSE` 时 `owner.touch_(now)`，`WINDOW_UPDATE` 时 `owner.mark_data_(now)`；
3. `raise_write_hint_(HINT_W_WINDOW)` 叫醒 `mux_loop` 重试被额度卡住的子流。

它**不产生事件、不写公共 RX、不组帧**。理由：这两个帧在数据热路径上可能很频繁；如果
每个都进 `main_loop`，控制面会成为吞吐瓶颈，而且它们的全部信息就是“窗口变了”。
这是“把帧字段搬到 owner 状态”，不是协议决策。

**窗口通告的回程（本端 → 对端）**：

- `demux_loop` 在写数据进接收环后，调用 `owner.recv_take_report_()` 计算是否需要通告；
- 若有报告，写入 owner 内联的 `outgoing_report_` 槽（`SpinningMutexOwned<Option<WindowReport>>`）
  并 `raise_main_hint_(HINT_MAIN_WINDOW_DIRTY)`；
- `main_loop` 在 `HINT_MAIN_WINDOW_DIRTY` 时遍历有报告的 owner，**先确认公共 TX ring
  有 `2 + len` 的空间**，再取报告、组帧、写 chunk；空间不足时不取报告（保留在槽里），
  下一轮重试。
- 报告槽是“每子流一个最新快照”，不是队列；同一子流的多次变化合并为最新值，符合
  `FlowCtrl::should_report_with_` 的语义。

这样 `demux_loop` 仍然不组控制帧、不发控制帧；它做的是“合法性初审 + 把接收窗口的
账记好 + 把待通告快照放进 owner 的槽”。

**控制帧写 ring 的细节**：

- 控制帧总长 ≤ `max_packet_size`，而公共 RX ring 的容量 ≥ `2 + max_packet_size`；因此
  “ring 空闲 ≥ `2 + len`”就一定写得下。`demux_loop` 在读完帧头后先检查空间；不足则在
  “帧头已解析、载荷未读”的状态 park，等 `main_loop` 消费出空间，再继续读载荷。
  不能丢帧，也不能先读载荷再找地方放。
- 建议让 `frame_parser_::read_header_async_` 同时返回**原始帧头字节**
  （`HeadBytes_ { bytes: [u8; K_MAX_FRAME_HEADER], len }`）。公共 RX ring 的 chunk 直接
  写原始头 + 载荷，避免“重新 `encode_header_` 得到非规范编码”的风险；`DATA` /
  `DATAGRAM` 仍用解析后的 `FrameHeader`。
- 上报口径不变：`demux_loop` 收到控制帧时上报 `on_frame(Recv, ...)` 与
  `on_transport_bytes(Recv, ...)`；`main_loop` 组控制帧时上报 `on_frame(Send, ...)`
  与 `on_transport_bytes(Send, ...)`（它在组帧时就知道 dock / kind / 长度，不需要
  `mux_loop` 解析控制帧头）。

### 8.3 本地表与 inbox

- `mux_inbox`: `InlineQueue_<WriteAttach_<A>, ATTACH_INBOX_CAP>`，生产者只有
  `main_loop`（单生产者），消费者 `mux_loop`。满时 `main_loop` 把 attach 留在
  `pending_attach`，不阻塞数据面。
- `demux_inbox`: 同上，承载 `ReadAttach_<A>`。
- 删除 `InlineQueue_::free_waiters_`：单生产者不再需要多等待者广播；如果将来出现
  多生产者，使用定容 `WakerSlots_`。
- `pending_attach_`（`ChannelHandle` 的字段）删除：`main_loop` 的 pending 状态替代。

### 8.4 提示位

`RegistryShared_` 的位域保留并扩展（`main` / `write` / `read` 三个方向）：

| 位 | 方向 | 含义 |
| --- | --- | --- |
| `HINT_W_TX_CLOSED` | write | 应用丢了发送半边，mux 扫 `APP_TX_CLOSED` 并排空 |
| `HINT_W_PEER_CLOSED` | write | 对端 `CLOSE` 到达，mux 扫 `PEER_FIN` / `PEER_RESET` |
| `HINT_W_RELEASED` | write | 身份已释放或中止，mux 摘写表、关发送环 |
| `HINT_W_WINDOW` | write | 对端窗口变大，mux 重试被额度卡住的子流 |
| `HINT_D_RELEASE` | read | 身份已释放，demux 摘读表、关接收环 |
| `HINT_D_CLOSE` | read | 对端 `FIN`，demux 关接收环 |
| `HINT_D_ABORT` | read | main 的 tick 判定中止，demux 关接收环 |
| `HINT_MAIN_RETIRED` | main | 有身份 / binding 被 Drop，扫 retired 位 |
| `HINT_MAIN_WINDOW_DIRTY` | main | 有窗口快照待通告 |
| `HINT_MAIN_TX_CLOSED` | main | 应用丢了发送半边，准备 `TX_DRAINED` 后发 FIN |
| `HINT_MAIN_RX_CLOSED` | main | 应用丢了接收半边，组 `CLOSE(RESET)` |
| `HINT_MAIN_TX_DRAINED` | main | mux 已排空某子流，可以发 `CLOSE(FIN)` |
| `HINT_MAIN_RELEASE` | main | 有子流两个方向都收尾，扫身份表释放（`claim_release_` 一次性 CAS） |
| `HINT_MAIN_CLOSE` | main | 连接进入等待关闭 / 关闭请求 |
| `HINT_MAIN_TIMER` | main | `timer_loop` 到期；main 执行 `tick_` |

所有提示都是“可能变了”，事实在 owner 状态字或身份节点位里；消费者扫描后清位。

### 8.5 收尾位

`ChannelState_` 增加（或复用）以下位：

- `TX_DRAINED`：发送环已空且不再有数据要发；`main_loop` 据此发 `CLOSE(FIN)`。
- `RETIRED`：`ChannelHandle` / 半部被 Drop，`main_loop` 据此撤销 / 释放。
- `ABORT`：`main_loop` 的 tick 判定空闲超时，据此推进 `CLOSE` / 释放。
- `WINDOW_DIRTY`：接收窗口快照待通告（与 `outgoing_report_` 槽配对）。

`main_loop` 的 `claim_release_()` 保持“两个方向都收尾”的一次性 CAS；`mux_loop` /
`demux_loop` 只读这些位执行本地动作。

---

## 9. API 映射

### 9.1 abs_smux API → 请求 → 帧 → 结果

| `abs_smux` 方法 | 请求 | 产生的帧 | TCS 结果 | 备注 |
| --- | --- | --- | --- | --- |
| `TrConnection::bind_async` | `Bind` | 无 | `Dock` | 取消返回 `BindError::Cancelled` |
| `TrConnection::close_async` | `Close` | `CLOSE(FIN)` 若干 | `Closed(bool)` | `Drop` 走 `HINT_MAIN_CLOSE`，不等待 |
| `TrDockBinding::listen_async` | `Listen` | 无 | `Listener` | 返回 `LsnOwner_`；message ring 配置来自 `TrConnCfg` |
| `TrChannelListener::income_async` | `TakeInbound`（循环） | 无 | `Inbound(Option<(Dock, Owner, MsgApp)>)` | 无待决请求时在 `LsnOwner_` 通知槽上等，醒来再请求 |
| `TrDockBinding::open_channel_async` | `ReserveChannel` | 无（`OPEN` 延到 `accept`） | `Channel { owner, msg_ }` | message 写进 pending outgoing ring |
| `TrChannelHandle::accept_async`（发起方） | `Accept`（role=Initiator） | `OPEN`（带 message + 真实窗口） | `Unit` | 等对端 `ACCEPT` / `REJECT` 仍走 owner 通知槽 |
| `TrChannelHandle::accept_async`（响应方） | `Accept`（role=Responder） | `OPEN`（响应方窗口）+ `ACCEPT`（welcome） | `Unit` | welcome 写进 pending outgoing ring |
| `TrChannelHandle::reject_async` | `Reject` | `REJECT`（reason） | `Unit` | reason 写进 pending outgoing ring；返回字节数由 API 本地记录 |
| `TrChannelHandle::read_message_async`（**新增**） | 无请求 | 无 | 本地结果 | 读 pending incoming ring；响应方读 `OPEN` message，发起方读 `ACCEPT` welcome |
| `TrDockBinding::open_telegraph_async` | `OpenTelegraph` | 无 | `Telegraph` | attach 投递 |
| `TrDatagramSender::send_async` | 无请求 | `DATAGRAM`（`mux_loop`） | 本地结果 | 数据面，不经过 main |
| `TrDatagramRecver::recv_async` | 无请求 | — | 本地结果 | `demux_loop` 投递整帧 |
| `TrChannelRx::close_async` | 无请求 | `CLOSE` 由 `main_loop` | 本地结果 | 半部状态位 + hint |

### 9.2 时序：open / accept

```text
调用方                    main_loop                        对端
  │ open_channel_async       │                              │
  │ 1. ReserveChannel ──────►│ 登记身份、建 message ring    │
  │ ◄──────── Tcs(owner+msg) │                              │
  │ 2. 写 message → outgoing │                              │
  │    pending msg ring      │                              │
  │ 3. 返回 ChannelHandle    │                              │
  │                          │                              │
  │ accept_async             │                              │
  │ 1. 建环（泛型 B）        │                              │
  │ 2. Accept(role=Initiator)│                              │
  │ ────────────────────────►│ 读 outgoing message          │
  │                          │ 组 OPEN(真实窗口, message)   │
  │                          │ → 公共 TX ──────────────────►│
  │ ◄──────────── Tcs(Unit)  │                              │ listener income
  │ 3. 等对端 OPEN/ACCEPT    │ ◄──── OPEN(真实窗口) ────────│
  │                          │ 更新发送窗口、PEER_OPENED     │
  │                          │ ◄──── ACCEPT(welcome) ───────│
  │                          │ welcome → incoming ring       │
  │                          │ set_establish_outcome_       │
  │ ◄── owner 通知槽 ────────│                              │
  │ 返回 (tx, rx)            │                              │
  │ 4. 可选 read_message_async 读 incoming ring（welcome）  │
```

响应方（listener 侧）：

```text
main_loop 收到 OPEN(message)
  → reserve_inbound_：建身份槽 + message ring、inbound_ = Pending(report)
  → OPEN message 写入该 pending 的 incoming ring
  → notify_inbound(local)
应用 income_async：
  → 循环 TakeInbound 请求；取到 (remote, owner, msg_) 后返回 handle
  → 可选 read_message_async 读对端 OPEN message
  → accept_async：建环；welcome → outgoing ring
  → Accept(role=Responder)
main_loop：
  → 从表槽取对端窗口、设置 owner 发送窗口
  → 读 outgoing welcome
  → 组 OPEN(真实窗口) + ACCEPT(welcome)
  → 投递 attach（先于 ACCEPT）
  → 公共 TX 发布
  → Tcs(Unit)
```

### 9.3 公开 API / 协议语义变化（需拍板）

1. **`OPEN` 仍在 `accept` 时发**（保持现状），不提前、不报 0 窗口：
   - `open_channel_async` 只登记身份 + 把 message 写进 pending outgoing ring；
   - `accept_async` 才发 `OPEN`（带真实窗口）。
   - 这样 message 跨 open→accept 的保存由 pending ring 承担，不再需要
     `ChannelHandle::message_` 的 `Vec`，也不需要改变协议语义。
   - 备选：若团队希望 `OPEN` 提前，也可以（窗口先 0、accept 时发 `WINDOW_UPDATE`），
     但本方案不再依赖它。
2. **发起方未裁决就 Drop 时仍只 `UnreserveChannel`**（因为 `OPEN` 尚未发出）；
   只有在 `accept` 已经发出 `OPEN` 之后取消，才走 `reject_after_open_` 回 `REJECT`。
   这与现状一致。
3. **`BindingError` / `HandleError` 新增 `MessageTooLong` / `Busy`**：
   - `MessageTooLong`：outgoing message 超过 `pending_msg_capacity`；`open_channel_async`
     落在 `BindingError`，`accept_async` / `reject_async` 落在 `HandleError`；
   - `Busy`：请求队列等待者 / pending ring 配置无法满足（`BindingError` / `BindError` /
     `ListenerError` 视入口而定）。
   属于公开 API 变更，需确认。
4. **`BindError` / `ListenerError` 是否新增 `Busy`**：取决于请求队列等待者上限是否
   会真的打满；若选择“等待者满则 park 自旋”则可避免新变体，但不推荐。
5. **`pending_msg_capacity` 默认值**：建议 512 字节/方向；0 表示只允许空 message。
   上限取 `min(config, max_packet_size - K_MAX_FRAME_HEADER, K_MAX_CHUNK_PAYLOAD)`；超过上限的 message
   按“outgoing 报 `MessageTooLong`、incoming 回 `REJECT`”处理（Q11）。
6. **新增公开 API `read_message_async`**：建议形状为
   ```rust
   type MessageAsync<'f, W>: TrMayCancel<'f, MayCancelOutput = Result<usize, Self::Err>>
   where Self: 'f, W: 'f + TrBuffWrite<C::Data>;

   fn read_message_async<'f, W>(&'f mut self, sink: &'f mut W)
       -> Self::MessageAsync<'f, W>;
   ```
   `ChannelHandle` 在 `accept` 之后仍可调用（handle 不被 `accept_async` 消耗），用于
   读 initiator 侧收到的 `ACCEPT` welcome；响应方可在 `accept` 前读 `OPEN` message。
   这是 `abs_smux::chan::TrChannelHandle` 的公开 API 变更，需确认。
7. **Listener 不共享一条 ring**：每个入向 pending channel 一对；Listener 只携带配置
   （容量 / 分配器）。若团队坚持 Listener 级单 ring，需要额外的消息边界与多路复用，
   不推荐（会重新引入共享容量问题）。

---

## 10. 分配与容量账

### 10.1 删除

| 项 | 现状 | 删除后 |
| --- | --- | --- |
| `WriteEvent_` 通道 | 256 × ~64 B ≈ 16 KiB | 0 |
| `ReadEvent_` 通道 | 256 × ~40 B ≈ 10 KiB | 0 |
| `Overflow_::durable_` | 满时无界 `VecDeque` | 0 |
| `SessionMailbox_` + `MailboxOverflow_` | 256 × 16 B + 无界 `VecDeque` | 0 |
| `ControlFrame_::payload_` | 每次控制帧一次 `Vec` | 0（ring chunk 内组帧） |
| `ChannelHandle::message_` | 每次发起 open 一次 `Vec` | 0（pending message ring） |
| `CtrlCell_` | 基线已不存在（前一阶段删除） | 0（无需动作） |
| `InlineQueue_::free_waiters_` | 按需 `Vec<Waker>` | 定容 `WakerSlots_` |
| `PendingFin_` | `BTreeSet` 节点 | owner 位 + 本地表扫描 |
| `HintSlot_` | 死代码 | 0 |

### 10.2 新增（均一次分配）

| 项 | 大小（默认 4 KiB 帧） | 说明 |
| --- | --- | --- |
| 公共 TX ring | `2 × (2 + max_packet_size)` ≈ 8 KiB | `main_loop` 写、`mux_loop` 读 |
| 公共 RX ring | `2 × (2 + max_packet_size)` ≈ 8 KiB | `demux_loop` 写、`main_loop` 读 |
| 请求队列 | 64 × ~128 B ≈ 8 KiB | `InlineQueue_` 内联 |
| main 控制帧暂存 | `max_packet_size` ≈ 4 KiB | `main_loop` 读 RX ring 的整帧 |
| 建流期 message ring 对 | 每个 pending channel `2 × (2 + cap)` ≈ 1 KiB（cap=512） | 每个方向一条普通 ring；按并发 pending 数计，不是连接常量 |
| `WakerSlots_` | 16 × 16 B = 256 B | 替换 `Vec<Waker>` |
| `DockCtx_` 的 unbind 标志 | 每绑定一次 `Shared<AtomicBool>` 分配 | 让 `DockBinding::Drop` 零锁；表值仍按值 |

净额：默认约 **28–36 KiB 连接常备 + 每个 pending channel ~1 KiB message ring +
每请求一个小 `Tcs_`**；比现状两条事件通道 + 邮箱 + 每次控制请求的临时 `Vec` /
4 KiB cell 更小，
且全部有界、热路径零分配、无无界增长。

### 10.3 保留的堆（需在评审时确认）

- `ChannelRegistry_` / `RegistryInner_` 的 `BPlusTree` / `BTree`：身份表、dock 表、反向
  索引、宽限到期索引。底层 `SlabList` 按 **2 KiB chunk** 分配（本 crate 默认），chunk 内槽位复用，不逐条
  分配；树非空期间一个 chunk 常驻（这是换取低碎片的固定开销，见 §18）；
- `mux_loop` / `demux_loop` 的本地 `BPlusTree`：每子流占一个数据槽 + 索引项，随表所在
  的 chunk 复用；
- `DockCtx_` 的现有计数不新增分配；`unbind_requested_: Shared<AtomicBool>` 每绑定一次
  分配（§18.4）；
- `reusable_docks_: VecDeque<Dock, A>`：自动 dock 池。它是连接常备池，但仍是
  `VecDeque`；建议后续改成 B+ 树 `(seq, dock)` 或定容环形数组（P6）；
- `Shared` 根对象、`spawn_local` 的装箱（后端内部）；
- 注册表读写锁 `CooperativeRwLockOwned` 内部的全局 `Arc`（既有例外；写者唯一化后仍保留）；
- `Tcs_` 的 `Shared`（本方案允许，后续池化）；
- 每个 pending channel 的 `PendingMsgApp_` / `PendingMsgMain_` 及两条 ring 的 buffer +
  ring block 分配（冷路径，后续可池化）。

---

## 11. 删除 / 替换清单（按文件）

| 文件 | 动作 |
| --- | --- |
| `signal_.rs` | 删除 `WriteEvent_` / `ReadEvent_` / `ControlFrame_` / `Overflow_` / `EventSender_` / `EventReceiver_` / `event_channel_` / `SessionEvent_` / `SessionMailbox_`；只保留必要的 payload 类型（迁到新模块） |
| `slot_.rs` | `InlineQueue_` 保留，`free_waiters_` 换 `WakerSlots_`；删除 `HintSlot_` 与模块级 `#![allow(dead_code)]` |
| `session_.rs` | 瘦身为 `mux_loop_async_` / `demux_loop_async_` + 数据面辅助函数；本地 `WriteTable_` / `ReadTable_` / `TgReadTable_` / `TgWriteTable_` 换 `BPlusTree`；删除 `handle_write_event_` / `handle_read_event_` / `drain_read_events_` / `flush_rx_closed_` / `scan_peer_closed_` / `maybe_release_` / `write_control_blocking_` / `control_close_via_` |
| `timer_.rs` | **保留** `timer_loop_async_`，收缩为计时服务（读 `next_deadline`、`delay`、`raise HINT_MAIN_TIMER`）；决策部分（`TimerAction_` / `timer_scan_`）迁入 `main_loop::tick_`；删除 `deliver_action_` 的事件投递 |
| `ctl_.rs` | 删除（被 `main_loop` 取代） |
| `mux_connection/core_.rs` | 删除 `w_events_` / `r_events_`；`loops_` 保持 4 个（demux / mux / timer / main）；新增 `control_()` / `timer_signal_()`；`Drop` 改为置关闭位 + raise main hint |
| `mux_connection/registry_.rs` | 写者收敛到 `main_loop`；**保留** `CooperativeRwLockOwned` 供 `demux_loop` 只读合法性初审；删除 `drain_session_events_`；`bindings_` / `docks_` / `remote_index_` / `wait_close_expiry_` 换 `BPlusTree` / `BTree`；`DockCtx_<A>` 增加 `unbind_requested_: Shared<AtomicBool, A>`；`ChanSlot_` 增加 `msg_app_` / `msg_main_`；`bindings_.range` / `range_mut`、`remote_index_.range` 直接换同一形状的树 API |
| `mux_connection/conn_.rs` | `CoreBundle_` 建 `ControlState_` / `PublicChannel_` / 请求队列 / `TimerSignal_` / arena 索引树，并取一次 `pending_msg_capacity` / `pending_msg_allocator` 快照；`spawn` 四个循环（demux / mux / main / timer）；`bind_async` / `close_async` 改请求 + TCS；`TreeError` 映射为连接构造失败 |
| `channel_handle/handle_.rs` | 删除 `message_` / `pending_attach_`；新增 `msg_: PendingMsgApp_`；accept/reject 把载荷写进 outgoing ring；新增 `read_message_async`（读 incoming ring）；`accept_prepare_` 只建用户子流环；Drop 置 `RETIRED`（ring 半部随句柄释放） |
| `dock_binding/binding_.rs` | `bind_dock` / `bind_any_dock` 取回 `DockCtx_` 的 `unbind_requested_` 句柄交给 `DockBinding`；`open_channel_async` 把 message 写进 pending outgoing ring；删除 `read_available_into_vec_` 调用；`BindingError` 映射 `MessageTooLong` |
| `channel_listener/listener_.rs` | `Drop` 置 `retired_`；`listen_async` 改请求；`income_async` 保留 owner 通知槽；每个入向 pending channel 的 message ring 由 main 在建槽时分配，Listener 只携带配置 |
| `telegraph/*` | `open_telegraph_async` 改请求；`TgAttach` / `TgReady` / `TgTxClosed` 改位 + hint；`TgLenQueue_` 保留 |
| `util_.rs` | 删除 `read_available_into_vec_`；新增 `write_pending_msg_` / `read_pending_msg_`（带 `u16` 长度前缀、按容量拒绝） |
| `frame_.rs` / `frame_parser_.rs` | 新增同步头解析 `decode_sync_`；保留 `encode_header_` / `encode_header_into_` |
| `connection/config_.rs` | 新增 `K_PENDING_MSG_CAP_DEFAULT`、`pending_msg_capacity()`、`pending_msg_allocator()`（后两者带默认实现，现有配置无需改）；新增 `K_DOCK_TREE_DEGREE`（默认 16） |
| `connection/dock_index_.rs`（新） | 树类型别名、degree、`OrdComparer` 适配、`DockCtx_` unbind 标志辅助、`get_or_insert_dock_` 等 |
| `Cargo.toml` | `bplus_tree_cmp` 已是 git 依赖；本地源码 `6163209` 已补齐 `range` / `range_mut` / `iter_mut` 等，`Cargo.lock` 已锁到该提交；建议显式 pin rev |
| `abs_smux/src/chan.rs` | `TrChannelHandle` 新增 `read_message_async`（公开 API，需确认） |

---

## 12. 模块组织

```text
connection/
  main_loop/
    mod.rs          // 只做逐项导出，不放逻辑
    loop_.rs        // main_loop_async_ / park / 批处理
    request_.rs     // MainRequest_ / MainReply_ / 请求处理
    tick_.rs        // tick_ / 期限计算（原 timer_ 的决策部分）
    close_.rs       // 优雅关闭状态机
  timer_.rs          // 保留：独立计时服务 + TimerSignal_
  public_channel_.rs // PublicChannel_ / chunked 读写（不新增 buffer 类型）
  pending_msg_.rs    // PendingMsgApp_ / PendingMsgMain_ / 建流期 message ring
  dock_index_.rs     // BPlusTree / BTree 类型别名、degree、comparer、DockCtx_ unbind 辅助
  tcs_.rs            // Tcs_ / TcsInner_ / TcsAbandonGuard_
  control_state_.rs  // 请求队列 + main 提示位
  tree_config_.rs    // 索引树的 2 KiB chunk 配置（P1.5 建树时使用）
  session_.rs        // 只留 mux_loop / demux_loop 与数据面辅助
  ...
```

`mod.rs` 按项目纪律逐项 `pub(crate) use`，不用 `*`。

---

## 13. 分阶段实施

| 阶段 | 内容 | 验证 |
| --- | --- | --- |
| P0 | `Tcs_` / `PendingMsgRing_`（ring + `u16` 长度）/ `WakerSlots_` / `PublicChunkIo_`（ring + chunked 读写）/ `TimerSignal_` 原语 + 单测（不接线） | 单测；无分配断言 |
| P1 | `main_loop` 骨架接管 bind / listen / reserve / unbind / release；删除 `ctl_`；API 改请求 + TCS | 现有 bind / listen 用例 |
| P1.5 | 新增 `dock_index_` 适配层；`bindings_` / `docks_` / `remote_index_` / `wait_close_expiry_` 换 arena 树；`DockCtx_` 只加 unbind 标志；`range` / `range_mut` 直接替换 | 树等价性 / range 边界 / pending 顺序用例 |
| P2 | 公共子流 TX/RX ring 接线；`main_loop` 处理入向控制帧；`mux_loop` 优先搬 chunk；删除 `WriteEvent_::Control` | `inmem_mux` / `smoke_*` |
| P3 | open / accept / reject 改请求；`OPEN` 仍在 accept 时发；message / welcome / reason 走 pending ring；删除 `message_` / `ControlFrame_.payload_` / `CtrlCell_` | 建流 / 拒绝 / 取消 / message 超长用例 |
| P4 | 拆流 / 关闭状态机迁移；`mux` 只搬 + 置位，`demux` 搬 + 合法性初审 + 置位；本地表换 `BPlusTree`；删除 `LocalAbort` / `SessionMailbox_` / 旧 `scan_*` | `cross_thread_*` / `keepalive` / 关闭用例 |
| P5 | 删除两条事件通道、`signal_.rs` 大部分、`util_` Vec、`Vec<Waker>`；`timer` 收缩为计时服务 | 全量三后端 |
| P6 | TCS 池化；容量调优；`reusable_docks_` 去 `VecDeque`（B+ 或环形）；degree/chunk 调优 | 基准 / 长时间 churn |

每阶段结束必须：

- `cargo test -p smux_v1 --lib`（≤1 min 超时）；
- `cargo test --doc`；
- 三后端 `--all-targets`（≤5 min 超时）；
- `cargo clippy -p smux_v1 --lib` 无新告警；
- `python3 format_use_imports.py --check` 合规；
- 分配计数：数据面稳态 0 次分配，`Drop` 路径 0 次分配。

---

## 14. 测试与验收

### 14.1 新增单元测试

- `PublicChunkIo_`：2 字节大端长度、长度 + 内容的原子性（不足 `2 + len` 就整体推迟）、
  跨 ring 回绕、半 chunk 恢复、`len == 0` / 超长拒绝、`mux` 只搬 payload 不搬前缀、
  `main` 读整帧；并发生产者/消费者的可见性（用 loom 或现有自旋锁测试模式）。
- `TimerSignal_`：期限变化唤醒、无期限时只等 `timer_wake_`、到期只 raise hint、取消。
- `demux` 合法性初审：未知 DATA / 宽限态 DATA / 非法 OPEN / 非法 CLOSE / 非法字段组合。
- `Tcs_`：set / wait / abandon / 取消竞争 / 结果 Drop 计数。
- `PendingMsgRing_`：两个方向、`[u16 len][bytes]`、空 message、超长拒绝、容量边界、
  `cap = 0` 只允许空 message、app/main 两半分持、多个 pending handle 各自独立
  （不共享 Listener 容量）。
- `WakerSlots_`：多等待者去重、满时返回 `Busy`、关闭唤醒。
- `dock_index_`：`BPlusTree` / `BTree` 与 `BTreeMap` 在 get / insert / remove / 全序迭代 /
  `range` 边界（Included / Excluded / 空区间）上的等价性；随机 churn 后无泄漏、无重复；
  `take_pending_inbound` 的 `range_mut` 顺序与 Pending → HandedOut → 释放/撤销正确；
  `DockCtx_` 的 `unbind_requested_` 生命周期正确。
- `main_loop` 状态机：用 `new_test_` + 手动 `step_()` 驱动，不起真实 socket：
  - accept 时 `OPEN(message)` → 对端 `OPEN/ACCEPT` → incoming welcome 的帧序列；
  - listener 入向 / 配额拒绝；
  - 对端 CLOSE 后 demux/mux 的位扫描；
  - timer 只注入 `HINT_MAIN_TIMER`，main 的 tick 推进保活 / 空闲 / 关闭；
  - `read_message_async`：响应方 accept 前读 OPEN message；发起方 accept 后读 welcome；
    reply abandon 时 `TakeInbound` 的请求仍可被下一次 `income_async` 取到；
  - 优雅关闭的 finalize 顺序；
  - Drop 并发置位 + retired 扫描。

### 14.2 集成测试

- 现有 `inmem_mux` / `smoke_*` / `cross_thread_*` / `keepalive` 三后端全通过。
- 新增：
  - 256 并发 open/accept/reject 与数据混合（验证请求队列背压、公共 chunk 优先、无死锁）；
  - 公共 TX / RX ring 写满与回绕（验证背压不丢控制帧、不无限增长、半 chunk 不交错）；
  - 大量子流 churn + Drop（验证 retired 扫描、无 `SessionMailbox_`）；
  - pending message ring 容量边界 / 超长 message / 多个 Listener pending 并行；
  - 建流完成后 message ring 被释放、不泄漏；
  - 长时间 dock / channel churn（插入/删除比例高）下 `SlabList` chunk 数稳定、没有
    按条增长的分配；与旧 `BTreeMap` 版本对比峰值 / 碎片。

### 14.3 分配断言

在测试配置的分配器上装计数器：

- `mux_loop` / `demux_loop` 稳态：0 次分配；
- `main_loop` 处理 PULSE / WINDOW_UPDATE / 数据活动：0 次分配；
- `Drop` 路径：0 次分配；
- 仅允许：连接建立、索引树 `SlabList` 的 **chunk** 分配（chunk 内槽位复用，不按条分配）、
  每次控制请求的 `Tcs_`、每个 pending channel 建 message ring 时的两次
  `new_buffered_channel`（buffer + block）。

### 14.4 超时

- 单元测试用脚本包 60 s 超时；
- 全量测试 5 min 超时；
- 任何新增的“等背压”用例必须有明确的看门狗，避免把死锁写成超时。

---

## 15. 风险与待拍板

### 15.1 风险

1. **建流期 message ring 的容量与超长策略**：默认 512 字节可能小于应用期望的
   message / welcome / reason；超长时 outgoing 报 `MessageTooLong`、incoming 回
   `REJECT`。需要在 P3 前明确默认值与错误语义，避免“建流成功但消息被拒”的歧义。
2. **`main_loop` 单点串行化**：所有控制请求排队；如果某个请求处理需要等 TX ring
   空间 / inbox 空位，会队头阻塞后续请求。控制是冷路径，但关闭时可能积累；需要用
   `K_MAIN_REQ_BATCH` 与 timer 注入的优先级缓解。
3. **公共 RX ring 背压传导**：`demux_loop` 在公共 RX ring 写满时停读 `ConnRx`，数据面一起暂停。
   这是正确背压，但如果 `main_loop` 被一个慢请求卡住，整条连接会停。需要保证
   `main_loop` 的处理不超过 O(1) 且不 `await`。
4. **请求等待者上限**：`WakerSlots_` 满时返回 `Busy` 是新的失败模式；需要评估并发
   API 调用数。
5. **迁移期双路径**：旧事件通道与新公共子流并存时可能重复发控制帧；每阶段必须
   一次性切换一条路径，不能长期双写。
6. **测试迁移成本**：现有测试大量直接调 `ChannelRegistry_` 的异步方法；写者收敛后
   需要改为 `main_loop` 的同步方法或测试专用步进接口。
7. **`DockCtx_` 的 unbind 标志**：每绑定一次 `Shared<AtomicBool>` 分配；如果绑定 churn
   很高，需要池化。`DockCtx_` 本身仍按值在树里，main 用 `get_mut` 改计数。
8. **`timer_loop` 保留后的收尾判据**：循环数回到 4，`MuxCore` 的收尾必须覆盖 4 个取消
   令牌；timer 退出前要确保 main 已经不再依赖它（先取消 main、再取消 timer，或让
   timer 在 main 退出后自然被取消）。
9. **`demux` 只读锁竞争**：main 写身份表与 demux 只读初审共用一把协作式读写锁。控制帧
   是冷路径，但高 churn 时读锁可能拖慢 main 的写。需要基准验证；若不可接受，再考虑
   只读快照（`IdentityView_` + 版本号）。
10. **chunked 半 chunk**：正常路径用“总空闲 ≥ 2+len”避免写半 chunk；连接收尾时可能
    留下半 chunk，必须保证此时两端都在退出、不会有人再解析它。
11. **建流期 ring 的分配次数**：每个 pending channel 两次 `new_buffered_channel`（buffer
    + ring block 各一次），高 churn 下分配次数明显；P6 需要评估池化，或把一对 ring 合并
    到同一块 slab。
12. **`read_message_async` 的语义**：发起方要等 `ACCEPT` 到达后才能读到 welcome，响应方
    可在 `accept` 前读 `OPEN` message；handle 在 `accept` 后仍保留 app 侧 ring。需要
    用用例钉住“什么时候可读、读不到时等还是立刻返回空”。
13. **`range` / Comparer 语义**：新 API 的范围方向由 Comparer 决定；`OrdComparer` 下
    与 `BTreeMap` 一致，但自定义 Comparer 下“起点晚于终点”返回空迭代器而不是 panic。
    迁移验收必须与 `BTreeMap` 对拍边界（Included / Excluded / 空区间 / 反向 Comparer）。
14. **`DockCtx_` 的 unbind 标志生命周期**：`Shared<AtomicBool>` 由 `DockBinding` 与
    `docks_` 表项共同持有；表项在 binding 存活期间不能被 `prune_dock_` 掉，否则
    `Drop` 置位后 main 再也找不到它。需要用例覆盖“绑定后无子流 / 只剩宽限墓碑”两种
    情形。
15. **`TreeError` 的构造失败**：`BPlusTree::new_in` / `BTree::new_in` 返回
    `Result<_, TreeError>`；`MuxConnection::new` 目前不返回 `Result`。需要决定是
    `expect`（配置错误即 panic）还是把构造改成 `try_new` / 加连接级错误（Q15）。
16. **chunk 常驻与 degree**：`SlabList` 的 2 KiB chunk 在树非空期间常驻；degree 过小
    会增加节点数，过大则单节点浪费。需要按 registry 规模 / 8~32 的 degree 做基准（Q16）。
17. **每连接树数量 / 固定 chunk 开销**：≈10 棵树 × 2 KiB（B+ 两个 arena 时更多）会在
    小连接上放大固定内存；必须先做基准，必要时缩小迁移范围或给上游加可配置 chunk。

### 15.2 待拍板

> **R18 状态：Q1–Q16 已全部按上表推荐值采纳**（实施记录见 §17；Q8 的「先答 TCS 再
> 取消」见 R10）。下表保留为决策记录与依据，不再是待办。

| # | 问题 | 推荐 |
| --- | --- | --- |
| Q1 | OPEN 是否仍然在 `accept` 时发？ | **是**；message 由 pending ring 保存，不需要提前，也不需要 0 窗口 |
| Q2 | `pending_msg_capacity` 默认值与是否可配置？ | 512 字节/方向，`TrConnCfg` 方法可覆盖；上限 `min(max_packet_size - K_MAX_FRAME_HEADER, K_MAX_CHUNK_PAYLOAD)` |
| Q3 | 是否允许新增公开错误变体 `MessageTooLong` / `Busy`？ | 允许（`BindingError` / `HandleError` / `BindError` / `ListenerError` 各自需要的变体）；否则用现有 `MuxError::FrameTooLarge` / `Closed` 近似 |
| Q4 | `PULSE` / `WINDOW_UPDATE` 是否允许 demux 快路径写 owner？ | **允许**；否则控制面会成为数据热路径瓶颈 |
| Q5 | `Tcs_` 用 `Shared<_, A>` 还是 `Arc`？ | `Shared<_, A>`，与项目分配器一致；池化时替换 |
| Q6 | `reusable_docks_` 的 `VecDeque` 是否本期改内联环形？ | 本期保留，P6 处理 |
| Q7 | 优雅关闭的“最后一条子流”判据是否仍由 `maybe_release_` 负责？ | 改由 `main_loop` 的 `claim_release_` + `is_closing_` 负责；`mux` 只搬 + 置位 |
| Q8 | `timer_loop` 保留后，`MuxCore` 的收尾判据是否覆盖 4 个令牌？ | 覆盖 4 个；先完成 `Close` 的 TCS，再取消 demux / mux / timer / main |
| Q9 | `demux` 只读身份表用共享读写锁，还是做无锁快照？ | 先用共享读锁（实现简单、控制帧冷路径）；基准显示竞争后再做快照 |
| Q10 | message ring 的分配器是否必须用**不同关联类型**？ | 先复用 `Self::Alloc`（`pending_msg_allocator` 可覆盖实例）；要不同类型再加 `MsgAlloc`（破坏性） |
| Q11 | incoming message 超过 `pending_msg_capacity` 时怎么办？ | 回 `REJECT` 并释放身份，不让整条连接失败；outgoing 报 `MessageTooLong` |
| Q12 | `read_message_async` 的公开形状？ | 见 §9.3 第 6 条：`fn read_message_async(&mut self, sink: &mut W) -> ...`，返回字节数 |
| Q13 | Listener 是否共享一条 message ring？ | **不共享**；每个入向 pending channel 一对，避免容量被并发请求打满 |
| Q14 | 是否还需要 dock 计数 / 侵入链表作为 range 的优化？ | **不需要**；`range` / `range_mut` 已够。计数只在基准显示 `has_any_identity` 成为热点时再加 |
| Q15 | `TreeError` 构造失败怎么处理？ | 先 `expect`（容量/degree 是配置错误，与 OOM 同级）；若需要可诊断，再加 `try_new` / 连接级错误 |
| Q16 | degree、chunk 大小与迁移范围？ | 先 `K_DOCK_TREE_DEGREE = 16`、**本 crate 默认 2 KiB chunk**（`K_DOCK_TREE_CHUNK_SIZE`）；按“每连接树数 × chunk”基准再调 |

---

## 16. 文档状态

| 文档 | 状态 |
| --- | --- |
| `event-loop-redesign-20261026-1930.md`（本文） | **设计稿（已评审）；P0–P5 完成（R17：事件通道与邮箱全部删除、timer 只计时、mux 只搬数据）** |
| `connection-delivery-20261026-1845.md` | 现行：现状矩阵；本文定稿后应把 §3 标为“历史基线” |
| `event-backpressure-20261026-0514.md` | 现行：位化与内联队列的实施记录；本文的 P4/P5 会取代其阶段 3/4 |
| `no-vec-audit-20261024-0008.md` | 历史：原则仍有效；本文 §10/§11 是其落地清单 |

评审通过后，本文升级为“现行”，并把 `connection-delivery` 的“待决问题”逐条标注
“已由 event-loop-redesign 解决 / 保留”。

---

## 17. 本稿修订记录

- 20261026-1930：初稿。
- 20261026-2000：按评审意见修订三处：
  1. 公共子流从“帧槽数组 / 每槽定长帧”改为**普通 ring + chunked 内部协议**
     `[u16 大端长度][整帧]`；`mux_loop` 读长度、只搬 payload，不再引入新的 buffer 类型；
  2. **保留 `timer_loop`** 为独立计时服务：它只负责 `delay` 到期后向 `main_loop` 注入
     `HINT_MAIN_TIMER`，决策仍在 main；
  3. **`demux_loop` 恢复查表**：帧级 + 路由级合法性初审仍由它承担，身份表只读、写者
     仍唯一为 `main_loop`。
- 20261026-2040：按“建流期 message ring”意见修订：
  1. 每个未完成建流的 channel（Listener 入向 pending + 发起方尚未发 `OPEN`、尚未
     `accept`）各有一对**小容量普通 ring**，容量与分配器由 `TrConnCfg` 决定；
  2. `OPEN` message / `ACCEPT` welcome / `REJECT` reason 都走这对 ring，删除 `CtrlCell_`
     与 `Vec<u8>` 缓存；Listener 不为所有入向请求共享一条 ring；
  3. `OPEN` 仍在 `accept` 时发（message 由 ring 保存到 accept），不再需要“提前 OPEN +
     0 窗口”的协议调整；
  4. 新增公开 API `TrChannelHandle::read_message_async` 读 incoming message（需拍板）。
- 20261026-2110：追加**索引迁移**与**重构任务总清单**：
  1. registry / dock 相关 `BTreeMap` / `BTreeSet` 改为 `bplus_tree_cmp` 的 `BPlusTree` /
     `BTree`（SlabList 64 KiB chunk）；当时 checkout 尚未提供 `range`，计划用计数 +
     侵入链表消除；
  2. 新增 §18（选型、范围替代、本地表 `iter_mut` 缺口）与 §19（T1–T30 清单）；
  3. 新增 Q14–Q16（`iter_mut` / `TreeError` / degree-chunk）。
- 20261026-2200：本地 `bplus_tree_cmp` 已补 `range` / `range_mut` / `iter_mut` /
  `keys` / `values` / `values_mut` / `contains_key` / `clear`（commit `6163209`）。
  §18 简化为**直接替换**：删除 dock 计数 / 侵入 pending 链表方案；`take_pending_inbound`
  直接用 `range_mut`，数据面本地表直接用 `iter_mut` / `range_mut`；Q14 改为“是否还需
  优化”的问项。

---

- 20261027-0140：**P0–P3.3a 实施汇总**（逐提交细节见 `git log`）。
  - **P0**：`tcs_.rs` / `WakerSlots_` / `PublicChunkIo_` / `PendingMsg` /
    `TimerSignal_` 原语与 22 个单测。
  - **P1**：`main_loop` 接管 bind / listen / reserve；删除 `ctl_`；定容请求队列 +
    `Tcs_`；索引树 2 KiB chunk 配置（`tree_config_`）。
  - **P2**：公共子流两条 ring + 四路唤醒槽；`CLOSE` 全链路（demux 写 RX、main
    处理、mux 搬 TX）。
  - **P3.1a**：每 pending channel 一对 message ring（`TrConnCfg` 默认 512）；
    `reserve_channel_` / `reserve_inbound_` / `take_pending_inbound_` 接线。
  - **P3.1b**：`SendControl` 统一 `OPEN`/`ACCEPT`/`REJECT` 的发送；mux 先搬公共帧再
    处理事件；修复 `FIN` 超车 `OPEN` 的启动竞态。
  - **P3.2**：`Accept`/`Reject` 请求化；`main` 读 message ring 组帧；响应方接管
    `take_inbound_report_`；删除 `ChannelHandle::message_`。
  - **P3.3a**：`read_message_async` 公开 API + app 侧读取（main 填充 incoming 待
    P3.3b）。
  - 期间修复：`InlineQueue_::poll_pop_` / `HintSlot_::poll_wait_` 的丢唤醒；
    `park_main_` 把公共 RX 唤醒误判为退出；`mux_auto_dock_allocation_dual_` 的
    `FIN/OPEN` 顺序竞态。
  - **P3.3b 首次尝试已回退**（见下一条记录），工作区停在此处。

- 20261027-0150：**P3.3b 前置：公共 TX 顺序闸门前移**。
  `mux_loop` 在 `take_write_hint_` **之前**先 `flush_public_chunks_`，因此任何由写提示
  触发的 `RESET` / `FIN` 都排在已入公共 TX 的 `OPEN` / `ACCEPT` 之后；2.5 的
  `finalize_entry_` 之前仍保留一次 flush。现有测试（lib 370 / inmem 20 / smoke 5 /
  cross_thread 1 / keepalive 5）全过；完整的入向迁移（demux 写 RX + main 处理
  `OPEN/ACCEPT/REJECT`）仍待 P3.3b。

- 20261027-0220：**P3.3b 入向迁移完成**。
  1. `demux` 对 `OPEN` 只做窗口字段校验后写公共 RX；读表里存在的 `ACCEPT` /
     `REJECT` 同样写公共 RX（无表项按在途帧静默丢弃）；直接状态推进删除。
  2. `main` 读整帧（header + payload）：`OPEN` 有 owner → 装窗口、`PEER_OPENED`、
     唤醒建流、写 incoming；`OPEN` 无 owner → `reserve_inbound_` + 写 incoming，
     配额 / 重复时回 `REJECT`；`ACCEPT` / `REJECT` → 置 outcome、写 welcome/reason、
     唤醒。
  3. `registry_` 新增 `write_msg_incoming_`；`read_message_async` 的 incoming 至此
     有生产者（响应方读 `OPEN` message、发起方读 `ACCEPT` welcome）。
  4. 前一条顺序闸门（mux 在写提示前 flush 公共 TX）解决了上一轮的
     `OPEN -> CLOSE -> OPEN` 超车；`mux_auto_dock_allocation_dual_` 恢复。
  5. 验收：lib 370 / inmem 20 / smoke 5 / cross_thread 1 / keepalive 5；
     clippy / format 合规。

- 20261027-0245：**P4 前置：`dock_index_` 别名 + 2 KiB 等价性**。
  `K_DOCK_TREE_DEGREE = 16`；`DockBPlusTree_` / `DockSet_` 以 `DockTreeCfg_`（2 KiB）
  为配置。单测与 `BTreeMap` 对拍 get / insert / iter / range / remove 以及 chunk
  大小。具体表（`bindings_` / `docks_` / `remote_index_` / `wait_close_expiry_` /
  本地 `WriteTable_` / `ReadTable_`）的迁移仍待 P4。

- 20261027-0320：**P4 第一刀：listener 的 Drop 位化**。
  1. `LsnRec_` 加 `retired_` 原子位；`ChannelListener::Drop` 只 `set_retired_()` +
     `raise_main_hint_(HINT_MAIN_RETIRED)`，不再投 `SessionEvent_::ReleaseListener`。
  2. `ControlState_` 定义 `HINT_MAIN_RETIRED`；`main_loop` 在提示与每条请求前调用
     `ChannelRegistry_::reap_retired_`（遍历 `bindings_` 的 Listener 变体并释放）。
  3. 删除 `SessionEvent_::ReleaseListener` 变体与 drain 分支。
  4. 验收：lib 371 / inmem 20 / smoke 5 / cross_thread 1 / keepalive 5；
     clippy / format 合规。

- 20261027-0350：**P4 第二刀：telegraph 的 Drop 位化**。
  1. `TgRec_` 加 `retired_`；`TelegraphCtxHolder_::drop` 在最后一份强引用时置位 +
     `HINT_MAIN_RETIRED`（不再投 `SessionEvent_::ReleaseTelegraph`）。
  2. `reap_retired_` 同时扫描 `bindings_` 的 Listener / Telegraph 变体并释放。
  3. 删除 `SessionEvent_::ReleaseTelegraph` 与 drain 分支；`endpoint_` 的登记失败
     回滚也改用 retired 位。
  4. 验收：lib 371 / inmem 20 / smoke 5 / cross_thread 1 / keepalive 5；
     clippy / format 合规。

- 20261027-0410：**`ChannelHandle` 未裁决 Drop 位化首次尝试回退**（记录，不落地）。
  1. 尝试：`ChannelHandle::Drop` 只置 `RETIRED` + `HINT_MAIN_RETIRED`；
     `reap_retired_` 按 `inbound_` 区分发起方（`unreserve_channel_`）/ 响应方
     （main 发 `REJECT` 后 `release_channel_`）。
  2. 结果：`mux_unsettled_handle_dual_` 挂起（>60s），回退到 Telegraph 位化
     （`32d437d`）。
  3. 下次尝试前先钉住三件事：
     - 未裁决 Drop 时 `ChanSlot_.msg_main_` / `msg_app_` 的关闭顺序；
     - `reap_retired_` 锁外 `unreserve_channel_` 与 `handle_request_` 的交互
       （重入 / hint 丢失）；
     - 响应方 `REJECT` 与 `release_channel_` 的先后与取消语义。

- 20261027-0440：**P4 前置：`DockCtx_` 泛型化 + `unbind_requested_`**。
  `DockCtx_<A>` 内嵌 `Shared<AtomicBool, A>`；条目创建时用注册表分配器建一次。
  bind / unbind 行为不变，`DockBinding` 的 Drop 位化在下一步接入。
  验收：lib 371 / inmem 20 / smoke 5；clippy / format 合规。

- 20261027-0500：**DockBinding 位化首次尝试回退**（记录，不落地）。
  1. 尝试：`bind_dock_` / `bind_any_dock_` 返回 `(Dock, Shared<AtomicBool, A>)`；
     `DockBinding::Drop` 只置位 + `HINT_MAIN_RETIRED`；`reap_retired_` 扫描
     `docks_` 的 `unbind_requested_`，锁外 `unbind_dock_`。
  2. 结果：`mux_bind_is_exclusive_dual_` 在「drop 后立刻重绑」处得到
     `DockInUse`——retired 解绑没有在 Bind 请求处理前落实（或标志可见性 / 时序
     有误）。回退到 `4f081cd`（`DockCtx_` 泛型化）。
  3. 下一步：先用 `new_test_` + 手动 `step_` 驱动 main，钉住
     「Drop → reap_retired_ → unbind_dock_」的顺序与返回值，再接入真实循环。
     这与 `ChannelHandle` 位化失败（上一节）是同一类问题：**main 的手动驱动
     测试缺失**，导致时序只能靠集成测试试错。

- 20261027-0530：**P4 索引迁移：`PendingFin_` 换 2 KiB B 树**。
  `type PendingFin_<A> = DockSet_<A, (Dock, Dock)>`；初始化用 `DockSet_::new_in`；
  `insert` 的 bool 语义用 `.is_none()`，`iter` / `range` 适配 `(&K, &V)` item 形态。
  验收：lib 371 / inmem 20 / smoke 5；clippy / format 合规。
  本地表 `WriteTable_` / `ReadTable_` 已迁移；registry 四表受 `Send` 限制保留标准库容器（见 0800 记录）。

- 20261027-0600：**P4 索引迁移：telegraph 两表换 2 KiB B+ 树**。
  `TgReadTable_` / `TgWriteTable_` -> `DockBPlusTree_<A, Dock, ...>`；
  `get_mut` 适配 `(&K, &mut V)`、`remove` 按键值、`iter_mut` item 形态不变。
  验收：lib 371 / inmem 20 / smoke 5；clippy / format 合规。

- 20261027-0630：**P4 测试前置：`main_step_` 与 `new_test_bundle_`**。
  1. `main_loop_async_` 的循环体抽成 `main_step_`（非阻塞一轮：drain、提示、
     公共 RX 帧、请求批）；循环只负责 `step + park`，返回 `false` 表示应退出。
  2. `new_test_bundle_` 同时交出 `MuxConnection` 与 `MuxShared_`，供测试手动驱动；
     `MainPublicRead_` 提升为 `pub(crate)`。
  3. 行为不变；lib 371。下一步用它钉住「Drop → reap → unbind / unreserve /
     REJECT」三条时序，再重试 DockBinding / ChannelHandle 位化。

- 20261027-0700：**P4 索引迁移：`WriteTable_` 换 2 KiB B+ 树**。
  `type WriteTable_<A> = DockBPlusTree_<A, (Dock, Dock), WriteEntry_<A>>`；mux 的
  `get` / `get_mut` 适配 `(&K, &V)`，`remove` / `contains_key` 改用值键；
  测试辅助建表改用 `DockBPlusTree_::new_in`。
  验收：lib 371 / inmem 20 / smoke 5；clippy / format 合规。
  `ReadTable_` 已迁移；registry 四表受 `Send` 限制保留标准库容器（见 0800 记录）。

- 20261027-0730：**P4 索引迁移：`ReadTable_` 换 2 KiB B+ 树**。
  `type ReadTable_<A> = DockBPlusTree_<A, (Dock, Dock), ReadEntry_<A>>`；demux 的
  `get` / `get_mut` 适配 `(&K, &V)`；两表统一后，全部 `table.*(&pair)` 借用改为
  值键（B+ 树 `Q: Borrow<K>`）。
  验收：lib 371 / inmem 20 / smoke 5；clippy / format 合规。
  registry 四表受 `Send` 限制保留标准库容器（见 0800 记录）。

- 20261027-0800：**P4 索引迁移的边界：`BPlusTree` / `BTree` 曾不是 `Send`**。
  1. 当时把 `remote_index_` / `wait_close_expiry_` 换 `DockBPlusTree_` / `DockSet_`
     后，`RegistryInner_<CoreAlloc>` 不再 `Send`，`cross_thread_compio` 编译失败
     （arena 含 `NonNull`，上游未 `unsafe impl Send`）；已回退。
  2. 当时的结论是「registry 四表换 arena 树不可行」；**该结论已作废**：上游
     `d65e5ec` 已补 `Send` / `Sync`，见下一条 0900。
  3. 本地数据面表（`WriteTable_` / `ReadTable_` / `TgReadTable_` / `TgWriteTable_`）
     在循环私有的 `MuxShared_` 中，`PendingFin_` 在循环栈上，均不受此限，已迁移。

- 20261027-0900：**上游补 `Send` / `Sync`，registry 索引继续迁移**。
  `bplus_tree_cmp` `d65e5ec` 为 `BPlusTree` / `BTree` 实现 `Send` / `Sync`
  （约束：`Cfg` / `Allocator` / `K` / `V` 均 `Send + Sync`；`DockTreeCfg_<A>` 与
  我们的键值类型满足）。`remote_index_` / `wait_close_expiry_` 已换
  `DockBPlusTree_`（值 `()`），`cross_thread_compio` 通过；`docks_` 已换
  `DockBPlusTree_<A, Dock, DockCtx_<A>>`；`bindings_` 已换
  `DockBPlusTree_<A, (Dock, Dock), BindingSlot_<A>>`。**registry 四表全部完成**
  2 KiB B+ 树迁移；`BTreeMap` / `BTreeSet` 只剩临时统计集合与测试辅助使用，
  `btreemap_alloc` feature 已删除。验收：lib 371 / inmem 20 / smoke 5 /
  cross_thread 1。

- 20261027-1000：**P5 的 `WakerSlots_` 首次尝试回退**（记录，不落地）。
  1. 把 `InlineQueue_.free_waiters_` 从 `Vec<Waker>` 换成 `WakerSlots_<16>`
     （并加 `PushErr_::Busy` 与 `BindError` / `BindingError` / `HandleError` 的
     `Busy` 变体）。
  2. `cross_thread_compio` 20 s 超时；诊断到 115 次 `PushErr_::Busy`——跨线程场景
     的并发等待者远超定容槽（16 / 64 均不足），而既有调用方与测试不重试 `Busy`。
  3. 结论：设计稿 §12.3 的「等待者上界 16」假设不成立。要落地 P5 的这项，必须先
     定清楚：(a) 等待者上界与 `Busy` 的公开重试语义；或 (b) 定容槽 + 溢出 `Vec`
     的混合策略；或 (c) 更大容量并接受内存换无分配。
  4. 本轮回退，`Vec<Waker>` 保留；工作区停在 `2bb65e1`（registry 四表完成）。

- 20261027-1100：**P4 位化：`DockBinding` 的 Drop 位化完成**。
  1. `bind_dock_` / `bind_any_dock_` 返回 `(Dock, Shared<AtomicBool, A>)`；
     `MainReply_::Dock { dock, unbind_ }` 携带该标志。
  2. `DockBinding::Drop` 只 `store(true)` + `HINT_MAIN_RETIRED`，不再投
     `SessionEvent_::UnbindDock`；该变体与 drain 分支删除。
  3. `reap_retired_` 扫描 `docks_` 的 `unbind_requested_`，在写锁外调用
     `unbind_dock_`。
  4. 新增 `main_step_` 时序测试 `drop_then_step_allows_rebind_`（drop 后立刻重绑）。
  5. 验收：lib 372 / inmem 20 / cross_thread 1 / smoke 5；clippy / format 合规。

- 20261027-1200：**P4 位化完成：`ChannelHandle` 的 Drop 位化**，并修复提示位丢失。
  1. `ChannelState_` 加 `HANDLE_RETIRED`（bit 18）；`ChannelHandle::Drop` 只
     `set_handle_retired_()` + `HINT_MAIN_RETIRED`，不再投 `SessionEvent_`。
     `abort_pending_` 仅保留给 `accept_async` 的**显式失败**分支。
  2. `reap_retired_` 增加 Channel 扫描：发起方（`inbound_ == None`）锁外
     `unreserve_channel_`；响应方返回 `(local, remote)` 列表——`REJECT` 需要公共
     TX 写端，只有 main 有。main 的 `reap_retired_and_release_` 先写 `REJECT`、
     再 `release_channel_`（设计稿 §6.7 的顺序）。
  3. **根因（上次挂起的原因）**：`park_main_` 的 `poll_main_hint_` 是「取出并返回」
     语义，主循环的 `MainWake_::Hint(_bits)` 分支却直接丢弃了 `_bits`；下一轮
     `main_step_` 的 `take_main_hint_` 得到空掩码，于是 retired 提示**只唤醒、
     从不落实**。此前 `DockBinding` 位化能过，是因为每条请求前都会再 `reap` 一次
     （有后续请求才掩盖了问题）；响应方 Drop 之后只等对端 `REJECT` 的场景没有后续
     请求，于是永久挂起。修复：`Hint(bits)` 分支 `ctl.raise_main_hint_(bits)` 回放。
  4. 新增单测 `drop_pending_responder_sends_reject_`：用 `new_test_bundle_` +
     `reserve_inbound_` / `take_pending_inbound_` 造响应方待决句柄，drop 后先 poll
     `park_main_` 拿提示并回放，再 `main_step_`；断言身份进 `WaitClose` 且公共 TX
     第一帧是 `REJECT`。
  5. 顺带修复既有格式违规：`handshake/mod.rs` 两条同根 `pub(crate) use magic_::…`
     合并为一条（基线 `format_use_imports.py --check` 未过，与本次改动无关）。
  6. 验收：lib 373 / inmem 20 / cross_thread 1 / smoke 5 / keepalive 5；
     clippy 0 error；`format_use_imports.py --check` 全绿（188 文件）。
  7. **仍未等价于设计稿的一处**：发起方在 `accept` 中已发出 `OPEN` 之后才取消，
     当前仍按「未发 `OPEN`」走 `unreserve`（与位化前的 `abort_pending_` 一致），
     而不是 `reject_after_open_`。需要新增「`OPEN` 已发」位，留待 P5。
  8. P4 剩余：拆流迁移（`PendingFin_` / `LocalAbort` 改 owner 位 + 扫描）。

- 20261027-1300：**P4 拆流迁移第一步：`LocalAbort` 位化**（按裁决先做这一块，
  `PendingFin_` / main 发 FIN 留到下一步与 T1 一起做）。
  1. 新增写提示位 `HINT_W_RELEASED`（`signal_`）：事实是 `ChannelState_` 的
     `RELEASED`，提示只负责唤醒。
  2. 计时循环的 `TimerAction_::Abort` 不再投 `WriteEvent_::LocalAbort`，改为置位 +
     `raise_write_hint_(HINT_W_RELEASED)`（读方向仍 `HINT_R_RELEASE`）。
  3. mux 新增 `scan_write_released_`：按写表扫描 `is_released_()` 的条目，
     **显式关闭发送环消费端**、摘写表、`pending_fin.remove`、`maybe_release_`
     ——与原事件分支的动作一一对应。与 `scan_peer_closed_` 的分界（RESET 不关
     消费端）保持原注释。
  4. 删除 `WriteEvent_::LocalAbort` 变体、`handle_write_event_` 的处理分支，以及
    该函数因此不再使用的三个参数（`table` / `pending_fin` / `read_events`）。
  5. 覆盖确认：临时探针统计到 `keepalive_compio` 的空闲超时用例会走到
     `TimerAction_::Abort`（1 次），`inmem` / `smoke` 为 0；`mux_graceful_close_timeout_dual_`
     走的是**连接关闭**动作（`close_action_`），不经过逐子流 `Abort`。
  6. 验收：lib 373 / inmem 20 / cross_thread 1 / smoke 5 / keepalive 5；clippy 0
     error；`format_use_imports.py --check` 全绿。
  7. P4 剩余：`PendingFin_` → `TX_DRAINED` 位 + main 发 `CLOSE(FIN)`（与 §8.1 / T1
     绑定，属于关闭状态机主体迁移）。

- 20261027-1400：**P5 第一步：请求队列换 `mpmc_chan`（Arena 等待队列）**。
  1. 背景：`InlineQueue_::free_waiters_: Vec<Waker>` 的等待者集合无上界；定容
     `WakerSlots_` 方案需要 `Busy` 契约，而「单连接并发建流数」没有依据。改用
     `mpmc_chan`：等待者逻辑无界，物理上是 chunked `SlabList`（4 KiB chunk），
     节点复用、取消/丢弃时归还 slab，不反复触碰全局分配器。
  2. `ControlState_.requests_` 从 `InlineQueue_<MainRequest_, 64>` 换成
     `mpmc_chan::{Sender, Receiver}`；通道的外层 `Arc` 与等待 chunk 都用**连接
     分配器**（`ControlChanCfg_` 的 `ChanAlloc = A`），不落全局堆。
  3. `park_main_` 改为在 `poll_fn` 外 `into_future` + `pin` 一个 `RecvFuture`
     跨 poll 持有（`RecvAsync` 是参数载体，不是 `Future`）；快路径先
     `try_pop_request_`，避免空通道时 `recv` 先做 512 次自旋。
  4. `poll_pop_request_` 删除；`close_requests_` 改用 `Receiver::close`（此后
     `send` 返回 `Closed`，`try_recv` 仍可 drain）。`Get`/`Cancelled` 映射回
     `PushErr_`，调用方语义不变。
  5. 验收：lib 373 / inmem 20 / cross_thread 1（256 并发无 Busy、无挂起）/
     smoke 5 / keepalive 5；clippy 0 error；format 合规。
  6. 剩余：`mux_inbox_` / `demux_inbox_` 两条 `Attach` inbox 仍是 `InlineQueue_`，
     下一步同样替换；`WakerSlots_` 方案（T22）据此搁置——请求队列已不需要它。

- 20261027-1500：**P5 第二步：两条 `Attach` inbox 换 `mpmc_chan`**。
  1. `mux_inbox_` / `demux_inbox_` 从 `InlineQueue_<_, 64>` 换成
     `mpmc_chan::{Sender, Receiver}`；`MuxChanCfg_` 抽到 `chan_cfg_.rs`，请求队列
     与两条 inbox 共用同一套调教（连接分配器 + 4 KiB 等待 chunk）。
  2. `push_*_attach_async_` 用 `send_async().may_cancel_with(cancel)`；
     `try_push_*` 把 `TrySendError::{Full, Closed}` 都还原成「原样带回」，
     `poll_*_attach_` 改为 `recv_*_attach_async_`（`RecvAsync` 载体）。
  3. `park_read_wake_` 与 mux 的 park 各多 pin 一个 `Attach` 的 `RecvFuture`；
     与事件通道 future 同寿命、跨 poll 持有。
  4. **公开 trait 约束收紧（需评审）**：`TrConnCfg::Alloc` 增加 `'static`。
     原因：`MpmcChan<'static, …>` 的 `Sender` / `Receiver` 把分配器类型嵌进类型，
     连接实例要求分配器活到 `'static`。所有现有实现（`CoreAlloc` 等）满足；但这是
     公开 trait 的关联类型约束变更。
  5. `InlineQueue_` / `SlotRing_` / `WakerSlots_` 在**生产路径**已无使用者（只剩
     `slot_.rs` 自己的单测）；`WakerSlots_ + Busy` 方案正式搁置。它们作为 T22 的
     清理项待删。
  6. 验收：lib 373 / inmem 20 / cross_thread 1 / smoke 5 / keepalive 5；
     clippy 0 error；format 合规（189 文件）。

- 20261027-1530：**T22 清理：删除 `InlineQueue_` / `SlotRing_` / `WakerSlots_`**。
  1. 三条通道换 `mpmc_chan` 后，这三个原语在生产路径已无使用者（只剩 `slot_.rs`
     自己的单测）。删除 `SlotRing_`、`InlineQueue_`（含 `push_async_` 背压与
     `free_waiters_`）、`WakerSlots_`，以及对应的 5 个队列测试与 2 个槽测试。
  2. `slot_.rs` 只剩 [`PushErr_`]（请求通道的失败形状，由 `SendError` 映射）与
     [`HintSlot_`]（连接级合并提示）；模块文档改为说明它们为何留下。
  3. 验收：lib 360（删 13 个测试变体后的数量）/ inmem 20 / cross_thread 1 /
     smoke 5 / keepalive 5；clippy 0 error；format 合规（189 文件）。

- 20261027-1600：**分配器配置改为 GAT（撤销 `TrConnCfg::Alloc` 的 `'static`）**。
  1. 先前的做法（给 `TrConnCfg::Alloc` 加 `'static`）是错误设计：`'static` 需求来自
     「分配器实例被嵌入 `Shared` 身份节点与 `MpmcChan<'static, …>` 的元素类型」，
     不应该把这条约束压到通用的结构分配器上，也不应该让 `TrConnCfg` 去实现
     `TrMuxAllocConfig`。
  2. 新增 `TrConnAllocCfg`（**不带 `'static` supertrait**），聚合连接内所有分配器
     用途：`RegistryAlloc` / `ChanOwnerAlloc` / GAT `MpmcAlloc<'a>`。只有
     `MpmcAlloc` 的 GAT 带生命周期约束（`MpmcAlloc<'static>: 'static`），`'static`
     需求停在通道这一层。
  3. `TrConnCfg` 通过 GAT `type AllocCfg<'a>: TrConnAllocCfg` 引用它，提供
     `fn alloc_cfg(&self) -> Self::AllocCfg<'static>`；删除 `type Alloc`。保留
     `fn allocator(&self)` 作为 `RegistryAlloc` 的便捷投影。
  4. `ControlState_<A, CA = A>` / `ChannelRegistry_<A, CA = A>` /
     `MuxLoopShared_<A, CA, R, M>`：`A = RegistryAlloc`（结构），
     `CA = MpmcAlloc`（通道）；`MuxCore<C>` 用投影别名
     `ConnAlloc_<C>` / `ConnMpmcAlloc_<C>`。
  5. 三个内建配置写 `type AllocCfg<'a> = SingleConnAllocCfg;`（当时的草稿把它写成
     了 `CoreAllocCfg`，以代码为准）；测试侧 `SmokeMuxConfig` / `FlowCtrlConfig` /
     `MetricsCfg` 也用 `SingleConnAllocCfg`，`CountMuxConfig` 用
     `SingleConnAllocCfg<CountingAlloc>`。
  6. 验收：lib 360 / inmem 20 / cross_thread 1 / smoke 5 / keepalive 5；
     clippy 0 error；format 合规（189 文件）。

- 20261027-1700：**分配器配置迁移第 2 阶段：连接内复合结构全部改为 `C`**。
  1. **目标形状**：连接内的复合结构只带一个分配器配置参数 `C: TrConnAllocCfg`，
     内部按用途投影取分配器实例。`DockHandle_<T, A>`（及 `ChannelOwner_` /
     `TgOwner_` / `LsnOwner_` 三个别名）这类**低层句柄**仍以分配器为参数，使用点
     写 `ChannelOwner_<C::ChanOwnerAlloc>`（与交接示例一致）；`Shared` /
     `CancelToken_` / `MuxChanCfg_` / `DockBPlusTree_` / `DockSet_` 等通用原语
     保持分配器参数——它们本身就是「任意分配器的适配器」，不是连接结构。
  2. **本轮改完的类型**：`WriteAttach_<C>` / `ReadAttach_<C>` / `WriteEvent_<C>` /
     `ReadEvent_<C>`（`signal_`），`ChanSlot_` / `BindingSlot_` / `DockCtx_` /
     `RegistryInner_` / `RegistryRoot_` / `ChannelRegistry_` 与两条 `Attach` 通道
     别名（`registry_`），`MuxLoopShared_<C, R, M>` 与本地表 / 表项 / `PendingFin_` /
     `ReadWake_`（`session_`），`PublicChannel_<C>`，`TgRecvCtx_<C>`；
     `wait_establish_` 改为泛型于 `C`。`MuxShared_<C>` 现在是
     `MuxLoopShared_<ConnAllocCfg_<C>, Rt, Metrics>`。
  3. **六个此前只声明未使用的 GAT 全部接上**：结构 / 索引与四本 B+ 树 =
     `RegistryAlloc`；身份节点与 `DockCtx_::unbind_requested_` = `ChanOwnerAlloc`；
     请求通道 = `MpmcAlloc`；两条 `Attach` inbox = `AttachAlloc`（新增
     `MuxChanCfg_<C::AttachAlloc>`）；本地表 / `PendingFin_` = `LocalTableAlloc`；
     循环 `Owned<[u8]>` 暂存 = `ScratchAlloc`；`MuxCore` 的 `Shared` 与四个
     `CancelToken_` = `CoreAlloc`；公共子流的 `Shared` 与两条 ring = `PublicAlloc`。
     `RegistryInner_` 因此保存四份分配器实例（结构 / 身份节点 / 本地表 / 暂存），
     `ChannelRegistry_::allocator_()` 拆成 `local_table_alloc_()` /
     `scratch_alloc_()`（原来同一个实例被表与暂存共用）。
  4. `MuxLoopShared_` 的 `#[derive(Clone)]` 改成手写：派生会给参数 `C` 也加上
     `Clone` 约束，而配置不应被要求可克隆。
  5. **尚未接线：`EventAlloc`**。事件通道仍是全局 `Box<MpmcBuf>` + `Arc<Overflow_>`，
     即 `connection-delivery` §3 记载的已知例外；接它要把 `EventSender_` /
     `EventReceiver_` 参数化、把 `Box`/`Arc` 换成 `Owned`/`Shared`，会改变
     `alloc_count` 的基线，留作下一步。
  6. **顺带修复 `alloc_count`**：该目标自 `11c19e5` 起**无法编译**（`CountingAllocCfg`
     没有实现新增的 GAT），改成 `SingleConnAllocCfg<CountingAlloc>`；其「绑定 + 监听」
     断言（全局 ≤2 / 注入 =11）是**内联队列时代**的旧基线，在 `de6b699` 与本轮各测
     一次，结果完全一致（全局 11 次 / 65648 B，注入 52 次 / 97888 B），说明本次
     类型参数迁移不改变分配行为；断言更新为 ≤16 / =52，并留下「逐笔归因那 11 次全局
     分配」的待办。
  7. 验收：lib 360 / inmem 20 / cross_thread 1 / smoke 5 / keepalive 5（compio）；
     tokio（inmem 20 / cross_thread 1 / smoke 5）；smol（同左）；`alloc_count` 1；
     clippy 0 error；`format_use_imports.py --check` 189 文件全绿。

- 20261029-0245：**分配器清理 R2：建流期 message 路径去 Global / T23**。
  1. `ControlFrame_` 删除 `payload_: Vec<u8>` 与 `payload_()`，改为 `Copy` 的纯帧头；
     `OPEN` / `ACCEPT` / `REJECT` 载荷由 `main_loop::write_control_frame_async_` 的
     独立 `&[u8]` 参数携带（T23 完成）。
  2. `read_msg_outgoing_` 换成 `read_msg_outgoing_into_`：写入 `main_loop` 从连接级
     Scope 一次分配、每请求复用的固定缓冲；超长在**读正文前**返回
     `ReadOutgoingErr_::TooLarge`。
  3. `read_message_async` 改为 1 KiB 栈上分块，逐块 `enqueue_frame_` 写 sink，不再
     先分配整条 `Vec<u8>`。
  4. `main_step_` / `handle_request_` 增加 `msg_buf` 参数；`main_loop` 启动时分配
     `pending_msg_capacity` 大小的缓冲。
  5. 新增单测 `read_outgoing_into_buffer_rejects_overlong` 与集成测试
     `mux_message_inmem_dual_`（`OPEN` message 非空；welcome 仍为空载荷，受
     `accept_async` 的 `TrBuffWrite` 形状限制，P3.3c 待办）。
  6. 验收：lib **365** / inmem **21**（compio / tokio / smol）/ smoke 5 /
     `alloc_count` 1；基线不变（绑定 + 监听：全局 11 / 注入 33）。

- 20261029-0450：**R5：删除 `PendingFin_`**。
  1. `PendingFin_`（`LocalTableAlloc` 上的 B 树）与 `scan_tx_closed_` 删除；半关闭的
     `FIN` 重试折进 `drain_once_` 的每轮本地表扫描：对
     `APP_TX_CLOSED && !LOCAL_FIN_SENT` 的条目直接 `finalize_entry_`。
  2. 阻塞条目（环里还有数据 / 额度不足 / 段借不出来）不登记集合，下一轮重扫时再试；
     不会饿死后面的条目。
  3. 删除 `pending_fin` 全部参数与 `tx_closed_handled_` probe。
  4. 验收：lib 365 / inmem 21 / keepalive 5 / smoke 5 / cross_thread 1 /
     `alloc_count` 1（建流全局 1→0、拆流注入 6→4）。
  5. **FIN 仍由 mux 直接写 `ConnTx`**；`TX_DRAINED` 位与「main 发 FIN / T1」仍待 R6，
     因为 public TX 的 FIN 完成语义尚未定义。

- 20261029-0520：**R6：`TX_DRAINED` + main 发 FIN**。
  1. `ChannelState_` 增加 `TX_DRAINED` / `FIN_ENQUEUED`；`drain_once_` 在发送环明确
     排空后置 `TX_DRAINED` 并 raise `HINT_MAIN_TX_DRAINED`，不再直接发 FIN；
     `finalize_entry_` / `flush_entry_` 删除。
  2. main `flush_tx_drained_` 把 `CLOSE(FIN)` 写进公共 TX ring，置 `FIN_ENQUEUED`；
     mux 在公共 TX 刷空后 `scan_fin_enqueued_` 置 `LOCAL_FIN_SENT` 并唤醒
     `close_async`。
  3. `MuxLoopShared_` 持 `ControlState_` 句柄，写循环可 raise `HINT_MAIN_*`。
  4. RESET 越序修正：`APP_TX_CLOSED && !LOCAL_FIN_SENT` 时先不发 `CLOSE(RESET)`，
     等 FIN 确认上线后再发；否则对端先收 RESET、后收 FIN，接收方向少一次
     `peer_fin` 释放触发点。
  5. 验收：lib 365 / inmem 21（compio / tokio / smol）/ keepalive 5 / smoke 5 /
     cross_thread 1 / `alloc_count` 1；clippy / doc / format 干净。
  6. **T1 剩余**：mux 仍发 `CLOSE(RESET)` 并做 `maybe_release_` 本地表动作。

- 20261029-0620：**R7：main 发 RESET**。
  1. `ChannelState_` 增加 `RESET_ENQUEUED`；新增 `HINT_MAIN_RESET` /
     `HINT_W_RESET_ENQUEUED`。
  2. `ChannelRx::seal_rx_` raise `HINT_MAIN_RESET`；`scan_reset_pending_` 扫注册表，
     若本端发送方向已封口则等 `LOCAL_FIN_SENT` 之后才返回。
  3. main `flush_reset_` 把 `CLOSE(RESET)` 写进公共 TX ring；mux 在公共 TX 刷空后
     `scan_reset_enqueued_` 置 `LOCAL_RESET_FLUSHED`、唤醒 `ChannelRx::close_async`。
  4. `scan_fin_enqueued_` 在 FIN 上线后 raise `HINT_MAIN_RESET`，释放被 FIN 顺序挡住
     的 RESET。
  5. 删除 `flush_rx_closed_` / `HINT_W_RX_CLOSED` / `control_close_via_`。
  6. 验收：lib 365 / inmem 21 / keepalive 5 / smoke 5 / cross_thread 1 /
     `alloc_count` 1；clippy / doc / format 干净。
  7. **T1 剩余**：mux 仍通过 `maybe_release_` 做本地表释放，并仍处理 timer 的
     `WriteEvent_::Control`。

- 20261029-0740：**R8：身份释放迁到 main（T1 第 1 步完成）**。
  1. `ControlState_` 新增 `HINT_MAIN_RELEASE`；写循环的四个扫描在完成本地动作后只
     raise 它，不再取注册表写锁、不再调 `maybe_release_`。
  2. 注册表新增 `release_done_channels_`：同一把写锁内对每条 `Channel` 尝试
     `claim_release_` 的一次性 CAS，成功者当场转 `WaitClose` 宽限态，返回
     `(释放列表, became_idle)`；「表项已摘、owner 仍在注册表」的兜底路径由全表扫描
     自然覆盖。
  3. `main_loop::flush_releasable_` 承接原 `maybe_release_` 的全部副作用：双向唤醒提示、
     `notify_both_progress_`、`Fin` 的 `on_channel_closed`（恰好一次）、
     `became_idle && is_closing` 时 `cancel_loops_`。
  4. **RESET 顺序**：释放扫描跳过「应用关读但 `LOCAL_RESET_FLUSHED` 未置位」的子流，
     否则 `WaitClose` 会让 `scan_reset_pending_` 再也看不到它，`RESET` 丢失。
  5. 四个扫描降为同步函数；删除 `maybe_release_` / `lock_or_fail_`；
     `mux_loop_async_` 去掉 `read_events` 参数。
  6. 新增单测 `step_releases_done_channel_` / `step_waits_for_reset_before_release_`。
  7. 验收：lib **367** / inmem 21（compio / tokio / smol）/ keepalive 5 / smoke 5 /
     cross_thread 1 / `alloc_count` 1 / doc 12；clippy / format 干净。
  8. **T1 仍未完全完成**：mux 仍处理 timer 的 `WriteEvent_::Control`（`PULSE`、
     空闲拆流的 `FIN`/`RESET`、建流超时的 `REJECT`），并在每轮调用
     `drain_session_events_`（`SessionMailbox_` 投递不带唤醒）。两者都留 R9/P5
     （timer 收缩 T4 + 事件通道清理 T8/T24）。

- 20261029-0900：**R9：timer 收缩 + 控制帧全走公共 TX（T1/T4 完成）**。
  1. `timer_loop` 只等 `TimerSignal_` 的期限、到期 raise `HINT_MAIN_TIMER`；`timer_scan_`
     的决策整段迁入 `main_loop::tick_`（`Pulse` / `Abort` / `RejectEstablish` /
     `Finalize` / 连接级关闭动作）。`TimerSignal_` 进 `ControlState_`。
  2. 所有控制帧改由 main 写公共 TX ring：`PULSE`、空闲拆流的 `CLOSE(FIN)`+`CLOSE(RESET)`、
     建流超时 `REJECT`、`WINDOW_UPDATE`、未裁决放弃的 `REJECT`。不可重建的帧在公共 TX
     满时留在 `MainTickState_` 定容队列，下一轮补写（main 不被背压卡住）。
  3. 窗口通告：`demux_loop` 只记账 + 判定 + raise `HINT_MAIN_WINDOW_DIRTY`；main 先确认
     空间再取快照（栈上定长缓冲，零分配），空间不足时不取（快照留在窗口里）。
  4. 未裁决放弃位化：新增 `LOCAL_OPEN_SENT`；`abort_pending_` / `reject_after_open_` 只置
     `HANDLE_RETIRED` + raise，`reap_retired_` 按「对端是否知道」决定撤销或回 `REJECT`。
  5. 删除 `WriteEvent_::Control` / `write_control_blocking_`；删除 `SessionEvent_` /
     `SessionMailbox_` / `MailboxOverflow_` / `post_session_event_` / `drain_session_events_`；
     `MuxCore::notify_timer_` 改为 raise `HINT_MAIN_TIMER`。
  6. 验收：lib 367 / inmem 21（compio / tokio / smol）/ keepalive 5 / smoke 5 /
     cross_thread 1 / `alloc_count` 1（稳态注入恢复 0）/ doc 12；clippy 干净。
  7. **T1 完成**（mux 不再写身份表、不再处理控制帧）；**T4 完成**（timer 只计时）。
     T8 只剩两条事件通道本身（`TxReady` / `TgReady` / `RxConsumed` 仍排队）；
     T10 仍缺 `TakeInbound` / `Close` / `OpenTelegraph`。

- 20261029-2100：**R10：`Close` 请求化（T10/Q8）+ 删 `ctl_.rs`**。
  1. `MainLoopRequest_::Close { reply }` + `MainLoopReply_::Closed(bool)`；
     `MainTickState_::close_reply_` 跨轮保存等待者。main 在 `tick_` 的连接级关闭动作里
     **先 `set_` 答复、再 `cancel_loops_()`**：活跃归零 ⇒ `Closed(true)`，整体超时 ⇒
     `Closed(false)`；连接级失败在 `main_step_` 开头直接答 `false`。
  2. `close_async` 改为「提交请求 + 等 TCS」，删除 `wait_for_finalize_`（它等取消令牌，
     分不清「收敛」与「被取消」）。连带删除 `MuxCore::opts_` /
     `MuxCore::enter_waiting_close_` / `is_forced_close_`。
  3. `flush_releasable_` 在「最后一条子流释放」时补 `set_ Closed(true)` 再取消——少了
     这一步 `mux_graceful_close_dual_` 会挂起（实测）。
  4. 删除 `ctl_.rs`（未编译死文件）与 `MainLoopRequest_::SendControl`。
  5. **T8 位化尝试后回退**：`TxReady` / `RxConsumed` 改 `HINT_W_TX_READY` /
     `HINT_R_RX_CONSUMED` 后，`mux_closed_ring_spin_dual_` / `mux_drop_flush_dual_` /
     `flow_ctrl_socket_compio_` 失败（窗口回补后发送环不再被排空），回退到事件通道。
     R11 位化前必须先把「窗口回补 → mux 重试」钉成单测。
  6. 验收：lib 367 / inmem 21（compio / tokio / smol）/ keepalive 5 / smoke 5 /
     cross_thread 1 / `alloc_count` 1 / doc 12；clippy 干净。

- 20261029-2240：**R11：`RxConsumed` 位化 + park 丢位修复**。
  1. `ReadEvent_::RxConsumed` 删除，改 `HINT_R_RX_CONSUMED`；`ChannelRx` 去重位成功后
     只 raise，`demux_loop` 顶部按全表 `rescan_read_`。`handle_read_event_` 只剩
     `TgAttach`（`ReadEvent_` 现在只有它一个变体）。
  2. 修复 demux park 的**丢位**缺陷：`ReadWake_::Hint(bits)` 原先只处理
     `HINT_R_RELEASE`，其余位在 `poll_read_hint_`（取出语义）里被取走即丢。现在未处理
     的位回放到顶部统一处理。
  3. `TxReady` 位化尝试后回退：删 `last_ready` / `last_ready_has_segment_` 并改
     `HINT_W_TX_READY` 后，`mux_drop_flush_dual_` 失败（写满后立即 drop，raise 之后
     mux 没有下一轮——它停在 write hint 打不断的等待源上）。回退，R12 先查清 mux 在
     ConnTx 背压 / 公共 chunk park 时的唤醒源。
  4. 验收：lib 367 / inmem 21（compio / tokio / smol）/ keepalive 5 / smoke 5 /
     cross_thread 1 / `alloc_count` 1 / doc 12；clippy 干净。

- 20261030-0120：**R12：`TakeInbound` / `OpenTelegraph` 请求化（T10 完成）**。
  1. `MainLoopRequest_::TakeInbound` + `MainLoopReply_::Inbound(..)`：`income_async` 改为
     「请求 + TCS + 无请求时等 listener 通知槽」。放弃答复时 main 在 `set_` 前检查
     `is_abandoned_`，并用新增的 `restore_pending_inbound_` 把 `HandedOut` 与 app 侧
     message ring 两半原样放回。
  2. `MainLoopRequest_::OpenTelegraph` + `MainLoopReply_::Telegraph(owner)`：main 先
     `reserve_telegraph_` 再投两条 `TgAttach`（带环半部、不可位化），最后答复。
     `MuxShared_` 因此持一份 `w_events_` / `r_events_` 克隆（R13 建 telegraph inbox 后删）。
  3. 删除 `MuxCore::r_events_` / `reserve_telegraph_` / `take_pending_inbound_` 转发。
  4. **I3 达成**：API 面（`income_async` / `open_telegraph_async`）不再直接写身份表。
  5. 验收：lib 367 / inmem 21（compio / tokio / smol）/ keepalive 5 / smoke 5 /
     cross_thread 1 / `alloc_count` 1 / doc 12；clippy 干净。

- 20261030-0220：**R13：`TxReady` 位化**。
  1. `WriteEvent_::TxReady` 删除，改 `HINT_W_TX_READY`：`ChannelTx` 写入路径在去重位
     第一次成功时 raise；`demux` 收到 `WINDOW_UPDATE` / `PULSE` 时也 raise（窗口回补
     必须叫醒写循环重试）。
  2. `demux_loop` 去掉 `events_tx` 参数；`handle_write_event_` 只剩 telegraph 三项。
  3. `last_ready` 判据**保留**（现在恒 `None`）：R11 曾把 park 的环等待一并删掉并在
     `mux_drop_flush_dual_` 上失败；本轮只删事件、不动 park 结构，全部用例通过。
     等 `WriteEvent_` 整体删除时再清这段死判据。
  4. 验收：lib 367 / inmem 21（compio / tokio / smol）/ keepalive 5 / smoke 5 /
     cross_thread 1 / `alloc_count` 1 / doc 12；clippy 干净。

- 20261030-0200：**R14：`Tg*` 位化尝试后回退**。
  1. 把 `TgReady` / `TgTxClosed` 改成 `HINT_W_TG_READY` / `HINT_W_TG_TX_CLOSED`
     （`TgRec_` 加 `tx_closed_`）后，`mux_telegraph_inmem_dual_` 挂起。
  2. 诊断：应用三次 `tg_ready raise` 之后写循环只跑一轮（`tg round local=7`），随后
     无 `local=11` —— 卡在 `flush_tg_endpoint_` → `enqueue_frame_` 的**传输写半边
     背压 park** 上。
  3. 结论：旧 `TgReady` 事件除了唤醒，还保证「每次提交都有一条唤醒」；位化后写循环
     一旦停在 `enqueue_frame_`，合并提示既打不断该等待、也没有「稍后重试」的队列
     元素。**下一轮先钉住「`enqueue_frame_` 背压时的唤醒源」**（排空方 raise hint，
     或把 flush 改成非阻塞写 + 保留队头），再位化。
  4. 已回退（`Tg*` 事件保留）。当轮无代码落地，只留这条记录。

- 20261030-0300：**R15：`TgTxClosed` 位化**。
  1. `WriteEvent_::TgTxClosed` 删除，改 `HINT_W_TG_TX_CLOSED`：`TgRec_` 加
     `tx_closed_`，`Sender::drop` 只置位 + raise，写循环扫本地 tg 表摘除并清长度队列。
  2. `TgReady` **仍走事件通道**：位化后 `mux_telegraph_inmem_dual_` 仍挂起（见 R14
     记录），连「排空后自保持 raise」也没解决——阻塞点不在重试次数，下一轮需要从
     接收侧或 `TgReady` 处理路径本身定位。
  3. 验收：lib 367 / inmem 21（compio / tokio / smol）/ keepalive 5 / smoke 5 /
     cross_thread 1 / `alloc_count` 1 / doc 12；clippy 干净。

- 20261030-0330：**R16：`TgAttach` 走 attach inbox，读事件通道删除**。
  1. `WriteAttach_` / `ReadAttach_` 改枚举（`Channel` / `Telegraph` 两变体），channel
     与 telegraph 共用同一条 attach inbox。
  2. `OpenTelegraph` 改投 inbox（满时背压），`TgAttach` 事件删除；`ReadEvent_` 变空后
     **整条读事件通道移除**（`DemuxEventsGuard_` / `drain_read_events_` /
     `handle_read_event_` / `park_read_wake_` 的 events 参数 / `ReadWake_::Event|Rescan`）。
  3. `WriteEvent_` 去泛型，只剩 `TgReady`；`MuxLoopShared_::w_events_` 降级为**纯保活**
     句柄（删掉会让 `mux_drop_flush_dual_` 稳定失败，原因见 alloc-ownership §4.15）。
  4. 验收：lib 367 / inmem 21（compio / tokio / smol）/ keepalive 5 / smoke 5 /
     cross_thread 1 / `alloc_count` 1 / doc 12；clippy 干净。

- 20261030-0430：**R17：事件通道整体删除（T8 完成）**。
  1. `TgReady` 改走 attach inbox 的**零 payload 信号**：`Sender::send_async` 只 try_push
     一条标记（满时丢弃，可重建），写循环收到后当场排空该端点。**不能用合并提示**：
     R14/R15 两次尝试都让 `mux_telegraph_inmem_dual_` 挂起——合并位只保证醒一次，
     队列语义才保证「写环刚才满、现在有空」时还能再被叫醒。
  2. 删除 `WriteEvent_` / `ReadEvent_` 与整条事件通道（`EventSender_` /
     `EventReceiver_` / `Overflow_` / `EventTake_` / `TrEventReceiver_` /
     `event_channel_` / `EVENT_CHAN_CAP` / `EventTx_` / `EventRx_`）、
     `MuxEventsGuard_`、`mux_loop` 的 `events` 参数、`MuxShared_`/`MuxCore` 的
     `w_events_`、`CoreBundle_::w_receiver_`。
  3. 保活语义改由「`MuxCore::Drop` → raise `HINT_MAIN_TIMER` → main tick →
     `cancel_loops_`」承担（R16 记录的隐性保障）。
  4. 删 13 个只测事件通道的单测（lib 367 → 354）与四个通道 probe。
  5. 验收：lib 354 / inmem 21（compio / tokio / smol）/ keepalive 5 / smoke 5 /
     cross_thread 1 / `alloc_count` 1 / doc 12；clippy 干净。

- 20261030-2130：**R18：修好 `metrics_e2e` 与 `thread_safety`（都只动测试壳 / 计数点，
  不动协议与锁模型）**。
  1. `metrics_e2e`（发送 24 / 接收 84）：按「帧种类 × 方向」定位，差额**全部来自控制帧**
     ——写侧只报 `Data` / `Datagram`，`Open` / `Accept` / `Close` 只在读侧计入。修法是在
     `main_loop` 的两个控制帧写点（`try_write_control_frame_` /
     `write_control_frame_async_`）**写成功之后**补报 `on_frame(Send, ..)` 与
     `on_transport_bytes(Send, ..)`，不放宽断言；`src/metrics/mod.rs` 新增 §5.1 计数点表。
     修复后 2 条 `Data` + 6 条控制帧在两方向逐条、逐字节相等。
  2. `thread_safety`（三例 >60 s 无结果）：根因是**测试壳没有宿主**——旧写法先
     `block_on(run_until(connect_pair_))` 建连，`block_on` 返回后本地队列没人驱动，
     工作线程的 `bind_async` 永远等不到 `main_loop`。把整段场景移进
     `block_on(scope.run_until(..))`，并用 `futures::channel::oneshot` 取代阻塞
     `thread::join()` 后，三例 0.11 s 通过。工作线程的 `block_on` **保留**：它是
     「句柄可跨线程 + 各自运行时」的被测对象本身。
  3. §19 的 T2/T6/T7/T11/T16/T24/T25/T26/T27/T29/T30 按实测状态校准；
     `dev-notes/README.md` 索引同步。
  4. **跨仓库依赖：`sw-scope` `4d624c5`**。壳修好、`LocalSet` 真正驱动起来之后，
     跨线程并发访问 `Tcs_` 暴露了 `sw-scope::WeakSlot::try_sharing` 的竞态：
     `Created -> Sharing` 的 CAS 失败后读 `data_state()` 不是原子的，若另一线程在这
     两步之间把**最后一个** `Sharing` 放回 `Created`，升级会被误报为
     `Err(Created)`；`Tcs_::is_abandoned_` 把它当成「等待方已放弃」，于是 `main_loop`
     对仍在等待的 `bind` 回 `Cancelled`（实测 `[Bound, Other(Cancelled)]`，约 1/3
     概率）。修法是在观察到 `Created` 时重试（它正是升级的成功前提），并补回归用例
     `scope_::tests_::try_sharing_survives_concurrent_last_drop_`（旧实现 10/10
     失败、修复后通过）。该修补在 sw-scope 仓库**单独提交**，不属于本仓的公开 API
     或协议变更；smux_v1 只按 path 依赖使用它。
  5. 验收：lib 354 / inmem 21（compio / tokio / smol）/ keepalive 5 / smoke 5 /
     cross_thread 1 / `alloc_count` 1 / doc 12 / `metrics_e2e` 1 / `thread_safety` 3
     （后者连跑 20 次全过）；`sw-scope` 全量 65 + 3 + 3；clippy 干净。

- 20261031-0230：**R19：补 attach inbox 等价语义单测 + 落地超时批次（T25 / T28）**。
  1. R17 删掉 13 个只测事件通道的单测后，两条 attach inbox 一直没有等价覆盖。R19 在
     `registry_::tests_` 补 4 例：**FIFO 交付**、**满时 `try_push` 原样带回**、
     **异步投放等空位 + 关闭后拒绝新元素但排空已入队项**、**等待空位可取消并带回
     元素**；读侧另覆盖 FIFO 与关闭拒绝。各例都走 `multi_art_test_!`（compio / tokio /
     smol），共 12 个运行时变体。
  2. T28：仓库根新增 `justfile`，把验收拆成 `lib`（≤60 s）/ `inmem` / `integration` /
     `alloc` / `doc` / `lint`（≤300 s）六批，每批自带 `timeout`；`just all` 跑全量。
     超时以退出码 124 明确失败，不再依赖人工看挂起。
  3. 未改生产逻辑、无分配归属变化；`TgReady` 丢失后的重建仍由
     `mux_telegraph_inmem_dual_` 集成覆盖（单测只钉封装层语义）。
  4. 验收：lib **366**（354 + 12）/ inmem 21×3 / keepalive 5 / smoke 5 / cross_thread 1 /
     `alloc_count` 1 / doc 12 / `metrics_e2e` 1 / `thread_safety` 3；`just all` 全绿；
     clippy 干净。

- 20261031-0300：**R20：删除恒为 `None` 的 `last_ready` 死判据（P2 清理之一）**。
  1. R13 把 `TxReady` 位化后，写循环 park 里「最近通知过的那条发送环」分支再也不会
     执行：`last_ready` 只在初始化与每轮结尾被赋 `None`，`last_ready_has_segment_`
     永远在 `None` 上立刻返回 `false`（连 waker 都不登记）。R20 删除该分支、
     `last_ready` / `ring_ready` 变量、`last_ready_has_segment_` 及其单测（该单测只测
     这个死函数），park 现在只轮询取消令牌 / 写侧合并提示 / 公共 TX / attach inbox。
  2. 同步修正 `ChannelTx::notify_tx_ready_` 的过时文档（仍在讲「事件通道被灌满」与
     `last_ready` park）；现在通知就是 `HINT_W_TX_READY` + 去重位，写循环全表扫描。
  3. 删掉的单测曾覆盖「环已关闭且空不得判为可读」的历史空转路径。该判据随分支一起
     消失，回归网改由现行 park 设计（**不** park 在子流环上）承担；若将来重新引入
     「按环 park」，必须带回 `contains_left` 判据（见
     `smux_v1_sock_demo/dev-notes/intermittent-stall-20261007-0200.md`）。
  4. 验收：lib **365**（366 − 1 个死函数单测）/ inmem 21×3（含 `mux_drop_flush_dual_`）
     / keepalive 5 / smoke 5 / cross_thread 1 / `alloc_count` 1 / doc 12 / `metrics_e2e` 1 /
     `thread_safety` 3；`just all` 全绿；clippy 干净。

- 20261031-0330：**R21：建流期 message 改为 chunked 格式 + 非空 welcome（T15 / P3.3c 的一半）**。
  1. **线上格式**：`OPEN` message / `ACCEPT` welcome / `REJECT` reason 从单块
     `[u16 len][content]` 改为 `*chunk + 0x0000`（2 字节大端长度 + 内容，`0x0000`
     终止），与 MPTP 的分块体同形。内容由调用方解释，smux 只负责切块与重组：
     `PublicChunkIo_` 新增 `framed_message_len_` / `try_write_chunked_message_` /
     `try_write_raw_` / `try_read_available_into_`；message ring 容量 =
     `framed_message_len_(cap)`。`read_message_async` 逐块读到终止块，把内容重组进
     sink 并返回内容总字节数。
  2. **welcome 签名**：`TrChannelHandle::accept_async` 的 `welcome` 从
     `&mut W: TrBuffWrite` 改为 `&mut R: TrBuffRead`（用户拍板）。smux_v1 的响应方从
     该源读出 welcome、写进 message ring；发起方的 welcome 无意义，仍忽略。
     `mptp_smux` 的两处调用改为传空读源。
  3. **空 message**：合法编码是 `0x0000`（2 字节）；响应方握手用的 `OPEN` 仍是 0 字节
     载荷，接收侧跳过它（否则会占掉 incoming ring 里真正的 welcome）。
  4. **测试**：`pending_msg_` 单测覆盖单块 / 空 / 多块（> `u16::MAX`）/ 超长 / main
     原样搬运；`mux_message_inmem_dual_` 改为「非空 OPEN message + 非空 welcome」端到端；
     `mptp_cs_smoke` 的 compio + UDS demo 仍跑通 11 条 RPC。
  5. 顺带清理 R20 遗留：删除 `ChannelState_::send_available_try_` 与
     `SendWindow::available_try_`（原 `last_ready_has_segment_` 的底层，已无调用者）。
  6. **未做**：跨帧流（真正解除长度上限）与 `MessageTooLong` 错误面；message 总长仍受
     `max_packet_size` 与 `pending_msg_capacity` 约束，只是线上格式统一成了 chunked。
  7. 验收：lib **366** / inmem 21×3 / keepalive 5 / smoke 5 / cross_thread 1 /
     `alloc_count` 1 / doc 12 / `metrics_e2e` 1 / `thread_safety` 3；`mptp_cs_smoke`
     check 零警告、3 个测试、demo 11 条 RPC 跑通；clippy 干净。

## 18. 索引结构：BTreeMap / BTreeSet → bplus_tree_cmp

### 18.1 动机

`std::collections::BTreeMap` / `BTreeSet` 的每个节点都是一次独立堆分配；在长时间运行、
高建流 / 拆流 churn 的连接上，节点反复分配 / 释放会把分配器切成碎片。`bplus_tree_cmp`
的 `BPlusTree` / `BTree` 把节点与数据放进 `SlabList`：

- `SlabList` 上游默认按 **64 KiB chunk** 分配（`DefaultSlabConfig`）；本 crate 通过
  `SlabListConfig<2 KiB, A>` 取 **2 KiB**（见 §18.3），chunk 内槽位复用；不按条目逐个分配；
- `BPlusTree` 的数据 `(K,V)` 放在独立 data slab，索引节点只保存 `SlotPtr`，分裂/合并
  不移动数据，值的地址稳定；
- comparer-first：`K` 不必 `Ord`，查询对象可以与 `K` 不同，等值查询可用
  `get_with_comparer` / `get_mut_with_comparer`；
- 代价：chunk 在树非空期间常驻；B+ tree 有 data + node 两个 arena，单树固定开销比
  `BTreeMap` 高；degree `M` 是编译期常量。

### 18.2 依赖现状（本地源码）

项目根目录的父目录有 `bplus_tree_cmp` 的本地源码（相对 `old-cantare` 是
`../bplus_tree_cmp`）；当前提交 `6163209`
（“Add `BTreeMap` APIs for both `btree` and `bplus_tree`”），`Cargo.lock` 已锁到该
提交。包名是 `bplus_tree_cmp`，**lib 名是 `bplustree_cmp`**，代码里应写
`use bplustree_cmp::{BPlusTree, BTree, OrdComparer};`。

当前本地源码已经补齐与 `BTreeMap` 对应的 API：

| API | 用途 |
| --- | --- |
| `new_in` / `with_comparer_in` | 用连接注入的 `C::Alloc` 建树；返回 `Result<_, TreeError>` |
| `get` / `get_mut` / `insert` / `remove` / `len` / `is_empty` / `clear` | 精确键操作 |
| `first` / `last` / `first_key_value` / `last_key_value` | 最小 / 最大 |
| `iter` / `iter_mut` / `keys` / `values` / `values_mut` | 全序迭代；可变迭代只改 value |
| `range` / `range_mut` | `RangeBounds<K>`；构造时一次定位首尾，无工作栈 / 无临时分配 |
| `range_with_comparer` / `range_mut_with_comparer` | 异构边界 / Comparer |
| `contains_key` | `BTreeSet::contains` 的替代 |
| `get_with_comparer` / `get_mut_with_comparer` | comparer-first 异构查询 |

**没有 `entry` API**：`docks_.entry(...).or_insert_with(...)` 仍需
`get_mut` + `insert` 的 helper（§18.3）。`range` / `range_mut` / `iter_mut` 的
迭代器只保存固定大小的游标，不引入 `Vec` / `Box` / 工作栈；`iter_mut` 只允许改
value，key 与树结构保持不变。

`Cargo.toml` 当前用 git 依赖、未固定 rev；迁移前应把依赖固定到 `6163209`（或改用
本地 path 做开发），避免 CI 取到旧 checkout。

### 18.3 选型

统一使用 `K_DOCK_TREE_DEGREE = 16`（const），allocator 为 `C::Alloc` 的克隆；每棵树
用 `new_in(alloc.clone())` 构造，树自己持有 allocator。键都是 `Ord`，先用
`OrdComparer`；需要特殊序时换成自定义 `TrComparer`。

| 结构 | 现状 | 新类型 | 理由 |
| --- | --- | --- | --- |
| `bindings_` | `BTreeMap<(Dock,Dock), BindingSlot_>` | `BPlusTree<16, OrdComparer<(Dock,Dock)>, (Dock,Dock), BindingSlot_, A>` | 值含句柄，B+ 数据地址稳定；全序迭代 |
| `docks_` | `BTreeMap<Dock, DockCtx_>` | `BTree<16, OrdComparer<Dock>, Dock, DockCtx_<A>, A>` | 值按值存放，main 用 `get_mut` 改计数；单 arena 更省 |
| `remote_index_` | `BTreeSet<(Dock,Dock)>` | `BTree<16, OrdComparer<(Dock,Dock)>, (Dock,Dock), (), A>` | 纯键集合；只做 insert/remove/`contains_key` |
| `wait_close_expiry_` | `BTreeSet<(u64,Dock,Dock)>` | `BTree<16, OrdComparer<(u64,Dock,Dock)>, (u64,Dock,Dock), (), A>` | `first()` 取最早到期；逐条 remove |
| `WriteTable_` / `ReadTable_` | `BTreeMap<(Dock,Dock), Entry>` | `BPlusTree<16, OrdComparer<(Dock,Dock)>, (Dock,Dock), Entry, A>` | 值含环半部，数据地址稳定 |
| `TgReadTable_` / `TgWriteTable_` | `BTreeMap<Dock, Entry>` | `BPlusTree<16, OrdComparer<Dock>, Dock, Entry, A>` | 同上 |
| `reusable_docks_` | `VecDeque<Dock>` | 可选 `BPlusTree<16, OrdComparer<(u64,Dock)>, (u64,Dock), (), A>` | FIFO 用单调 seq；P6 再做 |

`BPlusTree` / `BTree` 没有 `entry` API；`docks_.entry(dock).or_insert_with(...)` 改成
`dock_index_::get_or_insert_dock_`（先 `get_mut`，没有则 `insert` 后 `get_mut`），不要在
`insert` 后把旧的 `&mut` 带出借用。

`BPlusTree` 的 `iter()` 是有序的；`BTree` 也一样。两者的 `get_mut` 都可以直接改值；
`BPlusTree` 的 `(K,V)` 地址在删除前稳定，`BTree` 的值可能在分裂/合并时移动，因此
**不要跨修改持有 `&V` / `&mut V`**（两者都不承诺 BTree 的地址稳定）。

### 18.4 范围查询：直接使用 `range` / `range_mut`

新 API 已覆盖原来依赖 `BTreeMap` 范围查询的地方，不再需要计数或侵入链表：

| 原查询 | 新用法 |
| --- | --- |
| `bindings_.range(channel_range)` | `bindings_.range(channel_range)`（`BPlusTree::range`） |
| `bindings_.range_mut(channel_range)`（`take_pending_inbound`） | `bindings_.range_mut(channel_range)`，直接改 `inbound_` |
| `remote_index_.range(remote_range)` | `remote_index_.range(remote_range)`（`BTree::range`） |
| `wait_close_expiry_.first()` + `remove` | `first_key_value()` + `remove`；到期判断仍看 tuple 的 `until` |
| 本地表 `range((Excluded(last), Unbounded))` 游标 | `range` / `range_mut`，参数形状不变 |
| `keys().next()` | `keys().next()` 或 `first_key_value()` |
| `BTreeSet::contains` | `contains_key(query)` |
| `BTreeMap::clear` | `clear()` |
| `docks_.entry(dock).or_insert_with(...)` | `get_or_insert_dock_` helper（`get_mut` + `insert`） |

要点：

- 现有 `channel_range_(local_dock)` / `remote_range_(remote_dock)` 返回
  `(Bound<(Dock,Dock)>, Bound<(Dock,Dock)>)`，直接满足 `RangeBounds<K>`；不需要改
  键的形状。
- `BTreeSet` 换 `BTree<K, ()>` 后，`range` / `iter` 交出 `(&K, &())`，原来直接解构
  键的地方要取 key（或改用 `keys()`）；`contains_key` 替代 `contains`。
- 空区间（起点晚于终点、边界不含任何键）返回空迭代器，不 panic；`OrdComparer` 下
  行为与 `BTreeMap` 一致。
- `range_mut` 只交出 `(&K, &mut V)`，不能改 key / 顺序；`take_pending_inbound` 只改
  `inbound_` 字段，安全。
- `DockCtx_` 保留现有的 `chan_count_`（dock 配额）与 `unbind_requested_:
  Shared<AtomicBool, A>`（给 `Drop` 用）；不再新增 `wait_close_count_` / listener /
  telegraph 标志，也不需要 `pending_head_` / `pending_next_` 侵入链表——这些范围查询
  直接由树完成。
- `has_concrete_identity` / `has_any_identity` 仍用 `range(...).next().is_some()`；
  `for_each_local_of_remote` 用 `remote_index_.range(...)`；`for_each_live_telegraph`
  与 `timer_scan` 仍用全表 `iter()`。
- `range` / `range_mut` / `iter_mut` 都无临时堆分配（本地源码的 `tests/allocator.rs`
  已覆盖）；迁移验收要保留这条断言。

### 18.5 数据面本地表的可变遍历

本地源码已补齐 `iter_mut` / `values_mut` / `range_mut`，数据面本地表可以直接迁移：

- `drain_once_`：用 `table.iter_mut()` 做一次全表顺序遍历；每个 entry 拿到
  `(&K, &mut V)`，不再用 `range(Excluded(last))` 游标逐次推进。
- `drain_one_` / `flush_entry_`：需要单条时用 `get_mut`；需要从某个 key 之后继续时
  用 `range_mut((Bound::Excluded(last), Bound::Unbounded))`。
- `scan_tx_closed_` / `scan_peer_closed_` 的只读部分用 `iter()`；需要边遍历边删除的
  `scan_released_` / 释放类路径用 `range((Bound::Excluded(cursor), Bound::Unbounded))`
  每次重新定位、逐个 `remove`，**不要**持有迭代器跨删除。
- `take_pending_inbound`：`bindings_.range_mut(channel_range)`，按原逻辑找第一个
  `Pending` 并改 `HandedOut`；不需要侵入链表。
- `timer_scan_`：`bindings_.iter()`，owner 的时间戳是原子，保持不可变遍历。
- `keys().next()` / `first()` 的游标语义与 `BTreeMap` 一致；空区间返回空迭代器。

`iter_mut` / `range_mut` 只改 value，key 与树结构不变；它们不引入 `Vec` / 工作栈，
`tests/allocator.rs` 已验证分配次数不变。**不再需要**为本地表保留 `BTreeMap`，也
不需要 active list 或上游补丁。

### 18.6 构造、错误与分配账

- `BPlusTree::new_in` / `BTree::new_in` 返回 `Result<_, TreeError>`；`ChannelRegistry_::new_`
  需要把 `TreeError` 映射成连接构造失败。`MuxConnection::new` 当前不返回 `Result`，
  因此要么 `expect`（把 degree/chunk 当配置错误，与 OOM 同级），要么新增 `try_new`
  （公开 API 变更，Q15）。
- 每棵树首次插入时按 **2 KiB chunk** 分配；之后 chunk 内槽位复用。B+ tree 两个 arena，
  BTree 一个 arena。树清空后 chunk 仍可能保留（由 `SlabList` 管理）。
- 不再有“每条身份一次 BTreeMap 节点分配”；改为“每棵树若干个 chunk”。这降低长期
  碎片概率，但**单连接固定内存可能明显增加**：registry 四棵树 + 数据面本地四棵树 +
  telegraph 两棵树 ≈ 10 棵；每棵树首次插入按 **2 KiB chunk** 分配，B+ tree 有 data/node
  两个 arena（最多约 4 KiB/树）。小连接因此只多出几十 KiB，而不是数百 KiB。
- 迁移前必须用 `alloc_count` / RSS / 峰值基准决定哪些表值得迁：优先 `bindings_` 与
  数据面本地表（条目多、churn 高）；`remote_index_` / `wait_close_expiry_` 条目少时
  可以继续 `BTreeSet`，或等上游支持可配置 chunk 大小。

### 18.7 迁移与验收

- P1.5：registry 四本表 + `DockCtx_` unbind 标志；`range` / `range_mut` 直接替换；先不碰数据面本地表。
- P4：数据面本地表换 B+；直接用 `iter_mut` / `range_mut`，无额外侧索引。
- P6：`reusable_docks_` 去 `VecDeque`；degree / chunk 调优。
- 验收：
  - 与 `BTreeMap` 的 get / insert / remove / 全序迭代 / `range` 边界等价（随机 churn，
    debug 断言）；
  - `take_pending_inbound` 的 `range_mut` 顺序为 remote 升序，Pending → HandedOut →
    释放/撤销全路径正确；
  - `DockCtx_` 的 `unbind_requested_` 在 binding 存活期间不会被 prune 掉；
  - 长时间 churn 下 chunk 数稳定，不随操作次数线性增长；
  - 树构造失败路径有明确错误 / panic 文本。

## 19. 重构任务总清单

> 状态：`设计已定` = 本文已给出形状；`待做` = 实施项；`待拍板` = 依赖 Q 项。
> 阶段对应 §13；索引项见 §18。
>
> **本表状态截至 R21（2026-10-31，`feat/sw-scope`）**：T1–T20、T22–T24 已完成 / 已达成，
> T26 已有，R18 修好 `metrics_e2e` / `thread_safety`，R19 补 inbox 单测与超时批次，
> R20 删 `last_ready` 死判据，R21 把建流期 message 改为 chunked 并支持非空 welcome；
> 剩余为 T3 的「main 完全同步」、T21（`reusable_docks_`）、T24 尾的 probe 命名、
> T15 的 `MessageTooLong` / 跨帧流、T27（连接级 `Drop` 路径与 chunk 数）、T30（本仓无 CHANGELOG）。

### A. 循环与职责

| # | 任务 | 阶段 | 状态 |
| --- | --- | --- | --- |
| T1 | `mux_loop` 只搬数据 + 先搬公共 chunk + 置 `TX_DRAINED`；不再组控制帧 / 发 FIN / 写身份表 | P2/P4 | **完成**（R6 FIN / R7 RESET / R8 身份释放 / R9 控制帧全走 main） |
| T2 | `demux_loop` 只搬数据 + 帧级/路由级合法性初审 + 写公共 RX；不再做协议决策 | P2/P4 | **已达成**（R16/R17 后两条事件通道已删：只读帧 + 帧级/路由级初审 + 写公共 RX / attach inbox + 置合并提示位） |
| T3 | `main_loop` 接管身份表写、请求处理、控制帧组帧、关闭推进、TCS 答复 | P1/P3 | **完成**：P1–P3 建流 / 控制帧；R9 tick；R10 关闭推进 + `Close` TCS；R12 入向与 telegraph。剩余是「完全同步」（仍 `await` 注册表锁 / TX 空间） |
| T4 | `timer_loop` 收缩为计时服务；main 收 `HINT_MAIN_TIMER` 做 tick | P1/P5 | **完成**（R9：`TimerSignal_` 进 `ControlState_`，决策迁 `main_loop::tick_`） |
| T5 | 删除 `ctl_`；循环数 / 取消令牌覆盖 4 个 | P1 | 完成（`loops_[3]` = main） |

### B. 传输与公共子流

| # | 任务 | 阶段 | 状态 |
| --- | --- | --- | --- |
| T6 | 公共 TX/RX 两条普通 ring + `[u16 len][frame]` chunked 协议 | P2 | **已接线**（R6/R7/R9 起一切控制帧都走公共 TX；`PublicChunkIo_` 承担 `[u16 len][frame]` 编解码与公共 RX/TX 搬运） |
| T7 | `mux_loop` 优先搬公共 chunk；`demux_loop` 控制帧写公共 RX；背压不丢帧 | P2 | **完成**（P3.1b 起全部帧种类走公共子流；R9 后 `PULSE` / 定时 `CLOSE` 也由 main 组帧写公共 TX，背压时留在 `MainTickState_` 下一轮补写） |
| T8 | 删除 `WriteEvent_` / `ReadEvent_` / `Overflow_` / `SessionMailbox_` | P5 | **完成**（R9 邮箱、R11/R13/R15 位化、R16 `TgAttach` inbox、R17 `TgReady` inbox + 整条通道删除） |

### C. 请求 / TCS

| # | 任务 | 阶段 | 状态 |
| --- | --- | --- | --- |
| T9 | `Tcs_`（Shared/Box，完成源）+ 取消 / abandon 语义 | P0/P1 | P0 完成 |
| T10 | `MainRequest_` 定容队列 + `WakerSlots_` 背压；`Bind` / `Listen` / `TakeInbound` / `Reserve` / `Accept` / `Reject` / `Close` / `OpenTelegraph` | P1 | **完成**（`Close` R10、`TakeInbound`/`OpenTelegraph` R12；`SendControl` 已删） |
| T11 | `MainLoopReply_` 统一结果；`Channel` / `Inbound` 携带 app 侧 message ring | P1/P3 | **完成**（`Dock` / `Channel` / `Listener` / `Unit` / `Closed` / `Inbound` / `Telegraph` 齐全，见 `main_loop/request_.rs:182`） |

### D. 建流期 message ring

| # | 任务 | 阶段 | 状态 |
| --- | --- | --- | --- |
| T12 | 每个 pending channel 一对 ring；app/main 两半分持；Listener 每入向请求一对 | P3 | **完成**（P3.1a 登记 / 取出 / 句柄持有；P3.3b 起 main 从 ring 读出载荷组帧） |
| T13 | `TrConnCfg::pending_msg_capacity` / `pending_msg_allocator`（默认 512 / `Self::Alloc`） | P3 | 完成（默认实现 + 建连快照） |
| T14 | `open` message / `accept` welcome / `reject` reason 走 ring；删除 `CtrlCell_` / `message_` / `read_available_into_vec_` | P3 | P3.3b 完成：三类载荷都经 message ring；main 写 incoming 供 `read_message_async` 读 |
| T15 | `read_message_async`（abs_smux 公开 API）；超长 message 策略 | P3 | API + 分块流式读取完成；R21 起 message/welcome/reason 走 chunked 格式，`accept_async` 的 welcome 改为读源、**非空 welcome 已通**；剩余 `MessageTooLong` 错误面与跨帧流（解除长度上限） |

### E. 索引（bplus_tree_cmp）

| # | 任务 | 阶段 | 状态 |
| --- | --- | --- | --- |
| T16 | `dock_index_` 适配层：degree 16、allocator、comparer、树类型别名 | P1.5 | **完成**（degree 16 + 2 KiB chunk；`DockBPlusTree_` / `DockSet_` 类型别名在 `dock_index_.rs:25`） |
| T17 | `bindings_` / `docks_` / `remote_index_` / `wait_close_expiry_` 换 arena 树 | P1.5 | 完成（P4；上游 `d65e5ec` 补 `Send`/`Sync` 后四表全部迁移） |
| T18 | `DockCtx_` 只加 `unbind_requested`；`range` / `range_mut` / `remote_index_.range` 直接替换；`docks_` 的 entry helper | P1.5 | 完成（P4：`DockCtx_` 内嵌 `Shared<AtomicBool>`，`unbind_requested_` 由 `reap_retired_` 落实） |
| T19 | `take_pending_inbound` 改用 `bindings_.range_mut`；不引入侵入链表 | P1.5 | 完成（P3.1a；`bindings_` 已是 2 KiB B+ 树） |
| T20 | 数据面本地表换 B+；用 `iter_mut` / `range_mut`；range 边界与 `BTreeMap` 对拍 | P4 | 完成：数据面五表 + registry 四表全部 2 KiB B+ 树（上游 `d65e5ec` 补 `Send`/`Sync`） |
| T21 | `reusable_docks_` 去 `VecDeque`；degree / chunk 调优 | P6 | 待做 |

### F. 清理

| # | 任务 | 阶段 | 状态 |
| --- | --- | --- | --- |
| T22 | 删除 `HintSlot_` 死代码、`free_waiters_` 的 `Vec<Waker>`、`PendingFin_` | P1/P5 | `HintSlot_` 已用于 main 提示；Listener / Telegraph / DockBinding / ChannelHandle 的 Drop 位化、`LocalAbort` → `HINT_W_RELEASED` 完成（P4）；请求队列与两条 `Attach` inbox 已改由 `mpmc_chan` 的 Arena 等待队列承担（P5），`InlineQueue_` / `SlotRing_` / `WakerSlots_` 已删除；`PendingFin_` 已删（R5）；`TX_DRAINED` + main FIN 已接（R6） |
| T23 | 删除 `ControlFrame_.payload_`、`ChannelHandle::message_`、`util_` 的 `Vec` 读取 | P3/P5 | `ChannelHandle::message_` 已删（P3.2）；`ControlFrame_.payload_` 已删（R2，改独立切片）；`util_` 已改有界切片（R3） |
| T24 | `session_.rs` / `signal_.rs` / `registry_.rs` 按 §11 清理 | P4/P5 | **基本完成**（事件通道 / 会话邮箱 / `ctl_.rs` 全删；`signal_.rs` 只剩 attach inbox + 合并提示位 + `ControlFrame_`；R20 删掉 `last_ready` 死判据并修正 `notify_tx_ready_` 的过时文档；剩余是 probe 命名 `event_*` → `hint_*`，见 `handoff-20261030-0315.md` §5.2） |

### G. 测试与验收

| # | 任务 | 阶段 | 状态 |
| --- | --- | --- | --- |
| T25 | 单元：PublicChunk、TCS、PendingMsg、inbox 等价语义、TimerSignal、树等价 / range 边界 / unbind 标志 | P0/P1.5 | 大部分已有（`public_channel_::tests_` / `tcs_::tests_` / `pending_msg_` / `timer_` / `dock_index_::tests_` / `main_loop::tests_`）；`WakerSlots_` 已删、其测试随之消失；**R19 已补 attach inbox 等价语义 4 例**（FIFO / 满时原样带回 / 异步等待 + 关闭排空 / 取消带回，各跑三后端；`TgReady` 丢失后的重建仍由 `mux_telegraph_inmem_dual_` 集成覆盖） |
| T26 | 集成：三后端 `inmem` / `smoke` / `cross_thread` / `keepalive` | P2–P6 | **已有**（`inmem` 21×3、`smoke` 5×3、`cross_thread` 1×3、`keepalive` 5；`metrics_e2e` 与 `thread_safety` 于 R18 修好） |
| T27 | 分配：数据面稳态 0 分配、Drop 0 分配；chunk 数 / 长 churn 碎片对比 | P4/P6 | `alloc_count` 覆盖绑定+监听、建流、稳态、拆流四段；`measure_pair_from_alloc_` 已直测「环对 `Drop` 0 分配」；**缺**连接级 `Drop` 路径与 chunk 数 / 长 churn 对比 |
| T28 | 超时：单测 ≤60 s、全量 ≤5 min；背压用例看门狗 | 全程 | **R19 落地**：仓库根 `justfile`（`just lib` ≤60 s、`inmem` / `integration` / `alloc` / `doc` 各批 ≤120 s、`lint` ≤300 s、`just all` 全量），超时以退出码 124 明确失败 |

### H. 公开 API / 决策

| # | 任务 | 阶段 | 状态 |
| --- | --- | --- | --- |
| T29 | 确认 Q1–Q16（OPEN 时机、message 容量、`read_message_async`、错误变体、range/Comparer、`TreeError`、degree） | 实施前 | **已确认**：Q1–Q16 均按推荐值采纳（实施记录见 §17；Q8 的「先答 TCS 再取消」见 R10） |
| T30 | 更新 CHANGELOG / README / dev-notes 状态；清理删除文档索引 | 收尾 | dev-notes 与 `dev-notes/README.md` 索引于 R18 同步；本仓无 CHANGELOG |
