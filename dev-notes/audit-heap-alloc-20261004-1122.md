# `smux_v1` 生产代码中的堆分配容器：审计与改造意见

日期：2026-10-04 11:22
范围：`smux_v1/src/**`，**不含** `#[cfg(test)] mod` 内的代码
性质：跨模块调研 + 改造意见（含「没有好办法」的条目）

> **修订：2026-10-05**——「子流状态节点化 + 窗口带锁」一轮
> （`substream-state-20261005-0419.md`）落地后，本文有两条现状变化：
> **§3.1 #4 的 `wakers` 死代码已删除**；**每条子流的 owner 从 3 次 `Shared::new`
> 降到 1 次**（`Shared<ChannelState_>`，随注册表身份记录建立），两个窗口各持一把
> `SpinningMutexOwned`（零内部堆分配，不引入新的全局分配）。§3.1 #7 的
> `flume::bounded(1)` 与 §3.5 的依赖链分配**未动**。

## 1. 为什么查

`src/connection/mod.rs` §5 写着分配纪律：

> 本 crate 会做堆分配，但每一处都必须走**调用方注入的分配器**（`allocator_api`），
> 不得隐式落到全局分配器，也不得靠 `Vec` 之类的临时堆结构绕开设计。

本文件把当前**实际状况**逐条记下来：哪些真在全局分配器上分配、哪些只是空占位、
哪些是显式宣告的类型、哪些是依赖链上不受本仓管辖的分配。每条都给改造意见。

## 2. 扫描口径与总量

`Vec` / `vec!` / `VecDeque` / `Box` / `String` / `format!` / `to_string()` / `.collect()` /
`HashMap` / `HashSet` / `BTreeMap::new()` / `Rc` / `Arc` / `flume::` 全量匹配，
再按「是否位于 `#[cfg(test)] mod` 之后」切开。生产代码命中 **33 行**，去重后只有三类容器：

| 容器 | 生产命中 | 真分配 | 空 `Vec::new()`（不增长） | 死代码 | 显式宣告位置 |
| --- | --- | --- | --- | --- | --- |
| `Vec` | 19 | 3 | 6 | 1 | 9 |
| `Arc` | 3 | 1 | — | — | 2 |
| `flume` | 11 | 6（建点） | — | — | 5（4 处 `use` + 1 处类型） |
| `Box` / `String` / `format!` / `.collect()` / `VecDeque` / `HashMap` | 0 | — | — | — | — |
| **合计** | **33** | **10** | **6** | **1** | **16** |

「显式宣告位置」= 类型定义 / 形参 / 返回类型，即本次口径里**不计**的部分（见 §3.4）。

## 3. 发现清单

### 3.1 真正在**全局分配器**上分配

| # | 位置 | 容器 | 分配时机 |
| --- | --- | --- | --- |
| 1 | `src/connection/session_.rs:466` `encode_whole_frame_` | `Vec::with_capacity(64 + payload.len())` | **每一条 DATA 帧**（经 `drain_one_`，:1501）、每条控制帧（:1277 / :1324） |
| 2 | `src/connection/util_.rs:23` `read_available_into_vec_` | `Vec<u8>`（`resize` 增长） | 建流时的开场消息（`dock_binding/binding_.rs:280`）、拒绝理由（`channel_handle/handle_.rs:593`） |
| 3 | `src/connection/mux_connection/registry_.rs:1074` `mark_failed_` | `Vec<ChannelOwner_<A>>` | 连接级失败一次（**2026-10-05 已删除**：通知建流等待者不再需要离开注册表锁） |
| 4 | `src/connection/mux_connection/registry_.rs:1073` 同函数 | `Vec<Waker>` | **从不**（声明后未 `push`）——**2026-10-05 已删除**，见 §5.4 |
| 5 | `src/connection/ring_.rs:174` `MuxChanBuff::pair_from_alloc_` | `Arc<dyn Allocator + Send + Sync>` | 每次 `make_ring_buffs`（每条子流一次） |
| 6 | `dock_binding/binding_.rs:202` | `flume::bounded(1)` | 每个被监听的 dock 一次 |
| 7 | `connection/owner_.rs:74` | `flume::bounded(1)` | **每条子流一次** |
| 8 | `connection/sync_.rs:193` | `flume::bounded(1)` | 每次建取消令牌 |
| 9 | `connection/signal_.rs:341` | `flume::unbounded()` | 每条连接 2 个（读事件 / 写事件各一，`conn_.rs:224`/`:225`） |
| 10 | `connection/signal_.rs:400` | `flume::unbounded()` | 每条连接一次（`SessionMailbox_`） |

**数据面影响（#1）**：每帧一次全局分配 + 数据面变成 **3 次载荷拷贝**
（子流环 → `scratch`（注入分配器）→ `Vec`（全局）→ 写环）。
其中 `scratch` 那一跳是**必要**的（`move_items_to_buff` 推进段的已消费量、段的 drop
才把消费提交回环，见 `drain_one_` 的注释）；**多余的是它之后的 `Vec` 那一跳**——
载荷在注入分配器的暂存里已经连续，却被整体再拷进一块全局堆内存。

### 3.2 `Vec::new()` 空占位 —— **不分配**（容量 0，且从不增长）

`channel_handle/handle_.rs:108`（响应方构造，恒空）、`:184`、`:450`、`:463`；
`connection/signal_.rs:99`；`connection/session_.rs:684` —— 共 6 处。
语义都是「这一帧没有载荷」，不会走到堆上。（第 7 处 `registry_.rs:1073` 同为 `Vec::new()`
但**从不增长**，它是 §3.1 #4 的死代码，单列。）

### 3.3 显式合规（走注入分配器）—— 列作对照

`BTreeMap::new_in(alloc)` / `BTreeSet`（注册表三张索引表、两个循环的本地表）、
`mm_ptr::Owned::new_slice` / `new_uninit_slice`、`Shared::new(.., alloc)`、
环本体与共享容器。

### 3.4 显式宣告位置（类型定义 / 形参 / 返回类型）—— 按口径**不计**

| 位置 | 形态 |
| --- | --- |
| `channel_handle/handle_.rs:81` / `:120` / `:558` | `message_: Vec<u8>` 字段；`message: Vec<u8>` 与 `payload: Vec<u8>` 形参 |
| `connection/signal_.rs:87` / `:110` | `payload_: Vec<u8>` 字段；`payload: Vec<u8>` 形参 |
| `connection/frame_.rs:512` / `:559` | `sink: &mut Vec<u8>` 形参（`encode_header_into_` / `encode_field_into_`） |
| `connection/session_.rs:465` | `-> Result<Option<Vec<u8>>, MuxError>` 返回类型 |
| `connection/util_.rs:18` | `-> Vec<u8>` 返回类型 |
| `connection/ring_.rs:157` / `:183` | `alloc_: Arc<dyn Allocator + Send + Sync>` 字段与形参 |
| `mux_connection/core_.rs:200` | `notify_tx_: flume::Sender<()>` 字段 |
| `sync_.rs:59` / `owner_.rs:33` / `signal_.rs:45` / `registry_.rs:90` / `listener_.rs:6` | `use flume::{…}` 导入（5 处） |

> 两点要留意：
> 1. `message_` / `payload_` 是 #2 那次分配的**落点**——值会一直留到 `accept_async` /
>    帧被发出，因此 #2 的改造必然牵动这两个字段的类型；
> 2. `frame_.rs:512` / `:559` 的 `&mut Vec<u8>` **正是 #1 要改的签名**——「往一个可增长的
>    容器里追加头部」这个形状，本身就带着「必须有一块堆内存」的假设。

### 3.5 依赖链上的分配（不在本仓）

`atomic_sync` 的协作式锁与等待队列（`ChannelOwner_` 热状态锁，`connection/mod.rs` 自述
「内部会分配等待节点」）、`abs_art` 的作用域本地队列、`flume` 内部。
这些不受本仓分配器管辖，本文件只记录。

## 4. 人类决策（2026-10-04）

> **#1 ~ #4 都有足够充分的理由推进「不使用堆分配」的设计，但本轮先只记录，不动手。**

`#4` 附带一条：它其实不是分配问题，而是**死代码与注释不符**（见 §5.4），
这一条不涉及设计取舍，「推进」的含义就是要么补全、要么删掉。

## 5. 逐条改造意见（AI）

### 5.1 #1 每帧一个 `Vec` —— **有明确的好办法，且不难**

方案：
1. 帧头改用**栈上定长缓冲** `[u8; K_MAX_FRAME_HEADER]`（64 B）。上界够用，实测算式如下
   （`encode_field_` 的编码是「1 字节自描述头 + 大端值」，宽度取能容纳该值的**最小合法**者）：

   | 字段 | 允许宽度 | 最坏字节 |
   | --- | --- | --- |
   | 帧首字节 | — | 1 |
   | `LocalDock` / `RemoteDock` | `BeU8` / `BeU16` / `BeU32` | 5 + 5 |
   | `RecvWindow` | 任意（含 `BeU64`） | 9 |
   | `RecvTotal` | `BeU16` / `BeU32` / `BeU64` | 9 |
   | `PayloadLen` | 任意（含 `BeU64`） | 9 |
   | `ReasonCode`（仅 `REJECT`） | 任意 | 9 |

   六个字段**同时出现**的（不存在的）组合是 1 + 5 + 5 + 9 + 9 + 9 + 9 = 47 字节；
   协议允许的真实组合（窗口三项与 `ReasonCode` 互斥）最坏是 38 字节。**两者都在 64 以内**。

   > 顺带记一处文档不精确：`K_MAX_FRAME_HEADER` 的注释写「字段最坏情况是三字节宽」，
   > 但 `RecvTotal` / `PayloadLen` / `RecvWindow` 都接受 `BeU64`——真实最坏是 9 字节。
   > 结论（64 够用）不受影响，但注释宜按上表修正。
2. `encode_header_into_` 由「写进 `&mut Vec<u8>`」改成「写进 `&mut [u8]` 并返回长度」
   （现有实现已经是逐字段 `encode_field_` 返回 `[u8; 9] + len` 的形状，加个游标即可）。
   测试侧可以保留 `Vec` 版本——测试不受分配纪律约束。
3. `enqueue_frame_` 增加「两段」形式 `enqueue_frame_parts_(tx_stage, head, payload, cancel)`：
   先分块写头、再分块写载荷。写环是单生产者（复用循环），两段之间不会被他帧插入。

收益：数据面**每帧零全局分配**，载荷少一次拷贝。
代价：`enqueue_frame_` 多一个变体；`encode_header_into_` 的签名变更——**生产调用点只有
`session_.rs:467` 一处**，另有 3 处测试调用点（`frame_.rs:619` / `:656`、`frame_parser_.rs:665`）。
风险：低。建议同时加一条**计数分配器回归用例**（`buffex_compio_adapt` 的 `CountingAlloc`
是现成范式）：断言稳态搬运期间全局分配次数为 0。

### 5.2 #2 开场消息 / 拒绝理由的 `Vec` —— **有办法，但影响面大于 #1**

- **方案 A（类型替换）**：`ChannelHandle::message_` 与 `ControlFrame_::payload_` 改用
  `Owned<[u8], C::Alloc>`。障碍是 `ControlFrame_` 定义在 `signal_.rs`，是**非泛型**的
  事件载荷类型，两个循环共用；要带上分配器就得给整条事件类型加参数，影响面从
  `signal_` 铺到 `session_` / `session_pump_`。
- **方案 B（定长上限）**：用 `[u8; N] + len` 承载，超出即截断或拒绝。协议允许载荷到
  `max_packet_size`，所以这是**语义收窄**。但 `util_.rs` 自己的注释就记着：
  「**这三处载荷**（开场消息 / 欢迎信息 / 拒绝理由）在现行 `abs_smux` API 下**没有明确的
  长度语义**」（该结论的原始推导所在文档已按维护约定清理），
  收窄未必是损失——只是**必须先把这个语义定下来**。
  另外第三处（`welcome`）当前根本没被读取：`handle_.rs:450` 一带按「空载荷」发出，
  代码里明确标了「记为遗留」，因此它甚至还不是一个真实需求。
- **方案 C（不存）**：把开场消息直接读进写环、句柄只记「已写入多少」。与「`OPEN` 要等
  `accept_async` 才发」冲突，消息必须先存住 —— 不可行。

意见：先做 §5.1，再来定这几处载荷的长度语义；语义定了之后 B 最省，A 更彻底。

### 5.3 #3 `mark_failed_` 的 owner 列表 —— **有两个好办法**

- **方案 A（零分配，推荐）**：`bindings_` 是 `BTreeMap`，用
  `range((Excluded(cursor), Unbounded))` 做「取锁 → 找一个 → 释放锁 → await 通知」的
  游标式推进。零分配、不长期持锁；代价是 n 次取注册表锁（n = 已登记 dock 数）。
  连接失败是一次性路径，可以接受。
- **方案 B（一行）**：`Vec::new()` → `Vec::new_in(allocator)`。`ChannelRegistry_<A>` 本来
  就带 `A: AllocatorClone`，切到注入分配器即可；仍是 O(n) 内存，但不再违反纪律。
  （已实测：`Vec::new_in` 在本 crate 当前的 `#![feature(allocator_ext)]` 下可用。）

意见：先落 B（改动最小），有余力再换 A。

### 5.4 #4 `wakers` 死代码 —— **直接删**（**2026-10-05 已完成**）

声明后从未 `push`，`for waker in wakers` 恒空转，与「唤醒所有等待中的 API 面 future」
的注释不符。唤醒职责实际由 `ctx.notify_tx_.try_send(())`（监听者）与
`state.notify_establish_()`（子流建流等待者）承担，覆盖是完整的。

**落地**：「子流状态节点化」一轮把 `mark_failed_` 改为在注册表锁内直接
`notify_establish_`（状态无锁），`wakers` 与那个循环、以及收集 owner 的 `Vec` 一起删除。
见 `substream-state-20261005-0419.md` §4.3。

### 5.5 #5 `Arc<dyn Allocator>` —— **能减量，但做不到零分配**

- **方案 A（推荐）**：把这份 `Arc` 从**每条子流一份**降到**每条连接一份**——在核心建一次
  擦除分配器，各 `MuxChanBuff` 只 `Arc::clone`。次数从 O(子流数) 降到 O(1)。
- **彻底零分配**：做不到。类型擦除（trait object）本身必须占一块**全局**内存；
  `Box` 同样走全局分配器。要免除只能改上游
  `abs_smux::conf::TrMuxConfig::Buff` 让它带分配器类型参数——那是上游 API 变更，且会把
  分配器参数传染到所有下游类型。

意见：**记录下来，没有好办法**（除非接受上游改型）。A 值得做，但只是减量。

### 5.6 #6 `flume` 通道 —— **一半有好办法，一半没有**

**容量 1 的三个提示通道（#6 / #7 / #8）——有好办法。**
它们的语义都是「状态可能变了」的**幂等提示**（容量 1、重复投递无意义），可以换成
`AtomicBool`（置位 + 复检）+ **单个等待者槽**（`Waker` 存储），零分配。可抽成一个内部
原语 `NotifySlot_`；`buffex` 的 park 槽、`atomic_sync` 的等待槽都是同款形状，值得先看
有没有现成的可复用。

风险要写清楚：Waker 槽必须处理「登记 → 发布 → 复检」的丢唤醒问题（与 `buffex` 的 SPSC
唤醒协议同源），写错就是**偶发挂死**。建议单独一轮，配压力用例。

**两个 `unbounded()` 事件通道（#9 / #10）——没有「不改架构」的好办法。**
它们承载**真实事件流**（`WriteEvent_` / `ReadEvent_` / `SessionEvent_`），长度无界，
而当前的正确性前提正是「**不丢事件**」（`SessionMailbox_` 的注释还额外要求
「`Drop` 用，不取锁、不阻塞」）。改成固定容量 + 注入分配器（`buffex::ring` 或
`atomic_sync` 的注入队列）会引入**背压**：满时 `try_send` 失败即丢事件，而现在的
`try_send_event_` 返回的 `bool` 没有任何调用方按「丢失」处理。

意见：**记录为「已承认的例外」，暂不推进**。要推进的前提是先把「事件丢了会怎样」
论证清楚，并让所有 `try_send_event_` 调用点显式处理失败——那是一次架构级改动。

## 6. 建议的推进顺序与验收

| 序 | 项 | 验收方式 |
| --- | --- | --- |
| 1 | #4 删死代码 | **已完成（2026-10-05）**：代码审查 + 现有用例不回归 |
| 2 | #1 栈上帧头 + 两段入环 | **计数分配器回归用例**：稳态搬运期间全局分配次数为 0；数据面拷贝次数从 3 降到 2 |
| 3 | #3-B `Vec::new_in` | 现有 `mark_failed_` 对应用例 |
| 4 | #5-A `Arc` 降到每连接一份 | 计数分配器：每条子流不再新增一次全局分配 |
| 5 | #2 开场消息 / 拒绝理由 | 先定长度语义；再按 §5.2 A/B 落地 |
| 6 | #6 三个提示通道 → `NotifySlot_` | 丢唤醒压力用例（单轮内高频置位 + 等待者反复进出） |
| — | 每条子流的 owner 合并 | **已完成（2026-10-05）**：3 次 `Shared::new` → 1 次 `Shared<ChannelState_>`，见 `substream-state-20261005-0419.md` |
| — | #6 两个无界事件通道 | **不推进**，作为已承认的例外记在此处 |

## 7. 遗留

- **能否把纪律变成可验证的闸门？** 例如测试里挂一个「越界即 panic」的
  `#[global_allocator]`（或计数分配器 + 断言），让「生产路径零全局分配」从靠自觉
  变成 CI 可查。做到之后 §3.1 的表就是一份可执行的清单。
- **`K_MAX_FRAME_HEADER` 的注释与算式**：已在 §5.1 算出真实最坏 47 字节（`≤ 64`，结论安全），
  但常量注释里的「字段最坏情况是三字节宽」是错的，落地 §5.1 时一并修正。
- 依赖链上的分配（§3.5）是否需要向 `atomic_sync` / `abs_art` 提需求，留待分配纪律
  扩展到依赖链时再议。

---

# 第二轮重扫（2026-10-05）

**触发**：`substream-state-20261005-0419.md` 那一轮（状态节点化 + T1 + 窗口带锁）落地后
重扫一遍，核对 §3.1 的十条、找第一轮**漏掉**的项、并确认没有引入新分配。

**口径**与第一轮相同：`src/**`，切掉 `#[cfg(test)] mod`（含 `connection/test_support_.rs`、
`ring_::test_support_`）；容器 = `Vec` / `vec!` / `VecDeque` / `Box` / `String` / `format!` /
`.collect()` / `HashMap` / `HashSet` / `BTreeMap::new()` / `BTreeSet::new()` / `Rc` / `Arc` /
`flume::`。**第一轮的口径漏了 `BTreeSet::new()`**（只列了 `BTreeMap::new()`），本轮补上，
并额外扫了 `Box::new` / `Rc::new` / `Arc::new` / `with_capacity` / `to_vec` / `to_owned` /
`std::sync::Mutex` / `OnceLock` / `thread::spawn` 等，未再发现新项。

## A. §3.1 十条的现状

| # | 位置（第一轮） | 现状（第二轮） | 分配器 | 时机 |
| --- | --- | --- | --- | --- |
| 1 | `session_.rs:466` `encode_whole_frame_` | **待整改**（现 `session_.rs:573`） | 全局 | 每 DATA 帧 / 每控制帧 |
| 2 | `util_.rs:23` `read_available_into_vec_` | **待整改**（现 `util_.rs:23`） | 全局 | 建流开场消息 / 拒绝理由 |
| 3 | `registry_.rs:1074` `Vec<ChannelOwner_>` | **已整改** | — | `mark_failed_` 不再收集，锁内直接通知 |
| 4 | `registry_.rs:1073` `Vec<Waker>` | **已整改**（死代码删除，§5.4） | — | — |
| 5 | `ring_.rs:174` `Arc<dyn Allocator>` | **待整改**（§5.5-A 未做） | 全局 | 每子流一次（`MuxChanBuff` 配置） |
| 6 | `binding_.rs:202` `flume::bounded(1)` | **待整改** | 全局 | 每个被监听 dock 一次 |
| 7 | `owner_.rs:74` `flume::bounded(1)` | **待整改**（现 `owner_.rs:136`，`Establish_`） | 全局 | **每子流一次** |
| 8 | `sync_.rs:193` `flume::bounded(1)` | **待整改**（现 `sync_.rs:196`） | 全局 | 每个取消令牌；**每连接 4 个**（`registry_.rs` 建 4 个循环令牌，`child_token()` 是 `clone()`，不再分配） |
| 9 | `signal_.rs:341` `flume::unbounded()` | **待整改**（现 `signal_.rs:356`） | 全局 | 每连接 2 条（读 / 写事件通道） |
| 10 | `signal_.rs:400` `flume::unbounded()` | **待整改**（现 `signal_.rs:415`） | 全局 | 每连接 1 条（`SessionMailbox_`） |

## B. 本轮顺带消掉的分配（第一轮只记在 §3.3「合规」或 §3.5「依赖链」里）

| 项 | 第一轮 | 现状 |
| --- | --- | --- |
| 每条子流的共享状态 | `ChannelOwner_::new_` **3 次 `Shared::new`**（注入）+ 热状态锁 `CooperativeRwLockOwned` **内部一个 `Arc<RwCore>`（全局）** | **1 次 `Shared<ChannelState_>`（注入）**，锁改 `SpinningMutexOwned`（内联，零分配） |
| 每子流状态节点字节 | 3 块独立内存 + 1 个 Arc 控制块 | 1 块：`ChannelState_` = **192 B**（实测；其中两把锁 16 B，`FlowCtrl` 144 B） |
| 连接级失败路径 | `Vec<ChannelOwner_>`（全局）+ 死 `Vec<Waker>` | 零分配（#3/#4） |

即：**每子流的堆分配次数 4 → 2**（注入 3→1；全局 1→1，全局那一份是 #7 的建流通知），
另少一个 Arc 控制块。

## C. 新发现（第一轮漏项）

| # | 位置 | 容器 | 分配器 | 时机 |
| --- | --- | --- | --- | --- |
| N1 | `session_.rs:1200` `mux_loop_async_` 的 `pending_fin` | `BTreeSet<(Dock, Dock)>`（`BTreeSet::new()`） | **全局** | 「发送方向已丢、环还没排空」每条子流**首次入集合**时分配一个节点 |

- 类型定义在 `session_.rs:266`（`type PendingFin_ = BTreeSet<(Dock, Dock)>;`），**没有**像
  两个循环的本地表那样走 `new_in(allocator)`；第一轮的匹配式只写了 `BTreeMap::new()`，
  因此漏了它（`registry_.rs:1409` 的 `BTreeMap::new()` 在测试模块里，不在口径内）。
- 触发频率低于其它项（只在 `drop(tx)` 时发送环仍有数据、且额度为 0 时入集合），但它是
  **真实存在的全局分配**，且属于本仓可以自行整改的那一类。
- 整改方向：`PendingFin_` 的类型带上分配器参数 `BTreeSet<(Dock, Dock), C::Alloc>`，构造时
  `BTreeSet::new_in(...)`（与 `ReadTable_` / `WriteTable_` 同款；循环建表时已经从注册表拿到
  分配器克隆）。

## D. 有没有引入新问题

**分配维度：没有。** 本轮新增的运行时结构逐项核对：

| 新结构 | 是否分配 | 说明 |
| --- | --- | --- |
| `SpinningMutexOwned<SendInner>` / `<RecvInner>`（每子流 2 把） | **否** | `AtomicUsize` + `UnsafeCell<T>`，内联在状态节点里 |
| `AtomicFlags<usize>` 状态字 | **否** | 内联 `AtomicUsize` |
| `Shared<ChannelState_>`（每子流 1 个） | 是（注入分配器） | 取代原来的 3 次；见 B |
| `wait_or(|| unreachable!())` 等闭包 | **否** | ZST |

**非分配维度的两点代价（如实记录，不构成缺陷）**：

1. **状态节点常驻字节**（实测，64 位）：`ChannelState_` = **192 B**，其中 `FlowCtrl` 144 B
   （`SendWindow` 56 + `RecvWindow` 88）、两把锁共 16 B、两个 inner 共 128 B。与中间那版
   「逐字段原子」实现相比，多出的是锁字与对齐（原子字与锁字同宽，量级相同，未逐字节实测）；
   但对照第一轮的 **3 块独立内存 + 1 个 Arc 控制块**，堆分配次数与元数据开销都是下降的。
2. **`flow_ctrl` 多了一个并发原语依赖**：它现在 `use atomic_sync::mutex::preemptive`。
   模块自述「可被将来别的复用协议复用」，这条复用现在会带上 `atomic_sync` 依赖——已在模块
   文档里写明选型理由（`cooperative` 会引入每实例的全局分配）。

**没有新增**：`#[global_allocator]`、`Box` / `Rc` / `String` / `format!` / `.collect()` / `thread::spawn` /
`std::sync` 容器在 `src/**` 的生产代码里仍然为 0；新增依赖为零（`atomic_sync` 本就是直接依赖）。

## E. 当前生产分配清单（按生命周期归口）

| 粒度 | 全局分配器 | 注入分配器 |
| --- | --- | --- |
| 每连接 | 4（取消令牌通知）× `flume::bounded` + 2（事件通道）+ 1（会话邮箱）+ 1（注册表协作锁内部 `Arc<RwCore>`） | `Shared<MuxCore>`、注册表节点、4 本索引（惰性）、4 个取消令牌标志 |
| 每子流 | 1（`Establish_` 建流通知，#7）+ 1（`MuxChanBuff` 的 `Arc<dyn Allocator>`，#5） | `Shared<ChannelState_>`（本轮 3→1）、2 × `Shared<Ring>` |
| 每被监听 dock | 1（listener 通知，#6） | — |
| 每 DATA 帧 | 1（`encode_whole_frame_` 的 `Vec`，#1） | — |
| 每次建流 | 0~1（开场消息 / 拒绝理由的 `Vec<u8>`，#2） | — |
| 首次进入 `pending_fin` | 1（BTreeSet 节点，**N1**） | — |

**结论**：第一轮 10 条里 **2 条已整改（#3/#4）**，**8 条待整改**；另有 1 条第一轮只当
「合规/依赖链」记着的（每子流 3 次 `Shared` + 协作锁的 `Arc`）本轮**顺带消掉**。
新发现 1 条漏项（N1）；**没有引入新的分配问题**。最高优先仍是 #1（每帧一次全局分配 +
数据面多一跳拷贝）。
