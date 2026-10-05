# 身份记录的形状与分配裁减（形状三裁决 + C1 落地）

日期：2026-10-05 06:48
性质：**跨模块技术决策 + 落地记录**（`connection::owner_` / `connection::sync_` /
`connection::mux_connection::registry_` / `connection::session_` / `channel_half`）。
承接 `audit-heap-alloc-20261004-1122.md`（分配清单）与
`substream-state-20261005-0419.md`（状态节点化的落地记录）——本文**推翻**其中两处：

1. `Establish_` 用 `flume` 容量 1 通道（`sync_` 模块文档「唤醒」一节的旧结论）；
2. 「注册表记录与子流状态是两个可独立死亡的对象」（T2 的形状），改为
   **一个身份一个节点**。

---

## 1. 人类裁决（本轮原话，按原意记录）

> 1. 「**先不做 Arena**，先把内存分配做到 `Shared<DockBinding_>` 的程度，然后其他所有
>    子流、telegraph、listener 拿到的都是这个，并且因为实际逻辑实现的原因，
>    不可能是其他变体，一定是自身所对应的 `DockBinding_` 数据类型。」
> 2. 「**形状三，付法选 1**（构造时校验一次 + 之后一处带安全注释的 `unsafe` 取用），开工。」
> 3. 「`DockBinding_` 乃至 `ChanRec_` 内部都已经刻意不让做堆内存分配了，
>    为什么还要带 `A` 泛型参数？」——记录类型不带分配器参数。

由此确定的**形状三**：

```rust
// 身份节点：非泛型、内部零堆分配；每种身份内联自己那份内容
pub(crate) enum DockBinding_ {
    Channel(ChannelState_),      // 复用现有类型名，不再单独持有状态节点
    Telegraph(TgRec_),
    Listener(LsnRec_),
}

// 表槽：`A` 只出现在句柄上；墓碑只能是槽的另一种形态
// （节点活得比身份久：release 后应用仍持句柄）
pub(crate) enum BindingSlot_<A> {
    Live { rec_: ChannelOwner_<A>, /* 冷状态 */ .. },
    WaitClose(WaitCloseCtx_),
}
```

## 2. 结论速览

| 事项 | 结论 |
| --- | --- |
| 身份节点 | `DockBinding_`：**非泛型**、内部零堆分配；`ChannelState_` 是它的 `Channel` 变体内容 |
| 分配器参数 `A` | **只属于句柄**：`Shared<T, A>` 把分配器写进节点（`SharedInner { alloc_, .. }`，[`shared_.rs`](../../mm_ptr/src/shared_.rs)）以便最后一个强引用归还内存；记录内部零分配 ⇒ 记录不带 `A` |
| 变体取用 | 构造时校验一次，之后一处带安全注释的 `unsafe` 取用（热路径零分支、零 panic）；不变式是「节点一旦建出，变体终生不变」 |
| 表示 | 本轮仍是 `Shared`（引用计数），**不做 Arena / `NonNull`** |
| `Establish_` | 换成零分配内联槽 `NotifySlot_`（C1，已落地） |
| 释放语义 | **不变**：身份（表项/配额/宽限期）先于应用放手释放，内存由引用计数撑住 |
| 保活 | **不变**：句柄继续持 `MuxConnection` 强引用（详见 §4.3） |

## 3. 因果链

### 3.1 为什么记录不带 `A`

`ChanRec_` / `DockBinding_` 的内容里没有任何堆分配（状态字是 `AtomicFlags`、窗口是
「自旋锁 + 普通字段」、通知槽是内联的），因此它们**不需要**分配器参数。`A` 出现在
智能指针上只有一个原因：`mm_ptr::Shared<T, A>` 把分配器**写进节点**
（`(&raw mut (*inner).alloc_).write(alloc)`），最后一个强引用 drop 时按它归还整块节点。
`A: AllocatorClone` 则是给每个新句柄克隆分配器用的。

因此正确分工是：**记录内容不带 `A`，句柄带 `A`，表槽带 `A`，注册表（四本
`BTreeMap::new_in(alloc)`）本来就带 `A`**——泛型面没有扩大，只是从「状态节点句柄」
挪到「身份节点句柄」。等 Arena/`NonNull` 落地后，`A` 会从句柄上消失、归到连接/arena 一侧。

### 3.2 为什么本轮不做 `NonNull`

因为 `NonNull<DockBinding_>` 要成立，必须先解决「句柄可能活过身份」：

```text
对端 drop(tx) → 本端收 FIN  → PEER_FIN   → rx_done = true
对端 drop(rx) → 本端收 RESET → PEER_RESET → tx_done = true
⇒ claim_release_ 通过 ⇒ release_channel_ 把表项换成 WaitClose 墓碑
   —— 而**本端应用此刻可能仍持有 ChannelTx 与 ChannelRx**
```

这不是边角情形：对端正常关闭两个方向就会发生。今天不悬空，靠的正是 `Shared` 的
引用计数（**身份先释放、内存后回收**）。要用 `NonNull` 就必须先把「槽位回收」与
「所有持有者放手」绑定：条件至少是「身份已释放 + 应用两个半部都已 drop + 两个循环的
本地表项都已移除」，可由状态字上一次 CAS 认领（`APP_TX_CLOSED` / `APP_RX_CLOSED`
两位今天已有）。这属于 Arena 那一轮，本轮不做。

另有一条独立于释放的危害：宽限期到期后 `reap_wait_close_` 会删掉键，**同一 dock 对
可以重新登记**，而旧句柄可能仍在——所以槽位复用必须防止旧句柄撞上新一代（`NonNull`
不带世代号时，上面那套「放手才回收」正好也排除了这一情形）。

### 3.3 为什么 `Establish_` 可以不用通道（以及风险如何控制）

`sync_` 模块文档早先写着「唤醒一律走持久通知通道，因此不再有手写的 `poll` 与 waker 槽
（旧 `WakerSlot_` 已删除）」。本轮改为每条子流一个的 `NotifySlot_`，理由是**每子流一次
全局分配**（`audit-heap-alloc` §3.1 #7）——推翻的是当时那个选择，不是当时对丢唤醒的
警惕。风险控制：

- 协议写进类型文档：**通知方**先置持久位、再取走 waker，锁释放**之后**才 `wake`
  （`wake` 可能同步重入 `poll`，持自旋锁调用就是单线程上的自死锁）；**等待方**先消费
  持久位、没有则登记 waker、**登记之后再复检一次**；
- 「至多一个等待者」是调用方前提（与 `CancelToken_` 同一条契约），当前唯一用户是建流
  等待：`ChannelHandle` 是 `!Clone`、`accept_async` 取 `&mut self`；
- 三条用例 + 一条压测：`sync_::tests_` 的「无等待者也持久」「登记后恰好唤醒一次」
  「4096 轮高频置位不丢」，以及 `owner_::tests_` 改写后的
  `establish_notification_is_persistent`。

## 4. 落地与待办

### 4.1 C1（已落地，2026-10-05）：`Establish_` → `NotifySlot_`

- `connection/sync_.rs`：新增 `NotifySlot_`（`AtomicBool` + `SpinningMutexOwned<Option<Waker>>`，
  零堆分配），模块文档「唤醒」一节按「实例数」重写；
- `connection/owner_.rs`：`ChannelState_.establish_` 由 `Establish_`（`flume::bounded(1)`）
  改为 `NotifySlot_`；`establish_notify_rx_` 删除，改为 `establish_poll_wait_(&self, cx)`；
  `wait_establish_` 改为「查状态 → `poll_fn` 等槽 → 与取消竞争」；
- `registry_.rs` 的 `mark_failed_` 调用点**不变**（仍是 `state.notify_establish_()`）。

验收（本轮实测）：`cargo test --lib` **176 passed**；`--test inmem_mux` 20；
`--test smoke_tokio` 4；`--test smoke_compio`（`--no-default-features`）4；
`--test thread_safety` 2；`--doc` 4；`cargo clippy --all-targets -- -D warnings` 零告警。

**分配账变化**：每子流全局分配 **2 → 1**（只剩 `MuxChanBuff` 的 `Arc<dyn Allocator>`，
audit #5）；每子流注入分配仍是 3（`Shared<ChannelState_>` + 2×`Shared<Ring>`），形状三
落地后才会降到 1。

### 4.2 待办（按序）

| 序 | 内容 | 预期收益 |
| --- | --- | --- |
| C0 | 计数分配器基线用例（建连 → N 子流 → 双向收发 → 拆流，分记注入/全局） | **已落地（本轮）**：`tests/alloc_count.rs`，数字与修正见 `audit-heap-alloc…` §F |
| C2 | 形状三：`DockBinding_` 非泛型节点 + `BindingSlot_<A>` 表槽；`ChannelOwner_` 变成「类型化句柄 + 构造期校验 + 一处 `unsafe`」；`ChannelState_` 内联进节点 | **已落地（本轮）**：见 §4.5；**分配次数不变**（每侧仍是 1 个节点），句柄 8 → 16 B |
| C3 | telegraph / listener 也拿 `Shared<DockBinding_>`；listener 通知槽内联（audit #6） | **已落地（本轮）**：见 §4.6；监听期全局分配 −8 |
| C4 | ~~`pending_fin` 改注入分配器（N1）~~ **已落地**；`MuxChanBuff` 的 `Arc<dyn Allocator>` 降每连接一份（audit §5.5-A） | N1 完成；`Arc` 那半**需要公开 API 决策**（`MuxChanBuff::pair_from_alloc_` 是公开方法），见 `audit-heap-alloc…` §F.4 |

**别把 C2/C3 的收益算成「分配次数下降」**：形状三消掉的是「两个可独立死亡的对象」，
每侧的节点数仍是 1。真正能降次数的是 #1（每帧一次全局 `Vec`）、#5（每连接一份 `Arc`）与
N1——实测基线见 `audit-heap-alloc-20261004-1122.md` §F：**每条子流 10 次注入分配**
（2 状态 + 4 环存储 + 4 环节点，两侧合计；原 §E 的「3」是单侧且漏了环存储）、
**稳态搬运 0 次注入分配 / 每帧 ≥1 次全局分配**。

### 4.4 C0（已落地，2026-10-05）：基线 + 一条丢唤醒回归

- 新增 `tests/alloc_count.rs`（单目标、单用例、只用 tokio：`#[global_allocator]` 是进程级
  的）+ 计数注入分配器（`CountMuxConfig`，自带一份只换分配器的建连辅助）；
- **顺带复现并修掉一条丢唤醒**：安静连接上、第一条子流、第一次写入低于临界水位
  （512 B < 1024 B）时一条 `TxReady` 都不发，复用循环 `last_ready` 为空 ⇒ 数据无人搬运、
  直接挂死。修法是给 `ChannelTx::notify_tx_ready_` 的门槛补一条「**进入写入时环为空**」；
  新增带看门狗的回归用例 `tests/inmem_mux.rs::mux_idle_small_write_inmem_dual_`
  （场景 `tests/common/scenarios_/idle_write_.rs`）。因果与验收见
  `outlook-concurrency-20261002-2322.md` §12-T3、数字见
  `audit-heap-alloc-20261004-1122.md` §F。

### 4.3 本轮**没有**改的两件事（有意保留，勿误读）

- **保活链**：子流句柄仍持 `MuxConnection` 强引用。理由：注册表**刻意不持核心**
  （循环持注册表；注册表若持核心就永不析构，四个循环与状态永久泄漏）。若句柄只持
  `Shared<DockBinding_>`，最后一个应用对象丢弃后核心析构、循环被取消，句柄会挂在一个
  死连接上。把根迁到注册表是另一轮设计。
- **释放语义**（§3.2）：身份仍可先于应用放手而释放。

## 5. 遗留与风险

1. `NotifySlot_` 的「至多一个等待者」目前只由调用方保证，没有运行期检查。若将来出现
   第二个等待者（例如允许并发 `accept`），必须改成等待者列表（零分配侵入式链表）。
2. 手写 `poll` 的丢唤醒属于**偶发挂死**类缺陷，用例覆盖的是协议而非调度；C2/C3 会
   再动一次等待路径（`wait_establish_` 的等待对象可能换成记录句柄），届时三条用例与
   压测必须一起跑。
3. C2 的 `unsafe` 取用尚未落地：不变式、安全注释与「构造时校验一次」的落点要在 C2 的
   提交里写明（AGENTS §7）。
### 4.5 C2（已落地，2026-10-05）：形状三的节点与类型化句柄

**形状**（`owner_.rs` 拥有节点与句柄，`registry_.rs` 只拥有表槽）：

```rust
// 身份节点：非泛型、内部零堆分配；C2 只落了 Channel 变体（Telegraph/Listener 见 C3）
pub(crate) enum DockBinding_ { Channel(ChannelState_) }

// 类型化句柄：保活 + 指向节点内载荷的引用（构造期校验一次）
pub(crate) struct ChannelOwner_<A> { node_: Shared<DockBinding_, A>, state_: &'static ChannelState_ }

// 注册表表槽：冷状态内联在表里，热状态在节点里
struct ChanSlot_<A> { rec_: ChannelOwner_<A>, inbound_: Inbound_ }
enum BindingSlot_<A> { Channel(ChanSlot_<A>), Telegraph(TgCtx_), Listener(LsnCtx_), WaitClose(..) }
```

**付法 1 的实现细节（与原设想不同，值得记下来）**：原方案说「构造时校验一次 + 一处
`unsafe` 取用」，我先把载荷指针存成 `NonNull<ChannelState_>`，但 **`NonNull` 是
`!Send`/`!Sync`**——它会打破 `ChannelOwner_: Send + Sync` 这条 `tests/thread_safety.rs`
钉住的边界，逼出两条 `unsafe impl Send/Sync`。改用 **`&'static ChannelState_`**：引用在
`T: Sync` 时自动 `Send + Sync`，于是：

- `unsafe` 只有**一处**：构造函数里把节点内载荷的引用生命周期延长到 `'static`
  （真正的约束是「`node_` 活着」，而它与该字段同生共死；`Deref` 只在 `&self` 的生命周期
  内交出引用，`'static` 不会泄漏）；
- `Deref` 是一次普通加载：**零分支、零 panic**，热路径符合付法 1 的初衷；
- 句柄从 8 B 涨到 16 B（`Shared` + 引用），表槽同步变大；**分配次数一次没变**。

**验收**：`lib 176 / inmem_mux 22 / smoke_tokio 4 / thread_safety 2 / alloc_count 1` 全绿，
`clippy -D warnings` 零告警；`alloc_count` 的注入/全局**次数与 C2 之前逐项相同**
（20 / 3 / 93 / 0 / 2 与 46 / 14 / 395 / 1608 / 98），只有字节数因句柄变大而略增。

### 4.6 C3 + N1（已落地，2026-10-05）：三类身份统一 + listener 通知内联

**形状**（`owner_.rs` 一处 `unsafe`、三个类型化别名）：

```rust
pub(crate) enum DockBinding_ {
    Channel(ChannelState_),   // C2
    Telegraph(TgRec_),        // 占位：收发未实现，节点内部零堆分配
    Listener(LsnRec_),        // LsnRec_ { notify_: NotifySlot_ }，取代 flume::bounded(1)
}

pub(crate) struct DockHandle_<T: ?Sized + 'static, A> {   // 唯一的 `unsafe` 在这里
    node_: Shared<DockBinding_, A>,
    payload_: &'static T,     // 构造时校验变体一次，`Deref` 零分支零 panic
}
pub(crate) type ChannelOwner_<A> = DockHandle_<ChannelState_, A>;
pub(crate) type TgOwner_<A>      = DockHandle_<TgRec_, A>;
pub(crate) type LsnOwner_<A>     = DockHandle_<LsnRec_, A>;

enum BindingSlot_<A> {               // 注册表表槽
    Channel(ChanSlot_<A>),           // 冷状态留在表里（写锁保护，零额外可变性）
    Telegraph(TgOwner_<A>),          // 字段暂无读取者（telegraph 未实现），
                                     // `#[allow(dead_code)]` + 注释说明保留理由
    Listener(LsnOwner_<A>),
    WaitClose(WaitCloseCtx_),
}
```

`ChannelListener` 改为持 `LsnOwner_<C::Alloc>`，`income_async` 的第 3 步用
`poll_fn(|cx| rec.poll_wait_(cx))` 等通知（与建流等待用同一套「登记 → 复检」协议）；
`Telegraph` 也持一份 `TgOwner_`（保活 + 后续路由的挂点）。

**N1（C4 的一半）**：`session_` 的 `pending_fin` 由 `BTreeSet::new()`（全局分配）改成
`BTreeSet::new_in(注册表分配器)`——「丢半边 + 额度为 0」的收尾路径不再落到全局堆。

**验收**：`lib 176 / inmem_mux 22 / smoke_* 各 4 / thread_safety 2 / alloc_count 1` 全绿，
`clippy -D warnings` 零告警。计数（**更正口径后**）：监听期全局 11 → **3**，建流期 302 →
**286**，稳态仍是每帧 ≥1 次全局（#1 未动）。

> **计数口径更正**：第一版计数注入分配器转发给 `Global`（= 计数全局分配器），导致
> 每笔注入分配被重复计入全局；现已改为转发 `System`。基线表以
> `audit-heap-alloc-20261004-1122.md` §F.4 为准。

### 4.7 余下的一步：C4 的 `Arc<dyn Allocator>`（**需要你拍板**）

`MuxChanBuff::pair_from_alloc_` 是**公开方法**，本次没动。两条路（见
`audit-heap-alloc…` §F.4）：**A** 加一个新构造函数收 `Arc<dyn Allocator + Send + Sync>`
（旧方法保留、内部转调），**B** 直接改签名（破坏性）。

**成本已实测（§F.7）**：擦除路径 = 基准 + **每侧 1 次全局分配**（每子流 2 次），注入侧
完全不变；A/B 落地后应降为「每连接 1 次」。`tests/alloc_count.rs` 已把这条关系钉成断言，
也就是这两条路的**验收条件**。
