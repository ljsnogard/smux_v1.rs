# 保活与空闲超时（写法 B）：**第五个循环落地**

> **⚠ 历史记录（形态已被取代，2026-10-06 16:25）。** 本文记的是当时那一轮的方案与
> 因果，其中「`MuxConnection` 的类型/入参形状」与「作用域是否携带计时能力」两处
> **已经不再成立**。现行事实见
> [`timer-mock-clock-and-generic-drop-20261006-1625.md`](timer-mock-clock-and-generic-drop-20261006-1625.md) §6：
>
> - `MuxConnection<C>`——**只有配置一个类型参数**；运行时值由
>   `TrConnCfg::Rt`（`type Rt` + `fn runtime()`）提供，类型是 `C::Rt`；
> - `new(delivery, config, read_buff, write_buff)` 自己取运行时值与本地作用域
>   （后端经 `abs_art-bridge` 选定，缺省 compio）；要显式控制走 `new_with_rt`；
> - `TrLocalScope` **不**携带计时能力：`TrDelay` / `TrClock` / `TrTime` 都实现在
>   运行时值上（本文多处「作用域也实现 `TrTime`」的说法属于当时的中间形态）。
>
> 本文的**问题分析、实测证据与协议侧结论**仍然有效；受影响的是 API 形状。


日期：2026-10-05 14:20
性质：**实现记录 + 裁决落实 + 三处关键修正**。
取代：`keepalive-20261005-0901.md` 的 §4（施工顺序）与 §5（待裁决）——那些都已
在本轮定案并落地；该文保留为决策史。
依赖：`abs_art` 家族的 `TrTime`（`LocalScope` 亦实现）、`embedded-timers v0.4.0`
（`Clock` / `Instant`，本轮起启用其 `std` feature 以让 `std::time::Instant`
成为 `Clock::Instant`）。
关联：`time-capability-20261005-1230.md`（计时能力上移）、
`outlook-concurrency-20261002-2322.md` §12 T6。

---

## 1. 本轮裁决（人类，四项 + 一处语义）

| 问题 | 裁决 | 落点 |
| --- | --- | --- |
| `Clock` 怎么注入 | `TrConnCfg` 加 `type Clock` + `fn clock()`，`DefaultConnCfg` 给系统时钟 | §3.1 |
| `TrTime` 怎么进连接 | 复用作用域值：`new` 上 `S: TrLocalScope + TrTime` | §3.2 |
| tick 频率 | **动态**算最早到期 + 注册新身份时唤醒计时循环 | §3.3 |
| 超时的可观测面 | **新增**子流级错误回传面（`abort_reason()`） | §3.4 |
| 超时语义 | **只拆该子流，不拆连接** | §3.4 |

---

## 2. 落地形状

### 2.1 第五个循环

`src/connection/timer_.rs`：一个连接**一个**计时循环，与另四个同级、经同一个
`spawn_local` 投递，取消令牌序号 `4`。它不搬字节，只做两件事：认领保活动作、投递。

```text
loop {
    scan = 注册表.timer_scan_(now, timeout/2, timeout, &mut actions)   // 一次读锁
    锁外投递每个动作：PULSE → 写循环；Abort → 先落宽限态，再 CLOSE×2 + PeerClosed + Release
    睡到 { scan 给出的最早期限 | 注册新身份的通知 | 取消 }
}
```

- **锁内只做原子读改写**（判定与认领），不 `await`、不投事件、不 `wake`；
  投递一律留到锁外（唤醒纪律，见 §3.5）；
- 每轮**限频** `K_TIMER_ACTION_BATCH = 64` 条动作；写满就把 `is_full_` 报给调用方、
  由它**立刻**再扫一轮（动作不丢，只是分轮）；
- 与另四个循环同一条纪律：**不持有 `MuxCore` 强引用**，否则核心永不析构、取消令牌
  永不触发。

### 2.2 时间：连接内毫秒 + 两条时钟

`src/time/clock_.rs` 给出 crate 内部的 `ConnClock_`：注入的 `Clock` 值 + **建连时刻
（epoch）**，对外只有 `now_millis_()` / `deadline_(ms)`。于是协议里的时间量全是
`u64` 毫秒：

- `ChannelState_`：`active_millis_`（存活时钟）与 `data_millis_`（保活职责时钟）；
- 注册表的拆流宽限期索引：`BTreeSet<(u64, Dock, Dock)>`；
- `src`（除 `SystemClock` 与测试）**不再出现任何 `Instant` 类型**——正是
  `keepalive-20261005-0901.md` §3.2 要的那条缝。

### 2.3 派生的动作

- **`PULSE`**：`WindowUpdate` 的同形帧（带 `RecvTotal` + `RecvWindow`），载荷取
  `RecvWindow::report()`——它本就是「保活无条件用它取当前窗口」的那个入口，
  因此保活顺带完成一次窗口重同步；
- **拆流**：`claim_abort_` 认领一次 → `release_channel_` 落**宽限态**（此后在途帧
  静默丢弃）→ `CLOSE(FIN)` + `CLOSE(RESET)` 各一条给对端 → `WriteEvent_::PeerClosed`
  + `ReadEvent_::Release` 让两个循环各摘本地表项。

---

## 3. 本轮的六处关键修正（都是实测撞出来的）

### 3.1 修正一：**两条时钟**——收到 `PULSE` 不能让本端「少发一条」

**症状**：两端超时都设 1 秒、一条子流完全无数据时，`tests/keepalive.rs` 的
「互发 PULSE 应当都活着」用例里恰好**有一侧**被判 `IdleTimeout`。

**因果**：原设计只有一条「最后活动」时钟，`PULSE` 的发送判据是「空闲跨过阈值」。
于是两端在同一轮扫描里互相把对方的空闲清零：

```text
t=500  B 先扫到阈值 → 发 PULSE → B 的职责时钟自己不动（正确）
t=500  A 稍后被这条 PULSE 刷成「刚活动」→ A 不发了
t=1000 B 的存活时钟还停在 t=2（它一个字节都没收到）⇒ B 判空闲超时
```

**修正**：拆成两条时钟，并且**收到对端的 `PULSE` 只刷存活时钟**：

| 时钟 | 谁写 | 判什么 |
| --- | --- | --- |
| `active_millis_` | 收到的**任何**帧（含 `PULSE`）、本端写出的数据 | 距它 `max_channel_timeout` ⇒ 拆流 |
| `data_millis_` | **非保活**活动 + 本端上次发 `PULSE` 的时刻 | 距它 `timeout/2` ⇒ 发 `PULSE` |

于是两端都按自己的职责时钟继续发 `PULSE`，每一端每隔 `timeout/2` 必然收到对端一条，
存活时钟的余量正好是另一半——能容忍一整个周期的抖动。**本端发出的 `PULSE` 不刷存活
时钟**：否则对端已死时本端会一直给自己续命，空闲超时永不触发（这正是
`tests/keepalive.rs` 第一条用例的判别点）。

> 协议文档 `connection/mod.rs` §7.1 原先写的是「既无数据、也无**任何方向的** PULSE
> 往来时发 PULSE」——那句话按字面实现就是上面这个 bug。已按本节改写。

### 3.2 修正二：活动时刻必须**就地**写下，不能等扫描打点

**症状**：修正一之后，`PULSE` 仍然一侧不发：一端的职责期限被算成
`成立后 + 整个保活周期`。

**因果**：初版让活动点只置一个**脏位**、由计时循环扫描时打点（想省掉热路径的
`Instant::now()`）。但扫描有周期：一端在 `t=1` 扫描、`t=2` 收到建流帧（置脏），
下一次扫描是 `t=502`——于是「最后活动」被记成 **502** 而不是 2，本端的 `PULSE`
被推到 `1002`；对端的存活时钟从 2 起算，`t=1002` 就到了超时。两者只差 1 ms。

**修正**：`touch_(now_millis)` / `mark_data_(now_millis)` 由调用方把**活动发生那一刻**
传进来。解复用 / 复用循环手上都有连接级时钟，每帧读一次 `Instant::now()`（vDSO 读）
相对该帧的解析与环操作可以忽略。脏位与「扫描时打点」整套机制随之删除。

### 3.3 修正三：拆流必须**显式关闭环半部**（`drop` 不置位关闭标记）

**症状**：拆流之后 `abort_reason()` 已经是 `Some(IdleTimeout)`，但应用侧的
`is_tx_closed()` / `is_rx_closed()` 仍是 `false`——应用读不到 EOF。

**因果**：`buffex` 的 `RingWriter` / `RingReader` **没有 `Drop` impl**：丢掉一个半部
不会置位「生产端 / 消费端已关闭」。因此
- 解复用循环处理 `ReadEvent_::Release` 时要把 `writer_.close()` 显式调一次
  （此前只 `table.remove`）；
- 复用循环处理 `PeerClosed { reset_ }` 时要把 `reader_.close()` 显式调一次，
  否则应用会继续往一条没人取的发送环里写。

两条都是**既有的缺口**，由本轮的「超时拆流」路径第一次踩到（对端 `FIN` 那条路径
本来就在 `Close` 分支里显式 `close()` 过，所以一直没暴露）。

### 3.4 修正四：拆流路径必须用**专用事件**，不能借用「对端发来 `RESET`」

**症状**：把 `close()` 加在 `WriteEvent_::PeerClosed { reset_: true }` 上之后，
`tests/inmem_mux.rs::mux_recv_dropped_dual_` 立刻红了——`A` 在「对端丢弃接收半边」
之后写满一个窗口会拿到 `Closing`。

**因果**：那个用例钉住的是**对端** `RESET` 之后的语义——「对端在收到我们 `RESET`
之前已经收到的字节仍然写得完，到达对端后被静默丢弃」。协议对此只要求「**允许**放弃
已提交字节」（§7.0），并不要求立刻关闭发送方向。两条路径的差别是**谁有权关**：

- **对端 `RESET`**：对端已经声明不看，本端把待发字节丢掉即可；发送环消费端**不**关，
  免得把「已经写完、正在途中的那一次 `write_all`」打断；
- **本端计时拆流**：没有任何别的执行者会替这条路径关它，不关就永远不会关。

**修正**：新增 `WriteEvent_::LocalAbort`，只由计时循环发出；处理时显式
`reader_.close()` 并摘表。`PeerClosed { reset_ }` 恢复原样（只丢数据、不关环）。

### 3.5 修正五：`dual_runtime_test_!` 对这类用例不再成立

**症状**：`tests/inmem_mux.rs`（11 个用例 × 2 变体）与 `tests/layered_rpc.rs` 的
`::compio_` 变体开始随机 panic「no reactor running」。

**因果**：这些文件一直用 **tokio** 的 `LocalScope`，再套 `dual_runtime_test_!` 生成
tokio / compio 两个变体。前四个循环不碰计时器，所以那种写法「看起来能跑」——compio
运行时里 `LocalSet::run_until` 照样能驱动这些任务。第五个循环一落地就会调
`tokio::time::sleep`，在没有 tokio reactor 的 compio 运行时里直接 panic。是否命中
取决于计时循环有没有在用例结束前被 poll 到，因此表现为**随机失败**。

**修正**：新增 `single_runtime_test_!`——**每个 feature 组合只生成一个变体**（有
`test-tokio-runtime` 就生成 tokio 变体，否则生成 compio 变体），用例体按同一个
feature 选作用域类型。`inmem_mux` / `layered_rpc` 改用它，`justfile` 的 compio 配方
补上这两个 target。附带收益：这两个文件在 compio 侧终于是**真的**跑在 compio 后端
上（此前只是「在 compio 运行时里跑 tokio 作用域」）。

### 3.6 修正六（附带）：对端「沉默」也能立刻收尾

拆流时给对端发 `CLOSE(FIN)` + `CLOSE(RESET)` 各一条：一条帧只能表达一个方向，
两条合起来让对端两个方向都收尾，不必等它自己的空闲计时。对端看到的因此是**正常
关闭**（`abort_reason()` 为 `None`），与「被保活判定拆掉」在应用侧可区分。

---

## 4. 裁决落实细节

### 4.1 `Clock` 注入（公开面）

```rust
pub trait TrConnCfg {
    type Clock: Clock + Clone + 'static;   // 新增
    fn clock(&self) -> Self::Clock;        // 新增
    // …既有项不变
}
```

`DefaultConnCfg` 的实现用新增的 `smux_v1::time::SystemClock`（`Instant =
std::time::Instant`）。为此 `embedded-timers` 启用 `std` feature——没有它就只能用
`Instant32` / `Instant64` 这类 tick 计数器，而本 crate 没有产出真实 tick 的时钟源。

`time` 模块另外转出 `Clock` 与 `Instant` 两个 trait：实现 `TrConnCfg` 的调用方要能
命名它们。

### 4.2 `TrTime` 进连接（公开面）

`MuxConnection::new` 与 `from_delivery` 的 `S` 约束从 `TrLocalScope + Clone` 变成
`TrLocalScope + TrTime + Clone`。**没有**给 `MuxConnection` 加第三个类型参数：上游
三个后端的 `LocalScope` 都实现了 `TrDelay` / `TrTime`，因此「谁提供队列」与
「谁提供计时」仍然只有一处。

### 4.3 动态期限 + 注册唤醒

- 注册表新增**计时唤醒槽**（`Shared<NotifySlot_, A>`，每连接一次分配，零堆分配语义
  与建流通知槽一致）；
- `reserve_channel_` 在**释放注册表守卫之后**才 `notify_timer_()`（锁内唤醒是纪律
  禁止的，见 §4.4）；
- 子流**活动不唤醒**计时循环：两条时钟只前进，因此期限只会**往后**推；早醒一轮只是
  多算一次，没有正确性问题。这一点让热路径省掉一次唤醒。

### 4.4 唤醒纪律（风险 2 的处置）

`keepalive-20261005-0901.md` §6.3 记的「在注册表锁内 `wake`」风险，本轮**新增的
调用方一处都不违反**：`notify_timer_` 只在守卫作用域之外调用。既有的两处
（`notify_inbound_` / `mark_failed_`）本轮**未动**——它们与本轮改动无关，留给下一轮
统一整改（见 §7）。

### 4.5 应用侧可观测面（公开面）

```rust
impl<C, S> ChannelTx<C, S> { pub fn abort_reason(&self) -> Option<MuxError> }
impl<C, S> ChannelRx<C, S> { pub fn abort_reason(&self) -> Option<MuxError> }
```

一条子流的两个半部读到**同一个**原因（它在共享状态里）。目前只有一个生产者：
空闲超时 → `MuxError::IdleTimeout`。存成 `AtomicU8` 代码、读出来才映射成
`MuxError`：`MuxError` 是普通 `Copy` 枚举，没有稳定整数表示，直接存位模式会随编译
选项变化。

---

## 5. 验收

### 5.1 确定性单元用例（`src/`）

| 位置 | 钉住什么 |
| --- | --- |
| `time::tests_` | `ConnClock_` 的 epoch / 毫秒折算 / 绝对期限；`millis_of_` 饱和 |
| `owner_::tests_` | 两条时钟互相独立（`touch_` 只刷存活、`mark_data_` 两个都刷）；中止只认领一次并投影成 `IdleTimeout` |
| `registry_::tests_` | 扫描按「阈值 → 存活到顶」两级推进且每级只产出一次；**收到 PULSE 不会让本端少发一条**；真实活动推后职责时钟；批次写满报 `full`；listener / telegraph / 宽限态不参与 |

### 5.2 端到端（真实时间、真实后端）

`tests/keepalive.rs`（tokio）与 `tests/keepalive_compio.rs`（compio）共用
`tests/keepalive_common.inc`：

- **`idle_channel_times_out_`**：A 超时 1 s、B 超时 1 h（显式改写
  `HandshakeDelivery::opts` 模拟「对端不做保活」）；1.6 s 后 A 侧两个半部都报
  `IdleTimeout` 且环进入关闭态，B 侧**无**中止原因；随后**同一条连接上**再建一条
  子流双向收发成功 ⇒ 拆的只是子流。
- **`keepalive_pulses_`**：两端都 1 s；2.5 个超时周期（≈5 个保活周期）后两侧都没有
  中止原因，且这条一直空闲的子流仍能双向往返。

> 两条必须**同时**成立：只留「互发 PULSE 活着」会让「发出 PULSE 也算活跃」的实现
> 蒙混过关；只留「对端静默就拆」会让「两端互相劝退」的实现蒙混过关。

**为什么分成两个 target**：计时循环必须跑在**真正支持计时**的后端上，而 tokio 与
compio 的作用域是两个不同类型，同一测试进程里不能互换（见 §3.5）。因此按冒烟测试的
既有约定拆成两个薄壳 + 一份 `.inc`，`justfile` 的 `test-compio` 配方同步加一条。

**原有的双运行时用例改走哪条路**：`tests/inmem_mux.rs` 与 `tests/layered_rpc.rs`
改用 `single_runtime_test_!`（每个 feature 组合一个变体 + 按 feature 选作用域），
因此它们现在**也在 compio 侧真跑一遍**——`just test` 的 compio 步骤已经带上这两个
target。默认 feature 下的 `--all-targets` 只跑 tokio 变体，compio 变体被 cfg 掉，
两边都不重复。

---

## 6. 分配记账

新增的分配点只有一处、且是**每连接一次**：计时唤醒槽的 `Shared<NotifySlot_>` 节点
（与注册表根部同源分配器）。热路径的增量是**每次活动一次 `Instant::now()`**
（vDSO 读，无分配）；`PULSE` 与拆流各按限频走 `flume`，属 audit 已登记的
「控制帧一次全局分配」例外。`tests/alloc_count.rs` 全绿（稳态计数未变）。

---

## 7. 遗留

1. ~~**`max_channel_timeout` 的协商粒度是整秒**~~ —— **已按裁决补完校验（同日）**。
   先纠正本文初稿的一处误记，再说改了什么：

   - **线格式不改**：`max_channel_timeout` 的单位**就是秒**（`max_channel_wait_close`
     同），改毫秒没有意义。线格式照旧是秒，调用方给出的非整数秒按 `as_secs()`
     截断——这是该字段的既定语义。
   - **初稿说「没有校验」是错的**：读侧的 `parser_` 早在收满一个基础项取值时就判
     `value == 0 → MalformedBody`（`0` 在 v1 里表示「本项未提供」，就该省略该键），
     发起方 `invite_async` 也在写出任何字节之前对**入参**逐条判了同一个条件。
   - **真正的缺口在等待方**：`listen_async` 的 `local` 经 `complete_invite_` 补进
     `ACCEPT` 直接写出，**没有任何校验**。于是「本端把 `max_channel_timeout` 配成
     500 ms」会写出一个取值为 0 的条目，被对端的解析器按**结构非法**拒绝——调用方
     从一个 `MalformedBody` 里看不出是自己的配置问题。

   补法是三层同一条规则（`>= 1`），都不用碰协议：

   | 层 | 位置 | 时机 |
   | --- | --- | --- |
   | 发起方入参 | `handshake_invite_async_` 的入参循环（原有） | 写 `INVITE` **之前** |
   | 等待方本端项 | `listen_handshake_async_` 开头（**新增**） | 读 `INVITE` **之前** |
   | 编码咽喉 | `write_frame_` 的条目循环（**新增**） | 任何一帧的任何一个条目 |

   第三层是「任何调用者都写不出取值为 0 的基础项」的本地保证，前两层负责在**线上
   出现半条帧之前**失败。`BasicOpts` 两项的文档补上了「合法范围 `>= 1` 秒」，并注明
   `Duration::from_millis(500).as_secs() == 0` 属于会被拒的输入。
2. **宽限态仍是惰性回收**：计时循环只跟存活 / 保活两个期限，不主动扫
   `wait_close_expiry_`。宽限态仍由 `reserve_channel_` / `release_channel_` /
   `is_wait_close_` 顺带回收。连接完全静默时墓碑会一直留着（有上界、不泄漏）。
   要不要把「最早到期」并进计时循环的期限集，下一轮定。
3. **锁内 `wake` 的统一整改**：`notify_inbound_` / `mark_failed_` 两处仍在内层
   `NotifySlot_::notify_` 时持有注册表守卫（见 `keepalive…` §6.3）。本轮新增的
   计时唤醒槽不在此列（§4.4），但两处旧账未清。
4. **本地路径依赖未推**：`smux_v1/Cargo.toml` 仍把 `abs_art` 家族指向
   `../abs_art/…`（`feat/time` 只在本地）。`abs_art` 推上去（或合并 / 打 tag）之后
   才能改回 git 依赖并提交本轮改动。
5. **对端 `RESET` 之后本端发送环的消费端**：`PeerClosed { reset_ }` 只丢数据、
   **不**关环（§3.4 的裁决），因此应用若在该子流上继续写，会一直写到环满、此后
   `Stuffed` 空转，而 `ChannelTx::is_rx_closed()` 也一直为 `false`。这是**既有**行为，
   本轮为了不动 `mux_recv_dropped_dual_` 的语义而保留。要不要统一成「身份释放时
   统一关两端的环」，下一轮与 §7.2 的宽限态回收一起定。
6. **`x_deps` 是否导出 `abs_art` / `embedded_timers`**：`smux_v1` 的公开签名里
   现在既有 `abs_art` 的 trait（`TrLocalScope` / `TrTime`），也有
   `embedded_timers::clock::Clock`（`TrConnCfg::Clock` 的约束）。目前 `time` 转出了
   `Clock` / `Instant` / `SystemClock`，够用；是否再经 `x_deps` 转出一整份仍是公开面
   决定。
