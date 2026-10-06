> **⚠ 后半段已被取代（2026-10-06 13:52）**：本文描述的「注入式 `Clock`
> （`embedded_timers`）+ `TrDeadline` 绝对期限算术层」**已删除**。`abs_art` 随后把
> **时刻**也收回到运行时值上（新增 `TrClock`，且 `TrTime: TrDelay + TrClock`），
> 于是 `smux_v1::time` 只剩「自建连 epoch 起算的毫秒」这一层记账。
> 取代者：`runtime-adoption-20261006-1352.md`。
>
> 本文保留为**决策史**：它记录了「为什么当时把绝对形式留在消费方、并注入一个时钟」
> ——那条推理成立的前提是「抽象层不暴露 `Instant`」，而该前提在本轮被上游改写
> （`Instant` 成了 `TrClock` 的关联类型，由运行时值给出）。§6 的遗留项 1、2 随之关闭。

# 计时能力上移到 `abs_art` 家族：`smux_v1::time` 变成纯算术层

日期：2026-10-05 12:30
性质：**实现记录 + 试用结果**（`abs_art` 侧已提交；`smux_v1` 侧为本地 spike）。
取代：`time-mod-20261005-1130.md`（那版把「轮盘式计时器」建在 `smux_v1` 里）。
关联：`keepalive-20261005-0901.md` §5.2（异步等待源）本轮被消解。

---

## 1. 这一轮的转折：轮盘本来就是「缺能力」的补救

上一版把 compio 的**轮盘**（`Rc<RefCell<BTreeMap<(期限, 序号), Option<Waker>>>>`）
移植进 `smux_v1::time`，理由是「本仓没有全局运行时，拿不到可等待的计时源」。

但真正的缺口只有一个：**后端没有提供「异步睡到某时刻」这个能力**。把这个能力补进
`abs_art` 家族之后，轮盘**整个消失**——它存在的唯一理由是当时拿不到它。

`abs_art` 侧的形状（trait 在 `abs_art`、实现在三个后端，**零依赖、no_std、无 alloc**）：

| 项 | 形状 |
| --- | --- |
| `TrTime: TrDelay` | `interval(Duration) -> impl TrInterval` + `timeout(Duration, F)`（都是关联函数）。**一次性睡眠就是 `TrDelay::delay`**，不另起名字 |
| `TrInterval` | `tick(&mut self)` |
| `Elapsed` | 零依赖手写错误类型（`Display`「期限已到」+ `core::error::Error`） |
| `TrTime::timeout` | trait 的**默认方法**（挂在运行时类型上，`D::timeout(d, f)`），一份实现 |

关键设计：**抽象层不暴露 `Instant`**（`abs_art` 不能写 `std::time::Instant`，也不该为
此引入外部时钟抽象）。绝对形式对持有自己时钟的消费方是可推导的，而且**Duration-only
换来了可注入的假时钟**。完整因果与被否决方案见 `abs_art/dev-notes/time-20261005-1225.md`。

## 2. `smux_v1` 侧：删除 vs 保留

| | 上一版（轮盘） | 本轮 |
| --- | --- | --- |
| 等待源 | 自建轮盘 + `Timer<C>` 句柄 | 后端 `TrTime` |
| 分配 | 每次登记一个 `BTreeMap` 节点 | **0**（后端自带计时器） |
| `!Send` / `RefCell` / 唤醒重入 | 有（需专门纪律） | **无** |
| §5.5「登记时唤醒驱动方」 | 未决项 | **不存在了**（没有共享轮盘要唤醒） |
| 确定性测试 | 假时钟 + 手动驱动轮盘 | 假时钟 + 假 `TrTime`（更简单） |
| 绝对期限算术 | 在轮盘里 | 留在 `src/time/deadline_.rs`（注入式 `Clock`） |

`smux_v1::time` 现在只有：

- `TrDeadline`（smux 自己定义，对**所有** `T: TrTime` 做 blanket 实现）：把「后端计时」
  与「注入式时钟」绑成**绝对期限**的**运行时关联函数**——
  `D::sleep_until(&clock, deadline)` = `D::delay(deadline − clock.now())`；
  `D::timeout_at(&clock, deadline, future)` = `D::timeout(deadline − clock.now(), f)`；
- 转出 `abs_art` 的 `TrTime` / `TrInterval` / `Elapsed`。

**为什么要那个 trait**：两个方法各需要「后端计时 + 注入时钟」两样东西。做成**自由
函数**的话调用点会是 `sleep_until::<D, C>(&clock, deadline)`——运行时类型退化成
turbofish 参数，与 `abs_art` 家族既有的 `Runtime::block_on(..)` / `Runtime::delay(..)` /
`scope.spawn_local(..)` 形状不一致。trait + blanket 实现之后，调用点就是
`D::sleep_until(..)`，一眼能看出用的是哪个运行时。

相对形式（`D::delay(d)` / `D::interval(p)` / `D::timeout(d, f)`）**不在这里重新包装**：
调用方直接用 `abs_art` 的 `TrTime`。因此本模块只剩「绝对期限 → 相对时长」这一件事，
删掉了 4 个文件里的 3 个（`wheel_.rs` / `sleep_.rs` / `interval_.rs` / `timeout_.rs`
→ 只剩 `deadline_.rs`）。

## 3. 分层（本轮定下来的那条缝）

| 层 | 归谁 | 为什么 |
| --- | --- | --- |
| 「睡一段 / 每周期醒」 | 后端 `TrTime` | 只有运行时知道怎么等；三后端一致性由 `abs_art-smoke` 的契约矩阵钉住 |
| 「什么时候该醒」 | 本地 + 注入式 `Clock` | epoch / 空闲毫秒 / 宽限期是**协议语义**；注入式时钟让它们可确定性验收 |

保活的 tick 循环因此变成（不再有轮盘）：

```rust
loop {
    let next = 扫注册表算最早期限();          // 本地算术（注入式 Clock）
    D::sleep_until(&clock, next).await;       // 后端等待（运行时关联函数）
    timer_round();                             // 投 PULSE / 激活静默循环
}
```

## 4. 试用结果（这就是「好使不好使」的答案）

**好使**，而且是净减法：

- `smux_v1` 编译通过、`clippy --all-targets -D warnings` 零警告；
- `cargo test --all-targets` 全绿（186 lib + 全部集成用例，含 1024 子流 socket 冒烟）；
- `cargo test --doc` 全绿；`src/time` 无 rustdoc 警告；
- **10 个 `time` 单测**：8 个确定性（假时钟 + 假 `TrTime`：正好停在期限、已过期限不让
  时钟前进、内层赢时秒表不走、超时即丢弃内层、保活式循环三轮）+ **2 个真实后端**
  （`#[tokio::test]` / `#[compio::test]` 各一格，真实时钟 + 真实后端，断言不提前返回）；
- 上游 15 格跨后端契约矩阵（3 后端 × 5 条契约）全绿。

## 5. 本地依赖的处置（**未提交**）

`smux_v1/Cargo.toml` 目前把 `abs_art` / `abs_art-tokio` / `abs_art-compio` 指到
`../abs_art/…`（**本地路径**，已加注释标明是 spike）。原因：`feat/time` 只在本地
分支上，`smux_v1` 通过 git 依赖**取不到**它。

因此本轮 `smux_v1` 的改动**故意不提交**：若现在提交（`src/time` 用上了 `TrTime`），
别人拉到的 `smux_v1` 会因为 git 上的 `abs_art` 还没有 `TrTime` 而**编译不过**。

落地顺序应当是：`abs_art` 的 `feat/time` 推上去（或合并后打 tag）→ `smux_v1` 的依赖改成
`git = …, branch/tag = …` → 再提交 `src/time` 的改动。

## 6. 遗留

1. **`x_deps` 是否导出 `abs_art`**：`smux_v1` 的公开签名里一直有 `abs_art` 的 trait
   （先是 `TrLocalScope`，现在多了 `TrTime`），因此它是既有状况而非本轮新增；但既然
   消费方要写 `Runtime<{ FULL }>`，导出一份会更省事。仍是公开面决定。
2. **`D: TrTime` 怎么进 `MuxConnection`**：加第三个类型参数，还是把 `TrTime` 加到
   作用域值（`S: TrLocalScope + TrTime`，上游已为此给三个后端的 `LocalScope` 也实现了
   `TrTime`）。这是 §5.4「`Clock` 注入形状」的姊妹问题，接入保活循环时一并定。
3. **真实运行时的共享测试**：两格 `#[tokio::test]` / `#[compio::test]` 已覆盖「能等」，
   但同一份测试体跨两运行时（`dual_runtime_test_!`）需要后端类型按运行时分派，
   留待保活用例落地时按 `tests/` 的 `.inc` 机制处理。
4. **`interval_at` 的去留**：`TrTime::interval` 锚定在**调用时刻**，无法指定绝对起点；
   需要「从某个绝对时刻起算的周期」的调用方用 `sleep_until(start)` + `interval(p)` 拼。
   smux 目前不需要，故未保留上一版的自建 `Interval`。

## 7. 后续改型（同日）：全部改成**关联类型/GAT**，去掉 RPITIT

`abs_art` 侧的返回类型原本是 `impl Future`（RPITIT）。RPITIT 是不透明类型，签名里
**写不出** `Send`/`Unpin`，调用方也就无法在编译期判定「这个睡眠 future 能不能跨线程
投递」。现改为：

| 项 | 现在 |
| --- | --- |
| `TrDelay` | `type Delay: Future<Output = ()>` |
| `TrInterval` | 真正的 GAT：`type Tick<'a>: Future<Output = ()> where Self: 'a` |
| `TrTime` | `type Interval: TrInterval`；`timeout` 默认方法返回**具体结构体** `abs_art::Timeout<Self, F>` |
| `smux::TrDeadline::sleep_until` | 返回 `Self::Delay`（后端的具体类型） |
| `smux::TrDeadline::timeout_at` | 返回 `abs_art::Timeout<Self, F>`（具体类型） |

收益：tokio / smol 各有一条编译期正向断言 `<Runtime<{FULL}> as TrDelay>::Delay: Send`
能通过；`Timeout` 的 `Send` 由字段结构地决定。代价：家族里**只有 `abs_art-compio`
需要 nightly**（`impl_trait_in_assoc_type`——compio 的 time future 全是 `pub async fn`，
是唯一拿不到可命名类型的后端）。完整因果见 `abs_art/dev-notes/time-20261005-1225.md`
§10–§12。

另有一条本仓踩到的细节写进用例注释：**假后端的 `delay` 不能在「被调用」时推进虚拟
时钟**，而应在「第一次被 `poll`」时推进（真实后端在构造那一刻只是把期限记下来）。
本轮 `timeout_at_returns_the_output_when_the_inner_future_wins` 正是因为这个原因先红
后绿的——`Timeout` 在构造时就起了计时器。
