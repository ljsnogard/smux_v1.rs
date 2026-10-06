# 用 `abs_art-mock_clock` 取代自造 timer；以及「取消 `R` 泛型」为何仍被卡住

日期：2026-10-06 16:25
分支：`feat/abs_art-runtime`（承接 `runtime-adoption-20261006-1352.md`）
性质：**实现记录 + 阻塞点记录**。
关联：`runtime-adoption-20261006-1352.md`（上一轮的值化改造）、
`time-capability-20261005-1230.md`、`keepalive-timer-loop-20261005-1420.md`。
上游：`abs_art/dev-notes/local-scope-thread-local-20261006-1420.md`
（「作用域只能从运行时值经**固有**方法取得」是**刻意裁决**，不是遗漏）。

本轮人类下达三件事：①用 `abs_art-mock_clock` 取代自造 timer；②取消除 `C` 外的
泛型参数（理由是「`TrLocalScope` 现在应该能直接在构造时从 `abs_art` 取到」）；
③`MuxCore` 无条件 `Send + Sync`，并把因此被误改的多线程测试改回来。

**实际落地：只有 ① 完成；② 的前提在上游不成立，因此 ③ 一并搁置。** 因果见下。

---

## 1. 自造 timer 的**两层**，只有一层该换

`runtime-adoption-20261006-1352.md` §4 已经把这两层分开，本轮确认结论不变：

| 层 | 处置 | 理由 |
| --- | --- | --- |
| `connection/timer_.rs` 的**计时循环** | **保留** | 它是协议逻辑（`PULSE` 判定、空闲拆流、两条时钟的记账），与时钟来源无关；它已经只用 `R::delay`（`TrTime`） |
| `time/` 的**绝对期限算术 + 注入式时钟** | 已在上一轮删除 | 那是「`abs_art` 还没有计时能力」时的补救 |
| `time/tests_.rs` 的**自造假运行时值** | **本轮删除** | 这就是「自造 timer」的最后一处 |

### 1.1 删掉的是什么

`src/time/tests_.rs` 原先自己实现了一套虚拟时间：

```text
thread_local! { static VIRTUAL_MILLIS: Cell<u64> }   // 全局虚拟毫秒
struct FakeRt_;        impl TrClock for FakeRt_       // 读它
struct FakeInstant_;  impl Add/Sub                    // 自造时刻类型
struct FakeDelay_;    impl Future                     // poll 时把虚拟毫秒推进 duration
fn block_on_()                                        // 手动轮询抽干
```

这三样东西 `abs_art-mock_clock` 都已经提供，而且提供得更正确：`ManualClock` 同时
实现 `TrClock` 与 `TrDelay`（`ManualTime` 把它装饰成一个完整运行时值），因此
「时刻与计时器**同源**」这条本模块要钉的性质由类型保证，不需要手写。

### 1.2 为什么必须换：手写那版**已经实测在失败**

换之前跑 `cargo test --lib`，198 个用例里**恒有 2 个失败**：

```text
time::tests_::conn_clock_reports_millis_since_its_epoch      left: 2499  right: 2500
time::tests_::an_already_past_deadline_collapses_to_a_zero_delay
                                                            left: 4     right: 0
```

根因是那个 thread-local：`cargo test` 默认**多线程并行**，用例与用例共享同一份
`VIRTUAL_MILLIS`（thread-local 只在「每个用例一条线程」时等价于隔离，而 lib 测试
并不是每用例一线程）。先跑的用例把虚拟毫秒推走，后跑用例的 `virtual_reset_()` 与
断言之间就落进别的用例的读数。

换成 `ManualClock` 后**每个用例各建一份独立时钟**，隔离由类型保证；这两个失败
随本轮改动一并消失（`cargo test --lib` → 200 passed / 0 failed）。

### 1.3 一处行为差异（必须记下来，别把它当成 bug）

`ManualClock` 的格点是**整数毫秒**：`advance_by` 小于 1 ms 的部分按它的文档被截断，
且**不跨调用累加**。旧的手写 thread-local 版把小数部分留在 `f64`/整数累加里，因此
`1.5 ms` 之后再推 `2498.5 ms` 会得到 2500 ms；改 `ManualClock` 后是 `1 + 2498`。
处置：**用例的期望值按「整数毫秒」重写**，并把「不足 1 ms 的推进不产生新毫秒读数」
单独写成一条断言（`conn_clock_reports_millis_since_its_epoch`）。被钉的性质不变
（epoch 语义 + 向下取整），变的只是「不把 mock 的截断粒度当成被测性质」。

### 1.4 新增的两格：装饰器本身也要钉

既然 `ManualTime` 进了单测，顺手补两条：

- `manual_time_delay_waits_for_the_driver`：`delay` **不会自己推进时钟**，必须由驱动
  推进才完成（首次 poll `Pending` 且时钟不动）——这是 `Supervisor`「空闲即推进」
  能成立的前提；
- `manual_time_reports_the_same_axis_as_its_delay`：装饰器报出的 `now` 与它的 `delay`
  在同一条时间轴上（不存在第二个时间源）。

### 1.5 驱动：`Supervisor` 的最小等价物

单测里不需要作用域与后端装配，因此没有直接用 `Supervisor`，而是写了一个
`BlockOnAdvancing_<C: ManualClockApi>`：每轮 poll 未就绪就 `try_advance_to_next()`，
无到期定时器仍挂起则 panic（「在等一个永远不会来的东西」是用例写错，不是产品缺陷）。
它与 `Supervisor` 对 tokio 的处理同形（tokio 没有可用 tick 钩子，靠 `poll` 循环驱动）。

集成测试侧（`tests/keepalive.rs` / `keepalive_compio.rs` + `keepalive_common.inc`）
已经在用 `ManualTime` + `LocalScope::block_on_advancing`，本轮不动。

---

## 2. 议题 ②「取消 `R` 泛型」：前提不成立（阻塞点）

人类给出的理由是「`TrLocalScope` 现在应该可以直接在构造时从 `abs_art` 取到」。
对当前上游（`abs_art` HEAD = `5fe89c6`，已 `cargo check` 过）逐条核对，**这条不成立**：

| 想要的能力 | 上游现状 | 结论 |
| --- | --- | --- |
| 从运行时值取**本地作用域**（泛型地） | `local_scope()` 是 `abs_art_tokio::Runtime` / `abs_art_compio::Runtime` / `abs_art_smol::Runtime` 上的**固有方法**；`TrAsyncRuntime` 只有 `about()` | 取不到 |
| 取「当前运行时值」本身 | `current()` 同样是各后端**固有**函数，`abs_art` 没有 trait 入口 | 取不到 |
| 从某个值取「现在几点」 | `TrClock` **只**实现在各后端 `Runtime<CAPS>` 上（`ManualClock` 是唯一例外） | 只能由 `R` 提供 |

而且这不是遗漏，是**上游刻意的裁决**：`local-scope-thread-local-20261006-1420.md`
的配套选择原文写着「`block_on_advancing` 保留为**各后端固有方法**并写明它不属于
`TrLocalScope`」，`runtime.rs` 模块文档也把「作用域怎么来」定成
「只能从运行时值取得：`Runtime<CAPS>::local_scope()`」。本条与
`runtime-adoption-20261006-1352.md` §6.2 是同一个遗留项，至今未变。

### 2.1 为什么卡住的两条具体后果

`MuxCore<C, R>` 现在持有 `ConnClock_<R>`，API 面（`reserve_channel_` /
`release_channel_` / `drain_session_events_`）与计时循环都要「现在几点」。若把 `R`
从类型参数里拿掉：

1. **`MuxCore` 失去「现在几点」**：`now_millis_()` 没有来源，而它被 9 个 API 路径
   使用；`TrClock` 没有「从别的值上取回来」的入口。
2. **`MuxConnection::new` 失去作用域**：五个循环靠 `scope.spawn_local(..)` 投递，
   而没有 trait 入口能从一个运行时值换出 `TrLocalScope`。

### 2.2 可行的两条路（都需要人类裁决）

| 路线 | 形状 | 代价 |
| --- | --- | --- |
| **A：改上游，补 trait 入口** | 给 `TrAsyncRuntime` 补 `type Scope: TrLocalScope` + `fn local_scope(&self) -> Self::Scope`（以及可选的「运行时值」入口） | 跨仓公开 API 变更；上游刚以「不属 `TrLocalScope`」为由把这类入口挡在固有方法上，需先说服并改上游文档 |
| **B：把运行时值收进配置** | 给 `TrConnCfg` 加 `type Rt: TrTime + Clone + Send + Sync` + `fn runtime(&self) -> Self::Rt` | `runtime-adoption-20261006-1352.md` §2.2 **已否决**过（配置被迫承担运行时值语义、虚拟时间要同时换配置与时钟）；且作用域**仍**必须由调用方递进来，只减掉半个参数 |

两条路都只能减掉 `R` 或半个入参，**没有一条能只靠 smux 侧改动达成「只有 `C`」**。

---

## 3. 议题 ③「`MuxCore` 无条件 `Send + Sync`」依赖议题 ②

现在 `MuxCore<C, R>` 的 `Send` / `Sync` 完全由 `R` 决定：tokio 的 `Runtime` 是
`Send + Sync` 的 `Handle` 把手，compio 的 `Runtime` 持有线程本地的运行时实例、是
`!Send`（`tests/thread_safety.rs` 的表就是按这个事实写的）。

要让 `MuxCore` **无条件** `Send + Sync`，必须先把它与 `R` 解耦（即议题 ②），
再把「现在几点」改成按需传入的 `&R`。在 ② 未决之前，③ 只能记为**依赖阻塞**；
`tests/thread_safety.rs` 按现状保留（它当前钉的是「tokio 装配下句柄 `Send + Sync`」，
这个事实本身没变，不是「被误改」）。

---

## 4. 实测结论

- `cargo check --all-targets`：通过。
- `cargo test --lib`：**200 passed / 0 failed**（本轮之前是 196 passed / 2 failed，
  两个失败正是 §1.2 那两格）。
- `cargo test --no-default-features --features test-compio-runtime --lib --test inmem_mux
  --test smoke_compio`：199 passed / 0 failed（compio 装配单独跑一遍，两个后端都过）。
- 除 `keepalive` 外的全部 target（`--lib` / `alloc_count` / `inmem_mux` /
  `layered_rpc` / `smoke_tokio`）：全绿。
- `cargo test --test keepalive`：**挂起**（`idle_channel_times_out_tokio_` 打完
  `[probe] idle: slept; asserting` 之后不再推进），**与本次改动无关**——把
  `src/time/tests_.rs` 还原成 HEAD 版（即 §1 改动全部撤销）后同样挂起。
  见 §5 遗留。
- `python3 ../format_use_imports.py --check src/time/tests_.rs`：合规。

---

## 5. 遗留

1. **`keepalive` 集成测试挂起（既有问题，需要单独立项）**。现象：虚拟时间跑到
   2000 ms 并打印 `idle: slept; asserting` 之后不再有任何输出，直到外部 kill；
   tokio 与 compio 两个 target 都有这条用例。它出现在本轮改动之前，因此不计入本轮
   回归，但它挡住「全量测试」这条验收路径，应当优先于议题 ②/③ 处理。
2. **议题 ② 的路线选择**（A 改上游 / B 进配置 / 维持现状）待人类裁决；见 §2.2。
   在裁决之前，`MuxConnection<C, R>` 与 `MuxCore<C, R>` 保持现状。
3. **`tests/thread_safety.rs` 的说明文字**目前按「`R` 进类型参数」的现实书写。若
   议题 ② 走成，这份文档与 `src/connection/mod.rs` §6 需要同轮改写。


---

## 6. 追加（同日）：`abs_art-bridge` 集成，`MuxConnection` 只剩 `C`

人类随后下达：**依赖 `abs_art-bridge`，默认运行时 compio，测试下开其他后端 feature；
`Runtime` 与 `LocalScope` 由库自己取，不再要调用者指定；但保留一个调用者传入
`Runtime` 的接口。** 据此本轮把 §2 的两条候选路线定成了「运行时值进 `TrConnCfg`」。

### 6.1 形状（已落地）

```rust
pub trait TrConnCfg {                     // 新增
    type Rt: TrTime + Clone + 'static;    // 运行时值：时刻 + 计时
    fn runtime(&self) -> Self::Rt;        // 交出建连时抓住的那一个
    ...
}

pub struct MuxConnection<C> { ... }        // 只剩一个类型参数

impl<C> MuxConnection<C> where C: TrConnCfg {
    pub fn new(delivery, config, read_stage, write_stage) -> Self
    where C::Rt: ScopeHost;                // 自动取运行时值与作用域
    pub fn new_with_rt<S: TrLocalScope + Clone + 'static>(
        rt: &C::Rt, scope: &S, delivery, config, read_stage, write_stage,
    ) -> Self;                             // 「特别的需要」那条接口
}
```

于是公开类型族（`MuxConnection` / `DockBinding` / `ChannelListener` / `ChannelHandle` /
`Telegraph` / `ChannelTx` / `ChannelRx`）**全线只剩 `C` 一个参数**；`R` 从类型层面消失，
运行时值的类型由 `C::Rt` 唯一决定。

### 6.2 议题 ③ 同时达成：`MuxCore` 无条件 `Send + Sync`

之前卡在「`MuxCore` 持有运行时值 ⇒ compio 的 `!Send + !Sync` 传染上来」。
`Shared<T, A>: Send + Sync` **要求 `T: Send + Sync`**（`mm_ptr` 的 `unsafe impl` 不是
绕过 `!Send` 的通道，`&'static T` 也只在 `T: Sync` 时才 `Send`——两条都已编译验证），
所以唯一出路是**核心不持有运行时值**：现在 `MuxCore<C>` 只留
`epoch_: <C::Rt as TrClock>::Instant` 这个**纯数据**，「现在几点」由
`self.config_.runtime().now() - self.epoch_` 现算。运行时值只活在**本地循环**里
（`MuxLoopShared_<A, R>`，不跨线程）。

### 6.3 那条缺口：`local_scope()` 不在任何 trait 上

`abs_art` 的「取本地作用域」是各后端 `Runtime` 的**固有方法**（见
`abs_art/dev-notes/local-scope-thread-local-20261006-1420.md` 的刻意裁决），泛型代码里
写不出 `rt.local_scope()`。因此新增 `connection/scope_host_.rs` 的 [`ScopeHost`]：
一条**本地约束**，只为 **bridge 的具名别名**（`CompioRuntime` / `TokioRuntime`）实现，
由 smux 自己的测试 feature 二选一（`DefaultRt_` / `default_rt_()`）。

刻意**不**用 bridge 的裸名 `Runtime`：bridge 的守卫要求「同时启用多个 backend 时必须
显式声明 `default-backend-*`」，而 `cargo test` 的 feature 并集里两个后端会同时出现，
那时候裸名根本不存在。

`ManualTime` 的 `ScopeHost` 写在**本 crate**（孤儿规则：trait 与类型必须有一个是本地
的），并由 `test-mock-clock` feature 拉进 `abs_art-mock_clock`——生产依赖图里不出现它。

### 6.4 实测

- `cargo check --all-targets`：**零 error、零 warning**，两种 feature 组合都过。
- `cargo test --lib`：**199 passed / 0 failed**。
- `inmem_mux`（11）、`smoke_compio`（4）、`layered_rpc`（1）、doc-tests（4）：全绿。
- **两条未决**：
  1. ~~`alloc_count_baseline_tokio_`~~：**已处置**。它跑在 `#[tokio::test]` 下却要用
     **默认**运行时值，而缺省 feature 集里默认是 compio，于是在 tokio 测试里调
     `abs_art_compio::current()` → 「not in a compio runtime」。该目标本来就是
     「一个进程、一条用例、一个（tokio）运行时」，因此整文件门控到
     `test-tokio-runtime`（该 feature 下默认后端恰好就是 tokio，三者一致）。
  2. `keepalive_pulses_compio_`：**疑为既有问题**，本轮无法独立证实——§5 遗留 1 那条
     `idle_channel_times_out_tokio_` 的挂起在改动前就存在（基线复现过），而 `cargo test`
     在它处停住，因此基线里 `keepalive_compio` 从未跑到 `keepalive_pulses_`。现象是
     `left: Some(IdleTimeout) / right: None`（B 侧不该被判空闲超时），确定性复现。

### 6.5 议题 ② 的遗留：`thread_safety.rs`

核心无条件 `Send + Sync` 之后，连接的 `Send + Sync` 由 **`C` 是否 `Send + Sync`**
决定：`DefaultConnCfg` 里装着运行时值，而缺省后端 compio 的 `Runtime` 是 `!Send`。
因此本文件加 `#![cfg(feature = "test-tokio-runtime")]`——它验的是「**tokio 装配**下
句柄可跨线程」，这条性质在 tokio 后端仍然成立，只是不再覆盖默认（compio）装配。


---

## 7. 追加：文档清理、demo 修复，与「全量测试跑不完」的病因

### 7.1 demo 曾真的坏了（已修）

`examples/active_passive.rs` 跑在 **tokio** 上，而 `MuxConnection::from_delivery`
在建连时取的是**默认后端**（缺省 compio）的运行时值 → 在 tokio 上下文里调
`abs_art_compio::current()` → panic「not in a compio runtime」。

处置：示例改走「显式挑后端」那条路（`DefaultConnCfg::new_with_rt` 把 tokio 运行时值
传进配置），并把 `ScopeHost` 的实现从「bridge 具名别名」改挂到**后端 crate 自己的
类型**上（`abs_art_tokio::Runtime` / `abs_art_compio::Runtime`）——两者本是同一类型，
但后者的存在不依赖 bridge 的 feature，示例与下游因此都能直接用后端路径。

顺带两处 API 打磨：

- `DefaultConnCfg::new_with_rt` 泛型化到任意 `Rt`（原先写死 `DefaultRt_`）；
  `new()` 收回到 `DefaultConnCfg<W, R, P, DefaultRt_>` 这个具体形态上，因此
  `DefaultConnCfg::new(..)` 仍然不需要任何类型标注。
- `abs_art_bridge` 从 `crate::x_deps` 转出口，下游只依赖 smux 也能取到运行时值。

`cargo run --example active_passive` 已恢复输出两行预期文案。

### 7.2 全量测试跑不完的两个病因（都修了）

**病因 A：feature 缺省集与「默认后端」自相矛盾。**

`test-tokio-runtime` 与 `test-compio-runtime` 都在 `default` 里，而「谁是默认后端」由
feature 唯一决定 —— 缺省解是 compio。于是 `tests/keepalive.rs`（tokio 装配）在自己的
tokio 上下文里调 compio 的 `current()` 而 panic；`examples/` 更麻烦：示例**无法**开启
测试 feature，任何依赖默认后端的示例都会跟着坏。

处置：`default` 收敛为 `["test-compio-runtime"]`（与「默认后端 = compio」一致），
tokio 侧改为**显式 opt-in**；`tests/keepalive.rs` 加 `#![cfg(feature = "test-tokio-runtime")]`。
`justfile` 的配方与注释同步（`test-compio` = 缺省全量，`test-tokio` = 显式 opt-in）。

**效果**：`cargo test --all-targets`（缺省）从「**永远挂住**」变成 **约 20 秒跑完**，
唯一失败是下面 7.3 那条 `keepalive_pulses_compio_`。

**病因 B（真 bug）：空闲超时拆流之后，同一条连接上再建一条子流会挂住。**

`keepalive_pulses_compio_` 是它的一种表现（B 侧被判 `IdleTimeout`）；tokio 侧的
`idle_channel_times_out_tokio_` 则是**挂起**。带探针定位到的确切位置：

```text
[probe] idle: slept; asserting
[probe] idle: assert-1 (A 侧 abort_reason) 通过     ← 前两条断言都过了
[probe] idle: begin establish-2                     ← 卡在这里
...（此后无输出，虚拟时钟停在 2500ms）
```

即 `establish_one_channel_(&conn_a, &conn_b, 0x1001, 1)` **不返回**；`virtual_sleep`
在那之后又推进了 5 轮（2000→2500 ms）就再无动静，说明**已经没有待唤醒的本地任务在
跑了**——怀疑是拆流路径把连接的某个循环或某条发送路径停住了（而不是简单死锁）。
`keepalive_pulses_compio_` 的 `IdleTimeout` 很可能是同一条因果链的另一端。

**待查（下一步）**：`timer_::deliver_action_` 的 `Abort` 分支（`release_channel_` +
`LocalAbort` + `Release` 三连）之后，复用 / 解复用两条循环的本地表与注册表是否都回到
可继续建流的状态；以及 `binding_a` 在空闲拆流后是否仍能发起新的 `open`。

### 7.3 文档清理清单

- `README.md`：§2 的建连路径改成 `from_delivery(delivery)`（不再有 `scope` 入参）、
  新增 §2.1「想自己挑后端：显式传入运行时值」、§5 补第 5 条（后端在 `Cargo.toml` 选、
  连接 `Send` 与否看配置里的运行时值）、§4 的冒烟命令改用 `smoke_compio`（缺省装配）。
- `runtime-adoption-20261006-1352.md`：文首加**取代说明表**（`<C, R>` → `<C>`、
  `new` 入参、§2.2 的否决被推翻、§6 遗留 1/2/4 的现状）。
- `keepalive-timer-loop-20261005-1420.md`：加**历史记录**标注（作用域携带计时能力
  那一版是中间形态，现已回到运行时值上）。
- `src/connection/mux_connection/core_.rs`、`config_.rs`、`connection/mod.rs`、
  `tests/layered_rpc.rs`、`tests/common/scenarios_/{small_,kit_}.rs`：把
  「运行时值进类型参数」一类过时描述改为现状。
- `tests/keepalive_common.inc`：删掉「保留 `scope` 参数是为了不改动调用点」的过渡
  注释与那个参数本身（建连已经不需要它）。


---

## 8. 追加：只依赖 bridge（去掉两个后端 crate），以及「测试自报运行时」

### 8.1 依赖裁剪：库不再直接依赖任何后端 crate

`abs_art-bridge` 本来就是「按 feature 选后端」的那个 crate，此前 smux 却又直接依赖了
`abs_art-tokio` / `abs_art-compio`（理由是「示例与下游要写后端类型路径」）。本轮把它
去掉，库只留 `abs_art` + `abs_art-bridge`：

| 位置 | 现在怎么取后端 |
| --- | --- |
| `ScopeHost` 的实现 | `impl ScopeHost for abs_art_bridge::Runtime`（**唯一一条**） |
| 默认运行时类型 / 构造 | `abs_art_bridge::Runtime` / `abs_art_bridge::current()` |
| 示例、全部集成测试 | `abs_art_bridge::current()` + bridge 裸名类型 |

两处必须写下来的坑：

1. **bridge 的 `default` 要关掉**（`default-features = false`）。它自带
   `default = ["default-backend-compio"]`，会把 `backend-compio` 一起打开；于是
   `--features test-tokio-runtime` 下出现「两个 backend + 两个默认后端」，直接撞上
   bridge 的守护。关掉之后，后端选择**完全**由 smux 的 feature 决定。
2. **smux 的每个 feature 只开 bridge 的**一个** `backend-*`，不要再写
   `default-backend-*`。照 bridge 的规则「只启用一个 backend 时它就是默认」，裸名
   `Runtime` / `current` 因此有唯一解；写两位反而在 `--all-features`（或 dev-deps
   把第二个后端拉进并集）时冲突。
   于是 `default = ["test-compio-runtime"]`（缺省 compio），
   `test-tokio-runtime` 显式 opt-in。

示例也跟着回到 bridge 裸名：`examples/active_passive.rs` 的传输层从 tokio 改成
**compio**（缺省后端），因此 `cargo run --example active_passive` 在无任何 feature 的
情况下就能跑——`#[compio::main]` + `compio::net::UnixStream`（0.19 没有 `pair()`，走
`std` socket 对 + `from_std`，与 `tests/common/socket_.rs` 的 compio 版同款）。

`justfile` 相应收敛：`test-compio` = 缺省装配全量（含示例）、
`test-tokio` = `--no-default-features --features test-tokio-runtime` 的全量（**不带**
`--all-targets`：示例是 compio 传输，归 compio 那一趟）、`clippy` 改为**两个装配都查**
（此前只查缺省，tokio 专属文件的 lint 因此长期没被看见——本轮就抓到两处）。

### 8.2 让测试自报运行时（本节由人类提问促成）

**问题**：同一个 `smux_v1` 能在多个装配下编译，而**装配错了的失败模式很差**：

- 轻则在别的上下文里 panic 出一句与病因无关的文案（「not in a compio runtime」、
  「no reactor running」）；
- 重则**静默挂住**——本地队列没人驱动，或「驱动用的作用域」与「连接用的计时器」不是
  同一条时间轴（本仓的 `ManualTime` 装饰正是这种两源结构，见 §6 与
  `runtime-adoption-20261006-1352.md`）。

两种情况都要从现象反推配置，而配置本来可以写在测试第一句里。

**处置**：新增共享断言 `tests/common/config_.rs::assert_runtime_is_(&rt, RuntimeTag::X)`，
并在每个「依赖某个运行时」的测试**取得运行时值之后立刻**调用：

```rust
let value: abs_art_bridge::TokioRuntime =
    abs_art_bridge::TokioRuntime::with_handle(rt.handle().clone());
common::assert_runtime_is_(&value, abs_art_bridge::RuntimeTag::Tokio);   // ← 第一句
```

接入点：`keepalive` / `keepalive_compio` 两个壳、`smoke_tokio` / `smoke_compio`、
`inmem_mux`、`layered_rpc`、`alloc_count`。`RuntimeTag` 经 `smux_v1::x_deps` 转出，
测试不需要额外依赖。

**效果**：装配错了就是一句「本用例要求跑在 Tokio 后端上，但当前运行时值报告的身份是
Compio」，而不是若干秒后的怪 panic 或无限挂起。这也让「哪些用例本来就要求某个后端」
变成**可以 grep 的显式事实**。

### 8.3 `keepalive` 四格的现状（供下一步定位）

| 用例 | 装配 | 结果 |
| --- | --- | --- |
| `keepalive_pulses_tokio_` | tokio 壳 / 连接 Rt = `ManualTime<TokioRuntime>` | **pass** |
| `idle_channel_times_out_tokio_` | 同上 | **挂起**（§7.2 病因 B，已定位到 `establish_one_channel_`） |
| `idle_channel_times_out_compio_` | compio 壳 / 连接 Rt = `ManualTime<CompioRuntime>` | **pass** |
| `keepalive_pulses_compio_` | 同上 | **fail**（B 侧被判 `IdleTimeout`） |

这个「谁过谁不过」的组合本身就是线索：**两个壳的驱动各自是自己的后端**，而连接里那个
运行时值是 `ManualTime` 装饰过的**同一个后端**——差别只在「空闲拆流是否发生」。
`idle`（会走到拆流）在 **tokio** 上挂起、在 compio 上通过；`pulses`（不拆流）反过来。
因此下一步应当查：**拆流（`timer_::deliver_action_` 的 `Abort` 分支）之后，
复用 / 解复用两条循环在 tokio 与 compio 两种 `delay` 实现下的 park/wake 差异**，
而不是继续在「空闲超时判定」那一侧找。


---

## 9. `idle_channel_times_out_tokio_` 的病因：解复用循环**空转**（已定位到分支）

### 9.1 先排除掉的两个假设

用「同样的形状，只改一个变量」做了两组对照（诊断场景临时加在
`keepalive_common.inc`，结论记在这里、脚手架已撤除）：

| 对照 | 结果 | 排除了什么 |
| --- | --- | --- |
| 睡眠从 1600 ms 缩到 **0 ms**（建完第一条立刻建第二条） | **仍挂** | 「时间 / 计时器 / 虚拟时钟」不是必要条件 |
| 两端超时改成**对称**（1 s / 1 s ⇒ 不触发空闲拆流） | **仍挂** | **拆流路径无关**（`timer_::deliver_action_` 的 `Abort` 分支被洗清） |
| 第二次建流换**不同 dock 对** | **仍挂** | 与 dock 复用无关 |

因此真正的最小现象是：**在一条已经成功建过一条子流的连接上，再建第二条子流，
两端的帧交换做不完**。

### 9.2 卡在哪一步（探针）

```
[est] bind_b ok; bind_a ok; open_channel ok; join accept…
[est]   A accept_async 开始
[est]   B income_async 开始
[est]   B income_async 完成      ← B 收到了 A 的 OPEN
[est]   B accept_async 完成      ← B 也发出了自己的 OPEN + ACCEPT
（A accept_async 永不完成）      ← 但 B 那两条帧没到 A
```

即：**A→B 方向通，B→A 方向在第一次建流之后断了**。B 侧也**没有**报连接级失败
（否则 B 的句柄会以错误收尾，而它是正常完成的）。

### 9.3 决定性证据：循环在**空转**，不是停了

> **修订（见 §10）**：本节把 100% CPU 归因于「解复用循环空转」是**误读**。挂起时对进程
> 采线程态可见：跑测试的线程 `state=R / syscall=running / utime` 每 4 秒 +400 ticks
> （= 单核 100%），而 tokio worker 在 `do_epoll_wait`、主线程在 `futex_wait_queue`。
> 那 100% 来自 `Supervisor::poll` 结尾的**无条件自唤醒**（`abs_art-mock_clock/src/driver.rs`）
> 叠加「时钟永远有下一个到期时刻」；`demux_ticks_` 的采样本身也说明不了「谁在忙」。
> 真实病因与完整证据链见 §10。

给解复用循环加了一个只增的活动计数（现已是 `test-loop-probe` feature，见 §9.5），
在等待第二条子流建立的同时采样：

```
[probe2] demux_ticks 增量 = 943233
[probe2] demux_ticks 增量 = 943237     ← 每 300 ms 虚拟时间 +4
[probe2] demux_ticks 增量 = 943241
...
```

**每 300 ms 虚拟时间跑掉约 94 万轮**——这是**忙等**（`readable > 0` 却每轮都不推进
消费），不是死锁、也不是循环退出。

### 9.4 下一步该看哪里（分支已收窄）

循环体（`session_::demux_loop_async_`）的结构是：

```text
loop {
    0. drain_session_events_()
    1. drain_read_events_()
    2. readable = ring_readable_(&rx_stage)   ← 空转时这里恒 > 0
       if readable == 0 { park } else { 继续 }
    3. read_header_async_()                   ← 逐字节状态机
    4. 载荷长度校验 + 读载荷
    5. 派发
}
```

`readable > 0` 却无限空转，只可能是**第 3 步每轮都从头开始、且没有把字节真正消费掉**
（`read_header_async_` 每轮 `FrameHeaderParser::new()`，状态是**每次调用局部**的）。
因此要查的具体问题是：

- `ReadCursor::read_byte_async_` → `read_async_(&mut one, ..)` 在**环里只剩 1 字节**
  时是否真的提交消费（`buffex` 的段借出与 drop 提交语义）；
- 或者第 5 步派发某类帧之后**没有推进环读指针**（例如把段借出后持有不放）；
- 以及为什么唤醒方向只在「第二次建流」时才走到这条路径（第一次建流不出现）。

### 9.5 留下的诊断设施（`test-loop-probe`）

- `src/connection/loop_probe_.rs`：解复用循环的轮次计数（thread-local，零开销，
  **不在生产 feature 集里**）；
- `MuxConnection::demux_ticks_()`：`#[cfg(feature = "test-loop-probe")]` 的采样入口；
- 两个测试 feature 都转发开启它。

它把「循环停了」与「循环在跑但不推进」这一对**现象相同、修法完全不同**的可能一次分开，
建议保留。`assert_runtime_is_`（§8.2）同样保留。


---

## 10. 定论：`idle_channel_times_out_tokio_` 的病因是**虚拟时钟在 reserve 与 attach 之间推进了整整一个 `timeout`**

本轮用帧 / 事件级临时探针（跑完即撤，工作区已还原）把「第二条子流建不起来」拆到了
**单条事件的时间戳**上。§9.4 列出的三个待查点全部被排除：帧没有丢、
`read_header_async_` 也没有「每轮从头开始」，问题出在**计时循环把一条尚未建成的子流判成了空闲超时**。

### 10.1 决定性日志（逐条为真实探针输出）

```text
[idle: slept; asserting]
T69  REG:reserve local=Dock(4097) remote=Dock(1) now=2500      ← 第二条子流登记身份，活跃时钟打点 = 2500
T70  TMR:scan now=3500 timeout=1000 pulse=500
T71  TMR:abort local=Dock(4097) remote=Dock(1)                 ← 3500 − 2500 = 1000 ≥ timeout ⇒ 认领空闲超时
T72  TMR:sleep wait=1000 next=4500 now=3500
T73  W:attach local=Dock(4097) remote=Dock(1)                  ← 会话侧半部上线（Attach）此时才发生
T74  W:control kind=Open flags=0 local=Dock(4097) remote=Dock(1)
T76  W:control kind=Close flags=1 local=Dock(4097) remote=Dock(1)   ← 计时循环的 FIN
T78  W:control kind=Close flags=2 local=Dock(4097) remote=Dock(1)   ← 计时循环的 RESET
T80  W:local-abort local=Dock(4097) remote=Dock(1)             ← 复用循环放弃该子流
T81  D:ev-attach  local=Dock(4097) remote=Dock(1)              ← 解复用循环收到 Attach（太晚了）
T82  D:ev-release local=Dock(4097) remote=Dock(1)              ← 紧接着收到 Release，表项被摘掉
T85  D:frame kind=Open flags=0 local=Dock(1) remote=Dock(4097) ← B 侧正常收到 A 的 OPEN、登记入向
T99  D:frame kind=Open flags=0 local=Dock(4097) remote=Dock(1) ← A 侧收到 B 回的 OPEN
T100 D:open-miss-inbound local=Dock(4097) remote=Dock(1)       ← 但本地表里已没有它 ⇒ 当成「入向请求」
T102 W:control kind=Reject flags=0 local=Dock(4097) remote=Dock(1)
```

即：**A 的 `accept_async` 并不是等不到帧，而是帧到得太晚——子流在 `reserve` 之后、
`attach` 之前就被自己的计时循环拆掉了**。B 回的 `OPEN` 因此落入「入向请求」分支，
`reserve_inbound_` 返回 `WaitClose`（身份刚进宽限态），于是回 `REJECT`；
`wait_establish_` 永远等不到结论 ⇒ 测试挂起。

### 10.2 为什么推进量恰好等于 `timeout`

`timer_loop_async_` 每轮睡到「下一次期限」，而这个期限是：

- 有在册子流时：`next = active + timeout`（`registry_::timer_scan_` 的第 3 步）；
- **没有任何在册子流时**：兜底为 `next = now + timeout`（`timer_loop_async_` 第 3 步的
  `.min(now.saturating_add(timeout_millis))`）。

`Supervisor` 在 `body` 每次都 `Pending` 时调用 `try_advance_to_next()`，它会**一步跳到**
那个期限。于是「一次调度 ⇒ 时钟前进一个 `timeout`」。而
`timer_scan_` 的判据是 `now − active >= timeout`，**恰好命中**。

建流要跨过若干次 `await`（`open_channel_async` 取锁 / `join!` 的两支 / `accept_async_managed`），
每一次 `await` 都给 `Supervisor` 一次推进机会。于是：
**在虚拟时间下，只要「登记身份」与「会话侧半部上线 / 对端回帧」之间跨过一次调度，
这条子流就必然在建成前被判空闲超时。**

这同时解释了 §9.1 的两组对照为什么都仍挂（睡眠缩到 0 / 两端对称超时），
以及 `keepalive_pulses_compio_` 的 `B 侧被判 IdleTimeout`（同一条因果链的另一端）。

### 10.3 为什么第一条子流没事

第一条的 `reserve → attach → 对端 OPEN/ACCEPT` 全部落在虚拟时钟 0→1000 ms 的**同一格**里
（那时兜底期限还远），握手一次调度就走完。第二条的 `reserve` 落在 `now = 2500`，
而下一格就是 `3500`——整整一个 `timeout`。

### 10.4 反向验证：把 `timeout` 放大十倍，**仍然挂**

把 `K_TIMEOUT` 从 1 s 临时改成 10 s（并临时块注释掉 1、2 组断言）后重跑：

```text
T51  REG:reserve local=Dock(4097) remote=Dock(1) now=10000
T56  W:attach local=Dock(4097) remote=Dock(1)            ← 这次 attach 抢在超时之前
T71  D:open-miss-inbound local=Dock(1) remote=Dock(4097) ← B 收到 OPEN 并登记入向
T88  TMR:abort local=Dock(4097) remote=Dock(1)           ← 20000 − 10000 = 10000 = 新的 timeout
T100 D:open-miss-inbound local=Dock(4097) remote=Dock(1) ← A 收到 B 回的 OPEN 时已在宽限态
T102 D:inbound-reserve-failed err=WaitClose
```

超时点只是**后移了一个 `timeout`**，挂起照旧。这排除了「阈值设得太小」这一解释。

### 10.5 两个层面的问题（供裁决）

| 层面 | 事实 | 影响 |
| --- | --- | --- |
| **驱动层（第一性原因，见 §11）** | `Supervisor` 的推进只受 `is_frozen` 控制：`tick()` 的返回值**不参与**是否推进的决策，只参与 `stalled_` 计数；三端实测（tokio `LocalSet` / compio `Runtime` / smol `LocalExecutor`）行为**完全一致** | 与后端无关的语义缺陷。tokio 的 `\|\|false` 只是让「停滞保护」失效（`ran` 恒 false 时只能靠 `advanced` 归零）；「compio 的 idle 通过」是推进时机与本地任务驱动顺序把同一缺陷放大到不同轮次，不是语义正确 |
| **产品语义** | 空闲超时的活跃时钟从 `reserve_channel_` 起算，覆盖「建流尚未完成」的窗口 | 真实时间下该窗口是微秒级、不显现；虚拟时间把它放大成「任何跨调度握手都必然超时」。是否把活跃时钟起点改到 `OPEN`/`ACCEPT` 完成，或给建流中的子流单独一个期限，需要人类裁决 |

修法候选（按代价从低到高）：

1. **测试驱动**：让 tokio 后端的「推进」发生在「本地队列确实没活可干」之后（需要给
   `Supervisor` 一个能同步跑一轮本地任务的钩子，或改由 `LocalSet` 侧驱动）；
2. **测试场景**：建流期间冻结虚拟时钟（`set_frozen(true)`，建完再解冻）——能让用例变绿，
   但没有回答「建流中的子流该不该被空闲超时拆」；
3. **产品**：把空闲超时的计时起点从 `reserve` 后移到建流完成（或在建流中挂一个单独的
   建流期限）。

### 10.6 复现手段（探针已撤除）

- 直接跑：`cargo test --no-default-features --features test-tokio-runtime --test keepalive
  idle_channel_times_out_tokio_ -- --nocapture --test-threads=1`（会挂，需外部超时）；
- 挂起性质：在同一命令里对测试进程采样 `/proc/<tid>/stat` 与 `/proc/<tid>/syscall`，
  可见测试线程 `R / running`、`utime` 稳定增长（100% 单核），tokio worker 在 epoll；
- 需要再次做帧级追踪时，按 §10.1 的落点插 `eprintln` 即可——五个落点分别是
  `reserve_channel_`、`timer_scan_` 与 `deliver_action_`、`handle_write_event_`、
  `demux_loop_async_` 的派发点、两条泵的 `moved > 0` 处。


---

## 11. 追认：病因在 `abs_art-mock_clock` 的**推进判据**，不是 tokio 后端

问题被提出为：「这是不是 `abs_art-mock_clock` 的 bug？需要确定性证据——比如在 **smol** 下
表现一样，就说明它对 `LocalSet` / `LocalExecutor` 的理解不正确。」

本轮用一个**与 smux 无关的最小场景**取到了这份证据。

### 11.1 取证方式

新增 `tests/supervisor_semantics.rs`（临时文件，未跟踪；同时临时加了 `abs_art-smol`
的 dev-dependency）：在一条 **500 ms 周期定时器**存活的线程上，主体什么都不做、
只反复「让出」（自唤醒后返回 `Pending`），报告让出前后的虚拟时刻。三端各跑一遍，
用各自 `LocalScope::block_on_advancing` 的原生驱动入口。

### 11.2 三端结果**完全一致**

```text
[tokio]  纯让出 6 轮的虚拟时钟： 500 → 3500 ms（推进 3000 ms）
[compio] 纯让出 6 轮的虚拟时钟：1000 → 4000 ms（推进 3000 ms）
[smol]   纯让出 6 轮的虚拟时钟：1000 → 4000 ms（推进 3000 ms）
```

推进量 = 6 轮 × 500 ms（正好是那个定时器的周期）。**`LocalSet`、compio `Runtime`、
`LocalExecutor` 三者行为相同**——所以这不是 tokio 适配的偶发问题。

（起始值 500 / 1000 的差异来自「tick 是否在推进之前驱动执行器」：compio/smol 的 tick
会先跑一轮执行器，于是周期任务登记定时器发生在第一次推进之前；tokio 的 tick 是空操作。
这只影响**从哪个时刻起算**，不影响「每轮必推进」这一结论。）

### 11.3 判据本身：tick 返回什么都照样推

再把 `Supervisor` 单独拉出来，主体自旋 4 轮、每轮登记一个 `now + 500 ms` 的定时器，
**只改 tick 钩子的返回值**：

```text
[tick 取值对照] 同为「自旋 4 轮 × 每轮登记 500ms 定时器」：
                tick=true → 2000 ms；tick=false → 2000 ms
```

两种取值给出**同一个**虚拟时刻。即 `Supervisor::poll` 中 `tick()` 的返回值**不参与**
「是否推进」的决策，只参与 `stalled_` 计数。模块文档标题写的是「空闲即推进」，
实现却是**每次被 poll 就推进**（除非 `is_frozen`）；`tick` 唯一的作用是让停滞断言
不至于误报。

### 11.4 结论

1. **是 `abs_art-mock_clock` 的缺陷**，且与后端无关：`Supervisor` 把「虚拟时间推进」
   与「执行器是否还有就绪工作」解耦，只剩一个全局冻结开关；
2. tokio 的 `|| false` 的作用只是让停滞保护失效：`ran` 恒 false 时只能靠 `advanced`
   归零。这解释了两件事——smux 挂起时为什么**没有** panic（timer 每轮都算出兜底期限，
   `advanced` 恒 true），以及 §11.3 里 tick 恒 true 时**也没有** panic（`ran` 恒 true）；
3. 「compio 的 `idle` 通过、tokio 挂」**不是语义差异**：同一缺陷在不同后端被放大到
   不同轮次上（谁在推进之前驱动执行器），不能作为「tokio 适配错了、其余是对的」的证据；
4. smux 的 `idle_channel_times_out_tokio_` 是该缺陷的一个**具体后果**：建流跨了一次
   `await`，就被推进了整整一个 `max_channel_timeout`。

### 11.5 修法方向（供裁决）

| # | 方向 | 代价 |
| --- | --- | --- |
| 1 | 推进与「执行器是否空闲」绑定：`ran == true`（本轮确实跑到了任务）时**不推进**，只有 tick 报「什么都没跑」才推进 | 改 `Supervisor` 的一行判据 + 三端 tick 语义需重新对齐；要防止「执行器永远有活 ⇒ 时钟永不推进」的新死结 |
| 2 | 让抽象能表达 tokio 的**异步** tick：`LocalSet::tick()` 是 `async fn`，现有 `Fn() -> bool` 表达不了；可改成 `FnMut() -> impl Future<Output = bool>`，或把「先跑一轮执行器」挪到推进之前由驱动方 await | 改 `Supervisor` 的泛型契约（跨仓公开 API 变更），但这是 tokio 与另两端语义对齐的唯一路径 |
| 3 | 顺带处理停滞保护：`tick=true` 时 `stalled_` 永远归零，时钟推不动也不会报错（静默自旋） | 属 #1 的配套 |

### 11.6 复现

三端最小复现脚本（临时文件）已撤除，数据存档在 §11.2 / §11.3。要重跑同样的判据，
用已经沉淀到 `abs_art` 里的**契约用例**：

```bash
# 判据本身（与后端无关）
cargo test -p abs_art-mock_clock --test supervisor_contract -- --ignored --nocapture
# 三端各自的「纯调度不该消耗虚拟时间」
cargo test -p abs_art-tokio  --features mock-clock --lib -- --ignored --nocapture
cargo test -p abs_art-compio --features mock-clock --lib -- --ignored --nocapture
cargo test -p abs_art-smol   --features mock-clock --lib -- --ignored --nocapture
```

这些用例在 §13 的彻底修法落地后**已经转绿**（转成了常规用例，不再需要 `--ignored`）。
注意它们钉住的语义是「**执行器有活**时不得推进」；「纯让出 + 周期定时器」那类场景在
「空闲即推进」下本就该推进，那不是契约——见 §13.1 的纠正。


---

## 12. 修法验证：让「推进」排在「执行器跑一轮」之后，四格立刻全绿

§11 把病因定到 `Supervisor` 的推进判据上。本节按「先验证因果、再谈最终形态」做了一次
**最小修法实验**（改动在 `abs_art-mock_clock/src/driver.rs`，属临时实验，尚未定稿）。

### 12.1 改了什么

只加一件事：**主体 `Pending` 时，先「让出」一轮再推进**——即每两轮 poll 才推进一次，
而那一轮 `Pending` 正好让外层驱动（`LocalSet::run_until` / compio `block_on` /
`smol::block_on`）去 tick 执行器。于是「执行器跑一轮」被排到了「时钟推进」**之前**，
与 compio / smol 的 tick 钩子本来就保证的顺序一致。

```rust
if let Poll::Ready(value) = this.body_.as_mut().poll(cx) { return Poll::Ready(value); }
if !this.yielded_ {            // 【实验】推进前先让出一轮
    this.yielded_ = true;
    cx.waker().wake_by_ref();
    return Poll::Pending;
}
this.yielded_ = false;
// …… 原来的 tick / advance / stalled 逻辑不变
```

### 12.2 效果一：最小场景的推进量减半，且三端**起点也一致了**

```text
改前：[tokio]  500 → 3500（推进 3000）；[compio]/[smol] 1000 → 4000（推进 3000）
改后：[tokio]/[compio]/[smol] 一律 500 → 2000（推进 1500）
```

`tick=true / false` 仍是同一个结果（1000 ms）——推进判据本身还没动，本实验只改顺序。

### 12.3 效果二：smux 的 keepalive **四格全部变绿**

```text
== tokio 装配 ==
test idle_channel_times_out_tokio_ ... ok      （改前：挂起）
test keepalive_pulses_tokio_ ... ok
== compio 装配 ==
test idle_channel_times_out_compio_ ... ok
test keepalive_pulses_compio_ ... ok           （改前：B 侧被判 IdleTimeout）
```

这完成了「病因 → 后果」的闭环：**挂起与 compio 的 `pulses` 失败，确实是同一个
`Supervisor` 推进时机问题**，产品侧的计时逻辑（`reserve` 起算的空闲时钟）并没有算错。

### 12.4 无回归

`abs_art` 侧：

| 目标 | 结果 |
| --- | --- |
| `cargo test -p abs_art-mock_clock` | 26 passed（+ 3 doctest） |
| `cargo test -p abs_art-tokio --features mock-clock` | 25 + 9 + 2 passed |
| `cargo test -p abs_art-compio --features mock-clock` | 31 + 16 + 10 passed |
| `cargo test -p abs_art-smol --features mock-clock` | 36 + 9 + 2 passed |

（含各自的 `Supervisor` 停滞断言、`virtual_hour_passes_instantly` 等虚拟时间用例。）

`smux_v1` 侧：

| 装配 | 结果 |
| --- | --- |
| tokio：`--lib --test smoke_tokio --test keepalive --test inmem_mux --test alloc_count --test thread_safety` | 199 + 4 + 2 + 11 + 1 + 2 passed，0 failed |
| compio：`cargo test --all-targets`（缺省装配全量，含 example） | 全绿（含 `layered_rpc` 1 passed） |

**顺带发现的既有问题（与本轮改动无关）**：`layered_rpc` 的 **tokio** 用例会挂起——
`user` 时间 0.25 s、无输出、5 分钟不返回。把 `abs_art-mock_clock` 还原成基线后
**同样挂起**（2 分钟不返回），因此不是本轮回归；缺省（compio）装配下它正常通过。
记为遗留，需要另立一项排查。

### 12.5 这不是最终形态

本实验只把推进**推迟一轮**，推进判据仍是「每两轮一次」：§12.2 里「纯让出 6 轮 ⇒
推进 1500 ms」说明它依旧会在纯调度等待中消耗虚拟时间，只是粒度减半、顺序正确。
彻底的修法仍是 §11.5：

1. `ran == true` 时不推进（需要各后端 tick 的语义对齐，且要防「永远有活 ⇒ 永不推进」）；
2. 让抽象能表达 tokio 的异步 tick（`LocalSet::tick()` 是 `pub(crate)` 私有方法，
   公开 API 里取不到；`Fn() -> bool` 也表达不了 async）；
3. 配套处理 `tick=true` 时停滞保护失效。

也就是说：**顺序修法解决了 smux 当前的挂起，但没有解决「虚拟时间被调度消耗」这一
根本语义**；§11.5 的 1+2 仍应做，只是可以与本实验分开落地。

### 12.6 契约用例已沉淀进 `abs_art`

`smux_v1` 里的临时诊断文件已撤除，判据改以四处 `#[ignore]` 用例存档：

| 位置 | 钉住什么 |
| --- | --- |
| `abs_art-mock_clock/tests/supervisor_contract.rs` | ①「执行器有活 ⇒ 不得推进」（期望语义，红灯）；②「tick 取值不改变推进量」（修法前行为存档） |
| `abs_art-tokio/src/local_scope.rs`（`mock_clock_tests_`） | 纯调度让出不该消耗虚拟时间 |
| `abs_art-compio/src/local_scope.rs`（同） | 同上 |
| `abs_art-smol/src/local_scope.rs`（同） | 同上 |

它们当时以 `#[ignore]` 落在常规 CI 之外，把「期望语义」立成了可执行的契约；
§13 的彻底修法落地后已全部去掉 `#[ignore]`，成为常规用例。


---

## 13. 彻底修法落地：判据 + 三端「有活」信号；以及它逼出来的产品缺陷

§12 的顺序修法只是权宜。本节按 §11.5 的 1+2 做了彻底修法，并把上一轮误设的契约
场景一并纠正。

### 13.1 先行纠正：契约场景必须是「执行器有活」

§11 的三端最小复现（纯让出 + 一条周期定时器）在「空闲即推进」的语义下**本来就该
推进**——那时执行器确实没活。它不是契约。真正的契约是：

> **执行器还有就绪工作（有任务被唤醒 / 还有任务要跑）时，虚拟时间不得前进。**

因此三端契约用例的场景已改为「spawn 一个每轮自唤醒的任务」+ 断言 6 轮让出推进 `0 ms`。

### 13.2 改了什么

| 位置 | 改动 |
| --- | --- |
| `abs_art-mock_clock/src/driver.rs` | 推进判据由「只看 `is_frozen`」改为 **连续两轮 `ran == false` 且未冻结时才推进**；`tick` 的返回值从此真正参与决策（此前只喂停滞计数）。「连续两轮」是为了把「唤醒链真的走完了」与「刚跑完一环、下一环还没被驱动」分开——后端只需回答「有没有活」 |
| `abs_art-tokio/src/local_scope.rs` | `LocalSet::tick` 是 crate 私有的，拿不到「有没有活」。改为**唤醒登记**：`mock-clock` 下 `spawn_local` 的任务被 `Tracked_` 包一层，任务注册出去的 waker 被调用即置位 `WOKE_`；`block_on_advancing` 的 tick = 读取并清除它 |
| `abs_art-compio/src/local_scope.rs` | **不需要任何包装**：`Runtime::run()`（「队列里还有任务吗」）配合上一条的「连续两轮」判据就够了——单轮 `false` 只是「刚跑完一环」，下一轮 `run()` 会跑到它唤醒的下一环并返回 `true`。实测：去包装后契约与 keepalive 四格全绿 |
| `abs_art-smol/src/local_scope.rs` | `LocalExecutor::try_tick()` 返回「是否跑到了任务」，语义本就正确，未改 |

### 13.3 契约转绿

四个原先 `#[ignore]` 的用例现在都是常规用例：

- `abs_art-mock_clock/tests/supervisor_contract.rs`：`advance_must_wait_until_executor_is_idle_`（有活不推，`0 ms`）与 `advance_happens_when_executor_is_idle_`（连续没活时照推：tick=false、4 轮让出 ⇒ 2 格 = `1000 ms`）成对；
- 三端各自的 `busy_executor_does_not_advance_virtual_time`：均 `0 ms` 通过。

### 13.4 逼出来的产品缺陷：建流窗口内收到的 `PULSE` 被丢弃

彻底修法后 `idle` 四格与 tokio 的 `pulses` 通过，但 `keepalive_pulses_compio_` **失败**。
帧级探针（临时插入、已撤除）给出的序列：

```text
T8  TMR:scan now=500            ← A 侧扫描（身份已在册，虽然子流还没完成 accept）
T9  TMR:pulse 8192 → 2          ← A 发出 PULSE
T11 TMR:pulse 2 → 8192          ← B 也发 PULSE
T18 D:frame kind=Pulse … now=500   ← B **收到了** A 的 PULSE
T20 W:control kind=Open  2 → 8192  ← B 这时才发出自己的 OPEN（最终裁决）
T21 W:control kind=Accept 2 → 8192
T28 TMR:scan now=1000
T29 TMR:abort 2 → 8192          ← B 把自己拆了
```

**根因**：`reserve_inbound_` 已经登记身份、计时循环也已经按这个身份发 `PULSE`，但
**接收侧读表项**要等应用调用 `accept_async`（`install_channel_` 投
`ReadEvent_::Attach`）才安装。这段窗口里到达的 `PULSE` 在
`FrameKind::WindowUpdate | Pulse` 分支里 `table.get(&pair)` 落空，而该分支**没有
else**，于是什么也不做——对端的存活信号被吞掉。等本端扫描时
`now - active >= timeout` 成立，就把一条**双方都在正常保活**的子流拆了。

这个窗口在真实时间里是微秒级，所以只在虚拟时间下暴露；但它不是 mock-clock 的错，
是 demux 的健壮性缺口。

### 13.5 实验性修复（未定稿，待裁决）

在 `table.get(&pair)` 落空时，用注册表兜底取 owner 并刷新时钟（`session_.rs`，+13 行）：

```rust
} else if let Option::Some(owner) = lock_or_exit_!(shared.reg_.channel_owner_(
    local, remote, cancel.child_token()
)) {
    if header.kind() == FrameKind::Pulse { owner.touch_(now_millis); }
    else { owner.mark_data_(now_millis); }
}
```

加上之后 `keepalive_pulses_compio_` 通过。需要裁决的两点：

1. 这条兜底是否采纳（它把「身份已在册、表项未安装」的窗口纳入了活动记账）；
2. **同一窗口内到达的 `DATA` 帧**目前会走「未知子流」分支（可能把整条连接判成协议
   违例）——本轮没有触发，但按同一条因果链它值得单独确认。

### 13.6 验证矩阵

| 目标 | 结果 |
| --- | --- |
| `abs_art`：mock_clock / tokio / compio / smol（含 mock-clock） | 26 + 2、23 + 9 + 2、32 + 16 + 10、37 + 9 + 2，全绿 |
| `abs_art` 四 crate `clippy -D warnings` | 通过 |
| `smux_v1` tokio 装配（lib / smoke / keepalive / inmem / alloc / thread_safety） | 199 + 4 + 2 + 11 + 1 + 2，0 failed |
| `smux_v1` compio 全量 `--all-targets` | 全绿（含 `layered_rpc` 1 passed 与 example） |

`layered_rpc` 的 **tokio** 用例仍是既有挂起（§12.4 已记，基线可复现），与本轮改动无关。

### 13.7 修法的副作用与处置

`Tracked_` 起初每次 **poll** 都 `Arc::new` 一次记录 waker。`alloc_count_baseline_tokio_`
的「稳态搬运」全局分配实测三组：

| 版本 | 稳态分配 | 结果 |
| --- | --- | --- |
| 未改动基线（`abs_art` 全还原） | **1504** | 通过 |
| 每次 poll 一次 `Arc::new` | **1984** | 失败（上限 1600） |
| 最终实现（构造时一次 `Arc<RecState_>`，poll 只 `Arc::clone` + 更新 `Mutex` 里的 waker） | **1504** | 通过 |

即最终实现的额外堆分配为 **0**，多出的 480 次正是「搬运期间任务被 poll 的次数」。
这也是「mock-clock 的包装不得进入热路径分配」这条纪律的第一次实测约束。

顺带记一条**测试装配**的问题：`test-tokio-runtime` 里塞了 `test-mock-clock`，于是
`alloc_count` 这类与虚拟时钟无关的基线用例也在「包装版」构建下跑，其预算里混进了
mock-clock 的构造期分配。本轮数值恰好没有差异，但这个耦合值得单独拆开。

### 13.8 代价边界：谁必须包装

「给每个 `spawn_local` 任务套一层记录 waker」是 **tokio 后端**为取得「有活」信号
付出的代价，不是 `abs_art-mock_clock` 的必然逻辑。三种后端实测：

| 后端 | 可用的「有活」信息 | 是否包装 |
| --- | --- | --- |
| smol | `LocalExecutor::try_tick()`（同步、公开） | 否 |
| compio | `Runtime::run()` + 「连续两轮」判据 | 否（本轮已去包装） |
| tokio | `LocalSet::tick` 是 `pub(crate)`；公开 API 里没有同步/异步 tick | **是**（唯一可行） |

tokio 去包装的对照实验：契约用例 `busy_executor_does_not_advance_virtual_time`
立刻失败（`left: 1500`，即每两轮推进一格）——因为没有信号时 `tick` 恒 `false`。
若要彻底去掉这份代价，只有换掉本地队列（例如改用带有同步 tick 的执行器），
那会改变 `spawn_local` 的语义，属于另一个量级的决定。

包装本身的代价（仅 `mock-clock` 构建、生产构建里整块 `#[cfg]` 掉）：
每个被投递的任务一次 `Box::pin` + 一次 `Arc<RecState_>`（**构造期**），
每次 poll 一次 `Arc::clone` 与一次 `Mutex` 加解锁（**无堆分配**）。
