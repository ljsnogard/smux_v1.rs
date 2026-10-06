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
