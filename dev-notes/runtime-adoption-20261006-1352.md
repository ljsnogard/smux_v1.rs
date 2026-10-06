# 适配 `abs_art` 值化改造：`MuxConnection<C, R>` 与虚拟时间验收

日期：2026-10-06 13:52
分支：`feat/abs-art-runtime`（从 `feat/timer` 切出）
性质：**实现记录 + 决策因果**。
关联：`time-capability-20261005-1230.md`（本轮取代它的最后一环）、
`keepalive-timer-loop-20261005-1420.md`（计时循环本体不变，只换时钟来源）。
上游因果：`abs_art/dev-notes/local-scope-vs-runtime-20261006-1050.md`、
`instant-trait-and-manual-clock-20261006-1140.md`、`mock-clock-impl-20261006-1300.md`。

---

## 0. 一句话

`abs_art` 把**计时与时刻**从「消费方注入的时钟 + 后端计时器两个源」收回到
**运行时值一个源**（`TrTime: TrDelay + TrClock`），并把本地队列彻底剥出运行时值。
`smux_v1` 因此把公开类型从 `<C, S>`（配置 + 作用域）改成 `<C, R>`（配置 + 运行时值）、
删掉整层自造时钟注入与绝对期限算术，并接入 `abs_art-mock_clock` 做虚拟时间验收。

---

## 1. 上游变了什么（只列对 smux 有冲击的三条）

| 变化 | 形状 | 对 smux 的冲击 |
| --- | --- | --- |
| 能力方法**值化** | `D::delay(d)`（类型级关联函数）→ `rt.delay(d)`（`&self` 方法） | `TrDeadline` 的 blanket 实现两处调用点直接编译失败（明面上的 2 个错误） |
| 新增 `TrClock`，且 `TrTime: TrDelay + TrClock` | 「现在几点」与「怎么等」由**同一个值**回答 | smux 的注入式 `Clock`（`embedded-timers`）变成**第二个时钟源**，正是「睡在虚拟时钟、读在墙上时钟」的错配结构 |
| 计时能力从作用域**搬回**运行时值 | `TrClock`/`TrTime` 实现在 `Runtime<CAPS>` 上；`LocalScope` **只**实现 `TrLocalScope` | smux 的约束 `S: TrLocalScope + TrTime` **没有任何后端类型能满足**——这是必须改形状的那一条 |

第 3 条是结构性的：`feat/timer` 那版之所以能写 `S: TrLocalScope + TrTime`，是因为当时
三个后端的 `LocalScope` **也**实现了 `TrTime`。上游随后把队列从运行时值里剥出来
（因果见 `abs_art/dev-notes/local-scope-vs-runtime-20261006-1050.md`），计时便只剩
运行时值这一个宿主。

## 2. 决策：运行时值进类型、本地作用域不进

### 2.1 形状

```rust
pub struct MuxConnection<C, R>            // C = 资源策略；R = 运行时值（TrTime）
where C: TrConnCfg, R: TrClock { ... }

impl<C, R> MuxConnection<C, R> where C: TrConnCfg, R: TrTime + Clone + 'static {
    pub fn new<S>(rt: R, scope: &S, delivery, config, read_stage_buff, write_stage_buff) -> Self
    where S: TrLocalScope + Clone + 'static;
}
```

- **`R` 必须进类型**：API 面（`reserve_channel_` / `release_channel_` /
  `drain_session_events_`）都要读「现在」，因此 `MuxCore` 必须长期持有它。
- **`S` 不进类型**：它在 `new` 被用于 `spawn_local` 五个循环，此后不再需要。
  一个额外收益立刻显现：`abs_art_mock_clock::ManualTime` **不实现** `TrLocalScope`
  （队列不在时间装饰器上），所以「`S` 只当作用域」让虚拟时间验收**不需要**任何
  `TrLocalScope + TrTime` 的组合包装类型——这正是「两个关切分开」的红利。

### 2.2 为什么不是另外两个形状

| 备选 | 否决理由 |
| --- | --- |
| 保留 `<C, S>`，要求 `S: TrLocalScope + TrTime` | 新版**没有**任何后端类型同时满足两条；消费方（含本仓测试）都得自己写组合包装，负担外推 |
| 把 `R` 放进 `TrConnCfg`（`type Rt: TrTime` + `fn runtime()`） | 配置 trait 被迫承担运行时值语义；且虚拟时间验收要同时替换配置与时钟，两处耦合 |

### 2.3 保活责任：五个循环各持一份作用域克隆

`MuxCore` 原本有一个 `scope_` 字段（「作用域保活槽」）。去掉类型参数后它也不复存在，
于是「连接活着 ⇒ 队列活着」这条保证必须换人兑现——tokio 的 `LocalSet`、smol 的
`LocalExecutor` 都是**随作用域值存活**的 `Rc`。

选择：**投递时把 `scope.clone()` move 进循环 future**（`conn_.rs` 的
`keep_queue_alive_`）。于是只要还有一个循环在跑，队列就有持有者。

形成的 `队列 → 任务 → 队列` 引用环**不会泄漏**：最后一个应用面强引用消失时核心析构、
取消令牌触发（令牌是独立可克隆句柄，不依赖作用域），五个循环在下一个 await 点退出、
环随之解开。compio 的 `LocalScope` 是零大小标记（队列归运行时），本函数对它无实际
保活作用，但形状一致、无需分支。

被否决的备选：**完全不持有，把保活责任交回调用方**。它更薄，但调用方一旦先丢弃自己
那份作用域，五个循环会随队列一起消失、连接静默失效——这会**削弱**既有承诺。

## 3. 删掉了什么（净减法）

| 删除项 | 原用途 | 为什么不再需要 |
| --- | --- | --- |
| `TrConnCfg::Clock` + `fn clock()` | 注入时刻来源 | 时刻来自运行时值 `R`，且与 `delay` 同源 |
| `time::SystemClock` 与 `Clock` / `Instant` 重导出 | `embedded_timers` 的缺省实现与类型 | 同上；`R::Instant` 由各后端给出 |
| `time/deadline_.rs` 的 `TrDeadline` | 把绝对期限折算成 `Duration` | 计时循环只需要一个相对时长：`delay(期限 − 现在)`。两个量都已是「自 epoch 起算的毫秒」，相减即可 |
| `ConnClock_::deadline_()` | 把毫秒还原成后端的绝对时刻 | 同上。顺带绕开一个坑：`TrClock::Instant` 的结构约束**没有** `checked_add`（那是 `embedded_timers::Instant` 才有的），删掉它就不必面对溢出 |
| `embedded-timers` 依赖 | `std::time::Instant` 的适配 | 不再需要 |
| `test_support_::NullScope_`（含 `NullHandle_` / `NullJoinErr_`） | 「不驱动任务」的空作用域 | `new_test_` 不 spawn 任何循环，构造连接已不需要作用域；改用一个 `TrClock` 假运行时值 `NullRt_` |

`ConnClock_<R>` 自身保留，但收窄成**纯记账**：持 `R` 的克隆 + 建连 `epoch_`，
对外只剩 `now_millis_()`（自 epoch 起算、向下取整，饱和）与 `rt_()`（计时循环取等待源）。

## 4. 虚拟时间验收：`abs_art-mock_clock` 取代自造 timer

「自造 timer」在本仓其实有两层，要分开对待：

| 层 | 处置 | 理由 |
| --- | --- | --- |
| `connection/timer_.rs` 的**计时循环** | **保留** | 它是协议逻辑（PULSE 判定、空闲拆流、两条时钟的记账），与时钟来源无关 |
| `time/` 的自造绝对期限算术 + 注入式时钟 | **删除** | 见 §3；这是「abs_art 没有计时能力时」的补救 |

取代之后，虚拟时间验收的入口是各后端的
`LocalScope::block_on_advancing(&clock, body)`（`mock-clock` feature）+ `ManualTime`
装饰运行时值。关键形状：

```text
R = ManualTime<abs_art_tokio::Runtime<{ FULL }>>   // 时间：虚拟（时刻与 delay 同源）
S = abs_art_tokio::LocalScope                      // 队列：真实（由 block_on_advancing 驱动）
```

### 4.1 「推进」必须按定时器粒度，不能一次跳到底

一个容易写错的地方：**不能在 body 里写 `rt.delay(1600ms)` 来「等 1.6 秒」**。
那样 Supervisor 会一次把时钟推到 1.6 s，中间该发生的 `PULSE` 与超时判定全被跳过，
测出来的结论与真实运行无关。

正确形状是**让出**，由驱动的「空闲即推进」把时钟按**下一个到期时刻**逐格推进：

```rust
// 让出一次：body 返回 Pending，驱动借此推进时钟到下一个到期时刻并驱动本地队列。
async fn yield_once_() { /* poll 一次返回 Pending 并自唤醒 */ }

// 在虚拟时间上等够 duration：不主动定时，只让驱动逐格推进。
async fn virtual_sleep_<RT: TrClock>(rt: &RT, duration: Duration) {
    let start = rt.now();
    while rt.now() - start < duration { yield_once_().await; }
}
```

于是每个保活周期与每条期限都有机会执行，「虚拟 2.5 个超时周期」与真实运行同序。

## 5. 实测结论

（本节在实现与全量测试之后补写。）

## 6. 遗留

1. **`x_deps` 是否导出 `abs_art` / `abs_art_mock_clock`**：消费方现在要写
   `Runtime<{ FULL }>` 并自己取 `local_scope()`，导出一份会更省事；仍是公开面决定。
2. **`LocalScope` 的取得路径**：`local_scope()` 是各后端的**固有方法**，
   `abs_art::TrAsyncRuntime` 里没有对应入口。因此 `smux` 无法在泛型代码里「自己向
   abs_art 要作用域」，只能由调用方递进来。若将来上游补一条 trait 入口，
   `MuxConnection::new` 可以少一个参数。
3. **`keepalive` 虚拟时间的推进粒度**依赖 `Supervisor` 的「空闲即推进」。若某天后端
   的 tick 钩子语义变化（例如 tokio 有了可用 tick），需要复核 §4.1 的让出形状。
4. **compio 装配下连接句柄不再 `Send`（能力对调）**。改造前类型参数是各后端的
   `LocalScope`：compio 的那个是**零大小标记**，因此 `MuxConnection` 与各会话句柄
   在 compio 下是 `Send`（`tests/thread_safety.rs` 专门验这条）；tokio 的那个含
   `Rc<LocalSet>`，因而是 `!Send`。
   改造后类型参数换成**运行时值**：`abs_art_tokio::Runtime` 是 `Send + Sync` 的
   `Handle` 把手，而 `abs_art-compio::Runtime` **持有线程本地的 `compio` 运行时
   实例**、是 `!Send`（见该 crate `Runtime` 的文档）。于是这条性质在两个后端之间
   **对调**：tokio 下可跨线程、compio 下不可。
   这是「计时挂在运行时值上」的**直接后果**，smux 侧没有低成本绕法（API 面要读
   「现在」，核心就必须持有运行时值）。若将来需要恢复 compio 的跨线程句柄，只能
   由上游把 compio 的计时能力挂到一个 `Send` 的把手上。`thread_safety.rs` 已按新
   事实改写（迁到 tokio 装配）。
5. **`ManualTime` 的 `Clone` 缺口已在上游补上**。各后端的 `Runtime` 都实现了
   `Clone`（克隆共享同一句柄），但 `abs_art_mock_clock::ManualTime<R, C>` 原本**没有**
   `Clone` impl，而 `MuxConnection::new(rt: R)` 要求 `R: Clone`（核心与循环共享量
   各持一份连接级 `ConnClock_`），于是它不能直接当 `R`。
   处置：**给上游补上** `impl<R: Clone, C: ManualClockApi> Clone for ManualTime<R, C>`
   （非破坏性公开面扩展，已确认），本仓因此不需要任何包装类型，虚拟时间验收直接用
   `ManualTime`。记录见 `abs_art/dev-notes/mock-clock-impl-20261006-1300.md` 的同日补充。
