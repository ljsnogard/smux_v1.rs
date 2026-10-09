# `MuxCore` 的 `Send`/`Sync` 审计：根因、实证，与「配置随时取用运行时值」的裁决

日期：2026-10-09 12:18
分支：`main`（审计起点 `ab0ad30`，本次只改文档，见 §6）
性质：**调查 + 文档修正**（不改任何类型、trait 或函数签名）

---

## 0. 结论

1. 默认装配（compio）下 `MuxCore<C>` 与 `MuxConnection<C>` **既 `!Send` 也 `!Sync`**；
   tokio 装配下两者都是 `Send + Sync`。
2. 根因**不是**「核心持有了运行时值」——核心只存 `epoch_`（纯数据）。真正的入口是
   **核心持有整个配置 `config_: C`**，而 `DefaultConnCfg` 内含 `rt_: Rt`，compio 的
   `Rt` 牵着 `Rc<compio_executor::Executor>`。
3. 因此本仓多处文档里的论断「核心不持有运行时值，于是 `MuxCore<C>: Send + Sync` 与
   后端是否为 `Send` 无关」是**错的**，本次全部改掉（§6）。
4. 「让 `TrConnCfg` 不存储运行时值、改为随时取用」在**类型上成立**，但代价是放弃
   「注入运行时值」这条既有能力（`new_with_rt`、虚拟时钟 `ManualTime` 验收），并把
   「跨线程取时刻」从合法降级为「要求该线程也在后端上下文内」。因此**不是**改
   `DefaultConnCfg`，而是**新增**一个不存储的配置 `CurrentConnCfg`（方案 B，**已实施**，
   见 §5.1、§6.2），把这条代价交给**显式选择它**的调用方。

---

## 1. 症状与根因链

症状：把连接句柄移到别的线程时编译不过；`probe_send_.rs` 这类探针也只能得到否定答案。

rustc 在 compio 装配下给出的推导链（逐层 `note:`，原文摘录）：

```text
error[E0277]: `Rc<compio_executor::Executor>` cannot be sent between threads safely
   required because it appears within the type `compio_runtime::Runtime`
   required because it appears within the type `CompioRuntime<59>`      (abs_art-compio/src/lib.rs)
   required because it appears within the type `DefaultConnCfg<...>`    (src/connection/config_.rs)
   required because it appears within the type `MuxCore<...>`           (src/connection/mux_connection/core_.rs)
   required for `Shared<MuxCore<...>, _>` to implement `Send`
   required because it appears within the type `MuxConnection<...>`     (src/connection/mux_connection/conn_.rs)
```

整理成一条链：

```
MuxConnection<C>                      = Shared<MuxCore<C>, C::Alloc>
  └─ MuxCore<C>.config_: C            ← 唯一入口：!Send 从这里进来
       └─ DefaultConnCfg.rt_: Rt
            └─ DefaultRt_ = abs_art_bridge::Runtime = abs_art_compio::Runtime
                 └─ compio_runtime::Runtime
                      └─ Rc<compio_executor::Executor>   ← !Send + !Sync
```

关键是 `mm_ptr::Shared` 的 auto-trait impl 是**条件**的：

```rust
unsafe impl<T, A> Send for Shared<T, A>
where T: ?Sized + Send + Sync, A: AllocatorClone + Send + Sync {}
```

`MuxConnection` 只是它的薄封装，所以「连接能不能跨线程」完全由 `T = MuxCore<C>` 决定，
而 `MuxCore<C>` 又完全由 `C: Send + Sync` 决定。生产代码里**没有**任何显式
`C: Send + Sync` 约束——要求是**推**出来的，这也是它容易被文档写错的原因。

---

## 2. 实证矩阵

方法：临时 probe crate（`old-cantare/.tmp/probe_smux_send/`）逐条放编译期断言，
`cargo +nightly check --offline` 通过 = 实现该 trait，失败 = 未实现。
被测类型：`type C0 = DefaultConnCfg<BufferedTx, BufferedRx>;`（`M`/`P`/`Rt` 全默认）。

| 装配 | 断言 | 结果 | 失败首行指出的类型 |
| --- | --- | --- | --- |
| **compio**（默认） | `C0: Send` | ❌ | `Rc<compio_executor::Executor>` |
| **compio** | `C0: Sync` | ❌ | `Rc<compio_executor::Executor>` |
| **compio** | `MuxConnection<C0>: Send` | ❌ | 同上 |
| **compio** | `MuxConnection<C0>: Sync` | ❌ | 同上（`Shared` 的 `Sync` impl 也要求 `T: Send`，故报 Send） |
| **tokio** | 上述四条 | ✅ 全通过 | — |
| tokio（额外） | `DefaultRt_ == abs_art_tokio::Runtime` | ✅ | 证明「默认参数即 tokio 运行时值」 |
| compio（对照） | `NoMetrics: Send + Sync` | ✅ | 证明探针本身有效 |

复现要点（踩过的坑）：**`[patch]` 只在 workspace 根生效**。`smux_v1/Cargo.toml` 自己那份
指向 `../abs_art/*` 的 patch 对独立 workspace 的 probe **无效**，必须在 probe 的
`Cargo.toml` 里按 `../../abs_art/*` 重写六条，否则 Cargo 会去 gitee 取远端旧 commit，
于是 `TrLocalScope` 出现「同名不同源」。全程 `--offline` 可用，不需要联网。

---

## 3. 为什么「加个约束」解决不了

compio 的运行时值是**线程本地**的（`Rc` 簇，绑定创建它的线程），这是 `abs_art-compio`
刻意的如实表达，不是本仓能靠 where 子句绕过去的东西：

- 想要 `C: Send + Sync` 而 `C::Rt` 仍是 compio 的运行时值 —— 不可能，二者矛盾；
- 想要「核心不持有就没事」—— 已证伪：核心确实不直接持有，但**间接**持有（`config_: C`）。

---

## 4. 提案的裁决：「`TrConnCfg` 不存运行时值，改为随时取用」

提案形状：`DefaultConnCfg` 去掉 `rt_: Rt` 字段（换成 `PhantomData`），
`fn runtime(&self)` 改为每次调 `abs_art_bridge::current()`（或等价的「从当前上下文取」）。

**类型上成立**：去掉 `rt_` 后 `C` 只剩 `policy_: P`（`DefaultPolicy` 是 ZST）、
`metrics_: M`（`TrMetricsSink: Send + Sync + 'static`）、`PhantomData<fn() -> W/R>`，
于是 `C: Send + Sync`，compio 装配下 `MuxCore<C>` 也就 `Send + Sync`。
另有一条支持性事实：`DefaultConnCfg::new` **本来**就是建连时调 `current()` 抓住值，
所以提案只是把「抓住的时机」从建连推迟到每次使用。

**但有三处硬代价**，逐条给出本仓内的证据：

1. **「注入运行时值」这条能力会消失。**
   被放弃的不是边角功能：
   - `MuxConnection::new_with_rt`（上下文外建连、固定具名后端）；
   - **虚拟时钟端到端验收**：`tests/keepalive.rs`（tokio 侧用
     `Runtime::with_handle` 造值、再用 `ManualTime` 装饰）、`tests/keepalive_compio.rs`
     （`Runtime::with_runtime` 同理）、`src/time/tests_.rs`；
   - `src/connection/test_support_.rs` 的 `NullRt_` 假运行时值。
   这些值**无法**由 `current()` 取得（它只会给出后端真实的运行时值），配置一旦不存储
   就再也交不出来。
2. **跨线程取时刻从「合法」变成「panic」。**
   `MuxCore::now_millis_` 在 bind / release / 会话事件路径上被调用；改成随时取用后，
   这些调用点要求**所在线程**处于后端上下文内。tokio 装配下「句柄跨线程」
   （`tests/thread_safety.rs` 钉住的既有能力）将因此附加上这个前提，而 `Handle::current()`
   在非 tokio 线程上是 panic。
3. **契约要反过来写，且缺一个统一入口。**
   `TrConnCfg::runtime` 的现有契约明确写着「实现必须交出**建连时就已经抓住**的那个值，
   而不是临场重建」——正是提案的反面。且泛型上下文里的 `C::Rt` 调不出 `current()`：
   `abs_art` 目前只有各后端的**固有** `current()`，没有 trait 形式。

---

## 5. 方案对比与选择

| 方案 | 做法 | 代价 / 结论 |
| --- | --- | --- |
| **A. 只修文档**（本次采用） | 把「核心不持有 ⇒ 无条件 `Send+Sync`」改准确；修正过时的 `MuxConnection<C, R>` 描述 | 零代码风险；compio 装配仍 `!Send`（如实） |
| **B. 新增「无存储」配置**（已实施，见 §5.1） | 保留 `DefaultConnCfg`（注入照旧），新增 `CurrentConnCfg<…>`：`rt_` 换 `PhantomData`，`runtime()` 从上下文取 | 新增公开类型；调用点须在上下文内（**调用者责任**） |
| C. 拆 `TrConnCfg` 两层 | 核心配置（`Send+Sync`）与运行时值来源分开 | 改动面覆盖所有 API 面方法，收益与 B 相同 |
| D. 放弃「时刻与计时器同源」 | 核心改用进程级单调时钟 | 虚拟时钟失效，与 `abs_art` 的 `TrTime` 设计冲突，不推荐 |

本次裁决：**先做 A（把文档改诚实），随后做 B**。需求方确认了 B 的前提：**那些线程由
调用者保证处于 compio 上下文内**（否则 panic 属调用者违约），并要求在实现里以 debug
断言给出提示、在文档里明确声明这条责任。B 因此**不改 `DefaultConnCfg`**——注入
（虚拟时钟、`new_with_rt`、假运行时值）那条路原样保留，代价由显式选择 `CurrentConnCfg`
的调用方承担。

### 5.1 B 的落地形状

| 位置 | 内容 |
| --- | --- |
| `abs_art`（三后端 + bridge） | 新增 `try_current()`：各后端 `Runtime::try_current()` 与 crate 级自由函数的**不 panic** 版本（tokio `Handle::try_current` / compio `Runtime::try_current` / smol 恒 `Some`），bridge 按 feature 转发裸名 |
| `smux_v1::connection::TrRtCurrent` | 本地适配 trait（与 `ScopeHost` 同形）：`current_rt()` 从上下文取值、`try_current_rt()` 探测；`DefaultRt_` 的实现里 `debug_assert!` 给出「调用者责任」提示 |
| `smux_v1::connection::CurrentConnCfg` | 与 `DefaultConnCfg` **同序泛型**、**不存储**运行时值的配置；`runtime()` = `Rt::current_rt()`。额外约束 `Rt: TrRtCurrent` 天然排除 `ManualTime`（它无法凭空构造），把「虚拟时钟只能走 `DefaultConnCfg`」变成**编译期**事实 |

---

## 6. 本次落地

### 6.1 文档修正

六处（不含逻辑）：

| 文件 | 原论断 | 现在 |
| --- | --- | --- |
| `mux_connection/core_.rs` | 「`MuxCore<C>: Send + Sync` 与后端是否为 `Send` 无关」 | 改为「不**直接**持有，但经 `config_: C` 间接持有，故由 `C::Rt` 决定」，附 tokio/compio 对照表与提案代价 |
| `connection/config_.rs` | 「`MuxCore` 必须**无条件** `Send + Sync`」 | 同上口径 |
| `time/clock_.rs` | 「核心必须无条件 `Send + Sync`」（`epoch_` 注释） | 改为「直接持有会让核心永远无法 `Send+Sync`」，并注明光这样还不够 |
| `connection/session_.rs` | 「核心已因『必须无条件 `Send+Sync`』而不持有运行时值」 | 同上口径 |
| `mux_connection/conn_.rs` | 泛型参数写作 `C` + `R` 两个 | 改为「运行时值由 `C::Rt` 给出；连接是否 `Send+Sync` 由 `C::Rt` 决定」 |
| `tests/thread_safety.rs` | 同上旧形状；compio 只标 `!Send` | 补 `C::Rt` 一栏，compio 标 `!Send + !Sync`，并记录实测入口 |

另：`src/probe_send_.rs` 是**未被 `lib.rs` 声明、不参与编译**的孤立探针文件
（只有一个空的 `assert_send_sync`）。本次未触碰；后续要么接进模块写实，要么删除。

### 6.2 代码改动（方案 B）

| 仓 / 文件 | 改动 |
| --- | --- |
| `abs_art-tokio` / `-compio` / `-smol` | 各新增 `try_current()`（crate 级自由函数 + `Runtime<CAPS>` 固有方法），带文档与 doctest |
| `abs_art-bridge` | 按三条 feature 路径转发裸名 `try_current`（与 `current` 同一组 cfg 守卫） |
| `smux_v1` `connection/scope_host_.rs` | 新增 `TrRtCurrent`（`current_rt` / `try_current_rt`）与 `DefaultRt_` 的实现（含 `debug_assert!` 提示） |
| `smux_v1` `connection/config_.rs` | 新增 `CurrentConnCfg<W, R, M, P, Rt>` 及其 `TrMuxConfig` / `TrConnCfg` / `Clone` / `Copy` / `Debug` 实现 |
| `smux_v1` `connection/mod.rs` | 导出 `CurrentConnCfg`、`TrRtCurrent` |
| `smux_v1` `tests/current_conn_cfg.rs` | 编译期断言：默认（compio）装配下 `CurrentConnCfg<BufferedTx, BufferedRx>` 与 `MuxConnection<同>` 都是 `Send + Sync` |

验证：默认 compio 与 `--no-default-features --features test-tokio-runtime` 两种装配下
断言测试均通过；`cargo check` / `clippy` 干净；`--lib` 220 条单元测试全绿；三后端
doctest 全绿（含新增的 `try_current` 示例）。

`MuxConnection` 的公开 API **一行都没改**：`CurrentConnCfg` 与 `DefaultConnCfg` 泛型同序，
`new` / `new_with_rt` / 五个循环的投递路径全部原样。

---

## 7. 未决与后续

1. **跨线程行为测试未做**：`CurrentConnCfg` 目前只有编译期断言（类型确实 `Send + Sync`）。
   要覆盖「另一条线程上 bind / 解绑」，需要在那条线程里自建 compio 上下文——这是下一步的
   验收目标（形如 `tests/thread_safety.rs`，但 compio 侧）。
2. **「虚拟时钟」与 `CurrentConnCfg` 互斥是编译期事实**：`Rt: TrRtCurrent` 排除了
   `ManualTime`；虚拟时钟装配继续走 `DefaultConnCfg`（其连接在 compio 装配下仍 `!Send`）。
   测试矩阵因此按装配拆开，不做「同一份用例两种配置都跑」。
3. **smol 装配未实测**（`try_current` 恒 `Some`，`CurrentConnCfg` 在 smol 下无上下文前提；
   推断与 tokio 一致）。
4. **探针与脚本**保留在 `old-cantare/.tmp/probe_smux_send/`
   （`probe.sh`、`results_compio.txt`、`results_tokio.txt`），可随时重跑。
