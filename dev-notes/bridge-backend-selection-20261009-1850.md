# `abs_art-bridge` 的后端选择：具名别名 + 本 crate 三选一

## 1. 起因

上一提交 `Follow abs_art-bridge update` 之后，**tokio / smol 装配编不过**（缺省 compio
装配正常）。实测两类错误：

1. `cannot find function current in crate abs_art_bridge`：bridge 删掉了
   `current` / `try_current` 两个**自由函数**，统一改成各后端 `Runtime::current()` /
   `Runtime::try_current()` 的固有方法；调用点只改了一半。
2. `the trait bound TokioRuntime: ScopeHost is not satisfied`：本 crate 的
   `ScopeHost` 只对 bridge 的**裸名** `Runtime` 实现，而 tokio 装配下裸名并不是 tokio。

## 2. 关键裁决：不写 `default-features = false`

曾一度认为「要换默认后端，就得关掉 bridge 自己的默认 feature」。**这是错的**：

- bridge 的缺省后端就是 `default = ["default-backend-compio"]`，它始终在线；
- 按照 bridge 自己的设计（其 crate 文档「多个后端可以同时启用」一节），
  **非缺省后端用具名别名**取用：`TokioRuntime` / `CompioRuntime` / `SmolRuntime`
  以及对应的 `*LocalScope`；
- 同仓的 `abs_buff_stdio_adapt` 同样依赖 bridge，`[dependencies]` 里就是一条普通
  依赖，**没有** `default-features = false`。逼每个下游都去关默认 feature 不是约定用法。

因此本 crate 的 `[dependencies] abs_art-bridge` 保持普通依赖；`test-tokio-runtime` /
`test-smol-runtime` 只**追加** `abs_art-bridge/backend-tokio` / `backend-smol`，不碰
`default-backend-*`（碰了会与缺省的 compio 撞上 bridge 的「只能声明一个默认后端」守护）。

## 3. 实现

### 3.1 `ScopeHost` / `TrRtCurrent` 改为对具名别名逐个实现

`src/connection/scope_host_.rs` 用一个局部宏 `impl_scope_host_!` 对三个具名别名生成
两条实现。其中：

- `TokioRuntime` / `SmolRuntime` 只在对应 `test-*-runtime` 打开时实现；
- `CompioRuntime` 的实现**无条件**给出——bridge 的缺省后端始终在线，而且
  `examples/` 是 compio 演示，在 tokio 装配的 `cargo check --all-targets` 下也要编过。

### 3.2 `DefaultRt_` 由本 crate 的 feature 三选一

```text
test-tokio-runtime ⇒ TokioRuntime
test-smol-runtime  ⇒ SmolRuntime
否则                ⇒ CompioRuntime
```

于是「谁是本 crate 的默认后端」不再依赖 bridge 的裸名解析，`DefaultConnCfg` /
`CurrentConnCfg` 的 `Rt` 缺省值、`default_rt_()` 都跟着这条规则走
（`default_rt_()` 现在经 `<DefaultRt_ as TrRtCurrent>::current_rt()` 取）。

### 3.3 测试壳用后端 crate 的自由函数取当前值

`Runtime::current()` 有个坑：`Runtime<const CAPS: usize = FULL>` 的 `CAPS` 在调用点
类型没被别处钉死时**推不出来**（`tests/inmem_mux.rs` 早就记过这条）。因此测试壳统一走
各后端 crate 的自由函数 `abs_art_tokio::current()` / `abs_art_compio::current()` /
`abs_art_smol::current()`——它们的返回类型已经是 `Runtime<FULL>`。

## 4. 验收

- `cargo build`（无参数，缺省 compio）：通过；
- `cargo check --all-targets` 四格（缺省 / 只 tokio / 只 compio / smol 冒烟）：通过；
- `cargo clippy --all-targets -- -D warnings` 三格（同 `just check` 配方）：通过；
- `just test`（无参数，跑完 clippy → 编译矩阵 → smol/tokio/compio/metrics）：通过。

## 5. 遗留

`smux_v1_sock_demo` 仍把 `Rt` 定义成 `abs_art_bridge::Runtime`（裸名 = compio）并用
`abs_art_bridge::current()`。它是在本轮之前就坏的状态（那个自由函数已不存在），且是独立
workspace，本轮未动；要修的话得让它自己的 `rt-*` feature 决定 `Rt`，并改用
`abs_art_bridge::TokioRuntime` 一类具名别名。
