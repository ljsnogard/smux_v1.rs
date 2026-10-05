//! 冒烟测试的 **compio** 壳：只做「建立作用域 + 选运行时」。
//!
//! 场景本体在 `tests/smoke_common.inc`，与 tokio 壳引入的是**同一份文件**。
//! compio 的本地队列归运行时所有并由它驱动，因此作用域是零大小值。
//!
//! 跑本 target 时必须**显式关掉** `test-tokio-runtime`：
//!
//! ```bash
//! cargo test --test smoke_compio --no-default-features --features test-compio-runtime
//! ```
//!
//! `--all-targets`（两个 feature 都在默认里）下本 target 会被 cfg 掉、不重复跑 compio；
//! compio 侧由上面这条命令单独覆盖。

#![cfg(not(feature = "test-tokio-runtime"))]

#[path = "common/mod.rs"]
mod common;

#[path = "smoke_common.inc"]
mod smoke_common;

/// 测试目标、手段、判断见 [`smoke_common::smoke_socket_body_`]。
#[compio::test]
async fn smoke_socket_compio_() {
    let scope = abs_art_compio::LocalScope::new();
    smoke_common::smoke_socket_body_(&scope).await;
}

/// 测试目标、手段、判断见 [`smoke_common::small_socket_body_`]。
#[compio::test]
async fn small_socket_compio_() {
    let scope = abs_art_compio::LocalScope::new();
    smoke_common::small_socket_body_(&scope).await;
}

/// 测试目标、手段、判断见 [`smoke_common::flow_ctrl_socket_body_`]。
///
/// **本条曾经 `#[ignore]`，本轮已转绿（`#[ignore]` 已去掉）**：与 tokio 侧同名用例
/// 同因、同修——跨环末端的逻辑读段被判成 `MalformedFrame`、接收侧消费量按
/// `data_size` 采样记账被并发写入掩盖，见
/// `dev-notes/flow-ctrl-20261005-0115.md` §2、§3。
#[compio::test]
async fn flow_ctrl_socket_compio_() {
    let scope = abs_art_compio::LocalScope::new();
    smoke_common::flow_ctrl_socket_body_(&scope).await;
}

/// 测试目标、手段、判断见 [`smoke_common::flow_ctrl_isolation_socket_body_`]。
#[compio::test]
async fn flow_ctrl_isolation_socket_compio_() {
    let scope = abs_art_compio::LocalScope::new();
    smoke_common::flow_ctrl_isolation_socket_body_(&scope).await;
}
