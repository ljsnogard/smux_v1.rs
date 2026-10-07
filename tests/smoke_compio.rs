//! 冒烟测试的 **compio** 壳：只做「建立运行时值 + 作用域 + 选运行时」。
//!
//! 场景本体在 `tests/smoke_common.inc`，与 tokio 壳引入的是**同一份文件**。
//! compio 的本地队列归运行时所有并由它驱动，作用域只是那份运行时的一个把手。
//!
//! 跑本 target 时必须**显式关掉** `test-tokio-runtime`：
//!
//! ```bash
//! cargo test --test smoke_compio --no-default-features --features test-compio-runtime
//! ```
//!
//! `--all-targets`（两个 feature 都在默认里）下本 target 会被 cfg 掉、不重复跑 compio；
//! compio 侧由上面这条命令单独覆盖。
//!
//! # 两个值从哪里来
//!
//! 与 tokio 壳同理：`LocalScope::new()` 已不存在，作用域只能经
//! `abs_art_bridge::current().local_scope()` 取得（`current()` 要求调用点已在 compio
//! 运行时上下文内——`#[compio::test]` 满足）。

#![cfg(not(feature = "test-tokio-runtime"))]
#![feature(allocator_ext)]

#[path = "common/mod.rs"]
mod common;

#[path = "smoke_common.inc"]
mod smoke_common;

/// 测试目标、手段、判断见 [`smoke_common::smoke_socket_body_`]。
#[compio::test]
async fn smoke_socket_compio_() {
    let rt = abs_art_bridge::current();
    common::assert_runtime_is_(&rt, abs_art_bridge::RuntimeTag::Compio);
    let scope = rt.local_scope();
    smoke_common::smoke_socket_body_(&rt, &scope).await;
}

/// 测试目标、手段、判断见 [`smoke_common::small_socket_body_`]。
#[compio::test]
async fn small_socket_compio_() {
    let rt = abs_art_bridge::current();
    common::assert_runtime_is_(&rt, abs_art_bridge::RuntimeTag::Compio);
    let scope = rt.local_scope();
    smoke_common::small_socket_body_(&rt, &scope).await;
}

/// 测试目标、手段、判断见 [`smoke_common::flow_ctrl_socket_body_`]。
///
/// **本条曾经 `#[ignore]`，本轮已转绿（`#[ignore]` 已去掉）**：与 tokio 侧同名用例
/// 同因、同修——跨环末端的逻辑读段被判成 `MalformedFrame`、接收侧消费量按
/// `data_size` 采样记账被并发写入掩盖，见
/// `dev-notes/flow-ctrl-20261005-0115.md` §2、§3。
#[compio::test]
async fn flow_ctrl_socket_compio_() {
    let rt = abs_art_bridge::current();
    common::assert_runtime_is_(&rt, abs_art_bridge::RuntimeTag::Compio);
    let scope = rt.local_scope();
    smoke_common::flow_ctrl_socket_body_(&rt, &scope).await;
}

/// 测试目标、手段、判断见 [`smoke_common::flow_ctrl_isolation_socket_body_`]。
#[compio::test]
async fn flow_ctrl_isolation_socket_compio_() {
    let rt = abs_art_bridge::current();
    common::assert_runtime_is_(&rt, abs_art_bridge::RuntimeTag::Compio);
    let scope = rt.local_scope();
    smoke_common::flow_ctrl_isolation_socket_body_(&rt, &scope).await;
}
