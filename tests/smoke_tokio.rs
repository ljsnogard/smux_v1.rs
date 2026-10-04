//! 冒烟测试的 **tokio** 壳：只做「建立作用域 + 选运行时」。
//!
//! 场景本体在 `tests/smoke_common.inc`，与 compio 壳引入的是**同一份文件**——测试
//! target 是独立 crate，无法共享同一个模块定义，因此两个壳各自用 `#[path]` 包含它。
//!
//! 本 target 以 `required-features = ["test-tokio-runtime"]` 选中 tokio 的设备类型；
//! 共享文件按同一个 feature 分派（打开 ⇒ tokio 分支），因此 `cargo test --all-targets`
//! （两个 feature 都在默认里）两边的用例都能跑到。

#![cfg(feature = "test-tokio-runtime")]

#[path = "common/mod.rs"]
mod common;

#[path = "smoke_common.inc"]
mod smoke_common;

/// 测试目标、手段、判断见 [`smoke_common::smoke_socket_body_`]。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn smoke_socket_tokio_() {
    let scope = abs_art_tokio::LocalScope::new();
    smoke_common::smoke_socket_body_(&scope).await;
}

/// 测试目标、手段、判断见 [`smoke_common::small_socket_body_`]。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_socket_tokio_() {
    let scope = abs_art_tokio::LocalScope::new();
    smoke_common::small_socket_body_(&scope).await;
}
