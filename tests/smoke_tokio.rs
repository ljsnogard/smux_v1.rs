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

/// 测试目标、手段、判断见 [`smoke_common::flow_ctrl_socket_body_`]。
///
/// 「小接收环 + 大发送」在真实 UNIX socket 上验收流控：窗口用尽与回补不得丢字节、
/// 不得死锁。`multi_thread` 与真实 socket 一起构成「有真实 IO 参与时流控仍然成立」
/// 的验收（内存环直连的版本见 `tests/inmem_mux.rs`）。
///
/// **本条曾经 `#[ignore]`，本轮已转绿（`#[ignore]` 已去掉）**：它专为「窗口反复归零
/// 再回补」而设，因此曾稳定卡死。两个真因都已修：发送方复用循环把跨环末端的逻辑读段
/// 误判成 `MalformedFrame` 而终止；接收侧「已消费量」按 `data_size` 采样记账、被并发
/// 写入掩盖。因果链与实测记录见 `dev-notes/flow-ctrl-20261005-0115.md` §2、§3。
///
/// （同目录的 `flow_ctrl_isolation_socket_tokio_` 只把一条子流写到恰好一个窗口，
/// 不触发归零后的回补，是本条之外的独立对照。）
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flow_ctrl_socket_tokio_() {
    let scope = abs_art_tokio::LocalScope::new();
    smoke_common::flow_ctrl_socket_body_(&scope).await;
}

/// 测试目标、手段、判断见 [`smoke_common::flow_ctrl_isolation_socket_body_`]。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flow_ctrl_isolation_socket_tokio_() {
    let scope = abs_art_tokio::LocalScope::new();
    smoke_common::flow_ctrl_isolation_socket_body_(&scope).await;
}
