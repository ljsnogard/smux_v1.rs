//! 冒烟测试的 **smol** 壳：只做「建立运行时值 + 作用域 + 驱动本地队列」。
//!
//! 场景本体在 `tests/smoke_common.inc`，与 tokio / compio 壳引入的是**同一份文件**。
//!
//! # 与前两个壳的差别：谁来驱动本地队列
//!
//! tokio 壳用 `#[tokio::test]`（运行时自带执行体），compio 壳用 `#[compio::test]`
//! （thread-local `Runtime` 自己驱动）。smol **没有隐式运行时上下文**：本地队列就在本
//! 线程的 `thread_local!` 里（`Rc<LocalExecutor<'static>>`，见 `abs_art-smol` 的
//! `local_scope.rs`），必须由调用方驱动。因此这里是普通的 `#[test]`，整段场景挂在
//! `smol::block_on(scope.run_until(..))` 上：`block_on` 是外部阻塞驱动源，
//! `run_until` 在其中顺带推进本线程的本地队列。
//!
//! # 两个值从哪里来
//!
//! 与另两个壳同理：`abs_art_bridge::current()` 取当前后端（本 target 由
//! `required-features = ["test-smol-runtime"]` 钉死为 smol），再向它要本地作用域
//! （`rt.local_scope()`，同一线程多次取得拿到同一条队列）。
//!
//! # 怎么跑
//!
//! ```bash
//! cargo test --test smoke_smol --no-default-features --features test-smol-runtime
//! ```
//!
//! 必须 `--no-default-features`：缺省 feature 会同时打开 compio 后端，而 bridge 的
//! 裸名 `Runtime` 要求「当前编译里只有一个后端」。

#![cfg(feature = "test-smol-runtime")]
#![feature(allocator_ext)]

use abs_art::TrLocalScope;

#[path = "common/mod.rs"]
mod common;

#[path = "smoke_common.inc"]
mod smoke_common;

/// 测试目标、手段、判断见 [`smoke_common::smoke_socket_body_`]。
///
/// 这条同时是「`buffex_smol_adapt` 的设备级适配 + 调用方驱动的泵 + 全被动
/// `buffex::ring` 环」的**对接冒烟**：只要其中任何一环在类型或运行期对不上（设备
/// trait 不匹配、`poll_read` 的窗口语义不对、环的提交时机不对），场景就会挂住或字节
/// 对不上。
#[test]
fn smoke_socket_smol_() {
    let rt = abs_art_bridge::current();
    common::assert_runtime_is_(&rt, abs_art_bridge::RuntimeTag::Smol);
    let scope = rt.local_scope();
    smol::block_on(scope.run_until(smoke_common::smoke_socket_body_(&rt, &scope)));
}

/// 测试目标、手段、判断见 [`smoke_common::small_socket_body_`]。
#[test]
fn small_socket_smol_() {
    let rt = abs_art_bridge::current();
    common::assert_runtime_is_(&rt, abs_art_bridge::RuntimeTag::Smol);
    let scope = rt.local_scope();
    smol::block_on(scope.run_until(smoke_common::small_socket_body_(&rt, &scope)));
}

/// 测试目标、手段、判断见 [`smoke_common::flow_ctrl_socket_body_`]。
///
/// 「小接收环 + 大发送」在真实 UNIX socket 上验收流控：窗口用尽与回补不得丢字节、
/// 不得死锁。超时由场景内的 `smol::Timer` 看门狗负责。
#[test]
fn flow_ctrl_socket_smol_() {
    let rt = abs_art_bridge::current();
    common::assert_runtime_is_(&rt, abs_art_bridge::RuntimeTag::Smol);
    let scope = rt.local_scope();
    smol::block_on(scope.run_until(smoke_common::flow_ctrl_socket_body_(&rt, &scope)));
}

/// 测试目标、手段、判断见 [`smoke_common::flow_ctrl_isolation_socket_body_`]。
#[test]
fn flow_ctrl_isolation_socket_smol_() {
    let rt = abs_art_bridge::current();
    common::assert_runtime_is_(&rt, abs_art_bridge::RuntimeTag::Smol);
    let scope = rt.local_scope();
    smol::block_on(scope.run_until(smoke_common::flow_ctrl_isolation_socket_body_(&rt, &scope)));
}
