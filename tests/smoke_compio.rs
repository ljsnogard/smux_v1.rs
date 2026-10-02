//! compio 运行时下的 smux v1 集成冒烟测试。
//!
//! 场景本体与 tokio 版逐字相同（`tests/common/mod.rs`），差别只在接线：compio
//! 0.19 的 `UnixStream` 没有 `pair()`，因此先用 `std` 的 socket 对再注册进运行时；
//! 两个半边经 `buffex_compio_adapt` 的**设备级**适配（`ReadAsInput` /
//! `WriteAsOutput`）交给 `common` 的调用方驱动泵。
//!
//! 本文件有两个用例：
//!
//! - [`mux_smoke_compio_`]：`16 dock × 64 条 = 1024` 条子流的整链路冒烟；
//! - [`mux_small_socket_compio_`]：同一条链路的小场景（`2 dock × 2 条 = 4` 条），
//!   作为快速传输回归。
//!
//! 两个用例都用 **compio 的本地作用域值**（`abs_art_compio::LocalScope`，零大小）
//! 承载连接的两个循环：compio 的本地队列归运行时所有并由运行时自己驱动，因此用例
//! 只需取得作用域值即可。

mod common;

use abs_art::TrLocalScope;
use buffex::x_deps::abs_buff::{
    TrBuffRead, TrBuffWrite,
    io::{TrInput, TrOutput},
};
use buffex_compio_adapt::{BuffRead, BuffWrite, ReadAsInput, WriteAsOutput};
use compio::net::UnixStream;

/// 编译期断言：compio 侧两类适配都成立——(1) socket 半边经**缓冲级**适配后直接
/// 就是 `TrBuffRead` / `TrBuffWrite`（即不需要中间环即可充当 `Rx` / `Tx`）；(2)
/// 半边经**设备级**适配后满足 `common` 的 `TrInput<u8>` / `TrOutput<u8>` 契约
/// （本测试实际使用的形态）。函数体刻意不执行，只为让这两条接线在类型层面被编译
/// 检查。
#[allow(dead_code)]
fn assert_compio_adapters_fit_(read: &mut UnixStream, write: &mut UnixStream) {
    fn assert_buff_read_<R: TrBuffRead<u8>>() {}
    fn assert_buff_write_<W: TrBuffWrite<u8>>() {}
    fn assert_input_<I: TrInput<u8>>(_: I) {}
    fn assert_output_<O: TrOutput<u8>>(_: O) {}

    assert_buff_read_::<BuffRead<UnixStream>>();
    assert_buff_write_::<BuffWrite<UnixStream>>();
    assert_input_(ReadAsInput::new(read));
    assert_output_(WriteAsOutput::new(write));
}

/// 建立一条已注册进 compio 运行时的 UNIX socket 连接的两个端点。
///
/// compio 0.19 的 `UnixStream` 没有 `pair()`，因此先建 `std` 的 socket 对再逐个
/// `from_std` 注册。
fn compio_socket_pair_() -> (UnixStream, UnixStream) {
    let (std_a, std_b) =
        std::os::unix::net::UnixStream::pair().expect("建立 std UNIX socket 对应当成功");
    let stream_a = UnixStream::from_std(std_a).expect("a 端应能注册到 compio 运行时");
    let stream_b = UnixStream::from_std(std_b).expect("b 端应能注册到 compio 运行时");
    (stream_a, stream_b)
}

/// 测试目标：compio 运行时下，smux v1 的「握手 → 拆连接 → 16 dock × 1024
/// 子流收发」整条链路的冒烟级端到端行为，且传输是真实的 UNIX domain socket。
///
/// - 手段：用 `std::os::unix::net::UnixStream::pair()` 建立**一条**全双工 UNIX
///   domain socket 连接（socketpair 的两个端点属于同一条连接），两端分别经
///   `compio::net::UnixStream::from_std` 注册到当前运行时，各自 `into_split()`；
///   两个半边分别包成 `abs_buff_compio_adapt::{ReadAsInput, WriteAsOutput}`（设备级
///   适配），交给 [`common::run_socket_scenario_`]；后者用四条**调用方驱动**的
///   async 泵把「socket ↔ 全被动环」串起来（不使用 buffex 主动端，也不直接把
///   `TrBuffWrite` 当 `Tx`——原因见 `tests/common/mod.rs`），再跑与运行时无关的
///   1024 条子流场景；四个泵与场景用 `futures::select` 在同一任务内并发推进
///   （compio socket 是 `!Send`，因此刻意不 spawn）。
/// - 判断：两侧各 16 个 dock 上的 1024 次 `open_channel_async` 与 1024 次
///   `accept_async` 全部返回成功；每条子流按 `(dock, index)` 自洽校验载荷（不依赖
///   open / accept 的配对顺序），丢弃发送半边后对端读到 `Closing`（EOF）。任一
///   不满足即 panic，测试失败。
#[compio::test]
async fn mux_smoke_compio_() {
    let (stream_a, stream_b) = compio_socket_pair_();
    let (mut a_read, mut a_write) = stream_a.into_split();
    let (mut b_read, mut b_write) = stream_b.into_split();

    let scope = abs_art_compio::LocalScope::new();
    let scenario = common::run_socket_scenario_(
        &scope,
        ReadAsInput::new(&mut a_read),
        WriteAsOutput::new(&mut a_write),
        ReadAsInput::new(&mut b_read),
        WriteAsOutput::new(&mut b_write),
    );
    scope.run_until(scenario).await;
}

/// 测试目标：与 [`mux_smoke_compio_`] 同一条链路，但只跑 `2 dock × 2 条 = 4` 条
/// 子流的小场景，作为**快速传输回归**。
///
/// - 手段：同样用 std socket 对 + `from_std` + `into_split()` + 设备级适配 +
///   调用方驱动泵，只是把场景换成 [`common::run_small_socket_scenario_`]。
/// - 判断：4 条子流的 open / accept / 双向收发 / 半关闭全部成功；失败即 panic。
#[compio::test]
async fn mux_small_socket_compio_() {
    let (stream_a, stream_b) = compio_socket_pair_();
    let (mut a_read, mut a_write) = stream_a.into_split();
    let (mut b_read, mut b_write) = stream_b.into_split();

    let scope = abs_art_compio::LocalScope::new();
    let scenario = common::run_small_socket_scenario_(
        &scope,
        ReadAsInput::new(&mut a_read),
        WriteAsOutput::new(&mut a_write),
        ReadAsInput::new(&mut b_read),
        WriteAsOutput::new(&mut b_write),
    );
    scope.run_until(scenario).await;
}
