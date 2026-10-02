//! compio 运行时下的 smux v1 集成冒烟测试。
//!
//! 场景本体与 tokio 版逐字相同（`tests/common/mod.rs`），差别只在接线：compio
//! 0.19 的 `UnixStream` 没有 `pair()`，因此先用 `std` 的 socket 对再注册进运行时；
//! 两个半边经 `buffex_compio_adapt` 的**设备级**适配（`ReadAsInput` /
//! `WriteAsOutput`）交给 [`common::run_socket_scenario_`] 的四条调用方驱动泵。

mod common;

use buffex::x_deps::abs_buff::{
    TrBuffRead, TrBuffWrite,
    io::{TrInput, TrOutput},
};
use buffex_compio_adapt::{BuffRead, BuffWrite, ReadAsInput, WriteAsOutput};
use compio::net::UnixStream;

/// 编译期断言：compio 侧两类适配都成立——(1) socket 半边经**缓冲级**适配后直接
/// 就是 `TrBuffRead` / `TrBuffWrite`（即不需要中间环即可充当 `Rx` / `Tx`）；(2)
/// 半边经**设备级**适配后满足 [`common::run_socket_scenario_`] 的
/// `TrInput<u8>` / `TrOutput<u8>` 契约（本测试实际使用的形态）。函数体刻意不执行，
/// 只为让这两条接线在类型层面被编译检查。
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

/// 连接层使用的运行时标记类型。
///
/// 同时声明 `SPAWN_SEND` 与 `SPAWN_LOCAL` 两种能力：缺省（单线程）配置下
/// `MuxConnection::new` 要求 `Rt: TrSpawnLocal`，开启 `multi-thread` 后要求
/// `Rt: TrSpawnSend`；两种都声明，同一份测试才能在两种 feature 配置下都通过
/// 编译检查。compio 的运行时是线程本地的，因此实际执行路径始终是 `spawn_local`
/// 语义（见 `abs_art-compio` 的 `spawn_send` 模块文档）。
type SmokeRt =
    abs_art_compio::Runtime<{ abs_art_compio::SPAWN_SEND | abs_art_compio::SPAWN_LOCAL }>;

/// 测试目标：compio 运行时下，smux v1 的「握手 → 拆连接 → 16 dock × 1024 子流
/// 收发」整条链路的冒烟级端到端行为，且传输是真实的 UNIX domain socket。
///
/// - 手段：`std::os::unix::net::UnixStream::pair()` 建立**一条**全双工 UNIX
///   domain socket 连接（socketpair 的**两个端点属于同一条连接**），两端分别经
///   `compio::net::UnixStream::from_std` 注册到当前运行时，各自 `into_split()`；
///   两个半边分别包成 `abs_buff_compio_adapt::{ReadAsInput, WriteAsOutput}`（设备级
///   适配），交给 [`common::run_socket_scenario_`]；后者用四条**调用方驱动**的
///   async 泵把「socket ↔ 全被动环」串起来（不使用 buffex 主动端，也不直接把
///   `TrBuffWrite` 当 `Tx`——原因见 `tests/common/mod.rs` 与 dev-notes），再跑
///   与运行时无关的 [`common::run_smoke_scenario_`]；四个泵与场景用
///   `futures::select` 在同一任务内并发推进（compio socket 是 `!Send`，因此刻意
///   不 spawn）。
/// - 判断：两侧各 16 个 dock 上的 1024 次 `open_channel_async` 与 1024 次
///   `accept_async` 全部成功，且每条子流收到的载荷与按 `(dock, index)` 生成的
///   期望载荷逐字节相等；任何一次失败都会让场景 panic 从而测试失败。
///
/// 说明：连接层当前是 `todo!()` 骨架，因此本测试被标记为 `#[ignore]`；实现落地
/// 后移除。`cargo test -- --ignored` 会在 `MuxConnection::new` 处以
/// `not yet implemented` panic——这正说明测试已经编译、并成功跑过了运行时接线
/// 与握手。
#[compio::test]
#[ignore = "连接层尚未实现（todo!），实现落地后移除本属性"]
async fn mux_smoke_compio_() {
    let (std_a, std_b) =
        std::os::unix::net::UnixStream::pair().expect("建立 std UNIX socket 对应当成功");
    let stream_a = UnixStream::from_std(std_a).expect("a 端应能注册到 compio 运行时");
    let stream_b = UnixStream::from_std(std_b).expect("b 端应能注册到 compio 运行时");
    let (mut a_read, mut a_write) = stream_a.into_split();
    let (mut b_read, mut b_write) = stream_b.into_split();

    common::run_socket_scenario_::<_, _, _, _, SmokeRt>(
        ReadAsInput::new(&mut a_read),
        WriteAsOutput::new(&mut a_write),
        ReadAsInput::new(&mut b_read),
        WriteAsOutput::new(&mut b_write),
    )
    .await;
}
