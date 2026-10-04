#![warn(unused)]

//! 冒烟测试的**共用餐**:同一份场景与断言，两个运行时各跑一遍。
//!
//! 本文件由两个薄壳（`tests/smoke_tokio.rs` / `tests/smoke_compio.rs`）各自
//! `mod smoke_common;` 引入——测试 target 是独立 crate，无法共享同一个模块定义，
//! 但被引入的是**同一份文件**。壳里只做「建立作用域 + 选运行时」。
//!
//! 链路：真实 UNIX domain socket（同一条全双工连接的两个端点）→ 设备级适配 →
//! 四条**调用方驱动**的 async 泵 → 全被动环 → `MuxConnection` 内部的四个循环。
//! 为什么必须由调用方驱动泵、以及三个实测阻塞点，见 `tests/common/mod.rs` 模块文档。

use crate::common;

/// 测试目标：smux v1 的「握手 → 拆连接 → 16 dock × 1024 子流收发」整条链路的
/// 冒烟级端到端行为，且传输是真实的 UNIX domain socket。
///
/// - 手段：建立 socket 对并完成传输装配（设备级适配 + 四条调用方驱动的泵 + 全被动
///   环），再跑与运行时无关的 [`common::run_smoke_scenario_`]；连接内部的四个循环经
///   作用域 `spawn_local` 投递，整个场景由 `scope.run_until(..)` 驱动。
/// - 判断：两侧各 16 个 dock 上的 1024 次 `open_channel_async` 与 1024 次
///   `accept_async` 全部返回成功；每条子流按 `(dock, index)` 自洽校验载荷（不依赖
///   open / accept 的配对顺序），丢弃发送半边后对端读到 `Closing`（EOF）。任一
///   不满足即 panic。
///
/// 下面每个运行时一个实例：作用域类型是运行时**具体**的（tokio 的 `LocalScope` 与
/// compio 的零大小作用域是两个类型），因此这里只把同一个场景体重写两遍**签名**，
/// 场景本体与断言完全共用。
#[cfg(feature = "test-tokio-runtime")]
pub async fn smoke_socket_body_(scope: &abs_art_tokio::LocalScope) {
    common::run_socket_scenario_on_runtime_(scope, |a_rx, a_tx, b_rx, b_tx| async move {
        common::run_smoke_scenario_(scope, a_rx, a_tx, b_rx, b_tx).await;
    })
    .await;
}

/// 见 tokio 版说明。
#[cfg(not(feature = "test-tokio-runtime"))]
pub async fn smoke_socket_body_(scope: &abs_art_compio::LocalScope) {
    common::run_socket_scenario_on_runtime_(scope, |a_rx, a_tx, b_rx, b_tx| async move {
        common::run_smoke_scenario_(scope, a_rx, a_tx, b_rx, b_tx).await;
    })
    .await;
}

/// 测试目标：同一条链路，但只跑 `2 dock × 2 条 = 4` 条子流的**快速传输回归**——
/// 它覆盖传输装配，秒级返回。
///
/// - 手段：同样的 socket 装配，只是把场景换成与运行时无关的
///   [`common::run_small_mux_scenario_`]。
/// - 判断：4 条子流的 open / accept / 双向收发 / 半关闭全部成功；失败即 panic。
#[cfg(feature = "test-tokio-runtime")]
pub async fn small_socket_body_(scope: &abs_art_tokio::LocalScope) {
    common::run_socket_scenario_on_runtime_(scope, |a_rx, a_tx, b_rx, b_tx| async move {
        common::run_small_mux_scenario_(scope, a_rx, a_tx, b_rx, b_tx).await;
    })
    .await;
}

/// 见 tokio 版说明。
#[cfg(not(feature = "test-tokio-runtime"))]
pub async fn small_socket_body_(scope: &abs_art_compio::LocalScope) {
    common::run_socket_scenario_on_runtime_(scope, |a_rx, a_tx, b_rx, b_tx| async move {
        common::run_small_mux_scenario_(scope, a_rx, a_tx, b_rx, b_tx).await;
    })
    .await;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 编译期探针：设备级适配必须满足 common 对输入 / 输出设备的契约
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 编译期断言（tokio）：socket 的两个半边经设备级适配后满足 `TrInput<u8>` /
/// `TrOutput<u8>`。函数体刻意不执行，只为让接线在类型层面被编译检查。
#[cfg(feature = "test-tokio-runtime")]
#[allow(dead_code)]
fn assert_tokio_adapters_fit_() {
    use buffex::x_deps::abs_buff::io::{TrInput, TrOutput};
    use buffex_tokio_adapt::x_deps::abs_buff_tokio_adapt::{ReadAsInput, WriteAsOutput};
    use tokio::net::UnixStream;

    fn assert_input_<I: TrInput<u8>>(_: I) {}
    fn assert_output_<O: TrOutput<u8>>(_: O) {}

    let (stream, _) = UnixStream::pair().expect("建立 tokio UNIX socket 对应当成功");
    let (mut read, mut write) = stream.into_split();
    assert_input_(ReadAsInput::new(&mut read));
    assert_output_(WriteAsOutput::new(&mut write));
}

/// 编译期断言（compio）：两类适配都成立——(1) socket 半边经**缓冲级**适配后直接就是
/// `TrBuffRead` / `TrBuffWrite`（不需要中间环即可充当 `Rx` / `Tx`）；(2) 经**设备级**
/// 适配后满足 `common` 的 `TrInput<u8>` / `TrOutput<u8>` 契约（本测试实际使用的形态）。
#[cfg(not(feature = "test-tokio-runtime"))]
#[allow(dead_code)]
fn assert_compio_adapters_fit_() {
    use buffex::x_deps::abs_buff::{
        TrBuffRead, TrBuffWrite,
        io::{TrInput, TrOutput},
    };
    use buffex_compio_adapt::{BuffRead, BuffWrite, ReadAsInput, WriteAsOutput};
    use compio::net::UnixStream;

    fn assert_buff_read_<R: TrBuffRead<u8>>() {}
    fn assert_buff_write_<W: TrBuffWrite<u8>>() {}
    fn assert_input_<I: TrInput<u8>>(_: I) {}
    fn assert_output_<O: TrOutput<u8>>(_: O) {}

    assert_buff_read_::<BuffRead<UnixStream>>();
    assert_buff_write_::<BuffWrite<UnixStream>>();

    let (std_a, _) =
        std::os::unix::net::UnixStream::pair().expect("建立 std UNIX socket 对应当成功");
    let mut stream = UnixStream::from_std(std_a).expect("应能注册到 compio 运行时");
    assert_input_(ReadAsInput::new(&mut stream));
    assert_output_(WriteAsOutput::new(&mut stream));
}
