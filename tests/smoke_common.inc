#![warn(unused)]

//! 冒烟测试的**共用餐**:同一份场景与断言，三个运行时各跑一遍。
//!
//! 本文件由三个薄壳（`tests/smoke_tokio.rs` / `tests/smoke_compio.rs` /
//! `tests/smoke_smol.rs`）各自
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
/// 下面每个运行时一个实例：**运行时值**与**本地作用域**都是运行时**具体**的（tokio
/// 与 compio 的 `Runtime` / `LocalScope` 是两对不同的类型），因此这里只把同一个场景体
/// 重写两遍**签名**，场景本体与断言完全共用。`rt` 进连接的类型参数并提供计时与时刻，
/// `scope` 交给 [`MuxConnection::new`] 投递五个循环。
#[cfg(feature = "test-tokio-runtime")]
pub async fn smoke_socket_body_(rt: &abs_art_tokio::Runtime, scope: &abs_art_tokio::LocalScope) {
    common::run_socket_scenario_on_runtime_(scope, |a_tx, a_rx, b_tx, b_rx| async move {
        common::run_smoke_scenario_(rt, scope, a_tx, a_rx, b_tx, b_rx).await;
    })
    .await;
}

/// 见 tokio 版说明。
#[cfg(all(
    not(feature = "test-tokio-runtime"),
    not(feature = "test-smol-runtime")
))]
pub async fn smoke_socket_body_(rt: &abs_art_compio::Runtime, scope: &abs_art_compio::LocalScope) {
    common::run_socket_scenario_on_runtime_(scope, |a_tx, a_rx, b_tx, b_rx| async move {
        common::run_smoke_scenario_(rt, scope, a_tx, a_rx, b_tx, b_rx).await;
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
pub async fn small_socket_body_(rt: &abs_art_tokio::Runtime, scope: &abs_art_tokio::LocalScope) {
    common::run_socket_scenario_on_runtime_(scope, |a_tx, a_rx, b_tx, b_rx| async move {
        common::run_small_mux_scenario_(rt, scope, a_tx, a_rx, b_tx, b_rx).await;
    })
    .await;
}

/// 见 tokio 版说明。
#[cfg(all(
    not(feature = "test-tokio-runtime"),
    not(feature = "test-smol-runtime")
))]
pub async fn small_socket_body_(rt: &abs_art_compio::Runtime, scope: &abs_art_compio::LocalScope) {
    common::run_socket_scenario_on_runtime_(scope, |a_tx, a_rx, b_tx, b_rx| async move {
        common::run_small_mux_scenario_(rt, scope, a_tx, a_rx, b_tx, b_rx).await;
    })
    .await;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 流控验收：真实 socket 上的「窗口用尽 → 回补 → 收尾」
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 测试目标：**真实 UNIX socket** 上，接收窗口被反复用尽并回补之后，大载荷仍然
/// 逐字节完好、且 `FIN` 排在全部数据之后。
///
/// - 手段：同一条 socket 装配，只是把场景换成与运行时无关的
///   [`common::run_flow_ctrl_socket_scenario_`]（子流环上钳到 512 字节、载荷 64 KiB、
///   接收侧每次只读 64 字节），并把整段场景包在 [`K_FLOW_CTRL_WATCHDOG_`] 之内。
/// - 判断：字节逐字节相等、收尾读到 `Closing`；**超时即 panic**——窗口回补通告若被
///   误判为过期（本仓库修掉的那个缺陷），这条用例会死锁而不是失败，超时是它唯一的
///   可观测征兆。
#[cfg(feature = "test-tokio-runtime")]
pub async fn flow_ctrl_socket_body_(
    rt: &abs_art_tokio::Runtime,
    scope: &abs_art_tokio::LocalScope,
) {
    common::run_socket_scenario_on_runtime_(scope, |a_tx, a_rx, b_tx, b_rx| async move {
        let scenario = common::run_flow_ctrl_socket_scenario_(rt, scope, a_tx, a_rx, b_tx, b_rx);
        match tokio::time::timeout(common::K_FLOW_CTRL_WATCHDOG_, scenario).await {
            Result::Ok(()) => {}
            Result::Err(_) => panic!(
                "流控场景超时（{:?}）：疑似窗口回补之后发送方永久拿不到额度而卡死",
                common::K_FLOW_CTRL_WATCHDOG_
            ),
        }
    })
    .await;
}

/// 见 tokio 版说明。
#[cfg(all(
    not(feature = "test-tokio-runtime"),
    not(feature = "test-smol-runtime")
))]
pub async fn flow_ctrl_socket_body_(
    rt: &abs_art_compio::Runtime,
    scope: &abs_art_compio::LocalScope,
) {
    common::run_socket_scenario_on_runtime_(scope, |a_tx, a_rx, b_tx, b_rx| async move {
        let scenario = common::run_flow_ctrl_socket_scenario_(rt, scope, a_tx, a_rx, b_tx, b_rx);
        match compio::time::timeout(common::K_FLOW_CTRL_WATCHDOG_, scenario).await {
            Result::Ok(()) => {}
            Result::Err(_) => panic!(
                "流控场景超时（{:?}）：疑似窗口回补之后发送方永久拿不到额度而卡死",
                common::K_FLOW_CTRL_WATCHDOG_
            ),
        }
    })
    .await;
}

/// 测试目标：**真实 UNIX socket** 上，一条子流的发送额度被用尽，不影响同连接上
/// 其它子流的收发。
///
/// - 手段：同一条 socket 装配，场景换成与运行时无关的
///   [`common::run_flow_ctrl_isolation_scenario_`]（子流 1 写满窗口而接收侧故意不读、
///   子流 2 正常双向收发），同样包在 [`common::K_FLOW_CTRL_WATCHDOG_`] 之内。
/// - 判断：子流 2 逐字节相等且收到 `Closing`；子流 1 的 `Tx` 仍可用；超时即 panic。
#[cfg(feature = "test-tokio-runtime")]
pub async fn flow_ctrl_isolation_socket_body_(
    rt: &abs_art_tokio::Runtime,
    scope: &abs_art_tokio::LocalScope,
) {
    common::run_socket_scenario_on_runtime_(scope, |a_tx, a_rx, b_tx, b_rx| async move {
        let scenario = common::run_flow_ctrl_isolation_scenario_(rt, scope, a_tx, a_rx, b_tx, b_rx);
        match tokio::time::timeout(common::K_FLOW_CTRL_WATCHDOG_, scenario).await {
            Result::Ok(()) => {}
            Result::Err(_) => panic!(
                "流控隔离场景超时（{:?}）：一条子流的额度耗尽拖死了其它子流",
                common::K_FLOW_CTRL_WATCHDOG_
            ),
        }
    })
    .await;
}

/// 见 tokio 版说明。
#[cfg(all(
    not(feature = "test-tokio-runtime"),
    not(feature = "test-smol-runtime")
))]
pub async fn flow_ctrl_isolation_socket_body_(
    rt: &abs_art_compio::Runtime,
    scope: &abs_art_compio::LocalScope,
) {
    common::run_socket_scenario_on_runtime_(scope, |a_tx, a_rx, b_tx, b_rx| async move {
        let scenario = common::run_flow_ctrl_isolation_scenario_(rt, scope, a_tx, a_rx, b_tx, b_rx);
        match compio::time::timeout(common::K_FLOW_CTRL_WATCHDOG_, scenario).await {
            Result::Ok(()) => {}
            Result::Err(_) => panic!(
                "流控隔离场景超时（{:?}）：一条子流的额度耗尽拖死了其它子流",
                common::K_FLOW_CTRL_WATCHDOG_
            ),
        }
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
#[cfg(all(
    not(feature = "test-tokio-runtime"),
    not(feature = "test-smol-runtime")
))]
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

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// smol 版（与上面两版逐字同构，只有「设备类型 + 适配 crate + 驱动方式」不同）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 测试目标、手段、判断见 tokio 版同名函数。
///
/// smol 侧多一处**驱动方式**的差别：本地队列是本线程 `thread_local!` 里的
/// `Rc<LocalExecutor<'static>>`，因此壳里用 `smol::block_on(scope.run_until(..))`
/// 把它驱动起来（见 `tests/smoke_smol.rs`）；场景体本身逐字共用。
#[cfg(feature = "test-smol-runtime")]
pub async fn smoke_socket_body_(rt: &abs_art_smol::Runtime, scope: &abs_art_smol::LocalScope) {
    common::run_socket_scenario_on_runtime_(scope, |a_tx, a_rx, b_tx, b_rx| async move {
        common::run_smoke_scenario_(rt, scope, a_tx, a_rx, b_tx, b_rx).await;
    })
    .await;
}

/// 见 tokio 版说明。
#[cfg(feature = "test-smol-runtime")]
pub async fn small_socket_body_(rt: &abs_art_smol::Runtime, scope: &abs_art_smol::LocalScope) {
    common::run_socket_scenario_on_runtime_(scope, |a_tx, a_rx, b_tx, b_rx| async move {
        common::run_small_mux_scenario_(rt, scope, a_tx, a_rx, b_tx, b_rx).await;
    })
    .await;
}

/// 见 tokio 版说明。
///
/// 看门狗换成 `smol::Timer`：`futures::future::select` 让「场景跑完」与「计时到点」
/// 二者竞速，后者先到即判为挂死。超时的判据与另两版完全一致。
#[cfg(feature = "test-smol-runtime")]
pub async fn flow_ctrl_socket_body_(rt: &abs_art_smol::Runtime, scope: &abs_art_smol::LocalScope) {
    common::run_socket_scenario_on_runtime_(scope, |a_tx, a_rx, b_tx, b_rx| async move {
        let scenario = common::run_flow_ctrl_socket_scenario_(rt, scope, a_tx, a_rx, b_tx, b_rx);
        let watchdog = async {
            smol::Timer::after(common::K_FLOW_CTRL_WATCHDOG_).await;
        };
        futures::pin_mut!(scenario);
        futures::pin_mut!(watchdog);
        match futures::future::select(scenario, watchdog).await {
            futures::future::Either::Left(((), _)) => {}
            futures::future::Either::Right(((), _)) => panic!(
                "流控场景超时（{:?}）：疑似窗口回补之后发送方永久拿不到额度而卡死",
                common::K_FLOW_CTRL_WATCHDOG_
            ),
        }
    })
    .await;
}

/// 见 tokio 版说明。
#[cfg(feature = "test-smol-runtime")]
pub async fn flow_ctrl_isolation_socket_body_(
    rt: &abs_art_smol::Runtime,
    scope: &abs_art_smol::LocalScope,
) {
    common::run_socket_scenario_on_runtime_(scope, |a_tx, a_rx, b_tx, b_rx| async move {
        let scenario = common::run_flow_ctrl_isolation_scenario_(rt, scope, a_tx, a_rx, b_tx, b_rx);
        let watchdog = async {
            smol::Timer::after(common::K_FLOW_CTRL_WATCHDOG_).await;
        };
        futures::pin_mut!(scenario);
        futures::pin_mut!(watchdog);
        match futures::future::select(scenario, watchdog).await {
            futures::future::Either::Left(((), _)) => {}
            futures::future::Either::Right(((), _)) => panic!(
                "流控隔离场景超时（{:?}）：一条子流的额度耗尽拖死了其它子流",
                common::K_FLOW_CTRL_WATCHDOG_
            ),
        }
    })
    .await;
}

/// 编译期断言（smol）：三层适配都成立——(1) 设备级适配后的 socket 半边满足 `common`
/// 对输入 / 输出设备的契约（本测试实际使用的形态）；(2) 环层 `BuffRead` / `BuffWrite`
/// 直接就是 `TrBuffRead` / `TrBuffWrite`（不需要中间环即可充当 `Rx` / `Tx`）；
/// (3) 两层的设备都是 `smol` 生态的原生流类型（`smol::net::unix::UnixStream`）。
#[cfg(feature = "test-smol-runtime")]
#[allow(dead_code)]
fn assert_smol_adapters_fit_() {
    use buffex::x_deps::abs_buff::{
        TrBuffRead, TrBuffWrite,
        io::{TrInput, TrOutput},
    };
    use buffex_smol_adapt::{BuffRead, BuffWrite, ReadAsInput, WriteAsOutput};
    use smol::net::unix::UnixStream;

    fn assert_buff_read_<R: TrBuffRead<u8>>() {}
    fn assert_buff_write_<W: TrBuffWrite<u8>>() {}
    fn assert_input_<I: TrInput<u8>>(_: I) {}
    fn assert_output_<O: TrOutput<u8>>(_: O) {}

    assert_buff_read_::<BuffRead<UnixStream>>();
    assert_buff_write_::<BuffWrite<UnixStream>>();

    let (mut stream_a, _) = UnixStream::pair().expect("建立 smol UNIX socket 对应当成功");
    assert_input_(ReadAsInput::new(&mut stream_a));
    assert_output_(WriteAsOutput::new(&mut stream_a));
}
