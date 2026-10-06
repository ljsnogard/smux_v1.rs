//! socket 侧装配：建一条全双工 UNIX domain socket、铺四条调用方驱动的泵与全被动环，
//! 把「设备搬运」与「环提交」显式串起来；并给出两个运行时各自的作用域驱动入口。
//!
//! 场景本身与运行时无关（见 [`super::scenarios_`]），本文件只负责**装配与驱动**。

use buffex::x_deps::abs_buff::io::{TrInput, TrOutput};
use mm_ptr::x_deps::abs_mm::CoreAlloc;

use crate::common::{
    K_NET_BUFFER_SIZE,
    SmokeBuff,
    TrLocalScope,
    TrSmokeRt,
    TrSmokeScope,
    make_passive_ring_,
    run_small_mux_scenario_,
    run_smoke_scenario_,
};

use super::pump_::{pump_input_, pump_output_};

/// 用「socket + 调用方驱动的泵 + 全被动环」跑完 [`run_smoke_scenario_`]。
///
/// 参数依次是 A 端（握手发起方）与 B 端（等待方）的 socket 读设备（`TrInput`）与
/// 写设备（`TrOutput`）：tokio 侧由 `buffex_tokio_adapt`（设备级适配来自它依赖的
/// `abs_buff_tokio_adapt`）提供，compio 侧由 `buffex_compio_adapt` 直接提供。
///
/// 每端装配两个全被动环：
///
/// ```text
/// socket 读设备 --(pump_input_)--> ring_tx ──> ring_rx = smux 的 Rx
/// smux 的 Tx = ring_tx ──> ring_rx --(pump_output_)--> socket 写设备
/// ```
///
/// 四个泵与场景用 `select` 并发推进（同一任务内轮询，不要求任何类型 `Send`）；
/// 场景完成即丢弃泵 future，从而结束对设备（及其借用的 socket 半边）的借用。
pub async fn run_socket_scenario_<IA, OA, IB, OB, S, RT>(
    rt: &RT,
    scope: &S,
    input_a: IA,
    output_a: OA,
    input_b: IB,
    output_b: OB,
) where
    IA: TrInput<u8>,
    OA: TrOutput<u8>,
    IB: TrInput<u8>,
    OB: TrOutput<u8>,
    S: TrSmokeScope + Clone + 'static,
    RT: TrSmokeRt,
{
    run_socket_scenario_with_(input_a, output_a, input_b, output_b, |a_tx, a_rx, b_tx, b_rx| {
        run_smoke_scenario_::<_, _, _, _, S, RT>(rt, scope, a_tx, a_rx, b_tx, b_rx)
    })
    .await
}


/// 与 [`run_socket_scenario_`] 相同的传输装配，但跑**本轮验收用的小场景**
/// （2 dock × 各 2 条 channel，双向收发 + 半关闭），见 [`run_small_mux_scenario_`]。
pub async fn run_small_socket_scenario_<IA, OA, IB, OB, S, RT>(
    rt: &RT,
    scope: &S,
    input_a: IA,
    output_a: OA,
    input_b: IB,
    output_b: OB,
) where
    IA: TrInput<u8>,
    OA: TrOutput<u8>,
    IB: TrInput<u8>,
    OB: TrOutput<u8>,
    S: TrSmokeScope + Clone + 'static,
    RT: TrSmokeRt,
{
    run_socket_scenario_with_(
        input_a,
        output_a,
        input_b,
        output_b,
        |a_tx, a_rx, b_tx, b_rx| {
            run_small_mux_scenario_::<_, _, _, _, S, RT>(rt, scope, a_tx, a_rx, b_tx, b_rx)
        },
    )
    .await
}


/// 把一次场景挂在「socket + 调用方驱动的泵 + 全被动环」的传输上。
///
/// 四个泵与场景用 `select` 并发推进（同一任务内轮询，不要求任何类型 `Send`）；
/// 场景完成即丢弃泵 future，从而结束对设备（及其借用的 socket 半边）的借用。
async fn run_socket_scenario_with_<IA, OA, IB, OB, F, Fut>(
    input_a: IA,
    output_a: OA,
    input_b: IB,
    output_b: OB,
    scenario: F,
) where
    IA: TrInput<u8>,
    OA: TrOutput<u8>,
    IB: TrInput<u8>,
    OB: TrOutput<u8>,
    F: FnOnce(
        smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
    ) -> Fut,
    Fut: core::future::Future<Output = ()>,
{
    // 每端两个环：一个承载「socket → smux」（Rx），一个承载「smux → socket」（Tx）。
    let (a_rx_ring_tx, a_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (a_tx, a_tx_ring_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (b_rx_ring_tx, b_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (b_tx, b_tx_ring_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);

    let scenario_fut = scenario(a_tx, a_rx, b_tx, b_rx);
    let pumps_fut = async {
        futures::join!(
            pump_input_(input_a, a_rx_ring_tx),
            pump_output_(a_tx_ring_rx, output_a),
            pump_input_(input_b, b_rx_ring_tx),
            pump_output_(b_tx_ring_rx, output_b),
        )
    };

    futures::pin_mut!(scenario_fut);
    futures::pin_mut!(pumps_fut);
    // 左侧（场景）先被轮询：场景完成即返回；若泵全部退出（连接被提前关闭、
    // 四个泵都走到 EOF），说明场景没能跑完，判为失败。
    match futures::future::select(scenario_fut, pumps_fut).await {
        futures::future::Either::Left(((), _pumps)) => {}
        futures::future::Either::Right((_pumps, _scenario)) => {
            panic!("四个传输泵在场景完成之前全部退出")
        }
    }
}


/// **tokio** 版：建立一条已注册进运行时的 UNIX socket 对、装配好四条调用方驱动的
/// 泵，然后在给定作用域上跑 `scenario`。
///
/// 与下面的 compio / smol 版**逐字同构**，只有「设备类型 + 适配 crate + 怎么拆读写
/// 半边」不同；三个运行时因此共用同一份场景（[`run_smoke_scenario_`] /
/// [`run_small_mux_scenario_`]），不再各写一个测试文件。
#[cfg(feature = "test-tokio-runtime")]
pub async fn run_socket_scenario_on_runtime_<F, Fut>(
    scope: &abs_art_tokio::LocalScope,
    scenario: F,
) where
    F: FnOnce(
        smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
    ) -> Fut,
    Fut: core::future::Future<Output = ()>,
{
    use buffex_tokio_adapt::x_deps::abs_buff_tokio_adapt::{ReadAsInput, WriteAsOutput};
    use tokio::net::UnixStream;

    let (stream_a, stream_b) = UnixStream::pair().expect("建立 tokio UNIX socket 对应当成功");
    let (mut a_read, mut a_write) = stream_a.into_split();
    let (mut b_read, mut b_write) = stream_b.into_split();

    let fut = run_socket_scenario_with_(
        ReadAsInput::new(&mut a_read),
        WriteAsOutput::new(&mut a_write),
        ReadAsInput::new(&mut b_read),
        WriteAsOutput::new(&mut b_write),
        scenario,
    );
    scope.run_until(fut).await;
}


/// **compio** 版：0.19 的 `UnixStream` 没有 `pair()`，因此先建 `std` socket 对再
/// 逐个 `from_std` 注册进当前运行时。其余与 tokio 版逐字同构。
#[cfg(all(
    not(feature = "test-tokio-runtime"),
    not(feature = "test-smol-runtime")
))]
pub async fn run_socket_scenario_on_runtime_<F, Fut>(
    scope: &abs_art_compio::LocalScope,
    scenario: F,
) where
    F: FnOnce(
        smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
    ) -> Fut,
    Fut: core::future::Future<Output = ()>,
{
    use buffex_compio_adapt::{ReadAsInput, WriteAsOutput};

    let (std_a, std_b) =
        std::os::unix::net::UnixStream::pair().expect("建立 std UNIX socket 对应当成功");
    let stream_a =
        compio::net::UnixStream::from_std(std_a).expect("a 端应能注册到 compio 运行时");
    let stream_b =
        compio::net::UnixStream::from_std(std_b).expect("b 端应能注册到 compio 运行时");
    let (mut a_read, mut a_write) = stream_a.into_split();
    let (mut b_read, mut b_write) = stream_b.into_split();

    let fut = run_socket_scenario_with_(
        ReadAsInput::new(&mut a_read),
        WriteAsOutput::new(&mut a_write),
        ReadAsInput::new(&mut b_read),
        WriteAsOutput::new(&mut b_write),
        scenario,
    );
    scope.run_until(fut).await;
}


/// **smol** 版：`async-net` 的 `UnixStream` 没有 `into_split`，因此用
/// [`smol::io::split`]（即 `futures_lite::io::split`）把整条流拆成读写半边——它内部
/// 用一把**短临界区**的互斥锁共享同一条流（`poll_read` / `poll_write` 期间才持锁），
/// 与 tokio / compio 的 `into_split` 在语义上等价：两个半边仍然指向同一条全双工连接。
/// 其余与 tokio 版逐字同构。
#[cfg(feature = "test-smol-runtime")]
pub async fn run_socket_scenario_on_runtime_<F, Fut>(
    scope: &abs_art_smol::LocalScope,
    scenario: F,
) where
    F: FnOnce(
        smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
    ) -> Fut,
    Fut: core::future::Future<Output = ()>,
{
    use buffex_smol_adapt::{ReadAsInput, WriteAsOutput};
    use smol::net::unix::UnixStream;

    let (stream_a, stream_b) = UnixStream::pair().expect("建立 smol UNIX socket 对应当成功");
    let (mut a_read, mut a_write) = smol::io::split(stream_a);
    let (mut b_read, mut b_write) = smol::io::split(stream_b);

    let fut = run_socket_scenario_with_(
        ReadAsInput::new(&mut a_read),
        WriteAsOutput::new(&mut a_write),
        ReadAsInput::new(&mut b_read),
        WriteAsOutput::new(&mut b_write),
        scenario,
    );
    scope.run_until(fut).await;
}
