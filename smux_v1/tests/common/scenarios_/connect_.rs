//! 握手建连：A 端发起、B 端等待，并由交付物建出两个 `MuxConnection`。

use buffex::x_deps::abs_buff::{
    TrBuffRead, TrBuffWrite,
    buffer::{TrConsumerState, TrProducerState},
};
use mm_ptr::x_deps::abs_mm::CoreAlloc;
use smux_v1::{
    connection::{MuxConnection, TrConnCfg},
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
};

use crate::common::{TrSmokeRt, TrSmokeScope};

/// 测试配置的统一构造契约：由**运行时值**造出配置。
///
/// 这是测试侧对上位 trait 的镜像——生产的 [`TrConnCfg`] 从配置里**取**运行时值
/// （`runtime()`），测试则在构造时把它**放**进去。有了它，`connect_pair_` 才能在
/// 「配置类型是类型参数」的同时把 `rt` 喂进去。
pub trait TestConnCfg: TrConnCfg {
    /// 由运行时值构造配置。
    fn new_(rt: Self::Rt) -> Self;
}

impl<W, R, RT> TestConnCfg for crate::common::SmokeMuxConfig<W, R, RT>
where
    W: TrBuffWrite<u8> + TrProducerState + 'static,
    R: TrBuffRead<u8> + TrConsumerState + 'static,
    RT: TrSmokeRt,
{
    fn new_(rt: Self::Rt) -> Self {
        crate::common::SmokeMuxConfig::new(rt)
    }
}

impl<W, R, RT> TestConnCfg for crate::common::FlowCtrlConfig<W, R, RT>
where
    W: TrBuffWrite<u8> + TrProducerState + 'static,
    R: TrBuffRead<u8> + TrConsumerState + 'static,
    RT: TrSmokeRt,
{
    fn new_(rt: Self::Rt) -> Self {
        crate::common::FlowCtrlConfig::new(rt)
    }
}

/// 握手（A 端发起、B 端等待）并由交付物建立两个 [`MuxConnection`]，**协商项取默认值**。
///
/// 抽出来给「收发场景」与「绑定独占性场景」共用，保证两者走的是**同一套**
/// 连接建立路径；`tests/thread_safety.rs` 也直接用它装配连接。
///
/// **连接配置是泛型参数** `C`：默认调用点是 [`SmokeMuxConfig`]，流控验收传
/// [`crate::common::FlowCtrlConfig`]——两者的子流环容量不同，其余策略完全一致。
///
/// **运行时值**（`rt: &CA::Rt`）与**本地作用域**（`scope: &S`）是两个入参：前者用来
/// 构造两端配置（运行时值进配置，`TrConnCfg::Rt`）并提供计时与时刻，后者只作为
/// [`MuxConnection::new`] 的方法级泛型用来投递三个循环。两个配置各拿一份 `rt` 克隆
/// ——克隆只是同一个运行时的两个把手（见 `abs_art` 各后端的类型文档）。
///
/// **没有「连接级帧暂存缓冲」入参**：传输的两条半边（`WA` / `RA` / `WB` / `RB`）
/// 自带缓冲，本函数建连时不再向调用方要任何内存（见 `MuxConnection::new` 的文档）。
///
/// 需要**非默认协商量**的场景（目前只有一个：优雅关闭的超时验收要把
/// `max_channel_timeout` 压到 1 秒）用 [`connect_pair_with_opts_`]；本函数就是它的
/// 默认值转调。
pub async fn connect_pair_<CA, CB, RA, WA, RB, WB, S>(
    rt: &CA::Rt,
    scope: &S,
    tx_a: WA,
    rx_a: RA,
    tx_b: WB,
    rx_b: RB,
) -> (MuxConnection<CA>, MuxConnection<CB>)
where
    // 连接把 Rx / Tx 移交给 `'static` 的读写循环（`spawn_local` 要求 `'static`；
    // 本地投递**不要求** `Send`，因此 `!Send` 的传输也能直接当 `Rx` / `Tx`）。
    //
    // 两条半边还必须是**自带缓冲**的（`TrProducerState` / `TrConsumerState`）：
    // 内侧两个循环直接驱动它们，要靠这两个查询非阻塞地问出「有多少可读 / 可写、
    // 对端是否已经封口」。
    RA: TrBuffRead<u8> + TrConsumerState + 'static,
    WA: TrBuffWrite<u8> + TrProducerState + 'static,
    RB: TrBuffRead<u8> + TrConsumerState + 'static,
    WB: TrBuffWrite<u8> + TrProducerState + 'static,
    S: TrSmokeScope + Clone + 'static,
    // 两端的连接配置各自泛化：默认用例两侧都是 [`SmokeMuxConfig`]，流控用例换成
    // [`crate::common::FlowCtrlConfig`]——其余策略完全一致，避免把「容量」以外的差异
    // 带进对照。
    CA: TrConnCfg<ConnRx = RA, ConnTx = WA, Alloc = CoreAlloc> + TestConnCfg + Clone + 'static,
    CB: TrConnCfg<ConnRx = RB, ConnTx = WB, Alloc = CoreAlloc> + TestConnCfg + Clone + 'static,
    // 两端的运行时值类型必须一致（传进来的 `rt` 同时喂给两个配置），且必须能
    // 交出本地作用域（连接自己取）。
    CA::Rt: TrSmokeRt + abs_art_facade::ScopeHost,
    CB: TrConnCfg<Rt = CA::Rt>,
{
    connect_pair_with_opts_::<CA, CB, RA, WA, RB, WB, S>(
        rt,
        scope,
        tx_a,
        rx_a,
        tx_b,
        rx_b,
        BasicOpts::default(),
        BasicOpts::default(),
    )
    .await
}

/// 与 [`connect_pair_`] 相同，但**两端握手用的协商项由调用方给定**。
///
/// # 为什么需要它
///
/// 优雅关闭（`TrConnection::close_async`）的等待上限取协商项
/// [`BasicOpts::max_channel_timeout`]，默认 30 秒。验收「对端始终不回应 ⇒ 超时走
/// 强制路径并返回 `false`」这条分支时，必须把该量压到 1 秒，否则单个用例要等半分钟。
///
/// 两个协商项分别给发起方（`invite_opts`）与等待方（`listen_opts`）：本仓的用例总是
/// 传同一份值，但形状上不合并——两端本来就可以各自提出自己的偏好。
pub async fn connect_pair_with_opts_<CA, CB, RA, WA, RB, WB, S>(
    rt: &CA::Rt,
    scope: &S,
    tx_a: WA,
    rx_a: RA,
    tx_b: WB,
    rx_b: RB,
    invite_opts: BasicOpts,
    listen_opts: BasicOpts,
) -> (MuxConnection<CA>, MuxConnection<CB>)
where
    RA: TrBuffRead<u8> + TrConsumerState + 'static,
    WA: TrBuffWrite<u8> + TrProducerState + 'static,
    RB: TrBuffRead<u8> + TrConsumerState + 'static,
    WB: TrBuffWrite<u8> + TrProducerState + 'static,
    S: TrSmokeScope + Clone + 'static,
    CA: TrConnCfg<ConnRx = RA, ConnTx = WA, Alloc = CoreAlloc> + TestConnCfg + Clone + 'static,
    CB: TrConnCfg<ConnRx = RB, ConnTx = WB, Alloc = CoreAlloc> + TestConnCfg + Clone + 'static,
    CA::Rt: TrSmokeRt + abs_art_facade::ScopeHost,
    CB: TrConnCfg<Rt = CA::Rt>,
{
    let invite_fut = HandshakeAgent::new(tx_a, rx_a).invite_async(&invite_opts, AcceptAllEntries);
    let listen_fut = HandshakeAgent::new(tx_b, rx_b).listen_async(&listen_opts, AcceptAllEntries);
    let (invited, accepted) = futures::join!(async { invite_fut.await }, async { listen_fut.await });
    let delivery_a = invited.expect("发起方握手应当成功");
    let delivery_b = accepted.expect("等待方握手应当成功");

    // 配置由 `rt` 构造：运行时值进了配置（`TrConnCfg::Rt`），因此连接的类型参数
    // 只剩 `C` 一个——`rt.clone()` 只是同一个运行时的又一个把手。
    let config_a = CA::new_(rt.clone());
    let config_b = CB::new_(rt.clone());
    // 运行时值与作用域都由**配置**解决（`TrConnCfg::runtime` + `ScopeHost`），
    // 因此这里不再有运行时/作用域入参；`scope` 仅在自动取作用域不可用时才需要
    // 走 `new_with_rt`（本测试路径不需要）。
    let _ = scope;
    (
        MuxConnection::new(delivery_a, config_a),
        MuxConnection::new(delivery_b, config_b),
    )
}
