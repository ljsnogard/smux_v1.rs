//! **tokio** 后端的入向适配：TCP 与 UNIX 域。
//!
//! # 这一层做什么
//!
//! | 类型 | 接受入口 | 拆分入口 |
//! | --- | --- | --- |
//! | [`TcpTransport`] | `tokio::net::TcpListener::accept` | `tokio::io::split` |
//! | [`UdsTransport`] | `tokio::net::UnixListener::accept` | `tokio::io::split` |
//!
//! 每条连接产出两块 `buffex_tokio_adapt` 的环适配器（[`BuffRead`] + [`BuffWrite`]）。
//! 连接配置里的 `ConnTx` / `ConnRx` 必须写成 [`TcpTx`] / [`TcpRx`]（或 UDS 那两个），
//! 否则与 listener 对不上——对不上就编译不过。
//!
//! # 写半边为什么用**自驱动**形态
//!
//! `buffex_tokio_adapt::BuffWrite` 的「环 → 设备」有两种驱动方式（见该 crate 文档）：
//! 按需（谁记得谁调 `flush_async`）与自驱动（构造时把泵投到本线程 `LocalSet`）。这里取
//! **自驱动**：`MuxConnection` 只把适配器当 `TrBuffWrite` 用，从不调 `flush_async`，
//! 按需形态下最后一段会永远留在环里。
//!
//! 代价是构造 [`BuffWrite`] 的调用点必须落在
//! [`TrLocalScope::run_until`](smux_v1::x_deps::abs_art::TrLocalScope::run_until) 之类的
//! 本地驱动栈内。`SmuxListener` 满足这一点：建环就发生在接受槽位里。

use buffex_tokio_adapt::{
    BuffRead, BuffWrite, DefaultAllocConfig, SharedRingOf,
};
use tokio::{
    io::{ReadHalf, WriteHalf},
    net::{
        TcpListener as TokioTcpListener, TcpStream as TokioTcpStream,
        UnixListener as TokioUnixListener, UnixStream as TokioUnixStream,
    },
};

use crate::config::{AcceptOutput, InboundError, TrInbound};
use smux_v1::connection::{DefaultConnCfg, TrConnCfg};

/// 两个方向传输环共用的缺省容量：64 KiB。
///
/// `MuxConnection` 的**连接级帧暂存环**之外，传输侧还各有一个环（由适配器持有），两者
/// 职责不同：帧暂存环决定「一次能吞下多少帧」，传输环决定「一次能借出多长的段」。这里与
/// `smux_v1` 的
/// [`K_STAGE_RING_CAPACITY`](smux_v1::connection::K_STAGE_RING_CAPACITY) 同量级。
pub const K_DEFAULT_RING_CAPACITY: usize = 64usize * 1024usize;

/// TCP 读半边：设备 → 环。
pub type TcpRx = BuffRead<
    ReadHalf<TokioTcpStream>,
    DefaultAllocConfig,
    SharedRingOf<DefaultAllocConfig>,
>;

/// TCP 写半边：环 → 设备。
pub type TcpTx = BuffWrite<
    WriteHalf<TokioTcpStream>,
    DefaultAllocConfig,
    SharedRingOf<DefaultAllocConfig>,
>;

/// UNIX 域读半边：设备 → 环。
pub type UdsRx = BuffRead<
    ReadHalf<TokioUnixStream>,
    DefaultAllocConfig,
    SharedRingOf<DefaultAllocConfig>,
>;

/// UNIX 域写半边：环 → 设备。
pub type UdsTx = BuffWrite<
    WriteHalf<TokioUnixStream>,
    DefaultAllocConfig,
    SharedRingOf<DefaultAllocConfig>,
>;

/// 本后端 **TCP** 连接的资源策略类型。
///
/// # 为什么把它定成具体类型、而不是让 [`TrInbound::ConnCfg`] 用 `smux_v1` 的缺省值
///
/// `DefaultConnCfg<Tx, Rx>` 的第五个泛型参数（运行时值）缺省取
/// `smux_v1` 的 `DefaultRt_`，而那个类型由 **`smux_v1` 自己的 `test-*-runtime`
/// feature** 决定（缺省 compio）。若把它当作 [`TrInbound::ConnCfg`]，tokio 装配下
/// 就会得到「连接的运行时值是 `CompioRuntime`」这种自相矛盾的组合，用户还必须让
/// `TrListenerConfig::Rt` 跟着写 `CompioRuntime`。
///
/// 这里把它**钉死成本后端的运行时值**，于是类型链自洽：
/// `TcpTransport::ConnCfg::Rt == abs_art_tokio::Runtime`，用户的
/// `TrListenerConfig::Rt` 也必须是它——运行时选择由「选了哪个 `TcpTransport` /
/// `UdsTransport`」决定，
/// 而不是由 feature 决定。
pub type TcpConnCfg = DefaultConnCfg<
    TcpTx,
    TcpRx,
    smux_v1::metrics::NoMetrics,
    smux_v1::flow_ctrl::DefaultPolicy,
    abs_art_tokio::Runtime,
>;

/// 本后端 **UNIX 域**连接的资源策略类型；理由见 [`TcpConnCfg`]。
pub type UdsConnCfg = DefaultConnCfg<
    UdsTx,
    UdsRx,
    smux_v1::metrics::NoMetrics,
    smux_v1::flow_ctrl::DefaultPolicy,
    abs_art_tokio::Runtime,
>;

/// 任意写设备挂上环之后的类型（本模块所有写半边都是它的实例）。
pub type HeldTx<W> = BuffWrite<W, DefaultAllocConfig, SharedRingOf<DefaultAllocConfig>>;

/// 任意读设备挂上环之后的类型（本模块所有读半边都是它的实例）。
pub type HeldRx<R> = BuffRead<R, DefaultAllocConfig, SharedRingOf<DefaultAllocConfig>>;

/// **TCP** 的入向适配：**持有**一个 `tokio::net::TcpListener`。
///
/// 适配器持有监听套接字（而不是让 listener 单独拿着它）是因为接受与建环都属于本 trait
/// 的职责；`SmuxListener` 用 `Arc<Self>` 共享同一个实例给所有接受槽位。
pub struct TcpTransport {
    /// 底层的 tokio TCP 监听套接字。
    listener_: TokioTcpListener,
}

impl TcpTransport {
    /// 由已绑定的监听套接字构造。
    pub const fn new(listener: TokioTcpListener) -> Self {
        TcpTransport { listener_: listener }
    }

    /// 取底层监听套接字。
    pub const fn listener(&self) -> &TokioTcpListener {
        &self.listener_
    }
}

impl TrInbound for TcpTransport {
    type ConnCfg = TcpConnCfg;
    type Device = TokioTcpStream;
    type Error = InboundError;

    const RING_CAPACITY: (usize, usize) =
        (K_DEFAULT_RING_CAPACITY, K_DEFAULT_RING_CAPACITY);

    async fn accept_async(&self) -> AcceptOutput<Self> {
        let (stream, _peer) = self.listener_.accept().await?;
        // Nagle 会把「小回执跟在数据末尾」多拖一轮，而 smux 的帧普遍很小。这里失败
        // **不**拒绝连接：TCP_NODELAY 只影响时延，不值得为它放弃一条已建好的连接。
        let _ = stream.set_nodelay(true);
        Result::Ok(Option::Some(stream))
    }

    fn into_halves(
        &self,
        device: Self::Device,
    ) -> (
        <Self::ConnCfg as TrConnCfg>::ConnTx,
        <Self::ConnCfg as TrConnCfg>::ConnRx,
    ) {
        let (rd, wr) = tokio::io::split(device);
        make_halves_(rd, wr)
    }
}

/// **UNIX 域**的入向适配：**持有**一个 `tokio::net::UnixListener`。
///
/// 形态与 [`TcpTransport`] 逐字一致，理由见它的文档。
pub struct UdsTransport {
    /// 底层的 tokio UNIX 域监听套接字。
    listener_: TokioUnixListener,
}

impl UdsTransport {
    /// 由已绑定的监听套接字构造。
    pub const fn new(listener: TokioUnixListener) -> Self {
        UdsTransport { listener_: listener }
    }

    /// 取底层监听套接字。
    pub const fn listener(&self) -> &TokioUnixListener {
        &self.listener_
    }
}

impl TrInbound for UdsTransport {
    type ConnCfg = UdsConnCfg;
    type Device = TokioUnixStream;
    type Error = InboundError;

    const RING_CAPACITY: (usize, usize) =
        (K_DEFAULT_RING_CAPACITY, K_DEFAULT_RING_CAPACITY);

    async fn accept_async(&self) -> AcceptOutput<Self> {
        let (stream, _peer) = self.listener_.accept().await?;
        Result::Ok(Option::Some(stream))
    }

    fn into_halves(
        &self,
        device: Self::Device,
    ) -> (
        <Self::ConnCfg as TrConnCfg>::ConnTx,
        <Self::ConnCfg as TrConnCfg>::ConnRx,
    ) {
        let (rd, wr) = tokio::io::split(device);
        make_halves_(rd, wr)
    }
}

/// 把两条设备半边各接上一个环。
///
/// # Panics
///
/// 环容量不在 `buffex::ring` 允许区间内时 panic。容量是编译期常量
/// [`K_DEFAULT_RING_CAPACITY`]，因此这是**实现错误**，不是运行期输入问题。
fn make_halves_<R, W>(rd: R, wr: W) -> (HeldTx<W>, HeldRx<R>)
where
    R: tokio::io::AsyncRead + Send + Unpin + 'static,
    W: tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    let rx = BuffRead::<_, DefaultAllocConfig>::try_new(
        rd,
        K_DEFAULT_RING_CAPACITY,
        DefaultAllocConfig,
    )
    .expect("传输环容量必须是 buffex::ring 允许的取值");
    // 自驱动形态：泵投到本线程 `LocalSet`，因此调用点必须在本地驱动栈内（见模块文档）。
    let tx = BuffWrite::<_, DefaultAllocConfig>::try_new_local(
        wr,
        K_DEFAULT_RING_CAPACITY,
        DefaultAllocConfig,
    )
    .expect("传输环容量必须是 buffex::ring 允许的取值");
    (tx, rx)
}
