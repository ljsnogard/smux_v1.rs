//! **smol** 后端的入向适配：TCP 与 UNIX 域。
//!
//! # 这一层做什么
//!
//! | 类型 | 接受入口 | 拆分入口 |
//! | --- | --- | --- |
//! | [`TcpTransport`] | `smol::net::TcpListener::accept`（即 `async-net`） | `smol::io::split` |
//! | [`UdsTransport`] | `smol::net::UnixListener::accept` | `smol::io::split` |
//!
//! 每条连接产出两块 `buffex_smol_adapt` 的环适配器（[`BuffRead`] + [`BuffWrite`]）。
//!
//! # 写半边为什么用**自驱动**形态
//!
//! 理由与 tokio 侧逐字相同：`MuxConnection` 只把适配器当 `TrBuffWrite` 用，从不调
//! `flush_async`，按需形态下最后一段会永远留在环里。`try_new_local` 把泵投到本线程的
//! [`TrLocalScope`](smux_v1::x_deps::abs_art::TrLocalScope)，因此构造点必须落在
//! `scope.run_until(..)` 之类的本地驱动栈内。
//!
//! # 设备的 trait 是这一族
//!
//! smol 自己不做 IO：`smol::io` 就是 `futures_lite::io` 的重导出，`async-net` 的
//! `TcpStream` / `UnixStream` 实现的也是这一族 trait。因此 `smol::io::split` 对两者都
//! 成立，TCP 与 UNIX 域共用同一份拆分逻辑。

use buffex_smol_adapt::{
    BuffRead, BuffWrite, DefaultAllocConfig, SharedRingOf,
};
use futures_lite::{io::ReadHalf, io::WriteHalf};
use async_net::{
    TcpListener as SmolTcpListener, TcpStream as SmolTcpStream,
    unix::{UnixListener as SmolUnixListener, UnixStream as SmolUnixStream},
};

use crate::config::{AcceptOutput, InboundError, TrInbound};
use smux_v1::connection::{DefaultConnCfg, TrConnCfg};

/// 两个方向传输环共用的缺省容量：64 KiB。
///
/// 取值理由与 tokio 侧的同名常量一致（见 `runtime::tokio` 的文档）。
pub const K_DEFAULT_RING_CAPACITY: usize = 64usize * 1024usize;

/// TCP 读半边：设备 → 环。
pub type TcpRx = BuffRead<
    ReadHalf<SmolTcpStream>,
    DefaultAllocConfig,
    SharedRingOf<DefaultAllocConfig>,
>;

/// TCP 写半边：环 → 设备。
pub type TcpTx = BuffWrite<
    WriteHalf<SmolTcpStream>,
    DefaultAllocConfig,
    SharedRingOf<DefaultAllocConfig>,
>;

/// UNIX 域读半边：设备 → 环。
pub type UdsRx = BuffRead<
    ReadHalf<SmolUnixStream>,
    DefaultAllocConfig,
    SharedRingOf<DefaultAllocConfig>,
>;

/// UNIX 域写半边：环 → 设备。
pub type UdsTx = BuffWrite<
    WriteHalf<SmolUnixStream>,
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
    abs_art_smol::Runtime,
>;

/// 本后端 **UNIX 域**连接的资源策略类型；理由见 [`TcpConnCfg`]。
pub type UdsConnCfg = DefaultConnCfg<
    UdsTx,
    UdsRx,
    smux_v1::metrics::NoMetrics,
    smux_v1::flow_ctrl::DefaultPolicy,
    abs_art_smol::Runtime,
>;

/// 任意写设备挂上环之后的类型（本模块所有写半边都是它的实例）。
pub type HeldTx<W> = BuffWrite<W, DefaultAllocConfig, SharedRingOf<DefaultAllocConfig>>;

/// 任意读设备挂上环之后的类型（本模块所有读半边都是它的实例）。
pub type HeldRx<R> = BuffRead<R, DefaultAllocConfig, SharedRingOf<DefaultAllocConfig>>;

/// **TCP** 的入向适配：**持有**一个 `smol::net::TcpListener`。
///
/// 适配器持有监听套接字（而不是让 listener 单独拿着它）是因为接受与建环都属于本 trait
/// 的职责；`SmuxListener` 用 `Arc<Self>` 共享同一个实例给所有接受槽位。
pub struct TcpTransport {
    /// 底层的 `async-net` TCP 监听套接字。
    listener_: SmolTcpListener,
}

impl TcpTransport {
    /// 由已绑定的监听套接字构造。
    pub const fn new(listener: SmolTcpListener) -> Self {
        TcpTransport { listener_: listener }
    }

    /// 取底层监听套接字。
    pub const fn listener(&self) -> &SmolTcpListener {
        &self.listener_
    }
}

impl TrInbound for TcpTransport {
    type ConnCfg = TcpConnCfg;
    type Device = SmolTcpStream;
    type Error = InboundError;

    const RING_CAPACITY: (usize, usize) =
        (K_DEFAULT_RING_CAPACITY, K_DEFAULT_RING_CAPACITY);

    async fn accept_async(&self) -> AcceptOutput<Self> {
        let (stream, _peer) = self.listener_.accept().await?;
        // 与 tokio 侧同理：Nagle 对 smux 的小帧不利，但设置失败不值得放弃连接。
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
        let (rd, wr) = smol::io::split(device);
        make_halves_(rd, wr)
    }
}

/// **UNIX 域**的入向适配：**持有**一个 `smol::net::UnixListener`。
///
/// 形态与 [`TcpTransport`] 逐字一致，理由见它的文档。
pub struct UdsTransport {
    /// 底层的 `async-net` UNIX 域监听套接字。
    listener_: SmolUnixListener,
}

impl UdsTransport {
    /// 由已绑定的监听套接字构造。
    pub const fn new(listener: SmolUnixListener) -> Self {
        UdsTransport { listener_: listener }
    }

    /// 取底层监听套接字。
    pub const fn listener(&self) -> &SmolUnixListener {
        &self.listener_
    }
}

impl TrInbound for UdsTransport {
    type ConnCfg = UdsConnCfg;
    type Device = SmolUnixStream;
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
        let (rd, wr) = smol::io::split(device);
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
    R: futures_lite::io::AsyncRead + Send + Unpin + 'static,
    W: futures_lite::io::AsyncWrite + Send + Unpin + 'static,
{
    let rx = BuffRead::<_, DefaultAllocConfig>::try_new(
        rd,
        K_DEFAULT_RING_CAPACITY,
        DefaultAllocConfig,
    )
    .expect("传输环容量必须是 buffex::ring 允许的取值");
    // 自驱动形态：泵投到本线程队列，因此调用点必须在本地驱动栈内（见模块文档）。
    let tx = BuffWrite::<_, DefaultAllocConfig>::try_new_local(
        wr,
        K_DEFAULT_RING_CAPACITY,
        DefaultAllocConfig,
    )
    .expect("传输环容量必须是 buffex::ring 允许的取值");
    (tx, rx)
}
