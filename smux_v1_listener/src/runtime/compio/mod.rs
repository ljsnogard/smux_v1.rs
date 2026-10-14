//! **compio** 后端的入向适配：TCP 与 UNIX 域。
//!
//! # 这一层做什么
//!
//! | 类型 | 接受入口 | 拆分入口 |
//! | --- | --- | --- |
//! | [`TcpTransport`] | `compio::net::TcpListener::accept` | `TcpStream::into_split` |
//! | [`UdsTransport`] | `compio::net::UnixListener::accept` | `UnixStream::into_split` |
//!
//! 每条连接产出两块 `buffex_compio_adapt` 的环适配器（[`BuffRead`] + [`BuffWrite`]）。
//!
//! # 与 tokio / smol 侧的两点不同
//!
//! 1. **拆分**：compio 的 `into_split` 是固有方法，把一条流拆成**两个同类型的流**
//!    （`(stream, stream)`），而不是「读半边 + 写半边」两个不同类型。因此本模块的
//!    [`TcpTx`] 与 [`TcpRx`] 形态相同，只是环的方向不同。
//! 2. **构造前提**：`buffex_compio_adapt` 的 `try_new` 必须在**正在运行的 compio
//!    runtime 内**调用（它要 `compio::runtime::spawn` 后台泵），否则返回
//!    `TryNewError::NotInRuntime`。`SmuxListener` 的接受槽位本身就跑在该运行时的本地
//!    队列上，因此成立；但在运行时之外手工调
//!    [`into_halves`](crate::config::TrInbound::into_halves) 会 panic（见下面的
//!    `expect`）。
//!
//! 这一侧不区分「按需 / 自驱动」：compio 的 `try_new` 本来就 `spawn` 后台泵（它的
//! `spawn` 不要求 `Send`，而 tokio 的多线程 `spawn` 要求），因此 [`BuffWrite`] 只有
//! 一个构造入口。

use buffex_compio_adapt::{
    BuffRead, BuffWrite, DefaultAllocConfig, SharedRingOf,
};
use compio::net::{
    TcpListener as CompioTcpListener, TcpStream as CompioTcpStream,
    UnixListener as CompioUnixListener, UnixStream as CompioUnixStream,
};

use crate::config::{AcceptOutput, InboundError, TrInbound};
use smux_v1::connection::{DefaultConnCfg, TrConnCfg};

/// 两个方向传输环共用的缺省容量：64 KiB。
///
/// 取值理由与 tokio 侧的同名常量一致（见 `runtime::tokio` 的文档）。
pub const K_DEFAULT_RING_CAPACITY: usize = 64usize * 1024usize;

/// TCP 读半边：设备 → 环。
pub type TcpRx = BuffRead<
    CompioTcpStream,
    DefaultAllocConfig,
    SharedRingOf<DefaultAllocConfig>,
>;

/// TCP 写半边：环 → 设备。
pub type TcpTx = BuffWrite<
    CompioTcpStream,
    DefaultAllocConfig,
    SharedRingOf<DefaultAllocConfig>,
>;

/// UNIX 域读半边：设备 → 环。
pub type UdsRx = BuffRead<
    CompioUnixStream,
    DefaultAllocConfig,
    SharedRingOf<DefaultAllocConfig>,
>;

/// UNIX 域写半边：环 → 设备。
pub type UdsTx = BuffWrite<
    CompioUnixStream,
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
    abs_art_compio::Runtime,
>;

/// 本后端 **UNIX 域**连接的资源策略类型；理由见 [`TcpConnCfg`]。
pub type UdsConnCfg = DefaultConnCfg<
    UdsTx,
    UdsRx,
    smux_v1::metrics::NoMetrics,
    smux_v1::flow_ctrl::DefaultPolicy,
    abs_art_compio::Runtime,
>;

/// 任意写设备挂上环之后的类型（本模块所有写半边都是它的实例）。
pub type HeldTx<W> = BuffWrite<W, DefaultAllocConfig, SharedRingOf<DefaultAllocConfig>>;

/// 任意读设备挂上环之后的类型（本模块所有读半边都是它的实例）。
pub type HeldRx<R> = BuffRead<R, DefaultAllocConfig, SharedRingOf<DefaultAllocConfig>>;

/// **TCP** 的入向适配：**持有**一个 `compio::net::TcpListener`。
///
/// 适配器持有监听套接字（而不是让 listener 单独拿着它）是因为接受与建环都属于本 trait
/// 的职责；`SmuxListener` 用 `Arc<Self>` 共享同一个实例给所有接受槽位。
pub struct TcpTransport {
    /// 底层的 compio TCP 监听套接字。
    listener_: CompioTcpListener,
}

impl TcpTransport {
    /// 由已绑定的监听套接字构造。
    pub const fn new(listener: CompioTcpListener) -> Self {
        TcpTransport { listener_: listener }
    }

    /// 取底层监听套接字。
    pub const fn listener(&self) -> &CompioTcpListener {
        &self.listener_
    }
}

impl TrInbound for TcpTransport {
    type ConnCfg = TcpConnCfg;
    type Device = CompioTcpStream;
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
        // compio 的 `into_split` 交出 `(写侧, 读侧)`，与全 crate 的 `(tx, rx)` 顺序一致。
        let (tx_dev, rx_dev) = device.into_split();
        make_halves_(rx_dev, tx_dev)
    }
}

/// **UNIX 域**的入向适配：**持有**一个 `compio::net::UnixListener`。
///
/// 形态与 [`TcpTransport`] 逐字一致，理由见它的文档。
pub struct UdsTransport {
    /// 底层的 compio UNIX 域监听套接字。
    listener_: CompioUnixListener,
}

impl UdsTransport {
    /// 由已绑定的监听套接字构造。
    pub const fn new(listener: CompioUnixListener) -> Self {
        UdsTransport { listener_: listener }
    }

    /// 取底层监听套接字。
    pub const fn listener(&self) -> &CompioUnixListener {
        &self.listener_
    }
}

impl TrInbound for UdsTransport {
    type ConnCfg = UdsConnCfg;
    type Device = CompioUnixStream;
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
        let (tx_dev, rx_dev) = device.into_split();
        make_halves_(rx_dev, tx_dev)
    }
}

/// 把两条设备半边各接上一个环。
///
/// # Panics
///
/// 两种情况都会 panic，且都属于**实现 / 使用错误**而不是运行期输入问题：
///
/// - 环容量不在 `buffex::ring` 允许区间内（容量是编译期常量
///   [`K_DEFAULT_RING_CAPACITY`]）；
/// - 调用点不在 compio runtime 内——`try_new` 要 `spawn` 后台泵，做不到就是
///   `TryNewError::NotInRuntime`。
fn make_halves_<R, W>(rd: R, wr: W) -> (HeldTx<W>, HeldRx<R>)
where
    R: compio::io::AsyncRead + 'static,
    W: compio::io::AsyncWrite + 'static,
{
    let rx = BuffRead::<_, DefaultAllocConfig>::try_new(
        rd,
        K_DEFAULT_RING_CAPACITY,
        DefaultAllocConfig,
    )
    .expect("传输环容量须合法，且调用点必须在 compio runtime 内");
    let tx = BuffWrite::<_, DefaultAllocConfig>::try_new(
        wr,
        K_DEFAULT_RING_CAPACITY,
        DefaultAllocConfig,
    )
    .expect("传输环容量须合法，且调用点必须在 compio runtime 内");
    (tx, rx)
}
