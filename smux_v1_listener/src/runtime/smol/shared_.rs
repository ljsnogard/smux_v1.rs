use async_net::{
    TcpStream as SmolTcpStream,
    unix::UnixStream as SmolUnixStream,
};
use buffex_smol_adapt::{
    BuffRead, BuffWrite, DefaultAllocConfig, SharedRingOf,
};
use futures_lite::{io::ReadHalf, io::WriteHalf};

use smux_v1::connection::DefaultConnCfg;

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
/// # 为什么把它定成具体类型、而不是让 [`TrInbound::ConnCfg`](crate::config::TrInbound::ConnCfg) 用 `smux_v1` 的缺省值
///
/// `DefaultConnCfg<Tx, Rx>` 的第五个泛型参数（运行时值）缺省取
/// `smux_v1` 的 `DefaultRt_`，而那个类型由 **`smux_v1` 自己的 `test-*-runtime`
/// feature** 决定（缺省 compio）。若把它当作 [`TrInbound::ConnCfg`](crate::config::TrInbound::ConnCfg)，tokio 装配下
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

/// 把两条设备半边各接上一个环。
///
/// # Panics
///
/// 环容量不在 `buffex::ring` 允许区间内时 panic。容量是编译期常量
/// [`K_DEFAULT_RING_CAPACITY`]，因此这是**实现错误**，不是运行期输入问题。
pub(super) fn make_halves_<R, W>(rd: R, wr: W) -> (HeldTx<W>, HeldRx<R>)
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
