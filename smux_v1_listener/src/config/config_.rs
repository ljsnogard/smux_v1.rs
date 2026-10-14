//! 入向适配的契约与 listener 的策略参数。
//!
//! 本模块只放**契约**：三套运行时的具体适配在 [`crate::runtime`]，而「用哪个运行时值 /
//! 哪条本地队列 / 哪种帧暂存缓冲」由使用者在自己的 crate 里给出。
//!
//! # 三个 trait 的分工
//!
//! [`SmuxListener`](crate::listening::SmuxListener) 的四个类型参数里有三个来自本模块：
//!
//! | 参数 | 契约 | 回答什么 |
//! | --- | --- | --- |
//! | `Cfg` | [`TrListenerConfig`] | 握手条件、连接用哪套资源策略、帧暂存缓冲从哪来 |
//! | `A` | [`TrInbound`] | 从哪个 socket 接受连接、怎么把它接上传输环 |
//! | `N` | [`TrNegotiatorFactory`] | 每次握手交给谁去裁决 |
//!
//! 拆开的理由是它们的变化**互相独立**：换传输只换 `A`，换协商策略只换 `N`，换缓冲区
//! 来源只动 `Cfg` 的一个方法。

use core::mem::MaybeUninit;

use gen_mcf2::gen_may_cancel_future;
use smux_v1::{
    connection::{MuxChanBuffOwnedBy, TrConnCfg},
    handshake::{
        agent::TrNegotiator,
        opts::{BasicOpts, NegotiationEntry},
    },
    time::TrTime,
    x_deps::abs_buff::{TrBuffRead, TrBuffWrite},
};

use crate::x_deps::abs_cancel::TrCancellationToken;

/// 连接级帧暂存缓冲的拥有者类型：一块 `MaybeUninit<u8>` 存储 + `smux_v1` 内部结构同款的
/// 分配器。
///
/// 它是 [`TrListenerConfig::StageBuff`] 的**固定**取值，理由见那里的文档。
pub type CoreAllocOwned_ = smux_v1::x_deps::mm_ptr::Owned<
    [MaybeUninit<u8>],
    smux_v1::x_deps::abs_mm::CoreAlloc,
>;

/// 造一对连接级帧暂存缓冲的便捷函数：容量都取 `cap`，分配器取 [`CoreAllocOwned_`] 的
/// 那一个。
///
/// 它是给 [`TrListenerConfig::make_stage_buffs`] 的实现者用的糖，避免每个实现都手写
/// `Owned::new_uninit_slice`。
///
/// # Panics
///
/// 分配失败时 panic（`Owned::new_uninit_slice` 的行为）。
pub fn make_stage_buffs(
    cap: usize,
) -> (
    MuxChanBuffOwnedBy<CoreAllocOwned_>,
    MuxChanBuffOwnedBy<CoreAllocOwned_>,
) {
    use smux_v1::x_deps::abs_mm::CoreAlloc;
    (
        MuxChanBuffOwnedBy::new(smux_v1::x_deps::mm_ptr::Owned::new_uninit_slice(
            cap, CoreAlloc,
        )),
        MuxChanBuffOwnedBy::new(smux_v1::x_deps::mm_ptr::Owned::new_uninit_slice(
            cap, CoreAlloc,
        )),
    )
}

/// **能接受入向连接、并把设备接上传输环**。
///
/// 一个实现回答三件事：在哪接受、接受来的是什么、它怎么变成两条 `abs_buff` 半边。
/// 三套运行时 × 两种传输因此各有实现（见 [`crate::runtime`]）：
///
/// | 类型 | 接受入口 | 拆分入口 |
/// | --- | --- | --- |
/// | `runtime::tokio::TcpTransport` | `tokio::net::TcpListener::accept` | `tokio::io::split` |
/// | `runtime::tokio::UdsTransport` | `tokio::net::UnixListener::accept` | `tokio::io::split` |
/// | `runtime::compio::TcpTransport` | `compio::net::TcpListener::accept` | `TcpStream::into_split` |
/// | `runtime::compio::UdsTransport` | `compio::net::UnixListener::accept` | `UnixStream::into_split` |
/// | `runtime::smol::TcpTransport` | `smol::net::TcpListener::accept` | `smol::io::split` |
/// | `runtime::smol::UdsTransport` | `smol::net::UnixListener::accept` | `smol::io::split` |
///
/// # 接受语义
///
/// [`accept_async`](TrInbound::accept_async) 返回
/// `Result<Option<Device>, Self::Error>`：
///
/// - `Ok(Some(dev))`：拿到一条连接。**必须**在这里做完设备级的一次性配置
///   （例如 TCP 的 `set_nodelay(true)`），因为设备随即会被接上环、交给连接层；
/// - `Ok(None)`：本次没有连接，可以立刻重试。接受槽位把它当作「空手而归」处理并继续
///   等待，因此实现**不得**用忙循环反复返回它；
/// - `Err(..)`：接受失败（例如 `EMFILE`）。它以
///   `SmuxListenerError::Accept` 的形式由
///   [`next_async`](crate::listening::SmuxListener) 交给调用方——这是
///   **致命**信号：listener 自己已经产不出连接了。
///
/// # 为什么把三件事收在一个 trait 里
///
/// 它们对同一个类型**总是同时**被实现（TCP 与 UDS 的拆分方式往往逐字相同，但接受入口
/// 不同），拆成多个 trait 只会让 [`SmuxListener`](crate::listening::SmuxListener) 多出
/// 几个永远一起出现的类型参数。代价是「同一个设备类型只能有一种拆法」——需要第二种时
/// 新写一个包装类型即可。
/// 见 [`TrInbound::into_halves`]：名字沿用「设备 → 两条半边」的语义，
/// 但它**不消耗** `self`（适配器要一直持有监听套接字），因此显式放行命名 lint。
#[allow(clippy::wrong_self_convention)]
pub trait TrInbound {
    /// **本适配配套的连接配置类型**（例如
    /// `DefaultConnCfg<TcpTx, TcpRx>`）。
    ///
    /// 它是整条类型链的枢纽：listener 在
    /// [`TrListenerConfig::Conn`] 上取同一条等式约束（`A: TrInbound<ConnCfg = Cfg::Conn>`），
    /// 于是
    ///
    /// - 两条半边就是 `<A::ConnCfg as TrConnCfg>::ConnTx` / `ConnRx`；
    /// - 它们与 `MuxConnection<Cfg::Conn>` 要的半边**是同一个类型**，不需要任何转换；
    /// - 而 `ConnCfg: TrConnCfg` 自带的 `ConnTx: TrBuffWrite<u8>` 约束顺带保证了
    ///   `into_halves` 的返回值可以直接喂给握手。
    ///
    /// # 为什么不另设一组 `Tx` / `Rx` 关联类型
    ///
    /// 试过 `trait TrInbound { type Rx; type Tx; type ConnCfg: TrConnCfg<ConnTx = Self::Tx, …>; }`：
    /// **编译得过，但等式推导会失效**——`MuxConnection<Cfg::Conn>` 要的是
    /// `<Cfg::Conn as TrConnCfg>::ConnTx`，而适配器交出的是 `A::Tx`，编译器不认为两者
    /// 相等（实测确认）。改成「半边只在 `TrConnCfg` 上定义一次、`TrInbound` 交出整套
    /// 连接配置」之后，两侧是同一个投影，等式正常传递。
    type ConnCfg: TrConnCfg;

    /// 接受操作交出的**入向设备**（尚未接上环）。
    /// 接受操作交出的**入向设备**（尚未接上环）。
    ///
    /// 约束里的 `Send` 来自 listener 的实现方式：接受槽位与握手任务都要能被投递到本地
    /// 作用域，并在 `next_async` 的 future 里存活，因此设备必须能跨线程传递。
    type Device: 'static;

    /// 接受失败的错误类型。
    ///
    /// 要求 `Clone`：listener 会把一个「已关闭」错误值预存进等待器，取消发生时把它交出去。
    type Error: core::error::Error + Clone + 'static;

    /// 两条传输环的容量：`(读环, 写环)`。
    ///
    /// 它是**关联常量**而不是字段：容量在 [`TrInbound::into_halves`] 建环时用到，而
    /// 使用者需要在自己的连接配置里写死对应的环类型，因此容量必须是类型层面可知的
    /// 事实，不能等到运行期。
    const RING_CAPACITY: (usize, usize);

    /// 接受一条入向连接。语义见 trait 文档。
    fn accept_async(&self) -> impl core::future::Future<Output = AcceptOutput<Self>>;

    /// 把设备接上两条传输环，交出 `(tx, rx)`。
    ///
    /// 参数顺序与全 `smux_v1` 一致：**成对的收发一律 `(tx, rx)`**。
    ///
    /// # Panics
    ///
    /// 环容量非法时（不在 `buffex::ring` 允许区间内）panic。容量是
    /// [`TrInbound::RING_CAPACITY`] 这个编译期常量，因此这是**实现错误**，不是运行期
    /// 输入问题。
    fn into_halves(
        &self,
        device: Self::Device,
    ) -> (
        <Self::ConnCfg as TrConnCfg>::ConnTx,
        <Self::ConnCfg as TrConnCfg>::ConnRx,
    );
}

/// [`TrInbound::accept_async`] 的产出。
///
/// `Ok(Some(dev))` = 拿到连接；`Ok(None)` = 本次没有连接（可重试）；`Err(..)` = 接受失败。
pub type AcceptOutput<A> =
    Result<Option<<A as TrInbound>::Device>, <A as TrInbound>::Error>;

/// **每次握手当场造一个协商器**。
///
/// [`HandshakeAgent::listen_async`](smux_v1::handshake::agent::HandshakeAgent::listen_async)
/// 按值接过协商器，因此协商器不能是跨连接复用的一个实例：本 trait 就是那条
/// 「按需新建」的工厂。
///
/// # 与 [`TrNegotiator`] 分开的理由
///
/// 协商器按值消耗，「怎么造出下一个」属于策略配置。两者类型常常相同（无状态协商器
/// 写 `type Negotiator = Self`），但独立成 trait 之后，带状态的协商器也能表达。
pub trait TrNegotiatorFactory {
    /// 交给 `listen_async` 的协商器类型。
    ///
    /// `for<'x>` 来自 `HandshakeAgent::listen_async` 的签名：协商条目借用了读缓冲，
    /// 其生命周期由调用点决定，协商器必须对**任意**生命周期都能交出
    /// [`NegotiationEntry`]。
    type Negotiator: for<'x> TrNegotiator<Entry<'x> = NegotiationEntry<'x>> + 'static;

    /// 造一个协商器；每接受一条连接调用一次。
    ///
    /// 实现应当返回一个**互不共享状态**的实例。
    fn make(&self) -> Self::Negotiator;
}

/// listener 的**连接装配策略**：握手条件 + 连接资源策略 + 帧暂存缓冲工厂。
///
/// # 「预存下来的握手条件」在这里
///
/// [`TrListenerConfig::local_opts`] 是等待方在 `ACCEPT` 里用来**补全**发起方未提及项的
/// 本端取值（见 `smux_v1` 握手模块文档 §7.2）。发起方声明了哪些项就逐字采用哪些项，
/// 因此本端给出的值只对**对端没提**的项生效——listener 不做「取小合并」，也不替对端
/// 设限；要限制对端提出的取值，那是 [`TrNegotiatorFactory`] 的职责。
///
/// # 运行时值为什么由配置交出
///
/// 每条连接都要建一个 [`MuxConnection`](smux_v1::connection::MuxConnection)，而建连需要
/// 「怎么等」与「现在几点」。本 crate **不**用
/// [`default_rt_`](smux_v1::x_deps::abs_art_facade::default_rt_)（那个入口要求调用点已
/// 处于编译期选定的后端上下文内），而是要求配置显式交出运行时值：于是同一份 listener
/// 代码在哪个后端上跑**不再由 feature 决定**，只由配置里放的那个值决定。
///
/// # 缓冲为什么由配置交工厂
///
/// `smux_v1` 的纪律是「缓冲由调用方当场交出」。listener 看不到调用方，因此把「当场」
/// 提前到「实现本 trait 时」：[`TrListenerConfig::make_stage_buffs`] 每条连接调用一次。
/// 容量、分配器、是否池化都由实现决定。
pub trait TrListenerConfig: 'static {
    /// **运行时值类型**：提供「现在几点」「怎么等」，以及「交出本地队列」。
    ///
    /// 它必须与 [`TrConnCfg::Rt`] 是同一个类型——本 trait 的契约就是「握手与建连跑在
    /// 同一条时间轴上」。约束直接写在**这里**（而不是 `Conn: TrConnCfg<Rt: ScopeHost>`）：
    /// 关联类型上的 `where` 约束不会在使用点自动展开，写在 `Conn` 上会让每个使用点都得
    /// 重述一遍，写在这里则自动可用。
    type Rt: TrTime + Clone + 'static + crate::TrScopeHost;

    /// 连接级帧暂存缓冲的**拥有者类型**。实现请写 [`CoreAllocOwned_`]。
    ///
    /// 它写成关联类型（而不是把 [`make_stage_buffs`](TrListenerConfig::make_stage_buffs)
    /// 的返回类型直接钉死）是为了让「本 listener 用哪种缓冲」成为**契约的一部分**、在
    /// 文档里可见。当前只支持 [`CoreAllocOwned_`]：换内存策略属于「换一整套连接设施」，
    /// 那要走 [`MuxConnection::new`](smux_v1::connection::MuxConnection::new)。
    type StageBuff: smux_v1::x_deps::abs_mm::res_man::TrUnique<
        Item = [MaybeUninit<u8>],
    > + 'static;

    /// 每条连接的资源策略。
    ///
    /// 它必须与 [`TrInbound::ConnCfg`] 取相等约束（见
    /// [`next_async_`](crate::listening) 的 `where` 子句）：listener 的 `where` 子句里
    /// 写着 `A: TrInbound<ConnCfg = Cfg::Conn>`，于是适配器交出的两条半边**就是**这里
    /// 要的那两条，不需要任何转换。
    ///
    /// 约束里的 `Rt: ScopeHost` 是「连接策略交出的运行时值能交出本地队列」——建连时要用
    /// 它把五个循环投递出去。约束写在**这里**而不是
    /// [`TrListenerConfig::runtime`] 上，是因为方法返回的是
    /// `<Self::Conn as TrConnCfg>::Rt` 这个投影类型，投影上的约束不会在调用点自动展开。
    type Conn: TrConnCfg<
            Rt = Self::Rt,
            ConnTx: TrBuffWrite<u8> + 'static,
            ConnRx: TrBuffRead<u8> + 'static,
        >
        + Clone
        + 'static;

    /// 取一个运行时值（克隆句柄；所有连接共享同一时间轴）。
    fn runtime(&self) -> Self::Rt;

    /// 取本次握手预存的本端条件，见 trait 文档。
    fn local_opts(&self) -> BasicOpts;

    /// 取连接资源策略。
    fn conn_cfg(&self) -> Self::Conn;

    /// 造**一对**连接级帧暂存缓冲：`(读环, 写环)`；每条连接调用一次。
    ///
    /// 返回顺序与 [`MuxConnection::new`](smux_v1::connection::MuxConnection::new) 的
    /// 实参顺序一致：**先读后写**。
    fn make_stage_buffs(
        &self,
    ) -> (
        MuxChanBuffOwnedBy<CoreAllocOwned_>,
        MuxChanBuffOwnedBy<CoreAllocOwned_>,
    );

    /// 同时在途的握手条数上限。
    ///
    /// 「在途」= 已经从监听套接字接受、尚未被
    /// [`next_async`](crate::listening::SmuxListener) 取走。因此它同时是
    /// **背压**：取满之后不再接受新连接，内核的 accept 队列替本端挡住后续连接，而不是
    /// 让已建好的连接在本端无限堆积。
    ///
    /// 缺省取 [`K_DEFAULT_MAX_INFLIGHT`]。返回 `0` 与返回 `1` 等价：至少要有一个槽位
    /// 在跑，否则 listener 永远接不到连接。
    fn max_inflight(&self) -> usize {
        K_DEFAULT_MAX_INFLIGHT
    }
}

/// [`TrListenerConfig::max_inflight`] 的缺省值。
///
/// 取 16 是一个折中：足够让一串建连请求重叠起来（握手要两个 RTT，串行做时每条连接的
/// 建立延迟会叠加），又不会让一个长期不被拉取的 listener 攒下大量已建好的连接
/// （每条连接占着两块帧暂存环与五个循环）。
pub const K_DEFAULT_MAX_INFLIGHT: usize = 16usize;

/// 驱动一次「设备 → 两条半边」，并补上可取消形状。
///
/// 适配器内部建环本身没有等待点，因此这里只是把一次同步调用包进
/// [`gen_may_cancel_future`] 生成的 future：接受槽位与被 `spawn_local` 的握手任务于是
/// 共用同一种可取消形状（见 AGENTS.md 纪律 4）。
#[gen_may_cancel_future(IntoHalves, pub(crate))]
pub(crate) async fn into_halves_async_<'a, A, K>(
    inbound: &'a A,
    device: A::Device,
    _cancel: K,
) -> (
    <A::ConnCfg as TrConnCfg>::ConnTx,
    <A::ConnCfg as TrConnCfg>::ConnRx,
)
where
    A: TrInbound + 'a,
    A::Device: 'a,
    K: TrCancellationToken,
{
    inbound.into_halves(device)
}
