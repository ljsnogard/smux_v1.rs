//! 实现 [`abs_smux`] 各 trait 的具体类型。
//!
//! 类型与 trait 的对应关系、并发模型与缓冲策略见 [`crate::connection`] 模块文档。
//! 本文件只承载「对象与 trait 的接线」：状态机在
//! 内部中心循环（`session_` 模块，`pub(crate)`，不对外暴露），
//! 窗口算法在 [`crate::flow_ctrl`]，线格式在 `frame_`。
//!
//! # 资源 bundling：为什么要有 [`TrMuxConfig`]
//!
//! 「环存储类型 `B`、分配器 `A`、流控策略 `P`」三者在整个连接里是**同一组**，
//! 且总是成对出现。若把它们作为三个独立泛型参数写进每个公开类型，签名会迅速
//! 膨胀；因此把它们打包成 [`TrMuxConfig`]，公开类型只需要 `MuxConnection<R, W, C, Rt>`
//! 四个参数（`Rt` 是运行时类型，见后文）。这也是「调用方注入分配器 / 策略」的
//! 落点：实现 [`TrMuxConfig`] 即可换掉整套内存与窗口预算。
//!
//! # 运行时参数 `Rt`
//!
//! `MuxConnection` 内部经 [`abs_art`] spawn 读 / 写两个循环，因此需要知道用哪个
//! 运行时。`Rt` 是**纯类型**参数：`abs_art` 的 spawn 是无 `self` 的关联函数，
//! 连接不持有运行时的值。bound 按 feature 切换——缺省（单线程、compio 首要）
//! 要求 `Rt: TrSpawnLocal`，开启 `multi-thread` 后要求 `Rt: TrSpawnSend`；
//! 具体后端由最终二进制选择（本 crate 不出现任何后端名，也不依赖
//! `abs_art-bridge`）。
//!
//! 泛型参数必须落在**结构体**上而不是只做 `new` 的方法级泛型：内部循环的句柄
//! 类型要能出现在字段类型里（见 dev-notes 的「方案 A」）。
//!
//! # 关闭态：直接读环的两个端
//!
//! `TrChannelHalf::is_tx_closed` / `is_rx_closed` 不额外维护标志位，而是读
//! `buffex` 环本身的**两端关闭态**（`Producer::is_producer_closed` /
//! `is_consumer_closed`）：
//!
//! - 每条子流两个方向各一条环。发送环的应用端是 [`ChannelTx`]（生产端），另一端
//!   由中心循环的写路径持有；接收环的会话端是生产端，应用端是 [`ChannelRx`]（消费端）；
//! - `ChannelTx::is_tx_closed()` 因此是「应用端已关闭发送环的生产端」——丢弃
//!   [`ChannelTx`] 即置位（`buffex` 在半部 drop 时提交该事件）；而
//!   `ChannelTx::is_rx_closed()` 是「中心循环已关闭发送环的消费端」，即连接已经
//!   拆掉这条子流；
//! - 接收方向对称：`ChannelRx::is_tx_closed()` 表示会话（生产端）已关闭接收环，
//!   即对端不再发送（EOF）；`ChannelRx::is_rx_closed()` 表示应用端（消费端）
//!   已关闭。
//!
//! 好处是**关闭态不需要再引入一份共享状态**：环本身就是两个端共享的那点状态，
//! `buffex` 已在其中维护两个方向的关闭位。代价是这四个方法只在真正的 `buffex`
//! 半部上成立，因此 [`TrChannelHalf`] 的 impl 落在具体类型上（而不是泛型 `H`）。

// 本模块目前是**骨架**：类型、签名与文档已定稿，方法体统一为 `todo!()`。
// 实现落地后必须移除本行的 `allow`（见 `dev-notes/` 的待办）。
#![allow(dead_code, unused_variables)]

use core::{
    borrow::BorrowMut,
    marker::PhantomData,
    mem::MaybeUninit,
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    gen_may_cancel_future,
    x_deps::{abs_cancel, anylr},
};
#[cfg(feature = "multi-thread")]
use abs_art::TrSpawnSend;

#[cfg(not(feature = "multi-thread"))]
use abs_art::TrSpawnLocal;

use abs_cancel::TrCancellationToken;
use abs_mm::mem_alloc::TrMalloc;
use abs_smux::conn::{
    TrChannelHandle, TrChannelHalf, TrChannelListener, TrChannelRx, TrChannelTx, TrConnection,
    TrDockBinding,
};
use anylr::SomeOf;
use buffex::x_deps::abs_buff;
use mm_ptr::x_deps::abs_mm;

use crate::{
    connection::{BufferedRx, BufferedTx, Dock, MuxError},
    flow_ctrl::TrFlowCtrlPolicy,
    handshake::agent::HandshakeDelivery,
    handshake::opts::HandshakeOpts,
};

/// 会话派生类型（listener / handle / telegraph）的借用关系与连接泛型占位。
///
/// `&'s &'f ()` 把 `'f: 's` 编码进类型本身：这些类型都派生自「借用了连接的
/// 会话」，因此连接借用的生命周期必须覆盖类型自身。用类型别名而非裸
/// `PhantomData<...>`，既避免 `clippy::type_complexity`，也让三处占位语义一致。
pub(crate) type SessionMark_<'s, 'f, R, W, C, Rt> =
    PhantomData<(&'s &'f (), fn() -> (R, W, C, Rt))>;

/// 复用连接的资源策略：环存储、分配器与流控策略。
///
/// 由调用方实现并注入 [`MuxConnection::new`]；本 crate 只规定「必须能提供这
/// 三样东西」，不规定它们从哪来（堆、静态池、`mm_ptr`、自定义 arena 均可）。
///
/// # 缓冲策略
///
/// 每条子流需要一对 `buffex` 环（发送 / 接收）。[`TrMuxConfig::Buff`] 是环的
/// 存储类型，[`TrMuxConfig::channel_capacity`] 给出单条子流每个方向的容量；
/// 建流时由连接按容量实例化存储并交给 `buffex` 构建器。
pub trait TrMuxConfig {
    /// 环存储类型；通常是 `mm_ptr::Owned<[MaybeUninit<u8>], Self::Alloc>`。
    type Buff: BorrowMut<[MaybeUninit<u8>]> + Send + Sync;

    /// 环内存与帧暂存的分配器。
    type Alloc: TrMalloc + Clone + Send + Sync;

    /// 流控策略。
    type Policy: TrFlowCtrlPolicy;

    /// 取分配器（按值，`buffex` 的构建器按值接收）。
    fn allocator(&self) -> Self::Alloc;

    /// 取流控策略。
    fn policy(&self) -> &Self::Policy;

    /// 单条子流**每个方向**的环容量（字节）。
    fn channel_capacity(&self) -> usize;
}

/// 复用连接：**独占**网络收发半边，并对外提供流复用的全部功能。
///
/// 泛型参数：
///
/// - `R`：网络读半边（握手交付的 `Rx`）；
/// - `W`：网络写半边（握手交付的 `Tx`）；
/// - `C`：资源策略，见 [`TrMuxConfig`]；
/// - `Rt`：运行时类型（见模块文档「运行时参数 `Rt`」）。它必须是**结构体**上的
///   类型参数：内部循环的句柄类型要能出现在字段类型里。
///
/// # 封装边界
///
/// 握手完成后 `Rx` / `Tx` 的生命周期由本对象**完全接管**，它们不出现在任何公开
/// 签名里；读 / 写两个循环是本对象的内部实现（见 [`crate::connection`] 模块文档
/// §2）。`smux_v1` 里其它类型要完成任何功能，只能通过本对象的 API：
/// [`TrConnection::bind_async`] 派生会话；收发由内部任务自动推进。
///
/// [`TrConnection::bind_async`] 取 `&self`，因此同一个连接可以被多个业务逻辑同时
/// 绑定到不同 dock，读 / 写路径在内部互不争锁（模块文档 §2）。
pub struct MuxConnection<R, W, C, Rt> {
    /// 网络读半边：由本对象独占（并移交给内部读循环），不对外暴露。
    rx_: R,

    /// 网络写半边：由本对象独占（并移交给内部写循环），不对外暴露。
    tx_: W,

    /// 资源策略。
    config_: C,

    /// 握手协商结果（连接级配额）。
    opts_: HandshakeOpts,

    /// 运行时类型的占位：连接只需要运行时的**类型**，不需要它的值（`abs_art` 的
    /// spawn 是无 `self` 的关联函数，`Rt` 通常是零大小标记类型）。真正的取消令牌
    /// 与共享注册表随 `sync_` 落地，见 dev-notes 的「方案 A」。
    rt_: PhantomData<fn() -> Rt>,
}

/// 单线程版本（缺省，compio 首要）：内部循环经 `Rt::spawn_local` 投递，因此要求
/// `Rt: TrSpawnLocal`。
#[cfg(not(feature = "multi-thread"))]
impl<R, W, C, Rt> MuxConnection<R, W, C, Rt>
where
    R: TrBuffRead<u8>,
    W: TrBuffWrite<u8>,
    C: TrMuxConfig,
    Rt: TrSpawnLocal,
{
    /// 由一次成功的握手交付物与资源策略构造连接，接管 `Rx` / `Tx`，并**在内部
    /// 经 `abs_art` spawn** 读 / 写两个循环（排空控制帧后按对端发送窗口调度数据
    /// 帧，见 [`crate::connection`] 模块文档 §2）。
    ///
    /// # 循环的收尾方式
    ///
    /// 不靠句柄停任务：[`TrJoinHandle`](abs_art::TrJoinHandle) **没有 `abort`**，
    /// 而且三个后端对「drop 句柄」的语义并不一致（tokio 视作 detach、compio /
    /// smol 视作取消）。因此收尾统一走**取消令牌 + 「连接已失败」标志**：
    /// spawn 后立即 `detach()` 句柄，循环在每个 await 点检查令牌与失败标志自行
    /// 退出；连接 `Drop` 时置位即可。句柄因此不需要存成字段，`Rt::JoinHandle`
    /// 也不必出现在任何类型签名里。
    ///
    /// 对外不提供任何驱动 API：用户只使用 `abs_smux` 的 trait。后端运行时由最终
    /// 二进制经 `abs_art` 选择（本 crate 不依赖 `abs_art-bridge`）。
    pub fn new(delivery: HandshakeDelivery<W, R>, config: C) -> Self {
        todo!("接管 Rx / Tx，建立内部读写循环与共享注册表")
    }
}

/// 多线程版本（开启 `multi-thread`）：语义与单线程版本**完全一致**，唯一区别是
/// 内部循环经 `Rt::spawn` 投递（可跨线程），因此 bound 换成 `Rt: TrSpawnSend`。
#[cfg(feature = "multi-thread")]
impl<R, W, C, Rt> MuxConnection<R, W, C, Rt>
where
    R: TrBuffRead<u8>,
    W: TrBuffWrite<u8>,
    C: TrMuxConfig,
    Rt: TrSpawnSend,
{
    /// 与缺省配置下的同名方法语义相同（含「取消令牌 + 当场 detach」的收尾方式），
    /// 只是本配置下要求 `Rt: TrSpawnSend`。
    pub fn new(delivery: HandshakeDelivery<W, R>, config: C) -> Self {
        todo!("接管 Rx / Tx，建立内部读写循环与共享注册表")
    }
}

/// 在某个 `local_dock` 上派生的会话对象。
///
/// 一个 [`DockBinding`] 对应一个业务逻辑：它自带 `local_dock`，可以在其上监听
/// 入向请求（[`TrDockBinding::listen_async`]）、打开数据报端点
/// （[`TrDockBinding::open_telegraph_async`]）或向对端发起子流
/// （[`TrDockBinding::open_channel_async`]）。不同 binding 之间只在注册表上
/// 交集，因此可以并行持有。
///
/// # 身份与临时 dock
///
/// `(self.local_dock, remote_dock)` 即子流身份（见 [`crate::connection`] 模块
/// 文档 §4.1）：本会话的 `local_dock` 在 [`MuxConnection::new`] 之后由
/// [`TrConnection::bind_async`] 固定。若要在**同一时刻**向同一个 `remote_dock`
/// 发起多条子流，就必须用**多个 local_dock 各建一个会话**——协议不提供 channel
/// id，同一 dock 对上的两条并发子流无法区分。
pub struct DockBinding<'f, R, W, C, Rt> {
    /// 连接对象（共享，`bind_async` 取 `&self`）。
    conn_: &'f MuxConnection<R, W, C, Rt>,

    /// 本会话绑定的 local_dock。
    local_dock_: Dock,
}

/// 某 `local_dock` 上的入向子流监听器（类 `TcpListener`）。
///
/// [`TrChannelListener::income_async`] 每次返回一个**待决句柄**
/// [`ChannelHandle`]；调用方决定 accept 还是 reject，之后该 dock 才能继续接受
/// 下一个请求（同一 dock 的请求串行化，便于用户侧实现「排队 / 限流」）。
pub struct ChannelListener<'s, 'f, R, W, C, Rt> {
    /// 监听的 local_dock。
    local_dock_: Dock,

    /// 借用关系与连接泛型的占位；`&'s &'f ()` 同时编码了 `'f: 's`——监听器派生自
    /// 借用了连接的会话，因此连接借用的生命周期必须覆盖监听器自身。真实共享句柄
    /// 见 [`crate::connection`] 模块文档。
    _mark_: SessionMark_<'s, 'f, R, W, C, Rt>,
}

/// 一个入向建流请求的待决句柄。
///
/// 由 [`ChannelListener::income_async`] 产出；调用方用
/// [`TrChannelHandle::accept_async`] 接受并交付欢迎信息，或
/// [`TrChannelHandle::reject_async`] 拒绝并说明理由。句柄本身也是
/// [`TrChannelHalf`]，因此可以在决定之前查看两侧 dock。
///
/// 句柄上的 `(local_dock, remote_dock)` 就是这条待决子流的身份：响应方在自己的
/// `local_dock` 上用 `remote_dock` 区分不同请求端的连接（见
/// [`crate::connection`] 模块文档 §4.1）。
pub struct ChannelHandle<'s, 'f, R, W, C, Rt> {
    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,

    /// 借用关系与连接泛型的占位；语义同 [`ChannelListener`]。
    _mark_: SessionMark_<'s, 'f, R, W, C, Rt>,
}

/// 子流发送半边：包一个 `buffex` 生产端半部。
///
/// 实现 `TrBuffTryWrite<u8>`，因此应用侧写数据是**非阻塞**的：环满即返回
/// `WriteErrTag::Stuffed`，由应用决定等待还是丢弃。真正把数据推上网络的是
/// 内部写循环。
///
/// # 关闭语义（半关闭）
///
/// 本类型**按值独占**生产端半部，因此**丢弃它即关闭发送方向**（`buffex` 在
/// 半部 drop 时置位本端关闭标志）；会话据此得知「应用不再发送」并发出
/// `CLOSE(FIN)`。两个方向互不影响，关闭态直接取自环本身（见模块文档
/// 「关闭态」一节）。
pub struct ChannelTx<H> {
    /// `buffex` 生产端半部（[`BufferedTx`] 的实例）。
    half_: H,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,
}

impl<H> ChannelTx<H> {
    /// 由 `buffex` 生产端半部与 dock 对构造。
    ///
    /// 只供连接内部（中心循环、建流路径）与单元测试使用：对外部使用者而言，
    /// 这两个半边只应由 `abs_smux` 的 trait 产出。
    pub(crate) fn new_(half: H, local_dock: Dock, remote_dock: Dock) -> Self {
        ChannelTx {
            half_: half,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
        }
    }
}

/// 子流接收半边：包一个 `buffex` 消费端半部。
///
/// 实现 `TrBuffTryRead<u8>`；环空即返回 `ReadErrTag::Drained`。数据由
/// 内部读循环从网络解复用后写入。
///
/// # 关闭语义（半关闭）
///
/// 丢弃本类型即关闭接收方向；写端关闭后先把残留数据读走，再 `try_read` 才会
/// 报 `Closing`（EOF 语义，见模块文档「关闭态」一节）。
pub struct ChannelRx<H> {
    /// `buffex` 消费端半部（[`BufferedRx`] 的实例）。
    half_: H,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,
}

impl<H> ChannelRx<H> {
    /// 由 `buffex` 消费端半部与 dock 对构造；可见性同 [`ChannelTx::new_`]。
    pub(crate) fn new_(half: H, local_dock: Dock, remote_dock: Dock) -> Self {
        ChannelRx {
            half_: half,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// TrConnection
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<R, W, C, Rt> TrConnection for MuxConnection<R, W, C, Rt>
where
    R: TrBuffRead<u8>,
    W: TrBuffWrite<u8>,
    C: TrMuxConfig,
{
    type Data = u8;
    type Dock = Dock;
    type Err = MuxError<R::Err, W::Err>;

    type DockBinding<'f>
        = DockBinding<'f, R, W, C, Rt>
    where
        Self: 'f;

    type BindAsync<'f>
        = MuxBindAsync<'f, 'f, R, W, C, Rt>
    where
        Self: 'f;

    fn bind_async<'f>(&'f self, local_dock: Self::Dock) -> Self::BindAsync<'f> {
        MuxBindAsync::new(self, local_dock)
    }
}

/// [`TrConnection::bind_async`] 的 step 函数。
#[gen_may_cancel_future(MuxBind, pub)]
async fn mux_bind_async_<'f, R, W, C, Rt, K>(
    conn: &'f MuxConnection<R, W, C, Rt>,
    local_dock: Dock,
    _cancel: K,
) -> Result<DockBinding<'f, R, W, C, Rt>, MuxError<R::Err, W::Err>>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    K: TrCancellationToken,
{
    todo!("登记 dock 绑定并返回会话")
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// TrDockBinding
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<'f, R, W, C, Rt> TrDockBinding for DockBinding<'f, R, W, C, Rt>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
{
    type Data = u8;
    type Dock = Dock;
    type Err = MuxError<R::Err, W::Err>;

    type Listener<'s>
        = ChannelListener<'s, 'f, R, W, C, Rt>
    where
        Self: 's;

    type ListenAsync<'s>
        = MuxListenAsync<'s, 'f, 's, R, W, C, Rt>
    where
        Self: 's;

    type Telegraph<'s>
        = super::Telegraph<'s, 'f, R, W, C, Rt>
    where
        Self: 's;

    type OpenTelegraphAsync<'s>
        = MuxOpenTelegraphAsync<'s, 'f, 's, R, W, C, Rt>
    where
        Self: 's;

    type Tx = ChannelTx<BufferedTx<C::Buff, C::Alloc>>;
    type Rx = ChannelRx<BufferedRx<C::Buff, C::Alloc>>;

    type OpenChannelAsync<'s, M>
        = MuxOpenChannelAsync<'s, 'f, 's, R, W, C, Rt, M>
    where
        Self: 's,
        M: 's + TrBuffRead<Self::Data>;

    fn local_dock(&self) -> &Self::Dock {
        &self.local_dock_
    }

    fn listen_async(&mut self) -> Self::ListenAsync<'_> {
        MuxListenAsync::new(self)
    }

    fn open_telegraph_async(&mut self) -> Self::OpenTelegraphAsync<'_> {
        MuxOpenTelegraphAsync::new(self)
    }

    fn open_channel_async<'s, M>(
        &'s mut self,
        remote_dock: Self::Dock,
        message: &'s mut M,
    ) -> Self::OpenChannelAsync<'s, M>
    where
        M: TrBuffRead<Self::Data>,
    {
        MuxOpenChannelAsync::new(self, remote_dock, message)
    }
}

/// [`TrDockBinding::listen_async`] 的 step 函数。
#[gen_may_cancel_future(MuxListen, pub)]
async fn mux_listen_async_<'s, 'f, R, W, C, Rt, K>(
    binding: &'s mut DockBinding<'f, R, W, C, Rt>,
    _cancel: K,
) -> Result<ChannelListener<'s, 'f, R, W, C, Rt>, MuxError<R::Err, W::Err>>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    K: TrCancellationToken,
{
    todo!("在本地 dock 上建立监听器")
}

/// [`TrDockBinding::open_telegraph_async`] 的 step 函数。
#[gen_may_cancel_future(MuxOpenTelegraph, pub)]
async fn mux_open_telegraph_async_<'s, 'f, R, W, C, Rt, K>(
    binding: &'s mut DockBinding<'f, R, W, C, Rt>,
    _cancel: K,
) -> Result<super::Telegraph<'s, 'f, R, W, C, Rt>, MuxError<R::Err, W::Err>>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    K: TrCancellationToken,
{
    todo!("在本地 dock 上建立数据报端点")
}

/// [`TrDockBinding::open_channel_async`] 的 step 函数。
///
/// `message` 是随 `OPEN` 帧附带的开场消息；由于关联类型已泛型于它
/// （`OpenChannelAsync<'s, M>`），future 可以直接持有该缓冲。
#[gen_may_cancel_future(MuxOpenChannel, pub)]
async fn mux_open_channel_async_<'s, 'f, R, W, C, Rt, M, K>(
    binding: &'s mut DockBinding<'f, R, W, C, Rt>,
    remote_dock: Dock,
    message: &'s mut M,
    _cancel: K,
) -> Result<
    (
        ChannelTx<BufferedTx<C::Buff, C::Alloc>>,
        ChannelRx<BufferedRx<C::Buff, C::Alloc>>,
    ),
    MuxError<R::Err, W::Err>,
>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    M: TrBuffRead<u8> + 's,
    K: TrCancellationToken,
{
    todo!("建立发送环 / 接收环，写 OPEN 帧并等待 ACCEPT / REJECT")
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// TrChannelListener
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<'s, 'f, R, W, C, Rt> TrChannelListener for ChannelListener<'s, 'f, R, W, C, Rt>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
{
    type Data = u8;
    type Dock = Dock;
    type Err = MuxError<R::Err, W::Err>;

    type ChannelHandle = ChannelHandle<'s, 'f, R, W, C, Rt>;

    type IncomeAsync<'i>
        = MuxIncomeAsync<'i, 's, 'f, 'i, R, W, C, Rt>
    where
        Self: 'i;

    fn local_dock(&self) -> &Self::Dock {
        &self.local_dock_
    }

    fn income_async(&mut self) -> Self::IncomeAsync<'_> {
        MuxIncomeAsync::new(self)
    }
}

/// [`TrChannelListener::income_async`] 的 step 函数。
#[gen_may_cancel_future(MuxIncome, pub)]
async fn mux_income_async_<'i, 's, 'f, R, W, C, Rt, K>(
    listener: &'i mut ChannelListener<'s, 'f, R, W, C, Rt>,
    _cancel: K,
) -> Result<ChannelHandle<'s, 'f, R, W, C, Rt>, MuxError<R::Err, W::Err>>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    K: TrCancellationToken,
{
    todo!("从入向请求队列取下一个待决句柄")
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// TrChannelHandle
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<'s, 'f, R, W, C, Rt> TrChannelHandle for ChannelHandle<'s, 'f, R, W, C, Rt>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
{
    type Err = MuxError<R::Err, W::Err>;

    type Tx = ChannelTx<BufferedTx<C::Buff, C::Alloc>>;
    type Rx = ChannelRx<BufferedRx<C::Buff, C::Alloc>>;

    type AcceptAsync<'a, Wb>
        = MuxAcceptAsync<'a, 's, 'f, 'a, R, W, C, Rt, Wb>
    where
        Self: 'a,
        Wb: 'a + TrBuffWrite;

    type RejectAsync<'a, Rb>
        = MuxRejectAsync<'a, 's, 'f, 'a, R, W, C, Rt, Rb>
    where
        Self: 'a,
        Rb: 'a + TrBuffRead;

    fn accept_async<'a, Wb>(&'a mut self, welcome: &'a mut Wb) -> Self::AcceptAsync<'a, Wb>
    where
        Wb: TrBuffWrite,
    {
        MuxAcceptAsync::new(self, welcome)
    }

    fn reject_async<'a, Rb>(&'a mut self, reason: &'a mut Rb) -> Self::RejectAsync<'a, Rb>
    where
        Rb: TrBuffRead,
    {
        MuxRejectAsync::new(self, reason)
    }
}

impl<'s, 'f, R, W, C, Rt> TrChannelHalf for ChannelHandle<'s, 'f, R, W, C, Rt> {
    type Data = u8;
    type Dock = Dock;

    fn local_dock(&self) -> Self::Dock {
        self.local_dock_
    }

    fn remote_dock(&self) -> Self::Dock {
        self.remote_dock_
    }

    fn is_tx_closed(&self) -> bool {
        todo!("读入向请求的发送方向状态")
    }

    fn is_rx_closed(&self) -> bool {
        todo!("读入向请求的接收方向状态")
    }
}

/// [`TrChannelHandle::accept_async`] 的 step 函数。
#[gen_may_cancel_future(MuxAccept, pub)]
async fn mux_accept_async_<'a, 's, 'f, R, W, C, Rt, Wb, K>(
    handle: &'a mut ChannelHandle<'s, 'f, R, W, C, Rt>,
    welcome: &'a mut Wb,
    _cancel: K,
) -> Result<
    (
        ChannelTx<BufferedTx<C::Buff, C::Alloc>>,
        ChannelRx<BufferedRx<C::Buff, C::Alloc>>,
    ),
    MuxError<R::Err, W::Err>,
>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    Wb: TrBuffWrite<u8> + 'a,
    K: TrCancellationToken,
{
    todo!("写 ACCEPT（含欢迎信息），交付子流两端")
}

/// [`TrChannelHandle::reject_async`] 的 step 函数。
#[gen_may_cancel_future(MuxReject, pub)]
async fn mux_reject_async_<'a, 's, 'f, R, W, C, Rt, Rb, K>(
    handle: &'a mut ChannelHandle<'s, 'f, R, W, C, Rt>,
    reason: &'a mut Rb,
    _cancel: K,
) -> Result<usize, MuxError<R::Err, W::Err>>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    Rb: TrBuffRead<u8> + 'a,
    K: TrCancellationToken,
{
    todo!("写 REJECT（含理由），返回写出的理由字节数")
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 子流发送半边
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 只有真正的 `buffex` 生产端才能回答「两个方向各自的关闭态」，因此本 impl 落在
/// 具体半部类型上（泛型的 `TrBuffTryWrite` 转发 impl 仍然对任意 `H` 成立）。
impl<B, A> TrChannelHalf for ChannelTx<BufferedTx<B, A>>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: TrMalloc + Clone + Send + Sync,
{
    type Data = u8;
    type Dock = Dock;

    fn local_dock(&self) -> Self::Dock {
        self.local_dock_
    }

    fn remote_dock(&self) -> Self::Dock {
        self.remote_dock_
    }

    /// 发送方向是否已关闭：本端（应用）已不再发送，或会话已停止排空本环。
    fn is_tx_closed(&self) -> bool {
        self.half_.is_producer_closed()
    }

    /// 接收方向是否已关闭：环的消费端（由中心循环持有）已关闭，即整条子流已被
    /// 连接拆掉。
    fn is_rx_closed(&self) -> bool {
        self.half_.is_consumer_closed()
    }
}

impl<H> TrBuffWrite<u8> for ChannelTx<H>
where
    H: TrBuffWrite<u8>,
{
    type WriteAsync<'f>
        = H::WriteAsync<'f>
    where
        Self: 'f;

    type SegmMut<'f>
        = H::SegmMut<'f>
    where
        Self: 'f;

    type Err = H::Err;

    fn is_stuffed_closing(&self) -> bool {
        self.half_.is_stuffed_closing()
    }

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        self.half_.write_async(demand)
    }
}

impl<H> TrBuffTryWrite<u8> for ChannelTx<H>
where
    H: TrBuffTryWrite<u8>,
{
    fn try_write<'f>(&'f mut self, demand: &'f Demand<usize>) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        self.half_.try_write(demand)
    }
}

impl<B, A> TrChannelTx for ChannelTx<BufferedTx<B, A>>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: TrMalloc + Clone + Send + Sync,
{
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 子流接收半边
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<B, A> TrChannelHalf for ChannelRx<BufferedRx<B, A>>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: TrMalloc + Clone + Send + Sync,
{
    type Data = u8;
    type Dock = Dock;

    fn local_dock(&self) -> Self::Dock {
        self.local_dock_
    }

    fn remote_dock(&self) -> Self::Dock {
        self.remote_dock_
    }

    /// 发送方向是否已关闭：环的生产端（由中心循环持有）已关闭，即对端不再发送
    /// （EOF）或整条子流已被连接拆掉。
    fn is_tx_closed(&self) -> bool {
        self.half_.is_producer_closed()
    }

    /// 接收方向是否已关闭：本端（应用）已不再接收。
    fn is_rx_closed(&self) -> bool {
        self.half_.is_consumer_closed()
    }
}

impl<H> TrBuffRead<u8> for ChannelRx<H>
where
    H: TrBuffRead<u8>,
{
    type ReadAsync<'f>
        = H::ReadAsync<'f>
    where
        Self: 'f;

    type SegmRef<'f>
        = H::SegmRef<'f>
    where
        Self: 'f;

    type Err = H::Err;

    fn is_drained_closing(&self) -> bool {
        self.half_.is_drained_closing()
    }

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        self.half_.read_async(demand)
    }
}

impl<H> TrBuffTryRead<u8> for ChannelRx<H>
where
    H: TrBuffTryRead<u8>,
{
    fn try_read<'f>(&'f mut self, demand: &'f Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        self.half_.try_read(demand)
    }
}

impl<B, A> TrChannelRx for ChannelRx<BufferedRx<B, A>>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: TrMalloc + Clone + Send + Sync,
{
}

#[cfg(test)]
mod tests_ {
    use abs_buff::{
        Demand,
        buffer::{TrBuffSegmMut, TrBuffSegmRef},
    };
    use buffex::circular_buff::builder::CircularBuffBuilder;
    use mm_ptr::{Owned, x_deps::abs_mm};

    use abs_mm::mem_alloc::CoreAlloc;

    use super::*;

    /// 测试用的环存储类型与构建器（与集成测试的 `SmokeMuxConfig` 同款）。
    type TestBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;
    type TestBuilder = CircularBuffBuilder<TestBuff>;

    /// 测试用的子流半边类型。
    type TestTx = ChannelTx<BufferedTx<TestBuff, CoreAlloc>>;
    type TestRx = ChannelRx<BufferedRx<TestBuff, CoreAlloc>>;

    /// 构造一对包在**内存环**上的子流半边（容量 64，dock 对 `(3, 7)`）。
    /// - 手段：用 `buffex` 构建器装配「全被动 × 全被动」环，再把两个半部包进
    ///   [`ChannelTx`] / [`ChannelRx`]。
    /// - 判断：返回的 `(Tx, Rx)` 即被测对象；构建失败即测试失败。
    async fn make_halves_() -> (TestTx, TestRx) {
        let mut ready = TestBuilder::with_allocator(64usize, CoreAlloc)
            .expect("分配环缓冲应当成功")
            .producer_passive()
            .consumer_passive();
        let (half_tx, half_rx) = ready
            .build_async()
            .await
            .expect("构建全被动环应当成功");
        (
            ChannelTx::new_(half_tx, Dock::new(3u32), Dock::new(7u32)),
            ChannelRx::new_(half_rx, Dock::new(3u32), Dock::new(7u32)),
        )
    }

    /// 通过非阻塞接口写入全部字节。
    /// - 手段：按剩余长度要段、把实际写入量计入偏移、drop 段提交。
    /// - 判断：返回 `Ok` 表示写完；环满或关闭即返回错误。
    async fn try_write_all_<W>(tx: &mut W, bytes: &[u8]) -> Result<(), W::Err>
    where
        W: TrBuffTryWrite<u8>,
    {
        let mut offset = 0usize;
        while offset < bytes.len() {
            let rest = bytes.len() - offset;
            let demand = Demand::exactly(rest);
            let mut outcome = tx.try_write(&demand);
            let put = match outcome.as_mut().pick_left() {
                Option::Some(segm) => {
                    segm.as_segm_mut().clone_items_from_buff(&bytes[offset..])
                }
                Option::None => {
                    return Result::Err(
                        outcome.pick_right().expect("IO 结果必须要么是段、要么是错误"),
                    );
                }
            };
            if put == 0usize {
                break;
            }
            offset += put;
        }
        Result::Ok(())
    }

    /// 通过非阻塞接口读出恰好 `out.len()` 字节。
    /// - 手段：与 `try_write_all_` 对称，只搬走请求长度的前缀。
    /// - 判断：返回 `Ok` 表示读满；环空或关闭即返回错误。
    async fn try_read_exact_<R>(rx: &mut R, out: &mut [u8]) -> Result<(), R::Err>
    where
        R: TrBuffTryRead<u8>,
    {
        let mut offset = 0usize;
        while offset < out.len() {
            let rest = out.len() - offset;
            let demand = Demand::exactly(rest);
            let mut outcome = rx.try_read(&demand);
            let got = match outcome.as_mut().pick_left() {
                Option::Some(segm) => {
                    let mut child = segm.as_segm_ref();
                    let limit = core::cmp::min(rest, child.least_count());
                    let dst = &mut out[offset..offset + limit];
                    // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同，且 `dst` 是本地
                    // 独占的可写切片；`move_items_to_buff` 只写入已初始化前缀。
                    let uninit = unsafe {
                        core::slice::from_raw_parts_mut(
                            dst.as_mut_ptr() as *mut MaybeUninit<u8>,
                            dst.len(),
                        )
                    };
                    unsafe { child.move_items_to_buff(uninit) }
                }
                Option::None => {
                    return Result::Err(
                        outcome.pick_right().expect("IO 结果必须要么是段、要么是错误"),
                    );
                }
            };
            if got == 0usize {
                break;
            }
            offset += got;
        }
        Result::Ok(())
    }

    /// 测试两个半边如实报告 dock 对，并把非阻塞读写转发给底下的环。
    /// - 手段：在内存环上构造 `(Tx, Rx)`（dock 对 `(3, 7)`），先断言四个 dock
    ///   取值，再用 `try_write` 写入 5 字节、用 `try_read` 读出并比对。
    /// - 判断：dock 与写入值完全一致；读回的字节与写入逐字节相等——说明包装层
    ///   没有吞掉或改写数据。
    #[compio::test]
    async fn halves_report_docks_and_delegate_try_io() {
        let (mut tx, mut rx) = make_halves_().await;

        assert_eq!(tx.local_dock(), Dock::new(3u32));
        assert_eq!(tx.remote_dock(), Dock::new(7u32));
        assert_eq!(rx.local_dock(), Dock::new(3u32));
        assert_eq!(rx.remote_dock(), Dock::new(7u32));

        let payload = [1u8, 2, 3, 4, 5];
        try_write_all_(&mut tx, &payload)
            .await
            .expect("写入内存环应当成功");
        let mut got = [0u8; 5];
        try_read_exact_(&mut rx, &mut got)
            .await
            .expect("从内存环读出应当成功");
        assert_eq!(got, payload);
    }

    /// 测试四个关闭标志分别对应环的两端，且两端互相可见。
    /// - 手段：新建的环上先断言四个标志全为假；然后关闭发送端（`ChannelTx`
    ///   底下的生产端），再关闭接收端（`ChannelRx` 底下的消费端），每次都读四个
    ///   标志。
    /// - 判断：关闭生产端后两个半边的 `is_tx_closed` 都变为真，而 `is_rx_closed`
    ///   仍为假；关闭消费端后两个半边的 `is_rx_closed` 也变为真——证明两个方向
    ///   互不影响、且状态由环共享。
    #[compio::test]
    async fn close_flags_track_both_ends_independently() {
        let (mut tx, mut rx) = make_halves_().await;

        assert!(!tx.is_tx_closed());
        assert!(!tx.is_rx_closed());
        assert!(!rx.is_tx_closed());
        assert!(!rx.is_rx_closed());

        tx.half_.close();
        assert!(tx.is_tx_closed(), "关闭生产端后发送方向应视为已关闭");
        assert!(rx.is_tx_closed(), "发送方向的关闭应对接收半边可见");
        assert!(!tx.is_rx_closed(), "接收方向不应受影响");
        assert!(!rx.is_rx_closed(), "接收方向不应受影响");

        rx.half_.close_async().await;
        assert!(rx.is_rx_closed(), "关闭消费端后接收方向应视为已关闭");
        assert!(tx.is_rx_closed(), "接收方向的关闭应对发送半边可见");
    }

    /// 测试半关闭后的 EOF 语义：写端关闭不丢数据，排空后才报关闭。
    /// - 手段：写入 3 字节后关闭发送端；先把 3 字节读走，再尝试读 1 字节。
    /// - 判断：关闭后仍能读回全部残留数据；排空后 `is_drained_closing()` 为真，
    ///   且再读返回错误——即「先读完再 EOF」。
    #[compio::test]
    async fn send_close_keeps_buffered_data_then_eof() {
        let (mut tx, mut rx) = make_halves_().await;

        let payload = [9u8, 8, 7];
        try_write_all_(&mut tx, &payload)
            .await
            .expect("写入内存环应当成功");
        tx.half_.close();
        assert!(rx.is_tx_closed(), "写端关闭应立即可见");

        let mut got = [0u8; 3];
        try_read_exact_(&mut rx, &mut got)
            .await
            .expect("关闭写端不应丢弃已缓存的数据");
        assert_eq!(got, payload);

        assert!(
            rx.half_.is_drained_closing(),
            "排空且写端已关闭后应报告 drained"
        );
        let mut one = [0u8; 1];
        assert!(
            try_read_exact_(&mut rx, &mut one).await.is_err(),
            "排空后继续读应当报错而不是空转"
        );
    }
}
