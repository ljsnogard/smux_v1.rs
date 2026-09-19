//! 实现 [`abs_smux`] 各 trait 的具体类型。
//!
//! 类型与 trait 的对应关系、并发模型与缓冲策略见 [`crate::connection`] 模块文档。
//! 本文件只承载「对象与 trait 的接线」：状态机在
//! 内部的读 / 写循环（`session_` 模块，`pub(crate)`，不对外暴露），
//! 窗口算法在 [`crate::flow_ctrl`]，线格式在 `frame_`。
//!
//! # 资源 bundling：为什么要有 [`TrMuxConfig`]
//!
//! 「环存储类型 `B`、分配器 `A`、流控策略 `P`」三者在整个连接里是**同一组**，
//! 且总是成对出现。若把它们作为三个独立泛型参数写进每个公开类型，签名会迅速
//! 膨胀；因此把它们打包成 [`TrMuxConfig`]，公开类型只需要 `MuxConnection<R, W, C>`
//! 三个参数。这也是「调用方注入分配器 / 策略」的落点：实现 [`TrMuxConfig`] 即可
//! 换掉整套内存与窗口预算。

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
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::TrCancellationToken;
use abs_buff::gen_may_cancel_future;
use abs_smux::conn::{
    TrChannelHandle, TrChannelHalf, TrChannelListener, TrChannelRx, TrChannelTx, TrConnection,
    TrDockBinding,
};
use anylr::SomeOf;
use buffex::x_deps::abs_mm::mem_alloc::TrMalloc;

use crate::connection::{BufferedRx, BufferedTx, Dock, MuxError};
use crate::flow_ctrl::TrFlowCtrlPolicy;
use crate::handshake::agent::HandshakeDelivery;
use crate::handshake::opts::HandshakeOpts;

/// 会话派生类型（listener / handle / telegraph）的借用关系与连接泛型占位。
///
/// `&'s &'f ()` 把 `'f: 's` 编码进类型本身：这些类型都派生自「借用了连接的
/// 会话」，因此连接借用的生命周期必须覆盖类型自身。用类型别名而非裸
/// `PhantomData<...>`，既避免 `clippy::type_complexity`，也让三处占位语义一致。
pub(crate) type SessionMark_<'s, 'f, R, W, C> =
    PhantomData<(&'s &'f (), fn() -> (R, W, C))>;

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
/// - `C`：资源策略，见 [`TrMuxConfig`]。
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
pub struct MuxConnection<R, W, C> {
    /// 网络读半边：由本对象独占（并移交给内部读循环），不对外暴露。
    rx_: R,

    /// 网络写半边：由本对象独占（并移交给内部写循环），不对外暴露。
    tx_: W,

    /// 资源策略。
    config_: C,

    /// 握手协商结果（连接级配额）。
    opts_: HandshakeOpts,
}

impl<R, W, C> MuxConnection<R, W, C>
where
    R: TrBuffRead<u8>,
    W: TrBuffWrite<u8>,
    C: TrMuxConfig,
{
    /// 由一次成功的握手交付物与资源策略构造连接，接管 `Rx` / `Tx`，并**在内部
    /// 经 `abs_art` spawn** 读 / 写两个循环（各自持有 `JoinHandle`，随连接关闭而
    /// abort）。
    ///
    /// 对外不提供任何驱动 API：用户只使用 `abs_smux` 的 trait。后端运行时由最终
    /// 二进制经 `abs_art` 选择（本 crate 不依赖 `abs_art-bridge`）。
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
pub struct DockBinding<'f, R, W, C> {
    /// 连接对象（共享，`bind_async` 取 `&self`）。
    conn_: &'f MuxConnection<R, W, C>,

    /// 本会话绑定的 local_dock。
    local_dock_: Dock,
}

/// 某 `local_dock` 上的入向子流监听器（类 `TcpListener`）。
///
/// [`TrChannelListener::income_async`] 每次返回一个**待决句柄**
/// [`ChannelHandle`]；调用方决定 accept 还是 reject，之后该 dock 才能继续接受
/// 下一个请求（同一 dock 的请求串行化，便于用户侧实现「排队 / 限流」）。
pub struct ChannelListener<'s, 'f, R, W, C> {
    /// 监听的 local_dock。
    local_dock_: Dock,

    /// 借用关系与连接泛型的占位；`&'s &'f ()` 同时编码了 `'f: 's`——监听器派生自
    /// 借用了连接的会话，因此连接借用的生命周期必须覆盖监听器自身。真实共享句柄
    /// 见 [`crate::connection`] 模块文档。
    _mark_: SessionMark_<'s, 'f, R, W, C>,
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
pub struct ChannelHandle<'s, 'f, R, W, C> {
    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,

    /// 借用关系与连接泛型的占位；语义同 [`ChannelListener`]。
    _mark_: SessionMark_<'s, 'f, R, W, C>,
}

/// 子流发送半边：包一个 `buffex` 生产端半部。
///
/// 实现 `TrBuffTryWrite<u8>`，因此应用侧写数据是**非阻塞**的：环满即返回
/// `WriteErrTag::Stuffed`，由应用决定等待还是丢弃。真正把数据推上网络的是
/// 内部写循环。
pub struct ChannelTx<H> {
    /// `buffex` 生产端半部（[`BufferedTx`] 的实例）。
    half_: H,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,
}

/// 子流接收半边：包一个 `buffex` 消费端半部。
///
/// 实现 `TrBuffTryRead<u8>`；环空即返回 `ReadErrTag::Drained`。数据由
/// 内部读循环从网络解复用后写入。
pub struct ChannelRx<H> {
    /// `buffex` 消费端半部（[`BufferedRx`] 的实例）。
    half_: H,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// TrConnection
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<R, W, C> TrConnection for MuxConnection<R, W, C>
where
    R: TrBuffRead<u8>,
    W: TrBuffWrite<u8>,
    C: TrMuxConfig,
{
    type Data = u8;
    type Dock = Dock;
    type Err = MuxError<R::Err, W::Err>;

    type DockBinding<'f>
        = DockBinding<'f, R, W, C>
    where
        Self: 'f;

    type BindAsync<'f>
        = MuxBindAsync<'f, 'f, R, W, C>
    where
        Self: 'f;

    fn bind_async<'f>(&'f self, local_dock: Self::Dock) -> Self::BindAsync<'f> {
        MuxBindAsync::new(self, local_dock)
    }
}

/// [`TrConnection::bind_async`] 的 step 函数。
#[gen_may_cancel_future(MuxBind, pub)]
async fn mux_bind_async_<'f, R, W, C, K>(
    conn: &'f MuxConnection<R, W, C>,
    local_dock: Dock,
    _cancel: K,
) -> Result<DockBinding<'f, R, W, C>, MuxError<R::Err, W::Err>>
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

impl<'f, R, W, C> TrDockBinding for DockBinding<'f, R, W, C>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
{
    type Data = u8;
    type Dock = Dock;
    type Err = MuxError<R::Err, W::Err>;

    type Listener<'s>
        = ChannelListener<'s, 'f, R, W, C>
    where
        Self: 's;

    type ListenAsync<'s>
        = MuxListenAsync<'s, 'f, 's, R, W, C>
    where
        Self: 's;

    type Telegraph<'s>
        = super::Telegraph<'s, 'f, R, W, C>
    where
        Self: 's;

    type OpenTelegraphAsync<'s>
        = MuxOpenTelegraphAsync<'s, 'f, 's, R, W, C>
    where
        Self: 's;

    type Tx = ChannelTx<BufferedTx<C::Buff, C::Alloc>>;
    type Rx = ChannelRx<BufferedRx<C::Buff, C::Alloc>>;

    type OpenChannelAsync<'s, M>
        = MuxOpenChannelAsync<'s, 'f, 's, R, W, C, M>
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
async fn mux_listen_async_<'s, 'f, R, W, C, K>(
    binding: &'s mut DockBinding<'f, R, W, C>,
    _cancel: K,
) -> Result<ChannelListener<'s, 'f, R, W, C>, MuxError<R::Err, W::Err>>
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
async fn mux_open_telegraph_async_<'s, 'f, R, W, C, K>(
    binding: &'s mut DockBinding<'f, R, W, C>,
    _cancel: K,
) -> Result<super::Telegraph<'s, 'f, R, W, C>, MuxError<R::Err, W::Err>>
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
async fn mux_open_channel_async_<'s, 'f, R, W, C, M, K>(
    binding: &'s mut DockBinding<'f, R, W, C>,
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

impl<'s, 'f, R, W, C> TrChannelListener for ChannelListener<'s, 'f, R, W, C>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
{
    type Data = u8;
    type Dock = Dock;
    type Err = MuxError<R::Err, W::Err>;

    type ChannelHandle = ChannelHandle<'s, 'f, R, W, C>;

    type IncomeAsync<'i>
        = MuxIncomeAsync<'i, 's, 'f, 'i, R, W, C>
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
async fn mux_income_async_<'i, 's, 'f, R, W, C, K>(
    listener: &'i mut ChannelListener<'s, 'f, R, W, C>,
    _cancel: K,
) -> Result<ChannelHandle<'s, 'f, R, W, C>, MuxError<R::Err, W::Err>>
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

impl<'s, 'f, R, W, C> TrChannelHandle for ChannelHandle<'s, 'f, R, W, C>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
{
    type Err = MuxError<R::Err, W::Err>;

    type Tx = ChannelTx<BufferedTx<C::Buff, C::Alloc>>;
    type Rx = ChannelRx<BufferedRx<C::Buff, C::Alloc>>;

    type AcceptAsync<'a, Wb>
        = MuxAcceptAsync<'a, 's, 'f, 'a, R, W, C, Wb>
    where
        Self: 'a,
        Wb: 'a + TrBuffWrite;

    type RejectAsync<'a, Rb>
        = MuxRejectAsync<'a, 's, 'f, 'a, R, W, C, Rb>
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

impl<'s, 'f, R, W, C> TrChannelHalf for ChannelHandle<'s, 'f, R, W, C> {
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
async fn mux_accept_async_<'a, 's, 'f, R, W, C, Wb, K>(
    handle: &'a mut ChannelHandle<'s, 'f, R, W, C>,
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
async fn mux_reject_async_<'a, 's, 'f, R, W, C, Rb, K>(
    handle: &'a mut ChannelHandle<'s, 'f, R, W, C>,
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

impl<H> TrChannelHalf for ChannelTx<H> {
    type Data = u8;
    type Dock = Dock;

    fn local_dock(&self) -> Self::Dock {
        self.local_dock_
    }

    fn remote_dock(&self) -> Self::Dock {
        self.remote_dock_
    }

    fn is_tx_closed(&self) -> bool {
        todo!("读发送方向关闭标志")
    }

    fn is_rx_closed(&self) -> bool {
        todo!("读接收方向关闭标志（由会话标记）")
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

impl<H> TrChannelTx for ChannelTx<H> where H: TrBuffTryWrite<u8> {}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 子流接收半边
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<H> TrChannelHalf for ChannelRx<H> {
    type Data = u8;
    type Dock = Dock;

    fn local_dock(&self) -> Self::Dock {
        self.local_dock_
    }

    fn remote_dock(&self) -> Self::Dock {
        self.remote_dock_
    }

    fn is_tx_closed(&self) -> bool {
        todo!("读发送方向关闭标志（由会话标记）")
    }

    fn is_rx_closed(&self) -> bool {
        todo!("读接收方向关闭标志")
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

impl<H> TrChannelRx for ChannelRx<H> where H: TrBuffTryRead<u8> {}
