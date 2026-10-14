use core::{
    alloc::AllocatorClone,
    mem::MaybeUninit,
};

use abs_buff::{Demand, buffer::TrBuffSegmRef};
use abs_cancel::TrMayCancel;
use abs_mm::res_man::TrUnique;

use crate::{
    chan::TrPrepareRing,
    conf::TrMuxConfig,
    conn::TrDockBinding,
    x_deps::abs_cancel,
};

pub trait TrTelegraphBinding<C>
where
    C: TrMuxConfig,
    Self: TrDockBinding<C>,
{
    type Telegraph: TrTelegraph<C>;

    type OpenTelegraphAsync<'f, B, P>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::Telegraph, Self::Err>>
    where
        Self: 'f,
        B: 'static + TrUnique<Item = [MaybeUninit<C::Data>], Alloc: AllocatorClone> + Send + Sync,
        P: TrPrepareRing<B, C::Data>;

    /// 在本 binding 的 dock 上开一条数据报端点，并固定**默认对端地址**。
    ///
    /// ring 内存由调用者**当场**交出（`prepare`），与 channel 最终裁决时的
    /// [`accept_async`](crate::chan::TrChannelHandle::accept_async) 同形：用哪种智能指针、
    /// 多大容量、从哪来，全部由实参决定，不是连接的静态配置。
    ///
    /// `remote_dock` 是本端点所有出向数据报的目的地址（`DATAGRAM` 帧的
    /// `remote_dock`）。取 [`TrDock::unspecified`](crate::dock::TrDock::unspecified) 或
    /// `wildcard` 也是合法的——那表示「发给对端自己的策略去裁决」，但此时对端需要有
    /// 一个能兜住这类地址的端点（本仓库的实现按「目的地址 = 自己的 local_dock」索引）。
    fn open_telegraph_async<'f, B, P>(
        &'f mut self,
        prepare: P,
    ) -> Self::OpenTelegraphAsync<'f, B, P>
    where
        B: 'static
            + TrUnique<Item = [MaybeUninit<C::Data>], Alloc: AllocatorClone>
            + Send
            + Sync,
        P: TrPrepareRing<B, C::Data>;
}


/// 用户接收到的数据报报文
pub trait TrRecvDatagram<'a, C>
where
    C: TrMuxConfig,
    Self: TrBuffSegmRef<'a, C::Data>,
{
    fn remote_dock(&self) -> C::Dock;
}

/// 一条 telegraph 端点：**自带收发缓冲**的数据报通道。
///
/// 对比 TCP/IP：telegraph 像 UDP——不需要建流握手即可收发短报文；但**不像** UDP 的是，
/// 同一条连接上 channel 与 telegraph **不得共用同一个 local_dock**。
///
/// # 与 channel 的两点关键差别
///
/// 1. **没有活动局**：数据报不会进入半关闭 / 建流状态机，因此本 trait **不**要求
///    `TrChannelHalf`。端点只回答一个 `local_dock`，收发分别由
///    [`TrTelegraphTx`] / [`TrTelegraphRx`] 承担。
/// 2. **自带缓冲**：ring 内存由调用者在 [`TrDockBinding::open_telegraph_async`] 时当场
///    交出（[`TrPrepareRing`]），此后应用只与本地环打交道，
///    **IO 时不再自备
///    buffer**。
///
/// # 尽力交付
///
/// 数据报**不参与流控**：没有接收窗口、没有 `WINDOW_UPDATE`、没有信用回补，也不靠
/// 保活 `PULSE` 维持。连接只保证「尽力」：发送方向环里有多少就送多少，接收方向缓存
/// 装不下就**整条丢弃**（不做截断、不做重传）。
///
/// [`TrDockBinding::open_telegraph_async`]: crate::conn::TrDockBinding::open_telegraph_async
pub trait TrTelegraph<C>
where
    C: TrMuxConfig,
{
    /// 收发两个方向共用的错误类型（连接级失败须能经它回传）。
    type Err: core::error::Error;

    /// 发送半边。
    type Sender: TrDatagramSender<C, Err = Self::Err>;

    /// 接收半边。
    type Recver: TrDatagramRecver<C, Err = Self::Err>;

    /// 本端 dock（两个方向共享同一个身份）。
    fn local_dock(&self) -> C::Dock;

    /// 拆成发送半边与接收半边。
    ///
    /// 拆开之后两半可以分别存进结构体、分别传给不同的任务，**不必**让发送者与接收者
    /// 共享同一个 `&mut`。
    fn split(self) -> (Self::Sender, Self::Recver);
}

/// telegraph 的**发送半边**：应用写进本地环，连接负责把环里的字节送上网。
///
/// 实现 [`TrBuffWrite`]，因此「写数据」本身是普通的缓冲写入（[`TrBuffWrite::write_async`]）；
/// [`TrTelegraphTx::send_async`] 才是**提交**：把环里已写、尚未提交的字节定成**一条**
/// 报文。
pub trait TrDatagramSender<C>
where
    C: TrMuxConfig,
{
    /// 发送 / 提交方向的错误类型。
    type Err: core::error::Error;

    /// 本端 dock。
    fn local_dock(&self) -> C::Dock;

    /// [`TrTelegraphTx::send_async`] 的返回 future。
    type SendAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<usize, <Self as TrDatagramSender<C>>::Err>>
    where
        Self: 'f;

    /// 提交一条报文，发往 `remote_dock`：把环里**已写、尚未提交**的字节作为一条
    /// 数据报发出。
    ///
    /// # `remote_dock` 为什么在**发送时**给
    ///
    /// 数据报的远端地址是**地址**而不是**身份**（见 `crate::connection` 模块文档 §4）：
    /// 同一条 telegraph 可以对不同的目的地发报文，就像 UDP 的 `sendto`。因此它逐次
    /// 由调用者给出，而不是在 `open_telegraph_async` 时固定下来——那会把「一个端点」
    /// 错误地绑死到「一个对端」。
    ///
    /// `demand` 给出这条报文的可接受长度区间。这里的**下界**由调用者承担：它必须保证
    /// 一条报文至少有那么长，连接不会替它补齐，也不会把不足下界的量当成功——不满足时
    /// 返回错误（语义与 [`Demand`] 在其它缓冲上的用法一致）。**上界**则由连接尊重：
    /// 提交量取「环里剩余未提交字节数」与 `demand` 上界的较小者，因此调用者可以用
    /// `Demand::no_more_than(..)` 形式自己限制一条报文的长度上限。
    ///
    /// 返回**实际提交的字节数**，即这条数据报的载荷长度。返回 `0` 表示提交了一条空报文
    /// （合法，且与「没有提交任何东西」不同）。
    ///
    /// # Errors
    ///
    /// 环已关闭 / 报文的量不满足 `demand` 的下界时返回错误；连接级失败亦经此回传。
    fn send_async<'f>(
        &'f mut self,
        remote_dock: C::Dock,
        demand: &'f Demand<usize>,
    ) -> Self::SendAsync<'f>;
}

/// telegraph 的**接收半边**：连接把到达的整条报文写进本地环，应用从环里读走。
///
/// 实现 [`TrBuffRead`]，因此「读数据」本身是普通的缓冲读取
/// （[`TrBuffRead::read_async`] / `TrBuffTryRead::try_read`）。
pub trait TrDatagramRecver<C>
where
    C: TrMuxConfig,
{
    type DataGram<'f>: TrRecvDatagram<'f, C> where Self: 'f;

    /// 接收方向的错误类型。
    type Err: core::error::Error;

    /// 本端 dock。
    fn local_dock(&self) -> C::Dock;

    /// [`TrTelegraphRx::recv_async`] 的返回 future。
    type RecvAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::DataGram<'f>, <Self as TrDatagramRecver<C>>::Err>>
    where
        Self: 'f;

    /// 等一条**完整**报文并把它放进本地接收环，返回其载荷长度。
    ///
    /// 返回后调用者得到一个 Datagram，包含报文的远端垛口和报文内容。
    ///
    /// 接收环装不下整条报文时**整条丢弃**——调用者会继续等**下一条**，不会拿到半条，
    /// 也不会因为一条过大的报文而永久阻塞。丢弃是可观测的（连接的 metrics 会上报）。
    fn recv_async(&mut self) -> Self::RecvAsync<'_>;
}
