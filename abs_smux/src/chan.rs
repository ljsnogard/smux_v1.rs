use core::{
    alloc::AllocatorClone,
    marker::PhantomData,
    mem::MaybeUninit,
};

use abs_buff::{
    TrBuffFlush, TrBuffRead, TrBuffWrite,
    buffer::{TrConsumerState, TrProducerState},
};
use abs_cancel::TrMayCancel;
use abs_mm::res_man::TrUnique;

use crate::{
    conf::TrMuxConfig,
    x_deps::abs_cancel,
};

/// 远端将要建立 channel 但仍未完成时的半建立 channel。用于传递额外
/// 信息（如协商结果）以及供本端裁决是否最终建立 channel
pub trait TrChannelHandle<C>
where
    C: TrMuxConfig,
{
    type Err: core::error::Error;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // Acceptance
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type Tx: TrChannelTx<C>;
    type Rx: TrChannelRx<C>;

    /// 最终裁决：把**调用方当场交出的**那块 ring 内存接成两条环。
    ///
    /// ring 存储的智能指针类型 `B` 是**方法级泛型**，不再由
    /// [`TrMuxConfig`](crate::conf::TrMuxConfig) 规定——本条子流用什么智能指针、
    /// 从哪来、多大，完全由 `prepare` 的实参决定。
    type AcceptAsync<'f, W, B, P>: TrMayCancel<'f, MayCancelOutput =
        Result<(Self::Tx, Self::Rx), Self::Err>>
    where
        Self: 'f,
        W: 'f + TrBuffWrite<C::Data>,
        B: 'static + Send + Sync + TrUnique<Item = [MaybeUninit<C::Data>], Alloc: AllocatorClone>,
        P: TrPrepareRing<B, C::Data>;

    /// 发送同意建立 channel 的消息及欢迎信息，以及
    fn accept_async<'f, W, B, P>(
        &'f mut self,
        welcome: &'f mut W,
        prepare: P,
    ) -> Self::AcceptAsync<'f, W, B, P>
    where
        W: 'f + TrBuffWrite<C::Data>,
        B: 'static + Send + Sync + TrUnique<Item = [MaybeUninit<C::Data>], Alloc: AllocatorClone>,
        P: TrPrepareRing<B, C::Data>;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type RejectAsync<'f, R>: TrMayCancel<'f, MayCancelOutput =
        Result<usize, Self::Err>>
    where
        Self: 'f,
        R: 'f + TrBuffRead<C::Data>;

    /// 发送拒绝建立 channel 的消息及理由
    fn reject_async<'f, R>(
        &'f mut self,
        reason: &'f mut R,
    ) -> Self::RejectAsync<'f, R>
    where
        R: TrBuffRead<C::Data>;
}

pub trait TrChannelHalf<C>
where
    C: TrMuxConfig,
{
    fn local_dock(&self) -> C::Dock;

    fn remote_dock(&self) -> C::Dock;

    fn is_tx_closed(&self) -> bool;

    fn is_rx_closed(&self) -> bool;
}

pub trait TrChannelTx<C>
where
    C: TrMuxConfig,
    Self: TrBuffFlush<C::Data> + TrProducerState + TrChannelHalf<C>,
{}

pub trait TrChannelRx<C>
where
    C: TrMuxConfig,
    Self: TrBuffRead<C::Data> + TrConsumerState + TrChannelHalf<C>,
{
    type CloseAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<(), Self::Err>>
    where
        Self: 'f;

    fn close_async(&mut self) -> Self::CloseAsync<'_>;
}

/// **一条双向 ring 通道**的两块内存（各自用一个智能指针 `B` 持有）。
///
/// 「两条环、每环一个方向」这个形状是 channel 与 telegraph **共用**的：channel 用它装
/// 子流双向数据，telegraph 用它装数据报两个方向。因此这里只有**一套**契约与**一个**
/// 交付物类型：把「哪几个方向」这件事留给使用者解释，类型本身不按使用者分家。
///
/// `B::Alloc: AllocatorClone` 是连接侧释放这两块内存所需——连接会把 `B` 连同它的
/// ring 一起搬进自己的记账结构，之后**只能**靠 `B` 自己报出的分配器来归还。
///
#[derive(Clone, Debug, Default)]
pub struct RingBuffAlloc<B, T>
where
    B: 'static + TrUnique<Item = [MaybeUninit<T>]>,
    B::Alloc: AllocatorClone,
    T: 'static + Sized,
{
    pub tx_buff: B,
    pub rx_buff: B,
    _use_t_: PhantomData<fn() -> T>,
}

impl<B, T> RingBuffAlloc<B, T>
where
    B: 'static + TrUnique<Item = [MaybeUninit<T>]>,
    B::Alloc: AllocatorClone,
    T: 'static + Sized,
{
    /// 由两个方向的 ring 内存造出交付物（零堆分配）。
    pub const fn new(tx_buff: B, rx_buff: B) -> Self {
        RingBuffAlloc {
            tx_buff,
            rx_buff,
            _use_t_: PhantomData,
        }
    }

    /// 拆成 `(发送 / 前向环内存, 接收 / 反向环内存)`。
    pub fn destruct(self) -> (B, B) {
        (self.tx_buff, self.rx_buff)
    }
}

/// 描述「如何交出**两条环**的内存」的契约，channel 与 telegraph 共用。
///
/// 用哪种智能指针 `B`、两块内存各多大、从哪来，全部由实现者决定——连接只负责把它们
/// 建环并接管会话侧半部。把「两块内存」打包成一个 `self` 参数（而不是两个并列形参），
/// 是为了让调用点的类型参数保持在 `B` 一个上。
///
/// channel 的最终裁决（`accept_async`）与 telegraph 的开启（`open_telegraph_async`）
/// 收的都是**这一个**契约，因此为其中一方写好的 prepare 类型可以直接复用到另一方。
pub trait TrPrepareRing<B, T>
where
    B: 'static + TrUnique<Item = [MaybeUninit<T>]>,
    B::Alloc: AllocatorClone,
    T: 'static + Sized,
{
    /// 交出两个方向的 ring 内存。
    fn prepare(self) -> RingBuffAlloc<B, T>;
}
