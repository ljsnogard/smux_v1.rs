//! `TrPrepareRing` 的测试侧适配：把「闭包里塞两块缓冲」这个常见写法包成
//! [`ClosurePrepare`]，并给出 `accept` / `accept_async_closure` 的扩展 trait。

use core::{alloc::AllocatorClone, mem::MaybeUninit};
use buffex::x_deps::abs_buff::TrBuffWrite;
use abs_smux::{
    chan::{RingBuffAlloc, TrChannelHandle, TrPrepareRing},
    conn::TrDockBinding,
    telegraph::TrTelegraphBinding,
};
use mm_ptr::x_deps::abs_mm::res_man::TrUnique;
use smux_v1::connection::{
    ChannelHandle, ChannelRx, ChannelTx, DockBinding, HandleError, Telegraph, TrConnCfg,
};

/// 测试侧对「闭包造两块缓冲」这一常见写法的适配器。
///
/// 上游 `TrPrepareRing` 由环境自行实现；本测试套件为了不把每个调用点都手写
/// 一个 prepared 类型，提供一个最小的闭包包装器。
pub struct ClosurePrepare<F>(F);

impl<F> ClosurePrepare<F> {
    /// 包住一个 `FnOnce() -> (tx_buff, rx_buff)` 闭包。
    pub const fn new(inner: F) -> Self {
        ClosurePrepare(inner)
    }
}

impl<F, B> TrPrepareRing<B, u8> for ClosurePrepare<F>
where
    F: FnOnce() -> (B, B),
    B: 'static + TrUnique<Item = [MaybeUninit<u8>]>,
    B::Alloc: AllocatorClone,
{
    fn prepare(self) -> RingBuffAlloc<B, u8> {
        let (tx_buff, rx_buff) = (self.0)();
        RingBuffAlloc::new(tx_buff, rx_buff)
    }
}


/// 给真实 [`ChannelHandle`] 加一个「直接传闭包」的便捷方法，让测试场景保持可读。
///
/// 生产路径仍然按上游契约走 `accept_async(welcome, prepare)`；这里只是把闭包包进
/// [`ClosurePrepare`] 后转发。
///
/// 第二个类型参数 `S` 是**运行时值**（`ChannelHandle<C, S>` 的第二个参数）：
/// 时刻与「怎么等」由**连接配置**携带的运行时值提供
/// （[`TrConnCfg::Rt`](smux_v1::connection::TrConnCfg::Rt)），因此这里不再需要
/// 额外的运行时类型参数。
pub trait AcceptAsyncClosureExt<C>: Sized
where
    C: TrConnCfg,
{
    async fn accept_async_closure<'f, W, B, F>(
        &'f mut self,
        welcome: &'f mut W,
        prepare: F,
    ) -> Result<(ChannelTx<C>, ChannelRx<C>), HandleError>
    where
        W: 'f + TrBuffWrite<u8>,
        B: 'static + TrUnique<Item = [MaybeUninit<u8>]> + Send + Sync,
        B::Alloc: AllocatorClone,
        F: FnOnce() -> (B, B);
}

impl<C> AcceptAsyncClosureExt<C> for ChannelHandle<C>
where
    C: TrConnCfg,
{
    async fn accept_async_closure<'f, W, B, F>(
        &'f mut self,
        welcome: &'f mut W,
        prepare: F,
    ) -> Result<(ChannelTx<C>, ChannelRx<C>), HandleError>
    where
        W: 'f + TrBuffWrite<u8>,
        B: 'static + TrUnique<Item = [MaybeUninit<u8>]> + Send + Sync,
        B::Alloc: AllocatorClone,
        F: FnOnce() -> (B, B),
    {
        self.accept_async::<W, B, _>(welcome, ClosurePrepare::new(prepare))
            .await
    }
}


/// 测试侧对「闭包造两块 telegraph 环内存」这一常见写法的适配器。
///
/// 与 [`ClosurePrepare`] 同形，但产出的是 telegraph 的两块环内存。
///
/// 注意它实现的是**同一个**契约 [`TrPrepareRing`]——telegraph 与 channel 共用一套
/// prepare 语义，没有第二份 trait。
pub struct TgClosurePrepare<F>(F);

impl<F> TgClosurePrepare<F> {
    /// 包住一个 `FnOnce() -> (tx_buff, rx_buff)` 闭包。
    pub const fn new(inner: F) -> Self {
        TgClosurePrepare(inner)
    }
}

impl<F, B> TrPrepareRing<B, u8> for TgClosurePrepare<F>
where
    F: FnOnce() -> (B, B),
    B: 'static + TrUnique<Item = [MaybeUninit<u8>]>,
    B::Alloc: AllocatorClone,
{
    fn prepare(self) -> RingBuffAlloc<B, u8> {
        let (tx_buff, rx_buff) = (self.0)();
        RingBuffAlloc::new(tx_buff, rx_buff)
    }
}

/// 给上传 [`DockBinding`] 加一个「直接传闭包」的便捷方法，让测试场景保持可读。
pub trait OpenTelegraphClosureExt<C>: Sized
where
    C: TrConnCfg,
{
    async fn open_telegraph_async_closure<B, F>(
        &mut self,
        prepare: F,
    ) -> Result<Telegraph<C>, smux_v1::connection::BindingError>
    where
        B: 'static + TrUnique<Item = [MaybeUninit<u8>]> + Send + Sync,
        B::Alloc: AllocatorClone,
        F: FnOnce() -> (B, B);
}

impl<C> OpenTelegraphClosureExt<C> for DockBinding<C>
where
    C: TrConnCfg,
{
    async fn open_telegraph_async_closure<B, F>(
        &mut self,
        prepare: F,
    ) -> Result<Telegraph<C>, smux_v1::connection::BindingError>
    where
        B: 'static + TrUnique<Item = [MaybeUninit<u8>]> + Send + Sync,
        B::Alloc: AllocatorClone,
        F: FnOnce() -> (B, B),
    {
        <DockBinding<C> as TrTelegraphBinding<C>>::open_telegraph_async(
            self,
            TgClosurePrepare::new(prepare),
        )
        .await
    }
}
