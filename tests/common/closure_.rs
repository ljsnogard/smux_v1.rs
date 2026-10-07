//! `TrPrepareChannelRing` 的测试侧适配：把「闭包里塞两块缓冲」这个常见写法包成
//! [`ClosurePrepare`]，并给出 `accept` / `accept_async_closure` 的扩展 trait。

use core::{alloc::AllocatorClone, mem::MaybeUninit};
use buffex::x_deps::abs_buff::TrBuffWrite;
use abs_smux::chan::{ChannelBuffAlloc, TrChannelHandle, TrPrepareChannelRing};
use mm_ptr::x_deps::abs_mm::res_man::TrBoxed;
use smux_v1::connection::{ChannelHandle, ChannelRx, ChannelTx, HandleError, TrConnCfg};

/// 测试侧对「闭包造两块缓冲」这一常见写法的适配器。
///
/// 上游 `TrPrepareChannelRing` 由环境自行实现；本测试套件为了不把每个调用点都手写
/// 一个 prepared 类型，提供一个最小的闭包包装器。
pub struct ClosurePrepare<F>(F);

impl<F> ClosurePrepare<F> {
    /// 包住一个 `FnOnce() -> (tx_buff, rx_buff)` 闭包。
    pub const fn new(inner: F) -> Self {
        ClosurePrepare(inner)
    }
}

impl<F, B> TrPrepareChannelRing<B, u8> for ClosurePrepare<F>
where
    F: FnOnce() -> (B, B),
    B: 'static + TrBoxed<Item = [MaybeUninit<u8>]>,
    B::Alloc: AllocatorClone,
{
    fn prepare(self) -> ChannelBuffAlloc<B, u8> {
        let (tx_buff, rx_buff) = (self.0)();
        ChannelBuffAlloc::new(tx_buff, rx_buff)
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
    async fn accept_async_closure<'f, W, F>(
        &'f mut self,
        welcome: &'f mut W,
        prepare: F,
    ) -> Result<(ChannelTx<C>, ChannelRx<C>), HandleError>
    where
        W: 'f + TrBuffWrite<u8>,
        F: FnOnce() -> (C::Buff, C::Buff);
}

impl<C> AcceptAsyncClosureExt<C> for ChannelHandle<C>
where
    C: TrConnCfg,
{
    async fn accept_async_closure<'f, W, F>(
        &'f mut self,
        welcome: &'f mut W,
        prepare: F,
    ) -> Result<(ChannelTx<C>, ChannelRx<C>), HandleError>
    where
        W: 'f + TrBuffWrite<u8>,
        F: FnOnce() -> (C::Buff, C::Buff),
    {
        self.accept_async(welcome, ClosurePrepare::new(prepare)).await
    }
}
