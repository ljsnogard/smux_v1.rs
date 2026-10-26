//! 会话内 `mpmc_chan` 通道的分配调教。
//!
//! 请求队列与两条 `Attach` inbox 都是「逻辑上无上界、物理上按 chunk 复用」的
//! 等待者集合：等待节点由 `mpmc_chan` 的 chunked `SlabList` 承载，取消/丢弃时
//! 归还 slab，不反复触碰全局分配器。本模块只收口它们的共同调教：
//!
//! - 外层 `Arc<MpmcChan>` 与等待 chunk 都用 [`TrConnAllocCfg::MpmcAlloc`]
//!   （不落全局堆）；
//! - 单 chunk 取 4 KiB：控制面等待者通常只有个位数到数十个，4 KiB 已能容纳，
//!   比默认 16 KiB 省，又保持批量分配的可控性。

use core::{alloc::AllocatorClone, marker::PhantomData};

use mpmc_chan::{TrConfig, WaitSlab4K};

/// 会话内通道的 `mpmc_chan` 调教：连接分配器 + 4 KiB 等待 chunk。
///
/// 只作为 [`mpmc_chan::MpmcChan`] 的类型参数使用，不实例化。
pub(crate) struct MuxChanCfg_<A>(PhantomData<A>)
where
    A: AllocatorClone + Send + Sync;

impl<A> TrConfig for MuxChanCfg_<A>
where
    A: AllocatorClone + Send + Sync,
{
    type ChanAlloc = A;
    type ProdWaitSlab = WaitSlab4K<A>;
    type ConsWaitSlab = WaitSlab4K<A>;
    const FAST_SPIN: usize = 512usize;
}
