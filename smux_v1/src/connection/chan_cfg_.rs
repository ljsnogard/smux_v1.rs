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
pub(crate) struct MpmcChanCfg_<A>(PhantomData<A>)
where
    A: AllocatorClone + Send + Sync;

impl<A> TrConfig for MpmcChanCfg_<A>
where
    A: AllocatorClone + Send + Sync,
{
    type ChanAlloc = A;
    type ProdWaitSlab = WaitSlab4K<A>;
    type ConsWaitSlab = WaitSlab4K<A>;
    /// 控制通道（两条 `Attach` inbox 与 `main_loop` 请求队列）的空队列自旋预算。
    ///
    /// `mpmc_chan` 每次 poll 空队列时会先做 `FAST_SPIN` 次无锁尝试再注册等待。
    /// 这三个通道都是**低频控制通道**，而 mux/demux/main 会在每轮循环重建各自的
    /// park future；沿用数据面默认的 512 次自旋会让每次 park 都白烧 CPU。实测
    /// （4 vCPU 容器、UDS bench、64 MiB/client）把该值降到 0 后：tokio N=2
    /// 62→92 MiB/s、N=16 65→72；compio N=2 49→86、N=16 64→85；`task_cv` 与
    /// 并发因子同时改善。详见 `dev-notes/perf-20261031-0630.md`。
    const FAST_SPIN: usize = 0usize;
}
