//! 连接配置：**照 `smux_v1/tests/common/config_.rs` 的 `SmokeMuxConfig` 形状**自己写一份。
//!
//! # 为什么不直接用 `DefaultConnCfg`
//!
//! 两个理由：
//!
//! 1. **sink 句柄可见**：`DefaultConnCfg` 经 `M::default()` 造 sink，本 demo 的
//!    [`DemoMetrics::default`](crate::DemoMetrics) 正好返回进程级句柄，够用；但显式持有
//!    一份句柄更直白，也不依赖 `Default` 的约定；
//! 2. **子流环缓冲由调用方控制**：`smux_v1` 的配置里**没有**缓冲类型——子流环内存由
//!    调用方当场交出，本 demo 在 `transport.rs` 里按 `--ring-cap` 与 `CoreAlloc`
//!    分配。连接级帧暂存本版已经不存在（连接直接驱动传输适配器，适配器自带缓冲）。
//!
//! 形状与测试侧逐项对应：`Data = u8`、`Dock = Dock`、`Policy = DefaultPolicy`、
//! `Alloc = CoreAlloc`。**缓冲类型不在配置里**：子流环内存由调用方在 `accept_async`
//! 当场交出（见 `transport.rs` 的 `RingPrepare`）；传输两条半边自带缓冲。

use core::marker::PhantomData;

use abs_art::TrTime;
use abs_buff::{
    TrBuffRead, TrBuffWrite,
    buffer::{TrConsumerState, TrProducerState},
};
use abs_mm::CoreAlloc;
use abs_smux::conf::TrMuxConfig;
use mm_ptr::x_deps::abs_mm;
use smux_v1::{
    connection::{Dock, TrConnCfg},
    flow_ctrl::DefaultPolicy,
    x_deps::{abs_art, abs_buff, abs_smux, mm_ptr},
};

use crate::{Rt, metrics::DemoMetrics};

/// `DefaultPolicy` 是零大小类型；取静态引用即可满足 [`TrConnCfg::policy`]。
static DEMO_POLICY: DefaultPolicy = DefaultPolicy;

/// 本 demo 的连接配置。
#[derive(Debug)]
pub struct DemoCfg<W, R> {
    /// 建连时抓住的运行时值（计时与时刻的来源）。
    rt_: Rt,
    /// 本连接的 metrics sink 句柄（`Arc` 共享，进程级）。
    sink_: DemoMetrics,
    /// 两条连接半边的类型占位（只以类型形式参与）。
    _use_w_: PhantomData<fn() -> W>,
    _use_r_: PhantomData<fn() -> R>,
}

impl<W, R> Clone for DemoCfg<W, R> {
    fn clone(&self) -> Self {
        DemoCfg {
            rt_: self.rt_.clone(),
            sink_: self.sink_.clone(),
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        }
    }
}

impl<W, R> DemoCfg<W, R> {
    /// 由运行时值与 sink 句柄构造。
    pub fn new(rt: Rt, sink: DemoMetrics) -> Self {
        DemoCfg {
            rt_: rt,
            sink_: sink,
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        }
    }
}

impl<W, R> TrMuxConfig for DemoCfg<W, R>
where
    W: TrBuffWrite<u8> + TrProducerState + 'static,
    R: TrBuffRead<u8> + TrConsumerState + 'static,
{
    type Data = u8;
    type Dock = Dock;
}

impl<W, R> TrConnCfg for DemoCfg<W, R>
where
    W: TrBuffWrite<u8> + TrProducerState + 'static,
    R: TrBuffRead<u8> + TrConsumerState + 'static,
    Rt: TrTime + Clone + 'static,
{
    type Rt = Rt;
    type Alloc = CoreAlloc;
    type Policy = DefaultPolicy;
    type ConnTx = W;
    type ConnRx = R;
    type Metrics = DemoMetrics;

    fn runtime(&self) -> Self::Rt {
        self.rt_.clone()
    }

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &DEMO_POLICY
    }

    fn metrics(&self) -> &Self::Metrics {
        &self.sink_
    }
}
