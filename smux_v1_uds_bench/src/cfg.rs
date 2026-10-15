//! 连接配置：`smux_v1` 的 [`TrConnCfg`] 在本 bench 里的实现。
//!
//! 形状照 `smux_v1_socks_demo` 的 `DemoCfg`（也照 `smux_v1/tests/common/config_.rs` 的
//! `SmokeMuxConfig`）写一份，差别只有一处：**metrics sink 换成零开销的 `NoMetrics`**。
//!
//! # 为什么不用 `DebugCounters`
//!
//! 本 bench 要的是可比较的吞吐与延迟。内置采集器（`metrics` feature 下的
//! `DebugCounters`）在热路径上要抢原子计数，会同时污染两条被测路径——**两边污染得还
//! 不一样**（smux 侧每帧一次，raw 侧没有帧）。缺省 sink 是零大小的 `NoMetrics`，
//! 它的每个方法都是 `#[inline(always)]` 空实现，于是上报调用点被优化成零指令，两条
//! 路径的唯一差别就只剩「传输形态」本身。
//!
//! 想采集连接级帧 / 字节计数时用 `--metrics`：那会单独跑一格、并在报告里标出
//! 「本格打开了 metrics，数字不可与关闭时横向比较」。

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
    metrics::NoMetrics,
    x_deps::{abs_art, abs_buff, abs_smux, mm_ptr},
};

use crate::Rt;

/// `DefaultPolicy` 是零大小类型；取静态引用即可满足 [`TrConnCfg::policy`]。
static BENCH_POLICY: DefaultPolicy = DefaultPolicy;

/// 本 bench 的连接配置。
#[derive(Debug)]
pub struct BenchCfg<W, R> {
    /// 建连时抓住的运行时值（计时与时刻的来源）。
    rt_: Rt,
    /// 零开销 metrics sink。
    sink_: NoMetrics,
    /// 两条连接半边的类型占位（只以类型形式参与）。
    _use_w_: PhantomData<fn() -> W>,
    _use_r_: PhantomData<fn() -> R>,
}

impl<W, R> Clone for BenchCfg<W, R> {
    fn clone(&self) -> Self {
        BenchCfg {
            rt_: self.rt_.clone(),
            sink_: self.sink_,
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        }
    }
}

impl<W, R> BenchCfg<W, R> {
    /// 由运行时值构造（sink 恒为 [`NoMetrics`]）。
    pub fn new(rt: Rt) -> Self {
        BenchCfg {
            rt_: rt,
            sink_: NoMetrics,
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        }
    }
}

impl<W, R> TrMuxConfig for BenchCfg<W, R>
where
    W: TrBuffWrite<u8> + TrProducerState + 'static,
    R: TrBuffRead<u8> + TrConsumerState + 'static,
{
    type Data = u8;
    type Dock = Dock;
}

impl<W, R> TrConnCfg for BenchCfg<W, R>
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
    type Metrics = NoMetrics;

    fn runtime(&self) -> Self::Rt {
        self.rt_.clone()
    }

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &BENCH_POLICY
    }

    fn metrics(&self) -> &Self::Metrics {
        &self.sink_
    }
}
