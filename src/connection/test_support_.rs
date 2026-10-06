//! 连接层的**测试专用**支撑：无循环连接、空作用域与测试策略。
//!
//! 单元测试常常只需要「一个能提供事件发送端与保活的连接对象」，而不需要握手、
//! 传输与两个循环。本模块提供这条捷径：
//!
//! - [`make_test_conn_`]：建一个**不含任何循环**的连接（核心与两条事件通道真实，
//!   但事件接收端随即被丢弃）；
//! - [`NullRt_`]：`TrTime` 的空实现——时刻恒为 0、`delay` **永不就绪**；
//! - [`TestMuxConfig_`]：容量 64、`CoreAlloc`、[`DefaultPolicy`] 的测试策略，
//!   运行时值取 [`NullRt_`]。
//!
//! 走这条路径的对象**不会**推进任何协议状态机，只用于检查句柄与半部的本地行为
//! （环的关闭态、dock 上报、非阻塞转发）。端到端行为一律由 `tests/` 下的集成
//! 测试覆盖。

use mm_ptr::Owned;

use abs_smux::conf::TrMuxConfig;
use abs_art::{TrClock, TrDelay, TrInterval, TrTime};
use mm_ptr::x_deps::abs_mm::CoreAlloc;

use crate::{
    connection::{
        BufferedRx, BufferedTx, BuffAllocError, K_STAGE_RING_CAPACITY, MuxChanBuff,
        MuxConnection, TrConnCfg,
        ring_::test_support_::TestBuff,
    },
    flow_ctrl::DefaultPolicy,
    handshake::opts::{BasicOpts, HandshakeOpts},
};

/// 测试用的**假运行时值**：时刻恒为 0，`delay` 永不就绪。
///
/// 本模块的连接**不驱动任何循环**（见模块文档），因此「等」永远不会被真正 await；
/// 给出一个永不就绪的 `delay` 正是这个语义。时刻恒为 0 让 `ConnClock_` 的毫秒量恒为 0，
/// 而 epoch 与「现在」的比较结果稳定。
///
/// 只实现 [`TrClock`]：`MuxConnection` 的核心只需读「现在」；本模块不构造计时循环
/// （走的是 `new_test_`，它不 spawn 任何循环），因此不需要 [`TrDelay`] / `TrTime`。
///
/// [`TrDelay`]: abs_art::TrDelay
/// [`TrTime`]: abs_art::TrTime
#[derive(Clone, Copy, Default, Debug)]
pub(crate) struct NullRt_;

/// [`NullRt_`] 的时刻类型：毫秒计数，恒为 0。
///
/// 满足 [`TrClock::Instant`] 的结构约束
/// （`Copy + Ord + Add<Duration>` + `Sub<Self, Output = Duration>`）。
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct NullInstant_(u64);

impl core::ops::Add<core::time::Duration> for NullInstant_ {
    type Output = NullInstant_;

    fn add(self, rhs: core::time::Duration) -> NullInstant_ {
        NullInstant_(self.0.saturating_add(rhs.as_millis() as u64))
    }
}

impl core::ops::Sub<NullInstant_> for NullInstant_ {
    type Output = core::time::Duration;

    fn sub(self, rhs: NullInstant_) -> core::time::Duration {
        core::time::Duration::from_millis(self.0.saturating_sub(rhs.0))
    }
}

impl TrClock for NullRt_ {
    type Instant = NullInstant_;

    fn now(&self) -> NullInstant_ {
        NullInstant_(0)
    }
}

impl TrDelay for NullRt_ {
    /// 永不就绪的睡眠 future——本连接不驱动任何循环，因此没有人会真的 await 它。
    type Delay = core::future::Pending<()>;

    fn delay(&self, _duration: core::time::Duration) -> Self::Delay {
        core::future::pending()
    }
}

impl TrTime for NullRt_ {
    /// 永不 tick 的周期源（同上）。
    type Interval = NullInterval_;

    fn interval(&self, period: core::time::Duration) -> Self::Interval {
        assert!(
            period > core::time::Duration::ZERO,
            "`period` must be non-zero."
        );
        NullInterval_
    }
}

/// [`NullRt_`] 的周期源：每次 tick 都永不就绪。
#[derive(Clone, Copy, Debug)]
pub(crate) struct NullInterval_;

impl TrInterval for NullInterval_ {
    type Tick<'a> = core::future::Pending<()>;

    fn tick(&mut self) -> Self::Tick<'_> {
        core::future::pending()
    }
}

/// 测试用资源策略：`CoreAlloc` + [`DefaultPolicy`] + 容量 64。
pub(crate) struct TestMuxConfig_;

/// [`DefaultPolicy`] 是 ZST；取静态引用即可满足 `TrMuxConfig::policy`。
static TEST_POLICY_: DefaultPolicy = DefaultPolicy;

impl TrMuxConfig for TestMuxConfig_ {
    type Data = u8;
    type Dock = crate::connection::Dock;
    type Buff = TestBuff;
}

impl TrConnCfg for TestMuxConfig_ {
    type Rt = NullRt_;
    type Alloc = CoreAlloc;
    type Policy = DefaultPolicy;
    type ConnTx = TestWireTx_;
    type ConnRx = TestWireRx_;
    type StageBuff = TestBuff;

    fn runtime(&self) -> Self::Rt {
        NullRt_
    }

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &TEST_POLICY_
    }

    fn make_ring_buffs(
        &self,
        alloc: Self::Alloc,
        capacity: usize,
    ) -> Result<(Self::Buff, Self::Buff), BuffAllocError> {
        Result::Ok((
            Owned::new_uninit_slice(capacity, alloc),
            Owned::new_uninit_slice(capacity, alloc),
        ))
    }

    fn make_stage_buffs(
        &self,
        alloc: Self::Alloc,
    ) -> Result<(Self::StageBuff, Self::StageBuff), BuffAllocError> {
        Result::Ok((
            Owned::new_uninit_slice(K_STAGE_RING_CAPACITY, alloc),
            Owned::new_uninit_slice(K_STAGE_RING_CAPACITY, alloc),
        ))
    }
}

/// 与 [`TestMuxConfig_`] 同构，但 `Buff` 使用擦除分配器的 [`MuxChanBuff`]。
///
/// 用于在同一个二进制内对比「零 dyn 的具体缓冲」与「擦除载具」。
pub(crate) struct ErasedTestMuxConfig_;

impl TrMuxConfig for ErasedTestMuxConfig_ {
    type Data = u8;
    type Dock = crate::connection::Dock;
    type Buff = MuxChanBuff;
}

impl TrConnCfg for ErasedTestMuxConfig_ {
    type Rt = NullRt_;
    type Alloc = CoreAlloc;
    type Policy = DefaultPolicy;
    type ConnTx = TestWireTx_;
    type ConnRx = TestWireRx_;
    type StageBuff = MuxChanBuff;

    fn runtime(&self) -> Self::Rt {
        NullRt_
    }

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &TEST_POLICY_
    }

    fn make_ring_buffs(
        &self,
        alloc: Self::Alloc,
        capacity: usize,
    ) -> Result<(Self::Buff, Self::Buff), BuffAllocError> {
        MuxChanBuff::pair_from_alloc_(alloc, capacity).map_err(|_| BuffAllocError)
    }

    fn make_stage_buffs(
        &self,
        alloc: Self::Alloc,
    ) -> Result<(Self::StageBuff, Self::StageBuff), BuffAllocError> {
        MuxChanBuff::pair_from_alloc_(alloc, K_STAGE_RING_CAPACITY)
            .map_err(|_| BuffAllocError)
    }
}

/// 测试连接用的两个传输类型（真实的内存环端；本连接不驱动它们，只为满足类型参数）。
pub(crate) type TestWireRx_ = BufferedRx<TestBuff, CoreAlloc>;

/// 同 [`TestWireRx_`]，写半边。
pub(crate) type TestWireTx_ = BufferedTx<TestBuff, CoreAlloc>;

/// 建一个**不含任何循环**的测试连接。
///
/// 运行时值取 [`NullRt_`]（时刻恒为 0），本地作用域取 [`NullScope_`]（丢弃任务）。
/// 两者都只为满足类型参数与构造路径而存在——本连接不推进任何协议状态机。
pub(crate) fn make_test_conn_() -> MuxConnection<TestMuxConfig_> {
    MuxConnection::new_test_(
        HandshakeOpts {
            basic_opts: BasicOpts::default(),
        },
        TestMuxConfig_,
    )
}
