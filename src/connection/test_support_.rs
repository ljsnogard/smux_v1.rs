//! 连接层的**测试专用**支撑：无循环连接、空作用域与测试策略。
//!
//! 单元测试常常只需要「一个能提供事件发送端与保活的连接对象」，而不需要握手、
//! 传输与两个循环。本模块提供这条捷径：
//!
//! - [`make_test_conn_`]：建一个**不含任何循环**的连接（核心与两条事件通道真实，
//!   但事件接收端随即被丢弃）；
//! - [`NullScope_`]：`TrLocalScope` 的空实现——`spawn_local` 直接丢弃 future，
//!   因此不需要任何运行时就能构造连接；
//! - [`TestMuxConfig_`]：容量 64、`CoreAlloc`、[`DefaultPolicy`] 的测试策略。
//!
//! 走这条路径的对象**不会**推进任何协议状态机，只用于检查句柄与半部的本地行为
//! （环的关闭态、dock 上报、非阻塞转发）。端到端行为一律由 `tests/` 下的集成
//! 测试覆盖。

use core::{
    future::Future,
    marker::PhantomData,
    task::{Context, Poll},
};

use abs_art::{TrJoinHandle, TrLocalScope};
use mm_ptr::x_deps::abs_mm::CoreAlloc;

use crate::{
    connection::{
        BufferedRx, BufferedTx, MuxConnection, TrMuxConfig,
        ring_::test_support_::TestBuff,
    },
    flow_ctrl::DefaultPolicy,
    handshake::opts::{BasicOpts, HandshakeOpts},
};

/// 测试用的本地作用域：**不驱动也不保存**任何任务。
///
/// `spawn_local` 直接丢弃 future 并返回一个永不就绪的句柄——连接可以照常构造，
/// 但两个循环不会推进。这正是「只检查本地行为」的单元测试需要的语义。
#[derive(Clone, Copy, Default)]
pub(crate) struct NullScope_;

/// [`NullScope_`] 投递任务时给出的句柄：永不就绪。
pub(crate) struct NullHandle_<T> {
    /// 仅为携带输出类型。
    _mark_: PhantomData<fn() -> T>,
}

/// [`NullHandle_`] 的 join 错误类型（不会被构造）。
#[derive(Debug)]
pub(crate) struct NullJoinErr_;

impl core::fmt::Display for NullJoinErr_ {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("空作用域的句柄不会产出结果")
    }
}

impl core::error::Error for NullJoinErr_ {}

impl<T> Future for NullHandle_<T> {
    type Output = Result<T, NullJoinErr_>;

    fn poll(self: core::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl<T> TrJoinHandle<T> for NullHandle_<T> {
    type JoinErr = NullJoinErr_;

    fn detach(self) {}
}

impl TrLocalScope for NullScope_ {
    type Handle<T>
        = NullHandle_<T>
    where
        T: 'static;

    fn spawn_local<F>(&self, _future: F) -> Self::Handle<<F as Future>::Output>
    where
        F: Future + 'static,
        <F as Future>::Output: 'static,
    {
        NullHandle_ {
            _mark_: PhantomData,
        }
    }

    fn run_until<F>(&self, future: F) -> impl Future<Output = <F as Future>::Output>
    where
        F: Future,
    {
        future
    }

    fn block_on<F>(&self, _future: F) -> <F as Future>::Output
    where
        F: Future,
    {
        panic!("测试用的空作用域不驱动任务，不支持 block_on")
    }
}

/// 测试用资源策略：`CoreAlloc` + [`DefaultPolicy`] + 容量 64。
pub(crate) struct TestMuxConfig_;

/// [`DefaultPolicy`] 是 ZST；取静态引用即可满足 `TrMuxConfig::policy`。
static TEST_POLICY_: DefaultPolicy = DefaultPolicy;

impl TrMuxConfig for TestMuxConfig_ {
    type Buff = TestBuff;
    type Alloc = CoreAlloc;
    type Policy = DefaultPolicy;

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &TEST_POLICY_
    }

}

/// 测试连接用的两个传输类型（真实的内存环端；本连接不驱动它们，只为满足类型参数）。
pub(crate) type TestWireRx_ = BufferedRx<TestBuff, CoreAlloc>;

/// 同 [`TestWireRx_`]，写半边。
pub(crate) type TestWireTx_ = BufferedTx<TestBuff, CoreAlloc>;

/// 建一个**不含任何循环**的测试连接。
pub(crate) fn make_test_conn_() -> MuxConnection<TestWireTx_, TestWireRx_, NullScope_, TestMuxConfig_> {
    MuxConnection::new_test_(
        &NullScope_,
        HandshakeOpts {
            basic_opts: BasicOpts::default(),
        },
        TestMuxConfig_,
    )
}
