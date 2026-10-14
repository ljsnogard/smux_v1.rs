//! `next_async` 的返回类型：一个**类型擦除 + 可取消**的 future 包装。
//!
//! # 为什么需要它
//!
//! [`TrAsyncIterator::NextAsync`](abs_async_iter::TrAsyncIterator::NextAsync) 要求返回一个
//! **可命名**的类型。而 `next_async` 的等待体既长又与泛型参数纠缠，直接手写它的状态机
//! 不现实；[`gen_may_cancel_future`](gen_mcf2::gen_may_cancel_future) 又要求「取消令牌
//! 类型参数不得出现在其它参数类型中」，而 listener 的每个字段（配置、入向适配……）都要
//! 参与这次等待。
//!
//! 于是这里做一次**类型擦除**：等待体写成一条普通 `async` 块，装箱成
//! `Pin<Box<dyn Future>>`，再由本类型补上 [`TrMayCancel`] 的实现。
//!
//! # 代价
//!
//! 每次 `next_async` 一次堆分配。相对「接受一条连接 + 两个 RTT 的握手 + 建五个循环」，
//! 这一次分配可以忽略；换来的是类型签名可以写清楚。
//!
//! # 取消语义
//!
//! [`NextWait_::may_cancel_with`] 把等待体与取消信号竞速：令牌触发时立即以
//! `Err(cancelled)` 结束。**丢弃**这个 future（而不是取消它）同样安全——它不持有任何
//! 必须归还的资源。

use alloc::sync::Arc;
use core::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use crate::x_deps::abs_cancel::{TrCancellationToken, TrMayCancel};

/// 待决的 `next_async`：一条装箱的等待体 + 两个标记类型参数。
///
/// # 泛型参数
///
/// | 参数 | 含义 |
/// | --- | --- |
/// | `'f` | 等待体借用的生命周期（`next_async` 里就是 `&mut self` 那次借用） |
/// | `F` | 成功时交付的条目类型 |
/// | `E` | 失败类型 |
/// | `M` | 标记：让每种取消令牌类型对应一个**不同**的 `NextAsync` 类型 |
pub struct NextWait_<'f, F, E, M> {
    /// 等待体。
    inner_: Pin<Box<dyn Future<Output = Result<F, E>> + 'f>>,

    /// 取消时必须给出的错误值。
    ///
    /// 之所以**预存**而不是在取消时现造：`E` 的构造方式由调用方决定（本 crate 里就是
    /// [`SmuxListenerError::Cancelled`](crate::error::SmuxListenerError::Cancelled)），
    /// 等待体自己没有权利假定它可构造。
    ///
    /// 用 [`Arc`] 包着是为了让 [`TrMayCancel::may_cancel_with`] 能在**不要求 `E: Clone`**
    /// 的前提下把同一个值交给新包装：取消只可能发生一次，因此多一个句柄不会造成两次
    /// 消费。
    on_cancel_: Arc<E>,

    /// 类型占位。
    _mark_: PhantomData<fn() -> M>,
}

/// 从预存的错误值造出「取消」错误。
///
/// 错误类型 `E` 必须实现 `Clone` 才能这样用——本 crate 里它是
/// [`SmuxListenerError`](crate::error::SmuxListenerError)，是一个零大小的枚举，
/// `Clone` 不产生任何开销。
fn clone_cancelled_<E>(err: &Arc<E>) -> E
where
    E: Clone,
{
    (**err).clone()
}

impl<'f, F, E, M> NextWait_<'f, F, E, M> {
    /// 由一条等待体与「取消时给出的错误」构造。
    pub(crate) fn new_(
        inner: Pin<Box<dyn Future<Output = Result<F, E>> + 'f>>,
        on_cancel: E,
    ) -> Self {
        NextWait_ {
            inner_: inner,
            on_cancel_: Arc::new(on_cancel),
            _mark_: PhantomData,
        }
    }
}

impl<'f, F, E, M> Future for NextWait_<'f, F, E, M>
where
    M: 'f,
{
    type Output = Result<F, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: 本类型不实现 `Drop`，也不对外提供任何 `Pin` 投影 ⇒ 没有 structural
        // pinning；因此把 `&mut Self` 取出来只用于借用字段是安全的（`inner_` 自身仍以
        // `Pin<Box<_>>` 的形式被 poll）。
        let this = unsafe { self.get_unchecked_mut() };
        this.inner_.as_mut().poll(cx)
    }
}

impl<'a, F, E, M> TrMayCancel<'a> for NextWait_<'a, F, E, M>
where
    F: 'a,
    E: Clone + 'a,
    M: 'a,
{
    type MayCancelOutput = Result<F, E>;

    type MayCancelFuture<'f, C>
        = NextWait_<'f, F, E, (M, C)>
    where
        'f: 'a,
        Self: 'f,
        C: 'f + TrCancellationToken;

    fn may_cancel_with<C>(self, cancel: C) -> Self::MayCancelFuture<'a, C>
    where
        C: 'a + TrCancellationToken,
    {
        // 把等待体与取消信号合成一条新的等待体：谁先就绪谁决定结果。
        let NextWait_ {
            inner_, on_cancel_, ..
        } = self;
        let new_inner: Pin<Box<dyn Future<Output = Result<F, E>> + 'a>> = Box::pin({
            let on_cancel = on_cancel_.clone();
            async move {
                let mut inner = inner_;
                let stopped = cancel.cancellation();
                let mut stopped = core::pin::pin!(stopped);
                core::future::poll_fn(move |cx| {
                    if let Poll::Ready(out) = inner.as_mut().poll(cx) {
                        return Poll::Ready(out);
                    }
                    if stopped.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(Result::Err(clone_cancelled_(&on_cancel)));
                    }
                    Poll::Pending
                })
                .await
            }
        });
        NextWait_ {
            inner_: new_inner,
            on_cancel_,
            _mark_: PhantomData,
        }
    }
}
