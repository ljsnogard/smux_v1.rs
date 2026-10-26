//! 控制面共享状态：定容请求队列 + `main_loop` 的合并提示位。
//!
//! API future 把一次非荷载请求（bind / listen / reserve …）连同完成源塞进
//! [`ControlState_::submit_async_`]；`main_loop` 从另一端
//! [`ControlState_::poll_pop_request_`] 取走并处理。队列是**定容内联**的
//! （[`K_MAIN_REQ_CAP`]），满时生产者按背压等待空位。
//!
//! 提示位与请求分开：`HINT_MAIN_*` 只表示「某类事实可能变了」，`main_loop` 一次取走
//! 整个掩码后自己决定扫描什么。P1 只用到队列本身；提示位留给后续阶段的 retired /
//! window-dirty / timer（见设计稿 §6.1 / §8.4）。

use core::{
    alloc::AllocatorClone,
    task::{Context, Poll},
};

use abs_cancel::TrCancellationToken;
use mm_ptr::Shared;

use crate::{
    connection::{
        main_loop::MainRequest_,
        slot_::{HintSlot_, InlineQueue_, PushErr_},
    },
    x_deps::{abs_cancel, mm_ptr},
};

/// 请求队列容量（设计稿 §10.2：64 条，约 8 KiB）。
pub(crate) const K_MAIN_REQ_CAP: usize = 64usize;

/// 「有身份被 `Drop` 置了 retired 位」：`main_loop` 扫描并释放。
pub(crate) const HINT_MAIN_RETIRED: u64 = 1u64 << 0usize;

/// 控制面共享状态句柄（`MuxCore` 与 `main_loop` 各持一份）。
pub(crate) struct ControlState_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 共享本体；一次分配，句柄可克隆。
    inner_: Shared<ControlStateInner_<A>, A>,
}

impl<A> Clone for ControlState_<A>
where
    A: AllocatorClone + Send + Sync,
{
    fn clone(&self) -> Self {
        ControlState_ {
            inner_: self.inner_.clone(),
        }
    }
}

/// 共享本体的内容。
struct ControlStateInner_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 定容请求队列（单消费者 = `main_loop`）。
    requests_: InlineQueue_<MainRequest_<A>, K_MAIN_REQ_CAP>,

    /// `main_loop` 的合并提示位（`HINT_MAIN_*`）。
    main_hint_: HintSlot_,
}

impl<A> ControlState_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 建一个空的请求队列与提示位（一次 `Shared` 分配）。
    pub(crate) fn new_(alloc: A) -> Self {
        ControlState_ {
            inner_: Shared::new(
                ControlStateInner_ {
                    requests_: InlineQueue_::new_(),
                    main_hint_: HintSlot_::new_(),
                },
                alloc,
            ),
        }
    }

    /// 提交一个请求；队列满时等待空位（可被取消）。
    ///
    /// # Errors
    ///
    /// - [`PushErr_::Closed`]：`main_loop` 已退出（连接正在收尾）；
    /// - [`PushErr_::Cancelled`]：等空位期间被取消；请求原样带回。
    pub(crate) async fn submit_async_<K>(
        &self,
        request: MainRequest_<A>,
        cancel: K,
    ) -> Result<(), PushErr_<MainRequest_<A>>>
    where
        K: TrCancellationToken,
    {
        self.inner_.requests_.push_async_(request, cancel).await
    }

    /// 非阻塞取一个请求（`main_loop` 的批处理用）。
    pub(crate) fn try_pop_request_(&self) -> Option<MainRequest_<A>> {
        self.inner_.requests_.try_pop_()
    }

    /// `main_loop` 的 park 点：有请求 / 队列关闭时返回。
    pub(crate) fn poll_pop_request_(&self, cx: &mut Context<'_>) -> Poll<Option<MainRequest_<A>>> {
        self.inner_.requests_.poll_pop_(cx)
    }

    /// 关闭请求队列：此后 [`Self::submit_async_`] 一律返回 [`PushErr_::Closed`]，
    /// 消费者会看到 `Ready(None)`。
    pub(crate) fn close_requests_(&self) {
        self.inner_.requests_.close_();
    }

    /// 置位若干 `main_loop` 提示并唤醒它；幂等、零分配。
    ///
    pub(crate) fn raise_main_hint_(&self, bits: u64) {
        self.inner_.main_hint_.raise_(bits);
    }

    /// 取走并清除当前全部提示位。
    pub(crate) fn take_main_hint_(&self) -> u64 {
        self.inner_.main_hint_.take_()
    }

    /// `main_loop` 的 park 点：有提示时返回掩码。
    pub(crate) fn poll_main_hint_(&self, cx: &mut Context<'_>) -> Poll<u64> {
        self.inner_.main_hint_.poll_wait_(cx)
    }
}

#[cfg(test)]
mod tests_ {
    use abs_cancel::NonCancellableToken;
    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use super::*;
    use crate::connection::{Dock, Tcs_};

    /// 测试请求队列的 FIFO 与关闭语义。
    /// - 手段：依次提交两条 `Bind`（dock 1 / 2），逐条 `try_pop_request_`；随后
    ///   `close_requests_()` 再提交第三条。
    /// - 判断：两次 pop 的 dock 依次为 1 / 2；关闭后的提交返回 `PushErr_::Closed`。
    ///
    /// Test goal: the request queue is FIFO and rejects submissions after close.
    /// Method: submit two `Bind` requests (docks 1 and 2) and pop them one by one;
    /// then `close_requests_()` and submit a third.
    /// Assertion: pops yield docks 1 then 2; the third submit is `PushErr_::Closed`.
    #[test]
    fn control_state_is_fifo_and_closes_() {
        let ctl = ControlState_::<CoreAlloc>::new_(CoreAlloc);
        for dock in [1u32, 2u32] {
            let reply = Tcs_::new_(CoreAlloc);
            assert!(
                futures::executor::block_on(ctl.submit_async_(
                    MainRequest_::Bind {
                        local_dock: Dock::new(dock),
                        reply,
                    },
                    NonCancellableToken::new(),
                ))
                .is_ok(),
                "空队列应当立刻入队"
            );
        }
        for expected in [1u32, 2u32] {
            match ctl.try_pop_request_() {
                Option::Some(MainRequest_::Bind { local_dock, .. }) => {
                    assert_eq!(local_dock, Dock::new(expected));
                }
                _ => panic!("应当取到第 {expected} 条 Bind 请求"),
            }
        }
        assert!(ctl.try_pop_request_().is_none());

        ctl.close_requests_();
        let reply = Tcs_::new_(CoreAlloc);
        let err = futures::executor::block_on(ctl.submit_async_(
            MainRequest_::Bind {
                local_dock: Dock::new(3u32),
                reply,
            },
            NonCancellableToken::new(),
        ))
        .expect_err("关闭后应当拒绝入队");
        assert!(matches!(err, PushErr_::Closed(_)));
    }
}
