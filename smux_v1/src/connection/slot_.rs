//! 连接内部**零堆分配**的投递原语：合并提示位与请求通道的失败类型。
//!
//! 本模块只剩两件东西：
//!
//! - [`PushErr_`]：请求通道（`mpmc_chan`）的异步入队失败原因。通道本身由
//!   `mpmc_chan` 提供（等待者是 chunked `SlabList`，Arena 复用），这里只保留调用方
//!   要匹配的错误形状；
//! - [`HintSlot_`]：把一个方向上的多种「可能变了」合并成一个 64 位掩码。多个生产者
//!   只置位、消费者一次取走全部，因此**容量恒为 1**，不存在「满」这回事。
//!
//! 原来的 `InlineQueue_` / `SlotRing_` / `WakerSlots_` 已被 `mpmc_chan` 取代
//! （请求队列与两条 `Attach` inbox），`WakerSlots_ + Busy` 方案随之搁置；见
//! `dev-notes/event-loop-redesign-20261026-1930.md` 的 20261027-1400 / 1500 两条。

use core::{
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
};

use crate::connection::sync_::NotifySlot_;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 请求通道错误
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 请求通道（`mpmc_chan`）的异步入队失败原因。
///
/// 元素始终原样带回：失败**不吞掉**调用者交出的值（控制帧带载荷、环半部带资源）。
/// 由 `mpmc_chan` 的 `SendError` 映射而来（见 `ControlState_::submit_async_`）。
pub(crate) enum PushErr_<T> {
    /// 消费者已消失（连接正在收尾）：此后不会再有人取走它。
    Closed(T),

    /// 等待空位期间被取消；元素尚未入队。
    Cancelled(T),
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 连接级合并提示
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一个方向上的**合并提示**：把多种「可能变了」压成一个 64 位掩码。
///
/// 多个生产者只做一次 `fetch_or` + 唤醒；消费者一次 `swap(0)` 取走全部。因此：
///
/// - 没有队列、没有容量、没有「满」；
/// - 同一位被重复置位是幂等的（这正是「提示」与「事件」的区别）；
/// - 消费者拿到掩码后**自己决定**要扫什么（写循环全表排空、读循环按位重算窗口）。
///
/// 位定义由使用者（`signal_` / `session_`）给出，本类型只搬运掩码。
pub(crate) struct HintSlot_ {
    /// 待消费的提示位。
    pending_: AtomicU64,

    /// 唤醒消费者的持久通知槽。
    wake_: NotifySlot_,
}

impl HintSlot_ {
    /// 空提示。
    pub(crate) fn new_() -> Self {
        HintSlot_ {
            pending_: AtomicU64::new(0u64),
            wake_: NotifySlot_::new_(),
        }
    }

    /// 置位若干提示并唤醒消费者；幂等、不阻塞、零分配。
    pub(crate) fn raise_(&self, bits: u64) {
        if bits == 0u64 {
            return;
        }
        self.pending_.fetch_or(bits, Ordering::Release);
        self.wake_.notify_();
    }

    /// 取走并清除当前全部提示位；没有提示时返回 `0`。
    pub(crate) fn take_(&self) -> u64 {
        self.pending_.swap(0u64, Ordering::AcqRel)
    }

    /// 等下一次提示：已有提示时立刻返回，否则登记唤醒。
    pub(crate) fn poll_wait_(&self, cx: &mut Context<'_>) -> Poll<u64> {
        // 与 `NotifySlot_` 的持久位协议同理：消费之后若掩码仍为空，必须重新登记
        // waker，不能直接返回 `Pending`。
        loop {
            let bits = self.take_();
            if bits != 0u64 {
                return Poll::Ready(bits);
            }
            // 持久位的「先置位、后登记」保证不丢；醒后必须复检掩码（置位与唤醒之间
            // 可能已有另一批提示被 `take_` 清走，但 `raise_` 会重新置位）。
            if self.wake_.poll_wait_(cx).is_ready() {
                continue;
            }
            return Poll::Pending;
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 测试
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

#[cfg(test)]
mod tests_ {
    use core::task::{Context, Poll, Waker};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Wake,
    };

    use super::*;

    /// 计数唤醒器：统计 `wake` 次数，用来断言「确实被唤醒」而不只是断言状态。
    struct CountingWake_ {
        count_: AtomicUsize,
    }

    impl Wake for CountingWake_ {
        fn wake(self: Arc<Self>) {
            self.count_.fetch_add(1usize, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.count_.fetch_add(1usize, Ordering::SeqCst);
        }
    }

    /// 造一个带计数的 `Waker`。
    fn counting_waker_() -> (Waker, Arc<CountingWake_>) {
        let probe = Arc::new(CountingWake_ {
            count_: AtomicUsize::new(0),
        });
        (Waker::from(probe.clone()), probe)
    }

    /// 测试合并提示把多次置位压成一个掩码，取走即清零。
    /// - 手段：连续置位 bit0 与 bit2（其中 bit0 重复两次），再取两次。
    /// - 判断：第一次取到 `0b101`；第二次取到 `0`（重复置位不堆积）。
    ///
    /// Test goal: verify coalesced hints collapse into one mask and are cleared on take.
    /// Method: raise bit0 and bit2 (bit0 twice), then take twice.
    /// Assertion: the first take yields `0b101`; the second yields `0` (duplicates
    /// do not accumulate).
    #[test]
    fn hint_slot_coalesces_and_clears() {
        let slot = HintSlot_::new_();
        slot.raise_(0b001u64);
        slot.raise_(0b001u64);
        slot.raise_(0b100u64);
        assert_eq!(slot.take_(), 0b101u64);
        assert_eq!(slot.take_(), 0u64);
    }

    /// 测试合并提示唤醒正在等待的消费者。
    /// - 手段：空提示上 poll `poll_wait_` 一次（应 `Pending`）；置位后再次 poll。
    /// - 判断：首次 `Pending` 且未唤醒；置位后唤醒计数增加；再次 poll 得到掩码。
    ///
    /// Test goal: verify a coalesced hint wakes a parked consumer.
    /// Method: poll `poll_wait_` on an empty slot (expect `Pending`); raise bits;
    /// poll again.
    /// Assertion: first poll is `Pending` with no wake; raising wakes the waiter;
    /// the next poll yields the mask.
    async fn hint_slot_wakes_waiter() {
        let slot = HintSlot_::new_();
        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);

        assert!(slot.poll_wait_(&mut context).is_pending());
        assert_eq!(probe.count_.load(Ordering::SeqCst), 0usize);

        slot.raise_(0b11u64);
        assert!(
            probe.count_.load(Ordering::SeqCst) >= 1usize,
            "置位应当唤醒正在等待的消费者"
        );
        assert_eq!(slot.poll_wait_(&mut context), Poll::Ready(0b11u64));
    }
    multi_art_test_!(hint_slot_wakes_waiter);
}
