//! 连接内部两条**零堆分配**的投递原语：每子流定容内联队列与连接级合并提示。
//!
//! 它们替代「所有子流共享一条大容量事件队列」的两个角落（设计见
//! `dev-notes/mpmc-chan-20261026-0046.md` 之后的收敛方案）：
//!
//! - [`InlineQueue_`]：容量**按子流分摊**的一次性通知槽。因为「每子流至多 N 条」
//!   是协议状态机给出的常数上界，共享容量不会被并发子流数打满；槽满时生产者走
//!   [`InlineQueue_::push_async_`] 等待消费者取走（背压），而不是像旧的
//!   `Overflow_::durable_` 那样把事件推进无界 `VecDeque`。
//! - [`HintSlot_`]：把一个方向上的多种「可能变了」合并成一个 64 位掩码。多个生产者
//!   只置位、消费者一次取走全部，因此**容量恒为 1**，不存在「满」这回事。
//!
//! # 为什么不用通道
//!
//! 两条原语都内联在已经按身份分配好的节点里（[`ChannelState_`] /
//! [`TgRec_`](super::owner_::TgRec_)）：入队/出队不产生任何堆分配，也不引入新的
//! `Arc` / `Box`。这正是「减少运行时动态堆分配」这条纪律在本模块的落点。
//!
//! # 锁与唤醒
//!
//! 两个结构都用项目已有的 [`NotifySlot_`] 做持久唤醒、用自旋锁保护槽位内容：
//!
//! - 临界区只有一次 `push` / `pop`（或一次 `take`），不得 `await`、不得重入；
//! - 取 waker 与 `wake` 在 `NotifySlot_` 内部分离，不会在持锁时唤醒；
//! - 空/满的快速判断走一个**近似**的原子长度，正常路径不取锁；短暂偏差只会让调用者
//!   多取一次锁或晚一轮看到变化，而后者由持久通知位兜住。
//!
//! [`ChannelState_]: super::owner_::ChannelState_

// 过渡：两个原语在阶段 2 / 3 接入 `session_` 与 `owner_`；接入完成后删除本行。
#![allow(dead_code)]

use core::{
    future::{Future, poll_fn},
    pin::pin,
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    task::{Context, Poll},
};

use crate::{
    connection::sync_::NotifySlot_,
    x_deps::{
        abs_cancel::TrCancellationToken,
        abs_sync::may_break::TrMayBreak,
        atomic_sync::mutex::preemptive::SpinningMutexOwned,
    },
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 每子流定容内联队列
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// [`InlineQueue_`] 的异步入队失败原因。
///
/// 元素始终原样带回：失败**不吞掉**调用者交出的值（控制帧带载荷、环半部带资源）。
pub(crate) enum PushErr_<T> {
    /// 消费者已消失（连接正在收尾）：此后不会再有人取走它。
    Closed(T),

    /// 等待空位期间被取消；元素尚未入队。
    Cancelled(T),
}

/// 定容 FIFO 槽的裸环形存储（只在自旋锁内访问）。
struct SlotRing_<T, const N: usize> {
    /// 槽位；`None` 表示空闲。
    items_: [Option<T>; N],

    /// 下一个出队位置。
    head_: usize,

    /// 下一个入队位置。
    tail_: usize,

    /// 已占用槽数。
    len_: usize,
}

impl<T, const N: usize> SlotRing_<T, N> {
    /// 空环。
    fn new_() -> Self {
        SlotRing_ {
            items_: core::array::from_fn(|_| Option::None),
            head_: 0usize,
            tail_: 0usize,
            len_: 0usize,
        }
    }

    /// 入队一条；满时原样带回。
    fn push_(&mut self, item: T) -> Result<(), T> {
        if self.len_ >= N {
            return Result::Err(item);
        }
        let tail = self.tail_;
        self.items_[tail] = Option::Some(item);
        self.tail_ = (tail + 1usize) % N;
        self.len_ += 1usize;
        Result::Ok(())
    }

    /// 出队一条；空时返回 `None`。
    fn pop_(&mut self) -> Option<T> {
        if self.len_ == 0usize {
            return Option::None;
        }
        let head = self.head_;
        let item = self.items_[head].take();
        self.head_ = (head + 1usize) % N;
        self.len_ -= 1usize;
        item
    }

    /// 丢弃全部元素（关闭时用，保证带 Drop 的载荷就地释放）。
    fn clear_(&mut self) {
        for slot in self.items_.iter_mut() {
            *slot = Option::None;
        }
        self.head_ = 0usize;
        self.tail_ = 0usize;
        self.len_ = 0usize;
    }
}

/// 内联在身份节点里的**定容 FIFO 槽**（零堆分配）。
///
/// # 容量怎么定
///
/// `N` 取「该子流在协议状态机下最多可能同时挂起几条」这一**常数上界**，而不是并发
/// 子流数：
///
/// - OPEN + ACCEPT/REJECT ⇒ `N = 2`；
/// - telegraph 的环半部交接 ⇒ `N = 1`。
///
/// 因此它不会被「子流变多」打满；唯一可能等空位的情形是同一子流连续产生超过上界
/// 的条目（正常状态机不会发生，真发生时按背压处理，而不是丢）。
///
/// # 单消费者
///
/// `pop` 只允许一个消费者调用（写循环 / 读循环各自独占自己的那张表）。`push` 可以有
/// 多个生产者（API 面、ctl 循环），由内部自旋锁串行化。
pub(crate) struct InlineQueue_<T, const N: usize> {
    /// 槽位内容；只在自旋锁内访问。
    slots_: SpinningMutexOwned<SlotRing_<T, N>>,

    /// 近似占用数：`0` / `N` 时用来跳过取锁（正常路径）。
    len_hint_: AtomicUsize,

    /// 「可能有条目可出队」的持久通知（消费者）。
    ready_: NotifySlot_,

    /// 「可能腾出了空位」的持久通知（生产者背压）。
    free_: NotifySlot_,

    /// 消费者已消失：此后入队一律失败。
    closed_: AtomicBool,
}

impl<T, const N: usize> InlineQueue_<T, N> {
    /// 空槽。
    pub(crate) fn new_() -> Self {
        InlineQueue_ {
            slots_: SpinningMutexOwned::new_owned(SlotRing_::new_()),
            len_hint_: AtomicUsize::new(0usize),
            ready_: NotifySlot_::new_(),
            free_: NotifySlot_::new_(),
            closed_: AtomicBool::new(false),
        }
    }

    /// 在锁下访问槽位。
    fn with_ring_<R>(&self, f: impl FnOnce(&mut SlotRing_<T, N>) -> R) -> R {
        let mut session = self.slots_.lock_session();
        // 不可取消的自旋锁不会以取消告终（与 `NotifySlot_::with_waker_` 同款）。
        let mut guard = session
            .lock()
            .wait_or(|| unreachable!("不可取消的自旋锁不会以取消告终"));
        f(&mut guard)
    }

    /// 非阻塞入队；满时原样带回。
    ///
    /// 可在同步上下文（包括 `Drop`）调用：不 `await`、不阻塞。
    pub(crate) fn try_push_(&self, item: T) -> Result<(), T> {
        if self.closed_.load(Ordering::Acquire) {
            return Result::Err(item);
        }
        // 快路径：近似已满 ⇒ 不必取锁（满时自旋锁也只会原样带回）。
        if self.len_hint_.load(Ordering::Acquire) >= N {
            return Result::Err(item);
        }
        match self.with_ring_(|ring| ring.push_(item)) {
            Result::Ok(()) => {
                self.len_hint_.fetch_add(1usize, Ordering::Release);
                self.ready_.notify_();
                Result::Ok(())
            }
            // 近似长度偏小、锁内其实已满：原样带回。
            Result::Err(item) => Result::Err(item),
        }
    }

    /// 非阻塞出队；空时返回 `None`。
    pub(crate) fn try_pop_(&self) -> Option<T> {
        // 快路径：近似为空 ⇒ 不必取锁。多生产者下这个近似可能偏大，偏大只会多取一次
        // 锁；偏小则由 `ready_` 的持久位在 `poll_pop_` 里兜住。
        if self.len_hint_.load(Ordering::Acquire) == 0usize {
            return Option::None;
        }
        let popped = self.with_ring_(SlotRing_::pop_);
        if popped.is_some() {
            self.len_hint_.fetch_sub(1usize, Ordering::Release);
            self.free_.notify_();
        }
        popped
    }

    /// 异步出队：空时 park，消费者关闭后返回 `None`。
    pub(crate) fn poll_pop_(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if let Option::Some(item) = self.try_pop_() {
            return Poll::Ready(Option::Some(item));
        }
        if self.closed_.load(Ordering::Acquire) {
            return Poll::Ready(Option::None);
        }
        // 登记「有数据」的唤醒；持久位保证「先入队、后 park」不丢。
        if self.ready_.poll_wait_(cx).is_ready() {
            if let Option::Some(item) = self.try_pop_() {
                return Poll::Ready(Option::Some(item));
            }
            if self.closed_.load(Ordering::Acquire) {
                return Poll::Ready(Option::None);
            }
        }
        Poll::Pending
    }

    /// 异步入队：满时等待消费者取走（背压），可被取消令牌打断。
    ///
    /// # Errors
    ///
    /// - [`PushErr_::Closed`]：消费者已消失，元素原样带回；
    /// - [`PushErr_::Cancelled`]：等待期间被取消，元素尚未入队。
    pub(crate) async fn push_async_<K>(&self, item: T, cancel: K) -> Result<(), PushErr_<T>>
    where
        K: TrCancellationToken,
    {
        let mut pending = item;
        // 取消 future 只建一份、跨轮持有：每轮新建会重新登记 waker。
        let token = cancel.child_token();
        let mut cancelled = pin!(token.cancellation());
        loop {
            if cancel.is_cancelled() {
                return Result::Err(PushErr_::Cancelled(pending));
            }
            match self.try_push_(pending) {
                Result::Ok(()) => return Result::Ok(()),
                Result::Err(item) => pending = item,
            }
            if self.closed_.load(Ordering::Acquire) {
                return Result::Err(PushErr_::Closed(pending));
            }
            let woke = poll_fn(|cx| {
                if cancel.is_cancelled() {
                    return Poll::Ready(false);
                }
                if self.free_.poll_wait_(cx).is_ready() {
                    return Poll::Ready(true);
                }
                if Future::poll(cancelled.as_mut(), cx).is_ready() {
                    return Poll::Ready(false);
                }
                Poll::Pending
            })
            .await;
            if !woke {
                return Result::Err(PushErr_::Cancelled(pending));
            }
        }
    }

    /// 消费者消失：唤醒两侧等待者，丢弃槽内元素，此后入队一律失败。
    ///
    /// 幂等、不阻塞、零分配；可从任意线程调用。
    pub(crate) fn close_(&self) {
        if !self.closed_.swap(true, Ordering::AcqRel) {
            self.with_ring_(SlotRing_::clear_);
            self.len_hint_.store(0usize, Ordering::Release);
        }
        self.ready_.notify_();
        self.free_.notify_();
    }

    /// 当前近似占用数（诊断 / 断言用）。
    #[cfg(test)]
    pub(crate) fn len_(&self) -> usize {
        self.len_hint_.load(Ordering::Acquire)
    }
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
        let bits = self.take_();
        if bits != 0u64 {
            return Poll::Ready(bits);
        }
        // 持久位的「先置位、后登记」保证不丢；醒后必须复检掩码（置位与唤醒之间
        // 可能已有另一批提示被 `take_` 清走，但 `raise_` 会重新置位）。
        if self.wake_.poll_wait_(cx).is_ready() {
            let bits = self.take_();
            if bits != 0u64 {
                return Poll::Ready(bits);
            }
        }
        Poll::Pending
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 测试
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

#[cfg(test)]
mod tests_ {
    use core::{
        pin::pin,
        task::{Context, Poll, Waker},
    };
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Wake,
    };

    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use super::*;
    use crate::connection::sync_::CancelToken_;

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

    /// 测试内联队列在容量内按 FIFO 交付，取空后返回 `None`。
    /// - 手段：容量 4 的队列依次入队 0..4，再逐条出队。
    /// - 判断：出队顺序与入队顺序一致；第五次出队为 `None`。
    ///
    /// Test goal: verify FIFO delivery within capacity.
    /// Method: push 0..4 into a capacity-4 queue, then pop one by one.
    /// Assertion: popped order matches push order; the fifth pop is `None`.
    #[test]
    fn inline_queue_is_fifo_within_capacity() {
        let queue = InlineQueue_::<u32, 4>::new_();
        for value in 0u32..4u32 {
            assert!(queue.try_push_(value).is_ok(), "容量内入队不应当失败");
        }
        for value in 0u32..4u32 {
            assert_eq!(queue.try_pop_(), Option::Some(value));
        }
        assert_eq!(queue.try_pop_(), Option::None);
        assert_eq!(queue.len_(), 0usize);
    }

    /// 测试内联队列满时原样带回，不覆盖已有元素。
    /// - 手段：容量 2 的队列入队 1、2，再入队 3。
    /// - 判断：第三次入队返回 `Err(3)`；出队仍是 1、2。
    ///
    /// Test goal: verify a full inline queue rejects without overwriting.
    /// Method: fill a capacity-2 queue with 1 and 2, then push 3.
    /// Assertion: the third push returns `Err(3)`; pops still yield 1 and 2.
    #[test]
    fn inline_queue_rejects_when_full() {
        let queue = InlineQueue_::<u32, 2>::new_();
        assert!(queue.try_push_(1u32).is_ok());
        assert!(queue.try_push_(2u32).is_ok());
        assert!(matches!(queue.try_push_(3u32), Result::Err(3u32)));
        assert_eq!(queue.try_pop_(), Option::Some(1u32));
        assert_eq!(queue.try_pop_(), Option::Some(2u32));
    }

    /// 测试异步入队在满时 park，并在消费者取走一条后完成（背压而不是丢弃）。
    /// - 手段：容量 2 的队列填满；建 `push_async_` future 并 poll 一次（应 `Pending`）；
    ///   取走一条后再次 poll。
    /// - 判断：首次为 `Pending`；取走后 future 就绪且 `Ok`；最终队列里能按序取到三条。
    ///
    /// Test goal: verify async push parks when full and completes after a pop
    /// (backpressure instead of dropping).
    /// Method: fill a capacity-2 queue, poll the `push_async_` future once (expect
    /// `Pending`), pop one item, then poll again.
    /// Assertion: first poll is `Pending`; after the pop the future is `Ready(Ok)`;
    /// the three values can then be popped in order.
    async fn inline_queue_push_async_waits_for_free_slot() {
        let queue = InlineQueue_::<u32, 2>::new_();
        assert!(queue.try_push_(1u32).is_ok());
        assert!(queue.try_push_(2u32).is_ok());

        let token = CancelToken_::new_(CoreAlloc);
        let (waker, _probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);
        let mut wait = pin!(queue.push_async_(3u32, token));
        assert!(wait.as_mut().poll(&mut context).is_pending());

        assert_eq!(queue.try_pop_(), Option::Some(1u32));
        assert!(matches!(
            wait.as_mut().poll(&mut context),
            Poll::Ready(Result::Ok(()))
        ));
        assert_eq!(queue.try_pop_(), Option::Some(2u32));
        assert_eq!(queue.try_pop_(), Option::Some(3u32));
    }
    multi_art_test_!(inline_queue_push_async_waits_for_free_slot);

    /// 测试等待空位期间取消：元素不入队，且原样带回。
    /// - 手段：容量 1 的队列填 1；建 `push_async_(2, token)` 并 poll 一次（应
    ///   `Pending`）；触发 `cancel_()` 后再次 poll。
    /// - 判断：取消触发唤醒计数；future 返回 `PushErr_::Cancelled(2)`；队列里只有 1。
    ///
    /// Test goal: verify a cancelled async push leaves the queue untouched and
    /// hands the value back.
    /// Method: fill a capacity-1 queue with 1; poll `push_async_(2, token)` once
    /// (expect `Pending`); trigger `cancel_()` and poll again.
    /// Assertion: cancel wakes the waiter; the future returns `Cancelled(2)`;
    /// the queue still holds only 1.
    async fn inline_queue_push_async_can_be_cancelled() {
        let queue = InlineQueue_::<u32, 1>::new_();
        assert!(queue.try_push_(1u32).is_ok());

        let token = CancelToken_::new_(CoreAlloc);
        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);
        let mut wait = pin!(queue.push_async_(2u32, token.clone()));
        assert!(wait.as_mut().poll(&mut context).is_pending());

        token.cancel_();
        assert!(
            probe.count_.load(Ordering::SeqCst) >= 1usize,
            "取消应当唤醒等待空位的生产者"
        );
        assert!(matches!(
            wait.as_mut().poll(&mut context),
            Poll::Ready(Result::Err(PushErr_::Cancelled(2u32)))
        ));
        assert_eq!(queue.try_pop_(), Option::Some(1u32));
        assert_eq!(queue.try_pop_(), Option::None);
    }
    multi_art_test_!(inline_queue_push_async_can_be_cancelled);

    /// 测试关闭唤醒空队列上的消费者，且此后入队失败。
    /// - 手段：空队列上 poll `poll_pop_` 一次（应 `Pending`）；调用 `close_()` 后再 poll。
    /// - 判断：关闭唤醒消费者；再次 poll 返回 `Ready(None)`；`try_push_` 原样带回。
    ///
    /// Test goal: verify closing wakes a parked consumer and rejects further pushes.
    /// Method: poll `poll_pop_` on an empty queue (expect `Pending`); call `close_()`;
    /// poll again.
    /// Assertion: close wakes the consumer; the next poll is `Ready(None)`;
    /// `try_push_` hands the value back.
    async fn inline_queue_close_wakes_consumer() {
        let queue = InlineQueue_::<u32, 2>::new_();
        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);
        assert!(queue.poll_pop_(&mut context).is_pending());

        queue.close_();
        assert!(
            probe.count_.load(Ordering::SeqCst) >= 1usize,
            "关闭应当唤醒正在等数据的消费者"
        );
        assert!(matches!(
            queue.poll_pop_(&mut context),
            Poll::Ready(Option::None)
        ));
        assert!(matches!(queue.try_push_(7u32), Result::Err(7u32)));
    }
    multi_art_test_!(inline_queue_close_wakes_consumer);

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
