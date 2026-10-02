//! 上行通知通道：**多生产者（所有 `ChannelOwner`）→ 单消费者（中心循环）**。
//!
//! 应用从接收环取走数据后，接收窗口可以回补；owner 用这条通道把「这条子流的窗口
//! 记账有变化」告诉中心循环，由后者自己去查接收环的剩余空间、并按其策略决定何时向
//! 对端发窗口通告（通告时机见 [`crate::flow_ctrl::RecvWindow::should_report`]）。
//!
//! 通道是**提示**而非指令：消息只带子流身份，不带字节数、更不带数据本体。
//!
//! # 实现：`flume::bounded`
//!
//! 通道本体用 [`flume`]（见 dev-notes §11.4.1 的选型记录）：`flume::bounded(capacity)`
//! 给出的 `Sender` 可克隆（= 多生产者）、`Receiver` 只交给我方一处（= 单消费者），
//! 并自带 `try_send` / `recv_async` 与消费者唤醒。
//!
//! 契约由 [`TrSignalSender_`] / [`TrSignalReceiver_`] 固定：
//!
//! - 投递**不阻塞**（[`TrSignalSender_::try_send_signal_`]）：中心循环是唯一消费者，
//!   绝不可以在应用侧的读路径上等待；队列满时返回 `false`，由调用方走「全扫」兜底；
//! - 取出有**非阻塞**与**异步**两种：前者让中心循环在每轮循环里顺手清空积压，后者让
//!   它在没有事件时 park，直到某条通知到达（由取消令牌负责让它能被打断）。
//!
//! # 为什么「重复投递」是安全的
//!
//! 每条子流在自己的共享状态里有一个「已入队」位，只有 `0→1` 时才真正投递，因此
//! 队列里每条子流至多一条；**被丢掉的只可能是重复项**。这条约束由调用方（owner）
//! 保证，本模块只负责运输。

// 本模块的入口尚未被中心循环调用（第 6 步接线），因此这里保留 `dead_code` 允许；
// **接线完成后必须移除本行**。
#![allow(dead_code, unused_imports)]

use core::future::Future;

use flume::{Receiver, Sender};
use flume::TrySendError;

use crate::connection::Dock;

/// 一条上行通知：某条子流的接收窗口记账有变化。
///
/// 只带子流身份（dock 对）——窗口数值由中心循环自己查，见模块文档。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Signal_ {
    /// 本端 dock。
    pub(crate) local_dock: Dock,

    /// 对端 dock。
    pub(crate) remote_dock: Dock,
}

impl Signal_ {
    /// 以 dock 对构造。
    pub(crate) const fn new(local_dock: Dock, remote_dock: Dock) -> Self {
        Signal_ {
            local_dock,
            remote_dock,
        }
    }
}

/// 通知的生产端；可克隆，任意 `ChannelOwner` 各持一个。
pub(crate) trait TrSignalSender_ {
    /// 非阻塞投递。
    ///
    /// 返回 `false` 表示没送出去（队列满，或消费者已经没了）：调用方应当置连接级
    /// 「全扫」标志，让中心循环扫一遍注册表，保证这次消费不被漏掉。
    fn try_send_signal_(&self, signal: Signal_) -> bool;
}

/// 通知的消费端；中心循环独占。
pub(crate) trait TrSignalReceiver_ {
    /// 非阻塞取一条；没有积压时返回 `None`。
    fn try_take_signal_(&mut self) -> Option<Signal_>;

    /// 异步取一条：没有积压时 park，直到有通知到达。
    ///
    /// 生产端全部消失（通道断开）时返回 `None`。调用方应当把这个 future 放在取消
    /// 令牌的 `select` 里，连接关闭时才不会永远 park。
    fn take_signal_async_(&mut self) -> impl Future<Output = Option<Signal_>> + '_;
}

/// 生产端（`flume` 的 `Sender` 可克隆 ⇒ 多生产者）。
pub(crate) struct SignalSender_ {
    tx_: Sender<Signal_>,
}

impl Clone for SignalSender_ {
    fn clone(&self) -> Self {
        SignalSender_ {
            tx_: self.tx_.clone(),
        }
    }
}

/// 消费端（`Receiver` 不对外克隆 ⇒ 单消费者）。
pub(crate) struct SignalReceiver_ {
    rx_: Receiver<Signal_>,
}

/// 建一对定容通知通道。
pub(crate) fn signal_channel_(capacity: usize) -> (SignalSender_, SignalReceiver_) {
    let (tx_, rx_) = flume::bounded(capacity);
    (SignalSender_ { tx_ }, SignalReceiver_ { rx_ })
}

impl TrSignalSender_ for SignalSender_ {
    fn try_send_signal_(&self, signal: Signal_) -> bool {
        match self.tx_.try_send(signal) {
            Result::Ok(()) => true,
            // 队列满：由调用方走「全扫」兜底；
            // 消费者已消失（连接正在收尾）：同样按「没送出去」处理。
            Result::Err(TrySendError::Full(_)) | Result::Err(TrySendError::Disconnected(_)) => {
                false
            }
        }
    }
}

impl TrSignalReceiver_ for SignalReceiver_ {
    fn try_take_signal_(&mut self) -> Option<Signal_> {
        self.rx_.try_recv().ok()
    }

    async fn take_signal_async_(&mut self) -> Option<Signal_> {
        self.rx_.recv_async().await.ok()
    }
}

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

    /// 测试通道的定容语义：满时非阻塞投递返回 `false`，取走一条后又能投。
    /// - 手段：容量 4 的通道连投 5 条，然后把 4 条全部取走再来一轮。
    /// - 判断：前 4 条投递成功、第 5 条失败；取出的 4 条与投递顺序一致且再取为
    ///   `None`；随后第 5 条能投进去。
    #[test]
    fn signal_band_is_bounded_and_fifo() {
        let (sender, mut receiver) = signal_channel_(4usize);
        let a = Signal_::new(Dock::new(1u32), Dock::new(2u32));
        let b = Signal_::new(Dock::new(3u32), Dock::new(4u32));
        let c = Signal_::new(Dock::new(5u32), Dock::new(6u32));
        let d = Signal_::new(Dock::new(7u32), Dock::new(8u32));
        let e = Signal_::new(Dock::new(9u32), Dock::new(10u32));

        assert!(sender.try_send_signal_(a));
        assert!(sender.try_send_signal_(b));
        assert!(sender.try_send_signal_(c));
        assert!(sender.try_send_signal_(d));
        assert!(
            !sender.try_send_signal_(e),
            "容量 4 已满：非阻塞投递必须返回 false 而不是等待"
        );

        assert_eq!(receiver.try_take_signal_(), Option::Some(a));
        assert_eq!(receiver.try_take_signal_(), Option::Some(b));
        assert_eq!(receiver.try_take_signal_(), Option::Some(c));
        assert_eq!(receiver.try_take_signal_(), Option::Some(d));
        assert_eq!(receiver.try_take_signal_(), Option::None);

        assert!(sender.try_send_signal_(e), "腾出槽位后应当能再投");
        assert_eq!(receiver.try_take_signal_(), Option::Some(e));
    }

    /// 测试空通道上异步取出会 park，并在投递时被唤醒。
    /// - 手段：先把 `take_signal_async_` 的 future 建好并 poll 一次（此时应
    ///   `Pending`、唤醒计数为 0），再 `try_send_signal_`，最后再 poll。
    /// - 判断：第一次 `Pending`；投递后唤醒计数增加（登记中的 waker 被唤醒）；
    ///   再次 poll 返回 `Ready(Some(..))`。
    #[compio::test]
    async fn signal_async_take_parks_until_send() {
        let (sender, mut receiver) = signal_channel_(4usize);
        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);

        let signal = Signal_::new(Dock::new(11u32), Dock::new(12u32));
        let mut wait = pin!(receiver.take_signal_async_());
        assert_eq!(wait.as_mut().poll(&mut context), Poll::Pending);
        assert_eq!(probe.count_.load(Ordering::SeqCst), 0usize);

        assert!(sender.try_send_signal_(signal));
        assert!(
            probe.count_.load(Ordering::SeqCst) >= 1usize,
            "投递应当唤醒正在 park 的消费者"
        );
        assert_eq!(
            wait.as_mut().poll(&mut context),
            Poll::Ready(Option::Some(signal))
        );
    }

    /// 测试已有积压时异步取出立刻就绪，不产生多余唤醒。
    /// - 手段：先投一条，再建 future 并 poll。
    /// - 判断：首次 poll 即 `Ready(Some(..))`，唤醒计数保持 0。
    #[compio::test]
    async fn signal_async_take_is_immediate_with_backlog() {
        let (sender, mut receiver) = signal_channel_(4usize);
        let signal = Signal_::new(Dock::new(13u32), Dock::new(14u32));
        assert!(sender.try_send_signal_(signal));

        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);
        let mut wait = pin!(receiver.take_signal_async_());
        assert_eq!(
            wait.as_mut().poll(&mut context),
            Poll::Ready(Option::Some(signal))
        );
        assert_eq!(probe.count_.load(Ordering::SeqCst), 0usize);
    }

    /// 测试生产端可以克隆成多个（多生产者），任一克隆投递都能被同一个消费者取到。
    /// - 手段：把 sender 克隆成两份，各投一条。
    /// - 判断：两条都能取到。
    #[test]
    fn signal_band_supports_multiple_producers() {
        let (sender, mut receiver) = signal_channel_(4usize);
        let second = sender.clone();
        let a = Signal_::new(Dock::new(21u32), Dock::new(22u32));
        let b = Signal_::new(Dock::new(23u32), Dock::new(24u32));

        assert!(sender.try_send_signal_(a));
        assert!(second.try_send_signal_(b));
        assert_eq!(receiver.try_take_signal_(), Option::Some(a));
        assert_eq!(receiver.try_take_signal_(), Option::Some(b));
    }
}
