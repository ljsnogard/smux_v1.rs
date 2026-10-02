//! 连接的**事件通道**：中心循环与 API 面之间的唯一异步消息通路。
//!
//! 本模块在「演员核心 + 智能指针」架构下**保留**：需求怎么投递不是设计纪律，而
//! 水位这类高频提示走一次性通道恰好合适（见 [`crate::connection`] 模块文档 §2.3）。
//! 待办的优化是把**无界**通道换成**定容**通道（减少分配、仍受 `flume` 不支持
//! `allocator_api` 的限制），需要「队列满 → 置连接级全扫标志」的兜底机制，
//! 因此留待实测的竞争与分配数据再定。
//! 本模块在早先的「上行窗口通知」（见 `connection-20260919-1631.md` §11.4）基础
//! 上扩展成两条**带类型负载**的事件通道：
//!
//! - [`WriteEvent_`] → **写循环**：会话侧发送环读端上线（
//!   [`WriteEvent_::Attach`]）、待写出的控制帧（[`WriteEvent_::Control`]）、
//!   「某条子流发送环有数据」（[`WriteEvent_::TxReady`]）、应用消费了接收数据
//!   （[`WriteEvent_::RxConsumed`]）、拆流（[`WriteEvent_::Release`]）；
//! - [`ReadEvent_`] → **读循环**：会话侧接收环写端上线（[`ReadEvent_::Attach`]）、
//!   拆流（[`ReadEvent_::Release`]）。
//!
//! # 为什么是**无界**的（本轮裁决 Q6）
//!
//! 定容通道需要「每条子流一个已入队位 + 队列满置连接级全扫标志」这套兜底，才能
//! 保证通知不丢；而本轮新增的两类消息**不可重建**：「发送环有数据」丢了就是那条
//! channel 卡死，控制帧丢了就是建流永远完不成。因此改为**无界通道**，靠两条约束
//! 让队列长度天然有界：
//!
//! 1. 每条子流的「有数据」事件至多一条在队列里（由
//!    [`ChannelState_::tx_queued_`](super::owner_::ChannelState_::tx_queued_) 去重）；
//! 2. 控制帧与拆流事件的产生频率由协议状态机约束（每条子流建流 / 拆流各一次）。
//!
//! 于是 `try_send` 不会失败，「全扫标志」整个机制不再需要。这与 §11.4 的「定容」
//! 结论不同，属**本轮修订**，已记入 `connection-20261002-0548.md` §5。
//!
//! # 为什么不是一条通道
//!
//! 会话侧的两个环半部必须分别交给**不同**的循环：发送环的读端进写循环，接收环的
//! 写端进读循环。若只用一条通道，两个循环会互相取走对方的 `Attach` 事件。因此按
//! 消费者拆成两条。

use core::{
    alloc::AllocatorClone,
    borrow::BorrowMut,
    future::Future,
    mem::MaybeUninit,
};

use flume::{Receiver, Sender, TrySendError};

use crate::{
    connection::{
        Dock, FrameKind,
        owner_::ChannelOwner_,
        ring_::{BufferedRx, BufferedTx},
    },
    flow_ctrl::{Credit, RecvTotal},
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 事件负载
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一条待写出的**控制帧**。
///
/// 数据帧不走这里：写循环直接从各子流的发送环取数据并成帧，见
/// [`WriteEvent_::TxReady`]。
///
/// `payload_` 用 `Vec<u8>`：控制帧的产生频率由协议状态机约束（建流 / 拆流各一次），
/// 属**冷路径**；数据路径不产生任何堆分配。这与「不隐式分配」的纪律有一处明确的
/// 例外，已记入 dev-notes（`connection-20261002-0548.md` §5 Q6）。
#[derive(Debug, Clone)]
pub(crate) struct ControlFrame_ {
    /// 帧种类。
    kind_: FrameKind,

    /// 标志位（[`super::flags`] 的常量按位或）。
    flags_: u8,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,

    /// 窗口通告 `(累计已收 R, 接收窗口 W)`；只有 `OPEN` / `PULSE` /
    /// `WINDOW_UPDATE` 带。
    window_: Option<(RecvTotal, Credit)>,

    /// 帧载荷字节。
    payload_: Vec<u8>,
}

impl ControlFrame_ {
    /// 造一条不带窗口、不带载荷的控制帧（`CLOSE` 等）。
    pub(crate) fn plain_(kind: FrameKind, flags: u8, local_dock: Dock, remote_dock: Dock) -> Self {
        ControlFrame_ {
            kind_: kind,
            flags_: flags,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
            window_: Option::None,
            payload_: Vec::new(),
        }
    }

    /// 造一条带窗口通告与载荷的控制帧（`OPEN` / `ACCEPT` 等）。
    pub(crate) fn with_window_(
        kind: FrameKind,
        flags: u8,
        local_dock: Dock,
        remote_dock: Dock,
        window: Option<(RecvTotal, Credit)>,
        payload: Vec<u8>,
    ) -> Self {
        ControlFrame_ {
            kind_: kind,
            flags_: flags,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
            window_: window,
            payload_: payload,
        }
    }

    /// 帧种类。
    pub(crate) fn kind_(&self) -> FrameKind {
        self.kind_
    }

    /// 标志位。
    pub(crate) fn flags_(&self) -> u8 {
        self.flags_
    }

    /// 本端 dock。
    pub(crate) fn local_dock_(&self) -> Dock {
        self.local_dock_
    }

    /// 对端 dock。
    pub(crate) fn remote_dock_(&self) -> Dock {
        self.remote_dock_
    }

    /// 窗口通告（若带）。
    pub(crate) fn window_(&self) -> Option<(RecvTotal, Credit)> {
        self.window_
    }

    /// 帧载荷字节。
    pub(crate) fn payload_(&self) -> &[u8] {
        &self.payload_
    }
}

/// 送给**写循环**的事件。
pub(crate) enum WriteEvent_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
{
    /// 会话侧发送环读端上线：写循环把它存进本地表，此后按 dock 对索引。
    Attach {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
        /// 该子流的共享状态。
        owner: ChannelOwner_<A>,
        /// 会话侧发送环读端（应用写、本循环读）。
        reader_: BufferedRx<B, A>,
    },

    /// 一条待写出的控制帧。
    Control {
        /// 待写出的帧。
        frame_: ControlFrame_,
    },

    /// 某条子流的发送环有数据（应用侧写路径置位，见 dev-notes Q4 的契约）。
    TxReady {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
    },

    /// 应用从接收环取走了 `amount_` 字节，接收窗口可回补。
    RxConsumed {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
        /// 本次消费的字节数（应用侧报出，允许略微超前于段的提交）。
        amount_: Credit,
    },

    /// 应用丢弃了发送半边：写循环排空剩余数据后发 `CLOSE(FIN)`。
    TxClosed {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
    },

    /// 应用丢弃了接收半边：写循环发 `CLOSE(RESET)` 并让读循环关掉接收环。
    RxClosed {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
    },

    /// 对端发来 `CLOSE`：读循环已处理完帧面状态，通知写循环推进拆流记账。
    PeerClosed {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
        /// 是否为 `RESET`（对端不再接收）而非 `FIN`（对端不再发送）。
        reset_: bool,
    },
}

/// 送给**读循环**的事件。
pub(crate) enum ReadEvent_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
{
    /// 会话侧接收环写端上线：读循环把它存进本地表。
    Attach {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
        /// 该子流的共享状态。
        owner: ChannelOwner_<A>,
        /// 会话侧接收环写端（本循环写、应用读）。
        writer_: BufferedTx<B, A>,
    },

    /// 释放一条子流：读循环丢弃本地表项。
    Release {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
    },
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 通道
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 事件的生产端；可克隆，因此任意多处持有。
pub(crate) trait TrEventSender_<E> {
    /// 非阻塞投递。返回 `false` 表示消费者已消失（连接正在收尾）。
    fn try_send_event_(&self, event: E) -> bool;
}

/// 事件的消费端；对应循环独占。
pub(crate) trait TrEventReceiver_<E> {
    /// 非阻塞取一条；没有积压时返回 `None`。
    fn try_take_event_(&mut self) -> Option<E>;

    /// 异步取一条：没有积压时 park，直到有事件到达或生产端全部消失。
    ///
    /// 调用方应当把它放在取消令牌的 `select` 里，连接关闭时才不会永远 park。
    fn take_event_async_(&mut self) -> impl Future<Output = Option<E>> + '_;
}

/// 生产端（`flume::Sender` 可克隆 ⇒ 多生产者）。
pub(crate) struct EventSender_<E> {
    tx_: Sender<E>,
}

impl<E> Clone for EventSender_<E> {
    fn clone(&self) -> Self {
        EventSender_ {
            tx_: self.tx_.clone(),
        }
    }
}

/// 消费端（`Receiver` 不对外克隆 ⇒ 单消费者）。
pub(crate) struct EventReceiver_<E> {
    rx_: Receiver<E>,
}

/// 建一对**无界**事件通道（理由见模块文档）。
pub(crate) fn event_channel_<E>() -> (EventSender_<E>, EventReceiver_<E>) {
    let (tx_, rx_) = flume::unbounded();
    (EventSender_ { tx_ }, EventReceiver_ { rx_ })
}

impl<E> TrEventSender_<E> for EventSender_<E> {
    fn try_send_event_(&self, event: E) -> bool {
        match self.tx_.send(event) {
            Result::Ok(()) => true,
            // 消费者已消失（连接正在收尾）：按「没送出去」处理。
            Result::Err(_) => false,
        }
    }
}

impl<E> TrEventReceiver_<E> for EventReceiver_<E> {
    fn try_take_event_(&mut self) -> Option<E> {
        self.rx_.try_recv().ok()
    }

    async fn take_event_async_(&mut self) -> Option<E> {
        self.rx_.recv_async().await.ok()
    }
}

/// 抑制「`TrySendError` 未使用」的告警：无界通道不会返回 `Full`，但保留导入以便
/// 将来换回定容实现时不必再改导入表。
#[allow(dead_code)]
type UnusedTrySendError_ = TrySendError<()>;

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

    /// 测试用的事件负载：一个带序号的整数。
    type TestEvent = u32;

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

    /// 测试无界通道按 FIFO 交付，且投递条数不受容量限制。
    /// - 手段：连投 64 条，然后逐条取出。
    /// - 判断：取出的顺序与投递顺序逐一相等，且最后再取为 `None`。
    #[test]
    fn event_band_is_fifo_and_never_full() {
        let (sender, mut receiver) = event_channel_::<TestEvent>();
        for i in 0..64u32 {
            assert!(sender.try_send_event_(i), "无界通道的投递不应当失败");
        }
        for i in 0..64u32 {
            assert_eq!(receiver.try_take_event_(), Option::Some(i));
        }
        assert_eq!(receiver.try_take_event_(), Option::None);
    }

    /// 测试空通道上异步取出会 park，并在投递时被唤醒。
    /// - 手段：先把 `take_event_async_` 的 future 建好并 poll 一次（此时应
    ///   `Pending`、唤醒计数为 0），再投递，最后再 poll。
    /// - 判断：第一次 `Pending`；投递后唤醒计数增加；再次 poll 返回
    ///   `Ready(Some(..))`。
    #[compio::test]
    async fn event_async_take_parks_until_send() {
        let (sender, mut receiver) = event_channel_::<TestEvent>();
        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);

        let mut wait = pin!(receiver.take_event_async_());
        assert_eq!(wait.as_mut().poll(&mut context), Poll::Pending);
        assert_eq!(probe.count_.load(Ordering::SeqCst), 0usize);

        assert!(sender.try_send_event_(7u32));
        assert!(
            probe.count_.load(Ordering::SeqCst) >= 1usize,
            "投递应当唤醒正在 park 的消费者"
        );
        assert_eq!(
            wait.as_mut().poll(&mut context),
            Poll::Ready(Option::Some(7u32))
        );
    }

    /// 测试已有积压时异步取出立刻就绪，不产生多余唤醒。
    /// - 手段：先投一条，再建 future 并 poll。
    /// - 判断：首次 poll 即 `Ready(Some(..))`，唤醒计数保持 0。
    #[compio::test]
    async fn event_async_take_is_immediate_with_backlog() {
        let (sender, mut receiver) = event_channel_::<TestEvent>();
        assert!(sender.try_send_event_(9u32));

        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);
        let mut wait = pin!(receiver.take_event_async_());
        assert_eq!(
            wait.as_mut().poll(&mut context),
            Poll::Ready(Option::Some(9u32))
        );
        assert_eq!(probe.count_.load(Ordering::SeqCst), 0usize);
    }

    /// 测试生产端可以克隆成多个（多生产者），任一克隆投递都能被同一个消费者取到。
    /// - 手段：把 sender 克隆成两份，各投一条。
    /// - 判断：两条都能按投递顺序取到。
    #[test]
    fn event_band_supports_multiple_producers() {
        let (sender, mut receiver) = event_channel_::<TestEvent>();
        let second = sender.clone();
        assert!(sender.try_send_event_(1u32));
        assert!(second.try_send_event_(2u32));
        assert_eq!(receiver.try_take_event_(), Option::Some(1u32));
        assert_eq!(receiver.try_take_event_(), Option::Some(2u32));
    }

    /// 测试所有生产端消失后，异步取出返回 `None` 而不是永久 park。
    /// - 手段：建通道后立刻丢弃生产端，再 await 取出。
    /// - 判断：返回 `None`——这正是连接收尾时循环能退出的前提。
    #[compio::test]
    async fn event_async_take_ends_when_senders_drop() {
        let (sender, mut receiver) = event_channel_::<TestEvent>();
        drop(sender);
        assert_eq!(receiver.take_event_async_().await, Option::None);
    }
}
