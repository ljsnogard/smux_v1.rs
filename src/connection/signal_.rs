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
//!   「某条子流发送环有数据」（[`WriteEvent_::TxReady`]）、拆流（`TxClosed` /
//!   `RxClosed` / `PeerClosed`）；
//! - [`ReadEvent_`] → **读循环**：会话侧接收环写端上线（[`ReadEvent_::Attach`]）、
//!   拆流（[`ReadEvent_::Release`]）、「应用取走了数据，去看看接收水位」
//!   （[`ReadEvent_::RxConsumed`]）。
//!
//! # 两条水位通知的方向**刻意不同**
//!
//! - 「发送环有数据」投给**写循环**：只有它持有发送环读端，能立刻去搬。
//! - 「应用消费了」投给**读循环**：只有它持有接收环**写端**，能在同一任务里读到
//!   「环内实际积压」，从而算出**精确**的累计已消费量。应用侧采样差值会被并发写入
//!   掩盖，因此那条路早已废弃（见 `dev-notes/flow-ctrl-20261005-0115.md` §3）。
//!
//! # 为什么是**无界**的（本轮裁决 Q6）
//!
//! 定容通道需要「每条子流一个已入队位 + 队列满置连接级全扫标志」这套兜底，才能
//! 保证通知不丢；而本轮新增的两类消息**不可重建**：「发送环有数据」丢了就是那条
//! channel 卡死，控制帧丢了就是建流永远完不成。因此改为**无界通道**，靠两条约束
//! 让队列长度天然有界：
//!
//! 1. 每条子流的「有数据」事件至多一条在队列里（由状态字上的
//!    `mark_tx_queued_` 去重）、「消费了」事件同样至多一条（由
//!    `mark_rx_consumed_` 去重）；
//! 2. 控制帧与拆流事件的产生频率由协议状态机约束（每条子流建流 / 拆流各一次）。
//!
//! 于是 `try_send` 不会失败，「全扫标志」整个机制不再需要。这与此前「定容」
//! 的结论不同，属**本轮修订**。
//!
//! # 为什么不是一条通道
//!
//! 会话侧的两个环半部必须分别交给**不同**的循环：发送环的读端进写循环，接收环的
//! 写端进读循环。若只用一条通道，两个循环会互相取走对方的 `Attach` 事件。因此按
//! 消费者拆成两条。

use core::{
    alloc::AllocatorClone,
    future::Future,
};

use flume::{Receiver, Sender, TrySendError};

use crate::{
    connection::{
        Dock, FrameKind,
        owner_::{ChannelOwner_, TgOwner_},
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
/// 属**冷路径**；数据路径不产生任何堆分配。这是「不隐式分配」纪律的一处明确例外
/// （分配清单见 `dev-notes/audit-heap-alloc-20261004-1122.md`）。
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
pub(crate) enum WriteEvent_<A>
where
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
        reader_: BufferedRx,
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

    /// **本端计时循环**判定空闲超时：放弃该子流的发送方向、摘掉本地表项。
    ///
    /// 与 [`WriteEvent_::PeerClosed`]（对端发来 `CLOSE`）的区别是**谁有权关闭发送环的
    /// 消费端**：本端主动拆流时没有别的执行者会去关它，因此写循环在这里显式
    /// `close()`，应用侧才会立刻看到发送方向已关闭（`is_rx_closed()` 为真、写入返回
    /// `Closing`）。对端 `RESET` 那条路径**不**关——协议只要求「允许放弃已提交字节」，
    /// 而既有验收（`mux_recv_dropped_dual_`）钉住了「对端丢弃接收半边之前写入的字节
    /// 仍然写得完」。
    LocalAbort {
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

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // telegraph（数据报）：与 channel 走**同一套**「环半部上线」模式
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    /// 一条 telegraph 的**发送环读端**上线：复用循环把它存进本地表，此后按
    /// `local_dock` 索引。
    ///
    /// 与 [`WriteEvent_::Attach`] 同形，但键只有 `local_dock`——telegraph **独占**
    /// 该 dock，没有 `remote_dock` 这一维（对端地址是协议里的**地址**而非**身份**，
    /// 见 `crate::connection` 模块文档 §4）。
    TgAttach {
        /// 本端 dock。
        local_dock: Dock,
        /// 该端点的身份节点句柄（循环由它取「已提交报文长度」队列）。
        owner: TgOwner_<A>,
        /// 会话侧发送环读端（应用写、本循环读）。
        reader_: BufferedRx,
    },

    /// 某条 telegraph 的**发送环有已提交数据**（应用侧提交后置位）。
    ///
    /// 只带 `local_dock`：「目的地址 + 长度」按 FIFO 记在身份节点的发送队列里
    /// （目的地址是**逐次发送**的实参，不是端点的固有属性），本事件只负责把循环叫醒。
    TgReady {
        /// 本端 dock。
        local_dock: Dock,
    },

    /// 应用丢弃了 telegraph 的发送半边：复用循环摘掉本地表项。
    ///
    /// 接收半边是否还在与本事件无关——它只影响发送方向是否还值得排空。真的把
    /// **接收**环写端关掉的是身份释放（[`ReadEvent_::TgRelease`]）。
    TgTxClosed {
        /// 本端 dock。
        local_dock: Dock,
    },
}

/// 送给**读循环**的事件。
pub(crate) enum ReadEvent_<A>
where
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
        writer_: BufferedTx,
    },

    /// 释放一条子流：读循环丢弃本地表项。
    Release {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
    },

    /// 应用从接收环取走了数据：**接收窗口的账面水位可能变了**。
    ///
    /// 这是**裸通知**，刻意不带增量。增量必须由**持有接收环写端**的解复用循环按
    /// 「自己记账的累计已收 − 环内实际积压」重算：应用侧只能采样 `data_size` 的差值，
    /// 而一次并发写入就会把同一区间里的读出完全掩盖，额度被永久漏记、两端互等
    /// （因果链见 `dev-notes/flow-ctrl-20261005-0115.md` §3）。
    ///
    /// 投递按子流去重（`ChannelState_::mark_rx_consumed_`）：每条子流至多一条待处理
    /// 通知。
    RxConsumed {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
    },

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // telegraph（数据报）
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    /// 一条 telegraph 的**接收环写端**上线：解复用循环把它存进本地表，此后按
    /// `local_dock` 索引。
    ///
    /// 除环半部外还要带**身份句柄**：接收方向的「报文边界」记在身份节点内联的接收长度
    /// 队列里（`TgRec_::in_`），解复用循环每收下一条报文都要往那里入队并唤醒应用侧。
    TgAttach {
        /// 本端 dock。
        local_dock: Dock,
        /// 该端点的身份节点句柄（循环由它取「已收报文长度」队列）。
        owner: TgOwner_<A>,
        /// 会话侧接收环写端（本循环写、应用读）。
        writer_: BufferedTx,
    },

}

/// **会话释放消息**：会话句柄的 `Drop` 只投递它，不碰注册表。
///
/// # 为什么 `Drop` 不能自己改注册表
///
/// 改注册表要取锁；而锁在跨线程争用时是**阻塞等待**
/// （见 `sync_::acquire_write_`）。`Drop` 只允许
/// 「不阻塞、不做事」——它投一条消息，由核心执行者（两个循环、或任意下一次 API
/// 操作）在**异步上下文**里落实：改身份表、按协议进入 `WAIT_CLOSE`，必要时向对端
/// 通告。
///
/// `ChannelTx::drop` / `ChannelRx::drop` 早就是这个形状（只投 [`WriteEvent_`] /
/// [`ReadEvent_`]）；本枚举把剩下的四处会话身份释放拉回同一条纪律。
pub(crate) enum SessionEvent_ {
    /// `DockBinding` 被丢弃：解绑 `local_dock`。
    ///
    /// binding 是**纯本地**身份（对端不可见，帧属于 channel 而不属于 binding），
    /// 因此没有可宽限的对端状态：消息落实即可重绑（不需要定时宽限期）。
    UnbindDock {
        /// 被解绑的本地 dock。
        local_dock: Dock,
    },

    /// `ChannelListener` 被丢弃：释放 listener 身份。
    ReleaseListener {
        /// 被释放的本地 dock。
        local_dock: Dock,
    },

    /// `Telegraph` 被丢弃：释放 telegraph 身份。
    ReleaseTelegraph {
        /// 被释放的本地 dock。
        local_dock: Dock,
    },

    /// 发起方句柄未裁决就丢弃：**撤销**尚未露面的子流登记（不留宽限期）。
    UnreserveChannel {
        /// 本端 dock。
        local_dock: Dock,
        /// 对端 dock。
        remote_dock: Dock,
    },

    /// 已露过面的子流收尾（响应方未裁决就丢弃）：进入拆流宽限期 `WAIT_CLOSE`。
    ReleaseChannel {
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

/// **会话释放邮箱**：`Drop` 投递、核心执行者 drain。
///
/// # 与 `WriteEvent_` / `ReadEvent_` 通道的区别
///
/// 那两条通道是**单消费者**（各自的循环独占）；本邮箱**允许多个消费者**：
/// 两个循环与任意 API 面都可以 drain，每条消息只会被其中一个取到。这是「丢弃
/// binding 后立刻重绑」能否确定成立的关键——重绑的那次 `bind_async` 自己就能
/// 把积压的释放消息落实掉，而不必等某个特定任务被调度。
///
/// 无界（与另两条通道同理）：消息数有天然上界——每个会话句柄最多投一条。
pub(crate) struct SessionMailbox_ {
    /// 生产端；`flume::Sender` 可克隆。
    tx_: Sender<SessionEvent_>,

    /// 消费端；**可克隆**（多消费者）。
    rx_: Receiver<SessionEvent_>,
}

impl Clone for SessionMailbox_ {
    fn clone(&self) -> Self {
        SessionMailbox_ {
            tx_: self.tx_.clone(),
            rx_: self.rx_.clone(),
        }
    }
}

impl SessionMailbox_ {
    /// 建一个空邮箱。
    pub(crate) fn new_() -> Self {
        let (tx_, rx_) = flume::unbounded();
        SessionMailbox_ { tx_, rx_ }
    }

    /// 非阻塞投递（`Drop` 用；**不取任何锁、不阻塞**）。
    ///
    /// 返回 `false` 表示消费者已全部消失（注册表自己始终持一份消费者，因此只有
    /// 连接彻底收尾时才会发生）；调用方按「没送出去」处理即可。
    pub(crate) fn post_(&self, event: SessionEvent_) -> bool {
        self.tx_.send(event).is_ok()
    }

    /// 非阻塞取一条；没有积压时返回 `None`。
    pub(crate) fn try_take_(&self) -> Option<SessionEvent_> {
        self.rx_.try_recv().ok()
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
    dual_runtime_test_!(event_async_take_parks_until_send);

    /// 测试已有积压时异步取出立刻就绪，不产生多余唤醒。
    /// - 手段：先投一条，再建 future 并 poll。
    /// - 判断：首次 poll 即 `Ready(Some(..))`，唤醒计数保持 0。
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
    dual_runtime_test_!(event_async_take_is_immediate_with_backlog);

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
    async fn event_async_take_ends_when_senders_drop() {
        let (sender, mut receiver) = event_channel_::<TestEvent>();
        drop(sender);
        assert_eq!(receiver.take_event_async_().await, Option::None);
    }
    dual_runtime_test_!(event_async_take_ends_when_senders_drop);
}
