//! 连接的**事件通道**：中心循环与 API 面之间的唯一异步消息通路。
//!
//! 本模块在「演员核心 + 智能指针」架构下**保留**：需求怎么投递不是设计纪律，而
//! 水位这类高频提示走一次性通道恰好合适（见 [`crate::connection`] 模块文档 §2.3）。
//! 通道实现在 [`EVENT_CHAN_CAP`] 处**定容**（默认后端 `mpmc_chan`）；旧的 `flume`
//! 无界实现保留在 `chan-flume` feature 下用于 A/B 对比。
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
//! # 定容 + 全扫兜底（`feat/mpmc-chan` 试验）
//!
//! `mpmc_chan` 没有无界模式，因此按「事件能否由状态位重建」分两类投递：
//!
//! 1. **可重建的高频提示**（`TxReady` / `TgReady` / `RxConsumed`）：环满时直接丢弃，
//!    只置一个「需要全扫」标志并唤醒消费者。写循环的排空本来就是全表扫描，读循环的
//!    `rescan_read_` 则按 `RX_CONSUMED` 位 + 环内积压重算窗口。这些事件的信息量只是
//!    「可能变了」，状态位才是事实来源。
//! 2. **不可重建的一次性通知**（`Attach` / `Control` / `TgAttach` / `TxClosed` /
//!    `RxClosed` / `PeerClosed` / `LocalAbort` / `Release` / `TgTxClosed`）：环满时
//!    推进 [`Overflow_::durable_`] 暂存（自旋锁 FIFO），消费者下次取事件时先取它。
//!    它们携带环半部句柄、控制帧或一次性拆流决定，丢了就真的无法可靠恢复。
//!
//! 这样正常路径只走定容环与一次原子读；全扫只在环满的极端情况下发生。`chan-flume`
//! 后端是无界通道，两条兜底路径都不会被触发（保留它是为了同一提交内 A/B 对比）。
//!
//! # 为什么不是一条通道
//!
//! 会话侧的两个环半部必须分别交给**不同**的循环：发送环的读端进写循环，接收环的
//! 写端进读循环。若只用一条通道，两个循环会互相取走对方的 `Attach` 事件。因此按
//! 消费者拆成两条。

#[cfg(not(feature = "chan-flume"))]
use alloc::boxed::Box;
use alloc::{collections::VecDeque, sync::Arc};
use core::{
    alloc::AllocatorClone,
    future::Future,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{Context, Poll},
};

#[cfg(feature = "chan-flume")]
use flume::{Receiver, Sender, TryRecvError, TrySendError};
#[cfg(not(feature = "chan-flume"))]
use mpmc_chan::{MpmcBuf, MpmcChan, TryRecvError, TrySendError};

use crate::{
    connection::{
        Dock, FrameKind,
        owner_::{ChannelOwner_, TgOwner_},
        ring_::{BufferedRx, BufferedTx},
        sync_::NotifySlot_,
    },
    flow_ctrl::{Credit, RecvTotal},
    x_deps::{abs_sync::may_break::TrMayBreak, atomic_sync::mutex::preemptive::SpinningMutexOwned},
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

/// 定容事件通道的容量（`mpmc_chan` 要求 2 的幂且 `>= 2`）。
///
/// 事件通道承载两类东西：
///
/// - **不可重建的一次性通知**（`Attach` / `Control` / `TgAttach` / 拆流与释放）：
///   生产者走 [`EventSender_::try_send_durable_event_`]，满时进入兜底暂存，**绝不丢**；
/// - **可重建的高频提示**（`TxReady` / `TgReady` / `RxConsumed`）：走
///   [`TrEventSender_::try_send_event_`]，满时丢弃并置「需要全扫」标志，由消费者按
///   状态位 / 全表排空重建。
///
/// 因此容量只需覆盖不可重建通知的在途量与可重建提示的突发。首轮取 256，在 256 条
/// 并发子流的跨线程用例里会满到无法收敛；实测 1024 足够（事件枚举 64 字节，约
/// 64 KiB/通道），4096 没有再快。详见 `dev-notes/mpmc-chan-20261026-0046.md` §6。
#[cfg_attr(feature = "chan-flume", allow(dead_code))]
pub(crate) const EVENT_CHAN_CAP: usize = 1024;

/// 会话释放邮箱的容量（同样只是快速路径；满时走自旋锁溢出暂存）。
#[cfg_attr(feature = "chan-flume", allow(dead_code))]
pub(crate) const MAILBOX_CHAN_CAP: usize = 256;

#[cfg(feature = "chan-flume")]
type EventTx_<E> = Sender<E>;
#[cfg(feature = "chan-flume")]
type EventRx_<E> = Receiver<E>;
#[cfg(not(feature = "chan-flume"))]
type EventTx_<E> = mpmc_chan::Sender<'static, E, EVENT_CHAN_CAP, Box<MpmcBuf<E, EVENT_CHAN_CAP>>>;
#[cfg(not(feature = "chan-flume"))]
type EventRx_<E> = mpmc_chan::Receiver<'static, E, EVENT_CHAN_CAP, Box<MpmcBuf<E, EVENT_CHAN_CAP>>>;

/// 一次「取事件」的完整结论。
///
/// `Rescan` 是定容通道的**兜底入口**：某条可重建的高频提示（`TxReady` /
/// `TgReady` / `RxConsumed`）因为环满被丢弃时，生产者置位溢出标志并唤醒消费者。
/// 写循环看到它直接进入本轮全表排空；读循环调用 `rescan_read_` 按 `RX_CONSUMED`
/// 位重算窗口。它不等于「通道有数据」，也不等于「通道关闭」。
pub(crate) enum EventTake_<E> {
    /// 取到一条事件。
    Event(E),
    /// 有可重建事件被丢弃；请全扫。
    Rescan,
    /// 当前没有事件（方向锁短暂占用也归这里，调用方应按「稍后再试」处理）。
    Empty,
    /// 通道已关闭且无积压。
    Closed,
}

/// 定容通道的满时兜底。
///
/// 两条路：
///
/// - **可重建事件**（水位 / 拆流提示）：环满时丢弃并置 [`Self::flag_`]，消费者看到
///   `Rescan` 后按各子流状态位全扫重建；
/// - **不可重建事件**（`Attach` / `Control` / `TgAttach`）：环满时推进
///   [`Self::durable_`] 暂存，消费者下次取事件时先取它，**绝不丢**。
///
/// 暂存用自旋锁保护：投递可能发生在 `Drop` 里、任意线程上，临界区只有一次
/// `push_back` / `pop_front`。它只在环满时增长，正常路径不触碰。
///
/// 只有 `mpmc_chan` 后端会走到这里；`chan-flume` 是无界通道，标志与暂存永远为空。
struct Overflow_<E> {
    /// 「有可重建事件被丢弃，需要全扫」。
    flag_: AtomicBool,

    /// 唤醒消费者的单等待者槽（用 `NotifySlot_` 的持久位，避免丢唤醒）。
    wake_: NotifySlot_,

    /// 不可重建事件的满时暂存（FIFO）。
    durable_: SpinningMutexOwned<VecDeque<E>>,

    /// 暂存队列的近似长度：正常路径（为 0）据此**不取锁**。
    ///
    /// 多生产者在锁内 push 之后自增、单消费者 pop 之后自减；短暂偏差只会让消费者
    /// 多取一次锁或晚一轮看到事件，而后者由 `mark_` 的唤醒兜住。
    durable_len_: AtomicUsize,
}

impl<E> Overflow_<E> {
    /// 空兜底。
    fn new_() -> Self {
        Overflow_ {
            flag_: AtomicBool::new(false),
            wake_: NotifySlot_::new_(),
            durable_: SpinningMutexOwned::new_owned(VecDeque::new()),
            durable_len_: AtomicUsize::new(0usize),
        }
    }

    /// 报告「有可重建事件被丢弃 / 有暂存事件到达」：置位并唤醒消费者。
    /// 幂等、不阻塞。
    fn mark_(&self) {
        self.flag_.store(true, Ordering::Release);
        self.wake_.notify_();
    }

    /// 取得并清除标志；返回此前是否置位。
    fn take_(&self) -> bool {
        self.flag_.swap(false, Ordering::AcqRel)
    }

    /// 等待标志置位；已置位时立刻就绪并顺手消费通知槽的持久位。
    fn poll_(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.flag_.swap(false, Ordering::AcqRel) {
            // 清掉通知槽的持久位，避免下一轮因残留 pending 空转。
            let _ = self.wake_.poll_wait_(cx);
            return Poll::Ready(());
        }
        self.wake_.poll_wait_(cx)
    }

    /// 在锁下访问暂存队列。
    fn with_durable_<R>(&self, f: impl FnOnce(&mut VecDeque<E>) -> R) -> R {
        let mut session = self.durable_.lock_session();
        // 不可取消的自旋锁不会以取消告终（与 `NotifySlot_::with_waker_` 同款）。
        let mut guard = session
            .lock()
            .wait_or(|| unreachable!("不可取消的自旋锁不会以取消告终"));
        f(&mut guard)
    }

    /// 追加一条不可重建事件。
    fn push_durable_(&self, event: E) {
        self.with_durable_(|queue| queue.push_back(event));
        self.durable_len_.fetch_add(1usize, Ordering::Release);
    }

    /// 取一条不可重建事件。
    ///
    /// 队列为空时只做一次原子读，**不取自旋锁**——这是事件循环的取事件快路径上
    /// 最频繁的分支。
    fn pop_durable_(&self) -> Option<E> {
        if self.durable_len_.load(Ordering::Acquire) == 0usize {
            return Option::None;
        }
        let popped = self.with_durable_(|queue| queue.pop_front());
        if popped.is_some() {
            self.durable_len_.fetch_sub(1usize, Ordering::Release);
        }
        popped
    }
}

/// 事件的生产端；可克隆，因此任意多处持有。
pub(crate) trait TrEventSender_<E> {
    /// 非阻塞投递一条**可重建**事件。返回 `false` 表示消费者已消失（连接正在收尾）
    /// 或环满（此时已置位全扫标志）。
    fn try_send_event_(&self, event: E) -> bool;
}

/// 事件的消费端；对应循环独占。
pub(crate) trait TrEventReceiver_<E> {
    /// 非阻塞取一条。
    fn try_take_event_(&mut self) -> EventTake_<E>;

    /// 异步取一条：没有积压时 park，直到有事件到达、需要全扫、或生产端全部消失。
    ///
    /// 调用方应当把它放在取消令牌的 `select` 里，连接关闭时才不会永远 park。
    fn take_event_async_(&mut self) -> impl Future<Output = EventTake_<E>> + '_;
}

/// 生产端（两种后端都可克隆 ⇒ 多生产者）。
pub(crate) struct EventSender_<E: 'static> {
    tx_: EventTx_<E>,

    /// 满时兜底（与消费端共享）。
    overflow_: Arc<Overflow_<E>>,
}

impl<E> Clone for EventSender_<E> {
    fn clone(&self) -> Self {
        EventSender_ {
            tx_: self.tx_.clone(),
            overflow_: Arc::clone(&self.overflow_),
        }
    }
}

impl<E: Send + 'static> EventSender_<E> {
    /// 投递一条**不可重建**事件（`Attach` / `Control` / `TgAttach` / 拆流与释放）。
    ///
    /// 与 [`TrEventSender_::try_send_event_`] 的区别：环满时把事件推进兜底暂存，
    /// **不丢弃**。它是同步入口，因此可以在 `Drop` 里调用；暂存本身是自旋锁保护的
    /// FIFO（只在环满时增长）。
    ///
    /// 返回 `false` 表示消费者已全部消失（连接收尾）。
    pub(crate) fn try_send_durable_event_(&self, event: E) -> bool {
        match self.tx_.try_send(event) {
            Result::Ok(()) => true,
            Result::Err(TrySendError::Full(event)) => {
                self.overflow_.push_durable_(event);
                self.overflow_.mark_();
                true
            }
            Result::Err(_) => false,
        }
    }
}

/// 消费端（单消费者；`Receiver` 不在本类型上提供克隆）。
pub(crate) struct EventReceiver_<E: 'static> {
    rx_: EventRx_<E>,

    /// 满时兜底（与生产端共享）。
    overflow_: Arc<Overflow_<E>>,
}

/// 建一对事件通道。
///
/// - 默认 `mpmc_chan`：定容 [`EVENT_CHAN_CAP`]，满时按上述协议兜底；
/// - `chan-flume`：保持旧的**无界**实现，仅用于 A/B 对比。
pub(crate) fn event_channel_<E>() -> (EventSender_<E>, EventReceiver_<E>)
where
    E: Send + 'static,
{
    let overflow_ = Arc::new(Overflow_::<E>::new_());
    #[cfg(feature = "chan-flume")]
    let (tx_, rx_) = flume::unbounded();
    #[cfg(not(feature = "chan-flume"))]
    let (tx_, rx_) = MpmcChan::new(Box::new(MpmcBuf::<E, EVENT_CHAN_CAP>::new()));
    (
        EventSender_ {
            tx_,
            overflow_: Arc::clone(&overflow_),
        },
        EventReceiver_ { rx_, overflow_ },
    )
}

impl<E: Send + 'static> TrEventSender_<E> for EventSender_<E> {
    fn try_send_event_(&self, event: E) -> bool {
        match self.tx_.try_send(event) {
            Result::Ok(()) => true,
            // 环满（或生产者方向锁被慢写者占用）：丢弃并置全扫标志。
            Result::Err(TrySendError::Full(_)) => {
                self.overflow_.mark_();
                false
            }
            // 消费者已消失（连接正在收尾）：按「没送出去」处理。
            Result::Err(_) => false,
        }
    }
}

impl<E: Send + 'static> TrEventReceiver_<E> for EventReceiver_<E> {
    fn try_take_event_(&mut self) -> EventTake_<E> {
        // 先取不可重建事件的满时暂存：它们不能被当成 `Rescan` 丢掉。
        if let Option::Some(event) = self.overflow_.pop_durable_() {
            return EventTake_::Event(event);
        }
        match self.rx_.try_recv() {
            Result::Ok(event) => EventTake_::Event(event),
            Result::Err(TryRecvError::Empty) => {
                if self.overflow_.take_() {
                    EventTake_::Rescan
                } else {
                    EventTake_::Empty
                }
            }
            Result::Err(_) => EventTake_::Closed,
        }
    }

    async fn take_event_async_(&mut self) -> EventTake_<E> {
        let rx = &self.rx_;
        let overflow = Arc::clone(&self.overflow_);
        // 快路径：暂存 → 环 → 全扫标志。
        if let Option::Some(event) = overflow.pop_durable_() {
            return EventTake_::Event(event);
        }
        match rx.try_recv() {
            Result::Ok(event) => return EventTake_::Event(event),
            Result::Err(TryRecvError::Empty) => {}
            Result::Err(_) => return EventTake_::Closed,
        }
        if overflow.take_() {
            return if let Option::Some(event) = overflow.pop_durable_() {
                EventTake_::Event(event)
            } else {
                EventTake_::Rescan
            };
        }
        // 同时等「通道有事件」与「需要全扫 / 暂存到达」：只等其一都会丢掉另一种唤醒。
        //
        // `mpmc_chan` 的 `RecvAsync` 是参数载体（实现 `IntoFuture`），不是 `Future`
        // 本身；这里先转成 future 再 `pin`，以便跨 poll 持有它的唤醒登记。
        let recv_fut = core::future::IntoFuture::into_future(rx.recv_async());
        let mut recv_fut = core::pin::pin!(recv_fut);
        core::future::poll_fn(|cx| {
            if let Option::Some(event) = overflow.pop_durable_() {
                return Poll::Ready(EventTake_::Event(event));
            }
            if overflow.take_() {
                return if let Option::Some(event) = overflow.pop_durable_() {
                    Poll::Ready(EventTake_::Event(event))
                } else {
                    Poll::Ready(EventTake_::Rescan)
                };
            }
            match core::future::Future::poll(recv_fut.as_mut(), cx) {
                Poll::Ready(Result::Ok(event)) => Poll::Ready(EventTake_::Event(event)),
                Poll::Ready(Result::Err(_)) => Poll::Ready(EventTake_::Closed),
                Poll::Pending => match overflow.poll_(cx) {
                    Poll::Ready(()) => {
                        if let Option::Some(event) = overflow.pop_durable_() {
                            Poll::Ready(EventTake_::Event(event))
                        } else {
                            Poll::Ready(EventTake_::Rescan)
                        }
                    }
                    Poll::Pending => Poll::Pending,
                },
            }
        })
        .await
    }
}

/// **会话释放邮箱**：`Drop` 投递、核心执行者 drain。
///
/// # 与 `WriteEvent_` / `ReadEvent_` 通道的区别
///
/// 那两条通道是**单消费者**（各自的循环独占）；本邮箱**允许多个消费者**：
/// 两个循环与任意 API 面都可以 drain，每条消息只会被其中一个取到。这是「丢弃
/// binding 后立刻重绑」能否确定成立的关键——重绑的那次 `bind_async` 自己就能
/// 把积压的释放消息落实掉，而不必等某个特定任务被调度。
///
/// # 定容与兜底
///
/// 正常路径走 [`MAILBOX_CHAN_CAP`] 的定容通道；消息数天然有上界（每个会话句柄
/// 至多一条）。`Drop` 里不能 `await`，因此环满时把消息推进一个**自旋锁保护的
/// 溢出暂存**（`VecDeque`）。这条兜底路径只在极端并发 `Drop` 时才会走到；它保证
/// 释放消息不丢，代价是溢出期间有界之外的一次堆增长。
pub(crate) struct SessionMailbox_ {
    /// 生产端；可克隆。
    tx_: MailboxTx_,

    /// 消费端；**可克隆**（多消费者）。
    rx_: MailboxRx_,

    /// 满时溢出暂存（多消费者共享）。
    overflow_: Arc<MailboxOverflow_>,
}

#[cfg(feature = "chan-flume")]
type MailboxTx_ = Sender<SessionEvent_>;
#[cfg(feature = "chan-flume")]
type MailboxRx_ = Receiver<SessionEvent_>;
#[cfg(not(feature = "chan-flume"))]
type MailboxTx_ = mpmc_chan::Sender<
    'static,
    SessionEvent_,
    MAILBOX_CHAN_CAP,
    Box<MpmcBuf<SessionEvent_, MAILBOX_CHAN_CAP>>,
>;
#[cfg(not(feature = "chan-flume"))]
type MailboxRx_ = mpmc_chan::Receiver<
    'static,
    SessionEvent_,
    MAILBOX_CHAN_CAP,
    Box<MpmcBuf<SessionEvent_, MAILBOX_CHAN_CAP>>,
>;

/// 邮箱满时的溢出暂存。
///
/// 用自旋锁而不是 `std::sync::Mutex`：`post_` 可能在 `Drop` 里、任意线程上调用，
/// 临界区只有一次 `push_back` / `pop_front`，自旋比唤醒系统锁更符合本模块
/// 「不阻塞、不隐式分配」的纪律（溢出时的那次增长是明确例外）。
struct MailboxOverflow_ {
    /// 溢出消息（FIFO）。
    queue_: SpinningMutexOwned<VecDeque<SessionEvent_>>,
}

impl MailboxOverflow_ {
    /// 空溢出队列。
    fn new_() -> Self {
        MailboxOverflow_ {
            queue_: SpinningMutexOwned::new_owned(VecDeque::new()),
        }
    }

    /// 在锁下访问队列。
    fn with_<R>(&self, f: impl FnOnce(&mut VecDeque<SessionEvent_>) -> R) -> R {
        let mut session = self.queue_.lock_session();
        // 不可取消的自旋锁不会以取消告终（与 `NotifySlot_::with_waker_` 同款）。
        let mut guard = session
            .lock()
            .wait_or(|| unreachable!("不可取消的自旋锁不会以取消告终"));
        f(&mut guard)
    }

    /// 追加一条溢出消息。
    fn push_(&self, event: SessionEvent_) {
        self.with_(|queue| queue.push_back(event));
    }

    /// 取一条溢出消息。
    fn pop_(&self) -> Option<SessionEvent_> {
        self.with_(|queue| queue.pop_front())
    }
}

impl Clone for SessionMailbox_ {
    fn clone(&self) -> Self {
        SessionMailbox_ {
            tx_: self.tx_.clone(),
            rx_: self.rx_.clone(),
            overflow_: Arc::clone(&self.overflow_),
        }
    }
}

impl SessionMailbox_ {
    /// 建一个空邮箱。
    pub(crate) fn new_() -> Self {
        let overflow_ = Arc::new(MailboxOverflow_::new_());
        #[cfg(feature = "chan-flume")]
        let (tx_, rx_) = flume::unbounded();
        #[cfg(not(feature = "chan-flume"))]
        let (tx_, rx_) = MpmcChan::new(Box::new(MpmcBuf::<SessionEvent_, MAILBOX_CHAN_CAP>::new()));
        SessionMailbox_ {
            tx_,
            rx_,
            overflow_,
        }
    }

    /// 非阻塞投递（`Drop` 用；不取注册表锁、不 `await`）。
    ///
    /// 返回 `false` 表示消费者已全部消失（注册表自己始终持一份消费者，因此只有
    /// 连接彻底收尾时才会发生）；调用方按「没送出去」处理即可。
    pub(crate) fn post_(&self, event: SessionEvent_) -> bool {
        match self.tx_.try_send(event) {
            Result::Ok(()) => true,
            // 定容环满：推进溢出暂存，仍然算送达。
            Result::Err(TrySendError::Full(event)) => {
                self.overflow_.push_(event);
                true
            }
            Result::Err(_) => false,
        }
    }

    /// 非阻塞取一条；没有积压时返回 `None`。
    ///
    /// 先取定容环，再取溢出暂存。多个消费者并发时每条消息只会被取到一次。
    pub(crate) fn try_take_(&self) -> Option<SessionEvent_> {
        match self.rx_.try_recv() {
            Result::Ok(event) => return Option::Some(event),
            Result::Err(TryRecvError::Empty) => {}
            Result::Err(_) => {}
        }
        self.overflow_.pop_()
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

    /// 测试事件通道按 FIFO 交付，且容量内投递不丢。
    /// - 手段：连投 64 条（小于容量 256），然后逐条取出。
    /// - 判断：取出的顺序与投递顺序逐一相等，且最后再取为空。
    ///
    /// Test goal: verify FIFO delivery and no loss within capacity.
    /// Method: send 64 events (below capacity 256), then take them one by one.
    /// Assertion: taken events equal the send order; the next take is empty.
    #[test]
    fn event_band_is_fifo_and_within_capacity() {
        let (sender, mut receiver) = event_channel_::<TestEvent>();
        for i in 0..64u32 {
            assert!(sender.try_send_event_(i), "容量内投递不应当失败");
        }
        for i in 0..64u32 {
            assert!(matches!(
                receiver.try_take_event_(),
                EventTake_::Event(value) if value == i
            ));
        }
        assert!(matches!(receiver.try_take_event_(), EventTake_::Empty));
    }

    /// 测试可重建事件在环满时被丢弃，并在队列取空后报告一次「需要全扫」。
    /// - 手段：把容量 256 填满，再投一条可重建事件（应失败），然后取空全部。
    /// - 判断：多出的那条投递返回 `false`；取空后紧接着是 `Rescan`，再之后为空。
    ///
    /// Test goal: verify recoverable events are dropped when full and a single
    /// `Rescan` is reported once the ring is drained.
    /// Method: fill capacity 256, send one more recoverable event (must fail),
    /// then drain everything.
    /// Assertion: the extra send is `false`; after the drain comes `Rescan`,
    /// then empty.
    #[cfg(not(feature = "chan-flume"))]
    #[test]
    fn event_full_drops_recoverable_and_flags_rescan() {
        let (sender, mut receiver) = event_channel_::<TestEvent>();
        for i in 0..EVENT_CHAN_CAP {
            assert!(sender.try_send_event_(i as u32), "容量内投递不应当失败");
        }
        assert!(
            !sender.try_send_event_(u32::MAX),
            "环满时多出的可重建事件应当被丢弃"
        );
        for i in 0..EVENT_CHAN_CAP {
            assert!(matches!(
                receiver.try_take_event_(),
                EventTake_::Event(value) if value == i as u32
            ));
        }
        assert!(matches!(receiver.try_take_event_(), EventTake_::Rescan));
        assert!(matches!(receiver.try_take_event_(), EventTake_::Empty));
    }

    /// 测试不可重建事件在环满时进入暂存而不是被丢弃。
    /// - 手段：把容量 256 填满，再投一条 durable 事件（应成功），然后取空全部。
    /// - 判断：durable 事件先被取出（暂存优先），随后是 0..256，最后是 `Rescan`。
    ///
    /// Test goal: verify non-reconstructible events are stashed, not dropped,
    /// when the ring is full.
    /// Method: fill capacity 256, send one durable event (must succeed), then
    /// drain everything.
    /// Assertion: the durable event comes first (stash has priority), then
    /// 0..256, and finally `Rescan`.
    #[cfg(not(feature = "chan-flume"))]
    #[test]
    fn event_full_stashes_durable_event() {
        let (sender, mut receiver) = event_channel_::<TestEvent>();
        for i in 0..EVENT_CHAN_CAP {
            assert!(sender.try_send_event_(i as u32), "容量内投递不应当失败");
        }
        assert!(
            sender.try_send_durable_event_(999u32),
            "环满时不可重建事件必须进入暂存而不是丢弃"
        );
        assert!(matches!(
            receiver.try_take_event_(),
            EventTake_::Event(999u32)
        ));
        for i in 0..EVENT_CHAN_CAP {
            assert!(matches!(
                receiver.try_take_event_(),
                EventTake_::Event(value) if value == i as u32
            ));
        }
        assert!(matches!(receiver.try_take_event_(), EventTake_::Rescan));
        assert!(matches!(receiver.try_take_event_(), EventTake_::Empty));
    }

    /// 测试空通道上异步取出会 park，并在投递时被唤醒。
    /// - 手段：先把 `take_event_async_` 的 future 建好并 poll 一次（此时应
    ///   `Pending`、唤醒计数为 0），再投递，最后再 poll。
    /// - 判断：第一次 `Pending`；投递后唤醒计数增加；再次 poll 返回
    ///   `Ready(Event(..))`。
    ///
    /// Test goal: verify an async take parks on an empty channel and wakes on send.
    /// Method: build the `take_event_async_` future and poll once (expect Pending,
    /// wake count 0), send, then poll again.
    /// Assertion: first poll is Pending; wake count increases after send; the
    /// second poll is `Ready(Event(..))`.
    async fn event_async_take_parks_until_send() {
        let (sender, mut receiver) = event_channel_::<TestEvent>();
        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);

        let mut wait = pin!(receiver.take_event_async_());
        assert!(matches!(wait.as_mut().poll(&mut context), Poll::Pending));
        assert_eq!(probe.count_.load(Ordering::SeqCst), 0usize);

        assert!(sender.try_send_event_(7u32));
        assert!(
            probe.count_.load(Ordering::SeqCst) >= 1usize,
            "投递应当唤醒正在 park 的消费者"
        );
        assert!(matches!(
            wait.as_mut().poll(&mut context),
            Poll::Ready(EventTake_::Event(7u32))
        ));
    }
    multi_art_test_!(event_async_take_parks_until_send);

    /// 测试已有积压时异步取出立刻就绪，不产生多余唤醒。
    /// - 手段：先投一条，再建 future 并 poll。
    /// - 判断：首次 poll 即 `Ready(Event(..))`，唤醒计数保持 0。
    ///
    /// Test goal: verify an async take is immediately ready when a backlog exists.
    /// Method: send one event first, then build and poll the future.
    /// Assertion: the first poll is `Ready(Event(..))`; wake count stays 0.
    async fn event_async_take_is_immediate_with_backlog() {
        let (sender, mut receiver) = event_channel_::<TestEvent>();
        assert!(sender.try_send_event_(9u32));

        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);
        let mut wait = pin!(receiver.take_event_async_());
        assert!(matches!(
            wait.as_mut().poll(&mut context),
            Poll::Ready(EventTake_::Event(9u32))
        ));
        assert_eq!(probe.count_.load(Ordering::SeqCst), 0usize);
    }
    multi_art_test_!(event_async_take_is_immediate_with_backlog);

    /// 测试生产端可以克隆成多个（多生产者），任一克隆投递都能被同一个消费者取到。
    /// - 手段：把 sender 克隆成两份，各投一条。
    /// - 判断：两条都能按投递顺序取到。
    ///
    /// Test goal: verify multiple producers share one consumer.
    /// Method: clone the sender once and send one event through each clone.
    /// Assertion: both events are taken in send order.
    #[test]
    fn event_band_supports_multiple_producers() {
        let (sender, mut receiver) = event_channel_::<TestEvent>();
        let second = sender.clone();
        assert!(sender.try_send_event_(1u32));
        assert!(second.try_send_event_(2u32));
        assert!(matches!(
            receiver.try_take_event_(),
            EventTake_::Event(1u32)
        ));
        assert!(matches!(
            receiver.try_take_event_(),
            EventTake_::Event(2u32)
        ));
    }

    /// 测试所有生产端消失后，异步取出返回关闭而不是永久 park。
    /// - 手段：建通道后立刻丢弃生产端，再 await 取出。
    /// - 判断：返回 `Closed`——这正是连接收尾时循环能退出的前提。
    ///
    /// Test goal: verify an async take returns closed after all senders drop.
    /// Method: drop the sender immediately after building the channel, then await.
    /// Assertion: `Closed` is returned, which is what lets the loops exit.
    async fn event_async_take_ends_when_senders_drop() {
        let (sender, mut receiver) = event_channel_::<TestEvent>();
        drop(sender);
        assert!(matches!(
            receiver.take_event_async_().await,
            EventTake_::Closed
        ));
    }
    multi_art_test_!(event_async_take_ends_when_senders_drop);
}
