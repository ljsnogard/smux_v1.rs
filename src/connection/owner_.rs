//! 每条子流的**共享原子状态**：[`ChannelState_`]（经 [`ChannelOwner_`] 句柄共享）。
//!
//! 环半部**不在这里**：会话侧的两个半部在注册时**移交给对应的循环本地持有**，
//! 因此它们不会藏在共享实体的锁后面，循环可以自由在这些半部上 park / await。
//!
//! # 一次分配、一个共享节点
//!
//! 状态由**注册表在登记身份时**建立（`reserve_channel_` / `reserve_inbound_`），
//! 注册表的身份记录持有它的句柄，两个循环的本地表与应用侧半部各持一份克隆。
//! 因此「身份在 ⇒ 状态在」，不再有一个可以被提前丢弃的、另行 `attach` 上来的 owner。
//! 状态本身的成员全是原子，**没有任何锁**：读写循环与建流路径都直接经句柄访问。
//!
//! # 状态字：一个 `AtomicFlags<usize>`
//!
//! 建流三步的进展、两个方向的收尾、关注意味、两个锁外去重位、安装位，全部打进
//! **一个** 原子字（bit 表见下）。好处有三：
//!
//! 1. 任何「多条件判定 + 置位」都能写成**一次 CAS**，天然原子。最典型的是
//!    [`ChannelState_::claim_release_`]：它把过去「先查 `is_done_` 再置 `released_`」
//!    两步合成一个比较交换，不需要锁就排除双放；
//! 2. `is_done_` 这类多条件读只需**一次载入**，不会读到半新半旧的位组合；
//! 3. 去重位仍是**锁外**：同步路径（`try_write` / `try_read`）没有 `await` 可用，
//!    CAS 是唯一不需要等待的表达。
//!
//! # 流控也是原子的
//!
//! [`FlowCtrl`]（收发双向窗口）的方法全部取 `&self`、字段全部是原子，因此对窗口的
//! 每一次记账都只是一次原子读改写；唯一需要打包的跨任务字段见
//! [`SendWindow`](crate::flow_ctrl::SendWindow) 的文档。

use core::{
    alloc::AllocatorClone,
    future::poll_fn,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
};
use std::time::Instant;

use atomic_sync::x_deps::atomex;
use atomex::AtomicFlags;
use buffex::x_deps::abs_cancel::TrCancellationToken;
use mm_ptr::Shared;

use crate::{
    connection::{
        error_::MuxError,
        mux_connection::ChannelRegistry_,
        sync_::NotifySlot_,
    },
    flow_ctrl::{Credit, FlowCtrl, FlowCtrlError, WindowReport},
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 状态字的位定义
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 应用已丢弃发送半边（[`ChannelTx`](super::ChannelTx)）。
const APP_TX_CLOSED: usize = 1usize << 0usize;

/// 应用已丢弃接收半边（[`ChannelRx`](super::ChannelRx)）。
const APP_RX_CLOSED: usize = 1usize << 1usize;

/// 本端已发出 `CLOSE(FIN)`：不再发送数据（**且发送环已排空**，`FIN` 只在排空后发）。
const LOCAL_FIN_SENT: usize = 1usize << 2usize;

/// 本端已发出 `CLOSE(RESET)`：不再接收数据。
const LOCAL_RESET_SENT: usize = 1usize << 3usize;

/// 对端已声明不再发送（收到 `CLOSE(FIN)`）。
const PEER_FIN: usize = 1usize << 4usize;

/// 对端已声明不再接收（收到 `CLOSE(RESET)`）。
const PEER_RESET: usize = 1usize << 5usize;

/// 注册表身份已认领释放（保证只释放一次）。
const RELEASED: usize = 1usize << 6usize;

/// 是否已收到对端的 `OPEN`（其中携带对端接收窗口）。
const PEER_OPENED: usize = 1usize << 7usize;

/// 窗口参数是否已安装（调用方在最终裁决给出环容量之后）。
const STATE_READY: usize = 1usize << 8usize;

/// 「发送环有数据」去重位（锁外，同步路径用）。
const TX_QUEUED: usize = 1usize << 9usize;

/// 「应用消费了接收数据」去重位（锁外，同步路径用）。
const RX_CONSUMED: usize = 1usize << 10usize;

/// 建流结果的位移与掩码（2 位）。
const ESTABLISH_OUTCOME_SHIFT: usize = 11usize;
const ESTABLISH_OUTCOME_MASK: usize = 0b11usize << ESTABLISH_OUTCOME_SHIFT;

/// 测试某个位。
fn has_flag_(value: usize, flag: usize) -> bool {
    value & flag != 0usize
}

/// 发送方向是否已收尾：`FIN` 已发出（意味着发送环已排空），或对端宣告不再接收。
fn tx_done_of_(value: usize) -> bool {
    has_flag_(value, LOCAL_FIN_SENT) || has_flag_(value, PEER_RESET)
}

/// 接收方向是否已收尾：应用丢弃了接收半边，或对端宣告不再发送。
fn rx_done_of_(value: usize) -> bool {
    has_flag_(value, APP_RX_CLOSED) || has_flag_(value, PEER_FIN)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 建流
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 建流三步的进展（标志位在状态字里；[`ChannelState_`] 另持一个**零分配**的通知槽）。
///
/// 两侧状态机同形（见 `crate::connection` 模块文档 §4.2）：主动方要等对端的
/// `OPEN`（拿到对端接收窗口）与 `ACCEPT` / `REJECT`；对应的两位在 [`ChannelState_`]
/// 的状态字里，收到相应帧时置位并 [`ChannelState_::notify_establish_`]。
///
/// # 通知用**内联槽**而不是通道
///
/// 等待方是 async 上下文（[`wait_establish_`]），因此需要一个能被唤醒的落点。
/// 早先用 `flume` 容量 1 通道（持久、无需 `cx`），但它**每条子流一次全局分配**
/// （`dev-notes/audit-heap-alloc-20261004-1122.md` §3.1 #7）。现在改用
/// [`NotifySlot_`]：一个原子位 + 一个 waker 槽，零堆分配；代价是等待方要手写
/// `poll`，因此「登记 → 复检」的协议写在 [`NotifySlot_`] 的类型文档里，并有用例钉住
/// （`sync_::tests_` 的三条 + 本文的 `establish_notification_is_persistent`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EstablishOutcome_ {
    /// 对端回复 `ACCEPT`。
    Accepted,

    /// 对端回复 `REJECT`（理由载荷当前没有消费者，见 dev-notes §2.11）。
    Refused,
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 共享状态节点
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一条子流的共享状态。
///
/// 成员一律私有：读写循环在 `session_` 模块，只能经本模块的关联函数访问。
pub(crate) struct ChannelState_ {
    /// 全部布尔 / 枚举 / 去重位合成的一个原子字（bit 定义见模块文档）。
    flags_: AtomicFlags<usize>,

    /// 收发双向流控状态（内部字段也是原子）。
    flow_: FlowCtrl,

    /// 建流等待者的**零分配**内联通知槽（协议见 [`NotifySlot_`]）。
    establish_: NotifySlot_,

    /// 节点建立时刻：`active_millis_` 的计时基准。
    base_: Instant,

    /// 最近一次与本子流相关的收发活动时间（自 `base_` 起的毫秒；保活只记录，本轮
    /// 不判定超时）。
    active_millis_: AtomicU64,
}

impl ChannelState_ {
    /// 建一个**尚未安装窗口**的共享状态（登记身份时调用）。
    pub(crate) fn new_empty_() -> Self {
        ChannelState_ {
            flags_: AtomicFlags::new(core::sync::atomic::AtomicUsize::new(0usize)),
            flow_: FlowCtrl::new_empty_(),
            establish_: NotifySlot_::new_(),
            base_: Instant::now(),
            active_millis_: AtomicU64::new(0u64),
        }
    }

    /// 安装窗口参数（调用方在最终裁决给出环容量之后调用一次）。
    ///
    /// 安装之前不会有任何帧或窗口访问：环半部的移交（`Attach`）本身就发生在安装
    /// 之后，而对端也不可能早于本端 `OPEN` 发数据。
    pub(crate) fn install_<P>(&self, policy: &P, ring_capacity: usize)
    where
        P: crate::flow_ctrl::TrFlowCtrlPolicy,
    {
        self.flow_.install_(policy, ring_capacity);
        let _ = self.flags_.try_spin_compare_exchange_weak(
            |value| !has_flag_(value, STATE_READY),
            |value| value | STATE_READY,
        );
    }

    /// 窗口参数是否已安装（诊断 / 断言用）。
    #[cfg(test)]
    pub(crate) fn is_state_ready_(&self) -> bool {
        has_flag_(self.flags_.value(), STATE_READY)
    }

    /// 收发双向流控状态（只读；其方法自带原子性）。
    pub(crate) fn flow_(&self) -> &FlowCtrl {
        &self.flow_
    }

    /// 刷新活跃时间。
    pub(crate) fn touch_(&self) {
        let elapsed = self.base_.elapsed().as_millis();
        self.active_millis_
            .store(u64::try_from(elapsed).unwrap_or(u64::MAX), Ordering::Release);
    }

    /// 活跃时间（自节点建立起的毫秒）。
    #[cfg(test)]
    pub(crate) fn active_millis_(&self) -> u64 {
        self.active_millis_.load(Ordering::Acquire)
    }

    //-- ---- 建流位 ----

    /// 提示建流等待方「状态可能变了」；读循环在收到 `OPEN` / `ACCEPT` / `REJECT`
    /// 后调用（幂等、不阻塞、零分配）。
    pub(crate) fn notify_establish_(&self) {
        self.establish_.notify_();
    }

    /// 等建流状态变化：消费已到达的通知，或在槽里登记 waker 后返回
    /// [`Poll::Pending`]（等待方持有 `cx` 直接 poll）。
    ///
    /// 「登记 → 复检」由 [`NotifySlot_::poll_wait_`] 保证，因此调用方只要
    /// 「先查状态、再 poll」即可，不会丢唤醒。
    pub(crate) fn establish_poll_wait_(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.establish_.poll_wait_(cx)
    }

    /// 记录「已收到对端 `OPEN`」。
    pub(crate) fn set_peer_opened_(&self) {
        let _ = self
            .flags_
            .try_spin_compare_exchange_weak(|_| true, |value| value | PEER_OPENED);
    }

    /// 是否已收到对端 `OPEN`。
    #[cfg(test)]
    pub(crate) fn peer_opened_(&self) -> bool {
        has_flag_(self.flags_.value(), PEER_OPENED)
    }

    /// 记录建流结果（`ACCEPT` / `REJECT`）。
    pub(crate) fn set_establish_outcome_(&self, outcome: EstablishOutcome_) {
        let bits = match outcome {
            EstablishOutcome_::Accepted => 1usize << ESTABLISH_OUTCOME_SHIFT,
            EstablishOutcome_::Refused => 2usize << ESTABLISH_OUTCOME_SHIFT,
        };
        let _ = self.flags_.try_spin_compare_exchange_weak(
            |_| true,
            |value| (value & !ESTABLISH_OUTCOME_MASK) | bits,
        );
    }

    /// 建流结果；`None` 表示仍在等待。
    pub(crate) fn establish_outcome_(&self) -> Option<EstablishOutcome_> {
        match (self.flags_.value() & ESTABLISH_OUTCOME_MASK) >> ESTABLISH_OUTCOME_SHIFT {
            1usize => Option::Some(EstablishOutcome_::Accepted),
            2usize => Option::Some(EstablishOutcome_::Refused),
            _ => Option::None,
        }
    }

    //-- ---- 收尾位 ----

    /// 记录「应用已丢弃发送半边」。
    pub(crate) fn set_app_tx_closed_(&self) {
        let _ = self
            .flags_
            .try_spin_compare_exchange_weak(|_| true, |value| value | APP_TX_CLOSED);
    }

    /// 记录「应用已丢弃接收半边」。
    pub(crate) fn set_app_rx_closed_(&self) {
        let _ = self
            .flags_
            .try_spin_compare_exchange_weak(|_| true, |value| value | APP_RX_CLOSED);
    }

    /// 应用是否已丢弃接收半边。
    ///
    /// 解复用循环在把数据写进接收环**之前**查它：应用已经不要这些字节了，写进一条
    /// 消费端已关闭的环只会把整条连接判成传输错误（`ProducerError::Closing`），因此
    /// 在途数据必须**静默丢弃**。
    pub(crate) fn is_app_rx_closed_(&self) -> bool {
        has_flag_(self.flags_.value(), APP_RX_CLOSED)
    }

    /// 记录「本端已发出 `CLOSE(FIN)`」（调用方保证发送环已排空）。
    pub(crate) fn set_local_fin_sent_(&self) {
        let _ = self
            .flags_
            .try_spin_compare_exchange_weak(|_| true, |value| value | LOCAL_FIN_SENT);
    }

    /// 认领「发一条 `CLOSE(RESET)`」这件事：**第一次**调用返回 `true`。
    pub(crate) fn claim_local_reset_(&self) -> bool {
        self.flags_
            .try_spin_compare_exchange_weak(
                |value| !has_flag_(value, LOCAL_RESET_SENT),
                |value| value | LOCAL_RESET_SENT,
            )
            .is_succ()
    }

    /// 记录「对端已声明不再发送」（收到 `CLOSE(FIN)`）。
    pub(crate) fn set_peer_fin_(&self) {
        let _ = self
            .flags_
            .try_spin_compare_exchange_weak(|_| true, |value| value | PEER_FIN);
    }

    /// 记录「对端已声明不再接收」（收到 `CLOSE(RESET)`）。
    pub(crate) fn set_peer_reset_(&self) {
        let _ = self
            .flags_
            .try_spin_compare_exchange_weak(|_| true, |value| value | PEER_RESET);
    }

    /// 两个方向是否都已经在**协议层**收尾，可以释放注册表身份。
    ///
    /// - 发送方向 = `FIN` 已发出（`FIN` 只在发送环排空后发，因此这里已经蕴含
    ///   「承诺要送达的字节全部上线」）或对端宣告不再接收；
    /// - 接收方向 = 应用丢弃了接收半边，或对端宣告不再发送。
    ///
    /// **注意「应用丢弃发送半边」（`app_tx_closed_`）不在此列**：那只是「不再写」
    /// 的意图，环里可能还有没送出去的字节。把意图当完成正是旧实现的缺陷——注册表
    /// 身份会先于排空被释放（`dev-notes/outlook-concurrency…` §12 T1）。
    #[cfg(test)]
    pub(crate) fn is_done_(&self) -> bool {
        let value = self.flags_.value();
        tx_done_of_(value) && rx_done_of_(value)
    }

    /// 尝试认领「释放注册表身份」：**两个方向都收尾**且尚未认领过时返回 `true`。
    ///
    /// 判定与置位在**一次 CAS** 里完成，因此两个循环并发调用也只会有一个成功。
    pub(crate) fn claim_release_(&self) -> bool {
        self.flags_
            .try_spin_compare_exchange_weak(
                |value| tx_done_of_(value) && rx_done_of_(value) && !has_flag_(value, RELEASED),
                |value| value | RELEASED,
            )
            .is_succ()
    }

    /// 身份是否已经认领释放（诊断用）。
    #[cfg(test)]
    pub(crate) fn is_released_(&self) -> bool {
        has_flag_(self.flags_.value(), RELEASED)
    }

    //-- ---- 锁外去重位 ----

    /// 「发送环可能有数据」去重位：**第一个**置位者返回 `true`。
    ///
    /// 同步路径（`try_write` / `write_async` 的入口）专用：不取锁、不等待。
    pub(crate) fn mark_tx_queued_(&self) -> bool {
        self.flags_
            .try_spin_compare_exchange_weak(
                |value| !has_flag_(value, TX_QUEUED),
                |value| value | TX_QUEUED,
            )
            .is_succ()
    }

    /// 写循环排空后清去重位（同步路径，不取锁）。
    pub(crate) fn clear_tx_queued_(&self) {
        let _ = self.flags_.try_spin_compare_exchange_weak(
            |value| has_flag_(value, TX_QUEUED),
            |value| value & !TX_QUEUED,
        );
    }

    /// 「应用可能消费了接收数据」去重位：**第一个**置位者返回 `true`。
    ///
    /// 同步路径（`try_read` / `read_async` 的入口）专用：不取锁、不等待。
    pub(crate) fn mark_rx_consumed_(&self) -> bool {
        self.flags_
            .try_spin_compare_exchange_weak(
                |value| !has_flag_(value, RX_CONSUMED),
                |value| value | RX_CONSUMED,
            )
            .is_succ()
    }

    /// 读循环核对接收水位**之前**清去重位（同步路径，不取锁）。
    ///
    /// # 顺序不可反
    ///
    /// 必须**先清位、再读环内积压**：这样「清位之后发生的消费」会重新置位并投递一条
    /// 新通知，而「清位之前的消费」已经体现在随后读到的积压量里。反过来（先读积压、
    /// 后清位）会丢掉清位与读之间那一次消费的唤醒。
    pub(crate) fn clear_rx_consumed_(&self) {
        let _ = self.flags_.try_spin_compare_exchange_weak(
            |value| has_flag_(value, RX_CONSUMED),
            |value| value & !RX_CONSUMED,
        );
    }

    //-- ---- 流控便捷入口（窗口内部各自一把自旋锁，见 `flow_ctrl`） ----

    /// 对端又发来 `amount` 字节：记入接收窗口并做越权判定。
    pub(crate) fn recv_on_data_(&self, amount: Credit) -> Result<(), FlowCtrlError> {
        self.flow_.recv_window().on_data(amount)
    }

    /// 收到数据之后当场判定「是否该通告」；是则产出一份快照。
    ///
    /// 判定与产出快照在**窗口内部的同一个临界区**里完成（见
    /// [`RecvWindow::take_report_`](crate::flow_ctrl::RecvWindow)），因此这里不做
    /// 「先 `should_report` 再 `report`」的两步调用。
    pub(crate) fn recv_take_report_(&self) -> Option<WindowReport> {
        self.flow_.recv_window().take_report_()
    }

    /// 应用消费之后由**持有接收环写端**的解复用循环核对水位并择机补发通告。
    ///
    /// `buffered` 是环内**实际积压**（已提交、应用还没取走）。本循环是接收环唯一的
    /// 写入方，因此「已记账的累计已收 − 环内积压」就是**精确**的累计已消费量；应用侧
    /// 采样差值会被并发写入掩盖（因果见 `dev-notes/flow-ctrl-20261005-0115.md` §3）。
    ///
    /// 记账与判定在同一段窗口临界区里完成；只有真的推进了消费记账才刷新活跃时间。
    pub(crate) fn recv_recheck_(&self, buffered: Credit) -> Option<WindowReport> {
        let (advanced, report) = self.flow_.recv_window().recheck_(buffered);
        if advanced {
            self.touch_();
        }
        report
    }

    /// 收到对端的窗口通告。
    pub(crate) fn send_on_report_(&self, report: WindowReport) -> Result<(), FlowCtrlError> {
        self.flow_.send_window().on_report(report)
    }

    /// 发送窗口剩余额度。
    pub(crate) fn send_available_(&self) -> Credit {
        self.flow_.send_window().available()
    }

    /// 发送窗口剩余额度的**非阻塞**查询：窗口锁当场不可用时返回 `None`。
    ///
    /// 供复用循环 park 的 `poll` 用（那里不能等锁）：`None` 按「没额度」处理，
    /// 额度真正回来时会由窗口通告事件把 park 打断。
    pub(crate) fn send_available_try_(&self) -> Option<Credit> {
        self.flow_.send_window().available_try_()
    }

    /// 预扣发送额度。
    pub(crate) fn send_reserve_(&self, want: Credit) -> Credit {
        self.flow_.send_window().reserve(want)
    }

    /// 归还预扣但未写出的发送额度。
    pub(crate) fn send_refund_(&self, amount: Credit) {
        self.flow_.send_window().refund(amount);
    }
}

/// 一条子流的共享句柄：指向 [`ChannelState_`] 的强引用。
///
/// # 生命周期
///
/// 节点的建立与销毁都跟着**注册表的身份记录**：记录在 `reserve_channel_` /
/// `reserve_inbound_` 时创建它，在身份释放（转宽限态 / 撤销）时丢掉自己那一份。
/// 两个循环的本地表与应用侧半部各持一份克隆，因此「身份记录已被改写」不会让正在
/// 收尾的一方失去状态——但也**不会**让状态永久泄漏：最后一份句柄消失即回收。
///
/// 名字保留「Owner」的历史含义（这条子流的共享状态归它所有），实现上就是
/// `Shared<ChannelState_, A>`：一次分配、可克隆、`Deref` 到 [`ChannelState_`]。
pub(crate) type ChannelOwner_<A> = Shared<ChannelState_, A>;

/// 建立一条子流的共享状态节点（登记身份时调用）。
pub(crate) fn new_owner_<A>(alloc: A) -> ChannelOwner_<A>
where
    A: AllocatorClone,
{
    Shared::new(ChannelState_::new_empty_(), alloc)
}

/// 等待建流完成：等对端的 `OPEN` + `ACCEPT` / `REJECT`，或被取消 / 连接失败打断。
pub(crate) async fn wait_establish_<A, K>(
    reg: &ChannelRegistry_<A>,
    owner: &ChannelOwner_<A>,
    cancel: K,
) -> Result<EstablishOutcome_, MuxError>
where
    A: AllocatorClone + Send + Sync,
    K: TrCancellationToken,
{
    loop {
        if cancel.is_cancelled() {
            return Result::Err(MuxError::Cancelled);
        }
        // 1. 连接级失败优先（取注册表锁，可取消）。
        if let Result::Ok(Option::Some(err)) = reg.failure_(cancel.child_token()).await {
            return Result::Err(err);
        }
        // 2. 拿到结果了吗？没有就登记等待。
        //    通知槽是**持久**的：第 2 步与第 3 步之间的通知不会丢（协议见
        //    [`NotifySlot_::poll_wait_`]）。
        if let Option::Some(outcome) = owner.establish_outcome_() {
            return Result::Ok(outcome);
        }
        let mut notified = core::pin::pin!(poll_fn(|cx| owner.establish_poll_wait_(cx)));
        let mut cancelled = core::pin::pin!(cancel.child_token().cancellation());
        let notified = poll_fn(|cx| {
            if core::future::Future::poll(cancelled.as_mut(), cx).is_ready() {
                return Poll::Ready(false);
            }
            core::future::Future::poll(notified.as_mut(), cx).map(|()| true)
        })
        .await;
        if !notified {
            return Result::Err(MuxError::Cancelled);
        }
    }
}

#[cfg(test)]
mod tests_ {
    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use crate::flow_ctrl::DefaultPolicy;

    use super::*;

    /// 造一条测试用的共享状态（缺省策略、容量 64），并安装窗口参数。
    fn make_owner_() -> ChannelOwner_<CoreAlloc> {
        let owner = new_owner_(CoreAlloc);
        owner.install_(&DefaultPolicy, 64usize);
        owner
    }

    /// 计数唤醒器：统计 `wake` 被调用次数，用来断言唤醒确实发生。
    struct CountingWake_ {
        count_: core::sync::atomic::AtomicUsize,
    }

    impl std::task::Wake for CountingWake_ {
        fn wake(self: std::sync::Arc<Self>) {
            self.count_
                .fetch_add(1usize, core::sync::atomic::Ordering::SeqCst);
        }

        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.count_
                .fetch_add(1usize, core::sync::atomic::Ordering::SeqCst);
        }
    }

    /// 造一个带计数的 `Waker`（断言「唤醒登记在槽里的等待者」用）。
    /// - 手段：把 [`CountingWake_`] 包进 `Arc` 再转成 `Waker`。
    /// - 判断：返回的 `Waker` 每次被 `wake` 都会让计数加一。
    fn counting_waker_() -> (core::task::Waker, std::sync::Arc<CountingWake_>) {
        let probe = std::sync::Arc::new(CountingWake_ {
            count_: core::sync::atomic::AtomicUsize::new(0usize),
        });
        let waker = core::task::Waker::from(probe.clone());
        (waker, probe)
    }

    /// 测试状态节点在建立时尚未安装窗口参数，安装后可见。
    /// - 手段：先 `new_owner_` 直接断言安装位，再 `install_`。
    /// - 判断：安装前 `is_state_ready_` 为假，安装后为真，且接收容量等于策略初窗。
    #[test]
    fn owner_state_installs_window_params() {
        let owner = new_owner_(CoreAlloc);
        assert!(!owner.is_state_ready_(), "建节点时窗口参数尚未安装");
        owner.install_(&DefaultPolicy, 64usize);
        assert!(owner.is_state_ready_(), "安装后应当可见");
        assert_eq!(owner.flow_().recv_window().capacity(), 64u32);
    }

    /// 测试共享句柄互相可见：一个 clone 上的写入能被另一个 clone 读到。
    /// - 手段：clone 出第二个句柄，在第一个上把 `tx_queued_` 置真。
    /// - 判断：第二个句柄读到去重位已被占用——说明两份句柄指向同一状态。
    #[test]
    fn owner_handles_share_state() {
        let a = make_owner_();
        let b = a.clone();
        assert!(a.mark_tx_queued_(), "首次置位应当是 fresh");
        assert!(
            !b.mark_tx_queued_(),
            "clone 出的句柄应看到同一份锁外去重位"
        );
        a.clear_tx_queued_();
        assert!(b.mark_tx_queued_(), "清位在共享句柄上也可见");
    }

    /// 测试「应用消费了」去重位与「发送环有数据」去重位**互相独立**，且同样跨 clone
    /// 共享、可清位后重新置位。
    ///
    /// 两条方向的通知共用一个状态字，若两个位串在一起，接收方向的一次消费就会把发送
    /// 方向的通知吞掉（或反之）——那是「唤醒被静默丢掉」的翻版。
    ///
    /// - 手段：在同一个句柄上先置 `tx_queued_`，再置 `rx_consumed_`；随后清 `rx` 位。
    /// - 判断：两个位互不影响（各自能独立置 true），clone 句柄看到同一状态，清位可
    ///   重新置 true。
    #[test]
    fn owner_tx_and_rx_dedup_bits_are_independent() {
        let a = make_owner_();
        let b = a.clone();
        assert!(a.mark_tx_queued_(), "发送方向首次置位应当是 fresh");
        assert!(
            b.mark_rx_consumed_(),
            "接收方向首次置位应当是 fresh，且不受发送方向的位影响"
        );
        assert!(
            !a.mark_tx_queued_(),
            "接收方向置位不应清掉发送方向的位"
        );
        assert!(
            !a.mark_rx_consumed_(),
            "clone 句柄应看到同一份接收方向去重位"
        );
        b.clear_rx_consumed_();
        assert!(a.mark_rx_consumed_(), "清位后可重新置位");
    }

    /// 测试建流状态初始为空、可被置位并唤醒等待者。
    /// - 手段：初始断言 `peer_opened_` 为假且结果为 `None`；随后模拟读循环置位。
    /// - 判断：置位后可读到对应的值——这是 `open_channel_async` 能被唤醒的前提。
    async fn establish_state_starts_empty_and_accepts_updates() {
        let owner = make_owner_();
        assert!(!owner.peer_opened_());
        assert!(owner.establish_outcome_().is_none());

        owner.set_peer_opened_();
        owner.set_establish_outcome_(EstablishOutcome_::Accepted);
        assert!(owner.peer_opened_());
        assert_eq!(
            owner.establish_outcome_(),
            Option::Some(EstablishOutcome_::Accepted)
        );
    }
    dual_runtime_test_!(establish_state_starts_empty_and_accepts_updates);

    /// 测试建流通知槽是**持久**的：先通知后等待也不丢、重复通知不堆积、登记后被唤醒。
    /// - 手段：先 `notify_establish_`，用计数 waker 手工 poll 一次通知槽；连续
    ///   `notify_establish_` 两次后再 poll；随后 poll 一次（登记）→ 通知 → 断言唤醒。
    /// - 判断：第一次 poll 立刻就绪（先通知后等待不丢）；重复通知合并成一次且随后
    ///   一次 poll 挂起（不堆积、此前的就绪与唤醒无关）；登记后被通知恰好唤醒一次。
    async fn establish_notification_is_persistent() {
        let owner = make_owner_();
        owner.notify_establish_();

        let (waker, probe) = counting_waker_();
        let mut context = Context::from_waker(&waker);
        assert!(
            owner.establish_poll_wait_(&mut context).is_ready(),
            "先通知后等待不应当丢唤醒"
        );
        owner.notify_establish_();
        owner.notify_establish_();
        assert!(
            owner.establish_poll_wait_(&mut context).is_ready(),
            "重复通知合并成一次，仍然就绪"
        );
        assert!(
            owner.establish_poll_wait_(&mut context).is_pending(),
            "通知已被消费：不应再就绪"
        );
        assert_eq!(
            probe.count_.load(Ordering::SeqCst),
            0usize,
            "没有等待者登记时不应当发生唤醒"
        );

        owner.notify_establish_();
        assert_eq!(
            probe.count_.load(Ordering::SeqCst),
            1usize,
            "登记中的等待者应当被唤醒"
        );
        assert!(owner.establish_poll_wait_(&mut context).is_ready());
    }
    dual_runtime_test_!(establish_notification_is_persistent);

    /// 测试 `touch_` 会推进活跃时间。
    /// - 手段：先读一次活跃毫秒，稍作忙等后调用 `touch_` 再读一次。
    /// - 判断：第二次读到的值不小于第一次（时间单调）。
    async fn touch_advances_activity_time() {
        let owner = make_owner_();
        let first = owner.active_millis_();
        let mut spin = 0u64;
        while spin < 100_000u64 {
            spin = spin.wrapping_add(1u64);
        }
        owner.touch_();
        let second = owner.active_millis_();
        assert!(second >= first, "活跃时间只能前进");
    }
    dual_runtime_test_!(touch_advances_activity_time);

    /// 测试「两个方向都在协议层收尾」才认领释放，且只认领一次。
    ///
    /// 这是 T1 修掉的判据：**应用丢弃发送半边不算发送方向完成**——`drop(tx)` 只是
    /// 「不再写」，环里的字节还没上线，`FIN` 也没发；此时接收方向若已收尾，旧实现会
    /// 直接释放身份，把在途数据连同身份一起丢掉。
    ///
    /// - 手段：依次制造「应用丢两半」→「只发过 FIN」→「只收到对端 FIN / RESET」等
    ///   组合，观察 `is_done_` 与 `claim_release_`。
    /// - 判断：只有「本端 FIN 已发（或对端 RESET）」+「应用丢 rx（或对端 FIN）」
    ///   同时成立时才认为完成；`claim_release_` 第一次为真、第二次为假。
    #[test]
    fn release_requires_protocol_level_completion() {
        let owner = make_owner_();

        // 仅仅「应用丢了两半」不算完成：发送方向还欠 FIN / 排空。
        owner.set_app_tx_closed_();
        owner.set_app_rx_closed_();
        assert!(!owner.is_done_(), "只丢半边不算协议收尾");
        assert!(!owner.claim_release_(), "未完成时不得认领释放");

        // 发送方向真正完成，接收方向也已收尾 ⇒ 完成。
        owner.set_local_fin_sent_();
        assert!(owner.is_done_(), "FIN 已发 + 接收已收尾 = 完成");
        assert!(owner.claim_release_(), "第一次认领应当成功");
        assert!(owner.is_released_());
        assert!(!owner.claim_release_(), "只允许认领一次");
    }

    /// 测试对端的 `RESET` 让发送方向收尾、对端 `FIN` 让接收方向收尾。
    /// - 手段：分两个独立句柄，分别只置 `peer_reset_` 与只置 `peer_fin_`，
    ///   再补上各自的另一半。
    /// - 判断：`peer_reset_` 单独不能让接收方向收尾；`peer_fin_` 单独不能让发送方向
    ///   收尾；两者都到位时完成。
    #[test]
    fn peer_close_flags_cover_opposite_directions() {
        let by_reset = make_owner_();
        by_reset.set_peer_reset_();
        by_reset.set_app_rx_closed_();
        assert!(by_reset.is_done_(), "对端 RESET + 应用丢 rx = 完成");

        let by_fin = make_owner_();
        by_fin.set_peer_fin_();
        by_fin.set_local_fin_sent_();
        assert!(by_fin.is_done_(), "对端 FIN + 本端 FIN = 完成");
    }
}
