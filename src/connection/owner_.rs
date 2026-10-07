//! 每条身份的**共享节点**与它的类型化句柄：[`DockBinding_`]（节点）/
//! [`ChannelOwner_`]（channel 句柄）/ [`ChannelState_`]（节点内联的热状态）。
//!
//! 环半部**不在这里**：会话侧的两个半部在注册时**移交给对应的循环本地持有**，
//! 因此它们不会藏在共享实体的锁后面，循环可以自由在这些半部上 park / await。
//!
//! # 一个身份、一个节点、零内部堆分配
//!
//! 节点由**注册表在登记身份时**建立（`reserve_channel_` / `reserve_inbound_`），
//! 注册表的**表槽**持有它的句柄，两个循环的本地表与应用侧半部各持一份克隆。
//! 因此「身份在 ⇒ 状态在」，不再有一个可以被提前丢弃的、另行 `attach` 上来的 owner，
//! 也**没有**「状态节点」与「身份记录」两个可独立死亡的对象
//! （形状三，见 `dev-notes/identity-record-20261005-0648.md`）。
//!
//! 节点内部**零堆分配**：状态字是 `AtomicFlags`、两个窗口是「自旋锁 + 普通字段」、
//! 建流通知是内联的 [`NotifySlot_`]。分配器参数只出现在**句柄**上（`Shared<_, A>`
//! 要把分配器写进节点以便最后一个强引用归还内存）。
//!
//! # 取用路径零分支：构造时校验一次
//!
//! [`ChannelOwner_`] 是**类型化**句柄：字段私有、只能由 [`new_channel_owner_`] 从
//! `DockBinding_::Channel` 变体的节点建出（构造期校验一次），此后
//! [`Deref`](core::ops::Deref) 直接交出载荷引用——热路径（每次 `try_write`、每帧
//! 流控记账）**零分支、零 panic**。安全论证见该类型的文档与构造函数的 `SAFETY` 注释。
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
//! # 保活记账（活跃脏位 + 中止代码）
//!
//! 除状态字外，每条子流还有三个与保活有关的独立原子量，它们**不进**状态字，因为
//! 写入者不同、且都不参与「两个方向是否收尾」的 CAS 判据：
//!
//! | 字段 | 谁写 | 语义 |
//! | --- | --- | --- |
//! | `active_millis_` | 任何 `touch_` / `mark_data_` 调用点（热路径，每帧一次） | 最近一次活动（存活时钟） |
//! | `data_millis_` | 任何 `mark_data_` 调用点 + 计时循环发 `PULSE` 时 | 最近一次非保活活动（保活职责时钟） |
//! | `abort_` | **只有计时循环**（一次 CAS 认领） | 中止代码，见 [`AbortCode_`] |
//!
//! 两个时钟分开是**保活能否收敛**的前提：收到对端 `PULSE` 只刷新存活时钟，
//! 因此两端都会按自己的职责时钟继续发 `PULSE`（若合一，两端会互相把对方的保活
//! 「劝退」，先停手的那一侧必定被判空闲超时）。
//!
//! 这样切分的收益是热路径**不读时钟**：活动点只做一次原子置位，而「现在几点」由
//! 唯一持有时间语义的计时循环在扫描时打点（见 [`ChannelState_::touch_`]）。
//!
//! # 流控也是原子的
//!
//! [`FlowCtrl`]（收发双向窗口）的方法全部取 `&self`、字段全部是原子，因此对窗口的
//! 每一次记账都只是一次原子读改写；唯一需要打包的跨任务字段见
//! [`SendWindow`](crate::flow_ctrl::SendWindow) 的文档。

use core::{
    alloc::AllocatorClone,
    future::poll_fn,
    sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering},
    task::{Context, Poll},
};

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
    metrics::ConnCloseReason,
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

/// 建流是否已**裁决完毕**（本端 `accept` / `reject` 收尾）。
///
/// 计时循环据此区分同一根存活时钟上的两类到点（见 `TimerAction_`）：
/// - **未裁决** ⇒ 建流超时：向对端发 `REJECT`、释放身份、把原因留在 `abort_` 上；
/// - **已裁决** ⇒ 活跃子流空闲超时：`CLOSE(FIN)` + `CLOSE(RESET)` 拆流。
const ESTABLISH_SETTLED: usize = 1usize << 13usize;

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

/// 一条子流的**中止动作认领码**（保活判定用）。
///
/// 它只回答「谁负责执行中止这条子流的动作」（[`ChannelState_::claim_abort_`] 的 CAS
/// 首个生效），**不是**应用读到的原因——原因是 [`ChannelNotice`]，由认领成功的一方
/// 顺带发布。两者分开的理由（尤其是「连接级失败必须压过子流级原因」这条）见
/// [`ChannelNotice`]。
///
/// 目前只有一种：空闲超时。它由**计时循环**认领
/// （[`ChannelState_::claim_abort_`]）并通过 [`ChannelState_::abort_reason_`] 回传给
/// 应用侧的两个半部（[`ChannelTx::abort_reason`](super::ChannelTx::abort_reason)）。
///
/// 存成 `AtomicU8` 而不是把 `MuxError` 塞进原子：`MuxError` 是普通的 `Copy` 枚举，
/// 没有稳定的整数表示，直接把它的位模式存下来会随编译选项变化。这里只存一个
/// **代码**，读出来时再映射成 [`MuxError`]。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AbortCode_ {
    /// 空闲超时：`max_channel_timeout` 内既无数据、也无保活往来。
    IdleTimeout,
}

impl AbortCode_ {
    /// 「没有中止」的哨兵值。
    const NONE: u8 = 0u8;

    /// 空闲超时的代码值。
    const IDLE_TIMEOUT: u8 = 1u8;

    /// 编码成原子字。
    fn as_u8_(self) -> u8 {
        match self {
            AbortCode_::IdleTimeout => AbortCode_::IDLE_TIMEOUT,
        }
    }
}

/// 一条子流的**不可忽略通知**：应用被唤醒之后第一件要查的东西。
///
/// # 它与 [`AbortCode_`] 为什么是两个原子字
///
/// `AbortCode_` 是**动作认领**：CAS 成功的那一方负责执行中止动作（回 `CLOSE`、投
/// `LocalAbort` / `Release`），因此必须「首个生效、永不改写」。
/// 而通知是**应用要读的结论**，它的规则不同——见 [`ChannelNotice::priority_`]：
/// 连接级失败必须**压过**此前记下的子流级原因（连接正式判死之前，往往已经发生过
/// 大面积子流级错误；应用需要的结论是「连接没了」，而不是某一条子流为什么先停）。
/// 把两件事挤进一个字，就会让「已认领的动作」被后到的通知改写；拆开之后各守一条规则。
///
/// # 只有「不可忽略」的事实才进这里
///
/// 对端 `FIN` / `RESET` 这类**协议上正常**的半关闭不进这个槽：它们由环的关闭态与帧面
/// 状态表达（`TrChannelHalf::is_tx_closed` / `is_rx_closed`）。进槽位的只有「应用从
/// 环的 `Closing` 里读不出区别、但必须知道」的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChannelNotice {
    /// **连接级失败牵连**：连接已经不可用，在册子流一同终结。
    ///
    /// 只带**类别**；具体原因（首个 `MuxError`）留在连接级，由子流经它持有的
    /// `MuxConnection` 读取（见 [`MuxConnection::failure`]）。
    ConnFailed(ConnCloseReason),

    /// 空闲超时：`max_channel_timeout` 内既无数据往来、也无保活应答。
    ///
    /// 与 [`MuxError::IdleTimeout`] 同义，**建流未裁决**那一档也走它（两者共用同一根
    /// 存活时钟与同一个错误值，处置不同而已）。
    IdleTimeout,
}

impl ChannelNotice {
    /// 优先级：数值大的压过数值小的，槽位只允许**升级**。
    ///
    /// 顺序即裁决：连接级失败 > 空闲超时。加新变体时在这里给出它的档位。
    const fn priority_(self) -> u8 {
        match self {
            ChannelNotice::ConnFailed(_) => 2u8,
            ChannelNotice::IdleTimeout => 1u8,
        }
    }

    /// 投影成应用侧看到的那一个 [`MuxError`]。
    fn as_error_(self) -> MuxError {
        match self {
            ChannelNotice::ConnFailed(kind) => MuxError::ConnFailed(kind),
            ChannelNotice::IdleTimeout => MuxError::IdleTimeout,
        }
    }
}

/// 「没有通知」的哨兵值（整个槽位字为 `0`）。
const K_NOTICE_NONE: u32 = 0u32;

/// 通知槽里优先级字段的位移（低位留给「种类 + 载荷」）。
const K_NOTICE_PRIO_SHIFT: u32 = 8u32;

/// 空闲超时的种类码。
const K_NOTICE_IDLE_TIMEOUT: u32 = 1u32;

/// 连接级失败的种类码（低 4 位放 [`ConnCloseReason`] 的编码）。
const K_NOTICE_CONN_FAILED: u32 = 0x10u32;

/// 把一条通知编码进槽位字：高 8 位优先级、低位种类与载荷。
///
/// `ConnCloseReason` 只有 4 个无字段变体，因此低 4 位就装得下——这也是 [`AtomicU32`]
/// 足够的原因（没有分配、没有锁、任何线程都能读）。
const fn encode_notice_(notice: ChannelNotice) -> u32 {
    let prio = (notice.priority_() as u32) << K_NOTICE_PRIO_SHIFT;
    match notice {
        ChannelNotice::IdleTimeout => prio | K_NOTICE_IDLE_TIMEOUT,
        ChannelNotice::ConnFailed(kind) => prio | K_NOTICE_CONN_FAILED | encode_conn_reason_(kind),
    }
}

/// 槽位字的优先级。
const fn notice_prio_of_(raw: u32) -> u8 {
    (raw >> K_NOTICE_PRIO_SHIFT) as u8
}

/// 从槽位字解码；`0` 与未知组合都解码成 `None`。
const fn decode_notice_(raw: u32) -> Option<ChannelNotice> {
    if raw == K_NOTICE_NONE {
        return Option::None;
    }
    if raw & K_NOTICE_CONN_FAILED != 0u32 {
        return Option::Some(ChannelNotice::ConnFailed(decode_conn_reason_(
            raw & 0x0fu32,
        )));
    }
    if raw & K_NOTICE_IDLE_TIMEOUT != 0u32 {
        return Option::Some(ChannelNotice::IdleTimeout);
    }
    Option::None
}

/// [`ConnCloseReason`] 的锁外编码（只用于通知槽的低 4 位）。
const fn encode_conn_reason_(kind: ConnCloseReason) -> u32 {
    match kind {
        ConnCloseReason::Local => 1u32,
        ConnCloseReason::PeerClosed => 2u32,
        ConnCloseReason::Transport => 3u32,
        ConnCloseReason::ProtocolError => 4u32,
    }
}

/// [`encode_conn_reason_`] 的逆；未知值按「协议错误」处理（防御，正常不会出现）。
const fn decode_conn_reason_(code: u32) -> ConnCloseReason {
    match code {
        1u32 => ConnCloseReason::Local,
        2u32 => ConnCloseReason::PeerClosed,
        3u32 => ConnCloseReason::Transport,
        _ => ConnCloseReason::ProtocolError,
    }
}

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

    /// **创建时刻**：该子流登记身份时的连接内毫秒。
    ///
    /// 与下面两个「活动时钟」不同：它们会被每次活动刷新，本字段**只写一次**，唯一的
    /// 读者是子流关闭时的寿命结算（[`crate::metrics::TrMetricsSink::on_channel_closed`]）。
    created_millis_: AtomicU64,

    /// **存活时钟**：最近一次收到任何一帧（**含**对端 `PULSE`）或本端写出一段
    /// 数据的连接内毫秒。`max_channel_timeout` 的判据是它。
    active_millis_: AtomicU64,

    /// **保活职责时钟**：最近一次**非保活**活动（数据 / 建流帧 / 窗口通告）的连接内
    /// 毫秒，也包含本端自己上一次发出 `PULSE` 的时刻。`PULSE` 的职责以它为准。
    ///
    /// 与 [`ChannelState_::active_millis_`] 分开是**保活能否收敛**的前提：
    /// **收到对端的 `PULSE` 只刷新存活时钟、不刷新本端的保活职责时钟**。若两者合一，
    /// 两端会在同一时刻互相把对方的空闲清零、于是**都不发** `PULSE`，而各自的存活
    /// 时钟又从「最后一次真实活动」起算——先停手的那一侧必定先到
    /// `max_channel_timeout` 并被拆掉（`tests/keepalive.rs` 的第一版正是这么失败的：
    /// 被判 `IdleTimeout` 的恰好是先发出 `PULSE` 的那一侧）。
    data_millis_: AtomicU64,

    /// 中止代码：[`AbortCode_::NONE`] 表示未中止（见 [`AbortCode_`]）。
    abort_: AtomicU8,

    /// **不可忽略通知**槽：`0` = 无，否则是 [`ChannelNotice`] 的编码。
    ///
    /// 与 [`ChannelState_::abort_`] 分开的理由见 [`ChannelNotice`]；这里只补两条实现
    /// 上的理由：
    ///
    /// - **无锁、零分配**：应用可能在任意线程、甚至在非异步上下文里读它（`Closing`
    ///   也能从非阻塞接口拿到），而连接级失败路径可能在 `Drop` 的调用栈里写它；
    /// - **任何线程都写得进**：连接级失败由循环侧发布，不经过注册表锁（`mark_failed_`
    ///   虽然持锁，但发布本身只是原子 CAS）。
    notice_: AtomicU32,
}

impl ChannelState_ {
    /// 建一个**尚未安装窗口**的共享状态（登记身份时调用）。
    ///
    /// 两个时钟都从 `0` 起，**必须**由登记路径立刻用「现在」打点一次
    /// （[`ChannelState_::mark_data_`]）：否则连接建立了很久之后才出现的子流会被算成
    /// 「自连接建立起就没动过」，第一条扫描就判它空闲超时。
    pub(crate) fn new_empty_() -> Self {
        ChannelState_ {
            flags_: AtomicFlags::new(core::sync::atomic::AtomicUsize::new(0usize)),
            flow_: FlowCtrl::new_empty_(),
            establish_: NotifySlot_::new_(),
            created_millis_: AtomicU64::new(0u64),
            active_millis_: AtomicU64::new(0u64),
            data_millis_: AtomicU64::new(0u64),
            abort_: AtomicU8::new(AbortCode_::NONE),
            notice_: AtomicU32::new(K_NOTICE_NONE),
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

    /// 记下「本子流**还活着**」：刷新**存活时钟**（`max_channel_timeout` 的判据）。
    ///
    /// 调用点是**收到的任何一帧**——包括对端的 `PULSE`：收到保活应答正是「对端还
    /// 活着」的直接证据。
    ///
    /// **本端发出的 `PULSE` 不算**：它是探测本身，若能刷新自己的存活时钟，对端已死
    /// 时本端也会一直给自己续命，空闲超时永远不会触发。
    ///
    /// # 为什么时刻由调用方传进来
    ///
    /// 活动发生在没有时钟的地方不合适——**但时刻必须就地记下**，不能像早先那样只
    /// 置一个脏位、等计时循环扫描时才打点：扫描是有周期的，一条在扫描间隙里到达的
    /// 建流帧会被记成「扫描那一刻才活动」，于是本端的 `PULSE` 职责被无谓地推迟一整个
    /// 保活周期，对端可能先一步判超时（`tests/keepalive.rs` 撞到的正是这个）。
    ///
    /// 调用方（解复用 / 复用循环）手上都有连接级时钟，每次活动读一次 `Instant::now()`
    /// 就够——它是一次 vDSO 读，与本循环每帧的解析 / 环操作相比可以忽略。
    pub(crate) fn touch_(&self, now_millis: u64) {
        self.active_millis_.store(now_millis, Ordering::Release);
    }

    /// 记下「本子流有过**非保活**活动」：同时刷新存活时钟与**保活职责时钟**。
    ///
    /// 调用点覆盖收发两个方向的真实流量：收到的数据 / `OPEN` / `ACCEPT` / `CLOSE` /
    /// 窗口通告，以及**本端成功写出的一段数据**。
    ///
    /// 为什么本端发送也算：纯下载方向的接收方可能长时间一个字节都不发（应用消费得
    /// 慢、窗口通告迟迟不触发），若只认「收到」，发送方会把一条**完全健康**的连接
    /// 判成空闲超时。两个方向对称之后，只要还有数据在动，两端都看得见活动。
    ///
    /// 与 [`ChannelState_::touch_`] 的差别只有一处，但那一处决定保活能否收敛：
    /// 收到对端的 `PULSE` 只走 `touch_`，**不**重置本端的保活职责时钟——否则两端会
    /// 互相把对方的保活「劝退」，谁都等不到对方的 `PULSE`。
    pub(crate) fn mark_data_(&self, now_millis: u64) {
        self.active_millis_.store(now_millis, Ordering::Release);
        self.data_millis_.store(now_millis, Ordering::Release);
    }

    /// 最近一次活动的连接内毫秒（存活时钟）。
    pub(crate) fn active_millis_(&self) -> u64 {
        self.active_millis_.load(Ordering::Acquire)
    }

    /// 记下**创建时刻**：登记身份时调用一次（与 [`ChannelState_::mark_data_`] 同处）。
    ///
    /// 单独一个字段、而不复用 `data_millis_`：后者会被后续每一次活动刷新，而寿命结算
    /// 要的是「最初那一笔」。
    pub(crate) fn set_created_millis_(&self, now_millis: u64) {
        self.created_millis_.store(now_millis, Ordering::Release);
    }

    /// 创建时刻（连接内毫秒）。
    pub(crate) fn created_millis_(&self) -> u64 {
        self.created_millis_.load(Ordering::Acquire)
    }

    /// 最近一次非保活活动的连接内毫秒（保活职责时钟）。
    pub(crate) fn data_millis_(&self) -> u64 {
        self.data_millis_.load(Ordering::Acquire)
    }

    /// 写入「上一次发出 `PULSE`」的时刻（**只有计时循环调用**）。
    ///
    /// 发出保活探测**不**刷新存活时钟（否则对端已死也永远不超时），但它必须推进
    /// **保活职责时钟**，否则下一轮扫描会立刻再发一条。
    pub(crate) fn set_data_millis_(&self, millis: u64) {
        self.data_millis_.store(millis, Ordering::Release);
    }

    /// 认领「中止这条子流」：第一次调用返回 `true`（此后 `is_aborted_` 恒为真）。
    ///
    /// 认领成功时**顺带发布**对应的不可忽略通知（[`ChannelNotice::IdleTimeout`]）：
    /// 「谁负责执行中止动作」与「应用读到什么原因」是两件事（见 [`ChannelNotice`]），
    /// 但**原因必须在动作之前就位**——应用可能在任何一次唤醒之后立刻读它。
    pub(crate) fn claim_abort_(&self, code: AbortCode_) -> bool {
        let claimed = self
            .abort_
            .compare_exchange(
                AbortCode_::NONE,
                code.as_u8_(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        if claimed {
            let notice = match code {
                AbortCode_::IdleTimeout => ChannelNotice::IdleTimeout,
            };
            self.publish_notice_(notice);
        }
        claimed
    }

    /// 本子流是否已被中止（计时循环判定空闲超时后为真）。
    pub(crate) fn is_aborted_(&self) -> bool {
        self.abort_.load(Ordering::Acquire) != AbortCode_::NONE
    }

    /// 发布一条**不可忽略通知**；规则是「只升级、不降级」，同优先级首个生效。
    ///
    /// 返回 `true` 表示本次调用改写了槽位。用优先级 CAS 表达两条判据（见
    /// [`ChannelNotice`]）：
    ///
    /// - **连接级失败压过子流级原因**：连接正式判死之前往往已经发生过大面积子流级
    ///   错误，应用需要知道的结论是「连接没了」，因此后到的 `ConnFailed` 会覆盖
    ///   先到的 `IdleTimeout`；
    /// - **反向永远不成立**：已经写进去的连接级结论不会被后来的子流级原因改写，
    ///   于是任何时刻读一次都拿到「至今为止最重的那个结论」。
    ///
    /// 本方法是**同步、无锁、零分配**的，任何线程、任何上下文（包括 `Drop`）都能调。
    pub(crate) fn publish_notice_(&self, notice: ChannelNotice) -> bool {
        let desire = encode_notice_(notice);
        let prio = notice_prio_of_(desire);
        let mut current = self.notice_.load(Ordering::Acquire);
        loop {
            // 槽位里已经是同级或更重的结论：不改写（同优先级首个生效）。
            if notice_prio_of_(current) >= prio {
                return false;
            }
            match self.notice_.compare_exchange_weak(
                current,
                desire,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Result::Ok(_) => return true,
                Result::Err(observed) => current = observed,
            }
        }
    }

    /// 当前的不可忽略通知（诊断 / 用例用）。
    #[cfg(test)]
    pub(crate) fn notice_(&self) -> Option<ChannelNotice> {
        decode_notice_(self.notice_.load(Ordering::Acquire))
    }

    /// 中止 / 终结原因（若有）：应用侧的两个半部经它区分「空闲超时」「连接级失败牵连」
    /// 与「对端正常关闭」。
    ///
    /// 投影自**通知槽**（而不是认领位）：连接级失败不认领中止动作（连接已经死了，
    /// 逐条回 `CLOSE` 没有意义），但它必须能被应用读到。
    pub(crate) fn abort_reason_(&self) -> Option<MuxError> {
        decode_notice_(self.notice_.load(Ordering::Acquire)).map(ChannelNotice::as_error_)
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

    /// 记下「本端已完成建流裁决」（`accept` / `reject` 收尾时置位）。幂等。
    pub(crate) fn set_establish_settled_(&self) {
        let _ = self
            .flags_
            .try_spin_compare_exchange_weak(|_| true, |value| value | ESTABLISH_SETTLED);
    }

    /// 建流是否已裁决完毕（见 [`ESTABLISH_SETTLED`]）。
    pub(crate) fn establish_settled_(&self) -> bool {
        has_flag_(self.flags_.value(), ESTABLISH_SETTLED)
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
    /// 记账与判定在同一段窗口临界区里完成；只有真的推进了消费记账才刷新**存活时钟**
    /// （这是本端应用在消费，不构成「对端还在」以外的任何保活义务，因此不推进保活
    /// 职责时钟）。
    pub(crate) fn recv_recheck_(&self, buffered: Credit, now_millis: u64) -> Option<WindowReport> {
        let (advanced, report) = self.flow_.recv_window().recheck_(buffered);
        if advanced {
            self.touch_(now_millis);
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

/// 一条身份的**共享节点**：非泛型、内部零堆分配（见 `dev-notes/identity-record-20261005-0648.md`）。
///
/// 每种身份内联自己那份内容，因此节点里**不允许**再出现任何堆分配（`Vec` / `flume`
/// 通道 / 嵌套 `Shared` / `Arc` 都不行）。分配器参数 `A` 只出现在**句柄**上
/// （[`Shared<DockBinding_, A>`](Shared)）：`mm_ptr::Shared` 把分配器写进节点以便最后一个
/// 强引用归还内存，那是「谁负责归还」，与「节点内部要不要分配」无关。
///
/// 节点一旦建出，**变体终生不变**（身份释放换的是注册表**表槽**，不动节点），
/// 这是 [`DockHandle_`] 那处无检查取用的安全前提。
pub(crate) enum DockBinding_ {
    /// 一条 channel 的身份节点：内联它的全部热状态。
    Channel(ChannelState_),

    /// 一个 telegraph 端点的身份节点（本轮仍是占位：收发队列待实现）。
    Telegraph(TgRec_),

    /// 一个 listener 的身份节点：内联它的入向通知槽。
    Listener(LsnRec_),
}

impl DockBinding_ {
    /// 取出 `Channel` 变体的载荷；不是该变体时返回 `None`（**只用于构造期校验**）。
    fn channel_(&self) -> Option<&ChannelState_> {
        match self {
            DockBinding_::Channel(payload) => Option::Some(payload),
            _ => Option::None,
        }
    }

    /// 取出 `Telegraph` 变体的载荷；不是该变体时返回 `None`（**只用于构造期校验**）。
    fn telegraph_(&self) -> Option<&TgRec_> {
        match self {
            DockBinding_::Telegraph(payload) => Option::Some(payload),
            _ => Option::None,
        }
    }

    /// 取出 `Listener` 变体的载荷；不是该变体时返回 `None`（**只用于构造期校验**）。
    fn listener_(&self) -> Option<&LsnRec_> {
        match self {
            DockBinding_::Listener(payload) => Option::Some(payload),
            _ => Option::None,
        }
    }
}

/// 一个 telegraph 端点的身份载荷（**占位**）。
///
/// telegraph 本轮只登记身份（独占 `local_dock`）与释放路径；`send_async` /
/// `recv_async` 仍是 `todo!()`。落地时在这里挂收发队列与按 `remote_dock` 的分发状态
/// ——因为节点里不许有堆分配，那些队列**不能**用 `flume`（见
/// `dev-notes/identity-record-20261005-0648.md`）。
#[derive(Debug, Default)]
pub(crate) struct TgRec_;

/// 一个 listener 的身份载荷：`local_dock` 上的**零分配**入向通知槽。
///
/// 取代原先的 `flume::bounded(1)`（audit-heap-alloc §3.1 #6：每个被监听 dock 一次
/// **全局**分配）：通知语义完全一样是「可能有入向事件」的持久提示，而等待者只有一个
/// （`ChannelListener` 是 `!Clone`、`income_async` 取 `&mut self`）。
#[derive(Debug)]
pub(crate) struct LsnRec_ {
    /// 「本 dock 上可能有入向事件」的持久通知（协议见 [`NotifySlot_`]）。
    notify_: NotifySlot_,
}

impl LsnRec_ {
    /// 空载荷（注册 listener 身份时建立）。
    pub(crate) fn new_() -> Self {
        LsnRec_ {
            notify_: NotifySlot_::new_(),
        }
    }

    /// 提示「本 dock 上可能有入向事件」；幂等、不阻塞、零分配。
    pub(crate) fn notify_(&self) {
        self.notify_.notify_();
    }

    /// 等一次入向通知：消费已到达的提示，或登记 waker 后返回 [`Poll::Pending`]。
    pub(crate) fn poll_wait_(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.notify_.poll_wait_(cx)
    }
}

/// **类型化**的身份句柄：`T` 是它的身份节点里那份载荷的类型。
///
/// # 生命周期
///
/// 节点的建立与销毁都跟着**注册表的身份记录**：记录在 `reserve_channel_` /
/// `reserve_inbound_` / `reserve_listener_` / `reserve_telegraph_` 时创建它，在身份
/// 释放（转宽限态 / 撤销）时丢掉自己那一份。应用侧对象与两个循环的本地表各持一份克隆，
/// 因此「身份记录已被改写」不会让正在收尾的一方失去状态——但也**不会**让状态永久泄漏：
/// 最后一份句柄消失即回收。
///
/// # 变体在构造时校验一次，取用不再检查
///
/// 字段一律私有，只能经 [`DockBinding_`] 的三个 `*_` 取值函数 + [`handle_from_node_`]
/// 建出：构造时校验一次变体，此后 [`Deref`](core::ops::Deref) 直接交出那份载荷引用。
/// 热路径（每次 `try_write` / 每帧流控记账）因此**零分支、零 panic**。
pub(crate) struct DockHandle_<T, A>
where
    T: ?Sized + 'static,
    A: AllocatorClone,
{
    /// 保活句柄：本句柄存活期间节点不会被释放，也**不会移动**（`Shared` 只交出 `&T`，
    /// 拿不到 `&mut`，`try_into_inner` 在强引用多于一个时不会成功）。
    node_: Shared<DockBinding_, A>,

    /// 指向 `node_` 内对应变体载荷的引用（构造时已校验）。
    ///
    /// 生命周期被延长到 `'static` 是**本类型的私有实现细节**：真正的约束是
    /// 「`node_` 活着」，而它与本字段同生共死；[`Deref`](core::ops::Deref) 只在
    /// `&self` 的生命周期内把引用交出去，`'static` 不会泄漏到外部。
    payload_: &'static T,
}

/// 一条 **channel** 身份的句柄。
pub(crate) type ChannelOwner_<A> = DockHandle_<ChannelState_, A>;

/// 一个 **telegraph 端点**身份的句柄。
pub(crate) type TgOwner_<A> = DockHandle_<TgRec_, A>;

/// 一个 **listener** 身份的句柄。
pub(crate) type LsnOwner_<A> = DockHandle_<LsnRec_, A>;

impl<T, A> Clone for DockHandle_<T, A>
where
    T: ?Sized + 'static,
    A: AllocatorClone,
{
    fn clone(&self) -> Self {
        DockHandle_ {
            node_: self.node_.clone(),
            payload_: self.payload_,
        }
    }
}

impl<T, A> core::ops::Deref for DockHandle_<T, A>
where
    T: ?Sized + 'static,
    A: AllocatorClone,
{
    type Target = T;

    fn deref(&self) -> &T {
        self.payload_
    }
}

impl<T, A> core::fmt::Debug for DockHandle_<T, A>
where
    T: ?Sized + 'static,
    A: AllocatorClone,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DockHandle_").finish_non_exhaustive()
    }
}

/// 由一条身份节点与「取哪个变体」的判据建出**类型化句柄**（构造期校验一次）。
///
/// 三个 `new_*_owner_` 都走这里，因此 `unsafe` 在整棵身份体系里只有**这一处**。
///
/// # Panics
///
/// 节点的变体不是 `pick` 认得的那一个时 panic：这是**构造路径**的编程错误，只有注册表
/// 建身份时才会走到，因此冷路径上一次 `expect` 是可接受的代价（换来取用路径零分支）。
fn handle_from_node_<T, A>(
    node: Shared<DockBinding_, A>,
    pick: impl FnOnce(&DockBinding_) -> Option<&T>,
) -> DockHandle_<T, A>
where
    T: ?Sized + 'static,
    A: AllocatorClone,
{
    let checked = pick(&node).expect("身份句柄只能由对应变体的身份节点构造");
    let ptr = checked as *const T;
    // SAFETY: 节点由 `Shared` 独占拥有、且只交出 `&T`（不会移动、不会给出 `&mut`），
    // 因此 `ptr` 在句柄存活期间一直有效；句柄持有 `node_` 的强引用，保证节点活到句柄
    // 之后才可能被释放。把引用的生命周期延长到 `'static` 只是为了让结构体自持，
    // `'static` 不会经 `Deref` 泄漏（返回的生命周期绑定 `&self`）。
    let payload: &'static T = unsafe { &*ptr };
    DockHandle_ {
        node_: node,
        payload_: payload,
    }
}

/// 由一条**已经是 `Channel` 变体**的身份节点建出 channel 句柄。
fn new_channel_owner_<A>(node: Shared<DockBinding_, A>) -> ChannelOwner_<A>
where
    A: AllocatorClone,
{
    handle_from_node_(node, DockBinding_::channel_)
}

/// 建立一条 channel 身份的共享节点（登记身份时调用）。
pub(crate) fn new_owner_<A>(alloc: A) -> ChannelOwner_<A>
where
    A: AllocatorClone,
{
    new_channel_owner_(Shared::new(
        DockBinding_::Channel(ChannelState_::new_empty_()),
        alloc,
    ))
}

/// 建立一条 listener 身份的共享节点（登记身份时调用）。
pub(crate) fn new_listener_owner_<A>(alloc: A) -> LsnOwner_<A>
where
    A: AllocatorClone,
{
    handle_from_node_(
        Shared::new(DockBinding_::Listener(LsnRec_::new_()), alloc),
        DockBinding_::listener_,
    )
}

/// 建立一条 telegraph 端点身份的共享节点（登记身份时调用）。
pub(crate) fn new_telegraph_owner_<A>(alloc: A) -> TgOwner_<A>
where
    A: AllocatorClone,
{
    handle_from_node_(
        Shared::new(DockBinding_::Telegraph(TgRec_), alloc),
        DockBinding_::telegraph_,
    )
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
        // 0. 建流阶段超时：计时循环已经认领（`claim_abort_`）并把原因记在共享状态上，
        //    等待方据此**立刻**退出，而不是无限等到对端回帧。
        if owner.is_aborted_() {
            return Result::Err(MuxError::IdleTimeout);
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

    /// 测试两个时钟各自独立地记账。
    /// - 手段：新节点上读两个时钟（都应为 0）→ `touch_(1234)` → 再读；随后
    ///   `mark_data_(4321)` → 再读；最后 `set_data_millis_(99)` 模拟「刚发过 PULSE」。
    /// - 判断：`touch_` 只推进存活时钟、保活职责时钟不动；`mark_data_` 两个都推进；
    ///   `set_data_millis_` 只动保活职责时钟（发 PULSE 不给自己续命）。
    #[test]
    fn touch_and_mark_data_stamp_independent_clocks() {
        let owner = make_owner_();
        assert_eq!(owner.active_millis_(), 0u64);
        assert_eq!(owner.data_millis_(), 0u64);

        owner.touch_(1234u64);
        assert_eq!(owner.active_millis_(), 1234u64, "存活时钟应当被推进");
        assert_eq!(owner.data_millis_(), 0u64, "保活职责时钟不该被 touch_ 推进");

        owner.mark_data_(4321u64);
        assert_eq!(owner.active_millis_(), 4321u64);
        assert_eq!(owner.data_millis_(), 4321u64, "mark_data_ 两个时钟都推进");

        owner.set_data_millis_(99u64);
        assert_eq!(owner.active_millis_(), 4321u64, "发 PULSE 不该刷新存活时钟");
        assert_eq!(owner.data_millis_(), 99u64);
    }



    /// 测试中止认领只成功一次，且原因可回传给应用侧。
    /// - 手段：未中止时读一次 `abort_reason_`；`claim_abort_(IdleTimeout)` 两次。
    /// - 判断：未中止时为 `None`；第一次认领为真、第二次为假；`is_aborted_` 为真且
    ///   `abort_reason_` 投影为 `MuxError::IdleTimeout`。
    #[test]
    fn abort_is_claimed_once_and_reports_idle_timeout() {
        let owner = make_owner_();
        assert_eq!(owner.abort_reason_(), Option::None);
        assert!(!owner.is_aborted_(), "新身份不应处于中止态");

        assert!(owner.claim_abort_(AbortCode_::IdleTimeout), "首次认领应当成功");
        assert!(!owner.claim_abort_(AbortCode_::IdleTimeout), "只允许认领一次");
        assert!(owner.is_aborted_());
        assert_eq!(owner.abort_reason_(), Option::Some(MuxError::IdleTimeout));
    }

    /// 测试连接级失败牵连的通知**压过**此前记下的子流级原因，且反向永远不成立。
    ///
    /// - 背景：连接正式判死之前往往已经发生过大面积子流级错误（空闲超时、建流超时），
    ///   应用需要知道的结论是「连接没了」；反过来，后到的子流级原因不能把已经写下的
    ///   连接级结论改回去——否则应用读到的原因取决于「谁最后写」，不可推理。
    /// - 手段：在同一个 owner 上按两种顺序各发布一次（`IdleTimeout` 与
    ///   `ConnFailed(Transport)`），每一步读一次 `notice_()` 与 `abort_reason_()`。
    /// - 判断：先 `IdleTimeout` 后 `ConnFailed` ⇒ 槽位变成 `ConnFailed`；先
    ///   `ConnFailed` 后 `IdleTimeout` ⇒ 槽位**不变**。
    #[test]
    fn conn_failed_notice_overrides_channel_level_reason() {
        // 顺序一：子流级 → 连接级（升级）。
        let owner = make_owner_();
        assert!(owner.publish_notice_(ChannelNotice::IdleTimeout));
        assert_eq!(owner.notice_(), Option::Some(ChannelNotice::IdleTimeout));
        assert!(
            owner.publish_notice_(ChannelNotice::ConnFailed(ConnCloseReason::Transport)),
            "连接级失败必须压过已经记下的子流级原因"
        );
        assert_eq!(
            owner.notice_(),
            Option::Some(ChannelNotice::ConnFailed(ConnCloseReason::Transport))
        );
        assert_eq!(
            owner.abort_reason_(),
            Option::Some(MuxError::ConnFailed(ConnCloseReason::Transport))
        );

        // 顺序二：连接级 → 子流级（不得降级，也不得改写载荷）。
        let owner = make_owner_();
        assert!(owner.publish_notice_(ChannelNotice::ConnFailed(ConnCloseReason::ProtocolError)));
        assert!(
            !owner.publish_notice_(ChannelNotice::IdleTimeout),
            "子流级原因不得覆盖连接级结论"
        );
        assert_eq!(
            owner.notice_(),
            Option::Some(ChannelNotice::ConnFailed(ConnCloseReason::ProtocolError))
        );

        // 同优先级：首个生效（类别不同也不改写）。
        let owner = make_owner_();
        assert!(owner.publish_notice_(ChannelNotice::ConnFailed(ConnCloseReason::Transport)));
        assert!(!owner.publish_notice_(ChannelNotice::ConnFailed(ConnCloseReason::PeerClosed)));
        assert_eq!(
            owner.notice_(),
            Option::Some(ChannelNotice::ConnFailed(ConnCloseReason::Transport))
        );
    }

    /// 测试「中止动作认领」与「应用读到的通知」是两件事：认领会发布通知，但连接级
    /// 通知**不需要**先认领中止动作（连接已死，逐条回 `CLOSE` 没有意义）。
    ///
    /// - 手段：只发布 `ConnFailed`（不认领），检查 `is_aborted_` 与 `abort_reason_`；
    ///   再认领 `IdleTimeout`，检查认领位与槽位的关系。
    /// - 判断：只发布连接级通知时 `is_aborted_` 仍为假、但 `abort_reason_` 已经给出
    ///   连接级失败；认领空闲超时不会把连接级结论改回去（上一个用例已覆盖，这里只钉
    ///   「认领会顺带发布通知」这一条）。
    #[test]
    fn notice_and_abort_claim_are_independent() {
        let owner = make_owner_();
        assert!(owner.publish_notice_(ChannelNotice::ConnFailed(ConnCloseReason::PeerClosed)));
        assert!(!owner.is_aborted_(), "连接级牵连不认领中止动作");
        assert_eq!(
            owner.abort_reason_(),
            Option::Some(MuxError::ConnFailed(ConnCloseReason::PeerClosed))
        );

        assert!(owner.claim_abort_(AbortCode_::IdleTimeout));
        assert!(owner.is_aborted_());
        assert_eq!(
            owner.abort_reason_(),
            Option::Some(MuxError::ConnFailed(ConnCloseReason::PeerClosed)),
            "认领空闲超时不得覆盖已经写下的连接级结论"
        );
    }

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
