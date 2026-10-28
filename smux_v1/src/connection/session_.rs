//! 连接内部的**两个搬运循环**：解复用循环与复用循环。
//!
//! 本模块是经 `abs_art` 的本地作用域 spawn 出来的两个 `'static` 任务，**直接驱动传输
//! 的两条半边**（`C::ConnRx` / `C::ConnTx`）。结构见 [`crate::connection`] 模块文档
//! §2、§5。
//!
//! # 数据流
//!
//! ```text
//! 应用 --写--> 发送环 --读端(移交)--> 复用循环 --成帧--> 传输 Tx（自带缓冲）--> 对端
//!
//! 对端 --> 传输 Rx（自带缓冲）--> 解复用循环 --写端(移交)--> 接收环 --读--> 应用
//! ```
//!
//! 解复用循环读到的字节**不再**先落进连接自己的暂存环：传输半边本身就是那块缓冲
//! （`buffex::ring` 的环半部，或 `buffex_*_adapt` 的「设备 + 环」适配器），因此解析侧
//! 看到的是一个持续供给的字节流，且没有任何一次多余的搬运。
//!
//! # 为什么可以没有「连接级环」
//!
//! 旧版在传输与这两个循环之间还垫了**两条连接级帧暂存环**与**两个贴传输的泵循环**
//! （`session_pump_`），理由是「直接把传输端交给这两个循环，解析与写入就会 park 在
//! 设备上」：
//!
//! - **帧解析需要的字节数无法用一次 await 表达**：帧头是自描述字段序列，载荷长度到
//!   解析完才揭晓；一旦传输端长时间没有足够字节，循环就只能挂在传输上，事件通道里的
//!   `Attach` 等一次性通知全部滞留在队列里；
//! - **写路线上的「写到一半」状态无法与合作式取消共存**：成帧写进环是可重试的短操作，
//!   而写进**设备**是可能长期 park 的操作（对端不读时）；
//! - **批量化**：一次网络读装载整段、一次写交出连续段，把系统调用次数与帧数解耦。
//!
//! 本版把「传输必须是**自带缓冲**的」写进了配置的关联类型约束
//! （[`TrConnCfg::ConnTx`] / [`TrConnCfg::ConnRx`] 要求 `TrProducerState` /
//! `TrConsumerState`）：只要传输自己有环，上面三条就都不成立——填充它的是**另一个
//! 任务**（调用方的入向泵 / 适配器的自驱动泵），本模块只与一条环打交道，park 在环上
//! 与 park 在设备上是两回事。于是那两条环与两个泵只剩「第二次缓冲 + 多一次搬运」，
//! 本版直接去掉。
//!
//! **这条前提是使用者的责任**：若把「裸设备」（例如 socket 的读写半边）直接当
//! `ConnTx` / `ConnRx`，两个循环就会 park 在设备上，事件队列与另一方向都会被它拖住。
//!
//! # 循环不持有连接核心
//!
//! 三个循环（本模块两个 + [`timer_`](super::timer_)）都只拿 [`MuxLoopShared_`]——
//! 注册表句柄 + 单帧上限 + 时钟 + 指标 sink——**不持有**
//! [`MuxCore`](super::mux_connection::core_) 的强引用（因此也不持有
//! [`MuxConnection`](super::MuxConnection)）。
//!
//! 这不是随手的选择：若循环持有核心，核心将永远无法析构，其 `Drop` 里的取消令牌
//! 永远不会触发，任务与整个连接状态会永久泄漏。现在的形状是——最后一个
//! **应用面**对象（连接、四个句柄、两个半部之一）被丢弃 ⇒ 核心析构 ⇒ 取消三个
//! 令牌 ⇒ 循环在下一个 await 点退出。因此三个循环的 **park 点必须都能被取消令牌
//! 唤醒**：解复用循环经 `may_cancel_with`，复用循环的每处 park 都与
//! `cancellation()` 竞争（见下）。
//!
//! # 半部为什么是「移交」而不是共享
//!
//! `buffex` 的段借用绑定在环半部上，半部若藏在共享单元的锁后面，段就不能跨
//! `await` 使用（锁内的闭包不允许 `await`）。因此建流方在创建环之后，把**会话侧**
//! 的两个半部交给对应循环，循环把它们放进**本地表**（`BTreeMap`，节点用调用方
//! 注入的分配器分配），此后可以自由在这些半部上 park / await。
//!
//! # 传输环的容量
//!
//! 传输环的容量由**调用方**决定（它就是握手交付物里那两条半边背后的环）。协议上
//! 没有任何一步要求缓冲里先装下整帧——帧头逐字节解析、载荷按底层给出的段长分次搬入、
//! 出向按空闲空间分块推进（见 [`enqueue_frame_`]），因此下限由**环原语**决定
//! （`buffex::ring` 允许到 1 字节，`tests/inmem_mux.rs` 的最小容量用例钉着这一点）。
//!
//! 唯一需要注意整帧的是**数据报**（telegraph）：`write_frame_to_stage_` 要求缓冲
//! 当前放得下整帧才动手（宁可推迟一条，也不在缓冲里留残帧）。因此传输环容量应当
//! 不小于协商的 `max_packet_size`，参考值见
//! [`K_TRANSPORT_RING_CAPACITY`](crate::connection::K_TRANSPORT_RING_CAPACITY)。
//!
//! # 两个循环各自的 park 点
//!
//! - 解复用循环：`read_header_async_`（传输 Rx）与「接收环满时的 `write_async`」；
//!   两者都经 `may_cancel_with` 挂在取消令牌上，因此连接被丢弃时能立刻退出。
//!   事件队列只在其醒来后**非阻塞排空**——`Attach` 必然先于对端的数据帧到达
//!   （建流方先发 `Attach` 事件、后发 `OPEN`），所以「先排空事件、再派发帧」即可
//!   保证不丢；
//! - 复用循环：无数据可发时优先 park 在**最近一次收到 `TxReady` 的那条发送环**上
//!   （Q4 裁决的兜底，覆盖「应用还没提交就发了事件」的竞态），否则 park 在事件
//!   通道上。**这些 park 都必须同时与事件通道 / 取消令牌竞争**：只 park 在环上
//!   会让别的子流刚入队的事件（例如某条子流的 `Attach`）一直排在后面，多子流下
//!   就是死锁；不与取消令牌竞争则连接被丢弃后循环无法退出。
//!
//! 另有一条容易踩的约束：循环里**借出环数据一律用非阻塞的 `try_read`**
//! （`drain_one_`）。`read_async` 在空环上会 park，一旦在「排空数据」这一步 park，
//! 事件就再也送不进来了。
//!
//! 写侧还有一条同样来自踩坑的约束：**不许要求「整帧连续空间」**。传输环容量与调用方
//! 泵的节奏互相钳制时，「等环形装得下整帧」可能永远不成立（实测：环里剩 1987 字节、
//! 下一帧要 2010 字节，而对侧此刻正等本端腾空间）。因此 `enqueue_frame_` 按环当前能
//! 给的段**分块**推进，并且只在环满时才 park；**绝不能**先把子流发送环里的字节搬出来
//! 再判空间——段的 drop 提交消费，那批字节会永久丢失（这是本轮实际踩到的第二个坑）。
//!
//! # 已知限制（Q4）
//!
//! 「通知在进入写路径时发出、真正提交在段 drop 时」这个时间差由上面的兜底覆盖：
//! 复用循环 park 到刚通知的那条环，用环自身的提交唤醒补上。残留漏洞（两条环都通知
//! 且都不提交时，先通知的那条可能永远不被轮询）见 dev-notes §5.1，属**已知限制**。
//!
//! # 写半边的收尾（已由 `TrBuffTryFlush` 补上）
//!
//! 复用循环退出时经 [`TxSealGuard_`] 对传输写半边**封口**：
//! [`TrBuffTryFlush::try_flush`] 尽量推一轮、[`TrBuffTryFlush::close`] 置生产端关闭
//! 并唤醒 park 中的排空方。对「自驱动」的适配器（`BuffWrite`）这两步是幂等的；对
//! 「调用方驱动的全被动环」`close` 才是唤醒排空方的那一步。旧版在 `TrBuffWrite` 里
//! **没有**对应方法，只能靠具体类型上的固有方法，因此循环退出时只是 drop——唤醒就此
//! 丢失。上游补上收尾契约之后，这件事在泛型层面成立，循环不必知道传输的具体形态。
//! 语义与取舍见 [`TxSealGuard_`] 以及 `dev-notes/flush-contract-20261015-2340.md`。
//!
//! # 拆流
//!
//! 每个方向独立收尾，两个方向都收尾后释放注册表条目（`ChannelState_::is_done_`）。

use core::{
    future::poll_fn,
    mem::MaybeUninit,
    ops::Bound,
    ptr::NonNull,
    task::{Context, Poll},
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryFlush, TrBuffTryWrite, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrConsumerState, TrProducerState},
    error::{ReadErrTag, TrTaggedError, WriteErrTag},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::SomeOf;
use buffex::ring::ConsumerError;

use crate::{
    connection::{
        Dock, FrameHeader, FrameKind, MuxError, TrConnAllocCfg, TrConnCfg, flags,
        control_state_::ControlState_,
        frame_::{K_MAX_FRAME_HEADER, encode_header_},
        frame_parser_,
        mux_connection::ChannelRegistry_,
        owner_::{ChannelOwner_, TgOwner_},
        public_channel_::{ChunkReadState_, ChunkWriteErr_, PublicChannel_, PublicChunkIo_},
        ring_::{BufferedRx, BufferedTx},
        signal_::{
            ControlFrame_, EventReceiver_, EventSender_, EventTake_,
            HINT_R_RELEASE, HINT_W_FIN_ENQUEUED, HINT_W_PEER_CLOSED, HINT_W_RELEASED,
            HINT_W_RX_CLOSED, ReadAttach_, ReadEvent_, TrEventReceiver_,
            TrEventSender_, WriteAttach_, WriteEvent_,
        },
        telegraph::TgRecvCtx_,
    },
    flow_ctrl::{Credit, WindowReport},
    metrics::{ChannelCloseReason, FrameDir, TrMetricsSink},
    time::{ConnClock_, TrTime},
    wire_io_::{CursorError, ReadCursor},
    x_deps::{abs_buff, abs_cancel, anylr},
};

/// 单条子流数据帧的载荷上限（一次成帧最多携带多少字节）。
///
/// 与 `max_packet_size` 无关：后者是**帧总长**上限，这里只是想避免一次借出过大的
/// 段而让其他子流等太久（公平性，见 `connection-20260919-1631.md` §5.2）。
const K_MAX_DATA_CHUNK: usize = 16usize * 1024usize;

// 帧暂存环容量与**帧头上界**无关：帧头逐字节解析 / 编码（`frame_::encode_header_`
// 用栈上定长缓冲，长度上界 [`K_MAX_FRAME_HEADER`]），载荷按底层段长分块搬入 / 写出，
// 因此帧暂存容量取到环原语的下限也能跑通（见模块文档「连接级环的硬约束」）。

/// 两个**内侧**循环共享的、与调用方配置无关的量。
///
/// 它由建连路径从核心展开而来：注册表句柄 + 单帧上限 + **连接级时钟** + 空闲超时。
/// 注意它**不含核心引用**（模块文档「循环不持有连接核心」），因此核心可以在最后一个
/// 应用面对象被丢弃时正常析构并触发收尾。
///
/// 「初始接收窗口」与「通告阈值」**不在这里**：`abs_smux` 更新后，子流缓冲由调用方
/// 在最终裁决（`accept_async`）时给出，容量逐条子流不同，因此这两个量在**建流时**
/// 按该子流的接收缓冲容量算出，存进该子流的 [`ChannelState_`]（见
/// [`crate::connection::owner_`]）。循环侧不再需要任何连接级窗口快照。
///
/// # 时钟与运行时值
///
/// 注册表里与时间有关的操作（拆流宽限期回收、进入宽限态、判宽限态）都要「现在」，
/// 而两个内侧循环恰好都要调它们（解复用循环判宽限态、复用循环进宽限态）。把
/// [`ConnClock_`] 放在共享量里，三个循环因此都拿得到**同一个 epoch** 下的毫秒。
///
/// 运行时值在这里是「配置给的」（[`TrConnCfg::Rt`]），**不是**共享量自己的类型参数：
/// 核心**不直接**持有运行时值（compio 的运行时值是 `!Send + !Sync`），共享量只在
/// **本地**循环里活着，因此可以自由持有它。
pub(crate) struct MuxLoopShared_<C, R, M>
where
    C: TrConnAllocCfg + 'static,
    R: TrTime,
    M: TrMetricsSink,
{
    /// 注册表（dock / 子流索引与配额、失败标志、取消令牌、计时唤醒槽）。
    reg_: ChannelRegistry_<C>,

    /// 控制面共享句柄：写循环据此 raise `HINT_MAIN_*`（例如发送环已排空）。
    control_: ControlState_<C>,

    /// 协商出的单帧总长上限。
    max_packet_size_: usize,

    /// 建流期 message 的每方向载荷上限（建连时从配置取一次的快照）。
    pending_msg_cap_: usize,

    /// 连接级时钟（运行时值 + 建连 epoch）。
    conn_clock_: ConnClock_<R>,

    /// 协商出的**活跃子流空闲超时**（毫秒）；计时循环的判据。
    channel_timeout_millis_: u64,

    /// 公共报文子流的四路唤醒槽与关闭位（ring 半部按方向由各循环独占）。
    ///
    /// P2.1 先接线，搬运逻辑在 P2.2 接入；此前的短暂未读由 `allow` 标出。
    #[allow(dead_code)]
    public_: PublicChannel_<C>,

    /// 本次连接的**指标接收方**（建连时从配置克隆的一份）。
    ///
    /// 三个循环不持有核心（见模块文档 §2.2），拿不到 `MuxCore`，只能像运行时值那样
    /// 自持一份。克隆的代价由需求方选的 sink 类型决定（见 `crate::metrics` 模块文档 §3）。
    ///
    /// **它不是 `Option`**：「有没有 sink」是编译期由 `C::Metrics` 决定的**类型事实**，
    /// 做成运行期状态只会让每个上报点多一次判空分支。不上报的装配里它是零大小的
    /// `NoMetrics`，上报调用点随即被单态化消除。
    metrics_: M,
}

/// [`MuxLoopShared_`] 在具体连接策略上的简写：分配器配置与运行时值都取自 `C`。
///
/// 循环与事件处理的签名里满是这个类型，用别名把关联类型摊平；而只依赖
/// 「注册表 + 运行时值 + 指标」的辅助函数（例如 [`fail_mux_loop_`]）仍写成
/// 泛型于 `C` 的形状，不必被绑到某个连接配置上。
pub(crate) type MuxShared_<C> = MuxLoopShared_<
    crate::connection::ConnAllocCfg_<C>,
    <C as TrConnCfg>::Rt,
    <C as TrConnCfg>::Metrics,
>;

/// 手写 `Clone`：自动派生会给分配器配置参数 `C` 也加上 `Clone` 约束，而配置本身
/// 只需要在各字段里以**已取出的分配器实例**出现。
impl<C, R, M> Clone for MuxLoopShared_<C, R, M>
where
    C: TrConnAllocCfg + 'static,
    R: TrTime + Clone,
    M: TrMetricsSink + Clone,
{
    fn clone(&self) -> Self {
        MuxLoopShared_ {
            reg_: self.reg_.clone(),
            control_: self.control_.clone(),
            max_packet_size_: self.max_packet_size_,
            pending_msg_cap_: self.pending_msg_cap_,
            conn_clock_: self.conn_clock_.clone(),
            channel_timeout_millis_: self.channel_timeout_millis_,
            public_: self.public_.clone(),
            metrics_: self.metrics_.clone(),
        }
    }
}

impl<C, R, M> MuxLoopShared_<C, R, M>
where
    C: TrConnAllocCfg + 'static,
    R: TrTime,
    M: TrMetricsSink,
{
    /// 由建连路径展开后的量构造（成员私有，构造只能走这里）。
    #[allow(
        clippy::too_many_arguments,
        reason = "构造器一次接收建连时已展开的共享量"
    )]
    pub(crate) fn new_(
        reg: ChannelRegistry_<C>,
        control: ControlState_<C>,
        max_packet_size: usize,
        pending_msg_cap: usize,
        conn_clock: ConnClock_<R>,
        channel_timeout_millis: u64,
        public: PublicChannel_<C>,
        metrics: M,
    ) -> Self {
        MuxLoopShared_ {
            reg_: reg,
            control_: control,
            max_packet_size_: max_packet_size,
            pending_msg_cap_: pending_msg_cap,
            conn_clock_: conn_clock,
            channel_timeout_millis_: channel_timeout_millis,
            public_: public,
            metrics_: metrics,
        }
    }
}

impl<C, R, M> MuxLoopShared_<C, R, M>
where
    C: TrConnAllocCfg + 'static,
    R: TrTime,
    M: TrMetricsSink,
{
    /// 注册表句柄。
    pub(crate) fn reg_(&self) -> &ChannelRegistry_<C> {
        &self.reg_
    }

    /// 控制面共享句柄（写循环据此 raise `HINT_MAIN_*`）。
    pub(crate) fn control_(&self) -> &ControlState_<C> {
        &self.control_
    }

    /// 连接级时钟。
    pub(crate) fn conn_clock_(&self) -> &ConnClock_<R> {
        &self.conn_clock_
    }

    /// 协商出的活跃子流空闲超时（毫秒）。
    pub(crate) fn channel_timeout_millis_(&self) -> u64 {
        self.channel_timeout_millis_
    }

    /// 协商出的单帧总长上限。
    pub(crate) fn max_packet_size_(&self) -> usize {
        self.max_packet_size_
    }

    /// 建流期 message 的每方向载荷上限。
    pub(crate) fn pending_msg_cap_(&self) -> usize {
        self.pending_msg_cap_
    }

    /// 公共报文子流的唤醒槽。
    #[allow(dead_code, reason = "P2.2 起由 mux / main 的搬运逻辑读取")]
    pub(crate) fn public_(&self) -> &PublicChannel_<C> {
        &self.public_
    }

    /// 本次连接携带的**指标接收方**（总有值，见字段文档）。
    ///
    /// 循环侧的上报直接写成 `shared.metrics_().on_frame(..)`：**没有判空分支**；
    /// 缺省 sink（`NoMetrics`）下整句在单态化后消失（见 `crate::metrics` 模块文档 §4）。
    #[inline]
    pub(crate) fn metrics_(&self) -> &M {
        &self.metrics_
    }
}

/// 循环里等一把共享锁：**被取消就退出本循环**（连接正在收尾，`()` 返回值的循环用）。
macro_rules! lock_or_exit_ {
    ($e:expr) => {
        match $e.await {
            Result::Ok(value) => value,
            Result::Err(_) => return,
        }
    };
}

/// 同上，但用于 `Result<(), MuxError>` 返回值的循环内辅助函数。
macro_rules! lock_or_fail_ {
    ($e:expr) => {
        match $e.await {
            Result::Ok(value) => value,
            Result::Err(_) => return Result::Err(MuxError::Cancelled),
        }
    };
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 循环的本地表
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 解复用循环本地持有的一条子流：共享状态 + 会话侧**接收环写端**。
///
/// `local_dock` / `remote_dock` 不再是字段：dock 对已经是所在表的键。
struct ReadEntry_<C>
where
    C: TrConnAllocCfg + 'static,
{
    owner_: ChannelOwner_<C::ChanOwnerAlloc>,
    writer_: BufferedTx,
}

/// 复用循环本地持有的一条子流：共享状态 + 会话侧**发送环读端**。
struct WriteEntry_<C>
where
    C: TrConnAllocCfg + 'static,
{
    owner_: ChannelOwner_<C::ChanOwnerAlloc>,
    reader_: BufferedRx,

    /// 「对端 `FIN` 已经推进过拆流记账」：状态位是持久的，没有这一格，写循环每轮
    /// 扫描都会对同一条子流重复调一次 `maybe_release_`（并取注册表锁）。
    peer_fin_handled_: bool,

    /// 本端 `CLOSE(FIN)` 已经发出：此后这条子流不再发送数据，但表项要**留到身份
    /// 释放**——对端随后的 `FIN` / `RESET` 仍需要经本表扫描推进释放。
    fin_sent_: bool,
}

/// 解复用循环的本地表：dock 对 → 该子流的接收环写端与共享状态。
///
/// 用 `BTreeMap` 而不是手写单链表：链表的 `find` / `remove` 是 O(n)，且每次
/// 增删都要自己用分配器构造 / 释放节点；`BTreeMap` 直接以调用方注入的分配器
/// （`allocator_api` 的 `new_in`）承担这些分配，查找降到 O(log n)。
type ReadTable_<C> = crate::connection::dock_index_::DockBPlusTree_<
    <C as TrConnAllocCfg>::LocalTableAlloc,
    (Dock, Dock),
    ReadEntry_<C>,
>;

/// 复用循环的本地表：dock 对 → 该子流的发送环读端与共享状态。
type WriteTable_<C> = crate::connection::dock_index_::DockBPlusTree_<
    <C as TrConnAllocCfg>::LocalTableAlloc,
    (Dock, Dock),
    WriteEntry_<C>,
>;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// telegraph 的两个本地表
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// telegraph 的读表项：解复用循环持有的**接收上下文**（接收环写端 + 身份句柄）。
///
/// 它把「帧头 + 载荷」整帧投进接收环，并由**接收侧**自行拆出远端地址与载荷——中心
/// 循环因此完全不理解 `DATAGRAM` 的字段语义（见 [`TgRecvCtx_`] 的模块文档）。
///
/// 复用循环本地持有的一条 telegraph：发送环的**读端** + 身份句柄。
///
/// 身份句柄是必需的：**已提交报文长度**记在身份节点内联的队列里（`TgLenQueue_`），
/// 循环按 FIFO 取长度才知道环里哪一段是一条报文（环本身只是字节流）。
struct TgWriteEntry_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 该端点的身份节点句柄（含发送长度队列）。
    owner_: TgOwner_<C::ChanOwnerAlloc>,

    /// 会话侧发送环读端（应用写、本循环读）。
    reader_: BufferedRx,
}

/// 解复用循环的 telegraph 表：`local_dock` → 接收上下文。
///
/// 键只有 `local_dock`：telegraph **独占**该 dock（协议不允许它与 channel / listener
/// 共用），对端地址在数据报里是**地址**而不是**身份**，因此没有第二个键维度。
///
/// 表里**有**条目就是「有接收方」：没有条目时到达的 `DATAGRAM` 整帧丢弃，连丢弃计数
/// 都不记（无人可归因）。
type TgReadTable_<C> = crate::connection::dock_index_::DockBPlusTree_<
    <C as TrConnAllocCfg>::LocalTableAlloc,
    Dock,
    TgRecvCtx_<C>,
>;

/// 复用循环的 telegraph 表：`local_dock` → 发送环读端与身份句柄。
type TgWriteTable_<C> = crate::connection::dock_index_::DockBPlusTree_<
    <C as TrConnAllocCfg>::LocalTableAlloc,
    Dock,
    TgWriteEntry_<C>,
>;


// 半关闭的 `FIN` 不再维护独立的 `PendingFin_` 集合：
// `drain_once_` 每轮扫描本地写表时，对 `APP_TX_CLOSED && !TX_DRAINED` 的条目在
// 发送环排空后置 `TX_DRAINED` 并叫醒 main；main 把 `CLOSE(FIN)` 写进公共 TX ring，
// 由 `scan_fin_enqueued_` 在刷空后确认上线。既不会饿死后面的条目，也省掉一整棵
// 本地索引树。

/// 处理写循环收到的一条 `Attach`：插入本地写表，并补查那些可能早于 `Attach` 到达的
/// 一次性事实（应用封口 / 关接收、对端 `CLOSE`）。
fn handle_write_attach_<C>(
    attach: WriteAttach_<crate::connection::ConnAllocCfg_<C>>,
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
    shared: &MuxShared_<C>,
) where
    C: TrConnCfg,
{
    let (pair, owner, reader_) = attach.into_parts_();
    let rx_closed = owner.is_app_rx_closed_();
    let peer_closed = owner.is_peer_fin_() || owner.is_peer_reset_();
    table.insert(
        pair,
        WriteEntry_ {
            owner_: owner,
            reader_,
            peer_fin_handled_: false,
            fin_sent_: false,
        },
    );
    if rx_closed {
        shared.reg_().raise_write_hint_(HINT_W_RX_CLOSED);
    }
    if peer_closed {
        shared.reg_().raise_write_hint_(HINT_W_PEER_CLOSED);
    }
}

/// 处理读循环收到的一条 `Attach`：插入本地读表；身份已经释放时直接关掉写端。
fn handle_read_attach_<C>(attach: ReadAttach_<crate::connection::ConnAllocCfg_<C>>, table: &mut ReadTable_<crate::connection::ConnAllocCfg_<C>>)
where
    C: TrConnCfg,
{
    let (pair, owner, writer_) = attach.into_parts_();
    if owner.is_released_() {
        let mut writer_ = writer_;
        writer_.close();
        return;
    }
    table.insert(
        pair,
        ReadEntry_ {
            owner_: owner,
            writer_,
        },
    );
}

/// 对每条「应用已丢弃接收半边」的子流补一次 `CLOSE(RESET)`。
///
/// 与原 `RxClosed` 事件分支同序：认领发送权 → 整帧写出 → 置「已上线」位 → 唤醒应用侧
/// 接收方向 → 上报 → 尝试释放身份。认领（`claim_local_reset_`）保证同一条至多写一次。
async fn flush_rx_closed_<C, K>(
    tx_stage: &mut C::ConnTx,
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
    read_events: &EventSender_<ReadEvent_<crate::connection::ConnAllocCfg_<C>>>,
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    // 游标推进而不是收集 `Vec`：热路径上不引入堆分配，也不跨 `await` 持有表借用。
    let mut cursor: Option<(Dock, Dock)> = Option::None;
    loop {
        let next = match cursor {
            Option::None => table.keys().next().copied(),
            Option::Some(last) => table
                .range((Bound::Excluded(last), Bound::Unbounded))
                .next()
                .map(|(key, _)| *key),
        };
        let Option::Some(pair) = next else {
            return Result::Ok(());
        };
        cursor = Option::Some(pair);
        let Some((_, entry)) = table.get(pair) else {
            continue;
        };
        let owner = entry.owner_.clone();
        if !owner.is_app_rx_closed_() || owner.is_local_reset_flushed_() {
            continue;
        }
        // FIN 已经进公共 TX ring、但还没确认上线：先等它上线，再发 RESET。
        // 否则 RESET（写循环直接写 `ConnTx`）会越过 FIN（公共 TX ring），对端先看到
        // RESET、后看到 FIN，接收方向就少了一次 `peer_fin` 的释放触发点。
        if owner.is_app_tx_closed_() && !owner.is_local_fin_sent_() {
            continue;
        }
        if !owner.claim_local_reset_() {
            continue;
        }
        control_close_via_::<C, _>(
            tx_stage,
            shared,
            shared.max_packet_size_,
            pair.0,
            pair.1,
            true,
            cancel.child_token(),
        )
        .await?;
        owner.set_local_reset_flushed_();
        owner.notify_rx_progress_();
        shared.metrics_().on_reset(pair.0, pair.1, false);
        maybe_release_::<C, _>(shared, read_events, table, pair, cancel.child_token()).await?;
    }
}

/// 推进「对端已发来 `CLOSE`」的拆流记账。
///
/// `PEER_FIN` / `PEER_RESET` 位由读循环在处理帧面时置上；这里只负责本地表动作：
///
/// - `RESET`：对端不再接收，本端发送方向作废 —— 摘写表项、放弃「欠一条 `FIN`」的
///   登记，再尝试释放身份；
/// - `FIN`：对端不再发送，发送方向仍可继续；只做一次 `maybe_release_`（由表项的
///   `peer_fin_handled_` 去重，状态位本身是持久的）。
async fn scan_peer_closed_<C, K>(
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
    read_events: &EventSender_<ReadEvent_<crate::connection::ConnAllocCfg_<C>>>,
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    let mut cursor: Option<(Dock, Dock)> = Option::None;
    loop {
        let next = match cursor {
            Option::None => table.keys().next().copied(),
            Option::Some(last) => table
                .range((Bound::Excluded(last), Bound::Unbounded))
                .next()
                .map(|(key, _)| *key),
        };
        let Option::Some(pair) = next else {
            return Result::Ok(());
        };
        cursor = Option::Some(pair);
        let Option::Some((_, entry)) = table.get(pair) else {
            continue;
        };
        let owner = entry.owner_.clone();
        if owner.is_peer_reset_() {
            table.remove(pair);
            maybe_release_::<C, _>(shared, read_events, table, pair, cancel.child_token()).await?;
            continue;
        }
        if !owner.is_peer_fin_() {
            continue;
        }
        let Option::Some((_, entry)) = table.get_mut(pair) else {
            continue;
        };
        if entry.peer_fin_handled_ {
            continue;
        }
        entry.peer_fin_handled_ = true;
        maybe_release_::<C, _>(shared, read_events, table, pair, cancel.child_token()).await?;
        if table
            .get(pair)
            .map(|(_, entry)| entry.owner_.is_released_())
            .unwrap_or(false)
        {
            table.remove(pair);
        }
    }
}

/// 收尾写表里**身份已释放 / 已中止**的条目（`HINT_W_RELEASED`）。
///
/// 替换原来的 `WriteEvent_::LocalAbort`：计时循环拆流时只置 owner 的 `RELEASED` 位
/// 与合并提示，写循环在这里按表扫描做本地动作——**显式关闭发送环消费端**（应用
/// 立刻看到发送方向已关闭）、摘写表项、并尝试释放身份。
///
/// 与对端 `RESET` 那条路径的区别（`scan_peer_closed_`）：后者**不**关消费端，因为
/// 协议只要求允许放弃已提交字节，而既有验收（`mux_recv_dropped_dual_`）钉住了
/// 「对端丢弃接收半边之前写入的字节仍然写得完」。本扫描只处理本端主动拆流。
async fn scan_write_released_<C, K>(
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
    read_events: &EventSender_<ReadEvent_<crate::connection::ConnAllocCfg_<C>>>,
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    let mut cursor: Option<(Dock, Dock)> = Option::None;
    loop {
        let next = match cursor {
            Option::None => table.keys().next().copied(),
            Option::Some(last) => table
                .range((Bound::Excluded(last), Bound::Unbounded))
                .next()
                .map(|(key, _)| *key),
        };
        let Option::Some(pair) = next else {
            return Result::Ok(());
        };
        cursor = Option::Some(pair);
        let Option::Some((_, entry)) = table.get(pair) else {
            continue;
        };
        if !entry.owner_.is_released_() {
            continue;
        }
        if let Option::Some((_, entry)) = table.get_mut(pair) {
            entry.reader_.close();
            entry.owner_.notify_both_progress_();
        }
        table.remove(pair);
        maybe_release_::<C, _>(shared, read_events, table, pair, cancel.child_token()).await?;
    }
}

/// 确认 `main_loop` 已写进公共 TX ring 的 `CLOSE(FIN)` 真正上线。
///
/// 调用点保证公共 TX ring 已经刷空；此时 `FIN_ENQUEUED` 的条目可以置
/// `LOCAL_FIN_SENT`、关发送环消费端并唤醒 `close_async` 等待者。
async fn scan_fin_enqueued_<C, K>(
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
    read_events: &EventSender_<ReadEvent_<crate::connection::ConnAllocCfg_<C>>>,
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    let mut cursor: Option<(Dock, Dock)> = Option::None;
    loop {
        let next = match cursor {
            Option::None => table.keys().next().copied(),
            Option::Some(last) => table
                .range((Bound::Excluded(last), Bound::Unbounded))
                .next()
                .map(|(key, _)| *key),
        };
        let Option::Some(pair) = next else {
            return Result::Ok(());
        };
        cursor = Option::Some(pair);
        let Option::Some((_, entry)) = table.get(pair) else {
            continue;
        };
        if !entry.owner_.is_fin_enqueued_() || entry.owner_.is_local_fin_sent_() {
            continue;
        }
        let owner = entry.owner_.clone();
        owner.set_local_fin_sent_();
        #[cfg(feature = "test-loop-probe")]
        crate::connection::loop_probe_::local_fin_sent_();
        if let Option::Some((_, entry)) = table.get_mut(pair) {
            entry.reader_.close();
            entry.fin_sent_ = true;
            entry.owner_.notify_tx_progress_();
        }
        maybe_release_::<C, _>(shared, read_events, table, pair, cancel.child_token()).await?;
        if table
            .get(pair)
            .map(|(_, entry)| entry.owner_.is_released_())
            .unwrap_or(false)
        {
            table.remove(pair);
        }
    }
}

/// 摘掉「身份已释放」的读表项并关闭接收环写端。
///
/// `buffex` 的环半部被 drop **不会**置位关闭标记：少了显式 `close()`，应用侧正 park
/// 的读永远拿不到 EOF。释放事实是 `RELEASED` 位（`claim_release_` 的一次性 CAS），
/// 提示位只负责把读循环叫醒。
fn scan_released_<C>(table: &mut ReadTable_<crate::connection::ConnAllocCfg_<C>>)
where
    C: TrConnCfg,
{
    let mut cursor: Option<(Dock, Dock)> = Option::None;
    loop {
        let next = match cursor {
            Option::None => table.keys().next().copied(),
            Option::Some(last) => table
                .range((Bound::Excluded(last), Bound::Unbounded))
                .next()
                .map(|(key, _)| *key),
        };
        let Option::Some(pair) = next else {
            return;
        };
        cursor = Option::Some(pair);
        let released = table
            .get(pair)
            .map(|(_, entry)| entry.owner_.is_released_())
            .unwrap_or(false);
        if released
            && let Option::Some(mut entry) = table.remove(pair)
        {
            entry.writer_.close();
        }
    }
}

/// 复用循环 park 的就绪判据之一：**「最近通知过的那条发送环」此刻真的有段可借吗**。
///
/// 返回 `true` 表示可以回顶部交给 `drain_once_` 正式取走。判据有两处，缺一处都会
/// 让整圈在「回到顶部 → `drain_one_` 取不到段 → 立刻又就绪」之间空转（CPU 打满、
/// 同一条本地队列上的其它任务全被饿死）。
///
/// 1. 这条子流**确实还有发送额度**：用非阻塞的 `send_available_try_`，窗口锁当场取
///    不到就按「没额度」处理（安全方向：额度真正回来时，读循环收到窗口通告会投一条
///    事件把 park 打断）；
/// 2. 环的读 future **真的借到了段**：环在**生产端已关闭且已排空**（应用 `drop(tx)`
///    之后）、**消费端已关闭**、或需求不可满足时也会**立刻完成**（返回错误），
///    而那几种完成都不意味着 `drain_one_` 有活可干。因此只认 `contains_left`。
///
/// 真机现场与因果链见
/// `smux_v1_sock_demo/dev-notes/intermittent-stall-20261007-0200.md`；判据 2 的回归
/// 用例见本文件的 `last_ready_has_segment_rejects_closed_ring_`。
fn last_ready_has_segment_<C>(
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
    last_ready: Option<(Dock, Dock)>,
    cx: &mut Context<'_>,
) -> bool
where
    C: TrConnCfg,
{
    let Option::Some(pair) = last_ready else {
        return false;
    };
    let Option::Some((_, entry)) = table.get_mut(pair) else {
        // 「最近通知」的那条环可能已经不存在（子流已收尾并摘掉）。
        return false;
    };
    if !matches!(entry.owner_.send_available_try_(), Option::Some(credit) if credit > 0u32) {
        return false;
    }
    let demand = Demand::at_least(1usize);
    let mut ring_fut = core::pin::pin!(core::future::IntoFuture::into_future(
        entry.reader_.read_async(&demand),
    ));
    matches!(
        core::future::Future::poll(ring_fut.as_mut(), cx),
        Poll::Ready(outcome) if outcome.contains_left()
    )
}

/// 非阻塞地问一句：这条子流的发送环里**还有数据没发出去**吗？
///
/// - `Some(true)`：还有（`drain_one_` 此刻取不出来，多半是额度不够或环被写者占住）；
/// - `Some(false)`：已排空，可以发 `FIN`；
/// - `None`：**不可判定**（环给不出段也给不出「空」的错误，例如写者正持有段）。
///
/// 与 [`drain_one_`] 同一条纪律：这里**只能用 `try_read`**——`read_async` 会在空环上
/// park，而一 park 就再也看不到事件通道里的事件（多子流下直接死锁）。
fn has_writable_<C>(
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
    pair: (Dock, Dock),
) -> Option<bool>
where
    C: TrConnCfg,
{
    let entry = table.get_mut(pair).map(|(_, entry)| entry)?;
    let demand = Demand::at_least(1usize);
    let mut outcome = entry.reader_.try_read(&demand);
    match outcome.as_mut().pick_left() {
        // 借到了段：里面确实还有数据。段在这里被丢弃（**不消费**）——`drain_one_`
        // 才是唯一有权搬出字节的地方。
        Option::Some(_segm) => Option::Some(true),
        Option::None => match outcome.pick_right() {
            // 环空（`Drained`）或已被写者关闭（`Closing`）：都不会再有新数据。
            Option::Some(err) => {
                if matches!(
                    err,
                    ConsumerError::Drained(_) | ConsumerError::Closing
                ) {
                    Option::Some(false)
                } else {
                    // `Stuffed` / `Unsatisfiable` / `Cancelled` 都不足以断言「已排空」。
                    Option::None
                }
            }
            Option::None => Option::None,
        },
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 小工具
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


/// 提示建流等待者（`open_channel_async`）：状态可能变了。
///
/// 建流状态标志在**原子字**里、通知通道是持久的，因此这一步是纯同步的一次 CAS +
/// 一次非阻塞投递——不需要取锁，也没有 `await`。
pub(crate) async fn fail_mux_loop_<C, R, M, K>(
    shared: &MuxLoopShared_<C, R, M>,
    cancel: &K,
    kind: Option<FrameKind>,
    err: &MuxError,
) where
    C: TrConnAllocCfg + 'static,
    R: TrTime,
    M: TrMetricsSink,
    K: TrCancellationToken,
{
    if !cancel.is_cancelled() {
        // 上报帧 / 协议错误：**只有真的失败才报**——取消导致的收尾不是帧错误（见上方
        // 文档）。`kind` 给出「已经解析出帧种类」时的种类，帧头都没解析出来时为
        // `None`（见 [`crate::metrics::TrMetricsSink::on_frame_error`]）。
        shared.metrics_().on_frame_error(kind, *err);
        let _ = shared.reg_.mark_failed_(err, cancel.child_token()).await;
    }
}

/// 在「取消令牌触发」与「给定 future 完成」之间竞争：取消先到返回 `None`。
///
/// # 为什么收 `IntoFuture` 而不是 `Future`
///
/// `abs_buff` 的 `read_async` / `write_async` 返回的是只承诺 [`TrMayCancel`] 的
/// **中间类型**（见 dev-notes §18.4 第 4 条），要经 `IntoFuture` 才成为 future。
/// 本函数直接在内部做这一步投影，因此调用方不需要在每个 park 点手写
/// `.into_future()`——「每一次可能长期 park 的 await 都要挂上取消」这条纪律
/// 就只剩一个落点。
///
/// # 为什么循环需要它
///
/// `may_cancel_with` 只是把令牌交给内层 future（内层在被 poll 时才检查
/// `is_cancelled`），**是否在取消时唤醒 park 中的 future 取决于那一层的实现**：
/// 例如 `buffex` 的环半部会把 `cancellation()` 存进自己的 park 状态、并在取消时
/// 唤醒，而一个只做转发的传输适配层不会。循环的收尾不能建立在这条隐含契约上——
/// 一旦某一层的 park 不被唤醒，循环就再也看不到取消令牌，任务与传输永久泄漏。
///
/// 因此循环把**每一次可能长期 park 的 await**（环读、环写、传输读、传输写）都
/// 放到这里，与 `cancellation()`（它保证登记 waker）竞争：「丢弃连接即关闭连接」
/// 由此与传输的实现细节无关。
pub(crate) async fn race_cancel_<F, K>(cancel: &K, fut: F) -> Option<F::Output>
where
    F: core::future::IntoFuture,
    K: TrCancellationToken,
{
    let mut fut = core::pin::pin!(fut.into_future());
    let mut cancelled = core::pin::pin!(cancel.child_token().cancellation());
    poll_fn(|cx| {
        if core::future::Future::poll(cancelled.as_mut(), cx).is_ready() {
            return Poll::Ready(Option::None);
        }
        core::future::Future::poll(fut.as_mut(), cx).map(Option::Some)
    })
    .await
}

/// 传输**读半边**（`C::ConnRx`）此刻的状态。
///
/// 旧版这里问的是「传输读半边」——那是连接自己建的帧暂存环；本版没有再建环，问的
/// 就是**传输本身**。要求传输实现 [`TrConsumerState`] 正是为了让内侧循环在
/// **不 park** 的前提下回答「有没有字节、是不是已经到头了」。
pub(crate) enum RxState_ {
    /// 还有这么多字节可读（`0` 表示此刻没有，但链路还活着）。
    Readable(usize),

    /// 生产端已封口且已排空：字节流到此为止（干净 EOF）。
    Eof,

    /// 查询不可用（传输没给出状态）：交给 `read_async` 自己去回答。
    Unknown,
}

/// 问一次传输读半边：可读多少 / 是否已经到头。
pub(crate) fn rx_state_<R>(reader: &R) -> RxState_
where
    R: TrConsumerState,
{
    match reader.consumer_state() {
        // 生产端关闭 + 无剩余数据：字节流到此为止。注意「封口但还有数据」必须先读干净，
        // 因此它落到下面的 `Readable(count)` 分支。
        Option::Some((0usize, true)) => RxState_::Eof,
        Option::Some((count, _)) => RxState_::Readable(count),
        // 默认实现返回 `None`（传输没实现这个查询）。**不能**当成 EOF——那会让连接
        // 静默地「干净退出」；交给下面的 park 由 `read_async` 给出真正的结论。
        Option::None => RxState_::Unknown,
    }
}

/// 一次「借段 / 等唤醒」的结果分类。
///
/// `abs_buff` 的等待结果类型是 `SomeOf<段, 错误>`——它可以**同时**给出左与右
/// （`Both`），因此不能像 `Result` 那样做模式匹配。这里统一收敛成一个三态枚举，
/// 让每个 park 点的处理都是一行。
pub(crate) enum Took_<T, E> {
    /// 拿到段（`Both` 也按拿到段处理：错误信息可以从段自身的关闭态推断）。
    Segm(T),
    /// 只有错误。
    Failed(E),
    /// 既没有段也没有错误：关闭态 / 无数据，回到循环顶部重新判断。
    Nothing,
}

/// 把 `SomeOf<段, 错误>` 收敛成 [`Took_`]。
pub(crate) fn took_<T, E>(outcome: SomeOf<T, E>) -> Took_<T, E> {
    if outcome.contains_left() {
        Took_::Segm(outcome.pick_left().expect("已确认有左值"))
    } else if outcome.contains_right() {
        Took_::Failed(outcome.pick_right().expect("已确认有右值"))
    } else {
        Took_::Nothing
    }
}

/// 传输写半边上还剩多少可写空间；`None` 表示**查询不可用或对端已消失**。
fn ring_space_<W>(writer: &W) -> Option<usize>
where
    W: TrProducerState,
{
    writer.producer_state().map(|(free, _closed)| free)
}

/// 把一帧（已编码）**分块**写进传输的写半边，直到全部写完。
///
/// # 为什么不能要求「整帧一次写」
///
/// 传输半边的容量由**调用方**决定（它背后是环，也可能是「设备 + 环」适配器），而
/// 「等它装得下整帧」在容量互相钳制时可能永远不成立（实测：环里剩 1987 字节、下一帧
/// 要 2010 字节，而排空方此刻正等别的方向腾空间）。因此这里按它当前能给的段**用多少
/// 写多少**：每片都是分段接口给出的连续区，写完由段的 drop 提交写指针。
///
/// 分段推进与「单生产者」这两点合起来保证了**不会写出残帧**：本循环是这条写半边
/// 唯一的写入者，因此一帧的片与片之间不会被别的帧插进来。
///
/// # Errors
///
/// 写半边不可用（对端消失 / 适配器报错）→ [`MuxError::Transport`]（方向为写）；
/// 被取消 → [`MuxError::Cancelled`]。
pub(crate) async fn enqueue_frame_<W, K>(
    tx_stage: &mut W,
    frame: &[u8],
    cancel: K,
) -> Result<(), MuxError>
where
    W: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    let mut offset = 0usize;
    // 片长上限取环容量量级即可：真正的片长由 `try_write` 按当前空闲空间给出。
    let demand = Demand::at_least(1usize);
    while offset < frame.len() {
        let rest = frame.len() - offset;
        // 把 `try_write` 的借用限制在自己的块里：借出的段在块结束时 drop（提交写
        // 指针、唤醒下游排空方），随后才能重新可变借用 `tx_stage` 去 park。
        let written = {
            let mut outcome = tx_stage.try_write(&demand);
            match outcome.as_mut().pick_left() {
                Option::Some(segment) => {
                    // `move_items_from_buff` 会**推进段的已消费量**，段的 drop 才会把
                    // 写入提交回环（推进写指针、唤醒下游排空方）。直接对 `iter_slices_mut`
                    // 的切片做逐元素赋值**不会**推进偏移，提交量会是 0——写环永远空着。
                    let mut child = segment.as_segm_mut();
                    let limit = core::cmp::min(rest, child.least_count());
                    if limit == 0 {
                        0usize
                    } else {
                        // SAFETY: `u8` 与 `MaybeUninit<u8>` 布局相同、对齐相同（均为 1）；
                        // 源切片来自本函数独占的 `frame`，目标段由 `try_write` 借出。
                        let src = unsafe {
                            core::slice::from_raw_parts(
                                frame[offset..offset + limit].as_ptr()
                                    as *const MaybeUninit<u8>,
                                limit,
                            )
                        };
                        // SAFETY: 源与目标都是 `MaybeUninit<u8>`，`u8` 无 drop 资源；
                        // `src` 指向本函数独占的 `frame`，`limit` 已按段可容量取小。
                        unsafe { child.move_items_from_buff(src) }
                    }
                }
                // 写环已满：下面 park 等空间，然后重试同一片。
                Option::None => 0usize,
            }
        };
        if written > 0 {
            offset += written;
            continue;
        }
        match race_cancel_(&cancel, tx_stage.write_async(&demand)).await {
            Option::None => return Result::Err(MuxError::Cancelled),
            Option::Some(outcome) => match took_(outcome) {
                // 段不消费即释放：只为唤醒。
                Took_::Segm(segm) => drop(segm),
                Took_::Failed(_) | Took_::Nothing => {
                    return Result::Err(MuxError::Transport { write: true });
                }
            },
        }
    }
    Result::Ok(())
}

/// 把**帧头 + 载荷**分两段写进传输写半边（数据面与控制面共用的入环入口）。
///
/// # 为什么可以分两段
///
/// 传输写半边是**单生产者**（只有复用循环写它），因此先写头、再写载荷不会被别的帧
/// 插进来；两段各自都是「按环当前能给的段分块推进、环满即 park」，与单段写入的语义
/// 完全一致。
///
/// 这样每帧不必先拼出一个连续 `Vec`：帧头来自栈上定长缓冲（[`frame_::encode_header_`]），
/// 载荷来自 `scratch`（注入分配器）或控制帧自己的载荷。**每帧少一次全局分配，数据面
/// 少一跳拷贝**（见 `dev-notes/audit-heap-alloc-20261004-1122.md` §3.1 #1 与 §5.1）。
///
/// # Errors
///
/// 与 [`enqueue_frame_`] 相同。
async fn enqueue_frame_parts_<W, K>(
    tx_stage: &mut W,
    head: &[u8],
    payload: &[u8],
    cancel: K,
) -> Result<(), MuxError>
where
    W: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    enqueue_frame_::<_, _>(tx_stage, head, cancel.child_token()).await?;
    enqueue_frame_::<_, _>(tx_stage, payload, cancel.child_token()).await
}

/// 把**读侧**游标错误映射为连接错误。
fn map_read_cursor_err_<E>(err: CursorError<E, ()>) -> MuxError {
    match err {
        CursorError::Read(_) => MuxError::Transport { write: false },
        CursorError::Write(()) => MuxError::Transport { write: true },
        CursorError::PeerClosed => MuxError::PeerClosed,
    }
}

/// 把一条控制帧的「原始头 + 载荷」写进公共 RX ring（背压时 park 等空间）。
///
/// 写之前 `demux_loop` 已经做完帧级 / 路由级合法性初审；这里只负责搬运。返回
/// `WouldBlock` 时停在 RX 空间上，**不丢帧**（设计稿 §5.8）。
async fn write_public_frame_async_<C, K>(
    public_rx_w: &mut BufferedTx,
    shared: &MuxShared_<C>,
    head: &[u8],
    payload: &[u8],
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    loop {
        match PublicChunkIo_::try_write_chunk_(
            public_rx_w,
            head,
            payload,
            shared.max_packet_size_(),
        ) {
            Result::Ok(()) => {
                shared.public_().notify_rx_ready_();
                return Result::Ok(());
            }
            Result::Err(ChunkWriteErr_::WouldBlock) => {}
            Result::Err(_) => return Result::Err(MuxError::FrameTooLarge),
        }
        let wait_space = poll_fn(|cx| shared.public_().poll_rx_space_(cx));
        if race_cancel_(cancel, wait_space).await.is_none() {
            return Result::Err(MuxError::Cancelled);
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 解复用循环
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 解复用循环**本地状态**的收尾守卫（规则与理由见 [`MuxLocalGuard_`]）。
///
/// 与复用侧对称、但**关的是另一半**：本循环持有每条子流**接收环的写端**，因此退出时
/// 要 `close()` 它——把「接收环为空而 park 的读者」唤醒（拿到 EOF 语义的 `Closing`）。
struct DemuxLocalGuard_<'a, C>
where
    C: TrConnCfg,
{
    /// 本地读表：dock 对 → 该子流接收环的**写端**（本循环持有）。
    table: ReadTable_<crate::connection::ConnAllocCfg_<C>>,

    /// 连接共享面（上报关闭与读时钟用）。
    shared: &'a MuxShared_<C>,
}

impl<C> core::ops::Deref for DemuxLocalGuard_<'_, C>
where
    C: TrConnCfg,
{
    type Target = ReadTable_<crate::connection::ConnAllocCfg_<C>>;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

impl<C> core::ops::DerefMut for DemuxLocalGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.table
    }
}

impl<C> Drop for DemuxLocalGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        let failed = self.shared.reg_.fail_kind_();
        let now_millis = self.shared.conn_clock_().now_millis_();
        for (pair, entry) in self.table.iter_mut() {
            // 显式关掉生产端：把「接收环为空而 park 的读者」唤醒（拿到 `Closing`）。
            entry.writer_.close();
            if failed.is_some() {
                report_conn_failed_::<C>(self.shared, &entry.owner_, pair.0, pair.1, now_millis);
            }
        }
        // `Attach` inbox 里还没取走的环半部：显式关闭，别让应用侧永久 park。
        while let Option::Some(attach) = self.shared.reg_.take_read_attach_() {
            let (_, _, mut writer_) = attach.into_parts_();
            writer_.close();
        }
        self.shared.reg_.close_read_inbox_();
    }
}

/// 解复用循环**读事件队列**的收尾守卫（规则与理由见 [`MuxLocalGuard_`]）。
struct DemuxEventsGuard_<'a, C>
where
    C: TrConnCfg,
{
    /// 尚未处理的事件（里面可能还压着没进表的 `Attach` 半部）。
    events: EventReceiver_<ReadEvent_<crate::connection::ConnAllocCfg_<C>>>,

    /// 连接共享面（上报关闭与读时钟用）。
    shared: &'a MuxShared_<C>,
}

impl<C> core::ops::Deref for DemuxEventsGuard_<'_, C>
where
    C: TrConnCfg,
{
    type Target = EventReceiver_<ReadEvent_<crate::connection::ConnAllocCfg_<C>>>;

    fn deref(&self) -> &Self::Target {
        &self.events
    }
}

impl<C> core::ops::DerefMut for DemuxEventsGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.events
    }
}

impl<C> Drop for DemuxEventsGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        // 共享面保留给后续 telegraph 失败上报；本轮 `TgAttach` 只需关闭环半部。
        let _ = self.shared;
        loop {
            let event = match self.events.try_take_event_() {
                EventTake_::Event(event) => event,
                // Drop 阶段不再做全扫：其余半部由循环退出时的守卫统一关闭。
                EventTake_::Rescan | EventTake_::Empty | EventTake_::Closed => break,
            };
            // 通道 `Attach` 已经改走 inbox；事件里只剩 telegraph 的环半部需要显式关闭。
            if let ReadEvent_::TgAttach { mut writer_, .. } = event {
                writer_.close();
            }
        }
    }
}


/// 解复用循环：从传输读半边解析帧、投递载荷、推进建流状态机。
pub(crate) async fn demux_loop_async_<C, K>(
    mut rx_stage: C::ConnRx,
    shared: MuxShared_<C>,
    events: EventReceiver_<ReadEvent_<crate::connection::ConnAllocCfg_<C>>>,
    events_tx: EventSender_<WriteEvent_<crate::connection::ConnAllocCfg_<C>>>,
    mut public_rx_w: BufferedTx,
    cancel: K,
) where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    // 表与事件队列都包进收尾守卫（与复用侧对称；理由见 `MuxLocalGuard_`）。
    let mut table: DemuxLocalGuard_<'_, C> = DemuxLocalGuard_ {
        table: crate::connection::dock_index_::DockBPlusTree_::new_in(lock_or_exit_!(
            shared.reg_.local_table_alloc_(cancel.child_token())
        ))
        .expect("2 KiB B+ 树足以容纳一个 channel 表项"),
        shared: &shared,
    };
    // telegraph 的读表与 channel 分开持有：主循环在自查「哪些端点已被释放」时既要读
    // 它、又要（用同一个分配器）建一个临时集合，同处一个结构体会撞借用检查。
    let alloc = lock_or_exit_!(shared.reg_.local_table_alloc_(cancel.child_token()));
    // 包进守卫：循环退出时**显式关闭**每个接收环写端（`buffex` 的环半部被 drop 不会
    // 置位关闭标记，少了这一步应用侧正 park 的 `recv_async` 永远醒不过来）。
    let mut tg_table: TgReadGuard_<'_, C> = TgReadGuard_ {
        table: crate::connection::dock_index_::DockBPlusTree_::new_in(alloc.clone())
            .expect("2 KiB B+ 树足以容纳一个 telegraph 端点"),
        _use_shared_: core::marker::PhantomData,
    };
    // 自查用的临时集合：复用同一个（每轮先 `clear`）。用本地表分配器的 2 KiB B 树，
    // 不再落到全局 `BTreeSet`；它随循环退出一并归还。
    let mut tg_live = crate::connection::dock_index_::DockSet_::new_in(alloc.clone())
        .expect("2 KiB B 树足以容纳存活的 telegraph dock");
    // 「已消失」的 dock 列表：复用同一个 `Vec`，分配器同样取本地表分配器。
    let mut gone = alloc::vec::Vec::new_in(alloc);
    let mut events: DemuxEventsGuard_<'_, C> = DemuxEventsGuard_ {
        events,
        shared: &shared,
    };

    // 控制帧的暂存：只用于「帧头解析完成、载荷已经整块在环上」之后的取载荷。
    // 连接级环本身已经是暂存，因此这里只需要容纳**一帧**的最大载荷。
    let payload_retain = lock_or_exit_!(crate::connection::alloc_zeroed_bytes_async_(
        shared.reg_.scope_(),
        shared.max_packet_size_,
    ));
    let mut payload = payload_retain
        .try_owning()
        .expect("刚分配的载荷暂存应当可独占");

    loop {
        if cancel.is_cancelled() {
            return;
        }
        #[cfg(feature = "test-loop-probe")]
        crate::connection::loop_probe_::demux_tick_();
        // 0. 先落实**会话释放**消息：`Drop` 只投消息、不碰身份表，因此处理「未知
        // 子流」之前必须先让「刚被丢弃的句柄」的释放生效——否则在途帧会被误判成
        // 协议违例，而不是宽限期（`WAIT_CLOSE`）内的静默丢弃。
        lock_or_exit_!(shared
            .reg_
            .drain_session_events_(shared.conn_clock_().now_millis_(), cancel.child_token()));

        // 0.05 合并提示：身份释放只置位 + 唤醒，读循环按 `RELEASED` 位摘表。
        let read_hints = shared.reg_().take_read_hint_();
        if read_hints & HINT_R_RELEASE != 0u64 {
            scan_released_::<C>(&mut *table);
        }
        // 0.06 `Attach` inbox：接收环写端从这里进入本地读表。
        //
        // 先前的 `RxConsumed` 事件可能比 `Attach` 更早被取出（两者不再共享同一条 FIFO）；
        // 那时表里还没有这一对，事件被丢弃、但 `RX_CONSUMED` 位仍留着。因此每插入一条
        // 新的读表项，就按位重算一次水位，把那条通知补回来。
        let mut attached_new = false;
        while let Option::Some(attach) = shared.reg_().take_read_attach_() {
            handle_read_attach_::<C>(attach, &mut *table);
            attached_new = true;
        }
        if attached_new
            && let Result::Err(err) = rescan_read_::<C, _>(
                &mut *table,
                &events_tx,
                shared.conn_clock_().now_millis_(),
                &cancel,
            )
            .await
        {
            fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
            return;
        }

        // 0.1 telegraph 自查：本地表里哪些端点**已经不在身份表里**了（tx / rx 两个半边
        //     都被丢弃 ⇒ 身份已被上面那步释放）。这些端点的接收环写端必须关掉，否则
        //     应用侧正 park 的 `recv_async` 永远醒不过来。
        //
        //     自查本身**非阻塞**：拿不到注册表锁就跳过这一轮（下一轮再来），绝不能为它
        //     阻塞中心循环。回调里只做一件事——把在册的 dock 记进 `tg_live`。
        if shared
            .reg_
            .for_each_live_telegraph_(|local_dock| {
                tg_live.insert(local_dock, ());
            })
        {
            gone.clear();
            for local_dock in tg_table.keys() {
                if !tg_live.contains_key(local_dock) {
                    gone.push(*local_dock);
                }
            }
            for local_dock in gone.iter().copied() {
                if let Option::Some(mut entry) = tg_table.remove(local_dock) {
                    entry.close_();
                }
            }
            gone.clear();
            tg_live.clear();
        }
        // 1. 先把挂起的 `Attach` / `Release` / `RxConsumed` 成批排空。
        if let Result::Err(err) =
            drain_read_events_::<C, _>(
                &mut *events,
                &mut table.table,
                &mut *tg_table,
                table.shared.conn_clock_().now_millis_(),
                &events_tx,
                &cancel,
            )
            .await
        {
            fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
            return;
        }

        // 2. 问一次**传输读半边**有没有字节。本版没有再建连接级读环：字节直接从
        //    传输半边读进来（park 在这里，**同时与读事件通道、取消令牌竞争**）。
        let readable = match rx_state_(&rx_stage) {
            RxState_::Readable(count) => count,
            // 生产端已封口且已排空：字节流到此为止，本循环退出（对侧会随之收尾）。
            RxState_::Eof => return,
            // 查询不可用：当作「此刻没读到东西」，由下面的 park 让 `read_async` 给出
            // 结论（它要么给出段，要么给出 EOF / 错误）。
            RxState_::Unknown => 0,
        };
        if readable == 0 {
            // 此刻没字节：park 到「传输来了字节」/「有读事件」/「取消」三者之一。
            //
            // **必须同时等读事件通道**：`RxConsumed` 是「应用刚消费、去核对水位」的
            // 唯一触发点，而它到来时传输完全可能长期为空（对端正等额度、没有新帧）。
            // 只 park 在传输上会让这条通知一直排在一条睡着的循环后面——应用读空了环却
            // 没人补发额度，两端互等（本仓库流控验收用例的死锁形态）。
            match park_read_wake_::<C, _>(&cancel, &mut rx_stage, &mut *events, &shared).await {
                // 传输里有字节：回顶部交给正式的帧头解析。
                ReadWake_::Bytes => {}
                // 生产端封口且已排空：与上面的 `RxState_::Eof` 同义。
                ReadWake_::Eof => return,
                // 真读错误（非「封口」）：连接不可用。
                ReadWake_::Fatal => {
                    fail_mux_loop_(
                        &shared,
                        &cancel,
                        Option::None,
                        &MuxError::Transport { write: false },
                    )
                    .await;
                    return;
                }
                // 事件已由 park **取出**（异步接收是消费语义，丢掉它就等于把这条
                // `Attach` / `RxConsumed` 静默吞掉），因此必须在这里就地处理，不能只
                // 回顶部等 `drain_read_events_` 再取一次。
                ReadWake_::Event(event) => {
                    if let Result::Err(err) = handle_read_event_::<C, _>(
                        event,
                        &mut table.table,
                        &mut *tg_table,
                        table.shared.conn_clock_().now_millis_(),
                        &events_tx,
                        &cancel,
                    )
                    .await
                    {
                        fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                        return;
                    }
                }
                // 有可重建事件被丢弃：全扫读表（水位提示在状态位里）。
                ReadWake_::Rescan => {
                    if let Result::Err(err) = rescan_read_::<C, _>(
                        &mut table.table,
                        &events_tx,
                        table.shared.conn_clock_().now_millis_(),
                        &cancel,
                    )
                    .await
                    {
                        fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                        return;
                    }
                }
                // `Attach` inbox：插入本地读表（已释放则直接关写端）。
                ReadWake_::Attach(attach) => {
                    handle_read_attach_::<C>(attach, &mut table.table);
                }
                // 合并提示（身份释放）：按 `RELEASED` 位摘读表并关接收环写端。
                ReadWake_::Hint(bits) => {
                    if bits & HINT_R_RELEASE != 0u64 {
                        scan_released_::<C>(&mut table.table);
                    }
                }
                ReadWake_::Cancelled => return,
            }
            continue;
        }

        // 3. 读到帧头。解复用一律走 `frame_parser_` 的 **sans-IO 逐字节状态机**：
        //    它每次只向传输索要 `Demand::exactly(1)`，于是 `min_len == 1` 不可能超过任何
        //    合法环容量，旧入口「按字段索要 `width` 字节」在容量 < width 时拿到终态
        //    `Unsatisfiable` 而整条连接失败的路径**从构造上消失**。
        //    帧头可能消耗传输的读指针，因此必须与第 2 步合起来看：传输里此刻至少有一帧的
        //    **前若干字节**，帧头解析不会因为「此刻没字节」而永久 park——喂数据的一方
        //    （调用方的入向泵 / 对端连接）会继续填充。
        let (header, head_len) = match race_cancel_(
            &cancel,
            frame_parser_::read_header_async_::<_, _>(&mut rx_stage, cancel.child_token()),
        )
        .await
        {
            Option::None => return,
            Option::Some(Result::Ok(parsed)) => parsed,
            Option::Some(Result::Err(err)) => {
                fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                return;
            }
        };

        // 4. 载荷长度校验（与写侧同一条上限：帧总长 ≤ `max_packet_size`）。
        let len = header.payload_len();
        if len + K_MAX_FRAME_HEADER > shared.max_packet_size_ || len > payload.len() {
            fail_mux_loop_(
                &shared,
                &cancel,
                Option::Some(header.kind()),
                &MuxError::FrameTooLarge,
            )
            .await;
            return;
        }
        if len > 0 {
            let mut cursor = ReadCursor::new_(&mut rx_stage);
            match race_cancel_(
                &cancel,
                cursor.read_async_(&mut payload[..len], cancel.child_token()),
            )
            .await
            {
                Option::None => return,
                Option::Some(Result::Ok(())) => {}
                Option::Some(Result::Err(err)) => {
                    fail_mux_loop_(
                        &shared,
                        &cancel,
                        Option::Some(header.kind()),
                        &map_read_cursor_err_::<_>(err),
                    )
                    .await;
                    return;
                }
            }
        }
        let bytes = &payload[..len];

        // 帧种类在这里取一次：下面的上报与派发都用它。
        let kind = header.kind();

        // 5. 派发。帧头里的 `LocalDock` 是发送方的本端 dock，因此**本端的
        //    local_dock 是帧头的 `RemoteDock`**（镜像语义，见模块文档 §4.2）。
        let local = header.remote_dock();
        let remote = header.local_dock();
        let pair = (local, remote);

        // 帧已经完整到手（帧头解析 + 载荷读取 + 长度校验都过了），上报「收到一个帧」。
        //
        // **口径与写侧逐字一致**：`head_len` 是逐字节状态机自己数出的**帧头长度**
        // （[`frame_parser_::FrameHeaderParser::consumed_len_`]），加上载荷长度就是该帧的
        // **线上总长**。因此两侧的 `on_frame` 字节数可以直接相加比对——端到端用例正是
        // 用「发送帧字节之和 == 接收帧字节之和」把这条口径钉住的。
        shared.metrics_().on_frame(
            FrameDir::Recv,
            local,
            remote,
            kind,
            u32::try_from(head_len + len).unwrap_or(u32::MAX),
        );
        // 传输字节口径见 [`TrMetricsSink::on_transport_bytes`]：旧版由外侧读泵按
        // 「线上原始字节」上报（含零头与被丢弃的帧），本版没有泵，改由解复用循环按
        // 「已成型并搬运的帧字节」上报——与上面 `on_frame` 的口径一致。
        shared
            .metrics_()
            .on_transport_bytes(FrameDir::Recv, u64::try_from(head_len + len).unwrap_or(u64::MAX));

        // 每次活动就地读一次连接级时钟（vDSO 读，相对本帧的解析 / 环操作可忽略）：
        // 两个时钟必须记下**活动发生的时刻**，而不是计时循环扫描的时刻。
        let now_millis = shared.conn_clock_().now_millis_();
        match kind {
            FrameKind::Data => {
                let amount = Credit::try_from(len).unwrap_or(Credit::MAX);
                let Some((_, entry)) = table.get_mut(pair) else {
                    // 本地表里没有这条子流，分两种情况：
                    //
                    // - 该 dock 对刚被拆掉（**宽限态**）：这是拆流竞态里对端在收到
                    //   我们 `CLOSE` 之前发出的**在途帧**，静默丢弃即可——两个方向
                    //   是各自有序的独立字节流，「按序」并不能阻止它晚于本地拆流到达；
                    // - 真正的未知子流：协议违例，终止连接。
                    if lock_or_exit_!(shared.reg_.is_wait_close_(
                        local,
                        remote,
                        shared.conn_clock_().now_millis_(),
                        cancel.child_token()
                    )) {
                        continue;
                    }
                    fail_mux_loop_(&shared, &cancel, Option::Some(kind), &MuxError::MalformedFrame).await;
                    return;
                };
                if entry.owner_.is_app_rx_closed_() {
                    // 应用已丢弃接收半边：本端（已经或即将）向对端发 `RESET`，此刻到达的
                    // 数据都是「它在收到我们 `RESET` 之前发出的在途帧」。写进一条消费端
                    // 已关闭的环只会得到 `ProducerError::Closing`，并把整条连接判成传输
                    // 错误（旧实现的实测形态）；因此这里**静默丢弃**。
                    continue;
                }
                if entry.owner_.is_aborted_() {
                    // 计时循环已判定空闲超时并认领了中止：身份正在（或已经）转入宽限态。
                    // 这条判定不能省——从「认领中止」到「登记处落成宽限态」之间有一段
                    // 会被本循环插进来的窗口，此间到达的在途数据必须静默丢弃，而不是掉进
                    // 「未知子流」分支把整条连接判成协议违例。
                    continue;
                }
                entry.owner_.mark_data_(now_millis);
                let counted = entry.owner_.recv_on_data_(amount);
                if let Result::Err(err) = counted {
                    fail_mux_loop_(&shared, &cancel, Option::Some(kind), &MuxError::FlowCtrl(err))
                        .await;
                    return;
                }
                // **数据到达本身就是一次水位变化**（尤其是「刚好归零」），必须在这次
                // 派发里当场判定并通告。
                //
                // 不能只靠应用消费触发的核对（`ReadEvent_::RxConsumed`）：那条路径要等
                // 应用真的读一次才走，而「只收不消费」的那一段正是对端最需要知道水位已经
                // 变小的时刻。更要紧的是时序——应用完全可能在本循环反应过来之前就把刚到的
                // 字节读走，那时窗口已经不是 0 了，归零判据再也不会成立，而**已通告快照
                // 仍是旧值**（上面的 `on_data` 不推进它），对端会一直按「还有一整个窗口」
                // 行动。判据全在 `RecvWindow::should_report_with_` 里（窗口归零落在
                // **临界区**，临界区每变必报）。
                let imminent = entry.owner_.recv_take_report_();
                if let Option::Some(report) = imminent {
                    let _ = events_tx.try_send_durable_event_(WriteEvent_::Control {
                        frame_: window_update_frame_(pair, report),
                    });
                }
                let wrote = race_cancel_(
                    &cancel,
                    write_into_ring_(&mut entry.writer_, bytes, cancel.child_token()),
                )
                .await;
                match wrote {
                    Option::None => return,
                    Option::Some(Result::Ok(())) => {}
                    // 消费端在检查之后、写进环之前被应用丢掉：这批字节已经没人要了，
                    // 静默丢弃即可（**不能**按传输错误终止整条连接）。
                    Option::Some(Result::Err(err))
                        if err.err_tag() == WriteErrTag::Closing => {}
                    Option::Some(Result::Err(_err)) => {
                        fail_mux_loop_(
                            &shared,
                            &cancel,
                            Option::Some(kind),
                            &MuxError::Transport { write: true },
                        )
                        .await;
                        return;
                    }
                }
            }
            FrameKind::Open => {
                if window_report_of_(&header).is_none() {
                    fail_mux_loop_(&shared, &cancel, Option::Some(kind), &MuxError::MalformedFrame)
                        .await;
                    return;
                }
                let (head, head_len) = match encode_header_(&header) {
                    Result::Ok(value) => value,
                    Result::Err(err) => {
                        fail_mux_loop_(&shared, &cancel, Option::Some(kind), &err).await;
                        return;
                    }
                };
                if let Result::Err(err) = write_public_frame_async_::<C, _>(
                    &mut public_rx_w,
                    &shared,
                    &head[..head_len],
                    bytes,
                    &cancel,
                )
                .await
                {
                    fail_mux_loop_(&shared, &cancel, Option::Some(kind), &err).await;
                    return;
                }
            }
            FrameKind::Accept | FrameKind::Reject => {
                if table.get(pair).is_some() {
                    let (head, head_len) = match encode_header_(&header) {
                        Result::Ok(value) => value,
                        Result::Err(err) => {
                            fail_mux_loop_(&shared, &cancel, Option::Some(kind), &err).await;
                            return;
                        }
                    };
                    if let Result::Err(err) = write_public_frame_async_::<C, _>(
                        &mut public_rx_w,
                        &shared,
                        &head[..head_len],
                        bytes,
                        &cancel,
                    )
                    .await
                    {
                        fail_mux_loop_(&shared, &cancel, Option::Some(kind), &err).await;
                        return;
                    }
                }
            }
            FrameKind::Close => {
                let reset = header.flags() & flags::K_RESET != 0;
                if let Option::Some((_, entry)) = table.get_mut(pair) {
                    if reset {
                        // 对端宣告**不再接收**：收尾的是**本端的发送方向**，与本端
                        // 接收方向无关。因此**不**关接收环写端——那会把对端仍在途、
                        // 且已经写进本端接收环的数据一起丢掉（旧实现正是如此，实测
                        // 少一个窗口，见 `dev-notes/flow-ctrl…` §3.3）。
                        entry.owner_.set_peer_reset_();
                        // 发送方向到此不必再等：等待中的 `flush_async` / `close_async`
                        // 应当醒来读到「对端不再接收」这个终局。
                        entry.owner_.notify_tx_progress_();
                        // 上报「对端发来 RESET」。
                        shared.metrics_().on_reset(local, remote, true);
                    } else {
                        // 对端宣告**不再发送**：关掉接收环写端，应用读完已缓存数据
                        // 之后读到 EOF。
                        entry.writer_.close();
                        entry.owner_.set_peer_fin_();
                        // 接收方向到此自然收尾：`close_async` 的等待者由此醒来。
                        entry.owner_.notify_rx_progress_();
                    }
                    entry.owner_.mark_data_(now_millis);
                    // 拆流记账（移除写侧表项、释放身份）交给复用循环的统一入口。
                    // `PEER_FIN` / `PEER_RESET` 位已经置上，这里只负责唤醒写循环。
                    shared.reg_().raise_write_hint_(HINT_W_PEER_CLOSED);
                }
                // 公共子流：把整帧交给 `main_loop` 做协议状态推进。P2 期间上面的
                // 本地动作仍在（两处都是幂等置位）；P4 删除本循环的直接处理。
                let (head, head_len) = match encode_header_(&header) {
                    Result::Ok(value) => value,
                    Result::Err(err) => {
                        fail_mux_loop_(&shared, &cancel, Option::Some(kind), &err).await;
                        return;
                    }
                };
                if let Result::Err(err) = write_public_frame_async_::<C, _>(
                    &mut public_rx_w,
                    &shared,
                    &head[..head_len],
                    bytes,
                    &cancel,
                )
                .await
                {
                    fail_mux_loop_(&shared, &cancel, Option::Some(kind), &err).await;
                    return;
                }
            }
            FrameKind::WindowUpdate | FrameKind::Pulse => {
                if let Option::Some((_, entry)) = table.get(pair)
                    && let Option::Some(report) = window_report_of_(&header)
                {
                    let owner = entry.owner_.clone();
                    let _ = owner.send_on_report_(report);
                    // `WINDOW_UPDATE` 是**数据路径**上的活动（对端应用消费了数据），
                    // 因此也刷新保活职责时钟；`PULSE` 只是保活本身，**只**刷新存活
                    // 时钟——这正是两端都不会被对方的保活「劝退」的原因。
                    if header.kind() == FrameKind::Pulse {
                        owner.touch_(now_millis);
                    } else {
                        owner.mark_data_(now_millis);
                    }
                    // **必须叫醒复用循环**：这条子流的额度刚刚（可能）变大，而写循环
                    // 完全可能正 park 在事件通道上——它上一次尝试发送时额度为 0，
                    // 于是「环里有数据但发不出去」，此后应用不再写入（环是满的），
                    // 也就不会再产生 `TxReady`。少了这次唤醒，`WINDOW_UPDATE` 会被
                    // 白收：发送方永远停在 0，接收方空着额度（本仓库流控验收用例撞到
                    // 的第三个死锁）。
                    //
                    // 这里**不能用「已入队」去重位**：它可能正被应用上一次写入置着
                    // （额度为 0 时写循环根本没机会清它），于是这次唤醒会被静默丢掉。
                    let _ = events_tx.try_send_event_(WriteEvent_::TxReady {
                        local_dock: local,
                        remote_dock: remote,
                    });
                } else if let Option::Some(owner) = lock_or_exit_!(shared.reg_.channel_owner_(
                    local,
                    remote,
                    cancel.child_token()
                )) {
                    // 建流窗口：身份已在册（`reserve_inbound_`），而应用还没走到最终
                    // 裁决，读侧表项尚未安装。此刻到达的 `PULSE` / `WINDOW_UPDATE`
                    // 承载的正是对端的活动信号，不能因为「本地表还没这一项」就吞掉——
                    // 否则本端会在对端明明活着的时候判自己空闲超时。
                    //
                    // 这与「建流尚未完成的子流也要被超时控制」不冲突：超时判据仍是
                    // **本端自己的**（自身份登记起算，不进协议），这里只是把对端的
                    // 活动如实记账。
                    if header.kind() == FrameKind::Pulse {
                        owner.touch_(now_millis);
                    } else {
                        owner.mark_data_(now_millis);
                    }
                }
            }
            FrameKind::Datagram => {
                // 数据报（telegraph）：**地址**语义，按帧头的 `RemoteDock`（= 本端
                // local_dock）找到那条端点的接收上下文，把**整帧**（帧头 + 载荷）
                // 交给它——由它自行拆出远端地址与载荷，并自行处置「装不下就整条丢弃」。
                //
                // 两条要点：
                //
                // 1. **不查宽限态、不报协议违例**：数据报没有身份可言，发往一个本端
                //    没有 telegraph 的 dock 只是「没人收」（对端可能按 `wildcard` 发
                //    过来），**整个帧直接丢弃**即可——连丢弃计数都不记，因为无人可
                //    归因；
                // 2. 中心循环**不认识数据报的内部结构**：帧头不被拆解，帧头与载荷
                //    一起投出去（见 `telegraph::recv_ctx_` 的模块文档）。
                let Some(ctx) = tg_table.table.get_mut(local).map(|(_, value)| value) else {
                    continue;
                };
                ctx.deliver_frame_(&header, bytes, shared.metrics_());
            }
        }
    }
}

/// 从帧头取出窗口通告（`PULSE` / `WINDOW_UPDATE` / `OPEN` 上必需）。
fn window_report_of_(header: &FrameHeader) -> Option<WindowReport> {
    let total = header.recv_total()?;
    let window = header.recv_window()?;
    if header.is_total_reset() {
        Option::Some(WindowReport::new_reset(total, window))
    } else {
        Option::Some(WindowReport::new(total, window))
    }
}

/// 非阻塞排空读事件（`RxConsumed` / `TgAttach`），返回连接级错误。
///
/// 与本模块的复用循环同理：**每轮的批有上限**。事件是持续到来的（应用每读一次就会
/// 投一条 `RxConsumed`），若「处理完为止」而不设上限，事件队列始终非空时本循环就
/// 永远走不到「读一个帧头」那一步。
///
/// `RxConsumed` 的处理是纯原子读改写（不取锁），但通告要经事件通道投给写循环，
/// 因此本函数仍是 `async`。
async fn drain_read_events_<C, K>(
    events: &mut EventReceiver_<ReadEvent_<crate::connection::ConnAllocCfg_<C>>>,
    table: &mut ReadTable_<crate::connection::ConnAllocCfg_<C>>,
    tg_table: &mut TgReadTable_<crate::connection::ConnAllocCfg_<C>>,
    now_millis: u64,
    events_tx: &EventSender_<WriteEvent_<crate::connection::ConnAllocCfg_<C>>>,
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    const K_EVENT_BATCH: usize = 32usize;
    for _ in 0..K_EVENT_BATCH {
        match events.try_take_event_() {
            EventTake_::Event(event) => {
                handle_read_event_::<C, _>(event, table, tg_table, now_millis, events_tx, cancel)
                    .await?;
            }
            // 有可重建事件被丢弃：按各子流的状态位全扫一次，然后让主循环继续。
            EventTake_::Rescan => {
                rescan_read_::<C, _>(table, events_tx, now_millis, cancel).await?;
                break;
            }
            EventTake_::Empty | EventTake_::Closed => break,
        }
    }
    Result::Ok(())
}

/// 定容读事件通道的**全扫兜底**：重建被丢弃的可重建事件。
///
/// 读事件里只有 `RxConsumed` 是可重建的：位在 [`ChannelState_`] 上；清位时若真的
/// 置着，就按「环内实际积压」重算一次窗口通告。
///
/// `Attach` 携带环半部句柄，生产者走 [`EventSender_::try_send_durable_event_`]，
/// 不会丢；身份释放（`Release`）已经改为 `RELEASED` 位 + `HINT_R_RELEASE` 合并提示，
/// 由 [`scan_released_`] 按位摘表，不再经过事件通道。
async fn rescan_read_<C, K>(
    table: &mut ReadTable_<crate::connection::ConnAllocCfg_<C>>,
    events_tx: &EventSender_<WriteEvent_<crate::connection::ConnAllocCfg_<C>>>,
    now_millis: u64,
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let mut cursor: Option<(Dock, Dock)> = Option::None;
    loop {
        let next = match cursor {
            Option::None => table.keys().next().copied(),
            Option::Some(last) => table
                .range((Bound::Excluded(last), Bound::Unbounded))
                .next()
                .map(|(key, _)| *key),
        };
        let Option::Some(pair) = next else {
            return Result::Ok(());
        };
        cursor = Option::Some(pair);

        // 先取快照再 awaits：`entry` 的借用不能跨 `await`。
        let Option::Some((owner, buffered)) = table.get(pair).map(|(_, entry)| {
            (
                entry.owner_.clone(),
                entry.writer_.ring_state().data_size(),
            )
        }) else {
            continue;
        };

        if owner.clear_rx_consumed_() {
            recheck_recv_level_::<C, _>(
                &owner,
                buffered,
                now_millis,
                events_tx,
                pair,
                cancel,
            )
            .await?;
        }
    }
}

///
/// 处理**单条**读事件。
///
/// 单独成函数是因为同一种事件有**两个**来源：`drain_read_events_` 从队列成批取出的，
/// 以及循环 park 时从队列里**取出**的那一条。后者不可省——异步接收是**消费**语义，
/// park 拿到手又丢掉就等于把这条 `Attach` 静默吞掉（本仓库的
/// `mux_min_stage_inmem` 用例正是这么挂死的：附加事件被 park 吃掉，随后到达的数据帧
/// 在本地表里找不到表项，读循环掉进「未知子流」分支并卡在注册表锁上）。
async fn handle_read_event_<C, K>(
    event: ReadEvent_<crate::connection::ConnAllocCfg_<C>>,
    table: &mut ReadTable_<crate::connection::ConnAllocCfg_<C>>,
    tg_table: &mut TgReadTable_<crate::connection::ConnAllocCfg_<C>>,
    now_millis: u64,
    events_tx: &EventSender_<WriteEvent_<crate::connection::ConnAllocCfg_<C>>>,
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    match event {
        ReadEvent_::RxConsumed {
            local_dock,
            remote_dock,
        } => {
            let pair = (local_dock, remote_dock);
            let Some((_, entry)) = table.get_mut(pair) else {
                // 表里没有这条子流：附加事件还没到（或已被 Release 摘掉），
                // 本次通知已无对象，直接丢弃。
                return Result::Ok(());
            };
            // **先清位、再读环内积压**：顺序不可反，否则「清位与读积压之间」发生的
            // 那次消费会既没有置位、也没有被这次核对看到（见
            // `ChannelState_::clear_rx_consumed_`）。
            entry.owner_.clear_rx_consumed_();
            let owner = entry.owner_.clone();
            // 环内**实际积压**（已提交、应用还没取走）。本循环是接收环唯一的写入
            // 方，因此这次采样与上面的 `received_` 是同一时刻的一致快照。
            let buffered = entry.writer_.ring_state().data_size();
            recheck_recv_level_::<C, _>(&owner, buffered, now_millis, events_tx, pair, cancel)
                .await?;
        }
        // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
        // telegraph（数据报）
        // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
        ReadEvent_::TgAttach {
            local_dock,
            owner,
            writer_,
        } => {
            // 与 channel 的 `Attach` 同形：重复附直接覆盖，不会留下陈旧条目。
            tg_table.insert(local_dock, TgRecvCtx_::new_(local_dock, owner, writer_));
        }
    }
    Result::Ok(())
}

/// 应用消费之后，由**持有接收环写端**的本循环核对水位并择机补发通告。
///
/// # 为什么判定权在这里
///
/// 「累计已消费量」= `RecvWindow` 记账的累计已收 `R` − 环内实际积压 `buffered`。
/// 本循环是接收环**唯一的写入方**，两个量都在同一个任务里读，因此这个差值是**精确**
/// 的；而应用侧只能采样 `data_size` 求差，一次并发写入就会把同一区间的读出掩盖
/// （因果与实测终局见 `dev-notes/flow-ctrl-20261005-0115.md` §3）。
///
/// 通告仍走 [`WriteEvent_::Control`] 这条车道交给复用循环写出——本循环不持有连接
/// 写环。
async fn recheck_recv_level_<C, K>(
    owner: &ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,
    buffered: usize,
    now_millis: u64,
    events_tx: &EventSender_<WriteEvent_<crate::connection::ConnAllocCfg_<C>>>,
    pair: (Dock, Dock),
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let _ = cancel;
    let buffered = Credit::try_from(buffered).unwrap_or(Credit::MAX);
    let report = owner.recv_recheck_(buffered, now_millis);
    if let Option::Some(report) = report {
        let _ = events_tx.try_send_durable_event_(WriteEvent_::Control {
            frame_: window_update_frame_(pair, report),
        });
    }
    Result::Ok(())
}

/// 造一条携带窗口通告的 `WINDOW_UPDATE` 控制帧（读循环的两条通告路径共用）。
fn window_update_frame_(pair: (Dock, Dock), report: WindowReport) -> ControlFrame_ {
    ControlFrame_::with_window_(
        FrameKind::WindowUpdate,
        if report.is_reset() {
            flags::K_TOTAL_RESET
        } else {
            0u8
        },
        pair.0,
        pair.1,
        Option::Some((report.recv_total(), report.window())),
    )
}

/// 解复用循环在「传输此刻没字节」时的等待结果。
enum ReadWake_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 传输里来了字节。
    Bytes,

    /// 传输的**生产端已封口且已排空**：字节流到此为止（干净 EOF）。
    ///
    /// 传输的状态查询不一定能给出这一结论（例如它没实现 `TrConsumerState`），因此
    /// 这里也把「读 `Err(Closing)`」归到本变体——两种来源同义。
    Eof,

    /// 传输读失败（不是「封口」）：连接不可用。
    Fatal,

    /// 从读事件通道里**取出**了一条事件（`Attach` / `Release` / `RxConsumed`）。
    ///
    /// 必须由调用方处理：异步接收是**消费**语义，丢掉它就等于静默吞掉这条事件。
    Event(ReadEvent_<C>),

    /// 有可重建事件被丢弃：调用方应全扫读表，把水位提示重建出来。
    Rescan,

    /// 合并提示（身份释放等）：把掩码带回调用方，按状态位推进本地表。
    Hint(u64),

    /// `Attach` inbox 里来了一条新的接收环写端。
    Attach(ReadAttach_<C>),

    /// 取消令牌触发：连接正在收尾，本循环退出。
    Cancelled,
}

/// 解复用循环空转时的 park：**同时**等「传输有字节」与「读事件通道有事件」，
/// 并与取消令牌竞争。
///
/// # 三个 future 必须在闭包外 `pin` 好
///
/// 环的 park future 在 drop 时**撤回**自己的唤醒登记（`buffex::ring` 的等待槽是
/// **单槽信箱**），因此「就地建 future、poll 一次、丢掉」等于只问一句「此刻可读吗」，
/// 留不下任何唤醒——复用循环那条「park 在环上等提交」的兜底正是这么失效的（见
/// `dev-notes/flow-ctrl-20261005-0115.md` §5）。这里把三个 future 都跨 poll 持有。
async fn park_read_wake_<C, K>(
    cancel: &K,
    rx_stage: &mut C::ConnRx,
    events: &mut EventReceiver_<ReadEvent_<crate::connection::ConnAllocCfg_<C>>>,
    shared: &MuxShared_<C>,
) -> ReadWake_<crate::connection::ConnAllocCfg_<C>>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let demand = Demand::at_least(1usize);
    let cancel_fut = cancel.child_token().cancellation();
    let mut cancel_fut = core::pin::pin!(cancel_fut);
    let mut event_fut = core::pin::pin!(events.take_event_async_());
    let mut ring_fut = core::pin::pin!(core::future::IntoFuture::into_future(
        rx_stage.read_async(&demand),
    ));
    // `Attach` 通道的接收 future：`RecvAsync` 是参数载体，先转 future 再跨 poll 持有。
    let mut attach_fut = core::pin::pin!(core::future::IntoFuture::into_future(
        shared.reg_().recv_read_attach_async_()
    ));
    // 读事件的生产端全部消失 = 连接核心已析构，取消令牌随即触发。此后不再 poll 事件
    // 通道（它已 `Ready(None)` 且会立刻再次就绪），只等传输与取消令牌。
    let mut events_closed = false;
    let mut cancelled = false;
    // 从通道里**取出**的事件：必须原样带回调用方，不能在此丢掉。
    let mut taken: Option<ReadEvent_<crate::connection::ConnAllocCfg_<C>>> = Option::None;
    // 有可重建事件被丢弃：把「全扫」带回调用方（读表在调用方手里）。
    let mut need_rescan = false;
    // 传输读 future 就绪时的分类（段本身借用 `ring_fut`，因此就地判、就地 drop）。
    let mut woke: Option<ReadWake_<crate::connection::ConnAllocCfg_<C>>> = Option::None;
    // 合并提示（身份释放）：把掩码带回调用方。
    let mut hint_bits = 0u64;
    // `Attach` inbox 里取出的一条。
    let mut taken_attach: Option<ReadAttach_<crate::connection::ConnAllocCfg_<C>>> = Option::None;
    poll_fn(|cx| {
        // 取消令牌优先：连接已收尾，直接退出。
        if core::future::Future::poll(cancel_fut.as_mut(), cx).is_ready() {
            cancelled = true;
            return Poll::Ready(());
        }
        if !events_closed {
            match core::future::Future::poll(event_fut.as_mut(), cx) {
                Poll::Ready(EventTake_::Event(event)) => {
                    taken = Option::Some(event);
                    return Poll::Ready(());
                }
                Poll::Ready(EventTake_::Rescan) => {
                    need_rescan = true;
                    return Poll::Ready(());
                }
                Poll::Ready(EventTake_::Closed) => events_closed = true,
                // `take_event_async_` 不会返回 `Empty`；保守地当作「稍后再试」。
                Poll::Ready(EventTake_::Empty) => {}
                Poll::Pending => {}
            }
        }
        // 传输来了数据：回顶部交给正式的帧头解析。借出的段在此 drop（**不消费**，
        // 已消费量为 0——字节仍留在传输自己的缓冲里）。
        //
        // 就绪的也可能是**收尾**而不是数据：`read_async` 在「生产端封口」时给出
        // `Err(Closing)`。两种情形必须分开——前者回顶部继续解析，后者是本端该干净
        // 退出的时刻；把 `Closing` 当成普通读错误会让「对端正常关闭」变成连接级失败。
        if woke.is_none() {
            match core::future::Future::poll(ring_fut.as_mut(), cx) {
                Poll::Pending => {}
                Poll::Ready(outcome) => {
                    woke = Option::Some(match took_(outcome) {
                        // 段不消费即释放：只为唤醒，数据留给随后的正式解析。
                        Took_::Segm(segm) => {
                            drop(segm);
                            ReadWake_::Bytes
                        }
                        Took_::Failed(err) => {
                            if err.err_tag() == ReadErrTag::Closing {
                                ReadWake_::Eof
                            } else {
                                ReadWake_::Fatal
                            }
                        }
                        // 既没有段也没有错误：当作「来了字节」回顶部重判一次
                        // （与旧实现同义，避免在这里自旋）。
                        Took_::Nothing => ReadWake_::Bytes,
                    });
                    return Poll::Ready(());
                }
            }
        }
        // 合并提示：身份释放不再逐条投事件，这里把掩码带回调用方。
        if let Poll::Ready(bits) = shared.reg_().poll_read_hint_(cx) {
            hint_bits = bits;
            return Poll::Ready(());
        }
        match core::future::Future::poll(attach_fut.as_mut(), cx) {
            Poll::Ready(Result::Ok(attach)) => {
                taken_attach = Option::Some(attach);
                return Poll::Ready(());
            }
            // 读侧通道关闭（连接收尾）。
            Poll::Ready(Result::Err(_)) => return Poll::Ready(()),
            Poll::Pending => {}
        }
        Poll::Pending
    })
    .await;
    if cancelled {
        ReadWake_::Cancelled
    } else if need_rescan {
        ReadWake_::Rescan
    } else if hint_bits != 0u64 {
        ReadWake_::Hint(hint_bits)
    } else if let Option::Some(attach) = taken_attach {
        ReadWake_::Attach(attach)
    } else if let Option::Some(event) = taken {
        ReadWake_::Event(event)
    } else if let Option::Some(woke) = woke {
        woke
    } else {
        // 事件通道关闭（连接核心已析构）且传输与取消都没就绪：按收尾处理。
        ReadWake_::Cancelled
    }
}

/// 把 `bytes` 全部写进接收环（环满时 park，等应用消费）。
async fn write_into_ring_<W, K>(
    writer: &mut W,
    mut bytes: &[u8],
    cancel: K,
) -> Result<(), W::Err>
where
    W: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    while !bytes.is_empty() {
        let demand = Demand::at_least(1usize);
        let mut outcome = writer
            .write_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        let put = match outcome.as_mut().pick_left() {
            Option::Some(segm) => segm.as_segm_mut().clone_items_from_buff(bytes),
            Option::None => {
                return Result::Err(
                    outcome
                        .pick_right()
                        .expect("IO 结果必须要么是段、要么是错误"),
                );
            }
        };
        if put == 0usize {
            return Result::Err(
                outcome
                    .pick_right()
                    .expect("段为空时应当已经给出错误"),
            );
        }
        bytes = &bytes[put..];
    }
    Result::Ok(())
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 复用循环
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 连接级失败牵连的**逐子流关闭上报**（恰好一次）。
///
/// 「恰好一次」由 [`ChannelOwner_::claim_release_`] 的一次性 CAS 保证：它与
/// [`maybe_release_`] 的正常收尾路径共用同一把闸门，因此同一条子流不会被报两次。
fn report_conn_failed_<C>(
    shared: &MuxShared_<C>,
    owner: &ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,
    local: Dock,
    remote: Dock,
    now_millis: u64,
) where
    C: TrConnCfg,
{
    if !owner.claim_release_() {
        return;
    }
    shared.metrics_().on_channel_closed(
        local,
        remote,
        ChannelCloseReason::ConnFailed,
        now_millis.saturating_sub(owner.created_millis_()),
    );
}

/// 复用循环**本地状态**的收尾守卫（表 + 事件队列各一个）。
///
/// # 为什么必须显式 `close()`
///
/// 应用侧的写者可能正 park 在「发送环满」上、读者正 park 在「接收环为空」上；而
/// `buffex` 的环半部 **drop 不置关闭位、也不唤醒对端**，事件通道的消费者又正是这个
/// 正在退出的循环。于是「循环退出 ⇒ 表被丢掉」**不产生任何唤醒**：应用那半部永远
/// 睡下去——这就是「连接级失败之后应用无期限挂起」的机制。`close()` 是唯一既置关闭位
/// 又唤醒对端的方式，而这一半的持有者只有本循环。
///
/// # 为什么分成「表」与「事件队列」两个守卫
///
/// 半部有两拨：一拨已经进了本地表，另一拨（`Attach`）还**躺在事件队列里**——连接完全
/// 可能在子流刚建好、循环还没取走那条事件时就结束，此时应用已经拿着另一半。两拨都要
/// 收尾，漏掉后者同样是永久挂起。
///
/// # 为什么用 `Deref` 转发而不是把守卫穿进主体
///
/// 主体逐字不动（`table.get_mut(..)` / `events.try_take_event_()` 经自动解引用照常
/// 工作），收尾因此与「表 / 队列的生命周期」绑定：**任何**退出路径（正常返回、取消、
/// 连接级失败，乃至 panic 展开）都会跑到，不必在每个 `return` 前手写一遍。
struct MuxLocalGuard_<'a, C>
where
    C: TrConnCfg,
{
    /// 本地写表：dock 对 → 该子流发送环的**读端**（本循环持有）。
    table: WriteTable_<crate::connection::ConnAllocCfg_<C>>,

    /// 连接共享面（上报关闭与读时钟用）。
    shared: &'a MuxShared_<C>,
}

impl<C> core::ops::Deref for MuxLocalGuard_<'_, C>
where
    C: TrConnCfg,
{
    type Target = WriteTable_<crate::connection::ConnAllocCfg_<C>>;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

impl<C> core::ops::DerefMut for MuxLocalGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.table
    }
}

impl<C> Drop for MuxLocalGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        let failed = self.shared.reg_.fail_kind_();
        let now_millis = self.shared.conn_clock_().now_millis_();
        for (pair, entry) in self.table.iter_mut() {
            // 显式关掉消费端：把「发送环已满而 park 的写者」唤醒（拿到 `Closing`）。
            entry.reader_.close();
            if failed.is_some() {
                report_conn_failed_::<C>(self.shared, &entry.owner_, pair.0, pair.1, now_millis);
            }
        }
        // `Attach` inbox 里还没取走的环半部：显式关闭，别让应用侧永久 park。
        while let Option::Some(attach) = self.shared.reg_.take_write_attach_() {
            let (_, _, mut reader_) = attach.into_parts_();
            reader_.close();
        }
        self.shared.reg_.close_write_inbox_();
    }
}

/// 解复用循环所持 telegraph 读表的**收尾守卫**。
///
/// 退出时**必须显式 `close()`** 每个接收环写端：`buffex` 的环半部被 drop 不会置位
/// 关闭标记，少了这一步，应用侧正 park 的 `recv_async` 永远醒不过来（与
/// [`DemuxLocalGuard_`] 对 channel 接收环的处理同一条理由）。
struct TgReadGuard_<'a, C>
where
    C: TrConnCfg,
{
    /// telegraph 读表：`local_dock` → 接收环写端（本循环持有）。
    table: TgReadTable_<crate::connection::ConnAllocCfg_<C>>,

    /// 生命周期占位：守卫借在解复用循环的 `shared` 上（与 `table` 同寿命）。
    _use_shared_: core::marker::PhantomData<&'a MuxShared_<C>>,
}

impl<C> core::ops::Deref for TgReadGuard_<'_, C>
where
    C: TrConnCfg,
{
    type Target = TgReadTable_<crate::connection::ConnAllocCfg_<C>>;

    fn deref(&self) -> &Self::Target {
        &self.table
    }
}

impl<C> core::ops::DerefMut for TgReadGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.table
    }
}

impl<C> Drop for TgReadGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        for entry in self.table.values_mut() {
            entry.close_();
        }
    }
}

/// 复用循环**写事件队列**的收尾守卫（规则与理由见 [`MuxLocalGuard_`]）。
struct MuxEventsGuard_<'a, C>
where
    C: TrConnCfg,
{
    /// 尚未处理的事件（里面可能还压着没进表的 `Attach` 半部）。
    events: EventReceiver_<WriteEvent_<crate::connection::ConnAllocCfg_<C>>>,

    /// 连接共享面（上报关闭与读时钟用）。
    shared: &'a MuxShared_<C>,
}

impl<C> core::ops::Deref for MuxEventsGuard_<'_, C>
where
    C: TrConnCfg,
{
    type Target = EventReceiver_<WriteEvent_<crate::connection::ConnAllocCfg_<C>>>;

    fn deref(&self) -> &Self::Target {
        &self.events
    }
}

impl<C> core::ops::DerefMut for MuxEventsGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.events
    }
}

impl<C> Drop for MuxEventsGuard_<'_, C>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        // 共享面保留给后续 telegraph 失败上报；本轮 `TgAttach` 只需关闭环半部。
        let _ = self.shared;
        loop {
            let event = match self.events.try_take_event_() {
                EventTake_::Event(event) => event,
                // Drop 阶段不再做全扫：其余半部由循环退出时的守卫统一关闭。
                EventTake_::Rescan | EventTake_::Empty | EventTake_::Closed => break,
            };
            // 通道 `Attach` 已经改走 inbox；事件里只剩 telegraph 的环半部需要显式关闭。
            if let WriteEvent_::TgAttach { mut reader_, .. } = event {
                reader_.close();
            }
        }
    }
}

/// 传输写半边的**收尾守卫**：本循环退出时封口（[`TrBuffTryFlush::close`]）。
///
/// # 为什么用守卫、而不是在循环末尾手写一次
///
/// 复用循环有多条退出路径（取消、事件轮询被取消、处理事件/发数据/finalize 出错……），
/// 它们分散在几十行里，靠"记得每处都补一次 close"极不可靠。守卫把这件事绑到**作用域
/// 退出**这一个事件上：无论走哪条路径，都会封口一次。
///
/// # 为什么用同步的 `close` 而不是 await `flush_async` + `close_async`
///
/// [`TrBuffTryFlush::close`] 的契约是"**不再有新数据**并唤醒下游排空方"；而"把已提交
/// 的字节真正交给设备"由**下游**自己完成（全被动环由调用方的排空泵、自驱动适配器由
/// 后台泵、按需适配器由下一次 `flush_async`）。因此退出时**不需要**在本循环里 await：
///
/// * 本循环已经把所有帧提交进传输；
/// * 封口之后，"排空 + 设备 `shutdown` + 等泵退出"是下游的事，由调用方或适配器承担。
///
/// 这也让守卫能安全地只依赖同步入口——`Drop` 里不能 await。
///
/// # 为什么不直接持有 `&'a mut W`
///
/// 循环体在几十处把 `&mut tx_stage` 交给辅助函数；守卫若长期借用它，那些调用点全部
/// 变成"二次可变借用"。守卫因此只存一个指向该栈上局部量的指针——它**只为 `Drop` 而
/// 存在**：构造后不再被读、写或移动，声明点在 `tx_stage` 之后，因此 `tx_stage` 一定活
/// 得比守卫久。
struct TxSealGuard_<W>
where
    W: TrBuffTryFlush<u8>,
{
    /// 指向被守卫的传输写半边（借用关系见类型文档末节的说明）。
    tx_: NonNull<W>,
}

impl<W> Drop for TxSealGuard_<W>
where
    W: TrBuffTryFlush<u8>,
{
    fn drop(&mut self) {
        // SAFETY: 构造点只由 `NonNull::from(&mut tx_stage)` 得到，其声明点在该局部量
        // 之后，且本守卫是栈上值：它一定在 `tx_stage` 之前析构。守卫只存指针、不派生
        // 任何别名；`Drop` 期间循环体对 `tx_stage` 的借用都已结束，因此这里 `as_mut`
        // 不会与任何活跃借用重叠。
        let tx = unsafe { self.tx_.as_mut() };
        // 先尽量把已提交的数据推出去（对全被动环与按需适配器都可能立刻有进展），
        // 再封口。两者的失败都不改变"必须封口"这件事，因此忽略返回值——
        // 真正的设备错误会经由传输自己的错误槽（`take_error`）交给调用方。
        let _ = tx.try_flush();
        let _ = tx.close();
    }
}

/// 复用循环：按对端发送窗口调度各子流的发送环，把成帧写进**传输的写半边**。
///
/// 本循环是这条写半边**唯一**的写入者，因此成帧的「头 + 载荷」两段之间不会被别的帧
/// 插进来；字节到网络的最后一跳由调用方那一侧负责（入向 / 出向泵或适配器的自驱动泵）。
/// 循环退出时经 [`TxSealGuard_`] 封口，从而唤醒那一侧的排空者。
/// 把公共 TX ring 里的 chunk 搬进 `ConnTx`（控制帧优先）。
///
/// 只搬 payload（`[u16 len]` 前缀已被消费），不重新组帧、不解析帧头。一旦开始一个
/// chunk 就把它搬完；TX ring 暂时读空时 park 在 `tx_ready` 上，等 `main_loop` 写入。
/// 返回 `true` 表示本轮至少搬了一段（供诊断）。
async fn flush_public_chunks_<C, K>(
    tx_stage: &mut C::ConnTx,
    shared: &MuxShared_<C>,
    public_tx_r: &mut BufferedRx,
    state: &mut ChunkReadState_,
    scratch: &mut [u8],
    cancel: &K,
) -> Result<bool, MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let mut moved = false;
    loop {
        if !state.has_chunk_() {
            match PublicChunkIo_::try_read_prefix_(public_tx_r, state, shared.max_packet_size_()) {
                Result::Ok(Option::Some(_len)) => {}
                Result::Ok(Option::None) => {
                    if moved {
                        shared.public_().notify_tx_space_();
                    }
                    return Result::Ok(moved);
                }
                Result::Err(_) => return Result::Err(MuxError::MalformedFrame),
            }
        }
        while state.left_() > 0 {
            let n = PublicChunkIo_::try_read_payload_into_(public_tx_r, state, scratch)
                .map_err(|_| MuxError::MalformedFrame)?;
            if n == 0 {
                if moved {
                    shared.public_().notify_tx_space_();
                }
                let wait_chunk = poll_fn(|cx| shared.public_().poll_tx_ready_(cx));
                if race_cancel_(cancel, wait_chunk).await.is_none() {
                    return Result::Err(MuxError::Cancelled);
                }
                continue;
            }
            enqueue_frame_(tx_stage, &scratch[..n], cancel.child_token()).await?;
            moved = true;
        }
        shared.public_().notify_tx_space_();
    }
}

pub(crate) async fn mux_loop_async_<C, K>(
    tx_stage: C::ConnTx,
    shared: MuxShared_<C>,
    events: EventReceiver_<WriteEvent_<crate::connection::ConnAllocCfg_<C>>>,
    read_events: EventSender_<ReadEvent_<crate::connection::ConnAllocCfg_<C>>>,
    mut public_tx_r: BufferedRx,
    cancel: K,
) where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    // 本循环直接持有**传输的写半边**（不再有连接级写环、也没有外侧写泵）。
    //
    // # 退出时怎么收尾：封口交给守卫，不依赖 drop
    //
    // 旧版这里只能"随作用域 drop"，于是对**调用方驱动的全被动环**（`buffex::ring` 的
    // `RingWriter`）不会唤醒 park 在环读端上的排空方——那一半的关闭权只属于持有者，
    // 而 `TrBuffWrite` 里没有"封口"这一步。
    //
    // 现在写半边必须实现 `TrBuffFlush`（见 `TrConnCfg::ConnTx`），本循环据此在退出时
    // **显式封口**：
    //
    // * 全被动环：置 `PRODUCER_CLOSED` 并唤醒调用方的排空泵（它负责把尾巴交给设备）；
    // * 按需适配器（`try_new`）：同样置位，唤醒/放行下游；
    // * 自驱动适配器（`try_new_local` / compio）：置位后其后台泵把剩余数据送完并
    //   `shutdown` 设备。
    //
    // 守卫在作用域**最后**析构，因此"封口"晚于本循环自己那些表 / 事件队列的关闭，
    // 不会与它们的唤醒语义打架。
    let mut tx_stage = tx_stage;

    // 传输写半边的收尾守卫（见 `TxSealGuard_` 的类型文档）。
    #[allow(dead_code, reason = "只为其 Drop 而存在；构造后不再被读写")]
    let _tx_seal: TxSealGuard_<C::ConnTx> = TxSealGuard_ {
        tx_: NonNull::from(&mut tx_stage),
    };

    // 表与事件队列都包进收尾守卫：**退出时显式关闭本循环持有的那些半部**。
    // 为什么必须这样做、以及为什么分成两个守卫，见 `MuxLocalGuard_` 的类型文档。
    let mut table: MuxLocalGuard_<'_, C> = MuxLocalGuard_ {
        table: crate::connection::dock_index_::DockBPlusTree_::new_in(lock_or_exit_!(
            shared.reg_.local_table_alloc_(cancel.child_token())
        ))
        .expect("2 KiB B+ 树足以容纳一个 channel 表项"),
        shared: &shared,
    };
    // telegraph 的本地写表**单独持有**（不进 `MuxLocalGuard_`）：主循环在同一轮里既要
    // 把 channel 表借给 `handle_write_event_`，又要排空 telegraph——两者若同处一个结构体
    // 就会在 `&mut table.table` 与 `&mut table.tg_table_` 上撞借用检查。它自己的收尾
    // 很简单（关掉每个发送环读端即可），因此不值得为对称再加一个守卫类型。
    let mut tg_table: TgWriteTable_<crate::connection::ConnAllocCfg_<C>> =
        crate::connection::dock_index_::DockBPlusTree_::new_in(lock_or_exit_!(
            shared.reg_.local_table_alloc_(cancel.child_token())
        ))
        .expect("2 KiB B+ 树足以容纳一个 telegraph 端点");
    let mut events: MuxEventsGuard_<'_, C> = MuxEventsGuard_ {
        events,
        shared: &shared,
    };
    let mut last_ready: Option<(Dock, Dock)> = Option::None;
    // 公共 TX 的 chunked 读进度。
    let mut public_read = ChunkReadState_::new_();
    // park 期间被合并提示唤醒时，把掩码带到这里、在下一轮顶部统一处理。
    let mut pending_hints = 0u64;
    // 环段字节的搬出暂存：`clone_items_from_buff` / `move_items_to_buff` 需要一个
    // 连续切片作为中间落点（段本身不能作为 `write_all_async_` 的源）。走连接级临时
    // 对象域，整条连接只分配一次。
    let scratch_retain = lock_or_exit_!(crate::connection::alloc_zeroed_bytes_async_(
        shared.reg_.scope_(),
        K_MAX_DATA_CHUNK,
    ));
    let mut scratch = scratch_retain
        .try_owning()
        .expect("刚分配的搬出暂存应当可独占");

    loop {
        #[cfg(feature = "test-loop-probe")]
        crate::connection::loop_probe_::mux_tick_();
        if cancel.is_cancelled() {
            return;
        }

        // 0. 先落实**会话释放**消息（`Drop` 只投消息、不碰身份表）。写循环也做这
        // 件事，是为了让释放不必等某次 API 操作：两个内侧循环任一被调度即可推进。
        lock_or_exit_!(shared
            .reg_
            .drain_session_events_(shared.conn_clock_().now_millis_(), cancel.child_token()));

        // 0.05 `Attach` inbox：环半部从这里进入本地写表（不再走事件通道）。
        while let Option::Some(attach) = shared.reg_().take_write_attach_() {
            handle_write_attach_::<C>(attach, &mut *table, &shared);
        }

        // 3. 公共子流优先：先把 TX ring 里的 chunk 搬进传输，再发用户数据。
        match race_cancel_(
            &cancel,
            flush_public_chunks_::<C, _>(
                &mut tx_stage,
                &shared,
                &mut public_tx_r,
                &mut public_read,
                &mut scratch[..],
                &cancel,
            ),
        )
        .await
        {
            Option::None => return,
            Option::Some(Result::Ok(_)) => {}
            Option::Some(Result::Err(err)) => {
                fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                return;
            }
        }

        // 0.1 合并提示：应用侧封口 / 关接收只置位 + 唤醒，不再逐条投事件。
        //
        // 位是**唤醒**而不是事实：事实在各子流状态字里，这里按表扫一次即可补齐。
        let mut hints = shared.reg_().take_write_hint_();
        hints |= core::mem::take(&mut pending_hints);
        // main 已把 `CLOSE(FIN)` 放进公共 TX ring；只有把它刷空之后才能确认上线。
        // 这里不 park：若还有未刷出的 chunk / 半个前缀，下一轮或 `HINT_W_FIN_ENQUEUED`
        // 再次到达时再确认。
        if hints & HINT_W_FIN_ENQUEUED != 0u64 {
            if public_tx_r.ring_state().data_size() == 0usize {
                match race_cancel_(
                    &cancel,
                    scan_fin_enqueued_::<C, _>(&shared, &mut *table, &read_events, &cancel),
                )
                .await
                {
                    Option::None => return,
                    Option::Some(Result::Ok(())) => {}
                    Option::Some(Result::Err(err)) => {
                        fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                        return;
                    }
                }
            } else {
                // FIN 还在公共 TX 里：留到下一轮刷空之后再确认。
                pending_hints |= HINT_W_FIN_ENQUEUED;
            }
        }
        if hints & HINT_W_RX_CLOSED != 0u64 {
            match race_cancel_(
                &cancel,
                flush_rx_closed_::<C, _>(
                    &mut tx_stage,
                    &shared,
                    &mut *table,
                    &read_events,
                    &cancel,
                ),
            )
            .await
            {
                Option::None => return,
                Option::Some(Result::Ok(())) => {}
                Option::Some(Result::Err(err)) => {
                    fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                    return;
                }
            }
        }
        if hints & HINT_W_PEER_CLOSED != 0u64 {
            match race_cancel_(
                &cancel,
                scan_peer_closed_::<C, _>(
                    &shared,
                    &mut *table,
                    &read_events,
                    &cancel,
                ),
            )
            .await
            {
                Option::None => return,
                Option::Some(Result::Ok(())) => {}
                Option::Some(Result::Err(err)) => {
                    fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                    return;
                }
            }
        }
        if hints & HINT_W_RELEASED != 0u64 {
            match race_cancel_(
                &cancel,
                scan_write_released_::<C, _>(
                    &shared,
                    &mut *table,
                    &read_events,
                    &cancel,
                ),
            )
            .await
            {
                Option::None => return,
                Option::Some(Result::Ok(())) => {}
                Option::Some(Result::Err(err)) => {
                    fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                    return;
                }
            }
        }

        // 1. 先把**已经到达**的事件成批处理掉（非阻塞），**但批有上限**。
        //
        // 这一步的顺序很关键：下面第 2 步会 park 在「最近通知过的那条发送环」上；
        // 若不在 park 之前排空事件队列，其它子流的 `Attach` / `Control`（例如它们
        // 的 `OPEN`）就会排在一条正在 park 的循环后面，形成死锁。单线程运行时下
        // 「检查队列」与「登记 park」之间没有 `await`，因此不存在竞态。
        //
        // **上限不可省。** 事件是持续到来的（每条子流每次写入都会投 `TxReady`），
        // 若「每处理一条事件就 `continue` 回顶部」，那么只要事件队列始终非空，第 2 步
        // 的排空**永远轮不到**——发送方一帧也发不出去。这不是理论风险：真实 socket 的
        // 流控验收用例正是卡在这里（对端已经把窗口抬到 384，事件也不断到达，而本循环
        // 一直在顶部打转、从没进过排空段）。因此每轮只处理有上限的一批，然后必定走一次
        // 排空。
        const K_EVENT_BATCH: usize = 32usize;
        for _ in 0..K_EVENT_BATCH {
            let event = match events.try_take_event_() {
                EventTake_::Event(event) => event,
                // 有可重建事件（`TxReady` / `TgReady`）被丢弃：直接进入本轮的排空段。
                // 写循环的排空本来就是**全表**扫描（`drain_once_` 与 2.7 段），因此
                // 不需要额外的状态位全扫——这一轮必定会把可写的数据取走。
                EventTake_::Rescan => break,
                EventTake_::Empty | EventTake_::Closed => break,
            };
            match race_cancel_(
                &cancel,
                handle_write_event_::<C, _>(
                        event,
                    &mut tx_stage,
                    &mut tg_table,
                    &shared,
                    &cancel,
                    &mut last_ready,
                ),
            )
            .await
            {
                Option::None => return,
                Option::Some(Result::Ok(())) => {}
                Option::Some(Result::Err(err)) => {
                    fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                    return;
                }
            }
        }

        // 2. 尽量把各子流的数据发出去（一次一段），直到没有可发的。
        loop {
            match race_cancel_(
                &cancel,
                drain_once_::<C, _>(
                        &mut tx_stage,
                    &shared,
                    &mut *table,
                    &mut scratch[..],
                    &cancel,
                ),
            )
            .await
            {
                Option::None => return,
                Option::Some(Result::Ok(true)) => continue,
                Option::Some(Result::Ok(false)) => break,
                Option::Some(Result::Err(err)) => {
                    fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                    return;
                }
            }
        }

        // 2.7. telegraph：把各端点已提交的报文送出去。
        //
        //     必须在**主循环**里做、而不是只在 `TgReady` 事件处理里做：`TgReady` 只是
        //     「可能有新提交」的提醒（每次提交一条），而「写环刚腾出空间」这条进展不会
        //     再产生任何事件——只靠事件驱动的实现在写环满过一次之后就再也发不出去了。
        //     这里每轮把所有端点的队列排空（每条一次尝试，失败即留到下一轮），与第 2 步
        //     对 channel 的处理同构。
        for (local, entry) in tg_table.iter_mut() {
            match race_cancel_(
                &cancel,
                flush_tg_endpoint_::<C, _>(&mut tx_stage, entry, *local, &shared, &cancel),
            )
            .await
            {
                Option::None => return,
                Option::Some(Result::Ok(_)) => {}
                Option::Some(Result::Err(err)) => {
                    fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                    return;
                }
            }
        }

        // 3. 没有可发数据：若「最近通知过的那条发送环」就是目标，就 park 在它上面
        //    （Q4 裁决的兜底：应用可能在提交之前就发出了事件）。
        //
        //    **必须同时轮询事件通道、传输写半边与取消令牌**：只 park 在子流环上会让
        //    「别的子流刚入队的事件」一直排在一个睡着的循环后面（多子流下就是死锁）；
        //    不轮询传输写半边则「腾出空间」这条唤醒会丢（下游排空方把字节搬走之后没有
        //    任何人通知复用循环，若这里不同时 park 在它上面，drain 就再也不会被触发）。
        let mut taken: Option<WriteEvent_<crate::connection::ConnAllocCfg_<C>>> = Option::None;
        // `Attach` 通道的接收 future：与事件通道一样跨 poll 持有。
        let mut attach_fut = core::pin::pin!(core::future::IntoFuture::into_future(
            shared.reg_().recv_write_attach_async_()
        ));
        let mut need_rescan = false;
        let mut ring_ready = false;
        let mut hint_ready = false;
        let mut public_ready = false;
        let mut taken_attach: Option<WriteAttach_<crate::connection::ConnAllocCfg_<C>>> = Option::None;
        let alive = {
            let cancel_fut = cancel.child_token().cancellation();
            let mut event_fut = core::pin::pin!(events.take_event_async_());
            let mut cancel_fut = core::pin::pin!(cancel_fut);
            // 仅当上一轮是**因为写环没空间**停下时，才把写环的就绪作为唤醒条件。
            // 写环有空间就说明可以重试 `drain_once_`（这一步不消费借出的段）。
            // 若「最近通知」的那条发送环存在，还要同时 park 在它上面（Q4 兜底）。
            poll_fn(|cx| {
                // 取消令牌优先：连接已收尾，直接退出。
                if core::future::Future::poll(cancel_fut.as_mut(), cx).is_ready() {
                    return Poll::Ready(false);
                }
                match core::future::Future::poll(event_fut.as_mut(), cx) {
                    Poll::Ready(EventTake_::Event(event)) => {
                        taken = Option::Some(event);
                        return Poll::Ready(true);
                    }
                    // 有可重建事件被丢弃：醒来做全扫（状态位都在各子流上）。
                    Poll::Ready(EventTake_::Rescan) => {
                        need_rescan = true;
                        return Poll::Ready(true);
                    }
                    // 所有生产者都没了：连接正在收尾，退出。
                    Poll::Ready(EventTake_::Closed) => {
                        return Poll::Ready(false);
                    }
                    // `take_event_async_` 不会返回 `Empty`；保守地当作「稍后再试」。
                    Poll::Ready(EventTake_::Empty) => {}
                    Poll::Pending => {}
                }
                // 子流发送环有新数据：回到顶部由 `drain_once_` 正式取走。
                //
                // 这条 park 是**可选**的（「最近通知」那条环可能已经不存在），因此
                // 就地建出 future、就地 poll：`Demand` 与借出的段都只活在这个分支里，
                // 不会把 `table` 的借用带出闭包。
                //
                // **但「环可读」不等于「有进展可能」**，判据有两处，缺一处就是纯空转：
                //
                // 1. **额度**：额度为 0 时 `drain_one_` 一条也发不出去，若这里照样因
                //    「环可读」返回就绪，整圈会在「回到顶部 → drain 失败 → 立刻又
                //    就绪」之间空转。因此就绪条件要求「这条子流确实还有发送额度」；
                //    额度归零之后，唤醒一律来自读循环收到窗口通告时投的那条事件（见
                //    `write_into_ring` 的对端通告分支）。这里用**非阻塞**的
                //    `send_available_try_`：窗口锁当场取不到就按「没额度」处理，不
                //    唤醒——那同样是安全方向。
                // 2. **真的借到了段**：环的读 future 在**生产端已关闭**（应用
                //    `drop(tx)` 之后环里已排空）、**消费端已关闭**、或需求不可满足时
                //    **也会立刻完成**，而那几种完成都不意味着 `drain_one_` 有活可干
                //    ——它 `try_read` 一样取不到段。只看 `is_ready()` 会把「环已关闭
                //    且空」误判成「环里有数据」，同样是 CPU 打满的空转；而且此时
                //    `continue` 之后每轮都会重新判一次「可读」，于是**永远转下去**，
                //    把同一条本地队列上的解复用循环、两个泵与应用任务全部饿死
                //    （跨进程表现为两端同时静止、被空闲超时兜底拆流）。因此就绪判据
                //    取「poll 出了**段**」（`contains_left`），只有错误一律按
                //    「无可搬运」处理。真机取证与因果链见
                //    `smux_v1_sock_demo/dev-notes/intermittent-stall-20261007-0200.md`。
                //
                //    判据整体抽成 [`last_ready_has_segment_`]，好让「环已关闭且空」
                //    这种形态能被单元用例直接钉住。
                if last_ready_has_segment_::<C>(&mut *table, last_ready, cx) {
                    ring_ready = true;
                    return Poll::Ready(true);
                }
                // 合并提示：应用侧封口 / 关接收只置位并唤醒，这里把掩码带回顶部处理。
                if let Poll::Ready(bits) = shared.reg_().poll_write_hint_(cx) {
                    pending_hints |= bits;
                    hint_ready = true;
                    return Poll::Ready(true);
                }
                // 公共 TX 有新的 chunk：回顶部优先搬运。
                if shared.public_().poll_tx_ready_(cx).is_ready() {
                    public_ready = true;
                    return Poll::Ready(true);
                }
                // `Attach` 通道：有新的环半部要交给本地写表。
                match core::future::Future::poll(attach_fut.as_mut(), cx) {
                    Poll::Ready(Result::Ok(attach)) => {
                        taken_attach = Option::Some(attach);
                        return Poll::Ready(true);
                    }
                    // 写侧通道关闭（连接收尾）。
                    Poll::Ready(Result::Err(_)) => return Poll::Ready(false),
                    Poll::Pending => {}
                }
                Poll::Pending
            })
            .await
        };
        if !alive {
            return;
        }
        if need_rescan {
            // 有可重建事件（`TxReady` / `TgReady`）被丢弃：回顶部，本轮排空段本来
            // 就是全表扫描，足以把可写数据取走。
            continue;
        }
        if let Option::Some(attach) = taken_attach {
            handle_write_attach_::<C>(attach, &mut *table, &shared);
            continue;
        }
        if let Option::Some(event) = taken {
            match race_cancel_(
                &cancel,
                handle_write_event_::<C, _>(
                        event,
                    &mut tx_stage,
                    &mut tg_table,
                    &shared,
                    &cancel,
                    &mut last_ready,
                ),
            )
            .await
            {
                Option::None => return,
                Option::Some(Result::Ok(())) => {}
                Option::Some(Result::Err(err)) => {
                    fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                    return;
                }
            }
            continue;
        }
        if ring_ready {
            // 借出的段在 future 里被 drop（不消费）：回顶部由 `drain_once_` 正式取走。
            continue;
        }
        if hint_ready {
            // 收尾事实变了（排空/发 FIN/RESET）：回顶部按位扫描，不清 `last_ready`
            // ——那条环仍是下一轮值得 park 的目标。
            continue;
        }
        if public_ready {
            // 公共 TX 有新 chunk：回顶部优先搬运。
            continue;
        }
        last_ready = Option::None;
        continue;
    }
}

/// 处理一条写事件；返回 `Err` 表示连接级失败（调用方负责 `mark_failed_` 并退出）。
///
/// 只做**事件自身**的动作（写控制帧、置 `last_ready`、telegraph 收支）：本地表的
/// 扫描收尾已改为提示位驱动（`HINT_W_*`），因此这里不再借用写表。
async fn handle_write_event_<C, K>(
    event: WriteEvent_<crate::connection::ConnAllocCfg_<C>>,
    tx_stage: &mut C::ConnTx,
    tg_table: &mut TgWriteTable_<crate::connection::ConnAllocCfg_<C>>,
    shared: &MuxShared_<C>,
    cancel: &K,
    last_ready: &mut Option<(Dock, Dock)>,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    match event {
        WriteEvent_::Control { frame_ } => {
            // 控制帧也必须整帧进入写环；装不下时先 park 在写环上，再重试同一帧。
            write_control_blocking_::<C, _>(
                tx_stage,
                shared,
                shared.max_packet_size_,
                &frame_,
                cancel,
            )
                .await?;
        }
        WriteEvent_::TxReady {
            local_dock,
            remote_dock,
        } => {
            *last_ready = Option::Some((local_dock, remote_dock));
        }
        // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
        // telegraph（数据报）
        // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
        WriteEvent_::TgAttach {
            local_dock,
            owner,
            reader_,
        } => {
            tg_table.insert(
                local_dock,
                TgWriteEntry_ {
                    owner_: owner,
                    reader_,
                },
            );
        }
        WriteEvent_::TgReady { local_dock } => {
            // 只对**已在本地表**里的端点动手：`TgAttach` 可能还在队列后面（同一轮的
            // 事件顺序由应用侧保证不了），真正的排空在主循环第 2.7 步兜底。
            let Some(entry) = tg_table.get_mut(local_dock).map(|(_, value)| value) else {
                return Result::Ok(());
            };
            let _ = race_cancel_(
                cancel,
                flush_tg_endpoint_::<C, _>(tx_stage, entry, local_dock, shared, cancel),
            )
            .await;
        }
        WriteEvent_::TgTxClosed { local_dock } => {
            // 应用丢掉了发送半边：摘掉表项，并把身份节点里尚未送出的长度记录一并丢弃
            // （环里那些字节已经没有提交者，留着只会让队列永远非空）。身份本身何时
            // 释放由两个半边的守卫共同决定，与这里无关。
            if let Option::Some(entry) = tg_table.remove(local_dock) {
                entry.owner_.out_().clear_();
            }
        }
    }
    Result::Ok(())
}

/// 把一条 telegraph 发送端点里**已提交**的报文逐条送出。
///
/// # 循环推进的判据
///
/// 报文长度由身份节点内联的发送队列给出（FIFO）。每次尝试发送**一条**：
///
/// - 环里暂时没有足够字节（应用写了但还没提交完）或没有长度记录 → 本轮结束；
/// - 传输写半边满 → 由 [`write_frame_to_stage_`] 返回「未写出」，本轮结束、数据留在
///   发送环里，等主循环下一轮（写环腾出空间后会再来）。
///
/// 返回本轮成功送出的**报文条数**（仅供诊断 / 测试断言）。
async fn flush_tg_endpoint_<C, K>(
    tx_stage: &mut C::ConnTx,
    entry: &mut TgWriteEntry_<crate::connection::ConnAllocCfg_<C>>,
    local: Dock,
    shared: &MuxShared_<C>,
    cancel: &K,
) -> Result<usize, MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let mut sent = 0usize;
    // 用 `loop` 而非 `while let`：循环体里有多条**带副作用的提前退出**（丢弃一条卡住
    // 的长度记录后继续下一条），`while let` 的绑定会把「队头长度」钉在整个循环体上，
    // 而循环体里恰恰要把它出队改写。
    #[allow(clippy::while_let_loop)]
    loop {
        // 队列空 → 没有待发报文。
        let Some(_) = entry.owner_.out_().peek_len_() else {
            break;
        };
        match race_cancel_(
            cancel,
            send_one_datagram_::<C>(tx_stage, entry, local, shared),
        )
        .await
        {
            Option::None => return Result::Err(MuxError::Cancelled),
            // 送出了一条：继续试下一条。
            Option::Some(Result::Ok(true)) => {
                sent += 1usize;
                continue;
            }
            // 写环满 / 环里数据还没到齐：留到下一轮。
            Option::Some(Result::Ok(false)) => break,
            // 环被关闭（发送半边已丢弃 / 连接收尾）：丢弃这一条长度记录，继续下一条
            // ——否则它会永远卡在队头，后面的报文一条也发不出去。
            Option::Some(Result::Err(_)) => {
                let _ = entry.owner_.out_().try_pop_();
                continue;
            }
        }
    }
    Result::Ok(sent)
}

/// 尝试送出一条数据报。
///
/// 返回 `Ok(true)` 表示**确实送出了一条**（长度记录已出队、帧已进传输写半边）；
/// `Ok(false)` 表示本轮条件不满足（写环没空间，或环里还没有那么长的数据），调用方应
/// 留到下一轮重试——**长度记录不会出队**。
///
/// # Errors
///
/// 发送环已关闭（应用丢了发送半边）或读段失败时返回错误，调用方据此丢弃该长度记录。
async fn send_one_datagram_<C>(
    tx_stage: &mut C::ConnTx,
    entry: &mut TgWriteEntry_<crate::connection::ConnAllocCfg_<C>>,
    local: Dock,
    shared: &MuxShared_<C>,
) -> Result<bool, MuxError>
where
    C: TrConnCfg,
{
    // 1. 队头条目 `(目的地址, 长度)`（不出队）。目的地址是**逐次发送**的实参。
    let Some((remote, payload_len)) = entry.owner_.out_().peek_len_() else {
        return Result::Ok(false);
    };
    // 2. 传输写半边至少要放得下帧头——否则不必去动发送环（避免「读了却写不出去」时
    //    还要处理回退）。
    let Some(space) = ring_space_(tx_stage) else {
        return Result::Ok(false);
    };
    if space < K_MAX_FRAME_HEADER {
        return Result::Ok(false);
    }
    // 3. 应用写进环里的数据够不够这一条？不够说明它还没提交完（或已经丢弃发送半
    //    边）——两者都按「本轮不推进」处理，由下一轮或 `TgTxClosed` 收尾。注意这里
    //    只做**非阻塞**探测：数据报是尽力交付，主循环不能被一条尚未提交完的报文挡住。
    let available = entry.reader_.ring_state().data_size();
    if available < payload_len {
        return Result::Ok(false);
    }
    // 4. 编码并写出。目的地址来自本次发送的实参（见 `peek_len_` 取出的队头条目）。
    let header = FrameHeader::new_(
        FrameKind::Datagram,
        0u8,
        local,
        remote,
        payload_len,
        Option::None,
    );
    let (head, head_len) = encode_header_(&header)?;
    if head_len + payload_len > shared.max_packet_size_ {
        // 单条报文超过协商的帧总长上限。这是**应用侧**的问题（它提交了一条连一帧都装
        // 不下的数据报），**不是连接级故障**：整条丢弃并把长度记录出队，让后面的报文
        // 照常发出——绝不能把整条连接判失败，也不能把它留在队头（那会把后面的报文全
        // 堵死）。
        //
        // 丢弃与接收侧的「装不下就整条丢弃」同源（都是尽力交付），因此计入同一个计数器。
        let _ = entry.owner_.out_().try_pop_();
        shared.metrics_().on_datagram_dropped(
            local,
            remote,
            u32::try_from(payload_len).unwrap_or(u32::MAX),
        );
        return Result::Ok(false);
    }
    write_frame_to_stage_::<C>(tx_stage, entry, &head[..head_len], payload_len).await?;
    // 整帧已进传输写半边：长度记录出队（这一步之后才允许下一轮读下一条）。
    let popped = entry.owner_.out_().try_pop_();
    debug_assert_eq!(
        popped,
        Option::Some((remote, payload_len)),
        "队头条目不应在发送期间变化"
    );
    // 上报：帧总长与 channel 侧同口径（帧头 + 载荷）。数据报**不**上报子流级别的
    // 统计（它没有身份寿命可言）。
    shared.metrics_().on_frame(
        FrameDir::Send,
        local,
        remote,
        FrameKind::Datagram,
        u32::try_from(head_len + payload_len).unwrap_or(u32::MAX),
    );
    shared.metrics_().on_transport_bytes(
        FrameDir::Send,
        u64::try_from(head_len + payload_len).unwrap_or(u64::MAX),
    );
    Result::Ok(true)
}

/// 把**帧头 + 载荷**写进传输写半边，载荷直接从 telegraph 发送环里搬出。
///
/// # 为什么要求「整帧放得下」才动手
///
/// 帧一旦写出**帧头**就无法回退；而载荷可能分多段。若中途写环满，写环里就会留下一条
/// 「头已写、载荷不全」的残帧，对端解析必然错位。因此这里要求传输写半边当前至少有
/// `head_len + payload_len` 的可写空间才动手——**否则整帧推迟到下一轮**（调用方把长度
/// 记录留在队列里，不消费发送环）。
///
/// 这条要求在实践中很容易满足：telegraph 的单条载荷上限是 `max_packet_size` 量级
/// （默认 4 KiB），而传输写半边容量是 [`K_STAGE_RING_CAPACITY`]（64 KiB）。它是数据报
/// 「尽力交付、不背流控」的直接推论：宁愿推迟一条，也不在写环里留残帧。
///
/// # 零拷贝
///
/// 载荷从发送环的段直接搬进写环的段（`move_items_to_segm`），中间不经过任何暂存。
///
/// # Errors
///
/// 写环放不下整帧 / 源环被关闭 / 段借用失败时返回错误；调用方据此把该条留到下一轮。
async fn write_frame_to_stage_<C>(
    tx_stage: &mut C::ConnTx,
    entry: &mut TgWriteEntry_<crate::connection::ConnAllocCfg_<C>>,
    head: &[u8],
    payload_len: usize,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
{
    let total = head.len() + payload_len;
    if ring_space_(tx_stage).unwrap_or(0usize) < total {
        // 写环暂时放不下整帧：整帧推迟（长度记录留在队列里，发送环也不动）。
        return Result::Err(MuxError::Closed);
    }

    // **先把载荷段借到手，再往写环里写第一个字节**：帧头一旦写出就无法回退，因此一切
    // 可能失败的借段都必须发生在写之前。借出的段在函数末尾 drop，按**实际搬出量**提交
    // 消费；提前返回时消费量为 0，发送环不受影响。
    let read_demand = Demand::at_least(payload_len);
    let mut read_outcome = entry.reader_.try_read(&read_demand);
    let mut payload_segm = if payload_len == 0usize {
        Option::None
    } else {
        match read_outcome.as_mut().pick_left() {
            Option::Some(segm) => Option::Some(segm),
            Option::None => return Result::Err(MuxError::Closed),
        }
    };

    // 写环的**总**空间已判够，且本循环是它唯一的写入者：下面两个段级循环因此一定能推
    // 到底。跨**物理片**由循环承担——这正是「整帧原子性」的落点：`try_write` 每次只给
    // 当前那一片，写满就换下一片，绝不会把「头已写、载荷没跟上」的残帧留在环里。
    let mut head_left = head;
    while !head_left.is_empty() {
        let demand = Demand::at_least(1usize);
        let mut outcome = tx_stage.try_write(&demand);
        let writable = match outcome.as_mut().pick_left() {
            Option::Some(segm) => segm,
            Option::None => return Result::Err(MuxError::Closed),
        };
        let put = writable.as_segm_mut().clone_items_from_buff(head_left);
        if put == 0usize {
            return Result::Err(MuxError::Closed);
        }
        head_left = &head_left[put..];
    }

    let mut left = payload_len;
    while left > 0usize {
        let demand = Demand::at_least(1usize);
        let mut outcome = tx_stage.try_write(&demand);
        let writable = match outcome.as_mut().pick_left() {
            Option::Some(segm) => segm,
            Option::None => return Result::Err(MuxError::Closed),
        };
        let moved = {
            let mut dst = writable.as_segm_mut();
            let mut src = match payload_segm.as_mut() {
                Option::Some(segm) => segm.as_segm_ref(),
                Option::None => return Result::Err(MuxError::Closed),
            };
            // **按本条剩余长度精确裁剪源段**：`move_items_to_segm` 是整段搬运，不裁剪
            // 会把后续报文的载荷一起吃进这一帧（旧实现撞过的坑）。
            let ask = Demand::exactly(left);
            let Option::Some(mut piece) = src.take_segm_ref(&ask) else {
                return Result::Err(MuxError::Closed);
            };
            dst.move_items_from_segm(&mut piece)
        };
        if moved == 0usize {
            return Result::Err(MuxError::Closed);
        }
        left -= core::cmp::min(left, moved);
    }

    // 写段与读段在这里 drop：各自提交指针、唤醒对端。
    Result::Ok(())
}

/// 两个方向都在**协议层**收尾时释放注册表身份与接收环。
///
/// 「两个方向都收尾」的判据由 [`ChannelState_::claim_release_`] 在一次 CAS 里完成
/// （见 `dev-notes/outlook-concurrency…` §12 T1）：发送方向要等 `FIN` 真的发出
/// （即发送环已排空），接收方向要等应用丢半边或对端 `FIN`。**应用丢弃发送半边不算
/// 完成**——那只是「不再写」的意图。
async fn maybe_release_<C, K>(
    shared: &MuxShared_<C>,
    // 释放提示已改为 `HINT_R_RELEASE` 合并提示；该参数保留到事件通道整体清理时删除。
    _read_events: &EventSender_<ReadEvent_<crate::connection::ConnAllocCfg_<C>>>,
    table: &WriteTable_<crate::connection::ConnAllocCfg_<C>>,
    pair: (Dock, Dock),
    cancel: K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    // 找到 owner：表里可能已经移除，因此用注册表兜底（异步取锁、可取消）。
    let owner = match table.get(pair).map(|(_, entry)| entry.owner_.clone()) {
        Option::Some(owner) => Option::Some(owner),
        Option::None => {
            lock_or_fail_!(shared.reg_.channel_owner_(pair.0, pair.1, cancel.child_token()))
        }
    };
    let Some(owner) = owner else {
        return Result::Ok(());
    };
    if owner.claim_release_() {
        let now_millis = shared.conn_clock_().now_millis_();
        // 返回值：「本次释放之后连接上是否已经没有任何活跃子流」。
        let became_idle = matches!(
            shared
                .reg_
                .release_channel_(pair.0, pair.1, now_millis, cancel.child_token())
                .await,
            Result::Ok(true)
        );
        // 身份已释放：读循环据此摘本地读表并关接收环写端。事实是 `RELEASED` 位，
        // 提示只负责唤醒。
        shared.reg_().raise_read_hint_(HINT_R_RELEASE);
        // 两个方向都不会再有新的进展，等待中的 `TrBuffFlush` / `close_async` 必须醒来
        // 并**按终局复检**（否则它们会永远 park 在一条已经从注册表消失的子流上）。
        // 放在释放提示之后：先让读循环有机会摘表项，再叫醒应用——顺序反了只是多一次
        // 无谓唤醒，不会有正确性问题。
        owner.notify_both_progress_();
        // 上报子流关闭（正常协议收尾）。`claim_release_` 是一次性的 CAS，因此这里
        // **恰好上报一次**；超时与建流超时两条路径不经过本函数，各自在 `timer_` 里上报。
        shared.metrics_().on_channel_closed(
            pair.0,
            pair.1,
            ChannelCloseReason::Fin,
            now_millis.saturating_sub(owner.created_millis_()),
        );

        // **优雅关闭的收口点**：连接处于等待关闭，而这是最后一条活跃子流——它与对端
        // 双向收尾（本端 `FIN` 已发、对端 `FIN` 已收）意味着连接上再也没有需要排空的
        // 东西，此刻就收尾。
        //
        // 收尾幂等（取消令牌是幂等操作），计时循环在它自己的扫描里也可能同时发现
        // 「可以收尾」——两边谁先看到谁做，不会重复做什么坏事。放在这里而不是全交给
        // 计时循环，是因为延迟敏感：计时循环可能正睡在一个较远的期限上。
        if became_idle && shared.reg_().is_closing_() {
            // 本版没有写泵：复用循环成帧时直接写进传输，因此收尾就是「三个循环一起
            // 取消」——连接内部没有任何还压着字节的环要排空（见
            // `ChannelRegistry_::cancel_loops_` 的文档）。
            shared.reg_().cancel_loops_();
        }
    }
    Result::Ok(())
}

/// 发一条 `CLOSE`。用独立的 helper 以便在事件处理里直接 await。
async fn control_close_via_<C, K>(
    tx_stage: &mut C::ConnTx,
    shared: &MuxShared_<C>,
    max_packet_size: usize,
    local_dock: Dock,
    remote_dock: Dock,
    reset: bool,
    cancel: K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let frame = ControlFrame_::plain_(
        FrameKind::Close,
        if reset { flags::K_RESET } else { flags::K_FIN },
        local_dock,
        remote_dock,
    );
    write_control_blocking_::<C, _>(tx_stage, shared, max_packet_size, &frame, &cancel).await
}

/// 把控制帧写进传输写半边（分块；空间不足时 park 等下游排空方腾出空间）。
async fn write_control_blocking_<C, K>(
    tx_stage: &mut C::ConnTx,
    shared: &MuxShared_<C>,
    max_packet_size: usize,
    frame: &ControlFrame_,
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    // 这条路径只写**无载荷**控制帧（CLOSE / WINDOW_UPDATE 等）；`OPEN` / `ACCEPT` /
    // `REJECT` 的载荷由 `main_loop` 直接交给公共 TX，不经过事件通道。
    let payload: &[u8] = &[];
    let header = FrameHeader::new_(
        frame.kind_(),
        frame.flags_(),
        frame.local_dock_(),
        frame.remote_dock_(),
        payload.len(),
        frame.window_(),
    );
    // 帧头进栈上定长缓冲；帧总长上限的判定与「拼成整帧」时同义。
    let (head, head_len) = encode_header_(&header)?;
    if head_len + payload.len() > max_packet_size {
        return Result::Err(MuxError::FrameTooLarge);
    }
    enqueue_frame_parts_::<_, _>(tx_stage, &head[..head_len], payload, cancel.child_token()).await?;
    // 上报「写出一个帧」：控制帧在这里才真正**整帧**进了写环（`enqueue_frame_parts_`
    // 把头与载荷分两次入环，因此计数点必须在它之后）。
    shared.metrics_().on_frame(
        FrameDir::Send,
        frame.local_dock_(),
        frame.remote_dock_(),
        frame.kind_(),
        u32::try_from(head_len + payload.len()).unwrap_or(u32::MAX),
    );
    shared.metrics_().on_transport_bytes(
        FrameDir::Send,
        u64::try_from(head_len + payload.len()).unwrap_or(u64::MAX),
    );
    Result::Ok(())
}

/// 轮转一遍本地表，最多写出一段数据；返回是否有进展。
#[allow(clippy::too_many_arguments)]
async fn drain_once_<C, K>(
    tx_stage: &mut C::ConnTx,
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
    scratch: &mut [u8],
    cancel: &K,
) -> Result<bool, MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    // 逐个 dock 对轮转。这里**不能**先把键收集成 `Vec`：那会在每次轮转时引入一次
    // 堆分配，而热路径上只该有环与网络的内存动作。改为每轮用「上一个键之后的第一个
    // 键」推进（`range` 的 O(log n)，整轮 O(n log n)），既不分配，也不会在
    // `await` 期间持有对表的借用。
    let mut cursor: Option<(Dock, Dock)> = Option::None;
    loop {
        let next = match cursor {
            Option::None => table.keys().next().copied(),
            Option::Some(last) => table
                .range((Bound::Excluded(last), Bound::Unbounded))
                .next()
                .map(|(key, _)| *key),
        };
        let Option::Some(pair) = next else {
            return Result::Ok(false);
        };
        cursor = Option::Some(pair);
        if drain_one_::<C, _>(
            tx_stage,
            shared,
            table,
            scratch,
            pair,
            cancel,
        )
        .await?
        {
            return Result::Ok(true);
        }
        // 发送方向已封口、还没进入 FIN 流程：确认环是否已经排空。
        // 只有「明确排空」才置 `TX_DRAINED` 并叫醒 main 组 `CLOSE(FIN)`；环里还有
        // 数据 / 不可判定时留给下一轮。main 写进公共 TX ring 后由
        // `scan_fin_enqueued_` 在刷空时确认上线。
        let eligible = table
            .get(pair)
            .map(|(_, entry)| {
                entry.owner_.is_app_tx_closed_()
                    && !entry.owner_.is_local_fin_sent_()
                    && !entry.owner_.is_fin_enqueued_()
                    && !entry.owner_.is_tx_drained_()
            })
            .unwrap_or(false);
        if eligible && matches!(has_writable_::<C>(table, pair), Option::Some(false)) {
            let first = table
                .get(pair)
                .map(|(_, entry)| entry.owner_.set_tx_drained_())
                .unwrap_or(false);
            if first {
                shared
                    .control_()
                    .raise_main_hint_(crate::connection::HINT_MAIN_TX_DRAINED);
            }
            return Result::Ok(false);
        }
    }
}

/// 尝试为 `pair` 写出一段数据；返回是否写出。
#[allow(clippy::too_many_arguments)]
async fn drain_one_<C, K>(
    tx_stage: &mut C::ConnTx,
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<crate::connection::ConnAllocCfg_<C>>,
    scratch: &mut [u8],
    pair: (Dock, Dock),
    cancel: &K,
) -> Result<bool, MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    let token = cancel.child_token();
    let Some((_, entry)) = table.get_mut(pair) else {
        return Result::Ok(false);
    };
    // 本端 `FIN` 已发出：这条子流不再发送数据，表项保留只为等对端 `CLOSE`
    // （见 `scan_peer_closed_`）。热点排空循环直接跳过它，避免每轮对已关闭的环
    // 做一次 `try_read`。
    if entry.fin_sent_ {
        return Result::Ok(false);
    }

    // 清「已入队」位：此后新的写入会重新入队（顺序不可反，见 dev-notes §11.4）。
    let owner = entry.owner_.clone();
    // 去重位在锁外：同步清位，不取锁。
    owner.clear_tx_queued_();

    let available = owner.send_available_();
    if available == 0 {
        return Result::Ok(false);
    }
    // **每轮最多搬「一个帧装得下的量」**：帧总长必须 ≤ 协商的 `max_packet_size`，而帧头
    // 最长 `K_MAX_FRAME_HEADER`。这一收敛是必须的，不是优化：应用完全可能一口气把发送环
    // 写满（一次积压远超一个帧），下面的帧总长检查只是**兜底**，不该成为「应用一次写多了」
    // 的出口——少了这一收敛，一次正常的大写入会被判 `FrameTooLarge`、整条连接失败，而
    // 连接级失败不会唤醒在册子流，应用侧看到的是**挂死**（0 CPU）。
    // 回归用例：`tests/inmem_mux.rs` 的 `mux_frame_cap_burst_dual_`；
    // 因果与真机现场见 `dev-notes/frame-cap-20261007-0540.md`。
    let payload_cap = shared.max_packet_size_.saturating_sub(K_MAX_FRAME_HEADER);
    if payload_cap == 0usize {
        // 协商出的帧总长连一个帧头都装不下：这是真正的非法协商，按协议错误处理。
        return Result::Err(MuxError::FrameTooLarge);
    }
    let want = core::cmp::min(available as usize, K_MAX_DATA_CHUNK.min(payload_cap));

    // 借一段数据。**必须用非阻塞的 `try_read`**：`read_async` 在空环上会 park，
    // 而这里一旦 park 就再也看不到事件通道里的事件（多子流下直接死锁）。等待新数据
    // 是循环顶部「park 在最近通知的环上、同时与事件通道竞争」那一步的职责。
    let demand = Demand::no_more_than(want);
    let mut outcome = entry.reader_.try_read(&demand);
    let segm = match outcome.as_mut().pick_left() {
        Option::Some(segm) => segm,
        Option::None => {
            return Result::Ok(false);
        }
    };
    let take = segm.least_count();
    if take == 0 {
        return Result::Ok(false);
    }
    if take > scratch.len() {
        // `no_more_than(want)` 保证段长不超过暂存（`want <= K_MAX_DATA_CHUNK`）；
        // 走到这里说明上游的 demand 语义变了。
        return Result::Err(MuxError::MalformedFrame);
    }

    let header = FrameHeader::new_(
        FrameKind::Data,
        0u8,
        pair.0,
        pair.1,
        take,
        Option::None,
    );

    // 帧总长必须先满足协商上限（头是自描述字段，取上界）。
    if take + K_MAX_FRAME_HEADER > shared.max_packet_size_ {
        return Result::Err(MuxError::FrameTooLarge);
    }
    // 传输写半边还活着吗？对端（读端）消失说明它已经不可用。
    if ring_space_(tx_stage).is_none() {
        return Result::Err(MuxError::Transport { write: true });
    }

    // 把段的字节**搬出**到循环暂存里：`move_items_to_buff` 会推进段的已消费量，
    // 段的 drop 才会把消费提交回环。**不能用 `iter_slices()` 只读不消费**——那样
    // 环的读指针不前进，同一段数据会被反复取出、反复上线。
    //
    // **必须搬整个逻辑读段，不能先 `as_segm_ref()` 再搬一个子段。**
    // `take`（= `least_count()`）是**逻辑**长度：读指针靠近环末端时，这一段在物理上
    // 是**两段**（环尾 + 环首）。`as_segm_ref()` 只给出「当前物理段」，于是只搬一次
    // 会少搬 `take − 首段长` 个字节，下面的一致性检查便会把一条**正常**的数据帧判成
    // `MalformedFrame`，进而 `fail_mux_loop_` 终止整条连接——发送方此后一帧也不再
    // 搬出（本仓库流控验收用例的「第二轮窗口归零后停摆」正是死在这里）。
    // `ReclSliceRef::move_items_to_buff` 会自己按物理段循环，直到搬够 `take`。
    let moved = {
        let dst = &mut scratch[..take];
        // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同（同尺寸、同对齐、无 niche）；
        // `dst` 是本循环独占的可写区间，`move_items_to_buff` 只写入其中已初始化的
        // 前缀并返回写入长度，因此既不会读到未初始化内存，也不会越界。
        let uninit = unsafe {
            core::slice::from_raw_parts_mut(
                dst.as_mut_ptr() as *mut MaybeUninit<u8>,
                dst.len(),
            )
        };
        unsafe { segm.move_items_to_buff(uninit) }
    };
    if moved != take {
        // 段长度与搬出量应当一致；不一致说明上游语义变了。
        return Result::Err(MuxError::MalformedFrame);
    }

    // 帧头进**栈上定长缓冲**（零分配），并用它的**实际长度**做帧总长判定。
    let (head, head_len) = encode_header_(&header)?;
    if head_len + moved > shared.max_packet_size_ {
        return Result::Err(MuxError::FrameTooLarge);
    }

    // 预扣窗口（`take <= available`，因此必定足额）。
    let granted = owner.send_reserve_(take as Credit);
    if granted < take as Credit {
        owner.send_refund_(granted);
        return Result::Ok(false);
    }

    // **两段**写进传输写半边：先帧头（栈上缓冲）、再载荷（`scratch`）。写环是单生产者，
    // 因此两段之间不会被别的帧插进来。空间在上面已经判过（保守预判 + 实际头长），
    // 因此这里**不会再**出现「字节已离开子流环、帧却没写出去」的状态。
    if let Result::Err(err) = enqueue_frame_parts_::<_, _>(
        tx_stage,
        &head[..head_len],
        &scratch[..moved],
        token.child_token(),
    )
    .await
    {
        // 写环已结束：把预扣的窗口退回去，再按连接级失败处理。
        owner.send_refund_(take as Credit);
        return Result::Err(err);
    }
    owner.mark_data_(shared.conn_clock_().now_millis_());
    // 这一段已交给传输：发送环可能刚好被取空，等待中的 `flush_async` 由此醒来复检
    // 「环已排空」。每次取走都通知是有意的——排空是**渐进**发生的，没有别的时刻能
    // 精确覆盖「最后一段离开环」那一瞬。
    owner.notify_tx_progress_();
    // 上报「写出一个数据帧」：帧总长 = 帧头的**实际**长度 + 载荷长度。计数点必须在
    // `enqueue_frame_parts_` **之后**——它把头与载荷分两次入环，在那之前计数会把一个
    // 帧算成半个（见该函数的说明）。
    shared.metrics_().on_frame(
        FrameDir::Send,
        pair.0,
        pair.1,
        FrameKind::Data,
        u32::try_from(head_len + moved).unwrap_or(u32::MAX),
    );
    shared.metrics_().on_transport_bytes(
        FrameDir::Send,
        u64::try_from(head_len + moved).unwrap_or(u64::MAX),
    );
    Result::Ok(true)
}

#[cfg(test)]
mod tests_ {
    use core::task::{Context, Waker};

    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use crate::{
        connection::{
            Dock, SingleConnAllocCfg,
            owner_::new_owner_,
            ring_::test_support_::make_test_channel_,
            test_support_::TestMuxConfig_,
        },
        flow_ctrl::{DefaultPolicy, WindowReport},
    };

    use super::*;

    /// 造一份测试用的写侧表项：发送窗口已安装并有 `credit` 字节额度、发送环取自测试环。
    ///
    /// `ring` 是那条子流**发送环的读端**（复用循环持有的那一端），由调用方决定它的
    /// 状态（空 / 有数据 / 生产端已关闭）。
    fn make_table_(
        ring: BufferedRx,
        credit: Credit,
    ) -> WriteTable_<SingleConnAllocCfg> {
        let owner = new_owner_(CoreAlloc);
        owner.install_(&DefaultPolicy, 64usize);
        // 通告一条「累计已收 0、窗口 `credit`」的快照：发送窗口因此有 `credit` 额度。
        owner
            .send_on_report_(WindowReport::new(0u64, credit))
            .expect("测试额度远小于上限，通告应当被接受");
        let mut table = crate::connection::dock_index_::DockBPlusTree_::new_in(CoreAlloc)
            .expect("2 KiB B+ 树足以容纳测试表项");
        table.insert(
            (Dock::new(0x7501u32), Dock::new(0x7502u32)),
            WriteEntry_ {
                owner_: owner,
                reader_: ring,
                peer_fin_handled_: false,
                fin_sent_: false,
            },
        );
        table
    }

    /// 往环的写端提交 `bytes`（借段、写入、drop 提交）。
    fn fill_ring_(tx: &mut BufferedTx, bytes: &[u8]) {
        let demand = Demand::at_least(1usize);
        let mut outcome = tx.try_write(&demand);
        let segm = outcome
            .as_mut()
            .pick_left()
            .expect("空环上借写段应当成功");
        let mut child = segm.as_segm_mut();
        let put = child.clone_items_from_buff(bytes);
        assert_eq!(put, bytes.len(), "测试环容量应当装得下这一段");
        drop(child);
        drop(outcome);
    }

    /// 测试目标：复用循环 park 的「最近通知过的那条发送环」就绪判据，**只认真的借到了段**。
    ///
    /// - 背景：环的读 future 在**生产端已关闭且已排空**时也会立刻完成（返回
    ///   `Closing`）。若就绪判据只看 `is_ready()`，复用循环就会把「环已关闭且空」
    ///   误判成「环里有数据」，在「回顶部 → `drain_one_` 取不到段 → 立刻又就绪」之间
    ///   空转（CPU 打满、同一条本地队列上的其它任务全被饿死）。
    /// - 手段：用测试环与一份「有额度」的共享状态装出一张写侧表，分别在**四种形态**
    ///   下调 [`last_ready_has_segment_`]：环里有数据、环空且生产端开着、环空且生产端
    ///   已关闭（`close()` **之后**再调）、环里有数据但额度为 0。每次用 `Waker::noop()`
    ///   造一个无操作上下文。
    /// - 判断：只有「环里有数据且额度为正」为 `true`；其余三种必须为 `false`——它们若
    ///   判真，就是本用例要钉住的空转路径。
    #[test]
    fn last_ready_has_segment_rejects_closed_ring_() {
        let pair = (Dock::new(0x7501u32), Dock::new(0x7502u32));
        let mut cx = Context::from_waker(Waker::noop());

        // 形态一：环里有数据、额度为正 → 可读。
        let (mut tx, rx) = make_test_channel_(64usize);
        fill_ring_(&mut tx, &[7u8; 8usize]);
        let mut table = make_table_(rx, 64u32);
        assert!(
            last_ready_has_segment_::<TestMuxConfig_>(&mut table, Option::Some(pair), &mut cx),
            "环里有数据且额度为正时必须判为可读"
        );

        // 形态二：环空、生产端还开着 → 不可读（只是暂时没数据，靠事件唤醒）。
        let (tx, rx) = make_test_channel_(64usize);
        let mut table = make_table_(rx, 64u32);
        assert!(
            !last_ready_has_segment_::<TestMuxConfig_>(&mut table, Option::Some(pair), &mut cx),
            "环空且生产端开着时不得判为可读（否则就是空转）"
        );
        drop(tx);

        // 形态三（回归）：环空、生产端已关闭 → 读 future 立刻完成（`Closing`），
        // 但那不是「有数据可发」。
        let (mut tx, rx) = make_test_channel_(64usize);
        tx.close();
        let mut table = make_table_(rx, 64u32);
        assert!(
            !last_ready_has_segment_::<TestMuxConfig_>(&mut table, Option::Some(pair), &mut cx),
            "环已关闭且已排空时不得判为可读——只看 `is_ready()` 会在这里空转"
        );

        // 形态四：额度为 0 → 不可读（哪怕环里有数据，`drain_one_` 也发不出去）。
        let (mut tx, rx) = make_test_channel_(64usize);
        fill_ring_(&mut tx, &[7u8; 8usize]);
        let mut table = make_table_(rx, 0u32);
        assert!(
            !last_ready_has_segment_::<TestMuxConfig_>(&mut table, Option::Some(pair), &mut cx),
            "额度为 0 时不得判为可读（否则也是空转）"
        );
    }
}
