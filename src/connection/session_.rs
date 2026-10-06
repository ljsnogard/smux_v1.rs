//! 连接内部的**连接级环**：解复用循环与复用循环。
//!
//! 本模块是经 `abs_art` 的本地作用域 spawn 出来的**内侧**两个 `'static` 任务的全部
//! 实现（**外侧**两个贴传输的泵循环见 [`session_pump_`](super::session_pump_)）。
//! 结构见 [`crate::connection`] 模块文档 §2、§5。
//!
//! # 数据流
//!
//! ```text
//! 应用 --写--> 发送环 --读端(移交)--> 复用循环 --成帧--> 连接写环
//!                                                        |
//!                                              （写泵循环：环 --> 网络 Tx）
//!
//! 网络 Rx --（读泵循环：网络 --> 连接读环）-- 连接读环 --读端--> 解复用循环
//!          --写端(移交)--> 接收环 --读--> 应用
//! ```
//!
//! 解复用循环读到的字节**不再**先落进一块循环私有的暂存：连接读环本身就是那块
//! 暂存，而且它由外侧泵循环在「网络可读」时批量填充，因此解析侧看到的是一个持续
//! 供给的字节流。
//!
//! # 为什么必须有连接级环
//!
//! 直接把传输端交给这两个循环会带来三个问题，连接级环一次解决：
//!
//! - **帧解析需要的字节数无法用一次 await 表达**：帧头是自描述字段序列，载荷长度
//!   到解析完才揭晓。直接把传输端当字节流时，「读帧头」与「读载荷」都是不可中断的
//!   await，一旦传输端长时间没有足够字节，循环就只能挂在传输上，事件通道里的
//!   `Attach` / `TxClosed` 全部滞留在队列里；
//! - **写路线上的「写到一半」状态无法与合作式取消共存**：把成帧写进环是一条
//!   可重试的短操作，而写进传输端是可能长期 park 的操作（对端不读时）；
//! - **批量化**：读泵一次读可装载的整段、写泵一次把环上连续段交给传输端，
//!   把网络上的系统调用次数与帧数解耦。
//!
//! # 循环不持有连接核心
//!
//! 四个循环都只拿 [`MuxLoopShared_`] / [`ByteLoopShared_`]——注册表句柄（+ 内侧
//! 多一个单帧上限）——**不持有** [`MuxCore`](super::mux_connection::core_) 的强引用（因此也不持有
//! [`MuxConnection`](super::MuxConnection)）。
//!
//! 这不是随手的选择：若循环持有核心，核心将永远无法析构，其 `Drop` 里的取消令牌
//! 永远不会触发，四个任务与整个连接状态会永久泄漏。现在的形状是——最后一个
//! **应用面**对象（连接、四个句柄、两个半部之一）被丢弃 ⇒ 核心析构 ⇒ 取消四个
//! 令牌 ⇒ 循环在下一个 await 点退出。因此四个循环的 **park 点必须都能被取消令牌
//! 唤醒**：解复用循环经 `may_cancel_with`，复用循环的每处 park 都与
//! `cancellation()` 竞争（见下），泵循环的每处 park 都经 `race_cancel_`。
//!
//! # 半部为什么是「移交」而不是共享
//!
//! `buffex` 的段借用绑定在环半部上，半部若藏在共享单元的锁后面，段就不能跨
//! `await` 使用（锁内的闭包不允许 `await`）。因此建流方在创建环之后，把**会话侧**
//! 的两个半部交给对应循环，循环把它们放进**本地表**（`BTreeMap`，节点用调用方
//! 注入的分配器分配），此后可以自由在这些半部上 park / await。
//!
//! # 连接级环的硬约束（容量）
//!
//! 连接读环必须能**整块驻留一个满帧**（帧头 + `max_packet_size` 载荷）。否则会出现
//! 互等：外侧读泵因为环满而 park，内侧解复用循环却因为没有整帧而 park——两侧都在等
//! 对方，而环的唤醒只由写入 / 读出触发。容量由
//! [`TrConnCfg::make_stage_buffs`](crate::connection::TrConnCfg::make_stage_buffs)
//! 的提供者负责（默认实现见
//! [`K_STAGE_RING_CAPACITY`](crate::connection::K_STAGE_RING_CAPACITY) 的文档）。
//!
//! # 两个内侧循环各自的 park 点
//!
//! - 解复用循环：`read_header_async_`（连接读环）与「接收环满时的 `write_async`」；
//!   两者都经 `may_cancel_with` 挂在取消令牌上，因此连接被丢弃时能立刻退出。
//!   事件队列只在其醒来后**非阻塞排空**——`Attach` 必然先于对端的数据帧到达
//!   （建流方先发 `Attach` 事件、后发 `OPEN`），所以「先排空事件、再派发帧」即可
//!   保证不丢；
//! - 复用循环：无数据可发时优先 park 在**最近一次收到 `TxReady` 的那条发送环**上
//!   （Q4 裁决的兜底，覆盖「应用还没提交就发了事件」的竞态），否则 park 在事件
//!   通道上。**这些 park 都必须同时与事件通道 / 取消令牌竞争**：只 park 在环上
//!   会让别的子流刚入队的事件（例如某条子流的 `TxClosed`）一直排在后面，多子流下
//!   就是死锁；不与取消令牌竞争则连接被丢弃后循环无法退出。
//!
//! 另有一条容易踩的约束：循环里**借出环数据一律用非阻塞的 `try_read`**
//! （`drain_one_`）。`read_async` 在空环上会 park，一旦在「排空数据」这一步 park，
//! 事件就再也送不进来了。
//!
//! 写侧还有一条同样来自踩坑的约束：**不许要求「整帧连续空间」**。写环容量与传输环
//! 容量互相钳制时，「等环形装得下整帧」可能永远不成立（实测：写环剩 1987 字节、下一
//! 帧要 2010 字节，而写泵此刻正等传输环腾空间）。因此 `enqueue_frame_` 按环当前能
//! 给的段**分块**推进，并且只在环满时才 park；**绝不能**先把子流发送环里的字节搬出来
//! 再判空间——段的 drop 提交消费，那批字节会永久丢失（这是本轮实际踩到的第二个坑）。
//!
//! # 已知限制（Q4）
//!
//! 「通知在进入写路径时发出、真正提交在段 drop 时」这个时间差由上面的兜底覆盖：
//! 复用循环 park 到刚通知的那条环，用环自身的提交唤醒补上。残留漏洞（两条环都通知
//! 且都不提交时，先通知的那条可能永远不被轮询）见 dev-notes §5.1，属**已知限制**。
//!
//! # 拆流
//!
//! 每个方向独立收尾，两个方向都收尾后释放注册表条目（`ChannelState_::is_done_`）。

use core::{
    alloc::AllocatorClone,
    borrow::BorrowMut,
    future::poll_fn,
    mem::MaybeUninit,
    ops::Bound,
    task::Poll,
};
use std::collections::{BTreeMap, BTreeSet};

use abs_buff::{
    Demand, TrBuffWrite,
    buffer::TrBuffSegmMut,
    error::{TrTaggedError, WriteErrTag},
    x_deps::{abs_cancel, anylr::SomeOf},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use buffex::{ring::ConsumerError, x_deps::abs_buff};
use mm_ptr::Owned;

use crate::{
    connection::{
        Dock, FrameHeader, FrameKind, MuxError, TrConnCfg, flags,
        frame_::{K_MAX_FRAME_HEADER, encode_header_},
        frame_parser_,
        mux_connection::{ChannelRegistry_, ReserveErr_},
        owner_::ChannelOwner_,
        ring_::{BufferedRx, BufferedTx},
        signal_::{
            ControlFrame_, EventReceiver_, EventSender_, ReadEvent_, TrEventReceiver_,
            TrEventSender_, WriteEvent_,
        },
    },
    flow_ctrl::{Credit, WindowReport},
    metrics::{ChannelCloseReason, FrameDir, TrMetricsSink},
    time::{ConnClock_, TrTime},
    wire_io_::{CursorError, ReadCursor},
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
/// [`ConnClock_`] 放在共享量里，五个循环因此都拿得到**同一个 epoch** 下的毫秒。
///
/// 运行时值在这里是「配置给的」（[`TrConnCfg::Rt`]），**不是**共享量自己的类型参数：
/// 核心已经因为「必须无条件 `Send + Sync`」而不持有运行时值，共享量只在**本地**
/// 循环里活着，因此可以自由持有它。
#[derive(Clone)]
pub(crate) struct MuxLoopShared_<A, R, M>
where
    A: AllocatorClone + Send + Sync,
    R: TrTime,
    M: TrMetricsSink,
{
    /// 注册表（dock / 子流索引与配额、失败标志、取消令牌、计时唤醒槽）。
    reg_: ChannelRegistry_<A>,

    /// 协商出的单帧总长上限。
    max_packet_size_: usize,

    /// 连接级时钟（运行时值 + 建连 epoch）。
    conn_clock_: ConnClock_<R>,

    /// 协商出的**活跃子流空闲超时**（毫秒）；计时循环的判据。
    channel_timeout_millis_: u64,

    /// 本次连接的**指标接收方**（建连时从配置克隆的一份）。
    ///
    /// 五个循环不持有核心（见模块文档 §2.2），拿不到 `MuxCore`，只能像运行时值那样
    /// 自持一份。克隆的代价由需求方选的 sink 类型决定（见 `crate::metrics` 模块文档 §3）。
    ///
    /// **它不是 `Option`**：「有没有 sink」是编译期由 `C::Metrics` 决定的**类型事实**，
    /// 做成运行期状态只会让每个上报点多一次判空分支。不上报的装配里它是零大小的
    /// `NoMetrics`，上报调用点随即被单态化消除。
    metrics_: M,
}

/// [`MuxLoopShared_`] 在具体连接策略上的简写：分配器与运行时值都取自 `C`。
///
/// 循环与事件处理的签名里满是这个类型，用别名把两个关联类型摊平；而只依赖
/// 「注册表 + 分配器」的辅助函数（例如 [`fail_mux_loop_`]）仍写成泛型于
/// `A` / `R` 的形状，不必被绑到某个 `C` 上。
pub(crate) type MuxShared_<C> = MuxLoopShared_<
    <C as TrConnCfg>::Alloc,
    <C as TrConnCfg>::Rt,
    <C as TrConnCfg>::Metrics,
>;

impl<A, R, M> MuxLoopShared_<A, R, M>
where
    A: AllocatorClone + Send + Sync,
    R: TrTime,
    M: TrMetricsSink,
{
    /// 由建连路径展开后的量构造（成员私有，构造只能走这里）。
    pub(crate) fn new_(
        reg: ChannelRegistry_<A>,
        max_packet_size: usize,
        conn_clock: ConnClock_<R>,
        channel_timeout_millis: u64,
        metrics: M,
    ) -> Self {
        MuxLoopShared_ {
            reg_: reg,
            max_packet_size_: max_packet_size,
            conn_clock_: conn_clock,
            channel_timeout_millis_: channel_timeout_millis,
            metrics_: metrics,
        }
    }
}

impl<A, R, M> MuxLoopShared_<A, R, M>
where
    A: AllocatorClone + Send + Sync,
    R: TrTime,
    M: TrMetricsSink,
{
    /// 注册表句柄。
    pub(crate) fn reg_(&self) -> &ChannelRegistry_<A> {
        &self.reg_
    }

    /// 连接级时钟。
    pub(crate) fn conn_clock_(&self) -> &ConnClock_<R> {
        &self.conn_clock_
    }

    /// 协商出的活跃子流空闲超时（毫秒）。
    pub(crate) fn channel_timeout_millis_(&self) -> u64 {
        self.channel_timeout_millis_
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

/// 两个**外侧**泵循环共享的量（与 [`MuxLoopShared_`] 同源，少一个单帧上限）。
#[derive(Clone)]
pub(crate) struct ByteLoopShared_<A, M>
where
    A: AllocatorClone + Send + Sync,
    M: TrMetricsSink,
{
    /// 注册表（失败标志与取消令牌）。
    pub(crate) reg_: ChannelRegistry_<A>,

    /// 本次连接的**指标接收方**（建连时从配置克隆的一份；见 [`MuxLoopShared_::metrics_`]）。
    metrics_: M,
}

impl<A, M> ByteLoopShared_<A, M>
where
    A: AllocatorClone + Send + Sync,
    M: TrMetricsSink,
{
    /// 由注册表与指标接收方构造（成员私有，构造只能走这里）。
    pub(crate) fn new_(reg: ChannelRegistry_<A>, metrics: M) -> Self {
        ByteLoopShared_ {
            reg_: reg,
            metrics_: metrics,
        }
    }

    /// 本次连接携带的**指标接收方**（没有判空分支，理由见 [`MuxLoopShared_::metrics_`]）。
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
struct ReadEntry_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + 'static,
    A: AllocatorClone + Send + Sync + 'static,
{
    owner_: ChannelOwner_<A>,
    writer_: BufferedTx<B, A>,
}

/// 复用循环本地持有的一条子流：共享状态 + 会话侧**发送环读端**。
struct WriteEntry_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + 'static,
    A: AllocatorClone + Send + Sync + 'static,
{
    owner_: ChannelOwner_<A>,
    reader_: BufferedRx<B, A>,
}

/// 解复用循环的本地表：dock 对 → 该子流的接收环写端与共享状态。
///
/// 用 `BTreeMap` 而不是手写单链表：链表的 `find` / `remove` 是 O(n)，且每次
/// 增删都要自己用分配器构造 / 释放节点；`BTreeMap` 直接以调用方注入的分配器
/// （`allocator_api` 的 `new_in`）承担这些分配，查找降到 O(log n)。
type ReadTable_<B, A> = BTreeMap<(Dock, Dock), ReadEntry_<B, A>, A>;

/// 复用循环的本地表：dock 对 → 该子流的发送环读端与共享状态。
type WriteTable_<B, A> = BTreeMap<(Dock, Dock), WriteEntry_<B, A>, A>;

/// 「发送方向已收尾、但**数据还没排空**（或额度还没回来），因此还没发 `FIN`」的
/// 子流集合（复用循环本地持有）。
///
/// 存在的唯一理由是**半关闭的协议义务**：`drop(tx)` 只表示「不再写」，应用此前
/// 写进发送环的字节**仍然是承诺要送达的**，`FIN` 必须等它们全部上线之后才能发。
/// 而发送额度可能在中途用尽（`SendWindow::available() == 0`），此时循环不能 park
/// 在这一条子流上（那会挡住别的子流的事件），只能把它记在这里、等额度回补充或
/// 环被读空时再继续。见 [`finalize_entry_`]。
/// 分配器参数与两个本地表同源（调用方注入）：它的节点在**首次进入**该集合时分配，
/// 用全局分配器会让「丢半边 + 额度为 0」这条收尾路径隐式落到全局堆上
/// （`dev-notes/audit-heap-alloc-20261004-1122.md` N1）。
type PendingFin_<A> = BTreeSet<(Dock, Dock), A>;

/// 尝试把一条「已丢弃发送半边」的子流真正收尾：排空发送环 → 发 `CLOSE(FIN)` →
/// 设置 `local_fin_sent_` → 从本地表移除。
///
/// 返回 `true` 表示**本轮有进展**（循环值得再调一次），`false` 表示遇到下面两种
/// 合法阻塞之一、已把它留在 [`PendingFin_`] 里等条件变化：
///
/// - **发送环里还有数据但额度为 0**：等对端的窗口通告；
/// - **段一时借不出来**（环被写者占住）：等下一次 `TxReady` / 环提交。
///
/// 收尾成功后调用方仍需走 [`maybe_release_`]——真正释放注册表条目还要看接收方向
/// 是否也已收尾。
#[allow(clippy::too_many_arguments)]
async fn finalize_entry_<C, K>(
    tx_stage: &mut BufferedTx<C::StageBuff, C::Alloc>,
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<C::Buff, C::Alloc>,
    scratch: &mut Owned<[u8], C::Alloc>,
    pending_fin: &mut PendingFin_<C::Alloc>,
    read_events: &EventSender_<ReadEvent_<C::Buff, C::Alloc>>,
    pair: (Dock, Dock),
    cancel: &K,
) -> Result<bool, MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    // 1. 先尽力把环里剩下的字节送出去（额度用尽时 `drain_one_` 静默返回 `false`）。
    flush_entry_::<C, _>(tx_stage, shared, table, scratch, pair, cancel).await?;

    // 2. 环里还有没有数据？`None` = 不可判定（写者正占着环），下轮再来。
    match has_writable_::<C>(table, pair) {
        Option::None => return Result::Ok(false),
        // 还有数据没发出去：留在待收尾集合里等额度 / 环推进。
        Option::Some(true) => {
            pending_fin.insert(pair);
            return Result::Ok(false);
        }
        Option::Some(false) => {}
    }

    // 3. 环已排空：此刻才可以发 `CLOSE(FIN)`。
    control_close_via_::<C, _>(
        tx_stage,
        shared,
        shared.max_packet_size_,
        pair.0,
        pair.1,
        false,
        cancel.child_token(),
    )
    .await?;
    if let Option::Some(entry) = table.get(&pair) {
        entry.owner_.set_local_fin_sent_();
    }
    table.remove(&pair);
    pending_fin.remove(&pair);
    maybe_release_::<C, _>(shared, read_events, table, pair, cancel.child_token()).await?;
    Result::Ok(true)
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
    table: &mut WriteTable_<C::Buff, C::Alloc>,
    pair: (Dock, Dock),
) -> Option<bool>
where
    C: TrConnCfg,
{
    let entry = table.get_mut(&pair)?;
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
fn wake_establish_<A>(owner: &ChannelOwner_<A>)
where
    A: AllocatorClone,
{
    owner.notify_establish_();
}

/// 内侧循环的连接级失败处理：**取消导致的收尾不算失败**。
///
/// 连接被丢弃时 [`MuxCore::drop`](super::mux_connection::core_::MuxCore) 触发取消
/// 令牌，循环在 await 点上以「取消错误」的形式收到通知并退出——那是正常关闭，
/// 不应在注册表上留下失败标志（否则收尾路径会伪造出一个假的连接级失败）。
pub(crate) async fn fail_mux_loop_<A, R, M, K>(
    shared: &MuxLoopShared_<A, R, M>,
    cancel: &K,
    kind: Option<FrameKind>,
    err: &MuxError,
) where
    A: AllocatorClone + Send + Sync,
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

/// 连接读环上还剩多少可读字节；`None` 表示**外侧读泵已结束**且环已排空。
pub(crate) fn ring_readable_<B, A>(reader: &BufferedRx<B, A>) -> Option<usize>
where
    B: BorrowMut<[MaybeUninit<u8>]> + 'static,
    A: AllocatorClone + Send + Sync,
{
    match reader.consumer_state() {
        // 生产端关闭 + 无剩余数据：字节流到此为止。
        Option::Some((0usize, true)) => Option::None,
        Option::Some((count, _)) => Option::Some(count),
        // 只有切片之类的「永远有数据」的读端才会返回 `None`；环端不会。
        Option::None => Option::None,
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

/// 连接写环上还剩多少可写空间；`None` 表示**外侧写泵已结束**（读端消失）。
fn ring_space_<B, A>(writer: &BufferedTx<B, A>) -> Option<usize>
where
    B: BorrowMut<[MaybeUninit<u8>]> + 'static,
    A: AllocatorClone + Send + Sync,
{
    writer.producer_state().map(|(free, _closed)| free)
}

/// 把一帧（已编码）**分块**写进连接写环，直到全部写完。
///
/// # 为什么不能要求「整帧一次写」
///
/// 连接写环只有 `StageBuff` 那么大，而外侧写泵又受**传输环**容量制约：两者容量
/// 互相钳制时，「等环形装得下整帧」可能永远不成立（写环剩 1987 字节、下一帧要
/// 2010 字节，而写泵此刻只能等传输环腾空间）。因此这里按环当前能给的段**用多少
/// 写多少**：每片都是 `iter_slices` 给出的连续区，写完由段的 drop 提交写指针。
///
/// # Errors
///
/// 写环读端消失（外侧写泵已结束）→ [`MuxError::Transport`]（方向为写）；被取消 →
/// [`MuxError::Cancelled`]。
async fn enqueue_frame_<C, K>(
    tx_stage: &mut BufferedTx<C::StageBuff, C::Alloc>,
    frame: &[u8],
    cancel: K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let mut offset = 0usize;
    // 片长上限取环容量量级即可：真正的片长由 `try_write` 按当前空闲空间给出。
    let demand = Demand::at_least(1usize);
    while offset < frame.len() {
        let rest = frame.len() - offset;
        // 把 `try_write` 的借用限制在自己的块里：借出的段在块结束时 drop（提交写
        // 指针、唤醒外侧写泵），随后才能重新可变借用 `tx_stage` 去 park。
        let written = {
            let mut outcome = tx_stage.try_write(&demand);
            match outcome.as_mut().pick_left() {
                Option::Some(segment) => {
                    // `move_items_from_buff` 会**推进段的已消费量**，段的 drop 才会把
                    // 写入提交回环（推进写指针、唤醒外侧写泵）。直接对 `iter_slices_mut`
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

/// 把**帧头 + 载荷**分两段写进连接写环（数据面与控制面共用的入环入口）。
///
/// # 为什么可以分两段
///
/// 连接写环是**单生产者**（只有复用循环写它），因此先写头、再写载荷不会被别的帧
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
async fn enqueue_frame_parts_<C, K>(
    tx_stage: &mut BufferedTx<C::StageBuff, C::Alloc>,
    head: &[u8],
    payload: &[u8],
    cancel: K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    enqueue_frame_::<C, _>(tx_stage, head, cancel.child_token()).await?;
    enqueue_frame_::<C, _>(tx_stage, payload, cancel.child_token()).await
}

/// 把**读侧**游标错误映射为连接错误。
fn map_read_cursor_err_<E>(err: CursorError<E, ()>) -> MuxError {
    match err {
        CursorError::Read(_) => MuxError::Transport { write: false },
        CursorError::Write(()) => MuxError::Transport { write: true },
        CursorError::PeerClosed => MuxError::PeerClosed,
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 解复用循环
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 解复用循环：从连接读环解析帧、投递载荷、推进建流状态机。
pub(crate) async fn demux_loop_async_<C, K>(
    mut rx_stage: BufferedRx<C::StageBuff, C::Alloc>,
    shared: MuxShared_<C>,
    mut events: EventReceiver_<ReadEvent_<C::Buff, C::Alloc>>,
    events_tx: EventSender_<WriteEvent_<C::Buff, C::Alloc>>,
    cancel: K,
) where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    let mut table: ReadTable_<C::Buff, C::Alloc> =
        BTreeMap::new_in(lock_or_exit_!(shared.reg_.allocator_(cancel.child_token())));

    // 控制帧的暂存：只用于「帧头解析完成、载荷已经整块在环上」之后的取载荷。
    // 连接级环本身已经是暂存，因此这里只需要容纳**一帧**的最大载荷。
    let mut payload = Owned::new_slice(
        shared.max_packet_size_,
        |_idx, slot| {
            slot.write(0u8);
        },
        lock_or_exit_!(shared.reg_.allocator_(cancel.child_token())),
    );

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
        // 1. 先把挂起的 `Attach` / `Release` / `RxConsumed` 成批排空。
        if let Result::Err(err) =
            drain_read_events_::<C, _>(
                &mut events,
                &mut table,
                shared.conn_clock_().now_millis_(),
                &events_tx,
                &cancel,
            )
            .await
        {
            fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
            return;
        }

        // 2. 读一个帧头。字节由外侧读泵填进连接读环；这里只在环上等待
        //    （park 在环读端，**同时与读事件通道、取消令牌竞争**）。
        let readable = match ring_readable_(&rx_stage) {
            Option::Some(count) => count,
            // 读泵结束且环已排空：字节流到此为止，本循环退出（写泵会随之收尾）。
            Option::None => return,
        };
        if readable == 0 {
            // 环空：park 到「环里来了字节」/「有读事件」/「取消」三者之一。
            //
            // **必须同时等读事件通道**：`RxConsumed` 是「应用刚消费、去核对水位」的
            // 唯一触发点，而它到来时连接读环完全可能长期为空（对端正等额度、没有新帧）。
            // 只 park 在环上会让这条通知一直排在一条睡着的循环后面——应用读空了环却
            // 没人补发额度，两端互等（本仓库流控验收用例的死锁形态）。
            match park_read_wake_::<C, _>(&cancel, &mut rx_stage, &mut events).await {
                // 环里有字节：回顶部交给正式的帧头解析。
                ReadWake_::Bytes => {}
                // 事件已由 park **取出**（`flume` 的 `recv_async` 是消费语义，丢掉它就
                // 等于把这条 `Attach` / `Release` / `RxConsumed` 静默吞掉），因此必须
                // 在这里就地处理，不能只回顶部等 `drain_read_events_` 再取一次。
                ReadWake_::Event(event) => {
                    if let Result::Err(err) = handle_read_event_::<C, _>(
                        event,
                        &mut table,
                        shared.conn_clock_().now_millis_(),
                        &events_tx,
                        &cancel,
                    )
                    .await
                    {
                        fail_mux_loop_(&shared, &cancel, Option::None, &err).await;
                        return;
                    }
                }
                ReadWake_::Cancelled => return,
            }
            continue;
        }

        // 3. 读到帧头。解复用一律走 `frame_parser_` 的 **sans-IO 逐字节状态机**：
        //    它每次只向环索要 `Demand::exactly(1)`，于是 `min_len == 1` 不可能超过任何
        //    合法环容量，旧入口「按字段索要 `width` 字节」在容量 < width 时拿到终态
        //    `Unsatisfiable` 而整条连接失败的路径**从构造上消失**。
        //    帧头可能消耗环读指针，因此必须与第 2 步合起来看：环里此刻至少有一帧的
        //    **前若干字节**，帧头解析不会因为「环空」而永久 park——外侧读泵会继续填充。
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

        // 每次活动就地读一次连接级时钟（vDSO 读，相对本帧的解析 / 环操作可忽略）：
        // 两个时钟必须记下**活动发生的时刻**，而不是计时循环扫描的时刻。
        let now_millis = shared.conn_clock_().now_millis_();
        match kind {
            FrameKind::Data => {
                let amount = Credit::try_from(len).unwrap_or(Credit::MAX);
                let Some(entry) = table.get_mut(&pair) else {
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
                    let _ = events_tx.try_send_event_(WriteEvent_::Control {
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
                let Some(report) = window_report_of_(&header) else {
                    fail_mux_loop_(&shared, &cancel, Option::Some(kind), &MuxError::MalformedFrame).await;
                    return;
                };
                if let Option::Some(entry) = table.get(&pair) {
                    // 本端主动发起的子流：这是对端回的 `OPEN`，带上它的接收窗口。
                    let owner = entry.owner_.clone();
                    let _ = owner.send_on_report_(report);
                    owner.set_peer_opened_();
                    owner.mark_data_(now_millis);
                    wake_establish_(&owner);
                } else {
                    // 入向请求：**只登记，不回帧**。
                    //
                    // 本端自己的 `OPEN`（通告本端接收窗口）不能在读循环里发：窗口值
                    // 取决于调用方在最终裁决（`accept_async`）时给出的接收缓冲容量，
                    // 而读循环不知道那个容量。因此 `OPEN` 与 `ACCEPT` 都由
                    // `accept_async` 发出（见 `channel_handle` 模块文档）。
                    let reserved = shared
                        .reg_
                        .reserve_inbound_(
                            local,
                            remote,
                            report,
                            shared.conn_clock_().now_millis_(),
                            cancel.child_token(),
                        )
                        .await;
                    match reserved {
                        Result::Ok(_) => {}
                        // 等锁被取消：连接正在收尾，本循环退出。
                        Result::Err(ReserveErr_::Cancelled) => return,
                        Result::Err(_) => {
                            // 配额 / 重复：回 `REJECT`，理由载荷留空。
                            let _ = events_tx.try_send_event_(WriteEvent_::Control {
                                frame_: ControlFrame_::with_window_(
                                    FrameKind::Reject,
                                    0u8,
                                    local,
                                    remote,
                                    Option::None,
                                    Vec::new(),
                                ),
                            });
                        }
                    }
                }
            }
            FrameKind::Accept | FrameKind::Reject => {
                if let Option::Some(entry) = table.get(&pair) {
                    let owner = entry.owner_.clone();
                    let outcome = if header.kind() == FrameKind::Accept {
                        crate::connection::owner_::EstablishOutcome_::Accepted
                    } else {
                        crate::connection::owner_::EstablishOutcome_::Refused
                    };
                    owner.set_establish_outcome_(outcome);
                    owner.mark_data_(now_millis);
                    wake_establish_(&owner);
                }
            }
            FrameKind::Close => {
                let reset = header.flags() & flags::K_RESET != 0;
                if let Option::Some(entry) = table.get_mut(&pair) {
                    if reset {
                        // 对端宣告**不再接收**：收尾的是**本端的发送方向**，与本端
                        // 接收方向无关。因此**不**关接收环写端——那会把对端仍在途、
                        // 且已经写进本端接收环的数据一起丢掉（旧实现正是如此，实测
                        // 少一个窗口，见 `dev-notes/flow-ctrl…` §3.3）。
                        entry.owner_.set_peer_reset_();
                        // 上报「对端发来 RESET」。
                        shared.metrics_().on_reset(local, remote, true);
                    } else {
                        // 对端宣告**不再发送**：关掉接收环写端，应用读完已缓存数据
                        // 之后读到 EOF。
                        entry.writer_.close();
                        entry.owner_.set_peer_fin_();
                    }
                    entry.owner_.mark_data_(now_millis);
                    // 拆流记账（移除写侧表项、释放身份）交给复用循环的统一入口。
                    let _ = events_tx.try_send_event_(WriteEvent_::PeerClosed {
                        local_dock: local,
                        remote_dock: remote,
                        reset_: reset,
                    });
                }
            }
            FrameKind::WindowUpdate | FrameKind::Pulse => {
                if let Option::Some(entry) = table.get(&pair)
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
                // telegraph 本轮未实现（Q9 裁决）：收到即忽略。
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

/// 非阻塞排空读事件（`Attach` / `Release` / `RxConsumed`），返回连接级错误。
///
/// 与本模块的复用循环同理：**每轮的批有上限**。事件是持续到来的（应用每读一次就会
/// 投一条 `RxConsumed`），若「处理完为止」而不设上限，事件队列始终非空时本循环就
/// 永远走不到「读一个帧头」那一步。
///
/// `RxConsumed` 的处理是纯原子读改写（不取锁），但通告要经事件通道投给写循环，
/// 因此本函数仍是 `async`。
async fn drain_read_events_<C, K>(
    events: &mut EventReceiver_<ReadEvent_<C::Buff, C::Alloc>>,
    table: &mut ReadTable_<C::Buff, C::Alloc>,
    now_millis: u64,
    events_tx: &EventSender_<WriteEvent_<C::Buff, C::Alloc>>,
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    const K_EVENT_BATCH: usize = 32usize;
    for _ in 0..K_EVENT_BATCH {
        let Option::Some(event) = events.try_take_event_() else {
            break;
        };
        handle_read_event_::<C, _>(event, table, now_millis, events_tx, cancel).await?;
    }
    Result::Ok(())
}

/// 处理**单条**读事件。
///
/// 单独成函数是因为同一种事件有**两个**来源：`drain_read_events_` 从队列成批取出的，
/// 以及循环 park 时从队列里**取出**的那一条。后者不可省——`flume` 的异步接收是
/// **消费**语义，park 拿到手又丢掉就等于把这条 `Attach` 静默吞掉（本仓库的
/// `mux_min_stage_inmem` 用例正是这么挂死的：附加事件被 park 吃掉，随后到达的数据帧
/// 在本地表里找不到表项，读循环掉进「未知子流」分支并卡在注册表锁上）。
async fn handle_read_event_<C, K>(
    event: ReadEvent_<C::Buff, C::Alloc>,
    table: &mut ReadTable_<C::Buff, C::Alloc>,
    now_millis: u64,
    events_tx: &EventSender_<WriteEvent_<C::Buff, C::Alloc>>,
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    match event {
        ReadEvent_::Attach {
            local_dock,
            remote_dock,
            owner,
            writer_,
        } => {
            // 节点分配由表自己的分配器承担（`new_in` 时已注入），这里不再需要
            // 手工构造 `Owned`。重复 `Attach` 直接覆盖，不会留下陈旧条目。
            table.insert(
                (local_dock, remote_dock),
                ReadEntry_ {
                    owner_: owner,
                    writer_,
                },
            );
        }
        ReadEvent_::Release {
            local_dock,
            remote_dock,
        } => {
            // 身份已被释放：本循环不再向这条接收环投递任何字节。**必须显式关闭
            // 生产端**——`buffex` 的环半部被 drop **不会**置位关闭标记，少了这一步
            // 应用侧永远读不到 EOF（计时循环的超时拆流走的正是这条事件；对端 `FIN`
            // 那条路径由 `Close` 分支自己 `close()`）。
            if let Option::Some(mut entry) = table.remove(&(local_dock, remote_dock)) {
                entry.writer_.close();
            }
        }
        ReadEvent_::RxConsumed {
            local_dock,
            remote_dock,
        } => {
            let pair = (local_dock, remote_dock);
            let Some(entry) = table.get_mut(&pair) else {
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
    owner: &ChannelOwner_<C::Alloc>,
    buffered: usize,
    now_millis: u64,
    events_tx: &EventSender_<WriteEvent_<C::Buff, C::Alloc>>,
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
        let _ = events_tx.try_send_event_(WriteEvent_::Control {
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
        Vec::new(),
    )
}

/// 解复用循环在「连接读环为空」时的等待结果。
enum ReadWake_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]>,
    A: AllocatorClone + Send + Sync,
{
    /// 连接读环里来了字节（也可能是读端收尾：回顶部由 `ring_readable_` 判定）。
    Bytes,

    /// 从读事件通道里**取出**了一条事件（`Attach` / `Release` / `RxConsumed`）。
    ///
    /// 必须由调用方处理：`flume` 的异步接收是**消费**语义，丢掉它就等于静默吞掉
    /// 这条事件。
    Event(ReadEvent_<B, A>),

    /// 取消令牌触发：连接正在收尾，本循环退出。
    Cancelled,
}

/// 解复用循环空环时的 park：**同时**等「连接读环有字节」与「读事件通道有事件」，
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
    rx_stage: &mut BufferedRx<C::StageBuff, C::Alloc>,
    events: &mut EventReceiver_<ReadEvent_<C::Buff, C::Alloc>>,
) -> ReadWake_<C::Buff, C::Alloc>
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
    // 读事件的生产端全部消失 = 连接核心已析构，取消令牌随即触发。此后不再 poll 事件
    // 通道（它已 `Ready(None)` 且会立刻再次就绪），只等环与取消令牌。
    let mut events_closed = false;
    let mut cancelled = false;
    // 从通道里**取出**的事件：必须原样带回调用方，不能在此丢掉。
    let mut taken: Option<ReadEvent_<C::Buff, C::Alloc>> = Option::None;
    let mut ring_ready = false;
    poll_fn(|cx| {
        // 取消令牌优先：连接已收尾，直接退出。
        if core::future::Future::poll(cancel_fut.as_mut(), cx).is_ready() {
            cancelled = true;
            return Poll::Ready(());
        }
        if !events_closed {
            match core::future::Future::poll(event_fut.as_mut(), cx) {
                Poll::Ready(Option::Some(event)) => {
                    taken = Option::Some(event);
                    return Poll::Ready(());
                }
                Poll::Ready(Option::None) => events_closed = true,
                Poll::Pending => {}
            }
        }
        // 连接读环来了字节：回顶部交给正式的帧头解析。借出的段在此 drop（**不消费**，
        // 已消费量为 0）。
        if core::future::Future::poll(ring_fut.as_mut(), cx).is_ready() {
            ring_ready = true;
            return Poll::Ready(());
        }
        Poll::Pending
    })
    .await;
    if cancelled {
        ReadWake_::Cancelled
    } else if let Option::Some(event) = taken {
        ReadWake_::Event(event)
    } else if ring_ready {
        ReadWake_::Bytes
    } else {
        // 事件通道关闭（连接核心已析构）且环与取消都没就绪：按收尾处理。
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

/// 复用循环：按对端发送窗口调度各子流的发送环，把成帧写进**连接写环**。
///
/// 字节到网络的搬运由外侧写泵负责（见 `session_pump_`）。
pub(crate) async fn mux_loop_async_<C, K>(
    mut tx_stage: BufferedTx<C::StageBuff, C::Alloc>,
    shared: MuxShared_<C>,
    mut events: EventReceiver_<WriteEvent_<C::Buff, C::Alloc>>,
    read_events: EventSender_<ReadEvent_<C::Buff, C::Alloc>>,
    cancel: K,
) where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    let mut table: WriteTable_<C::Buff, C::Alloc> =
        BTreeMap::new_in(lock_or_exit_!(shared.reg_.allocator_(cancel.child_token())));
    let mut last_ready: Option<(Dock, Dock)> = Option::None;
    // 「发送方向已丢弃、但发送环还没排空（或额度没回来）」的子流：它们还欠对端一条
    // `CLOSE(FIN)`，由下面第 2.5 步在有进展时继续收尾（见 `finalize_entry_`）。
    let mut pending_fin: PendingFin_<C::Alloc> =
        BTreeSet::new_in(lock_or_exit_!(shared.reg_.allocator_(cancel.child_token())));
    // 环段字节的搬出暂存：`clone_items_from_buff` / `move_items_to_buff` 需要一个
    // 连续切片作为中间落点（段本身不能作为 `write_all_async_` 的源）。走调用方注入
    // 的分配器（`mm_ptr::Owned`），整条连接只分配一次。
    let mut scratch: Owned<[u8], C::Alloc> = Owned::new_slice(
        K_MAX_DATA_CHUNK,
        |_idx, slot| {
            slot.write(0u8);
        },
        lock_or_exit_!(shared.reg_.allocator_(cancel.child_token())),
    );

    loop {
        if cancel.is_cancelled() {
            return;
        }

        // 0. 先落实**会话释放**消息（`Drop` 只投消息、不碰身份表）。写循环也做这
        // 件事，是为了让释放不必等某次 API 操作：两个内侧循环任一被调度即可推进。
        lock_or_exit_!(shared
            .reg_
            .drain_session_events_(shared.conn_clock_().now_millis_(), cancel.child_token()));

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
            let Option::Some(event) = events.try_take_event_() else {
                break;
            };
            match race_cancel_(
                &cancel,
                handle_write_event_::<C, _>(
                        event,
                    &mut tx_stage,
                    &mut table,
                    &mut scratch,
                    &mut pending_fin,
                    &shared,
                    &read_events,
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
                    &mut table,
                    &mut scratch,
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

        // 2.5. 额度回补或环被读空之后，把「欠着 FIN」的子流继续收尾。
        //
        //    收尾要等两个条件同时成立：发送环已排空、且能写出 `CLOSE` 帧。`drain_once_`
        //    刚刚尽力把数据发出去了，因此这里每次只取一条试收尾、且**不跨 `await`
        //    持有对集合的借用**（收尾本身会改写集合）；「本轮有进展」就继续轮，每一轮
        //    要么写出一段数据、要么真正收尾掉一条，必然收敛。
        while let Some(pair) = pending_fin.iter().next().copied() {
            match race_cancel_(
                &cancel,
                finalize_entry_::<C, _>(
                    &mut tx_stage,
                    &shared,
                    &mut table,
                    &mut scratch,
                    &mut pending_fin,
                    &read_events,
                    pair,
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

        // 3. 没有可发数据：若「最近通知过的那条发送环」就是目标，就 park 在它上面
        //    （Q4 裁决的兜底：应用可能在提交之前就发出了事件）。
        //
        //    **必须同时轮询事件通道、写环与取消令牌**：只 park 在子流环上会让「别的
        //    子流刚入队的事件」一直排在一个睡着的循环后面（多子流下就是死锁）；不轮询
        //    连接写环则「写环腾出空间」这条唤醒会丢（写泵把字节搬走之后没有任何人
        //    通知复用循环，若这里不同时 park 在写环上，drain 就再也不会被触发）。
        let mut taken: Option<WriteEvent_<C::Buff, C::Alloc>> = Option::None;
        let mut ring_ready = false;
        let alive = {
            let cancel_fut = cancel.child_token().cancellation();
            let mut event_fut = core::pin::pin!(events.take_event_async_());
            let mut cancel_fut = core::pin::pin!(cancel_fut);
            // 仅当上一轮是**因为写环没空间**停下时，才把写环的就绪作为唤醒条件。
            // 写环有空间就说明可以重试 `drain_once_`（这一步不消费借出的段）。
            // 若「最近通知」的那条发送环存在，还要同时 park 在它上面（Q4 兜底）。
            let ring_demand = Demand::at_least(1usize);
            poll_fn(|cx| {
                // 取消令牌优先：连接已收尾，直接退出。
                if core::future::Future::poll(cancel_fut.as_mut(), cx).is_ready() {
                    return Poll::Ready(false);
                }
                match core::future::Future::poll(event_fut.as_mut(), cx) {
                    Poll::Ready(Option::Some(event)) => {
                        taken = Option::Some(event);
                        return Poll::Ready(true);
                    }
                    // 所有生产者都没了：连接正在收尾，退出。
                    Poll::Ready(Option::None) => return Poll::Ready(false),
                    Poll::Pending => {}
                }
                // 子流发送环有新数据：回到顶部由 `drain_once_` 正式取走。
                //
                // 这条 park 是**可选**的（「最近通知」那条环可能已经不存在），因此
                // 就地建出 future、就地 poll：`Demand` 与借出的段都只活在这个分支里，
                // 不会把 `table` 的借用带出闭包。
                //
                // **但「环可读」不等于「有进展可能」**：额度为 0 时 `drain_one_`
                // 一条也发不出去，若这里照样因「环可读」返回就绪，整圈会在
                // 「回到顶部 → drain 失败 → 立刻又就绪」之间**纯空转**：CPU 打满，
                // 同运行时上的其它任务被饿死，连超时看门狗都来不及触发。因此就绪
                // 条件必须同时要求「这条子流确实还有发送额度」；额度归零之后，唤醒
                // 一律来自读循环收到窗口通告时投的那条事件（见 `write_into_ring` 的
                // 对端通告分支）。这里用**非阻塞**的 `send_available_try_`：窗口锁当场
                // 取不到就按「没额度」处理，不唤醒——那同样是安全方向。
                if let Option::Some(pair) = last_ready
                    && let Option::Some(entry) = table.get_mut(&pair)
                    && matches!(entry.owner_.send_available_try_(), Option::Some(credit) if credit > 0u32)
                {
                    let ring_fut = core::pin::pin!(core::future::IntoFuture::into_future(
                        entry.reader_.read_async(&ring_demand),
                    ));
                    if core::future::Future::poll(ring_fut, cx).is_ready() {
                        ring_ready = true;
                        return Poll::Ready(true);
                    }
                }
                Poll::Pending
            })
            .await
        };
        if !alive {
            return;
        }
        if let Option::Some(event) = taken {
            match race_cancel_(
                &cancel,
                handle_write_event_::<C, _>(
                        event,
                    &mut tx_stage,
                    &mut table,
                    &mut scratch,
                    &mut pending_fin,
                    &shared,
                    &read_events,
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
        last_ready = Option::None;
        continue;
    }
}

/// 处理一条写事件；返回 `Err` 表示连接级失败（调用方负责 `mark_failed_` 并退出）。
#[allow(clippy::too_many_arguments)]
async fn handle_write_event_<C, K>(
    event: WriteEvent_<C::Buff, C::Alloc>,
    tx_stage: &mut BufferedTx<C::StageBuff, C::Alloc>,
    table: &mut WriteTable_<C::Buff, C::Alloc>,
    scratch: &mut Owned<[u8], C::Alloc>,
    pending_fin: &mut PendingFin_<C::Alloc>,
    shared: &MuxShared_<C>,
    read_events: &EventSender_<ReadEvent_<C::Buff, C::Alloc>>,
    cancel: &K,
    last_ready: &mut Option<(Dock, Dock)>,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    match event {
        WriteEvent_::Attach {
            local_dock,
            remote_dock,
            owner,
            reader_,
        } => {
            // 节点分配由表自己的分配器承担（`new_in` 时已注入）。
            table.insert(
                (local_dock, remote_dock),
                WriteEntry_ {
                    owner_: owner,
                    reader_,
                },
            );
        }
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
        WriteEvent_::TxClosed {
            local_dock,
            remote_dock,
        } => {
            let pair = (local_dock, remote_dock);
            // 应用已不再写：记下这件事，然后**尝试**收尾。
            //
            // 收尾不一定一次成功：应用此前写进发送环的字节是「承诺要送达」的，`FIN`
            // 必须等它们全部上线（`drop(tx)` 本身不等待送达，见 README §5）；而发送
            // 额度可能中途用尽。因此这里不再「无条件发 FIN」，而是交给
            // [`finalize_entry_`]——它在「环已排空」时才发 FIN，否则把这条子流记进
            // `pending_fin`，等窗口通告 / 环推进之后由主循环第 2.5 步继续。
            if let Option::Some(entry) = table.get(&pair) {
                entry.owner_.set_app_tx_closed_();
            }
            // 返回值只表示「本轮有进展」；成功收尾与暂时受阻两种结果都已经由
            // [`finalize_entry_`] 自己落实（受阻时它已登记进 `pending_fin`），
            // 主循环第 2.5 步会在有进展时再试。
            let _ = finalize_entry_::<C, _>(
                tx_stage,
                shared,
                table,
                scratch,
                pending_fin,
                read_events,
                pair,
                cancel,
            )
            .await?;
        }
        WriteEvent_::RxClosed {
            local_dock,
            remote_dock,
        } => {
            let pair = (local_dock, remote_dock);
            // 找到 owner：本地表里可能已经没有（例如对端先发了 `RESET`），用注册表兜底。
            let owner = match table.get(&pair).map(|entry| entry.owner_.clone()) {
                Option::Some(owner) => Option::Some(owner),
                Option::None => {
                    lock_or_fail_!(shared
                        .reg_
                        .channel_owner_(local_dock, remote_dock, cancel.child_token()))
                }
            };
            if let Option::Some(owner) = owner {
                owner.set_app_rx_closed_();
                // 向对端宣告「我不再接收」：这条通知与发送方向无关，`RESET` 只关对端
                // 的发送方向（见 `connection` 模块文档 §7 的半关闭映射）。
                if owner.claim_local_reset_() {
                    control_close_via_::<C, _>(
                        tx_stage,
                        shared,
                        shared.max_packet_size_,
                        local_dock,
                        remote_dock,
                        true,
                        cancel.child_token(),
                    )
                    .await?;
                    // 本端发出的 `RESET` 到此才真正上线，因此上报点放在发送成功之后。
                    shared.metrics_().on_reset(local_dock, remote_dock, false);
                }
            }
            // **绝不在这里摘写侧表项、也不在这里丢掉读侧表项**：
            //
            // - 写侧：`drop(rx)` 只结束接收方向，发送方向写进环里的字节仍是承诺要送达
            //   的，`FIN` 还没发。旧实现无条件 `table.remove`，于是已入 `pending_fin` 的
            //   条目变成查不到表项的僵尸、在途数据被放弃；
            // - 读侧：读表项要留到**身份真正释放**（`maybe_release_`）时由 `Release`
            //   摘掉。否则对端在收到我们 `RESET` 之前发出的在途帧会掉进「未知子流」
            //   分支，把整条连接判成协议违例。留着的这段窗口里，数据分支按
            //   `is_app_rx_closed_` 静默丢弃。
            maybe_release_::<C, _>(
                shared,
                read_events,
                table,
                pair,
                cancel.child_token(),
            )
            .await?;
        }
        WriteEvent_::LocalAbort {
            local_dock,
            remote_dock,
        } => {
            let pair = (local_dock, remote_dock);
            // 本端主动拆流（空闲超时）：显式关闭发送环的消费端——没有别的执行者会替
            // 这条路径关它，而应用必须立刻看到发送方向已关闭。理由见事件类型文档。
            if let Option::Some(entry) = table.get_mut(&pair) {
                entry.reader_.close();
            }
            table.remove(&pair);
            pending_fin.remove(&pair);
            maybe_release_::<C, _>(shared, read_events, table, pair, cancel.child_token())
                .await?;
        }
        WriteEvent_::PeerClosed {
            local_dock,
            remote_dock,
            reset_,
        } => {
            let pair = (local_dock, remote_dock);
            if reset_ {
                // 对端不再接收：本端发送方向就此作废——未发出的数据与「欠一条 FIN」的
                // 登记一起放弃（这是唯一允许放弃已提交字节的情形，`RESET` 就是它的授权）。
                table.remove(&pair);
                pending_fin.remove(&pair);
            }
            maybe_release_::<C, _>(
                shared,
                read_events,
                table,
                pair,
                cancel.child_token(),
            )
            .await?;
        }
    }
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
    read_events: &EventSender_<ReadEvent_<C::Buff, C::Alloc>>,
    table: &WriteTable_<C::Buff, C::Alloc>,
    pair: (Dock, Dock),
    cancel: K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    // 找到 owner：表里可能已经移除，因此用注册表兜底（异步取锁、可取消）。
    let owner = match table.get(&pair).map(|entry| entry.owner_.clone()) {
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
        let _ = shared
            .reg_
            .release_channel_(pair.0, pair.1, now_millis, cancel.child_token())
            .await;
        let _ = read_events.try_send_event_(ReadEvent_::Release {
            local_dock: pair.0,
            remote_dock: pair.1,
        });
        // 上报子流关闭（正常协议收尾）。`claim_release_` 是一次性的 CAS，因此这里
        // **恰好上报一次**；超时与建流超时两条路径不经过本函数，各自在 `timer_` 里上报。
        shared.metrics_().on_channel_closed(
            pair.0,
            pair.1,
            ChannelCloseReason::Fin,
            now_millis.saturating_sub(owner.created_millis_()),
        );
    }
    Result::Ok(())
}

/// 发一条 `CLOSE`。用独立的 helper 以便在事件处理里直接 await。
async fn control_close_via_<C, K>(
    tx_stage: &mut BufferedTx<C::StageBuff, C::Alloc>,
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

/// 把控制帧写进写环（分块；空间不足时由写泵持续搬运腾出空间）。
async fn write_control_blocking_<C, K>(
    tx_stage: &mut BufferedTx<C::StageBuff, C::Alloc>,
    shared: &MuxShared_<C>,
    max_packet_size: usize,
    frame: &ControlFrame_,
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let header = FrameHeader::new_(
        frame.kind_(),
        frame.flags_(),
        frame.local_dock_(),
        frame.remote_dock_(),
        frame.payload_().len(),
        frame.window_(),
    );
    // 帧头进栈上定长缓冲；帧总长上限的判定与「拼成整帧」时同义。
    let (head, head_len) = encode_header_(&header)?;
    let payload = frame.payload_();
    if head_len + payload.len() > max_packet_size {
        return Result::Err(MuxError::FrameTooLarge);
    }
    enqueue_frame_parts_::<C, _>(tx_stage, &head[..head_len], payload, cancel.child_token()).await?;
    // 上报「写出一个帧」：控制帧在这里才真正**整帧**进了写环（`enqueue_frame_parts_`
    // 把头与载荷分两次入环，因此计数点必须在它之后）。
    shared.metrics_().on_frame(
        FrameDir::Send,
        frame.local_dock_(),
        frame.remote_dock_(),
        frame.kind_(),
        u32::try_from(head_len + payload.len()).unwrap_or(u32::MAX),
    );
    Result::Ok(())
}

/// 把某条子流发送环里已提交的数据全部写出（直到取空）。
async fn flush_entry_<C, K>(
    tx_stage: &mut BufferedTx<C::StageBuff, C::Alloc>,
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<C::Buff, C::Alloc>,
    scratch: &mut Owned<[u8], C::Alloc>,
    pair: (Dock, Dock),
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    loop {
        let progressed =
            drain_one_::<C, _>(
                tx_stage,
                shared,
                table,
                scratch,
                pair,
                cancel,
            )
            .await?;
        if !progressed {
            return Result::Ok(());
        }
    }
}

/// 轮转一遍本地表，最多写出一段数据；返回是否有进展。
#[allow(clippy::too_many_arguments)]
async fn drain_once_<C, K>(
    tx_stage: &mut BufferedTx<C::StageBuff, C::Alloc>,
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<C::Buff, C::Alloc>,
    scratch: &mut Owned<[u8], C::Alloc>,
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
    }
}

/// 尝试为 `pair` 写出一段数据；返回是否写出。
#[allow(clippy::too_many_arguments)]
async fn drain_one_<C, K>(
    tx_stage: &mut BufferedTx<C::StageBuff, C::Alloc>,
    shared: &MuxShared_<C>,
    table: &mut WriteTable_<C::Buff, C::Alloc>,
    scratch: &mut Owned<[u8], C::Alloc>,
    pair: (Dock, Dock),
    cancel: &K,
) -> Result<bool, MuxError>
where
    C: TrConnCfg,
    C::Rt: TrTime + Clone,
    K: TrCancellationToken,
{
    let token = cancel.child_token();
    let Some(entry) = table.get_mut(&pair) else {
        return Result::Ok(false);
    };

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
    // 写环还活着吗？读端消失说明外侧写泵已结束，连接不可用。
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

    // **两段**写进连接写环：先帧头（栈上缓冲）、再载荷（`scratch`）。写环是单生产者，
    // 因此两段之间不会被别的帧插进来。空间在上面已经判过（保守预判 + 实际头长），
    // 因此这里**不会再**出现「字节已离开子流环、帧却没写出去」的状态。
    if let Result::Err(err) = enqueue_frame_parts_::<C, _>(
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
    Result::Ok(true)
}
