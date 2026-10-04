//! 连接内部的**连接级环**：解复用循环与复用循环。
//!
//! 本模块是经 `abs_art` 的本地作用域 spawn 出来的**内侧**两个 `'static` 任务的全部
//! 实现（**外侧**两个贴传输的泵循环见 [`session_pump_`](super::session_pump_)）。
//! 结构见 [`crate::connection`] 模块文档 §2、§5，落地裁决见
//! `dev-notes/connection-20261002-0548.md` §5，连接级环的形状见
//! `dev-notes/connection-20261122-0000.md`。
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
use std::collections::BTreeMap;

use abs_buff::{
    Demand, TrBuffWrite,
    buffer::TrBuffSegmMut,
    x_deps::{abs_cancel, anylr::SomeOf},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use buffex::x_deps::abs_buff;
use mm_ptr::Owned;

use crate::{
    connection::{
        Dock, FrameHeader, FrameKind, MuxError, TrConnCfg, flags,
        frame_::encode_header_into_,
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
    wire_io_::{CursorError, ReadCursor},
};

/// 单条子流数据帧的载荷上限（一次成帧最多携带多少字节）。
///
/// 与 `max_packet_size` 无关：后者是**帧总长**上限，这里只是想避免一次借出过大的
/// 段而让其他子流等太久（公平性，见 `connection-20260919-1631.md` §5.2）。
const K_MAX_DATA_CHUNK: usize = 16usize * 1024usize;

/// 帧暂存环容量的**理论下限**：一帧 = 头（自描述字段序列）+ 载荷。
///
/// 字段最坏情况是三字节宽（每种字段各一个、加上帧首字节），这里给一个宽松的上限
/// 就够——真正的容量由配置给出（见模块文档「连接级环的硬约束」）。
const K_MAX_FRAME_HEADER: usize = 64usize;

/// 两个**内侧**循环共享的、与调用方配置无关的量。
///
/// 它由建连路径从核心展开而来：注册表句柄 + 单帧上限。注意它**不含核心引用**
/// （模块文档「循环不持有连接核心」），因此核心可以在最后一个应用面对象被丢弃时
/// 正常析构并触发收尾。
///
/// 「初始接收窗口」与「通告阈值」**不在这里**：`abs_smux` 更新后，子流缓冲由调用方
/// 在最终裁决（`accept_async`）时给出，容量逐条子流不同，因此这两个量在**建流时**
/// 按该子流的接收缓冲容量算出，存进该子流的 [`ChannelState_`]（见
/// [`crate::connection::owner_`]）。循环侧不再需要任何连接级窗口快照。
#[derive(Clone)]
pub(crate) struct MuxLoopShared_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 注册表（dock / 子流索引与配额、失败标志、取消令牌）。
    reg_: ChannelRegistry_<A>,

    /// 协商出的单帧总长上限。
    max_packet_size_: usize,
}

impl<A> MuxLoopShared_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 由建连路径展开后的量构造（成员私有，构造只能走这里）。
    pub(crate) fn new_(reg: ChannelRegistry_<A>, max_packet_size: usize) -> Self {
        MuxLoopShared_ {
            reg_: reg,
            max_packet_size_: max_packet_size,
        }
    }
}

/// 两个**外侧**泵循环共享的量（与 [`MuxLoopShared_`] 同源，少一个单帧上限）。
#[derive(Clone)]
pub(crate) struct ByteLoopShared_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 注册表（失败标志与取消令牌）。
    pub(crate) reg_: ChannelRegistry_<A>,
}

impl<A> ByteLoopShared_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 由注册表构造（成员私有，构造只能走这里）。
    pub(crate) fn new_(reg: ChannelRegistry_<A>) -> Self {
        ByteLoopShared_ { reg_: reg }
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

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 小工具
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


/// 提示建流等待者（`open_channel_async`）：状态可能变了。
///
/// 走子流热状态的**通知通道**（异步取锁、可取消）；通道持久，因此不丢唤醒。
async fn wake_establish_<A, K>(owner: &ChannelOwner_<A>, cancel: K)
where
    A: AllocatorClone + Send + Sync,
    K: TrCancellationToken,
{
    let _ = owner
        .with_mut_async_(cancel, |state| state.notify_establish_())
        .await;
}

/// 内侧循环的连接级失败处理：**取消导致的收尾不算失败**。
///
/// 连接被丢弃时 [`MuxCore::drop`](super::mux_connection::core_::MuxCore) 触发取消
/// 令牌，循环在 await 点上以「取消错误」的形式收到通知并退出——那是正常关闭，
/// 不应在注册表上留下失败标志（否则收尾路径会伪造出一个假的连接级失败）。
pub(crate) async fn fail_mux_loop_<A, K>(
    shared: &MuxLoopShared_<A>,
    cancel: &K,
    err: &MuxError,
) where
    A: AllocatorClone + Send + Sync,
    K: TrCancellationToken,
{
    if !cancel.is_cancelled() {
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

/// 把「帧头 + 载荷」编码成一个连续帧。
///
/// 返回 `Ok(None)` 表示**帧总长超限**（头 + 载荷 > `max_packet_size`）——由调用方
/// 决定这是「连接级失败」还是「调用方构造了过大的帧」。
fn encode_whole_frame_(
    header: &FrameHeader,
    payload: &[u8],
    max_packet_size: usize,
) -> Result<Option<Vec<u8>>, MuxError> {
    let mut frame = Vec::with_capacity(K_MAX_FRAME_HEADER + payload.len());
    encode_header_into_(&mut frame, header)?;
    if frame.len() + payload.len() > max_packet_size {
        return Result::Ok(Option::None);
    }
    frame.extend_from_slice(payload);
    Result::Ok(Option::Some(frame))
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
    shared: MuxLoopShared_<C::Alloc>,
    mut events: EventReceiver_<ReadEvent_<C::Buff, C::Alloc>>,
    events_tx: EventSender_<WriteEvent_<C::Buff, C::Alloc>>,
    cancel: K,
) where
    C: TrConnCfg,
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
        // 0. 先落实**会话释放**消息：`Drop` 只投消息、不碰身份表，因此处理「未知
        // 子流」之前必须先让「刚被丢弃的句柄」的释放生效——否则在途帧会被误判成
        // 协议违例，而不是宽限期（`WAIT_CLOSE`）内的静默丢弃。
        lock_or_exit_!(shared.reg_.drain_session_events_(cancel.child_token()));
        // 1. 先把挂起的 Attach / Release 排空。
        drain_read_events_::<C>(&mut events, &mut table);

        // 2. 读一个帧头。字节由外侧读泵填进连接读环；这里只在环上等待
        //    （park 在环读端，并与取消令牌竞争）。
        let readable = match ring_readable_(&rx_stage) {
            Option::Some(count) => count,
            // 读泵结束且环已排空：字节流到此为止，本循环退出（写泵会随之收尾）。
            Option::None => return,
        };
        if readable == 0 {
            // 环空：park 到「可读」或「取消」。
            match race_cancel_(
                &cancel,
                rx_stage.read_async(&Demand::at_least(1usize)),
            )
            .await
            {
                Option::None => return,
                // 借出的段立即丢弃（不消费）：只是为了拿到「有字节了」这个事实。
                Option::Some(outcome) => match took_(outcome) {
                    Took_::Segm(segm) => drop(segm),
                    Took_::Failed(_) | Took_::Nothing => return,
                },
            }
            continue;
        }

        // 3. 读到帧头。解复用一律走 `frame_parser_` 的 **sans-IO 逐字节状态机**：
        //    它每次只向环索要 `Demand::exactly(1)`，于是 `min_len == 1` 不可能超过任何
        //    合法环容量，旧入口「按字段索要 `width` 字节」在容量 < width 时拿到终态
        //    `Unsatisfiable` 而整条连接失败的路径**从构造上消失**。
        //    帧头可能消耗环读指针，因此必须与第 2 步合起来看：环里此刻至少有一帧的
        //    **前若干字节**，帧头解析不会因为「环空」而永久 park——外侧读泵会继续填充。
        let header = match race_cancel_(
            &cancel,
            frame_parser_::read_header_async_::<_, _>(&mut rx_stage, cancel.child_token()),
        )
        .await
        {
            Option::None => return,
            Option::Some(Result::Ok(header)) => header,
            Option::Some(Result::Err(err)) => {
                fail_mux_loop_(&shared, &cancel, &err).await;
                return;
            }
        };

        // 4. 载荷长度校验（与写侧同一条上限：帧总长 ≤ `max_packet_size`）。
        let len = header.payload_len();
        if len + K_MAX_FRAME_HEADER > shared.max_packet_size_ || len > payload.len() {
            fail_mux_loop_(&shared, &cancel, &MuxError::FrameTooLarge).await;
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
                    fail_mux_loop_(&shared, &cancel, &map_read_cursor_err_::<_>(err)).await;
                    return;
                }
            }
        }
        let bytes = &payload[..len];

        // 5. 派发。帧头里的 `LocalDock` 是发送方的本端 dock，因此**本端的
        //    local_dock 是帧头的 `RemoteDock`**（镜像语义，见模块文档 §4.2）。
        let local = header.remote_dock();
        let remote = header.local_dock();
        let pair = (local, remote);

        match header.kind() {
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
                        cancel.child_token()
                    )) {
                        continue;
                    }
                    fail_mux_loop_(&shared, &cancel, &MuxError::MalformedFrame).await;
                    return;
                };
                lock_or_exit_!(entry.owner_.with_mut_async_(cancel.child_token(), |state| {
                    state.touch_();
                }));
                let counted = lock_or_exit_!(entry
                    .owner_
                    .with_mut_async_(cancel.child_token(), |state| state
                        .flow_mut_()
                        .recv_window_mut()
                        .on_data(amount)));
                if let Result::Err(err) = counted {
                    fail_mux_loop_(&shared, &cancel, &MuxError::FlowCtrl(err)).await;
                    return;
                }
                let wrote = race_cancel_(
                    &cancel,
                    write_into_ring_(&mut entry.writer_, bytes, cancel.child_token()),
                )
                .await;
                match wrote {
                    Option::None => return,
                    Option::Some(Result::Ok(())) => {}
                    Option::Some(Result::Err(_err)) => {
                        fail_mux_loop_(&shared, &cancel, &MuxError::Transport { write: true })
                            .await;
                        return;
                    }
                }
            }
            FrameKind::Open => {
                let Some(report) = window_report_of_(&header) else {
                    fail_mux_loop_(&shared, &cancel, &MuxError::MalformedFrame).await;
                    return;
                };
                if let Option::Some(entry) = table.get_mut(&pair) {
                    // 本端主动发起的子流：这是对端回的 `OPEN`，带上它的接收窗口。
                    let owner = entry.owner_.clone();
                    lock_or_exit_!(owner.with_mut_async_(cancel.child_token(), |state| {
                        let _ = state.flow_mut_().send_window_mut().on_report(report);
                        state.set_peer_opened_();
                        state.touch_();
                    }));
                    wake_establish_(&owner, cancel.child_token()).await;
                } else {
                    // 入向请求：**只登记，不回帧**。
                    //
                    // 本端自己的 `OPEN`（通告本端接收窗口）不能在读循环里发：窗口值
                    // 取决于调用方在最终裁决（`accept_async`）时给出的接收缓冲容量，
                    // 而读循环不知道那个容量。因此 `OPEN` 与 `ACCEPT` 都由
                    // `accept_async` 发出（见 `channel_handle` 模块文档）。
                    let reserved = shared
                        .reg_
                        .reserve_inbound_(local, remote, report, cancel.child_token())
                        .await;
                    match reserved {
                        Result::Ok(()) => {}
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
                if let Option::Some(entry) = table.get_mut(&pair) {
                    let owner = entry.owner_.clone();
                    let outcome = if header.kind() == FrameKind::Accept {
                        crate::connection::owner_::EstablishOutcome_::Accepted
                    } else {
                        crate::connection::owner_::EstablishOutcome_::Refused
                    };
                    lock_or_exit_!(owner.with_mut_async_(cancel.child_token(), |state| {
                        state.set_establish_outcome_(outcome);
                        state.touch_();
                    }));
                    wake_establish_(&owner, cancel.child_token()).await;
                }
            }
            FrameKind::Close => {
                let reset = header.flags() & flags::K_RESET != 0;
                if let Option::Some(entry) = table.get_mut(&pair) {
                    // 关掉接收环写端：应用先把已缓存数据读完，再读到 EOF。
                    entry.writer_.close();
                    let owner = entry.owner_.clone();
                    lock_or_exit_!(owner.with_mut_async_(cancel.child_token(), |state| {
                        if reset {
                            state.set_peer_reset_();
                        } else {
                            state.set_peer_fin_();
                        }
                        state.touch_();
                    }));
                    let claimed = lock_or_exit_!(owner.with_mut_async_(
                        cancel.child_token(),
                        |state| {
                            if !state.is_done_() {
                                return false;
                            }
                            state.claim_release_()
                        }
                    ));
                    if claimed {
                        let _ = shared
                            .reg_
                            .release_channel_(local, remote, cancel.child_token())
                            .await;
                    }
                    let _ = events_tx.try_send_event_(WriteEvent_::PeerClosed {
                        local_dock: local,
                        remote_dock: remote,
                        reset_: reset,
                    });
                }
            }
            FrameKind::WindowUpdate | FrameKind::Pulse => {
                if let Option::Some(entry) = table.get_mut(&pair)
                    && let Option::Some(report) = window_report_of_(&header)
                {
                    lock_or_exit_!(entry
                        .owner_
                        .with_mut_async_(cancel.child_token(), |state| {
                            let _ = state.flow_mut_().send_window_mut().on_report(report);
                            state.touch_();
                        }));
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

/// 非阻塞排空读事件（`Attach` / `Release`）。
fn drain_read_events_<C>(
    events: &mut EventReceiver_<ReadEvent_<C::Buff, C::Alloc>>,
    table: &mut ReadTable_<C::Buff, C::Alloc>,
) where
    C: TrConnCfg,
{
    while let Option::Some(event) = events.try_take_event_() {
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
                table.remove(&(local_dock, remote_dock));
            }
        }
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
    shared: MuxLoopShared_<C::Alloc>,
    mut events: EventReceiver_<WriteEvent_<C::Buff, C::Alloc>>,
    read_events: EventSender_<ReadEvent_<C::Buff, C::Alloc>>,
    cancel: K,
) where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let mut table: WriteTable_<C::Buff, C::Alloc> =
        BTreeMap::new_in(lock_or_exit_!(shared.reg_.allocator_(cancel.child_token())));
    let mut last_ready: Option<(Dock, Dock)> = Option::None;
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
        lock_or_exit_!(shared.reg_.drain_session_events_(cancel.child_token()));

        // 1. 先把**已经到达**的事件处理掉（非阻塞）。
        //
        // 这一步的顺序很关键：下面第 2 步会 park 在「最近通知过的那条发送环」上；
        // 若不在 park 之前排空事件队列，其它子流的 `Attach` / `Control`（例如它们
        // 的 `OPEN`）就会排在一条正在 park 的循环后面，形成死锁。单线程运行时下
        // 「检查队列」与「登记 park」之间没有 `await`，因此不存在竞态。
        if let Option::Some(event) = events.try_take_event_() {
            match race_cancel_(
                &cancel,
                handle_write_event_::<C, _>(
                        event,
                    &mut tx_stage,
                    &mut table,
                    &mut scratch,
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
                    fail_mux_loop_(&shared, &cancel, &err).await;
                    return;
                }
            }
            continue;
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
                    fail_mux_loop_(&shared, &cancel, &err).await;
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
                if let Option::Some(pair) = last_ready
                    && let Option::Some(entry) = table.get_mut(&pair)
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
                    fail_mux_loop_(&shared, &cancel, &err).await;
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
    shared: &MuxLoopShared_<C::Alloc>,
    read_events: &EventSender_<ReadEvent_<C::Buff, C::Alloc>>,
    cancel: &K,
    last_ready: &mut Option<(Dock, Dock)>,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
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
        WriteEvent_::RxConsumed {
            local_dock,
            remote_dock,
            amount_,
        } => {
            let pair = (local_dock, remote_dock);
            let Some(entry) = table.get_mut(&pair) else {
                return Result::Ok(());
            };
            let owner = entry.owner_.clone();
            let report = lock_or_fail_!(owner.with_mut_async_(cancel.child_token(), |state| {
                state.flow_mut_().recv_window_mut().on_consumed(amount_);
                state.touch_();
                let thresholds = state.thresholds_();
                if state
                    .flow_mut_()
                    .recv_window()
                    .should_report_with_(&thresholds)
                {
                    Option::Some(state.flow_mut_().recv_window_mut().report())
                } else {
                    Option::None
                }
            }));
            if let Option::Some(report) = report {
                send_window_update_::<C, _>(
                        tx_stage,
                    shared.max_packet_size_,
                    pair,
                    report,
                    cancel.child_token(),
                )
                .await?;
            }
        }
        WriteEvent_::TxClosed {
            local_dock,
            remote_dock,
        } => {
            let pair = (local_dock, remote_dock);
            // 把已缓存数据全部发完，再发 FIN。
            flush_entry_::<C, _>(
                tx_stage,
                shared,
                table,
                scratch,
                pair,
                cancel,
            )
            .await?;
            control_close_via_::<C, _>(
                tx_stage,
                shared.max_packet_size_,
                local_dock,
                remote_dock,
                false,
                cancel.child_token(),
            )
            .await?;
            if let Option::Some(entry) = table.get_mut(&pair) {
                lock_or_fail_!(entry
                    .owner_
                    .with_mut_async_(cancel.child_token(), |state| {
                        state.set_app_tx_closed_();
                        state.set_local_fin_sent_();
                    }));
            }
            table.remove(&pair);
            maybe_release_::<C, _>(
                shared,
                read_events,
                table,
                pair,
                cancel.child_token(),
            )
            .await?;
        }
        WriteEvent_::RxClosed {
            local_dock,
            remote_dock,
        } => {
            let pair = (local_dock, remote_dock);
            if let Option::Some(entry) = table.get_mut(&pair) {
                lock_or_fail_!(entry
                    .owner_
                    .with_mut_async_(cancel.child_token(), |state| {
                        state.set_app_rx_closed_();
                    }));
            }
            let _ = read_events.try_send_event_(ReadEvent_::Release {
                local_dock,
                remote_dock,
            });
            table.remove(&pair);
            control_close_via_::<C, _>(
                tx_stage,
                shared.max_packet_size_,
                local_dock,
                remote_dock,
                true,
                cancel.child_token(),
            )
            .await?;
            maybe_release_::<C, _>(
                shared,
                read_events,
                table,
                pair,
                cancel.child_token(),
            )
            .await?;
        }
        WriteEvent_::PeerClosed {
            local_dock,
            remote_dock,
            reset_,
        } => {
            let pair = (local_dock, remote_dock);
            if reset_ {
                // 对端不再接收：停止发送并释放该方向。
                table.remove(&pair);
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

/// 两个方向都收尾时释放注册表条目与接收环。
async fn maybe_release_<C, K>(
    shared: &MuxLoopShared_<C::Alloc>,
    read_events: &EventSender_<ReadEvent_<C::Buff, C::Alloc>>,
    table: &WriteTable_<C::Buff, C::Alloc>,
    pair: (Dock, Dock),
    cancel: K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
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
    let release = lock_or_fail_!(owner.with_mut_async_(cancel.child_token(), |state| {
        if !state.is_done_() {
            return false;
        }
        state.claim_release_()
    }));
    if release {
        let _ = shared
            .reg_
            .release_channel_(pair.0, pair.1, cancel.child_token())
            .await;
        let _ = read_events.try_send_event_(ReadEvent_::Release {
            local_dock: pair.0,
            remote_dock: pair.1,
        });
    }
    Result::Ok(())
}

/// 发送一条窗口更新（`WINDOW_UPDATE`）。
async fn send_window_update_<C, K>(
    tx_stage: &mut BufferedTx<C::StageBuff, C::Alloc>,
    max_packet_size: usize,
    pair: (Dock, Dock),
    report: WindowReport,
    cancel: K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let header = FrameHeader::new_(
        FrameKind::WindowUpdate,
        if report.is_reset() {
            flags::K_TOTAL_RESET
        } else {
            0u8
        },
        pair.0,
        pair.1,
        0usize,
        Option::Some((report.recv_total(), report.window())),
    );
    let Some(bytes) = encode_whole_frame_(&header, &[], max_packet_size)? else {
        return Result::Err(MuxError::FrameTooLarge);
    };
    enqueue_frame_::<C, _>(tx_stage, &bytes, cancel).await
}

/// 发一条 `CLOSE`。用独立的 helper 以便在事件处理里直接 await。
async fn control_close_via_<C, K>(
    tx_stage: &mut BufferedTx<C::StageBuff, C::Alloc>,
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
    write_control_blocking_::<C, _>( tx_stage, max_packet_size, &frame, &cancel).await
}

/// 把控制帧写进写环（分块；空间不足时由写泵持续搬运腾出空间）。
async fn write_control_blocking_<C, K>(
    tx_stage: &mut BufferedTx<C::StageBuff, C::Alloc>,
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
    let Some(bytes) = encode_whole_frame_(&header, frame.payload_(), max_packet_size)? else {
        return Result::Err(MuxError::FrameTooLarge);
    };
    enqueue_frame_::<C, _>(tx_stage, &bytes, cancel.child_token()).await
}

/// 把某条子流发送环里已提交的数据全部写出（直到取空）。
async fn flush_entry_<C, K>(
    tx_stage: &mut BufferedTx<C::StageBuff, C::Alloc>,
    shared: &MuxLoopShared_<C::Alloc>,
    table: &mut WriteTable_<C::Buff, C::Alloc>,
    scratch: &mut Owned<[u8], C::Alloc>,
    pair: (Dock, Dock),
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
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
    shared: &MuxLoopShared_<C::Alloc>,
    table: &mut WriteTable_<C::Buff, C::Alloc>,
    scratch: &mut Owned<[u8], C::Alloc>,
    cancel: &K,
) -> Result<bool, MuxError>
where
    C: TrConnCfg,
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
    shared: &MuxLoopShared_<C::Alloc>,
    table: &mut WriteTable_<C::Buff, C::Alloc>,
    scratch: &mut Owned<[u8], C::Alloc>,
    pair: (Dock, Dock),
    cancel: &K,
) -> Result<bool, MuxError>
where
    C: TrConnCfg,
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

    let available = lock_or_fail_!(owner.with_async_(cancel.child_token(), |state| state
        .flow_()
        .send_window()
        .available()));
    if available == 0 {
        return Result::Ok(false);
    }
    let want = core::cmp::min(available as usize, K_MAX_DATA_CHUNK);

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
    let moved = {
        let mut child = segm.as_segm_ref();
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
        unsafe { child.move_items_to_buff(uninit) }
    };
    if moved != take {
        // 段长度与搬出量应当一致；不一致说明上游语义变了。
        return Result::Err(MuxError::MalformedFrame);
    }

    // 整帧写进连接写环。空间在上面已经判过（保守预判保证够），因此这里**不会再**
    // 出现「字节已离开子流环、帧却没写出去」的状态。
    let Some(frame) = encode_whole_frame_(&header, &scratch[..moved], shared.max_packet_size_)?
    else {
        return Result::Err(MuxError::FrameTooLarge);
    };

    // 预扣窗口（`take <= available`，因此必定足额）。
    let granted = lock_or_fail_!(owner.with_mut_async_(cancel.child_token(), |state| state
        .flow_mut_()
        .send_window_mut()
        .reserve(take as Credit)));
    if granted < take as Credit {
        let _ = owner
            .with_mut_async_(cancel.child_token(), |state| {
                state.flow_mut_().send_window_mut().refund(granted)
            })
            .await;
        return Result::Ok(false);
    }

    if let Result::Err(err) =
        enqueue_frame_::<C, _>(tx_stage, &frame, token.child_token()).await
    {
        // 写环已结束：把预扣的窗口退回去，再按连接级失败处理。
        let _ = owner
            .with_mut_async_(cancel.child_token(), |state| {
                state.flow_mut_().send_window_mut().refund(take as Credit)
            })
            .await;
        return Result::Err(err);
    }
    let _ = owner
        .with_mut_async_(cancel.child_token(), |state| state.touch_())
        .await;
    Result::Ok(true)
}
