//! 连接内部的**读循环**与**写循环**。
//!
//! 本模块是经 `abs_art` 的本地作用域 spawn 出来的两个 `'static` 任务的全部实现。
//! 结构见 [`crate::connection`] 模块文档 §2，落地裁决见
//! `dev-notes/connection-20261002-0548.md` §5（Q3 双循环 / Q4 唤醒 / Q5 流控 /
//! Q7 半部移交）。
//!
//! # 数据流
//!
//! ```text
//! 应用 --写--> 发送环 --读端(移交)--> 写循环 --成帧--> 网络 Tx
//! 网络 Rx --解复用--> 读循环 --写端(移交)--> 接收环 --读--> 应用
//! ```
//!
//! # 循环不持有连接核心
//!
//! 两个循环只拿 [`LoopShared_`]——注册表句柄 + 三个标量——**不持有
//! [`MuxCore`](super::mux_connection::core_) 的强引用**（因此也不持有
//! [`MuxConnection`](super::MuxConnection)）。
//!
//! 这不是随手的选择：若循环持有核心，核心将永远无法析构，其 `Drop` 里的取消令牌
//! 永远不会触发，两个任务与整个连接状态会永久泄漏。现在的形状是——最后一个
//! **应用面**对象（连接、四个句柄、两个半部之一）被丢弃 ⇒ 核心析构 ⇒ 取消两个
//! 令牌 ⇒ 循环在下一个 await 点退出。因此两个循环的 **park 点必须都能被取消令牌
//! 唤醒**：读循环经 `may_cancel_with`，写循环的每处 park 都与
//! `cancellation()` 竞争（见下）。
//!
//! # 半部为什么是「移交」而不是共享
//!
//! `buffex` 的段借用绑定在环半部上，半部若藏在共享单元的锁后面，段就不能跨
//! `await` 使用（锁内的闭包不允许 `await`）。因此建流方在创建环之后，把**会话侧**
//! 的两个半部交给对应循环，循环把它们放进**本地表**（`BTreeMap`，节点用调用方
//! 注入的分配器分配），此后可以自由在这些半部上 park / await。
//!
//! # 两个循环各自的 park 点
//!
//! - 读循环：`read_header_async_`（网络读）与「接收环满时的 `write_async`」；
//!   两者都经 `may_cancel_with` 挂在取消令牌上，因此连接被丢弃时能立刻退出。
//!   事件队列只在其醒来后**非阻塞排空**——`Attach` 必然先于对端的数据帧到达
//!   （建流方先发 `Attach` 事件、后发 `OPEN`），所以「先排空事件、再派发帧」即可
//!   保证不丢；
//! - 写循环：无数据可发时优先 park 在**最近一次收到 `TxReady` 的那条发送环**上
//!   （Q4 裁决的兜底，覆盖「应用还没提交就发了事件」的竞态），否则 park 在事件
//!   通道上。**这两个 park 都必须同时与事件通道 / 取消令牌竞争**：只 park 在环上
//!   会让别的子流刚入队的事件（例如某条子流的 `TxClosed`）一直排在后面，多子流下
//!   就是死锁；不与取消令牌竞争则连接被丢弃后循环无法退出。实现见
//!   `write_loop_async_` 第 2、3 步里的 `poll_fn`。
//!
//! 另有一条容易踩的约束：循环里**借出环数据一律用非阻塞的 `try_read`**
//! （`drain_one_`）。`read_async` 在空环上会 park，一旦在「排空数据」这一步 park，
//! 事件就再也送不进来了。
//!
//! # 已知限制（Q4）
//!
//! 「通知在进入写路径时发出、真正提交在段 drop 时」这个时间差由上面的兜底覆盖：
//! 写循环 park 到刚通知的那条环，用环自身的提交唤醒补上。残留漏洞（两条环都通知
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
    x_deps::abs_cancel,
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use buffex::x_deps::abs_buff;
use mm_ptr::Owned;

use crate::{
    connection::{
        Dock, FrameHeader, FrameKind, MuxError, TrConnCfg, flags,
        frame_::read_header_async_,
        mux_connection::{ChannelRegistry_, ReserveErr_},
        owner_::ChannelOwner_,
        ring_::{BufferedRx, BufferedTx},
        signal_::{
            ControlFrame_, EventReceiver_, EventSender_, ReadEvent_, TrEventReceiver_,
            TrEventSender_, WriteEvent_,
        },
    },
    flow_ctrl::{Credit, WindowReport},
    wire_io_::{CursorError, ReadCursor, write_all_async_},
};

/// 单条子流数据帧的载荷上限（一次成帧最多携带多少字节）。
///
/// 与 `max_packet_size` 无关：后者是**帧总长**上限，这里只是想避免一次借出过大的
/// 段而让其他子流等太久（公平性，见 `connection-20260919-1631.md` §5.2）。
const K_MAX_DATA_CHUNK: usize = 16usize * 1024usize;

/// 两个循环共享的、与调用方配置无关的量，也是**循环持有的全部连接状态**。
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
pub(crate) struct LoopShared_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 注册表（dock / 子流索引与配额、失败标志、取消令牌）。
    reg_: ChannelRegistry_<A>,

    /// 协商出的单帧总长上限。
    max_packet_size_: usize,
}

impl<A> LoopShared_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 由建连路径展开后的量构造（成员私有，构造只能走这里）。
    pub(crate) fn new_(reg: ChannelRegistry_<A>, max_packet_size: usize) -> Self {
        LoopShared_ {
            reg_: reg,
            max_packet_size_: max_packet_size,
        }
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

/// 读循环本地持有的一条子流：共享状态 + 会话侧**接收环写端**。
///
/// `local_dock` / `remote_dock` 不再是字段：dock 对已经是所在表的键。
struct ReadEntry_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + 'static,
    B: BorrowMut<[MaybeUninit<u8>]> + 'static,
    A: AllocatorClone + Send + Sync + 'static,
{
    owner_: ChannelOwner_<A>,
    writer_: BufferedTx<B, A>,
}

/// 写循环本地持有的一条子流：共享状态 + 会话侧**发送环读端**。
struct WriteEntry_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + 'static,
    B: BorrowMut<[MaybeUninit<u8>]> + 'static,
    A: AllocatorClone + Send + Sync + 'static,
{
    owner_: ChannelOwner_<A>,
    reader_: BufferedRx<B, A>,
}

/// 读循环的本地表：dock 对 → 该子流的接收环写端与共享状态。
///
/// 用 `BTreeMap` 而不是手写单链表：链表的 `find` / `remove` 是 O(n)，且每次
/// 增删都要自己用分配器构造 / 释放节点；`BTreeMap` 直接以调用方注入的分配器
/// （`allocator_api` 的 `new_in`）承担这些分配，查找降到 O(log n)。
type ReadTable_<B, A> = BTreeMap<(Dock, Dock), ReadEntry_<B, A>, A>;

/// 写循环的本地表：dock 对 → 该子流的发送环读端与共享状态。
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

/// 在「取消令牌触发」与「给定 future 完成」之间竞争：取消先到返回 `None`。
///
/// # 为什么循环需要它
///
/// `may_cancel_with` 只是把令牌交给内层 future（内层在被 poll 时才检查
/// `is_cancelled`），**是否在取消时唤醒 park 中的 future 取决于那一层的实现**：
/// 例如 `buffex` 的环半部会把 `cancellation()` 存进自己的 park 状态、并在取消时
/// 唤醒，而一个只做转发的传输适配层不会。循环的收尾不能建立在这条隐含契约上——
/// 一旦某一层的 park 不被唤醒，循环就再也看不到取消令牌，任务与传输永久泄漏。
///
/// 因此循环把**每一次可能长期 park 的 await**（网络读、网络写、环写满）都放到
/// 这里，与 `cancellation()`（它保证登记 waker）竞争：「丢弃连接即关闭连接」由此
/// 与传输的实现细节无关。
async fn race_cancel_<F, K>(cancel: &K, fut: F) -> Option<F::Output>
where
    F: core::future::Future,
    K: TrCancellationToken,
{
    let mut fut = core::pin::pin!(fut);
    let mut cancelled = core::pin::pin!(cancel.child_token().cancellation());
    poll_fn(|cx| {
        if core::future::Future::poll(cancelled.as_mut(), cx).is_ready() {
            return Poll::Ready(Option::None);
        }
        core::future::Future::poll(fut.as_mut(), cx).map(Option::Some)
    })
    .await
}

/// 循环侧的连接级失败处理：**取消导致的收尾不算失败**。
///
/// 连接被丢弃时 [`MuxCore::drop`](super::mux_connection::core_::MuxCore) 触发取消
/// 令牌，循环在 await 点上以「取消错误」的形式收到通知并退出——那是正常关闭，
/// 不应在注册表上留下失败标志（否则收尾路径会伪造出一个假的连接级失败）。
async fn fail_loop_<A, K>(shared: &LoopShared_<A>, cancel: &K, err: &MuxError)
where
    A: AllocatorClone + Send + Sync,
    K: TrCancellationToken,
{
    if !cancel.is_cancelled() {
        let _ = shared.reg_.mark_failed_(err, cancel.child_token()).await;
    }
}

/// 读循环：把 `bytes` 全部写进接收环（环满时 park，等应用消费）。
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

/// 把帧头与载荷依次写出。
async fn write_frame_<C, K>(
    tx: &mut C::ConnTx,
    header: &FrameHeader,
    payload: &[u8],
    cancel: K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    crate::connection::frame_::write_header_async_(tx, header, cancel.child_token()).await?;
    if !payload.is_empty() {
        write_all_async_(tx, payload, cancel.child_token())
            .await
            .map_err(map_write_cursor_err_)?;
    }
    Result::Ok(())
}

/// 把控制帧编码后写出。
async fn write_control_<C, K>(
    tx: &mut C::ConnTx,
    frame: &ControlFrame_,
    cancel: K,
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
    write_frame_::<C, _>(tx, &header, frame.payload_(), cancel).await
}

/// 把**读侧**游标错误映射为连接错误。
fn map_read_cursor_err_<E>(err: CursorError<E, ()>) -> MuxError {
    match err {
        CursorError::Read(_) => MuxError::Transport { write: false },
        CursorError::Write(()) => MuxError::Transport { write: true },
        CursorError::PeerClosed => MuxError::PeerClosed,
    }
}

/// 把**写侧**游标错误映射为连接错误；语义与 [`map_read_cursor_err_`] 对称。
fn map_write_cursor_err_<E>(err: CursorError<(), E>) -> MuxError {
    match err {
        CursorError::Write(_) => MuxError::Transport { write: true },
        CursorError::Read(()) => MuxError::Transport { write: false },
        CursorError::PeerClosed => MuxError::PeerClosed,
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 读循环
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 读循环：解复用网络帧、投递载荷、推进建流状态机。
///
/// `scratch` 是连接建立时分配一次的载荷暂存（长度 = `max_packet_size`，由调用方
/// 注入的分配器分配）。
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_loop_async_<C, K>(
    mut rx: C::ConnRx,
    shared: LoopShared_<C::Alloc>,
    mut events: EventReceiver_<ReadEvent_<C::Buff, C::Alloc>>,
    events_tx: EventSender_<WriteEvent_<C::Buff, C::Alloc>>,
    mut scratch: Owned<[u8], C::Alloc>,
    cancel: K,
) where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let mut table: ReadTable_<C::Buff, C::Alloc> =
        BTreeMap::new_in(lock_or_exit_!(shared.reg_.allocator_(cancel.child_token())));

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

        // 2. 读一个帧头（park 在网络读上，并与取消令牌竞争）。
        let header = match race_cancel_(
            &cancel,
            read_header_async_::<_, _>(&mut rx, cancel.child_token()),
        )
        .await
        {
            Option::None => return,
            Option::Some(Result::Ok(header)) => header,
            Option::Some(Result::Err(err)) => {
                fail_loop_(&shared, &cancel, &err).await;
                return;
            }
        };

        // 3. 读到帧头之后再排空一次：`Attach` 可能就在这段时间里到齐，
        //    而该帧正是它要送进去的那条子流的数据。
        drain_read_events_::<C>(&mut events, &mut table);

        // 4. 载荷长度校验。
        let len = header.payload_len();
        if len > shared.max_packet_size_ || len > scratch.len() {
            fail_loop_(&shared, &cancel, &MuxError::FrameTooLarge).await;
            return;
        }
        if len > 0 {
            let mut cursor = ReadCursor::new_(&mut rx);
            match race_cancel_(
                &cancel,
                cursor.read_async_(&mut scratch[..len], cancel.child_token()),
            )
            .await
            {
                Option::None => return,
                Option::Some(Result::Ok(())) => {}
                Option::Some(Result::Err(err)) => {
                    fail_loop_(&shared, &cancel, &map_read_cursor_err_::<_>(err)).await;
                    return;
                }
            }
        }
        let payload = &scratch[..len];

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
                    fail_loop_(&shared, &cancel, &MuxError::MalformedFrame).await;
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
                    fail_loop_(&shared, &cancel, &MuxError::FlowCtrl(err)).await;
                    return;
                }
                match race_cancel_(
                    &cancel,
                    write_into_ring_(&mut entry.writer_, payload, cancel.child_token()),
                )
                .await
                {
                    Option::None => return,
                    Option::Some(Result::Ok(())) => {}
                    Option::Some(Result::Err(_err)) => {
                        fail_loop_(
                            &shared,
                            &cancel,
                            &MuxError::Transport { write: true },
                        )
                        .await;
                        return;
                    }
                }
            }
            FrameKind::Open => {
                let Some(report) = window_report_of_(&header) else {
                    fail_loop_(&shared, &cancel, &MuxError::MalformedFrame).await;
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
                    match shared
                        .reg_
                        .reserve_inbound_(local, remote, report, cancel.child_token())
                        .await
                    {
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

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 写循环
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 写循环：按对端发送窗口调度各子流的发送环，并写出控制帧。
pub(crate) async fn write_loop_async_<C, K>(
    mut tx: C::ConnTx,
    shared: LoopShared_<C::Alloc>,
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
    // 循环独占的载荷暂存：把环段的字节搬进来（**搬出即消费**，见 `drain_one_`），
    // 再写上网。整条连接只分配一次，且走调用方注入的分配器（`mm_ptr::Owned`）。
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
        // 件事，是为了让释放不必等某次 API 操作：两个循环任一被调度即可推进。
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
                    &mut tx,
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
                    fail_loop_(&shared, &cancel, &err).await;
                    return;
                }
            }
            continue;
        }

        // 1. 尽量把各子流的数据发出去（一次一段），直到没有可发的。
        loop {
            match race_cancel_(
                &cancel,
                drain_once_::<C, _>(&mut tx, &mut table, &mut scratch, &cancel),
            )
            .await
            {
                Option::None => return,
                Option::Some(Result::Ok(true)) => continue,
                Option::Some(Result::Ok(false)) => break,
                Option::Some(Result::Err(err)) => {
                    fail_loop_(&shared, &cancel, &err).await;
                    return;
                }
            }
        }

        // 2. 没有可发数据：若「最近通知过的那条发送环」就是目标，就 park 在它上面
        //    （Q4 裁决的兜底：应用可能在提交之前就发出了事件）。
        //
        //    **必须同时轮询事件通道与取消令牌**：只 park 在环上会让「别的子流刚入队
        //    的事件」一直排在一个睡着的循环后面（多子流下就是死锁——某条环的发送方
        //    在等对端的 FIN，而它的 FIN 事件排在队列里没人处理）；不轮询取消令牌则
        //    连接被丢弃后循环无法退出（见模块文档「循环不持有连接核心」）。
        if let Option::Some(pair) = last_ready {
            // 这个块把 `entry`（借自 `table`）与事件通道的竞争限制在内部，
            // 出块后 `table` 的可变借用结束，才能交给 `handle_write_event_`。
            let mut taken: Option<WriteEvent_<C::Buff, C::Alloc>> = Option::None;
            let mut ring_ready = false;
            let alive = {
                let Option::Some(entry) = table.get_mut(&pair) else {
                    last_ready = Option::None;
                    continue;
                };
                let demand = Demand::at_least(1usize);
                let ring_fut = entry
                    .reader_
                    .read_async(&demand)
                    .may_cancel_with(cancel.child_token());
                let cancel_fut = cancel.child_token().cancellation();
                let mut ring_fut = core::pin::pin!(ring_fut);
                let mut event_fut = core::pin::pin!(events.take_event_async_());
                let mut cancel_fut = core::pin::pin!(cancel_fut);
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
                    match core::future::Future::poll(ring_fut.as_mut(), cx) {
                        Poll::Ready(_outcome) => {
                            ring_ready = true;
                            Poll::Ready(true)
                        }
                        Poll::Pending => Poll::Pending,
                    }
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
                        &mut tx,
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
                        fail_loop_(&shared, &cancel, &err).await;
                        return;
                    }
                }
                continue;
            }
            if ring_ready {
                // 段在 `ring_fut` 里被 drop（不消费）：回顶部由 `drain_once_` 正式取走。
                continue;
            }
            last_ready = Option::None;
            continue;
        }

        // 3. 否则 park 在事件通道上（同样与取消令牌竞争：核心析构时事件发送端也会
        //    随之消失，但不能把退出时机寄托在「发送端都死了」这个间接条件上）。
        let event = {
            let cancel_fut = cancel.child_token().cancellation();
            let mut event_fut = core::pin::pin!(events.take_event_async_());
            let mut cancel_fut = core::pin::pin!(cancel_fut);
            poll_fn(|cx| {
                if core::future::Future::poll(cancel_fut.as_mut(), cx).is_ready() {
                    return Poll::Ready(Option::None);
                }
                core::future::Future::poll(event_fut.as_mut(), cx)
            })
            .await
        };
        let Option::Some(event) = event else {
            return;
        };
        match race_cancel_(
            &cancel,
            handle_write_event_::<C, _>(
                event,
                &mut tx,
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
                fail_loop_(&shared, &cancel, &err).await;
                return;
            }
        }
    }
}

/// 处理一条写事件；返回 `Err` 表示连接级失败（调用方负责 `mark_failed_` 并退出）。
#[allow(clippy::too_many_arguments)]
async fn handle_write_event_<C, K>(
    event: WriteEvent_<C::Buff, C::Alloc>,
    tx: &mut C::ConnTx,
    table: &mut WriteTable_<C::Buff, C::Alloc>,
    scratch: &mut Owned<[u8], C::Alloc>,
    shared: &LoopShared_<C::Alloc>,
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
            write_control_::<C, _>(tx, &frame_, cancel.child_token()).await?;
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
                send_window_update_::<C, _>(tx, pair, report, cancel.child_token()).await?;
            }
        }
        WriteEvent_::TxClosed {
            local_dock,
            remote_dock,
        } => {
            let pair = (local_dock, remote_dock);
            // 把已缓存数据全部发完，再发 FIN。
            flush_entry_::<C, _>(tx, table, scratch, pair, cancel).await?;
            control_close_via_::<C, _>(tx, local_dock, remote_dock, false, cancel.child_token()).await?;
            if let Option::Some(entry) = table.get_mut(&pair) {
                lock_or_fail_!(entry
                    .owner_
                    .with_mut_async_(cancel.child_token(), |state| {
                        state.set_app_tx_closed_();
                        state.set_local_fin_sent_();
                    }));
            }
            table.remove(&pair);
            maybe_release_::<C, _>(shared, read_events, table, pair, cancel.child_token()).await?;
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
            control_close_via_::<C, _>(tx, local_dock, remote_dock, true, cancel.child_token()).await?;
            maybe_release_::<C, _>(shared, read_events, table, pair, cancel.child_token()).await?;
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
            maybe_release_::<C, _>(shared, read_events, table, pair, cancel.child_token()).await?;
        }
    }
    Result::Ok(())
}

/// 两个方向都收尾时释放注册表条目与接收环。
async fn maybe_release_<C, K>(
    shared: &LoopShared_<C::Alloc>,
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
    tx: &mut C::ConnTx,
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
    write_frame_::<C, _>(tx, &header, &[], cancel).await
}

/// 发一条 `CLOSE`。用独立的 helper 以便在事件处理里直接 await。
async fn control_close_via_<C, K>(
    tx: &mut C::ConnTx,
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
    write_control_::<C, _>(tx, &frame, cancel).await
}

/// 把某条子流发送环里已提交的数据全部写出（直到取空）。
async fn flush_entry_<C, K>(
    tx: &mut C::ConnTx,
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
        let progressed = drain_one_::<C, _>(&mut *tx, table, scratch, pair, cancel).await?;
        if !progressed {
            return Result::Ok(());
        }
    }
}

/// 轮转一遍本地表，最多写出一段数据；返回是否有进展。
async fn drain_once_<C, K>(
    tx: &mut C::ConnTx,
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
        if drain_one_::<C, _>(tx, table, scratch, pair, cancel).await? {
            return Result::Ok(true);
        }
    }
}

/// 尝试为 `pair` 写出一段数据；返回是否写出。
async fn drain_one_<C, K>(
    tx: &mut C::ConnTx,
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

    let header = FrameHeader::new_(
        FrameKind::Data,
        0u8,
        pair.0,
        pair.1,
        take,
        Option::None,
    );
    crate::connection::frame_::write_header_async_(tx, &header, token.child_token()).await?;

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
        let _ = owner
            .with_mut_async_(cancel.child_token(), |state| {
                state.flow_mut_().send_window_mut().refund(take as Credit)
            })
            .await;
        return Result::Err(MuxError::MalformedFrame);
    }
    write_all_async_(tx, &scratch[..moved], token.child_token())
        .await
        .map_err(map_write_cursor_err_)?;
    // `outcome` 在此 drop：提交消费，唤醒环的写端（若有 park 者）。
    let _ = owner
        .with_mut_async_(cancel.child_token(), |state| state.touch_())
        .await;
    Result::Ok(true)
}
