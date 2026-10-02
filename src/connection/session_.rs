//! 连接内部的**读循环**与**写循环**。
//!
//! 本模块是 `abs_art` spawn 出来的两个 `'static` 任务的全部实现。结构见
//! [`crate::connection`] 模块文档 §2，落地裁决见
//! `dev-notes/connection-20261002-0548.md` §6.5（Q3 双循环 / Q4 唤醒 / Q5 流控 /
//! Q7 半部移交）。
//!
//! # 数据流
//!
//! ```text
//! 应用 --写--> 发送环 --读端(移交)--> 写循环 --成帧--> 网络 Tx
//! 网络 Rx --解复用--> 读循环 --写端(移交)--> 接收环 --读--> 应用
//! ```
//!
//! # 半部为什么是「移交」而不是共享
//!
//! `buffex` 的段借用绑定在环半部上，半部若藏在共享单元的锁后面，段就不能跨
//! `await` 使用（锁内的闭包不允许 `await`）。因此建流方在创建环之后，把**会话侧**
//! 的两个半部经事件通道送给对应循环，循环把它们放进**本地表**（节点用调用方注入
//! 的分配器分配），此后可以自由在这些半部上 park / await。
//!
//! # 两个循环各自的 park 点
//!
//! - 读循环：`read_header_async_`（网络读）与「接收环满时的 `write_async`」；
//!   事件队列只在其醒来后**非阻塞排空**——`Attach` 必然先于对端的数据帧到达
//!   （建流方先发 `Attach` 事件、后发 `OPEN`），所以「先排空事件、再派发帧」即可
//!   保证不丢；
//! - 写循环：无数据可发时优先 park 在**最近一次收到 `TxReady` 的那条发送环**上
//!   （Q4 裁决的兜底，覆盖「应用还没提交就发了事件」的竞态），否则 park 在事件
//!   通道上。**这个 park 必须同时与事件通道竞争**：只 park 在环上会让别的子流
//!   刚入队的事件（例如某条子流的 `TxClosed`）一直排在后面，多子流下就是死锁。
//!   实现见 `write_loop_async_` 第 2 步里的 `poll_fn`。
//!
//! 另有一条容易踩的约束：循环里**借出环数据一律用非阻塞的 `try_read`**
//! （`drain_one_`）。`read_async` 在空环上会 park，一旦在「排空数据」这一步 park，
//! 事件就再也送不进来了。
//!
//! # 已知限制（Q4）
//!
//! 「通知在进入写路径时发出、真正提交在段 drop 时」这个时间差由上面的兜底覆盖：
//! 写循环 park 到刚通知的那条环，用环自身的提交唤醒补上。残留漏洞（两条环都通知
//! 且都不提交时，先通知的那条可能永远不被轮询）见 dev-notes §6.5.1，属**已知限制**。
//!
//! # 拆流
//!
//! 每个方向独立收尾，两个方向都收尾后释放注册表条目（`ChannelState_::is_done_`）。

// 多线程配置下 `MuxConnection::new` 仍是 `todo!()`（见 dev-notes），读写循环整块
// 暂时不可达，因此**仅在该配置下**允许 dead_code；缺省（单线程）配置不放开，
// 保持零告警。多线程驱动落地后请连同本行一起移除。
#![cfg_attr(feature = "multi-thread", allow(dead_code))]

use core::{
    alloc::AllocatorClone,
    borrow::BorrowMut,
    future::poll_fn,
    mem::MaybeUninit,
    task::Poll,
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::TrBuffSegmMut,
    x_deps::abs_cancel,
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use buffex::x_deps::abs_buff;
use mm_ptr::Owned;

use crate::{
    connection::{
        Dock, FrameHeader, FrameKind, MuxError, flags,
        frame_::read_header_async_,
        mux_connection::ChannelRegistry_,
        owner_::ChannelOwner_,
        ring_::{BufferedRx, BufferedTx},
        signal_::{
            ControlFrame_, EventReceiver_, EventSender_, ReadEvent_, TrEventReceiver_,
            TrEventSender_, WriteEvent_,
        },
    },
    flow_ctrl::{Credit, RecvTotal, ReportThresholds_, WindowReport},
    wire_io_::{CursorError, ReadCursor, write_all_async_},
};

/// 单条子流数据帧的载荷上限（一次成帧最多携带多少字节）。
///
/// 与 `max_packet_size` 无关：后者是**帧总长**上限，这里只是想避免一次借出过大的
/// 段而让其他子流等太久（公平性，见 `connection-20260919-1631.md` §5.2）。
const K_MAX_DATA_CHUNK: usize = 16usize * 1024usize;

/// 两个循环共享的、与调用方配置无关的量。
///
/// 连接建立时把策略展开成 [`ReportThresholds_`]，此后循环不再需要 `C` / `P`。
#[derive(Clone)]
pub(crate) struct LoopShared_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 注册表（dock / 子流索引与配额、失败标志、取消令牌）。
    reg_: ChannelRegistry_<A>,

    /// 本端建流时通告的初始接收窗口（被动方回 `OPEN` 用）。
    initial_window_: Credit,

    /// 通告判定的阈值快照。
    thresholds_: ReportThresholds_,

    /// 协商出的单帧总长上限。
    max_packet_size_: usize,
}

impl<A> LoopShared_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 由建连路径展开后的量构造（成员私有，构造只能走这里）。
    pub(crate) fn new_(
        reg: ChannelRegistry_<A>,
        initial_window: Credit,
        thresholds: ReportThresholds_,
        max_packet_size: usize,
    ) -> Self {
        LoopShared_ {
            reg_: reg,
            initial_window_: initial_window,
            thresholds_: thresholds,
            max_packet_size_: max_packet_size,
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 循环的本地表
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 读循环本地持有的一条子流：共享状态 + 会话侧**接收环写端**。
struct ReadEntry_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
{
    local_dock_: Dock,
    remote_dock_: Dock,
    owner_: ChannelOwner_<A>,
    writer_: BufferedTx<B, A>,
    next_: Option<Owned<ReadEntry_<B, A>, A>>,
}

/// 写循环本地持有的一条子流：共享状态 + 会话侧**发送环读端**。
struct WriteEntry_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
{
    local_dock_: Dock,
    remote_dock_: Dock,
    owner_: ChannelOwner_<A>,
    reader_: BufferedRx<B, A>,
    next_: Option<Owned<WriteEntry_<B, A>, A>>,
}

fn find_read_mut_<B, A>(
    head: &mut Option<Owned<ReadEntry_<B, A>, A>>,
    pair: (Dock, Dock),
) -> Option<&mut ReadEntry_<B, A>>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
{
    let mut cursor = head.as_mut();
    while let Option::Some(entry) = cursor {
        if entry.local_dock_ == pair.0 && entry.remote_dock_ == pair.1 {
            return Option::Some(entry);
        }
        cursor = entry.next_.as_mut();
    }
    Option::None
}

fn remove_read_<B, A>(
    head: &mut Option<Owned<ReadEntry_<B, A>, A>>,
    pair: (Dock, Dock),
) -> bool
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
{
    let remove_head = match head.as_ref() {
        Option::Some(entry) => entry.local_dock_ == pair.0 && entry.remote_dock_ == pair.1,
        Option::None => false,
    };
    if remove_head {
        let next = head.as_mut().and_then(|entry| entry.next_.take());
        *head = next;
        return true;
    }
    let mut cursor = head.as_mut();
    while let Option::Some(entry) = cursor {
        let remove_next = match entry.next_.as_ref() {
            Option::Some(next) => next.local_dock_ == pair.0 && next.remote_dock_ == pair.1,
            Option::None => false,
        };
        if remove_next {
            let next_next = entry.next_.as_mut().and_then(|next| next.next_.take());
            entry.next_ = next_next;
            return true;
        }
        cursor = entry.next_.as_mut();
    }
    false
}

fn find_write_mut_<B, A>(
    head: &mut Option<Owned<WriteEntry_<B, A>, A>>,
    pair: (Dock, Dock),
) -> Option<&mut WriteEntry_<B, A>>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
{
    let mut cursor = head.as_mut();
    while let Option::Some(entry) = cursor {
        if entry.local_dock_ == pair.0 && entry.remote_dock_ == pair.1 {
            return Option::Some(entry);
        }
        cursor = entry.next_.as_mut();
    }
    Option::None
}

fn remove_write_<B, A>(
    head: &mut Option<Owned<WriteEntry_<B, A>, A>>,
    pair: (Dock, Dock),
) -> bool
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
{
    let remove_head = match head.as_ref() {
        Option::Some(entry) => entry.local_dock_ == pair.0 && entry.remote_dock_ == pair.1,
        Option::None => false,
    };
    if remove_head {
        let next = head.as_mut().and_then(|entry| entry.next_.take());
        *head = next;
        return true;
    }
    let mut cursor = head.as_mut();
    while let Option::Some(entry) = cursor {
        let remove_next = match entry.next_.as_ref() {
            Option::Some(next) => next.local_dock_ == pair.0 && next.remote_dock_ == pair.1,
            Option::None => false,
        };
        if remove_next {
            let next_next = entry.next_.as_mut().and_then(|next| next.next_.take());
            entry.next_ = next_next;
            return true;
        }
        cursor = entry.next_.as_mut();
    }
    false
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 小工具
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 唤醒建流等待者（`open_channel_async`）；唤醒在释放锁之后进行。
fn wake_establish_<A>(owner: &ChannelOwner_<A>)
where
    A: AllocatorClone + Send + Sync,
{
    let waker = owner.with_mut_(|state| state.take_establish_waker_());
    if let Option::Some(waker) = waker {
        waker.wake();
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
async fn write_frame_<W, K>(
    tx: &mut W,
    header: &FrameHeader,
    payload: &[u8],
    cancel: K,
) -> Result<(), MuxError<(), W::Err>>
where
    W: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    crate::connection::frame_::write_header_async_(tx, header, cancel.child_token()).await?;
    if !payload.is_empty() {
        write_all_async_(tx, payload, cancel.child_token())
            .await
            .map_err(map_cursor_err_)?;
    }
    Result::Ok(())
}

/// 把控制帧编码后写出。
async fn write_control_<W, K>(
    tx: &mut W,
    frame: &ControlFrame_,
    cancel: K,
) -> Result<(), MuxError<(), W::Err>>
where
    W: TrBuffWrite<u8>,
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
    write_frame_(tx, &header, frame.payload_(), cancel).await
}

/// 把游标错误映射为 [`MuxError`]。
fn map_cursor_err_<RE, WE>(err: CursorError<RE, WE>) -> MuxError<RE, WE> {
    match err {
        CursorError::Read(err) => MuxError::Rx(err),
        CursorError::Write(err) => MuxError::Tx(err),
        CursorError::PeerClosed => MuxError::PeerClosed,
    }
}

/// 控制帧发送助手：`OPEN`（携带本端接收窗口）。
fn send_open_<B, A>(
    events: &EventSender_<WriteEvent_<B, A>>,
    local_dock: Dock,
    remote_dock: Dock,
    window: Credit,
    payload: Vec<u8>,
) where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
{
    let _ = events.try_send_event_(WriteEvent_::Control {
        frame_: ControlFrame_::with_window_(
            FrameKind::Open,
            0u8,
            local_dock,
            remote_dock,
            Option::Some((0u64 as RecvTotal, window)),
            payload,
        ),
    });
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 读循环
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 读循环：解复用网络帧、投递载荷、推进建流状态机。
///
/// `scratch` 是连接建立时分配一次的载荷暂存（长度 = `max_packet_size`）。
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_loop_async_<R, B, A, K>(
    mut rx: R,
    shared: LoopShared_<A>,
    mut events: EventReceiver_<ReadEvent_<B, A>>,
    events_tx: EventSender_<WriteEvent_<B, A>>,
    mut scratch: Vec<u8>,
    cancel: K,
) where
    R: TrBuffRead<u8>,
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
    K: TrCancellationToken,
{
    let mut table: Option<Owned<ReadEntry_<B, A>, A>> = Option::None;

    loop {
        if cancel.is_cancelled() {
            return;
        }
        // 1. 先把挂起的 Attach / Release 排空。
        drain_read_events_(&mut events, &mut table, &shared);

        // 2. 读一个帧头（park 在网络读上）。
        let header = match read_header_async_(&mut rx, cancel.child_token()).await {
            Result::Ok(header) => header,
            Result::Err(err) => {
                shared.reg_.mark_failed_(&err);
                return;
            }
        };

        // 3. 读到帧头之后再排空一次：`Attach` 可能就在这段时间里到齐，
        //    而该帧正是它要送进去的那条子流的数据。
        drain_read_events_(&mut events, &mut table, &shared);

        // 4. 载荷长度校验。
        let len = header.payload_len();
        if len > shared.max_packet_size_ || len > scratch.len() {
            shared
                .reg_
                .mark_failed_(&MuxError::<R::Err, ()>::FrameTooLarge);
            return;
        }
        if len > 0 {
            let mut cursor = ReadCursor::new_(&mut rx);
            if let Result::Err(err) = cursor
                .read_async_(&mut scratch[..len], cancel.child_token())
                .await
            {
                shared.reg_.mark_failed_(&map_cursor_err_(err));
                return;
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
                let Some(entry) = find_read_mut_(&mut table, pair) else {
                    // 未知子流的数据帧：协议违例。
                    shared
                        .reg_
                        .mark_failed_(&MuxError::<R::Err, ()>::MalformedFrame);
                    return;
                };
                entry.owner_.with_mut_(|state| {
                    state.touch_();
                });
                let counted = entry
                    .owner_
                    .with_mut_(|state| state.flow_mut_().recv_window_mut().on_data(amount));
                if let Result::Err(err) = counted {
                    shared
                        .reg_
                        .mark_failed_(&MuxError::<R::Err, ()>::FlowCtrl(err));
                    return;
                }
                if let Result::Err(err) =
                    write_into_ring_(&mut entry.writer_, payload, cancel.child_token()).await
                {
                    shared
                        .reg_
                        .mark_failed_(&MuxError::<R::Err, ()>::Transport { write: true });
                    let _ = err;
                    return;
                }
            }
            FrameKind::Open => {
                let Some(report) = window_report_of_(&header) else {
                    shared
                        .reg_
                        .mark_failed_(&MuxError::<R::Err, ()>::MalformedFrame);
                    return;
                };
                if let Option::Some(entry) = find_read_mut_(&mut table, pair) {
                    // 本端主动发起的子流：这是对端回的 `OPEN`，带上它的接收窗口。
                    let owner = entry.owner_.clone();
                    owner.with_mut_(|state| {
                        let _ = state.flow_mut_().send_window_mut().on_report(report);
                        state.set_peer_opened_();
                        state.touch_();
                    });
                    wake_establish_(&owner);
                } else {
                    // 入向请求：登记并回一条自己的 `OPEN`（通告接收窗口）。
                    match shared.reg_.reserve_inbound_(local, remote, report) {
                        Result::Ok(()) => {
                            send_open_(
                                &events_tx,
                                local,
                                remote,
                                shared.initial_window_,
                                Vec::new(),
                            );
                        }
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
                if let Option::Some(entry) = find_read_mut_(&mut table, pair) {
                    let owner = entry.owner_.clone();
                    let outcome = if header.kind() == FrameKind::Accept {
                        crate::connection::owner_::EstablishOutcome_::Accepted
                    } else {
                        crate::connection::owner_::EstablishOutcome_::Refused
                    };
                    owner.with_mut_(|state| {
                        state.set_establish_outcome_(outcome);
                        state.touch_();
                    });
                    wake_establish_(&owner);
                }
            }
            FrameKind::Close => {
                let reset = header.flags() & flags::K_RESET != 0;
                if let Option::Some(entry) = find_read_mut_(&mut table, pair) {
                    // 关掉接收环写端：应用先把已缓存数据读完，再读到 EOF。
                    entry.writer_.close();
                    let owner = entry.owner_.clone();
                    owner.with_mut_(|state| {
                        if reset {
                            state.set_peer_reset_();
                        } else {
                            state.set_peer_fin_();
                        }
                        state.touch_();
                    });
                    owner.with_mut_(|state| {
                        if !state.is_done_() {
                            return;
                        }
                        if state.claim_release_() {
                            shared.reg_.release_channel_(local, remote);
                        }
                    });
                    let _ = events_tx.try_send_event_(WriteEvent_::PeerClosed {
                        local_dock: local,
                        remote_dock: remote,
                        reset_: reset,
                    });
                }
            }
            FrameKind::WindowUpdate | FrameKind::Pulse => {
                if let Option::Some(entry) = find_read_mut_(&mut table, pair)
                    && let Option::Some(report) = window_report_of_(&header)
                {
                    entry.owner_.with_mut_(|state| {
                        let _ = state.flow_mut_().send_window_mut().on_report(report);
                        state.touch_();
                    });
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
fn drain_read_events_<B, A>(
    events: &mut EventReceiver_<ReadEvent_<B, A>>,
    table: &mut Option<Owned<ReadEntry_<B, A>, A>>,
    shared: &LoopShared_<A>,
) where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
{
    while let Option::Some(event) = events.try_take_event_() {
        match event {
            ReadEvent_::Attach {
                local_dock,
                remote_dock,
                owner,
                writer_,
            } => {
                let alloc = shared.reg_.allocator_();
                *table = Option::Some(Owned::new(
                    ReadEntry_ {
                        local_dock_: local_dock,
                        remote_dock_: remote_dock,
                        owner_: owner,
                        writer_,
                        next_: table.take(),
                    },
                    alloc,
                ));
            }
            ReadEvent_::Release {
                local_dock,
                remote_dock,
            } => {
                remove_read_(table, (local_dock, remote_dock));
            }
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 写循环
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 写循环：按对端发送窗口调度各子流的发送环，并写出控制帧。
pub(crate) async fn write_loop_async_<W, B, A, K>(
    mut tx: W,
    shared: LoopShared_<A>,
    mut events: EventReceiver_<WriteEvent_<B, A>>,
    read_events: EventSender_<ReadEvent_<B, A>>,
    cancel: K,
) where
    W: TrBuffWrite<u8>,
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
    K: TrCancellationToken,
{
    let mut table: Option<Owned<WriteEntry_<B, A>, A>> = Option::None;
    let mut last_ready: Option<(Dock, Dock)> = Option::None;
    // 循环独占的载荷暂存：把环段的字节搬进来（**搬出即消费**，见 `drain_one_`），
    // 再写上网。整条连接只分配一次。
    let mut scratch: Vec<u8> = vec![0u8; K_MAX_DATA_CHUNK];

    loop {
        if cancel.is_cancelled() {
            return;
        }

        // 0. 先把**已经到达**的事件处理掉（非阻塞）。
        //
        // 这一步的顺序很关键：下面第 2 步会 park 在「最近通知过的那条发送环」上；
        // 若不在 park 之前排空事件队列，其它子流的 `Attach` / `Control`（例如它们
        // 的 `OPEN`）就会排在一条正在 park 的循环后面，形成死锁。单线程运行时下
        // 「检查队列」与「登记 park」之间没有 `await`，因此不存在竞态。
        if let Option::Some(event) = events.try_take_event_() {
            if let Result::Err(err) = handle_write_event_(
                event,
                &mut tx,
                &mut table,
                &mut scratch,
                &shared,
                &read_events,
                &cancel,
                &mut last_ready,
            )
            .await
            {
                shared.reg_.mark_failed_(&err);
                return;
            }
            continue;
        }

        // 1. 尽量把各子流的数据发出去（一次一段），直到没有可发的。
        loop {
            match drain_once_(&mut tx, &mut table, &mut scratch, &cancel).await {
                Result::Ok(true) => continue,
                Result::Ok(false) => break,
                Result::Err(err) => {
                    shared.reg_.mark_failed_(&err);
                    return;
                }
            }
        }

        // 2. 没有可发数据：若「最近通知过的那条发送环」就是目标，就 park 在它上面
        //    （Q4 裁决的兜底：应用可能在提交之前就发出了事件）。
        //
        //    **必须同时轮询事件通道**：只 park 在环上会让「别的子流刚入队的事件」
        //    一直排在一个睡着的循环后面（多子流下就是死锁——某条环的发送方在等对端
        //    的 FIN，而它的 FIN 事件排在队列里没人处理）。
        if let Option::Some(pair) = last_ready {
            // 这个块把 `entry`（借自 `table`）与事件通道的竞争限制在内部，
            // 出块后 `table` 的可变借用结束，才能交给 `handle_write_event_`。
            let mut taken: Option<WriteEvent_<B, A>> = Option::None;
            let mut ring_ready = false;
            let alive = {
                let Option::Some(entry) = find_write_mut_(&mut table, pair) else {
                    last_ready = Option::None;
                    continue;
                };
                let demand = Demand::at_least(1usize);
                let ring_fut = entry
                    .reader_
                    .read_async(&demand)
                    .may_cancel_with(cancel.child_token());
                let mut ring_fut = core::pin::pin!(ring_fut);
                let mut event_fut = core::pin::pin!(events.take_event_async_());
                poll_fn(|cx| {
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
                if let Result::Err(err) = handle_write_event_(
                    event,
                    &mut tx,
                    &mut table,
                    &mut scratch,
                    &shared,
                    &read_events,
                    &cancel,
                    &mut last_ready,
                )
                .await
                {
                    shared.reg_.mark_failed_(&err);
                    return;
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

        // 3. 否则 park 在事件通道上。
        let Option::Some(event) = events.take_event_async_().await else {
            return;
        };
        if let Result::Err(err) = handle_write_event_(
            event,
            &mut tx,
            &mut table,
            &mut scratch,
            &shared,
            &read_events,
            &cancel,
            &mut last_ready,
        )
        .await
        {
            shared.reg_.mark_failed_(&err);
            return;
        }
    }
}

/// 处理一条写事件；返回 `Err` 表示连接级失败（调用方负责 `mark_failed_` 并退出）。
#[allow(clippy::too_many_arguments)]
async fn handle_write_event_<W, B, A, K>(
    event: WriteEvent_<B, A>,
    tx: &mut W,
    table: &mut Option<Owned<WriteEntry_<B, A>, A>>,
    scratch: &mut Vec<u8>,
    shared: &LoopShared_<A>,
    read_events: &EventSender_<ReadEvent_<B, A>>,
    cancel: &K,
    last_ready: &mut Option<(Dock, Dock)>,
) -> Result<(), MuxError<(), W::Err>>
where
    W: TrBuffWrite<u8>,
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
    K: TrCancellationToken,
{
    match event {
        WriteEvent_::Attach {
            local_dock,
            remote_dock,
            owner,
            reader_,
        } => {
            let alloc = shared.reg_.allocator_();
            *table = Option::Some(Owned::new(
                WriteEntry_ {
                    local_dock_: local_dock,
                    remote_dock_: remote_dock,
                    owner_: owner,
                    reader_,
                    next_: table.take(),
                },
                alloc,
            ));
        }
        WriteEvent_::Control { frame_ } => {
            write_control_(tx, &frame_, cancel.child_token()).await?;
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
            let Some(entry) = find_write_mut_(table, pair) else {
                return Result::Ok(());
            };
            let owner = entry.owner_.clone();
            let report = owner.with_mut_(|state| {
                state.flow_mut_().recv_window_mut().on_consumed(amount_);
                state.touch_();
                if state
                    .flow_mut_()
                    .recv_window()
                    .should_report_with_(&shared.thresholds_)
                {
                    Option::Some(state.flow_mut_().recv_window_mut().report())
                } else {
                    Option::None
                }
            });
            if let Option::Some(report) = report {
                send_window_update_(tx, pair, report, cancel.child_token()).await?;
            }
        }
        WriteEvent_::TxClosed {
            local_dock,
            remote_dock,
        } => {
            let pair = (local_dock, remote_dock);
            // 把已缓存数据全部发完，再发 FIN。
            flush_entry_(tx, table, scratch, pair, cancel).await?;
            control_close_via_(tx, local_dock, remote_dock, false, cancel.child_token()).await?;
            if let Option::Some(entry) = find_write_mut_(table, pair) {
                entry.owner_.with_mut_(|state| {
                    state.set_app_tx_closed_();
                    state.set_local_fin_sent_();
                });
            }
            remove_write_(table, pair);
            maybe_release_(shared, read_events, table, pair);
        }
        WriteEvent_::RxClosed {
            local_dock,
            remote_dock,
        } => {
            let pair = (local_dock, remote_dock);
            if let Option::Some(entry) = find_write_mut_(table, pair) {
                entry.owner_.with_mut_(|state| {
                    state.set_app_rx_closed_();
                });
            }
            let _ = read_events.try_send_event_(ReadEvent_::Release {
                local_dock,
                remote_dock,
            });
            remove_write_(table, pair);
            control_close_via_(tx, local_dock, remote_dock, true, cancel.child_token()).await?;
            maybe_release_(shared, read_events, table, pair);
        }
        WriteEvent_::PeerClosed {
            local_dock,
            remote_dock,
            reset_,
        } => {
            let pair = (local_dock, remote_dock);
            if reset_ {
                // 对端不再接收：停止发送并释放该方向。
                remove_write_(table, pair);
            }
            maybe_release_(shared, read_events, table, pair);
        }
    }
    Result::Ok(())
}

/// 两个方向都收尾时释放注册表条目与接收环。
fn maybe_release_<B, A>(
    shared: &LoopShared_<A>,
    read_events: &EventSender_<ReadEvent_<B, A>>,
    table: &Option<Owned<WriteEntry_<B, A>, A>>,
    pair: (Dock, Dock),
) where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
{
    // 找到 owner：表里可能已经移除，因此用注册表兜底。
    let owner = {
        let mut cursor = table.as_ref();
        let mut found = Option::None;
        while let Option::Some(entry) = cursor {
            if entry.local_dock_ == pair.0 && entry.remote_dock_ == pair.1 {
                found = Option::Some(entry.owner_.clone());
                break;
            }
            cursor = entry.next_.as_ref();
        }
        match found {
            Option::Some(owner) => Option::Some(owner),
            Option::None => shared.reg_.channel_owner_(pair.0, pair.1),
        }
    };
    let Some(owner) = owner else {
        return;
    };
    let release = owner.with_mut_(|state| {
        if !state.is_done_() {
            return false;
        }
        state.claim_release_()
    });
    if release {
        shared.reg_.release_channel_(pair.0, pair.1);
        let _ = read_events.try_send_event_(ReadEvent_::Release {
            local_dock: pair.0,
            remote_dock: pair.1,
        });
    }
}

/// 发送一条窗口更新（`WINDOW_UPDATE`）。
async fn send_window_update_<W, K>(
    tx: &mut W,
    pair: (Dock, Dock),
    report: WindowReport,
    cancel: K,
) -> Result<(), MuxError<(), W::Err>>
where
    W: TrBuffWrite<u8>,
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
    write_frame_(tx, &header, &[], cancel).await
}

/// 发一条 `CLOSE`。用独立的 helper 以便在事件处理里直接 await。
async fn control_close_via_<W, K>(
    tx: &mut W,
    local_dock: Dock,
    remote_dock: Dock,
    reset: bool,
    cancel: K,
) -> Result<(), MuxError<(), W::Err>>
where
    W: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    let frame = ControlFrame_::plain_(
        FrameKind::Close,
        if reset { flags::K_RESET } else { flags::K_FIN },
        local_dock,
        remote_dock,
    );
    write_control_(tx, &frame, cancel).await
}

/// 把某条子流发送环里已提交的数据全部写出（直到取空）。
async fn flush_entry_<W, B, A, K>(
    tx: &mut W,
    table: &mut Option<Owned<WriteEntry_<B, A>, A>>,
    scratch: &mut Vec<u8>,
    pair: (Dock, Dock),
    cancel: &K,
) -> Result<(), MuxError<(), W::Err>>
where
    W: TrBuffWrite<u8>,
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
    K: TrCancellationToken,
{
    loop {
        let progressed = drain_one_(&mut *tx, table, scratch, pair, cancel).await?;
        if !progressed {
            return Result::Ok(());
        }
    }
}

/// 轮转一遍本地表，最多写出一段数据；返回是否有进展。
async fn drain_once_<W, B, A, K>(
    tx: &mut W,
    table: &mut Option<Owned<WriteEntry_<B, A>, A>>,
    scratch: &mut Vec<u8>,
    cancel: &K,
) -> Result<bool, MuxError<(), W::Err>>
where
    W: TrBuffWrite<u8>,
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
    K: TrCancellationToken,
{
    // 收集待轮转的 dock 对，避免在持有表可变借用的同时 await。
    let mut pairs: Vec<(Dock, Dock)> = Vec::new();
    {
        let mut cursor = table.as_ref();
        while let Option::Some(entry) = cursor {
            pairs.push((entry.local_dock_, entry.remote_dock_));
            cursor = entry.next_.as_ref();
        }
    }
    for pair in pairs {
        if drain_one_(tx, table, scratch, pair, cancel).await? {
            return Result::Ok(true);
        }
    }
    Result::Ok(false)
}

/// 尝试为 `pair` 写出一段数据；返回是否写出。
async fn drain_one_<W, B, A, K>(
    tx: &mut W,
    table: &mut Option<Owned<WriteEntry_<B, A>, A>>,
    scratch: &mut Vec<u8>,
    pair: (Dock, Dock),
    cancel: &K,
) -> Result<bool, MuxError<(), W::Err>>
where
    W: TrBuffWrite<u8>,
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    A: AllocatorClone + Send + Sync + 'static,
    K: TrCancellationToken,
{
    let token = cancel.child_token();
    let Some(entry) = find_write_mut_(table, pair) else {
        return Result::Ok(false);
    };

    // 清「已入队」位：此后新的写入会重新入队（顺序不可反，见 dev-notes §11.4）。
    let owner = entry.owner_.clone();
    owner.with_mut_(|state| state.set_tx_queued_(false));

    let available = owner.with_(|state| state.flow_().send_window().available());
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

    // 预扣窗口（`take <= available`，因此必定足额）。
    let granted = owner.with_mut_(|state| state.flow_mut_().send_window_mut().reserve(take as Credit));
    if granted < take as Credit {
        owner.with_mut_(|state| state.flow_mut_().send_window_mut().refund(granted));
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
    scratch.resize(take, 0u8);
    let moved = {
        let mut child = segm.as_segm_ref();
        // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同（同尺寸、同对齐、无 niche）；
        // `scratch[..take]` 是本循环独占的可写区间，`move_items_to_buff` 只写入其中
        // 已初始化的前缀并返回写入长度，因此既不会读到未初始化内存，也不会越界。
        let dst = unsafe {
            core::slice::from_raw_parts_mut(
                scratch.as_mut_ptr() as *mut MaybeUninit<u8>,
                take,
            )
        };
        unsafe { child.move_items_to_buff(dst) }
    };
    if moved != take {
        // 段长度与搬出量应当一致；不一致说明上游语义变了。
        owner.with_mut_(|state| state.flow_mut_().send_window_mut().refund(take as Credit));
        return Result::Err(MuxError::MalformedFrame);
    }
    write_all_async_(tx, &scratch[..moved], token.child_token())
        .await
        .map_err(map_cursor_err_)?;
    // `outcome` 在此 drop：提交消费，唤醒环的写端（若有 park 者）。
    owner.with_mut_(|state| state.touch_());
    Result::Ok(true)
}
