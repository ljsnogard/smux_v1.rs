//! `main_loop`：控制面单写者循环（P1 骨架）。
//!
//! 它接管三件事：
//!
//! - **请求队列**：bind / listen / reserve 从 API future 经 [`ControlState_`] 进来；
//! - **身份表写**：所有登记 / 释放都经 [`ChannelRegistry_`] 的写锁（`main_loop`
//!   是唯一写者；`demux_loop` 后续只读）；
//! - **过渡期的释放落实**：P1 仍让 `DockBinding` / `ChannelListener` 的 `Drop` 投
//!   `SessionEvent_`，由本循环 `drain_session_events_` 落实；P4 改成 retired 位 +
//!   `HINT_MAIN_RETIRED` 后删除这条过渡路径（设计稿 §6.7）。
//!
//! 公共报文子流的 RX / TX（P2）与 tick（P5）尚未接线；park 点已经按设计稿 §6.1
//! 的形状保留「请求 / 提示 / 取消」三类唤醒源，后续加 ring 时只需再加一个 poll 分支。

use core::{alloc::AllocatorClone, future::poll_fn, pin::pin, task::Poll};

use abs_cancel::{NonCancellableToken, TrCancellationToken};
use abs_smux::dock::TrDock;

use alloc::vec::Vec;
use mm_ptr::Owned;

use crate::{
    connection::{
        FrameHeader, FrameKind, MuxError, ReserveErr_, TrConnCfg,
        control_state_::ControlState_,
        flags,
        frame_::encode_header_,
        frame_parser_::decode_header_sync_,
        main_loop::{EstablishRole_, MainErr_, MainReply_, MainRequest_},
        owner_::EstablishOutcome_,
        public_channel_::{ChunkReadState_, ChunkWriteErr_, PublicChunkIo_},
        ring_::{BufferedRx, BufferedTx},
        session_::race_cancel_,
        session_::MuxShared_,
        HINT_MAIN_RETIRED,
        signal_::{ControlFrame_, HINT_W_PEER_CLOSED},
    },
    flow_ctrl::{RecvTotal, WindowReport},
    x_deps::{abs_cancel, abs_smux, mm_ptr},
};

/// 每轮最多处理多少条请求。
///
/// 防止「请求源源不断 ⇒ 提示 / 取消饿死」；控制面是冷路径，16 已远高于常见并发。
pub(crate) const K_MAIN_REQ_BATCH: usize = 16usize;

/// 每轮最多处理多少条公共 RX 控制帧。
pub(crate) const K_MAIN_FRAME_BATCH: usize = 32usize;

/// `main_loop` 的 park 点被唤醒的原因。
enum MainWake_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 队列里取到一条请求。
    Request(MainRequest_<A>),

    /// 合并提示位（P1 尚无 `HINT_MAIN_*` 使用者）。
    Hint(u64),

    /// 公共 RX 有新的字节（前缀或内容），回顶部继续读。
    RxReady,
}

/// 控制面循环本体。
///
/// 不持有 `ConnTx` / `ConnRx`，也不持有用户子流的环半部；只持共享面与请求队列句柄。
/// 退出前关闭请求队列并把仍排队的请求以 [`MuxError::Closed`] 结束，避免调用方永久
/// 等待。
pub(crate) async fn main_loop_async_<C, K>(
    shared: MuxShared_<C>,
    ctl: ControlState_<C::Alloc>,
    mut public_tx_w: BufferedTx,
    mut public_rx_r: BufferedRx,
    cancel: K,
) where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    // 公共 RX 的整帧暂存：连接一次分配，容量就是一个最大帧。
    let alloc = match shared.reg_().allocator_(cancel.child_token()).await {
        Result::Ok(alloc) => alloc,
        Result::Err(_) => {
            exit_(&ctl);
            return;
        }
    };
    let mut rx_scratch: Owned<[u8], C::Alloc> = Owned::new_slice(
        shared.max_packet_size_(),
        |_idx, slot| {
            slot.write(0u8);
        },
        alloc,
    );
    let mut public_read = MainPublicRead_::new_();

    loop {
        if cancel.is_cancelled() {
            exit_(&ctl);
            return;
        }
        if !main_step_::<C, _>(
            &shared,
            &ctl,
            &mut public_tx_w,
            &mut public_rx_r,
            &mut rx_scratch[..],
            &mut public_read,
            &cancel,
        )
        .await
        {
            exit_(&ctl);
            return;
        }

        match park_main_::<C, _>(&shared, &ctl, &cancel).await {
            Some(MainWake_::Request(request)) => {
                handle_request_::<C, _>(&shared, &mut public_tx_w, request, &cancel).await;
            }
            // 提示已由下一轮顶部的 `take_main_hint_` 取走；这里只需回顶部。
            Some(MainWake_::Hint(_bits)) => {}
            // 公共 RX 有新字节：回顶部继续读帧。
            Some(MainWake_::RxReady) => {}
            // 取消或请求队列关闭：连接正在收尾。
            None => {
                exit_(&ctl);
                return;
            }
        }
    }
}

/// 非阻塞地跑一轮控制面工作：释放落实、提示、公共 RX 帧、请求批。
///
/// 返回 `false` 表示连接应退出（drain 被取消 / 帧解析失败）。测试可手动调用
/// 本函数驱动 `main_loop` 的状态推进，不必 park。
pub(crate) async fn main_step_<C, K>(
    shared: &MuxShared_<C>,
    ctl: &ControlState_<C::Alloc>,
    public_tx_w: &mut BufferedTx,
    public_rx_r: &mut BufferedRx,
    rx_scratch: &mut [u8],
    public_read: &mut MainPublicRead_,
    cancel: &K,
) -> bool
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    // 过渡：`Drop` 仍投 `SessionEvent_`（P4 改为 retired 位）。
    if shared
        .reg_()
        .drain_session_events_(shared.conn_clock_().now_millis_(), cancel.child_token())
        .await
        .is_err()
    {
        return false;
    }

    // 合并提示：目前只有 retired 位（P4 起逐步接入 window-dirty 等）。
    let hints = ctl.take_main_hint_();
    if hints & HINT_MAIN_RETIRED != 0u64 {
        let _ = shared.reg_().reap_retired_(cancel.child_token()).await;
    }

    // 公共 RX：每轮最多 `K_MAIN_FRAME_BATCH` 条完整帧。
    for _ in 0..K_MAIN_FRAME_BATCH {
        match try_read_public_frame_(
            public_rx_r,
            public_read,
            rx_scratch,
            shared.max_packet_size_(),
        ) {
            Result::Ok(Option::Some((header, head_len, total_len))) => {
                let payload = &rx_scratch[head_len..total_len];
                handle_public_frame_::<C, _>(shared, public_tx_w, &header, payload, cancel)
                    .await;
                // 读走一帧就腾出 RX 空间，唤醒可能停在背压上的 demux。
                shared.public_().notify_rx_space_();
            }
            Result::Ok(Option::None) => break,
            Result::Err(err) => {
                let _ = shared.reg_().mark_failed_(&err, cancel.child_token()).await;
                shared.reg_().cancel_loops_();
                return false;
            }
        }
    }

    // 请求：每轮最多 `K_MAIN_REQ_BATCH` 条。
    for _ in 0..K_MAIN_REQ_BATCH {
        let Some(request) = ctl.try_pop_request_() else {
            break;
        };
        handle_request_::<C, _>(shared, public_tx_w, request, cancel).await;
    }
    true
}

/// 收尾：关闭队列，并把仍排队的请求以 `Closed` 结束。
fn exit_<A>(ctl: &ControlState_<A>)
where
    A: AllocatorClone + Send + Sync,
{
    ctl.close_requests_();
    while let Some(request) = ctl.try_pop_request_() {
        request.fail_(MainErr_::Mux(MuxError::Closed));
    }
}

/// 处理一条请求。
///
/// 取身份表写锁是协作式异步获取（可取消）；临界区本身只做一次登记 / 查询，不 `await`。
async fn handle_request_<C, K>(
    shared: &MuxShared_<C>,
    public_tx_w: &mut BufferedTx,
    request: MainRequest_<C::Alloc>,
    cancel: &K,
)
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    // 过渡：`Drop` 仍投 `SessionEvent_`。登记之前先把它落实，保证「丢弃后立刻重绑 /
    // 重听 / 重新登记同一条 dock 对」与旧 API 的语义一致（P4 改为 retired 位后删除）。
    if shared
        .reg_()
        .drain_session_events_(shared.conn_clock_().now_millis_(), cancel.child_token())
        .await
        .is_err()
    {
        request.fail_(MainErr_::Mux(MuxError::Cancelled));
        return;
    }
    // 同一轮合并的 retired 释放先落实：保证「丢弃后立刻重听」看到的是已释放。
    let _ = shared.reg_().reap_retired_(cancel.child_token()).await;
    match request {
        MainRequest_::SendControl { frame, reply } => {
            match write_control_frame_async_::<C, _>(public_tx_w, shared, &frame, cancel).await {
                Result::Ok(()) => {
                    let _ = reply.set_(Result::Ok(MainReply_::Unit));
                }
                Result::Err(err) => {
                    let _ = reply.set_(Result::Err(MainErr_::Mux(err)));
                }
            }
        }
        MainRequest_::Bind { local_dock, reply } => {
            if reply.is_abandoned_() {
                let _ = reply.set_(Result::Err(MainErr_::Reserve(ReserveErr_::Cancelled)));
                return;
            }
            let result = if local_dock.is_unspecified() {
                shared
                    .reg_()
                    .bind_any_dock_(cancel.child_token())
                    .await
                    .map(|(dock, unbind_)| MainReply_::Dock { dock, unbind_ })
            } else {
                shared
                    .reg_()
                    .bind_dock_(local_dock, cancel.child_token())
                    .await
                    .map(|(dock, unbind_)| MainReply_::Dock { dock, unbind_ })
            };
            // 登记之后才发现等待方已放弃：**回滚刚登记的身份**，否则这个 dock 会
            // 永久占住（没有句柄能解绑它）。回滚用不可取消令牌：它是收尾动作。
            if reply.is_abandoned_()
                && let Result::Ok(MainReply_::Dock { dock, .. }) = &result
            {
                let _ = shared
                    .reg_()
                    .unbind_dock_(*dock, NonCancellableToken::new())
                    .await;
            }
            let _ = reply.set_(result.map_err(MainErr_::Reserve));
        }
        MainRequest_::Listen {
            local_dock,
            reserve: _reserve,
            reply,
        } => {
            // `reserve` 仍未用于预分配：入向通知目前内联在身份节点里。
            if reply.is_abandoned_() {
                let _ = reply.set_(Result::Err(MainErr_::Reserve(ReserveErr_::Cancelled)));
                return;
            }
            let result = shared
                .reg_()
                .reserve_listener_(local_dock, cancel.child_token())
                .await
                .map(MainReply_::Listener);
            if reply.is_abandoned_() {
                let _ = shared
                    .reg_()
                    .release_listener_(local_dock, NonCancellableToken::new())
                    .await;
            }
            let _ = reply.set_(result.map_err(MainErr_::Reserve));
        }
        MainRequest_::ReserveChannel {
            local_dock,
            remote_dock,
            reply,
        } => {
            if reply.is_abandoned_() {
                let _ = reply.set_(Result::Err(MainErr_::Reserve(ReserveErr_::Cancelled)));
                return;
            }
            let now_millis = shared.conn_clock_().now_millis_();
            let result = shared
                .reg_()
                .reserve_channel_(
                    local_dock,
                    remote_dock,
                    now_millis,
                    shared.pending_msg_cap_(),
                    cancel.child_token(),
                )
                .await
                .map(|(owner, msg_)| MainReply_::Channel { owner, msg_ });
            if reply.is_abandoned_() {
                let _ = shared
                    .reg_()
                    .unreserve_channel_(local_dock, remote_dock, NonCancellableToken::new())
                    .await;
            }
            let _ = reply.set_(result.map_err(MainErr_::Reserve));
        }
        MainRequest_::Accept {
            local_dock,
            remote_dock,
            owner,
            role_,
            initial_window,
            reply,
        } => {
            let cap = shared.pending_msg_cap_();
            let message = match shared
                .reg_()
                .read_msg_outgoing_(local_dock, remote_dock, cap, cancel.child_token())
                .await
            {
                Result::Ok(Option::Some(message)) => message,
                Result::Ok(Option::None) => Vec::new(),
                Result::Err(_) => {
                    let _ = reply.set_(Result::Err(MainErr_::Mux(MuxError::Cancelled)));
                    return;
                }
            };
            let result = match role_ {
                EstablishRole_::Initiator => {
                    write_control_frame_async_::<C, _>(
                        public_tx_w,
                        shared,
                        &ControlFrame_::with_window_(
                            FrameKind::Open,
                            0u8,
                            local_dock,
                            remote_dock,
                            Option::Some((0u64 as RecvTotal, initial_window)),
                            message,
                        ),
                        cancel,
                    )
                    .await
                }
                EstablishRole_::Responder => {
                    // 对端 `OPEN` 的窗口通告在登记时存下；先装进发送窗口。
                    match shared
                        .reg_()
                        .take_inbound_report_(local_dock, remote_dock, cancel.child_token())
                        .await
                    {
                        Result::Ok(Option::Some(report)) => {
                            if let Result::Err(err) = owner.send_on_report_(report) {
                                Result::Err(MuxError::FlowCtrl(err))
                            } else {
                                let open = write_control_frame_async_::<C, _>(
                                    public_tx_w,
                                    shared,
                                    &ControlFrame_::with_window_(
                                        FrameKind::Open,
                                        0u8,
                                        local_dock,
                                        remote_dock,
                                        Option::Some((0u64 as RecvTotal, initial_window)),
                                        Vec::new(),
                                    ),
                                    cancel,
                                )
                                .await;
                                match open {
                                    Result::Ok(()) => {
                                        write_control_frame_async_::<C, _>(
                                            public_tx_w,
                                            shared,
                                            &ControlFrame_::with_window_(
                                                FrameKind::Accept,
                                                0u8,
                                                local_dock,
                                                remote_dock,
                                                Option::None,
                                                message,
                                            ),
                                            cancel,
                                        )
                                        .await
                                    }
                                    Result::Err(err) => Result::Err(err),
                                }
                            }
                        }
                        _ => Result::Err(MuxError::Closed),
                    }
                }
            };
            match result {
                Result::Ok(()) => {
                    if role_ == EstablishRole_::Responder {
                        owner.set_establish_settled_();
                    }
                    let _ = reply.set_(Result::Ok(MainReply_::Unit));
                }
                Result::Err(err) => {
                    let _ = reply.set_(Result::Err(MainErr_::Mux(err)));
                }
            }
        }
        MainRequest_::Reject {
            local_dock,
            remote_dock,
            owner,
            role_,
            reply,
        } => {
            let cap = shared.pending_msg_cap_();
            let reason = match shared
                .reg_()
                .read_msg_outgoing_(local_dock, remote_dock, cap, cancel.child_token())
                .await
            {
                Result::Ok(Option::Some(reason)) => reason,
                Result::Ok(Option::None) => Vec::new(),
                Result::Err(_) => {
                    let _ = reply.set_(Result::Err(MainErr_::Mux(MuxError::Cancelled)));
                    return;
                }
            };
            let result = match role_ {
                EstablishRole_::Responder => {
                    let sent = write_control_frame_async_::<C, _>(
                        public_tx_w,
                        shared,
                        &ControlFrame_::with_window_(
                            FrameKind::Reject,
                            0u8,
                            local_dock,
                            remote_dock,
                            Option::None,
                            reason,
                        ),
                        cancel,
                    )
                    .await;
                    let _ = shared
                        .reg_()
                        .release_channel_(
                            local_dock,
                            remote_dock,
                            shared.conn_clock_().now_millis_(),
                            cancel.child_token(),
                        )
                        .await;
                    sent
                }
                EstablishRole_::Initiator => {
                    // 发起方还没发过 `OPEN`：对端不知道这条子流，撤销预留即可。
                    let _ = shared
                        .reg_()
                        .unreserve_channel_(local_dock, remote_dock, cancel.child_token())
                        .await;
                    Result::Ok(())
                }
            };
            owner.set_establish_settled_();
            match result {
                Result::Ok(()) => {
                    let _ = reply.set_(Result::Ok(MainReply_::Unit));
                }
                Result::Err(err) => {
                    let _ = reply.set_(Result::Err(MainErr_::Mux(err)));
                }
            }
        }
    }
}

/// park 到「有请求 / 有提示 / 取消」之一。
///
/// 返回 `None` 表示连接正在收尾（取消令牌触发，或请求队列被关闭）。
async fn park_main_<C, K>(
    shared: &MuxShared_<C>,
    ctl: &ControlState_<C::Alloc>,
    cancel: &K,
) -> Option<MainWake_<C::Alloc>>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let cancel_fut = cancel.child_token().cancellation();
    let mut cancel_fut = pin!(cancel_fut);
    let mut taken: Option<MainWake_<C::Alloc>> = None;
    poll_fn(|cx| {
        if core::future::Future::poll(cancel_fut.as_mut(), cx).is_ready() {
            return Poll::Ready(());
        }
        // 公共 RX 有字节（可能只是前缀的一部分）：回顶部按 chunk 协议继续读。
        if shared.public_().poll_rx_ready_(cx).is_ready() {
            taken = Some(MainWake_::RxReady);
            return Poll::Ready(());
        }
        match ctl.poll_pop_request_(cx) {
            Poll::Ready(Some(request)) => {
                taken = Some(MainWake_::Request(request));
                return Poll::Ready(());
            }
            // 队列关闭：连接收尾。
            Poll::Ready(None) => return Poll::Ready(()),
            Poll::Pending => {}
        }
        if let Poll::Ready(bits) = ctl.poll_main_hint_(cx) {
            taken = Some(MainWake_::Hint(bits));
            return Poll::Ready(());
        }
        Poll::Pending
    })
    .await;
    taken
}

/// 公共 RX 的整帧读取进度。
pub(crate) struct MainPublicRead_ {
    /// chunked 前缀 / 内容状态。
    state_: ChunkReadState_,

    /// 当前 chunk 的声明长度（0 = 还没解析出）。
    len_: usize,

    /// 已读入暂存的字节数。
    filled_: usize,
}

impl MainPublicRead_ {
    /// 空状态。
    pub(crate) fn new_() -> Self {
        MainPublicRead_ {
            state_: ChunkReadState_::new_(),
            len_: 0usize,
            filled_: 0usize,
        }
    }
}

/// 尝试从公共 RX 读出一条完整控制帧；不足一帧时返回 `Ok(None)` 并保留进度。
///
/// 返回 `(header, head_len, total_len)`；载荷是 `scratch[head_len..total_len]`。
fn try_read_public_frame_(
    public_rx: &mut BufferedRx,
    read: &mut MainPublicRead_,
    scratch: &mut [u8],
    max_packet_size: usize,
) -> Result<Option<(FrameHeader, usize, usize)>, MuxError> {
    if !read.state_.has_chunk_() {
        match PublicChunkIo_::try_read_prefix_(public_rx, &mut read.state_, max_packet_size) {
            Result::Ok(Option::Some(_len)) => {}
            Result::Ok(Option::None) => return Result::Ok(Option::None),
            Result::Err(_) => return Result::Err(MuxError::MalformedFrame),
        }
    }
    if read.len_ == 0usize {
        read.len_ = read.state_.left_();
        read.filled_ = 0usize;
    }
    if read.filled_ < read.len_ {
        let start = read.filled_;
        let n = PublicChunkIo_::try_read_payload_into_(
            public_rx,
            &mut read.state_,
            &mut scratch[start..read.len_],
        )
        .map_err(|_| MuxError::MalformedFrame)?;
        read.filled_ += n;
        if read.filled_ < read.len_ {
            return Result::Ok(Option::None);
        }
    }
    let len = read.len_;
    read.len_ = 0usize;
    read.filled_ = 0usize;
    let (header, head_len) = decode_header_sync_(&scratch[..len])?;
    Result::Ok(Option::Some((header, head_len, len)))
}

/// 处理一条入向控制帧（`OPEN` / `ACCEPT` / `REJECT` / `CLOSE`）。
async fn handle_public_frame_<C, K>(
    shared: &MuxShared_<C>,
    public_tx_w: &mut BufferedTx,
    header: &FrameHeader,
    payload: &[u8],
    cancel: &K,
) where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let local = header.remote_dock();
    let remote = header.local_dock();
    let now_millis = shared.conn_clock_().now_millis_();
    match header.kind() {
        FrameKind::Close => {
            let owner = shared
                .reg_()
                .channel_owner_(local, remote, cancel.child_token())
                .await;
            let Result::Ok(Option::Some(owner)) = owner else {
                return;
            };
            if header.flags() & flags::K_RESET != 0u8 {
                owner.set_peer_reset_();
                owner.notify_tx_progress_();
            } else {
                owner.set_peer_fin_();
                owner.notify_rx_progress_();
            }
            owner.mark_data_(now_millis);
            shared.reg_().raise_write_hint_(HINT_W_PEER_CLOSED);
        }
        FrameKind::Open => {
            let Some(report) = window_report_(header) else {
                return;
            };
            let owner = shared
                .reg_()
                .channel_owner_(local, remote, cancel.child_token())
                .await;
            match owner {
                Result::Ok(Option::Some(owner)) => {
                    let _ = owner.send_on_report_(report);
                    owner.set_peer_opened_();
                    owner.mark_data_(now_millis);
                    owner.notify_establish_();
                    let _ = shared
                        .reg_()
                        .write_msg_incoming_(
                            local,
                            remote,
                            payload,
                            shared.pending_msg_cap_(),
                            cancel.child_token(),
                        )
                        .await;
                }
                Result::Ok(Option::None) => {
                    let cap = shared.pending_msg_cap_();
                    match shared
                        .reg_()
                        .reserve_inbound_(
                            local,
                            remote,
                            report,
                            now_millis,
                            cap,
                            cancel.child_token(),
                        )
                        .await
                    {
                        Result::Ok(_) => {
                            let _ = shared
                                .reg_()
                                .write_msg_incoming_(
                                    local,
                                    remote,
                                    payload,
                                    cap,
                                    cancel.child_token(),
                                )
                                .await;
                        }
                        Result::Err(_) => {
                            let _ = write_control_frame_async_::<C, _>(
                                public_tx_w,
                                shared,
                                &ControlFrame_::with_window_(
                                    FrameKind::Reject,
                                    0u8,
                                    local,
                                    remote,
                                    Option::None,
                                    Vec::new(),
                                ),
                                cancel,
                            )
                            .await;
                        }
                    }
                }
                Result::Err(_) => {}
            }
        }
        FrameKind::Accept | FrameKind::Reject => {
            let owner = shared
                .reg_()
                .channel_owner_(local, remote, cancel.child_token())
                .await;
            let Result::Ok(Option::Some(owner)) = owner else {
                return;
            };
            let outcome = if header.kind() == FrameKind::Accept {
                EstablishOutcome_::Accepted
            } else {
                EstablishOutcome_::Refused
            };
            owner.set_establish_outcome_(outcome);
            owner.mark_data_(now_millis);
            let _ = shared
                .reg_()
                .write_msg_incoming_(
                    local,
                    remote,
                    payload,
                    shared.pending_msg_cap_(),
                    cancel.child_token(),
                )
                .await;
            owner.notify_establish_();
        }
        _ => {}
    }
}

/// 从 `OPEN` 帧头取出窗口通告。
fn window_report_(header: &FrameHeader) -> Option<WindowReport> {
    let total = header.recv_total()?;
    let window = header.recv_window()?;
    if header.is_total_reset() {
        Option::Some(WindowReport::new_reset(total, window))
    } else {
        Option::Some(WindowReport::new(total, window))
    }
}

/// 把一条控制帧写进公共 TX ring（背压时 park 等 `tx_space`）。
///
/// 帧头由 `FrameHeader` + `encode_header_` 重新编码；载荷仍在 `ControlFrame_` 里。
/// 这是 P3.1b 的过渡：P3.2 起 `OPEN` / `ACCEPT` / `REJECT` 的载荷直接从建流期
/// message ring 读出，`ControlFrame_::payload_` 只留给 timer / demux 的控制帧。
async fn write_control_frame_async_<C, K>(
    public_tx_w: &mut BufferedTx,
    shared: &MuxShared_<C>,
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
    let (head, head_len) = encode_header_(&header).map_err(|_| MuxError::MalformedFrame)?;
    loop {
        match PublicChunkIo_::try_write_chunk_(
            public_tx_w,
            &head[..head_len],
            frame.payload_(),
            shared.max_packet_size_(),
        ) {
            Result::Ok(()) => {
                shared.public_().notify_tx_ready_();
                return Result::Ok(());
            }
            Result::Err(ChunkWriteErr_::WouldBlock) => {
            }
            Result::Err(_) => return Result::Err(MuxError::FrameTooLarge),
        }
        let wait_space = poll_fn(|cx| shared.public_().poll_tx_space_(cx));
        if race_cancel_(cancel, wait_space).await.is_none() {
            return Result::Err(MuxError::Cancelled);
        }
    }
}

#[cfg(test)]
mod tests_ {
    use core::task::{Context, Poll, Waker};

    use abs_cancel::NonCancellableToken;
    use abs_mm::CoreAlloc;
    use mm_ptr::Owned;

    use super::{main_step_, MainPublicRead_};
    use crate::{
        connection::{
            main_loop::{MainRequest_, MainReply_},
            ring_::{new_buffered_channel, BufferedRx, BufferedTx},
            session_::MuxShared_,
            test_support_::TestMuxConfig_,
            ControlState_, Dock, DockBinding, MuxConnection, Tcs_,
        },
        handshake::opts::{BasicOpts, HandshakeOpts},
        x_deps::{abs_cancel, abs_mm, mm_ptr},
    };

    /// 驱动一个「测试里应当立即完成」的 future。
    /// - 手段：用 noop waker poll 一次，直到 `Ready` 或 panic。
    /// - 判断：`Pending` 视为测试失败（说明被挂住了）。
    ///
    /// Drive a future that must complete immediately in tests.
    /// Method: poll once with a noop waker until Ready, panicking on Pending.
    /// Assertion: Pending is a test failure, meaning something blocked.
    fn block_on_ready_<F: core::future::Future>(fut: F) -> F::Output {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut fut = core::pin::pin!(fut);
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("测试期望 future 立即完成，却返回了 Pending"),
        }
    }

    /// 提交一次 `Bind`，驱动一轮 `main_step_`，再构造 `DockBinding`。
    #[allow(clippy::too_many_arguments, reason = "测试辅助需要传全部驱动状态")]
    fn bind_through_step_(
        conn: &MuxConnection<TestMuxConfig_>,
        shared: &MuxShared_<TestMuxConfig_>,
        ctl: &ControlState_<CoreAlloc>,
        tx_w: &mut BufferedTx,
        rx_r: &mut BufferedRx,
        scratch: &mut [u8],
        public_read: &mut MainPublicRead_,
        dock: Dock,
    ) -> DockBinding<TestMuxConfig_> {
        let cancel = NonCancellableToken::new();
        let tcs = Tcs_::new_(CoreAlloc);
        let waiter = tcs.clone();
        let submitted = block_on_ready_(ctl.submit_async_(
            MainRequest_::Bind {
                local_dock: dock,
                reply: tcs,
            },
            NonCancellableToken::new(),
        ));
        assert!(submitted.is_ok(), "Bind 请求应能入队");
        assert!(block_on_ready_(main_step_::<TestMuxConfig_, _>(
            shared, ctl, tx_w, rx_r, scratch, public_read, &cancel
        )));
        let reply = block_on_ready_(core::future::poll_fn(|cx| waiter.poll_wait_(cx)))
            .expect("Bind 请求应完成")
            .expect("Bind 应成功");
        let MainReply_::Dock { dock, unbind_ } = reply else {
            panic!("Bind 的答复应当是 Dock");
        };
        DockBinding::new_(conn.clone(), dock, unbind_)
    }

    /// 测试 `DockBinding::Drop` 与下一轮 `main_step_` 的时序：drop 后立刻重绑应成功。
    /// - 手段：`new_test_bundle_` 建无循环连接；提交 Bind、跑 `main_step_`，得到
    ///   `DockBinding`；drop 它之后立刻再提交同 dock 的 Bind 并跑 `main_step_`。
    /// - 判断：第二次 Bind 仍得到 `MainReply_::Dock`，说明解绑在 Bind 请求处理前落实。
    ///
    /// Test the ordering between `DockBinding::Drop` and the next `main_step_`.
    /// Method: build a loop-less connection with `new_test_bundle_`; run a Bind
    /// through `main_step_` to obtain a `DockBinding`; drop it and immediately
    /// submit another Bind for the same dock, running `main_step_` again.
    /// Assertion: the second Bind also yields `MainReply_::Dock`, proving the
    /// unbind is applied before the Bind request is served.
    #[test]
    fn drop_then_step_allows_rebind_() {
        let (conn, shared) = MuxConnection::<TestMuxConfig_>::new_test_bundle_(
            HandshakeOpts {
                basic_opts: BasicOpts::default(),
            },
            TestMuxConfig_,
        );
        let ctl = conn.core_().control_().clone();
        let (mut tx_w, _tx_r) =
            new_buffered_channel(Owned::new_uninit_slice(64usize, CoreAlloc))
                .expect("建公共 TX 环");
        let (_rx_w, mut rx_r) =
            new_buffered_channel(Owned::new_uninit_slice(64usize, CoreAlloc))
                .expect("建公共 RX 环");
        let mut scratch = [0u8; 256];
        let mut public_read = MainPublicRead_::new_();
        let dock = Dock::new(42u32);
        let binding = bind_through_step_(
            &conn,
            &shared,
            &ctl,
            &mut tx_w,
            &mut rx_r,
            &mut scratch,
            &mut public_read,
            dock,
        );
        drop(binding);
        let binding = bind_through_step_(
            &conn,
            &shared,
            &ctl,
            &mut tx_w,
            &mut rx_r,
            &mut scratch,
            &mut public_read,
            dock,
        );
        drop(binding);
    }
}
