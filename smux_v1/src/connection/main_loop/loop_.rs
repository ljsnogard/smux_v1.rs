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

use core::{future::poll_fn, pin::pin, task::Poll};

use abs_cancel::{NonCancellableToken, TrCancellationToken};
use abs_smux::dock::TrDock;

use crate::{
    connection::{
        FrameHeader, FrameKind, MuxError, ReadOutgoingErr_, ReserveErr_, TrConnCfg,
        control_state_::ControlState_,
        flags,
        frame_::encode_header_,
        frame_parser_::decode_header_sync_,
        main_loop::{EstablishRole_, MainLoopErr_, MainLoopReply_, MainLoopRequest_},
        TrConnAllocCfg,
        owner_::EstablishOutcome_,
        public_channel_::{ChunkReadState_, ChunkWriteErr_, PublicChunkIo_},
        ring_::{BufferedRx, BufferedTx},
        session_::race_cancel_,
        session_::MuxShared_,
        HINT_MAIN_RELEASE, HINT_MAIN_RESET, HINT_MAIN_RETIRED, HINT_MAIN_TX_DRAINED,
        signal_::{
            ControlFrame_, HINT_R_RELEASE, HINT_W_FIN_ENQUEUED, HINT_W_PEER_CLOSED,
            HINT_W_RELEASED, HINT_W_RESET_ENQUEUED,
        },
    },
    flow_ctrl::{RecvTotal, WindowReport},
    metrics::{ChannelCloseReason, TrMetricsSink},
    x_deps::{abs_cancel, abs_smux},
};

/// 每轮最多处理多少条请求。
///
/// 防止「请求源源不断 ⇒ 提示 / 取消饿死」；控制面是冷路径，16 已远高于常见并发。
pub(crate) const K_MAIN_REQ_BATCH: usize = 16usize;

/// 每轮最多处理多少条公共 RX 控制帧。
pub(crate) const K_MAIN_FRAME_BATCH: usize = 32usize;

/// `main_loop` 的 park 点被唤醒的原因。
enum MainWake_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 队列里取到一条请求。
    Request(MainLoopRequest_<C>),

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
    ctl: ControlState_<crate::connection::ConnAllocCfg_<C>>,
    mut public_tx_w: BufferedTx,
    mut public_rx_r: BufferedRx,
    cancel: K,
) where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    // 公共 RX 的整帧暂存：连接一次分配，容量就是一个最大帧。走连接级临时对象域，
    // 不再为它单独配置分配器（见 `dev-notes/sw-scope-20261028-2130.md`）。
    let rx_scratch_retain = match crate::connection::alloc_zeroed_bytes_async_(
        shared.reg_().scope_(),
        shared.max_packet_size_(),
    )
    .await
    {
        Result::Ok(retain) => retain,
        Result::Err(_) => {
            exit_(&ctl);
            return;
        }
    };
    let mut rx_scratch = rx_scratch_retain
        .try_owning()
        .expect("刚分配的暂存应当可独占");

    // 建流期 message 缓冲：每次 `OPEN` / `ACCEPT` / `REJECT` 都要从 message ring
    // 读一条载荷；它属于**重复性**临时分配。这里先借用连接级 Scope 分配一块固定
    // 缓冲（整条连接只分配一次），避免每请求一个全局 `Vec`；每循环 Scope 留给
    // 后面的每轮临时对象（见 `dev-notes/alloc-ownership-20261029-0230.md`）。
    let msg_cap = shared.pending_msg_cap_();
    // `cap == 0` 时仍分配 1 字节，避免零长度布局；实际可用切片仍是 `[..0]`。
    let msg_retain = match crate::connection::alloc_zeroed_bytes_async_(
        shared.reg_().scope_(),
        msg_cap.max(1usize),
    )
    .await
    {
        Result::Ok(retain) => retain,
        Result::Err(_) => {
            exit_(&ctl);
            return;
        }
    };
    let mut msg_buf = msg_retain
        .try_owning()
        .expect("刚分配的 message 缓冲应当可独占");
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
            &mut msg_buf[..msg_cap],
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
                handle_request_::<C, _>(
                    &shared,
                    &mut public_tx_w,
                    request,
                    &mut msg_buf[..msg_cap],
                    &cancel,
                )
                .await;
            }
            // `park_main_` 在 poll 时**已经取走**提示位（`HintSlot_::poll_wait_` 是
            // 「取出并返回」语义）；这里必须放回去，否则下一轮 `main_step_` 的
            // `take_main_hint_` 看到的是空掩码——提示只唤醒、从不落实（P4 的
            // retired 位正是靠它驱动）。回顶部后 step 立即 `take_main_hint_` 清空，
            // 因此这一次回放只会多一次 park 循环，不会丢位也不会忙等。
            Some(MainWake_::Hint(bits)) => ctl.raise_main_hint_(bits),
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
#[allow(
    clippy::too_many_arguments,
    reason = "驱动一轮控制面需要全部循环状态（含两块暂存与请求缓冲）"
)]
pub(crate) async fn main_step_<C, K>(
    shared: &MuxShared_<C>,
    ctl: &ControlState_<crate::connection::ConnAllocCfg_<C>>,
    public_tx_w: &mut BufferedTx,
    public_rx_r: &mut BufferedRx,
    rx_scratch: &mut [u8],
    msg_buf: &mut [u8],
    public_read: &mut MainPublicRead_,
    cancel: &K,
) -> bool
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    // 过渡：显式失败路径（`abort_pending_`）仍投 `SessionEvent_`；`Drop` 已改为
    // retired 位（P4），剩余的投递点在 P5 清理。
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
        reap_retired_and_release_::<C, _>(shared, public_tx_w, cancel).await;
    }
    if hints & HINT_MAIN_TX_DRAINED != 0u64
        && let Result::Err(err) = flush_tx_drained_::<C, _>(shared, public_tx_w, cancel).await
    {
        let _ = shared.reg_().mark_failed_(&err, cancel.child_token()).await;
        shared.reg_().cancel_loops_();
        return false;
    }
    if hints & HINT_MAIN_RESET != 0u64
        && let Result::Err(err) = flush_reset_::<C, _>(shared, public_tx_w, cancel).await
    {
        let _ = shared.reg_().mark_failed_(&err, cancel.child_token()).await;
        shared.reg_().cancel_loops_();
        return false;
    }
    // 身份释放放在最后：本轮的 `CLOSE(FIN)` / `CLOSE(RESET)` 已经进公共 TX ring，
    // 释放扫描看到的是最新状态。判据由 `claim_release_` 的一次性 CAS 给出，因此写
    // 循环重复 raise 本提示不会重复释放。
    if hints & HINT_MAIN_RELEASE != 0u64 {
        flush_releasable_::<C, _>(shared, cancel).await;
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
        handle_request_::<C, _>(shared, public_tx_w, request, msg_buf, cancel).await;
    }
    true
}

/// 收尾：关闭队列，并把仍排队的请求以 `Closed` 结束。
fn exit_<C>(ctl: &ControlState_<C>)
where
    C: TrConnAllocCfg + 'static,
{
    ctl.close_requests_();
    while let Some(request) = ctl.try_pop_request_() {
        request.fail_(MainLoopErr_::Mux(MuxError::Closed));
    }
}

/// 处理一条请求。
///
/// 取身份表写锁是协作式异步获取（可取消）；临界区本身只做一次登记 / 查询，不 `await`。
async fn handle_request_<C, K>(
    shared: &MuxShared_<C>,
    public_tx_w: &mut BufferedTx,
    request: MainLoopRequest_<crate::connection::ConnAllocCfg_<C>>,
    msg_buf: &mut [u8],
    cancel: &K,
)
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    // 过渡：显式失败路径仍投 `SessionEvent_`。登记之前先把它落实，保证「丢弃后
    // 立刻重绑 / 重听 / 重新登记同一条 dock 对」与旧 API 的语义一致。
    if shared
        .reg_()
        .drain_session_events_(shared.conn_clock_().now_millis_(), cancel.child_token())
        .await
        .is_err()
    {
        request.fail_(MainLoopErr_::Mux(MuxError::Cancelled));
        return;
    }
    // 同一轮合并的 retired 释放先落实：保证「丢弃后立刻重听」看到的是已释放。
    reap_retired_and_release_::<C, _>(shared, public_tx_w, cancel).await;
    match request {
        MainLoopRequest_::SendControl { frame, reply } => {
            match write_control_frame_async_::<C, _>(public_tx_w, shared, &frame, &[], cancel).await {
                Result::Ok(()) => {
                    let _ = reply.set_(Result::Ok(MainLoopReply_::Unit));
                }
                Result::Err(err) => {
                    let _ = reply.set_(Result::Err(MainLoopErr_::Mux(err)));
                }
            }
        }
        MainLoopRequest_::Bind { local_dock, reply } => {
            if reply.is_abandoned_() {
                let _ = reply.set_(Result::Err(MainLoopErr_::Reserve(ReserveErr_::Cancelled)));
                return;
            }
            let result = if local_dock.is_unspecified() {
                shared
                    .reg_()
                    .bind_any_dock_(cancel.child_token())
                    .await
                    .map(|(dock, unbind_)| MainLoopReply_::Dock { dock, unbind_ })
            } else {
                shared
                    .reg_()
                    .bind_dock_(local_dock, cancel.child_token())
                    .await
                    .map(|(dock, unbind_)| MainLoopReply_::Dock { dock, unbind_ })
            };
            // 登记之后才发现等待方已放弃：**回滚刚登记的身份**，否则这个 dock 会
            // 永久占住（没有句柄能解绑它）。回滚用不可取消令牌：它是收尾动作。
            if reply.is_abandoned_()
                && let Result::Ok(MainLoopReply_::Dock { dock, .. }) = &result
            {
                let _ = shared
                    .reg_()
                    .unbind_dock_(*dock, NonCancellableToken::new())
                    .await;
            }
            let _ = reply.set_(result.map_err(MainLoopErr_::Reserve));
        }
        MainLoopRequest_::Listen {
            local_dock,
            reserve: _reserve,
            reply,
        } => {
            // `reserve` 仍未用于预分配：入向通知目前内联在身份节点里。
            if reply.is_abandoned_() {
                let _ = reply.set_(Result::Err(MainLoopErr_::Reserve(ReserveErr_::Cancelled)));
                return;
            }
            let result = shared
                .reg_()
                .reserve_listener_(local_dock, cancel.child_token())
                .await
                .map(MainLoopReply_::Listener);
            if reply.is_abandoned_() {
                let _ = shared
                    .reg_()
                    .release_listener_(local_dock, NonCancellableToken::new())
                    .await;
            }
            let _ = reply.set_(result.map_err(MainLoopErr_::Reserve));
        }
        MainLoopRequest_::ReserveChannel {
            local_dock,
            remote_dock,
            reply,
        } => {
            if reply.is_abandoned_() {
                let _ = reply.set_(Result::Err(MainLoopErr_::Reserve(ReserveErr_::Cancelled)));
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
                .map(|(owner, msg_)| MainLoopReply_::Channel { owner, msg_ });
            if reply.is_abandoned_() {
                let _ = shared
                    .reg_()
                    .unreserve_channel_(local_dock, remote_dock, NonCancellableToken::new())
                    .await;
            }
            let _ = reply.set_(result.map_err(MainLoopErr_::Reserve));
        }
        MainLoopRequest_::Accept {
            local_dock,
            remote_dock,
            owner,
            role_,
            initial_window,
            reply,
        } => {
            let msg_len = match shared
                .reg_()
                .read_msg_outgoing_into_(local_dock, remote_dock, msg_buf, cancel.child_token())
                .await
            {
                Result::Ok(Option::Some(len)) => len,
                Result::Ok(Option::None) => 0usize,
                Result::Err(ReadOutgoingErr_::TooLarge) => {
                    let _ = reply.set_(Result::Err(MainLoopErr_::Mux(MuxError::FrameTooLarge)));
                    return;
                }
                Result::Err(ReadOutgoingErr_::Cancelled) => {
                    let _ = reply.set_(Result::Err(MainLoopErr_::Mux(MuxError::Cancelled)));
                    return;
                }
            };
            let message = &msg_buf[..msg_len];
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
                        ),
                        message,
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
                                    ),
                                    &[],
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
                                            ),
                                            message,
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
                    let _ = reply.set_(Result::Ok(MainLoopReply_::Unit));
                }
                Result::Err(err) => {
                    let _ = reply.set_(Result::Err(MainLoopErr_::Mux(err)));
                }
            }
        }
        MainLoopRequest_::Reject {
            local_dock,
            remote_dock,
            owner,
            role_,
            reply,
        } => {
            let reason_len = match shared
                .reg_()
                .read_msg_outgoing_into_(local_dock, remote_dock, msg_buf, cancel.child_token())
                .await
            {
                Result::Ok(Option::Some(len)) => len,
                Result::Ok(Option::None) => 0usize,
                Result::Err(ReadOutgoingErr_::TooLarge) => {
                    let _ = reply.set_(Result::Err(MainLoopErr_::Mux(MuxError::FrameTooLarge)));
                    return;
                }
                Result::Err(ReadOutgoingErr_::Cancelled) => {
                    let _ = reply.set_(Result::Err(MainLoopErr_::Mux(MuxError::Cancelled)));
                    return;
                }
            };
            let reason = &msg_buf[..reason_len];
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
                        ),
                        reason,
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
                    let _ = reply.set_(Result::Ok(MainLoopReply_::Unit));
                }
                Result::Err(err) => {
                    let _ = reply.set_(Result::Err(MainLoopErr_::Mux(err)));
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
    ctl: &ControlState_<crate::connection::ConnAllocCfg_<C>>,
    cancel: &K,
) -> Option<MainWake_<crate::connection::ConnAllocCfg_<C>>>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let cancel_fut = cancel.child_token().cancellation();
    let mut cancel_fut = pin!(cancel_fut);
    // `mpmc_chan` 的 `RecvAsync` 是参数载体（实现 `IntoFuture`），不是 `Future`：
    // 先转成 future 再 `pin`，以便跨 poll 持有它的唤醒登记；本次 park 内复用同一份。
    let mut request_fut = pin!(core::future::IntoFuture::into_future(
        ctl.recv_request_async_()
    ));
    let mut taken: Option<MainWake_<crate::connection::ConnAllocCfg_<C>>> = None;
    poll_fn(|cx| {
        if core::future::Future::poll(cancel_fut.as_mut(), cx).is_ready() {
            return Poll::Ready(());
        }
        // 公共 RX 有字节（可能只是前缀的一部分）：回顶部按 chunk 协议继续读。
        if shared.public_().poll_rx_ready_(cx).is_ready() {
            taken = Some(MainWake_::RxReady);
            return Poll::Ready(());
        }
        // 快路径：通道里已有请求就直接取，避免空通道时 `recv` future 先做有界自旋。
        if let Option::Some(request) = ctl.try_pop_request_() {
            taken = Some(MainWake_::Request(request));
            return Poll::Ready(());
        }
        match core::future::Future::poll(request_fut.as_mut(), cx) {
            Poll::Ready(Result::Ok(request)) => {
                taken = Some(MainWake_::Request(request));
                return Poll::Ready(());
            }
            // 通道关闭：连接收尾。
            Poll::Ready(Result::Err(_)) => return Poll::Ready(()),
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
                                ),
                                &[],
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

/// 落实 `Drop` 置上的 retired 位：释放 listener / telegraph / dock，并把**响应方**
/// 未裁决 channel 的 `REJECT` 发出去、再让它进拆流宽限。
///
/// 发起方（`inbound_ == None`）的撤销预留已在注册表的 `reap_retired_` 内部
/// 完成；响应方必须先回帧，而那需要公共 TX 写端，所以在这里做。
///
/// 顺序刻意是「先发 `REJECT`、后 `release_channel_`」：对端此刻正在等裁决，身份
/// 先转宽限态并不影响它，但帧必须真的发出去（设计稿 §6.7）。
async fn reap_retired_and_release_<C, K>(
    shared: &MuxShared_<C>,
    public_tx_w: &mut BufferedTx,
    cancel: &K,
) where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let Result::Ok(pending_rejects) = shared.reg_().reap_retired_(cancel.child_token()).await
    else {
        return;
    };
    for (local_dock, remote_dock) in pending_rejects {
        let _ = write_control_frame_async_::<C, _>(
            public_tx_w,
            shared,
            &ControlFrame_::plain_(FrameKind::Reject, 0u8, local_dock, remote_dock),
            &[],
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
    }
}

/// 把 `TX_DRAINED` 的子流收成 `CLOSE(FIN)`，写进公共 TX ring。
///
/// FIN 的**上线确认**仍由复用循环完成：它在把公共 TX ring 刷空之后置
/// `local_fin_sent_`。因此这里只置 `FIN_ENQUEUED`，不提前唤醒 `close_async`。
async fn flush_tx_drained_<C, K>(
    shared: &MuxShared_<C>,
    public_tx_w: &mut BufferedTx,
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let Result::Ok(drained) = shared
        .reg_()
        .scan_tx_drained_(cancel.child_token())
        .await
    else {
        return Result::Ok(());
    };
    for (pair, owner) in drained {
        write_control_frame_async_::<C, _>(
            public_tx_w,
            shared,
            &ControlFrame_::plain_(FrameKind::Close, flags::K_FIN, pair.0, pair.1),
            &[],
            cancel,
        )
        .await?;
        if owner.set_fin_enqueued_() {
            shared.reg_().raise_write_hint_(HINT_W_FIN_ENQUEUED);
        }
    }
    Result::Ok(())
}

/// 把「应用已关闭接收方向」的子流收成 `CLOSE(RESET)`，写进公共 TX ring。
///
/// 与 FIN 相同：上线确认由复用循环在公共 TX 刷空后完成；这里只置
/// `RESET_ENQUEUED`，不提前唤醒 `ChannelRx::close_async`。若本端发送方向也已封口，
/// 注册表扫描会等到 `LOCAL_FIN_SENT` 之后才返回该子流，保证 RESET 不越过 FIN。
async fn flush_reset_<C, K>(
    shared: &MuxShared_<C>,
    public_tx_w: &mut BufferedTx,
    cancel: &K,
) -> Result<(), MuxError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let Result::Ok(pending) = shared
        .reg_()
        .scan_reset_pending_(cancel.child_token())
        .await
    else {
        return Result::Ok(());
    };
    for (pair, owner) in pending {
        write_control_frame_async_::<C, _>(
            public_tx_w,
            shared,
            &ControlFrame_::plain_(FrameKind::Close, flags::K_RESET, pair.0, pair.1),
            &[],
            cancel,
        )
        .await?;
        if owner.set_reset_enqueued_() {
            shared.reg_().raise_write_hint_(HINT_W_RESET_ENQUEUED);
        }
    }
    Result::Ok(())
}

/// 释放所有「两个方向都已在协议层收尾」的子流身份（T1 的收口动作）。
///
/// # 为什么在 main
///
/// 注册表身份的唯一写者是 `main_loop`；写循环只把本地表 / 半部收干净、置位并
/// raise [`HINT_MAIN_RELEASE`]，不再自己取注册表写锁。判据与「恰好一次」的语义都在
/// [`ChannelRegistry_::release_done_channels_`] 内部（`claim_release_` 的一次性 CAS），
/// 因此这里的重复调用没有副作用。
///
/// # 释放之后要唤醒谁
///
/// - 读循环按 `RELEASED` 位摘本地读表并关接收环写端（`HINT_R_RELEASE`）；
/// - 写循环按同一位摘本地写表、关发送环消费端（`HINT_W_RELEASED`）；
/// - 应用侧的 `flush_async` / `close_async` 等待者由 `notify_both_progress_` 叫醒。
///
/// 「最后一条活跃子流与对端双向收尾」时的连接收尾也在这里：`became_idle` 只有
/// `main_loop` 看得到（它是唯一写者），因此不再需要 `maybe_release_` 在复用循环里
/// 顺手 `cancel_loops_`。
///
/// [`ChannelRegistry_::release_done_channels_`]:
///     crate::connection::ChannelRegistry_::release_done_channels_
async fn flush_releasable_<C, K>(shared: &MuxShared_<C>, cancel: &K)
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let now_millis = shared.conn_clock_().now_millis_();
    let Result::Ok((released, became_idle)) = shared
        .reg_()
        .release_done_channels_(now_millis, cancel.child_token())
        .await
    else {
        return;
    };
    // 「本次确实释放过」才可能触发连接收尾：与旧的 `maybe_release_` 行为一致，
    // 不在空扫描上做判断（closing + 无活跃子流的情形由计时循环兜底）。
    let did_release = !released.is_empty();
    for (pair, owner) in released {
        // 身份已释放：两侧循环据此摘本地表并关环半部。事实是 `RELEASED` 位，
        // 提示只负责唤醒。
        shared.reg_().raise_read_hint_(HINT_R_RELEASE);
        shared.reg_().raise_write_hint_(HINT_W_RELEASED);
        // 两个方向都不会再有新的进展：等待中的 `TrBuffFlush` / `close_async` 必须醒来
        // 并按终局复检（否则它们会永远 park 在一条已经从注册表消失的子流上）。
        owner.notify_both_progress_();
        // 上报子流关闭（正常协议收尾）。`claim_release_` 是一次性的 CAS，因此这里
        // **恰好上报一次**；超时与建流超时两条路径不经过本函数，各自在 `timer_` 里上报。
        shared.metrics_().on_channel_closed(
            pair.0,
            pair.1,
            ChannelCloseReason::Fin,
            now_millis.saturating_sub(owner.created_millis_()),
        );
    }
    // **优雅关闭的收口点**：连接处于等待关闭，而这是最后一条活跃子流——它与对端
    // 双向收尾意味着连接上再也没有需要排空的东西，此刻就收尾。
    //
    // 收尾幂等（取消令牌是幂等操作），计时循环在它自己的扫描里也可能同时发现
    // 「可以收尾」——两边谁先看到谁做。放在这里而不是全交给计时循环，是因为延迟
    // 敏感：计时循环可能正睡在一个较远的期限上。
    if did_release && became_idle && shared.reg_().is_closing_() {
        // 本版没有写泵：复用循环成帧时直接写进传输，因此收尾就是「几个循环一起
        // 取消」——连接内部没有任何还压着字节的环要排空（见
        // `ChannelRegistry_::cancel_loops_` 的文档）。
        shared.reg_().cancel_loops_();
    }
}

/// 把一条控制帧写进公共 TX ring（背压时 park 等 `tx_space`）。
///
/// 帧头由 `FrameHeader` + `encode_header_` 重新编码；`payload` 是 `OPEN` / `ACCEPT` /
/// `REJECT` 从建流期 message ring 读出的载荷，其它控制帧传空切片。载荷不再挂在
/// `ControlFrame_` 上，避免为每条控制帧持有一个 `Vec<u8>`。
async fn write_control_frame_async_<C, K>(
    public_tx_w: &mut BufferedTx,
    shared: &MuxShared_<C>,
    frame: &ControlFrame_,
    payload: &[u8],
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
        payload.len(),
        frame.window_(),
    );
    let (head, head_len) = encode_header_(&header).map_err(|_| MuxError::MalformedFrame)?;
    loop {
        match PublicChunkIo_::try_write_chunk_(
            public_tx_w,
            &head[..head_len],
            payload,
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

    use super::{main_step_, park_main_, try_read_public_frame_, MainPublicRead_, MainWake_};
    use crate::{
        connection::{
            main_loop::{MainLoopReply_, MainLoopRequest_},
            ring_::{new_buffered_channel, BufferedRx, BufferedTx},
            session_::MuxShared_,
            test_support_::TestMuxConfig_,
            ChannelHandle, ControlState_, Dock, DockBinding, FrameKind, MuxConnection, Tcs_,
            HINT_MAIN_RELEASE, HINT_MAIN_RETIRED,
        },
        flow_ctrl::{Credit, RecvTotal, WindowReport},
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
        ctl: &ControlState_<crate::connection::SingleConnAllocCfg>,
        tx_w: &mut BufferedTx,
        rx_r: &mut BufferedRx,
        scratch: &mut [u8],
        public_read: &mut MainPublicRead_,
        dock: Dock,
    ) -> DockBinding<TestMuxConfig_> {
        let cancel = NonCancellableToken::new();
        let scope = crate::connection::new_test_scope_();
        let tcs = block_on_ready_(Tcs_::new_(&scope)).expect("测试域可用");
        let waiter = tcs.clone();
        let submitted = block_on_ready_(ctl.submit_async_(
            MainLoopRequest_::Bind {
                local_dock: dock,
                reply: tcs,
            },
            NonCancellableToken::new(),
        ));
        assert!(submitted.is_ok(), "Bind 请求应能入队");
        // 本辅助只驱动 Bind，不读 message；给 `main_step_` 一个足够的栈上缓冲。
        let mut msg_buf = [0u8; 512usize];
        assert!(block_on_ready_(main_step_::<TestMuxConfig_, _>(
            shared,
            ctl,
            tx_w,
            rx_r,
            scratch,
            &mut msg_buf,
            public_read,
            &cancel
        )));
        let reply = block_on_ready_(core::future::poll_fn(|cx| waiter.poll_wait_(cx)))
            .expect("Bind 请求应完成")
            .expect("Bind 应成功");
        let MainLoopReply_::Dock { dock, unbind_ } = reply else {
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

    /// 测试响应方未裁决句柄被丢弃后的时序：`main_step_` 先回 `REJECT` 再进宽限。
    /// - 手段：`new_test_bundle_` 建无循环连接；直接 `reserve_inbound_` 登记一条入向
    ///   待决子流，用 `take_pending_inbound_` 取出并构造响应方 `ChannelHandle`；
    ///   未裁决就 drop 它（只置 retired 位），先 poll 一次 `park_main_` 拿到提示并
    ///   回放（复刻主循环的 `Hint` 分支），再跑一轮 `main_step_`。
    /// - 判断：`main_step_` 之后该 dock 对处于 `WaitClose`（身份已进宽限期），且公共
    ///   TX 的第一条帧是 `REJECT`——说明 retired 位被 main 落实，并且「先回 `REJECT`、
    ///   后释放身份」的顺序成立；若提示在 park 处丢失，则两个断言都不会成立。
    ///
    /// Test the ordering after a responder handle is dropped before settling.
    /// Method: build a loop-less connection with `new_test_bundle_`; register a
    /// pending inbound via `reserve_inbound_`, take it with
    /// `take_pending_inbound_` and wrap it in a responder `ChannelHandle`; drop it
    /// unsettled (retired bit only), poll `park_main_` once to obtain the hint and
    /// replay it (mirroring the main loop's `Hint` arm), then run one `main_step_`.
    /// Assertion: the dock pair is `WaitClose` after the step and the first public
    /// TX frame is `REJECT`, proving the retired bit is applied by main and that
    /// `REJECT` precedes the identity release; a lost hint fails both assertions.
    #[test]
    fn drop_pending_responder_sends_reject_() {
        let (conn, shared) = MuxConnection::<TestMuxConfig_>::new_test_bundle_(
            HandshakeOpts {
                basic_opts: BasicOpts::default(),
            },
            TestMuxConfig_,
        );
        let ctl = conn.core_().control_().clone();
        let (mut tx_w, mut tx_r) =
            new_buffered_channel(Owned::new_uninit_slice(64usize, CoreAlloc))
                .expect("建公共 TX 环");
        let (_rx_w, mut rx_r) =
            new_buffered_channel(Owned::new_uninit_slice(64usize, CoreAlloc))
                .expect("建公共 RX 环");
        let mut scratch = [0u8; 256usize];
        let mut rx_public_read = MainPublicRead_::new_();
        let mut tx_public_read = MainPublicRead_::new_();
        let cancel = NonCancellableToken::new();
        let local = Dock::new(7u32);
        let remote = Dock::new(9u32);

        // 造一条响应方待决入向请求：登记 → 标 Pending → 由 listener 侧取走。
        block_on_ready_(shared.reg_().reserve_inbound_(
            local,
            remote,
            WindowReport::new(0u64 as RecvTotal, 1024u32 as Credit),
            0u64,
            512usize,
            NonCancellableToken::new(),
        ))
        .expect("入向登记应当成功");
        let (_remote, owner, msg_app) =
            block_on_ready_(shared.reg_().take_pending_inbound_(
                local,
                NonCancellableToken::new(),
            ))
            .expect("查询待决入向请求应当成功")
            .expect("应当能取到待决入向请求");
        let handle = ChannelHandle::new_(conn.clone(), local, remote, owner, msg_app);
        drop(handle);

        // 复刻主循环：park 取到提示后必须回放，下一轮 `main_step_` 才看得到。
        let wake = block_on_ready_(park_main_::<TestMuxConfig_, _>(&shared, &ctl, &cancel))
            .expect("丢弃句柄后 main 应当被唤醒");
        let MainWake_::Hint(bits) = wake else {
            panic!("park 应当因提示而返回");
        };
        assert_ne!(
            bits & HINT_MAIN_RETIRED,
            0u64,
            "提示里应当含 retired 位"
        );
        ctl.raise_main_hint_(bits);

        // 本用例驱动的是 retired → REJECT，不需要读 message ring；给一个栈上缓冲。
        let mut msg_buf = [0u8; 512usize];
        assert!(block_on_ready_(main_step_::<TestMuxConfig_, _>(
            &shared,
            &ctl,
            &mut tx_w,
            &mut rx_r,
            &mut scratch,
            &mut msg_buf,
            &mut rx_public_read,
            &cancel,
        )));
        assert!(
            block_on_ready_(shared.reg_().is_wait_close_(
                local,
                remote,
                0u64,
                NonCancellableToken::new(),
            ))
            .expect("查询宽限态应当成功"),
            "响应方句柄丢弃后身份应进入拆流宽限"
        );
        let (header, _head_len, _total_len) = try_read_public_frame_(
            &mut tx_r,
            &mut tx_public_read,
            &mut scratch,
            shared.max_packet_size_(),
        )
        .expect("读公共 TX 应当成功")
        .expect("公共 TX 里应当有一条 REJECT");
        assert_eq!(header.kind(), FrameKind::Reject, "第一帧应当是 REJECT");
    }

    /// 测试 main 在 `HINT_MAIN_RELEASE` 上释放「两个方向都收尾」的子流身份。
    /// - 手段：`new_test_bundle_` 建无循环连接；`reserve_channel_` 登记一条子流并拿到
    ///   owner；手动置上两个方向的收尾位（`LOCAL_FIN_SENT` + `APP_RX_CLOSED`，并把
    ///   `LOCAL_RESET_FLUSHED` 一起置上以表示 RESET 已上线）；raise
    ///   `HINT_MAIN_RELEASE` 后跑一轮 `main_step_`。
    /// - 判断：`owner.is_released_()` 为真、注册表里该 dock 对进入 `WaitClose`，说明
    ///   身份释放确实由 main 落实（写循环不再持有这条职责）；`claim_release_` 的第二
    ///   次调用返回假，说明「恰好一次」的闸门仍然有效。
    ///
    /// Test that main releases a fully-closed channel identity on
    /// `HINT_MAIN_RELEASE`.
    /// Method: build a loop-less connection with `new_test_bundle_`, register a
    /// channel through `reserve_channel_`, set both directions' completion bits
    /// (`LOCAL_FIN_SENT` + `APP_RX_CLOSED`, plus `LOCAL_RESET_FLUSHED` to model an
    /// already-flushed RESET), raise `HINT_MAIN_RELEASE` and run one `main_step_`.
    /// Assertion: `owner.is_released_()` holds and the dock pair is `WaitClose`,
    /// proving main performs the release; a second `claim_release_` returns false,
    /// proving the exactly-once gate is intact.
    #[test]
    fn step_releases_done_channel_() {
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
        let mut scratch = [0u8; 256usize];
        let mut msg_buf = [0u8; 512usize];
        let mut public_read = MainPublicRead_::new_();
        let cancel = NonCancellableToken::new();
        let local = Dock::new(7u32);
        let remote = Dock::new(9u32);

        let (owner, _msg) = block_on_ready_(shared.reg_().reserve_channel_(
            local,
            remote,
            0u64,
            512usize,
            NonCancellableToken::new(),
        ))
        .expect("登记子流身份应当成功");
        assert!(!owner.is_released_(), "刚登记的身份不应已释放");
        // 两个方向都收尾：发送方向（FIN 已上线）+ 接收方向（应用关读、RESET 已上线）。
        owner.set_local_fin_sent_();
        owner.set_app_rx_closed_();
        owner.set_local_reset_flushed_();
        ctl.raise_main_hint_(HINT_MAIN_RELEASE);
        assert!(block_on_ready_(main_step_::<TestMuxConfig_, _>(
            &shared,
            &ctl,
            &mut tx_w,
            &mut rx_r,
            &mut scratch,
            &mut msg_buf,
            &mut public_read,
            &cancel,
        )));
        assert!(owner.is_released_(), "main 应当释放已完成收尾的身份");
        assert!(
            block_on_ready_(shared.reg_().is_wait_close_(
                local,
                remote,
                0u64,
                NonCancellableToken::new(),
            ))
            .expect("查询宽限态应当成功"),
            "释放后的身份应处于拆流宽限期"
        );
        assert!(
            !owner.claim_release_(),
            "释放闸门只允许认领一次，重复扫描不得再次成功"
        );
    }

    /// 测试「应用关读但 RESET 还没上线」的子流不会被 main 提前释放。
    /// - 手段：`new_test_bundle_` + `reserve_channel_` 登记身份；置
    ///   `LOCAL_FIN_SENT` + `APP_RX_CLOSED`，**不**置 `LOCAL_RESET_FLUSHED`；raise
    ///   `HINT_MAIN_RELEASE` 跑一轮 `main_step_`，然后再置 `LOCAL_RESET_FLUSHED` 跑第二轮。
    /// - 判断：第一轮之后身份仍在活跃表（未 `RELEASED`、非 `WaitClose`），第二轮之后
    ///   才释放——保证 `CLOSE(RESET)` 不会因为身份先被摘掉而丢掉。
    ///
    /// Test that a channel whose RESET is still pending is not released early.
    /// Method: register an identity, set `LOCAL_FIN_SENT` + `APP_RX_CLOSED` but
    /// leave `LOCAL_RESET_FLUSHED` unset; run one `main_step_`, then set the RESET
    /// bit and run a second step.
    /// Assertion: after the first step the identity is still active (not
    /// `RELEASED`, not `WaitClose`); only the second step releases it, so the
    /// pending `CLOSE(RESET)` cannot be dropped.
    #[test]
    fn step_waits_for_reset_before_release_() {
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
        let mut scratch = [0u8; 256usize];
        let mut msg_buf = [0u8; 512usize];
        let mut public_read = MainPublicRead_::new_();
        let cancel = NonCancellableToken::new();
        let local = Dock::new(11u32);
        let remote = Dock::new(13u32);

        let (owner, _msg) = block_on_ready_(shared.reg_().reserve_channel_(
            local,
            remote,
            0u64,
            512usize,
            NonCancellableToken::new(),
        ))
        .expect("登记子流身份应当成功");
        owner.set_local_fin_sent_();
        owner.set_app_rx_closed_();

        ctl.raise_main_hint_(HINT_MAIN_RELEASE);
        assert!(block_on_ready_(main_step_::<TestMuxConfig_, _>(
            &shared,
            &ctl,
            &mut tx_w,
            &mut rx_r,
            &mut scratch,
            &mut msg_buf,
            &mut public_read,
            &cancel,
        )));
        assert!(
            !owner.is_released_(),
            "RESET 尚未上线时不得释放身份（否则 RESET 会丢）"
        );
        assert!(
            !block_on_ready_(shared.reg_().is_wait_close_(
                local,
                remote,
                0u64,
                NonCancellableToken::new(),
            ))
            .expect("查询宽限态应当成功"),
            "身份应当仍在活跃表里"
        );

        // RESET 上线之后，下一次提示才允许释放。
        owner.set_local_reset_flushed_();
        ctl.raise_main_hint_(HINT_MAIN_RELEASE);
        assert!(block_on_ready_(main_step_::<TestMuxConfig_, _>(
            &shared,
            &ctl,
            &mut tx_w,
            &mut rx_r,
            &mut scratch,
            &mut msg_buf,
            &mut public_read,
            &cancel,
        )));
        assert!(owner.is_released_(), "RESET 上线后应当释放身份");
    }
}
