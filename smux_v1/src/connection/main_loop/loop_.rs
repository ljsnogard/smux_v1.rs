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
        Dock, FrameHeader, FrameKind, MuxError, ReadOutgoingErr_, ReserveErr_, TrConnCfg,
        control_state_::ControlState_,
        flags,
        frame_::{K_MAX_FRAME_HEADER, encode_header_},
        frame_parser_::decode_header_sync_,
        main_loop::{
            EstablishRole_, MainLoopErr_, MainLoopReply_, MainLoopRequest_, MainLoopResult_,
        },
        TrConnAllocCfg,
        owner_::EstablishOutcome_,
        public_channel_::{ChunkReadState_, ChunkWriteErr_, PublicChunkIo_},
        ring_::{BufferedRx, BufferedTx},
        session_::race_cancel_,
        session_::MuxShared_,
        HINT_MAIN_RELEASE, HINT_MAIN_RESET, HINT_MAIN_RETIRED, HINT_MAIN_TIMER,
        HINT_MAIN_TX_DRAINED, HINT_MAIN_WINDOW_DIRTY,
        mux_connection::{K_CLOSE_NONE, K_CLOSE_TIMEOUT},
        tcs_::Tcs_,
        signal_::{
            ControlFrame_, HINT_R_RELEASE, HINT_W_FIN_ENQUEUED, HINT_W_PEER_CLOSED,
            HINT_W_RELEASED, HINT_W_RESET_ENQUEUED, HINT_W_TX_CLOSED,
        },
        timer_::{K_TIMER_ACTION_BATCH, TimerAction_},
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

/// 每轮最多处理多少条接收窗口通告（栈上定长缓冲，零分配）。
pub(crate) const K_WINDOW_REPORT_BATCH: usize = 32usize;

/// 每轮 tick 最多缓存多少条待写控制帧。
///
/// `Abort` 每个动作产 2 条（`CLOSE(FIN)` + `CLOSE(RESET)`），其余动作至多 1 条，
/// 因此取动作批次的 2 倍；不会溢出。
pub(crate) const K_TICK_FRAME_BATCH: usize = 2usize * K_TIMER_ACTION_BATCH;

/// `main_loop` 的 tick 跨轮状态：**已认领但还没写进公共 TX 的控制帧**。
///
/// # 为什么需要它
///
/// `timer_scan_` 的认领是**一次性**的（`claim_abort_` 等原子位）：动作一旦认领，
/// 下一轮扫描不会再返回同一条子流。而不可重建的帧（`CLOSE(FIN)` / `CLOSE(RESET)` /
/// `REJECT`）不能丢，因此它们在公共 TX 满时留在本结构里，下一轮先补写。
///
/// 定容、零分配；`ControlFrame_` 是纯帧头（`Copy`），不持载荷。
pub(crate) struct MainTickState_<A>
where
    A: TrConnAllocCfg + 'static,
{
    /// 待写帧的 FIFO（`None` = 空位）。
    frames_: [Option<ControlFrame_>; K_TICK_FRAME_BATCH],

    /// 有效前缀长度。
    len_: usize,

    /// 正在等答复的 `Close` 请求（同一时刻至多一个：`close_async` 的语义是幂等的）。
    ///
    /// 收尾发生时**先** `set_` 它、**再** `cancel_loops_()`，调用方拿到的是「收尾
    /// 结果」而不是「被取消」（设计稿 §6.3 / Q8）。
    close_reply_: Option<Tcs_<MainLoopResult_<A>>>,
}

impl<A> MainTickState_<A>
where
    A: TrConnAllocCfg + 'static,
{
    /// 空状态。
    pub(crate) fn new_() -> Self {
        MainTickState_ {
            frames_: [Option::None; K_TICK_FRAME_BATCH],
            len_: 0usize,
            close_reply_: Option::None,
        }
    }

    /// 记下等待答复的 `Close` 请求；已有等待者时覆盖（旧的那条由 `set_` 的
    /// abandoned 语义自然丢弃——调用方已经放弃它）。
    fn set_close_reply_(&mut self, reply: Tcs_<MainLoopResult_<A>>) {
        if let Option::Some(old) = self.close_reply_.replace(reply) {
            let _ = old.set_(Result::Ok(MainLoopReply_::Closed(false)));
        }
    }

    /// 取走等待答复的 `Close` 请求。
    fn take_close_reply_(&mut self) -> Option<Tcs_<MainLoopResult_<A>>> {
        self.close_reply_.take()
    }

    /// 追加一条待写帧（调用方保证不溢出）。
    fn push_(&mut self, frame: ControlFrame_) {
        debug_assert!(
            self.len_ < K_TICK_FRAME_BATCH,
            "tick 帧队列溢出：动作批次与帧批次必须满足 2:1"
        );
        if self.len_ < K_TICK_FRAME_BATCH {
            self.frames_[self.len_] = Option::Some(frame);
            self.len_ += 1usize;
        }
    }

    /// 队头帧（不移除）。
    fn first_(&self) -> Option<ControlFrame_> {
        self.frames_.first().copied().flatten()
    }

    /// 移除队头帧（左移一格；批次很小，O(n) 可接受）。
    fn pop_front_(&mut self) {
        if self.len_ == 0usize {
            return;
        }
        for idx in 1usize..self.len_ {
            self.frames_[idx - 1usize] = self.frames_[idx];
        }
        self.len_ -= 1usize;
        self.frames_[self.len_] = Option::None;
    }

    /// 是否还有待写帧。
    pub(crate) fn has_pending_(&self) -> bool {
        self.len_ > 0usize
    }
}

/// `MainTickState_` 在连接级泛型 `C` 下的别名（状态里的完成源要 `C` 的分配器配置）。
pub(crate) type MainTickStateOf_<C> = MainTickState_<crate::connection::ConnAllocCfg_<C>>;

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
    let mut tick = MainTickStateOf_::<C>::new_();

    // 先发布一次初始期限：`timer_loop` 据此睡到「第一个 `PULSE` 到期」而不是
    // 傻等 `channel_timeout`。此后每轮 `tick_` 结束都会重新发布。
    {
        let now = shared.conn_clock_().now_millis_();
        let pulse = shared.channel_timeout_millis_() / 2u64;
        shared
            .control_()
            .timer_signal_()
            .publish_(now.saturating_add(pulse));
    }

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
            &mut tick,
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
                    &mut tick,
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
    tick: &mut MainTickStateOf_<C>,
    cancel: &K,
) -> bool
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    // 连接级失败优先：已登记的 `Close` 等待者要拿到 `Closed(false)`，而不是永远
    // 等一个不会再来的收尾信号。
    if shared.reg_().fail_kind_().is_some() {
        if let Option::Some(reply) = tick.take_close_reply_() {
            let _ = reply.set_(Result::Ok(MainLoopReply_::Closed(false)));
        }
        shared.reg_().cancel_loops_();
        return false;
    }

    // 合并提示：定时到期 → retired（身份释放）→ 窗口通告 → FIN/RESET → 身份释放。
    //
    // tick 放在最前：它的「连接级关闭动作」可能直接结束连接，后续工作已无意义。
    let hints = ctl.take_main_hint_();
    if hints & HINT_MAIN_TIMER != 0u64 {
        tick_::<C, _>(shared, public_tx_w, tick, cancel).await;
    }
    if hints & HINT_MAIN_WINDOW_DIRTY != 0u64 {
        flush_window_dirty_::<C, _>(shared, public_tx_w, cancel).await;
    }
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
        flush_releasable_::<C, _>(shared, tick, cancel).await;
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
        handle_request_::<C, _>(shared, public_tx_w, tick, request, msg_buf, cancel).await;
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
    tick: &mut MainTickStateOf_<C>,
    request: MainLoopRequest_<crate::connection::ConnAllocCfg_<C>>,
    msg_buf: &mut [u8],
    cancel: &K,
)
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    // 同一轮合并的 retired 释放先落实：保证「丢弃后立刻重听」看到的是已释放。
    reap_retired_and_release_::<C, _>(shared, public_tx_w, cancel).await;
    match request {
        MainLoopRequest_::Close { reply } => {
            handle_close_request_::<C>(shared, tick, reply).await;
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
        MainLoopRequest_::OpenTelegraph {
            local_dock,
            reader_,
            writer_,
            reply,
        } => {
            // 身份登记由 main 做（I3）：`reserve_telegraph_` 产出该端点的身份句柄。
            let owner = match shared
                .reg_()
                .reserve_telegraph_(local_dock, cancel.child_token())
                .await
            {
                Result::Ok(owner) => owner,
                Result::Err(err) => {
                    let _ = reply.set_(Result::Err(MainLoopErr_::Reserve(err)));
                    return;
                }
            };
            // 两个环半部经 **Attach inbox** 交给两个循环（与 channel 的 `Attach` 同一条
            // 通道，`TgAttach` 事件因此彻底删除）。inbox 满时按背压等空位——环半部是
            // 一次性资源，不能丢。
            let write_ok = match shared.reg_().try_push_write_attach_(
                crate::connection::signal_::WriteAttach_::new_tg_(
                    local_dock,
                    owner.clone(),
                    reader_,
                ),
            ) {
                Result::Ok(()) => true,
                Result::Err(item) => shared
                    .reg_()
                    .push_write_attach_async_(item, cancel.child_token())
                    .await
                    .is_ok(),
            };
            let read_ok = write_ok
                && match shared.reg_().try_push_read_attach_(
                    crate::connection::signal_::ReadAttach_::new_tg_(
                        local_dock,
                        owner.clone(),
                        writer_,
                    ),
                ) {
                    Result::Ok(()) => true,
                    Result::Err(item) => shared
                        .reg_()
                        .push_read_attach_async_(item, cancel.child_token())
                        .await
                        .is_ok(),
                };
            if !read_ok {
                // 循环已不在：撤销刚登记的身份，再如实答复 Closed。
                let _ = shared
                    .reg_()
                    .release_telegraph_(local_dock, NonCancellableToken::new())
                    .await;
                let _ = reply.set_(Result::Err(MainLoopErr_::Mux(MuxError::Closed)));
                return;
            }
            let _ = reply.set_(Result::Ok(MainLoopReply_::Telegraph(owner)));
        }
        MainLoopRequest_::TakeInbound { local_dock, reply } => {
            let taken = shared
                .reg_()
                .take_pending_inbound_(local_dock, cancel.child_token())
                .await;
            match taken {
                Result::Ok(Option::Some((remote_dock, owner, msg_))) => {
                    // 答复**之前**先判放弃：`set_` 在 `Abandoned` 上会 drop 结果，
                    // 那对 message ring 两半来说等于丢失（对端会一直等 `ACCEPT`）。
                    if reply.is_abandoned_() {
                        let _ = shared
                            .reg_()
                            .restore_pending_inbound_(
                                local_dock,
                                remote_dock,
                                msg_,
                                NonCancellableToken::new(),
                            )
                            .await;
                        return;
                    }
                    let _ = reply.set_(Result::Ok(MainLoopReply_::Inbound(Option::Some((
                        remote_dock, owner, msg_,
                    )))));
                }
                Result::Ok(Option::None) => {
                    let _ = reply.set_(Result::Ok(MainLoopReply_::Inbound(Option::None)));
                }
                Result::Err(err) => {
                    let _ = reply.set_(Result::Err(MainLoopErr_::Reserve(err)));
                }
            }
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
                    if role_ == EstablishRole_::Initiator {
                        // 发起方的 `OPEN` 已经进公共 TX：此后若应用放弃，对端已经知道
                        // 这条子流，必须回 `REJECT`（`reap_retired_` 据此判据）。
                        owner.set_local_open_sent_();
                    }
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

/// 处理一条 `Close` 请求：进入「等待关闭」，把完成源留给 `tick_`。
///
/// 收尾的判定与执行都在 `tick_`（连接级关闭动作）：活跃子流归零 ⇒ `Closed(true)`；
/// 整体超时 ⇒ `Closed(false)`。这里只负责「进入状态 + 立刻催一轮 tick」。
async fn handle_close_request_<C>(
    shared: &MuxShared_<C>,
    tick: &mut MainTickStateOf_<C>,
    reply: Tcs_<MainLoopResult_<crate::connection::ConnAllocCfg_<C>>>,
) where
    C: TrConnCfg,
{
    if reply.is_abandoned_() {
        // 调用方已经放弃：协议动作仍要推进（进入等待关闭），但不保留完成源。
        shared
            .reg_()
            .enter_waiting_close_(shared.conn_clock_().now_millis_());
        shared
            .control_()
            .raise_main_hint_(crate::connection::HINT_MAIN_TIMER);
        return;
    }
    // 连接已经失败：没有「优雅」可言，直接如实答复 false。
    if shared.reg_().fail_kind_().is_some() {
        let _ = reply.set_(Result::Ok(MainLoopReply_::Closed(false)));
        shared.reg_().cancel_loops_();
        return;
    }
    shared
        .reg_()
        .enter_waiting_close_(shared.conn_clock_().now_millis_());
    tick.set_close_reply_(reply);
    // 立刻来一轮 tick：关闭点上本来就没有活跃子流时，不必等下一个期限。
    shared
        .control_()
        .raise_main_hint_(crate::connection::HINT_MAIN_TIMER);
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
async fn flush_releasable_<C, K>(
    shared: &MuxShared_<C>,
    tick: &mut MainTickStateOf_<C>,
    cancel: &K,
) where
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
        // **先完成 `Close` 的 TCS，再取消循环**（Q8）：连接是与对端双向收敛的，
        // 调用方应当拿到 `Closed(true)` 而不是「被取消」。
        if let Option::Some(reply) = tick.take_close_reply_() {
            let _ = reply.set_(Result::Ok(MainLoopReply_::Closed(true)));
        }
        // 本版没有写泵：复用循环成帧时直接写进传输，因此收尾就是「几个循环一起
        // 取消」——连接内部没有任何还压着字节的环要排空（见
        // `ChannelRegistry_::cancel_loops_` 的文档）。
        shared.reg_().cancel_loops_();
    }
}

/// 执行一轮定时决策（原 `timer_loop` 的「扫描 + 认领 + 投递」整段搬到这里）。
///
/// # 为什么在 main
///
/// `timer_scan_` 的判定与认领都是**身份表写操作**（认领 `PULSE` 职责、拆流、建流
/// 超时、优雅关闭 finalize、`WaitClose` 回收），而身份表的唯一写者是 `main_loop`
/// （设计稿 §6.4 / I3）。`timer_loop` 收缩成纯计时后，这里成为唯一的定时决策者。
///
/// # 组帧与背压
///
/// 所有帧都进**公共 TX ring**（FIFO）：`Abort` 的 `CLOSE(FIN)` 一定排在
/// `CLOSE(RESET)` 之前，与 R6/R7 建立的「RESET 不越过 FIN」顺序一致。公共 TX 满时
/// 帧留在 [`MainTickState_`] 里（不可重建的帧不能丢），下一轮先补写。
async fn tick_<C, K>(
    shared: &MuxShared_<C>,
    public_tx_w: &mut BufferedTx,
    tick: &mut MainTickStateOf_<C>,
    cancel: &K,
) where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    // 先把上一轮没写出去的帧尽力补发；仍写不进就本轮不再产生新动作（避免队列
    // 继续增长——虽然定容，但认领是不可逆的，攒太多会占满缓冲区）。
    flush_tick_frames_::<C>(shared, public_tx_w, tick);
    if tick.has_pending_() {
        return;
    }

    let now = shared.conn_clock_().now_millis_();
    let timeout_millis = shared.channel_timeout_millis_();
    let pulse_millis = timeout_millis / 2u64;
    let mut actions = [TimerAction_::Idle; K_TIMER_ACTION_BATCH];
    let Result::Ok(scan) = shared
        .reg_()
        .timer_scan_(
            now,
            pulse_millis,
            timeout_millis,
            &mut actions,
            cancel.child_token(),
        )
        .await
    else {
        return;
    };

    // 连接级关闭动作优先：等待关闭已经不需要再排空（活跃子流归零），或关闭整体
    // 超时——两种情形都到此为止，本轮认领的逐子流动作已无意义。
    if scan.close_action_() != K_CLOSE_NONE {
        let timed_out = scan.close_action_() == K_CLOSE_TIMEOUT;
        if timed_out {
            shared.reg_().mark_forced_close_();
        }
        // **先完成 `Close` 的 TCS，再取消循环**（Q8）：调用方拿到的是「收尾结果」，
        // 而不是「被取消」。`Closed(true)` = 与对端双向收敛；`false` = 整体超时。
        if let Option::Some(reply) = tick.take_close_reply_() {
            let _ = reply.set_(Result::Ok(MainLoopReply_::Closed(!timed_out)));
        }
        shared.reg_().cancel_loops_();
        return;
    }

    for action in &actions[..scan.actions_()] {
        deliver_timer_action_::<C, _>(shared, action, tick, cancel).await;
    }

    // 发布下一次期限；批次满则立刻再来一轮（`now` 会被 `max(1)` 兜成 1 ms 后）。
    let next = if scan.is_full_() {
        now
    } else {
        scan.next_millis_().min(now.saturating_add(timeout_millis))
    };
    shared.control_().timer_signal_().publish_(next);

    flush_tick_frames_::<C>(shared, public_tx_w, tick);
}

/// 落实一条定时动作：状态 / 身份副作用立即做，控制帧进 [`MainTickState_`]。
///
/// 副作用与组帧严格按「先状态、后帧」的顺序：即使帧因背压留到下一轮，状态也已经
/// 是终局，`scan_write_released_` / `scan_fin_enqueued_` 之类的位扫描不会漏。
async fn deliver_timer_action_<C, K>(
    shared: &MuxShared_<C>,
    action: &TimerAction_,
    tick: &mut MainTickStateOf_<C>,
    cancel: &K,
) where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    match action {
        TimerAction_::Idle => {}

        TimerAction_::Finalize {
            local_dock,
            remote_dock,
        } => {
            // 复用「应用丢弃发送半边」路径：置 `APP_TX_CLOSED`，写循环在发送环排空后
            // 置 `TX_DRAINED`，再由本循环组 `CLOSE(FIN)`。
            let owner = shared
                .reg_()
                .channel_owner_(*local_dock, *remote_dock, cancel.child_token())
                .await;
            if let Result::Ok(Option::Some(owner)) = owner {
                let _ = owner.set_app_tx_closed_();
                shared.reg_().raise_write_hint_(HINT_W_TX_CLOSED);
            }
        }

        TimerAction_::Pulse {
            local_dock,
            remote_dock,
            report,
        } => {
            // `PULSE` 与 `WINDOW_UPDATE` 共用「携带窗口通告」的形状：保活的同时把本端
            // 接收窗口重同步一次（协议 §7.1）。可重建（下一 tick 会再到期），但这里
            // 仍然入队而不是丢弃：入队才保证与同批的 CLOSE 保持 FIFO。
            tick.push_(ControlFrame_::with_window_(
                FrameKind::Pulse,
                if report.is_reset() {
                    flags::K_TOTAL_RESET
                } else {
                    0u8
                },
                *local_dock,
                *remote_dock,
                Option::Some((report.recv_total(), report.window())),
            ));
        }

        TimerAction_::Abort {
            local_dock,
            remote_dock,
        } => {
            let (local, remote) = (*local_dock, *remote_dock);
            // 1. 先取状态句柄：身份一转宽限态就再也取不到了。
            let owner = match shared
                .reg_()
                .channel_owner_(local, remote, cancel.child_token())
                .await
            {
                Result::Ok(owner) => owner,
                Result::Err(_) => return,
            };
            // 2. **先**落宽限态：此后到达的在途帧一律静默丢弃。必须早于本地表动作，
            //    否则读循环可能把在途数据帧判成「未知子流」而终止整条连接。
            let _ = shared
                .reg_()
                .release_channel_(
                    local,
                    remote,
                    shared.conn_clock_().now_millis_(),
                    cancel.child_token(),
                )
                .await;
            // 3. 两个方向都记成「协议收尾」：发送环里剩下的字节被主动放弃，接收方向
            //    也不再收。`mark_released_` 让两侧循环摘本地表（绕过 `claim_release_`
            //    的两方向判据——空闲超时本就没有「应用收尾」）。
            if let Option::Some(owner) = owner {
                owner.set_local_fin_sent_();
                let _ = owner.claim_local_reset_();
                owner.mark_released_();
                owner.notify_both_progress_();
                let now_millis = shared.conn_clock_().now_millis_();
                shared.metrics_().on_channel_closed(
                    local,
                    remote,
                    ChannelCloseReason::IdleTimeout,
                    now_millis.saturating_sub(owner.created_millis_()),
                );
            }
            // 4. 尽力告诉对端两个方向都收尾：`FIN` 关掉对端的接收方向、`RESET` 关掉
            //    对端的发送方向，一条帧只能表达一个方向，因此发两条（FIFO 保证顺序）。
            tick.push_(ControlFrame_::plain_(
                FrameKind::Close,
                flags::K_FIN,
                local,
                remote,
            ));
            tick.push_(ControlFrame_::plain_(
                FrameKind::Close,
                flags::K_RESET,
                local,
                remote,
            ));
            // 5. 本地两个循环各自摘表（事实位 `RELEASED`，提示只负责唤醒）。
            shared.reg_().raise_write_hint_(HINT_W_RELEASED);
            shared.reg_().raise_read_hint_(HINT_R_RELEASE);
        }

        TimerAction_::RejectEstablish {
            local_dock,
            remote_dock,
        } => {
            let (local, remote) = (*local_dock, *remote_dock);
            let owner = match shared
                .reg_()
                .channel_owner_(local, remote, cancel.child_token())
                .await
            {
                Result::Ok(owner) => owner,
                Result::Err(_) => return,
            };
            // 对端此刻一定在等裁决，必须明确回 `REJECT` 把它放走。
            tick.push_(ControlFrame_::plain_(
                FrameKind::Reject,
                0u8,
                local,
                remote,
            ));
            let _ = shared
                .reg_()
                .release_channel_(
                    local,
                    remote,
                    shared.conn_clock_().now_millis_(),
                    cancel.child_token(),
                )
                .await;
            if let Option::Some(owner) = owner {
                owner.set_establish_settled_();
                owner.notify_establish_();
                let now_millis = shared.conn_clock_().now_millis_();
                shared.metrics_().on_channel_closed(
                    local,
                    remote,
                    ChannelCloseReason::EstablishTimeout,
                    now_millis.saturating_sub(owner.created_millis_()),
                );
            }
        }
    }
}

/// 把 [`MainTickState_`] 里已组好的控制帧**非阻塞**写进公共 TX ring。
///
/// 写不进（ring 当前空闲不足 `2 + len`）就停在这里，剩余的留到下一轮——不能 park，
/// 否则 `main_loop` 会被一个控制帧的背压卡住（§15.1 风险 3）。
fn flush_tick_frames_<C>(
    shared: &MuxShared_<C>,
    public_tx_w: &mut BufferedTx,
    tick: &mut MainTickStateOf_<C>,
) where
    C: TrConnCfg,
{
    while let Option::Some(frame) = tick.first_() {
        match try_write_control_frame_::<C>(public_tx_w, shared, &frame) {
            Result::Ok(true) => tick.pop_front_(),
            // ring 满：保留队头，下一轮再试。
            Result::Ok(false) => break,
            // 帧本身非法（超长）：丢弃队头，避免永久卡住整条队列。帧头是内部的
            // 定长编码，走到这里说明协商上限与帧头不一致。
            Result::Err(_) => tick.pop_front_(),
        }
    }
}

/// 尝试把一条无载荷控制帧写进公共 TX ring；`Ok(false)` 表示当前空间不足。
///
/// 与 [`write_control_frame_async_`] 的区别只有一处：**不 park**。定时决策产生的帧
/// 不能把 `main_loop` 卡在背压上。
fn try_write_control_frame_<C>(
    public_tx_w: &mut BufferedTx,
    shared: &MuxShared_<C>,
    frame: &ControlFrame_,
) -> Result<bool, MuxError>
where
    C: TrConnCfg,
{
    let header = FrameHeader::new_(
        frame.kind_(),
        frame.flags_(),
        frame.local_dock_(),
        frame.remote_dock_(),
        0usize,
        frame.window_(),
    );
    let (head, head_len) = encode_header_(&header).map_err(|_| MuxError::MalformedFrame)?;
    match PublicChunkIo_::try_write_chunk_(
        public_tx_w,
        &head[..head_len],
        &[],
        shared.max_packet_size_(),
    ) {
        Result::Ok(()) => {
            shared.public_().notify_tx_ready_();
            Result::Ok(true)
        }
        Result::Err(ChunkWriteErr_::WouldBlock) => Result::Ok(false),
        Result::Err(_) => Result::Err(MuxError::FrameTooLarge),
    }
}

/// 把「接收窗口快照待通告」的子流收成 `WINDOW_UPDATE`，写进公共 TX ring。
///
/// # 判定为什么由 main 做
///
/// `should_report` 的判定是**消费式**的（取了就记为已通告），因此必须与「帧真的写
/// 进 ring」在同一个成功路径上：先确认公共 TX 还有空间，**再**取快照。空间不足时
/// 一个都不取——快照留在窗口里，下一轮 `HINT_MAIN_WINDOW_DIRTY` 再来。
///
/// 空间预算按最坏帧长（`2 + K_MAX_FRAME_HEADER`）估算，因此本轮取出的每一条都一定
/// 写得进；不会出现「取了快照却写不进去」而丢通告。
async fn flush_window_dirty_<C, K>(
    shared: &MuxShared_<C>,
    public_tx_w: &mut BufferedTx,
    cancel: &K,
) where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    // 每条通告最坏占用（chunk 前缀 + 帧头上界）；WINDOW_UPDATE 无载荷。
    let per_frame = 2usize.saturating_add(K_MAX_FRAME_HEADER);
    let free = public_tx_w
        .producer_state()
        .map(|(free, _closed)| free)
        .unwrap_or(0usize);
    let budget = free / per_frame.max(1usize);
    if budget == 0usize {
        // 没有空间：保留快照（不取），重新挂起提示，等 mux 搬走后再来。
        shared.control_().raise_main_hint_(HINT_MAIN_WINDOW_DIRTY);
        return;
    }
    // 定长缓冲（栈上，零分配）：窗口通告是低频事件，一轮最多处理
    // `K_WINDOW_REPORT_BATCH` 条；超出部分留到下一轮（快照没有被取走，仍在窗口里）。
    let mut reports: [Option<((Dock, Dock), WindowReport)>; K_WINDOW_REPORT_BATCH] =
        [Option::None; K_WINDOW_REPORT_BATCH];
    let Result::Ok(count) = shared
        .reg_()
        .take_window_reports_(budget, &mut reports, cancel.child_token())
        .await
    else {
        return;
    };
    if count == budget.min(K_WINDOW_REPORT_BATCH) && count == K_WINDOW_REPORT_BATCH {
        // 本轮可能有剩余快照：挂起提示，下一轮继续。
        shared.control_().raise_main_hint_(HINT_MAIN_WINDOW_DIRTY);
    }
    for (pair, report) in reports[..count].iter().flatten() {
        let frame = ControlFrame_::with_window_(
            FrameKind::WindowUpdate,
            if report.is_reset() {
                flags::K_TOTAL_RESET
            } else {
                0u8
            },
            pair.0,
            pair.1,
            Option::Some((report.recv_total(), report.window())),
        );
        match try_write_control_frame_::<C>(public_tx_w, shared, &frame) {
            Result::Ok(true) => {}
            // 预算已按最坏帧长算过，正常不会到这里；保守起见停下来，剩余快照
            // 由下一轮提示重试。
            Result::Ok(false) | Result::Err(_) => {
                shared.control_().raise_main_hint_(HINT_MAIN_WINDOW_DIRTY);
                break;
            }
        }
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

    use super::{
        main_step_, park_main_, try_read_public_frame_, MainPublicRead_, MainTickStateOf_, MainWake_,
    };
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
        let mut tick = MainTickStateOf_::<TestMuxConfig_>::new_();
        assert!(block_on_ready_(main_step_::<TestMuxConfig_, _>(
            shared,
            ctl,
            tx_w,
            rx_r,
            scratch,
            &mut msg_buf,
            public_read,
            &mut tick,
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
        let mut tick = MainTickStateOf_::<TestMuxConfig_>::new_();
        assert!(block_on_ready_(main_step_::<TestMuxConfig_, _>(
            &shared,
            &ctl,
            &mut tx_w,
            &mut rx_r,
            &mut scratch,
            &mut msg_buf,
            &mut rx_public_read,
            &mut tick,
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
        let mut tick = MainTickStateOf_::<TestMuxConfig_>::new_();
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
            &mut tick,
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
        let mut tick = MainTickStateOf_::<TestMuxConfig_>::new_();
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
            &mut tick,
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
            &mut tick,
            &cancel,
        )));
        assert!(owner.is_released_(), "RESET 上线后应当释放身份");
    }
}
