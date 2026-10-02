use core::marker::PhantomData;

use abs_buff::{
    TrBuffRead, TrBuffWrite,
    gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use abs_smux::conn::{TrChannelHalf, TrChannelHandle};
use buffex::x_deps::abs_buff;

use crate::{
    connection::{
        Dock, FrameKind, MuxConnection, MuxError, TrMuxConfig,
        channel_half::{ChannelRx, ChannelTx, RxRing_, TxRing_},
        owner_::{ChannelOwner_, ChannelState_},
        ring_::new_buffered_channel_,
        signal_::{ControlFrame_, ReadEvent_, TrEventSender_, WriteEvent_},
        types_::SessionMark_,
        util_::read_available_into_vec_,
    },
    flow_ctrl::FlowCtrl,
};

/// 一个入向建流请求的待决句柄。
///
/// 由 [`TrChannelListener::income_async`](abs_smux::conn::TrChannelListener::income_async) 产出；调用方用
/// [`TrChannelHandle::accept_async`] 接受并交付欢迎信息，或
/// [`TrChannelHandle::reject_async`] 拒绝并说明理由。句柄本身也是
/// [`TrChannelHalf`]，因此可以在决定之前查看两侧 dock。
///
/// 句柄上的 `(local_dock, remote_dock)` 就是这条待决子流的身份：响应方在自己的
/// `local_dock` 上用 `remote_dock` 区分不同请求端的连接（见
/// [`crate::connection`] 模块文档 §4.1）。
pub struct ChannelHandle<'s, 'f, R, W, C, Rt>
where
    C: TrMuxConfig,
{
    /// 连接对象：`accept_async` 用它建子流环并登记会话侧半部。
    conn_: &'f MuxConnection<R, W, C, Rt>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,

    /// 借用关系与连接泛型的占位；语义同 [`ChannelListener`]。
    _mark_: SessionMark_<'s, 'f, R, W, C, Rt>}

impl<'s, 'f, R, W, C, Rt> ChannelHandle<'s, 'f, R, W, C, Rt>
where
    C: TrMuxConfig,
{
    /// 由连接与 dock 对构造（只允许 `income_async` 调用）。
    pub(crate) fn new_(
        conn: &'f MuxConnection<R, W, C, Rt>,
        local_dock: Dock,
        remote_dock: Dock,
    ) -> Self {
        ChannelHandle {
            conn_: conn,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
            _mark_: PhantomData}
    }
}

impl<'s, 'f, R, W, C, Rt> TrChannelHandle for ChannelHandle<'s, 'f, R, W, C, Rt>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
{
    type Err = MuxError<R::Err, W::Err>;

    type Tx = ChannelTx<TxRing_<C::Buff, C::Alloc>>;
    type Rx = ChannelRx<RxRing_<C::Buff, C::Alloc>>;

    type AcceptAsync<'a, Wb>
        = MuxAcceptAsync<'a, 's, 'f, 'a, R, W, C, Rt, Wb>
    where
        Self: 'a,
        Wb: 'a + TrBuffWrite;

    type RejectAsync<'a, Rb>
        = MuxRejectAsync<'a, 's, 'f, 'a, R, W, C, Rt, Rb>
    where
        Self: 'a,
        Rb: 'a + TrBuffRead;

    fn accept_async<'a, Wb>(&'a mut self, welcome: &'a mut Wb) -> Self::AcceptAsync<'a, Wb>
    where
        Wb: TrBuffWrite,
    {
        MuxAcceptAsync::new(self, welcome)
    }

    fn reject_async<'a, Rb>(&'a mut self, reason: &'a mut Rb) -> Self::RejectAsync<'a, Rb>
    where
        Rb: TrBuffRead,
    {
        MuxRejectAsync::new(self, reason)
    }
}

impl<'s, 'f, R, W, C, Rt> TrChannelHalf for ChannelHandle<'s, 'f, R, W, C, Rt>
where
    C: TrMuxConfig,
{
    type Data = u8;
    type Dock = Dock;

    fn local_dock(&self) -> Self::Dock {
        self.local_dock_
    }

    fn remote_dock(&self) -> Self::Dock {
        self.remote_dock_
    }

    /// 待决的入向请求尚未建环，因此两个方向都还没有关闭。
    fn is_tx_closed(&self) -> bool {
        false
    }

    /// 同 [`TrChannelHalf::is_tx_closed`]：待决期间恒为假。
    fn is_rx_closed(&self) -> bool {
        false
    }
}

/// [`TrChannelHandle::accept_async`] 的 step 函数。
#[gen_may_cancel_future(MuxAccept, pub)]
async fn mux_accept_async_<'a, 's, 'f, R, W, C, Rt, Wb, K>(
    handle: &'a mut ChannelHandle<'s, 'f, R, W, C, Rt>,
    welcome: &'a mut Wb,
    _cancel: K,
) -> Result<
    (
        ChannelTx<TxRing_<C::Buff, C::Alloc>>,
        ChannelRx<RxRing_<C::Buff, C::Alloc>>,
    ),
    MuxError<R::Err, W::Err>,
>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    Wb: TrBuffWrite<u8> + 'a,
    K: TrCancellationToken,
{
    let conn = handle.conn_;
    let local = handle.local_dock_;
    let remote = handle.remote_dock_;
    // 取回对端 `OPEN` 里带过来的窗口通告（由读循环在入向建流时存下）。
    let peer_report = match conn.reg_().take_inbound_report_(local, remote) {
        Option::Some(report) => report,
        Option::None => return Result::Err(MuxError::Closed)};

    let capacity = conn.config_().channel_capacity();
    let alloc = conn.config_().allocator();
    let (tx_w, tx_r) = match new_buffered_channel_(conn.config_().make_buff(capacity), alloc.clone())
    {
        Result::Ok(pair) => pair,
        Result::Err(_) => {
            conn.reg_().release_channel_(local, remote);
            return Result::Err(MuxError::Closed);
        }
    };
    let (rx_w, rx_r) = match new_buffered_channel_(conn.config_().make_buff(capacity), alloc.clone())
    {
        Result::Ok(pair) => pair,
        Result::Err(_) => {
            conn.reg_().release_channel_(local, remote);
            return Result::Err(MuxError::Closed);
        }
    };

    let mut flow = FlowCtrl::new(conn.config_().policy(), capacity);
    // 与主动方同理：读循环已经用 `initial_window` 回了自己的 `OPEN`，这里把那份
    // 快照记进 `RecvWindow`，否则对端第一帧会被误判为越权。
    flow.recv_window_mut().report();
    // 被动方的发送额度来自主动方（对端）`OPEN` 里的通告。
    if let Result::Err(err) = flow.send_window_mut().on_report(peer_report) {
        conn.reg_().release_channel_(local, remote);
        return Result::Err(MuxError::FlowCtrl(err));
    }
    let owner = ChannelOwner_::new_(ChannelState_::new_(flow), alloc);
    conn.reg_().attach_owner_(local, remote, owner.clone());
    let _ = conn.w_events_().try_send_event_(WriteEvent_::Attach {
        local_dock: local,
        remote_dock: remote,
        owner: owner.clone(),
        reader_: tx_r});
    let _ = conn.r_events_().try_send_event_(ReadEvent_::Attach {
        local_dock: local,
        remote_dock: remote,
        owner: owner.clone(),
        writer_: rx_w});

    // `ACCEPT` 不带窗口字段。`welcome` 的契约在 `abs_smux` 里是 `TrBuffWrite`，
    // 即「由库写入应用缓冲」的方向，与「把欢迎信息发给对端」相反，语义存疑；本轮
    // 按空载荷发出，并把它记为遗留（见 dev-notes §2.11）。
    let _ = welcome;
    let _ = conn.w_events_().try_send_event_(WriteEvent_::Control {
        frame_: ControlFrame_::with_window_(
            FrameKind::Accept,
            0u8,
            local,
            remote,
            Option::None,
            Vec::new(),
        )});

    Result::Ok((
        ChannelTx::new_(
            TxRing_::new_(tx_w, owner.clone(), conn.w_events_().clone(), local, remote),
            local,
            remote,
        ),
        ChannelRx::new_(
            RxRing_::new_(rx_r, conn.w_events_().clone(), local, remote),
            local,
            remote,
        ),
    ))
}

/// [`TrChannelHandle::reject_async`] 的 step 函数。
#[gen_may_cancel_future(MuxReject, pub)]
async fn mux_reject_async_<'a, 's, 'f, R, W, C, Rt, Rb, K>(
    handle: &'a mut ChannelHandle<'s, 'f, R, W, C, Rt>,
    reason: &'a mut Rb,
    cancel: K,
) -> Result<usize, MuxError<R::Err, W::Err>>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    Rb: TrBuffRead<u8> + 'a,
    K: TrCancellationToken,
{
    let conn = handle.conn_;
    let local = handle.local_dock_;
    let remote = handle.remote_dock_;
    let payload =
        read_available_into_vec_(reason, conn.opts_().basic_opts.max_packet_size, cancel.child_token())
            .await;
    let written = payload.len();
    let _ = conn.w_events_().try_send_event_(WriteEvent_::Control {
        frame_: ControlFrame_::with_window_(
            FrameKind::Reject,
            0u8,
            local,
            remote,
            Option::None,
            payload,
        )});
    // 拒绝即拆掉这条待决子流（环尚未建立）。
    conn.reg_().release_channel_(local, remote);
    Result::Ok(written)
}
