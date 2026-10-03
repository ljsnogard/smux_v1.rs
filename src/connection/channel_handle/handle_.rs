use core::{
    borrow::BorrowMut,
    mem::MaybeUninit,
};

use abs_buff::{
    TrBuffRead, TrBuffWrite,
    gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use abs_smux::chan::{
    ChannelBuffAlloc, TrChannelHalf, TrChannelHandle, TrPrepareChannelRing,
};
use buffex::x_deps::abs_buff;

use crate::{
    connection::{
        Dock, FrameKind, HandleError, MuxConnection, MuxError, TrConnCfg,
        channel_half::{ChannelRx, ChannelTx},
        owner_::{ChannelOwner_, ChannelState_, EstablishOutcome_, wait_establish_},
        ring_::new_buffered_channel_,
        signal_::{ControlFrame_, ReadEvent_, TrEventSender_, WriteEvent_},
        util_::read_available_into_vec_,
    },
    flow_ctrl::{Credit, FlowCtrl, RecvTotal, ReportThresholds_, TrFlowCtrlPolicy, WindowReport},
};

/// 一个入向建流请求的待决句柄。
///
/// **不借用连接**：自己持有一份 [`MuxConnection`] 克隆，因此生命周期参数从公开
/// 类型上消失，可以存进结构体、可以从函数返回。
pub struct ChannelHandle<C, S>
where
    C: TrConnCfg,
{
    /// 连接智能指针：`accept_async` 用它建子流环并登记会话侧半部。
    conn_: MuxConnection<C, S>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,

    /// 本端是否为**发起方**（[`open_channel_async`] 产出）。
    is_initiator_: bool,

    /// 发起方暂存的开场消息（响应方恒为空）：`OPEN` 要等 `accept_async` 才发。
    message_: Vec<u8>,

    /// 成功接受后的**子流共享状态**。
    accepted_owner_: Option<ChannelOwner_<C::Alloc>>,

    /// 接受后本端要通告的**接收窗口**（由接收环实际容量算出）。
    accepted_initial_window_: Credit,

    /// 是否已经裁决完毕（接受 / 拒绝 / 已拆）。
    settled_: bool,
}

impl<C, S> ChannelHandle<C, S>
where
    C: TrConnCfg,
{
    /// 由连接与 dock 对构造**响应方**句柄（只允许 `income_async` 调用）。
    pub(crate) fn new_(
        conn: MuxConnection<C, S>,
        local_dock: Dock,
        remote_dock: Dock,
    ) -> Self {
        ChannelHandle {
            conn_: conn,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
            is_initiator_: false,
            message_: Vec::new(),
            accepted_owner_: Option::None,
            accepted_initial_window_: 0u32 as Credit,
            settled_: false,
        }
    }

    /// 由连接、dock 对与开场消息构造**发起方**句柄。
    pub(crate) fn new_initiator_(
        conn: MuxConnection<C, S>,
        local_dock: Dock,
        remote_dock: Dock,
        message: Vec<u8>,
    ) -> Self {
        ChannelHandle {
            conn_: conn,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
            is_initiator_: true,
            message_: message,
            accepted_owner_: Option::None,
            accepted_initial_window_: 0u32 as Credit,
            settled_: false,
        }
    }
}

/// 未裁决就丢弃句柄 = **放弃建立**。
impl<C, S> Drop for ChannelHandle<C, S>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        if self.settled_ {
            return;
        }
        abort_pending_(
            &self.conn_,
            self.local_dock_,
            self.remote_dock_,
            self.is_initiator_,
        );
    }
}

/// 裁决失败 / 未裁决就丢弃时的收尾：按**角色**把这条待决子流收拾干净。
fn abort_pending_<C, S>(
    conn: &MuxConnection<C, S>,
    local: Dock,
    remote: Dock,
    is_initiator: bool,
) where
    C: TrConnCfg,
{
    let core = conn.core_();
    if is_initiator {
        core.reg_().unreserve_channel_(local, remote);
        return;
    }
    let _ = core.w_events_().try_send_event_(WriteEvent_::Control {
        frame_: ControlFrame_::with_window_(
            FrameKind::Reject,
            0u8,
            local,
            remote,
            Option::None,
            Vec::new(),
        ),
    });
    core.reg_().release_channel_(local, remote);
}

impl<C, S> ChannelHandle<C, S>
where
    C: TrConnCfg,
{
    /// 「接受」这一步：校验调用方给的两块缓冲，建两条环，并把会话侧半部交给两个
    /// 循环。
    fn accept_prepare_<P>(&mut self, prepare: P) -> AcceptOutcomeProj_<C, S>
    where
        P: TrPrepareChannelRing<C::Buff, C::Data>,
    {
        let conn = self.conn_.clone();
        let local = self.local_dock_;
        let remote = self.remote_dock_;

        // 1. 调用方给出本子流的环存储。
        let ChannelBuffAlloc { tx_buff, rx_buff, .. } = prepare.prepare();

        // 2. 对端的窗口通告：发起方还没有（对端 `OPEN` 未到，读循环稍后写进发送
        //    窗口），响应方在登记时已存下。
        let peer_report = if self.is_initiator_ {
            Option::None
        } else {
            match conn.core_().reg_().take_inbound_report_(local, remote) {
                Option::Some(report) => Option::Some(report),
                Option::None => return Result::Err(HandleError::Mux(MuxError::Closed)),
            }
        };

        // 3. 建环 + 登记 + 移交会话侧半部。
        let (owner, tx, rx, initial) =
            install_channel_(&conn, local, remote, tx_buff, rx_buff, peer_report)?;
        self.accepted_initial_window_ = initial;
        self.accepted_owner_ = Option::Some(owner);
        Result::Ok((tx, rx))
    }
}

impl<C, S> TrChannelHandle<C> for ChannelHandle<C, S>
where
    C: TrConnCfg,
{
    type Err = HandleError<C>;

    type Tx = ChannelTx<C, S>;
    type Rx = ChannelRx<C, S>;

    type AcceptAsync<'f, Wb, P> = MuxAcceptAsync<'f, 'f, C, S, Wb>
    where
        Self: 'f,
        Wb: 'f + TrBuffWrite<C::Data>,
        P: TrPrepareChannelRing<C::Buff, C::Data>;

    type RejectAsync<'f, Rb> = MuxRejectAsync<'f, 'f, C, S, Rb>
    where
        Self: 'f,
        Rb: 'f + TrBuffRead<C::Data>;

    fn accept_async<'f, Wb, P>(
        &'f mut self,
        welcome: &'f mut Wb,
        prepare: P,
    ) -> Self::AcceptAsync<'f, Wb, P>
    where
        Wb: 'f + TrBuffWrite<C::Data>,
        P: TrPrepareChannelRing<C::Buff, C::Data>,
    {
        let accept_result = self.accept_prepare_(prepare);
        if accept_result.is_err() {
            self.settled_ = true;
            abort_pending_(
                &self.conn_,
                self.local_dock_,
                self.remote_dock_,
                self.is_initiator_,
            );
        }
        MuxAcceptAsync::new(self, welcome, accept_result)
    }

    fn reject_async<'f, Rb>(&'f mut self, reason: &'f mut Rb) -> Self::RejectAsync<'f, Rb>
    where
        Rb: TrBuffRead<C::Data>,
    {
        MuxRejectAsync::new(self, reason)
    }
}

impl<C, S> TrChannelHalf<C> for ChannelHandle<C, S>
where
    C: TrConnCfg,
{
    fn local_dock(&self) -> C::Dock {
        self.local_dock_
    }

    fn remote_dock(&self) -> C::Dock {
        self.remote_dock_
    }

    fn is_tx_closed(&self) -> bool {
        false
    }

    fn is_rx_closed(&self) -> bool {
        false
    }
}

/// [`TrChannelHandle::accept_async`] 的 step 函数：**建流最终裁决**。
#[gen_may_cancel_future(MuxAccept, pub, new(pub(crate)))]
async fn mux_accept_async_<'f, C, S, Wb, K>(
    handle: &'f mut ChannelHandle<C, S>,
    welcome: &'f mut Wb,
    accepted: AcceptOutcomeProj_<C, S>,
    cancel: K,
) -> AcceptOutcomeProj_<C, S>
where
    C: TrConnCfg + 'f,
    S: 'f,
    Wb: TrBuffWrite<u8> + 'f,
    K: TrCancellationToken,
{
    let conn = handle.conn_.clone();
    let local = handle.local_dock_;
    let remote = handle.remote_dock_;

    let (tx, rx) = accepted?;

    if handle.is_initiator_ {
        // 发起方：此刻才发 `OPEN`（通告的接收窗口取决于接收环容量）。
        let message = core::mem::take(&mut handle.message_);
        let initial = handle.accepted_initial_window_;
        send_open_(&conn, local, remote, initial, message);
        // 对端的 `OPEN` 到达时，读循环会把它的接收窗口写进发送窗口；`ACCEPT` /
        // `REJECT` 到达时唤醒这里。
        let Some(owner) = handle.accepted_owner_.clone() else {
            handle.settled_ = true;
            return Result::Err(HandleError::Mux(MuxError::Closed));
        };
        match wait_establish_(conn.core_().reg_(), &owner, cancel.child_token()).await? {
            EstablishOutcome_::Accepted => {
                handle.settled_ = true;
                Result::Ok((tx, rx))
            }
            EstablishOutcome_::Refused => {
                conn.core_().reg_().release_channel_(local, remote);
                handle.settled_ = true;
                Result::Err(HandleError::Refused)
            }
        }
    } else {
        // 响应方：对端 `OPEN` 的窗口通告已在登记时存下（读循环存的）。
        let initial = handle.accepted_initial_window_;
        // 先回自己的 `OPEN`，再发 `ACCEPT`。
        send_open_(&conn, local, remote, initial, Vec::new());
        // `welcome` 的契约本轮按空载荷发出，记为遗留。
        let _ = welcome;
        let _ = conn
            .core_()
            .w_events_()
            .try_send_event_(WriteEvent_::Control {
                frame_: ControlFrame_::with_window_(
                    FrameKind::Accept,
                    0u8,
                    local,
                    remote,
                    Option::None,
                    Vec::new(),
                ),
            });
        handle.settled_ = true;
        Result::Ok((tx, rx))
    }
}

/// 「接受」这一步的产物：该 channel 的收发半边，或一个连接错误。
type AcceptOutcomeProj_<C, S> = Result<(ChannelTx<C, S>, ChannelRx<C, S>), HandleError<C>>;

/// 建流最终裁决的产物。
type InstallOutcome_<C, S> = (
    ChannelOwner_<<C as TrConnCfg>::Alloc>,
    ChannelTx<C, S>,
    ChannelRx<C, S>,
    Credit,
);

/// 建流最终裁决的公共部分：按调用方给的缓冲建两条环、登记共享状态、把会话侧半部
/// 交给两个循环。
fn install_channel_<C, S>(
    conn: &MuxConnection<C, S>,
    local: Dock,
    remote: Dock,
    tx_buff: C::Buff,
    mut rx_buff: C::Buff,
    peer_report: Option<WindowReport>,
) -> Result<InstallOutcome_<C, S>, HandleError<C>>
where
    C: TrConnCfg,
{
    let core = conn.core_();
    let alloc = core.config_().allocator();
    let policy = core.config_().policy();
    let rx_cap = BorrowMut::<[MaybeUninit<u8>]>::borrow_mut(&mut rx_buff).len();

    // 本端接收窗口由**接收环容量**决定；发送窗口先按同一初值起算，随后被对端 `OPEN`
    // 的通告覆盖。
    let initial = policy.initial_window(rx_cap);
    let thresholds = ReportThresholds_::new_(policy, initial);
    let mut flow = FlowCtrl::new(policy, rx_cap);
    flow.recv_window_mut().report();
    if let Option::Some(report) = peer_report
        && let Result::Err(err) = flow.send_window_mut().on_report(report)
    {
        core.reg_().release_channel_(local, remote);
        return Result::Err(HandleError::FlowCtrl(err));
    }

    // 两条环：应用写 / 循环读的是发送环，循环写 / 应用读的是接收环。
    let (tx_w, tx_r) = match new_buffered_channel_(tx_buff, alloc.clone()) {
        Result::Ok(pair) => pair,
        Result::Err(_) => {
            core.reg_().release_channel_(local, remote);
            return Result::Err(HandleError::RingRejected);
        }
    };
    let (rx_w, rx_r) = match new_buffered_channel_(rx_buff, alloc.clone()) {
        Result::Ok(pair) => pair,
        Result::Err(_) => {
            core.reg_().release_channel_(local, remote);
            return Result::Err(HandleError::RingRejected);
        }
    };

    let owner = ChannelOwner_::new_(ChannelState_::new_(flow, thresholds), alloc.clone());
    core.reg_().attach_owner_(local, remote, owner.clone());
    let _ = core.w_events_().try_send_event_(WriteEvent_::Attach {
        local_dock: local,
        remote_dock: remote,
        owner: owner.clone(),
        reader_: tx_r,
    });
    let _ = core.r_events_().try_send_event_(ReadEvent_::Attach {
        local_dock: local,
        remote_dock: remote,
        owner: owner.clone(),
        writer_: rx_w,
    });

    Result::Ok((
        owner.clone(),
        ChannelTx::new_(tx_w, owner.clone(), conn.clone(), local, remote),
        ChannelRx::new_(rx_r, conn.clone(), local, remote),
        initial,
    ))
}

/// 把 `OPEN` 投给写循环。
fn send_open_<C, S>(
    conn: &MuxConnection<C, S>,
    local: Dock,
    remote: Dock,
    window: Credit,
    payload: Vec<u8>,
) where
    C: TrConnCfg,
{
    let _ = conn
        .core_()
        .w_events_()
        .try_send_event_(WriteEvent_::Control {
            frame_: ControlFrame_::with_window_(
                FrameKind::Open,
                0u8,
                local,
                remote,
                Option::Some((0u64 as RecvTotal, window)),
                payload,
            ),
        });
}

/// [`TrChannelHandle::reject_async`] 的 step 函数。
#[gen_may_cancel_future(MuxReject, pub, new(pub(crate)))]
async fn mux_reject_async_<'f, C, S, Rb, K>(
    handle: &'f mut ChannelHandle<C, S>,
    reason: &'f mut Rb,
    cancel: K,
) -> Result<usize, HandleError<C>>
where
    C: TrConnCfg + 'f,
    S: 'f,
    Rb: TrBuffRead<u8> + 'f,
    K: TrCancellationToken,
{
    let conn = handle.conn_.clone();
    let local = handle.local_dock_;
    let remote = handle.remote_dock_;
    let payload = read_available_into_vec_(
        reason,
        conn.core_().opts_().basic_opts.max_packet_size,
        cancel.child_token(),
    )
    .await;
    let written = payload.len();
    if handle.is_initiator_ {
        conn.core_().reg_().unreserve_channel_(local, remote);
    } else {
        let _ = conn
            .core_()
            .w_events_()
            .try_send_event_(WriteEvent_::Control {
                frame_: ControlFrame_::with_window_(
                    FrameKind::Reject,
                    0u8,
                    local,
                    remote,
                    Option::None,
                    payload,
                ),
            });
        conn.core_().reg_().release_channel_(local, remote);
    }
    handle.settled_ = true;
    Result::Ok(written)
}
