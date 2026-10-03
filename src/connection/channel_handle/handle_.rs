use core::{borrow::BorrowMut, mem::MaybeUninit};

use abs_buff::{
    TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use abs_smux::chan::{
    ChannelBuff, TrAcceptBuff, TrChannelHalf, TrChannelHandle, TrPrepareChannelRing,
};
use buffex::x_deps::abs_buff;

use crate::{
    connection::{
        Dock, FrameKind, HandleError, MuxConnection, MuxError, TrMuxConfig,
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
///
/// 由 [`TrChannelListener::income_async`](abs_smux::conn::TrChannelListener::income_async)
/// 产出；调用方用 [`TrChannelHandle::accept_async`] 接受并交付欢迎信息，或
/// [`TrChannelHandle::reject_async`] 拒绝并说明理由。句柄本身也是
/// [`TrChannelHalf`]，因此可以在决定之前查看两侧 dock。
///
/// 句柄上的 `(local_dock, remote_dock)` 就是这条待决子流的身份：响应方在自己的
/// `local_dock` 上用 `remote_dock` 区分不同请求端的连接（见
/// [`crate::connection`] 模块文档 §4.1）。
pub struct ChannelHandle<W, R, S, C>
where
    C: TrMuxConfig,
{
    /// 连接智能指针：`accept_async` 用它建子流环并登记会话侧半部。
    conn_: MuxConnection<W, R, S, C>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,

    /// 本端是否为**发起方**（[`open_channel_async`] 产出）。
    ///
    /// 两个角色的「最终裁决」不同：
    ///
    /// - **发起方**：此刻还没在线上发过任何帧；`accept_async` 才发 `OPEN`，并等对端
    ///   的 `OPEN` + `ACCEPT`（对端拒绝则报 [`HandleError::Refused`]）。
    /// - **响应方**：对端 `OPEN` 已到并已登记；`accept_async` 回自己的 `OPEN` +
    ///   `ACCEPT`，不等任何东西。
    ///
    /// [`open_channel_async`]: abs_smux::conn::TrDockBinding::open_channel_async
    is_initiator_: bool,

    /// 发起方暂存的开场消息（响应方恒为空）：`OPEN` 要等 `accept_async` 才发。
    message_: Vec<u8>,

    /// `TrAcceptBuff::accept_buff` 成功后的**子流共享状态**。
    ///
    /// 发起方还要拿它等待对端的 `OPEN` + `ACCEPT`（见 `accept_async` 的 step 函数），
    /// 因此由句柄暂存；响应方不用，保持 `None`。
    accepted_owner_: Option<ChannelOwner_<C::Alloc>>,

    /// `accept_buff` 之后本端要通告的**接收窗口**（由接收环实际容量算出）。
    accepted_initial_window_: Credit,

    /// 是否已经裁决完毕（接受 / 拒绝 / 已拆）。
    ///
    /// `Drop` 据此决定要不要回收登记：未裁决就丢弃的句柄必须释放身份与配额，否则
    /// dock 对会被永久占住。
    settled_: bool,
}

impl<W, R, S, C> ChannelHandle<W, R, S, C>
where
    C: TrMuxConfig,
{
    /// 由连接与 dock 对构造**响应方**句柄（只允许 `income_async` 调用）。
    pub(crate) fn new_(
        conn: MuxConnection<W, R, S, C>,
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
            accepted_initial_window_: 0u64 as Credit,
            settled_: false,
        }
    }

    /// 由连接、dock 对与开场消息构造**发起方**句柄（只允许 `open_channel_async` 调用）。
    pub(crate) fn new_initiator_(
        conn: MuxConnection<W, R, S, C>,
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
            accepted_initial_window_: 0u64 as Credit,
            settled_: false,
        }
    }
}

/// 未裁决就丢弃句柄 = **放弃建立**。
///
/// 两个角色都要回收登记（dock 对身份 + 配额），否则同一个 dock 对再也用不了：
///
/// - **响应方**：对端已经在等我们的裁决，静默回收会让它的 `accept_async` 永远悬着，
///   因此补一条空理由的 `REJECT`（与 [`TrChannelHandle::reject_async`] 同义）；
/// - **发起方**：此刻还**没有在线上发出过任何帧**（本端 `OPEN` 要等
///   [`TrChannelHandle::accept_async`] 拿到缓冲后才发），对端
///   根本不知道这条子流存在，直接回收登记即可。
impl<W, R, S, C> Drop for ChannelHandle<W, R, S, C>
where
    C: TrMuxConfig,
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
///
/// - **发起方**（`is_initiator`）：本端 `OPEN` 还没发出去（它要等 `accept_async`
///   拿到缓冲），对端**不知道**这条子流存在 ⇒ 直接
///   [撤销](crate::connection::mux_connection::ChannelRegistry_::unreserve_channel_)
///   登记：身份与配额立刻归还，同一 dock 对马上可复用；
/// - **响应方**：对端 `OPEN` 已到、正在等本端裁决 ⇒ 必须先补一条空理由的 `REJECT`
///   （否则对端的 `accept_async` 会**永远悬着**），再把 dock 对按拆流放进宽限期
///   （对端可能还有在途帧）。
fn abort_pending_<W, R, S, C>(
    conn: &MuxConnection<W, R, S, C>,
    local: Dock,
    remote: Dock,
    is_initiator: bool,
) where
    C: TrMuxConfig,
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

impl<W, R, S, C> ChannelHandle<W, R, S, C>
where
    C: TrMuxConfig,
    R: TrBuffRead<u8>,
    W: TrBuffWrite<u8>,
{
    /// 「接受」这一步：校验调用方给的两块内存、建两条环、把会话侧半部交给两个循环。
    ///
    /// 类型是**本连接声明的那一种**（`C::Buff`）——`TrAcceptBuff<C::Buff, _>` 这个标记
    /// 就是这个声明的落点。
    fn accept_buff_(
        &mut self,
        tx_buff: C::Buff,
        rx_buff: C::Buff,
    ) -> AcceptOutcome_<W, R, S, C> {
        let conn = self.conn_.clone();
        let local = self.local_dock_;
        let remote = self.remote_dock_;

        // 1) 校验：容量至少要够建出环（不替调用方改尺寸，不合格就拒绝）。
        let mut tx_buff = tx_buff;
        let mut rx_buff = rx_buff;
        let tx_cap = BorrowMut::<[MaybeUninit<u8>]>::borrow_mut(&mut tx_buff).len();
        let rx_cap = BorrowMut::<[MaybeUninit<u8>]>::borrow_mut(&mut rx_buff).len();
        if tx_cap < K_MIN_RING_CAPACITY_ || rx_cap < K_MIN_RING_CAPACITY_ {
            return Result::Err(HandleError::RingRejected);
        }

        // 2) 对端的窗口通告：发起方还没有（对端 `OPEN` 未到，读循环稍后写进发送窗口），
        //    响应方在登记时已存下。
        let peer_report = if self.is_initiator_ {
            Option::None
        } else {
            match conn.core_().reg_().take_inbound_report_(local, remote) {
                Option::Some(report) => Option::Some(report),
                Option::None => return Result::Err(HandleError::Mux(MuxError::Closed)),
            }
        };

        // 3) 建环 + 登记 + 移交会话侧半部。
        let (owner, tx, rx, initial) = install_channel_(
            &conn,
            local,
            remote,
            ChannelBuff::new(tx_buff, rx_buff),
            peer_report,
        )?;
        self.accepted_initial_window_ = initial;
        self.accepted_owner_ = Option::Some(owner);
        Result::Ok((tx, rx))
    }
}

impl<W, R, S, C> TrChannelHandle for ChannelHandle<W, R, S, C>
where
    C: TrMuxConfig,
    R: TrBuffRead<u8> + 'static,
    W: TrBuffWrite<u8> + 'static,
{
    type Err = HandleError<R, W>;

    type Tx = ChannelTx<W, R, S, C>;
    type Rx = ChannelRx<W, R, S, C>;

    type AcceptAsync<'f, Wb, P> = MuxAcceptAsync<'f, 'f, W, R, S, C, Wb>
    where
        Self: 'f,
        Wb: 'f + TrBuffWrite,
        P: TrPrepareChannelRing<C::Buff, u8>;

    type RejectAsync<'f, Rb> = MuxRejectAsync<'f, 'f, W, R, S, C, Rb>
    where
        Self: 'f,
        Rb: 'f + TrBuffRead;

    fn accept_async<'f, Wb, P>(
        &'f mut self,
        welcome: &'f mut Wb,
        prepare: P,
    ) -> Self::AcceptAsync<'f, Wb, P>
    where
        Wb: TrBuffWrite<u8>,
        P: TrPrepareChannelRing<C::Buff, u8>,
    {
        // 调用方给的智能指针类型是否正确，由 trait 的
        // `Self: TrAcceptBuff<P::Buff>` 约束保证；**接受**（校验 + 建环 + 移交给循环）
        // 在这里同步完成，future 只负责剩下的握手。
        let accept_result: AcceptOutcomeProj_<W, R, S, C> = {
            let (tx_buff, rx_buff) = prepare.prepare().into_inner();
            self.accept_buff_(tx_buff, rx_buff)
        };
        if accept_result.is_err() {
            // 拒绝接受这两份内存：按角色收尾（响应方补 `REJECT`，发起方撤销登记）。
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
        Rb: TrBuffRead,
    {
        MuxRejectAsync::new(self, reason)
    }
}

impl<W, R, S, C> TrChannelHalf for ChannelHandle<W, R, S, C>
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

/// [`TrChannelHandle::accept_async`] 的 step 函数：**建流最终裁决**。
///
/// 「最终裁决」在这里意味着三件事同时发生（`abs_smux` 更新后的语义）：
///
/// 1. 调用方给出本条子流要用的两块缓冲（`prepare`），**本端接收窗口**由其中的接收
///    缓冲容量算出——因此本端的 `OPEN` 也只能在这一刻发出；
/// 2. 两条环建好，会话侧半部移交给两个循环（`Attach`），应用侧半部在这里返回；
/// 3. 按角色把握手推完：发起方发 `OPEN` 后等对端的 `OPEN` + `ACCEPT`；响应方发自己
///    的 `OPEN` + `ACCEPT` 后立刻完成。
///
/// # 缓冲归属
///
/// `prepare` 给的两块存储**就是**本条子流的环存储：类型由本连接声明的
/// [`TrChannelHandle::Buff`](abs_smux::chan::TrChannelHandle::Buff)（即 `C::Buff`）
/// 决定，**分配方式与容量由调用方在这一次调用里决定**。因此这里直接把它们交给
/// `buffex` 建环，没有任何装箱或间接层。
#[gen_may_cancel_future(MuxAccept, pub, new(pub(crate)))]
async fn mux_accept_async_<'f, W, R, S, C, Wb, K>(
    handle: &'f mut ChannelHandle<W, R, S, C>,
    welcome: &'f mut Wb,
    accepted: AcceptOutcomeProj_<W, R, S, C>,
    cancel: K,
) -> Result<(
    <ChannelHandle<W, R, S, C> as TrChannelHandle>::Tx,
    <ChannelHandle<W, R, S, C> as TrChannelHandle>::Rx,
), <ChannelHandle<W, R, S, C> as TrChannelHandle>::Err>
where
    W: 'f + TrBuffWrite<u8> + 'static,
    R: 'f + TrBuffRead<u8> + 'static,
    S: 'f,
    C: 'f + TrMuxConfig,
    Wb: 'f + TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    let conn = handle.conn_.clone();
    let local = handle.local_dock_;
    let remote = handle.remote_dock_;

    // 「准备 + 接受」已经在 `accept_async` 的方法体里同步做完；这里只处理结果，然后
    // 完成剩下的握手（发起方发 `OPEN` 并等对端，响应方回自己的 `OPEN` + `ACCEPT`）。
    let (tx, rx) = accepted?;

    if handle.is_initiator_ {
        // 发起方：此刻才发 `OPEN`（通告的接收窗口取决于接收环容量）。
        let message = core::mem::take(&mut handle.message_);
        let initial = handle.accepted_initial_window_;
        send_open_(&conn, local, remote, initial, message);
        // 对端的 `OPEN` 到达时，读循环会把它的接收窗口写进发送窗口；`ACCEPT` /
        // `REJECT` 到达时唤醒这里。
        let Some(owner) = handle.accepted_owner_.clone() else {
            // `accept_buff` 成功就一定会把 owner 存进句柄；这里是不可达分支。
            handle.settled_ = true;
            return Result::Err(HandleError::Mux(MuxError::Closed));
        };
        match wait_establish_(conn.core_().reg_(), &owner, cancel.child_token()).await? {
            EstablishOutcome_::Accepted => {
                handle.settled_ = true;
                Result::Ok((tx, rx))
            }
            EstablishOutcome_::Refused => {
                // 对方拒绝：本端 `OPEN` 已经发出（对端知道这条子流），因此按拆流
                // 释放——宽限期用来吸收可能的在途帧。
                conn.core_().reg_().release_channel_(local, remote);
                handle.settled_ = true;
                Result::Err(HandleError::Refused)
            }
        }
    } else {
        // 响应方：对端 `OPEN` 的窗口通告已在登记时存下（读循环存的）。
        let initial = handle.accepted_initial_window_;
        // 先回自己的 `OPEN`（通告本端接收窗口，发起方的发送额度由此而来），再发
        // `ACCEPT`（裁决）。
        send_open_(&conn, local, remote, initial, Vec::new());
        // `welcome` 的契约是 `TrBuffWrite`，即「由库写入应用缓冲」的方向，与「把欢迎
        // 信息发给对端」相反，语义存疑；本轮按空载荷发出，并记为遗留（§2.11）。
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

/// 环能构造的**最小**容量（`buffex::Ring` 要求容量落在 `[2, MAX]`，这里取同一口径）。
const K_MIN_RING_CAPACITY_: usize = 2usize;

/// **本连接只接受一种环存储智能指针**：使用环境在 [`TrMuxConfig::Buff`] 里声明的那一种。
///
/// `TrAcceptBuff` 只是**声明**：它不做事，只说明「这一种 `B` 我能用起来」。因此
/// 「一条连接只能用一种环存储」这条约束落在 smux 的实现里，而不是写进 `abs_smux` 的
/// 类型定义里——能同时应付多种智能指针的实现可以声明一个自己的载体类型在内部消化。
impl<W, R, S, C> TrAcceptBuff<u8> for ChannelHandle<W, R, S, C>
where
    C: TrMuxConfig,
{
    type Buff = C::Buff;
}

/// 「接受」这一步的产物：该 channel 的收发半边，或一个连接错误。
type AcceptOutcome_<W, R, S, C> = Result<
    (ChannelTx<W, R, S, C>, ChannelRx<W, R, S, C>),
    HandleError<R, W>,
>;

/// 同上，但按 `TrChannelHandle` 的**关联类型投影**书写（用于与 trait 声明的类型对齐）。
type AcceptOutcomeProj_<W, R, S, C> = Result<
    (
        <ChannelHandle<W, R, S, C> as TrChannelHandle>::Tx,
        <ChannelHandle<W, R, S, C> as TrChannelHandle>::Rx,
    ),
    <ChannelHandle<W, R, S, C> as TrChannelHandle>::Err,
>;

/// 建流最终裁决的产物：`(子流共享状态, 应用侧发送半部, 应用侧接收半部, 本端通告的接收窗口)`。
type InstallOutcome_<W, R, S, C> = (
    ChannelOwner_<<C as TrMuxConfig>::Alloc>,
    ChannelTx<W, R, S, C>,
    ChannelRx<W, R, S, C>,
    Credit,
);

/// 建流最终裁决的公共部分：**按调用方给的容量**建两条环、登记共享状态、把会话侧
/// 半部交给两个循环。
///
/// 返回 `(owner, 应用侧发送半部, 应用侧接收半部, 本端通告的接收窗口)`；会话侧的
/// 两个半部已经随 `Attach` 事件进入循环。
fn install_channel_<W, R, S, C>(
    conn: &MuxConnection<W, R, S, C>,
    local: Dock,
    remote: Dock,
    buffs: ChannelBuff<C::Buff, u8>,
    peer_report: Option<WindowReport>,
) -> Result<InstallOutcome_<W, R, S, C>, HandleError<R, W>>
where
    C: TrMuxConfig,
    R: TrBuffRead<u8>,
    W: TrBuffWrite<u8>,
{
    let core = conn.core_();
    let alloc = core.config_().allocator();
    let policy = core.config_().policy();
    // 两块存储就是调用方给的那两块（0 号位 Tx、1 号位 Rx）。
    let (tx_buff, mut rx_buff) = buffs.into_inner();
    let rx_cap = BorrowMut::<[MaybeUninit<u8>]>::borrow_mut(&mut rx_buff).len();

    // 本端接收窗口由**接收环容量**决定；发送窗口先按同一初值起算，随后被对端 `OPEN`
    // 的通告覆盖（响应方在此就地覆盖，发起方由读循环覆盖）。
    let initial = policy.initial_window(rx_cap);
    let thresholds = ReportThresholds_::new_(policy, initial);
    let mut flow = FlowCtrl::new(policy, rx_cap);
    // 记下即将在 `OPEN` 里通告的快照：越权判定以「最近一次通告」为准。
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
            return Result::Err(HandleError::Mux(MuxError::Closed));
        }
    };
    let (rx_w, rx_r) = match new_buffered_channel_(rx_buff, alloc.clone()) {
        Result::Ok(pair) => pair,
        Result::Err(_) => {
            core.reg_().release_channel_(local, remote);
            return Result::Err(HandleError::Mux(MuxError::Closed));
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


/// 把 `OPEN` 投给写循环（`option` 里是**本端接收窗口**通告）。
fn send_open_<W, R, S, C>(
    conn: &MuxConnection<W, R, S, C>,
    local: Dock,
    remote: Dock,
    window: Credit,
    payload: Vec<u8>,
) where
    C: TrMuxConfig,
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
async fn mux_reject_async_<'f, W, R, S, C, Rb, K>(
    handle: &'f mut ChannelHandle<W, R, S, C>,
    reason: &'f mut Rb,
    cancel: K,
) -> Result<usize, HandleError<R, W>>
where
    C: TrMuxConfig + 'f,
    S: 'f,
    R: TrBuffTryRead<u8> + 'f + 'static,
    W: TrBuffTryWrite<u8> + 'f + 'static,
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
    // 发起方句柄此刻还没在线上发过任何帧（`OPEN` 要等 `accept_async`），因此没有
    // 「拒绝」需要告诉对端——撤销登记就够了，对端根本不知道这条子流存在。
    if handle.is_initiator_ {
        conn.core_().reg_().unreserve_channel_(local, remote);
    } else {
        // 响应方：把理由载荷作为 `REJECT` 的理由发给对端，再按拆流释放。
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
