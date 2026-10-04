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
        Dock, FrameKind, MuxConnection, MuxError, ReserveErr_, TrConnCfg,
        channel_half::{ChannelRx, ChannelTx},
        owner_::{ChannelOwner_, ChannelState_, EstablishOutcome_, wait_establish_},
        ring_::new_buffered_channel_,
        signal_::{ControlFrame_, ReadEvent_, SessionEvent_, TrEventSender_, WriteEvent_},
            util_::read_available_into_vec_,
    },
    flow_ctrl::{
        Credit, FlowCtrl, FlowCtrlError, RecvTotal, ReportThresholds_, TrFlowCtrlPolicy,
    },
};

/// [`TrChannelHandle`](abs_smux::chan::TrChannelHandle) 的错误类型
/// （`accept_async` / `reject_async` 共用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum HandleError {
    /// **拒绝接受**调用方给出的环内存：大小不合用。
    #[error("调用方给出的环内存大小不合用，已拒绝接受")]
    RingRejected,

    /// 对端拒绝建立这条子流（`accept_async` 的发起方一侧）。
    #[error("对端拒绝建立这条子流")]
    Refused,

    /// 由连接管理内存时，底层分配器分配失败。
    #[error("由连接管理内存时，底层分配器分配失败")]
    AllocationFailed,

    /// 流控失败（窗口违例或计数溢出）。
    #[error("流控失败")]
    FlowCtrl(FlowCtrlError),

    /// 本次操作被取消。
    #[error("本次操作被取消")]
    Cancelled,

    /// 连接级失败：`Display` 用内层文案，`source` 指回内层（`?` 亦直通）。
    #[error("{0}")]
    Mux(#[from] MuxError),
}

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
///
/// **不取注册表锁**（本函数由 `Drop` 调用）：身份表改动一律投成
/// [`SessionEvent_`]，由核心执行者 drain 后落实。协议帧（响应方的 `REJECT`）本来
/// 就是投事件，保持原样。
fn abort_pending_<C, S>(
    conn: &MuxConnection<C, S>,
    local: Dock,
    remote: Dock,
    is_initiator: bool,
) where
    C: TrConnCfg,
{
    let core = conn.core_();
    let reg = core.reg_();
    if is_initiator {
        // 发起方还没发过 `OPEN`：对端不知道这条子流存在，撤销要**立刻**归还身份与
        // 配额（不留宽限期）。
        let _ = reg.post_session_event_(SessionEvent_::UnreserveChannel {
            local_dock: local,
            remote_dock: remote,
        });
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
    // 响应方已经回过 `OPEN`：进入拆流宽限期，让在途帧被静默丢弃。
    let _ = reg.post_session_event_(SessionEvent_::ReleaseChannel {
        local_dock: local,
        remote_dock: remote,
    });
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
        self.accept_buffs_(prepare.prepare())
    }

    /// 已经拿到两块 `C::Buff` 之后的公共安装逻辑。
    ///
    /// **不取任何锁**：建环与注册表登记都在这里完成不了——注册表登记挪到 step 函数
    /// （那里才有可取消的异步上下文）。因此本函数只做纯本地构造。
    fn accept_buffs_(
        &mut self,
        buffs: ChannelBuffAlloc<C::Buff, C::Data>,
    ) -> AcceptOutcomeProj_<C, S> {
        let conn = self.conn_.clone();
        let local = self.local_dock_;
        let remote = self.remote_dock_;

        // 调用方 / managed 路径已经给出本子流的环存储。
        let ChannelBuffAlloc { tx_buff, rx_buff, .. } = buffs;

        // 建环（纯本地；注册表登记在 step 函数里做）。
        let (owner, tx, rx, initial) =
            install_channel_(&conn, local, remote, tx_buff, rx_buff)?;
        self.accepted_initial_window_ = initial;
        self.accepted_owner_ = Option::Some(owner);
        Result::Ok((tx, rx))
    }

    /// **由 `MuxConnection` 管理内存的 `accept` 路径**：不需要调用方实现
    /// `TrPrepareChannelRing`，连接按调用方给出的 `ring_cap` 从自身分配器申请两块缓冲，
    /// 具体类型由 `C::Buff` 决定。欢迎消息也要由调用方给（不需要就传一个空切片）。
    ///
    /// 连「空欢迎消息 + 缺省容量」都不想写时用 [`Self::accept_async_default`]。
    pub async fn accept_async_managed<'f, W>(
        &'f mut self,
        welcome: &'f mut W,
        ring_cap: usize,
    ) -> Result<(ChannelTx<C, S>, ChannelRx<C, S>), HandleError>
    where
        W: 'f + TrBuffWrite<u8>,
        S: 'f,
    {
        let conn = self.conn_.clone();
        let config = conn.core_().config_();
        let alloc = config.allocator();
        let accept_result = match config.make_ring_buffs(alloc, ring_cap) {
            Result::Ok((tx_buff, rx_buff)) => {
                self.accept_buffs_(ChannelBuffAlloc::new(tx_buff, rx_buff))
            }
            Result::Err(_) => Result::Err(HandleError::AllocationFailed),
        };
        if accept_result.is_err() {
            self.settled_ = true;
            abort_pending_(
                &self.conn_,
                self.local_dock_,
                self.remote_dock_,
                self.is_initiator_,
            );
        }
        MuxAcceptAsync::new(self, welcome, accept_result).await
    }

    /// **`accept` 的省事形式**：空欢迎消息 + [`TrConnCfg::RING_CAPACITY`] 缺省容量。
    ///
    /// 等价于 `self.accept_async_managed(&mut [], <C as TrConnCfg>::RING_CAPACITY)`。
    ///
    /// # 为什么不直接叫 `accept_async`
    ///
    /// [`TrChannelHandle::accept_async`]（`abs_smux` 的 trait 方法）已经占了这个名字，
    /// 而且**收两个参数**（欢迎消息 + 环准备策略）。在 `ChannelHandle` 上再加一个同名的
    /// 固有方法会把 trait 方法**遮蔽**掉——它就只能用全限定语法调用，泛型下游代码
    /// （`handle.accept_async(&mut w, prep)`）会直接编不过。因此这里另起一个名字，
    /// 与同族的 [`Self::accept_async_managed`] / [`Self::accept_async_closure`] 保持
    /// 同样的「加后缀」惯例。
    ///
    /// # Errors
    ///
    /// 与 [`Self::accept_async_managed`] 相同。
    pub async fn accept_async_default(
        &mut self,
    ) -> Result<(ChannelTx<C, S>, ChannelRx<C, S>), HandleError> {
        let ring_cap = <C as TrConnCfg>::RING_CAPACITY;
        let mut empty: &mut [u8] = &mut [];
        self.accept_async_managed(&mut empty, ring_cap).await
    }
}

impl<C, S> TrChannelHandle<C> for ChannelHandle<C, S>
where
    C: TrConnCfg,
{
    type Err = HandleError;

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

    // 环已经建好（同步、不取锁）；下面**取注册表锁的两步都在可取消的异步上下文里**。
    let (tx, rx) = accepted?;

    // 1. 对端窗口通告：响应方在登记入向请求时已由读循环存下。
    if !handle.is_initiator_ {
        let report = match conn
            .core_()
            .take_inbound_report_(local, remote, cancel.child_token())
            .await
        {
            Result::Ok(Option::Some(report)) => report,
            Result::Err(ReserveErr_::Cancelled) => {
                return Result::Err(HandleError::Cancelled);
            }
            _ => return Result::Err(HandleError::Mux(MuxError::Closed)),
        };
        let Some(owner) = handle.accepted_owner_.clone() else {
            return Result::Err(HandleError::Mux(MuxError::Closed));
        };
        let applied = owner
            .with_mut_async_(cancel.child_token(), |state| {
                state.flow_mut_().send_window_mut().on_report(report)
            })
            .await;
        if let Result::Ok(Result::Err(err)) = applied {
            let _ = conn
                .core_()
                .release_channel_(local, remote, cancel.child_token())
                .await;
            handle.settled_ = true;
            return Result::Err(HandleError::FlowCtrl(err));
        }
    }

    // 2. 把共享状态句柄挂到身份表上（失败由调用方按内部错误处理，与旧行为一致）。
    if let Some(owner) = handle.accepted_owner_.clone() {
        let _ = conn
            .core_()
            .attach_owner_(local, remote, owner, cancel.child_token())
            .await;
    }

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
                let _ = conn
                    .core_()
                    .release_channel_(local, remote, cancel.child_token())
                    .await;
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
type AcceptOutcomeProj_<C, S> = Result<(ChannelTx<C, S>, ChannelRx<C, S>), HandleError>;


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
) -> Result<InstallOutcome_<C, S>, HandleError>
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

    // 两条环：应用写 / 循环读的是发送环，循环写 / 应用读的是接收环。
    // 环被拒时**投一条释放消息**（不取锁）：身份由核心在下一轮 drain 里归还。
    let (tx_w, tx_r) = match new_buffered_channel_(tx_buff, alloc.clone()) {
        Result::Ok(pair) => pair,
        Result::Err(_) => {
            let _ = core.post_session_event_(SessionEvent_::ReleaseChannel {
                local_dock: local,
                remote_dock: remote,
            });
            return Result::Err(HandleError::RingRejected);
        }
    };
    let (rx_w, rx_r) = match new_buffered_channel_(rx_buff, alloc.clone()) {
        Result::Ok(pair) => pair,
        Result::Err(_) => {
            let _ = core.post_session_event_(SessionEvent_::ReleaseChannel {
                local_dock: local,
                remote_dock: remote,
            });
            return Result::Err(HandleError::RingRejected);
        }
    };

    let owner = ChannelOwner_::new_(ChannelState_::new_(flow, thresholds), alloc.clone());
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
) -> Result<usize, HandleError>
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
        let _ = conn
            .core_()
            .unreserve_channel_(local, remote, cancel.child_token())
            .await;
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
        let _ = conn
            .core_()
            .release_channel_(local, remote, cancel.child_token())
            .await;
    }
    handle.settled_ = true;
    Result::Ok(written)
}

#[cfg(test)]
mod tests_ {
    use std::time::Instant;

    use crate::{
        connection::test_support_::{
            ErasedTestMuxConfig_, NullScope_, TestMuxConfig_,
        },
        flow_ctrl::WindowReport,
        handshake::opts::{BasicOpts, HandshakeOpts},
    };

    use super::*;

    const BENCH_WARMUP: usize = 2_000;
    const BENCH_ITERS: usize = 50_000;

    fn make_conn_<C>(config: C) -> MuxConnection<C, NullScope_>
    where
        C: TrConnCfg,
    {
        MuxConnection::new_test_(
            &NullScope_,
            HandshakeOpts {
                basic_opts: BasicOpts::default(),
            },
            config,
        )
    }

    /// 测一次 responder accept 的建环 + 登记 + 返回半部 + drop 半部。
    async fn accept_once_<C, S>(conn: &MuxConnection<C, S>, remote: Dock) -> u128
    where
        C: TrConnCfg,
        S: Clone + 'static,
    {
        let local = Dock::new(2u32);
        let mut welcome: [u8; 0] = [];
        let mut welcome_slice: &mut [u8] = &mut welcome[..];

        conn.core_()
            .reg_()
            .reserve_inbound_(
                local,
                remote,
                WindowReport::new(0u64, 64u32),
                buffex::x_deps::abs_cancel::NonCancellableToken::new(),
            )
            .await
            .expect("登记入向请求应当成功");
        let mut handle = ChannelHandle::new_(conn.clone(), local, remote);

        let t0 = Instant::now();
        let accepted = handle.accept_async_managed(&mut welcome_slice, 4096).await;
        let elapsed = t0.elapsed().as_nanos();
        drop(accepted.expect("responder accept 应当成功"));
        elapsed
    }

    async fn bench_both_managed_buffers_() -> (f64, f64) {
        let conn_owned = make_conn_(TestMuxConfig_);
        let conn_erased = make_conn_(ErasedTestMuxConfig_);

        // 交替顺序，消除“某个模式总是先跑、堆/缓存更冷”的顺序偏差。
        let mut total_owned = 0u128;
        let mut total_erased = 0u128;
        for i in 0..BENCH_ITERS {
            let base = 0x1000u32 + (i as u32) * 2u32;
            if i % 2 == 0 {
                total_owned += accept_once_(&conn_owned, Dock::new(base)).await;
                total_erased += accept_once_(&conn_erased, Dock::new(base + 1)).await;
            } else {
                total_erased += accept_once_(&conn_erased, Dock::new(base + 1)).await;
                total_owned += accept_once_(&conn_owned, Dock::new(base)).await;
            }
        }
        (
            total_owned as f64 / BENCH_ITERS as f64,
            total_erased as f64 / BENCH_ITERS as f64,
        )
    }

    /// 非正式 benchmark：managed 路径下 `Owned<..., CoreAlloc>`（零 dyn）与
    /// `MuxChanBuff`（擦除分配器）的每 channel accept 建/拆开销。
    ///
    /// 默认 `#[ignore]`，用 release + `--nocapture` 手动运行：
    ///
    /// ```bash
    /// cargo test --release --lib -- \
    ///   --ignored --nocapture managed_owned_vs_erased_accept_bench_
    /// ```
    async fn managed_owned_vs_erased_accept_bench_() {
        // 先跑一次短预热，避免首次分配 / 缺页被算进正式数据。
        let conn = make_conn_(TestMuxConfig_);
        for i in 0..BENCH_WARMUP {
            let _ = accept_once_(&conn, Dock::new(0x8000u32 + i as u32)).await;
        }

        let (owned, erased) = bench_both_managed_buffers_().await;
        eprintln!("owned managed accept:  {owned:8.1} ns/channel");
        eprintln!("erased managed accept: {erased:8.1} ns/channel");
        eprintln!("delta: {:+.1} ns/channel", erased - owned);
    }
    dual_runtime_test_!(managed_owned_vs_erased_accept_bench_);
}
