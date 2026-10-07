use core::{
    alloc::AllocatorClone,
    mem::MaybeUninit,
};

use abs_buff::{
    TrBuffRead, TrBuffWrite,
    gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use abs_mm::res_man::TrUnique;
use mm_ptr::x_deps::abs_mm;
use abs_smux::chan::{
    RingBuffAlloc, TrChannelHalf, TrChannelHandle, TrPrepareRing,
};
use buffex::x_deps::abs_buff;

use crate::{
    connection::{
        Dock, FrameKind, MuxConnection, MuxError, ReserveErr_, TrConnCfg,
        channel_half::{ChannelRx, ChannelTx},
        owner_::{ChannelOwner_, EstablishOutcome_, wait_establish_},
        ring_::{RingBuildErr, new_buffered_channel},
        signal_::{ControlFrame_, ReadEvent_, SessionEvent_, TrEventSender_, WriteEvent_},
            util_::read_available_into_vec_,
    },
    flow_ctrl::{
        Credit, FlowCtrlError, RecvTotal, TrFlowCtrlPolicy,
    },
    metrics::TrMetricsSink,
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
    ///
    /// 在建流裁决（`accept` / `reject`）上，取消**不是「什么都不做」**：只要本端已经
    /// 参与过这条子流的建流（发出过 `OPEN`，或收到过对端的 `OPEN`），连接就已经向对端
    /// 宣告了 `REJECT` 并释放身份——否则对端会永久悬在等裁决上。
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
pub struct ChannelHandle<C>
where
    C: TrConnCfg,
{
    /// 连接智能指针：`accept_async` 用它建子流环并登记会话侧半部。
    conn_: MuxConnection<C>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,

    /// 本端是否为**发起方**（[`open_channel_async`] 产出）。
    is_initiator_: bool,

    /// 发起方暂存的开场消息（响应方恒为空）：`OPEN` 要等 `accept_async` 才发。
    message_: Vec<u8>,

    /// 这条子流的**共享原子状态句柄**。
    ///
    /// 它在**登记身份**（`reserve_channel_` / `reserve_inbound_`）时建立，句柄随
    /// `ChannelHandle` 一路持有到 `accept_async`，因此「状态与身份同寿命」在这一侧
    /// 就是「句柄与句柄同寿命」。窗口参数直到 `accept_async` 拿到环容量才安装。
    accepted_owner_: ChannelOwner_<C::Alloc>,

    /// 接受后本端要通告的**接收窗口**（由接收环实际容量算出）。
    accepted_initial_window_: Credit,

    /// 是否已经裁决完毕（接受 / 拒绝 / 已拆）。
    settled_: bool,
}

impl<C> ChannelHandle<C>
where
    C: TrConnCfg,
{
    /// 由连接、dock 对与**登记身份时建立的共享状态句柄**构造**响应方**句柄
    /// （只允许 `income_async` 调用）。
    pub(crate) fn new_(
        conn: MuxConnection<C>,
        local_dock: Dock,
        remote_dock: Dock,
        owner: ChannelOwner_<C::Alloc>,
    ) -> Self {
        ChannelHandle {
            conn_: conn,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
            is_initiator_: false,
            message_: Vec::new(),
            accepted_owner_: owner,
            accepted_initial_window_: 0u32 as Credit,
            settled_: false,
        }
    }

    /// 由连接、dock 对、共享状态句柄与开场消息构造**发起方**句柄。
    pub(crate) fn new_initiator_(
        conn: MuxConnection<C>,
        local_dock: Dock,
        remote_dock: Dock,
        owner: ChannelOwner_<C::Alloc>,
        message: Vec<u8>,
    ) -> Self {
        ChannelHandle {
            conn_: conn,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
            is_initiator_: true,
            message_: message,
            accepted_owner_: owner,
            accepted_initial_window_: 0u32 as Credit,
            settled_: false,
        }
    }
}

/// 未裁决就丢弃句柄 = **放弃建立**。
impl<C> Drop for ChannelHandle<C>
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
fn abort_pending_<C>(
    conn: &MuxConnection<C>,
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

impl<C> ChannelHandle<C>
where
    C: TrConnCfg,
{
    /// 「接受」这一步：把调用方交出的两块缓冲建两条环，并把会话侧半部交给两个循环。
    ///
    /// `B` 是**调用方当场给出的** ring 存储智能指针类型；它不来自配置，因此同一条
    /// 连接上不同子流可以用不同的 `B`（见 `ring_` 模块）。
    fn accept_prepare_<B, P>(&mut self, prepare: P) -> AcceptOutcomeProj_<C>
    where
        B: 'static + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone> + Send + Sync,
        P: TrPrepareRing<B, C::Data>,
    {
        self.accept_buffs_::<B>(prepare.prepare())
    }

    /// 已经拿到两块 `B` 之后的公共安装逻辑。
    ///
    /// **不取任何锁**：建环与注册表登记都在这里完成不了——注册表登记挪到 step 函数
    /// （那里才有可取消的异步上下文）。因此本函数只做纯本地构造。
    fn accept_buffs_<B>(&mut self, buffs: RingBuffAlloc<B, C::Data>) -> AcceptOutcomeProj_<C>
    where
        B: 'static + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone> + Send + Sync,
    {
        let conn = self.conn_.clone();
        let local = self.local_dock_;
        let remote = self.remote_dock_;

        let RingBuffAlloc { tx_buff, rx_buff, .. } = buffs;

        // 建环 + 安装窗口参数（纯本地；注册表登记在身份登记时已经完成）。
        let (tx, rx, initial) =
            install_channel_::<C, B>(&conn, local, remote, self.accepted_owner_.clone(), tx_buff, rx_buff)?;
        self.accepted_initial_window_ = initial;
        Result::Ok((tx, rx))
    }

    /// 本条子流**为什么停下来**（若它不是被对端正常关闭的）。
    ///
    /// # 什么时候会有值
    ///
    /// 两档，与 [`ChannelTx::abort_reason`](super::ChannelTx::abort_reason) 同源
    /// （读的是同一份共享状态）：[`MuxError::ConnFailed`]（**连接级失败牵连**，压过
    /// 子流级原因）与 [`MuxError::IdleTimeout`]（`max_channel_timeout` 到点）。
    /// 后者覆盖两个阶段：
    ///
    /// - **建流尚未裁决**（本句柄还没走过 `accept_async` / `reject_async`）：计时循环
    ///   已经向对端回过 `REJECT` 并释放了身份，同时把原因留在这里；之后调用方无论是
    ///   走 `accept_async` 还是
    ///   `reject_async`，都会**直接拿到这个超时错误**，而不是一个语义含糊的
    ///   `Closed`（或一次注定失败的成功）。
    /// - **已裁决之后**：与 [`ChannelTx::abort_reason`](super::ChannelTx::abort_reason)
    ///   同源——两者读的是同一份共享状态，因此两条路得到的结论一致。
    pub fn abort_reason(&self) -> Option<MuxError> {
        self.accepted_owner_.abort_reason_()
    }
}

impl<C> TrChannelHandle<C> for ChannelHandle<C>
where
    C: TrConnCfg,
{
    type Err = HandleError;

    type Tx = ChannelTx<C>;
    type Rx = ChannelRx<C>;

    type AcceptAsync<'f, Wb, B, P> = MuxAcceptAsync<'f, 'f, C, Wb>
    where
        Self: 'f,
        Wb: 'f + TrBuffWrite<C::Data>,
        B: 'static + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone> + Send + Sync,
        P: TrPrepareRing<B, C::Data>;

    type RejectAsync<'f, Rb> = MuxRejectAsync<'f, 'f, C, Rb>
    where
        Self: 'f,
        Rb: 'f + TrBuffRead<C::Data>;

    fn accept_async<'f, Wb, B, P>(
        &'f mut self,
        welcome: &'f mut Wb,
        prepare: P,
    ) -> Self::AcceptAsync<'f, Wb, B, P>
    where
        Wb: 'f + TrBuffWrite<C::Data>,
        B: 'static + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone> + Send + Sync,
        P: TrPrepareRing<B, C::Data>,
    {
        let accept_result = self.accept_prepare_::<B, P>(prepare);
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

impl<C> TrChannelHalf<C> for ChannelHandle<C>
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
async fn mux_accept_async_<'f, C, Wb, K>(
    handle: &'f mut ChannelHandle<C>,
    welcome: &'f mut Wb,
    accepted: AcceptOutcomeProj_<C>,
    cancel: K,
) -> AcceptOutcomeProj_<C>
where
    C: TrConnCfg + 'f,

    Wb: TrBuffWrite<u8> + 'f,
    K: TrCancellationToken,
{
    let conn = handle.conn_.clone();
    let local = handle.local_dock_;
    let remote = handle.remote_dock_;

    // 环已经建好（同步、不取锁）；下面**取注册表锁的两步都在可取消的异步上下文里**。
    let (tx, rx) = accepted?;
    // 共享状态在登记身份时就建立了，句柄一直在本对象手里；不再有「挂 owner」这一步。
    let owner = handle.accepted_owner_.clone();

    // 0. 建流阶段已经超时：计时循环早已向对端回过 `REJECT` 并释放了身份，这里只需把
    //    超时结果告诉调用方——不再建流、也不再发 `OPEN` / `ACCEPT`。
    // 建流阶段已经有结论：**用通知槽里的实际原因回答**，不要一律报 `IdleTimeout`
    // ——连接级失败牵连在这里必须如实表达（子流级超时只是其中一档）。
    if let Option::Some(reason) = owner.abort_reason_() {
        handle.settled_ = true;
        return Result::Err(HandleError::Mux(reason));
    }
    if owner.is_aborted_() {
        handle.settled_ = true;
        return Result::Err(HandleError::Mux(MuxError::IdleTimeout));
    }

    // 0.1 取消已经到达：本端此刻还没发出任何建流帧，按角色收尾——发起方撤销本地预留
    //    （对端不知道这条子流），响应方回 `REJECT`（对端的 `OPEN` 在登记时已经到达，
    //    它正等裁决）。
    if cancel.is_cancelled() {
        abort_pending_(&conn, local, remote, handle.is_initiator_);
        handle.settled_ = true;
        return Result::Err(HandleError::Cancelled);
    }

    // 1. 对端窗口通告：响应方在登记入向请求时已由读循环存下。
    if !handle.is_initiator_ {
        let report = match conn
            .core_()
            .take_inbound_report_(local, remote, cancel.child_token())
            .await
        {
            Result::Ok(Option::Some(report)) => report,
            Result::Err(ReserveErr_::Cancelled) => {
                // 对端已经在等裁决（它的 `OPEN` 正是登记入向请求的前提）：本端放弃时
                // 必须回 `REJECT`，否则它会一直悬在 `wait_establish_` 上。
                abort_pending_(&conn, local, remote, handle.is_initiator_);
                handle.settled_ = true;
                return Result::Err(HandleError::Cancelled);
            }
            _ => return Result::Err(HandleError::Mux(MuxError::Closed)),
        };
        if let Result::Err(err) = owner.send_on_report_(report) {
            let _ = conn
                .core_()
                .release_channel_(local, remote, cancel.child_token())
                .await;
            handle.settled_ = true;
            return Result::Err(HandleError::FlowCtrl(err));
        }
    }

    if handle.is_initiator_ {
        // 发起方：此刻才发 `OPEN`（通告的接收窗口取决于接收环容量）。
        let message = core::mem::take(&mut handle.message_);
        let initial = handle.accepted_initial_window_;
        send_open_(&conn, local, remote, initial, message);
        // 对端的 `OPEN` 到达时，读循环会把它的接收窗口写进发送窗口；`ACCEPT` /
        // `REJECT` 到达时唤醒这里。
        let outcome = match wait_establish_(conn.core_().reg_(), &owner, cancel.child_token()).await
        {
            Result::Ok(outcome) => outcome,
            // 取消：本端已经发出 `OPEN`，对端可能已经回过 `OPEN` 甚至 `ACCEPT`，
            // 因此必须主动宣告拒绝，否则两端对这条子流的认知会不一致。
            Result::Err(MuxError::Cancelled) => {
                reject_after_open_(&conn, local, remote);
                handle.settled_ = true;
                return Result::Err(HandleError::Cancelled);
            }
            // 建流阶段超时（`IdleTimeout`）或连接级失败：对端通知与身份释放都已经由
            // 计时循环 / 连接收尾完成，这里只把结果交给调用方。
            Result::Err(err) => {
                handle.settled_ = true;
                return Result::Err(HandleError::Mux(err));
            }
        };
        match outcome {
            EstablishOutcome_::Accepted => {
                handle.settled_ = true;
                owner.set_establish_settled_();
                Result::Ok((tx, rx))
            }
            EstablishOutcome_::Refused => {
                let _ = conn
                    .core_()
                    .release_channel_(local, remote, cancel.child_token())
                    .await;
                handle.settled_ = true;
                owner.set_establish_settled_();
                Result::Err(HandleError::Refused)
            }
        }
    } else {
        // 取消已经到达：绝不能「已经取消却照发 `ACCEPT`」——那会让对端以为建流成功，
        // 而调用方拿到的却是 `Cancelled`。此刻对端仍停在等裁决上，必须回 `REJECT`。
        if cancel.is_cancelled() {
            abort_pending_(&conn, local, remote, false);
            handle.settled_ = true;
            return Result::Err(HandleError::Cancelled);
        }
        // 响应方：对端 `OPEN` 的窗口通告已在登记时存下（读循环存的）。
        let initial = handle.accepted_initial_window_;
        // 先回自己的 `OPEN`，再发 `ACCEPT`。
        send_open_(&conn, local, remote, initial, Vec::new());
        // `welcome` 的契约本轮按空载荷发出，记为遗留。
        let _ = welcome;
        owner.set_establish_settled_();
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
type AcceptOutcomeProj_<C> = Result<(ChannelTx<C>, ChannelRx<C>), HandleError>;


/// 建流最终裁决的产物。
type InstallOutcome_<C> = (
    ChannelTx<C>,
    ChannelRx<C>,
    Credit,
);

/// 建流最终裁决的公共部分：按调用方给的缓冲建两条环、把会话侧半部交给两个循环，
/// 并把**登记身份时建立的共享状态**安装上窗口参数。
fn install_channel_<C, B>(
    conn: &MuxConnection<C>,
    local: Dock,
    remote: Dock,
    owner: ChannelOwner_<<C as TrConnCfg>::Alloc>,
    tx_buff: B,
    mut rx_buff: B,
) -> Result<InstallOutcome_<C>, HandleError>
where
    C: TrConnCfg,
    B: 'static + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone> + Send + Sync,
{
    let core = conn.core_();
    let policy = core.config_().policy();
    let rx_cap = rx_buff.deref_mut().len();

    // 本端接收窗口由**接收环容量**决定；发送窗口先按同一初值起算，随后被对端 `OPEN`
    // 的通告覆盖。窗口参数安装进**随身份一起建立**的共享状态里。
    let initial = policy.initial_window(rx_cap);
    // 发送环的临界水位：与接收侧同源（`容量 × 1/N`）。发送环容量与接收环同批给出，
    // 用同一个 `initial` 即可。
    let backlog = initial / policy.critical_denominator().max(1u32);
    owner.install_(policy, rx_cap);
    // 建流那次通告就是初始窗口：先记为「已通告」，此后 `should_report_` 只在窗口真的
    // 变化时才产出新快照。
    owner.flow_().recv_window().report();

    // 两条环：应用写 / 循环读的是发送环，循环写 / 应用读的是接收环。
    // 环被拒（容量非法）或块分配失败时**投一条释放消息**（不取锁）：身份由核心在
    // 下一轮 drain 里归还。
    let (tx_w, tx_r) = match new_buffered_channel(tx_buff) {
        Result::Ok(pair) => pair,
        Result::Err(err) => {
            let _ = core.post_session_event_(SessionEvent_::ReleaseChannel {
                local_dock: local,
                remote_dock: remote,
            });
            return Result::Err(match err {
                RingBuildErr::Capacity(_) => HandleError::RingRejected,
                RingBuildErr::Alloc => HandleError::AllocationFailed,
            });
        }
    };
    let (rx_w, rx_r) = match new_buffered_channel(rx_buff) {
        Result::Ok(pair) => pair,
        Result::Err(err) => {
            let _ = core.post_session_event_(SessionEvent_::ReleaseChannel {
                local_dock: local,
                remote_dock: remote,
            });
            return Result::Err(match err {
                RingBuildErr::Capacity(_) => HandleError::RingRejected,
                RingBuildErr::Alloc => HandleError::AllocationFailed,
            });
        }
    };

    // **半部交给循环这一步可能失败**（对应的循环已经不在 = 连接正在收尾）。失败时
    // 绝不能把应用侧那半部交出去：它的对端会随失败的事件一起被丢弃，而 `buffex` 的环
    // 半部 **drop 不置关闭位、也不唤醒对端**，应用一用就永久 park。如实报连接级失败
    // （原因优先取共享状态上的通知槽；它没来得及写时退到锁外快照；都没有就按已关闭）。
    let attached = core.w_events_().try_send_event_(WriteEvent_::Attach {
        local_dock: local,
        remote_dock: remote,
        owner: owner.clone(),
        reader_: tx_r,
    }) && core.r_events_().try_send_event_(ReadEvent_::Attach {
        local_dock: local,
        remote_dock: remote,
        owner: owner.clone(),
        writer_: rx_w,
    });
    // 唯一的例外是**单元测试专用的无循环连接**（`MuxConnection::new_test_`：只建核心与
    // 两条事件通道，接收端随即被丢弃）：那里的「投递失败」是构造方式使然，不是连接出了
    // 问题，照旧把半部交出去，别把测试专用的构造变成错误路径。
    if !attached && !cfg!(test) {
        // 原因按**代价从低到高**取，全是同步、锁外读（安装路径是同步的，不能为报原因
        // 去 `await` 注册表锁）：子流通知槽 → 连接失败类别快照 → 已关闭。
        let reason = match owner.abort_reason_() {
            Option::Some(reason) => reason,
            Option::None => match core.reg_().fail_kind_() {
                Option::Some(kind) => MuxError::ConnFailed(kind),
                Option::None => MuxError::Closed,
            },
        };
        return Result::Err(HandleError::Mux(reason));
    }

    // 子流到这里才算**建成**：两条环已建好、两侧半部已交给循环。上报点必须在这里，
    // 而不是登记身份时——登记时既没有环，也还没有任何线上痕迹（见 [`crate::connection`]
    // 模块文档 §4.2「最终裁决是唯一的提交点」）。
    conn.core_()
        .config_()
        .metrics()
        .on_channel_opened(local, remote);

    Result::Ok((
        ChannelTx::new_(tx_w, owner.clone(), conn.clone(), local, remote, backlog),
        ChannelRx::new_(rx_r, owner, conn.clone(), local, remote),
        initial,
    ))
}

/// 把 `OPEN` 投给写循环。
fn send_open_<C>(
    conn: &MuxConnection<C>,
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
async fn mux_reject_async_<'f, C, Rb, K>(
    handle: &'f mut ChannelHandle<C>,
    reason: &'f mut Rb,
    cancel: K,
) -> Result<usize, HandleError>
where
    C: TrConnCfg + 'f,

    Rb: TrBuffRead<u8> + 'f,
    K: TrCancellationToken,
{
    let conn = handle.conn_.clone();
    let local = handle.local_dock_;
    let remote = handle.remote_dock_;

    // 建流阶段已经超时：超时结果优先——此刻「拒绝」已经没有可拒的对象，超时才是
    // 调用方需要知道的结论。
    // 同 `accept`：原因取通知槽里的实际值（连接级失败要如实表达）。
    if let Option::Some(reason) = handle.accepted_owner_.abort_reason_() {
        handle.settled_ = true;
        return Result::Err(HandleError::Mux(reason));
    }
    if handle.accepted_owner_.is_aborted_() {
        handle.settled_ = true;
        return Result::Err(HandleError::Mux(MuxError::IdleTimeout));
    }

    // 取消只能截断「理由载荷」，**不能**让拒绝本身消失：先记下取消，照常把 `REJECT`
    // 发出去，最后再把 `Cancelled` 交给调用方。
    let cancelled = cancel.is_cancelled();
    let payload = read_available_into_vec_(
        reason,
        conn.core_().opts_().basic_opts.max_packet_size,
        cancel.child_token(),
    )
    .await;
    let written = payload.len();
    if handle.is_initiator_ {
        // 发起方此刻还没发过 `OPEN`（`OPEN` 由 `accept_async` 发出）：撤销本地预留即可，
        // 对端并不知道这条子流存在。
        let _ = conn
            .core_()
            .unreserve_channel_(local, remote, cancel.child_token())
            .await;
    } else {
        // 响应方：对端正在等裁决，`REJECT` 必须发出去，否则它会一直悬在
        // `wait_establish_` 上。
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
    handle.accepted_owner_.set_establish_settled_();
    if cancelled {
        return Result::Err(HandleError::Cancelled);
    }
    Result::Ok(written)
}

/// 建流**已经发出 `OPEN` 之后**本端放弃裁决（取消）：向对端宣告 `REJECT` 并释放身份。
///
/// 与 [`abort_pending_`] 只差一处前提：那个函数假设发起方**还没发过** `OPEN`（因此只需
/// 撤销本地预留、不必回帧）；走到这里的发起方已经发过 `OPEN`，对端可能已经回过 `OPEN`
/// 甚至已经 `accept` 完成——不回帧就会让对端永久悬在 `wait_establish_` 上。
fn reject_after_open_<C>(conn: &MuxConnection<C>, local: Dock, remote: Dock)
where
    C: TrConnCfg,
{
    let core = conn.core_();
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
    // 进拆流宽限期：对端可能已经在途的帧会被静默丢弃。
    let _ = core
        .reg_()
        .post_session_event_(SessionEvent_::ReleaseChannel {
            local_dock: local,
            remote_dock: remote,
        });
}

