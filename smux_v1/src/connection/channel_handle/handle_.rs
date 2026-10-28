use core::{
    alloc::AllocatorClone,
    future::poll_fn,
    mem::MaybeUninit,
};

use abs_buff::{TrBuffRead, TrBuffWrite};
use abs_cancel::TrCancellationToken;
use abs_mm::res_man::TrUnique;
use abs_smux::chan::{
    RingBuffAlloc, TrChannelHalf, TrChannelHandle, TrPrepareRing,
};
use gen_mcf2::gen_may_cancel_future;

use crate::{
    connection::{
        Dock, EstablishRole_, FrameKind, MainLoopErr_, MainLoopReply_, MainLoopRequest_, MuxConnection, MuxError,
        TcsAbandonGuard_, Tcs_, TrConnAllocCfg, TrConnCfg,
        channel_half::{ChannelRx, ChannelTx},
        owner_::{ChannelOwner_, EstablishOutcome_, wait_establish_},
        ring_::{RingBuildErr, new_buffered_channel},
        signal_::{ControlFrame_, ReadAttach_, SessionEvent_, WriteAttach_, WriteEvent_},
        slot_::PushErr_,
        pending_msg_::PendingMsgApp_,
        public_channel_::ChunkReadState_,
        session_::enqueue_frame_,
        util_::read_available_into_slice_,
    },
    flow_ctrl::{
        Credit, FlowCtrlError, TrFlowCtrlPolicy,
    },
    metrics::TrMetricsSink,
    x_deps::{abs_buff, abs_cancel, abs_mm, abs_smux, gen_mcf2},
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
/// 建流时因 inbox 满而待补投的两侧 `Attach`。
type PendingAttach_<C> = (
    Option<WriteAttach_<crate::connection::ConnAllocCfg_<C>>>,
    Option<ReadAttach_<crate::connection::ConnAllocCfg_<C>>>,
);

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

    /// app 侧建流期 message ring（写 outgoing / 读 incoming）。
    ///
    /// P3.1b 起 `main_loop` 从这里读 `OPEN` message / welcome，应用从 incoming 读
    /// 对端 message；本轮先随句柄保活。
    #[allow(dead_code, reason = "P3.1b 起由 main / read_message_async 读取")]
    msg_: PendingMsgApp_,

    /// 这条子流的**共享原子状态句柄**。
    ///
    /// 它在**登记身份**（`reserve_channel_` / `reserve_inbound_`）时建立，句柄随
    /// `ChannelHandle` 一路持有到 `accept_async`，因此「状态与身份同寿命」在这一侧
    /// 就是「句柄与句柄同寿命」。窗口参数直到 `accept_async` 拿到环容量才安装。
    accepted_owner_: ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,

    /// 接受后本端要通告的**接收窗口**（由接收环实际容量算出）。
    accepted_initial_window_: Credit,

    /// 是否已经裁决完毕（接受 / 拒绝 / 已拆）。
    settled_: bool,

    /// 建环时因 inbox 满而**未能投递**的 `Attach`；由 `accept_async` 在异步上下文里
    /// 补投（背压）。两边都为 `None` 是正常路径。
    pending_attach_: Option<PendingAttach_<C>>,
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
        owner: ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,
        msg_: PendingMsgApp_,
    ) -> Self {
        ChannelHandle {
            conn_: conn,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
            is_initiator_: false,
            msg_,
            accepted_owner_: owner,
            accepted_initial_window_: 0u32 as Credit,
            settled_: false,
            pending_attach_: Option::None,
        }
    }

    /// 由连接、dock 对、共享状态句柄与开场消息构造**发起方**句柄。
    pub(crate) fn new_initiator_(
        conn: MuxConnection<C>,
        local_dock: Dock,
        remote_dock: Dock,
        owner: ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,
        msg_: PendingMsgApp_,
    ) -> Self {
        ChannelHandle {
            conn_: conn,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
            is_initiator_: true,
            msg_,
            accepted_owner_: owner,
            accepted_initial_window_: 0u32 as Credit,
            settled_: false,
            pending_attach_: Option::None,
        }
    }
}

/// 未裁决就丢弃句柄 = **放弃建立**。
///
/// P4 起只置位 + 唤醒 `main_loop`（不再在 `Drop` 里投 `SessionEvent_`）：发起方 /
/// 响应方的收尾动作由 `main_loop` 的 retired 扫描按角色落实（设计稿 §6.7）。
/// 应用侧建流期 message ring 随本句柄字段一并释放。
impl<C> Drop for ChannelHandle<C>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        if self.settled_ {
            return;
        }
        self.accepted_owner_.set_handle_retired_();
        self.conn_
            .core_()
            .control_()
            .raise_main_hint_(crate::connection::HINT_MAIN_RETIRED);
    }
}

/// 裁决**显式失败**时的收尾：按**角色**把这条待决子流收拾干净。
///
/// 只由 `accept_async` 的失败分支调用（建环失败 / attach 失败 / 提交被拒 / 取消）；
/// 应用直接丢弃句柄走的是 [`ChannelHandle::Drop`] 的 retired 位，不经过这里。
///
/// **不取注册表锁**：身份表改动一律投成 [`SessionEvent_`]，由核心执行者 drain 后
/// 落实。协议帧（响应方的 `REJECT`）本来就是投事件，保持原样。
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
    let _ = core.w_events_().try_send_durable_event_(WriteEvent_::Control {
        frame_: ControlFrame_::with_window_(
            FrameKind::Reject,
            0u8,
            local,
            remote,
            Option::None,
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
        let outcome = install_channel_::<C, B>(
            &conn,
            local,
            remote,
            self.accepted_owner_.clone(),
            tx_buff,
            rx_buff,
        )?;
        self.accepted_initial_window_ = outcome.initial_;
        self.pending_attach_ = match (outcome.pending_w_, outcome.pending_r_) {
            (Option::None, Option::None) => Option::None,
            (w, r) => Option::Some((w, r)),
        };
        Result::Ok((outcome.tx_, outcome.rx_))
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

    type MessageAsync<'f, W> = MuxReadMessageAsync<'f, 'f, C, W>
    where
        Self: 'f,
        W: 'f + TrBuffWrite<C::Data>;

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

    fn read_message_async<'f, W>(&'f mut self, sink: &'f mut W) -> Self::MessageAsync<'f, W>
    where
        W: 'f + TrBuffWrite<C::Data>,
    {
        MuxReadMessageAsync::new(self, sink)
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
    // 建环时若 inbox 满，`Attach` 被带回到 `pending_attach_`；这里在异步上下文里补投。
    if let Option::Some((pending_w, pending_r)) = handle.pending_attach_.take()
        && let Result::Err(err) =
            attach_channel_async_(&conn, pending_w, pending_r, cancel.child_token()).await
    {
        handle.settled_ = true;
        abort_pending_(&conn, local, remote, handle.is_initiator_);
        return Result::Err(err);
    }
    let owner = handle.accepted_owner_.clone();

    if let Option::Some(reason) = owner.abort_reason_() {
        handle.settled_ = true;
        return Result::Err(HandleError::Mux(reason));
    }
    if owner.is_aborted_() {
        handle.settled_ = true;
        return Result::Err(HandleError::Mux(MuxError::IdleTimeout));
    }
    if cancel.is_cancelled() {
        abort_pending_(&conn, local, remote, handle.is_initiator_);
        handle.settled_ = true;
        return Result::Err(HandleError::Cancelled);
    }

    // `welcome` 的契约本轮仍按空载荷发出（`W: TrBuffWrite` 无法读出内容；设计稿
    // §9.3 的 Q12 在 `read_message_async` 一起处理）。
    let _ = welcome;
    let initial = handle.accepted_initial_window_;
    let role_ = if handle.is_initiator_ {
        EstablishRole_::Initiator
    } else {
        EstablishRole_::Responder
    };
    // 把裁决交给 main：它从 message ring 读 `OPEN` message / welcome，组帧并写公共 TX。
    if let Result::Err(err) = submit_accept_(
        &conn,
        local,
        remote,
        owner.clone(),
        role_,
        initial,
        cancel.child_token(),
    )
    .await
    {
        handle.settled_ = true;
        abort_pending_(&conn, local, remote, handle.is_initiator_);
        return Result::Err(err);
    }

    if handle.is_initiator_ {
        let outcome = match wait_establish_(conn.core_().reg_(), &owner, cancel.child_token()).await
        {
            Result::Ok(outcome) => outcome,
            Result::Err(MuxError::Cancelled) => {
                reject_after_open_(&conn, local, remote);
                handle.settled_ = true;
                return Result::Err(HandleError::Cancelled);
            }
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
        owner.set_establish_settled_();
        handle.settled_ = true;
        Result::Ok((tx, rx))
    }
}

/// 「接受」这一步的产物：该 channel 的收发半边，或一个连接错误。
type AcceptOutcomeProj_<C> = Result<(ChannelTx<C>, ChannelRx<C>), HandleError>;


/// 建流最终裁决的产物。
struct InstallOutcome_<C>
where
    C: TrConnCfg,
{
    /// 应用侧发送半边。
    tx_: ChannelTx<C>,

    /// 应用侧接收半边。
    rx_: ChannelRx<C>,

    /// 本端要通告的初始接收窗口。
    initial_: Credit,

    /// 因写侧 inbox 满而待补投的 `Attach`。
    pending_w_: Option<WriteAttach_<crate::connection::ConnAllocCfg_<C>>>,

    /// 因读侧 inbox 满而待补投的 `Attach`。
    pending_r_: Option<ReadAttach_<crate::connection::ConnAllocCfg_<C>>>,
}

/// 建流最终裁决的公共部分：按调用方给的缓冲建两条环、把会话侧半部交给两个循环，
/// 并把**登记身份时建立的共享状态**安装上窗口参数。
fn install_channel_<C, B>(
    conn: &MuxConnection<C>,
    local: Dock,
    remote: Dock,
    owner: ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,
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

    // **半部交给循环**：写 / 读各一条内联 inbox。inbox 满时把 `Attach` 带回去，
    // 由 `accept_async` 的异步阶段补投（背压）；inbox 已关闭（循环退出）才报错。
    let reg = core.reg_();
    let pair = (local, remote);
    let pending_w_ = match reg.try_push_write_attach_(WriteAttach_::new_(
        pair,
        owner.clone(),
        tx_r,
    )) {
        Result::Ok(()) => Option::None,
        Result::Err(attach) => {
            if reg.is_write_inbox_closed_() {
                let (_, _, mut reader_) = attach.into_parts_();
                reader_.close();
                return Result::Err(HandleError::Mux(attach_closed_reason_(&owner, conn)));
            }
            // 单元测试专用的无循环连接：没有消费者，直接放弃投递（旧行为）。
            if cfg!(test) {
                Option::None
            } else {
                Option::Some(attach)
            }
        }
    };
    let pending_r_ = match reg.try_push_read_attach_(ReadAttach_::new_(
        pair,
        owner.clone(),
        rx_w,
    )) {
        Result::Ok(()) => Option::None,
        Result::Err(attach) => {
            if reg.is_read_inbox_closed_() {
                let (_, _, mut writer_) = attach.into_parts_();
                writer_.close();
                return Result::Err(HandleError::Mux(attach_closed_reason_(&owner, conn)));
            }
            if cfg!(test) {
                Option::None
            } else {
                Option::Some(attach)
            }
        }
    };

    // 子流到这里才算**建成**：两条环已建好、两侧半部已交给（或待补投给）循环。上报点
    // 必须在这里，而不是登记身份时——登记时既没有环，也还没有任何线上痕迹（见
    // [`crate::connection`] 模块文档 §4.2「最终裁决是唯一的提交点」）。
    conn.core_()
        .config_()
        .metrics()
        .on_channel_opened(local, remote);

    Result::Ok(InstallOutcome_ {
        tx_: ChannelTx::new_(tx_w, owner.clone(), conn.clone(), local, remote, backlog),
        rx_: ChannelRx::new_(rx_r, owner, conn.clone(), local, remote),
        initial_: initial,
        pending_w_,
        pending_r_,
    })
}

/// `Attach` 投递失败（inbox 已关闭）时的连接级原因。
fn attach_closed_reason_<C>(
    owner: &ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,
    conn: &MuxConnection<C>,
) -> MuxError
where
    C: TrConnCfg,
{
    match owner.abort_reason_() {
        Option::Some(reason) => reason,
        Option::None => match conn.core_().reg_().fail_kind_() {
            Option::Some(kind) => MuxError::ConnFailed(kind),
            Option::None => MuxError::Closed,
        },
    }
}

/// 把建流时因 inbox 满而带回来的 `Attach` 补投出去（背压、可取消）。
async fn attach_channel_async_<C, K>(
    conn: &MuxConnection<C>,
    pending_w: Option<WriteAttach_<crate::connection::ConnAllocCfg_<C>>>,
    pending_r: Option<ReadAttach_<crate::connection::ConnAllocCfg_<C>>>,
    cancel: K,
) -> Result<(), HandleError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let reg = conn.core_().reg_();
    if let Option::Some(attach) = pending_w {
        match reg
            .push_write_attach_async_(attach, cancel.child_token())
            .await
        {
            Result::Ok(()) => {}
            Result::Err(PushErr_::Closed(attach)) => {
                let (_, _, mut reader_) = attach.into_parts_();
                reader_.close();
                return Result::Err(HandleError::Mux(MuxError::Closed));
            }
            Result::Err(PushErr_::Cancelled(attach)) => {
                let (_, _, mut reader_) = attach.into_parts_();
                reader_.close();
                return Result::Err(HandleError::Cancelled);
            }
        }
    }
    if let Option::Some(attach) = pending_r {
        match reg
            .push_read_attach_async_(attach, cancel.child_token())
            .await
        {
            Result::Ok(()) => {}
            Result::Err(PushErr_::Closed(attach)) => {
                let (_, _, mut writer_) = attach.into_parts_();
                writer_.close();
                return Result::Err(HandleError::Mux(MuxError::Closed));
            }
            Result::Err(PushErr_::Cancelled(attach)) => {
                let (_, _, mut writer_) = attach.into_parts_();
                writer_.close();
                return Result::Err(HandleError::Cancelled);
            }
        }
    }
    Result::Ok(())
}

/// 等一个「只表示受理」的请求完成（`MainReply_::Unit`）。
async fn wait_unit_<C>(
    waiter: &Tcs_<Result<MainLoopReply_<C>, MainLoopErr_>>,
) -> Result<(), HandleError>
where
    C: TrConnAllocCfg + 'static,
{
    match poll_fn(|cx| waiter.poll_wait_(cx)).await {
        Option::Some(Result::Ok(MainLoopReply_::Unit)) => Result::Ok(()),
        Option::Some(Result::Err(MainLoopErr_::Mux(err))) => Result::Err(HandleError::Mux(err)),
        Option::Some(Result::Err(MainLoopErr_::Reserve(_))) | Option::None => {
            Result::Err(HandleError::Cancelled)
        }
        Option::Some(Result::Ok(_)) => Result::Err(HandleError::Mux(MuxError::Closed)),
    }
}

/// 提交一次 `Accept` 裁决并等 main 把 `OPEN` / `ACCEPT` 写进公共 TX。
async fn submit_accept_<C, K>(
    conn: &MuxConnection<C>,
    local: Dock,
    remote: Dock,
    owner: ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,
    role_: EstablishRole_,
    initial_window: Credit,
    cancel: K,
) -> Result<(), HandleError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let tcs = match Tcs_::new_(conn.core_().reg_().scope_()).await {
        Result::Ok(tcs) => tcs,
        Result::Err(_) => return Result::Err(HandleError::Mux(MuxError::Closed)),
    };
    let waiter = tcs.clone();
    let _abandon = TcsAbandonGuard_::new_(&waiter);
    conn.core_()
        .control_()
        .submit_async_(
            MainLoopRequest_::Accept {
                local_dock: local,
                remote_dock: remote,
                owner,
                role_,
                initial_window,
                reply: tcs,
            },
            cancel.child_token(),
        )
        .await
        .map_err(|err| match err {
            PushErr_::Closed(_) => HandleError::Mux(MuxError::Closed),
            PushErr_::Cancelled(_) => HandleError::Cancelled,
        })?;
    wait_unit_(&waiter).await
}

/// 提交一次 `Reject` 裁决并等 main 写 `REJECT` / 释放身份。
async fn submit_reject_<C, K>(
    conn: &MuxConnection<C>,
    local: Dock,
    remote: Dock,
    owner: ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,
    role_: EstablishRole_,
    cancel: K,
) -> Result<(), HandleError>
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    let tcs = match Tcs_::new_(conn.core_().reg_().scope_()).await {
        Result::Ok(tcs) => tcs,
        Result::Err(_) => return Result::Err(HandleError::Mux(MuxError::Closed)),
    };
    let waiter = tcs.clone();
    let _abandon = TcsAbandonGuard_::new_(&waiter);
    conn.core_()
        .control_()
        .submit_async_(
            MainLoopRequest_::Reject {
                local_dock: local,
                remote_dock: remote,
                owner,
                role_,
                reply: tcs,
            },
            cancel.child_token(),
        )
        .await
        .map_err(|err| match err {
            PushErr_::Closed(_) => HandleError::Mux(MuxError::Closed),
            PushErr_::Cancelled(_) => HandleError::Cancelled,
        })?;
    wait_unit_(&waiter).await
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

    if let Option::Some(reason) = handle.accepted_owner_.abort_reason_() {
        handle.settled_ = true;
        return Result::Err(HandleError::Mux(reason));
    }
    if handle.accepted_owner_.is_aborted_() {
        handle.settled_ = true;
        return Result::Err(HandleError::Mux(MuxError::IdleTimeout));
    }

    // 取消只能截断「理由载荷」，不能不让拒绝发生：先记下取消，照常裁决。
    let cancelled = cancel.is_cancelled();
    let cap = conn.core_().config_().pending_msg_capacity();
    let payload_retain = crate::connection::alloc_zeroed_bytes_async_(
        conn.core_().reg_().scope_(),
        cap.saturating_add(1usize),
    )
    .await
    .map_err(|_| HandleError::Mux(MuxError::Closed))?;
    let mut payload = payload_retain
        .try_owning()
        .expect("刚分配的 reason 缓冲应当可独占");
    let written = read_available_into_slice_(reason, &mut payload[..], cancel.child_token()).await;
    if written > cap {
        handle.settled_ = true;
        return Result::Err(HandleError::Mux(MuxError::FrameTooLarge));
    }
    if handle
        .msg_
        .write_outgoing_(&payload[..written], cap)
        .is_err()
    {
        handle.settled_ = true;
        return Result::Err(HandleError::Mux(MuxError::FrameTooLarge));
    }
    let role_ = if handle.is_initiator_ {
        EstablishRole_::Initiator
    } else {
        EstablishRole_::Responder
    };
    if let Result::Err(err) = submit_reject_(
        &conn,
        local,
        remote,
        handle.accepted_owner_.clone(),
        role_,
        cancel.child_token(),
    )
    .await
    {
        handle.settled_ = true;
        return Result::Err(err);
    }
    handle.settled_ = true;
    handle.accepted_owner_.set_establish_settled_();
    if cancelled {
        return Result::Err(HandleError::Cancelled);
    }
    Result::Ok(written)
}

/// [`TrChannelHandle::read_message_async`] 的 step 函数。
#[gen_may_cancel_future(MuxReadMessage, pub, new(pub(crate)))]
async fn mux_read_message_async_<'f, C, W, K>(
    handle: &'f mut ChannelHandle<C>,
    sink: &'f mut W,
    cancel: K,
) -> Result<usize, HandleError>
where
    C: TrConnCfg + 'f,

    W: TrBuffWrite<C::Data> + 'f,
    K: TrCancellationToken,
{
    let cap = handle.conn_.core_().config_().pending_msg_capacity();
    let mut state = ChunkReadState_::new_message_();
    let len = loop {
        if cancel.is_cancelled() {
            return Result::Err(HandleError::Cancelled);
        }
        match handle.msg_.read_prefix_incoming_(&mut state, cap) {
            Result::Ok(Option::Some(len)) => break len,
            Result::Ok(Option::None) => {
                let _ = poll_fn(|cx| handle.msg_.poll_incoming_readable_(cx)).await;
            }
            Result::Err(_) => return Result::Err(HandleError::Mux(MuxError::MalformedFrame)),
        }
    };
    if len == 0usize {
        return Result::Ok(0usize);
    }
    // 分块搬运到 sink：不再先分配一个 `Vec<u8>` 装整条 message。message ring 本身
    // 已经是暂存，这里只需要一个栈上小块；每块立刻写进 sink，背压由 `enqueue_frame_`
    // 的 `write_async` 承担。
    const K_READ_MSG_CHUNK: usize = 1024usize;
    let mut buf = [0u8; K_READ_MSG_CHUNK];
    let mut filled = 0usize;
    while filled < len {
        let want = core::cmp::min(buf.len(), len - filled);
        match handle
            .msg_
            .read_body_incoming_(&mut state, &mut buf[..want])
        {
            Result::Ok(n) if n > 0usize => {
                enqueue_frame_(sink, &buf[..n], cancel.child_token()).await?;
                filled += n;
            }
            Result::Ok(_) => {
                let _ = poll_fn(|cx| handle.msg_.poll_incoming_readable_(cx)).await;
            }
            Result::Err(_) => return Result::Err(HandleError::Mux(MuxError::MalformedFrame)),
        }
    }
    Result::Ok(len)
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
    let _ = core.w_events_().try_send_durable_event_(WriteEvent_::Control {
        frame_: ControlFrame_::with_window_(
            FrameKind::Reject,
            0u8,
            local,
            remote,
            Option::None,
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
