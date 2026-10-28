use core::{
    future::poll_fn,
    sync::atomic::{AtomicBool, Ordering},
};

use abs_buff::TrBuffRead;
use mm_ptr::Shared;
use abs_cancel::TrCancellationToken;
use abs_smux::{
    conn::TrDockBinding,
    dock::TrDock,
};
use gen_mcf2::gen_may_cancel_future;

use crate::{
    connection::{
        ChannelHandle, Dock, MainLoopErr_, MainLoopReply_, MainLoopRequest_, MuxConnection, MuxError,
        PushErr_, ReserveErr_, TcsAbandonGuard_, Tcs_, TrConnCfg,
        channel_listener::ChannelListener,
        util_::read_available_into_vec_,
    },
    x_deps::{abs_buff, abs_cancel, abs_smux, gen_mcf2},
};

/// [`TrDockBinding`](abs_smux::conn::TrDockBinding) 的错误类型。
///
/// 一个 binding 上可以做三件事——`listen_async` / `open_telegraph_async` /
/// `open_channel_async`——它们共用这一个错误类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BindingError {
    /// 对端 dock 是协议保留值，不能当身份用（只可能来自 `open_channel`）。
    #[error("对端 dock 是协议保留值，不能作为身份")]
    ReservedDock,

    /// 同一 dock 对上已有活跃子流（`open_channel`）。
    #[error("同一 dock 对上已有活跃子流")]
    Duplicate,

    /// 该 dock 对刚关闭，仍在拆流宽限期内（`open_channel`）。
    #[error("该 dock 对刚关闭，仍在拆流宽限期内")]
    WaitClose,

    /// 该 dock 上的活动子流数已达上限（`open_channel`）。
    #[error("该 dock 上的活动子流数已达上限")]
    DockChanLimit,

    /// 连接上的活动子流数已达上限（`open_channel`）。
    #[error("连接上的活动子流数已达上限")]
    ChanLimit,

    /// 该 local_dock 已被占用（listener / telegraph / channel 不得冲突）。
    #[error("该 local_dock 已被占用")]
    DockInUse,

    /// 子流 / 连接已关闭（`open_channel`）。
    #[error("子流 / 连接已关闭")]
    Closed,

    /// 调用方给出的环内存不合用（`open_telegraph`：容量非法或分配失败）。
    ///
    /// 与 channel 裁决时的 [`HandleError::RingRejected`] 同义：环容量由调用方决定，连接
    /// 只校验「能不能建出环」，不替它改尺寸。
    ///
    /// [`HandleError::RingRejected`]: crate::connection::HandleError::RingRejected
    #[error("调用方给出的环内存不合用")]
    RingRejected,

    /// **等锁期间被取消**（cancel token 触发）。
    #[error("本次操作被取消")]
    Cancelled,

    /// 连接级失败：`Display` 用内层文案，`source` 指回内层（`?` 亦直通）。
    #[error("{0}")]
    Mux(#[from] MuxError),
}


/// 把注册表的预留失败映射进 binding 的错误类型。
///
/// `pub(crate)`：telegraph 的开启路径（`telegraph::endpoint_`）也要复用这一套映射，
/// 不能有第二份「预留失败 → BindingError」的理解。
pub(crate) fn map_reserve_err_(err: ReserveErr_) -> BindingError {
    match err {
        ReserveErr_::DockInUse => BindingError::DockInUse,
        ReserveErr_::Duplicate => BindingError::Duplicate,
        ReserveErr_::WaitClose => BindingError::WaitClose,
        ReserveErr_::DockChanLimit => BindingError::DockChanLimit,
        ReserveErr_::ChanLimit => BindingError::ChanLimit,
        // 连接正在优雅关闭：不再接受新子流 / 新端点。
        ReserveErr_::Closing => BindingError::Closed,
        ReserveErr_::Cancelled => BindingError::Cancelled,
    }
}

/// 在某个 `local_dock` 上派生的会话对象。
///
/// **不借用连接**：自己持有一份 [`MuxConnection`] 克隆（= 指向演员核心的智能
/// 指针），因此生命周期参数从公开类型上消失，可以存进结构体、可以从函数返回，
/// 也可以与连接对象同处一个结构体（旧模型下那是自引用结构体，写不出来）。
///
/// 一个 [`DockBinding`] 对应一个业务逻辑：它自带 `local_dock`，可以在其上监听
/// 入向请求（[`TrDockBinding::listen_async`]）、打开数据报端点
/// （[`TrTelegraphBinding::open_telegraph_async`](abs_smux::telegraph::TrTelegraphBinding::open_telegraph_async)）或向对端发起子流
/// （[`TrDockBinding::open_channel_async`]）。不同 binding 之间只在注册表上
/// 交集，因此可以并行持有。
pub struct DockBinding<C>
where
    C: TrConnCfg,
{
    /// 连接智能指针（`bind_async` 取 `&self`，克隆廉价）。
    conn_: MuxConnection<C>,

    /// 本会话绑定的 local_dock。
    local_dock_: Dock,

    /// 「绑定方已丢弃」标志；`Drop` 只置位 + raise，由 main 解绑。
    unbind_: Shared<AtomicBool, crate::connection::ConnChanOwnerAlloc_<C>>,
}

impl<C> DockBinding<C>
where
    C: TrConnCfg,
{
    /// 由连接与 `local_dock` 构造（只允许 `bind_async` 调用）。
    pub(crate) fn new_(
        conn: MuxConnection<C>,
        local_dock: Dock,
        unbind_: Shared<AtomicBool, crate::connection::ConnChanOwnerAlloc_<C>>,
    ) -> Self {
        DockBinding {
            conn_: conn,
            local_dock_: local_dock,
            unbind_,
        }
    }

    /// 连接智能指针的一份克隆（telegraph 的开启路径需要它投递身份与环半部）。
    pub(crate) fn conn_(&self) -> MuxConnection<C> {
        self.conn_.clone()
    }

    /// 本会话绑定的 `local_dock`。
    pub(crate) fn local_dock_(&self) -> Dock {
        self.local_dock_
    }

    pub fn listen_async_default<'f>(&'f mut self) -> MuxListenAsync<'f, 'f, C> {
        const DEFAULT_RESERVE: usize = 8usize;
        MuxListenAsync::new(self, DEFAULT_RESERVE)
    }
}

/// 丢弃绑定即**解绑**：向核心投递一条释放消息，使同一个 dock 之后可以再次
/// `bind_async`（见 `ChannelRegistry_::unbind_dock_` 与 `SessionEvent_::UnbindDock`）。
///
/// **本 `Drop` 不取任何锁、不阻塞**：真正的解绑由核心执行者 drain 后落实。因为
/// `bind_async` 在动身份表之前会先清空释放邮箱，「丢弃后立刻重绑」仍然是确定的。
///
/// 注意解绑**不**影响已经由该 binding 建立、且仍在应用手里的子流半部：那些
/// [`ChannelTx`] / [`ChannelRx`] 不借用 binding，`unbind_dock_` 也不会动它们。
impl<C> Drop for DockBinding<C>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        self.unbind_.store(true, Ordering::Release);
        self.conn_
            .core_()
            .control_()
            .raise_main_hint_(crate::connection::HINT_MAIN_RETIRED);
    }
}

impl<C> TrDockBinding<C> for DockBinding<C>
where
    C: TrConnCfg,
{
    type Err = BindingError;

    type ChannelHandle = ChannelHandle<C>;

    type Listener = ChannelListener<C>;

    type ListenAsync<'f> = MuxListenAsync<'f, 'f, C> where Self: 'f;

    type OpenChannelAsync<'f, M> = MuxOpenChannelAsync<'f, 'f, C, M>
    where
        Self: 'f,
        M: 'f + TrBuffRead<C::Data>;

    fn local_dock(&self) -> &C::Dock {
        &self.local_dock_
    }

    fn listen_async(&mut self, reserve: usize) -> Self::ListenAsync<'_> {
        MuxListenAsync::new(self, reserve)
    }

    fn open_channel_async<'f, M>(
        &'f mut self,
        remote_dock: C::Dock,
        message: &'f mut M,
    ) -> Self::OpenChannelAsync<'f, M>
    where
        M: TrBuffRead<C::Data>,
    {
        MuxOpenChannelAsync::new(self, remote_dock, message)
    }
}

/// [`TrDockBinding::listen_async`] 的 step 函数。
#[gen_may_cancel_future(MuxListen, pub, new(pub(crate)))]
async fn mux_listen_async_<'f, C, K>(
    binding: &'f mut DockBinding<C>,
    reserve: usize,
    cancel: K,
) -> Result<ChannelListener<C>, BindingError>
where
    C: TrConnCfg + 'f,

    K: TrCancellationToken,
{
    // `reserve` 是上游新加的「入向邀请最多同时挂起多少条」的上限。当前实现还没有按它
    // 预分配的队列（入向通知是身份节点里的**内联**槽），因此它只随请求带给
    // `main_loop`，等定容队列落地后再按它预分配。
    let conn = binding.conn_.clone();
    let local = binding.local_dock_;
    // 一次请求 + 完成源：登记由 `main_loop` 执行（它是身份表唯一写者）。
    let tcs = Tcs_::new_(conn.core_().config_().alloc_cfg());
    let waiter = tcs.clone();
    let _abandon = TcsAbandonGuard_::new_(&waiter);
    conn.core_()
        .control_()
        .submit_async_(
            MainLoopRequest_::Listen {
                local_dock: local,
                reserve,
                reply: tcs,
            },
            cancel.child_token(),
        )
        .await
        .map_err(|err| match err {
            PushErr_::Closed(_) => BindingError::Closed,
            PushErr_::Cancelled(_) => BindingError::Cancelled,
        })?;
    let reply = poll_fn(|cx| waiter.poll_wait_(cx)).await;
    match reply {
        Option::Some(Result::Ok(MainLoopReply_::Listener(rec))) => {
            Result::Ok(ChannelListener::new_(conn, local, rec))
        }
        Option::Some(Result::Err(MainLoopErr_::Reserve(err))) => Result::Err(map_reserve_err_(err)),
        Option::Some(Result::Err(MainLoopErr_::Mux(err))) => Result::Err(BindingError::Mux(err)),
        Option::Some(Result::Ok(_)) | Option::None => Result::Err(BindingError::Closed),
    }
}

/// [`TrDockBinding::open_channel_async`] 的 step 函数。
///
/// `message` 是随 `OPEN` 帧附带的开场消息；由于关联类型已泛型于它
/// （`OpenChannelAsync<'f, M>`），future 可以直接持有该缓冲。
///
/// # 本函数**不发任何帧**
///
/// 子流缓冲由调用方在最终裁决建立 channel 时（返回的
/// [`ChannelHandle::accept_async`](abs_smux::chan::TrChannelHandle::accept_async)）
/// 给出，而本端 `OPEN` 要通告的接收窗口正是由那块接收缓冲的容量算出来的。因此在
/// 拿到缓冲之前**不能**发 `OPEN`——否则通告值与真实容量不符（要么违约、要么浪费）。
#[gen_may_cancel_future(MuxOpenChannel, pub, new(pub(crate)))]
async fn mux_open_channel_async_<'f, C, M, K>(
    binding: &'f mut DockBinding<C>,
    remote_dock: Dock,
    message: &'f mut M,
    cancel: K,
) -> Result<ChannelHandle<C>, BindingError>
where
    C: TrConnCfg + 'f,

    M: TrBuffRead<u8> + 'f,
    K: TrCancellationToken,
{
    let conn = binding.conn_.clone();
    let local = binding.local_dock_;
    if remote_dock.is_special() {
        return Result::Err(BindingError::ReservedDock);
    }
    // 1. 登记身份（dock 对即身份，重复即 `Duplicate`）：由 `main_loop` 执行。
    let tcs = Tcs_::new_(conn.core_().config_().alloc_cfg());
    let waiter = tcs.clone();
    let _abandon = TcsAbandonGuard_::new_(&waiter);
    conn.core_()
        .control_()
        .submit_async_(
            MainLoopRequest_::ReserveChannel {
                local_dock: local,
                remote_dock,
                reply: tcs,
            },
            cancel.child_token(),
        )
        .await
        .map_err(|err| match err {
            PushErr_::Closed(_) => BindingError::Closed,
            PushErr_::Cancelled(_) => BindingError::Cancelled,
        })?;
    let reply = poll_fn(|cx| waiter.poll_wait_(cx)).await;
    let (owner, mut msg_) = match reply {
        Option::Some(Result::Ok(MainLoopReply_::Channel { owner, msg_ })) => (owner, msg_),
        Option::Some(Result::Err(MainLoopErr_::Reserve(err))) => {
            return Result::Err(map_reserve_err_(err));
        }
        Option::Some(Result::Err(MainLoopErr_::Mux(err))) => {
            return Result::Err(BindingError::Mux(err));
        }
        Option::Some(Result::Ok(_)) | Option::None => {
            return Result::Err(BindingError::Closed);
        }
    };

    // 2. 把开场消息搬进句柄（`OPEN` 要等 `accept_async` 才发）。
    let payload = read_available_into_vec_(
        message,
        conn.core_().opts_().basic_opts.max_packet_size,
        cancel.child_token(),
    )
    .await;

    // 同时写进建流期 message ring：P3.1b 起由 `main_loop` 从这里读走组 `OPEN`。
    if msg_
        .write_outgoing_(&payload, conn.core_().config_().pending_msg_capacity())
        .is_err()
    {
        return Result::Err(BindingError::Mux(MuxError::FrameTooLarge));
    }
    Result::Ok(ChannelHandle::new_initiator_(
        conn,
        local,
        remote_dock,
        owner,
        msg_,
    ))
}
