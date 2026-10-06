use abs_buff::{
    TrBuffRead, gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use abs_smux::{conn::TrDockBinding, dock::TrDock};
use buffex::x_deps::abs_buff;

use crate::connection::{
    ChannelHandle, Dock, MuxError, MuxConnection, ReserveErr_, TrConnCfg,
    channel_half::{ChannelRx, ChannelTx},
    channel_listener::ChannelListener,
    signal_::SessionEvent_,
    util_::read_available_into_vec_,
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

    /// **等锁期间被取消**（cancel token 触发）。
    #[error("本次操作被取消")]
    Cancelled,

    /// 连接级失败：`Display` 用内层文案，`source` 指回内层（`?` 亦直通）。
    #[error("{0}")]
    Mux(#[from] MuxError),
}


/// 把注册表的预留失败映射进 binding 的错误类型。
fn map_reserve_err_(err: ReserveErr_) -> BindingError {
    match err {
        ReserveErr_::DockInUse => BindingError::DockInUse,
        ReserveErr_::Duplicate => BindingError::Duplicate,
        ReserveErr_::WaitClose => BindingError::WaitClose,
        ReserveErr_::DockChanLimit => BindingError::DockChanLimit,
        ReserveErr_::ChanLimit => BindingError::ChanLimit,
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
/// （[`TrDockBinding::open_telegraph_async`]）或向对端发起子流
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
}

impl<C> DockBinding<C>
where
    C: TrConnCfg,
{
    /// 由连接与 `local_dock` 构造（只允许 `bind_async` 调用）。
    pub(crate) fn new_(conn: MuxConnection<C>, local_dock: Dock) -> Self {
        DockBinding {
            conn_: conn,
            local_dock_: local_dock,
        }
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
        let _ = self
            .conn_
            .core_()
            .reg_()
            .post_session_event_(SessionEvent_::UnbindDock {
                local_dock: self.local_dock_,
            });
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

    type Telegraph = crate::connection::Telegraph<C>;

    type OpenTelegraphAsync<'f> = MuxOpenTelegraphAsync<'f, 'f, C> where Self: 'f;

    type Tx = ChannelTx<C>;
    type Rx = ChannelRx<C>;

    type OpenChannelAsync<'f, M> = MuxOpenChannelAsync<'f, 'f, C, M>
    where
        Self: 'f,
        M: 'f + TrBuffRead<C::Data>;

    fn local_dock(&self) -> &C::Dock {
        &self.local_dock_
    }

    fn listen_async(&mut self) -> Self::ListenAsync<'_> {
        MuxListenAsync::new(self)
    }

    fn open_telegraph_async(&mut self) -> Self::OpenTelegraphAsync<'_> {
        MuxOpenTelegraphAsync::new(self)
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
    cancel: K,
) -> Result<ChannelListener<C>, BindingError>
where
    C: TrConnCfg + 'f,

    K: TrCancellationToken,
{
    let conn = binding.conn_.clone();
    let local = binding.local_dock_;
    // 先清空释放邮箱：上一位持有者可能刚丢弃它的 listener（Drop 只投消息），
    // 不先落实就会把「已释放」误判成「已被占用」。
    conn.core_()
        .drain_session_events_(cancel.child_token())
        .await
        .map_err(map_reserve_err_)?;
    // 登记 listener 身份（统一身份表里的 `(local, wildcard)`）：它既是「本 dock 在
    // 监听」的事实，也是入向等待者的落点；若该 dock 已作 telegraph 会被拒。
    // 入向通知是身份节点里**内联的零分配通知槽**（不再是 `flume` 通道），listener
    // 持节点句柄并在它上面等通知（协议见 `sync_::NotifySlot_`）。
    let rec = conn
        .core_()
        .reserve_listener_(local, cancel.child_token())
        .await
        .map_err(map_reserve_err_)?;
    Result::Ok(ChannelListener::new_(conn, local, rec))
}

/// [`TrDockBinding::open_telegraph_async`] 的 step 函数。
///
/// 本轮只登记端点身份（telegraph **独占**该 `local_dock`）并交付端点对象；
/// 端点的收发方法仍是 `todo!()`（见 [`crate::connection::Telegraph`]）。
#[gen_may_cancel_future(MuxOpenTelegraph, pub, new(pub(crate)))]
async fn mux_open_telegraph_async_<'f, C, K>(
    binding: &'f mut DockBinding<C>,
    cancel: K,
) -> Result<crate::connection::Telegraph<C>, BindingError>
where
    C: TrConnCfg + 'f,

    K: TrCancellationToken,
{
    let conn = binding.conn_.clone();
    let local = binding.local_dock_;
    // 同 `listen_async`：先落实积压的释放消息，再认领身份。
    conn.core_()
        .drain_session_events_(cancel.child_token())
        .await
        .map_err(map_reserve_err_)?;
    let rec = conn
        .core_()
        .reserve_telegraph_(local, cancel.child_token())
        .await
        .map_err(map_reserve_err_)?;
    Result::Ok(crate::connection::Telegraph::new_(conn, local, rec))
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
    // 先落实积压的释放消息：发起方「未裁决就丢弃」的撤销（`UnreserveChannel`）与
    // 响应方的宽限登记（`ReleaseChannel`）都可能是同一条 dock 对的上一轮残留。
    conn.core_()
        .drain_session_events_(cancel.child_token())
        .await
        .map_err(map_reserve_err_)?;
    // 1. 登记身份（dock 对即身份，重复即 `Duplicate`）；共享状态随身份一起建立。
    let owner = conn
        .core_()
        .reserve_channel_(local, remote_dock, cancel.child_token())
        .await
        .map_err(map_reserve_err_)?;

    // 2. 把开场消息搬进句柄（`OPEN` 要等 `accept_async` 才发）。
    let payload = read_available_into_vec_(
        message,
        conn.core_().opts_().basic_opts.max_packet_size,
        cancel.child_token(),
    )
    .await;

    Result::Ok(ChannelHandle::new_initiator_(
        conn,
        local,
        remote_dock,
        owner,
        payload,
    ))
}
