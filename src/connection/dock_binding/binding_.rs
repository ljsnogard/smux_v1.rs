use abs_buff::{
    TrBuffRead, gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use abs_smux::{conn::TrDockBinding, dock::TrDock};
use buffex::x_deps::abs_buff;

use crate::connection::{
    BindingError, ChannelHandle, Dock, MuxConnection, ReserveErr_, TrConnCfg,
    channel_half::{ChannelRx, ChannelTx},
    channel_listener::ChannelListener,
    util_::read_available_into_vec_,
};

/// 把注册表的预留失败映射进 binding 的错误类型。
fn map_reserve_err_<C>(err: ReserveErr_) -> BindingError<C>
where
    C: TrConnCfg,
{
    match err {
        ReserveErr_::DockInUse => BindingError::DockInUse,
        ReserveErr_::Duplicate => BindingError::Duplicate,
        ReserveErr_::WaitClose => BindingError::WaitClose,
        ReserveErr_::DockChanLimit => BindingError::DockChanLimit,
        ReserveErr_::ChanLimit => BindingError::ChanLimit,
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
pub struct DockBinding<C, S>
where
    C: TrConnCfg,
{
    /// 连接智能指针（`bind_async` 取 `&self`，克隆廉价）。
    conn_: MuxConnection<C, S>,

    /// 本会话绑定的 local_dock。
    local_dock_: Dock,
}

impl<C, S> DockBinding<C, S>
where
    C: TrConnCfg,
{
    /// 由连接与 `local_dock` 构造（只允许 `bind_async` 调用）。
    pub(crate) fn new_(conn: MuxConnection<C, S>, local_dock: Dock) -> Self {
        DockBinding {
            conn_: conn,
            local_dock_: local_dock,
        }
    }
}

/// 丢弃绑定即**解绑**：释放 `local_dock` 的独占占用，使同一个 dock 之后可以再次
/// `bind_async`（见 `ChannelRegistry_::unbind_dock_`）。
///
/// 注意解绑**不**影响已经由该 binding 建立、且仍在应用手里的子流半部：那些
/// [`ChannelTx`] / [`ChannelRx`] 不借用 binding，`unbind_dock_` 也不会动它们。
impl<C, S> Drop for DockBinding<C, S>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        self.conn_.core_().reg_().unbind_dock_(self.local_dock_);
    }
}

impl<C, S> TrDockBinding<C> for DockBinding<C, S>
where
    C: TrConnCfg,
{
    type Err = BindingError<C>;

    type ChannelHandle = ChannelHandle<C, S>;

    type Listener = ChannelListener<C, S>;

    type ListenAsync<'f> = MuxListenAsync<'f, 'f, C, S> where Self: 'f;

    type Telegraph = crate::connection::Telegraph<C, S>;

    type OpenTelegraphAsync<'f> = MuxOpenTelegraphAsync<'f, 'f, C, S> where Self: 'f;

    type Tx = ChannelTx<C, S>;
    type Rx = ChannelRx<C, S>;

    type OpenChannelAsync<'f, M> = MuxOpenChannelAsync<'f, 'f, C, S, M>
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
async fn mux_listen_async_<'f, C, S, K>(
    binding: &'f mut DockBinding<C, S>,
    _cancel: K,
) -> Result<ChannelListener<C, S>, BindingError<C>>
where
    C: TrConnCfg + 'f,
    S: 'f,
    K: TrCancellationToken,
{
    let conn = binding.conn_.clone();
    let local = binding.local_dock_;
    // 登记 listener 身份（统一身份表里的 `(local, wildcard)`）：它既是「本 dock 在
    // 监听」的事实，也是入向等待者的落点；若该 dock 已作 telegraph 会被拒。
    conn.core_()
        .reg_()
        .reserve_listener_(local)
        .map_err(map_reserve_err_)?;
    Result::Ok(ChannelListener::new_(conn, local))
}

/// [`TrDockBinding::open_telegraph_async`] 的 step 函数。
///
/// 本轮只登记端点身份（telegraph **独占**该 `local_dock`）并交付端点对象；
/// 端点的收发方法仍是 `todo!()`（见 [`crate::connection::Telegraph`]）。
#[gen_may_cancel_future(MuxOpenTelegraph, pub, new(pub(crate)))]
async fn mux_open_telegraph_async_<'f, C, S, K>(
    binding: &'f mut DockBinding<C, S>,
    _cancel: K,
) -> Result<crate::connection::Telegraph<C, S>, BindingError<C>>
where
    C: TrConnCfg + 'f,
    S: 'f,
    K: TrCancellationToken,
{
    let conn = binding.conn_.clone();
    let local = binding.local_dock_;
    conn.core_()
        .reg_()
        .reserve_telegraph_(local)
        .map_err(map_reserve_err_)?;
    Result::Ok(crate::connection::Telegraph::new_(conn, local))
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
async fn mux_open_channel_async_<'f, C, S, M, K>(
    binding: &'f mut DockBinding<C, S>,
    remote_dock: Dock,
    message: &'f mut M,
    cancel: K,
) -> Result<ChannelHandle<C, S>, BindingError<C>>
where
    C: TrConnCfg + 'f,
    S: 'f,
    M: TrBuffRead<u8> + 'f,
    K: TrCancellationToken,
{
    let conn = binding.conn_.clone();
    let local = binding.local_dock_;
    if remote_dock.is_special() {
        return Result::Err(BindingError::ReservedDock);
    }
    // 1. 登记身份（dock 对即身份，重复即 `Duplicate`）。
    conn.core_()
        .reg_()
        .reserve_channel_(local, remote_dock)
        .map_err(map_reserve_err_)?;

    // 2. 把开场消息搬进句柄（`OPEN` 要等 `accept_async` 才发）。
    let payload = read_available_into_vec_(
        message,
        conn.core_().opts_().basic_opts.max_packet_size,
        cancel.child_token(),
    )
    .await;

    Result::Ok(ChannelHandle::new_initiator_(conn, local, remote_dock, payload))
}
