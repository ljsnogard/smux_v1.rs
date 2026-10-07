use core::{
    alloc::AllocatorClone,
    mem::MaybeUninit,
};

use abs_buff::{
    TrBuffRead, gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use abs_mm::res_man::TrUnique;
use abs_smux::{
    chan::TrPrepareRing,
    conn::TrDockBinding,
    dock::TrDock,
};
use buffex::x_deps::abs_buff;
use mm_ptr::x_deps::abs_mm;

use crate::connection::{
    ChannelHandle, Dock, MuxError, MuxConnection, ReserveErr_, Telegraph, TrConnCfg,
    channel_half::{ChannelRx, ChannelTx},
    channel_listener::ChannelListener,
    signal_::{ReadEvent_, SessionEvent_, TrEventSender_, WriteEvent_},
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

    type OpenTelegraphAsync<'f, B, P> = MuxOpenTelegraphAsync<'f, 'f, C>
    where
        Self: 'f,
        B: 'static + TrUnique<Item = [MaybeUninit<C::Data>], Alloc: AllocatorClone> + Send + Sync,
        P: TrPrepareRing<B, C::Data>;

    type Tx = ChannelTx<C>;
    type Rx = ChannelRx<C>;

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

    fn open_telegraph_async<'f, B, P>(
        &'f mut self,
        prepare: P,
    ) -> Self::OpenTelegraphAsync<'f, B, P>
    where
        B: 'static + TrUnique<Item = [MaybeUninit<C::Data>], Alloc: AllocatorClone> + Send + Sync,
        P: TrPrepareRing<B, C::Data>,
    {
        // **先同步建环**：`P` 与 `B` 因此都不进 future 的类型（与 channel 的
        // `accept_async` 同一形状）。建环失败在这里就定局，step 只处理结果。
        let rings = crate::connection::telegraph::build_telegraph_rings_(prepare.prepare());
        MuxOpenTelegraphAsync::new(self, rings)
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
    // 预分配的队列（入向通知是身份节点里的**内联**槽），因此这里先显式消费掉参数；
    // 等定容队列落地后再按它预分配（见 `crate::connection` 模块文档 §2.3 的遗留项）。
    let _ = reserve;
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
/// 与 channel 的最终裁决同形：**ring 内存由调用方当场交出**（`prepare`），本函数把两块
/// 内存各建一条环，然后
///
/// 1. 登记 telegraph 身份（**独占**该 `local_dock`）；
/// 2. 把**会话侧**两个半部经事件通道交给对应的中心循环
///    （发送环读端 → 复用循环，接收环写端 → 解复用循环）；
/// 3. 把**应用侧**两个半部与身份句柄包成端点工厂交给调用方。
///
/// 数据报没有建流握手，因此这里**不发任何帧**：身份与环一就位即可收发。
///
/// # 失败路径
///
/// - 建环失败（容量非法 / 分配失败）→ [`BindingError::RingRejected`]，身份**尚未**登记，
///   无需撤销；
/// - 身份被占（`local_dock` 已作 telegraph / listener / channel）→
///   [`BindingError::DockInUse`]（由 `reserve_telegraph_` 判定）；
/// - 事件投递失败（连接正在收尾）→ [`BindingError::Closed`]，并投一条
///   `ReleaseTelegraph` 撤销刚登记的身份。
#[gen_may_cancel_future(MuxOpenTelegraph, pub, new(pub(crate)))]
async fn mux_open_telegraph_async_<'f, C, K>(
    binding: &'f mut DockBinding<C>,
    rings: Result<
        crate::connection::telegraph::TelegraphRings_,
        crate::connection::TelegraphError,
    >,
    cancel: K,
) -> Result<crate::connection::Telegraph<C>, BindingError>
where
    C: TrConnCfg + 'f,

    K: TrCancellationToken,
{
    let conn = binding.conn_.clone();
    let local = binding.local_dock_;

    // 1. 纯本地：建两条环（结果在 `open_telegraph_async` 里已经算出）。
    let ((tx_w, tx_r), (rx_w, rx_r)) = rings.map_err(map_telegraph_err_)?;

    // 2. 先落实积压的释放消息，再认领身份（与 `listen_async` 同一纪律）。
    conn.core_()
        .drain_session_events_(cancel.child_token())
        .await
        .map_err(map_reserve_err_)?;
    let rec = conn
        .core_()
        .reserve_telegraph_(local, cancel.child_token())
        .await
        .map_err(map_reserve_err_)?;

    // 3. 把会话侧两个半部交给对应的中心循环。失败（循环已不在 = 连接正在收尾）时必须
    //    撤销身份：否则该 `local_dock` 会被一个永远不能收发、也没有使用者的身份占住。
    let attached = conn
        .core_()
        .w_events_()
        .try_send_event_(WriteEvent_::TgAttach {
            local_dock: local,
            owner: rec.clone(),
            reader_: tx_r,
        })
        && conn
            .core_()
            .r_events_()
            .try_send_event_(ReadEvent_::TgAttach {
                local_dock: local,
                owner: rec.clone(),
                writer_: rx_w,
            });
    if !attached && !cfg!(test) {
        let _ = conn
            .core_()
            .reg_()
            .post_session_event_(SessionEvent_::ReleaseTelegraph { local_dock: local });
        return Result::Err(BindingError::Closed);
    }

    // 4. 应用侧两个半部 + 身份句柄 → 端点工厂。
    Result::Ok(Telegraph::new_(conn, local, rec, tx_w, rx_r))
}

/// 把 telegraph 的建环错误映射为 binding 面的错误。
fn map_telegraph_err_(err: crate::connection::TelegraphError) -> BindingError {
    match err {
        crate::connection::TelegraphError::RingRejected => BindingError::RingRejected,
        _ => BindingError::Closed,
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
