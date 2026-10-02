use abs_buff::{TrBuffRead, gen_may_cancel_future, x_deps::abs_cancel};
use abs_cancel::TrCancellationToken;
use abs_smux::conn::{TrDock, TrDockBinding};
use buffex::x_deps::abs_buff;

use crate::{
    connection::{
        Dock, FrameKind, MuxConnection, MuxError, TrMuxConfig,
        channel_half::{ChannelRx, ChannelTx},
        channel_listener::ChannelListener,
        owner_::{ChannelOwner_, ChannelState_, EstablishOutcome_, wait_establish_},
        ring_::new_buffered_channel_,
        signal_::{ControlFrame_, ReadEvent_, TrEventSender_, WriteEvent_},
        util_::read_available_into_vec_,
    },
    flow_ctrl::{FlowCtrl, RecvTotal, TrFlowCtrlPolicy},
};

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
///
/// # 身份与临时 dock
///
/// `(self.local_dock, remote_dock)` 即子流身份（见 [`crate::connection`] 模块
/// 文档 §4.1）：本会话的 `local_dock` 在
/// [`TrConnection::bind_async`](abs_smux::conn::TrConnection::bind_async) 时固定。
/// 若要在**同一时刻**向同一个 `remote_dock` 发起多条子流，就必须用**多个
/// local_dock 各建一个会话**——协议不提供 channel id，同一 dock 对上的两条并发
/// 子流无法区分。
pub struct DockBinding<C, S, RE, WE>
where
    C: TrMuxConfig,
{
    /// 连接智能指针（`bind_async` 取 `&self`，克隆廉价）。
    conn_: MuxConnection<C, S, RE, WE>,

    /// 本会话绑定的 local_dock。
    local_dock_: Dock,
}

impl<C, S, RE, WE> DockBinding<C, S, RE, WE>
where
    C: TrMuxConfig,
{
    /// 由连接与 `local_dock` 构造（只允许 `bind_async` 调用）。
    pub(crate) fn new_(conn: MuxConnection<C, S, RE, WE>, local_dock: Dock) -> Self {
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
impl<C, S, RE, WE> Drop for DockBinding<C, S, RE, WE>
where
    C: TrMuxConfig,
{
    fn drop(&mut self) {
        self.conn_.core_().reg_().unbind_dock_(self.local_dock_);
    }
}

impl<C, S, RE, WE> TrDockBinding for DockBinding<C, S, RE, WE>
where
    C: TrMuxConfig,
    RE: core::error::Error,
    WE: core::error::Error,
{
    type Data = u8;
    type Dock = Dock;
    type Err = MuxError<RE, WE>;

    type Listener<'f>
        = ChannelListener<C, S, RE, WE>
    where
        Self: 'f;

    type ListenAsync<'f>
        = MuxListenAsync<'f, 'f, C, S, RE, WE>
    where
        Self: 'f;

    type Telegraph<'f>
        = crate::connection::Telegraph<C, S, RE, WE>
    where
        Self: 'f;

    type OpenTelegraphAsync<'f>
        = MuxOpenTelegraphAsync<'f, 'f, C, S, RE, WE>
    where
        Self: 'f;

    type Tx = ChannelTx<C, S, RE, WE>;
    type Rx = ChannelRx<C, S, RE, WE>;

    type OpenChannelAsync<'f, M>
        = MuxOpenChannelAsync<'f, 'f, C, S, RE, WE, M>
    where
        Self: 'f,
        M: 'f + TrBuffRead<Self::Data>;

    fn local_dock(&self) -> &Self::Dock {
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
        remote_dock: Self::Dock,
        message: &'f mut M,
    ) -> Self::OpenChannelAsync<'f, M>
    where
        M: TrBuffRead<Self::Data>,
    {
        MuxOpenChannelAsync::new(self, remote_dock, message)
    }
}

/// [`TrDockBinding::listen_async`] 的 step 函数。
#[gen_may_cancel_future(MuxListen, pub, new(pub(crate)))]
async fn mux_listen_async_<'f, C, S, RE, WE, K>(
    binding: &'f mut DockBinding<C, S, RE, WE>,
    _cancel: K,
) -> Result<ChannelListener<C, S, RE, WE>, MuxError<RE, WE>>
where
    C: TrMuxConfig + 'f,
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
        .map_err(|err| err.cast_())?;
    Result::Ok(ChannelListener::new_(conn, local))
}

/// [`TrDockBinding::open_telegraph_async`] 的 step 函数。
///
/// 本轮只登记端点身份（telegraph **独占**该 `local_dock`）并交付端点对象；
/// 端点的收发方法仍是 `todo!()`（见 [`crate::connection::Telegraph`]）。
#[gen_may_cancel_future(MuxOpenTelegraph, pub, new(pub(crate)))]
async fn mux_open_telegraph_async_<'f, C, S, RE, WE, K>(
    binding: &'f mut DockBinding<C, S, RE, WE>,
    _cancel: K,
) -> Result<crate::connection::Telegraph<C, S, RE, WE>, MuxError<RE, WE>>
where
    C: TrMuxConfig + 'f,
    S: 'f,
    K: TrCancellationToken,
{
    let conn = binding.conn_.clone();
    let local = binding.local_dock_;
    conn.core_()
        .reg_()
        .reserve_telegraph_(local)
        .map_err(|err| err.cast_())?;
    Result::Ok(crate::connection::Telegraph::new_(conn, local))
}

/// [`TrDockBinding::open_channel_async`] 的 step 函数。
///
/// `message` 是随 `OPEN` 帧附带的开场消息；由于关联类型已泛型于它
/// （`OpenChannelAsync<'f, M>`），future 可以直接持有该缓冲。
#[gen_may_cancel_future(MuxOpenChannel, pub, new(pub(crate)))]
async fn mux_open_channel_async_<'f, C, S, RE, WE, M, K>(
    binding: &'f mut DockBinding<C, S, RE, WE>,
    remote_dock: Dock,
    message: &'f mut M,
    cancel: K,
) -> Result<(ChannelTx<C, S, RE, WE>, ChannelRx<C, S, RE, WE>), MuxError<RE, WE>>
where
    C: TrMuxConfig + 'f,
    S: 'f,
    M: TrBuffRead<u8> + 'f,
    K: TrCancellationToken,
{
    let conn = binding.conn_.clone();
    let local = binding.local_dock_;
    if remote_dock.is_special() {
        return Result::Err(MuxError::ReservedDock);
    }
    // 1. 登记身份（dock 对即身份，重复即 `Duplicate`）。
    conn.core_()
        .reg_()
        .reserve_channel_(local, remote_dock)
        .map_err(|err| err.cast_())?;

    // 2. 建两条环：发送环（应用写 / 写循环读）与接收环（读循环写 / 应用读）。
    let capacity = conn.core_().config_().channel_capacity();
    let alloc = conn.core_().config_().allocator();
    let initial = conn.core_().config_().policy().initial_window(capacity);
    let (tx_w, tx_r) = match new_buffered_channel_(
        conn.core_().config_().make_buff(capacity),
        alloc.clone(),
    ) {
        Result::Ok(pair) => pair,
        Result::Err(_) => {
            // 容量已在建连时校验过，这里不可达。
            conn.core_().reg_().release_channel_(local, remote_dock);
            return Result::Err(MuxError::Closed);
        }
    };
    let (rx_w, rx_r) = match new_buffered_channel_(
        conn.core_().config_().make_buff(capacity),
        alloc.clone(),
    ) {
        Result::Ok(pair) => pair,
        Result::Err(_) => {
            conn.core_().reg_().release_channel_(local, remote_dock);
            return Result::Err(MuxError::Closed);
        }
    };

    // 3. 共享状态 + 把会话侧半部移交给两个循环。
    let mut flow = FlowCtrl::new(conn.core_().config_().policy(), capacity);
    // 记下即将在 `OPEN` 里通告的那份窗口快照：`RecvWindow` 的越权判定以「最近一次
    // 通告」为准，不记录的话对端的第一帧就会被误判为 `PeerViolation`。
    flow.recv_window_mut().report();
    let owner = ChannelOwner_::new_(ChannelState_::new_(flow), alloc.clone());
    conn.core_()
        .reg_()
        .attach_owner_(local, remote_dock, owner.clone());
    let _ = conn
        .core_()
        .w_events_()
        .try_send_event_(WriteEvent_::Attach {
            local_dock: local,
            remote_dock,
            owner: owner.clone(),
            reader_: tx_r});
    let _ = conn
        .core_()
        .r_events_()
        .try_send_event_(ReadEvent_::Attach {
            local_dock: local,
            remote_dock,
            owner: owner.clone(),
            writer_: rx_w});

    // 4. 发 `OPEN`（带开场消息与本端接收窗口）。
    let payload = read_available_into_vec_(
        message,
        conn.core_().opts_().basic_opts.max_packet_size,
        cancel.child_token(),
    )
    .await;
    let _ = conn
        .core_()
        .w_events_()
        .try_send_event_(WriteEvent_::Control {
            frame_: ControlFrame_::with_window_(
                FrameKind::Open,
                0u8,
                local,
                remote_dock,
                Option::Some((0u64 as RecvTotal, initial)),
                payload,
            )});

    // 5. 等待对端 `OPEN` + `ACCEPT` / `REJECT`。
    match wait_establish_(conn.core_().reg_(), &owner, cancel.child_token()).await? {
        EstablishOutcome_::Accepted => Result::Ok((
            ChannelTx::new_(tx_w, owner.clone(), conn.clone(), local, remote_dock),
            ChannelRx::new_(rx_r, conn, local, remote_dock),
        )),
        EstablishOutcome_::Refused => {
            conn.core_().reg_().release_channel_(local, remote_dock);
            Result::Err(MuxError::Refused)
        }
    }
}
