use abs_buff::{
    TrBuffRead, TrBuffWrite,
    gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use abs_smux::conn::{TrDock, TrDockBinding};
use buffex::x_deps::abs_buff;

use crate::{
    connection::{
        Dock, FrameKind, MuxConnection, MuxError, TrMuxConfig,
        channel_half::{ChannelRx, ChannelTx, RxRing_, TxRing_},
        channel_listener::ChannelListener,
        owner_::{ChannelOwner_, ChannelState_, EstablishOutcome_, wait_establish_},
        ring_::new_buffered_channel_,
        signal_::{ControlFrame_, ReadEvent_, TrEventSender_, WriteEvent_},
        util_::read_available_into_vec_,
    },
    flow_ctrl::{FlowCtrl, RecvTotal, TrFlowCtrlPolicy},
};

/// 在某个 `local_dock` 上派生的会话对象。///
/// > **目标形状（本轮确定，迁移中）**：本类型改为**持有一份 [`MuxConnection`] 的
/// > 克隆**（= 指向 `MuxCore` 的智能指针），不再借用连接，因此生命周期参数从公开
/// > 类型上消失，可以存进结构体、可以从函数返回。设计见
/// > [`crate::connection`] 模块文档 §2 与 `dev-notes/connection-20261002-0548.md` §17。
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
/// 文档 §4.1）：本会话的 `local_dock` 在 [`MuxConnection::new`](crate::connection::MuxConnection::new) 之后由
/// [`TrConnection::bind_async`](abs_smux::conn::TrConnection::bind_async) 固定。若要在**同一时刻**向同一个 `remote_dock`
/// 发起多条子流，就必须用**多个 local_dock 各建一个会话**——协议不提供 channel
/// id，同一 dock 对上的两条并发子流无法区分。
pub struct DockBinding<'f, R, W, C, Rt>
where
    C: TrMuxConfig,
{
    /// 连接对象（共享，`bind_async` 取 `&self`）。
    conn_: &'f MuxConnection<R, W, C, Rt>,

    /// 本会话绑定的 local_dock。
    local_dock_: Dock,
}

impl<'f, R, W, C, Rt> DockBinding<'f, R, W, C, Rt>
where
    C: TrMuxConfig,
{
    /// 由连接与 `local_dock` 构造（只允许 `bind_async` 调用）。
    pub(crate) fn new_(conn: &'f MuxConnection<R, W, C, Rt>, local_dock: Dock) -> Self {
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
impl<R, W, C, Rt> Drop for DockBinding<'_, R, W, C, Rt>
where
    C: TrMuxConfig,
{
    fn drop(&mut self) {
        self.conn_.reg_().unbind_dock_(self.local_dock_);
    }
}

impl<'f, R, W, C, Rt> TrDockBinding for DockBinding<'f, R, W, C, Rt>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
{
    type Data = u8;
    type Dock = Dock;
    type Err = MuxError<R::Err, W::Err>;

    type Listener<'s>
        = ChannelListener<'s, 'f, R, W, C, Rt>
    where
        Self: 's;

    type ListenAsync<'s>
        = MuxListenAsync<'s, 'f, 's, R, W, C, Rt>
    where
        Self: 's;

    type Telegraph<'s>
        = crate::connection::Telegraph<'s, 'f, R, W, C, Rt>
    where
        Self: 's;

    type OpenTelegraphAsync<'s>
        = MuxOpenTelegraphAsync<'s, 'f, 's, R, W, C, Rt>
    where
        Self: 's;

    type Tx = ChannelTx<TxRing_<C::Buff, C::Alloc>>;
    type Rx = ChannelRx<RxRing_<C::Buff, C::Alloc>>;

    type OpenChannelAsync<'s, M>
        = MuxOpenChannelAsync<'s, 'f, 's, R, W, C, Rt, M>
    where
        Self: 's,
        M: 's + TrBuffRead<Self::Data>;

    fn local_dock(&self) -> &Self::Dock {
        &self.local_dock_
    }

    fn listen_async(&mut self) -> Self::ListenAsync<'_> {
        MuxListenAsync::new(self)
    }

    fn open_telegraph_async(&mut self) -> Self::OpenTelegraphAsync<'_> {
        MuxOpenTelegraphAsync::new(self)
    }

    fn open_channel_async<'s, M>(
        &'s mut self,
        remote_dock: Self::Dock,
        message: &'s mut M,
    ) -> Self::OpenChannelAsync<'s, M>
    where
        M: TrBuffRead<Self::Data>,
    {
        MuxOpenChannelAsync::new(self, remote_dock, message)
    }
}

/// [`TrDockBinding::listen_async`] 的 step 函数。
#[gen_may_cancel_future(MuxListen, pub)]
async fn mux_listen_async_<'s, 'f, R, W, C, Rt, K>(
    binding: &'s mut DockBinding<'f, R, W, C, Rt>,
    _cancel: K,
) -> Result<ChannelListener<'s, 'f, R, W, C, Rt>, MuxError<R::Err, W::Err>>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    Rt: 'f,
    K: TrCancellationToken,
{
    let conn = binding.conn_;
    let local = binding.local_dock_;
    // 登记 listener 身份（统一身份表里的 `(local, wildcard)`）：它既是「本 dock 在
    // 监听」的事实，也是入向等待者的落点；若该 dock 已作 telegraph 会被拒。
    conn.reg_()
        .reserve_listener_(local)
        .map_err(|err| err.cast_())?;
    Result::Ok(ChannelListener::new_(conn, local))
}

/// [`TrDockBinding::open_telegraph_async`] 的 step 函数。
///
/// 本轮只登记端点身份（telegraph **独占**该 `local_dock`）并交付端点对象；
/// 端点的收发方法仍是 `todo!()`（见 [`crate::connection::Telegraph`]）。
#[gen_may_cancel_future(MuxOpenTelegraph, pub)]
async fn mux_open_telegraph_async_<'s, 'f, R, W, C, Rt, K>(
    binding: &'s mut DockBinding<'f, R, W, C, Rt>,
    _cancel: K,
) -> Result<crate::connection::Telegraph<'s, 'f, R, W, C, Rt>, MuxError<R::Err, W::Err>>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    Rt: 'f,
    K: TrCancellationToken,
{
    let conn = binding.conn_;
    let local = binding.local_dock_;
    conn.reg_()
        .reserve_telegraph_(local)
        .map_err(|err| err.cast_())?;
    Result::Ok(crate::connection::Telegraph::new_(conn, local))
}

/// [`TrDockBinding::open_channel_async`] 的 step 函数。
///
/// `message` 是随 `OPEN` 帧附带的开场消息；由于关联类型已泛型于它
/// （`OpenChannelAsync<'s, M>`），future 可以直接持有该缓冲。
#[gen_may_cancel_future(MuxOpenChannel, pub)]
async fn mux_open_channel_async_<'s, 'f, R, W, C, Rt, M, K>(
    binding: &'s mut DockBinding<'f, R, W, C, Rt>,
    remote_dock: Dock,
    message: &'s mut M,
    cancel: K,
) -> Result<
    (
        ChannelTx<TxRing_<C::Buff, C::Alloc>>,
        ChannelRx<RxRing_<C::Buff, C::Alloc>>,
    ),
    MuxError<R::Err, W::Err>,
>
where
    'f: 's,
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    M: TrBuffRead<u8> + 's,
    K: TrCancellationToken,
{
    let conn = binding.conn_;
    let local = binding.local_dock_;
    if remote_dock.is_special() {
        return Result::Err(MuxError::ReservedDock);
    }
    // 1. 登记身份（dock 对即身份，重复即 `Duplicate`）。
    conn.reg_()
        .reserve_channel_(local, remote_dock)
        .map_err(|err| err.cast_())?;

    // 2. 建两条环：发送环（应用写 / 写循环读）与接收环（读循环写 / 应用读）。
    let capacity = conn.config_().channel_capacity();
    let alloc = conn.config_().allocator();
    let initial = conn.config_().policy().initial_window(capacity);
    let (tx_w, tx_r) = match new_buffered_channel_(
        conn.config_().make_buff(capacity),
        alloc.clone(),
    ) {
        Result::Ok(pair) => pair,
        Result::Err(_) => {
            // 容量已在 `MuxConnection::new` 校验过，这里不可达。
            conn.reg_().release_channel_(local, remote_dock);
            return Result::Err(MuxError::Closed);
        }
    };
    let (rx_w, rx_r) = match new_buffered_channel_(
        conn.config_().make_buff(capacity),
        alloc.clone(),
    ) {
        Result::Ok(pair) => pair,
        Result::Err(_) => {
            conn.reg_().release_channel_(local, remote_dock);
            return Result::Err(MuxError::Closed);
        }
    };

    // 3. 共享状态 + 把会话侧半部移交给两个循环。
    let mut flow = FlowCtrl::new(conn.config_().policy(), capacity);
    // 记下即将在 `OPEN` 里通告的那份窗口快照：`RecvWindow` 的越权判定以「最近一次
    // 通告」为准，不记录的话对端的第一帧就会被误判为 `PeerViolation`。
    flow.recv_window_mut().report();
    let owner = ChannelOwner_::new_(ChannelState_::new_(flow), alloc);
    conn.reg_().attach_owner_(local, remote_dock, owner.clone());
    let _ = conn.w_events_().try_send_event_(WriteEvent_::Attach {
        local_dock: local,
        remote_dock,
        owner: owner.clone(),
        reader_: tx_r});
    let _ = conn.r_events_().try_send_event_(ReadEvent_::Attach {
        local_dock: local,
        remote_dock,
        owner: owner.clone(),
        writer_: rx_w});

    // 4. 发 `OPEN`（带开场消息与本端接收窗口）。
    let payload = read_available_into_vec_(message, conn.opts_().basic_opts.max_packet_size, cancel.child_token()).await;
    let _ = conn.w_events_().try_send_event_(WriteEvent_::Control {
        frame_: ControlFrame_::with_window_(
            FrameKind::Open,
            0u8,
            local,
            remote_dock,
            Option::Some((0u64 as RecvTotal, initial)),
            payload,
        )});

    // 5. 等待对端 `OPEN` + `ACCEPT` / `REJECT`。
    match wait_establish_(conn.reg_(), &owner, cancel.child_token()).await? {
        EstablishOutcome_::Accepted => Result::Ok((
            ChannelTx::new_(
                TxRing_::new_(
                    tx_w,
                    owner.clone(),
                    conn.w_events_().clone(),
                    local,
                    remote_dock,
                ),
                local,
                remote_dock,
            ),
            ChannelRx::new_(
                RxRing_::new_(rx_r, conn.w_events_().clone(), local, remote_dock),
                local,
                remote_dock,
            ),
        )),
        EstablishOutcome_::Refused => {
            conn.reg_().release_channel_(local, remote_dock);
            Result::Err(MuxError::Refused)
        }
    }
}
