#[cfg(not(feature = "multi-thread"))]
use core::marker::PhantomData;

use abs_buff::{
    TrBuffRead, TrBuffWrite,
    gen_may_cancel_future,
    x_deps::abs_cancel,
};
#[cfg(feature = "multi-thread")]
use abs_art::TrSpawnSend;
// `TrJoinHandle` 只被单线程 `new` 的文档链接引用，因此只在缺省配置下导入。
#[cfg(not(feature = "multi-thread"))]
use abs_art::{TrJoinHandle, TrSpawnLocal};

use abs_cancel::TrCancellationToken;
use abs_smux::conn::{TrConnection, TrDock};
use buffex::x_deps::abs_buff;

// 建连驱动（单线程配置）专用：多线程配置下 `new` 仍是 `todo!()`（见 dev-notes），
// 这些名字在该配置下不可达，因此按 feature 收窄导入范围，保持两种配置都零告警。
#[cfg(not(feature = "multi-thread"))]
use crate::connection::session_::{LoopShared_, read_loop_async_, write_loop_async_};
#[cfg(not(feature = "multi-thread"))]
use crate::connection::signal_::event_channel_;
#[cfg(not(feature = "multi-thread"))]
use crate::flow_ctrl::{ReportThresholds_, TrFlowCtrlPolicy};

use crate::{
    connection::{
        Dock, MuxError, TrMuxConfig,
        dock_binding::DockBinding,
        signal_::{EventSender_, ReadEvent_, WriteEvent_},
        sync_::ChannelRegistry_,
        types_::MuxMark_,
    },
    handshake::{agent::HandshakeDelivery, opts::HandshakeOpts},
};

/// 复用连接：**独占**网络收发半边，并对外提供流复用的全部功能。
///
/// 泛型参数：
///
/// - `R`：网络读半边（握手交付的 `Rx`）；
/// - `W`：网络写半边（握手交付的 `Tx`）；
/// - `C`：资源策略，见 [`TrMuxConfig`]；
/// - `Rt`：运行时类型（见模块文档「运行时参数 `Rt`」）。它必须是**结构体**上的
///   类型参数：内部循环的句柄类型要能出现在字段类型里。
///
/// # 封装边界
///
/// 握手完成后 `Rx` / `Tx` 的生命周期由本对象**完全接管**，它们不出现在任何公开
/// 签名里；读 / 写两个循环是本对象的内部实现（见 [`crate::connection`] 模块文档
/// §2）。`smux_v1` 里其它类型要完成任何功能，只能通过本对象的 API：
/// [`TrConnection::bind_async`] 派生会话；收发由内部任务自动推进。
///
/// [`TrConnection::bind_async`] 取 `&self`，因此同一个连接可以被多个业务逻辑同时
/// 绑定到不同 dock，读 / 写路径在内部互不争锁（模块文档 §2）。
pub struct MuxConnection<R, W, C, Rt>
where
    C: TrMuxConfig,
{
    /// 资源策略：建子流环与判定窗口时在 API 面就地取用（`policy()` 返回引用，
    /// 因此它不能被移进 `'static` 的循环里，只能留在连接对象上）。
    config_: C,

    /// 握手协商结果（连接级配额）。
    opts_: HandshakeOpts,

    /// dock / 子流索引、失败标志与两个循环的取消令牌。
    reg_: ChannelRegistry_<C::Alloc>,

    /// 写事件发送端（控制帧、建流注册）。
    w_events_: EventSender_<WriteEvent_<C::Buff, C::Alloc>>,

    /// 读事件发送端（接收环注册）。
    r_events_: EventSender_<ReadEvent_<C::Buff, C::Alloc>>,

    /// `Rx` / `Tx` 与 `Rt` 只以类型形式出现在签名里：前者已被移交给内部循环，
    /// 后者是纯类型标记（`abs_art` 的 spawn 是无 `self` 的关联函数）。
    _mark_: MuxMark_<R, W, Rt>}

/// 单线程版本（缺省，compio 首要）：内部循环经 `Rt::spawn_local` 投递，因此要求
/// `Rt: TrSpawnLocal`。
#[cfg(not(feature = "multi-thread"))]
impl<R, W, C, Rt> MuxConnection<R, W, C, Rt>
where
    R: TrBuffRead<u8> + 'static,
    W: TrBuffWrite<u8> + 'static,
    C: TrMuxConfig + 'static,
    C::Buff: 'static,
    C::Alloc: 'static,
    Rt: TrSpawnLocal,
{
    /// 由一次成功的握手交付物与资源策略构造连接，接管 `Rx` / `Tx`，并**在内部
    /// 经 `abs_art` spawn** 读 / 写两个循环（排空控制帧后按对端发送窗口调度数据
    /// 帧，见 [`crate::connection`] 模块文档 §2）。
    ///
    /// # 循环的收尾方式
    ///
    /// 不靠句柄停任务：[`TrJoinHandle`](abs_art::TrJoinHandle) **没有 `abort`**，
    /// 而且三个后端对「drop 句柄」的语义并不一致（tokio 视作 detach、compio /
    /// smol 视作取消）。因此收尾统一走**取消令牌 + 「连接已失败」标志**：
    /// spawn 后立即 `detach()` 句柄，循环在每个 await 点检查令牌与失败标志自行
    /// 退出；连接 `Drop` 时置位即可。句柄因此不需要存成字段，`Rt::JoinHandle`
    /// 也不必出现在任何类型签名里。
    ///
    /// # Panics
    ///
    /// 帧暂存缓冲按协商出的 `max_packet_size` 分配，分配失败即 panic（与标准库
    /// 容器一致）。
    ///
    /// 对外不提供任何驱动 API：用户只使用 `abs_smux` 的 trait。后端运行时由最终
    /// 二进制经 `abs_art` 选择（本 crate 不依赖 `abs_art-bridge`）。
    pub fn new(delivery: HandshakeDelivery<W, R>, config: C) -> Self {
        let HandshakeDelivery { opts, tx, rx } = delivery;
        let max_packet_size = opts.basic_opts.max_packet_size;
        let capacity = config.channel_capacity();
        let initial = config.policy().initial_window(capacity);
        let thresholds = ReportThresholds_::new_(config.policy(), initial);
        let alloc = config.allocator();

        let reg = ChannelRegistry_::new_(opts.basic_opts.clone(), alloc.clone());
        let (w_events, w_receiver) = event_channel_();
        let (r_events, r_receiver) = event_channel_();
        let shared = LoopShared_::new_(reg.clone(), initial, thresholds, max_packet_size);

        let read_fut = read_loop_async_(
            rx,
            shared.clone(),
            r_receiver,
            w_events.clone(),
            vec![0u8; max_packet_size],
            reg.loop_token_(0usize),
        );
        Rt::spawn_local(read_fut).detach();

        let write_fut = write_loop_async_(
            tx,
            shared,
            w_receiver,
            r_events.clone(),
            reg.loop_token_(1usize),
        );
        Rt::spawn_local(write_fut).detach();

        MuxConnection {
            config_: config,
            opts_: opts,
            reg_: reg,
            w_events_: w_events,
            r_events_: r_events,
            _mark_: PhantomData}
    }
}

/// 多线程版本（开启 `multi-thread`）：语义与单线程版本**完全一致**，唯一区别是
/// 内部循环经 `Rt::spawn` 投递（可跨线程），因此 bound 换成 `Rt: TrSpawnSend`。
///
/// # 本轮状态（待实现）
///
/// `Rt::spawn` 要求循环 future `Send`，而 `abs_buff` 的 trait 没有给关联 future
/// 加 `Send`；把该约束沿「共享字节游标」(`wire_io_`) 补齐会连带要求握手模块
/// （它也复用同一个游标）全部改为 `Send` 版本。该改动与「多 channel 并发」这一
/// 本轮目标无关，因此**多线程配置的驱动留作待办**（缺省的单线程 / compio 配置
/// 已完整实现）。见 `dev-notes/connection-20261002-0548.md` §7。
#[cfg(feature = "multi-thread")]
impl<R, W, C, Rt> MuxConnection<R, W, C, Rt>
where
    R: TrBuffRead<u8> + Send + 'static,
    W: TrBuffWrite<u8> + Send + 'static,
    C: TrMuxConfig + Send + Sync + 'static,
    C::Buff: 'static,
    C::Alloc: 'static,
    Rt: TrSpawnSend,
{
    /// 与缺省配置下的同名方法语义相同（含「取消令牌 + 当场 detach」的收尾方式），
    /// 只是本配置下要求 `Rt: TrSpawnSend`。
    ///
    /// # Panics
    ///
    /// 本配置尚未实现驱动：调用即 panic（见类型文档「本轮状态」）。
    pub fn new(delivery: HandshakeDelivery<W, R>, config: C) -> Self {
        let _ = (delivery, config);
        todo!("多线程配置的连接驱动待实现：Send 约束需沿共享字节游标与握手模块一并补齐")
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 会话侧只读访问器（字段一律私有，跨模块只能经这些关联函数）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<R, W, C, Rt> MuxConnection<R, W, C, Rt>
where
    C: TrMuxConfig,
{
    /// 资源策略。
    pub(crate) fn config_(&self) -> &C {
        &self.config_
    }

    /// 握手协商结果（连接级配额）。
    pub(crate) fn opts_(&self) -> &HandshakeOpts {
        &self.opts_
    }

    /// dock / 子流索引与失败标志。
    pub(crate) fn reg_(&self) -> &ChannelRegistry_<C::Alloc> {
        &self.reg_
    }

    /// 写事件发送端。
    pub(crate) fn w_events_(&self) -> &EventSender_<WriteEvent_<C::Buff, C::Alloc>> {
        &self.w_events_
    }

    /// 读事件发送端。
    pub(crate) fn r_events_(&self) -> &EventSender_<ReadEvent_<C::Buff, C::Alloc>> {
        &self.r_events_
    }
}

impl<R, W, C, Rt> TrConnection for MuxConnection<R, W, C, Rt>
where
    R: TrBuffRead<u8>,
    W: TrBuffWrite<u8>,
    C: TrMuxConfig,
{
    type Data = u8;
    type Dock = Dock;
    type Err = MuxError<R::Err, W::Err>;

    type DockBinding<'f>
        = DockBinding<'f, R, W, C, Rt>
    where
        Self: 'f;

    type BindAsync<'f>
        = MuxBindAsync<'f, 'f, R, W, C, Rt>
    where
        Self: 'f;

    fn bind_async<'f>(&'f self, local_dock: Self::Dock) -> Self::BindAsync<'f> {
        MuxBindAsync::new(self, local_dock)
    }
}

/// [`TrConnection::bind_async`] 的 step 函数。
#[gen_may_cancel_future(MuxBind, pub)]
async fn mux_bind_async_<'f, R, W, C, Rt, K>(
    conn: &'f MuxConnection<R, W, C, Rt>,
    local_dock: Dock,
    _cancel: K,
) -> Result<DockBinding<'f, R, W, C, Rt>, MuxError<R::Err, W::Err>>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    C: TrMuxConfig + 'f,
    K: TrCancellationToken,
{
    if local_dock.is_special() {
        return Result::Err(MuxError::ReservedDock);
    }
    // 独占绑定：同一 local_dock 在任意时刻至多一个 `DockBinding`（见
    // `ChannelRegistry_::bind_dock_` 与 `DockBinding` 的「绑定的独占性」）。
    conn.reg_()
        .bind_dock_(local_dock)
        .map_err(|err| err.cast_())?;
    Result::Ok(DockBinding::new_(conn, local_dock))
}

