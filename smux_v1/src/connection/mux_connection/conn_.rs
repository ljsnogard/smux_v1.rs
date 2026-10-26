use core::{future::poll_fn, task::Poll};

use abs_art::{TrClock, TrDelay, TrJoinHandle, TrLocalScope};
use abs_art_facade::ScopeHost;
use abs_cancel::TrCancellationToken;
use abs_smux::{
    conf::TrMuxConfig,
    conn::TrConnection,
    dock::TrDock,
};
use gen_mcf2::gen_may_cancel_future;
use mm_ptr::{Owned, Shared};

use crate::{
    connection::{
        BufferedRx, BufferedTx, ControlState_, Dock, MainErr_, MainReply_, MainRequest_, MuxError,
        PushErr_, TrConnAllocCfg, TrConnCfg, TcsAbandonGuard_, Tcs_,
        dock_binding::DockBinding,
        main_loop_async_,
        public_channel_::{PublicChannel_, PublicChunkIo_},
        ring_::new_buffered_channel,
        session_::{MuxShared_, demux_loop_async_, mux_loop_async_, race_cancel_},
        signal_::{EventReceiver_, ReadEvent_, WriteEvent_, event_channel_},
        timer_::timer_loop_async_,
    },
    handshake::{agent::HandshakeDelivery, opts::HandshakeOpts},
    metrics::TrMetricsSink,
    time::{ConnClock_, millis_of_},
    x_deps::{abs_art, abs_cancel, abs_smux, gen_mcf2, mm_ptr},
};
use super::{
    core_::MuxCore,
    registry_::{ChannelRegistry_, ReserveErr_},
};

/// [`TrConnection`](abs_smux::conn::TrConnection) 的错误类型：目前只有 `bind_async`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BindError {
    /// 要绑定的 dock 是协议保留值 `wildcard`，不能当身份用。
    ///
    /// `unspecified` **不会**引发这个错误：它表示「由连接自行安排一个空闲 dock」，
    /// 见 [`TrConnection::bind_async`](abs_smux::conn::TrConnection::bind_async)。
    #[error("要绑定的 dock 是协议保留值，不能作为身份")]
    ReservedDock,

    /// 该 local_dock 已被占用；`bind_async(unspecified)` 下则是**候选范围已占满**。
    ///
    /// 两种情形共用一个变体：调用方在它们面前能做的事没有区别（换一个 dock、或者
    /// 先释放一些 binding 再试）。
    #[error("该 local_dock 已被占用")]
    DockInUse,

    /// **等锁期间被取消**（cancel token 触发）。
    #[error("本次操作被取消")]
    Cancelled,

    /// 连接级失败：`Display` 用内层文案，`source` 指回内层（`?` 亦直通）。
    #[error("{0}")]
    Mux(#[from] MuxError),
}

/// 复用连接：**对一个 `MuxCore` 的智能指针的薄封装**，同时实现
/// [`TrConnection`]。
///
/// 泛型参数只有 `C`：资源策略，见 [`TrConnCfg`]。**运行时值不是类型参数**——它由
/// [`TrConnCfg::Rt`] 给出（`abs_art::TrTime` 的实现值，例如
/// `abs_art_tokio::Runtime<{ FULL }>`），提供「现在几点」与「怎么等」，二者同源——
/// 见 [`crate::time`] 模块文档。
///
/// 因此连接是否 `Send + Sync` 由 `C::Rt` 决定：tokio 装配下是，compio 装配下否
/// （见 `mux_connection::core_` 的类型文档）。
///
/// 本地作用域同样**不**出现在本类型的参数上：它只在 [`MuxConnection::new`] 的
/// 方法级泛型里出现，投递完三个循环后由循环各自持有（见
/// [`core_`](super::core_) 模块文档「队列保活」）。
///
/// 收发半边类型由 `C::ConnTx` / `C::ConnRx` 给出，不再出现在公开类型上。
pub struct MuxConnection<C>
where
    C: TrConnCfg,
{
    /// 指向演员核心的强引用；最后一个强引用消失时核心析构（并触发收尾）。
    core_: Shared<MuxCore<C>, crate::connection::ConnAlloc_<C>>,
}

impl<C> Clone for MuxConnection<C>
where
    C: TrConnCfg,
{
    /// 克隆即多一个强引用：核心不复制，句柄因此可以与任意多个对象共享同一连接。
    fn clone(&self) -> Self {
        MuxConnection {
            core_: self.core_.clone(),
        }
    }
}

impl<C> MuxConnection<C>
where
    C: TrConnCfg,
{
    /// 由一次成功的握手交付物、资源策略与**两块连接级缓冲**构造连接：运行时值与
    /// 本地作用域都由 `C` 交出（[`TrConnCfg::runtime`] 与各后端的
    /// `Runtime::local_scope()`），接管 `C::ConnRx` / `C::ConnTx`，并**经作用域
    /// `spawn_local`** 投递三个循环。
    ///
    /// # 三个循环
    ///
    /// | 令牌序号 | 循环 | 搬运方向 |
    /// | --- | --- | --- |
    /// | 0 | 解复用 | `C::ConnRx` → 各子流接收环（解析帧、拉取读事件） |
    /// | 1 | 复用 | 各子流发送环 → `C::ConnTx`（成帧、拉取写事件） |
    /// | 2 | 计时 | 不搬字节：保活 `PULSE` 与空闲超时拆流（见私有模块 `timer_`） |
    ///
    /// 内侧两个循环（0 / 1）从事件队列拉取 `Attach` / `Control` 等事件、并按各子流的
    /// 收尾状态位推进拆流，据此决定哪些子流此刻可搬；计时循环（2）在前两者都静默时仍然会醒来，把
    /// 保活与拆流的需求投进同两条事件队列。
    ///
    /// # 为什么不再有连接级帧暂存环
    ///
    /// 旧版把传输交给两条「贴传输」的泵循环，由它们把字节搬进 / 搬出**连接自己**的
    /// 两条帧暂存环，内侧两个循环只与那两条环打交道。但那两条环是**第二次缓冲**：
    /// 握手的交付物（`ConnTx` / `ConnRx`）本身就自带缓冲——它要么是 `buffex::ring`
    /// 的环半部，要么是「设备 + 环」的适配器（`buffex_*_adapt` 的 `BuffWrite` /
    /// `BuffRead`），两者都实现了 `abs_buff` 的 `TrProducerState` /
    /// `TrConsumerState`。于是「泵 + 环」这一层只带来一次多余的搬运，本版直接去掉：
    /// 解复用循环读 `ConnRx`、复用循环写 `ConnTx`，park 与提交都发生在传输自身之上。
    ///
    /// 代价是传输必须**自己带缓冲**（这正是 [`TrConnCfg::ConnTx`] /
    /// [`TrConnCfg::ConnRx`] 上的两个状态 trait 约束的用途）：如果直接把一个
    /// 「裸设备」当传输（例如 socket 的读写半边），内侧循环就会 park 在设备上，
    /// 事件队列与另一方向都会被它拖住。
    ///
    /// # 运行时值与作用域都**不再由调用者指定**
    ///
    /// 在 `abs_art` 的分层里这两件事仍然分开：
    ///
    /// | 关切 | 挂在哪 | 为什么 |
    /// | --- | --- | --- |
    /// | 计时与时刻（[`TrTime`]） | 运行时值（`C::Rt`） | 与「在哪个线程调度」无关；虚拟时间只需换掉它 |
    /// | 本地队列（[`TrLocalScope`]） | 作用域值 | 线程独占、必须由持有者驱动 |
    ///
    /// 但**取用路径**变了：本版经 `abs_art-bridge` 集成，后端由集成方在 `Cargo.toml`
    /// 里选定（本仓缺省 compio），于是
    ///
    /// - 运行时值由 [`TrConnCfg::runtime`] 交出（`DefaultConnCfg` 里存着建连时抓住的
    ///   那一个），类型是 `C::Rt`；
    /// - 本地作用域由该运行时值的固有方法 `local_scope()` 就地取得，只作为**方法级
    ///   泛型** `S` 出现在本函数上，投递完三个循环后由循环各自持有一份克隆来保活队列
    ///   （见 [`core_`](super::core_) 模块文档）。
    ///
    /// 需要显式控制两者时走 [`MuxConnection::new_with_rt`]；只想换运行时值而不动
    /// 调用形状时，自写一个 `C` 并让它的 [`TrConnCfg::runtime`] 返回别的值即可。
    ///
    /// # Panics
    ///
    /// 本函数自身不再有 panic 点（旧版会在「调用方交出的缓冲容量非法」时 panic）；
    /// 取默认后端运行时值失败时仍由后端 panic（见 [`TrConnCfg::runtime`]）。
    pub fn new(
        delivery: HandshakeDelivery<C::ConnTx, C::ConnRx>,
        config: C,
    ) -> Self
    where
        C: 'static + Clone,
        crate::connection::ConnAlloc_<C>: 'static,
        C::Rt: ScopeHost,
    {
        let rt = config.runtime();
        let scope = ScopeHost::local_scope(&rt);
        Self::new_with_rt_(&rt, &scope, delivery, config)
    }

    /// 由**调用者给定**的运行时值与作用域构造连接（「特别的需要」那条接口）。
    ///
    /// [`MuxConnection::new`] 就是本函数的默认实现：它取 [`TrConnCfg::runtime`] 交出的
    /// 运行时值、再用该值的 `local_scope()` 取作用域。本入口把两者显式交给调用者，
    /// 适用于：
    ///
    /// - 建连点**不在**后端运行时上下文内（compio 的 `Runtime::current()` 会 panic）；
    /// - 想用**虚拟时钟**验收（传入 `abs_art_mock_clock::ManualTime` 装饰出的运行时值）；
    /// - 想固定某个具名后端（例如默认 compio 但本条连接要跑在 tokio 上）。
    ///
    /// 运行时值的类型就是 [`TrConnCfg::Rt`]：类型参数仍然只有 `C` 一个。
    pub fn new_with_rt<S>(
        rt: &C::Rt,
        scope: &S,
        delivery: HandshakeDelivery<C::ConnTx, C::ConnRx>,
        config: C,
    ) -> Self
    where
        C: 'static + Clone,
        crate::connection::ConnAlloc_<C>: 'static,
        S: TrLocalScope + Clone + 'static,
    {
        Self::new_with_rt_(rt, scope, delivery, config)
    }

    /// 两个公开构造入口的共同实现体。
    fn new_with_rt_<S>(
        rt: &C::Rt,
        scope: &S,
        delivery: HandshakeDelivery<C::ConnTx, C::ConnRx>,
        config: C,
    ) -> Self
    where
        C: 'static + Clone,
        crate::connection::ConnAlloc_<C>: 'static,
        S: TrLocalScope + Clone + 'static,
    {
        let HandshakeDelivery { opts, tx, rx } = delivery;
        let bundle = CoreBundle_::build_(rt, opts, config.clone());
        let CoreBundle_ {
            core_,
            shared_,
            control_,
            public_tx_w_,
            public_tx_r_,
            public_rx_w_,
            public_rx_r_,
            w_receiver_,
            r_receiver_,
        } = bundle;

        // 解复用：**直接读传输的读半边**（没有读泵，也没有连接读环）。
        let demux_fut = keep_queue_alive_(
            scope.clone(),
            demux_loop_async_::<C, _>(
                rx,
                shared_.clone(),
                r_receiver_,
                core_.w_events_().clone(),
                public_rx_w_,
                core_.loop_token_(0usize),
            ),
        );
        scope.spawn_local(demux_fut).detach();

        // 复用：**直接写传输的写半边**（没有写泵，也没有连接写环）。
        let mux_fut = keep_queue_alive_(
            scope.clone(),
            mux_loop_async_::<C, _>(
                tx,
                shared_.clone(),
                w_receiver_,
                core_.r_events_().clone(),
                public_tx_r_,
                core_.loop_token_(1usize),
            ),
        );
        scope.spawn_local(mux_fut).detach();

        // 计时循环：保活（PULSE）与空闲超时拆流。它不搬字节，只按连接级时钟算
        // 期限、把保活动作投进上面两条事件通道（见 `timer_` 模块文档）。
        //
        // 控制面：请求队列 + 身份表写者（P1 接管 bind / listen / reserve）。
        let main_fut = keep_queue_alive_(
            scope.clone(),
            main_loop_async_::<C, _>(
                shared_.clone(),
                control_.clone(),
                public_tx_w_,
                public_rx_r_,
                core_.loop_token_(3usize),
            ),
        );
        scope.spawn_local(main_fut).detach();

        let timer_fut = keep_queue_alive_(
            scope.clone(),
            timer_loop_async_::<C, _>(
                shared_,
                core_.w_events_().clone(),
                core_.r_events_().clone(),
                core_.loop_token_(2usize),
            ),
        );
        scope.spawn_local(timer_fut).detach();

        // 连接到此才算**建立成功**：三个循环都已经投递。上报点放在这里而不是 `new`
        // 的开头——「建立成功」的语义正是「循环已经开始跑」（见 `crate::metrics`）。
        core_.config_().metrics().on_conn_opened();

        MuxConnection { core_ }
    }
}

/// 把本地作用域的一份克隆**绑进循环 future**，让队列随循环存活。
///
/// # 为什么需要它
///
/// `abs_art` 的本地队列（tokio 的 `LocalSet`、smol 的 `LocalExecutor`）随**作用域值**
/// 存活，而本版核心已经不再持有作用域（见 [`core_`](super::core_) 模块文档
/// 「队列保活」）。于是投递时把一份克隆 move 进 future：只要循环还在跑，队列就有
/// 持有者，调用方提前丢弃自己那份作用域也不会让连接静默失效。
///
/// 形成的 `队列 → 任务 → 队列` 引用环由取消令牌打破：核心析构触发令牌，三个循环
/// 退出，环随之解开。
///
/// compio 后端的 `LocalScope` 是零大小标记（队列归运行时），本函数对它没有实际
/// 保活作用，但形状一致、无需分支。
async fn keep_queue_alive_<S, F>(scope: S, inner: F) -> <F as core::future::Future>::Output
where
    F: core::future::Future,
{
    // 绑定活到本 future 结束。`LocalScope` 内含 `Rc`（有 `Drop`），不会被提前析构；
    // 下划线前缀只是说明「此处刻意不使用它」，不为抑制无意义告警之外的任何目的。
    let _keep_scope = scope;
    inner.await
}

impl<C> MuxConnection<C>
where
    C: TrConnCfg,
{
    /// **诊断专用**：解复用循环的活动计数（只增不减）。
    ///
    /// 用来回答「循环到底还在不在跑」——挂起类问题的第一分岔点。只在 `cfg(test)`
    /// 下存在，不在生产 API 上。
    #[cfg(feature = "test-loop-probe")]
    pub fn demux_ticks_(&self) -> u64 {
        crate::connection::loop_probe_::demux_ticks_()
    }

    /// 演员核心（crate 内部句柄与两个循环都经它访问共享状态）。
    pub(crate) fn core_(&self) -> &MuxCore<C> {
        &self.core_
    }
}

/// 建连的中间产物：核心 + 两个内侧循环的共享量 + 两条事件接收端。
struct CoreBundle_<C>
where
    C: TrConnCfg,
{
    core_: Shared<MuxCore<C>, crate::connection::ConnAlloc_<C>>,
    shared_: MuxShared_<C>,
    control_: ControlState_<crate::connection::ConnAlloc_<C>, crate::connection::ConnMpmcAlloc_<C>>,
    public_tx_w_: BufferedTx,
    public_tx_r_: BufferedRx,
    public_rx_w_: BufferedTx,
    public_rx_r_: BufferedRx,
    w_receiver_: EventReceiver_<WriteEvent_<crate::connection::ConnAlloc_<C>>>,
    r_receiver_: EventReceiver_<ReadEvent_<crate::connection::ConnAlloc_<C>>>,
}

impl<C> CoreBundle_<C>
where
    C: TrConnCfg,
{
    /// 展开策略、建注册表与两条事件通道，并把全部共享状态装进核心。
    fn build_(rt: &C::Rt, opts: HandshakeOpts, config: C) -> Self
    {
        let max_packet_size = opts.basic_opts.max_packet_size;
        let channel_timeout_millis = millis_of_(opts.basic_opts.max_channel_timeout);
        let pending_msg_cap = config.pending_msg_capacity();
        let alloc_cfg = config.alloc_cfg();
        let alloc = alloc_cfg.registry_alloc();
        let mpmc_alloc = alloc_cfg.mpmc_alloc();
        // 建连这一刻就是连接的 epoch；此后协议里的时间量一律是「自它起算的毫秒」。
        let conn_clock = ConnClock_::new_(rt.clone());

        let reg = ChannelRegistry_::new_(
            opts.basic_opts.clone(),
            alloc.clone(),
            mpmc_alloc.clone(),
        );
        let control = ControlState_::new_(alloc.clone(), mpmc_alloc);
        // 公共报文子流：两条普通 ring + 四路唤醒槽。容量下界保证一个最大控制帧的
        // chunk 能完整写进空 ring（设计稿 §5.3）；建环失败等同连接建立失败。
        let public_cap = PublicChunkIo_::ring_capacity_(max_packet_size);
        let (public_tx_w_, public_tx_r_) =
            new_buffered_channel(Owned::new_uninit_slice(public_cap, alloc.clone()))
                .expect("公共 TX ring：容量由 max_packet_size 决定，失败等同分配失败");
        let (public_rx_w_, public_rx_r_) =
            new_buffered_channel(Owned::new_uninit_slice(public_cap, alloc.clone()))
                .expect("公共 RX ring：容量由 max_packet_size 决定，失败等同分配失败");
        let public = PublicChannel_::new_(alloc.clone());
        let (w_events, w_receiver) = event_channel_();
        let (r_events, r_receiver) = event_channel_();

        // 指标接收方在**建连这一刻**从配置克隆一份：循环不持有核心，拿不到
        // `MuxCore`，只能像运行时值那样自持一份（见 `core_` 模块文档「队列保活」）。
        // 缺省配置交出的是零大小的 `NoMetrics`，于是整段上报在单态化后消失
        // （见 `crate::metrics` 模块文档 §4）。
        //
        // `C::Metrics::clone(..)` 显式写成关联函数调用：`config.metrics().clone()` 会
        // 解析到 `<&C::Metrics as Clone>::clone`（克隆**引用**），不是我们要的那一份值。
        let metrics = <C as TrConnCfg>::Metrics::clone(config.metrics());

        let shared = MuxShared_::<C>::new_(
            reg.clone(),
            max_packet_size,
            pending_msg_cap,
            conn_clock.clone(),
            channel_timeout_millis,
            public,
            metrics.clone(),
        );
        // 核心自持一份三个循环的取消令牌：`MuxCore::drop` 因此不必去注册表取锁
        // （那会阻塞，而 `Drop` 可能在任意线程上发生）。
        let loops = [
            reg.loop_token_(0usize),
            reg.loop_token_(1usize),
            reg.loop_token_(2usize),
            reg.loop_token_(3usize),
        ];

        let core = Shared::new(
            MuxCore::new_(
                config,
                opts,
                reg,
                control.clone(),
                conn_clock.epoch_(),
                w_events,
                r_events,
                loops,
            ),
            alloc.clone(),
        );

        CoreBundle_ {
            core_: core,
            shared_: shared,
            control_: control,
            public_tx_w_,
            public_tx_r_,
            public_rx_w_,
            public_rx_r_,
            w_receiver_: w_receiver,
            r_receiver_: r_receiver,
        }
    }
}

#[cfg(test)]
impl<C> MuxConnection<C>
where
    C: TrConnCfg,
{
    /// 测试专用构造：只建核心与两条事件通道，**不 spawn 任何循环**。
    ///
    /// 运行时值取自 `config.runtime()`（测试配置给出的是假运行时值，时刻恒为 0）。
    pub(crate) fn new_test_(opts: HandshakeOpts, config: C) -> Self {
        Self::new_test_bundle_(opts, config).0
    }

    /// 测试专用：同时交出连接与循环共享面，供手动 `main_step_` 驱动。
    pub(crate) fn new_test_bundle_(opts: HandshakeOpts, config: C) -> (Self, MuxShared_<C>) {
        let rt = config.runtime();
        let CoreBundle_ { core_, shared_, .. } = CoreBundle_::build_(&rt, opts, config);
        (MuxConnection { core_ }, shared_)
    }
}

impl<C> TrConnection for MuxConnection<C>
where
    C: TrConnCfg,
{
    type Config = C;
    type DockBinding = DockBinding<C>;
    type Err = BindError;

    type BindAsync<'f> = MuxBindAsync<'f, 'f, C>
    where
        Self: 'f;

    fn bind_async<'f>(
        &'f self,
        local_dock: <Self::Config as TrMuxConfig>::Dock,
    ) -> Self::BindAsync<'f> {
        MuxBindAsync::new(self, local_dock)
    }

    type CloseAsync<'f> = MuxConnCloseAsync<'f, 'f, C>;

    fn close_async(&mut self) -> Self::CloseAsync<'_> {
        MuxConnCloseAsync::new(self)
    }
}

/// [`TrConnection::bind_async`] 的 step 函数。
///
/// `local_dock` 取 `unspecified` 表示**由连接自行安排**：本函数会在注册表的同一个
/// 写锁临界区里挑一个空闲 dock 并占住它（见 `ChannelRegistry_::bind_any_dock_`），
/// 实际拿到哪个由返回的 `DockBinding::local_dock` 给出。取 `wildcard` 仍然拒绝
/// ——它是「任意 remote」的哨兵，不是「请自行安排」。
#[gen_may_cancel_future(MuxBind, pub, new(pub(crate)))]
async fn mux_bind_async_<'f, C, K>(
    conn: &'f MuxConnection<C>,
    local_dock: Dock,
    cancel: K,
) -> Result<DockBinding<C>, BindError>
where
    C: TrConnCfg + 'f,
    K: TrCancellationToken,
{
    if local_dock.is_wildcard() {
        return Result::Err(BindError::ReservedDock);
    }
    // 连接已经进入优雅关闭：不再受理新 binding。这里是**锁外快路径**（main 的登记
    // 路径在锁内还会复查一次，见 `ChannelRegistry_::begin_close_`），省掉一次可能
    // 很长的等锁；关闭之后调用方本来也只能改去丢弃句柄。
    if conn.core_().is_closing_() {
        return Result::Err(BindError::Mux(MuxError::Closed));
    }
    // 一次请求 + 完成源：登记动作由 `main_loop`（身份表唯一写者）执行。
    let tcs = Tcs_::new_(conn.core_().config_().allocator());
    let waiter = tcs.clone();
    // 等待方取消 / 丢弃 future 时放弃结果；`main_loop` 在登记前后都会检查
    // `is_abandoned_()`，已放弃就回滚刚登记的身份（见 `main_loop::loop_`）。
    let _abandon = TcsAbandonGuard_::new_(&waiter);
    conn.core_()
        .control_()
        .submit_async_(
            MainRequest_::Bind {
                local_dock,
                reply: tcs,
            },
            cancel.child_token(),
        )
        .await
        .map_err(|err| match err {
            PushErr_::Closed(_) => BindError::Mux(MuxError::Closed),
            PushErr_::Cancelled(_) => BindError::Cancelled,
        })?;
    let reply = poll_fn(|cx| waiter.poll_wait_(cx)).await;
    match reply {
        Option::Some(Result::Ok(MainReply_::Dock { dock, unbind_ })) => {
            Result::Ok(DockBinding::new_(conn.clone(), dock, unbind_))
        }
        Option::Some(Result::Err(MainErr_::Reserve(ReserveErr_::DockInUse))) => {
            Result::Err(BindError::DockInUse)
        }
        // 连接已经进入优雅关闭：不再接受任何新 binding。用连接级 `Closed` 表达——
        // 「这条连接不再提供服务」正是调用方此刻的处境，它不必区分「dock 被占」
        // 与「连接在关」这两种在关闭语义下都无法补救的情形。
        Option::Some(Result::Err(MainErr_::Reserve(ReserveErr_::Closing))) => {
            Result::Err(BindError::Mux(MuxError::Closed))
        }
        Option::Some(Result::Err(MainErr_::Reserve(ReserveErr_::Cancelled))) => {
            Result::Err(BindError::Cancelled)
        }
        Option::Some(Result::Err(MainErr_::Mux(err))) => Result::Err(BindError::Mux(err)),
        // 其他预留原因 / 非 Dock 结果：绑定路径不该出现，保守映射为关闭。
        Option::Some(Result::Err(MainErr_::Reserve(_)))
        | Option::Some(Result::Ok(_))
        | Option::None => Result::Err(BindError::Mux(MuxError::Closed)),
    }
}

/// [`TrConnection::close_async`] 的 step 函数：**优雅关闭**。
///
/// # 状态驱动，而不是「收集 + 遍历」
///
/// 本函数只做三件事：
///
/// 1. **进入「等待关闭」**（[`MuxCore::enter_waiting_close_`]）：一次 CAS，不取锁、
///    **不分配**（上一版要在这里把活跃子流收进一个 `Vec`），随后唤醒计时循环；
/// 2. **等收尾发生**：判据是三个循环的取消令牌被触发——那是「连接确实收口了」的
///    唯一信号，由**循环自己**在合适的时刻执行（见下）；
/// 3. **超时或取消就强制收尾**（幂等）。
///
/// 谁在什么时候执行收尾：
///
/// | 时机 | 执行者 | 依据 |
/// | --- | --- | --- |
/// | 最后一条活跃子流与对端双向收尾 | 复用循环（`session_::maybe_release_`） | 身份释放后活跃子流归零 |
/// | 关闭点上没有活跃子流，或关闭整体超时 | 计时循环（`timer_loop_async_`） | `TimerScan_::close_action_` |
///
/// 「所有正在活动的子流排空并向对端发关闭信号」这一步同样由计时循环完成：它在
/// 等待关闭状态下为每条已裁决子流认领一次 `Finalize` 动作（复用循环据此**在发送环
/// 排空之后**补 `CLOSE(FIN)`），为每条尚未裁决的子流直接回 `REJECT`。整条路径一次
/// 堆分配都没有。
///
/// # 超时与取消
///
/// 等待上限取协商项 `max_channel_timeout`；计时循环在**同一时刻**也用它作关闭整体
/// 的超时兜底（`drop` 路径没有调用方，只有它兜得住）。调用方的取消令牌打断时同样
/// 强制收尾并返回 `false`——本函数不得假定调用者一定会等到最后。
///
/// 两个期限**数值相同、起算点不同**（计时循环从进入等待关闭那一刻起算，比本函数算
/// 自己的期限更早），因此谁先到是时序问题，不能当作判据。收尾由计时循环强制促成时，
/// 它会把 [`K_STATE_FORCED_CLOSE`](super::registry_::K_STATE_FORCED_CLOSE) 记在连接
/// 状态里；本函数据此把「超时收尾」与「与对端收敛」分开。
///
/// # 返回值
///
/// `true` = 收尾发生在**收敛之后**（最后一条子流已与对端双向收尾）；`false` = 超时
/// （包括「计时循环先于本函数自己的期限判定超时」这一情形）、被取消、或连接此前
/// 已经失败。
#[gen_may_cancel_future(MuxConnClose, pub, new(pub(crate)))]
async fn mux_conn_close_async_<'f, C, K>(
    conn: &'f mut MuxConnection<C>,
    cancel: K,
) -> bool
where
    C: TrConnCfg + 'f,
    K: TrCancellationToken,
{
    let core = conn.core_();

    // 连接已经失败（传输错误 / 对端关闭）：没有「优雅」可言——已提交的字节此刻
    // 是否送达不由本端决定。直接收尾，并如实报 `false`。
    if core.reg_().fail_kind_().is_some() {
        core.reg_().cancel_loops_();
        return false;
    }

    // 第 1 步：进入「等待关闭」。只有首次进入才唤醒推进引擎（已经关闭中的连接，
    // 引擎早就在转了）。
    if core.enter_waiting_close_() {
        core.notify_timer_();
    }

    // 第 2 步：等收尾（令牌触发）、超时或取消。
    let rt = core.config_().runtime();
    let deadline = rt.now() + core.opts_().basic_opts.max_channel_timeout;
    let closed = wait_for_finalize_(core, &rt, deadline, &cancel).await;

    // 第 3 步：超时 / 取消 / 失败 ⇒ 强制收尾（幂等；收敛路径上循环已经做过）。
    if !closed {
        core.reg_().cancel_loops_();
    }
    closed
}

/// 优雅关闭的**等待步**：等「收尾已经发生」（取消令牌被触发）、`deadline` 到期或
/// 调用方取消，三者任一先到就返回。
///
/// # 判据为什么是取消令牌
///
/// 上一版等的是「每条子流都收到对端 `FIN`」，因此必须在调用方手里攥着每条子流的
/// 状态句柄（那正是 `Vec` 的来源）。本版把「最后一条子流」的判定下沉到**循环**里
/// ——它们本来就在身份释放路径上（`total_` 归零）与扫描路径上（活跃子流归零）——
/// 于是调用方只需等一个连接级的信号：令牌有没有被触发。
///
/// 令牌触发可能来自收尾，也可能来自连接级失败（失败也会取消循环），因此醒来之后
/// 还要看一次 [`ChannelRegistry_::fail_kind_`]：非空 ⇒ 不是「优雅完成」，报 `false`。
async fn wait_for_finalize_<C, K>(
    core: &MuxCore<C>,
    rt: &C::Rt,
    deadline: <C::Rt as TrClock>::Instant,
    cancel: &K,
) -> bool
where
    C: TrConnCfg,
    K: TrCancellationToken,
{
    // 两个句柄：一个用来**问**（`is_cancelled`），一个用来**等**（`cancellation()`
    // 按值消费令牌）。克隆是零分配的共享句柄。
    let probe = core.loop_token_(0usize);
    let mut finalized = core::pin::pin!(probe.clone().cancellation());
    loop {
        // 复检：收尾可能就发生在进入本函数之前（或上一次醒来时）。
        //
        // 令牌触发只说明「连接确实收口了」，**不**说明是哪种收口：与对端收敛、计时
        // 循环的整体超时兜底、连接级失败，三者都会取消它。因此判据要把后两者排除
        // 掉——失败看 `fail_kind_`，超时看计时循环留下的**显式**标志
        // （`is_forced_close_`，见 `K_STATE_FORCED_CLOSE` 的文档）。
        if probe.is_cancelled() {
            return core.reg_().fail_kind_().is_none() && !core.reg_().is_forced_close_();
        }
        // 连接级失败：收尾是失败牵连的结果，不是「优雅完成」。
        if core.reg_().fail_kind_().is_some() {
            return false;
        }
        let now = rt.now();
        if now >= deadline {
            return false;
        }
        let remaining = deadline - now;
        let mut sleeping = core::pin::pin!(rt.delay(remaining));
        let woke = race_cancel_(
            cancel,
            core::future::poll_fn(|cx| {
                // 收尾信号：令牌一旦触发就永久保持，因此这里只需等一次。
                if core::future::Future::poll(finalized.as_mut(), cx).is_ready() {
                    return Poll::Ready(());
                }
                core::future::Future::poll(sleeping.as_mut(), cx)
            }),
        )
        .await;
        if woke.is_none() {
            // 调用方取消：强制收尾。
            return false;
        }
    }
}
