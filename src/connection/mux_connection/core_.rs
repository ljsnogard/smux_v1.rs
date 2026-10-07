//! `MuxCore`：复用连接的**演员核心**。
//!
//! 连接的全部共享状态都在这里：资源策略、握手协商结果、身份索引
//! （`ChannelRegistry_`）、两条事件通道的发送端，以及**作用域保活槽**。
//!
//! 注意「建流时展开的三个标量」（初始接收窗口、通告阈值快照、单帧上限）**不在**
//! 这里，而在 [`LoopShared_`](crate::connection::session_::LoopShared_)：它们只有
//! 两个循环读，API 面一个都不用，放进核心只会多一份需要同步的副本。
//!
//! # 为什么它是「演员」而不是「连接对象」
//!
//! [`MuxConnection`](super::MuxConnection) 只是指向本类型的智能指针
//! （`mm_ptr::Shared<MuxCore<C, R>, C::Alloc>`）：句柄与两个循环各自持有一份，
//! 因此「谁借用谁」被换成「谁持有谁的一份指针」，生命周期参数从公开类型上消失。
//! 对共享状态的修改一律经内部读写锁串行化（不使用 actor 框架、不使用消息通道）。
//!
//! # 谁持有它，什么时候析构
//!
//! 持有强引用的只有**应用面对象**：`MuxConnection` 及其派生句柄
//! （binding / listener / handle / telegraph / 两个半部）。两个读写循环**不**持有
//! 它——循环只持有 [`LoopShared_`](crate::connection::session_::LoopShared_)（注册表
//! 句柄 + 三个标量），因此最后一个应用面对象被丢弃时本类型确实会析构，
//! [`Drop`](MuxCore::drop) 就在那里触发两个循环的取消令牌。
//!
//! 若循环持有强引用，核心将永远无法析构、取消令牌永远不会被触发，两个任务与
//! 整个连接状态会永久泄漏——这是本设计里最容易写错的一处，务必保持。
//!
//! # 队列保活：由五个循环各自承担，核心不再持有作用域
//!
//! 本地队列（tokio 的 `LocalSet`、smol 的 `LocalExecutor`）**随作用域值存活**，
//! 所以「连接活着 ⇒ 队列活着」这条保证必须有人兑现。上一版由
//! `MuxCore::scope_` 持有作用域克隆来兑现。
//!
//! 本版核心的类型参数**只剩资源策略 `C`**（运行时值由 `TrConnCfg::Rt` 提供、不进
//! 类型参数，见类型文档），保活改为**五个循环各持一份克隆**：投递时把克隆 move 进循环 future，
//! 于是只要还有一个循环在跑，队列就不会被回收。
//!
//! 这样会在 tokio / smol 上形成 `队列 → 任务 → 队列` 的引用环，但**不会泄漏**：
//! 最后一个应用面强引用消失时核心析构、取消令牌触发（令牌是独立的可克隆句柄，
//! 不依赖作用域），五个循环随即退出并从队列中移除，环就解开了。
//!
//! 注意队列**是否被驱动**仍然由调用方负责（tokio 需要 `scope.run_until(..)`、
//! compio 由运行时驱动）；循环持有的克隆只保证队列不被提前回收。

use buffex::x_deps::abs_cancel::TrCancellationToken;

use abs_art::TrClock;

use crate::{
    connection::{
        Dock, TrConnCfg,
        mux_connection::{ChannelRegistry_, ReserveErr_},
        owner_::{ChannelOwner_, LsnOwner_, TgOwner_},
        signal_::{EventSender_, ReadEvent_, WriteEvent_},
        sync_::CancelToken_,
    },
    flow_ctrl::WindowReport,
    handshake::opts::HandshakeOpts,
    metrics::{ConnCloseReason, TrMetricsSink},
};

/// 复用连接的**演员核心**：连接的全部共享状态与全部资源句柄。
///
/// 泛型参数只有 `C`：资源策略（见 [`TrConnCfg`](crate::connection::TrConnCfg)），
/// 它同时给出**运行时值**（[`C::Rt`](crate::connection::TrConnCfg::Rt)：时刻与计时）。
///
/// # 为什么核心**不**持有运行时值
///
/// 本类型是 `mm_ptr::Shared` 的被指对象，而
/// `Shared<T, A>: Send + Sync` 要求 `T: Send + Sync`。compio 后端的
/// `Runtime` 是 `!Send + !Sync`（内含线程本地执行器），一旦核心持有它就再也不可能
/// `Send + Sync`——那不是加约束能解决的，是结构性的。
///
/// 因此核心只留**建连 epoch**（一个纯数据），需要「现在」时经
/// [`TrConnCfg::runtime`](crate::connection::TrConnCfg::runtime) 取一个运行时值
/// （克隆句柄，廉价）再相减。于是 `MuxCore<C>: Send + Sync` 与后端是否为
/// `Send` **无关**。
///
/// 本地作用域**不在**本类型的参数里：它只出现在
/// [`MuxConnection::new`](super::MuxConnection::new) 的方法级泛型上，投递完五个循环
/// 之后由循环各自持有（见模块文档「队列保活」）。
///
/// # 生命周期
///
/// 最后一个应用面强引用消失时本类型析构，`Drop` 触发五个循环的取消令牌
/// （`ChannelRegistry_::cancel_loops_`），循环随即在下一个 await 点自行退出。
pub(crate) struct MuxCore<C>
where
    C: TrConnCfg,
{
    /// 资源策略：建子流环与判定窗口时在 API 面就地取用。
    config_: C,

    /// 握手协商结果（连接级配额）。
    opts_: HandshakeOpts,

    /// **建连时刻**：一切「连接内毫秒」的零点。
    ///
    /// 存的是**纯数据**（时刻类型来自运行时值），不是运行时值本身——理由见类型文档。
    epoch_: <C::Rt as TrClock>::Instant,

    /// dock / 子流身份索引、失败标志与两个循环的取消令牌。
    reg_: ChannelRegistry_<C::Alloc>,

    /// 写事件发送端（控制帧、建流注册、水位与拆流通知）。
    w_events_: EventSender_<WriteEvent_<C::Alloc>>,

    /// 读事件发送端（接收环注册与释放）。
    r_events_: EventSender_<ReadEvent_<C::Alloc>>,

    /// 五个循环的取消令牌（`0` = 读泵、`1` = 解复用、`2` = 复用、`3` = 写泵、
    /// `4` = 计时）。
    ///
    /// 核心自己持一份克隆，**而不是在 `Drop` 里去注册表取**：注册表取锁在跨线程
    /// 争用时是阻塞等待，而 `Drop` 必须不阻塞。令牌是可克隆的共享句柄，因此这里
    /// 持有的就是循环在用的那一个。
    loops_: [CancelToken_<C::Alloc>; 5],
}

impl<C> MuxCore<C>
where
    C: TrConnCfg,
{
    /// 由建连路径展开后的全部量构造（成员私有，构造只能走这里）。
    ///
    /// 参数确实多（策略 / 协商结果 / 注册表 / 建连时刻 / 两条通道 / 五个令牌）：
    /// 它们全部来自同一个建连路径，打成一个中间结构只会多一层壳而没有别的收益。
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_(
        config: C,
        opts: HandshakeOpts,
        reg: ChannelRegistry_<C::Alloc>,
        epoch: <C::Rt as TrClock>::Instant,
        w_events: EventSender_<WriteEvent_<C::Alloc>>,
        r_events: EventSender_<ReadEvent_<C::Alloc>>,
        loops: [CancelToken_<C::Alloc>; 5],
    ) -> Self {
        MuxCore {
            config_: config,
            opts_: opts,
            epoch_: epoch,
            reg_: reg,
            w_events_: w_events,
            r_events_: r_events,
            loops_: loops}
    }

    /// 资源策略。
    pub(crate) fn config_(&self) -> &C {
        &self.config_
    }

    /// 握手协商结果（连接级配额）。
    pub(crate) fn opts_(&self) -> &HandshakeOpts {
        &self.opts_
    }

    /// 「现在」的连接内毫秒（自建连 epoch 起算、向下取整、饱和）。
    ///
    /// 时刻来源是配置给出的运行时值——每次调用取一个克隆句柄，随后相减。这是核心
    /// 不持有运行时值的代价，也是它无条件 `Send + Sync` 的来源。
    pub(crate) fn now_millis_(&self) -> u64 {
        crate::time::millis_of_(self.config_.runtime().now() - self.epoch_)
    }

    /// dock / 子流身份索引与失败标志。
    pub(crate) fn reg_(&self) -> &ChannelRegistry_<C::Alloc> {
        &self.reg_
    }

    /// 写事件发送端。
    pub(crate) fn w_events_(&self) -> &EventSender_<WriteEvent_<C::Alloc>> {
        &self.w_events_
    }

    /// 读事件发送端。
    pub(crate) fn r_events_(&self) -> &EventSender_<ReadEvent_<C::Alloc>> {
        &self.r_events_
    }

    /// 第 `idx` 个循环的取消令牌（`0` = 读泵、`1` = 解复用、`2` = 复用、
    /// `3` = 写泵、`4` = 计时）。
    pub(crate) fn loop_token_(&self, idx: usize) -> CancelToken_<C::Alloc> {
        self.loops_[idx].clone()
    }
}

/// # 身份表的唯一入口：会话句柄只能经 `MuxCore` 访问注册表
///
/// 注册表是**核心的内部实现细节**：会话句柄（`DockBinding` / `ChannelListener` /
/// `ChannelHandle` / `Telegraph`）不直接持有它，而是经这里的方法转达。两个循环是
/// 例外——它们持有 [`LoopShared_`](crate::connection::session_)（核心在循环侧的
/// 展开），因此直接访问注册表句柄。
///
/// 每个方法都是**异步且可取消**的：等锁走协作式锁的异步获取，取消经调用方传进来的
/// token 生效（见 `sync_::acquire_read_` / `acquire_write_`）。
impl<C> MuxCore<C>
where
    C: TrConnCfg,
{
    /// 投递一条会话释放消息（**同步、不取锁**；`Drop` 与错误清理路径用）。
    pub(crate) fn post_session_event_(
        &self,
        event: crate::connection::signal_::SessionEvent_,
    ) -> bool {
        self.reg_.post_session_event_(event)
    }

    /// 落实积压的会话释放消息（`Drop` 只投消息，见 `signal_::SessionEvent_`）。
    pub(crate) async fn drain_session_events_<K: TrCancellationToken>(
        &self,
        cancel: K,
    ) -> Result<(), ReserveErr_> {
        self.reg_
            .drain_session_events_(self.now_millis_(), cancel)
            .await
    }

    /// 独占绑定一个 `local_dock`（[`TrConnection::bind_async`] 的登记点）。
    ///
    /// [`TrConnection::bind_async`]: abs_smux::conn::TrConnection::bind_async
    pub(crate) async fn bind_dock_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        cancel: K,
    ) -> Result<(), ReserveErr_> {
        self.reg_.bind_dock_(local_dock, cancel).await
    }

    /// 登记 listener 身份，并交出它的**身份节点句柄**（listener 在该句柄上等入向通知）。
    pub(crate) async fn reserve_listener_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        cancel: K,
    ) -> Result<LsnOwner_<C::Alloc>, ReserveErr_> {
        self.reg_.reserve_listener_(local_dock, cancel).await
    }

    /// 登记 telegraph 身份，并交出它的**身份节点句柄**。
    pub(crate) async fn reserve_telegraph_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        cancel: K,
    ) -> Result<TgOwner_<C::Alloc>, ReserveErr_> {
        self.reg_.reserve_telegraph_(local_dock, cancel).await
    }

    /// 为一条子流登记身份（dock 对即身份），并返回它的共享状态句柄。
    ///
    /// 状态与身份**一起建立**：调用方拿到句柄后，在建流最终裁决时把窗口参数安装进去。
    pub(crate) async fn reserve_channel_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
        cancel: K,
    ) -> Result<ChannelOwner_<C::Alloc>, ReserveErr_> {
        self.reg_
            .reserve_channel_(local_dock, remote_dock, self.now_millis_(), cancel)
            .await
    }

    /// 撤销一条尚未露面的子流登记。
    pub(crate) async fn unreserve_channel_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
        cancel: K,
    ) -> Result<(), ReserveErr_> {
        self.reg_
            .unreserve_channel_(local_dock, remote_dock, cancel)
            .await
    }

    /// 把一条已露面的子流放进拆流宽限期。
    pub(crate) async fn release_channel_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
        cancel: K,
    ) -> Result<(), ReserveErr_> {
        self.reg_
            .release_channel_(local_dock, remote_dock, self.now_millis_(), cancel)
            .await
    }

    /// 取走入向请求保存的对端窗口通告。
    pub(crate) async fn take_inbound_report_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
        cancel: K,
    ) -> Result<Option<WindowReport>, ReserveErr_> {
        self.reg_
            .take_inbound_report_(local_dock, remote_dock, cancel)
            .await
    }

    /// 取走一个待决入向请求（对端 dock + 该子流的共享状态句柄）。
    pub(crate) async fn take_pending_inbound_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        cancel: K,
    ) -> Result<Option<(Dock, ChannelOwner_<C::Alloc>)>, ReserveErr_> {
        self.reg_.take_pending_inbound_(local_dock, cancel).await
    }

    /// 连接级失败原因（若有）。
    pub(crate) async fn failure_<K: TrCancellationToken>(
        &self,
        cancel: K,
    ) -> Result<Option<crate::connection::MuxError>, ReserveErr_> {
        self.reg_.failure_(cancel).await
    }
}

impl<C> Drop for MuxCore<C>
where
    C: TrConnCfg,
{
    /// 连接收尾：触发五个循环的取消令牌。
    ///
    /// 循环在每个 await 点检查令牌并自行退出（泵与解复用循环的 park 经
    /// `race_cancel_`、复用循环的 park 与取消 future 竞争），因此这里是「丢弃
    /// 连接即关闭连接」的唯一入口，不依赖句柄的 `abort` / `drop` 语义。
    ///
    /// 令牌就在核心自己的字段里，因此本 `Drop` **不取注册表锁、不阻塞**——这一点
    /// 是跨线程收尾的前提：最后一个句柄可能在任意线程上被丢弃。计时循环也在这份
    /// 令牌上竞争，因此它不会在连接析构后继续持有状态。
    fn drop(&mut self) {
        // 上报连接关闭。**它必须在本函数里完成**，因此 sink 的方法必须同步、非阻塞、
        // 不 panic、不隐式分配——`Drop` 的硬纪律见本类型文档与 [`crate::metrics`]
        // 模块文档的「硬契约」。
        //
        // 关闭原因来自注册表的**锁外**快照（`fail_kind_`）：本函数不能取锁。
        // 时刻取自配置的运行时值——`TrConnCfg::runtime` 的契约保证交出的正是建连时
        // 抓住的那一个，读时刻不需要重新进入运行时上下文。
        //
        // 注意这里是**无条件**取一次时刻：没有 `Option` 短路之后，即使 sink 是静默的
        // `NoMetrics`，这次时钟读也会发生。它落在收尾路径上、每条连接只发生一次，
        // 因此可以接受；换成「按需才读」反而要把判空重新引回调用点。
        self.config_.metrics().on_conn_closed(
            self.reg_
                .fail_kind_()
                .unwrap_or(ConnCloseReason::Local),
            self.now_millis_(),
        );
        for token in &self.loops_ {
            token.cancel_();
        }
    }
}
