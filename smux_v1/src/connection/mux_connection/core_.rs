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
//! # 队列保活：由三个循环各自承担，核心不再持有作用域
//!
//! 本地队列（tokio 的 `LocalSet`、smol 的 `LocalExecutor`）**随作用域值存活**，
//! 所以「连接活着 ⇒ 队列活着」这条保证必须有人兑现。上一版由
//! `MuxCore::scope_` 持有作用域克隆来兑现。
//!
//! 本版核心的类型参数**只剩资源策略 `C`**（运行时值由 `TrConnCfg::Rt` 提供、不进
//! 类型参数，见类型文档），保活改为**三个循环各持一份克隆**：投递时把克隆 move 进循环 future，
//! 于是只要还有一个循环在跑，队列就不会被回收。
//!
//! 这样会在 tokio / smol 上形成 `队列 → 任务 → 队列` 的引用环，但**不会泄漏**：
//! 最后一个应用面强引用消失时核心析构、取消令牌触发（令牌是独立的可克隆句柄，
//! 不依赖作用域），三个循环随即退出并从队列中移除，环就解开了。
//!
//! 注意队列**是否被驱动**仍然由调用方负责（tokio 需要 `scope.run_until(..)`、
//! compio 由运行时驱动）；循环持有的克隆只保证队列不被提前回收。

use abs_art::TrClock;
use abs_cancel::TrCancellationToken;

use crate::{
    connection::{
        Dock, TrConnCfg,
        control_state_::ControlState_,
        mux_connection::{ChannelRegistry_, ReserveErr_},
        owner_::{ChannelOwner_, TgOwner_},
        pending_msg_::PendingMsgApp_,
        scope_::close_conn_scope_,
        signal_::{EventSender_, ReadEvent_, WriteEvent_},
        sync_::CancelToken_,
    },
    metrics::{ConnCloseReason, TrMetricsSink},
    x_deps::abs_cancel,
};

/// 复用连接的**演员核心**：连接的全部共享状态与全部资源句柄。
///
/// 泛型参数只有 `C`：资源策略（见 [`TrConnCfg`](crate::connection::TrConnCfg)），
/// 它同时给出**运行时值**（[`C::Rt`](crate::connection::TrConnCfg::Rt)：时刻与计时）。
///
/// # 核心**不直接**持有运行时值，但 `Send`/`Sync` 仍由后端决定
///
/// 本类型是 `mm_ptr::Shared` 的被指对象，而
/// `Shared<T, A>: Send + Sync` 要求 `T: Send + Sync`。compio 后端的
/// `Runtime` 是 `!Send + !Sync`（内含线程本地执行器），直接持有它就再也不可能
/// `Send + Sync`——那不是加约束能解决的，是结构性的。
///
/// 因此核心只留**建连 epoch**（一个纯数据），需要「现在」时经
/// [`TrConnCfg::runtime`](crate::connection::TrConnCfg::runtime) 取一个运行时值
/// （克隆句柄，廉价）再相减。
///
/// 但这条纪律**不足以**让 `MuxCore<C>` 无条件 `Send + Sync`：核心还持有整个配置
/// （`config_: C`），而配置（如
/// [`DefaultConnCfg`](crate::connection::DefaultConnCfg)）内含 `C::Rt`。于是
/// `MuxCore<C>: Send + Sync` 依然取决于后端：
///
/// | 装配 | `C::Rt` | `MuxCore<C>` / `MuxConnection<C>` |
/// | --- | --- | --- |
/// | tokio | `Handle` 把手（`Send + Sync`） | `Send + Sync` |
/// | compio | 线程本地的运行时实例（`!Send + !Sync`） | `!Send + !Sync` |
///
/// compio 那一侧是**如实**的表达（它的运行时绑定创建线程），由
/// `tests/thread_safety.rs` 的编译期断言与本文档共同钉住。要让核心在 compio 装配下
/// 也是 `Send + Sync`，用**不存储**运行时值的配置
/// [`CurrentConnCfg`](crate::connection::CurrentConnCfg)：它每次从当前上下文取用，
/// 代价是每个取用点都要求处于后端上下文内（**调用者责任**，见
/// [`TrRtCurrent`](crate::connection::TrRtCurrent)），且它不适用于「注入运行时值」的
/// 装配（虚拟时钟等仍走 `DefaultConnCfg`）。
///
/// 本地作用域**不在**本类型的参数里：它只出现在
/// [`MuxConnection::new`](super::MuxConnection::new) 的方法级泛型上，投递完三个循环
/// 之后由循环各自持有（见模块文档「队列保活」）。
///
/// # 生命周期
///
/// 最后一个应用面强引用消失时本类型析构。`Drop` **不再直接拆连接**，而是进入「等待
/// 关闭」并唤醒计时循环：已提交的字节先被排空、发出 `CLOSE(FIN)`，等对端也宣告不再
/// 发送之后，循环才收尾（取消令牌）——与
/// [`MuxConnection::close_async`](super::MuxConnection::close_async) 是**同一套**流程，
/// 区别只在没有调用方在等结果（见该方法的 `Drop` 实现与 `crate::connection` §7.2）。
pub(crate) struct MuxCore<C>
where
    C: TrConnCfg,
{
    /// 资源策略：建子流环与判定窗口时在 API 面就地取用。
    config_: C,

    /// 握手协商结果（连接级配额）。

    /// **建连时刻**：一切「连接内毫秒」的零点。
    ///
    /// 存的是**纯数据**（时刻类型来自运行时值），不是运行时值本身——理由见类型文档。
    epoch_: <C::Rt as TrClock>::Instant,

    /// dock / 子流身份索引、失败标志与四个循环的取消令牌。
    reg_: ChannelRegistry_<crate::connection::ConnAllocCfg_<C>>,

    /// 控制面共享状态：定容请求队列 + `main_loop` 的合并提示位。
    control_: ControlState_<crate::connection::ConnAllocCfg_<C>>,

    /// 写事件发送端（控制帧、建流注册、水位与拆流通知）。
    w_events_: EventSender_<WriteEvent_<crate::connection::ConnAllocCfg_<C>>>,

    /// 读事件发送端（接收环注册与释放）。
    r_events_: EventSender_<ReadEvent_<crate::connection::ConnAllocCfg_<C>>>,

    /// 四个循环的取消令牌（`0` = 解复用、`1` = 复用、`2` = 计时、`3` = 控制面）。
    ///
    /// 核心自己持一份克隆，**而不是在 `Drop` 里去注册表取**：注册表取锁在跨线程
    /// 争用时是阻塞等待，而 `Drop` 必须不阻塞。令牌是可克隆的共享句柄，因此这里
    /// 持有的就是循环在用的那一个。
    ///
    /// 旧版是五个：贴传输的读 / 写泵连同连接级帧暂存环一起去掉了（见 `session_`
    /// 模块文档）。控制面这一格在 P1 之前是空转的 `ctl_`，现在由 `main_loop` 承担。
    loops_: [CancelToken_<crate::connection::ConnCoreAlloc_<C>>; 4],
}

impl<C> MuxCore<C>
where
    C: TrConnCfg,
{
    /// 由建连路径展开后的全部量构造（成员私有，构造只能走这里）。
    ///
    /// 参数确实多（策略 / 协商结果 / 注册表 / 控制面 / 建连时刻 / 两条通道 / 四个令牌）：
    /// 它们全部来自同一个建连路径，打成一个中间结构只会多一层壳而没有别的收益。
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_(
        config: C,
        reg: ChannelRegistry_<crate::connection::ConnAllocCfg_<C>>,
        control: ControlState_<crate::connection::ConnAllocCfg_<C>>,
        epoch: <C::Rt as TrClock>::Instant,
        w_events: EventSender_<WriteEvent_<crate::connection::ConnAllocCfg_<C>>>,
        r_events: EventSender_<ReadEvent_<crate::connection::ConnAllocCfg_<C>>>,
        loops: [CancelToken_<crate::connection::ConnCoreAlloc_<C>>; 4],
    ) -> Self {
        MuxCore {
            config_: config,
            epoch_: epoch,
            reg_: reg,
            control_: control,
            w_events_: w_events,
            r_events_: r_events,
            loops_: loops}
    }

    /// 资源策略。
    pub(crate) fn config_(&self) -> &C {
        &self.config_
    }

    /// 控制面共享状态（API 面提交请求用）。
    pub(crate) fn control_(&self) -> &ControlState_<crate::connection::ConnAllocCfg_<C>> {
        &self.control_
    }

    /// 「现在」的连接内毫秒（自建连 epoch 起算、向下取整、饱和）。
    ///
    /// 时刻来源是配置给出的运行时值——每次调用取一个克隆句柄，随后相减。这是核心
    /// 不持有运行时值的代价，也是它无条件 `Send + Sync` 的来源。
    pub(crate) fn now_millis_(&self) -> u64 {
        crate::time::millis_of_(self.config_.runtime().now() - self.epoch_)
    }

    /// dock / 子流身份索引与失败标志。
    pub(crate) fn reg_(&self) -> &ChannelRegistry_<crate::connection::ConnAllocCfg_<C>> {
        &self.reg_
    }

    /// 写事件发送端。
    pub(crate) fn w_events_(&self) -> &EventSender_<WriteEvent_<crate::connection::ConnAllocCfg_<C>>> {
        &self.w_events_
    }

    /// 读事件发送端。
    pub(crate) fn r_events_(&self) -> &EventSender_<ReadEvent_<crate::connection::ConnAllocCfg_<C>>> {
        &self.r_events_
    }

    /// 第 `idx` 个循环的取消令牌（`0` = 解复用、`1` = 复用、`2` = 计时）。
    pub(crate) fn loop_token_(&self, idx: usize) -> CancelToken_<crate::connection::ConnCoreAlloc_<C>> {
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
    /// 连接是否已经进入**优雅关闭**（锁外原子读，热路径可用）。
    ///
    /// 语义有两条：**不再接受任何新身份**；已登记的子流由连接自己推进「排空 + 发
    /// `FIN`」，并在最后一条与对端双向收尾时收尾（见
    /// [`ChannelRegistry_::enter_waiting_close_`]）。
    pub(crate) fn is_closing_(&self) -> bool {
        self.reg_.is_closing_()
    }

    /// 唤醒**控制面**立刻做一轮定时决策。
    ///
    /// R9 起 `timer_loop` 只做计时（只等 `TimerSignal_` 的期限），定时决策的唯一
    /// 执行者是 `main_loop::tick_`。因此「进入等待关闭 / 关闭请求」需要的是让 main
    /// 立刻 tick 一次，而不是唤醒 timer 重算——直接 raise `HINT_MAIN_TIMER`。
    pub(crate) fn notify_timer_(&self) {
        self.control_
            .raise_main_hint_(crate::connection::HINT_MAIN_TIMER);
    }

    /// 登记 telegraph 身份，并交出它的**身份节点句柄**。
    pub(crate) async fn reserve_telegraph_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        cancel: K,
    ) -> Result<TgOwner_<crate::connection::ConnChanOwnerAlloc_<C>>, ReserveErr_> {
        self.reg_.reserve_telegraph_(local_dock, cancel).await
    }

    /// 把一条已露面的子流放进拆流宽限期。
    ///
    /// 返回值同 [`MuxCore::unreserve_channel_`]。
    pub(crate) async fn release_channel_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
        cancel: K,
    ) -> Result<bool, ReserveErr_> {
        self.reg_
            .release_channel_(local_dock, remote_dock, self.now_millis_(), cancel)
            .await
    }

    /// 取走一个待决入向请求（对端 dock + 该子流的共享状态句柄）。
    pub(crate) async fn take_pending_inbound_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        cancel: K,
    ) -> Result<Option<(Dock, ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>, PendingMsgApp_)>, ReserveErr_> {
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
    /// 连接收尾：**发起优雅关闭**（与 [`MuxConnection::close_async`](super::MuxConnection::close_async)
    /// 同一条流程），而不是立即拆除。
    ///
    /// # 为什么不是「立即取消三个循环」
    ///
    /// 上一版在这里直接触发取消令牌：已提交但还没上网的字节随循环一起消失。本版把
    /// 它换成与 `close_async` **完全同一套**的状态驱动流程——置「等待关闭」、唤醒
    /// 计时循环，由循环排空已提交字节、发 `FIN`、等对端 `FIN`，并在最后一条子流
    /// 双向收尾时执行收尾（取消令牌）。区别只有一处：**没有调用方在等结果**，因此
    /// 关闭的整体超时由计时循环兜底（`max_channel_timeout`）。
    ///
    /// 「丢弃连接」因此从「立刻断开」变成「尽力把话说完再断开」：数据有机会送达，
    /// 代价是连接在 `drop` 之后继续存活一段时间。要注意**本地队列必须仍被驱动**
    /// （tokio 下调用方要继续 `run_until`；compio / smol 由运行时驱动）——否则循环
    /// 推进不了，连接会停在「等待关闭」直到没人再驱动它。README §5 第 3 条写明了
    /// 这条。
    ///
    /// # 硬纪律
    ///
    /// 本函数仍然**不取注册表锁、不阻塞、不分配**：状态推进是一次 CAS，唤醒走的
    /// [`NotifySlot_`](crate::connection::sync_::NotifySlot_) 是零分配的自旋短临界区
    /// （且取 waker 与 `wake` 分开，不会在持锁时重入）。最后一个句柄可能在任意线程上
    /// 被丢弃，这条纪律是跨线程收尾的前提。
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
        #[cfg(feature = "test-loop-probe")]
        crate::connection::loop_probe_::core_drop_();
        let now_millis = self.now_millis_();
        self.config_.metrics().on_conn_closed(
            self.reg_
                .fail_kind_()
                .unwrap_or(ConnCloseReason::Local),
            now_millis,
        );

        // 进入「等待关闭」，并唤醒推进引擎（计时循环）。
        //
        // 只有**首次**进入才需要唤醒：如果这条连接早就被 `close_async` 置过状态，
        // 引擎已经在转了，重复唤醒没有意义（`notify_timer_` 本身幂等，这里只是
        // 省一次自旋锁）。
        if self.reg_.enter_waiting_close_(now_millis) {
            #[cfg(feature = "test-loop-probe")]
            crate::connection::loop_probe_::note_core_entered_close_();
            // 走本核心的统一入口（raise `HINT_MAIN_TIMER`），而不是 registry 的
            // `notify_timer_`——R9 起 timer 只做计时，不再扫描注册表。
            self.notify_timer_();
        }

        // 关闭连接级临时对象域：拒绝后续放入，并尝试清盘本域对象。`try_flush_` 用
        // `try_write`（非阻塞），因此本函数仍满足「不取锁、不阻塞」的硬纪律；仍有
        // `Owning` / `Sharing` 视图的对象会跳过，等视图释放后重试。
        //
        // 放在最后：状态推进与指标上报必须先完成，清盘只负责归还临时内存。
        close_conn_scope_(self.reg_.scope_());
    }
}
