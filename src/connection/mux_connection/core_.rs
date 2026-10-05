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
//! （`mm_ptr::Shared<MuxCore<C, S>, C::Alloc>`）：句柄与两个循环各自持有一份，
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
//! # 作用域保活槽
//!
//! [`MuxCore::scope_`] 存一份调用方注入的本地作用域克隆（`abs_art::TrLocalScope`
//! 的实现值）。它**不参与** spawn 之后的任何调用，只保证「连接活着，本地队列就
//! 活着」——否则调用方一旦先丢弃作用域值，两个循环会被连带销毁，症状是连接
//! 静默失去响应。
//!
//! 注意队列**是否被驱动**仍然由调用方负责（tokio 需要 `scope.run_until(..)`、
//! compio 由运行时驱动）；本槽位只保证队列不被提前回收。两个循环也是经同一个
//! 字段投递的（[`MuxCore::scope_`]），因此「谁提供队列」在核心上只有一处。

use buffex::x_deps::abs_cancel::TrCancellationToken;

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
};

/// 复用连接的**演员核心**：连接的全部共享状态与全部资源句柄。
///
/// 泛型参数：
///
/// - `C`：资源策略，见 [`TrConnCfg`](crate::connection::TrConnCfg)；
/// - `S`：调用方注入的本地作用域类型（`abs_art::TrLocalScope` 的实现值）。
///   本类型只**保存**它，不要求它在类型上实现任何 trait；`spawn_local` 所需的
///   bound 只出现在 [`MuxConnection::new`](super::MuxConnection::new) 上。
///
/// # 生命周期
///
/// 最后一个应用面强引用消失时本类型析构，`Drop` 触发两个循环的取消令牌
/// （`ChannelRegistry_::cancel_loops_`），循环随即在下一个 await 点自行退出。
pub(crate) struct MuxCore<C, S>
where
    C: TrConnCfg,
{
    /// 资源策略：建子流环与判定窗口时在 API 面就地取用。
    config_: C,

    /// 握手协商结果（连接级配额）。
    opts_: HandshakeOpts,

    /// 本地作用域：既用于建连时 `spawn_local`，也作为队列的保活槽。
    scope_: S,

    /// dock / 子流身份索引、失败标志与两个循环的取消令牌。
    reg_: ChannelRegistry_<C::Alloc>,

    /// 写事件发送端（控制帧、建流注册、水位与拆流通知）。
    w_events_: EventSender_<WriteEvent_<C::Buff, C::Alloc>>,

    /// 读事件发送端（接收环注册与释放）。
    r_events_: EventSender_<ReadEvent_<C::Buff, C::Alloc>>,

    /// 四个循环的取消令牌（`0` = 读泵、`1` = 解复用、`2` = 复用、`3` = 写泵）。
    ///
    /// 核心自己持一份克隆，**而不是在 `Drop` 里去注册表取**：注册表取锁在跨线程
    /// 争用时是阻塞等待，而 `Drop` 必须不阻塞。令牌是可克隆的共享句柄，因此这里
    /// 持有的就是循环在用的那一个。
    loops_: [CancelToken_<C::Alloc>; 4],
}

impl<C, S> MuxCore<C, S>
where
    C: TrConnCfg,
{
    /// 由建连路径展开后的全部量构造（成员私有，构造只能走这里）。
    pub(crate) fn new_(
        config: C,
        opts: HandshakeOpts,
        scope: S,
        reg: ChannelRegistry_<C::Alloc>,
        w_events: EventSender_<WriteEvent_<C::Buff, C::Alloc>>,
        r_events: EventSender_<ReadEvent_<C::Buff, C::Alloc>>,
        loops: [CancelToken_<C::Alloc>; 4],
    ) -> Self {
        MuxCore {
            config_: config,
            opts_: opts,
            scope_: scope,
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

    /// 本地作用域：建连时经它投递两个循环（队列的保活也由本字段承担）。
    pub(crate) fn scope_(&self) -> &S {
        &self.scope_
    }

    /// dock / 子流身份索引与失败标志。
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

    /// 第 `idx` 个循环的取消令牌（`0` = 读循环，`1` = 写循环）。
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
impl<C, S> MuxCore<C, S>
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
        self.reg_.drain_session_events_(cancel).await
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
            .reserve_channel_(local_dock, remote_dock, cancel)
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
            .release_channel_(local_dock, remote_dock, cancel)
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

impl<C, S> Drop for MuxCore<C, S>
where
    C: TrConnCfg,
{
    /// 连接收尾：触发四个循环的取消令牌。
    ///
    /// 循环在每个 await 点检查令牌并自行退出（泵与解复用循环的 park 经
    /// `race_cancel_`、复用循环的 park 与取消 future 竞争），因此这里是「丢弃
    /// 连接即关闭连接」的唯一入口，不依赖句柄的 `abort` / `drop` 语义。
    ///
    /// 令牌就在核心自己的字段里，因此本 `Drop` **不取注册表锁、不阻塞**——这一点
    /// 是跨线程收尾的前提：最后一个句柄可能在任意线程上被丢弃。
    fn drop(&mut self) {
        for token in &self.loops_ {
            token.cancel_();
        }
    }
}
