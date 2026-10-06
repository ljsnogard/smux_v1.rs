use abs_art::{TrJoinHandle, TrLocalScope};
use abs_buff::gen_may_cancel_future;
use abs_cancel::TrCancellationToken;
use abs_smux::{conn::TrConnection, dock::TrDock};
use buffex::x_deps::{abs_buff, abs_cancel};
use mm_ptr::Shared;

use crate::{
    connection::{
        Dock, MuxError, ScopeHost, TrConnCfg,
        dock_binding::DockBinding,
        ring_::StageRingPair_,
        session_::{ByteLoopShared_, MuxShared_, demux_loop_async_, mux_loop_async_},
        session_pump_::{rx_pump_loop_async_, tx_pump_loop_async_},
        signal_::{EventReceiver_, ReadEvent_, WriteEvent_, event_channel_},
        timer_::timer_loop_async_,
    },
    handshake::{agent::HandshakeDelivery, opts::HandshakeOpts},
    time::{ConnClock_, millis_of_},
};

use super::{core_::MuxCore, registry_::{ChannelRegistry_, ReserveErr_}};


/// [`TrConnection`](abs_smux::conn::TrConnection) 的错误类型：目前只有 `bind_async`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BindError {
    /// 要绑定的 dock 是协议保留值（`unspecified` / `wildcard`），不能当身份用。
    #[error("要绑定的 dock 是协议保留值，不能作为身份")]
    ReservedDock,

    /// 该 local_dock 已被占用。
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
/// 泛型参数：
///
/// - `C`：资源策略，见 [`TrConnCfg`]；
/// - `R`：**运行时值**（`abs_art::TrTime` 的实现值，例如
///   `abs_art_tokio::Runtime<{ FULL }>`）。它提供「现在几点」与「怎么等」，
///   二者同源——见 [`crate::time`] 模块文档。
///
/// 本地作用域**不**出现在本类型的参数上：它只在 [`MuxConnection::new`] 的
/// 方法级泛型里出现，投递完五个循环后由循环各自持有（见
/// [`core_`](super::core_) 模块文档「队列保活」）。
///
/// 收发半边类型由 `C::ConnTx` / `C::ConnRx` 给出，不再出现在公开类型上。
pub struct MuxConnection<C>
where
    C: TrConnCfg,
{
    /// 指向演员核心的强引用；最后一个强引用消失时核心析构（并触发收尾）。
    core_: Shared<MuxCore<C>, C::Alloc>,
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
    /// `Runtime::local_scope()`），接管 `C::ConnRx` / `C::ConnTx`，把两块缓冲建为
    /// 连接级的两条帧暂存环，并**经作用域 `spawn_local`** 投递五个循环。
    ///
    /// # 五个循环
    ///
    /// | 令牌序号 | 循环 | 搬运方向 |
    /// | --- | --- | --- |
    /// | 0 | 读泵 | `C::ConnRx` → 连接读环 |
    /// | 1 | 解复用 | 连接读环 → 各子流接收环（解析帧、拉取读事件） |
    /// | 2 | 复用 | 各子流发送环 → 连接写环（成帧、拉取写事件） |
    /// | 3 | 写泵 | 连接写环 → `C::ConnTx` |
    /// | 4 | 计时 | 不搬字节：保活 `PULSE` 与空闲超时拆流（见私有模块 `timer_`） |
    ///
    /// 内侧两个循环（1 / 2）从事件队列拉取 `Attach` / `TxClosed` / `Control` 等事件，
    /// 据此决定哪些子流此刻可搬；计时循环（4）在前两者都静默时仍然会醒来，把
    /// 保活与拆流的需求投进同两条事件队列。
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
    ///   泛型** `S` 出现在本函数上，投递完五个循环后由循环各自持有一份克隆来保活队列
    ///   （见 [`core_`](super::core_) 模块文档）。
    ///
    /// 需要显式控制两者时走 [`MuxConnection::new_with_rt`]；只想换运行时值而不动
    /// 调用形状时，自写一个 `C` 并让它的 [`TrConnCfg::runtime`] 返回别的值即可。
    ///
    /// # Panics
    ///
    /// 传入的连接级缓冲容量不在 `buffex::ring` 允许区间内时 panic。容量由
    /// [`TrConnCfg::StageBuff`] 的提供者与调用的构造路径决定，属**配置错误**，
    /// 与子流环走 `Err` 的处理方式不同：那一条是运行期按调用方给的 buff 建，
    /// 这一条在建连前就定死了。
    pub fn new(
        delivery: HandshakeDelivery<C::ConnTx, C::ConnRx>,
        config: C,
        read_stage_buff: C::StageBuff,
        write_stage_buff: C::StageBuff,
    ) -> Self
    where
        C: 'static + Clone,
        C::Alloc: 'static,
        C::StageBuff: Send + Sync,
        C::Rt: ScopeHost,
    {
        let rt = config.runtime();
        let scope = ScopeHost::local_scope(&rt);
        Self::new_with_rt_(&rt, &scope, delivery, config, read_stage_buff, write_stage_buff)
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
    ///
    /// # Panics
    ///
    /// 同 [`MuxConnection::new`]：连接级缓冲容量非法时 panic。
    pub fn new_with_rt<S>(
        rt: &C::Rt,
        scope: &S,
        delivery: HandshakeDelivery<C::ConnTx, C::ConnRx>,
        config: C,
        read_stage_buff: C::StageBuff,
        write_stage_buff: C::StageBuff,
    ) -> Self
    where
        C: 'static + Clone,
        C::Alloc: 'static,
        C::StageBuff: Send + Sync,
        S: TrLocalScope + Clone + 'static,
    {
        Self::new_with_rt_(rt, scope, delivery, config, read_stage_buff, write_stage_buff)
    }

    /// 两个公开构造入口的共同实现体。
    fn new_with_rt_<S>(
        rt: &C::Rt,
        scope: &S,
        delivery: HandshakeDelivery<C::ConnTx, C::ConnRx>,
        config: C,
        read_stage_buff: C::StageBuff,
        write_stage_buff: C::StageBuff,
    ) -> Self
    where
        C: 'static + Clone,
        C::Alloc: 'static,
        C::StageBuff: Send + Sync,
        S: TrLocalScope + Clone + 'static,
    {
        let HandshakeDelivery { opts, tx, rx } = delivery;
        let bundle = CoreBundle_::build_(rt, opts, config.clone());
        let CoreBundle_ {
            core_,
            shared_,
            byte_shared_,
            w_receiver_,
            r_receiver_,
        } = bundle;

        // 两块连接级缓冲 ⇒ 两条环的四个半部；「外侧」（贴传输）与「内侧」（贴子流）
        // 各拿一份（见 `ring_::StageRingPair_`）。
        let read_stage_cap = ring_capacity_(&read_stage_buff);
        let write_stage_cap = ring_capacity_(&write_stage_buff);
        let stage = StageRingPair_::from_buffs_(
            read_stage_buff,
            write_stage_buff,
            config.allocator(),
        )
        .unwrap_or_else(|bad_cap| {
            panic!(
                "连接级帧暂存环容量非法：读环 {read_stage_cap}、写环 {write_stage_cap}，\
                 被拒的是 {bad_cap}；容量须落在 buffex::ring 允许区间内，\
                 该容量源自 TrConnCfg::StageBuff 的构造，属配置错误"
            )
        });
        let ((rx_stage_w_, tx_stage_r_), (rx_stage_r_, tx_stage_w_)) = stage.into_halves_();

        // 投递五个循环。每一份 future 都**自带作用域的一份克隆**（`keep_queue_alive_`）
        // ——核心不再持有作用域，队列的存活改由循环自己兑现（见 `core_` 模块文档
        // 「队列保活」）。
        let read_pump_fut = keep_queue_alive_(
            scope.clone(),
            rx_pump_loop_async_::<C, _>(
                rx,
                byte_shared_.clone(),
                rx_stage_w_,
                core_.loop_token_(0usize),
            ),
        );
        scope.spawn_local(read_pump_fut).detach();

        let demux_fut = keep_queue_alive_(
            scope.clone(),
            demux_loop_async_::<C, _>(
                rx_stage_r_,
                shared_.clone(),
                r_receiver_,
                core_.w_events_().clone(),
                core_.loop_token_(1usize),
            ),
        );
        scope.spawn_local(demux_fut).detach();

        let mux_fut = keep_queue_alive_(
            scope.clone(),
            mux_loop_async_::<C, _>(
                tx_stage_w_,
                shared_.clone(),
                w_receiver_,
                core_.r_events_().clone(),
                core_.loop_token_(2usize),
            ),
        );
        scope.spawn_local(mux_fut).detach();

        let write_pump_fut = keep_queue_alive_(
            scope.clone(),
            tx_pump_loop_async_::<C, _>(
                tx,
                byte_shared_,
                tx_stage_r_,
                core_.loop_token_(3usize),
            ),
        );
        scope.spawn_local(write_pump_fut).detach();

        // 第五个循环：保活（PULSE）与空闲超时拆流。它不搬字节，只按连接级时钟算
        // 期限、把保活动作投进上面两条事件通道（见 `timer_` 模块文档）。
        let timer_fut = keep_queue_alive_(
            scope.clone(),
            timer_loop_async_::<C, _>(
                shared_,
                core_.w_events_().clone(),
                core_.r_events_().clone(),
                core_.loop_token_(4usize),
            ),
        );
        scope.spawn_local(timer_fut).detach();

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
/// 形成的 `队列 → 任务 → 队列` 引用环由取消令牌打破：核心析构触发令牌，五个循环
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

/// 取一块连接级缓冲的容量（`MaybeUninit<u8>` 的个数）。
fn ring_capacity_<B>(buff: &B) -> usize
where
    B: core::borrow::BorrowMut<[core::mem::MaybeUninit<u8>]>,
{
    core::borrow::Borrow::<[core::mem::MaybeUninit<u8>]>::borrow(buff).len()
}

impl<C> MuxConnection<C>
where
    C: TrConnCfg,
{
    /// 演员核心（crate 内部句柄与两个循环都经它访问共享状态）。
    pub(crate) fn core_(&self) -> &MuxCore<C> {
        &self.core_
    }
}

/// 建连的中间产物：核心 + 内侧 / 外侧两套循环共享量 + 两条事件接收端。
struct CoreBundle_<C>
where
    C: TrConnCfg,
{
    core_: Shared<MuxCore<C>, C::Alloc>,
    shared_: MuxShared_<C>,
    byte_shared_: ByteLoopShared_<C::Alloc>,
    w_receiver_: EventReceiver_<WriteEvent_<C::Buff, C::Alloc>>,
    r_receiver_: EventReceiver_<ReadEvent_<C::Buff, C::Alloc>>,
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
        let alloc = config.allocator();
        // 建连这一刻就是连接的 epoch；此后协议里的时间量一律是「自它起算的毫秒」。
        let conn_clock = ConnClock_::new_(rt.clone());

        let reg = ChannelRegistry_::new_(opts.basic_opts.clone(), alloc.clone());
        let (w_events, w_receiver) = event_channel_();
        let (r_events, r_receiver) = event_channel_();

        let shared = MuxShared_::<C>::new_(
            reg.clone(),
            max_packet_size,
            conn_clock.clone(),
            channel_timeout_millis,
        );
        let byte_shared = ByteLoopShared_::new_(reg.clone());

        // 核心自持一份五个循环的取消令牌：`MuxCore::drop` 因此不必去注册表取锁
        // （那会阻塞，而 `Drop` 可能在任意线程上发生）。
        let loops = [
            reg.loop_token_(0usize),
            reg.loop_token_(1usize),
            reg.loop_token_(2usize),
            reg.loop_token_(3usize),
            reg.loop_token_(4usize),
        ];

        let core = Shared::new(
            MuxCore::new_(
                config,
                opts,
                reg,
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
            byte_shared_: byte_shared,
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
    pub(crate) fn new_test_(opts: HandshakeOpts, config: C) -> Self
    {
        let rt = config.runtime();
        MuxConnection {
            core_: CoreBundle_::build_(&rt, opts, config).core_,
        }
    }
}

impl<C> TrConnection<C> for MuxConnection<C>
where
    C: TrConnCfg,
{
    type Err = BindError;

    type DockBinding = DockBinding<C>;

    type BindAsync<'f> = MuxBindAsync<'f, 'f, C>
    where
        Self: 'f;

    fn bind_async<'f>(&'f self, local_dock: C::Dock) -> Self::BindAsync<'f> {
        MuxBindAsync::new(self, local_dock)
    }
}

/// [`TrConnection::bind_async`] 的 step 函数。
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
    if local_dock.is_special() {
        return Result::Err(BindError::ReservedDock);
    }
    // 先清空释放邮箱：上一个 `DockBinding` 可能刚被丢弃（`Drop` 只投消息），
    // 不先落实就会把「已解绑」误判成 `DockInUse`——这是「丢弃后立刻重绑」这条
    // 既有约定的确定性来源。
    conn.core_()
        .drain_session_events_(cancel.child_token())
        .await
        .map_err(|_| BindError::Cancelled)?;
    // 独占绑定：同一 local_dock 在任意时刻至多一个 `DockBinding`。
    conn.core_()
        .bind_dock_(local_dock, cancel.child_token())
        .await
        .map_err(|err| match err {
            ReserveErr_::Cancelled => BindError::Cancelled,
            // `bind_dock_` 只有这两种失败：被占用，或等锁被取消。
            _ => BindError::DockInUse,
        })?;
    Result::Ok(DockBinding::new_(conn.clone(), local_dock))
}
