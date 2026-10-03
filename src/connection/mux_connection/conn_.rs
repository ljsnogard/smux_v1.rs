use core::marker::PhantomData;

use abs_buff::{
    TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_art::{TrJoinHandle, TrLocalScope};
use abs_cancel::TrCancellationToken;
use abs_smux::{conn::TrConnection, dock::TrDock};
use buffex::x_deps::abs_buff;
use mm_ptr::{Owned, Shared};

use crate::{
    connection::{
        BindError, Dock, TrMuxConfig,
        dock_binding::DockBinding,
        session_::{LoopShared_, read_loop_async_, write_loop_async_},
        signal_::{EventReceiver_, ReadEvent_, WriteEvent_, event_channel_},
    },
    handshake::{agent::HandshakeDelivery, opts::HandshakeOpts},
};

use super::{core_::MuxCore, registry_::ChannelRegistry_};

/// 复用连接：**对一个 `MuxCore` 的智能指针的薄封装**，同时实现
/// [`TrConnection`]。
///
/// 泛型参数：
///
/// - `C`：资源策略，见 [`TrMuxConfig`]；
/// - `S`：调用方注入的本地作用域类型（`abs_art::TrLocalScope` 的实现值，
///   tokio 的 `LocalScope` / compio 的 `LocalScope`）；
/// - `R` / `W`：网络读 / 写半边的**类型**（传输本身）。它们在建连时被移进循环，
///   此后不参与任何行为，只作为「这条连接是在什么传输上建的」的**类型级事实**；
///   连接对外的错误类型由它们**派生**：[`TrConnection::Err`] 是
///   `MuxError<R, W>`。
///
/// # 为什么参数是 `R` / `W` 而不是它们的错误类型
///
/// 早先的形状把两个**错误类型**直接放在公开类型上（`MuxConnection<C, S, RE, WE>`）。
/// 那是个坏味道：错误是**次要信息**，产生错误的传输才是主要信息；而且错误类型常常
/// **不可命名**（可能是私有的、或只以 `<T as TrBuffTryRead<u8>>::Err` 这样的投影
/// 存在），于是调用方被迫在每一个类型别名里写投影——「连接是什么」反而由错误牵着走。
/// 现在主参数是传输，错误一律经 `R::Err` / `W::Err` **推断**得到。
///
/// 注意底层错误**值**只在循环那一侧存在，连接级失败经共享状态回传时只保留方向
/// （[`MuxError::Transport`](crate::connection::MuxError::Transport)），因此 API 面实际不会产出
/// `Rx` / `Tx` 两个带载荷变体。
/// [`MuxError`](crate::connection::MuxError) 自身的两个参数也**是传输**：它的载荷变体由
/// `R::Err` / `W::Err` 派生，
/// 因此开发者对错误写代码（含 `match`）时不需要命名载荷类型。
///
/// # 为什么是智能指针
///
/// `Clone` 一份就是多一个强引用（分配走调用方注入的分配器），因此它可以被任意
/// 分发：存进结构体、传进函数、跨层持有。会话侧的四个句柄
/// （[`DockBinding`] / [`ChannelListener`](crate::connection::ChannelListener) /
/// [`ChannelHandle`](crate::connection::ChannelHandle) /
/// [`Telegraph`](crate::connection::Telegraph)）与两个 channel 半部同样各自持有
/// 一份克隆，而不是借用上层对象——这是本架构的全部要点：**把「谁借用谁」换成
/// 「谁持有谁的一份指针」**。「绑定 → 监听 → 接流 → 业务」因此可以拆到不同函数、
/// 不同结构体里表达，不再被生命周期参数绑成一串。
///
/// # 生命周期
///
/// 最后一个应用面对象被丢弃时 `MuxCore` 析构，其 `Drop` 触发两个循环的取消令牌，
/// 连接随之关闭（循环不持有核心强引用，见
/// `MuxCore` 的模块文档）。
///
/// # 作用域
///
/// [`MuxConnection::new`] 会把 `scope` 的一份克隆存进核心保活，因此**队列**不会
/// 先于连接消失；但**驱动**队列仍然是调用方的责任：tokio 必须把整段使用期包在
/// `scope.run_until(..)` 里，compio 由运行时自己驱动，smol 由 `LocalExecutor` 驱动。
/// 忘记驱动不会有编译错误，症状是两个循环从不推进（连接静默无响应）。
pub struct MuxConnection<W, R, S, C>
where
    C: TrMuxConfig,
{
    /// 指向演员核心的强引用；最后一个强引用消失时核心析构（并触发收尾）。
    core_: Shared<MuxCore<C, S>, C::Alloc>,

    /// 两个传输类型只以类型形式参与 [`TrConnection::Err`]（`MuxError<R, W>`）
    /// ——收发半边本身已在 [`MuxConnection::new`] 里移进循环。用 `fn() -> (..)` 占位，
    /// 使它们不影响本类型的 auto trait（`Send` / `Sync` / `Unpin`）。
    _mark_: PhantomData<fn() -> (R, W)>,
}

impl<W, R, S, C> Clone for MuxConnection<W, R, S, C>
where
    C: TrMuxConfig,
{
    /// 克隆即多一个强引用：核心不复制，句柄因此可以与任意多个对象共享同一连接。
    fn clone(&self) -> Self {
        MuxConnection {
            core_: self.core_.clone(),
            _mark_: PhantomData,
        }
    }
}

impl<W, R, S, C> MuxConnection<W, R, S, C>
where
    C: TrMuxConfig,
{
    /// 由一次成功的握手交付物、调用方的本地作用域与资源策略构造连接：接管
    /// `Rx` / `Tx`，并**经作用域 `spawn_local`** 投递读 / 写两个循环。
    ///
    /// # 循环的投递与收尾
    ///
    /// 两个循环投递后**立即 `detach()`**：
    ///
    /// - `detach` 之后任务继续由作用域驱动（这是 `abs_art::TrLocalScope` 的实现
    ///   契约，三个后端一致），不需要调用方保存句柄；
    /// - 收尾**不靠句柄**：`abs_art::TrJoinHandle` 没有 `abort`，三个后端对
    ///   「drop 句柄」的语义也不一致。连接改用**取消令牌**收尾——最后一个应用面
    ///   对象被丢弃时 `MuxCore` 析构并触发令牌，循环在每个 await 点检查令牌
    ///   自行退出。
    ///
    /// # 作用域契约
    ///
    /// `scope` 的一份克隆被存进核心保活，但**队列必须由调用方驱动**（`new` 只
    /// 负责投递，不负责推进）：tokio 用 `scope.run_until(..)`、smol 用
    /// `LocalExecutor`、compio 由运行时自己驱动。三个后端的统一写法见
    /// `abs_art` 的 `TrLocalScope` 文档。
    ///
    /// # Panics
    ///
    /// 帧暂存缓冲按协商出的 `max_packet_size` 分配，分配失败即 panic（与标准库
    /// 容器一致）。
    ///
    /// 对外不提供任何驱动 API：用户只使用 `abs_smux` 的 trait。后端运行时由最终
    /// 二进制经 `abs_art` 选择（本 crate 不依赖 `abs_art-bridge`）。
    /// 这里的 `R` / `W` 就是类型参数本身（不是方法级泛型）：**传输类型决定
    /// `Self`**，而错误类型由它们派生，调用方不需要（也常常无法）命名错误类型。
    pub fn new(scope: &S, delivery: HandshakeDelivery<W, R>, config: C) -> Self
    where
        S: TrLocalScope + Clone,
        R: TrBuffRead<u8> + 'static,
        W: TrBuffWrite<u8> + 'static,
        C: 'static,
        C::Alloc: 'static,
    {
        let HandshakeDelivery { opts, tx, rx } = delivery;
        let bundle = CoreBundle_::build_(scope, opts, config);
        let CoreBundle_ {
            core_,
            shared_,
            w_receiver_,
            r_receiver_,
            alloc_,
        } = bundle;
        let max_packet_size = core_.opts_().basic_opts.max_packet_size;
        // 投递也走核心里的那一份作用域：调用方给出的值只用来克隆保活。
        let scope = core_.scope_();

        // 帧暂存一律走调用方注入的分配器（`mm_ptr::Owned`），不再落到全局分配器。
        let read_fut = read_loop_async_(
            rx,
            shared_.clone(),
            r_receiver_,
            core_.w_events_().clone(),
            Owned::new_slice(
                max_packet_size,
                |_idx, slot| {
                    slot.write(0u8);
                },
                alloc_.clone(),
            ),
            core_.reg_().loop_token_(0usize),
        );
        scope.spawn_local(read_fut).detach();

        let write_fut = write_loop_async_(
            tx,
            shared_,
            w_receiver_,
            core_.r_events_().clone(),
            core_.reg_().loop_token_(1usize),
        );
        scope.spawn_local(write_fut).detach();

        MuxConnection {
            core_,
            _mark_: PhantomData,
        }
    }

    /// 演员核心（crate 内部句柄与两个循环都经它访问共享状态）。
    pub(crate) fn core_(&self) -> &MuxCore<C, S> {
        &self.core_
    }
}

/// 建连的中间产物：核心 + 两个循环共享的一份量 + 两条事件接收端 + 分配器。
///
/// 抽出来是为了让「测试专用构造」（[`MuxConnection::new_test_`]）与正式建连走
/// **同一份**策略展开逻辑，不在测试里复制一遍。
struct CoreBundle_<C, S>
where
    C: TrMuxConfig,
{
    core_: Shared<MuxCore<C, S>, C::Alloc>,
    shared_: LoopShared_<C::Alloc>,
    w_receiver_: EventReceiver_<WriteEvent_<C::Buff, C::Alloc>>,
    r_receiver_: EventReceiver_<ReadEvent_<C::Buff, C::Alloc>>,
    alloc_: C::Alloc,
}

impl<C, S> CoreBundle_<C, S>
where
    C: TrMuxConfig,
{
    /// 展开策略、建注册表与两条事件通道，并把全部共享状态装进核心。
    ///
    /// 本函数**不 spawn 任何任务**：`Rx` / `Tx` 的移交由调用方（[`MuxConnection::new`]）
    /// 完成。
    fn build_(scope: &S, opts: HandshakeOpts, config: C) -> Self
    where
        S: Clone,
    {
        // 建连时把策略展开一次：此后循环只需要注册表与单帧上限，不再持有 `C`。
        //
        // 窗口与通告阈值**不在这里**：子流缓冲由调用方在建流最终裁决时给出，容量逐条
        // 子流不同，因此它们在 `accept_async` 里按该子流的接收缓冲容量算出。
        let max_packet_size = opts.basic_opts.max_packet_size;
        let alloc = config.allocator();

        let reg = ChannelRegistry_::new_(opts.basic_opts.clone(), alloc.clone());
        let (w_events, w_receiver) = event_channel_();
        let (r_events, r_receiver) = event_channel_();

        // 两个循环共享的那一份量：注册表句柄 + 单帧上限。**它不含核心引用**，
        // 这是核心能被析构（因而连接能被关闭）的前提，见 `core_` 模块文档。
        let shared = LoopShared_::new_(reg.clone(), max_packet_size);

        let core = Shared::new(
            MuxCore::new_(
                config,
                opts,
                scope.clone(),
                reg,
                w_events,
                r_events,
            ),
            alloc.clone(),
        );

        CoreBundle_ {
            core_: core,
            shared_: shared,
            w_receiver_: w_receiver,
            r_receiver_: r_receiver,
            alloc_: alloc,
        }
    }
}

#[cfg(test)]
impl<W, R, S, C> MuxConnection<W, R, S, C>
where
    C: TrMuxConfig,
{
    /// 测试专用构造：只建核心与两条事件通道，**不 spawn 任何循环**。
    ///
    /// 因此它既不需要传输，也不需要被驱动的本地作用域——用于直接检查句柄 / 半部的
    /// 本地行为（环的关闭态、dock 上报、非阻塞转发）。两条事件通道的接收端随本
    /// 函数返回即被丢弃，因此半部发出的通知按「没有消费者」处理（投递返回 `false`），
    /// 与生产环境的行为差异仅此一处。
    pub(crate) fn new_test_(scope: &S, opts: HandshakeOpts, config: C) -> Self
    where
        S: Clone,
    {
        MuxConnection {
            core_: CoreBundle_::build_(scope, opts, config).core_,
            _mark_: PhantomData,
        }
    }
}

impl<W, R, S, C> TrConnection for MuxConnection<W, R, S, C>
where
    C: TrMuxConfig,
    R: TrBuffRead<u8> + 'static,
    W: TrBuffWrite<u8> + 'static,
{
    type Data = u8;
    type Dock = Dock;
    /// 载荷类型由两个**传输**类型派生（`TrTaggedError: core::error::Error`，因此
    /// `MuxError<R, W>` 自动满足 `Err: Error`）。
    type Err = BindError<R, W>;

    /// 会话对象是**独立持有者**（自带一份连接克隆），不带 `'f` 之类的生命周期：
    /// 它可以从函数返回、可以存进结构体、可以与连接同处一个结构体。
    type DockBinding = DockBinding<W, R, S, C>;

    /// future 仍然借用 `&self`（调用期间），但**输出是 owned 的**。
    type BindAsync<'f> = MuxBindAsync<'f, 'f, C, S, R, W>
    where
        Self: 'f;

    fn bind_async<'f>(&'f self, local_dock: Self::Dock) -> Self::BindAsync<'f> {
        MuxBindAsync::new(self, local_dock)
    }
}

/// [`TrConnection::bind_async`] 的 step 函数。
#[gen_may_cancel_future(MuxBind, pub, new(pub(crate)))]
async fn mux_bind_async_<'f, C, S, R, W, K>(
    conn: &'f MuxConnection<W, R, S, C>,
    local_dock: Dock,
    _cancel: K,
) -> Result<DockBinding<W, R, S, C>, BindError<R, W>>
where
    C: TrMuxConfig + 'f,
    S: 'f,
    R: TrBuffTryRead<u8> + 'f + 'static,
    W: TrBuffTryWrite<u8> + 'f + 'static,
    K: TrCancellationToken,
{
    if local_dock.is_special() {
        return Result::Err(BindError::ReservedDock);
    }
    // 独占绑定：同一 local_dock 在任意时刻至多一个 `DockBinding`（见
    // `ChannelRegistry_::bind_dock_` 与 `DockBinding` 的「绑定的独占性」）。
    conn.core_()
        .reg_()
        // `bind_dock_` 只可能报「该 dock 已被占用」。
        .bind_dock_(local_dock)
        .map_err(|_| BindError::DockInUse)?;
    Result::Ok(DockBinding::new_(conn.clone(), local_dock))
}
