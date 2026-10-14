//! [`SmuxListener`]：把「接受连接 → 完成 smux 握手 → 交出成形连接」自动化。
//!
//! # 交付模型
//!
//! 握手**并发进行**，完成的连接按**完成顺序**排进一条 FIFO 队列，
//! [`TrAsyncIterator::next_async`] 每次取出队首。于是：
//!
//! - 一条握手慢的连接不会挡住后面已经握完的连接；
//! - 调用方看到的顺序是「谁先握完谁先出」，而不是「谁先连上来谁先出」——后者要等到
//!   前面所有连接都处理完，会把并发带来的收益全部退回。
//!
//! # 谁在驱动
//!
//! 接受循环**不**在构造函数里启动，而是由 `next_async` 在需要时按需补充。这条选择让
//! listener 保持「惰性」：
//!
//! - 调用方不拉取，就没有人在接受连接，内核的 accept 队列替本端挡着；
//! - 调用方拉取得越勤，同时在途的握手越多，直到
//!   [`TrListenerConfig::max_inflight`] 的上限——它同时就是**背压**。
//!
//! # 槽位、在途与背压的关系
//!
//! 一个**接受槽位**是一条常驻任务：它反复「接受一条连接 → 把握手交给新任务」，自己
//! 立刻回去接受下一条。因此一个槽位同一时刻最多推动**一条**握手任务，于是「在途条数
//! 上限」就等于「槽位数上限」，两者是同一个数（[`TrListenerConfig::max_inflight`]），
//! 不需要额外的预算表。
//!
//! # 单条连接的失败不会打扰调用方
//!
//! 不符合握手条件的连入（不是 smux 帧、CRC 不匹配、协商被拒、对端半途关闭……）一律
//! **静默丢弃**，接受槽位继续。只有两类事会以 `Err` 交给调用方：本次等待被取消
//! （[`SmuxListenerError::Cancelled`]），以及从监听套接字接受失败
//! （[`SmuxListenerError::Accept`]）——后者说明 listener 自己已经产不出连接了。

use core::{
    marker::PhantomData,
    sync::atomic::{AtomicBool, Ordering},
};
use std::sync::Mutex;

use abs_async_iter::TrAsyncIterator;
use alloc::sync::Arc;
use smux_v1::{
    connection::{MuxConnection, TrConnCfg},
    handshake::agent::HandshakeAgent,
    x_deps::{
        abs_art::{TrJoinHandle, TrLocalScope},
        abs_buff::{TrBuffRead, TrBuffWrite},
    },
};

use crate::{
    listening::wait_::NextWait_,
    config::{
        TrInbound, TrListenerConfig, TrNegotiatorFactory, into_halves_async_,
    },
    SmuxListenerError,
    x_deps::abs_cancel::{
        NonCancellableToken, TrCancellationToken, TrMayCancel,
    },
};

/// FIFO 队列的元素：一条**已成形**的连接，或一次致命失败。
///
/// 失败也走队列而不是旁路：接受槽位可能在没有任何 `next_async` 在等待的时刻失败，
/// 旁路会把那次失败丢掉。
enum QueueMsg_<T, AE> {
    /// 握手完成、连接已建好。
    Connection(T),

    /// 接受失败（致命）。
    AcceptFailed(AE),

    /// listener 已被关闭，不会再有连接进来。
    Stopped,
}

/// [`SmuxListener`] 的共享状态：只放**槽位之间真正共享**的东西。
///
/// 配置、入向适配、协商策略工厂都不在这里——它们按值克隆进每个槽位。这里只留队列与
/// 计数，于是本结构体与「用哪个运行时、哪种协商策略」都无关。
/// 一个**已分离（detached）且仍在运行**的任务。
///
/// # 为什么必须 detach
///
/// `TrLocalScope::spawn_local` 交出的句柄一旦被丢弃，任务**可能随之被取消**：
/// tokio 的 `JoinHandle` drop 即 detach（任务继续跑），而 compio 与 smol 的句柄
/// drop 会**取消**任务（实测：compio 下投递后立刻 `drop` 句柄，任务体一次都没跑）。
/// listener 里所有 `spawn_local` 出来的任务（接受槽位、每条连接的握手任务）都必须活到
/// 自身跑完，因此一律调用 [`TrJoinHandle::detach`] 显式分离。
///
/// # 那怎么知道还有几个任务活着
///
/// 句柄被 `detach` 消费掉之后没法再 `join`，所以「存活」改由本结构体的 `done_` 标记
/// 承载：任务体跑完（无论正常结束还是被取消）时把它置位。[`Live_::retain_done_`]
/// 据此清理，`Vec` 的长度于是就是存活任务数——那正是
/// [`TrListenerConfig::max_inflight`] 要限的东西。
///
/// [`TrJoinHandle::detach`]: smux_v1::x_deps::abs_art::TrJoinHandle::detach
struct Live_ {
    /// 任务是否已经跑完；由任务体自己置位。
    ///
    /// `detach` 之后句柄不可再 `join`，因此「还活着吗」只能靠这个标记回答。
    done_: Arc<AtomicBool>,
}

impl Live_ {
    /// 投递一条任务并立刻分离它，返回可放进句柄表的记录。
    ///
    /// `body` 是任务体；它跑完时自动把 `done_` 置位（`Drop` 保证 panic 也算跑完）。
    fn spawn_<S, F>(scope: &S, body: F) -> Self
    where
        S: TrLocalScope,
        F: core::future::Future<Output = ()> + 'static,
    {
        let done = Arc::new(AtomicBool::new(false));
        let flag = done.clone();
        let handle = scope.spawn_local(async move {
            // 用 guard 而不是在末尾赋值：任务 panic 时也要把标记置上，否则这个槽位会
            // 永远算作「存活」，`max_inflight` 的额度再也放不出来。
            let _guard = DoneGuard_(flag);
            body.await;
        });
        // 显式分离：compio / smol 的句柄 drop 会**取消**任务，tokio 的 drop 才等价于
        // detach。统一走 `detach` 才有跨后端一致的语义。
        TrJoinHandle::detach(handle);
        Live_ { done_: done }
    }

    /// 清掉已完成的记录；返回剩下的存活数。
    fn retain_done_(live: &mut Vec<Live_>) -> usize {
        live.retain(|item| !item.done_.load(Ordering::SeqCst));
        live.len()
    }
}

/// 任务退出时置位「已完成」标记的守卫。
struct DoneGuard_(Arc<AtomicBool>);

impl Drop for DoneGuard_ {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct Shared_<Cfg, A>
where
    Cfg: TrListenerConfig,
    A: TrInbound,
{
    /// 已完成连接的投递队列（**发送端**）：每个槽位克隆一份，把握完的连接投进来。
    queue_: flume::Sender<QueueMsg_<MuxConnection<Cfg::Conn>, A::Error>>,

    /// 已完成连接的投递队列（**接收端**）：只有 `next_async` 持有，它因此也是
    /// 「队列是否还活着」的唯一裁判。
    inbox_: flume::Receiver<QueueMsg_<MuxConnection<Cfg::Conn>, A::Error>>,

    /// 当前存活任务（接受槽位与在途握手）的句柄表。
    ///
    /// 它同时承担两件事：**保命**（句柄被丢弃会取消任务，见 [`ErasedHandle_`]）与
    /// **计数**（`len()` 就是存活数）。句柄在任务结束时不会自动从这里消失，因此
    /// `next_async` 每次醒来会顺手清理一遍已经完成的那些。
    live_: Arc<Mutex<Vec<Live_>>>,

    /// 在途握手条数上限（已归一化：至少 1）。
    max_inflight_: usize,

    /// 类型占位：`queue_` 的元素类型里已经用到了 `Cfg` / `A`，但编译器不会因此推断
    /// 「本结构体是按它们参数化的」，因此显式声明一次。
    _mark_: PhantomData<fn() -> (A,)>,
}

/// 一个**接受槽位**的全部输入。
///
/// 它整体按值搬进 `spawn_local` 的 future，因此每个字段都必须拥有其值——这是
/// 「槽位要能在 `next_async` 返回之后独立存活」的直接结果。
///
/// 适配器与协商策略都以 [`Arc`] 共享（不要求它们 `Clone`）：监听套接字本来就该被所有
/// 槽位共用同一个实例，而协商策略工厂只需要在每条连接上 `make()` 出一个新协商器。
struct Slot_<Cfg, A, N, U, K>
where
    Cfg: TrListenerConfig,
    A: TrInbound,
    N: TrNegotiatorFactory,
    U: TrLocalScope + 'static,
    K: TrCancellationToken,
{
    /// 入向适配（含监听套接字）。
    inbound_: Arc<A>,

    /// 协商策略工厂。
    neg_: Arc<N>,

    /// 本地作用域：建连的五个循环要投到这里。
    scope_: U,

    /// 已完成连接的投递队列。
    queue_: flume::Sender<QueueMsg_<MuxConnection<Cfg::Conn>, A::Error>>,

    /// 本 listener 的关闭信号。
    shutdown_: K,
}

/// 跨运行时、跨传输的 smux v1 连接监听器。
///
/// 它把「底层监听套接字 + 预存的握手条件」变成一条**已握手连接的异步迭代器**：每次
/// [`next_async`](TrAsyncIterator::next_async) 交出队首那条已经握手完成、可以直接使用
/// 的 [`MuxConnection`]。
///
/// # 类型参数
///
/// | 参数 | 含义 |
/// | --- | --- |
/// | `Cfg` | 连接装配策略，见 [`TrListenerConfig`] |
/// | `A` | 入向适配，见 [`TrInbound`]（如 [`runtime::TcpTransport`](crate::runtime::TcpTransport)） |
/// | `N` | 协商策略工厂，见 [`TrNegotiatorFactory`] |
/// | `U` | 本地作用域：运行时值交出的那个（`Runtime::local_scope()`） |
/// | `K` | 关闭令牌类型，缺省 [`NonCancellableToken`]（永不关闭） |
///
/// # 取消
///
/// `K` 是 **listener 的关闭令牌**：取消它之后所有槽位与在途握手都会退出，正在
/// [`next_async`](TrAsyncIterator::next_async) 上等待的调用方立刻拿到
/// [`SmuxListenerError::Cancelled`]。已经排进队列的连接不受影响——下一次调用仍能取到。
///
/// `next_async` **自身**被取消只中止**这一次**等待，listener 仍然可用。
///
/// # Examples
///
/// ```ignore
/// // `cfg` 回答「握手条件 + 连接策略 + 缓冲从哪来」，`neg` 回答「谁裁决对端的条件」。
/// let listener = SmuxListener::new_(TcpTransport, cfg, neg, scope);
/// while let Ok(conn) = listener.next_async().await {
///     // `conn` 已经是握手完成的 MuxConnection
/// }
/// ```
pub struct SmuxListener<Cfg, A, N, U, K = NonCancellableToken>
where
    Cfg: TrListenerConfig,
    A: TrInbound,
    N: TrNegotiatorFactory,
    U: TrLocalScope + Clone + 'static,
    K: TrCancellationToken,
{
    /// 入向适配（含监听套接字）：以 [`Arc`] 共享给所有接受槽位。
    inbound_: Arc<A>,

    /// 本次握手预存的本端条件与连接装配策略。
    cfg_: Cfg,

    /// 协商策略工厂：以 [`Arc`] 共享给所有接受槽位。
    neg_: Arc<N>,

    /// 本地作用域：接受槽位与建连的五个循环都投到这里。
    scope_: U,

    /// 共享状态。
    shared_: Arc<Shared_<Cfg, A>>,

    /// 本 listener 的关闭信号。
    shutdown_: K,
}

impl<Cfg, A, N, U, K> SmuxListener<Cfg, A, N, U, K>
where
    Cfg: TrListenerConfig,
    A: TrInbound,
    N: TrNegotiatorFactory,
    U: TrLocalScope + Clone + 'static,
    K: TrCancellationToken,
{
    /// 用一个**关闭令牌**构造 listener。
    ///
    /// 令牌被取消后：在途的接受与握手全部退出，等待中的
    /// [`next_async`](TrAsyncIterator::next_async) 立刻返回
    /// [`SmuxListenerError::Cancelled`]。语义详见类型文档。
    pub fn new_with_cancel_(
        inbound: A,
        cfg: Cfg,
        neg: N,
        scope: U,
        shutdown: K,
    ) -> Self {
        // 上限归一化：0 与 1 等价。若允许 0，就一个槽位都不跑，listener 永远接不到连接。
        let max_inflight = cfg.max_inflight().max(1usize);
        let (tx, rx) = flume::unbounded();
        SmuxListener {
            inbound_: Arc::new(inbound),
            cfg_: cfg,
            neg_: Arc::new(neg),
            scope_: scope,
            shared_: Arc::new(Shared_ {
                queue_: tx,
                inbox_: rx,
                live_: Arc::new(Mutex::new(Vec::new())),
                max_inflight_: max_inflight,
                _mark_: PhantomData,
            }),
            shutdown_: shutdown,
        }
    }

    /// 取在途握手条数上限（已归一化为至少 1）。
    pub fn max_inflight(&self) -> usize {
        self.shared_.max_inflight_
    }

    /// 取本 listener 的关闭令牌（克隆）。
    ///
    /// 可用于从别处主动关闭：取消之后所有槽位退出，`next_async` 返回
    /// [`SmuxListenerError::Cancelled`]。
    pub fn shutdown_token(&self) -> K
    where
        K: Clone,
    {
        self.shutdown_.clone()
    }

    /// 当前存活的接受槽位数。
    ///
    /// 主要用于诊断：为 0 且队列为空、关闭令牌又没触发，说明槽位刚刚退出而
    /// `next_async` 还没来得及补，属正常瞬时状态。
    pub fn active_slots_(&self) -> usize {
        self.shared_
            .live_
            .lock()
            .map(|live| live.len())
            .unwrap_or(0usize)
    }
}

impl<Cfg, A, N, U> SmuxListener<Cfg, A, N, U, NonCancellableToken>
where
    Cfg: TrListenerConfig,
    A: TrInbound,
    N: TrNegotiatorFactory,
    U: TrLocalScope + Clone + 'static,
{
    /// 构造一个**永不关闭**的 listener。
    ///
    /// 等价于把 [`SmuxListener::new_with_cancel_`] 的令牌取成
    /// [`NonCancellableToken`]。要能主动停，用那个入口传一个可取消的令牌。
    pub fn new_(inbound: A, cfg: Cfg, neg: N, scope: U) -> Self {
        Self::new_with_cancel_(inbound, cfg, neg, scope, NonCancellableToken::new())
    }
}

impl<Cfg, A, N, U, K> TrAsyncIterator for SmuxListener<Cfg, A, N, U, K>
where
    Cfg: TrListenerConfig + Clone + 'static,
    <Cfg::Conn as TrConnCfg>::ConnTx: TrBuffWrite<u8> + 'static,
    <Cfg::Conn as TrConnCfg>::ConnRx: TrBuffRead<u8> + 'static,
    A: TrInbound<ConnCfg = Cfg::Conn> + 'static,
    N: TrNegotiatorFactory + 'static,
    U: TrLocalScope + Clone + 'static,
    K: TrCancellationToken + Clone + 'static,
{
    type Item = MuxConnection<Cfg::Conn>;

    type Err = SmuxListenerError<A::Error>;

    type NextAsync<'f>
        = NextWait_<'f, Self::Item, Self::Err, fn() -> K>
    where
        Self: 'f;

    /// 等一条**已握手完成**的连接。
    ///
    /// 返回的 future 可以用 [`TrMayCancel::may_cancel_with`] 施加取消令牌；取消只中止
    /// **这一次**等待，listener 与已排队的连接都不受影响。
    fn next_async(&mut self) -> Self::NextAsync<'_> {
        let shared = self.shared_.clone();
        let inbound = self.inbound_.clone();
        let cfg = self.cfg_.clone();
        let neg = self.neg_.clone();
        let scope = self.scope_.clone();
        let cancel = self.shutdown_.clone();
        NextWait_::new_(
            Box::pin(async move {
                next_async_(shared, inbound, cfg, neg, scope, cancel).await
            }),
            SmuxListenerError::Cancelled,
        )
    }
}

/// [`TrAsyncIterator::next_async`] 的等待体。
///
/// # 它做三件事
///
/// 1. **补槽位**：若「存活槽位数 < 上限」且「队列为空」，就投一个新槽位到本地作用域。
///    按需补充而不是一次性铺满，是为了让「惰性」成立——没有调用方拉取时不接受连接；
/// 2. **等队列**：与关闭信号竞速，因此关闭能立刻返回；
/// 3. **取队首**：队列非空时直接取，不补槽位——那说明调用方还没把已有结果取走，
///    此刻再加槽位只会把在途条数推过上限。
async fn next_async_<Cfg, A, N, U, K>(
    shared: Arc<Shared_<Cfg, A>>,
    inbound: Arc<A>,
    cfg: Cfg,
    neg: Arc<N>,
    scope: U,
    cancel: K,
) -> Result<MuxConnection<Cfg::Conn>, SmuxListenerError<A::Error>>
where
    Cfg: TrListenerConfig + Clone + 'static,
    <Cfg::Conn as TrConnCfg>::ConnTx: TrBuffWrite<u8> + 'static,
    <Cfg::Conn as TrConnCfg>::ConnRx: TrBuffRead<u8> + 'static,
    A: TrInbound<ConnCfg = Cfg::Conn> + 'static,
    N: TrNegotiatorFactory + 'static,
    U: TrLocalScope + Clone + 'static,
    K: TrCancellationToken + Clone + 'static,
{
    if cancel.is_cancelled() {
        return Result::Err(SmuxListenerError::Cancelled);
    }

    // 1. 补槽位。队列非空时不补——那说明调用方还没把已有结果取走，此刻再加槽位只会把
    //    在途条数推过上限。
    if shared.queue_.is_empty() {
        // 先清理已完成任务：`live_` 的长度就是存活数，不清就会一直涨。
        let active = match shared.live_.lock() {
            Result::Ok(mut live) => Live_::retain_done_(&mut live),
            Result::Err(_) => 0usize,
        };
        if active < shared.max_inflight_ {
            let slot = Slot_ {
                inbound_: inbound.clone(),
                neg_: neg.clone(),
                scope_: scope.clone(),
                queue_: shared.queue_.clone(),
                shutdown_: cancel.clone(),
            };
            let live = Live_::spawn_(&scope, async move {
                acceptor_async_(slot, cfg).await;
            });
            if let Result::Ok(mut table) = shared.live_.lock() {
                table.push(live);
            }
        }
    }

    // 2. 等队列 / 关闭。三者合并在一次竞速里：调用方的令牌一触发，等待立刻结束，
    //    不会被「先入队的连接」压住。
    match receive_(shared.inbox_.clone(), cancel.clone()).await {
        Result::Ok(QueueMsg_::Connection(conn)) => Result::Ok(conn),
        // 接受失败是致命的：交给调用方（见 `SmuxListenerError::Accept`）。
        Result::Ok(QueueMsg_::AcceptFailed(err)) => {
            Result::Err(SmuxListenerError::Accept(err))
        }
        // 关闭信号已触发：不会再有连接进来。
        Result::Ok(QueueMsg_::Stopped) => Result::Err(SmuxListenerError::Cancelled),
        // 队列断开（发送端全没了）：同样按「不会再有连接」处理。
        Result::Err(_recv_err) => Result::Err(SmuxListenerError::Cancelled),
    }
}

/// 等队列里出现下一条消息，或等到关闭信号。
///
/// 语义见 [`next_async_`] 的第 2 步。
async fn receive_<T, AE, K>(
    queue: flume::Receiver<QueueMsg_<T, AE>>,
    cancel: K,
) -> Result<QueueMsg_<T, AE>, flume::RecvError>
where
    K: TrCancellationToken + Clone,
{
    let token = cancel.clone();
    let stopped = token.cancellation();
    let msg = select_with_cancel_(queue.recv_async(), stopped).await;
    match msg {
        // 关闭信号先到：本次等待结束，队列里已排队的连接不受影响。
        Option::None => Result::Err(flume::RecvError::Disconnected),
        Option::Some(msg) => msg,
    }
}

/// 两条 future 竞速，返回**先就绪**那条的输出；第二条就绪时返回 `None`。
///
/// 第二条的输出类型不做限定：取消信号的输出类型由
/// [`TrCancellationToken::Cancellation`] 给出（各后端都是 `()`），本函数只关心它是否
/// 已经就绪。
///
/// # 为什么不用 `futures::select!` 宏
///
/// 那个宏要求两条 future 都是 `FusedFuture`，而
/// [`TrCancellationToken::Cancellation`] 只有 `Future` 约束——它的语义是「可反复
/// select 同一个 future」，本处只需要用一次，不必也不该要求它。
///
/// # 为什么不用 `futures::future::select`
///
/// 后者要求两条 future 都 `Unpin`，而 `Cancellation` 是按值持有的关联类型，没有任何
/// `Unpin` 保证。这里在**栈上**用 `pin!` 固定它们（不涉及堆分配），因此对 `Unpin`
/// 没有要求。
async fn select_with_cancel_<F, S>(first: F, second: S) -> Option<F::Output>
where
    F: core::future::Future,
    S: core::future::Future,
{
    let mut first = core::pin::pin!(first);
    let mut second = core::pin::pin!(second);
    core::future::poll_fn(move |cx| {
        if let core::task::Poll::Ready(out) = first.as_mut().poll(cx) {
            return core::task::Poll::Ready(Option::Some(out));
        }
        if second.as_mut().poll(cx).is_ready() {
            return core::task::Poll::Ready(Option::None);
        }
        core::task::Poll::Pending
    })
    .await
}

/// 一个接受槽位：反复「接受 → 建环 → 投递握手」，直到被关闭或接受失败。
///
/// # 为什么每条连接都单独投一个任务
///
/// 握手要并发，而握手是异步的（要等对端的帧）。槽位若就地 await 它，就退化成串行。
/// 因此槽位只负责拿到设备、建好环，然后把「握手 + 建连 + 投递」整段交给新任务，自己
/// 立刻回去接受下一条。于是槽位始终在推接受，同时在途的连接数等于槽位数。
///
/// # 退出条件
///
/// - 关闭信号触发：把 [`QueueMsg_::Stopped`] 投进队列，让等待中的 `next_async` 立刻
///   醒过来（否则它会一直等到队列断开，而发送端还握在本槽位手里）；
/// - 接受失败：把错误投进队列并退出（致命，见 [`SmuxListenerError::Accept`]）。
///
/// 无论怎么退出，都把存活槽位计数减一——`next_async` 下一次醒来时会按需补新槽位。
async fn acceptor_async_<Cfg, A, N, U, K>(
    slot: Slot_<Cfg, A, N, U, K>,
    cfg: Cfg,
) where
    Cfg: TrListenerConfig + Clone + 'static,
    <Cfg::Conn as TrConnCfg>::ConnTx: TrBuffWrite<u8> + 'static,
    <Cfg::Conn as TrConnCfg>::ConnRx: TrBuffRead<u8> + 'static,
    A: TrInbound<ConnCfg = Cfg::Conn> + 'static,
    N: TrNegotiatorFactory + 'static,
    U: TrLocalScope + Clone + 'static,
    K: TrCancellationToken + Clone + 'static,
{
    let outcome = acceptor_inner_async_(&slot, cfg).await;
    if let Result::Err(msg) = outcome {
        // 队列可能已经断开（接收端被丢弃）：那时没人会看这条消息，丢掉即可。
        let _ = slot.queue_.send(msg);
    }
}

/// [`acceptor_async_`] 的主体；把「要不要往队列里投终止消息」留给调用者。
async fn acceptor_inner_async_<Cfg, A, N, U, K>(
    slot: &Slot_<Cfg, A, N, U, K>,
    cfg: Cfg,
) -> Result<(), QueueMsg_<MuxConnection<Cfg::Conn>, A::Error>>
where
    Cfg: TrListenerConfig + Clone + 'static,
    <Cfg::Conn as TrConnCfg>::ConnTx: TrBuffWrite<u8> + 'static,
    <Cfg::Conn as TrConnCfg>::ConnRx: TrBuffRead<u8> + 'static,
    A: TrInbound<ConnCfg = Cfg::Conn> + 'static,
    N: TrNegotiatorFactory + 'static,
    U: TrLocalScope + Clone + 'static,
    K: TrCancellationToken + Clone + 'static,
{
    loop {
        if slot.shutdown_.is_cancelled() {
            return Result::Err(QueueMsg_::Stopped);
        }

        // 接受：与关闭信号竞速，否则一个永久没有连入的 listener 无法被关闭。
        let token = slot.shutdown_.clone();
        let stopped = token.cancellation();
        let Some(accepted) =
            select_with_cancel_(slot.inbound_.accept_async(), stopped).await
        else {
            return Result::Err(QueueMsg_::Stopped);
        };

        let device = match accepted {
            Result::Ok(Option::Some(device)) => device,
            // 本次没有连接：继续等，不算失败。
            Result::Ok(Option::None) => continue,
            // 接受失败是致命的：交给调用方（见 `SmuxListenerError::Accept`）。
            Result::Err(err) => return Result::Err(QueueMsg_::AcceptFailed(err)),
        };

        // 建环：同步、无等待点，因此留在这里做。
        let (tx, rx) =
            into_halves_async_(&*slot.inbound_, device, slot.shutdown_.clone()).await;

        // 握手 + 建连 + 投递：交给一条独立任务，槽位立刻回去接受下一条。这样握手才
        // 真正并发——否则一条慢握手会把整个槽位占住。`Arc` 的克隆保证这条任务是
        // `'static` 的（它只借用自己拥有的东西）。
        let task_cfg = cfg.clone();
        let task_inbound = slot.inbound_.clone();
        let task_neg = slot.neg_.clone();
        let task_queue = slot.queue_.clone();
        let task_scope = slot.scope_.clone();
        let task_shutdown = slot.shutdown_.clone();
        // 同样必须 detach：丢弃句柄会取消任务（见 `Live_`）。握手任务的存活数**不**计入
        // `max_inflight`——那是「同时接受几条」的上限，而握手任务由槽位数间接界定。
        let _ = Live_::spawn_(&task_scope, async move {
            serve_async_(
                task_cfg,
                task_inbound,
                task_neg,
                task_queue,
                task_shutdown,
                tx,
                rx,
            )
            .await;
        });
    }
}

/// 完成一条连接的握手与建连，并把连接投进队列。
///
/// 三段与 `smux_v1` 的 `connection::connect_` 模块文档一一对应：
///
/// 1. **握手**：`HandshakeAgent::listen_async` 在两条半边上往返几个帧，协商出连接规格；
/// 2. **建连接**：用配置交出的帧暂存缓冲与连接策略把交付物接成 `MuxConnection`；
/// 3. **投递**：把成形连接送进 FIFO 队列，等 `next_async` 取走。
///
/// # 单条连接的失败一律吞掉
///
/// 握手相关的任何失败（magic 不对、CRC 不匹配、协商被拒、对端半途关闭、缓冲分配失败
/// ……）都只意味着「这条连接不符合条件」，因此这里直接返回、什么都不投——语义见
/// [`SmuxListener`] 的类型文档。唯一会外泄的错误是**接受**失败，那在槽位里处理。
async fn serve_async_<Cfg, A, N, K>(
    cfg: Cfg,
    inbound: Arc<A>,
    neg: Arc<N>,
    queue: flume::Sender<
        QueueMsg_<MuxConnection<Cfg::Conn>, <A as TrInbound>::Error>,
    >,
    shutdown: K,
    tx: <A::ConnCfg as TrConnCfg>::ConnTx,
    rx: <A::ConnCfg as TrConnCfg>::ConnRx,
) where
    Cfg: TrListenerConfig + 'static,
    <Cfg::Conn as TrConnCfg>::ConnTx: TrBuffWrite<u8> + 'static,
    <Cfg::Conn as TrConnCfg>::ConnRx: TrBuffRead<u8> + 'static,
    // 这一行是「传输适配与连接策略配套」的编译期防线：`A::Transport` 必须**就是**
    // `Cfg::Conn`，于是 `A::Tx` / `A::Rx` 与 `ConnTx` / `ConnRx` 逐字相同。
    A: TrInbound<ConnCfg = Cfg::Conn> + 'static,
    N: TrNegotiatorFactory + 'static,
    K: TrCancellationToken + Clone + 'static,
{
    // `inbound` 只用于让本函数的泛型参数 `A` 可被推断（队列的错误类型就来自它）；
    // 建环已经在槽位里做完了。
    let _ = &inbound;

    // 1. 握手：等待方流程。取消由 listener 的关闭令牌施加。
    let local = cfg.local_opts();
    let agent = HandshakeAgent::new(tx, rx);
    let Ok(delivery) = agent
        .listen_async(&local, neg.make())
        .may_cancel_with(shutdown.clone())
        .await
    else {
        // 这条连接不符合握手条件：静默丢弃，listener 继续。
        return;
    };

    // 2. 建连接：两块帧暂存缓冲由配置当场交出；运行时值也由配置交出，因此建连点
    //    **不需要**处于任何「编译期选定的后端」上下文内。
    let (read_stage, write_stage) = cfg.make_stage_buffs();
    let rt = cfg.runtime();
    // 作用域就地取自运行时值：语义与 `MuxConnection::new` 完全一致，只是本 crate 用
    // 自己的 `TrScopeHost`（见 `crate::scope_` 的说明）。
    let scope = crate::TrScopeHost::local_scope(&rt);
    let conn = MuxConnection::<Cfg::Conn>::new_with_rt(
        &rt,
        &scope,
        delivery,
        cfg.conn_cfg(),
        read_stage,
        write_stage,
    );

    // 3. 投递。发送失败只可能是「所有接收端都没了」（调用方已丢弃 listener）。
    let _ = queue.send(QueueMsg_::Connection(conn));
}
