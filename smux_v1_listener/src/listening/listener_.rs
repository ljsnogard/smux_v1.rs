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
//! **静默丢弃**，接受槽位继续。只有三类事会以 `Err` 交给调用方：本次等待被调用方取消
//! （[`SmuxListenerError::Cancelled`]）、listener 已关闭
//! （[`SmuxListenerError::Closed`]），以及从监听套接字接受失败
//! （[`SmuxListenerError::Accept`]）——后者说明 listener 自己已经产不出连接了。
//!
//! # 取消与关闭是两件事
//!
//! 这两件事**不共用**一个令牌，因此 listener 的类型上也没有令牌参数：
//!
//! - **单次等待的取消**：`next_async` 交出的 future 自身实现
//!   [`TrMayCancel`](smux_v1::x_deps::abs_cancel::TrMayCancel)，调用方在**每一次**等待上
//!   用 `may_cancel_with(token)` 施加自己的令牌。取消只结束这一次等待
//!   （[`SmuxListenerError::Cancelled`]）：listener 仍然可用，已经排进队列的连接不会丢，
//!   下一次调用立刻把它取出来。
//! - **listener 整体的关闭**：[`SmuxListener::close_async`]——停止接受、取消在途握手、
//!   等所有任务退出、丢弃残留连接；此后 `next_async` 一律返回
//!   [`SmuxListenerError::Closed`]。直接丢弃（drop）`SmuxListener` 则是**不优雅**的停止：
//!   它不等待任何任务收尾。
//!
//! 内部的关闭信号（[`CloseSignal_`]）是 listener 自己的东西，因此它既是**可触发**的
//! （`close_async` 触发它），也**不需要**出现在任何公开签名里。

use alloc::sync::Arc;
use core::{
    future::{Future, poll_fn},
    marker::PhantomData,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{Context, Poll, Waker},
};
use std::sync::Mutex;

use futures::{
    channel::oneshot,
    future::{FutureExt, Shared},
};

use abs_art::{TrJoinHandle, TrLocalScope};
use abs_buff::{TrBuffRead, TrBuffWrite};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_async_iter::TrAsyncIterator;
use smux_v1::{
    connection::{MuxConnection, TrConnCfg},
    handshake::agent::HandshakeAgent,
    x_deps::{abs_art, abs_buff, abs_cancel},
};

use crate::{
    SmuxListenerError,
    config::{TrInbound, TrListenerConfig, TrNegotiatorFactory, into_halves_async_},
    listening::{
        queue_::{Receiver_, RingQueue_, Sender_},
        wait_::NextWait_,
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
}

/// listener 的**关闭信号**：可触发、可克隆、支持任意多个等待者。
///
/// # 为什么需要它
///
/// listener 内部有好几处「等一个可能永远不来的事件」：槽位在等 accept、在途握手在等
/// 对端的帧、投递端可能卡在环的背压上。关闭时必须能同时把它们全部叫醒。`abs_cancel`
/// 只给两个极端（永远已取消 / 永不取消），都不适用，因此这里自带一份。
///
/// # 为什么「已触发」既存布尔又存通知
///
/// - [`AtomicBool`] 让 [`TrCancellationToken::is_cancelled`] 能**同步**回答（各处循环的
///   第一句检查的就是它）；
/// - [`Shared`] 让任意多个等待者共享**同一次**通知，并且等待体被丢弃时它自己会从登记表
///   里摘除——因此长时间运行的 listener 不会因为「等过很多次」而积累垃圾。这点是自己
///   拿 `Mutex<Vec<Waker>>` 实现时最容易做错的地方。
#[derive(Clone)]
struct CloseSignal_ {
    /// 是否已经触发。
    closed_: Arc<AtomicBool>,

    /// 「已触发」通知。等待体的原型是 [`CloseWait_`]。
    notify_: Shared<oneshot::Receiver<()>>,
}

impl CloseSignal_ {
    /// 造一份信号，连同它的触发端。
    ///
    /// 触发端不可克隆，且只由 [`SmuxListener`] 持有：关闭是 listener 整体的动作，不该被
    /// 复制出去（那正是「用一个外部令牌参数表达关闭」的老毛病）。
    fn new_() -> (Self, oneshot::Sender<()>) {
        let (tx, rx) = oneshot::channel();
        (
            CloseSignal_ {
                closed_: Arc::new(AtomicBool::new(false)),
                notify_: rx.shared(),
            },
            tx,
        )
    }

    /// 触发关闭：先置位，再唤醒所有等待者。
    ///
    /// `send` 失败只可能表示「已经没有等待者」，不影响语义。
    fn trigger_(&self, tx: oneshot::Sender<()>) {
        self.closed_.store(true, Ordering::SeqCst);
        let _ = tx.send(());
    }
}

impl TrCancellationToken for CloseSignal_ {
    type Cancellation = CloseWait_;

    type ChildToken = CloseSignal_;

    #[inline]
    fn is_cancelled(&self) -> bool {
        self.closed_.load(Ordering::SeqCst)
    }

    #[inline]
    fn can_be_cancelled(&self) -> bool {
        true
    }

    #[inline]
    fn child_token(&self) -> Self::ChildToken {
        self.clone()
    }

    fn cancellation(self) -> Self::Cancellation {
        CloseWait_ {
            notify_: self.notify_,
        }
    }
}

/// [`CloseSignal_`] 的等待体：信号触发时（或触发端被丢弃时）就绪。
struct CloseWait_ {
    /// 共享的「已触发」通知。
    notify_: Shared<oneshot::Receiver<()>>,
}

impl Future for CloseWait_ {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.notify_).poll(cx) {
            // `Ok(())` = 已触发；`Err(Canceled)` = 触发端已消失（listener 被丢弃）。
            // 后者同样意味着「不会再有连接」，所以两者都算就绪。
            Poll::Ready(_result) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// 任务登记表：谁还在跑，以及「全跑完了」这件事。
///
/// # 为什么不用「句柄 + 完成标记」的表
///
/// 句柄一律 [`detach`](TrJoinHandle::detach)（见 [`spawn_task_`]），因此无法 `join`；
/// 旧设计用一张 `Vec<Live_>`（每条任务一个 `done_` 标记）回答「还有几个存活」，靠调用方
/// 顺手清理。它只能**查询**，而 [`SmuxListener::close_async`] 需要**等待**——于是这里改成
/// 原子计数 + 归零通知：任务退出时自己减一，减到零时唤醒等待者。
struct TaskSet_ {
    /// 存活任务总数（接受槽位 + 在途握手）。
    tasks_: AtomicUsize,

    /// 其中**接受槽位**的数量；它就是 `max_inflight` 要限的那个额度。
    slots_: AtomicUsize,

    /// 「归零」等待者。至多一个（`close_async`），锁只覆盖 waker 的存取。
    idle_: Mutex<Option<Waker>>,
}

impl TaskSet_ {
    const fn new_() -> Self {
        TaskSet_ {
            tasks_: AtomicUsize::new(0usize),
            slots_: AtomicUsize::new(0usize),
            idle_: Mutex::new(Option::None),
        }
    }

    /// 登记一个即将投递的任务。
    ///
    /// `slot` 表示它是不是接受槽位（只有槽位计入限流额度）。
    fn enter_(&self, slot: bool) {
        if slot {
            self.slots_.fetch_add(1usize, Ordering::SeqCst);
        }
        self.tasks_.fetch_add(1usize, Ordering::SeqCst);
    }

    /// 任务退出时记账；返回「这是不是最后一个」。
    fn leave_(&self, slot: bool) -> bool {
        if slot {
            self.slots_.fetch_sub(1usize, Ordering::SeqCst);
        }
        self.tasks_.fetch_sub(1usize, Ordering::SeqCst) == 1usize
    }

    /// 当前存活槽位数。
    fn slots_(&self) -> usize {
        self.slots_.load(Ordering::SeqCst)
    }

    /// 等所有任务退出。
    ///
    /// 首次 poll 完成「登记 → 复检」，因此不存在「最后一个任务在我登记之前就退出」的
    /// 丢唤醒窗口。
    async fn idle_async_(&self) {
        poll_fn(|cx| {
            if self.tasks_.load(Ordering::SeqCst) == 0usize {
                return Poll::Ready(());
            }
            match self.idle_.lock() {
                Result::Ok(mut waker) => *waker = Option::Some(cx.waker().clone()),
                Result::Err(poisoned) => *poisoned.into_inner() = Option::Some(cx.waker().clone()),
            }
            if self.tasks_.load(Ordering::SeqCst) == 0usize {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }

    /// 唤醒归零等待者。
    ///
    /// `wake` 放在锁外调用（与 `buffex::ring` 的 park 协议同一条纪律）。
    fn wake_idle_(&self) {
        let waker = match self.idle_.lock() {
            Result::Ok(mut waker) => waker.take(),
            Result::Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Option::Some(waker) = waker {
            waker.wake();
        }
    }
}

/// 任务退出时的记账守卫。
struct TaskGuard_ {
    /// 登记表。
    set_: Arc<TaskSet_>,

    /// 本任务是否计入槽位额度。
    slot_: bool,
}

impl Drop for TaskGuard_ {
    fn drop(&mut self) {
        // 记账放在 `Drop` 里而不是任务体末尾：任务 panic 时也要算作退出，否则
        // `close_async` 会永远等不到归零。
        if self.set_.leave_(self.slot_) {
            self.set_.wake_idle_();
        }
    }
}

/// 投递一条任务并**立刻分离**它。
///
/// # 为什么必须 detach
///
/// `TrLocalScope::spawn_local` 交出的句柄一旦被丢弃，任务**可能随之被取消**：
/// tokio 的 `JoinHandle` drop 即 detach（任务继续跑），而 compio 与 smol 的句柄
/// drop 会**取消**任务（实测：compio 下投递后立刻 `drop` 句柄，任务体一次都没跑）。
/// listener 里所有 `spawn_local` 出来的任务（接受槽位、每条连接的握手任务）都必须活到
/// 自身跑完，因此一律调用 [`TrJoinHandle::detach`] 显式分离。
///
/// # 分离之后怎么知道它跑完了
///
/// 由登记表 [`TaskSet_`] 回答：投递前加一，任务体退出时（经 [`TaskGuard_`]）减一，
/// 减到零唤醒 [`SmuxListener::close_async`] 的等待者。
///
/// [`TrJoinHandle::detach`]: smux_v1::x_deps::abs_art::TrJoinHandle::detach
fn spawn_task_<S, F>(scope: &S, set_: &Arc<TaskSet_>, slot: bool, body: F)
where
    S: TrLocalScope,
    F: Future<Output = ()> + 'static,
{
    set_.enter_(slot);
    let set = set_.clone();
    let handle = scope.spawn_local(async move {
        let _guard = TaskGuard_ {
            set_: set,
            slot_: slot,
        };
        body.await;
    });
    TrJoinHandle::detach(handle);
}

/// [`SmuxListener`] 的共享状态：只放**槽位之间真正共享**的东西。
///
/// 配置、入向适配、协商策略工厂都不在这里——它们按值克隆进每个槽位。这里只留队列、
/// 任务登记表与关闭信号，于是本结构体与「用哪个运行时、哪种协商策略」都无关。
struct Shared_<Cfg, A>
where
    Cfg: TrListenerConfig,
    A: TrInbound,
{
    /// 已完成连接的投递队列（**生产端**）：每个槽位克隆一份，把握完的连接投进来。
    ///
    /// 队列本身是 [`RingQueue_`]，容量即 `max_inflight`——因此「队列长度」与「在途条数」
    /// 是同一个量，不需要两套账（见 [`crate::listening::queue_`] 的模块文档）。
    queue_: Sender_<QueueMsg_<MuxConnection<Cfg::Conn>, A::Error>>,

    /// 任务登记表：`slots_` 用于限流，`idle_async_` 供 `close_async` 等待收尾。
    tasks_: Arc<TaskSet_>,

    /// 关闭信号：槽位、在途握手、投递 park 都挂在它上面。
    close_: CloseSignal_,

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
struct Slot_<Cfg, A, N, U>
where
    Cfg: TrListenerConfig,
    A: TrInbound,
    N: TrNegotiatorFactory,
    U: TrLocalScope + 'static,
{
    /// 入向适配（含监听套接字）。
    inbound_: Arc<A>,

    /// 协商策略工厂。
    neg_: Arc<N>,

    /// 本地作用域：建连的五个循环要投到这里。
    scope_: U,

    /// 已完成连接的投递队列。
    queue_: Sender_<QueueMsg_<MuxConnection<Cfg::Conn>, A::Error>>,

    /// 任务登记表：槽位为每条连接投出的握手任务也要计入总数（`close_async` 要等它）。
    tasks_: Arc<TaskSet_>,

    /// 本 listener 的关闭信号。
    close_: CloseSignal_,
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
///
/// **没有令牌参数**：关闭不靠外部令牌表达（见模块文档「取消与关闭是两件事」）。
///
/// # 取消
///
/// 每次 [`next_async`](TrAsyncIterator::next_async) 都**各自**可取消：对返回的 future 调用
/// [`TrMayCancel::may_cancel_with`] 施加调用方自己的令牌即可。取消只中止**这一次**等待
/// （[`SmuxListenerError::Cancelled`]），listener 仍然可用，已经排进队列的连接不受影响。
///
/// # 关闭
///
/// [`close_async`](SmuxListener::close_async) 优雅停止：不再接受新连接、取消在途握手、
/// 等所有任务收尾，此后 `next_async` 一律返回 [`SmuxListenerError::Closed`]。丢弃本对象
/// 是**不优雅**的停止，不等待任何任务。
///
/// # Examples
///
/// ```ignore
/// // `cfg` 回答「握手条件 + 连接策略 + 缓冲从哪来」，`neg` 回答「谁裁决对端的条件」。
/// let mut listener = SmuxListener::new_(TcpTransport, cfg, neg, scope);
/// while let Ok(conn) = listener.next_async().await {
///     // `conn` 已经是握手完成的 MuxConnection
/// }
/// // 不想再听了就优雅收尾：
/// listener.close_async().await;
/// ```
pub struct SmuxListener<Cfg, A, N, U>
where
    Cfg: TrListenerConfig,
    A: TrInbound,
    N: TrNegotiatorFactory,
    U: TrLocalScope + Clone + 'static,
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

    /// 已完成连接的队列**消费端**。
    ///
    /// 它由 listener 独占（只有一个消费者），因此不放进 `Arc<Shared_>`。
    inbox_: Receiver_<QueueMsg_<MuxConnection<Cfg::Conn>, A::Error>>,

    /// 关闭信号的**触发端**；`None` 表示已经触发过（[`close_async`](SmuxListener::close_async)
    /// 幂等）。
    close_tx_: Option<oneshot::Sender<()>>,

    /// [`close_async`](SmuxListener::close_async) 是否已经返回过。
    closed_: bool,
}

impl<Cfg, A, N, U> SmuxListener<Cfg, A, N, U>
where
    Cfg: TrListenerConfig,
    A: TrInbound,
    N: TrNegotiatorFactory,
    U: TrLocalScope + Clone + 'static,
{
    /// 构造一个 listener。
    pub fn new_(inbound: A, cfg: Cfg, neg: N, scope: U) -> Self {
        // 上限归一化：0 与 1 等价。若允许 0，就一个槽位都不跑，listener 永远接不到连接。
        let max_inflight = cfg.max_inflight().max(1usize);
        // 队列容量就是在途上限：环满即背压（见 `crate::listening::queue_`）。
        let (queue, inbox) = RingQueue_::new_(max_inflight).split_();
        let (close, close_tx) = CloseSignal_::new_();
        SmuxListener {
            inbound_: Arc::new(inbound),
            cfg_: cfg,
            neg_: Arc::new(neg),
            scope_: scope,
            shared_: Arc::new(Shared_ {
                queue_: queue,
                tasks_: Arc::new(TaskSet_::new_()),
                close_: close,
                max_inflight_: max_inflight,
                _mark_: PhantomData,
            }),
            inbox_: inbox,
            close_tx_: Option::Some(close_tx),
            closed_: false,
        }
    }

    /// 取在途握手条数上限（已归一化为至少 1）。
    pub fn max_inflight(&self) -> usize {
        self.shared_.max_inflight_
    }

    /// 当前存活的接受槽位数。
    ///
    /// 主要用于诊断：为 0 且队列为空、又没调用过 [`close_async`](SmuxListener::close_async)，
    /// 说明槽位刚刚退出而 `next_async` 还没来得及补，属正常瞬时状态。
    pub fn active_slots_(&self) -> usize {
        self.shared_.tasks_.slots_()
    }

    /// **优雅地**停止监听。
    ///
    /// # 它依次做三件事
    ///
    /// 1. **触发关闭信号**：正卡在 accept、在途握手、或投递背压上的任务全部被唤醒并
    ///    结束——在途握手是被**取消**的，不会为了等对端而无限拖下去；
    /// 2. **等所有任务退出**（槽位与在途握手都已收尾）；
    /// 3. **丢弃队列里残留的已握完连接**并关闭投递端，因此此后
    ///    [`next_async`](TrAsyncIterator::next_async) 一律返回
    ///    [`SmuxListenerError::Closed`]。
    ///
    /// 幂等：重复调用直接返回。
    ///
    /// # 与 drop 的区别
    ///
    /// 丢弃 `SmuxListener` 不做上面任何一步：在途任务不保证收尾，残留连接随队列一起丢。
    /// 要确定「已经停干净」，只能用它。
    ///
    /// # 取消
    ///
    /// 本方法自身不接取消令牌：它等的只是本 crate 自己投出去的任务，而这些任务全都挂在
    /// 关闭信号上，退出时间是有限的。
    pub async fn close_async(&mut self) {
        if self.closed_ {
            return;
        }
        // 1. 触发关闭：唤醒所有等待点。
        if let Option::Some(tx) = self.close_tx_.take() {
            self.shared_.close_.trigger_(tx);
        }
        // 2. 等槽位与在途握手全部退出。
        self.shared_.tasks_.idle_async_().await;
        // 3. 关闭投递端。任务都已退出，此刻没有别的写者，因此这次关闭必然成功；关掉之后
        //    即便还有残留数据，`pop` 也会先把它交完再报终结。
        self.shared_.queue_.close_();
        // 4. 排空残留：关闭之后不再交付任何连接，取出来的直接丢弃。
        while let Result::Ok(Option::Some(_msg)) = self.inbox_.pop(NonCancellableToken::new()).await
        {
        }
        self.closed_ = true;
    }
}

impl<Cfg, A, N, U> TrAsyncIterator for SmuxListener<Cfg, A, N, U>
where
    Cfg: TrListenerConfig + Clone + 'static,
    <Cfg::Conn as TrConnCfg>::ConnTx: TrBuffWrite<u8> + 'static,
    <Cfg::Conn as TrConnCfg>::ConnRx: TrBuffRead<u8> + 'static,
    A: TrInbound<ConnCfg = Cfg::Conn> + 'static,
    N: TrNegotiatorFactory + 'static,
    U: TrLocalScope + Clone + 'static,
{
    type Item = MuxConnection<Cfg::Conn>;

    type Err = SmuxListenerError<A::Error>;

    /// 等待体的类型标记取 `()`：listener 身上没有令牌，取消令牌由**每一次**等待自己在
    /// `may_cancel_with` 里带进来（它只作类型标记，见 [`NextWait_`]）。
    type NextAsync<'f>
        = NextWait_<'f, Self::Item, Self::Err, ()>
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
        // 队列接收端由 listener 独占，因此**借**给内部实现（它不能被克隆）。
        let inbox = &mut self.inbox_;
        let closed = self.closed_;
        NextWait_::new_(
            Box::pin(async move {
                // 关闭之后不再交付任何东西（残留连接已在 `close_async` 里丢弃）。
                if closed {
                    return Result::Err(SmuxListenerError::Closed);
                }
                next_async_(inbox, shared, inbound, cfg, neg, scope).await
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
/// 2. **等队列**：队列非空时直接取，不补槽位——那说明调用方还没把已有结果取走，
///    此刻再加槽位只会把在途条数推过上限；
/// 3. **取队首**：取到就交付；投递端全关（不会再有连接）时报
///    [`SmuxListenerError::Closed`]。
///
/// 本次等待的取消**不在这里**：它由调用方在返回的 future 上施加（见模块文档）。
async fn next_async_<Cfg, A, N, U>(
    inbox: &mut Receiver_<QueueMsg_<MuxConnection<Cfg::Conn>, A::Error>>,
    shared: Arc<Shared_<Cfg, A>>,
    inbound: Arc<A>,
    cfg: Cfg,
    neg: Arc<N>,
    scope: U,
) -> Result<MuxConnection<Cfg::Conn>, SmuxListenerError<A::Error>>
where
    Cfg: TrListenerConfig + Clone + 'static,
    <Cfg::Conn as TrConnCfg>::ConnTx: TrBuffWrite<u8> + 'static,
    <Cfg::Conn as TrConnCfg>::ConnRx: TrBuffRead<u8> + 'static,
    A: TrInbound<ConnCfg = Cfg::Conn> + 'static,
    N: TrNegotiatorFactory + 'static,
    U: TrLocalScope + Clone + 'static,
{
    // 1. 补槽位。队列非空时不补——那说明调用方还没把已有结果取走，此刻再加槽位只会把
    //    在途条数推过上限。
    if inbox.len_() == 0usize && shared.tasks_.slots_() < shared.max_inflight_ {
        let slot = Slot_ {
            inbound_: inbound.clone(),
            neg_: neg.clone(),
            scope_: scope.clone(),
            queue_: shared.queue_.clone(),
            tasks_: shared.tasks_.clone(),
            close_: shared.close_.clone(),
        };
        spawn_task_(&scope, &shared.tasks_, true, async move {
            acceptor_async_(slot, cfg).await;
        });
    }

    // 2. 等队列。这里用「永不取消」的令牌：本次等待的取消由调用方在 `next_async` 返回的
    //    future 上施加，而「关闭」会让投递端关闭，从而以终结的形式唤醒这里。
    match inbox.pop(NonCancellableToken::new()).await {
        Result::Ok(Option::Some(QueueMsg_::Connection(conn))) => Result::Ok(conn),
        // 接受失败是致命的：交给调用方（见 `SmuxListenerError::Accept`）。
        Result::Ok(Option::Some(QueueMsg_::AcceptFailed(err))) => {
            Result::Err(SmuxListenerError::Accept(err))
        }
        // 投递端全关且环已取空：不会再有连接进来了。
        Result::Ok(Option::None) => Result::Err(SmuxListenerError::Closed),
        // 不可达：`NonCancellableToken` 不会被取消。按「不会再有连接」处理。
        Result::Err(_empty) => Result::Err(SmuxListenerError::Closed),
    }
}

/// 两条 future 竞速，返回**先就绪**那条的输出；第二条就绪时返回 `None`。
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
async fn select_with_close_<F, S>(first: F, second: S) -> Option<F::Output>
where
    F: Future,
    S: Future,
{
    let mut first = core::pin::pin!(first);
    let mut second = core::pin::pin!(second);
    poll_fn(move |cx| {
        if let Poll::Ready(out) = first.as_mut().poll(cx) {
            return Poll::Ready(Option::Some(out));
        }
        if second.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Option::None);
        }
        Poll::Pending
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
/// - 关闭信号触发：直接退出。不需要往队列里投任何哨兵——关闭由
///   [`SmuxListener::close_async`] 自己收尾，而等待中的
///   [`next_async`](TrAsyncIterator::next_async) 会被投递端的关闭唤醒；
/// - 接受失败：把错误投进队列并退出（致命，见 [`SmuxListenerError::Accept`]）。
async fn acceptor_async_<Cfg, A, N, U>(slot: Slot_<Cfg, A, N, U>, cfg: Cfg)
where
    Cfg: TrListenerConfig + Clone + 'static,
    <Cfg::Conn as TrConnCfg>::ConnTx: TrBuffWrite<u8> + 'static,
    <Cfg::Conn as TrConnCfg>::ConnRx: TrBuffRead<u8> + 'static,
    A: TrInbound<ConnCfg = Cfg::Conn> + 'static,
    N: TrNegotiatorFactory + 'static,
    U: TrLocalScope + Clone + 'static,
{
    let outcome = acceptor_inner_async_(&slot, cfg).await;
    if let Result::Err(msg) = outcome {
        // 投递「接受失败」这类致命消息。队列已满只可能是「调用方长时间不拉取」，
        // 那时这条消息也没人看，丢掉即可；关闭已触发时同样丢掉。
        let _ = slot.queue_.push(msg, slot.close_.clone()).await;
    }
}

/// [`acceptor_async_`] 的主体；把「要不要往队列里投终止消息」留给调用者。
async fn acceptor_inner_async_<Cfg, A, N, U>(
    slot: &Slot_<Cfg, A, N, U>,
    cfg: Cfg,
) -> Result<(), QueueMsg_<MuxConnection<Cfg::Conn>, A::Error>>
where
    Cfg: TrListenerConfig + Clone + 'static,
    <Cfg::Conn as TrConnCfg>::ConnTx: TrBuffWrite<u8> + 'static,
    <Cfg::Conn as TrConnCfg>::ConnRx: TrBuffRead<u8> + 'static,
    A: TrInbound<ConnCfg = Cfg::Conn> + 'static,
    N: TrNegotiatorFactory + 'static,
    U: TrLocalScope + Clone + 'static,
{
    loop {
        if slot.close_.is_cancelled() {
            return Result::Ok(());
        }

        // 接受：与关闭信号竞速，否则一个永久没有连入的 listener 无法被关闭。
        let token = slot.close_.clone();
        let stopped = token.cancellation();
        let Some(accepted) = select_with_close_(slot.inbound_.accept_async(), stopped).await else {
            return Result::Ok(());
        };

        let device = match accepted {
            Result::Ok(Option::Some(device)) => device,
            // 本次没有连接：继续等，不算失败。
            Result::Ok(Option::None) => continue,
            // 接受失败是致命的：交给调用方（见 `SmuxListenerError::Accept`）。
            Result::Err(err) => return Result::Err(QueueMsg_::AcceptFailed(err)),
        };

        // 建环：同步、无等待点，因此留在这里做。
        let (tx, rx) = into_halves_async_(&*slot.inbound_, device, slot.close_.clone()).await;

        // 握手 + 建连 + 投递：交给一条独立任务，槽位立刻回去接受下一条。这样握手才
        // 真正并发——否则一条慢握手会把整个槽位占住。`Arc` 的克隆保证这条任务是
        // `'static` 的（它只借用自己拥有的东西）。
        let task_cfg = cfg.clone();
        let task_inbound = slot.inbound_.clone();
        let task_neg = slot.neg_.clone();
        let task_queue = slot.queue_.clone();
        let task_scope = slot.scope_.clone();
        let task_close = slot.close_.clone();
        // 同样必须 detach：丢弃句柄会取消任务（见 `spawn_task_`）。握手任务的存活数
        // **不**计入 `max_inflight`——那是「同时接受几条」的上限，而握手任务由槽位数
        // 间接界定；但它计入任务总数，因为 `close_async` 要等到它退出。
        spawn_task_(&task_scope, &slot.tasks_, false, async move {
            serve_async_(
                task_cfg,
                task_inbound,
                task_neg,
                task_queue,
                task_close,
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
async fn serve_async_<Cfg, A, N>(
    cfg: Cfg,
    inbound: Arc<A>,
    neg: Arc<N>,
    queue: Sender_<QueueMsg_<MuxConnection<Cfg::Conn>, <A as TrInbound>::Error>>,
    close: CloseSignal_,
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
{
    // `inbound` 只用于让本函数的泛型参数 `A` 可被推断（队列的错误类型就来自它）；
    // 建环已经在槽位里做完了。
    let _ = &inbound;

    // 1. 握手：等待方流程。取消由 listener 的关闭信号施加——关闭时这条在途握手被
    //    立刻取消，而不是等对端把帧发完。
    let local = cfg.local_opts();
    let agent = HandshakeAgent::new(tx, rx);
    let Ok(delivery) = agent
        .listen_async(&local, neg.make())
        .may_cancel_with(close.clone())
        .await
    else {
        // 这条连接不符合握手条件（或被关闭取消）：静默丢弃，listener 继续。
        return;
    };

    // 2. 建连接：**不需要**帧暂存缓冲——传输的两条半边自带缓冲（`ConnTx` / `ConnRx`）。
    //    运行时值由配置交出，因此建连点不需要处于任何「编译期选定的后端」上下文内。
    let rt = cfg.runtime();
    // 作用域就地取自运行时值：语义与 `MuxConnection::new` 完全一致，只是本 crate 用
    // 自己的 `TrScopeHost`（见 `crate::scope_` 的说明）。
    let scope = crate::TrScopeHost::local_scope(&rt);
    let conn = MuxConnection::<Cfg::Conn>::new_with_rt(&rt, &scope, delivery, cfg.conn_cfg());

    // 3. 投递。失败只可能是「等待被取消（listener 正在收尾）」或「消费端已消失」——
    //    那时放弃这条连接。
    let _ = queue.push(QueueMsg_::Connection(conn), close).await;
}
