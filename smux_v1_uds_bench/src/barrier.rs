//! 让 N 个发送任务**同时出发**的异步栅栏。
//!
//! # 为什么需要它
//!
//! 本 bench 要测的是「N 个客户端并发发送」。如果让 N 个任务各自 `spawn` / `join_all`
//! 之后直接开跑，那么第 1 个任务可能已经发完几 MiB，第 N 个任务才刚被第一次 poll 到
//! ——测出来的「并发」其实是**启动错位**，方差会被启动顺序而不是调度质量支配。
//!
//! 因此每个任务先在栅栏上等，直到 N 个任务**全部就位**；最后一个到达者负责唤醒其余
//! 所有等待者。栅栏释放的那一刻是**所有任务共同的起始时间**，本 bench 的墙钟与
//! 「每任务时长」都从它起算。
//!
//! # 为什么可以是一把 `RefCell` + [`Rc`]
//!
//! 三个后端下本 bench 的发送任务都跑在**同一个线程**上（compio 的 socket 半边是
//! `!Send`，`spawn` 用不了，因此全部并发都用 `join_all` / `FuturesUnordered`）。
//! 既然如此就不引入原子与锁，用单线程的 `Rc<RefCell<..>>` 更直白，也不会因为
//! `Mutex` 的 `Send` 约束而牵出一堆 `!Send` 的取值范围问题。
//!
//! # 失败路径也要能醒
//!
//! 若某个任务在到达栅栏之前就出错（建连失败、适配器构造失败……），剩下的任务会永远
//! 等下去。因此 [`StartBarrier::abort`] 会把所有等待者唤醒成 [`Release::Failed`]，
//! 编排层在任一任务失败时调用它。

use core::task::{Context, Poll, Waker};
use std::{cell::RefCell, rc::Rc};

/// 栅栏释放的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Release {
    /// N 个任务全部就位，可以同时出发。
    Go,
    /// 有任务提前失败，本次用例作废。
    Failed,
}

/// N 个任务的「同时出发」栅栏（单线程共享句柄）。
#[derive(Debug, Clone)]
pub struct StartBarrier {
    /// 内部状态；`Rc` 使句柄可克隆分发给 N 个任务。
    inner_: Rc<RefCell<BarrierState_>>,
}

/// 栅栏内部状态。
#[derive(Debug)]
struct BarrierState_ {
    /// 需要就位的任务数。
    parties_: usize,
    /// 已经就位的任务数。
    arrived_: usize,
    /// 已到达、正在等待释放的 waker。
    waiters_: Vec<Waker>,
    /// 是否已经被判定失败。
    aborted_: bool,
}

impl StartBarrier {
    /// 造一个需要 `parties` 个任务就位的栅栏。
    ///
    /// `parties == 0` 时第一次 `arrive` 立即放行（不会永久阻塞）。
    pub fn new(parties: usize) -> Self {
        StartBarrier {
            inner_: Rc::new(RefCell::new(BarrierState_ {
                parties_: parties,
                arrived_: 0usize,
                waiters_: Vec::new(),
                aborted_: false,
            })),
        }
    }

    /// 本栅栏需要多少个任务就位。
    pub fn parties(&self) -> usize {
        self.inner_.borrow().parties_
    }

    /// 已在等待释放的任务数（诊断用；不参与判据）。
    pub fn waiting(&self) -> usize {
        self.inner_.borrow().waiters_.len()
    }

    /// 登记当前任务并返回释放结果。
    ///
    /// 最后一个到达者返回 [`Release::Go`] 并唤醒其余等待者；其余到达者先返回
    /// [`Poll::Pending`]，待被唤醒时再返回 [`Release::Go`]。已判定失败时一律返回
    /// [`Release::Failed`]。
    pub fn arrive(&self) -> Poll<Release> {
        let mut state = self.inner_.borrow_mut();
        if state.aborted_ {
            return Poll::Ready(Release::Failed);
        }
        state.arrived_ += 1usize;
        if state.arrived_ >= state.parties_ {
            // 自己是最后一个：唤醒所有先到者（不唤醒自己，直接返回 Go）。
            for waiter in state.waiters_.drain(..) {
                waiter.wake();
            }
            return Poll::Ready(Release::Go);
        }
        // 先到者：登记唤醒者。同一个任务不会被重复登记，因此不必去重。
        Poll::Pending
    }

    /// 登记当前 [`Waker`]（与 [`StartBarrier::arrive`] 配对：返回 `Pending` 之后由
    /// 调用方把它登记进来）。
    pub fn register(&self, cx: &Context<'_>) {
        let mut state = self.inner_.borrow_mut();
        state.waiters_.push(cx.waker().clone());
    }

    /// 判定本用例失败：唤醒全部等待者，它们会拿到 [`Release::Failed`]。
    pub fn abort(&self) {
        let mut state = self.inner_.borrow_mut();
        state.aborted_ = true;
        for waiter in state.waiters_.drain(..) {
            waiter.wake();
        }
    }

    /// 是否已判定失败。
    pub fn is_aborted(&self) -> bool {
        self.inner_.borrow().aborted_
    }

    /// 就位 + 等待的**一步式**异步包装。
    ///
    /// 用法：
    ///
    /// ```ignore
    /// if barrier.wait().await == Release::Failed { return Err(..) }
    /// ```
    pub async fn wait(&self) -> Release {
        futures::future::poll_fn(|cx| self.arrive_at(cx)).await
    }

    /// 就位 + 等待的**一步式**包装：`poll_fn` 里直接用。
    ///
    /// 用法：
    ///
    /// ```ignore
    /// let release = futures::future::poll_fn(|cx| barrier.arrive_at(cx)).await;
    /// ```
    pub fn arrive_at(&self, cx: &Context<'_>) -> Poll<Release> {
        match self.arrive() {
            Poll::Ready(release) => Poll::Ready(release),
            Poll::Pending => {
                self.register(cx);
                // 登记之后**必须复检**：最后一个到达者可能刚好在自己登记之前放行并
                // 清空了 waiters_，那样就丢了一次唤醒（经典竞态）。单线程下这只在
                // 「arrive 返回 Pending 与 register 之间没有其他任务跑」时才安全，
                // 因此这里显式补一遍 abort / 全到的检查。
                let state = self.inner_.borrow();
                if state.aborted_ {
                    return Poll::Ready(Release::Failed);
                }
                if state.arrived_ >= state.parties_ {
                    return Poll::Ready(Release::Go);
                }
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试栅栏在 N 个任务全部就位后放行，且放行前一直是 `Pending`。
    /// - 手段：造一个 3 方栅栏，手工模拟前两次到达（`arrive` 应返回 `Pending`），
    ///   第三次到达应返回 `Go`。
    /// - 判断：前两次 `Pending`、第三次 `Ready(Go)`，且 `arrived` 计数没有被重复放行。
    ///
    /// Checks the barrier releases only after all parties arrive.
    /// - Method: a 3-party barrier with two manual arrivals followed by the last.
    /// - Assertion: the first two are `Pending`, the third is `Ready(Go)`.
    #[test]
    fn barrier_releases_on_last_arrival_() {
        let barrier = StartBarrier::new(3usize);
        assert_eq!(barrier.parties(), 3usize);
        assert_eq!(barrier.arrive(), Poll::Pending);
        assert_eq!(barrier.arrive(), Poll::Pending);
        assert_eq!(barrier.arrive(), Poll::Ready(Release::Go));
    }

    /// 测试 `abort` 之后所有到达者都拿到失败结论。
    /// - 手段：2 方栅栏上先 `abort`，再连续 `arrive` 两次。
    /// - 判断：两次都返回 `Ready(Failed)`；`is_aborted` 为真。
    ///
    /// Checks aborting fails every subsequent arrival immediately.
    /// - Method: abort a 2-party barrier, then arrive twice.
    /// - Assertion: both arrivals return `Ready(Failed)` and `is_aborted` is true.
    #[test]
    fn aborted_barrier_fails_all_arrivals_() {
        let barrier = StartBarrier::new(2usize);
        barrier.abort();
        assert!(barrier.is_aborted());
        assert_eq!(barrier.arrive(), Poll::Ready(Release::Failed));
        assert_eq!(barrier.arrive(), Poll::Ready(Release::Failed));
    }

    /// 测试 0 方栅栏不会永久阻塞（退化输入立刻放行）。
    /// - 手段：造 0 方栅栏并 `arrive` 一次。
    /// - 判断：立刻返回 `Ready(Go)`。
    ///
    /// Checks a zero-party barrier does not block forever.
    /// - Method: arrive on a barrier created with zero parties.
    /// - Assertion: it returns `Ready(Go)` immediately.
    #[test]
    fn zero_party_barrier_releases_immediately_() {
        let barrier = StartBarrier::new(0usize);
        assert_eq!(barrier.arrive(), Poll::Ready(Release::Go));
    }
}
