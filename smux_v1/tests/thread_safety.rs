//! 线程安全相关的验收测试（阶段一：句柄可跨线程 + 身份表真正共享）。
//!
//! 本文件钉住两条**行为**不变量：
//!
//! 1. 同一个 `MuxCore` 的两个 `MuxConnection` 实例分处两条线程、**并发**对同一个
//!    dock 调 `bind_async` 时，绑定独占性成立：恰有一个成功，另一个拿到
//!    `BindError::DockInUse`（[`mux_bind_cross_thread_is_exclusive_tokio_`]）；
//! 2. 一条线程丢弃 `DockBinding`（`Drop` 只向核心投递释放消息），**另一条线程**
//!    立刻重绑同一个 dock 必须成功
//!    （[`mux_rebind_after_cross_thread_drop_tokio_`]）；
//! 3. 两条线程**同时**用 `bind_async(Dock::unspecified())` 索要一个空闲 dock 时，
//!    「找一个空闲的」与「占住它」是一次**原子**操作：两条都必须成功，且拿到的 dock
//!    **互不相同**（[`mux_auto_dock_allocation_is_atomic_tokio_`]）。
//!
//! # 为什么是 tokio 装配（性质在两个后端之间对调了）
//!
//! 运行时值是配置的关联类型 **`C::Rt`**（不是 `MuxConnection` 的类型参数——公开类型
//! 只有 `<C>` 一个参数），于是连接与各句柄是否 `Send`/`Sync` 由它决定：
//!
//! | 装配 | `C::Rt` | `MuxConnection` / 各句柄 |
//! | --- | --- | --- |
//! | tokio | `abs_art_tokio::Runtime`（`tokio::runtime::Handle` 把手） | `Send + Sync` |
//! | compio | `abs_art_compio::Runtime`（线程本地的运行时实例） | `!Send + !Sync` |
//!
//! compio 那一格的 `!Send + !Sync` 由编译期探针实测：核心经 `config_: C` →
//! `DefaultConnCfg::rt_` 间接持有 `compio_runtime::Runtime`（内含
//! `Rc<compio_executor::Executor>`），这是 `MuxCore<C>` 无法 `Send + Sync` 的唯一
//! 入口；详见 `mux_connection::core_` 的类型文档。
//!
//! 这与改造前**恰好相反**：当时类型参数是零大小的作用域**标记**，tokio 的
//! `LocalScope` 含 `Rc<LocalSet>` 而 `!Send`，compio 的作用域是零大小的值而 `Send`。
//! 因此这两条「句柄跨线程」的用例现在只能跑在 **tokio** 装配下；compio 侧不可能等价
//! 迁移——`abs_art_compio::Runtime` 持有 `compio::runtime::Runtime`（`Rc` 构成，
//! 绑定创建它的线程），含该值的连接移动不到别的线程，而且它的 `Runtime::current()`
//! 还要求调用点已在 compio 上下文内，连「在上下文之外先造一个值」都做不到。
//!
//! 这正是 `dev-notes/outlook-concurrency-20261002-2322.md` §3 所说的「句柄可以走，
//! reactor 不走」：跨线程的只有连接句柄，读写循环仍留在建连线程的本地队列上。
//!
//! # 实现品质要求（本文件**测不到**）
//!
//! 争用一律**零 CPU 忙等**：注册表与取消令牌都只走协作式锁的 `try_read` / `try_write`
//! 快路径，失败则 `read_async` / `write_async()` 配 `may_cancel_with(cancel)` **异步等待**
//! ——让出执行权、可被取消，既不自旋也不阻塞本线程
//! （见 `connection::sync_::{acquire_read_, acquire_write_}`）；
//! `Drop` 不取任何锁，只投一条释放消息。这两条是**实现约束**，行为断言覆盖不到，
//! 因此写在这里作为阅读本文件时的前提。
//!
//! # 为什么要 `block_on`，以及为什么场景整体包在 `run_until` 里
//!
//! 每个竞争线程必须**自建一个 tokio 运行时**、在自己的运行时上 `block_on` 一次绑定
//! 操作，才能证明「句柄跨线程可用」——`block_on` 在这里是**被测对象本身**，不是
//! 「把异步压成同步」的偷懒写法。按项目纪律，只有这种「特意测 `block_on` 效果本身」
//! 的用例才允许保留它。
//!
//! 主线程那侧的 `block_on` 则承担**宿主**职责：`bind_async` 不是本地完成的，它经
//! `MainRequest_` 队列交给 `main_loop`，而 `main_loop` 是主线程 `LocalSet` 里的本地
//! 任务。因此只要场景 future 处于挂起状态，`block_on(scope.run_until(场景))` 就必须
//! 继续驱动本地队列，`main_loop` 才有机会处理工作线程投来的请求。
//!
//! **曾经的形态是错的**：旧版先 `block_on(run_until(connect_pair_))` 建连、拿到
//! `(a, b, scope)` 之后在 `block_on` 之外用阻塞的 `thread::join()` 汇合工作线程。
//! `block_on` 返回后本地队列再没人驱动，工作线程的 `bind_async` 永远等不到
//! `main_loop`，于是三个用例全部挂死（实测 >60 s 无结果，单跑也一样）。R18 把整段
//! 场景移进 `run_until`，并用 `futures::channel::oneshot` **异步**等待工作线程回传
//! 结果——汇合点绝不能再阻塞主线程。
//!
//! **挂起本身**不是产品缺陷：跨线程的只有连接句柄，读写循环仍留在建连线程的本地
//! 队列上，宿主必须负责驱动它（见 `dev-notes/sw-scope-20261028-2130.md` §7）。
//!
//! 但把壳修好、`LocalSet` 真正驱动起来之后，这组用例立刻抓到了**真正**的并发缺陷：
//! `sw-scope::WeakSlot::try_sharing` 在「另一个线程刚把最后一个 `Sharing` 放回
//! `Created`」的瞬间会误报 `Err(Created)`，而 `Tcs_::is_abandoned_` 把升级失败当成
//! 「等待方已放弃」，于是仍在等待的 `bind` 被答成 `Cancelled`（实测约 1/3 概率
//! `[Bound, Other(Cancelled)]`）。该竞态已在 sw-scope 仓库单独修复（`4d624c5`），
//! 本文件因此是本轮「跨线程句柄共享」行为的最终回归网。

#![cfg(feature = "test-tokio-runtime")]
#![feature(allocator_ext)]
mod common;

/// 本文件用到的连接配置别名：两侧同构的冒烟策略（传输类型由 [`common::connect_pair_`]
/// 的类型参数推断，这里只固定「配置」这一层，便于给连接类型起名）。
type SmokeCfg_ = common::SmokeMuxConfig<
    smux_v1::connection::BufferedTx,
    smux_v1::connection::BufferedRx,
    common::DefaultRt,
>;

/// 本文件使用的连接类型：**tokio** 运行时值（`Send + Sync` 的 `Handle` 把手）。
type Conn_ = smux_v1::connection::MuxConnection<SmokeCfg_>;

use core::sync::atomic::{AtomicBool, Ordering};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use abs_art::TrLocalScope;
use abs_smux::conn::{TrConnection, TrDockBinding};
use smux_v1::connection::{BindError, Dock};

/// 竞争轮数：每轮换一个 dock，避免上一轮的结果影响下一轮。
///
/// 取锁的临界区很短，两条线程需要真正同时进入才可能撞上真正的并发热点，
/// 因此这里用「自旋对齐起跑线 + 多轮」把命中概率拉到接近确定。
const K_RACE_ROUNDS: usize = 32;

/// 测试用 dock 起始值（远离其它用例的取值区间）。
const K_FIRST_DOCK: u32 = 0x4000;

/// 自旋等待对侧进展的上限（纯兜底：正常路径上对侧在微秒级就会到达；即便对侧真的
/// 在 `catch_unwind` 之外消失，本用例也应当以断言失败收场，而不是挂死）。
const K_SPIN_LIMIT: usize = 1_000_000;

/// 编译期性质断言：**tokio** 装配下连接与句柄是 `Send + Sync`，所以它们可以跨线程
/// 持有并操作——[`mux_bind_cross_thread_is_exclusive_tokio_`] 与
/// [`mux_rebind_after_cross_thread_drop_tokio_`] 正是靠这条性质才写得出来。
///
/// 反方向（compio 装配下 `!Send`）无法用同样的断言表达（Rust 没有「不实现某 trait」
/// 的稳定写法），只能由 `abs_art_compio::Runtime` 的类型文档与 `src/connection/mod.rs`
/// §6 的说明钉住。**刻意不执行**，只为把这条结论钉在编译期。
#[allow(dead_code)]
fn assert_tokio_conn_is_send_sync_() {
    fn assert_send_sync_<T: Send + Sync>() {}
    assert_send_sync_::<Conn_>();
}

/// 自旋等待一个标志置位（有界；每 1024 圈让出一次 CPU，避免单核上互相饿死）。
fn wait_flag_(flag: &AtomicBool) {
    let mut spins = 0usize;
    while !flag.load(Ordering::Acquire) {
        core::hint::spin_loop();
        spins += 1usize;
        if spins.is_multiple_of(1024usize) {
            std::thread::yield_now();
        }
        if spins > K_SPIN_LIMIT {
            return;
        }
    }
}

/// 从 `catch_unwind` 的载荷里取出可读信息（`panic!` 的两种常见载荷 + 兜底）。
fn panic_message_(payload: Box<dyn core::any::Any + Send>) -> String {
    if let Option::Some(msg) = payload.downcast_ref::<&'static str>() {
        (*msg).to_owned()
    } else if let Option::Some(msg) = payload.downcast_ref::<String>() {
        msg.clone()
    } else {
        "<非字符串 panic 载荷>".to_owned()
    }
}

/// 一次竞争的结果（`DockBinding` 在本线程内丢弃，不跨线程回传）。
enum RaceOutcome_ {
    /// 抢到了绑定；binding 已在对侧也出结果之后丢弃（= 解绑）。
    Bound,

    /// 该 dock 已被另一条线程占用（期望的输家结果）。
    DockInUse,

    /// 其它错误：绑定路径给出了意料之外的失败。
    Other(BindError),

    /// 竞争路径 panic（**回归信号**：合法并发竞争不得 panic）。
    Panicked(String),
}

/// 手写 `Debug`（而不是派生）：`Other` 的载荷只在断言消息里被打印，派生实现不会
/// 被 dead-code 分析算作「读过字段」，手写则顺带把载荷带进失败信息。
impl core::fmt::Debug for RaceOutcome_ {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RaceOutcome_::Bound => f.write_str("Bound"),
            RaceOutcome_::DockInUse => f.write_str("DockInUse"),
            RaceOutcome_::Other(err) => write!(f, "Other({err:?})"),
            RaceOutcome_::Panicked(msg) => write!(f, "Panicked({msg:?})"),
        }
    }
}

/// 一次**自动分配**竞争的结果：`DockBinding` 在本线程内丢弃，只回传实际拿到的 dock。
enum AutoOutcome_ {
    /// 抢到了绑定，并回传由连接自行安排的那个 dock。
    Bound(Dock),

    /// 自动分配失败：候选远未用尽，不该发生。
    Failed(BindError),

    /// 竞争路径 panic（**回归信号**：合法并发竞争不得 panic）。
    Panicked(String),
}

/// 手写 `Debug` 的理由同 [`RaceOutcome_`]：把载荷带进断言失败信息。
impl core::fmt::Debug for AutoOutcome_ {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            AutoOutcome_::Bound(dock) => write!(f, "Bound({dock:?})"),
            AutoOutcome_::Failed(err) => write!(f, "Failed({err:?})"),
            AutoOutcome_::Panicked(msg) => write!(f, "Panicked({msg:?})"),
        }
    }
}
/// 在当前线程新建一个 **tokio** 运行时（竞争线程各自一个）。
fn new_tokio_rt_() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio 运行时应当创建成功")
}

/// 测试目标：同一 `MuxCore` 的两个 `MuxConnection` 克隆分处两条线程、各自在独立的
/// **tokio** 运行时上并发 `bind_async` 同一个 dock 时，必须恰好一个成功、另一个报
/// `BindError::DockInUse`；既不允许两个都成功（身份被写坏），也不允许以 panic 收场
/// （合法竞争不是临界区重入）。
///
/// - 手段：整段场景跑在主线程 `block_on(scope.run_until(..))` 里（本地队列因此全程
///   被驱动：`main_loop` 能处理工作线程投来的请求）。先用两条被动内存环完成握手并
///   建出连接（读写循环留在主线程的本地队列，跨线程的只有连接句柄）；随后逐轮起两条
///   线程，各持一份连接克隆、各自新建一个 tokio 运行时：两条线程先自旋对齐起跑线，
///   再立刻对**同一个** dock 调 `bind_async`，并把 panic 收进结果里
///   （`catch_unwind`）；随后经「已出结果 / 等对侧也出结果」两阶段汇合，最后才丢弃
///   binding（= 解绑）。主线程等两条线程报到后放行，并经 `oneshot` 异步收集两侧结果
///   （**不能**用阻塞 `join`，否则本地队列停摆）。每轮换一个新的 dock。
/// - 判断：每一轮都必须**恰好**得到一个 `Bound` 与一个 `DockInUse`；任一轮出现
///   「两个都成功 / 两个都失败 / 其它错误 / panic」即判失败，并在断言消息里指出轮次与
///   两侧结果。
///
/// Test goal: two `MuxConnection` clones of the same core, on separate threads with
/// independent tokio runtimes, racing `bind_async` on the same dock must yield
/// exactly one `Bound` and one `BindError::DockInUse` (never two successes, never a
/// panic).
/// Method: the whole scenario runs inside `block_on(scope.run_until(..))` so the main
/// thread keeps driving its `LocalSet` (and therefore `main_loop`); workers spin to a
/// shared start line, call `bind_async`, and report back through a `oneshot`.
/// Assertion: every round produces exactly one `Bound` + one `DockInUse`, with the
/// round number and both outcomes in the failure message.
#[test]
fn mux_bind_cross_thread_is_exclusive_tokio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    // 建连与全部竞争都在同一段 `run_until` 里：`block_on` 驱动它时，`LocalSet` 会
    // 在场景 future 处于 `Pending` 期间持续被 poll —— 这正是 `main_loop` 处理工作
    // 线程投来的 `bind` 请求所需要的宿主。
    let tokio_rt = new_tokio_rt_();
    tokio_rt.block_on(async {
        let art_rt = abs_art_tokio::current();
        let scope = art_rt.local_scope();
        scope
            .run_until(async {
                let (conn_a, _conn_b): (Conn_, Conn_) = common::connect_pair_(
                    &art_rt, &scope, a_tx, a_rx, b_tx, b_rx,
                )
                .await;

                for round in 0..K_RACE_ROUNDS {
                    let dock = Dock::new(K_FIRST_DOCK + round as u32);
                    let ready = Arc::new([AtomicBool::new(false), AtomicBool::new(false)]);
                    let start = Arc::new(AtomicBool::new(false));
                    let done = Arc::new([AtomicBool::new(false), AtomicBool::new(false)]);
                    let mut handles = Vec::with_capacity(2usize);
                    let mut results = Vec::with_capacity(2usize);

                    for idx in 0..2usize {
                        let conn = conn_a.clone();
                        let ready = Arc::clone(&ready);
                        let start = Arc::clone(&start);
                        let done = Arc::clone(&done);
                        let (outcome_tx, outcome_rx) = futures::channel::oneshot::channel();
                        results.push(outcome_rx);
                        handles.push(std::thread::spawn(move || {
                            let rt = new_tokio_rt_();
                            let raced = catch_unwind(AssertUnwindSafe(|| {
                                rt.block_on(async move {
                                    ready[idx].store(true, Ordering::Release);
                                    wait_flag_(&start);
                                    conn.bind_async(dock).await
                                })
                            }));

                            done[idx].store(true, Ordering::Release);
                            wait_flag_(&done[1usize - idx]);

                            let outcome = match raced {
                                Result::Ok(Result::Ok(binding)) => {
                                    drop(binding);
                                    RaceOutcome_::Bound
                                }
                                Result::Ok(Result::Err(BindError::DockInUse)) => {
                                    RaceOutcome_::DockInUse
                                }
                                Result::Ok(Result::Err(other)) => RaceOutcome_::Other(other),
                                Result::Err(payload) => {
                                    RaceOutcome_::Panicked(panic_message_(payload))
                                }
                            };
                            let _ = outcome_tx.send(outcome);
                        }));
                    }

                    // 两条线程都站上起跑线后再放行；有界等待的理由同 [`wait_flag_`]。
                    wait_flag_(&ready[0]);
                    wait_flag_(&ready[1]);
                    start.store(true, Ordering::Release);

                    let mut outcomes = Vec::with_capacity(2usize);
                    for rx in results {
                        outcomes.push(rx.await.unwrap_or_else(|_| {
                            RaceOutcome_::Panicked("工作线程在回传结果前消失".to_owned())
                        }));
                    }
                    for handle in handles {
                        handle.join().expect("竞争线程不应 panic");
                    }

                    assert!(
                        matches!(
                            (&outcomes[0], &outcomes[1]),
                            (RaceOutcome_::Bound, RaceOutcome_::DockInUse)
                                | (RaceOutcome_::DockInUse, RaceOutcome_::Bound)
                        ),
                        "第 {round} 轮（dock {dock:?}）的两条线程并发绑定未表现为\
                         「一个成功 + 一个 DockInUse」：{outcomes:?}"
                    );
                }
            })
            .await
    });
}

/// 测试目标：`DockBinding` 的 `Drop` **不取锁、不阻塞**，只向核心投递释放消息；而
/// 重绑的那次 `bind_async` 会先把释放邮箱清空，因此「另一条线程丢弃 binding 后，
/// 本线程立刻重绑同一个 dock」仍然**确定**成功。
///
/// - 手段：整段场景跑在主线程 `block_on(scope.run_until(..))` 里（本地队列因此全程
///   被驱动）。主线程建连后起一条线程 A（自带 tokio 运行时）：A 绑定 dock、置
///   「已绑定」标志，等主线程放行后才丢弃 binding，再置「已丢弃」标志并经 `oneshot`
///   回传退出。主线程在「已绑定」之后先对同一个 dock 调 `bind_async`（必须报
///   `DockInUse`），然后放行 A、等「已丢弃」标志，最后**立刻**再次 `bind_async`
///   （不得等任何循环被单独调度）。
/// - 判断：主线程第一次绑定必须是 `BindError::DockInUse`；A 丢弃之后主线程的第二次
///   绑定必须成功。任一不满足即 panic。
///
/// Test goal: dropping a `DockBinding` on one thread (its `Drop` only posts a release
/// message) must be visible immediately: another thread rebinding the same dock
/// succeeds right after the drop.
/// Method: the scenario runs inside `block_on(scope.run_until(..))` so `main_loop`
/// keeps running; thread A binds, main rebinds (`DockInUse`), A drops, main rebinds.
/// Assertion: first rebind returns `DockInUse`, second returns `Ok`.
#[test]
fn mux_rebind_after_cross_thread_drop_tokio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    // 同 [`mux_bind_cross_thread_is_exclusive_tokio_`]：场景整体在 `run_until` 内，
    // 由 `block_on` 驱动本地队列，`main_loop` 才能在场景 future 挂起时处理 bind 请求。
    let tokio_rt = new_tokio_rt_();
    tokio_rt.block_on(async {
        let art_rt = abs_art_tokio::current();
        let scope = art_rt.local_scope();
        scope
            .run_until(async {
                let (conn_a, _conn_b): (Conn_, Conn_) = common::connect_pair_(
                    &art_rt, &scope, a_tx, a_rx, b_tx, b_rx,
                )
                .await;

                let dock = Dock::new(K_FIRST_DOCK);
                let bound = Arc::new(AtomicBool::new(false));
                let release = Arc::new(AtomicBool::new(false));
                let dropped = Arc::new(AtomicBool::new(false));

                let (exited_tx, exited_rx) = futures::channel::oneshot::channel::<()>();
                let holder = {
                    let conn = conn_a.clone();
                    let bound = Arc::clone(&bound);
                    let release = Arc::clone(&release);
                    let dropped = Arc::clone(&dropped);
                    std::thread::spawn(move || {
                        let rt = new_tokio_rt_();
                        let binding = rt
                            .block_on(async { conn.bind_async(dock).await })
                            .expect("线程 A 上的首次绑定应当成功");
                        bound.store(true, Ordering::Release);
                        wait_flag_(&release);
                        // 丢弃即投递释放消息（不取锁）；随后才公布「已丢弃」。
                        drop(binding);
                        dropped.store(true, Ordering::Release);
                        let _ = exited_tx.send(());
                    })
                };

                wait_flag_(&bound);
                // 同一个 dock 已被另一条线程占用：本线程必须看到 `DockInUse`。
                let taken = conn_a.bind_async(dock).await.err();
                assert_eq!(
                    taken,
                    Option::Some(BindError::DockInUse),
                    "另一条线程持有时，本线程绑定应当报 DockInUse"
                );

                // 放行 A 丢弃 binding；等它公布「已丢弃」后**立刻**重绑，不 await 任何
                // 循环推进。`dropped` 由 A 在 `drop(binding)` 之后置位，而释放消息的
                // 落实发生在下一次 `bind_async` 的请求处理里（无需先单独跑一轮）。
                release.store(true, Ordering::Release);
                wait_flag_(&dropped);
                let rebound = conn_a.bind_async(dock).await.err();
                assert!(
                    rebound.is_none(),
                    "跨线程丢弃 binding 后立刻重绑应当成功（释放消息需在 bind 前被清空），\
                     实际错误 = {rebound:?}"
                );

                // 让 holder 线程确实先退出，再 join（此时它已回传，不会再等主循环）。
                let _ = exited_rx.await;
                holder.join().expect("竞争线程不应 panic");
            })
            .await
    });
}

/// 测试目标：两条线程**同时**用 `bind_async(Dock::unspecified())` 索要一个空闲 dock
/// 时，「找一个空闲的」与「占住它」必须是一次**原子**操作——两条都必须成功，且拿到
/// 的 dock **互不相同**。
///
/// - 手段：整段场景跑在主线程 `block_on(scope.run_until(..))` 里（本地队列因此全程
///   被驱动）。主线程建连后逐轮起两条线程，各持一份连接克隆、各自新建一个 tokio
///   运行时：两条线程先自旋对齐起跑线，再立刻调 `bind_async(Dock::unspecified())`；
///   拿到 binding 后先把 dock 值记下来并公布「本轮已出结果」，**等对侧也公布完**才
///   丢弃 binding（两边在整个竞争窗口里都持有各自的 binding，避免「一边先放掉、另一
///   边于是合法复用」这种时序假象）。主线程等两条线程报到后放行，并经 `oneshot`
///   异步收集两侧结果（**不能**用阻塞 `join`）。
/// - 判断：每一轮两条线程都必须成功，且两个 dock **不相等**。若实现把「查空闲」与
///   「占位」拆成两个临界区，两条线程就可能选中同一个 dock（表现为「两个成功的 dock
///   相同」，或一方据此报 `DockInUse`），本用例即失败。任一轮不满足即在断言消息里指出
///   轮次与两侧结果。
///
/// Test goal: concurrent `bind_async(Dock::unspecified())` from two threads must be
/// atomic — both succeed and receive distinct docks.
/// Method: the scenario runs inside `block_on(scope.run_until(..))` so `main_loop`
/// keeps running; two workers race on a shared start line, keep their bindings until
/// both have reported, then report via `oneshot`.
/// Assertion: every round both succeed with different docks; failures name the round
/// and both outcomes.
#[test]
fn mux_auto_dock_allocation_is_atomic_tokio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    // 建连 + 全部竞争都在同一段 `run_until` 内（理由见文件头「为什么用 block_on」）。
    let tokio_rt = new_tokio_rt_();
    tokio_rt.block_on(async {
        let art_rt = abs_art_tokio::current();
        let scope = art_rt.local_scope();
        scope
            .run_until(async {
                let (conn_a, _conn_b): (Conn_, Conn_) = common::connect_pair_(
                    &art_rt, &scope, a_tx, a_rx, b_tx, b_rx,
                )
                .await;

                for round in 0..K_RACE_ROUNDS {
                    let ready = Arc::new([AtomicBool::new(false), AtomicBool::new(false)]);
                    let start = Arc::new(AtomicBool::new(false));
                    let done = Arc::new([AtomicBool::new(false), AtomicBool::new(false)]);
                    let mut handles = Vec::with_capacity(2usize);
                    let mut results = Vec::with_capacity(2usize);

                    for idx in 0..2usize {
                        let conn = conn_a.clone();
                        let ready = Arc::clone(&ready);
                        let start = Arc::clone(&start);
                        let done = Arc::clone(&done);
                        let (outcome_tx, outcome_rx) = futures::channel::oneshot::channel();
                        results.push(outcome_rx);
                        handles.push(std::thread::spawn(move || {
                            let rt = new_tokio_rt_();
                            let raced = catch_unwind(AssertUnwindSafe(|| {
                                let bound = rt.block_on(async move {
                                    ready[idx].store(true, Ordering::Release);
                                    wait_flag_(&start);
                                    conn.bind_async(Dock::unspecified()).await
                                });
                                // 两阶段汇合：先定格结果并公布，再等对侧也公布完，最后
                                // 才解绑。
                                match bound {
                                    Result::Ok(binding) => {
                                        let dock = *binding.local_dock();
                                        done[idx].store(true, Ordering::Release);
                                        wait_flag_(&done[1usize - idx]);
                                        drop(binding);
                                        AutoOutcome_::Bound(dock)
                                    }
                                    Result::Err(err) => {
                                        done[idx].store(true, Ordering::Release);
                                        wait_flag_(&done[1usize - idx]);
                                        AutoOutcome_::Failed(err)
                                    }
                                }
                            }));
                            let outcome = match raced {
                                Result::Ok(outcome) => outcome,
                                Result::Err(payload) => {
                                    AutoOutcome_::Panicked(panic_message_(payload))
                                }
                            };
                            let _ = outcome_tx.send(outcome);
                        }));
                    }

                    // 两条线程都站上起跑线后再放行；有界等待的理由同 [`wait_flag_`]。
                    wait_flag_(&ready[0]);
                    wait_flag_(&ready[1]);
                    start.store(true, Ordering::Release);

                    let mut outcomes = Vec::with_capacity(2usize);
                    for rx in results {
                        outcomes.push(rx.await.unwrap_or_else(|_| {
                            AutoOutcome_::Panicked("工作线程在回传结果前消失".to_owned())
                        }));
                    }
                    for handle in handles {
                        handle.join().expect("竞争线程不应 panic");
                    }

                    match (&outcomes[0], &outcomes[1]) {
                        (AutoOutcome_::Bound(a), AutoOutcome_::Bound(b)) => assert_ne!(
                            a, b,
                            "第 {round} 轮两条线程拿到了同一个 dock {a:?}：\
                             「找空闲」与「占住」不是一次原子操作"
                        ),
                        _ => panic!(
                            "第 {round} 轮两条线程并发自动分配未表现为「两个都成功且各不相同」：\
                             {outcomes:?}"
                        ),
                    }
                }
            })
            .await
    });
}
