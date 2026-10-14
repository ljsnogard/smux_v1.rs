//! [`queue_`](super::queue_) 的单元测试：多生产者、背压与取消三条路径。
//!
//! # 为什么单独测队列
//!
//! 端到端验收（`tests/listen_*.rs`）每次只交付**一条**连接，因此它证明不了队列本身的
//! 并发性质：多个投递端如何共享写权、环满时投递端会不会真的 park、取消等待时会不会
//! 污染环状态。这三件事恰好是本轮把队列改成「`buffex::ring` + 写权仲裁」之后的全部
//! 风险点，所以在这里用**中立执行器**（`futures-executor`）单独压一遍——不牵扯任何
//! `rt-*` 后端。

use core::sync::atomic::{AtomicBool, Ordering};
use std::{sync::Arc, thread, time::Duration};

use smux_v1::x_deps::abs_cancel::{CancelledToken, NonCancellableToken};

use super::queue_::{Full_, RingQueue_};

/// 验证多生产者并发投递时，交付物被逐条搬出，既不重复也不丢失。
///
/// - 手段：容量 4 的队列上起 2 条线程，各投递 2 个互不相同的 `usize`（合起来是
///   `0..4`）；先 `join` 全部线程、再放下主线程自己那一份投递端，然后在主线程上连取
///   4 次。
/// - 判断：取出的 4 个值排序后逐项等于 `[0, 1, 2, 3]`（每个值恰好出现一次）；随后再
///   取一次得到 `Ok(None)`——那是「最后一个投递端已关闭写端」的语义。
///
/// Verifies that concurrent pushes by several producers are each moved out exactly once
/// — no duplicates, no losses.
///
/// - Means: spawn two threads on a queue of capacity 4, each pushing two distinct
///   `usize` values (so `0..4` in total); join both threads, drop the main thread's own
///   sender, then pop four times on the main thread.
/// - Criteria: after sorting, the four popped values equal `[0, 1, 2, 3]`; one further
///   pop returns `Ok(None)`, meaning the last sender has closed the write side.
///
/// 先 `join` 再放下最后一份投递端是刻意的：关闭写端要抢写锁，若在别的投递端还在跑时
/// 抢不到，「最后一个退出者负责关闭」这条约定就会落空，本测试也就会在最后那一次取件上
/// 永久挂起。
#[test]
fn multi_producer_delivers_each_value_once_() {
    let (sender, mut receiver) = RingQueue_::<usize>::new_(4usize).split_();
    let mut workers = Vec::new();
    for lane in 0..2usize {
        let sender = sender.clone();
        workers.push(thread::spawn(move || {
            futures_executor::block_on(async move {
                for offset in 0..2usize {
                    sender
                        .push(lane * 2usize + offset, NonCancellableToken::new())
                        .await
                        .expect("队列未关闭、未取消时投递必然成功");
                }
            });
        }));
    }
    for worker in workers {
        worker.join().expect("投递线程不应 panic");
    }
    drop(sender);

    let mut got = futures_executor::block_on(async {
        let mut got = Vec::new();
        for _ in 0..4usize {
            let value = receiver
                .pop(NonCancellableToken::new())
                .await
                .expect("未被取消时取件必然成功")
                .expect("投递了 4 条，前 4 次取件必然拿到值");
            got.push(value);
        }
        got
    });
    got.sort_unstable();
    assert_eq!(got, vec![0usize, 1usize, 2usize, 3usize]);

    let tail = futures_executor::block_on(receiver.pop(NonCancellableToken::new()));
    assert!(
        matches!(tail, Result::Ok(Option::None)),
        "所有投递端退出后，队列应当终结为 Ok(None)，实际为 {tail:?}"
    );
}

/// 验证环满时的背压：投递端 park 等待，直到消费端取走一条才被唤醒。
///
/// - 手段：容量 1 的队列先由主线程投满一条；随后在另一条线程上投递第二条，并在它成功
///   后置位一个原子标记。主线程等 100 ms，检查标记仍为 `false`（说明第二条确实被挡在
///   park 上），然后取走第一条，再 `join` 那条线程。
/// - 判断：等待期间标记必须为 `false`（否则说明环满时投递端没等，背压失效）；取走一条
///   后标记必须为 `true`，且两次取件拿到的值恰好是 `1`、`2`。
///
/// Verifies back-pressure: with a full ring, a producer parks until the consumer takes
/// one item out.
///
/// - Means: fill a capacity-1 queue from the main thread; push a second item from
///   another thread and record its completion in an atomic flag. The main thread waits
///   100 ms, checks the flag is still `false` (the second push is parked), pops the first
///   item, then joins that thread.
/// - Criteria: the flag must be `false` while waiting (otherwise nothing parked and
///   back-pressure is broken), and `true` after one item is popped; the two popped
///   values are exactly `1` and `2`.
#[test]
fn full_queue_applies_backpressure_() {
    let (sender, mut receiver) = RingQueue_::<usize>::new_(1usize).split_();
    futures_executor::block_on(async {
        sender
            .push(1usize, NonCancellableToken::new())
            .await
            .expect("首条投递成功");
    });

    let done = Arc::new(AtomicBool::new(false));
    let flag = done.clone();
    let sender_in_thread = sender.clone();
    let worker = thread::spawn(move || {
        futures_executor::block_on(async move {
            sender_in_thread
                .push(2usize, NonCancellableToken::new())
                .await
                .expect("第二条投递成功");
            flag.store(true, Ordering::SeqCst);
        });
    });

    // 100 ms 足够任何调度下的 park 完成登记；此处的等待方向是**安全**的：即便线程还没
    // 跑到 push，标记同样为 `false`，断言不会误报。
    thread::sleep(Duration::from_millis(100));
    assert!(
        !done.load(Ordering::SeqCst),
        "环已满时投递端应当 park 等待，而不是直接返回"
    );

    let first = futures_executor::block_on(receiver.pop(NonCancellableToken::new()))
        .expect("未被取消时取件必然成功")
        .expect("首条已在环里");
    assert_eq!(first, 1usize);

    worker.join().expect("投递线程不应 panic");
    assert!(
        done.load(Ordering::SeqCst),
        "消费端取走一条后，park 的投递端应当被唤醒"
    );

    let second = futures_executor::block_on(receiver.pop(NonCancellableToken::new()))
        .expect("未被取消时取件必然成功")
        .expect("第二条已被唤醒的投递端投进来了");
    assert_eq!(second, 2usize);
}

/// 验证投递在等待空位时可以被取消：等待立刻结束、值不进环、返回 [`Full_`]。
///
/// - 手段：容量 1 的队列先投满一条（值为 `7`），再用**已取消**的令牌
///   （[`CancelledToken`]）投递第二条（值为 `8`）。
/// - 判断：第二条投递返回 `Err(Full_)`；放下投递端后连续取件只拿得到 `7`，接着便是
///   `Ok(None)`——若第二条曾经进环，`8` 必然出现在它之前。
///
/// Verifies that a push waiting for free space can be cancelled: the wait ends at once,
/// nothing enters the ring, and `Full_` is returned.
///
/// - Means: fill a capacity-1 queue with `7`, then push `8` using an already-cancelled
///   token ([`CancelledToken`]).
/// - Criteria: the second push returns `Err(Full_)`; after dropping the sender, popping
///   yields only `7` and then `Ok(None)` — had `8` entered the ring, it would show up
///   before that.
#[test]
fn cancelled_push_leaves_queue_untouched_() {
    let (sender, mut receiver) = RingQueue_::<usize>::new_(1usize).split_();
    futures_executor::block_on(async {
        sender
            .push(7usize, NonCancellableToken::new())
            .await
            .expect("首条投递成功");
        let rejected = sender.push(8usize, CancelledToken::new()).await;
        assert_eq!(
            rejected,
            Result::Err(Full_),
            "等待空位时被取消的投递应当回报 Full_"
        );
    });
    drop(sender);

    let first = futures_executor::block_on(receiver.pop(NonCancellableToken::new()))
        .expect("未被取消时取件必然成功")
        .expect("首条已在环里");
    assert_eq!(first, 7usize);

    let tail = futures_executor::block_on(receiver.pop(NonCancellableToken::new()));
    assert!(
        matches!(tail, Result::Ok(Option::None)),
        "被取消的那条不应进环，因此取完 7 之后就该终结，实际为 {tail:?}"
    );
}
