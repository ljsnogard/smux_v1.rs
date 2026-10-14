//! 待交付连接的队列：**基于 [`buffex::ring`]**，元素就是交付物本身。
//!
//! # 结构
//!
//! ```text
//!   生产者（多）                                     消费者（单）
//!        |                                                |
//!        v                                                v
//!   CooperativeRwLock<RingWriter>  ──►  Ring<_, T>  ──►  RingReader
//!        写权由读写锁仲裁              环里直接放交付物 T       独占读端
//! ```
//!
//! 只有三样东西，没有中间层：
//!
//! - **环里直接放交付物 `T`**，既不包 `Option<T>`、也不包 `Box<T>`。交付物是 **move
//!   语义**的：进环是「把值放进去」，出环是「把值取走」，中间不存在「同一格里的值需要被
//!   读取两次」或「需要在元素类型上区分已初始化与未初始化」的需求——`buffex::ring` 的
//!   存储本来就按 `MaybeUninit<T>` 组织，格子的初始化与否由读写位置（`rp` / `wp`）表达，
//!   再叠一层标志位纯属冗余。
//! - **写权交给 [`CooperativeRwLockOwned`]**：`buffex::ring` 的契约是 SPSC，而生产者有多
//!   个（每个接受槽位、每条握手任务都会投递）。读写锁把「谁在写环」仲裁成单入口。
//! - **容量即背压**：环的容量取
//!   [`TrListenerConfig::max_inflight`](crate::config::TrListenerConfig::max_inflight)。
//!   环满时生产者 park 等待（`RingWriter::write_async`），不自旋、也不丢包。
//!
//! # 搬移两端的语义由谁承接
//!
//! 元素进出都走 `abs_buff` 段类型上的**搬移原语**，而不是手工操作 `MaybeUninit`：
//!
//! | 方向 | 入口 | 记账 |
//! | --- | --- | --- |
//! | 进环 | `ReclSliceMut::move_items_from_buff` | 段 drop 时按已搬入量提交 `wp` |
//! | 出环 | `TrBuffSegmRef::move_items_to_buff` | 段 drop 时按已搬出量提交 `rp` |
//!
//! 这条选择消掉了原设计里那层 `Box<T>`：搬移是**位拷贝**，而两端都用
//! [`MaybeUninit`] 装载——它**不会自动析构**，因此「值被搬走之后源处不得再次析构」这件事
//! 由类型系统直接兑现，不需要装箱、也不需要 `Option::take` 这类借用体操：
//!
//! - 写侧：值先落进栈上的 `[MaybeUninit<T>; 1]`，搬入环后那个 `MaybeUninit` 随作用域结束
//!   被丢弃，但它不析构——环自此独占这份所有权；
//! - 读侧：搬进栈上的 `MaybeUninit<T>`，再用一次 `assume_init` 把所有权取出来。这是正常
//!   路径上唯一的一处 `unsafe`：`moved == 1` 恰好证明了那一格已被写入（另一处在写侧
//!   「一格都搬不进去」这个不可达分支里，只为把值取回来析构）。
//!
//! 环本身的存储体是 `Owned<[MaybeUninit<T>], CoreAlloc>`，`Ring` 不实现 `Drop`、也不析构
//! 元素，因此「被搬空的格子」不会在队列析构时被二次析构。代价是：写进环后从未被取走的
//! 值不会被析构（只泄漏、不 UB）——这与「listener 被丢弃时在途连接随之放弃」的语义一致。
//!
//! # 为什么写侧是读写锁，而不是互斥锁
//!
//! 生产者对环的访问是**短小的独占写**（搬一格），而消费者在**独立的读端**上推进读位置。
//! 读写锁在把写权串行化的同时，不把消费者挡在外面——而「消费者推进读位置」恰恰是写端从
//! 「满」变回「有空位」的**唯一**途径。
//!
//! # 写权为什么必须覆盖 park：SPSC 契约的等待槽
//!
//! `buffex::ring` 的等待协议**每个方向只有一个单值信箱**：环内部各存一份
//! [`Producer`](smux_v1::x_deps::buffex::ring::Producer) /
//! [`Consumer`](smux_v1::x_deps::buffex::ring::Consumer)，登记落在它上面。因此：
//!
//! - **写侧的 park 必须在写权之内**：否则多个生产者会同时往同一个信箱登记，先登记的
//!   waker 会被后来的覆盖；更糟的是消费者推进读位置时是在**环内部那一份** `Producer` 上
//!   唤醒，而各写端若各持一份自己 `new` 出来的 `Producer`，唤醒就落在没有登记者的信箱上，
//!   投递端将永久睡下去、消费者也就再也等不到东西。
//! - 反过来说，写锁把「同一时刻只有一个写者」变成不变式之后，环内部的单值信箱恰好够用，
//!   这正是 SPSC 契约与多生产者模型能共存的原因。
//!
//! 由此还得到一条推论：**park 期间持写权不会死锁**。让写端从「满」变回来的只有消费者，
//! 而消费者走独立读端、不需要写权；持写权 park 只是把其他写者挡在锁外（它们随后照常排队），
//! 不会挡住让条件成立的那一方。
//!
//! # 挂起与唤醒
//!
//! 两侧都用 `buffex::ring` 现成的 park 协议，不自建 waker 槽：
//!
//! - 生产者：`RingWriter::write_async`（等可写空间）；
//! - 消费者：`RingReader::read_async`（等数据）。
//!
//! 两者的等待体都实现了
//! [`TrMayCancel`](smux_v1::x_deps::abs_cancel::TrMayCancel)，因此取消令牌直接透传——
//! listener 被关闭时卡的正是这两处等待，它们必须能被取消。

use alloc::sync::Arc;
use core::mem::MaybeUninit;

use abs_buff::{Demand, buffer::TrBuffSegmRef};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_mm::CoreAlloc;
use abs_sync::preludes::TrAsyncRwLock;
use atomic_sync::rwlock::cooperative::CooperativeRwLockOwned;
use buffex::ring::{ConsumerError, Ring, RingReader, RingWriter};
use mm_ptr::Owned;
use smux_v1::x_deps::{
    abs_buff, abs_cancel, abs_mm, abs_sync, atomic_sync, buffex, mm_ptr,
};

/// 环的存储体：一块 `MaybeUninit<T>`。
type RingBuf_<T> = Owned<[MaybeUninit<T>], CoreAlloc>;

/// 环本身；元素就是交付物 `T`。
type Ring_<T> = Ring<RingBuf_<T>, T>;

/// 环的写端（持有一份环的 `Arc`，因此与队列同寿命）。
type Writer_<T> = RingWriter<Arc<Ring_<T>>, RingBuf_<T>, T>;

/// 环的读端。
type Reader_<T> = RingReader<Arc<Ring_<T>>, RingBuf_<T>, T>;

/// 写权：把「谁在写环」仲裁成单入口。
type WriteLock_<T> = CooperativeRwLockOwned<Writer_<T>>;

/// 投递失败：等待被取消（listener 正在收摊），或消费端已经消失。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("投递待交付连接失败：等待被取消，或消费端已经消失")]
pub struct Full_;

/// 取出失败：等待被取消（队列非空与否无关，下次可以接着等）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("等待待交付连接被取消")]
pub struct Empty_;

/// **生产端**：把已握手的连接投进队列。
///
/// 每个接受槽位、每条握手任务各持一份克隆；它们共享同一条环与同一把写锁。
pub struct Sender_<T> {
    /// 写权：既是「谁在写环」的仲裁者，也是「写侧 park 至多一个登记者」这条不变式的
    /// 来源（见模块文档）。
    ///
    /// 每份生产端各持一个 `Arc` 克隆，因此**它的强引用计数就是生产端的份数**——不需要
    /// 再单独分配一个计数器来记账。
    write_: Arc<WriteLock_<T>>,
}

impl<T> Clone for Sender_<T> {
    /// 克隆即「多一个生产者」。份数由 `write_` 的强引用计数承载，因此这里只需克隆写权。
    fn clone(&self) -> Self {
        Sender_ {
            write_: self.write_.clone(),
        }
    }
}

impl<T> Drop for Sender_<T> {
    fn drop(&mut self) {
        // 「我是不是最后一个生产端」由写权的强引用计数回答：计数里除自己之外还有人，
        // 就说明写端仍有人在用。判据成立的前提是**克隆只发生在独占借用下**（本 crate 里
        // 只有 `next_async` 持 `&mut self` 补槽位时才克隆），因此这里读到的计数不会因为
        // 并发的 clone 而偏小。
        if Arc::strong_count(&self.write_) > 1usize {
            return;
        }
        // 最后一个生产者退出时关闭写端。这一步是**必需的**：`buffex::ring` 的写端一关，
        // 等待中的消费者会被唤醒并拿到 `ConsumerError::Closing`，`pop` 据此回报「不会再有
        // 东西进来了」。少了它，一个已停摆的 listener 会让 `next_async` 永久挂起。
        //
        // 此刻没有别的写者能持有写权：`push` 必须借 `&self`，其等待体一旦存在就说明本份
        // 尚未 drop、计数尚未归 1。于是这次 `try_write` 必然成功。
        let mut session = self.write_.acq_session();
        if let Result::Ok(mut writer) = session.try_write() {
            RingWriter::close(&mut *writer);
        }
    }
}

impl<T> Sender_<T> {
    /// 立刻关闭写端：此后不再接受投递，等待中的消费者会被唤醒并拿到「不会再有东西」。
    ///
    /// 与 [`Drop`](Sender_) 里的关闭是同一件事，区别是它**不需要**「我是最后一个生产端」
    /// 这个前提——[`close_async`](crate::listening::SmuxListener::close_async) 用它收尾：
    /// 那时 listener 自己那一份投递端仍然活着，写端不会自行关闭。
    ///
    /// 幂等。
    pub fn close_(&self) {
        // 此刻所有投递任务都已退出（`close_async` 是等它们收尾之后才调用的），因此没有
        // 别的写者会占着写权，这次 `try_write` 必然成功。
        let mut session = self.write_.acq_session();
        if let Result::Ok(mut writer) = session.try_write() {
            RingWriter::close(&mut *writer);
        }
    }

    /// 投递一条已握手的连接。
    ///
    /// # 语义
    ///
    /// - 环有空位：搬进环并提交，返回 `Ok(())`；
    /// - 环已满：park 等消费者腾出空位（**这就是背压**），或等到 `cancel` 触发；
    /// - 等写权或等空位时被取消：返回 `Err(Full_)`，且**不**写入任何东西。
    pub async fn push<K>(&self, value: T, cancel: K) -> Result<(), Full_>
    where
        K: TrCancellationToken + Clone,
    {
        // 1. 先取写权，再碰环。写权一旦到手就持有到本次投递结束（**包含等空位的 park
        //    期间**）：环内部那一份写侧等待槽是单值的，写权正是「同一时刻至多一个登记
        //    者」这条不变式的来源（见模块文档）。
        let mut session = self.write_.acq_session();
        let mut writer = match session.write_async().may_cancel_with(cancel.clone()).await {
            Result::Ok(guard) => guard,
            // 等写权时被取消（或写端已关）：这次投递放弃。
            Result::Err(_) => return Result::Err(Full_),
        };

        // 2. 等一格可写。`RingWriter::write_async` 本身就是「试写 → park → 重试」的
        //    循环，且 park 用的正是**环内部**那一份 `Producer`——也就是消费者推进读
        //    位置时会去唤醒的那一份，因此这里不需要自建等待槽、也不需要重试循环。
        let demand = Demand::exactly(1usize);
        let waited = writer.write_async(&demand).may_cancel_with(cancel).await;
        let Some(mut segm) = waited.pick_left() else {
            // 右半：本次等待被取消、写端已关、或请求不可满足。都按「这次投不进去」处理。
            return Result::Err(Full_);
        };

        // 3. 搬一格。值先落到栈上的 `MaybeUninit`，再由段的搬移入口搬进环。
        //
        // 这里**不需要** `unsafe`：`MaybeUninit` 不析构，搬移（位拷贝）之后源处仍在栈上、
        // 但随作用域结束被丢弃时什么也不做——环自此独占这份所有权。
        let slot = [MaybeUninit::new(value)];
        let moved = segm.move_items_from_buff(&slot);
        // 段在此 drop：按已搬入量提交，推进写位置并唤醒等待中的消费者。
        drop(segm);
        if moved == 1usize {
            return Result::Ok(());
        }
        // 写段只可能由满足 `Demand` 的请求交出，所以「一格都搬不进去」不可达；万一发生，
        // 值仍留在 `slot` 里（`MaybeUninit` 不析构），把所有权取回来交给调用方析构。
        //
        // SAFETY: `moved == 0` 说明上面那次搬移一个元素都没搬走，`slot[0]` 仍是写入时的
        // 那个 `T`；它在本次调用中从未被取出过。
        #[allow(unsafe_code)]
        drop(unsafe { slot[0].assume_init_read() });
        Result::Err(Full_)
    }
}

/// **消费端**：取出下一条已握手的连接。
pub struct Receiver_<T> {
    /// 环的读端。只有一个消费者，因此不需要锁。
    reader_: Reader_<T>,
}

impl<T> Drop for Receiver_<T> {
    fn drop(&mut self) {
        // 消费端消失后，任何仍在 park 等空位的生产者都必须被唤醒，否则它们会守着一条
        // 再也不会有人来取的环睡下去。关闭消费端会让 `write_async` 以
        // `ProducerError::Closing` 结束，投递端据此放弃这条连接。
        self.reader_.close();
    }
}

impl<T> Receiver_<T> {
    /// 队列里现在有几条。
    ///
    /// `next_async` 用它判断「是否该补一个接受槽位」。它是**瞬时值**：查到非空之后可能
    /// 立刻被取走，但那个方向的误差是安全的（最坏少补一个槽位，下一次调用就会补）。
    pub fn len_(&self) -> usize {
        self.reader_.ring_state().data_size()
    }

    /// 取出下一条连接。
    ///
    /// - `Ok(Some(value))`：取到一条；
    /// - `Ok(None)`：**生产端已全部关闭**且环已空——不会再有东西进来了；
    /// - `Err(Empty_)`：本次等待被取消（下次可以接着等）。
    pub async fn pop<K>(&mut self, cancel: K) -> Result<Option<T>, Empty_>
    where
        K: TrCancellationToken + Clone,
    {
        if let Option::Some(value) = self.take_now_() {
            return Result::Ok(Option::Some(value));
        }
        // 环空：用 ring 自带的 park 协议（登记 → 发布 → 复检）等生产者推进一条。
        //
        // 这一段放在块里：`read_async` 交出的等待体借用了 `self.reader_`，而「醒来后再读
        // 一次」还需要同一份可变借用。先取出结论、让等待体随块结束而 drop，两处借用就
        // 不重叠了。
        let stopped = {
            let demand = Demand::exactly(1usize);
            let waited = self
                .reader_
                .read_async(&demand)
                .may_cancel_with(cancel)
                .await;
            if waited.contains_left() {
                // 有数据可读。
                false
            } else if let Option::Some(err) = waited.pick_right() {
                // 只有「生产端已全部关闭」算终结；其余（含 `Cancelled`、`Closed`）都按
                // 「这一次没取到」处理，调用方下次可以接着等。
                matches!(err, ConsumerError::Closing)
            } else {
                false
            }
        };
        // 段已随上面的块 drop（提交消费量、唤醒生产者），现在可以安全地再借一次读端。
        match (stopped, self.take_now_()) {
            // 取到了：这是普通情形。
            (_, Option::Some(value)) => Result::Ok(Option::Some(value)),
            // 生产端全关且没有残留数据：终结。
            (true, Option::None) => Result::Ok(Option::None),
            // 其余（被取消，或醒来但没读到）：报「这一次没取到」。
            (false, Option::None) => Result::Err(Empty_),
        }
    }

    /// 立刻搬一格出来；空返回 `None`。
    fn take_now_(&mut self) -> Option<T> {
        let demand = Demand::exactly(1usize);
        let mut segm = self.reader_.try_read(&demand).pick_left()?;
        // 搬移目标：栈上的一格 `MaybeUninit`。它不会自动析构，因此「源格被搬空后不得再次
        // 析构」由类型本身保证，不需要 `unsafe`。
        let mut slot = MaybeUninit::<T>::uninit();
        // 走 `TrBuffSegmRef` 的搬移入口：它按 move 语义逐段把元素搬进 `dst`，并推进本段的
        // 已消费量（内部分段经子段回收器累计）。
        let moved = TrBuffSegmRef::move_items_to_buff(&mut segm, core::slice::from_mut(&mut slot));
        // 段在此 drop：按已搬出量提交，推进读位置并唤醒等待空位的生产者。
        drop(segm);
        if moved == 0usize {
            return Option::None;
        }
        // SAFETY: `moved == 1` 表示上面那次搬移确实把一格写进了 `slot`；`slot` 只在这里被
        // 取出一次，而环里那一格已随段的 drop 记入已消费量，不会再被交回来。
        #[allow(unsafe_code)]
        Option::Some(unsafe { slot.assume_init() })
    }
}

/// **待交付连接的队列**。
///
/// 由 [`RingQueue_::new_`] 造出，再由 [`RingQueue_::split_`] 拆成生产端与消费端。
pub struct RingQueue_<T> {
    /// 环。
    ring_: Arc<Ring_<T>>,
}

impl<T> RingQueue_<T> {
    /// 按容量造一条队列。
    ///
    /// `capacity` 是环的容量，也就是「同时在途的连接条数」上限。
    ///
    /// # Panics
    ///
    /// `capacity` 不在 `buffex::ring` 允许区间内时 panic。它来自
    /// [`TrListenerConfig::max_inflight`](crate::config::TrListenerConfig::max_inflight)，
    /// 已被归一化为至少 1，因此这属于**配置错误**。
    pub fn new_(capacity: usize) -> Self {
        let buffer = Owned::new_uninit_slice(capacity, CoreAlloc);
        let ring = Arc::new(Ring_::try_new(buffer).unwrap_or_else(|cap| {
            panic!(
                "待交付连接队列的容量 {cap} 不在 buffex::ring 允许区间内；\
                 它来自 TrListenerConfig::max_inflight，属配置错误"
            )
        }));
        RingQueue_ { ring_: ring }
    }

    /// 拆成 `(生产端, 消费端)`。
    ///
    /// # 这里为什么需要一次 `unsafe`
    ///
    /// `buffex::ring` 只提供两条拆分入口：`Ring::split`（借环，半部的生命周期不得超过
    /// 环）与 `Ring::split_unchecked`（环由智能指针持有，`unsafe`）。本队列要让半部与环
    /// 同住一个结构体、再一起被 `Arc` 共享，即半部的生命周期必须与环自身等长，因此只能
    /// 用后者。这一处 `unsafe` 只涉及**环的拆分**，不涉及任何元素的所有权转移。
    ///
    /// 它的两条安全前提由本方法一次性兑现：
    ///
    /// - **环被智能指针独占**：传入的 `Arc` 是刚 `Arc::new` 出来的唯一强引用
    ///   （`Weak` 计数为 0，本 crate 从不制造弱引用）；
    /// - **不存在从弱指针升级**：同上。
    ///
    /// SPSC 的另一半——「写端 / 读端各只有一份」——由本方法的**消费性**（`self` 按值）
    /// 与 [`Sender_`] 对写端的读写锁共同保证。
    pub fn split_(self) -> (Sender_<T>, Receiver_<T>) {
        // SAFETY: 见方法文档的两条前提；本方法按值消费 `self`，因此不可能被调用两次。
        let (writer, reader) = unsafe { Ring_::split_unchecked(self.ring_.clone()) };
        (
            Sender_ {
                write_: Arc::new(CooperativeRwLockOwned::new_owned(writer)),
            },
            Receiver_ { reader_: reader },
        )
    }
}
