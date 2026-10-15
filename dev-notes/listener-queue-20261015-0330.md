# listener 交付队列：ring + 写权仲裁（20261015-0330）

本文记录 `smux_v1_listener` 交付队列从 `flume` 队列改为「`buffex::ring` + 写权仲裁」
这一轮重构的决策因果链。触发这次改动的两条需求是：

1. 交付物队列直接用 ring 实现，环里放的就是交付物本身——**不包 `Option<T>`、不包
   `Box<T>`**，因为交付物是 move 语义的；
2. ring 的契约是 SPSC，而投递端有多个，因此用 `atomic_sync` 的
   `CooperativeRwLock` 裁决写入权。

代码落点在 `smux_v1_listener/src/listening/queue_.rs`（模块文档比本文更贴近实现，
本文只记录「为什么是这条路」。

---

## 1. 环里直接放 `T`

### 1.1 为什么可以不放 `Option<T>` / `Box<T>`

`buffex::ring` 的存储体本来就是 `[MaybeUninit<T>]`，「某一格有没有值」由读写位置
（`rp` / `wp`）表达，不需要在**元素类型**上再叠一层「有没有值」的标志位。在元素类型上
加 `Option<T>`，等于把同一个信息存两遍，并且强迫每次进出都做一次「拆包 / 装包」。

`Option<T>` 之所以一度看起来必要，是因为「把值从 `&[T]` 里取出来」在借用层面是
move-out-of-shared-reference：

- `buffex::ring` 的读段（`RingSegmRef`）经由 `as_segm_ref` 只交出 `&[T]`；
- `Option::take` 需要 `&mut`；而绕过借用检查去写读段会撞上
  `invalid_reference_casting`。

上一版的绕法是**原子指针**：环里放 `AtomicPtr<T>`，写侧 `Box::into_raw` 装箱，读侧
`swap(ptr::null_mut())` 把所有权换回来。它确实完全不需要 `unsafe` 的位拷贝，但代价是：

- 每次交付多一次堆分配（`Box::new`）；
- 环的元素类型不再是交付物，多出一层指针解引用；
- 为了读侧的 `swap` 与写侧的空指针语义，多出「布局一致」这条隐式前提；
- 为了让「park 期间不持写权」而缓存了 `*const RingState`，于是需要
  `unsafe impl Send/Sync for Sender_<T>` 与一处裸指针解引用。

### 1.2 现在的做法：搬移原语 + `MaybeUninit`

`abs_buff` 的段类型本来就带**搬移**入口，语义就是 move：

| 方向 | 入口 | 记账方式 |
| --- | --- | --- |
| 进环 | `ReclSliceMut::move_items_from_buff(&[MaybeUninit<T>])` | 段 drop 时按已搬入量提交 `wp` |
| 出环 | `TrBuffSegmRef::move_items_to_buff(&mut [MaybeUninit<T>])` | 段 drop 时按已搬出量提交 `rp` |

两端都用 `MaybeUninit<T>` 装载，于是「值被搬走之后源处不得再次析构」这件事由类型系统
直接兑现——`MaybeUninit<T>` 的 drop 是**空操作**。因此：

- 写侧：`let slot = [MaybeUninit::new(value)];` → 搬进环 → `slot` 随作用域结束被丢弃，
  什么也不做；环自此独占所有权。**全程无 `unsafe`**。
- 读侧：`let mut slot = MaybeUninit::<T>::uninit();` → 搬出来 → 一次
  `unsafe { slot.assume_init() }`。这是整个模块**唯一**的 `unsafe`，而它的前提由
  `moved == 1` 直接给出。

## 2. 多写者 + SPSC：写权仲裁

### 2.1 写侧 park 必须在写权之内（本轮最重要的教训）

最初的实现想把 park 挪到写权之外：缓存 `RingState` 地址，每个 `Sender_` 自己 `new` 一个
`Producer`，park 期间不持锁。动机是担心「持锁 park 会把其他生产者挡在外面」。

这个设计是错的，而且错得很隐蔽：`buffex::ring` 每个方向只有**一份单值等待槽**
（`Ring::producer_` / `Ring::consumer_`）。消费者推进读位置时，`advance_read` 唤醒的是
**环内部那一份** `Producer`；而各写端自建的 `Producer` 是另外的对象——登记落在一个信箱，
唤醒落在另一个信箱上，投递端于是永久睡下去。

它被单元测试 `full_queue_applies_backpressure_` 抓出来（容量 1：投满、第二条后台投递、
取走第一条——后台线程 60 秒不醒）。

由此得到两条结论：

1. **写权必须覆盖 park**：写锁把「同一时刻只有一个写者」变成不变式之后，环内部的单值
   等待槽恰好够用。这就是 SPSC 契约与多生产者模型共存的方式。
2. **持锁 park 不会死锁**：让写端从「满」变回「有空位」的只有消费者，而消费者走独立
   读端、不需要写权。持锁只是把其他写者挡在锁外（它们在锁的等待队列里排队），不会挡住
   让条件成立的那一方。

实现上也就顺理成章：`push` 先 `session.write_async()` 取写权（可取消），再调用
`RingWriter::write_async(&demand)`——后者本身就是「试写 → park → 重试」的循环，且 park
用的是环内部那一份 `Producer`。自建的 `Producer`、缓存的状态字地址、以及随之而来的
`unsafe impl Send/Sync` 全部消失。

### 2.2 为什么写侧用读写锁而不是互斥锁

生产者对环的访问是短小的独占写（搬一格），而消费者在独立读端推进读位置。读写锁在把写权
串行化的同时不把消费者挡在外面——而「消费者推进读位置」恰恰是写端从「满」变回「有空位」
的唯一途径。用互斥锁把整条环（含读端）包起来也能正确，但会让消费被投递阻塞。

### 2.3 生产端份数不需要单独记账

「还有几个生产端活着」直接读写权的 `Arc::strong_count` 即可：`write_` 在生产端之间共享，
每一份 `Sender_` 各持一个克隆，因此它的强引用计数**就是**生产端份数。原先那个
`Arc<AtomicUsize>` 计数器是同一件事记两遍，已删除。

判据的可靠性依赖一个前提：**克隆只发生在独占借用下**——本 crate 里只有
`SmuxListener::next_async` 持 `&mut self` 补槽位时才克隆，因此 `Drop` 里读到的计数不会
因为并发的 clone 而偏小。

## 3. 顺带修掉的两处缺陷

### 3.1 写端根本没提交写位置

上一版写侧用 `segm.iter_slices_mut()` 拿到 `&mut [MaybeUninit<T>]` 直接写格，然后靠段
drop「按已写入量提交」。但 `ReclSliceMut::offset_` 只在经 `as_segm_mut` /
`take_segm_mut` 借出子段时才被累计——直接写 `iter_slices_mut` 交出的格并不会推进它，
段 drop 时提交的是 `0`，写位置不动，消费者永远读不到。

改用 `move_items_from_buff` 之后，搬移量由段自己记账，提交随之正确。

### 3.2 消费端消失后投递端会永久睡下去

`Receiver_` 被丢弃（listener 先消失）时，仍在 park 等空位的投递端没有人来唤醒。现在
`Receiver_` 的 `Drop` 会关闭消费端，`write_async` 随即以 `ProducerError::Closing` 结束，
投递端据此放弃这条连接。这也与旧的 `flume` 语义对齐（接收端没了，发送必失败）。

## 4. 验收

- 单元测试（中立执行器 `futures-executor`，不牵扯任何 `rt-*`）：
  - `multi_producer_delivers_each_value_once_`：两条线程各投两份，收齐且不重不漏；
  - `full_queue_applies_backpressure_`：容量 1 时第二条投递确实 park，取走一条后被唤醒；
  - `cancelled_push_leaves_queue_untouched_`：等待空位时取消，值不进环、回报 `Full_`。
- 集成测试：`compio` / `tokio` / `smol` 三套后端 × TCP / UNIX 域，端到端交付握手完成的
  连接全部通过。
