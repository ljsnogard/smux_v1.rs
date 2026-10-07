# 环缓冲的类型擦除：任意智能指针、一次分配

日期：2026-10-07 15:18

## 1. 目标

调用方交给连接的是一块**由某个智能指针 `P` 拥有的 ring 内存**——`accept` 时经
`prepare` 交出，建连时作为帧暂存缓冲交出。要求：

1. **任意 `P`**：同一条连接上不同子流可以用不同的智能指针类型（`Owned`、`Arc`、自家
   类型…），连接内部**不出现 `P`**；
2. **零额外分配**：连「造一条 ring 本身要的那块堆内存」也一并算进去——不能为了保存
   `P`、也不能为了共享句柄再分配一次；
3. **`P` 只在释放时被关心**：它的 drop 是唯一被 `P` 单态化的东西。

## 2. 三个角色

```rust
// ① ring 的存储视图 + 释放 P 的擦除 vtable（类型无关）
pub struct MuxChanBuff_ {
    buffer_: NonNull<[MaybeUninit<u8>]>,
    drop_owner_: unsafe fn(NonNull<()>),   // 参数是 P 自身的地址
}

// ② RingWriter / RingReader 的 S：指向那块分配的共享句柄（类型无关）
pub struct ErasedRing_ {
    block_: NonNull<()>,
    ring_of_: unsafe fn(NonNull<()>) -> *const Ring<MuxChanBuff_>,
    retain_: unsafe fn(NonNull<()>),
    release_: unsafe fn(NonNull<()>),
}

// ③ 建环入口的包装：视图 + P（零分配，只在入口出现一次）
pub struct MuxChanBuffOwnedBy<P> { chan_buff_: MuxChanBuff_, owner_ptr_: P }
```

半部别名因此**没有类型参数**：

```rust
pub type BufferedTx = RingWriter<ErasedRing_, MuxChanBuff_, u8>;
pub type BufferedRx = RingReader<ErasedRing_, MuxChanBuff_, u8>;
```

谁拥有那块内存、用的是哪种指针，一律不进类型——这才是「内部统一抽象成
`MuxChanBuff_`」的字面含义。

## 3. 一次分配

```
RingBlock_<P>            ← 建环那一次分配的全部内容
├── chan_buff_: MuxChanBuff_      // 与 ring 内那份同内容；release_ 经它调 drop vtable
├── ref_cnt_: AtomicUsize
├── ring_: Ring<MuxChanBuff_>     // B 类型无关；内含同一块缓冲区的另一份视图
└── owner_: ManuallyDrop<P>       // P 就在这里
```

`Ring` 内联在块里，`P` 是同一块的字段，`ErasedRing_` 是指向块的句柄——**没有第二次
分配**。块的分配器就是 `P` 自己的分配器（`TrBoxed::Alloc`），因此也不需要额外的
`A` 参数；释放时先从 `P` clone 出分配器，再释放 `P`，最后归还块。

### 3.1 为什么 `MuxChanBuff_` 不实现 `Drop`

块里有两份 `MuxChanBuff_`（环内一份、块首一份），若它自带 `Drop`，块被放下时会释放
两次 `P`。释放的唯一触发点是持有块的 `ErasedRing_`：计数归零时它用块布局的
`offset_of!` 定位 `P`，并经 `MuxChanBuff_::drop_owner_` 释放。

### 3.2 为什么 `release_` 不直接用 `drop_owner_::<P>` 而要经字段

「容纳 P 的 drop 的类型擦除」这件事必须落在 `MuxChanBuff_` 上（它是唯一持有 vtable
的类型）。块首那份视图就是给 `release_` 用的：它从块指针取得到，而环内那份取不到
（`Ring` 的字段私有、偏移不可见）。

## 4. 上游契约的连带改动（`abs_smux`）

- `TrMuxConfig::Buff` **删除**：一条子流用哪种指针持有 ring 内存不再是连接的静态配置；
- `TrChannelHandle::accept_async<'f, W, B, P>` / `type AcceptAsync<'f, W, B, P>`：
  `B` 升为**方法级泛型**，`B: 'static + Send + Sync + TrUnique<Item = [MaybeUninit<C::Data>], Alloc: AllocatorClone>`；
- `RingBuffAlloc<B, T>` / `TrPrepareRing<B, T>`（当时叫 `ChannelBuffAlloc` /
  `TrPrepareChannelRing`，后经 telegraph 落地一并改名）：补 `B::Alloc: AllocatorClone`
  （连接侧要用 `P` 自己的分配器释放块）；`B` 也从 `BorrowMut` 收紧为 `TrUnique`。

原先 `P: TrPrepareRing<C::Buff, C::Data>` 会直接编不过——`C::Buff` 只保证
`TrBoxed`（有 `Deref` 没有 `DerefMut`），这正是「不该由配置规定 prepare 的指针类型」
的硬证据。

## 5. `smux_v1` 侧的裁撤

- `TrConnCfg`：删除 `StageBuff`、`make_stage_buffs`、`make_ring_buffs` 与
  `BuffAllocError`——它们的存在就是「规定缓冲类型」；
- `MuxConnection::new<P>(delivery, config, read: MuxChanBuffOwnedBy<P>, write: …)`：
  帧暂存缓冲由调用方交出；
- `accept_async_managed` / `accept_async_default` 两条 managed 路径**删除**；
- `WriteEvent_` / `ReadEvent_` / `WriteTable_` / `ReadTable_` / `ReadEntry_` /
  `WriteEntry_` 全部去掉 `B` 参数，只按分配器 `A` 参数化；
- 新增公开入口 `new_buffered_channel<P>(owner) -> Result<BufferedChannel, RingBuildErr>`
  ——测试与示例要自建「传输环」时用它（以前它们自己拼 `Shared`）。

## 6. 测试面的连带变化

- `SmokeBuff` 回到「智能指针」语义（`Owned<[MaybeUninit<u8>], CoreAlloc>`），
  `SmokeStageBuff = MuxChanBuffOwnedBy<SmokeBuff>` 是交给 `MuxConnection::new` 的形态；
- `common::connect_pair_` 增加两个 stage 参数（帧暂存的容量不再由配置给），
  `MinStageConfig_` 那条「1 字节帧暂存」用例改从参数传；
- 各场景的 `accept_async_managed(&mut w, cap)` 换成
  `accept_async_closure(&mut w, || (make_channel_buff_with_(cap), …))`；
- `alloc_count`：探针改测**建一条环**的分配足迹（注入 +2、全局 +0），子流环缓冲也
  改用注入分配器（否则块的分配会落到全局分配器上，正是这条用例要抓的东西）。

## 7. 遗留

- `tests/inmem_mux.rs` 仍只有 `test-tokio-runtime` 与「其它（=compio）」两支分派，
  `--features test-smol-runtime` 下它编不过——**改动前就存在**，本轮未动；
- `TrDockBinding::listen_async(reserve)` 的 `reserve` 仍只被显式消费
  （入向通知还是身份节点里的内联槽，等定容队列落地）。
