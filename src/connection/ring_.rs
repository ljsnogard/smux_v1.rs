//! 子流收发环的型号与构建。
//!
//! 上游把 `buffex::circular_buff` 换成了 [`buffex::ring`]：环本体 [`Ring`] 持有缓冲
//! 与 park 原语，`split` 出来的读端 [`RingReader`] / 写端 [`RingWriter`] 各自通过一个
//! **智能指针**访问同一条环（[`Ring::split_unchecked`]）。
//!
//! 本模块把「环内存」「环的共享句柄」与「环的构建入口」都收敛成**类型无关**的固定
//! 类型：`accept` 与握手产物交出的任何智能指针 `P`，进来之后都只剩三种角色。
//!
//! # 三个角色
//!
//! ```text
//! MuxChanBuff_        ring 的存储视图 + 「释放 P」的擦除 vtable（类型无关）
//! ErasedRing_         RingWriter / RingReader 的 S：指向那块分配的共享句柄（类型无关）
//! MuxChanBuffOwnedBy<P>  建环入口的包装：视图 + P（零分配，只在入口出现一次）
//! ```
//!
//! # 一次分配：控制块 + 环 + P 同处一块
//!
//! [`Ring`] 的存储 `B` 就是 [`MuxChanBuff_`]（类型无关），而 `P` 与 `Ring`、引用计数
//! 控制块一起放在**同一块分配**里（[`RingBlock_`]）。因此：
//!
//! - 造环本身只有**一次**堆分配——就是这一块，`P` 是它的一部分；
//! - `RingWriter` / `RingReader` 的第一个泛型参数 `S` 也不再另起一次分配：它就是指向
//!   那块内存的 [`ErasedRing_`]；
//! - 「`P` 是什么」只活在两处：`P` 在这个块里的位置，以及 `drop_owner_` 这一个函数
//!   ――[`MuxChanBuff_`] 里存一份、建环时给 [`ErasedRing_`] 记一份，两者是同一个单态化
//!   实例。
//!
//! # 释放顺序
//!
//! [`ErasedRing_`] 的引用计数归零时：先从 `P` 取出它自己的分配器（块就是用那个分配器
//! 分配的，释放它还得靠它），再释放 `P`，然后放下 [`Ring`]，最后归还块内存。
//! [`MuxChanBuff_`] **不实现 `Drop`**：它是视图，同一个块里可能有多份（环内一份、句柄
//! 一份），由谁持有块谁负责释放，否则会重复释放 `P`。
//!
//! # 容量约束
//!
//! [`Ring::check_buffer_size`] 要求容量落在 `[1, MAX]` 区间内；容量来自调用方交出的
//! 那块内存，因此构建失败只会是**配置错误**，由调用方决定如何处理。

use core::{
    alloc::{AllocError, Allocator, AllocatorClone, Layout},
    borrow::{Borrow, BorrowMut},
    convert::Infallible,
    mem::{ManuallyDrop, MaybeUninit, offset_of},
    ops::{ControlFlow, Deref, DerefMut, Try},
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
};

use abs_mm::res_man::{TrBoxed, TrUnique};
use buffex::ring::{Ring, RingReader, RingWriter};
use mm_ptr::x_deps::abs_mm;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 类型无关的半部：S 与 B 都是 MuxChanBuff_ / ErasedRing_，没有类型参数
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 子流**发送**半边的环端类型（写端）。
///
/// `S = `[`ErasedRing_`]、`B = `[`MuxChanBuff_`] 都是固定类型：谁拥有那块内存、用的是
/// 哪种智能指针，一律不进类型。
pub type BufferedTx = RingWriter<ErasedRing_, MuxChanBuff_, u8>;

/// 子流**接收**半边的环端类型（读端）。
pub type BufferedRx = RingReader<ErasedRing_, MuxChanBuff_, u8>;

/// 一条子流收发环的两个半部：`(写端, 读端)`，共享同一条 [`Ring`]。
pub type BufferedChannel = (BufferedTx, BufferedRx);

/// 交给**外侧**（贴传输）两个泵循环的半部：`(读环写端, 写环读端)`。
pub(crate) type StageOuterHalves_ = (BufferedTx, BufferedRx);

/// 交给**内侧**（贴子流）两个循环的半部：`(读环读端, 写环写端)`。
pub(crate) type StageInnerHalves_ = (BufferedRx, BufferedTx);

/// 连接级两条帧暂存环的四个半部：`(读环写端, 读环读端, 写环写端, 写环读端)`。
///
/// # 命名约定
///
/// 「读 / 写」一律站在**连接**视角：读环承载 `transport → 解复用`，写环承载
/// `复用 → transport`。
///
/// # 谁持有什么
///
/// - **外侧**（贴传输的两个泵循环）拿 [`Self::into_halves_`] 的第一组：
///   读环**写端** + 写环**读端**——它们只与传输打交道；
/// - **内侧**（贴子流的解复用 / 复用两个循环）拿第二组：读环**读端** + 写环**写端**。
///
/// 一条环的同一端不得同时被两个任务持有：环的 park / 唤醒状态是按端记的，
/// 两端各由固定的一方驱动才能保证「写入唤醒读端、读到空 park 读端」这条约定成立。
pub(crate) struct StageRingPair_ {
    /// 读环写端（外侧：泵循环把网络字节搬进来）。
    read_w_: BufferedTx,
    /// 读环读端（内侧：解复用循环解析帧）。
    read_r_: BufferedRx,
    /// 写环写端（内侧：复用循环成帧）。
    write_w_: BufferedTx,
    /// 写环读端（外侧：泵循环把环上字节写上网）。
    write_r_: BufferedRx,
}

impl StageRingPair_ {
    /// 由两块连接级缓冲造出两条环的四个半部。
    ///
    /// # Errors
    ///
    /// 容量不在 [`buffex::ring`] 允许区间内、或块分配失败时返回 [`RingBuildErr_`]。
    pub(crate) fn from_owners_<P>(read_owner: P, write_owner: P) -> Result<Self, RingBuildErr>
    where
        P: TrUnique<Item = [MaybeUninit<u8>]> + Send + Sync,
        P::Alloc: AllocatorClone,
    {
        let (read_w_, read_r_) = new_buffered_channel(read_owner)?;
        let (write_w_, write_r_) = new_buffered_channel(write_owner)?;
        Result::Ok(StageRingPair_ {
            read_w_,
            read_r_,
            write_w_,
            write_r_,
        })
    }

    /// 拆成「外侧」与「内侧」两份半部，供两条 spawn 出来的循环各持一份。
    pub(crate) fn into_halves_(self) -> (StageOuterHalves_, StageInnerHalves_) {
        let StageRingPair_ {
            read_w_,
            read_r_,
            write_w_,
            write_r_,
        } = self;
        ((read_w_, write_r_), (read_r_, write_w_))
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 环的存储：一块缓冲区视图 + 释放其拥有者 P 的擦除 vtable
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 环存储的**类型无关**形态：缓冲区视图 + 「释放拥有者 `P`」的擦除 vtable。
///
/// 它就是 [`Ring`] 的存储 `B`；凡是交给连接的智能指针，进来之后一律以它出现。
///
/// # 它不拥有任何东西
///
/// - 缓冲区由某个智能指针 `P` 拥有，`P` 与 [`Ring`] 同在 [`RingBlock_`] 那一块分配里；
/// - 本类型**不实现 `Drop`**：同一个块里它可能有多份（环内一份、句柄一份），释放 `P`
///   的唯一触发点是持有块的 [`ErasedRing_`]。
///
/// `drop_owner_` 是唯一被 `P` 单态化的地方，参数是 **`P` 自身的地址**。
pub struct MuxChanBuff_ {
    /// 指向 `capacity` 个未初始化字节的切片指针。
    ///
    /// 用胖指针而不是「裸指针 + 长度」两个字段：长度不可能与指针失去同步，也省掉一处
    /// 手工维护。元素是 `u8`（对齐为 1），因此布局只由长度决定。
    buffer_: NonNull<[MaybeUninit<u8>]>,
    /// 释放拥有这块缓冲区的那个 `P`（参数是 `P` 的地址）。
    ///
    /// 参数为什么不直接是块首地址：本类型内联在 [`Ring`] 里，从它自己算不回块首；而
    /// 调用方（[`ErasedRing_`]）拿得到 `P` 在块里的地址。
    drop_owner_: unsafe fn(NonNull<()>),
}

impl MuxChanBuff_ {
    /// 由「某个 `P` 的缓冲区视图」造出擦除句柄。
    fn new_<P>(buffer: NonNull<[MaybeUninit<u8>]>) -> Self
    where
        P: TrUnique<Item = [MaybeUninit<u8>]>,
    {
        MuxChanBuff_ {
            buffer_: buffer,
            drop_owner_: drop_owner_::<P>,
        }
    }

    /// 由一块缓冲区切片直接造出视图（不绑定任何 `P` 的释放语义）。
    ///
    /// 仅供 [`MuxChanBuffOwnedBy`] 这类「自己持有 `P`」的包装使用。
    const fn from_slice_(slice: &mut [MaybeUninit<u8>]) -> Self {
        MuxChanBuff_ {
            buffer_: NonNull::from_mut(slice),
            drop_owner_: no_drop_owner_,
        }
    }

    /// 容量（`MaybeUninit<u8>` 的个数）。
    fn capacity_(&self) -> usize {
        self.buffer_.len()
    }

    /// 借出为共享切片。
    fn as_slice_(&self) -> &[MaybeUninit<u8>] {
        // SAFETY: `buffer_` 由构造方保证指向 `capacity_()` 个 `MaybeUninit<u8>`，且整块
        // 缓冲区由那个 `P` 独占；`&self` 借用期间不存在 `&mut self`（`as_mut_slice_`
        // 要求 `&mut self`），故按切片读安全。
        unsafe { self.buffer_.as_ref() }
    }

    /// 借出为独占切片。
    fn as_mut_slice_(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: 同 `as_slice_`；`&mut self` 额外保证此刻没有别的借用存在。
        unsafe { self.buffer_.as_mut() }
    }
}

/// 「没有拥有者」的 `drop_owner_`：仅用于纯视图实例（释放由持有 `P` 的那一方负责）。
///
/// # Safety
///
/// 调用它说明有人把纯视图当成了拥有者，属于逻辑错误；这里 `unreachable!` 而不是静默
/// 忽略，是为了让错误立刻可见。
unsafe fn no_drop_owner_(_owner: NonNull<()>) {
    unreachable!("纯视图不负责释放任何东西：拥有者由持有它的那一方释放")
}

impl Deref for MuxChanBuff_ {
    type Target = [MaybeUninit<u8>];

    fn deref(&self) -> &Self::Target {
        self.as_slice_()
    }
}

impl DerefMut for MuxChanBuff_ {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice_()
    }
}

impl Borrow<[MaybeUninit<u8>]> for MuxChanBuff_ {
    fn borrow(&self) -> &[MaybeUninit<u8>] {
        self.as_slice_()
    }
}

impl BorrowMut<[MaybeUninit<u8>]> for MuxChanBuff_ {
    fn borrow_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        self.as_mut_slice_()
    }
}

impl TrBoxed for MuxChanBuff_ {
    type Item = [MaybeUninit<u8>];
    type Alloc = NoAlloc_;

    fn try_get_mem_alloc(&self) -> impl Try<Output = &Self::Alloc> {
        Result::<&NoAlloc_, Infallible>::Ok(&NoAlloc_)
    }
}

/// [`MuxChanBuff_`] 的占位分配器：它自己**不分配**（缓冲区与块都由 `P` 的分配器负责），
/// 但上游 `TrBoxed` 要求有个 `Alloc` 关联类型。
///
/// 任何真正调用它的人都会 panic——这不是缺陷，而是「谁拥有谁释放」这条约定的哨兵：
/// 需要分配器时应当去问块里那个 `P`。
#[derive(Clone, Copy, Debug, Default)]
pub struct NoAlloc_;

// SAFETY: 零大小、无状态；两个方法都是「走错门」的哨兵。
unsafe impl Allocator for NoAlloc_ {
    fn allocate(&self, _layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        unreachable!("MuxChanBuff_ 自己不做分配：缓冲区与块都由 P 的分配器负责")
    }

    unsafe fn deallocate(&self, _ptr: NonNull<u8>, _layout: Layout) {
        unreachable!("MuxChanBuff_ 自己不做释放：释放 P 走它的 drop vtable")
    }
}

// SAFETY: 零大小、可平凡克隆。
unsafe impl AllocatorClone for NoAlloc_ {}

// SAFETY: `Send`——本类型只有「指向某块缓冲区的裸指针 + 一个纯函数指针」，没有内部
// 可变性，也不拥有任何东西；块的所有权在 `ErasedRing_` 里，那里另行约束了 `P: Send`。
// 没有这条 impl，连接级帧暂存环的存储就无法跨线程搬进循环。
unsafe impl Send for MuxChanBuff_ {}

// SAFETY: `Sync`——`&MuxChanBuff_` 只暴露 `&[MaybeUninit<u8>]`（经 `Deref` / `Borrow`），
// 写入路径全部要求 `&mut self`；`drop_owner_` 是纯函数指针，不含数据。
unsafe impl Sync for MuxChanBuff_ {}

/// 释放 `owner` 指向的那个 `P`。
///
/// # Safety
///
/// `owner` 必须指向一块**有效、已初始化、且尚未被释放**的 `P`；调用方还要保证此后
/// 不再以任何方式使用它。
unsafe fn drop_owner_<P>(owner: NonNull<()>)
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    // SAFETY: 由调用方保证（见 `# Safety`）。
    unsafe { owner.cast::<P>().as_ptr().drop_in_place() };
}

/// 取出智能指针自带的分配器。
///
/// `TrBoxed` 的契约是「一定持有自己的分配器」，因此 `Break` 分支不可达；用
/// `unreachable!` 而不是 `unwrap` 是为了在违反契约时给出可读的原因。
fn mem_alloc_of_<B>(boxed: &B) -> &B::Alloc
where
    B: TrBoxed,
{
    match TrBoxed::try_get_mem_alloc(boxed).branch() {
        ControlFlow::Continue(alloc) => alloc,
        ControlFlow::Break(_) => unreachable!("TrBoxed 的契约：智能指针一定持有其分配器"),
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 建环入口的包装：视图 + P（零分配）
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一个**由智能指针 `P` 拥有**的 ring 缓冲区。
///
/// 这是「调用方交给连接」的标准形态（`MuxConnection::new` 的帧暂存缓冲就是它）。
/// 构造它**零堆分配**——`P` 就内联在结构里；连接随后把它整个搬进 [`RingBlock_`]
/// （那一步才发生唯一一次分配，且 `P` 是那块的一部分）。
///
/// 它只在**入口**出现：进了连接之后一律以 [`MuxChanBuff_`] / [`ErasedRing_`] 的形式
/// 流动，因此内部签名里看不到 `P`。
pub struct MuxChanBuffOwnedBy<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    /// 缓冲区视图。
    chan_buff_: MuxChanBuff_,
    /// 真正拥有这块缓冲区的智能指针。
    owner_ptr_: P,
}

impl<P> MuxChanBuffOwnedBy<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    /// 由智能指针 `P` 造出「视图 + 拥有者」（零堆分配）。
    pub fn new(mut owner_ptr: P) -> Self {
        MuxChanBuffOwnedBy {
            chan_buff_: MuxChanBuff_::from_slice_(owner_ptr.deref_mut()),
            owner_ptr_: owner_ptr,
        }
    }

    /// 取出类型无关的缓冲区视图。
    pub const fn as_buff(&self) -> &MuxChanBuff_ {
        &self.chan_buff_
    }

    /// 取出类型无关的缓冲区视图（可变）。
    pub const fn as_buff_mut(&mut self) -> &mut MuxChanBuff_ {
        &mut self.chan_buff_
    }

    /// 容量（`MaybeUninit<u8>` 的个数）。
    pub fn capacity(&self) -> usize {
        self.chan_buff_.capacity_()
    }

    /// 拆成「缓冲区视图」与「拥有者」，供建环路径使用。
    pub fn into_owner_(self) -> P {
        self.owner_ptr_
    }
}

impl<P> Deref for MuxChanBuffOwnedBy<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    type Target = [MaybeUninit<u8>];

    fn deref(&self) -> &Self::Target {
        self.chan_buff_.as_slice_()
    }
}

impl<P> DerefMut for MuxChanBuffOwnedBy<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.chan_buff_.as_mut_slice_()
    }
}

impl<P> Borrow<[MaybeUninit<u8>]> for MuxChanBuffOwnedBy<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    fn borrow(&self) -> &[MaybeUninit<u8>] {
        self.chan_buff_.as_slice_()
    }
}

impl<P> BorrowMut<[MaybeUninit<u8>]> for MuxChanBuffOwnedBy<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    fn borrow_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        self.chan_buff_.as_mut_slice_()
    }
}

impl<P> TrBoxed for MuxChanBuffOwnedBy<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    type Item = [MaybeUninit<u8>];
    type Alloc = P::Alloc;

    fn try_get_mem_alloc(&self) -> impl Try<Output = &Self::Alloc> {
        // 本类型只是在 `P` 外面包了一层视图，分配器仍归 `P` 自己回答。
        self.owner_ptr_.try_get_mem_alloc()
    }
}

impl<P> TrUnique for MuxChanBuffOwnedBy<P> where P: TrUnique<Item = [MaybeUninit<u8>]> {}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 块：控制块 + 环 + P，同处一次分配
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 建环的**那一次分配**：引用计数控制块 + 环（内含 [`MuxChanBuff_`]）+ `P`。
///
/// 它只出现在建环函数内部——`accept` 与握手产物交出的 `P` 都在这里被吸收，此后连接
/// 内部只认 [`MuxChanBuff_`] 与 [`ErasedRing_`]。
///
/// 布局由本模块定义，因此 [`ErasedRing_`] 的 vtable 与 `release_` 都能用
/// `offset_of!` 正确定位 `P`。
#[repr(C)]
struct RingBlock_<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    /// 擦除句柄（与 `ring_` 内那份同内容）。
    ///
    /// 块里之所以还要一份：`release_` 拿得到块指针，但**拿不到** [`Ring`] 内联的那份
    /// 存储（`Ring` 的字段是私有的、偏移不可见）。释放 `P` 走的正是它记下的 vtable。
    chan_buff_: MuxChanBuff_,
    /// 强引用计数。
    ref_cnt_: AtomicUsize,
    /// 环本体；其存储 `B` 就是 [`MuxChanBuff_`]。
    ring_: Ring<MuxChanBuff_>,
    /// `P`——由 `drop_owner_` 释放，故不再自动 drop。
    owner_: ManuallyDrop<P>,
}

/// 环的**共享句柄**（`RingWriter` / `RingReader` 的 `S`），类型无关。
///
/// 它指向 [`RingBlock_`]，并携带三个函数指针：取环、增加引用、减少引用（归零时释放
/// 整个块）。这就是「不再额外分配第二次」的那一环——句柄本身就是块内存的代表。
pub struct ErasedRing_ {
    /// `*mut RingBlock_<P>`（`P` 已擦除）。
    block_: NonNull<()>,
    /// 从块取出环本体。
    ring_of_: unsafe fn(NonNull<()>) -> *const Ring<MuxChanBuff_>,
    /// 增加强引用计数。
    retain_: unsafe fn(NonNull<()>),
    /// 减少强引用计数；归零时释放整个块（含 `P`）。
    release_: unsafe fn(NonNull<()>),
}

impl ErasedRing_ {
    /// 由块指针造出共享句柄（**不**增加引用计数：调用方把初始的那份交给它）。
    fn new_<P>(block: NonNull<RingBlock_<P>>) -> Self
    where
        P: TrUnique<Item = [MaybeUninit<u8>]>,
        P::Alloc: AllocatorClone,
    {
        ErasedRing_ {
            block_: block.cast::<()>(),
            ring_of_: ring_of_::<P>,
            retain_: retain_::<P>,
            release_: release_::<P>,
        }
    }
}

impl Borrow<Ring<MuxChanBuff_>> for ErasedRing_ {
    fn borrow(&self) -> &Ring<MuxChanBuff_> {
        // SAFETY: `block_` 指向一个活着的 `RingBlock_`（本句柄持有一份强引用），
        // `ring_of_` 是本模块为它填的合法取环函数；返回的引用借用 `&self`。
        unsafe { &*(self.ring_of_)(self.block_) }
    }
}

impl Clone for ErasedRing_ {
    fn clone(&self) -> Self {
        // SAFETY: 同 `Borrow`；`retain_` 为本模块填入的合法计数函数。
        unsafe { (self.retain_)(self.block_) };
        ErasedRing_ {
            block_: self.block_,
            ring_of_: self.ring_of_,
            retain_: self.retain_,
            release_: self.release_,
        }
    }
}

impl Drop for ErasedRing_ {
    fn drop(&mut self) {
        // SAFETY: 同 `Borrow`；本句柄持有的这一份强引用在这里交回。
        unsafe { (self.release_)(self.block_) };
    }
}

// SAFETY: `Send`——句柄是那块块内存的**共享代表**（引用计数是原子的），块里只有环与
// `P`：环的读写走它自己的原子状态（`buffex::Ring` 已声明 `Send + Sync`），而 `P` 只在
// 计数归零时被 drop。建环入口强约束了 `P: Send + Sync`，因此把句柄搬到别的线程后
// 释放 `P` 是合法的。
unsafe impl Send for ErasedRing_ {}

// SAFETY: `Sync`——`&ErasedRing_` 只暴露 `&Ring<MuxChanBuff_>`（经 `Borrow`），没有内部
// 可变性，也不给出对 `P` 的任何访问；计数是原子的。同理要求建环时 `P: Send + Sync`。
unsafe impl Sync for ErasedRing_ {}

/// 从块取环本体。
///
/// # Safety
///
/// `block` 必须指向活着的 `RingBlock_<P>`。
unsafe fn ring_of_<P>(block: NonNull<()>) -> *const Ring<MuxChanBuff_>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    // SAFETY: 由调用方保证；`ring_` 字段按本模块定义的布局存在。
    unsafe { &block.cast::<RingBlock_<P>>().as_ref().ring_ as *const _ }
}

/// 递增强引用计数。
///
/// # Safety
///
/// 同 [`ring_of_`]。
unsafe fn retain_<P>(block: NonNull<()>)
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    // SAFETY: 由调用方保证；计数本身是原子的。
    unsafe { block.cast::<RingBlock_<P>>().as_ref().ref_cnt_.fetch_add(1, Ordering::Relaxed) };
}

/// 递减强引用计数；归零时释放整个块。
///
/// # Safety
///
/// 同 [`ring_of_`]；且调用方必须真的持有过一份强引用。
unsafe fn release_<P>(block: NonNull<()>)
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
    P::Alloc: AllocatorClone,
{
    let block_ptr = block.cast::<RingBlock_<P>>();
    // SAFETY: 由调用方保证 `block_ptr` 有效。
    let block_ref = unsafe { block_ptr.as_ref() };
    if block_ref.ref_cnt_.fetch_sub(1, Ordering::AcqRel) != 1 {
        return;
    }
    // 归零：这一段由最后一个持有者独占执行。
    let owner_ptr = unsafe {
        block
            .cast::<u8>()
            .as_ptr()
            .add(offset_of!(RingBlock_<P>, owner_))
            .cast::<P>()
    };
    // 释放块还得用 `P` 自己的分配器，所以先把它 clone 出来再释放 `P`。
    // SAFETY: `owner_ptr` 指向本块里那份 `P`（已初始化、尚未 drop）。
    let alloc = mem_alloc_of_(unsafe { &*owner_ptr }).clone();
    // SAFETY: 由 `RingBlock_` 的契约保证只释放一次；此后不再触碰 `owner_`。
    // 释放走 `MuxChanBuff_` 记下的擦除 vtable —— 「容纳 P 的 drop」这件事的唯一落点。
    unsafe {
        (block_ref.chan_buff_.drop_owner_)(NonNull::new_unchecked(owner_ptr.cast::<()>()));
    }
    // SAFETY: 环与本块同寿，此处是它唯一一次被放下；`MuxChanBuff_` 无 `Drop`，因此
    // 不会与上面那次释放重复。
    unsafe { core::ptr::drop_in_place(core::ptr::addr_of_mut!((*block_ptr.as_ptr()).ring_)) };
    // SAFETY: 块正是由 `alloc` 按这个布局分配的，且此处不再使用它。
    unsafe { alloc.deallocate(block.cast::<u8>(), Layout::new::<RingBlock_<P>>()) };
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 构建入口
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 建环失败的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RingBuildErr {
    /// 调用方交出的那块内存容量不在 [`buffex::ring`] 允许区间内（回带该容量）。
    #[error("环容量 {0} 不在 buffex::ring 允许的区间内")]
    Capacity(usize),

    /// 块内存分配失败。
    #[error("环节点分配失败")]
    Alloc,
}

/// 由「一个智能指针 `P` 拥有的 ring 缓冲」建立一条环，切成 `(写端, 读端)`。
///
/// # 分配次数
///
/// **只有一次**：`RingBlock_`（控制块 + 环 + `P`）本身。`P` 交出时已经持有它自己那块
/// 缓冲区，本函数不再为「保存 `P`」或「共享句柄」做任何额外分配。
///
/// # Errors
///
/// 容量非法时返回 [`RingBuildErr::Capacity`]（调用方应视为配置错误）；块分配失败时
/// 返回 [`RingBuildErr::Alloc`]（此时 `P` 尚未被搬进块，随栈上作用域正常释放）。
pub fn new_buffered_channel<P>(owner: P) -> Result<BufferedChannel, RingBuildErr>
where
    P: TrUnique<Item = [MaybeUninit<u8>]> + Send + Sync,
    P::Alloc: AllocatorClone,
{
    let mut owner = owner;
    let buffer_ = NonNull::from_mut(owner.deref_mut());
    // 先做**不分配**的容量校验，避免为一次注定失败的建环去分配块。
    let view = MuxChanBuff_::new_::<P>(buffer_);
    Ring::check_buffer_size(&view).map_err(RingBuildErr::Capacity)?;

    let alloc = mem_alloc_of_(&owner).clone();
    let raw = alloc
        .allocate(Layout::new::<RingBlock_<P>>())
        .map_err(|_| RingBuildErr::Alloc)?;
    let block_ptr = raw.cast::<RingBlock_<P>>();

    let ring = Ring::new_unchecked(MuxChanBuff_::new_::<P>(buffer_));
    // SAFETY: 刚分配的一块 `RingBlock_<P>` 大小的内存，此处首次且唯一一次写入；
    // `owner` 的所有权随之转进块，此后只经块的 `owner_` 访问。
    unsafe {
        block_ptr.as_ptr().write(RingBlock_ {
            chan_buff_: MuxChanBuff_::new_::<P>(buffer_),
            ref_cnt_: AtomicUsize::new(1),
            ring_: ring,
            owner_: ManuallyDrop::new(owner),
        });
    }
    let erased = ErasedRing_::new_::<P>(block_ptr);
    // SAFETY: 这条环刚建出、只被 `erased` 独占，且本模块不提供 `Weak`（不存在升级
    // 路径），因此两个半部各持一个强引用是安全的；与 `Ring::split_unchecked` 的
    // 两条调用方保证一致。
    Result::Ok(unsafe { Ring::split_unchecked(erased) })
}

/// 测试专用：建一条容量为 `capacity` 的内存环，供本 crate 的单测复用。
///
/// 放在这里而不是各个测试模块里，是为了让「环怎么建」只有一处实现——上游改动
/// 构建入口时只需要改这里。
#[cfg(test)]
pub(crate) mod test_support_ {
    use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};

    use super::{BufferedChannel, new_buffered_channel};

    /// 建一条容量为 `capacity` 的内存环，返回 `(写端, 读端)`。
    pub(crate) fn make_test_channel_(capacity: usize) -> BufferedChannel {
        let owner = Owned::new_uninit_slice(capacity, CoreAlloc);
        new_buffered_channel(owner).expect("测试容量应当落在 ring 允许区间内")
    }
}
