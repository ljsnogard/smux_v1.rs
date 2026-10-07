//! 子流收发环的型号与构建。
//!
//! 上游把 `buffex::circular_buff` 换成了 [`buffex::ring`]：环本体 [`Ring`] 持有缓冲
//! 与 park 原语，`split` 出来的读端 [`RingReader`] / 写端 [`RingWriter`] 各自通过一个
//! **智能指针**访问同一条环（[`Ring::split_unchecked`]）。本模块把这对半部与本项目
//! 用到的共享句柄绑成别名，并提供唯一的构建入口。
//!
//! # 为什么两个半部要共享一条环
//!
//! 子流的发送环由应用侧（`ChannelTx`）与会话侧（中心循环）各持一端；接收环相反。
//! 两半必须指向同一条环，因此这里用 [`mm_ptr::Shared`] 做共享句柄：两边各持一个强
//! 引用，环在两者都释放后才回收。
//!
//! # 环内存从哪来
//!
//! 「环存储」是一个**由调用方交出的智能指针**，上游只要求它实现 [`TrBoxed`]——即
//! `Deref` 到 `[MaybeUninit<u8>]` 并能报出**自己那块内存的分配器**。本模块给出其中
//! 一种实现：[`MuxChanBuffAlloc<A>`]，供「连接自己管内存」的路径使用——它把「一块
//! 内存」与「释放这块内存的分配器」打包成同一个值，因此**不存在**为了保存分配器而
//! 额外做的那一次堆分配（早期版本把分配器擦除成 `Arc<dyn Allocator>`，每次建缓冲都
//! 多一次分配）。调用方也可以交自己的智能指针：`mm_ptr::Owned`、`alloc::sync::Arc`
//! 等都已经实现 [`TrBoxed`]。
//!
//! # 容量约束
//!
//! [`Ring::try_new`] 要求容量落在 `[1, MAX]` 区间内（见 buffex 的
//! `Ring::check_buffer_size`）；容量来自调用方通过
//! [`TrMuxConfig::channel_capacity`](crate::connection::TrMuxConfig::channel_capacity)
//! 给出的预算，因此构建失败只会是**配置错误**，由调用方决定如何处理。

use core::{
    alloc::{AllocError, Allocator, AllocatorClone, Layout},
    borrow::{Borrow, BorrowMut},
    convert::Infallible,
    mem::MaybeUninit,
    ops::{Deref, DerefMut, Try},
    ptr::NonNull,
};

use abs_mm::res_man::TrUnique;
use buffex::ring::{Ring, RingReader, RingWriter};
use mm_ptr::{Shared, x_deps::abs_mm::{self, res_man::TrBoxed}};

/// 子流**发送**半边的环端类型（写端）。
pub type BufferedTx<B, A> = RingWriter<Shared<Ring<B>, A>, B, u8>;

/// 子流**接收**半边的环端类型（读端）。
pub type BufferedRx<B, A> = RingReader<Shared<Ring<B>, A>, B, u8>;

/// 一条子流收发环的两个半部：`(写端, 读端)`，共享同一条 [`Ring`]。
pub type BufferedChannel<B, A> = (BufferedTx<B, A>, BufferedRx<B, A>);

/// 连接级**帧暂存**环的两个半部：`(写端, 读端)`。
///
/// 与 [`BufferedChannel`] 是同一对类型，差别只在用途与容量来源（见
/// [`crate::connection::TrConnCfg::StageBuff`]）。分开起名是为了让
/// 「这条环是连接级还是子流级」在签名上一眼可辨。
pub type StageRing<B, A> = (BufferedTx<B, A>, BufferedRx<B, A>);

/// 交给**外侧**（贴传输）两个泵循环的半部：`(读环写端, 写环读端)`。
pub(crate) type StageOuterHalves_<B, A> = (BufferedTx<B, A>, BufferedRx<B, A>);

/// 交给**内侧**（贴子流）两个循环的半部：`(读环读端, 写环写端)`。
pub(crate) type StageInnerHalves_<B, A> = (BufferedRx<B, A>, BufferedTx<B, A>);

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
pub(crate) struct StageRingPair_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + 'static,
    A: AllocatorClone + Send + Sync,
{
    /// 读环写端（外侧：泵循环把网络字节搬进来）。
    read_w_: BufferedTx<B, A>,
    /// 读环读端（内侧：解复用循环解析帧）。
    read_r_: BufferedRx<B, A>,
    /// 写环写端（内侧：复用循环成帧）。
    write_w_: BufferedTx<B, A>,
    /// 写环读端（外侧：泵循环把环上字节写上网）。
    write_r_: BufferedRx<B, A>,
}

impl<B, A> StageRingPair_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + 'static,
    A: AllocatorClone + Send + Sync,
{
    /// 由两块连接级缓冲造出两条环的四个半部。
    ///
    /// # Errors
    ///
    /// 任一块容量不在 [`buffex::ring`] 允许区间内时返回**该块的容量**（调用方应当
    /// 视为配置错误：容量来自 [`TrConnCfg::make_stage_buffs`]）。
    ///
    /// [`TrConnCfg::make_stage_buffs`]: crate::connection::TrConnCfg::make_stage_buffs
    pub(crate) fn from_buffs_(read_buff: B, write_buff: B, alloc: A) -> Result<Self, usize> {
        let (read_w_, read_r_) = new_stage_ring_(read_buff, alloc.clone())?;
        let (write_w_, write_r_) = new_stage_ring_(write_buff, alloc)?;
        Result::Ok(StageRingPair_ {
            read_w_,
            read_r_,
            write_w_,
            write_r_,
        })
    }

    /// 拆成「外侧」与「内侧」两份半部，供两条 spawn 出来的循环各持一份。
    pub(crate) fn into_halves_(self) -> (StageOuterHalves_<B, A>, StageInnerHalves_<B, A>) {
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
// 环内存：一块未初始化内存 + 释放它的分配器
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 环内存的**裸载体**：只表达「一块容量若干字节的未初始化内存」这件事。刻意剥离
/// 了智能指针的语义，只保留智能指针所指向的缓冲区的信息。可理解为 `MuxChanBuff_`
/// 的父类。
pub(super) struct MuxChanBuff_ {
    /// 指向 `capacity` 个未初始化字节的切片指针。
    ///
    /// 用胖指针而不是「裸指针 + 长度」两个字段：长度不可能与指针失去同步，也省掉一处
    /// 手工维护。元素是 `u8`（对齐为 1），因此释放所需的 [`Layout`] 只由长度决定。
    buffer_: NonNull<[MaybeUninit<u8>]>,
}

impl MuxChanBuff_ {
    pub const fn new_(slice: &mut [MaybeUninit<u8>]) -> Self {
        MuxChanBuff_ { buffer_: NonNull::from_mut(slice) }
    }

    /// 容量（`MaybeUninit<u8>` 的个数）。
    fn capacity_(&self) -> usize {
        self.buffer_.len()
    }

    /// 释放这块内存所需的布局。
    ///
    /// 构造时已经用同一个表达式成功算过一次，因此这里**实际上不会失败**；长度不合法
    /// （理论不可能）时返回 `Option::None`，调用方据此放弃释放——那只是**泄漏**，
    /// 不是 UB，符合「宁可漏、不可错」的取舍。
    fn layout_(&self) -> Option<Layout> {
        Layout::array::<MaybeUninit<u8>>(self.capacity_()).ok()
    }

    /// 借出为共享切片。
    fn as_slice_(&self) -> &[MaybeUninit<u8>] {
        // SAFETY: `buffer_` 由 `try_new_` 从分配器取得、恰好指向 `capacity_()` 个
        // `MaybeUninit<u8>`，且整块内存由本类型独占（没有别名持有者）；`&self` 借用
        // 期间不存在 `&mut self`（`as_mut_slice_` 要求 `&mut self`），故按切片读安全。
        unsafe { self.buffer_.as_ref() }
    }

    /// 借出为独占切片。
    fn as_mut_slice_(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: 同 `as_slice_`；`&mut self` 额外保证此刻没有别的借用存在。
        unsafe { self.buffer_.as_mut() }
    }
}

/// 承载「由**外部**分配器分配、供环存储使用」的内存的智能指针。
/// 这是真正保存 ring 缓冲区的对象，但几乎不会真实被使用，而总是以 `MuxChanBuff_` 的引用
/// 的形式出现。
pub struct MuxChanBuffOwnedBy<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    /// 裸内存（与分配器无关的那一半）。
    chan_buff_: MuxChanBuff_,
    /// 释放这块内存时要交回的分配器。
    owner_ptr_: P,
}

impl<P> MuxChanBuffOwnedBy<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    pub unsafe fn new(mut owner_ptr: P) -> Self {
        let slice = owner_ptr.deref_mut() as *mut [MaybeUninit<u8>];
        MuxChanBuffOwnedBy {
            chan_buff_: unsafe { MuxChanBuff_::new_(&mut *slice) },
            owner_ptr_: owner_ptr,
        }
    }

    pub const fn as_buff(&self) -> &MuxChanBuff_ {
        &self.chan_buff_
    }

    pub const fn as_buff_mut(&mut self) -> &mut MuxChanBuff_ {
        &mut self.chan_buff_
    }

    /// 造这块内存所用的分配器。
    pub fn allocator(&self) -> &<P as TrBoxed>::Alloc {
        self.owner_ptr_.try_get_mem_alloc().expect()
    }

    /// 容量（`MaybeUninit<u8>` 的个数）。
    pub fn capacity(&self) -> usize {
        self.chan_buff_.capacity_()
    }
}

impl<P> Deref for MuxChanBuffOwnedBy<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    type Target = [MaybeUninit<u8>];

    fn deref(&self) -> &Self::Target {
        self.owner_ptr_.deref()
    }
}

impl<P> DerefMut for MuxChanBuffOwnedBy<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.owner_ptr_.deref_mut()
    }
}

impl<P> Borrow<[MaybeUninit<u8>]> for MuxChanBuffOwnedBy<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>
        + Borrow<[MaybeUninit<u8>]>,
{
    fn borrow(&self) -> &[MaybeUninit<u8>] {
        self.owner_ptr_.borrow()
    }
}

impl<P> BorrowMut<[MaybeUninit<u8>]> for MuxChanBuffOwnedBy<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>
        + BorrowMut<[MaybeUninit<u8>]>,
{
    fn borrow_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        self.owner_ptr_.borrow_mut()
    }
}

impl<P> TrBoxed for MuxChanBuffOwnedBy<P>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{
    type Item = [MaybeUninit<u8>];
    type Alloc = P;

    fn try_get_mem_alloc(&self) -> impl Try<Output = &Self::Alloc> {
        self.owner_ptr_.try_get_mem_alloc()
    }
}

impl<A> TrUnique for MuxChanBuffOwnedBy<A>
where
    P: TrUnique<Item = [MaybeUninit<u8>]>,
{}

// SAFETY: `Send`——本类型**独占**一块由 `allocator_` 分配的内存，字段只有「切片指针
// + 分配器」，没有任何指向别处或与其它线程共享的状态。把值整体搬到另一个线程时，内存
// 的所有权与分配器**一起**搬迁，因此在新线程上经 `Drop` 释放依然合法（`A: Send` 正是
// 「分配器句柄可跨线程移动」）。`NonNull` 默认不是 `Send`，所以要手写这条 impl；
// 没有它，`MuxConnection::new` 对 `C::StageBuff: Send + Sync` 的要求就无人满足。
unsafe impl<A> Send for MuxChanBuffOwnedBy<A> where A: AllocatorClone + Send {}

// SAFETY: `Sync`——`&self` 只暴露 `&[MaybeUninit<u8>]`（经 `Deref` / `Borrow`），
// 不含任何内部可变性；唯一的写入路径 `DerefMut` / `BorrowMut` 要求 `&mut self`，而
// `&mut` 与跨线程共享的 `&self` 在借用检查下不可能同时存在。因此只要分配器本身可跨
// 线程共享（`A: Sync`），`&MuxChanBuffAlloc<A>` 就可以安全地送到别的线程。
unsafe impl<A> Sync for MuxChanBuffOwnedBy<A> where A: AllocatorClone + Sync {}

/// 用调用方注入的存储与分配器建立**一条连接级**帧暂存环，切成 `(写端, 读端)`。
///
/// # Errors
///
/// 容量不在 [`buffex::ring`] 允许的区间内时返回该容量本身。
fn new_stage_ring_<B, A>(buffer: B, alloc: A) -> Result<StageRing<B, A>, usize>
where
    B: BorrowMut<[MaybeUninit<u8>]>,
    A: AllocatorClone,
{
    new_buffered_channel_(buffer, alloc)
}

/// 用调用方注入的存储与分配器建立一条环，并把它切成 `(写端, 读端)`。
///
/// 环的存储由 `buffer` 自己负责（它的智能指针在 `Drop` 时归还内存），而**共享句柄**
/// [`Shared`] 的分配器由 `alloc` 给出——两者在连接侧来自同一个配置，但类型上互相
/// 独立，因此本函数不需要它们相等。
///
/// # Errors
///
/// 容量不在 [`buffex::ring`] 允许的区间内时返回该容量本身（调用方应当把它视为配置
/// 错误——容量来自 [`TrMuxConfig::channel_capacity`]）。
///
/// [`TrMuxConfig::channel_capacity`]: crate::connection::TrMuxConfig::channel_capacity
#[allow(dead_code)] // 接线（第 5、6 步）后由子流建立路径调用。
pub(crate) fn new_buffered_channel_<B, A>(
    buffer: B,
    alloc: A,
) -> Result<BufferedChannel<B, A>, usize>
where
    B: BorrowMut<[MaybeUninit<u8>]>,
    A: AllocatorClone,
{
    let ring = Ring::try_new(buffer)?;
    let shared = Shared::new(ring, alloc);
    // SAFETY: 这条环只被刚建出的 `Shared` 独占，且没有对应的 `Weak`（不存在升级
    // 路径），因此两个半部各持一个强引用是安全的；环在最后一个强引用释放时回收。
    // 该使用方式与 `Ring::split_unchecked` 文档要求的两条调用方保证一致。
    Result::Ok(unsafe { Ring::split_unchecked(shared) })
}

/// 测试专用：用 [`MuxChanBuffAlloc`] 建内存环，供本 crate 的单测复用。
///
/// 放在这里而不是各个测试模块里，是为了让「环怎么建」只有一处实现——上游改动
/// 构建入口时只需要改这里。
#[cfg(test)]
pub(crate) mod test_support_ {
    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use super::{BufferedChannel, MuxChanBuffOwnedBy, new_buffered_channel_};

    /// 测试用的环存储类型（与生产默认装配同一种）。
    pub(crate) type TestBuff = MuxChanBuffOwnedBy<CoreAlloc>;

    /// 建一条容量为 `capacity` 的内存环，返回 `(写端, 读端)`。
    pub(crate) fn make_test_channel_(capacity: usize) -> BufferedChannel<TestBuff, CoreAlloc> {
        let buffer = TestBuff::try_new(CoreAlloc, capacity).expect("测试环内存应当分配成功");
        new_buffered_channel_(buffer, CoreAlloc).expect("测试容量应当落在 ring 允许区间内")
    }
}
