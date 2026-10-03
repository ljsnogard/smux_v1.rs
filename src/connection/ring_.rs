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
//! # 容量约束
//!
//! [`Ring::try_new`] 要求容量落在 `[2, MAX]` 区间内（见 buffex 的
//! `Ring::check_buffer_size`）；容量来自调用方通过
//! [`TrMuxConfig::channel_capacity`](crate::connection::TrMuxConfig::channel_capacity)
//! 给出的预算，因此构建失败只会是**配置错误**，由调用方决定如何处理。

use alloc::sync::Arc;
use core::{
    alloc::{AllocError, Allocator, AllocatorClone, Layout},
    borrow::{Borrow, BorrowMut},
    mem::MaybeUninit,
    ptr::NonNull,
};

use buffex::ring::{Ring, RingReader, RingWriter};
use mm_ptr::Shared;

/// 子流**发送**半边的环端类型（写端）。
pub type BufferedTx<B, A> = RingWriter<Shared<Ring<B>, A>, B, u8>;

/// 子流**接收**半边的环端类型（读端）。
pub type BufferedRx<B, A> = RingReader<Shared<Ring<B>, A>, B, u8>;

/// 一条子流收发环的两个半部：`(写端, 读端)`，共享同一条 [`Ring`]。
pub type BufferedChannel<B, A> = (BufferedTx<B, A>, BufferedRx<B, A>);

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 环内存的智能指针：把分配器类型擦除成 `dyn Allocator`
// -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 承载「由**外部**分配器分配、供 channel ring 使用」的内存的智能指针。
///
/// # 为什么要把分配器擦除
///
/// `accept_async` 那时调用方交来的是一个**分配器**（`A: Allocator`），而连接侧要
/// 装的环存储类型 `C::Buff` 是**配置里写死**的（半部的类型里没有生命周期、也没有
/// 多余的泛型位置可以再挂一个 `A`）。于是这里把「指针 + 容量 + **分配器本身**」打包
/// 成本类型：分配器以 [`Arc<dyn Allocator + Send + Sync>`] 的形式被**拥有**，
/// 释放时按 vtable 交回去（[`Drop`]）。因此本类型与调用方用的是哪种分配器无关，
/// 而调用方也**没有**机会指定指针类型——那是连接的安排（见 `accept_async` 的设计）。
///
/// 两个方向（Tx / Rx）的缓冲由 [`MuxChanBuff::pair_from_alloc_`] 一次造出，两者
/// **共用**同一个被擦除的分配器（`Arc`），所以不要求 `A: Clone`。
///
/// # 零开销的替代
///
/// 能自己声明环存储类型的环境（例如用 `Owned<[MaybeUninit<u8>], 自家分配器>`）可以
/// 不采用本类型：`C::Buff` 由环境声明，缓冲实例由调用方在 `accept_async` 的
/// `prepare` 参数里当场交出。
pub struct MuxChanBuff {
    /// 环内存首地址，指向 `cap_` 个未初始化的字节（`MaybeUninit<u8>`）。
    ptr_: NonNull<MaybeUninit<u8>>,
    /// 容量（`MaybeUninit<u8>` 的个数）。
    cap_: usize,
    /// 分配时用的 layout，释放时原样交回分配器。
    layout_: Layout,
    /// 类型擦除的分配器；两个方向的缓冲共享同一份。
    alloc_: Arc<dyn Allocator + Send + Sync>,
}

impl MuxChanBuff {
    /// 用调用方给的分配器造出**两块**同容量环内存（Tx、Rx 各一块）。
    ///
    /// # Errors
    ///
    /// `capacity` 换算 layout 溢出、或分配器拒绝分配时返回 [`AllocError`]；调用方
    /// （连接侧）把它视为「这份内存不可用」。
    pub fn pair_from_alloc_<A>(
        alloc: A,
        capacity: usize,
    ) -> Result<(Self, Self), AllocError>
    where
        A: Allocator + Send + Sync + 'static,
    {
        let erased: Arc<dyn Allocator + Send + Sync> = Arc::new(alloc);
        Result::Ok((
            Self::one_from_erased_(erased.clone(), capacity)?,
            Self::one_from_erased_(erased, capacity)?,
        ))
    }

    /// 由一份被擦除的分配器与容量造出**一块**环内存。
    fn one_from_erased_(
        alloc: Arc<dyn Allocator + Send + Sync>,
        capacity: usize,
    ) -> Result<Self, AllocError> {
        let layout = Layout::array::<MaybeUninit<u8>>(capacity).map_err(|_| AllocError)?;
        let raw = alloc.allocate(layout)?;
        Result::Ok(MuxChanBuff {
            // SAFETY: `allocate` 返回的非空指针在 `layout` 下有效，`cap_`/`layout_`
            // 与之对应；本类型独占这块内存，直到 `Drop` 释放。
            ptr_: raw.cast::<MaybeUninit<u8>>(),
            cap_: capacity,
            layout_: layout,
            alloc_: alloc,
        })
    }

    /// 环内存的容量（`MaybeUninit<u8>` 的个数）。
    pub fn capacity_(&self) -> usize {
        self.cap_
    }
}

impl Borrow<[MaybeUninit<u8>]> for MuxChanBuff {
    fn borrow(&self) -> &[MaybeUninit<u8>] {
        // SAFETY: 指针与长度由构造时的分配保证（见 `one_from_erased_`），且 `&self`
        // 借用期间不会有别的可变借用（`Borrow`/`BorrowMut` 的常规约定）。
        unsafe { core::slice::from_raw_parts(self.ptr_.as_ptr(), self.cap_) }
    }
}

impl BorrowMut<[MaybeUninit<u8>]> for MuxChanBuff {
    fn borrow_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        // SAFETY: 同 `Borrow::borrow`；`&mut self` 保证独占。
        unsafe { core::slice::from_raw_parts_mut(self.ptr_.as_ptr(), self.cap_) }
    }
}

impl Drop for MuxChanBuff {
    fn drop(&mut self) {
        // SAFETY: 指针由 `alloc_` 按 `layout_` 分配而来，且只在本类型独占期间释放一次；
        // 分配器以 `Arc` 形式被本类型拥有，因此释放时它仍然有效。
        unsafe { self.alloc_.deallocate(self.ptr_.cast::<u8>(), self.layout_) };
    }
}

/// 用调用方注入的存储与分配器建立一条环，并把它切成 `(写端, 读端)`。
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

/// 测试专用：用 `CoreAlloc` 建内存环，供本 crate 的单测复用。
///
/// 放在这里而不是各个测试模块里，是为了让「环怎么建」只有一处实现——上游改动
/// 构建入口时只需要改这里。
#[cfg(test)]
pub(crate) mod test_support_ {
    use core::mem::MaybeUninit;

    use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};

    use super::{BufferedChannel, new_buffered_channel_};

    /// 测试用的环存储类型。
    pub(crate) type TestBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

    /// 建一条容量为 `capacity` 的内存环，返回 `(写端, 读端)`。
    ///
    /// 环存储用与生产路径相同的**借用型**形式（测试里直接泄漏一块内存）。
    pub(crate) fn make_test_channel_(capacity: usize) -> BufferedChannel<TestBuff, CoreAlloc> {
        let buffer = Owned::new_uninit_slice(capacity, CoreAlloc);
        new_buffered_channel_(buffer, CoreAlloc).expect("测试容量应当落在 ring 允许区间内")
    }
}
