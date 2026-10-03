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

use core::{
    alloc::AllocatorClone,
    borrow::BorrowMut,
    mem::MaybeUninit,
};

use buffex::ring::{Ring, RingReader, RingWriter};
use mm_ptr::Shared;

/// **类型擦除的环载具**：把「这条子流的缓冲由什么承载」交给使用环境决定。
///
/// # 为什么它是公开类型（但仍属实现细节）
///
/// 它会出现在 [`ChannelTx`](crate::connection::ChannelTx) /
/// [`ChannelRx`](crate::connection::ChannelRx) 的 `TrBuffTryWrite` / `TrBuffTryRead`
/// 关联类型里（那些 GAT 就是环自己的段类型），因此不能是 crate 私有；但它**不是**给
/// 使用者手工构造的类型（构造入口是 `ChanBuff` 的 `prepare` 返回值），所以标
/// `#[doc(hidden)]`。
///
/// 环本身只要求缓冲满足 `BorrowMut<[MaybeUninit<u8>]>`，因此任何存储都能用；但连接
/// 的两个循环、两条事件通道与两个半部的类型必须先定下来（它们各自只被创建一次），
/// 所以连接侧需要一个**固定**的缓冲类型。本类型就是这个固定类型：它内部是一个
/// trait object，于是**每条子流**都可以用**不同的具体存储**（自有所有权、借用切片、
/// 池分配、`Vec`、静态区……），由 [`TrPrepareChannelBuff`] 在最终裁决时交进来。
///
/// 为什么需要一层 newtype：`Box<T>` 只为 `T` 自身实现 `BorrowMut<T>`，`Box<dyn
/// BorrowMut<[MaybeUninit<u8>]>>` **不是** `BorrowMut<[MaybeUninit<u8>]>`；这里显式
/// 把 `borrow_mut` 转发给内部 trait object。
///
/// 代价：每条子流每方向一次装箱（冷路径，建流时），以及环每次取切片时一次虚调用。
/// 换来的是连接对缓冲类型**一无所知**——不再需要「传进来的缓冲类型必须等于配置里
/// 声明的类型」这种等式约束。
///
/// [`TrPrepareChannelBuff`]: abs_smux::chan::TrPrepareChannelBuff
#[doc(hidden)]
pub struct MuxChanBuff {
    inner_: Box<dyn BorrowMut<[MaybeUninit<u8>]> + Send + Sync>,
}

impl MuxChanBuff {
    /// 把任意满足环要求的存储装箱成载具。
    pub(crate) fn boxed_<B>(buffer: B) -> Self
    where
        B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync + 'static,
    {
        MuxChanBuff {
            inner_: Box::new(buffer),
        }
    }
}

impl MuxChanBuff {
    /// 底层存储的容量（字节）。
    ///
    /// 建流最终裁决用它算接收窗口与环大小；用固有方法而不是 `borrow_mut()`，是因为
    /// `BorrowMut<Borrowed>` 的 `Borrowed` 在这里只能靠标注推断。
    pub(crate) fn capacity_(&self) -> usize {
        core::borrow::Borrow::<[MaybeUninit<u8>]>::borrow(&*self.inner_).len()
    }
}

impl core::borrow::Borrow<[MaybeUninit<u8>]> for MuxChanBuff {
    fn borrow(&self) -> &[MaybeUninit<u8>] {
        // 显式 UFCS + 解引用：`Box<T>` 自己也有 `Borrow<T>` 实现，直接 `.borrow()`
        // 会选中 `Box` 那一层（借出 `dyn …` 而不是底层切片）。
        core::borrow::Borrow::<[MaybeUninit<u8>]>::borrow(&*self.inner_)
    }
}

impl BorrowMut<[MaybeUninit<u8>]> for MuxChanBuff {
    fn borrow_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        BorrowMut::<[MaybeUninit<u8>]>::borrow_mut(&mut *self.inner_)
    }
}

/// 子流**发送**半边的环端类型（写端）。
pub type BufferedTx<B, A> = RingWriter<Shared<Ring<B>, A>, B, u8>;

/// 子流**接收**半边的环端类型（读端）。
pub type BufferedRx<B, A> = RingReader<Shared<Ring<B>, A>, B, u8>;

/// 一条子流收发环的两个半部：`(写端, 读端)`，共享同一条 [`Ring`]。
pub type BufferedChannel<B, A> = (BufferedTx<B, A>, BufferedRx<B, A>);

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

    use super::{BufferedChannel, MuxChanBuff, new_buffered_channel_};

    /// 测试用的环存储类型（**具体**类型；生产路径上它会被装箱成 [`MuxChanBuff`]）。
    pub(crate) type TestBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

    /// 建一条容量为 `capacity` 的内存环，返回 `(写端, 读端)`。
    ///
    /// 缓冲同样经 [`MuxChanBuff`] 装箱：这样半部类型与生产路径一致，测试不会因为
    /// 「测试用了具体类型、生产用了载具」而在类型上分叉。
    pub(crate) fn make_test_channel_(capacity: usize) -> BufferedChannel<MuxChanBuff, CoreAlloc> {
        let buffer = MuxChanBuff::boxed_(Owned::<[MaybeUninit<u8>], CoreAlloc>::new_uninit_slice(
            capacity, CoreAlloc,
        ));
        new_buffered_channel_(buffer, CoreAlloc).expect("测试容量应当落在 ring 允许区间内")
    }
}

#[cfg(test)]
mod tests_ {
    use core::{borrow::BorrowMut, mem::MaybeUninit};

    use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};

    use super::MuxChanBuff;

    /// 测试目标：擦除载具能把**不同具体类型**的存储装进同一个 `MuxChanBuff`，并如实
    /// 转发切片。
    ///
    /// - 手段：把 `Owned<[MaybeUninit<u8>], CoreAlloc>`（容量 8）与
    ///   `Box<[MaybeUninit<u8>]>`（容量 16）分别装箱，各自 `borrow_mut()` 取切片并在
    ///   首字节写入 `0xAB`，再 `borrow()` 取只读切片。
    /// - 判断：两个载具报告的容量分别是 8 与 16（**容量来自调用方存储**），写入的字节
    ///   能在同一个载具的只读切片上读到。任一不满足即 panic。
    ///
    /// 环级别的读写往返（即 `buffex` 确实接受该载具）由 `tests/` 的 channel 场景覆盖。
    #[test]
    fn erased_carrier_forwards_and_reports_capacity_test_() {
        let mut owned: MuxChanBuff =
            MuxChanBuff::boxed_(Owned::<[MaybeUninit<u8>], CoreAlloc>::new_uninit_slice(
                8usize, CoreAlloc,
            ));
        let mut boxed: MuxChanBuff =
            MuxChanBuff::boxed_(vec![MaybeUninit::<u8>::uninit(); 16].into_boxed_slice());

        for (buff, capacity) in [(&mut owned, 8usize), (&mut boxed, 16usize)] {
            let ptr = {
                let slice: &mut [MaybeUninit<u8>] = BorrowMut::borrow_mut(buff);
                assert_eq!(slice.len(), capacity, "载具应当如实报告底层存储容量");
                slice[0].write(0xABu8);
                slice.as_ptr()
            };

            let view: &[MaybeUninit<u8>] =
                core::borrow::Borrow::<[MaybeUninit<u8>]>::borrow(buff);
            assert_eq!(view.len(), capacity, "只读切片长度应当与容量一致");
            assert!(
                core::ptr::eq(view.as_ptr(), ptr),
                "只读切片应当指向同一块底层存储"
            );
            // SAFETY: 上面刚在同一位置写入 `0xAB`，且 `MaybeUninit<u8>` 无需校验位模式。
            assert_eq!(unsafe { view[0].assume_init() }, 0xABu8, "写入应当可见");
        }
    }
}
