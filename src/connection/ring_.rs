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
