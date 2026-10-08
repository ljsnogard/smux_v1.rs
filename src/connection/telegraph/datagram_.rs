//! 用户收到的**一条数据报**：载荷段 + 远端地址。
//!
//! # 它为什么长这样
//!
//! 上游 `abs_smux` 把「收一条报文」定义成一个**自描述的对象**：它既是一段可读的
//! 载荷（[`TrBuffSegmRef`]），又能回答这条报文来自哪个远端垛口
//! （[`TrRecvDatagram::remote_dock`]）。本类型就是那条契约在 `smux_v1` 上的实现。
//!
//! # 载荷段是怎么得到的
//!
//! 接收环里存的是**整帧原始字节**（帧头 + 载荷）。构造本类型时，接收侧先只读地
//! 解析出帧头（得到远端地址与帧头长度），再把帧头那段**消费掉**——消费量记在同一个
//! 段的偏移上，于是剩下的部分恰好就是载荷。
//!
//! 因此本类型内部的段**不含帧头**，`least_count()` / `iter_slices()` / `take_segm_ref()`
//! 一律只反映载荷；远端地址则是构造时就从帧头里拆出来并存下的。

use abs_buff::{
    Demand,
    buffer::{SegmRef, TrBuffSegmRef, TrBuffSegmView},
};
use buffex::x_deps::abs_buff;
use abs_smux::telegraph::TrRecvDatagram;

use crate::connection::{Dock, TrConnCfg};

/// 用户收到的一条数据报（载荷段 + 远端地址）。
///
/// 类型参数 `S` 是承载载荷的那个**段引用**（由接收环借出）。本类型对它的全部操作
/// 都直接转发，因此它与 `S` 的借用语义完全一致：段被丢弃时把已消费量归还给环。
pub struct RecvDatagram<'a, S>
where
    S: TrBuffSegmRef<'a, u8>,
{
    /// 本条报文的**远端地址**：帧头里的 `RemoteDock`（对本端而言就是发送方写下的
    /// 目的地址）。数据报的远端是**地址**而不是身份，因此这里逐条给出。
    remote_dock_: Dock,

    /// 载荷段。构造时帧头已经被消费掉（见模块文档），因此它的起点就是报文内容的
    /// 第一个字节。
    segm_: S,

    /// 生命周期标记：段 `S` 的借用周期写进了它的类型（`S: TrBuffSegmRef<'a, u8>`），
    /// 但本结构体自身还要把 `'a` 用在关联类型上，因此需要它显式出现。
    _marker_: core::marker::PhantomData<&'a ()>,
}

impl<'a, S> RecvDatagram<'a, S>
where
    S: TrBuffSegmRef<'a, u8>,
{
    /// 由远端地址与**已跳过帧头**的载荷段构造（只允许接收侧调用）。
    pub(crate) fn new_(remote_dock: Dock, segm: S) -> Self {
        RecvDatagram {
            remote_dock_: remote_dock,
            segm_: segm,
            _marker_: core::marker::PhantomData,
        }
    }
}

impl<'a, S> TrBuffSegmView for RecvDatagram<'a, S>
where
    S: TrBuffSegmRef<'a, u8>,
{
    type SlicesIter<'f>
        = S::SlicesIter<'f>
    where
        Self: 'f;

    type Item = u8;

    #[inline]
    fn is_empty(&self) -> bool {
        self.segm_.is_empty()
    }

    #[inline]
    fn least_count(&self) -> usize {
        self.segm_.least_count()
    }

    #[inline]
    fn iter_slices(&self) -> <Self as TrBuffSegmView>::SlicesIter<'_> {
        self.segm_.iter_slices()
    }
}

impl<'a, S> TrBuffSegmRef<'a, u8> for RecvDatagram<'a, S>
where
    S: TrBuffSegmRef<'a, u8>,
{
    type Reclaimer<'f>
        = S::Reclaimer<'f>
    where
        Self: 'f,
        S: 'f;

    type TakeSegmRef<'f>
        = S::TakeSegmRef<'f>
    where
        Self: 'f,
        S: 'f;

    #[inline]
    fn take_segm_ref<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> <Self as TrBuffSegmRef<'a, u8>>::TakeSegmRef<'f> {
        self.segm_.take_segm_ref(demand)
    }

    #[inline]
    fn as_segm_ref<'f>(&'f mut self) -> SegmRef<'f, u8, Self::Reclaimer<'f>> {
        self.segm_.as_segm_ref()
    }
}

impl<'a, C, S> TrRecvDatagram<'a, C> for RecvDatagram<'a, S>
where
    C: TrConnCfg,
    S: TrBuffSegmRef<'a, u8>,
{
    fn remote_dock(&self) -> C::Dock {
        self.remote_dock_
    }
}
