//! 把**握手交付物**接成连接的省事入口。
//!
//! 一次连接建立是**两段**，本模块只管第二段：
//!
//! 1. **握手**：[`HandshakeAgent`] 只与两条半边打交道，协商出
//!    [`HandshakeDelivery`]（协商结果 + 归还的收发通道）。它**不需要本地作用域值**
//!    ——这一阶段没有任何东西要被投递到别处，纯粹是两条半边上的字节往返；
//! 2. **建连接**（本模块）：用默认配置铺开资源策略，把交付物、**调用方交出的两块
//!    连接级帧暂存缓冲**与五个收发循环交给 [`MuxConnection::new`]。**运行时值与作用域
//!    值都只在这一段出现**：前者提供计时与时刻，后者提供五个循环要投递进去的本地队列。
//!
//! 之所以把两段分开、而不是合成一个「从 socket 建连接」的调用：握手的协商条目、
//! 拒绝策略、要不要取消，都是**使用者真会碰**的东西，藏进一层 `async fn` 反而不好用。
//! 本模块只负责第二段里最省事的那一档——想自选分配器 / 流控 / 环存储 / 暂存容量，
//! 直接走 [`MuxConnection::new`]。
//!
//! [`HandshakeAgent`]: crate::handshake::agent::HandshakeAgent

use core::{
    alloc::AllocatorClone,
    mem::MaybeUninit,
};

use abs_mm::res_man::TrUnique;
use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use mm_ptr::x_deps::abs_mm;

use crate::{
    connection::{DefaultConnCfg, DefaultRt_, MuxChanBuffOwnedBy, MuxConnection},
    flow_ctrl::DefaultPolicy,
    handshake::agent::HandshakeDelivery,
    metrics::NoMetrics,
};

impl<Tx, Rx> MuxConnection<DefaultConnCfg<Tx, Rx, NoMetrics, DefaultPolicy, DefaultRt_>>
where
    Tx: TrBuffWrite<u8> + 'static,
    Rx: TrBuffRead<u8> + 'static,
{
    /// 用**默认配置**把一次成功的握手交付物接成连接。
    ///
    /// 等价于「[`DefaultConnCfg`] + [`MuxConnection::new`]」两步；要自选资源策略就自己走
    /// 那两步。两块帧暂存缓冲由调用方给出（类型任意 `TrUnique`，容量自定）。
    ///
    /// 运行时值与本地作用域都**由配置与后端自己解决**：建连时取
    /// `abs_art_bridge::current()`（默认后端的运行时值）、再由它交出 `local_scope()`。
    /// 需要显式控制时走 [`MuxConnection::new_with_rt`] 或自写一个 `C`。
    ///
    /// 它是**同步**的：两段里只有握手需要 await，建连接本身没有等待点。
    ///
    /// # Panics
    ///
    /// 调用点不在所选后端的运行时上下文内时 panic（同 [`DefaultConnCfg::new`]）。
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // ① 握手：不需要运行时值，也不需要作用域值。
    /// let delivery = HandshakeAgent::new(tx, rx)
    ///     .invite_async(&BasicOpts::default(), AcceptAllEntries)
    ///     .await?;
    /// // ② 建连接：运行时值与作用域都由连接自己取；两块帧暂存缓冲由调用方交出。
    /// let conn = MuxConnection::from_delivery(delivery, read_stage, write_stage);
    /// ```
    pub fn from_delivery<P>(
        delivery: HandshakeDelivery<Tx, Rx>,
        read_stage: MuxChanBuffOwnedBy<P>,
        write_stage: MuxChanBuffOwnedBy<P>,
    ) -> Self
    where
        P: TrUnique<Item = [MaybeUninit<u8>]> + Send + Sync,
        P::Alloc: AllocatorClone,
    {
        let (delivery, cfg) = DefaultConnCfg::new(delivery, DefaultPolicy);
        MuxConnection::new(delivery, cfg, read_stage, write_stage)
    }
}
