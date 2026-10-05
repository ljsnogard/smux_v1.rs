//! 把**握手交付物**接成连接的省事入口。
//!
//! 一次连接建立是**两段**，本模块只管第二段：
//!
//! 1. **握手**：[`HandshakeAgent`] 只与两条半边打交道，协商出
//!    [`HandshakeDelivery`]（协商结果 + 归还的收发通道）。它**不需要本地作用域值**
//!    ——这一阶段没有任何东西要被投递到别处，纯粹是两条半边上的字节往返；
//! 2. **建连接**（本模块）：用默认配置铺开资源策略、造两块连接级帧暂存缓冲，再把
//!    交付物与五个收发循环交给 [`MuxConnection::new`]。**作用域值只在这一段出现**，
//!    因为五个循环要经它投递到本地队列（计时循环的等待能力也挂在后端类型上，
//!    因此作用域值上多了 `TrTime` 这一条约束）。
//!
//! 之所以把两段分开、而不是合成一个「从 socket 建连接」的调用：握手的协商条目、
//! 拒绝策略、要不要取消，都是**使用者真会碰**的东西，藏进一层 `async fn` 反而不好用。
//! 本模块只负责第二段里最省事的那一档——想自选分配器 / 流控 / 环存储 / 暂存容量，
//! 直接走 [`MuxConnection::new`]。
//!
//! [`HandshakeAgent`]: crate::handshake::agent::HandshakeAgent

use abs_art::{TrLocalScope, TrTime};
use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};

use crate::{
    connection::{BuffAllocError, DefaultConnCfg, MuxConnection, TrConnCfg},
    flow_ctrl::DefaultPolicy,
    handshake::agent::HandshakeDelivery,
};

impl<Tx, Rx, S> MuxConnection<DefaultConnCfg<Tx, Rx>, S>
where
    Tx: TrBuffWrite<u8> + 'static,
    Rx: TrBuffRead<u8> + 'static,
    S: TrLocalScope + TrTime + Clone + 'static,
{
    /// 用**默认配置**把一次成功的握手交付物接成连接。
    ///
    /// 等价于「[`DefaultConnCfg`] + [`TrConnCfg::make_stage_buffs`] +
    /// [`MuxConnection::new`]」三步；要自选资源策略就自己走那三步。
    ///
    /// 它是**同步**的：两段里只有握手需要 await，建连接本身没有等待点。
    ///
    /// # Errors
    ///
    /// 连接级帧暂存的分配失败时返回 [`BuffAllocError`]。
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // ① 握手：不需要作用域值。
    /// let delivery = HandshakeAgent::new(tx, rx)
    ///     .invite_async(&BasicOpts::default(), AcceptAllEntries)
    ///     .await?;
    /// // ② 建连接：作用域值只在这里出现。
    /// let conn = MuxConnection::from_delivery(scope, delivery)?;
    /// ```
    pub fn from_delivery(
        scope: &S,
        delivery: HandshakeDelivery<Tx, Rx>,
    ) -> Result<Self, BuffAllocError> {
        let (delivery, cfg) = DefaultConnCfg::new(delivery, DefaultPolicy);
        let (stage_r, stage_w) = cfg.make_stage_buffs(cfg.allocator())?;
        Result::Ok(MuxConnection::new(scope, delivery, cfg, stage_r, stage_w))
    }
}
