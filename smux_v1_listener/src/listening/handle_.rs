//! 已成形连接的句柄（占位）。
//!
//! # 当前状态
//!
//! 本类型**尚未参与交付路径**：[`SmuxListener`](crate::listening::SmuxListener) 的
//! [`next_async`](abs_async_iter::TrAsyncIterator::next_async) 直接交出
//! [`MuxConnection`](smux_v1::connection::MuxConnection)——它本身就已经是廉价可克隆的
//! 智能指针封装，中间再套一层句柄只会多一次间接。
//!
//! 保留本类型是因为它还有一个明确的将来用途：连接级的**看门狗与统计**（在途连接计数、
//! 建立时刻、关闭原因）。那些信息不属于 `MuxConnection`（它只描述复用协议本身），
//! 而属于「listener 交付了哪些连接」这一层。真要做时，它会携带一条连接 + 一份
//! listener 侧的账目句柄，并由 `next_async` 交付。
//!
//! 在它落地之前，本类型不提供任何方法，也不出现在任何签名里。

/// 一条已握手连接的 listener 侧句柄（占位，尚未参与交付路径）。
///
/// 用途与当前状态见模块文档。
pub struct SmuxConnectionHandle<C>
where
    C: crate::config::TrListenerConfig,
{
    /// 未来的连接句柄本体与账目句柄。当前刻意留空：没有交付路径就不该有字段，
    /// 否则会误导使用者以为它已经是可用的入口。
    _mark_: core::marker::PhantomData<fn() -> C>,
}
