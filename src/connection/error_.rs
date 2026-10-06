//! 连接级统一错误 [`MuxError`]。
//!
//! 各 API 面的错误类型（`BindError` / `BindingError` / `ListenerError` /
//! `HandleError` / `TelegraphError`）由各自产生它们的模块定义；它们把连接级失败
//! 包在 `Mux(..)` 里，并用 `#[error("{0}")]` + `#[from]` 复用本模块的实现：
//! `Display` 用内层文案、`source` 指回内层、`From<MuxError>` 让 `?` 直通。
//!
//! # 错误类型的写法约定
//!
//! 全 crate 的错误类型一律 `#[derive(thiserror::Error)]`，不再手写 `Display` /
//! `Error` / `From` 模板代码。三点约定：
//!
//! - **一律写全 `thiserror::Error`**，不额外 `use thiserror::Error`：本 crate 多处
//!   直接用 `core::error::Error` 作约束，短名 `Error` 会与它混淆；
//! - 包裹内层错误用 `#[error("{0}")] + #[from]`，**不用** `#[error(transparent)]`：
//!   后者的 `source()` 会穿透到内层自己的 `source`，而这里要的是「外层错误的
//!   source 就是内层」（`?` 直通、`downcast_ref::<MuxError>()` 可用）；
//! - 一个变体要按字段取值给不同文案时，把「取值 → 词」写成小函数放进 `#[error]`
//!   的参数（见 [`MuxError::Transport`] 与 `transport_direction_`）。

use crate::flow_ctrl::FlowCtrlError;

/// 复用连接与子流操作失败的统一类型。
///
/// 它只承载**跨模块可共享**的失败语义：传输读 / 写错误在帧层与循环里已经按方向
/// 投影为 [`MuxError::Transport`]，因此本类型不需要携带传输错误载荷，可以安全地
/// 存进共享注册表、在 API 面与循环之间复制。
///
/// 各 API 面的错误类型把它包在 `Mux(..)` 里，例如
/// [`HandleError::Mux`](crate::connection::HandleError::Mux)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MuxError {
    /// 连接因底层传输错误而中断，无法继续收发。
    ///
    /// `write` 指出是哪一半先出的错：`true` = 写方向，`false` = 读方向。
    #[error("复用连接{}因传输错误中断", transport_direction_(*write))]
    Transport {
        /// `true` = 写方向，`false` = 读方向。
        write: bool,
    },

    /// 操作被取消令牌终止。
    #[error("复用操作被取消")]
    Cancelled,

    /// **对端主动**关闭了连接或该子流（收到 `CLOSE` / `FIN`，或对端读端正常收尾）。
    #[error("对端已主动关闭连接或子流")]
    PeerClosed,

    /// 目标子流已被关闭（本端关闭或已拆流）。
    #[error("子流已关闭")]
    Closed,

    /// 子流超时：`max_channel_timeout` 内既无数据往来、也无保活应答。
    ///
    /// 它覆盖**两个阶段**，判据都是同一根存活时钟（连接内毫秒）：
    ///
    /// - **建流尚未裁决**：对端还没回 `ACCEPT` / `REJECT`（本端是发起方），或本端还没
    ///   裁决对端已经发出的 `OPEN`（本端是响应方）。此时连接会代替调用方**向对端回
    ///   `REJECT`** 并把原因留下：正在等待的 `accept_async` 直接得到本错误，而已经拿到
    ///   句柄、还没裁决的一侧之后调 `accept_async` / `reject_async` 也同样得到它；
    /// - **已建立子流**：空闲超时拆流（`CLOSE(FIN)` + `CLOSE(RESET)`），应用侧两个半部
    ///   经 `abort_reason()` 读到本错误。
    #[error("子流超时：保活无应答或建流未裁决")]
    IdleTimeout,

    /// 该 dock 是保留取值，不能作为子流 dock。
    #[error("wildcard / unspecified dock 不能作为子流 dock")]
    ReservedDock,

    /// 帧结构非法：缺少必需字段、字段重复、字段顺序与种类不符等。
    #[error("复用帧结构非法")]
    MalformedFrame,

    /// 未知 / 保留的字段标识，或字段宽度对该字段非法。
    #[error("复用帧包含未知或非法的字段")]
    UnsupportedField,

    /// 帧总长超过协商出的 `max_packet_size`。
    #[error("复用帧超过协商的最大报文长度")]
    FrameTooLarge,

    /// 流控失败（窗口违例或计数溢出）。
    #[error("流控失败")]
    FlowCtrl(FlowCtrlError),
}

/// `Transport { write }` 的方向描述词（只用于 [`MuxError`] 的 `Display`）。
///
/// 同一个变体要按字段取值给两句不同的文案，而 `thiserror` 的 `#[error(..)]` 只吃
/// 一个格式串——把「取值 → 词」这一步交给函数，文案与字段仍然只写一处。
fn transport_direction_(write: bool) -> &'static str {
    if write { "写方向" } else { "读方向" }
}

#[cfg(test)]
mod tests_ {
    use super::*;

    use crate::connection::{BindError, HandleError};

    /// 测试 `Display` 文案在改用 `thiserror` 之后**逐字未变**。
    ///
    /// - 手段：对若干代表性变体直接 `to_string()`：`Transport` 的两个方向
    ///   （同一变体按字段取不同文案）、载荷不参与文案的 `FlowCtrl`、以及各面的
    ///   固定文案变体。
    /// - 判断：字符串与改造前由 `face_error_impls!` / 手写 `Display` 生成的完全一致。
    #[test]
    fn display_texts_are_unchanged() {
        assert_eq!(
            MuxError::Transport { write: true }.to_string(),
            "复用连接写方向因传输错误中断"
        );
        assert_eq!(
            MuxError::Transport { write: false }.to_string(),
            "复用连接读方向因传输错误中断"
        );
        assert_eq!(
            MuxError::FlowCtrl(FlowCtrlError::Overflow).to_string(),
            "流控失败"
        );
        assert_eq!(MuxError::Closed.to_string(), "子流已关闭");
        assert_eq!(
            HandleError::RingRejected.to_string(),
            "调用方给出的环内存大小不合用，已拒绝接受"
        );
        assert_eq!(BindError::DockInUse.to_string(), "该 local_dock 已被占用");
    }

    /// 测试面错误对 `Mux(..)` 的**透传**语义与旧宏一致：`Display` 与 `source` 都指内层。
    ///
    /// - 手段：构造 `HandleError::Mux(MuxError::PeerClosed)`，取 `to_string()` 与
    ///   `core::error::Error::source`。
    /// - 判断：文案等于内层文案；`source` 为 `Some`，向下转型回 `MuxError` 后与内层相等。
    #[test]
    fn face_error_is_transparent_to_mux_error() {
        let err = HandleError::Mux(MuxError::PeerClosed);
        assert_eq!(err.to_string(), MuxError::PeerClosed.to_string());
        let source = core::error::Error::source(&err).expect("应当暴露内层 source");
        assert_eq!(
            source.downcast_ref::<MuxError>(),
            Option::Some(&MuxError::PeerClosed)
        );
    }

    /// 测试 `From<MuxError>` 仍然可用（旧宏生成的转换，现由 `#[from]` 提供）。
    ///
    /// - 手段：`BindError::from(MuxError::Cancelled)`。
    /// - 判断：得到 `BindError::Mux(MuxError::Cancelled)`——`?` 直通的前提。
    #[test]
    fn from_mux_error_still_works() {
        assert_eq!(
            BindError::from(MuxError::Cancelled),
            BindError::Mux(MuxError::Cancelled)
        );
    }
}
