//! 连接级统一错误 [`MuxError`]。
//!
//! 各 API 面的错误类型（`BindError` / `BindingError` / `ListenerError` /
//! `HandleError` / `TelegraphError`）由各自产生它们的模块定义；它们把连接级失败
//! 包在 `Mux(..)` 里，并复用本模块的 [`face_error_impls`] 生成公共实现。

use crate::flow_ctrl::FlowCtrlError;

/// 复用连接与子流操作失败的统一类型。
///
/// 它只承载**跨模块可共享**的失败语义：传输读 / 写错误在帧层与循环里已经按方向
/// 投影为 [`MuxError::Transport`]，因此本类型不需要携带传输错误载荷，可以安全地
/// 存进共享注册表、在 API 面与循环之间复制。
///
/// 各 API 面的错误类型把它包在 `Mux(..)` 里，例如
/// [`HandleError::Mux`](crate::connection::HandleError::Mux)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MuxError {
    /// 连接因底层传输错误而中断，无法继续收发。
    ///
    /// `write` 指出是哪一半先出的错：`true` = 写方向，`false` = 读方向。
    Transport { write: bool },

    /// 操作被取消令牌终止。
    Cancelled,

    /// **对端主动**关闭了连接或该子流（收到 `CLOSE` / `FIN`，或对端读端正常收尾）。
    PeerClosed,

    /// 目标子流已被关闭（本端关闭或已拆流）。
    Closed,

    /// 子流空闲超时：`max_channel_timeout` 内既无数据往来、也无保活应答。
    IdleTimeout,

    /// 该 dock 是保留取值，不能作为子流 dock。
    ReservedDock,

    /// 帧结构非法：缺少必需字段、字段重复、字段顺序与种类不符等。
    MalformedFrame,

    /// 未知 / 保留的字段标识，或字段宽度对该字段非法。
    UnsupportedField,

    /// 帧总长超过协商出的 `max_packet_size`。
    FrameTooLarge,

    /// 流控失败（窗口违例或计数溢出）。
    FlowCtrl(FlowCtrlError),
}

impl core::fmt::Display for MuxError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            MuxError::Transport { write: true } => f.write_str("复用连接写方向因传输错误中断"),
            MuxError::Transport { write: false } => f.write_str("复用连接读方向因传输错误中断"),
            MuxError::Cancelled => f.write_str("复用操作被取消"),
            MuxError::PeerClosed => f.write_str("对端已主动关闭连接或子流"),
            MuxError::Closed => f.write_str("子流已关闭"),
            MuxError::IdleTimeout => f.write_str("子流空闲超时：保活无应答"),
            MuxError::ReservedDock => f.write_str("wildcard / unspecified dock 不能作为子流 dock"),
            MuxError::MalformedFrame => f.write_str("复用帧结构非法"),
            MuxError::UnsupportedField => f.write_str("复用帧包含未知或非法的字段"),
            MuxError::FrameTooLarge => f.write_str("复用帧超过协商的最大报文长度"),
            MuxError::FlowCtrl(_) => f.write_str("流控失败"),
        }
    }
}

impl core::error::Error for MuxError {}

/// 为各 API 面的错误类型生成公共实现。
///
/// 各面错误均以 `Mux(MuxError)` 承载连接级失败；本宏生成 [`Display`](core::fmt::Display)、
/// [`Error`](core::error::Error)（`source` 指回内层 [`MuxError`]）与
/// [`From<MuxError>`]。
macro_rules! face_error_impls {
    ($name:ident $(, $pat:pat => $text:expr)* $(,)?) => {
        impl core::fmt::Display for $name {
            fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                match self {
                    $name::Mux(err) => core::fmt::Display::fmt(err, f),
                    $( $pat => f.write_str($text), )*
                }
            }
        }

        impl core::error::Error for $name {
            fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
                match self {
                    $name::Mux(err) => Option::Some(err),
                    _ => Option::None,
                }
            }
        }

        impl From<$crate::connection::MuxError> for $name {
            fn from(err: $crate::connection::MuxError) -> Self {
                $name::Mux(err)
            }
        }
    };
}

pub(crate) use face_error_impls;
