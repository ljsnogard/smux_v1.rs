//! 握手错误类型。
//!
//! `HandshakeError` 同时承载读、写两侧的底层错误类型参数，使上层可以用一个
//! 错误类型覆盖整个握手流程。

use core::fmt;

use abs_buff::error::{ReadErrTag, TrTaggedError, WriteErrTag};

/// 握手过程中的失败。
///
/// # 泛型参数
///
/// - `RE`：底层读错误的类型（`TrBuffRead::Err`）；
/// - `WE`：底层写错误的类型（`TrBuffWrite::Err`）。
#[derive(Debug)]
pub enum HandshakeError<RE, WE> {
    /// 本端上层调用者拒绝了协商结果。
    Rejected,

    /// 对端发送了 `REJECT`。
    PeerRejected,

    /// 收到取消信号。
    Cancelled,

    /// 底层读错误。
    RxErr(RE),

    /// 底层写错误。
    TxErr(WE),

    /// 帧首 magic 不是本状态期望的值。
    InvalidMagic,

    /// 校验尾 CRC 不匹配。
    ChecksumErr,

    /// 帧长超过 `max_size` 或内部缓冲上限。
    FrameTooLarge,

    /// 条目区结构非法（重复键、基础项取值为 0、宽度越界等）。
    MalformedBody,

    /// 未知/保留键，或校验尾头不是 `0x1C` / `0x2C`。
    UnsupportedOption,

    /// `ACCEPT` / `CONFIRM` 与预期不一致。
    Inconsistent,

    /// 对端在帧中途关闭了连接。
    PeerClosed,
}

impl<RE, WE> fmt::Display for HandshakeError<RE, WE> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            HandshakeError::Rejected => "握手协商被本端上层调用者拒绝",
            HandshakeError::PeerRejected => "对端发送了 REJECT",
            HandshakeError::Cancelled => "握手被取消",
            HandshakeError::RxErr(_) => "握手读取出错",
            HandshakeError::TxErr(_) => "握手写入出错",
            HandshakeError::InvalidMagic => "握手帧 magic 不匹配",
            HandshakeError::ChecksumErr => "握手帧校验失败",
            HandshakeError::FrameTooLarge => "握手帧超过长度上限",
            HandshakeError::MalformedBody => "握手帧条目区结构非法",
            HandshakeError::UnsupportedOption => {
                "握手帧包含不支持的键或校验类型"
            }
            HandshakeError::Inconsistent => "握手协商结果与预期不一致",
            HandshakeError::PeerClosed => "对端在握手帧中途关闭连接",
        };
        f.write_str(text)
    }
}

impl<RE, WE> core::error::Error for HandshakeError<RE, WE>
where
    RE: core::error::Error,
    WE: core::error::Error,
{
}

/// 把底层读错误映射为握手错误。
///
/// 取消与对端关闭使用 [`ReadErrTag`] 区分，其余错误原样放入
/// [`HandshakeError::RxErr`]。
pub(crate) fn map_read_err_<RE, WE>(err: RE) -> HandshakeError<RE, WE>
where
    RE: TrTaggedError<ReadErrTag>,
{
    let tag = err.err_tag();
    match tag {
        ReadErrTag::Cancelled => HandshakeError::Cancelled,
        ReadErrTag::Closing => HandshakeError::PeerClosed,
        _ => HandshakeError::RxErr(err),
    }
}

/// 把底层写错误映射为握手错误。
pub(crate) fn map_write_err_<RE, WE>(err: WE) -> HandshakeError<RE, WE>
where
    WE: TrTaggedError<WriteErrTag>,
{
    let tag = err.err_tag();
    match tag {
        WriteErrTag::Cancelled => HandshakeError::Cancelled,
        _ => HandshakeError::TxErr(err),
    }
}
