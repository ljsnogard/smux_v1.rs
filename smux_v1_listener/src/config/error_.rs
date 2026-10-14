//! 入向接受操作的自有错误类型。

/// **接受一条入向连接失败**。
///
/// # 为什么不直接用各后端的 `io::Error`
///
/// 三个后端的 `accept` 都返回 `std::io::Error`，但它**不实现 `Clone`**，而
/// listener 需要把一个「取消」错误值预存进等待器（见
/// [`NextWait_`](crate::listening::NextWait_)），因此错误类型必须是可克隆的。
///
/// 本类型只保留调用方真正会用的两样东西：错误码与一段说明。所以它不会替代
/// `io::Error` 的完整信息——需要原始错误的场景请直接使用后端的监听套接字
/// （`TcpTransport::listener()` 等入口把它借出来）。
///
/// # Examples
///
/// ```
/// use smux_v1_listener::config::InboundError;
///
/// let err = InboundError::new(
///     core::num::NonZeroI32::new(24),
///     "进程的 fd 用光了",
/// );
/// assert_eq!(err.raw_os_error(), Some(24));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("接受入向连接失败（错误码 {code_:?}）：{message_}")]
pub struct InboundError {
    /// 底层错误码；后端没有给出错误码时为 `None`。
    code_: Option<core::num::NonZeroI32>,

    /// 人类可读的说明。
    message_: alloc::string::String,
}

impl InboundError {
    /// 由错误码与说明构造。
    pub fn new(code: Option<core::num::NonZeroI32>, message: impl Into<alloc::string::String>) -> Self {
        InboundError {
            code_: code,
            message_: message.into(),
        }
    }

    /// 由后端的 `io::Error` 构造。
    pub fn from_io(err: &std::io::Error) -> Self {
        InboundError {
            code_: err.raw_os_error().and_then(core::num::NonZeroI32::new),
            message_: alloc::string::ToString::to_string(&err.kind()),
        }
    }

    /// 底层错误码（例如 `EMFILE` 是 24）。
    pub const fn raw_os_error(&self) -> Option<i32> {
        match self.code_ {
            Option::Some(code) => Option::Some(code.get()),
            Option::None => Option::None,
        }
    }

    /// 人类可读的说明。
    pub fn message(&self) -> &str {
        &self.message_
    }
}

impl From<std::io::Error> for InboundError {
    fn from(err: std::io::Error) -> Self {
        InboundError::from_io(&err)
    }
}
