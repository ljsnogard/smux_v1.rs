/// 流控失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowCtrlError {
    /// 对端发送的数据超过了本端**已通告**的接收窗口。
    ///
    /// 属于协议违例：调用方应当终止该子流，并可按需终止整条连接。
    PeerViolation,

    /// 窗口计数溢出，或窗口 / 容量无法收窄为 [`Credit`](crate::flow_ctrl::Credit)。
    ///
    /// 属于本端内部或配置错误，正常运行的协议不会触发。
    Overflow,
}


impl core::fmt::Display for FlowCtrlError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            FlowCtrlError::PeerViolation => f.write_str("对端发送数据超过已通告的接收窗口"),
            FlowCtrlError::Overflow => f.write_str("流控窗口计数溢出"),
        }
    }
}


impl core::error::Error for FlowCtrlError {}
