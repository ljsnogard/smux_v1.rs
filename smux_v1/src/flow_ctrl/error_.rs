/// 流控失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FlowCtrlError {
    /// 对端发送的数据超过了本端**已通告**的接收窗口。
    ///
    /// 属于协议违例：调用方应当终止该子流，并可按需终止整条连接。
    #[error("对端发送数据超过已通告的接收窗口")]
    PeerViolation,

    /// 窗口计数溢出，或窗口 / 容量无法收窄为 [`Credit`](crate::flow_ctrl::Credit)。
    ///
    /// 属于本端内部或配置错误，正常运行的协议不会触发。
    #[error("流控窗口计数溢出")]
    Overflow,
}
