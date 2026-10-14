/// 以**字节**为单位的信用量（窗口大小）。
///
/// 窗口计数用 `u32`：单条子流的窗口不可能超过 [`TrFlowCtrlPolicy::max_window`](crate::flow_ctrl::TrFlowCtrlPolicy::max_window)
/// 的量级，而 `u32` 足以覆盖
/// [`crate::handshake::opts::BasicOpts::DEFAULT`] 中的 `max_packet_size`。
/// 与 `usize` 的换算在调用方完成：环容量是 `usize`，通告到线格式时收窄为
/// `u32`，收窄失败按 [`FlowCtrlError::Overflow`](crate::flow_ctrl::FlowCtrlError::Overflow) 处理。
pub type Credit = u32;


/// **累计**字节数（已经收到 / 已经发出去的总量）。
///
/// 通告必须携带「窗口对应的累计已收字节数」，否则发送方无法扣掉在途数据——推导
/// 见 [`WindowReport`](crate::flow_ctrl::WindowReport)。累计量单调不减，用 `u64` 以免在长寿命子流上回绕。
pub type RecvTotal = u64;
