//! 复用连接与子流的错误类型。
//!
//! 控制面（`TrConnection` / `TrDockBinding` / `TrChannelListener` /
//! `TrChannelHandle` / `TrTelegraph`）统一用 [`MuxError`]。
//!
//! 数据面（子流的 `TrBuffTryRead` / `TrBuffTryWrite`）**不复用** `MuxError`：
//! 子流半边只是 `buffex` 端半部的薄包装，因此它们的 `Err` 直接是 `buffex` 的
//! `ConsumerError` / `ProducerError`——这两者已经实现
//! [`TrTaggedError`](abs_buff::error::TrTaggedError) 并携带
//! `ReadErrTag` / `WriteErrTag`，足以让 `is_drained_closing()` 之类的判定脱离
//! 具体错误类型工作。

use crate::flow_ctrl::FlowCtrlError;

/// 复用连接与子流操作失败的统一类型。
///
/// # 泛型参数
///
/// - `RE`：底层网络读半边的错误类型；
/// - `WE`：底层网络写半边的错误类型。
///
/// 数据面不复用本类型直接作为 `Err`：子流半边的错误就是 `buffex` 端半部的
/// `ConsumerError` / `ProducerError`（已携带方向标签），见模块文档。
#[derive(Debug)]
pub enum MuxError<RE, WE> {
    /// 底层网络读失败。
    Rx(RE),

    /// 底层网络写失败。
    Tx(WE),

    /// 操作被取消令牌终止。
    Cancelled,

    /// 对端在帧中途关闭了连接。
    PeerClosed,

    /// 目标子流已被关闭（本端或对端）。
    Closed,

    /// 帧结构非法：缺少必需字段、字段重复、字段顺序与种类不符等。
    MalformedFrame,

    /// 未知 / 保留的字段标识，或字段宽度对该字段非法。
    UnsupportedField,

    /// 帧总长超过协商出的 `max_packet_size`。
    FrameTooLarge,

    /// 请求的 local_dock 已被占用：channel 与 telegraph 不得共用 dock。
    DockInUse,

    /// 该 dock 上的活动子流数已达 `max_dock_chan_count`。
    DockChanLimit,

    /// 连接上的活动子流数已达 `max_channel_count`。
    ChanLimit,

    /// 对端拒绝或无人监听（收到 `REJECT`）。
    Refused,

    /// 同一条子流上出现重复的建立 / 应答请求。
    Duplicate,

    /// 流控失败（窗口违例或计数溢出）。
    FlowCtrl(FlowCtrlError),
}

impl<RE, WE> core::fmt::Display for MuxError<RE, WE> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let text = match self {
            MuxError::Rx(_) => "复用连接读取失败",
            MuxError::Tx(_) => "复用连接写入失败",
            MuxError::Cancelled => "复用操作被取消",
            MuxError::PeerClosed => "对端在帧中途关闭连接",
            MuxError::Closed => "子流已关闭",
            MuxError::MalformedFrame => "复用帧结构非法",
            MuxError::UnsupportedField => "复用帧包含未知或非法的字段",
            MuxError::FrameTooLarge => "复用帧超过协商的最大报文长度",
            MuxError::DockInUse => "该 dock 已被 channel 或 telegraph 占用",
            MuxError::DockChanLimit => "该 dock 上的活动子流数已达上限",
            MuxError::ChanLimit => "连接上的活动子流数已达上限",
            MuxError::Refused => "对端拒绝建立子流",
            MuxError::Duplicate => "同一条子流上出现重复请求",
            MuxError::FlowCtrl(_) => "流控失败",
        };
        f.write_str(text)
    }
}

impl<RE, WE> core::error::Error for MuxError<RE, WE>
where
    RE: core::error::Error,
    WE: core::error::Error,
{
}
