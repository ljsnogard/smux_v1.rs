//! `metrics` 模块的单元测试。
//!
//! 端到端（真实连接上真的上报了多少帧 / 多少子流）由 `tests/` 下的集成测试负责；
//! 本文件只钉住两件事：**内置采集器的会计正确**与**空实现确实什么都不做**。
//!
//! # 内置采集器是**每份独立**的
//!
//! [`DebugMetrics`] 是 `Arc` 包住的一份计数：`default()` 造一份新的、`clone()` 与本体
//! 共享同一份。因此本文件的用例各自 `default()` 一份、互不干扰，断言可以精确到数值，
//! 也不必与并行执行的用例协调。

use crate::{
    connection::{Dock, FrameKind, MuxError},
    metrics::{FrameDir, NoMetrics, TrMetricsSink},
};

/// 测试 `NoMetrics` 是**完全空操作**：调遍每一个方法都不 panic、也不产生任何状态。
///
/// - 手段：构造 `NoMetrics`（零大小），把 trait 的每个方法都调用一次，含边界取值
///   （`bytes = 0`、`u64::MAX` 的时长、对端重置与超时两种关闭原因）。
/// - 判断：函数正常返回（没有 panic），且类型尺寸为 `0`。这是「不上报的装配不影响任何
///   行为」这条前提的直接证据——8 处 `impl TrConnCfg` 都取它作 `Metrics`。
#[test]
fn no_metrics_is_a_total_no_op_() {
    let sink = NoMetrics;
    let a = Dock::new(1u32);
    let b = Dock::new(2u32);

    sink.on_conn_opened();
    sink.on_conn_closed(crate::metrics::ConnCloseReason::Local, u64::MAX);
    sink.on_channel_opened(a, b);
    sink.on_channel_closed(
        a,
        b,
        crate::metrics::ChannelCloseReason::EstablishTimeout,
        0u64,
    );
    sink.on_frame(FrameDir::Send, a, b, FrameKind::Data, 0u32);
    sink.on_frame(FrameDir::Recv, a, b, FrameKind::Pulse, u32::MAX);
    sink.on_frame_error(Option::None, MuxError::MalformedFrame);
    sink.on_frame_error(
        Option::Some(FrameKind::Close),
        MuxError::FlowCtrl(crate::flow_ctrl::FlowCtrlError::Overflow),
    );
    sink.on_flow_stalled(a, b, 0u64);
    sink.on_reset(a, b, true);
    sink.on_reset(a, b, false);
    sink.on_transport_bytes(FrameDir::Send, u64::MAX);
    sink.on_transport_bytes(FrameDir::Recv, 0u64);

    // 零大小类型：`NoMetrics` 不携带任何状态，因此「上报」无处可存。
    assert_eq!(core::mem::size_of::<NoMetrics>(), 0usize);
}

#[cfg(feature = "metrics")]
mod debug_ {
    use crate::{
        connection::{Dock, FrameKind, MuxError},
        metrics::{
            ChannelCloseReason, ConnCloseReason, DebugMetrics, DebugSnapshot, FrameDir,
            TrMetricsSink,
        },
    };

    /// 测试内置采集器的会计：**按帧种类分桶**、总量、以及 [`DebugMetrics::reset`] 的清零。
    ///
    /// - 手段：先把进程级计数清零，再经零大小的 `DebugMetrics` 上报若干帧（两个方向、
    ///   两类帧、字节数互不相同）与各一次连接 / 子流 / 错误 / 重置 / 连接级字节事件；
    ///   取快照断言；最后再 `reset` 一次并断言快照回到全零。
    /// - 判断：总量等于逐笔之和、分桶只落在对应下标（`Data` = 4、`Pulse` = 7，见
    ///   [`crate::metrics::K_KIND_SLOTS`]）、`reset` 之后**每个字段**都归零（含两个分桶
    ///   数组）。
    ///
    /// 本用例用自己的那一份 `DebugMetrics`，因此与并行执行的用例互不干扰。
    #[test]
    fn debug_metrics_accounting_() {
        // 每份独立：自己造一份，起点必定是空的。
        let sink = DebugMetrics::default();
        let a = Dock::new(3u32);
        let b = Dock::new(7u32);

        // 帧：发送侧两类各一、接收侧一类一。
        sink.on_frame(FrameDir::Send, a, b, FrameKind::Data, 100u32);
        sink.on_frame(FrameDir::Send, a, b, FrameKind::Pulse, 20u32);
        sink.on_frame(FrameDir::Recv, a, b, FrameKind::Data, 7u32);

        // 连接 / 子流 / 错误 / 重置 / 连接级字节。
        sink.on_conn_opened();
        sink.on_conn_closed(ConnCloseReason::PeerClosed, 5u64);
        sink.on_channel_opened(a, b);
        sink.on_channel_closed(a, b, ChannelCloseReason::Fin, 3u64);
        sink.on_frame_error(Option::Some(FrameKind::Open), MuxError::FrameTooLarge);
        sink.on_flow_stalled(a, b, 2u64);
        sink.on_reset(a, b, true);
        sink.on_reset(a, b, false);
        sink.on_transport_bytes(FrameDir::Send, 1024u64);

        let snap = sink.snapshot();
        assert_eq!(snap.frames_sent, 2u64);
        assert_eq!(snap.frames_recv, 1u64);
        assert_eq!(snap.frame_bytes_sent, 120u64);
        assert_eq!(snap.frame_bytes_recv, 7u64);
        assert_eq!(snap.sent_of(FrameKind::Data), 1u64);
        assert_eq!(snap.sent_of(FrameKind::Pulse), 1u64);
        assert_eq!(snap.recv_of(FrameKind::Data), 1u64);
        assert_eq!(snap.recv_of(FrameKind::Pulse), 0u64);
        assert_eq!(snap.conns_opened, 1u64);
        assert_eq!(snap.conns_closed, 1u64);
        assert_eq!(snap.channels_opened, 1u64);
        assert_eq!(snap.channels_closed, 1u64);
        assert_eq!(snap.frame_errors, 1u64);
        assert_eq!(snap.flow_stalled, 1u64);
        assert_eq!(snap.resets_by_peer, 1u64);
        assert_eq!(snap.resets_by_local, 1u64);
        assert_eq!(snap.transport_bytes_sent, 1024u64);

        // 逐笔之和与总量必须相等：分桶不是另一套账。
        let sent_sum: u64 = snap.frames_sent_by_kind.iter().sum();
        assert_eq!(sent_sum, snap.frames_sent);

        // 清零：不遗漏任何字段（与「全新快照」逐字段相等）。
        sink.reset();
        assert_eq!(sink.snapshot(), DebugSnapshot::default());
    }
}
