//! 指标采集：本 demo 自己的 sink，以及快照 / 增量口径。
//!
//! # 为什么要自己写 sink
//!
//! `smux_v1` 内置的 [`DebugMetrics`] 给了帧数、字节数、连接 / 子流数、流控阻塞**次数**，
//! 但它把 `on_channel_closed` 的 `lifetime_millis`（子流寿命）与 `on_flow_stalled` 的
//! `millis`（阻塞持续时长）丢掉了。本 demo 想量「子流寿命」这类延迟口径，因此在这里
//! **组合**内置采集器：计数部分转发给它，时长部分另外记一份。
//!
//! # sink 句柄怎么共享
//!
//! `smux_v1` 刻意不做全局注册表；`DefaultConnCfg` 又是经 `M::default()` 造 sink 的。
//! 于是这里让 [`DemoMetrics`] 的 `Default` 返回**进程级静态句柄**（`OnceLock`），
//! 每条连接 `clone` 到的是同一份计数。场景结束后直接 [`DemoMetrics::global`] 读快照，
//! 不需要自定义配置类型，也不需要把 sink 从连接里掏出来。
//!
//! 代价要说清楚：这份计数是**进程级**的，不区分连接。需要在同一进程里分辨不同连接的
//! 计数时，应按 `smux_v1/tests/metrics_e2e.rs` 的 `MetricsCfg` 写自定义配置类型，
//! 让每条连接各持一份句柄。

use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};

use smux_v1::{
    connection::{Dock, FrameKind, MuxError},
    metrics::{
        ChannelCloseReason, ConnCloseReason, DebugMetrics, DebugSnapshot, FrameDir, K_KIND_SLOTS,
        TrMetricsSink,
    },
};

/// 全部计数都用 `Relaxed`：它们只是观测值，不承担同步语义。
const K_ORDER: Ordering = Ordering::Relaxed;

/// 子流寿命的分桶个数：`[<10ms, <100ms, <1s, <10s, >=10s]`。
const K_BUCKETS: usize = 5usize;

/// `frames_*_by_kind` 里几种本 demo 关心的下标（顺序见 `K_KIND_SLOTS` 的表）。
const K_IDX_CLOSE: usize = 3usize;
const K_IDX_DATA: usize = 4usize;
const K_IDX_WINDOW_UPDATE: usize = 6usize;
const K_IDX_PULSE: usize = 7usize;

/// 内置采集器丢掉的那些量。
#[derive(Debug)]
struct Extra_ {
    /// 已关闭的子流数（内置采集器也有一份，这里为了配对寿命而重记）。
    channels_closed_: AtomicU64,
    /// 子流寿命之和（毫秒）。
    lifetime_sum_ms_: AtomicU64,
    /// 子流寿命最大值（毫秒）。
    lifetime_max_ms_: AtomicU64,
    /// 子流寿命分桶计数。
    lifetime_buckets_: [AtomicU64; K_BUCKETS],
    /// 流控阻塞的持续时长之和（毫秒）。
    flow_stall_ms_: AtomicU64,
    /// **第一条**帧 / 协议错误的详情（排障用，不参与计数）。
    ///
    /// 为什么记在内存里而不是当场 `eprintln`：「静默挂起」类问题是竞态，而 stderr 的
    /// 同步写会改变时序、让竞态躲起来。这里只做一次内存写，随后随结果行一起输出。
    frame_error_note_: Mutex<Option<String>>,
}

impl Extra_ {
    /// 造一份全 0 的额外计数。
    fn new_() -> Self {
        Extra_ {
            channels_closed_: AtomicU64::new(0u64),
            lifetime_sum_ms_: AtomicU64::new(0u64),
            lifetime_max_ms_: AtomicU64::new(0u64),
            lifetime_buckets_: core::array::from_fn(|_| AtomicU64::new(0u64)),
            flow_stall_ms_: AtomicU64::new(0u64),
            frame_error_note_: Mutex::new(None),
        }
    }
}

/// 本 demo 的 metrics sink：内置计数 + 时长口径。
#[derive(Debug, Clone)]
pub struct DemoMetrics {
    /// 内置采集器（帧数 / 字节数 / 连接数 / 子流数 / 阻塞次数）。
    inner_: DebugMetrics,
    /// 内置采集器丢掉的时长口径。
    extra_: Arc<Extra_>,
}

impl Default for DemoMetrics {
    /// 返回**进程级**句柄（见模块文档）。
    fn default() -> Self {
        global_()
    }
}

impl DemoMetrics {
    /// 取进程级句柄。
    pub fn global() -> Self {
        global_()
    }

    /// 造一份**独立**的计数（不挂进程级句柄）。
    ///
    /// 单元测试用它避开共享状态；需要「每条连接一份计数」时也可以用它是同一形状。
    pub fn fresh() -> Self {
        DemoMetrics {
            inner_: DebugMetrics::default(),
            extra_: Arc::new(Extra_::new_()),
        }
    }

    /// 取**第一条**帧 / 协议错误的详情（没有时为 `None`）。
    pub fn frame_error_note(&self) -> Option<String> {
        self.extra_
            .frame_error_note_
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
    }

    /// 取一份读数快照。
    ///
    /// 每个计数各读一次原子量、不做整体串行化，因此并发下不是一致切面（与内置采集器
    /// 同一取舍）。本 demo 都在两端数据面收尾之后读，不并发，因此是准的。
    pub fn snapshot(&self) -> MetricsSnapshot {
        let mut buckets = [0u64; K_BUCKETS];
        for (i, slot) in self.extra_.lifetime_buckets_.iter().enumerate() {
            buckets[i] = slot.load(K_ORDER);
        }
        MetricsSnapshot {
            debug: self.inner_.snapshot(),
            extra: ExtraSnapshot {
                channels_closed: self.extra_.channels_closed_.load(K_ORDER),
                lifetime_sum_ms: self.extra_.lifetime_sum_ms_.load(K_ORDER),
                lifetime_max_ms: self.extra_.lifetime_max_ms_.load(K_ORDER),
                lifetime_buckets: buckets,
                flow_stall_ms: self.extra_.flow_stall_ms_.load(K_ORDER),
            },
        }
    }
}

/// 取（必要时创建）进程级 sink。
fn global_() -> DemoMetrics {
    static GLOBAL: OnceLock<DemoMetrics> = OnceLock::new();
    GLOBAL
        .get_or_init(|| DemoMetrics {
            inner_: DebugMetrics::default(),
            extra_: Arc::new(Extra_::new_()),
        })
        .clone()
}

impl TrMetricsSink for DemoMetrics {
    fn on_conn_opened(&self) {
        self.inner_.on_conn_opened();
    }

    fn on_conn_closed(&self, reason: ConnCloseReason, lifetime_millis: u64) {
        self.inner_.on_conn_closed(reason, lifetime_millis);
    }

    fn on_channel_opened(&self, local: Dock, remote: Dock) {
        self.inner_.on_channel_opened(local, remote);
    }

    fn on_channel_closed(
        &self,
        local: Dock,
        remote: Dock,
        reason: ChannelCloseReason,
        lifetime_millis: u64,
    ) {
        self.inner_
            .on_channel_closed(local, remote, reason, lifetime_millis);
        self.extra_.channels_closed_.fetch_add(1u64, K_ORDER);
        self.extra_
            .lifetime_sum_ms_
            .fetch_add(lifetime_millis, K_ORDER);
        self.extra_
            .lifetime_max_ms_
            .fetch_max(lifetime_millis, K_ORDER);
        let bucket = match lifetime_millis {
            0..=9 => 0usize,
            10..=99 => 1usize,
            100..=999 => 2usize,
            1_000..=9_999 => 3usize,
            _ => 4usize,
        };
        self.extra_.lifetime_buckets_[bucket].fetch_add(1u64, K_ORDER);
    }

    fn on_frame(&self, dir: FrameDir, local: Dock, remote: Dock, kind: FrameKind, bytes: u32) {
        // 逐帧热路径：只转发给内置采集器（它内部是 `Relaxed` 原子加法）。
        self.inner_.on_frame(dir, local, remote, kind, bytes);
    }

    fn on_frame_error(&self, kind: Option<FrameKind>, err: MuxError) {
        // 帧 / 协议错误是「静默挂起」类问题的第一手证据：除了计数之外，把**第一条**的
        // 详情记在内存里（随结果行输出）；`SMUX_DEMO_TRACE` 打开时另留一条 trace。
        if let Ok(mut slot) = self.extra_.frame_error_note_.lock()
            && slot.is_none()
        {
            *slot = Some(format!("kind={kind:?} err={err}"));
        }
        crate::trace_!("帧 / 协议错误：kind={kind:?} err={err}");
        self.inner_.on_frame_error(kind, err);
    }

    fn on_flow_stalled(&self, local: Dock, remote: Dock, millis: u64) {
        self.inner_.on_flow_stalled(local, remote, millis);
        self.extra_.flow_stall_ms_.fetch_add(millis, K_ORDER);
    }

    fn on_reset(&self, local: Dock, remote: Dock, by_peer: bool) {
        self.inner_.on_reset(local, remote, by_peer);
    }

    /// 数据报整条丢弃：转发给内置采集器（它不采集这一项，见其实现），本 demo 也不额外
    /// 记账——「接收环装不下整条就丢弃」是尽力交付下的正常事件，结果行里没有它的位置。
    fn on_datagram_dropped(&self, local: Dock, remote: Dock, bytes: u32) {
        self.inner_.on_datagram_dropped(local, remote, bytes);
    }

    fn on_transport_bytes(&self, dir: FrameDir, bytes: u64) {
        self.inner_.on_transport_bytes(dir, bytes);
    }
}

/// 内置采集器丢掉的时长口径的一份读数。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExtraSnapshot {
    /// 已关闭子流数。
    pub channels_closed: u64,
    /// 子流寿命之和（毫秒）。
    pub lifetime_sum_ms: u64,
    /// 子流寿命最大值（毫秒）。
    pub lifetime_max_ms: u64,
    /// 子流寿命分桶：`[<10ms, <100ms, <1s, <10s, >=10s]`。
    pub lifetime_buckets: [u64; K_BUCKETS],
    /// 流控阻塞持续时长之和（毫秒）。
    pub flow_stall_ms: u64,
}

/// 一次读数。
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    /// 内置采集器的读数。
    pub debug: DebugSnapshot,
    /// 时长口径的读数。
    pub extra: ExtraSnapshot,
}

impl MetricsSnapshot {
    /// 与更早的一次读数做差，得到这一段的增量。
    ///
    /// 用 `saturating_sub`：内置采集器的 `reset()` 或读序不同都可能让后一次更小，
    /// 这种时候按 0 处理，不 panic。
    pub fn delta(&self, base: &Self) -> MetricsDelta {
        let d = &self.debug;
        let b = &base.debug;
        let mut frames_sent_by_kind = [0u64; K_KIND_SLOTS];
        let mut frames_recv_by_kind = [0u64; K_KIND_SLOTS];
        for i in 0..K_KIND_SLOTS {
            frames_sent_by_kind[i] =
                d.frames_sent_by_kind[i].saturating_sub(b.frames_sent_by_kind[i]);
            frames_recv_by_kind[i] =
                d.frames_recv_by_kind[i].saturating_sub(b.frames_recv_by_kind[i]);
        }
        let mut buckets = [0u64; K_BUCKETS];
        for i in 0..K_BUCKETS {
            buckets[i] =
                self.extra.lifetime_buckets[i].saturating_sub(base.extra.lifetime_buckets[i]);
        }
        MetricsDelta {
            conns_opened: d.conns_opened.saturating_sub(b.conns_opened),
            conns_closed: d.conns_closed.saturating_sub(b.conns_closed),
            channels_opened: d.channels_opened.saturating_sub(b.channels_opened),
            channels_closed: self
                .extra
                .channels_closed
                .saturating_sub(base.extra.channels_closed),
            frames_sent: d.frames_sent.saturating_sub(b.frames_sent),
            frames_recv: d.frames_recv.saturating_sub(b.frames_recv),
            data_frames_sent: frames_sent_by_kind[K_IDX_DATA],
            data_frames_recv: frames_recv_by_kind[K_IDX_DATA],
            window_updates_sent: frames_sent_by_kind[K_IDX_WINDOW_UPDATE],
            window_updates_recv: frames_recv_by_kind[K_IDX_WINDOW_UPDATE],
            pulse_sent: frames_sent_by_kind[K_IDX_PULSE],
            pulse_recv: frames_recv_by_kind[K_IDX_PULSE],
            close_sent: frames_sent_by_kind[K_IDX_CLOSE],
            close_recv: frames_recv_by_kind[K_IDX_CLOSE],
            frame_bytes_sent: d.frame_bytes_sent.saturating_sub(b.frame_bytes_sent),
            frame_bytes_recv: d.frame_bytes_recv.saturating_sub(b.frame_bytes_recv),
            transport_bytes_sent: d
                .transport_bytes_sent
                .saturating_sub(b.transport_bytes_sent),
            transport_bytes_recv: d
                .transport_bytes_recv
                .saturating_sub(b.transport_bytes_recv),
            resets_by_local: d.resets_by_local.saturating_sub(b.resets_by_local),
            resets_by_peer: d.resets_by_peer.saturating_sub(b.resets_by_peer),
            flow_stalled: d.flow_stalled.saturating_sub(b.flow_stalled),
            flow_stall_ms: self
                .extra
                .flow_stall_ms
                .saturating_sub(base.extra.flow_stall_ms),
            frame_errors: d.frame_errors.saturating_sub(b.frame_errors),
            channel_lifetime_sum_ms: self
                .extra
                .lifetime_sum_ms
                .saturating_sub(base.extra.lifetime_sum_ms),
            channel_lifetime_max_ms: self.extra.lifetime_max_ms,
            channel_lifetime_buckets: buckets,
        }
    }
}

/// 一段区间的指标增量（进程级或阶段级）。
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsDelta {
    /// 建成的连接数。
    pub conns_opened: u64,
    /// 关闭的连接数。
    pub conns_closed: u64,
    /// 建成的子流数。
    pub channels_opened: u64,
    /// 关闭的子流数。
    pub channels_closed: u64,
    /// 帧总数（发）。
    pub frames_sent: u64,
    /// 帧总数（收）。
    pub frames_recv: u64,
    /// `DATA` 帧数（发）。
    pub data_frames_sent: u64,
    /// `DATA` 帧数（收）。
    pub data_frames_recv: u64,
    /// `WINDOW_UPDATE` 帧数（发）。
    pub window_updates_sent: u64,
    /// `WINDOW_UPDATE` 帧数（收）。
    pub window_updates_recv: u64,
    /// `PULSE` 帧数（发）。
    pub pulse_sent: u64,
    /// `PULSE` 帧数（收）。
    pub pulse_recv: u64,
    /// `CLOSE` 帧数（发）。
    pub close_sent: u64,
    /// `CLOSE` 帧数（收）。
    pub close_recv: u64,
    /// 帧总字节（发，含帧头）。
    pub frame_bytes_sent: u64,
    /// 帧总字节（收，含帧头）。
    pub frame_bytes_recv: u64,
    /// 传输层原始字节（发，连接写泵口径）。
    pub transport_bytes_sent: u64,
    /// 传输层原始字节（收，连接读泵口径）。
    pub transport_bytes_recv: u64,
    /// 本端发出的 `CLOSE(RESET)` 数。
    pub resets_by_local: u64,
    /// 对端发来的 `CLOSE(RESET)` 数。
    pub resets_by_peer: u64,
    /// 流控阻塞事件数。
    pub flow_stalled: u64,
    /// 流控阻塞持续时长之和（毫秒）。
    pub flow_stall_ms: u64,
    /// 帧 / 协议错误数。
    pub frame_errors: u64,
    /// 子流寿命之和（毫秒）。
    pub channel_lifetime_sum_ms: u64,
    /// 子流寿命最大值（毫秒）——**读数里的绝对值，不是区间增量**。
    pub channel_lifetime_max_ms: u64,
    /// 子流寿命分桶：`[<10ms, <100ms, <1s, <10s, >=10s]`。
    pub channel_lifetime_buckets: [u64; K_BUCKETS],
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 校验：自写 sink 能记录内置采集器丢掉的寿命与阻塞时长，且增量口径正确。
    ///
    /// - 手段：用 [`DemoMetrics::fresh`] 造一份独立计数（避开进程级共享状态），
    ///   取两次快照，中间直接调 `on_channel_closed`（寿命 250ms）与
    ///   `on_flow_stalled`（300ms），再对两次快照做差。
    /// - 判断：增量的 `channels_closed == 1`、`channel_lifetime_sum_ms == 250`、
    ///   `channel_lifetime_max_ms == 250`、寿命桶落在 `[100ms, 1s)` 一档、且
    ///   `flow_stall_ms == 300`。
    #[test]
    fn records_lifetime_and_stall_durations() {
        let sink = DemoMetrics::fresh();
        let before = sink.snapshot();
        sink.on_channel_closed(
            Dock::new(4096u32),
            Dock::new(1u32),
            ChannelCloseReason::Fin,
            250u64,
        );
        sink.on_flow_stalled(Dock::new(4096u32), Dock::new(1u32), 300u64);
        let delta = sink.snapshot().delta(&before);

        assert_eq!(delta.channels_closed, 1u64);
        assert_eq!(delta.channel_lifetime_sum_ms, 250u64);
        assert_eq!(delta.channel_lifetime_max_ms, 250u64);
        assert_eq!(delta.channel_lifetime_buckets[2], 1u64);
        assert_eq!(delta.channel_lifetime_buckets[1], 0u64);
        assert_eq!(delta.flow_stall_ms, 300u64);
    }

    /// 校验：转发到内置采集器的计数不丢。
    ///
    /// - 手段：用独立计数取快照，调一次 `on_channel_opened` 与一次 `on_conn_opened`，
    ///   再取快照做差。
    /// - 判断：`channels_opened == 1`、`conns_opened == 1`。
    #[test]
    fn forwards_builtin_counters() {
        let sink = DemoMetrics::fresh();
        let before = sink.snapshot();
        sink.on_channel_opened(Dock::new(1u32), Dock::new(4096u32));
        sink.on_conn_opened();
        let delta = sink.snapshot().delta(&before);
        assert_eq!(delta.channels_opened, 1u64);
        assert_eq!(delta.conns_opened, 1u64);
    }
}
