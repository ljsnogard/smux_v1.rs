//! **内置**的 bench / debug 采集器：mux 自己也需要的那个 sink。
//!
//! # 形状：`Arc` 包住的独立计数
//!
//! ```text
//! let sink = DebugMetrics::default();   ← 自己的一份计数（内部 Arc 新分配一次）
//! let handle = sink.clone();            ← 交给配置；与 sink 指向**同一份**计数
//! ...
//! let snap = sink.snapshot();           ← 读自己那一份
//! ```
//!
//! 三个目标同时成立：
//!
//! - **每份独立**：计数是实例自己的，不再有进程级单例——多个连接、多个用例互不干扰；
//! - **调用方不用准备存储**：不必再写 `static COUNTERS: … = …;`，`DebugMetrics::default()`
//!   就是一份可用的计数；
//! - **`Default` 成立**：于是 `DefaultConnCfg<W, R, M, P, Rt>` 的 `M` 能真的取它。
//!
//! # 代价：构造时一次分配（热路径没有）
//!
//! `Arc::new` 只在**造 sink 那一刻**发生一次；`Clone` 只是引用计数 +1，而每个上报调用点
//! 是「一次指针解引用 + 一次 `Relaxed` 原子加」，与「全局静态计数」形态没有可测差别。
//!
//! 那次分配走**全局分配器**，且它可能由 mux 的代码触发——例如
//! `DefaultConnCfg::new` 里的 `M::default()`。这是刻意的例外：本类型是**给调试与基准用
//! 的便利件**，不是生产数据面的一环；要求它走注入分配器只会让每个 bench 都得自己写一份
//! sink。生产路径要精确控制分配，请自己实现 [`TrMetricsSink`]。
//!
//! # 读的是累计量
//!
//! 每个计数只增不减（[`DebugMetrics::reset`] 除外），因此 [`DebugMetrics::snapshot`] 给出的
//! 是「这一份 sink 自上次 `reset` 以来的累计量」。
//!
//! # Examples
//!
//! ```
//! use smux_v1::connection::{Dock, FrameKind};
//! use smux_v1::metrics::{DebugMetrics, FrameDir, TrMetricsSink};
//!
//! let sink = DebugMetrics::default();
//! sink.on_frame(FrameDir::Send, Dock::new(3u32), Dock::new(7u32), FrameKind::Data, 128);
//!
//! // 每份独立：断言不会被别的用例污染。
//! let snap = sink.snapshot();
//! assert_eq!(snap.frames_sent, 1);
//! assert_eq!(snap.frame_bytes_sent, 128);
//! assert_eq!(snap.sent_of(FrameKind::Data), 1);
//!
//! // `Clone` 与本体指向同一份计数。
//! let alias = sink.clone();
//! assert_eq!(alias.snapshot().frames_sent, 1);
//! ```

use alloc::sync::Arc;
use core::sync::atomic::{AtomicU64, Ordering};

use crate::{
    connection::{Dock, FrameKind, MuxError},
    metrics::{ChannelCloseReason, ConnCloseReason, FrameDir, TrMetricsSink},
};

/// [`DebugSnapshot`] 里「按帧种类」数组的长度。
///
/// 顺序**就是** [`FrameKind`] 的声明序，两个数组（发送 / 接收）同序：
///
/// | 下标 | 帧种类 |
/// | --- | --- |
/// | 0 | [`FrameKind::Open`] |
/// | 1 | [`FrameKind::Accept`] |
/// | 2 | [`FrameKind::Reject`] |
/// | 3 | [`FrameKind::Close`] |
/// | 4 | [`FrameKind::Data`] |
/// | 5 | [`FrameKind::Datagram`] |
/// | 6 | [`FrameKind::WindowUpdate`] |
/// | 7 | [`FrameKind::Pulse`] |
pub const K_KIND_SLOTS: usize = 8usize;

/// 把 [`FrameKind`] 映射成 [`K_KIND_SLOTS`] 数组下标（顺序见该常量的表）。
const fn kind_index_(kind: FrameKind) -> usize {
    match kind {
        FrameKind::Open => 0usize,
        FrameKind::Accept => 1usize,
        FrameKind::Reject => 2usize,
        FrameKind::Close => 3usize,
        FrameKind::Data => 4usize,
        FrameKind::Datagram => 5usize,
        FrameKind::WindowUpdate => 6usize,
        FrameKind::Pulse => 7usize,
    }
}

/// 一份计数的**本体**（`Arc` 的被指对象）。
///
/// 内部全是 `Relaxed` 原子、没有堆结构；除 [`DebugMetrics::reset`] 外只增不减。
/// 它是私有实现细节——对外只经 [`DebugMetrics`] 的句柄读写。
#[derive(Debug, Default)]
struct Counters_ {
    conns_opened: AtomicU64,
    conns_closed: AtomicU64,
    channels_opened: AtomicU64,
    channels_closed: AtomicU64,
    frames_sent: AtomicU64,
    frames_recv: AtomicU64,
    frame_bytes_sent: AtomicU64,
    frame_bytes_recv: AtomicU64,
    frame_errors: AtomicU64,
    flow_stalled: AtomicU64,
    resets_by_local: AtomicU64,
    resets_by_peer: AtomicU64,
    transport_bytes_sent: AtomicU64,
    transport_bytes_recv: AtomicU64,
    frames_sent_by_kind: [AtomicU64; K_KIND_SLOTS],
    frames_recv_by_kind: [AtomicU64; K_KIND_SLOTS],
}

/// [`DebugMetrics`] 的**一份读数快照**。
///
/// 字段全是纯数据（`u64` / 定长数组），可以任意复制、跨线程传递，也可以拿去做基准
/// 报告的输出。两个「按帧种类」数组的下标含义见 [`K_KIND_SLOTS`]。
///
/// # Examples
///
/// ```
/// use smux_v1::metrics::{DebugMetrics, DebugSnapshot};
///
/// let sink = DebugMetrics::default();
/// let snap: DebugSnapshot = sink.snapshot();
/// assert_eq!(snap.frames_sent, 0);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DebugSnapshot {
    /// 建成的连接数（累计）。
    pub conns_opened: u64,

    /// 关闭的连接数（累计）。
    pub conns_closed: u64,

    /// 建成的子流数（累计）。
    pub channels_opened: u64,

    /// 关闭的子流数（累计）。
    pub channels_closed: u64,

    /// 发送的帧数（累计，不分种类）。
    pub frames_sent: u64,

    /// 接收的帧数（累计，不分种类）。
    pub frames_recv: u64,

    /// 发送的帧总字节数（帧头 + 载荷，累计）。
    pub frame_bytes_sent: u64,

    /// 接收的帧总字节数（口径与发送侧一致，累计）。
    pub frame_bytes_recv: u64,

    /// 帧处理失败 / 协议错误数（累计）。
    pub frame_errors: u64,

    /// 流控阻塞事件数（累计，按事件计、不按持续时长）。
    pub flow_stalled: u64,

    /// 本端发出的 `CLOSE(RESET)` 数（累计）。
    pub resets_by_local: u64,

    /// 对端发来的 `CLOSE(RESET)` 数（累计）。
    pub resets_by_peer: u64,

    /// 连接级**原始字节**发送量（两泵口径，累计）。
    pub transport_bytes_sent: u64,

    /// 连接级**原始字节**接收量（两泵口径，累计）。
    pub transport_bytes_recv: u64,

    /// 按帧种类分的**发送**帧数，下标含义见 [`K_KIND_SLOTS`]。
    pub frames_sent_by_kind: [u64; K_KIND_SLOTS],

    /// 按帧种类分的**接收**帧数，下标含义见 [`K_KIND_SLOTS`]。
    pub frames_recv_by_kind: [u64; K_KIND_SLOTS],
}

impl DebugSnapshot {
    /// 某一种帧的发送帧数（下标换算的便捷入口）。
    ///
    /// # Examples
    ///
    /// ```
    /// use smux_v1::connection::FrameKind;
    /// use smux_v1::metrics::DebugSnapshot;
    ///
    /// let snap = DebugSnapshot::default();
    /// assert_eq!(snap.sent_of(FrameKind::Data), 0);
    /// ```
    pub const fn sent_of(&self, kind: FrameKind) -> u64 {
        self.frames_sent_by_kind[kind_index_(kind)]
    }

    /// 某一种帧的接收帧数（下标换算的便捷入口）。
    ///
    /// # Examples
    ///
    /// ```
    /// use smux_v1::connection::FrameKind;
    /// use smux_v1::metrics::DebugSnapshot;
    ///
    /// let snap = DebugSnapshot::default();
    /// assert_eq!(snap.recv_of(FrameKind::Pulse), 0);
    /// ```
    pub const fn recv_of(&self, kind: FrameKind) -> u64 {
        self.frames_recv_by_kind[kind_index_(kind)]
    }
}

/// **内置采集器**：`Arc` 包住的一份独立计数。
///
/// 形状、代价与用法见[模块文档](self)：它是 `Clone + Default` 的**值语义句柄**——
/// `clone()` 与本体指向同一份计数，`default()` 造一份新的。因此
///
/// - 想要**每连接 / 每场景独立**：`let sink = DebugMetrics::default();` 自己留一份，
///   把 `sink.clone()` 交给配置；
/// - 想要**多连接共享**：把同一份 clone 分发给它们。
///
/// # Examples
///
/// ```
/// use smux_v1::metrics::{DebugMetrics, TrMetricsSink};
///
/// let sink = DebugMetrics::default();
/// sink.on_conn_opened();
/// assert_eq!(sink.snapshot().conns_opened, 1);
///
/// sink.reset();
/// assert_eq!(sink.snapshot().conns_opened, 0);
/// ```
#[derive(Debug, Clone, Default)]
pub struct DebugMetrics(Arc<Counters_>);

impl DebugMetrics {
    /// 读**这一份** sink 的现状快照（每个计数一次 `Relaxed` load；不做整体串行化）。
    ///
    /// 因为不做整体串行化，**并发上报下的快照不保证是同一瞬间的一致切面**（例如
    /// 「帧数」与「字节数」可能分别落在相邻两次自增之间）。作为 bench / debug 读数
    /// 这是刻意的取舍：一致切面要么要锁、要么要版本号，都会给热路径加成本。
    ///
    /// # Examples
    ///
    /// ```
    /// use smux_v1::metrics::DebugMetrics;
    ///
    /// let sink = DebugMetrics::default();
    /// assert_eq!(sink.snapshot().frames_sent, 0);
    /// ```
    pub fn snapshot(&self) -> DebugSnapshot {
        let inner = &*self.0;
        let mut frames_sent_by_kind = [0u64; K_KIND_SLOTS];
        let mut frames_recv_by_kind = [0u64; K_KIND_SLOTS];
        for idx in 0usize..K_KIND_SLOTS {
            frames_sent_by_kind[idx] = inner.frames_sent_by_kind[idx].load(Ordering::Relaxed);
            frames_recv_by_kind[idx] = inner.frames_recv_by_kind[idx].load(Ordering::Relaxed);
        }
        DebugSnapshot {
            conns_opened: inner.conns_opened.load(Ordering::Relaxed),
            conns_closed: inner.conns_closed.load(Ordering::Relaxed),
            channels_opened: inner.channels_opened.load(Ordering::Relaxed),
            channels_closed: inner.channels_closed.load(Ordering::Relaxed),
            frames_sent: inner.frames_sent.load(Ordering::Relaxed),
            frames_recv: inner.frames_recv.load(Ordering::Relaxed),
            frame_bytes_sent: inner.frame_bytes_sent.load(Ordering::Relaxed),
            frame_bytes_recv: inner.frame_bytes_recv.load(Ordering::Relaxed),
            frame_errors: inner.frame_errors.load(Ordering::Relaxed),
            flow_stalled: inner.flow_stalled.load(Ordering::Relaxed),
            resets_by_local: inner.resets_by_local.load(Ordering::Relaxed),
            resets_by_peer: inner.resets_by_peer.load(Ordering::Relaxed),
            transport_bytes_sent: inner.transport_bytes_sent.load(Ordering::Relaxed),
            transport_bytes_recv: inner.transport_bytes_recv.load(Ordering::Relaxed),
            frames_sent_by_kind,
            frames_recv_by_kind,
        }
    }

    /// 把**这一份** sink 的全部计数归零（典型用途：一个 bench 场景跑完之后清一次）。
    ///
    /// 它只影响与 `self` 指向同一份计数的句柄（即 `clone()` 出来的那些）；别的
    /// `DebugMetrics::default()` 不受影响。
    ///
    /// # Examples
    ///
    /// ```
    /// use smux_v1::metrics::{DebugMetrics, TrMetricsSink};
    ///
    /// let sink = DebugMetrics::default();
    /// sink.on_conn_opened();
    /// sink.reset();
    /// assert_eq!(sink.snapshot().conns_opened, 0);
    /// ```
    pub fn reset(&self) {
        let inner = &*self.0;
        for counter in [
            &inner.conns_opened,
            &inner.conns_closed,
            &inner.channels_opened,
            &inner.channels_closed,
            &inner.frames_sent,
            &inner.frames_recv,
            &inner.frame_bytes_sent,
            &inner.frame_bytes_recv,
            &inner.frame_errors,
            &inner.flow_stalled,
            &inner.resets_by_local,
            &inner.resets_by_peer,
            &inner.transport_bytes_sent,
            &inner.transport_bytes_recv,
        ] {
            counter.store(0u64, Ordering::Relaxed);
        }
        for counter in inner.frames_sent_by_kind.iter() {
            counter.store(0u64, Ordering::Relaxed);
        }
        for counter in inner.frames_recv_by_kind.iter() {
            counter.store(0u64, Ordering::Relaxed);
        }
    }
}

impl TrMetricsSink for DebugMetrics {
    fn on_conn_opened(&self) {
        self.0.conns_opened.fetch_add(1u64, Ordering::Relaxed);
    }

    fn on_conn_closed(&self, _reason: ConnCloseReason, _lifetime_millis: u64) {
        self.0.conns_closed.fetch_add(1u64, Ordering::Relaxed);
    }

    fn on_channel_opened(&self, _local: Dock, _remote: Dock) {
        self.0.channels_opened.fetch_add(1u64, Ordering::Relaxed);
    }

    fn on_channel_closed(
        &self,
        _local: Dock,
        _remote: Dock,
        _reason: ChannelCloseReason,
        _lifetime_millis: u64,
    ) {
        self.0.channels_closed.fetch_add(1u64, Ordering::Relaxed);
    }

    fn on_frame(&self, dir: FrameDir, _local: Dock, _remote: Dock, kind: FrameKind, bytes: u32) {
        let bytes = u64::from(bytes);
        let idx = kind_index_(kind);
        match dir {
            FrameDir::Send => {
                self.0.frames_sent.fetch_add(1u64, Ordering::Relaxed);
                self.0.frame_bytes_sent.fetch_add(bytes, Ordering::Relaxed);
                self.0.frames_sent_by_kind[idx].fetch_add(1u64, Ordering::Relaxed);
            }
            FrameDir::Recv => {
                self.0.frames_recv.fetch_add(1u64, Ordering::Relaxed);
                self.0.frame_bytes_recv.fetch_add(bytes, Ordering::Relaxed);
                self.0.frames_recv_by_kind[idx].fetch_add(1u64, Ordering::Relaxed);
            }
        }
    }

    fn on_frame_error(&self, _kind: Option<FrameKind>, _err: MuxError) {
        self.0.frame_errors.fetch_add(1u64, Ordering::Relaxed);
    }

    fn on_flow_stalled(&self, _local: Dock, _remote: Dock, _millis: u64) {
        self.0.flow_stalled.fetch_add(1u64, Ordering::Relaxed);
    }

    fn on_reset(&self, _local: Dock, _remote: Dock, by_peer: bool) {
        if by_peer {
            self.0.resets_by_peer.fetch_add(1u64, Ordering::Relaxed);
        } else {
            self.0.resets_by_local.fetch_add(1u64, Ordering::Relaxed);
        }
    }

    fn on_transport_bytes(&self, dir: FrameDir, bytes: u64) {
        match dir {
            FrameDir::Send => {
                self.0.transport_bytes_sent.fetch_add(bytes, Ordering::Relaxed);
            }
            FrameDir::Recv => {
                self.0.transport_bytes_recv.fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }
}
