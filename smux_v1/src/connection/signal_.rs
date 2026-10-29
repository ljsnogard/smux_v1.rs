//! 连接内部的**一次性资源交接**与**合并提示位**（R17 起不再有事件通道）。
//!
//! # 这个模块现在装什么
//!
//! | 内容 | 说明 |
//! | --- | --- |
//! | [`WriteAttach_`] / [`ReadAttach_`] | 环半部（发送环读端 / 接收环写端）经内联 inbox 交给两个数据循环；channel 与 telegraph 共用同一条 inbox |
//! | `HINT_W_*` / `HINT_R_*` | 合并提示位：可重建的「可能变了」只置位 + 唤醒，事实在各 owner 的状态字里 |
//! | [`ControlFrame_`] | main 组帧用的纯帧头（载荷由独立切片携带） |
//!
//! # 两条事件通道已删除（R17）
//!
//! `WriteEvent_` / `ReadEvent_` 曾承载四类通知，逐轮迁走：
//!
//! - `TxClosed` / `RxClosed` / `PeerClosed` / `Release` / `LocalAbort`：**位化**（状态字 + `HINT_*`）；
//! - `Control`（`PULSE` / `CLOSE` / `REJECT`）：改由 `main_loop` 组帧写公共 TX ring（R9）；
//! - `RxConsumed`：`HINT_R_RX_CONSUMED` + `demux` 全表重算（R11）；
//! - `TxReady`：`HINT_W_TX_READY`（R13）；
//! - `TgTxClosed`：`HINT_W_TG_TX_CLOSED` + 扫本地 tg 表（R15）；
//! - `TgAttach`：改走 attach inbox（R16）；
//! - `TgReady`：改走 attach inbox 的零 payload 信号（R17）——**用队列而不是合并位**，
//!   因为「写环刚才满、现在有空」时必须保证写循环还能被再叫醒一次。
//!
//! # 两条 inbox 的方向**刻意不同**
//!
//! 会话侧的两个环半部必须分别交给**不同**的循环：发送环的读端进写循环，接收环的
//! 写端进读循环。若只用一条通道，两个循环会互相取走对方的 `Attach`。

use crate::{
    connection::{
        Dock, FrameKind, TrConnAllocCfg,
        owner_::{ChannelOwner_, TgOwner_},
        ring_::{BufferedRx, BufferedTx},
    },
    flow_ctrl::{Credit, RecvTotal},
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 事件负载
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一条待写出的**控制帧**。
///
/// 数据帧不走这里：写循环直接从各子流的发送环取数据并成帧，见
/// [`WriteEvent_::TxReady`]。
///
/// 控制帧本身只带帧头字段；`OPEN` / `ACCEPT` / `REJECT` 的 message / welcome /
/// reason 由 `main_loop` 从建流期 message ring 读出后，作为**独立切片**交给写帧
/// 函数（见 `main_loop::write_control_frame_async_`），不再挂在帧结构上。这样控制
/// 帧不再持有 `Vec<u8>`，事件通道里的控制帧也就没有载荷分配。
#[derive(Debug, Clone, Copy)]
pub(crate) struct ControlFrame_ {
    /// 帧种类。
    kind_: FrameKind,

    /// 标志位（[`super::flags`] 的常量按位或）。
    flags_: u8,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,

    /// 窗口通告 `(累计已收 R, 接收窗口 W)`；只有 `OPEN` / `PULSE` /
    /// `WINDOW_UPDATE` 带。
    window_: Option<(RecvTotal, Credit)>,
}

impl ControlFrame_ {
    /// 造一条不带窗口的控制帧（`CLOSE` 等）。
    pub(crate) fn plain_(kind: FrameKind, flags: u8, local_dock: Dock, remote_dock: Dock) -> Self {
        ControlFrame_ {
            kind_: kind,
            flags_: flags,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
            window_: Option::None,
        }
    }

    /// 造一条带窗口通告的控制帧（`OPEN` / `PULSE` 等）。
    pub(crate) fn with_window_(
        kind: FrameKind,
        flags: u8,
        local_dock: Dock,
        remote_dock: Dock,
        window: Option<(RecvTotal, Credit)>,
    ) -> Self {
        ControlFrame_ {
            kind_: kind,
            flags_: flags,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
            window_: window,
        }
    }

    /// 帧种类。
    pub(crate) fn kind_(&self) -> FrameKind {
        self.kind_
    }

    /// 标志位。
    pub(crate) fn flags_(&self) -> u8 {
        self.flags_
    }

    /// 本端 dock。
    pub(crate) fn local_dock_(&self) -> Dock {
        self.local_dock_
    }

    /// 对端 dock。
    pub(crate) fn remote_dock_(&self) -> Dock {
        self.remote_dock_
    }

    /// 窗口通告（若带）。
    pub(crate) fn window_(&self) -> Option<(RecvTotal, Credit)> {
        self.window_
    }
}

/// 写循环的 `Attach` 负载：`(dock 对, 身份节点, 发送环读端)`。
///
/// 它不再走事件通道，而是经**内联 inbox**（每连接一条，容量固定）交给写循环：
/// 环半部是 move 语义的一次性资源，放在共享槽里比塞进通用事件枚举更直接，也不受
/// 「子流变多 ⇒ 队列被打满」的影响。
pub(crate) enum WriteAttach_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 一条 channel 子流的发送环读端。
    Channel {
        /// 本端 / 对端 dock。
        pair_: (Dock, Dock),

        /// 该子流的共享状态。
        owner_: ChannelOwner_<C::ChanOwnerAlloc>,

        /// 会话侧发送环读端（应用写、写循环读）。
        reader_: BufferedRx,
    },

    /// 「某条 telegraph 有已提交报文」的信号（零 payload）。
    ///
    /// 它是**可重建**的轻量信号：丢了也不要紧——写循环每轮都会把所有端点过一遍；
    /// 用 inbox 而不用合并提示，是因为「一次提交一条信号」的队列语义能保证「写环
    /// 刚才满、现在有空」时写循环**一定会再被叫醒**（合并提示只保证一次唤醒）。
    TgReady {
        /// 本端 dock。
        local_dock_: Dock,
    },

    /// 一条 telegraph 端点的发送环读端（键只有 `local_dock`）。
    Telegraph {
        /// 本端 dock。
        local_dock_: Dock,

        /// 该端点的身份节点句柄（循环由它取「已提交报文长度」队列）。
        owner_: TgOwner_<C::ChanOwnerAlloc>,

        /// 会话侧发送环读端（应用写、本循环读）。
        reader_: BufferedRx,
    },
}

impl<C> WriteAttach_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 造一条 channel 的写侧 `Attach`。
    pub(crate) fn new_(
        pair: (Dock, Dock),
        owner_: ChannelOwner_<C::ChanOwnerAlloc>,
        reader_: BufferedRx,
    ) -> Self {
        WriteAttach_::Channel {
            pair_: pair,
            owner_,
            reader_,
        }
    }

    /// 造一条「telegraph 有已提交报文」的信号。
    pub(crate) fn new_tg_ready_(local_dock_: Dock) -> Self {
        WriteAttach_::TgReady { local_dock_ }
    }

    /// 造一条 telegraph 的写侧 `Attach`。
    pub(crate) fn new_tg_(
        local_dock_: Dock,
        owner_: TgOwner_<C::ChanOwnerAlloc>,
        reader_: BufferedRx,
    ) -> Self {
        WriteAttach_::Telegraph {
            local_dock_,
            owner_,
            reader_,
        }
    }

    /// 拆成 `(dock 对, 身份节点, 发送环读端)`；telegraph 变体返回 `None`。
    #[allow(clippy::type_complexity, reason = "返回元组即调用方要的三件套")]
    pub(crate) fn into_parts_(
        self,
    ) -> Option<((Dock, Dock), ChannelOwner_<C::ChanOwnerAlloc>, BufferedRx)> {
        match self {
            WriteAttach_::Channel {
                pair_,
                owner_,
                reader_,
            } => Option::Some((pair_, owner_, reader_)),
            WriteAttach_::Telegraph { .. }
            | WriteAttach_::TgReady { .. } => Option::None,
        }
    }

}

/// 读循环的 `Attach` 负载：`(dock 对, 身份节点, 接收环写端)`。
pub(crate) enum ReadAttach_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 一条 channel 子流的接收环写端。
    Channel {
        /// 本端 / 对端 dock。
        pair_: (Dock, Dock),

        /// 该子流的共享状态。
        owner_: ChannelOwner_<C::ChanOwnerAlloc>,

        /// 会话侧接收环写端（读循环写、应用读）。
        writer_: BufferedTx,
    },

    /// 一条 telegraph 端点的接收环写端（键只有 `local_dock`）。
    Telegraph {
        /// 本端 dock。
        local_dock_: Dock,

        /// 该端点的身份节点句柄（接收方向的报文边界记在它内联的队列里）。
        owner_: TgOwner_<C::ChanOwnerAlloc>,

        /// 会话侧接收环写端（读循环写、本循环读）。
        writer_: BufferedTx,
    },
}

impl<C> ReadAttach_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 造一条 channel 的读侧 `Attach`。
    pub(crate) fn new_(
        pair: (Dock, Dock),
        owner_: ChannelOwner_<C::ChanOwnerAlloc>,
        writer_: BufferedTx,
    ) -> Self {
        ReadAttach_::Channel {
            pair_: pair,
            owner_,
            writer_,
        }
    }

    /// 造一条 telegraph 的读侧 `Attach`。
    pub(crate) fn new_tg_(
        local_dock_: Dock,
        owner_: TgOwner_<C::ChanOwnerAlloc>,
        writer_: BufferedTx,
    ) -> Self {
        ReadAttach_::Telegraph {
            local_dock_,
            owner_,
            writer_,
        }
    }

    /// 拆成 `(dock 对, 身份节点, 接收环写端)`；telegraph 变体返回 `None`。
    #[allow(clippy::type_complexity, reason = "返回元组即调用方要的三件套")]
    pub(crate) fn into_parts_(
        self,
    ) -> Option<((Dock, Dock), ChannelOwner_<C::ChanOwnerAlloc>, BufferedTx)> {
        match self {
            ReadAttach_::Channel {
                pair_,
                owner_,
                writer_,
            } => Option::Some((pair_, owner_, writer_)),
            ReadAttach_::Telegraph { .. } => Option::None,
        }
    }

}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 合并提示的位定义
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 「应用已丢弃发送半边」：写循环据此在发送环排空后补 `CLOSE(FIN)`。
///
/// 事实本身记在 `owner_::ChannelState_` 的 `APP_TX_CLOSED` 位；这一位只是**唤醒**
/// 写循环的提示，重复置位幂等。
pub(crate) const HINT_W_TX_CLOSED: u64 = 1u64 << 0usize;

/// 「应用已丢弃接收半边」：写循环据此发 `CLOSE(RESET)` 并关闭接收环。
/// 「对端发来 `CLOSE`」：写循环据此丢弃发送方向 / 推进身份释放。
///
/// `FIN` 与 `RESET` 的事实同样在各子流状态字（`PEER_FIN` / `PEER_RESET`）上。
pub(crate) const HINT_W_PEER_CLOSED: u64 = 1u64 << 2usize;

/// 「身份已释放 / 已中止」：写循环据此摘写表项、关发送环消费端。
///
/// 事实是 `ChannelState_` 的 `RELEASED` 位；本位只负责把写循环叫醒。替换了原先的
/// 一次性 `WriteEvent_::LocalAbort`：一条子流至多释放一次，扫描是幂等的。
pub(crate) const HINT_W_RELEASED: u64 = 1u64 << 3usize;

/// 「应用丢弃了某条 telegraph 的发送半边」：写循环据此摘本地 tg 表项。
pub(crate) const HINT_W_TG_TX_CLOSED: u64 = 1u64 << 7usize;

/// 「某条子流的发送环刚有数据入队」：写循环据此回顶部做全表排空。
///
/// 可重建的高频提示：事实是 owner 的 `TX_QUEUED` 去重位（`mark_tx_queued_` 的
/// 第一次置位才 raise）。重复或丢失都不会漏数据——写循环每轮本来就是全表扫描。
pub(crate) const HINT_W_TX_READY: u64 = 1u64 << 6usize;

/// 「`main_loop` 已把 `CLOSE(FIN)` 写进公共 TX ring」：写循环在刷出后确认上线。
pub(crate) const HINT_W_FIN_ENQUEUED: u64 = 1u64 << 4usize;

/// 「main 已把 `CLOSE(RESET)` 写进公共 TX ring」：写循环在刷出后确认上线。
pub(crate) const HINT_W_RESET_ENQUEUED: u64 = 1u64 << 5usize;

/// 「应用刚消费了接收数据」：读循环据此按全表重算接收水位。
///
/// 可重建的高频提示：事实是 owner 的 `RX_CONSUMED` 去重位；读循环的清位顺序
/// （**先清位、再读环内积压**）保证不丢。
pub(crate) const HINT_R_RX_CONSUMED: u64 = 1u64 << 1usize;

/// 「身份已释放」：读循环据此摘掉接收表项并关闭接收环写端。
pub(crate) const HINT_R_RELEASE: u64 = 1u64 << 0usize;
