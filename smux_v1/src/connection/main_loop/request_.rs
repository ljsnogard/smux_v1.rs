//! `main_loop` 的请求与统一结果类型（P1 子集）。
//!
//! 一次非荷载 API 请求只带**参数 + 完成源**；载荷（message / welcome / reason）在
//! P3 改走建流期 message ring，不再进请求枚举。
//!
//! 结果统一成 [`MainReply_`]，避免请求枚举里出现大量不同的 `Tcs_<T>`；API 侧做一次
//! 穷举转换。错误统一成 [`MainErr_`]：注册表预留失败与连接级失败都在里面，API 侧按
//! 入口映射成 `BindError` / `BindingError`（P3 之后还有 `HandleError`）。

use core::alloc::AllocatorClone;

use crate::connection::{
    Dock, MuxError,
    mux_connection::ReserveErr_,
    owner_::{ChannelOwner_, LsnOwner_},
    pending_msg_::PendingMsgApp_,
    signal_::ControlFrame_,
    tcs_::Tcs_,
};

/// 一次请求的完整结果：成功时是 [`MainReply_`]，失败时是 [`MainErr_`]。
pub(crate) type MainResult_<A> = Result<MainReply_<A>, MainErr_>;

/// 送到 `main_loop` 的一条请求。
pub(crate) enum MainRequest_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 独占绑定一个 dock（显式或自动分配）。
    Bind {
        /// 调用方指定的 dock；`unspecified` 表示由连接自行安排。
        local_dock: Dock,

        /// 完成源。
        reply: Tcs_<MainResult_<A>, A>,
    },

    /// 登记 listener 身份。
    Listen {
        /// 监听 dock。
        local_dock: Dock,

        /// 入向邀请最多同时挂起多少条（P1 尚未按它预分配）。
        reserve: usize,

        /// 完成源。
        reply: Tcs_<MainResult_<A>, A>,
    },

    /// 把一条已组好的控制帧写进公共 TX ring（fire-and-forget）。
    ///
    /// P3.1b 的过渡：`OPEN` / `ACCEPT` / `REJECT` 的载荷仍在
    /// `ControlFrame_::payload_`；P3.2 起改由 `Accept` / `Reject` 请求从 message ring
    /// 读载荷。没有完成源——写失败（队列关闭 / 帧过大）只记在连接状态上。
    SendControl {
        /// 待写出的控制帧。
        frame: ControlFrame_,

        /// 完成源：帧真正写进公共 TX ring 后完成，调用方据此保证后续帧的顺序。
        reply: Tcs_<MainResult_<A>, A>,
    },

    /// 为一条子流登记身份（`open_channel_async` 的第一步）。
    ReserveChannel {
        /// 本端 dock。
        local_dock: Dock,

        /// 对端 dock。
        remote_dock: Dock,

        /// 完成源。
        reply: Tcs_<MainResult_<A>, A>,
    },
}

impl<A> MainRequest_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 本次请求的完成源。
    pub(crate) fn reply_(&self) -> Option<&Tcs_<MainResult_<A>, A>> {
        match self {
            MainRequest_::Bind { reply, .. }
            | MainRequest_::Listen { reply, .. }
            | MainRequest_::ReserveChannel { reply, .. }
            | MainRequest_::SendControl { reply, .. } => Option::Some(reply),
        }
    }

    /// 用同一个错误结束本次请求（连接收尾 / 队列关闭时用）。
    pub(crate) fn fail_(self, err: MainErr_) {
        if let Option::Some(reply) = self.reply_() {
            let _ = reply.set_(Result::Err(err));
        }
    }
}

/// `main_loop` 的统一结果。
pub(crate) enum MainReply_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 绑定完成，返回**实际**绑定的 dock（自动分配时与请求值不同）。
    Dock(Dock),

    /// 新建的 pending channel：身份节点 + app 侧 message ring 两半。
    Channel {
        /// 该子流的共享状态句柄。
        owner: ChannelOwner_<A>,

        /// app 侧 message ring（写 outgoing / 读 incoming）。
        msg_: PendingMsgApp_,
    },

    /// 新建的 listener 身份节点。
    Listener(LsnOwner_<A>),

    /// 仅表示「已受理」（P3.2 的 `Accept` / `Reject` 请求使用）。
    #[allow(dead_code, reason = "P3.2 起由 Accept / Reject 使用")]
    Unit,
}

/// `main_loop` 的统一错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MainErr_ {
    /// 注册表预留 / 配额 / 关闭状态失败。
    Reserve(ReserveErr_),

    /// 连接级失败。
    Mux(MuxError),
}
