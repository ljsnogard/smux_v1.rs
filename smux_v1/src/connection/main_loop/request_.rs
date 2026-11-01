//! `main_loop` 的请求与统一结果类型（P1 子集）。
//!
//! 一次非荷载 API 请求只带**参数 + 完成源**；载荷（message / welcome / reason）在
//! P3 改走建流期 message ring，不再进请求枚举。
//!
//! 结果统一成 [`MainReply_`]，避免请求枚举里出现大量不同的 `Tcs_<T>`；API 侧做一次
//! 穷举转换。错误统一成 [`MainErr_`]：注册表预留失败与连接级失败都在里面，API 侧按
//! 入口映射成 `BindError` / `BindingError`（P3 之后还有 `HandleError`）。

use core::sync::atomic::AtomicBool;

use mm_ptr::Shared;

use crate::connection::{
    Dock, MuxError, TrConnAllocCfg,
    mux_connection::ReserveErr_,
    owner_::{ChannelOwner_, LsnOwner_, TgOwner_},
    pending_msg_::PendingMsgApp_,
    tcs_::Tcs_,
};

use crate::flow_ctrl::Credit;

/// 建流裁决时本端的角色。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EstablishRole_ {
    /// 发起方（`open_channel_async` 产出的句柄）。
    Initiator,

    /// 响应方（listener 产出的入向句柄）。
    Responder,
}

/// 一次请求的完整结果：成功时是 [`MainReply_`]，失败时是 [`MainErr_`]。
pub(crate) type MainLoopResult_<C> = Result<MainLoopReply_<C>, MainLoopErr_>;

/// 送到 `main_loop` 的一条请求。
pub(crate) enum MainLoopRequest_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 独占绑定一个 dock（显式或自动分配）。
    Bind {
        /// 调用方指定的 dock；`unspecified` 表示由连接自行安排。
        local_dock: Dock,

        /// 完成源。
        reply: Tcs_<MainLoopResult_<C>>,
    },

    /// 登记 listener 身份。
    Listen {
        /// 监听 dock。
        local_dock: Dock,

        /// 入向邀请最多同时挂起多少条（P1 尚未按它预分配）。
        reserve: usize,

        /// 完成源。
        reply: Tcs_<MainLoopResult_<C>>,
    },

    /// 为一条子流登记身份（`open_channel_async` 的第一步）。
    ReserveChannel {
        /// 本端 dock。
        local_dock: Dock,

        /// 对端 dock。
        remote_dock: Dock,

        /// 完成源。
        reply: Tcs_<MainLoopResult_<C>>,
    },

    /// 从 listener 的入向表里取一个待决请求（`income_async` 的一步）。
    ///
    /// 没有待决请求时答复 `Inbound(None)`；调用方在 `LsnOwner_` 的通知槽上等下一次
    /// 提示后重试（槽是持久位，「先通知、后等待」也不丢）。
    TakeInbound {
        /// 监听 dock。
        local_dock: Dock,

        /// 完成源：`Inbound(Some(..))` 表示取到一条入向请求。
        reply: Tcs_<MainLoopResult_<C>>,
    },

    /// 最终裁决：建环已完成，main 从 message ring 读载荷并组 `OPEN` / `ACCEPT`。
    Accept {
        /// 本端 dock。
        local_dock: Dock,

        /// 对端 dock。
        remote_dock: Dock,

        /// 该子流的共享状态句柄。
        owner: ChannelOwner_<C::ChanOwnerAlloc>,

        /// 本端角色。
        role_: EstablishRole_,

        /// 本端接收窗口（取自接收环容量）。
        initial_window: Credit,

        /// 完成源：帧写进公共 TX 后完成。
        reply: Tcs_<MainLoopResult_<C>>,
    },

    /// 拒绝入向建流；reason 已写进 outgoing message ring。
    Reject {
        /// 本端 dock。
        local_dock: Dock,

        /// 对端 dock。
        remote_dock: Dock,

        /// 该子流的共享状态句柄。
        owner: ChannelOwner_<C::ChanOwnerAlloc>,

        /// 本端角色。
        role_: EstablishRole_,

        /// 完成源。
        reply: Tcs_<MainLoopResult_<C>>,
    },

    /// 开一条 telegraph 端点：登记身份并把两个环半部交给两个中心循环。
    ///
    /// `TgAttach` 携带环半部（不可位化），因此由 main 在登记之后**直接投递**两条
    /// 事件；登记本身是身份表写操作，必须由 main 做（I3）。
    OpenTelegraph {
        /// 本端 dock（端点独占该 dock）。
        local_dock: Dock,

        /// 发送环读端（应用写、写循环读）。
        reader_: crate::connection::ring_::BufferedRx,

        /// 接收环写端（读循环写、应用读）。
        writer_: crate::connection::ring_::BufferedTx,

        /// 完成源。
        reply: Tcs_<MainLoopResult_<C>>,
    },

    /// 优雅关闭：进入「等待关闭」并交由 `main_loop` 推进，答复里给出「是否与对端收敛」。
    ///
    /// 收尾的**判定与执行**都在 main（`tick_` 的连接级关闭动作）：活跃子流归零 ⇒
    /// `Closed(true)`；整体超时 ⇒ `Closed(false)`。答复**先于** `cancel_loops_()`
    /// 完成，调用方拿到的是「收尾结果」而不是「被取消」。
    Close {
        /// 完成源。
        reply: Tcs_<MainLoopResult_<C>>,
    },
}

impl<C> MainLoopRequest_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 本次请求的完成源。
    pub(crate) fn reply_(&self) -> Option<&Tcs_<MainLoopResult_<C>>> {
        match self {
            MainLoopRequest_::Bind { reply, .. }
            | MainLoopRequest_::Listen { reply, .. }
            | MainLoopRequest_::ReserveChannel { reply, .. }
            | MainLoopRequest_::TakeInbound { reply, .. }
            | MainLoopRequest_::OpenTelegraph { reply, .. }
            | MainLoopRequest_::Accept { reply, .. }
            | MainLoopRequest_::Reject { reply, .. }
            | MainLoopRequest_::Close { reply } => Option::Some(reply),
        }
    }

    /// 用同一个错误结束本次请求（连接收尾 / 队列关闭时用）。
    pub(crate) fn fail_(self, err: MainLoopErr_) {
        if let Option::Some(reply) = self.reply_() {
            let _ = reply.set_(Result::Err(err));
        }
    }
}

/// `main_loop` 的统一结果。
pub(crate) enum MainLoopReply_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 绑定完成，返回**实际**绑定的 dock（自动分配时与请求值不同）与
    /// 「绑定方已丢弃」标志（`DockBinding::Drop` 置位）。
    Dock {
        /// 实际绑定的 dock。
        dock: Dock,

        /// 解绑请求标志。
        unbind_: Shared<AtomicBool, C::ChanOwnerAlloc>,
    },

    /// 新建的 pending channel：身份节点 + app 侧 message ring 两半。
    Channel {
        /// 该子流的共享状态句柄。
        owner: ChannelOwner_<C::ChanOwnerAlloc>,

        /// app 侧 message ring（写 outgoing / 读 incoming）。
        msg_: PendingMsgApp_,
    },

    /// 新建的 listener 身份节点。
    Listener(LsnOwner_<C::ChanOwnerAlloc>),

    /// 仅表示「已受理」（`Accept` / `Reject` 请求使用）。
    Unit,

    /// 优雅关闭的答复：`true` = 与对端双向收敛；`false` = 整体超时 / 连接失败。
    Closed(bool),

    /// `TakeInbound` 的答复：`Some((remote, owner, msg_))` = 取到一条待决入向请求；
    /// `None` = 当前没有（调用方等 listener 通知槽再请求）。
    Inbound(Option<(Dock, ChannelOwner_<C::ChanOwnerAlloc>, PendingMsgApp_)>),

    /// `OpenTelegraph` 的答复：该端点的身份节点句柄。
    Telegraph(TgOwner_<C::ChanOwnerAlloc>),
}

/// `main_loop` 的统一错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MainLoopErr_ {
    /// 注册表预留 / 配额 / 关闭状态失败。
    Reserve(ReserveErr_),

    /// 连接级失败。
    Mux(MuxError),
}
