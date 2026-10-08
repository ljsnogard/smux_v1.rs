use crate::{
    connection::{
        Dock, MuxConnection, TrConnCfg,
        owner_::{TgLenQueue_, TgOwner_},
        signal_::SessionEvent_,
    },
};

/// telegraph 端点身份的**共享守卫**：两份克隆（tx / rx）都丢掉时才释放身份。
///
/// # 为什么需要它
///
/// telegraph 的 `local_dock` 是**一个**身份，却由 tx / rx 两个对象使用。任一半边先被
/// 丢弃都不该释放身份（另一半还在用同一个 dock），所以两半各持一份本守卫；每份在被丢弃
/// 时先看「现在还剩几份强引用」——`<= 1` 说明自己是最后一份，由它投递
/// [`SessionEvent_::ReleaseTelegraph`](crate::connection::signal_::SessionEvent_)。
///
/// # `Drop` 不取锁
///
/// 释放只投一条消息（与 `ChannelTx` / `ChannelRx` 的 `Drop` 同一条纪律）：真正的身份表
/// 改动由核心执行者在异步上下文里落实。
pub(super) struct TelegraphCtxHolder_<C>
where
    C: TrConnCfg,
{
    /// 身份节点句柄（最后一份被丢弃时节点随之释放）。
    owner_: TgOwner_<C::Alloc>,

    /// 连接智能指针（投递释放消息用）。
    conn_: MuxConnection<C>,

    /// 本端 dock。
    local_dock_: Dock,
}

impl<C> TelegraphCtxHolder_<C>
where
    C: TrConnCfg,
{
    /// 由身份句柄、连接与 dock 构造。
    pub(super) fn new_(owner: TgOwner_<C::Alloc>, conn: MuxConnection<C>, local_dock: Dock) -> Self {
        TelegraphCtxHolder_ {
            owner_: owner,
            conn_: conn,
            local_dock_: local_dock,
        }
    }

    /// 发送方向的长度队列（身份节点内联）。
    pub(super) fn out_(&self) -> &TgLenQueue_ {
        self.owner_.out_()
    }

    /// 接收方向的长度队列（身份节点内联）。
    pub(super) fn in_(&self) -> &TgLenQueue_ {
        self.owner_.in_()
    }
}

impl<C> Clone for TelegraphCtxHolder_<C>
where
    C: TrConnCfg,
{
    fn clone(&self) -> Self {
        TelegraphCtxHolder_ {
            owner_: self.owner_.clone(),
            conn_: self.conn_.clone(),
            local_dock_: self.local_dock_,
        }
    }
}

impl<C> Drop for TelegraphCtxHolder_<C>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        // 此刻本份仍持有 `owner_`，因此 `<= 1` 表示本份就是最后一份强引用。先问再放下，
        // 顺序不可反：放下之后节点可能已经被回收。
        if !self.owner_.is_last_strong_ref_() {
            return;
        }
        let _ = self
            .conn_
            .core_()
            .reg_()
            .post_session_event_(SessionEvent_::ReleaseTelegraph {
                local_dock: self.local_dock_,
            });
    }
}
