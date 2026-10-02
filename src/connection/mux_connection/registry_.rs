
//! 连接级的 **dock / 子流索引（注册表）**。
//!
//! 从 `sync_` 拆出：注册表管理的是「连接上哪些 dock 被绑定、哪些子流在册」，
//! 与连接的其余控制面同属 `mux_connection`；`sync_` 只保留通用的共享单元、
//! 取消令牌、失败投影与唤醒槽。

// 多线程配置下 `MuxConnection::new` 仍是 `todo!()`（见 dev-notes），注册表整块
// 暂时不可达，因此**仅在该配置下**允许 dead_code；缺省（单线程）配置不放开。
#![cfg_attr(feature = "multi-thread", allow(dead_code))]

use core::{
    alloc::AllocatorClone,
    task::Waker,
};

use atomic_sync::rwlock::cooperative::CooperativeRwLockOwned;
use mm_ptr::{Owned, Shared};

use crate::{
    connection::{
        Dock, MuxError,
        owner_::ChannelOwner_,
        sync_::{CancelToken_, FailKind_, WakerSlot_, on_lock_contended_},
    },
    flow_ctrl::WindowReport,
    handshake::opts::BasicOpts,
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// dock / 子流索引
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// dock 的占用用途：channel 与 telegraph **不得共用**同一个 local_dock。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DockUse_ {
    /// 已作为 channel 的 local_dock（监听或发起）。
    Channel,

    /// 已作为 telegraph 端点。
    ///
    /// telegraph 端点尚未实现（`open_telegraph_async` 仍是 `todo!()`），该分支
    /// 目前只在测试里构造。
    #[allow(dead_code)]
    Telegraph,
}

/// 入向建流请求在索引节点上的状态。
///
/// 被动方收到 `OPEN` 后**先回一条自己的 `OPEN`**（通告接收窗口），此时环还没有
/// 建立——真正建环发生在应用 `accept_async` 那一刻（那时才拿得到调用方注入的
/// 配置）。因此这里要先把对端 `OPEN` 携带的窗口通告存住，直到 `accept` 使用。
pub(crate) enum Inbound_ {
    /// 该节点不是入向请求（主动方发起的子流，或入向请求已处理完）。
    None,

    /// 已收到 `OPEN`、等待 `income_async` 取走。
    Pending(WindowReport),

    /// 已被 `income_async` 取走（`ChannelHandle` 在应用手里），等待 accept / reject。
    HandedOut(WindowReport),
}

/// 一条活动子流在索引里的条目。
struct ChannelNode_<A>
where
    A: AllocatorClone,
{
    /// 对端 dock；与本节点所属 dock 节点合起来就是子流身份。
    remote_dock_: Dock,

    /// 该子流的共享状态句柄。
    ///
    /// `None` 表示环尚未建立（被动方的入向请求在 `accept` 之前）。主动方在
    /// `open_channel_async` 建好环后立刻挂上；被动方在 `accept_async` 挂上。
    owner_: Option<ChannelOwner_<A>>,

    /// 入向建流请求的状态。
    inbound_: Inbound_,

    /// 同一条链上的下一个子流。
    next_: Option<Owned<ChannelNode_<A>, A>>,
}

impl<A> DockNode_<A>
where
    A: AllocatorClone,
{
    /// 是否「空」：无子流、无显式占用、无绑定、无等待者。
    ///
    /// `bound_` 必须计入：绑定是**持久**占用（直到对应 [`DockBinding`] 被 drop），
    /// 不能因为该 dock 上暂时没有子流就把节点剪掉、把绑定状态一起丢掉。
    ///
    /// [`DockBinding`]: crate::connection::DockBinding
    fn is_empty_(&self) -> bool {
        self.chan_count_ == 0
            && self.use_.is_none()
            && !self.bound_
            && !self.inbound_waker_.is_registered_()
    }
}

/// 一个 local_dock 的索引条目。
struct DockNode_<A>
where
    A: AllocatorClone,
{
    /// 本节点对应的 local_dock。
    dock_: Dock,

    /// 显式登记的用途（telegraph）；`None` 表示尚未被显式占用。
    use_: Option<DockUse_>,

    /// 该 dock 是否已被某个 [`DockBinding`] **独占绑定**。
    ///
    /// 与 `use_` 分开记：`use_` 是「用途种类」标记，会被
    /// [`ChannelRegistry_::release_channel_`] 在子流清零时清空；而绑定是比子流
    /// 更长寿的占用（见 `DockBinding` 的「绑定的独占性」），必须独立持久。
    ///
    /// [`DockBinding`]: crate::connection::DockBinding
    bound_: bool,

    /// 本 dock 上当前在册的子流数（受 `max_dock_chan_count` 约束）。
    chan_count_: usize,

    /// 本 dock 上的子流链头。
    head_: Option<Owned<ChannelNode_<A>, A>>,

    /// 下一个 dock 节点。
    next_: Option<Owned<DockNode_<A>, A>>,

    /// 「本 dock 上有入向事件」的等待者（listener 的 `income_async`）。
    inbound_waker_: WakerSlot_,
}

/// 共享注册表的内部状态。
struct RegistryInner_<A>
where
    A: AllocatorClone,
{
    /// 协商结果：本层只用到其中的两个配额。
    opts_: BasicOpts,

    /// 分配器（每个节点按需分配时克隆使用）。
    alloc_: A,

    /// dock 索引链头。
    docks_: Option<Owned<DockNode_<A>, A>>,

    /// 整条连接上当前在册的子流数（受 `max_channel_count` 约束）。
    total_: usize,

    /// 连接级失败（**首个**原因保留，之后的失败不再覆盖）。
    fail_: Option<FailKind_>,

    /// 两个循环各自的取消令牌（见 [`ChannelRegistry_::cancel_loops_`]）。
    loops_: [CancelToken_<A>; 2],
}

/// 读写循环与 API 面共享的 dock / 子流索引。
///
/// 所有方法都取 `&self`：内部可变性由 `atomic_sync` 的**协作式读写锁**
/// （`rwlock::cooperative::CooperativeRwLock`）提供。临界区都很短（链表增删与
/// 计数），且**不跨 `await`**——这里只取它的同步快路径（`try_read` / `try_write`
/// + 自旋）；保留异步守卫是为了后续把需要跨 `await` 持锁的路径迁进来。
pub(crate) struct ChannelRegistry_<A>
where
    A: AllocatorClone,
{
    inner_: Shared<CooperativeRwLockOwned<RegistryInner_<A>>, A>,
}

impl<A> Clone for ChannelRegistry_<A>
where
    A: AllocatorClone,
{
    fn clone(&self) -> Self {
        ChannelRegistry_ {
            inner_: self.inner_.clone(),
        }
    }
}

impl<A> ChannelRegistry_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 取分配器（读 / 写循环为自己的本地表按需分配节点）。
    pub(crate) fn allocator_(&self) -> A {
        self.with_(|inner| inner.alloc_.clone())
    }

    /// 建立注册表：分配根对象，并为两个循环建好取消令牌。
    ///
    /// # Panics
    ///
    /// 分配根对象失败时 panic（与标准库容器在 OOM 时的行为一致）。调用方应按
    /// `max_channel_count` 量级准备分配器。
    pub(crate) fn new_(opts: BasicOpts, alloc: A) -> Self {
        let loops = [
            CancelToken_::new_(alloc.clone()),
            CancelToken_::new_(alloc.clone()),
        ];
        ChannelRegistry_ {
            inner_: Shared::new(
                CooperativeRwLockOwned::new_owned(RegistryInner_ {
                    opts_: opts,
                    alloc_: alloc.clone(),
                    docks_: Option::None,
                    total_: 0usize,
                    fail_: Option::None,
                    loops_: loops,
                }),
                alloc,
            ),
        }
    }

    /// 协商出的「整条连接最多同时在册子流数」。
    // 仅供单元测试与尚未实现的 telegraph 路径使用；telegraph 落地后即可移除本行。
    #[allow(dead_code)]
    pub(crate) fn max_channel_count_(&self) -> usize {
        self.with_(|inner| inner.opts_.max_channel_count)
    }

    /// 协商出的「单个 dock 最多同时在册子流数」。
    // 仅供单元测试与尚未实现的 telegraph 路径使用；telegraph 落地后即可移除本行。
    #[allow(dead_code)]
    pub(crate) fn max_dock_chan_count_(&self) -> usize {
        self.with_(|inner| inner.opts_.max_dock_chan_count)
    }

    /// 当前在册子流总数。
    // 仅供单元测试与尚未实现的 telegraph 路径使用；telegraph 落地后即可移除本行。
    #[allow(dead_code)]
    pub(crate) fn total_channels_(&self) -> usize {
        self.with_(|inner| inner.total_)
    }

    /// 为 `(local_dock, remote_dock)` 登记一条子流。
    ///
    /// # Errors
    ///
    /// - `local_dock` 已被 telegraph 占用 → [`MuxError::DockInUse`]；
    /// - 同一 dock 对上已有活动子流 → [`MuxError::Duplicate`]（dock 对即身份，
    ///   见 `crate::connection` 模块文档 §4.1）；
    /// - 该 dock 上的在册子流数已达 `max_dock_chan_count` → [`MuxError::DockChanLimit`]；
    /// - 连接上的在册子流数已达 `max_channel_count` → [`MuxError::ChanLimit`]。
    pub(crate) fn reserve_channel_(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
    ) -> Result<(), MuxError<(), ()>> {
        self.with_mut_(|inner| {
            if inner.total_ >= inner.opts_.max_channel_count {
                return Result::Err(MuxError::ChanLimit);
            }
            // 先把限额与分配器取出来：下面要可变借用 dock 节点。
            let max_dock = inner.opts_.max_dock_chan_count;
            let alloc = inner.alloc_.clone();

            let dock = inner.dock_mut_(local_dock);
            if let Option::Some(DockUse_::Telegraph) = dock.use_ {
                return Result::Err(MuxError::DockInUse);
            }
            if find_channel_(&dock.head_, remote_dock).is_some() {
                return Result::Err(MuxError::Duplicate);
            }
            if dock.chan_count_ >= max_dock {
                return Result::Err(MuxError::DockChanLimit);
            }
            dock.use_ = Option::Some(DockUse_::Channel);
            dock.chan_count_ += 1;
            dock.head_ = Option::Some(Owned::new(
                ChannelNode_ {
                    remote_dock_: remote_dock,
                    owner_: Option::None,
                    inbound_: Inbound_::None,
                    next_: dock.head_.take(),
                },
                alloc,
            ));
            inner.total_ += 1;
            Result::Ok(())
        })
    }

    /// 拆掉一条子流并释放配额；不存在时是空操作。
    ///
    /// 若该 dock 随之「无子流、无显式占用、无等待者」，节点会被释放。
    pub(crate) fn release_channel_(&self, local_dock: Dock, remote_dock: Dock) {
        self.with_mut_(|inner| {
            let removed = match inner.find_dock_mut_(local_dock) {
                Option::None => false,
                Option::Some(dock) => {
                    let removed = remove_channel_(&mut dock.head_, remote_dock);
                    if removed {
                        dock.chan_count_ = dock.chan_count_.saturating_sub(1);
                    }
                    if dock.chan_count_ == 0 {
                        dock.use_ = Option::None;
                    }
                    removed
                }
            };
            if removed {
                inner.total_ = inner.total_.saturating_sub(1);
            }
            inner.prune_dock_(local_dock);
        })
    }

    /// 把建好环之后的共享状态句柄挂到已有节点上。
    ///
    /// 主动方在 `open_channel_async`、被动方在 `accept_async` 调用；节点必须已经由
    /// [`ChannelRegistry_::reserve_channel_`] 登记过，否则返回 `false`（调用方按
    /// 内部错误处理）。
    pub(crate) fn attach_owner_(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
        owner: ChannelOwner_<A>,
    ) -> bool {
        self.with_mut_(|inner| {
            let Some(dock) = inner.find_dock_mut_(local_dock) else {
                return false;
            };
            let Some(node) = find_channel_mut_(&mut dock.head_, remote_dock) else {
                return false;
            };
            node.owner_ = Option::Some(owner);
            true
        })
    }

    /// 取一条子流的共享状态句柄（克隆）；不存在或尚未挂上时返回 `None`。
    pub(crate) fn channel_owner_(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
    ) -> Option<ChannelOwner_<A>> {
        self.with_(|inner| {
            let dock = inner.find_dock_(local_dock)?;
            let node = find_channel_(&dock.head_, remote_dock)?;
            node.owner_.clone()
        })
    }

    /// 被动方收到 `OPEN`：登记节点并把它标为「待决入向请求」，随后唤醒该 dock 上的
    /// 监听者。
    ///
    /// # Errors
    ///
    /// 与 [`ChannelRegistry_::reserve_channel_`] 相同（含 dock 对已存在时的
    /// [`MuxError::Duplicate`]）。
    pub(crate) fn reserve_inbound_(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
        peer_report: WindowReport,
    ) -> Result<(), MuxError<(), ()>> {
        self.reserve_channel_(local_dock, remote_dock)?;
        let marked = self.with_mut_(|inner| {
            let Some(dock) = inner.find_dock_mut_(local_dock) else {
                return false;
            };
            let Some(node) = find_channel_mut_(&mut dock.head_, remote_dock) else {
                return false;
            };
            node.inbound_ = Inbound_::Pending(peer_report);
            true
        });
        if marked {
            self.notify_inbound_(local_dock);
        }
        Result::Ok(())
    }

    /// 是否有**尚未被 `income_async` 取走**的入向请求。
    pub(crate) fn has_pending_inbound_(&self, local_dock: Dock) -> bool {
        self.with_(|inner| {
            let Some(dock) = inner.find_dock_(local_dock) else {
                return false;
            };
            let mut cursor = dock.head_.as_ref();
            while let Option::Some(node) = cursor {
                if matches!(node.inbound_, Inbound_::Pending(_)) {
                    return true;
                }
                cursor = node.next_.as_ref();
            }
            false
        })
    }

    /// 取走 `local_dock` 上最早的一个待决入向请求（改成 `HandedOut`），返回对端 dock。
    ///
    /// 返回 `None` 表示当前没有待决请求（调用方应当先登记 waker 再重试）。
    pub(crate) fn take_pending_inbound_(&self, local_dock: Dock) -> Option<Dock> {
        self.with_mut_(|inner| {
            let dock = inner.find_dock_mut_(local_dock)?;
            let mut cursor = dock.head_.as_mut();
            while let Option::Some(node) = cursor {
                if matches!(node.inbound_, Inbound_::Pending(_)) {
                    let remote = node.remote_dock_;
                    if let Inbound_::Pending(report) = node.inbound_ {
                        node.inbound_ = Inbound_::HandedOut(report);
                    }
                    return Option::Some(remote);
                }
                cursor = node.next_.as_mut();
            }
            Option::None
        })
    }

    /// `accept_async` 取走该入向请求保存的对端窗口通告（状态归为 `None`）。
    pub(crate) fn take_inbound_report_(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
    ) -> Option<WindowReport> {
        self.with_mut_(|inner| {
            let dock = inner.find_dock_mut_(local_dock)?;
            let node = find_channel_mut_(&mut dock.head_, remote_dock)?;
            match core::mem::replace(&mut node.inbound_, Inbound_::None) {
                Inbound_::HandedOut(report) | Inbound_::Pending(report) => Option::Some(report),
                Inbound_::None => Option::None,
            }
        })
    }

    /// 在 `local_dock` 上显式登记一个**用途种类**（telegraph 端点 / channel），
    /// 用于保证 channel 与 telegraph 不共用同一个 local_dock。
    ///
    /// 注意：本方法只是「种类标记」，**不**承担绑定的独占性——重复登记同一种用途
    /// 是幂等的。`bind_async` 的独占性由 [`ChannelRegistry_::bind_dock_`] 负责，
    /// 因为 `use_` 会被 `release_channel_` 在子流清零时抹掉，不能当作绑定的凭据。
    ///
    /// # Errors
    ///
    /// 该 dock 已被**另一种**用途占用 → [`MuxError::DockInUse`]。
    // 仅供单元测试与尚未实现的 telegraph 路径使用；telegraph 落地后即可移除本行。
    #[allow(dead_code)]
    pub(crate) fn reserve_dock_(
        &self,
        local_dock: Dock,
        use_: DockUse_,
    ) -> Result<(), MuxError<(), ()>> {
        self.with_mut_(|inner| {
            let dock = inner.dock_mut_(local_dock);
            match dock.use_ {
                Option::None => {
                    dock.use_ = Option::Some(use_);
                    Result::Ok(())
                }
                Option::Some(existing) if existing == use_ => Result::Ok(()),
                Option::Some(_) => Result::Err(MuxError::DockInUse),
            }
        })
    }

    /// 解除 [`ChannelRegistry_::reserve_dock_`] 的登记；不存在时是空操作。
    // 仅供单元测试与尚未实现的 telegraph 路径使用；telegraph 落地后即可移除本行。
    #[allow(dead_code)]
    pub(crate) fn release_dock_(&self, local_dock: Dock) {
        self.with_mut_(|inner| {
            let Some(dock) = inner.find_dock_mut_(local_dock) else {
                return;
            };
            if dock.chan_count_ == 0 {
                dock.use_ = Option::None;
            }
            inner.prune_dock_(local_dock);
        })
    }

    /// **独占绑定** `local_dock`（[`TrConnection::bind_async`] 的登记点）。
    ///
    /// 一个 `local_dock` 在任意时刻至多被一个 [`DockBinding`] 占用：绑定即认领
    /// 该 dock 的「会话身份」，之后第二次 `bind_async` 必须失败，而不是静默地
    /// 再发一个 binding 出去。这是 §4.1「dock 对即身份」在**绑定层**的前置检查
    /// ——子流层的 `reserve_channel_` 只拦得住「同一个 dock 对上的第二条并发
    /// 子流」，拦不住「同一个 dock 上两个各自独立的 binding」。
    ///
    /// 绑定是**持久**占用：直到 [`ChannelRegistry_::unbind_dock_`] 被调用（即对应
    /// `DockBinding` 被 drop）为止，该 dock 一直处于已绑定状态，即使其上暂时没有
    /// 任何子流（对比 `use_` 会被 `release_channel_` 清零）。
    ///
    /// # Errors
    ///
    /// - 该 dock 已被另一个绑定占用 → [`MuxError::DockInUse`]；
    /// - 该 dock 已被 telegraph 占用 → [`MuxError::DockInUse`]（channel 与
    ///   telegraph 不得共用 local_dock，见 [crate::connection] 模块文档 §4.1）。
    ///
    /// [`TrConnection::bind_async`]: abs_smux::conn::TrConnection::bind_async
    /// [`DockBinding`]: crate::connection::DockBinding
    pub(crate) fn bind_dock_(&self, local_dock: Dock) -> Result<(), MuxError<(), ()>> {
        self.with_mut_(|inner| {
            let dock = inner.dock_mut_(local_dock);
            if dock.bound_ {
                return Result::Err(MuxError::DockInUse);
            }
            if let Option::Some(DockUse_::Telegraph) = dock.use_ {
                return Result::Err(MuxError::DockInUse);
            }
            dock.bound_ = true;
            dock.use_ = Option::Some(DockUse_::Channel);
            Result::Ok(())
        })
    }

    /// 解除 [`ChannelRegistry_::bind_dock_`] 的独占绑定；不存在时是空操作。
    ///
    /// 由 `DockBinding` 的 `Drop` 调用。解绑只清 `bound_`；`use_` 的清除与节点
    /// 回收交给 `prune_dock_` 判空，因此「解绑后该 dock 上还有活动子流」不会
    /// 影响这些子流。
    pub(crate) fn unbind_dock_(&self, local_dock: Dock) {
        self.with_mut_(|inner| {
            if let Option::Some(dock) = inner.find_dock_mut_(local_dock) {
                dock.bound_ = false;
                if dock.chan_count_ == 0 {
                    dock.use_ = Option::None;
                }
            }
            inner.prune_dock_(local_dock);
        })
    }

    /// 登记 `local_dock` 上的入向等待者（listener 的 `income_async`）。
    pub(crate) fn register_inbound_waker_(&self, local_dock: Dock, waker: &Waker) {
        self.with_mut_(|inner| {
            inner.dock_mut_(local_dock).inbound_waker_.register_(waker);
        })
    }

    /// 唤醒 `local_dock` 上的入向等待者（若有）；唤醒在释放锁之后进行。
    pub(crate) fn notify_inbound_(&self, local_dock: Dock) {
        let waker = self.with_mut_(|inner| {
            inner.dock_mut_(local_dock).inbound_waker_.take_()
        });
        if let Option::Some(waker) = waker {
            waker.wake();
        }
    }

    /// 记下连接级失败（**首个**原因生效），唤醒两个循环的取消令牌，并唤醒所有
    /// 等待中的 API 面 future（监听者的 `income_async` 与建流的
    /// `open_channel_async`），避免它们空等一个已经死掉的循环。
    pub(crate) fn mark_failed_<RE, WE>(&self, err: &MuxError<RE, WE>) {
        let kind = FailKind_::of_(err);
        let wakers = self.with_mut_(|inner| {
            if inner.fail_.is_none() {
                inner.fail_ = Option::Some(kind);
            }
            let mut out: Vec<Waker> = Vec::new();
            let mut cursor = inner.docks_.as_mut();
            while let Option::Some(node) = cursor {
                if let Option::Some(waker) = node.inbound_waker_.take_() {
                    out.push(waker);
                }
                let mut chan = node.head_.as_mut();
                while let Option::Some(entry) = chan {
                    if let Option::Some(owner) = entry.owner_.as_ref()
                        && let Option::Some(waker) =
                            owner.with_mut_(|state| state.take_establish_waker_())
                    {
                        out.push(waker);
                    }
                    chan = entry.next_.as_mut();
                }
                cursor = node.next_.as_mut();
            }
            out
        });
        for waker in wakers {
            waker.wake();
        }
        self.cancel_loops_();
    }

    /// 连接级失败的原因（若有）。
    pub(crate) fn failure_(&self) -> Option<FailKind_> {
        self.with_(|inner| inner.fail_)
    }

    /// 连接是否已经失败。
    pub(crate) fn is_failed_(&self) -> bool {
        self.with_(|inner| inner.fail_.is_some())
    }

    /// 取第 `idx` 个循环的取消令牌（`0` = 读循环，`1` = 写循环）。
    pub(crate) fn loop_token_(&self, idx: usize) -> CancelToken_<A> {
        self.with_(|inner| inner.loops_[idx].clone())
    }

    /// 触发两个循环的取消令牌（连接关闭或失败时调用）。
    pub(crate) fn cancel_loops_(&self) {
        let tokens = self.with_(|inner| inner.loops_.clone());
        for token in tokens.iter() {
            token.cancel_();
        }
    }

    /// 持读锁执行 `f`（临界区不得 `await`、不得重入）。
    fn with_<R>(&self, f: impl FnOnce(&RegistryInner_<A>) -> R) -> R {
        let mut session = self.inner_.acquire_session();
        let guard = loop {
            match session.try_read() {
                Result::Ok(guard) => break guard,
                Result::Err(_) => on_lock_contended_(),
            }
        };
        f(&guard)
    }

    /// 持写锁执行 `f`（临界区不得 `await`、不得重入）。
    fn with_mut_<R>(&self, f: impl FnOnce(&mut RegistryInner_<A>) -> R) -> R {
        let mut session = self.inner_.acquire_session();
        let mut guard = loop {
            match session.try_write() {
                Result::Ok(guard) => break guard,
                Result::Err(_) => on_lock_contended_(),
            }
        };
        f(&mut guard)
    }
}

impl<A> RegistryInner_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 找到 `dock` 对应的节点（共享借用）；不存在时返回 `None`。
    fn find_dock_(&self, dock: Dock) -> Option<&DockNode_<A>> {
        let mut cursor = self.docks_.as_ref();
        while let Option::Some(node) = cursor {
            if node.dock_ == dock {
                return Option::Some(node);
            }
            cursor = node.next_.as_ref();
        }
        Option::None
    }

    /// 找到 `dock` 对应的节点；不存在时返回 `None`。
    fn find_dock_mut_(&mut self, dock: Dock) -> Option<&mut DockNode_<A>> {
        let mut cursor = self.docks_.as_mut();
        while let Option::Some(node) = cursor {
            if node.dock_ == dock {
                return Option::Some(node);
            }
            cursor = node.next_.as_mut();
        }
        Option::None
    }

    /// 找到（找不到就按需分配并插到链头）`dock` 对应的节点。
    fn dock_mut_(&mut self, dock: Dock) -> &mut DockNode_<A> {
        if self.find_dock_mut_(dock).is_some() {
            // 上一行已确认存在；这里再走一次，把借用缩到本次调用。
            match self.find_dock_mut_(dock) {
                Option::Some(node) => return node,
                Option::None => unreachable!("上一步已确认节点存在"),
            }
        }
        let alloc = self.alloc_.clone();
        self.docks_ = Option::Some(Owned::new(
            DockNode_ {
                dock_: dock,
                use_: Option::None,
                bound_: false,
                chan_count_: 0usize,
                head_: Option::None,
                next_: self.docks_.take(),
                inbound_waker_: WakerSlot_::new_(),
            },
            alloc,
        ));
        match self.docks_.as_mut() {
            Option::Some(node) => node,
            // 刚插入，必然存在。
            Option::None => unreachable!("dock 节点刚被插入"),
        }
    }

    /// 释放「空」的 dock 节点：无子流、无显式占用、无等待者。
    ///
    /// 单链表摘除需要同时看到「前驱」与「后继」，因此头节点单独处理，避免在一次
    /// 遍历里同时持有两个可变借用。
    fn prune_dock_(&mut self, dock: Dock) {
        let remove_head = match self.docks_.as_ref() {
            Option::Some(node) => node.dock_ == dock && node.is_empty_(),
            Option::None => false,
        };
        if remove_head {
            let next = self.docks_.as_mut().and_then(|node| node.next_.take());
            self.docks_ = next;
            return;
        }

        let mut cursor = self.docks_.as_mut();
        while let Option::Some(node) = cursor {
            let remove_next = match node.next_.as_ref() {
                Option::Some(next) => next.dock_ == dock && next.is_empty_(),
                Option::None => false,
            };
            if remove_next {
                let next_next = node.next_.as_mut().and_then(|next| next.next_.take());
                node.next_ = next_next;
                return;
            }
            cursor = node.next_.as_mut();
        }
    }
}

/// 在子流链上按 `remote_dock` 找一个节点（共享借用）。
fn find_channel_<A>(
    head: &Option<Owned<ChannelNode_<A>, A>>,
    remote_dock: Dock,
) -> Option<&ChannelNode_<A>>
where
    A: AllocatorClone,
{
    let mut cursor = head.as_ref();
    while let Option::Some(node) = cursor {
        if node.remote_dock_ == remote_dock {
            return Option::Some(node);
        }
        cursor = node.next_.as_ref();
    }
    Option::None
}

/// 在子流链上按 `remote_dock` 找一个节点（可变借用）。
fn find_channel_mut_<A>(
    head: &mut Option<Owned<ChannelNode_<A>, A>>,
    remote_dock: Dock,
) -> Option<&mut ChannelNode_<A>>
where
    A: AllocatorClone,
{
    let mut cursor = head.as_mut();
    while let Option::Some(node) = cursor {
        if node.remote_dock_ == remote_dock {
            return Option::Some(node);
        }
        cursor = node.next_.as_mut();
    }
    Option::None
}

fn remove_channel_<A>(
    head: &mut Option<Owned<ChannelNode_<A>, A>>,
    remote_dock: Dock,
) -> bool
where
    A: AllocatorClone,
{
    let remove_head = match head.as_ref() {
        Option::Some(node) => node.remote_dock_ == remote_dock,
        Option::None => false,
    };
    if remove_head {
        let next = head.as_mut().and_then(|node| node.next_.take());
        *head = next;
        return true;
    }

    let mut cursor = head.as_mut();
    while let Option::Some(node) = cursor {
        let remove_next = match node.next_.as_ref() {
            Option::Some(next) => next.remote_dock_ == remote_dock,
            Option::None => false,
        };
        if remove_next {
            let next_next = node.next_.as_mut().and_then(|next| next.next_.take());
            node.next_ = next_next;
            return true;
        }
        cursor = node.next_.as_mut();
    }
    false
}


#[cfg(test)]
mod tests_ {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Wake,
    };

    use core::task::Waker;

    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use crate::{
        connection::{Dock, MuxError, owner_::ChannelOwner_},
        handshake::opts::BasicOpts,
    };

    use super::*;

    /// 计数唤醒器：统计 `wake` 被调用次数，用来断言唤醒确实发生。
    struct CountingWake_ {
        count_: AtomicUsize,
    }

    impl Wake for CountingWake_ {
        fn wake(self: Arc<Self>) {
            self.count_.fetch_add(1usize, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.count_.fetch_add(1usize, Ordering::SeqCst);
        }
    }

    /// 造一个带计数的 `Waker`。
    /// - 手段：把 [`CountingWake_`] 包进 `Arc` 再转成 `Waker`。
    /// - 判断：返回的 `Waker` 每次被 `wake` 都会让计数加一。
    fn counting_waker_() -> (Waker, Arc<CountingWake_>) {
        let probe = Arc::new(CountingWake_ {
            count_: AtomicUsize::new(0),
        });
        let waker = Waker::from(probe.clone());
        (waker, probe)
    }

    /// 测试 dock 级与连接级配额各自生效、释放后可重新登记。
    /// - 手段：协商值收紧为「整条连接 2 条、单 dock 1 条」；依次登记
    ///   `(1,9)`、`(1,10)`、`(2,9)`、`(3,9)`，再释放 `(1,9)` 后重新登记。
    /// - 判断：同一 dock 上的第二条报 `DockChanLimit`；连接上的第三条报
    ///   `ChanLimit`；释放后总数回落且同一 dock 可再登记。
    #[test]
    fn reserve_enforces_dock_and_connection_limits() {
        let opts = BasicOpts {
            max_channel_count: 2usize,
            max_dock_chan_count: 1usize,
            ..BasicOpts::default()
        };
        let registry = ChannelRegistry_::new_(opts, CoreAlloc);
        assert_eq!(registry.max_channel_count_(), 2usize);
        assert_eq!(registry.max_dock_chan_count_(), 1usize);

        assert!(registry.reserve_channel_(Dock::new(1u32), Dock::new(9u32)).is_ok());
        assert_eq!(registry.total_channels_(), 1usize);

        let err = registry
            .reserve_channel_(Dock::new(1u32), Dock::new(10u32))
            .unwrap_err();
        assert!(matches!(err, MuxError::DockChanLimit));
        assert!(registry.reserve_channel_(Dock::new(2u32), Dock::new(9u32)).is_ok());
        let err = registry
            .reserve_channel_(Dock::new(3u32), Dock::new(9u32))
            .unwrap_err();
        assert!(matches!(err, MuxError::ChanLimit));

        registry.release_channel_(Dock::new(1u32), Dock::new(9u32));
        assert_eq!(registry.total_channels_(), 1usize);
        assert!(
            registry.reserve_channel_(Dock::new(1u32), Dock::new(11u32)).is_ok(),
            "释放配额后同一 dock 应当可以重新登记"
        );
        assert_eq!(registry.total_channels_(), 2usize);
    }
    /// 测试 channel 与 telegraph 不得共用同一个 local_dock（双向都拒绝）。
    /// - 手段：先在一个 dock 上登记 telegraph 再登记 channel；换一个 dock 反向
    ///   操作。
    /// - 判断：两次都报 `MuxError::DockInUse`；同一种用途重复登记是幂等的。
    #[test]
    fn channel_and_telegraph_are_mutually_exclusive_on_a_dock() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);

        assert!(
            registry
                .reserve_dock_(Dock::new(5u32), DockUse_::Telegraph)
                .is_ok()
        );
        assert!(
            registry
                .reserve_dock_(Dock::new(5u32), DockUse_::Telegraph)
                .is_ok(),
            "重复登记同一种用途应当幂等"
        );
        let err = registry
            .reserve_channel_(Dock::new(5u32), Dock::new(9u32))
            .unwrap_err();
        assert!(matches!(err, MuxError::DockInUse));

        assert!(registry.reserve_channel_(Dock::new(6u32), Dock::new(9u32)).is_ok());
        let err = registry
            .reserve_dock_(Dock::new(6u32), DockUse_::Telegraph)
            .unwrap_err();
        assert!(matches!(err, MuxError::DockInUse));
    }
    /// 测试 dock 绑定是**独占**且**持久**的：同一 `local_dock` 第二次绑定必须报错，
    /// 解绑后可以重绑，且绑定状态不因该 dock 上子流清零而丢失。
    ///
    /// - 手段：对同一 dock 连续 `bind_dock_`；在另一个 dock 上正常绑定；用
    ///   telegraph 占用第三个 dock 后再绑定；再在一个已绑定的 dock 上登记并释放
    ///   一条子流；最后 `unbind_dock_` 后重绑。
    /// - 判断：第二次绑定、绑定 telegraph 占用的 dock 都报 `MuxError::DockInUse`；
    ///   不同 dock 互不影响；子流清零后再次绑定**仍**报 `DockInUse`（绑定是持久
    ///   占用，这正是只能靠独立的 `bound_` 而不能靠 `use_` 的地方）；解绑后重绑
    ///   成功。
    #[test]
    fn dock_binding_is_exclusive_and_persistent() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);

        // 首次绑定成功；同一 dock 第二次绑定必须失败。
        assert!(registry.bind_dock_(Dock::new(7u32)).is_ok());
        let err = registry.bind_dock_(Dock::new(7u32)).unwrap_err();
        assert!(
            matches!(err, MuxError::DockInUse),
            "同一 dock 重复绑定应当报 DockInUse"
        );

        // 不同 dock 互不影响。
        assert!(registry.bind_dock_(Dock::new(8u32)).is_ok());

        // telegraph 占用的 dock 不能绑定。
        assert!(
            registry
                .reserve_dock_(Dock::new(9u32), DockUse_::Telegraph)
                .is_ok()
        );
        let err = registry.bind_dock_(Dock::new(9u32)).unwrap_err();
        assert!(
            matches!(err, MuxError::DockInUse),
            "已作 telegraph 的 dock 不能绑定"
        );

        // 绑定状态不因该 dock 上子流清零而丢失（`use_` 会被 release_channel_ 清掉，
        // 因此绑定必须是独立字段）。
        assert!(
            registry
                .reserve_channel_(Dock::new(7u32), Dock::new(3u32))
                .is_ok()
        );
        registry.release_channel_(Dock::new(7u32), Dock::new(3u32));
        let err = registry.bind_dock_(Dock::new(7u32)).unwrap_err();
        assert!(
            matches!(err, MuxError::DockInUse),
            "子流清零不应解除绑定"
        );

        // 解绑后可重新绑定。
        registry.unbind_dock_(Dock::new(7u32));
        assert!(
            registry.bind_dock_(Dock::new(7u32)).is_ok(),
            "解绑后应当可以重新绑定"
        );
    }
    /// 测试入向等待者只被唤醒一次，且唤醒发生在取出之后。
    /// - 手段：在 dock 4 上登记等待者，调用两次 `notify_inbound_`。
    /// - 判断：第一次唤醒计数为 1；第二次仍是 1（槽已空，没有可唤醒的等待者）。
    #[test]
    fn notify_inbound_wakes_registered_listener_once() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);
        let (waker, probe) = counting_waker_();

        registry.register_inbound_waker_(Dock::new(4u32), &waker);
        registry.notify_inbound_(Dock::new(4u32));
        assert_eq!(probe.count_.load(Ordering::SeqCst), 1usize);

        registry.notify_inbound_(Dock::new(4u32));
        assert_eq!(
            probe.count_.load(Ordering::SeqCst),
            1usize,
            "没有登记等待者时不应再唤醒"
        );
    }
    /// 测试同一 dock 对上的第二条并发子流被拒（dock 对即身份）。
    /// - 手段：在 `(1,9)` 上登记一次后重复登记；释放后再登记。
    /// - 判断：重复登记报 `MuxError::Duplicate`；释放后同一 dock 对可以重新登记。
    #[test]
    fn duplicate_dock_pair_is_rejected() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);
        assert!(registry.reserve_channel_(Dock::new(1u32), Dock::new(9u32)).is_ok());
        assert_eq!(
            registry.total_channels_(),
            1usize,
            "重复登记不应计入第二条"
        );

        let err = registry
            .reserve_channel_(Dock::new(1u32), Dock::new(9u32))
            .unwrap_err();
        assert!(matches!(err, MuxError::Duplicate));
        assert_eq!(registry.total_channels_(), 1usize);

        registry.release_channel_(Dock::new(1u32), Dock::new(9u32));
        assert!(
            registry.reserve_channel_(Dock::new(1u32), Dock::new(9u32)).is_ok(),
            "释放后同一 dock 对应当可以重新登记"
        );
    }
    /// 测试入向请求的完整流转：登记 → 唤醒监听者 → 取走（HandedOut）→ 取回窗口通告。
    /// - 手段：先用 `reserve_inbound_` 登记 `(2,7)` 并带上对端窗口通告 `(0, 64)`；
    ///   再依次调用 `has_pending_inbound_` / `take_pending_inbound_` /
    ///   `take_inbound_report_`。
    /// - 判断：登记时唤醒计数为 1；取走前 `has_pending_inbound_` 为真、取走后为假；
    ///   取回的通告与登记时给出的完全相同。
    #[test]
    fn inbound_request_is_handed_out_with_report() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);
        let (waker, probe) = counting_waker_();
        registry.register_inbound_waker_(Dock::new(2u32), &waker);

        let report = WindowReport::new(0u64, 64u32);
        assert!(
            registry
                .reserve_inbound_(Dock::new(2u32), Dock::new(7u32), report)
                .is_ok()
        );
        assert_eq!(probe.count_.load(Ordering::SeqCst), 1usize, "登记入向请求应唤醒监听者");
        assert!(registry.has_pending_inbound_(Dock::new(2u32)));

        assert_eq!(
            registry.take_pending_inbound_(Dock::new(2u32)),
            Option::Some(Dock::new(7u32))
        );
        assert!(
            !registry.has_pending_inbound_(Dock::new(2u32)),
            "取走之后不应再报告有待决请求"
        );
        assert_eq!(registry.take_pending_inbound_(Dock::new(2u32)), Option::None);

        assert_eq!(
            registry.take_inbound_report_(Dock::new(2u32), Dock::new(7u32)),
            Option::Some(report)
        );
    }
    /// 测试建好环之后挂上的共享状态句柄能被按 dock 对查回。
    /// - 手段：登记 `(3,8)`，用 `FlowCtrl::new` 建一个 owner 并 `attach_owner_`；
    ///   再查 `channel_owner_`。
    /// - 判断：挂上之前查不到；挂上之后能查到同一个句柄（改一处、另一处可见）。
    #[test]
    fn attached_owner_is_visible_by_dock_pair() {
        use crate::flow_ctrl::{DefaultPolicy, FlowCtrl};

        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);
        assert!(registry.reserve_channel_(Dock::new(3u32), Dock::new(8u32)).is_ok());
        assert!(registry.channel_owner_(Dock::new(3u32), Dock::new(8u32)).is_none());

        let flow = FlowCtrl::new(&DefaultPolicy, 64usize);
        let owner = ChannelOwner_::new_(
            crate::connection::owner_::ChannelState_::new_(flow),
            CoreAlloc,
        );
        assert!(registry.attach_owner_(Dock::new(3u32), Dock::new(8u32), owner.clone()));

        let found = registry
            .channel_owner_(Dock::new(3u32), Dock::new(8u32))
            .expect("挂上之后应当能查到");
        owner.with_mut_(|state| state.set_tx_queued_(true));
        assert!(found.with_(|state| state.tx_queued_()), "查回的应是同一个共享句柄");
    }
}
