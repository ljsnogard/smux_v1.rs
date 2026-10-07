
//! 连接级的 **dock / 子流身份索引（注册表）**。
//!
//! 从 `sync_` 拆出：注册表管理的是「连接上哪些 dock 被绑定、哪些身份在册」，
//! 与连接的其余控制面同属 `mux_connection`；`sync_` 只保留通用的共享单元、
//! 取消令牌、失败投影与唤醒槽。
//!
//! # 三张 BTreeMap：统一身份表 + 两本索引
//!
//! 索引一律用 `BTreeMap` / `BTreeSet`（`allocator_api` 指定分配器，不再手写单链表
//! ——链表上每一次 `find` / `prune` 都是 O(n) 线性扫描）：
//!
//! | 结构 | 键 | 值 | 服务的查询 |
//! | --- | --- | --- | --- |
//! | `docks_` | `Dock`（local） | [`DockCtx_`]：绑定独占 + 在册子流计数 | `bind_dock_` / `unbind_dock_` / 配额 |
//! | `bindings_` | `(Dock, Dock)` | [`BindingSlot_`]：**四类身份** | 按 dock 对 O(log n) 定位；唯一事实源 |
//! | `remote_index_` | `(remote, local)` | —（`BTreeSet`） | 按 `remote_dock` 反查活跃子流的 `local_dock` |
//!
//! 另有一本**到期索引** `wait_close_expiry_`（`(Instant, local, remote)` 有序集合），
//! 只服务于宽限态回收，见下节。
//!
//! # 统一身份表：channel / telegraph / listener / wait-close
//!
//! 三类子流的身份**元数并不相同**（见 `abs_smux::conn`）：channel 有
//! `(local, remote)` 两个具体值；telegraph 与 listener 只占一个 `local_dock`
//! ——telegraph 的 `remote_dock` 是逐次操作的实参，listener 天然等待任意
//! `remote_dock`。要让三者共用一张表、一套查找路径，就得给后两者的键补一个
//! **哨兵 remote**。协议恰好提供了现成的哨兵：
//!
//! [`Dock::unspecified`]（`0`）与 [`Dock::wildcard`]（`u32::MAX`）是**双方保留值，
//! 永远不会作为真实子流的 `remote_dock` 出现**（见 [`crate::connection`] 模块
//! 文档 §4），因此它们在表内是安全的内部标记：
//!
//! | 身份 | 键 | `BindingSlot_` 变体 |
//! | --- | --- | --- |
//! | channel | `(local, 具体值)` | [`BindingSlot_::Channel`] |
//! | telegraph | `(local, unspecified)` | [`BindingSlot_::Telegraph`] |
//! | listener | `(local, wildcard)` | [`BindingSlot_::Listener`] |
//! | 已关闭、宽限期内 | `(local, 具体值)` | [`BindingSlot_::WaitClose`] |
//!
//! 键序因此天然是「telegraph(0) < channel < listener(MAX)」，带来三个好处：
//!
//! 1. 「按 `local_dock` 枚举**具体子流**」是一次开区间 `range`，两个哨兵行自然被
//!    排除（见 [`channel_range_`]）；
//! 2. 「channel 与 telegraph 不得共用 local_dock」（[`abs_smux::conn::TrTelegraph`]
//!    的约束）退化成对 `(local, unspecified)` 的一次查找，不再需要单独的用途枚举；
//! 3. listener 与 channel 是不同键，天然可以共存于同一个 `local_dock`。
//!
//! ## 为什么主键一定得是 dock 对
//!
//! `remote_dock` 单独**不能**作主键：发起方会把发往同一个对端监听 dock 的多条
//! 并发子流用不同的 `local_dock` 区分（见 [`crate::connection`] 模块文档 §4.1），
//! 于是同一个 `remote_dock` 会对应多条在册子流。反过来 `local_dock` 单独也不能
//! 作主键：响应方的监听 dock 同样被多条子流共用。**只有 `(local, remote)` 这个
//! 有序对恒唯一**。
//!
//! # `WaitClose`：拆流宽限期
//!
//! TCP 关闭后短时间内不复用端口，是为了让网络上的陈旧报文过期。本协议跑在
//! **可靠、有序**的字节流上，同一连接内不存在重排 / 重传，同连接内的身份复用不会
//! 把旧报文投给新化身——**经典 TIME_WAIT 的动机在这里不成立**。
//!
//! 但拆流本身有一个真实竞态：本端判定「两个方向都收尾」后立即摘掉读侧表项，而
//! 对端在**收到我们 CLOSE 之前**已经发出的数据帧仍在途，它们到达时会变成「未知
//! 子流」。若直接按协议违例处理（[`MuxError::MalformedFrame`]），**整条连接会被
//! 杀死**。因此一条子流释放后，其键不会立即消失，而是转成
//! [`BindingSlot_::WaitClose`] 并在 `max_channel_wait_close` 内保留：
//!
//! - 读循环据此**静默丢弃**在途帧，而真正的未知子流仍然判协议违例
//!   （见 [`ChannelRegistry_::is_wait_close_`]）；
//! - 同一 dock 对在宽限期内**不得复用**，[`ChannelRegistry_::reserve_channel_`]
//!   报 [`ReserveErr_::WaitClose`]——这是顺带得到的、与 TCP 同形的复用保护。
//!
//! 宽限态由 [`ChannelRegistry_::reap_wait_close_`] 按到期时刻回收；到期索引让回收
//! 是 O(k log n)（k 为本轮到期数）而不是全表 O(n)。
//!
//! # 按 local_dock 枚举
//!
//! 不需要第 4 张业务表：[`channel_range_`] 给出的开区间恰好覆盖某个
//! `local_dock` 下的全部具体子流。

use alloc::collections::{BTreeMap, BTreeSet};
use core::{
    alloc::AllocatorClone,
    ops::Bound,
    sync::atomic::{AtomicU8, Ordering},
    task::{Context, Poll},
};

use abs_cancel::TrCancellationToken;
use atomic_sync::rwlock::cooperative::CooperativeRwLockOwned;
use buffex::x_deps::abs_cancel;
use mm_ptr::Shared;

use crate::{
    connection::{
        Dock, MuxError,
        owner_::{
            AbortCode_, ChannelNotice, ChannelOwner_, LsnOwner_, TgOwner_, new_listener_owner_,
            new_owner_, new_telegraph_owner_,
        },
        signal_::{SessionEvent_, SessionMailbox_},
        sync_::{CancelToken_, LockCancelled_, NotifySlot_, acquire_read_, acquire_write_},
        timer_::TimerAction_,
    },
    flow_ctrl::WindowReport,
    handshake::opts::BasicOpts,
    metrics::ConnCloseReason,
    time::millis_of_,
};

/// [`ChannelRegistry_::fail_kind_`] 的取值；`0` = 从未发生连接级失败（正常收尾）。
const K_FAIL_NONE: u8 = 0u8;

/// 对端主动关闭。
const K_FAIL_PEER_CLOSED: u8 = 1u8;

/// 传输层读 / 写错误。
const K_FAIL_TRANSPORT: u8 = 2u8;

/// 协议错误（非法帧、状态机错误、流控违例等）。
const K_FAIL_PROTOCOL: u8 = 3u8;

/// 把连接级错误投影成**锁外的关闭原因类别**。
///
/// 之所以要这个投影：[`RegistryInner_::fail_`] 在锁内，而 `MuxCore::drop` 的硬纪律是
/// **不取锁、不阻塞**，因此「连接为什么结束」必须在锁外也读得到（见
/// [`ChannelRegistry_::fail_kind_`]）。
fn fail_kind_of_(err: &MuxError) -> u8 {
    match err {
        MuxError::PeerClosed => K_FAIL_PEER_CLOSED,
        MuxError::Transport { .. } => K_FAIL_TRANSPORT,
        _ => K_FAIL_PROTOCOL,
    }
}

/// [`fail_kind_of_`] 的逆：把锁外快照解码成连接级失败类别；`0`（从未失败）与未知值
/// 都给出 `None`。
fn decode_fail_kind_(raw: u8) -> Option<ConnCloseReason> {
    match raw {
        K_FAIL_PEER_CLOSED => Option::Some(ConnCloseReason::PeerClosed),
        K_FAIL_TRANSPORT => Option::Some(ConnCloseReason::Transport),
        K_FAIL_PROTOCOL => Option::Some(ConnCloseReason::ProtocolError),
        _ => Option::None,
    }
}

/// 注册表在「预留 / 绑定一个身份」时能给出的失败。
///
/// 它只在本模块内部使用；不同 API 面会把它映射进各自公开错误枚举。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReserveErr_ {
    /// 该 local_dock 已被占用（binding / channel / telegraph / listener）。
    DockInUse,

    /// 同一 dock 对上已有活跃子流。
    Duplicate,

    /// 该 dock 对刚关闭，仍在拆流宽限期内。
    WaitClose,

    /// 该 dock 上的活动子流数已达上限。
    DockChanLimit,

    /// 连接上的活动子流数已达上限。
    ChanLimit,

    /// **等锁期间被取消**（cancel token 触发）。
    ///
    /// 它不是业务规则的拒绝，而是调用方主动放弃；各 API 面把它映射成各自的
    /// `Cancelled` 变体。
    Cancelled,
}

impl From<LockCancelled_> for ReserveErr_ {
    fn from(_: LockCancelled_) -> Self {
        ReserveErr_::Cancelled
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 键的构造与区间
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// telegraph 身份在统一表里的键：`(local, unspecified)`。
///
/// `unspecified` 是协议保留值，永远不会作为真实子流的 `remote_dock`。
fn telegraph_key_(local_dock: Dock) -> (Dock, Dock) {
    (local_dock, Dock::unspecified())
}

/// listener 身份在统一表里的键：`(local, wildcard)`。
///
/// `wildcard` 是协议保留值，永远不会作为真实子流的 `remote_dock`；且它是 dock
/// 序的**最大值**，因此排在所有具体子流之后，便于用开区间切出具体子流。
fn listener_key_(local_dock: Dock) -> (Dock, Dock) {
    (local_dock, Dock::wildcard())
}

/// `bindings_` 上「某个 `local_dock` 的**具体子流**」键区间：两端都开。
///
/// 两个哨兵行（`unspecified` / `wildcard`）被自然排除，只剩真正带具体
/// `remote_dock` 的身份（channel 与 wait-close）。
type KeyRange_ = (Bound<(Dock, Dock)>, Bound<(Dock, Dock)>);

fn channel_range_(local_dock: Dock) -> KeyRange_ {
    (
        Bound::Excluded(telegraph_key_(local_dock)),
        Bound::Excluded(listener_key_(local_dock)),
    )
}

/// `remote_index_` 上「某个 `remote_dock` 关联的全部 `local_dock`」键区间。
///
/// 反向索引只登记 `local_dock` 为具体值的活跃子流，因此两端闭区间即可覆盖。
fn remote_range_(remote_dock: Dock) -> KeyRange_ {
    (
        Bound::Included((remote_dock, Dock::unspecified())),
        Bound::Included((remote_dock, Dock::wildcard())),
    )
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 身份表
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 入向建流请求在索引节点上的状态。
///
/// 被动方收到 `OPEN` 后**先回一条自己的 `OPEN`**（通告接收窗口），此时环还没有
/// 建立——真正建环发生在应用 `accept_async` 那一刻（那时才拿得到调用方注入的
/// 配置）。因此这里要先把对端 `OPEN` 携带的窗口通告存住，直到 `accept` 使用。
///
/// `WindowReport` 是 `Copy`，本枚举因此也是 `Copy`：取出 / 改写状态不必
/// `mem::replace` 兜底。
#[derive(Debug, Clone, Copy)]
pub(crate) enum Inbound_ {
    /// 该节点不是入向请求（主动方发起的子流，或入向请求已处理完）。
    None,

    /// 已收到 `OPEN`、等待 `income_async` 取走。
    Pending(WindowReport),

    /// 已被 `income_async` 取走（`ChannelHandle` 在应用手里），等待 accept / reject。
    HandedOut(WindowReport),
}

/// 一条 channel 身份记录的表槽部分：**冷**状态内联在表里 + 一个热状态节点句柄。
///
/// # 状态与身份同寿命（2026-10 改造；2026-10-05 形状三）
///
/// `rec_` 是这条子流的**身份节点句柄**（[`ChannelOwner_`]，类型化地只指向
/// `DockBinding_::Channel` 变体的共享节点）：热状态（状态字 + 两个窗口 + 内联建流通知槽）
/// 内联在那个节点里，随身份登记一起建立、一起消亡。它**不是**建流后期再 `attach`
/// 上来的独立对象，也**不是**另一个可独立死亡的共享实体——「一个身份一个状态对象」。
///
/// `inbound_` 留在**表槽**里（而不是节点里）：只有建流冷路径会改它，而 `Shared` 只交出
/// `&T`，放进节点就得为它再加内部可变性（锁或原子打包）；放表槽里正好由注册表的写锁
/// 保护，零额外代价。
struct ChanSlot_<A>
where
    A: AllocatorClone,
{
    /// 该 channel 身份节点的类型化句柄（登记身份时建立；窗口参数在最终裁决时安装）。
    rec_: ChannelOwner_<A>,

    /// 入向建流请求的状态。
    inbound_: Inbound_,
}

impl<A> ChanSlot_<A>
where
    A: AllocatorClone,
{
    /// 新登记的子流：状态已建（窗口参数待安装）、不是入向请求。
    fn new_(rec_: ChannelOwner_<A>) -> Self {
        ChanSlot_ {
            rec_,
            inbound_: Inbound_::None,
        }
    }
}

/// 一条已关闭子流的宽限状态（墓碑）。
///
/// 只作**身份标记**：到期时刻存在 [`RegistryInner_::wait_close_expiry_`] 里
/// （那是按时间有序、供回收使用的索引），这里不再重复一份，避免两处状态需要同步。
struct WaitCloseCtx_;

/// 统一身份表里一个 `(local, remote)` 键的形态（**表槽**；身份节点本身在
/// [`DockBinding_`](crate::connection::owner_::DockBinding_) 里）。
///
/// 四个变体与键的对应关系见模块文档「统一身份表」。三类活身份的载荷都只是一个
/// **类型化节点句柄**（`Channel` 额外带一份留在表槽里的冷状态 [`ChanSlot_`]），
/// 因此表槽本身不含堆分配，节点的热状态也在锁外可达。
enum BindingSlot_<A>
where
    A: AllocatorClone,
{
    /// 一条子流（键的 `remote` 是具体值）；冷状态与节点句柄见 [`ChanSlot_`]。
    Channel(ChanSlot_<A>),

    /// 数据报端点（键的 `remote` 固定为 `unspecified`）。
    ///
    /// 句柄当前**没有读取者**：telegraph 的收发未实现，端点自己持一份句柄来保活；
    /// 保留在表槽里是为了形状统一，以及后续「按 `remote_dock` 路由 DATAGRAM」能直接
    /// 从身份表拿到该端点的节点。
    #[allow(dead_code)]
    Telegraph(TgOwner_<A>),

    /// 监听器（键的 `remote` 固定为 `wildcard`）。
    Listener(LsnOwner_<A>),

    /// 已关闭、宽限期内的子流身份（键的 `remote` 是具体值）。
    WaitClose(WaitCloseCtx_),
}

/// 一个 `local_dock` 的 dock 级簿记。
///
/// 这里**只**放与「身份种类」无关的量：绑定独占与在册子流计数。身份本身在
/// `bindings_` 里按键区分（`use_` 之类用途标记已由 `BindingSlot_` 变体承担）。
struct DockCtx_ {
    /// 该 dock 是否已被某个 [`DockBinding`] **独占绑定**。
    ///
    /// 绑定是**持久**占用：直到 [`ChannelRegistry_::unbind_dock_`] 被调用（即对应
    /// `DockBinding` 被 drop）为止一直有效，即使其上暂时没有任何子流。
    ///
    /// [`DockBinding`]: crate::connection::DockBinding
    bound_: bool,

    /// 本 dock 上当前在册的**活跃 channel** 数（受 `max_dock_chan_count` 约束）。
    ///
    /// `WaitClose` 不计入（它已经不是活跃子流），由 `bindings_` 里具体键的变体
    /// 决定归属；两者的一致性由单测钉住。
    chan_count_: usize,
}

impl DockCtx_ {
    /// 空条目（未绑定、无子流）。
    const fn new_() -> Self {
        DockCtx_ {
            bound_: false,
            chan_count_: 0usize,
        }
    }

    /// 是否「空」：未绑定且无活跃子流。
    ///
    /// `bound_` 必须计入：绑定是**持久**占用，不能因为该 dock 上暂时没有子流就
    /// 把条目剪掉、把绑定状态一起丢掉。
    fn is_empty_(&self) -> bool {
        self.chan_count_ == 0usize && !self.bound_
    }
}

/// 共享注册表的内部状态。
struct RegistryInner_<A>
where
    A: AllocatorClone,
{
    /// 协商结果：本层用到配额与拆流宽限期。
    opts_: BasicOpts,

    /// 分配器（各索引结构各自持有它的一个克隆）。
    alloc_: A,

    /// `local_dock` → dock 级簿记。
    docks_: BTreeMap<Dock, DockCtx_, A>,

    /// `(local, remote)` → 身份；**唯一事实源**。
    bindings_: BTreeMap<(Dock, Dock), BindingSlot_<A>, A>,

    /// `(remote, local)` → 活跃子流的反向索引。
    ///
    /// 当前协议路径**不依赖**它（每帧都同时带两个 dock），保留是为了让「只知
    /// `remote_dock`」的查询能以 O(log n + k) 枚举；只登记活跃 channel，
    /// `WaitClose` 不入索引（它是墓碑，不是活跃子流）。
    remote_index_: BTreeSet<(Dock, Dock), A>,

    /// `(到期时刻, local, remote)` → 宽限态回收索引。
    ///
    /// 有了它，回收是「从最早到期的开始拿，直到没到期」的 O(k log n)，而不是全表
    /// O(n) 扫描；宽限态在高 churn 下数量可以很大。
    ///
    /// 时刻是**连接内毫秒**（自连接 epoch 起算，见 [`crate::time`]），因此这套索引
    /// 与「子流最后活动」用的是同一把尺子，也不需要任何 `Instant` 类型。
    wait_close_expiry_: BTreeSet<(u64, Dock, Dock), A>,

    /// 整条连接上当前在册的**活跃子流**数（受 `max_channel_count` 约束）。
    total_: usize,

    /// 连接级失败（**首个**原因保留，之后的失败不再覆盖）。
    fail_: Option<MuxError>,
}

impl<A> RegistryInner_<A>
where
    A: AllocatorClone,
{
    /// 回收已到期的 `WaitClose` 身份。
    ///
    /// 借助 [`RegistryInner_::wait_close_expiry_`] 的到期序，只动真正过期的条目：
    /// 每步 O(log n)，总计 O(k log n)（k 为本轮到期数）。
    ///
    /// 释放键时再确认它此刻**仍是** `WaitClose`：极端情况下该键可能已在同一
    /// 宽限期窗口内被别的路径改写（例如测试直接构造的配置），确认一次即可避免
    /// 误删活跃身份。
    fn reap_wait_close_(&mut self, now_millis: u64) {
        while let Option::Some((until, local_dock, remote_dock)) =
            self.wait_close_expiry_.first().copied()
        {
            if until > now_millis {
                break;
            }
            self.wait_close_expiry_
                .remove(&(until, local_dock, remote_dock));
            if matches!(
                self.bindings_.get(&(local_dock, remote_dock)),
                Option::Some(BindingSlot_::WaitClose(_))
            ) {
                self.bindings_.remove(&(local_dock, remote_dock));
            }
        }
    }

    /// 释放「空」的 dock 条目：未绑定且无活跃子流。
    ///
    /// `WaitClose` 墓碑不阻止剪枝——它们不需要 dock 级簿记。
    fn prune_dock_(&mut self, dock: Dock) {
        let empty = match self.docks_.get(&dock) {
            Option::Some(ctx) => ctx.is_empty_(),
            Option::None => false,
        };
        if empty {
            self.docks_.remove(&dock);
        }
    }

    /// `local_dock` 上是否已有**具体子流**（channel 或 wait-close）。
    fn has_concrete_identity_(&self, local_dock: Dock) -> bool {
        let (start, end) = channel_range_(local_dock);
        self.bindings_.range((start, end)).next().is_some()
    }
}

/// 计时循环一次扫描的结果。
///
/// 字段私有、只经关联函数读出——与本模块其余类型同一条纪律。
pub(crate) struct TimerScan_ {
    /// 本轮认领到的动作条数（`actions` 切片的前缀）。
    actions_: usize,

    /// 是否因为动作批次已满而提前停下：调用方应当**立刻**再扫一轮，不要睡。
    full_: bool,

    /// 下一次必须醒来的连接内毫秒（`u64::MAX` = 当前没有需要等待的期限）。
    next_millis_: u64,
}

impl TimerScan_ {
    /// 本轮认领到的动作条数。
    pub(crate) fn actions_(&self) -> usize {
        self.actions_
    }

    /// 是否因为批次满而提前停下。
    pub(crate) fn is_full_(&self) -> bool {
        self.full_
    }

    /// 下一次必须醒来的连接内毫秒。
    pub(crate) fn next_millis_(&self) -> u64 {
        self.next_millis_
    }
}

/// 读写循环与 API 面共享的 dock / 子流身份索引。
///
/// 所有方法都取 `&self`：内部可变性由 `atomic_sync` 的**协作式读写锁**
/// （`rwlock::cooperative::CooperativeRwLock`）提供。临界区都很短（映射增删与
/// 计数），且**不跨 `await`**；取锁一律 `acquire_session` + 异步获取
/// （`read_async` / `write_async().may_cancel_with(cancel).await`），因此争用时
/// 让出执行权、可被外部 cancel token 取消，也没有 CPU 忙等。
///
/// # 会话释放邮箱
///
/// 会话句柄的 `Drop` **不碰注册表**，只向 [`SessionMailbox_`] 投一条
/// [`SessionEvent_`]；真正的身份表改动由核心执行者在异步上下文里 drain 后落实
/// （[`ChannelRegistry_::drain_session_events_`]）。因此 `Drop` 既不取锁也不阻塞。
///
/// # 分配
///
/// 四本索引在构造时各拿一份分配器克隆（`allocator_api` 的 `new_in`），此后每个
/// 条目的分配 / 归还都由该分配器承担，注册表本身不隐式分配。
///
/// 已知例外：外层锁 `CooperativeRwLockOwned` 内部用全局 `Arc` 持有同步核心
/// （`atomic_sync` 尚未支持 `allocator_api`），即**每次连接一次**的全局分配；
/// 释放邮箱走 `flume`，同样是全局分配（已有例外）。后续引入 `TrMaxAllocConfig`
/// 时会连同这两处一起记账。
pub(crate) struct ChannelRegistry_<A>
where
    A: AllocatorClone,
{
    inner_: Shared<CooperativeRwLockOwned<RegistryInner_<A>>, A>,

    /// 五个循环的取消令牌：`0` = 读泵、`1` = 解复用、`2` = 复用、`3` = 写泵、
    /// `4` = 计时（保活 / 空闲超时）。
    ///
    /// **放在锁外**：构造后不再变化，因此取用与触发都无需取锁（`Drop` 路径要的
    /// 正是这一点）。
    loops_: [CancelToken_<A>; 5],

    /// 会话释放邮箱：`Drop` 投递、核心执行者 drain（**不入锁**）。
    mailbox_: SessionMailbox_,

    /// **计时循环的唤醒槽**：「身份表变了，期限可能要重算」的持久提示。
    ///
    /// 它让计时循环可以**动态**睡到「最早的期限」而不是固定 tick：一条新子流的
    /// 登记、或一次身份释放，都会在锁外 `notify_` 一次，把睡着的计时循环叫醒重算。
    ///
    /// 用 `Shared` 包一层是为了让它的各个注册表克隆（核心、五个循环各持一份）指向
    /// **同一个**槽；`NotifySlot_` 本身只有一个 waker 位与一个原子位，零堆分配，
    /// `Shared` 这一次分配是**每连接一次**。
    timer_wake_: Shared<NotifySlot_, A>,

    /// **连接级失败的种类**（锁外的原子快照）。
    ///
    /// 存在理由见 [`fail_kind_of_`]：`MuxCore::drop` 不能取锁，却要报出「连接为什么
    /// 结束」。用 `Shared` 包一层是为了让注册表的**各个克隆**（核心、五个循环各持一份）
    /// 指向同一份值——失败由循环侧写入、由核心侧在收尾时读出。
    /// 每连接一次分配，与 [`ChannelRegistry_::timer_wake_`] 同源。
    fail_kind_: Shared<AtomicU8, A>,
}

impl<A> Clone for ChannelRegistry_<A>
where
    A: AllocatorClone,
{
    fn clone(&self) -> Self {
        ChannelRegistry_ {
            inner_: self.inner_.clone(),
            loops_: self.loops_.clone(),
            // 邮箱按值克隆：生产端共享同一条队列，消费端因此有**多个** drain 者。
            mailbox_: self.mailbox_.clone(),
            timer_wake_: self.timer_wake_.clone(),
            fail_kind_: self.fail_kind_.clone(),
        }
    }
}

impl<A> ChannelRegistry_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 取分配器（读 / 写循环为自己的本地表按需分配）。
    pub(crate) async fn allocator_<K: TrCancellationToken>(
        &self,
        cancel: K,
    ) -> Result<A, ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let guard = acquire_read_(&mut session, cancel).await?;
        Result::Ok(guard.alloc_.clone())
    }

    /// 建立注册表：分配根对象与四本索引，并为两个循环建好取消令牌。
    ///
    /// # Panics
    ///
    /// 分配根对象失败时 panic（与标准库容器在 OOM 时的行为一致）。调用方应按
    /// `max_channel_count` 量级准备分配器。
    pub(crate) fn new_(opts: BasicOpts, alloc: A) -> Self {
        let loops = [
            // 0：读泵（transport → 连接读环）
            // 1：解复用（连接读环 → 各子流接收环）
            // 2：复用（各子流发送环 → 连接写环）
            // 3：写泵（连接写环 → transport）
            // 4：计时（保活 PULSE 与空闲超时拆流）
            CancelToken_::new_(alloc.clone()),
            CancelToken_::new_(alloc.clone()),
            CancelToken_::new_(alloc.clone()),
            CancelToken_::new_(alloc.clone()),
            CancelToken_::new_(alloc.clone()),
        ];
        let docks_ = BTreeMap::new_in(alloc.clone());
        // 计时唤醒槽的节点：与注册表根部同源分配器，**每连接一次**。
        let timer_wake_ = Shared::new(NotifySlot_::new_(), alloc.clone());
        // 连接级失败种类的共享快照：同样**每连接一次**，让所有克隆看得同一份。
        let fail_kind_ = Shared::new(AtomicU8::new(K_FAIL_NONE), alloc.clone());
        let bindings_ = BTreeMap::new_in(alloc.clone());
        let remote_index_ = BTreeSet::new_in(alloc.clone());
        let wait_close_expiry_ = BTreeSet::new_in(alloc.clone());
        ChannelRegistry_ {
            inner_: Shared::new(
                CooperativeRwLockOwned::new_owned(RegistryInner_ {
                    opts_: opts,
                    alloc_: alloc.clone(),
                    docks_,
                    bindings_,
                    remote_index_,
                    wait_close_expiry_,
                    total_: 0usize,
                    fail_: Option::None,
                }),
                alloc,
            ),
            loops_: loops,
            mailbox_: SessionMailbox_::new_(),
            timer_wake_,
            fail_kind_,
        }
    }

    /// 提示**计时循环**「期限表可能变了」，请醒来重算。
    ///
    /// # 调用纪律（与 [`NotifySlot_`] 的唤醒纪律同源）
    ///
    /// **不得在持有注册表守卫时调用**：`notify_` 会 `wake` 计时循环的 waker，而在
    /// 单线程执行器上 `wake` 可能同步重入 `poll`，后者又要取注册表锁
    /// （见 `keepalive…` §6.3）。因此登记路径一律「先出作用域、再 call」。
    pub(crate) fn notify_timer_(&self) {
        self.timer_wake_.notify_();
    }

    /// 计时循环的等待点：有唤醒提示时立刻就绪，否则登记 waker 后 `Pending`。
    pub(crate) fn poll_timer_wake_(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.timer_wake_.poll_wait_(cx)
    }

    /// 投递一条**会话释放消息**（会话句柄的 `Drop` 调用）。
    ///
    /// **不取锁、不阻塞**：消息由 [`ChannelRegistry_::drain_session_events_`] 落实。
    /// 返回值表示邮箱是否仍在（连接收尾时为 `false`，按「没送出去」处理）。
    pub(crate) fn post_session_event_(&self, event: SessionEvent_) -> bool {
        self.mailbox_.post_(event)
    }

    /// 取出并落实**全部**积压的会话释放消息。
    ///
    /// 这是「核心内部调度」的入口：两个循环每轮调用它；会创建 / 认领身份的 API
    /// 操作（`bind` / `listen` / `open_telegraph` / `open_channel` / `accept` /
    /// `reject`）在动身份表之前也调用它，于是「丢弃句柄后立刻复用同一身份」不需要
    /// 等某个特定任务被调度。
    ///
    /// # Panics
    ///
    /// **调用方不得正持有本注册表的锁**：落实每条消息都要再取一次锁。
    ///
    /// `now_millis` 是连接内毫秒，供落实 `ReleaseChannel`（进入拆流宽限期）时打点。
    pub(crate) async fn drain_session_events_<K: TrCancellationToken>(
        &self,
        now_millis: u64,
        cancel: K,
    ) -> Result<(), ReserveErr_> {
        while let Option::Some(event) = self.mailbox_.try_take_() {
            self.apply_session_event_(event, now_millis, cancel.child_token())
                .await?;
        }
        Result::Ok(())
    }

    /// 落实一条释放消息（内部方法：只由 drain 调用，**不自行 drain**）。
    async fn apply_session_event_<K: TrCancellationToken>(
        &self,
        event: SessionEvent_,
        now_millis: u64,
        cancel: K,
    ) -> Result<(), ReserveErr_> {
        match event {
            SessionEvent_::UnbindDock { local_dock } => {
                self.unbind_dock_(local_dock, cancel).await
            }
            SessionEvent_::ReleaseListener { local_dock } => {
                self.release_listener_(local_dock, cancel).await
            }
            SessionEvent_::ReleaseTelegraph { local_dock } => {
                self.release_telegraph_(local_dock, cancel).await
            }
            SessionEvent_::UnreserveChannel {
                local_dock,
                remote_dock,
            } => self.unreserve_channel_(local_dock, remote_dock, cancel).await,
            SessionEvent_::ReleaseChannel {
                local_dock,
                remote_dock,
            } => {
                self.release_channel_(local_dock, remote_dock, now_millis, cancel)
                    .await
            }
        }
    }

    /// 协商出的「整条连接最多同时在册子流数」。
    // 仅供单元测试与尚未实现的 telegraph 路径使用；telegraph 落地后即可移除本行。
    #[allow(dead_code)]
    pub(crate) async fn max_channel_count_<K: TrCancellationToken>(
        &self,
        cancel: K,
    ) -> Result<usize, ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let guard = acquire_read_(&mut session, cancel).await?;
        Result::Ok(guard.opts_.max_channel_count)
    }

    /// 协商出的「单个 dock 最多同时在册子流数」。
    // 仅供单元测试与尚未实现的 telegraph 路径使用；telegraph 落地后即可移除本行。
    #[allow(dead_code)]
    pub(crate) async fn max_dock_chan_count_<K: TrCancellationToken>(
        &self,
        cancel: K,
    ) -> Result<usize, ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let guard = acquire_read_(&mut session, cancel).await?;
        Result::Ok(guard.opts_.max_dock_chan_count)
    }

    /// 当前在册的**活跃子流**总数（不含宽限态）。
    // 仅供单元测试与尚未实现的 telegraph 路径使用；telegraph 落地后即可移除本行。
    #[allow(dead_code)]
    pub(crate) async fn total_channels_<K: TrCancellationToken>(
        &self,
        cancel: K,
    ) -> Result<usize, ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let guard = acquire_read_(&mut session, cancel).await?;
        Result::Ok(guard.total_)
    }

    /// 为 `(local_dock, remote_dock)` 登记一条 channel，并返回它的共享状态句柄。
    ///
    /// **状态随身份一起建立**：调用方拿到句柄后，在最终裁决（`accept_async`）时
    /// 把窗口参数安装进去（`ChannelState_::install_`）。
    ///
    /// # Errors
    ///
    /// - `local_dock` 已作 telegraph → [`ReserveErr_::DockInUse`]；
    /// - 同一 dock 对上有活跃子流 → [`ReserveErr_::Duplicate`]；
    /// - 同一 dock 对处于拆流宽限期 → [`ReserveErr_::WaitClose`]；
    /// - 该 dock 上的在册子流数已达 `max_dock_chan_count` → [`ReserveErr_::DockChanLimit`]；
    /// - 连接上的在册子流数已达 `max_channel_count` → [`ReserveErr_::ChanLimit`]。
    ///
    /// `now_millis` 是连接内毫秒（见 [`crate::time`]），用于顺带回收已到期的宽限态。
    pub(crate) async fn reserve_channel_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
        now_millis: u64,
        cancel: K,
    ) -> Result<ChannelOwner_<A>, ReserveErr_> {
        let state = {
            let mut session = self.inner_.acquire_session();
            let mut guard = acquire_write_(&mut session, cancel).await?;
            let inner = &mut *guard;
            inner.reap_wait_close_(now_millis);
            if inner.total_ >= inner.opts_.max_channel_count {
                return Result::Err(ReserveErr_::ChanLimit);
            }
            // 先把限额取出来：下面要可变借用 `docks_`。
            let max_dock = inner.opts_.max_dock_chan_count;

            // 检查顺序与旧链表实现一致：用途 → 重复 → dock 限额。
            if matches!(
                inner.bindings_.get(&telegraph_key_(local_dock)),
                Option::Some(BindingSlot_::Telegraph(_))
            ) {
                return Result::Err(ReserveErr_::DockInUse);
            }
            match inner.bindings_.get(&(local_dock, remote_dock)) {
                Option::Some(BindingSlot_::WaitClose(_)) => {
                    return Result::Err(ReserveErr_::WaitClose);
                }
                Option::Some(_) => return Result::Err(ReserveErr_::Duplicate),
                Option::None => {}
            }
            if let Option::Some(dock) = inner.docks_.get(&local_dock)
                && dock.chan_count_ >= max_dock
            {
                return Result::Err(ReserveErr_::DockChanLimit);
            }

            let state = new_owner_(inner.alloc_.clone());
            // 两个时钟立刻用「现在」打点：连接的 epoch 可能远早于本条子流的诞生，
            // 从 0 起算会让它第一条扫描就被判空闲超时。
            state.mark_data_(now_millis);
            // 创建时刻只写这一次：子流关闭时用它结算寿命（`metrics` 的
            // `on_channel_closed`），而 `data_millis_` 会被后续活动不断刷新。
            state.set_created_millis_(now_millis);
            let dock = inner
                .docks_
                .entry(local_dock)
                .or_insert_with(DockCtx_::new_);
            dock.chan_count_ += 1usize;
            inner.bindings_.insert(
                (local_dock, remote_dock),
                BindingSlot_::Channel(ChanSlot_::new_(state.clone())),
            );
            inner.remote_index_.insert((remote_dock, local_dock));
            inner.total_ += 1usize;
            state
        };
        // 守卫已在上面那个块结束时释放：新身份要参与保活计时，因此现在才唤醒计时循环
        // （**不得在持锁时唤醒**，理由见 `ChannelRegistry_::notify_timer_`）。
        self.notify_timer_();
        Result::Ok(state)
    }

    /// 拆掉一条 channel：**不删键**，改成宽限态 [`BindingSlot_::WaitClose`]；不存在
    /// 或已不是活跃 channel 时是空操作。
    ///
    /// 宽限期内到达的在途帧被读循环静默丢弃，同一 dock 对也不得复用
    /// （见模块文档「`WaitClose`：拆流宽限期」）。
    /// **撤销**一条尚未在线上露面的子流登记（不留拆流宽限期）。
    ///
    /// 与 [`release_channel_`](Self::release_channel_) 的区别只在宽限期：
    ///
    /// - `release_channel_`：用于「已经和对端交换过帧」的拆流。同一 dock 对在
    ///   `max_channel_wait_close` 内不可复用，好让在途帧被静默丢弃（§16.3 F8）；
    /// - `unreserve_channel_`：用于**发起方还没发出 `OPEN`** 就放弃的情形。对端根本
    ///   不知道这条子流存在、没有任何在途帧，因此身份与配额**立即**归还，同一 dock
    ///   对可以马上再用（否则「打开后反悔」会白白占住 dock 对一整个宽限期）。
    pub(crate) async fn unreserve_channel_<K: TrCancellationToken>(&self, local_dock: Dock, remote_dock: Dock, cancel: K) -> Result<(), ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let mut guard = acquire_write_(&mut session, cancel).await?;
        let inner = &mut *guard;
        let removed = inner
            .bindings_
            .remove(&(local_dock, remote_dock));
        if !matches!(removed, Option::Some(BindingSlot_::Channel(_))) {
            // 不是活跃子流（已拆、已宽限、或从来不是子流）：无事可做。
            return Result::Ok(());
        }
        inner.remote_index_.remove(&(remote_dock, local_dock));
        inner.total_ = inner.total_.saturating_sub(1usize);
        if let Option::Some(dock) = inner.docks_.get_mut(&local_dock) {
            dock.chan_count_ = dock.chan_count_.saturating_sub(1usize);
        }
        inner.prune_dock_(local_dock);
        Result::Ok(())
    }

    pub(crate) async fn release_channel_<K: TrCancellationToken>(&self, local_dock: Dock, remote_dock: Dock, now_millis: u64, cancel: K) -> Result<(), ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let mut guard = acquire_write_(&mut session, cancel).await?;
        let inner = &mut *guard;
        inner.reap_wait_close_(now_millis);
        if !matches!(
            inner.bindings_.get(&(local_dock, remote_dock)),
            Option::Some(BindingSlot_::Channel(_))
        ) {
            return Result::Ok(());
        }
        let until = now_millis.saturating_add(millis_of_(inner.opts_.max_channel_wait_close));
        inner.bindings_.insert(
            (local_dock, remote_dock),
            BindingSlot_::WaitClose(WaitCloseCtx_),
        );
        inner
            .wait_close_expiry_
            .insert((until, local_dock, remote_dock));
        // 反向索引只跟活跃子流；宽限态不再登记。
        inner.remote_index_.remove(&(remote_dock, local_dock));
        inner.total_ = inner.total_.saturating_sub(1usize);
        if let Option::Some(dock) = inner.docks_.get_mut(&local_dock) {
            dock.chan_count_ = dock.chan_count_.saturating_sub(1usize);
        }
        inner.prune_dock_(local_dock);
        Result::Ok(())
    }

    /// 该 dock 对是否处于「已关闭、宽限期内」。
    ///
    /// 读循环用它区分两类「本地表里查不到」的帧：命中 → 刚关闭，静默丢弃；
    /// 未命中 → 真正的未知子流，按协议违例处理。查询顺带回收已到期的宽限态。
    pub(crate) async fn is_wait_close_<K: TrCancellationToken>(&self, local_dock: Dock, remote_dock: Dock, now_millis: u64, cancel: K) -> Result<bool, ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let mut guard = acquire_write_(&mut session, cancel).await?;
        let inner = &mut *guard;
        inner.reap_wait_close_(now_millis);
        Result::Ok(matches!(
            inner.bindings_.get(&(local_dock, remote_dock)),
            Option::Some(BindingSlot_::WaitClose(_))
        ))
    }

    /// 取一条子流的共享状态句柄（克隆）；不存在时返回 `None`。
    ///
    /// 身份记录里始终有一份状态句柄——它随身份在 `reserve_channel_` 建立、随身份记录
    /// 消亡，因此不存在「登记了身份但状态还没挂上」的中间态。
    pub(crate) async fn channel_owner_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
        cancel: K,
    ) -> Result<Option<ChannelOwner_<A>>, ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let guard = acquire_read_(&mut session, cancel).await?;
        let inner = &*guard;
        Result::Ok(match inner.bindings_.get(&(local_dock, remote_dock)) {
            Option::Some(BindingSlot_::Channel(ctx)) => Option::Some(ctx.rec_.clone()),
            _ => Option::None,
        })
    }

    /// 被动方收到 `OPEN`：登记 channel 并把它标为「待决入向请求」，随后唤醒该 dock
    /// 上的监听者。返回这条子流的共享状态句柄。
    ///
    /// # Errors
    ///
    /// 与 [`ChannelRegistry_::reserve_channel_`] 相同（含 dock 对已存在时的
    /// [`ReserveErr_::Duplicate`] / [`ReserveErr_::WaitClose`]）。
    pub(crate) async fn reserve_inbound_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
        peer_report: WindowReport,
        now_millis: u64,
        cancel: K,
    ) -> Result<ChannelOwner_<A>, ReserveErr_> {
        // 先登记身份（自带配额与重复检查），再标成「待决入向请求」。
        let state = self
            .reserve_channel_(local_dock, remote_dock, now_millis, cancel.child_token())
            .await?;
        let marked = {
            let mut session = self.inner_.acquire_session();
            let mut guard = acquire_write_(&mut session, cancel.child_token()).await?;
            match guard.bindings_.get_mut(&(local_dock, remote_dock)) {
                Option::Some(BindingSlot_::Channel(ctx)) => {
                    ctx.inbound_ = Inbound_::Pending(peer_report);
                    true
                }
                _ => false,
            }
        };
        if marked {
            self.notify_inbound_(local_dock, cancel.child_token()).await?;
        }
        Result::Ok(state)
    }

    /// 取走 `local_dock` 上最早的一个待决入向请求（改成 `HandedOut`），返回对端 dock
    /// 与这条子流的共享状态句柄。
    ///
    /// 返回 `None` 表示当前没有待决请求（调用方应当先登记 waker 再重试）。
    ///
    /// 「最早」按 `remote_dock` 升序——这是 `bindings_` 的键序，与旧链表「链头最新」
    /// 的取出顺序不同；协议不要求入向请求按到达顺序配对（每条请求各自独立），
    /// 但**同一 dock 上的请求本就串行化**（`income_async` 取一个、处理完再取下一个），
    /// 因此顺序变化不影响语义。
    pub(crate) async fn take_pending_inbound_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        cancel: K,
    ) -> Result<Option<(Dock, ChannelOwner_<A>)>, ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let mut guard = acquire_write_(&mut session, cancel).await?;
        let inner = &mut *guard;
        let (start, end) = channel_range_(local_dock);
        let mut found = Option::None;
        for (key, binding) in inner.bindings_.range_mut((start, end)) {
            if let BindingSlot_::Channel(ctx) = binding
                && let Inbound_::Pending(report) = ctx.inbound_
            {
                ctx.inbound_ = Inbound_::HandedOut(report);
                found = Option::Some((key.1, ctx.rec_.clone()));
                break;
            }
        }
        Result::Ok(found)
    }

    /// `accept_async` 取走该入向请求保存的对端窗口通告（状态归为 `None`）。
    pub(crate) async fn take_inbound_report_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        remote_dock: Dock,
        cancel: K,
    ) -> Result<Option<WindowReport>, ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let mut guard = acquire_write_(&mut session, cancel).await?;
        let inner = &mut *guard;
        let Option::Some(BindingSlot_::Channel(ctx)) =
            inner.bindings_.get_mut(&(local_dock, remote_dock))
        else {
            return Result::Ok(Option::None);
        };
        Result::Ok(match core::mem::replace(&mut ctx.inbound_, Inbound_::None) {
            Inbound_::HandedOut(report) | Inbound_::Pending(report) => Option::Some(report),
            Inbound_::None => Option::None,
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
    /// 任何子流。
    ///
    /// # Errors
    ///
    /// 该 dock 已被另一个绑定占用 → [`ReserveErr_::DockInUse`]。
    ///
    /// [`TrConnection::bind_async`]: abs_smux::conn::TrConnection::bind_async
    /// [`DockBinding`]: crate::connection::DockBinding
    pub(crate) async fn bind_dock_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        cancel: K,
    ) -> Result<(), ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let mut guard = acquire_write_(&mut session, cancel).await?;
        let inner = &mut *guard;
        let dock = inner
            .docks_
            .entry(local_dock)
            .or_insert_with(DockCtx_::new_);
        if dock.bound_ {
            return Result::Err(ReserveErr_::DockInUse);
        }
        dock.bound_ = true;
        Result::Ok(())
    }

    /// 解除 [`ChannelRegistry_::bind_dock_`] 的独占绑定；不存在时是空操作。
    ///
    /// 由 `DockBinding` 的 `Drop` 调用。解绑只清 `bound_`；条目回收交给
    /// `prune_dock_` 判空，因此「解绑后该 dock 上还有活动子流」不会影响这些子流。
    pub(crate) async fn unbind_dock_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        cancel: K,
    ) -> Result<(), ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let mut guard = acquire_write_(&mut session, cancel).await?;
        let inner = &mut *guard;
        if let Option::Some(dock) = inner.docks_.get_mut(&local_dock) {
            dock.bound_ = false;
        }
        inner.prune_dock_(local_dock);
        Result::Ok(())
    }

    /// 在 `local_dock` 上登记**监听器身份**（`TrDockBinding::listen_async` 的登记点）。
    ///
    /// 同一 dock 上重复登记是**幂等**的（同一个 `DockBinding` 的 `&mut self` 借用
    /// 保证同时至多一个监听器）。
    ///
    /// # Errors
    ///
    /// 该 dock 已作 telegraph（datagram 与 channel 不得共用 dock）→
    /// [`ReserveErr_::DockInUse`]。
    pub(crate) async fn reserve_listener_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        cancel: K,
    ) -> Result<LsnOwner_<A>, ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let mut guard = acquire_write_(&mut session, cancel).await?;
        let inner = &mut *guard;
        if matches!(
            inner.bindings_.get(&telegraph_key_(local_dock)),
            Option::Some(BindingSlot_::Telegraph(_))
        ) {
            return Result::Err(ReserveErr_::DockInUse);
        }
        match inner.bindings_.get(&listener_key_(local_dock)) {
            // 已经在监听：交出同一份句柄（幂等，与旧实现的 `Ok(())` 等价）。
            Option::Some(BindingSlot_::Listener(rec)) => Result::Ok(rec.clone()),
            Option::Some(_) => Result::Err(ReserveErr_::DockInUse),
            Option::None => {
                // 身份节点由调用方注入的分配器建立；它内部零堆分配（入向通知是内联槽）。
                let rec = new_listener_owner_(inner.alloc_.clone());
                inner
                    .bindings_
                    .insert(listener_key_(local_dock), BindingSlot_::Listener(rec.clone()));
                Result::Ok(rec)
            }
        }
    }

    /// 解除 [`ChannelRegistry_::reserve_listener_`] 的登记；不存在时是空操作。
    ///
    /// 由 `ChannelListener` 的 `Drop` 调用。
    pub(crate) async fn release_listener_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        cancel: K,
    ) -> Result<(), ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let mut guard = acquire_write_(&mut session, cancel).await?;
        let inner = &mut *guard;
        inner.bindings_.remove(&listener_key_(local_dock));
        Result::Ok(())
    }

    /// 在 `local_dock` 上登记**telegraph 端点身份**（`open_telegraph_async` 的登记点）。
    ///
    /// telegraph **独占**该 `local_dock`：不允许已有 telegraph、listener 或任何
    /// 具体子流（见 [`abs_smux::conn::TrTelegraph`] 的文档约束）。
    ///
    /// # Errors
    ///
    /// 该 dock 上已有其它身份 → [`ReserveErr_::DockInUse`]。
    pub(crate) async fn reserve_telegraph_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        cancel: K,
    ) -> Result<TgOwner_<A>, ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let mut guard = acquire_write_(&mut session, cancel).await?;
        let inner = &mut *guard;
        if inner.bindings_.contains_key(&telegraph_key_(local_dock))
            || inner.bindings_.contains_key(&listener_key_(local_dock))
            || inner.has_concrete_identity_(local_dock)
        {
            return Result::Err(ReserveErr_::DockInUse);
        }
        let rec = new_telegraph_owner_(inner.alloc_.clone());
        inner
            .bindings_
            .insert(telegraph_key_(local_dock), BindingSlot_::Telegraph(rec.clone()));
        Result::Ok(rec)
    }

    /// 解除 [`ChannelRegistry_::reserve_telegraph_`] 的登记；不存在时是空操作。
    ///
    /// 由 `Telegraph` 的 `Drop` 调用。
    pub(crate) async fn release_telegraph_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        cancel: K,
    ) -> Result<(), ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let mut guard = acquire_write_(&mut session, cancel).await?;
        let inner = &mut *guard;
        inner.bindings_.remove(&telegraph_key_(local_dock));
        Result::Ok(())
    }

    /// 枚举当前**仍在册**的 telegraph 端点 `local_dock`，逐个回调（**非阻塞**）。
    ///
    /// 解复用循环用它做一次自查：本地表里那些 telegraph 端点，哪些已经不在身份表里了
    /// （tx / rx 两个半边都被丢弃 ⇒ 身份已释放）——那些端点的接收环写端必须关掉，否则
    /// 应用侧正 park 的 `recv_async` 永远醒不过来。
    ///
    /// # 为什么是「非阻塞 + 回调」
    ///
    /// - **非阻塞**：用 `try_read` 快路径，拿不到锁就返回 `false` 让调用方下一轮再来。
    ///   这条路径在**每一次**会话释放消息之后都会试一次，绝不能为它阻塞中心循环；
    /// - **回调**：既不引入堆分配（不建临时集合）、也不让读守卫逃出锁外——因此它不需要
    ///   调用方提供分配器，也就绕开了「集合必须带自定义分配器」那类类型麻烦。
    ///
    /// 返回 `false` 表示**这次没拿到锁**（结果不可用）；`true` 表示回调已按当前快照跑完。
    pub(crate) fn for_each_live_telegraph_(&self, mut f: impl FnMut(Dock)) -> bool {
        let mut session = self.inner_.acquire_session();
        let Result::Ok(guard) = session.try_read() else {
            return false;
        };
        let inner = &*guard;
        for (local_dock, remote_dock) in inner.bindings_.keys() {
            if *remote_dock == Dock::unspecified() {
                f(*local_dock);
            }
        }
        true
    }

    /// 与 `remote_dock` 建立过**活跃子流**的全部 `local_dock`（按 `local` 升序逐个回调）。
    ///
    /// 这是反向索引 `remote_index_` 的读取入口：把「只知对端 dock」的查询做成一次
    /// O(log n + k) 的区间枚举。用回调而不是返回集合，是为了既不引入堆分配、也不让
    /// 索引守卫逃出锁外。宽限态不入索引，因此这里只反映活跃子流。
    // 仅供单元测试与后续「只知 remote_dock」的查询（telegraph 路由 / 排查）使用。
    #[allow(dead_code)]
    pub(crate) async fn for_each_local_of_remote_<K: TrCancellationToken>(
        &self,
        remote_dock: Dock,
        mut f: impl FnMut(Dock),
        cancel: K,
    ) -> Result<(), ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let guard = acquire_read_(&mut session, cancel).await?;
        let inner = &*guard;
        let (start, end) = remote_range_(remote_dock);
        for (_, local) in inner.remote_index_.range((start, end)) {
            f(*local);
        }
        Result::Ok(())
    }

    /// 唤醒 `local_dock` 上的入向等待者（若有）；唤醒在释放锁之后进行。
    pub(crate) async fn notify_inbound_<K: TrCancellationToken>(
        &self,
        local_dock: Dock,
        cancel: K,
    ) -> Result<(), ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let mut guard = acquire_write_(&mut session, cancel).await?;
        if let Option::Some(BindingSlot_::Listener(rec)) =
            guard.bindings_.get_mut(&listener_key_(local_dock))
        {
            rec.notify_();
        }
        Result::Ok(())
    }

    /// 记下连接级失败（**首个**原因生效），唤醒两个循环的取消令牌，并唤醒所有
    /// 等待中的 API 面 future（监听者的 `income_async` 与建流的
    /// `open_channel_async`），避免它们空等一个已经死掉的循环。
    pub(crate) async fn mark_failed_<K: TrCancellationToken>(
        &self,
        err: &MuxError,
        cancel: K,
    ) -> Result<(), ReserveErr_> {
        // 子流状态是**无锁**的原子字，因此通知建流等待者不需要离开注册表锁：直接在
        // 同一次遍历里投持久通知即可（旧实现要先把 owner 收集出来、再逐个取子流锁）。
        {
            let mut session = self.inner_.acquire_session();
            let mut guard = acquire_write_(&mut session, cancel.child_token()).await?;
            if guard.fail_.is_none() {
                guard.fail_ = Option::Some(*err);
                // 锁外的关闭原因快照：与 `fail_` 同一判据，只记**首个**失败。
                self.fail_kind_.store(fail_kind_of_(err), Ordering::Release);
            }
            // 逐条发布通知时用的**类别**：读锁外快照而不是当前这次调用的 `err`——
            // 第二次调用 `mark_failed_` 时，保留的仍然是**第一个**失败（与 `fail_` 一致）。
            let fail_kind = decode_fail_kind_(self.fail_kind_.load(Ordering::Acquire));
            for binding in guard.bindings_.values() {
                match binding {
                    BindingSlot_::Listener(rec) => {
                        // 连接失败也要唤醒监听者（否则它会一直等入向）。
                        rec.notify_();
                    }
                    BindingSlot_::Channel(ctx) => {
                        // 1. **先**发布「连接级失败牵连」这条不可忽略通知，再唤醒等待者。
                        //    顺序不可反：应用被唤醒后的第一件事就是读原因（`abort_reason()`
                        //    与 `wait_establish_` 都直接读它），先唤醒会让它读到「还没有
                        //    通知」而把连接级失败误判成普通半关闭。
                        //    这里是**逐条遍历写 N 个槽位**，代价与上面那次逐条叫醒同阶：
                        //    连接级失败是终结事件，一次 O(N) 完全可以接受；子流级原因
                        //    （空闲超时）本来就只写一条，没有遍历。
                        if let Option::Some(kind) = fail_kind {
                            ctx.rec_.publish_notice_(ChannelNotice::ConnFailed(kind));
                        }
                        ctx.rec_.notify_establish_();
                    }
                    _ => {}
                }
            }
        }
        self.cancel_loops_();
        Result::Ok(())
    }

    /// 连接级失败的种类（**锁外**同步读）。
    ///
    /// `None` 表示从未发生连接级失败，即「正常收尾」。两个调用方都要求它在锁外可读：
    /// [`MuxCore::drop`](super::core_::MuxCore)（那条路径按设计不取锁）与
    /// [`ChannelRegistry_::mark_failed_`] 的通知发布（它虽然持写锁，但读的是**首个**
    /// 失败的快照，而不是本次调用的参数）。见 [`fail_kind_of_`] 与
    /// [`ChannelRegistry_::fail_kind_`]。
    pub(crate) fn fail_kind_(&self) -> Option<ConnCloseReason> {
        decode_fail_kind_(self.fail_kind_.load(Ordering::Acquire))
    }

    /// 连接级失败的原因（若有）。
    pub(crate) async fn failure_<K: TrCancellationToken>(
        &self,
        cancel: K,
    ) -> Result<Option<MuxError>, ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let guard = acquire_read_(&mut session, cancel).await?;
        Result::Ok(guard.fail_)
    }

    /// 连接是否已经失败。
    #[allow(dead_code)]
    pub(crate) async fn is_failed_<K: TrCancellationToken>(
        &self,
        cancel: K,
    ) -> Result<bool, ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let guard = acquire_read_(&mut session, cancel).await?;
        Result::Ok(guard.fail_.is_some())
    }

    /// **计时循环的扫描入口**：按 `bindings_` 的键序走一遍活跃子流，为每条认领一个
    /// 保活动作，并算出下一次必须醒来的时刻。
    ///
    /// # 为什么整轮只取一次读锁、动作走调用方给的切片
    ///
    /// - 判定与认领（`claim_pulse_` / `claim_abort_`）全是**原子读改写**，因此整轮
    ///   可以只取一次读锁；锁内**不做任何 `await`、不投递事件、不唤醒等待者**；
    /// - 事件投递必须留到锁外（唤醒纪律，见 [`ChannelRegistry_::notify_timer_`]），
    ///   所以动作先写进调用方的切片、由调用方在锁释放后投递；
    /// - 切片的长度即「每轮最多投多少条事件」的**限频**：写满即提前返回并把
    ///   [`TimerScan_::is_full_`] 置真，调用方立刻再扫一轮即可，动作不会丢。
    ///
    /// # 参数
    ///
    /// - `now_millis`：连接内毫秒（自连接 epoch 起算）；
    /// - `pulse_millis`：发 `PULSE` 的空闲阈值；
    /// - `timeout_millis`：判空闲超时的空闲上限（**不早于此值**）；
    /// - `actions`：本轮动作的输出缓冲（长度即限频）。
    ///
    /// # Errors
    ///
    /// 等锁期间被取消 → [`ReserveErr_::Cancelled`]（连接正在收尾）。
    pub(crate) async fn timer_scan_<K: TrCancellationToken>(
        &self,
        now_millis: u64,
        pulse_millis: u64,
        timeout_millis: u64,
        actions: &mut [TimerAction_],
        cancel: K,
    ) -> Result<TimerScan_, ReserveErr_> {
        let mut session = self.inner_.acquire_session();
        let guard = acquire_read_(&mut session, cancel).await?;
        let inner = &*guard;

        let mut count = 0usize;
        let mut full = false;
        let mut next = u64::MAX;

        for (key, slot) in inner.bindings_.iter() {
            // 只有活跃 channel 参与保活：listener / telegraph / 宽限态都不参加。
            let BindingSlot_::Channel(ctx) = slot else {
                continue;
            };
            let owner = &ctx.rec_;
            // 已中止（正在拆）的子流不再排任何期限。
            if owner.is_aborted_() {
                continue;
            }
            // 两个时钟都由活动点**就地**写下（`touch_` / `mark_data_`），这里只读：
            // 存活时钟是 `max_channel_timeout` 的判据，保活职责时钟是 `PULSE` 的判据。
            let last = owner.active_millis_();
            let last_data = owner.data_millis_();

            // 1. 存活时钟到顶：认领中止（只可能成功一次）。
            //
            //    **不早于** `timeout_millis`：判据是 `now - last >= timeout`，
            //    因此只可能在该期限之后的第一轮扫描里触发。
            if now_millis.saturating_sub(last) >= timeout_millis {
                if count == actions.len() {
                    full = true;
                    break;
                }
                // 建流**尚未裁决**：这是「建流超时」。它与活跃子流的空闲超时共用
                // 同一根存活时钟与同一个 `max_channel_timeout`，但处置完全不同——
                // 对端此刻正等裁决，必须回 `REJECT` 把它放走，而不是给一条还没有
                // 数据的子流发 `FIN` / `RESET`。
                if !owner.establish_settled_() {
                    if owner.claim_abort_(AbortCode_::IdleTimeout) {
                        actions[count] = TimerAction_::RejectEstablish {
                            local_dock: key.0,
                            remote_dock: key.1,
                        };
                        count += 1;
                    }
                    continue;
                }
                if owner.claim_abort_(AbortCode_::IdleTimeout) {
                    actions[count] = TimerAction_::Abort {
                        local_dock: key.0,
                        remote_dock: key.1,
                    };
                    count += 1;
                }
                continue;
            }

            // 2. 保活职责：距上一次**非保活活动**（或上一次 `PULSE`）满一个保活周期
            //    就发一条 `PULSE`，并把它记成「刚发过」。它与存活判定**无关**：
            //    收到对端的 `PULSE` 不会让本端少发一条（见 `ChannelState_`）。
            let mut duty = last_data.saturating_add(pulse_millis);
            if now_millis >= duty {
                if count == actions.len() {
                    full = true;
                    break;
                }
                actions[count] = TimerAction_::Pulse {
                    local_dock: key.0,
                    remote_dock: key.1,
                    report: owner.flow_().recv_window().report(),
                };
                count += 1;
                owner.set_data_millis_(now_millis);
                duty = now_millis.saturating_add(pulse_millis);
            }

            // 3. 下一个期限 = min(存活到顶, 下一次保活职责)。
            next = next.min(last.saturating_add(timeout_millis));
            next = next.min(duty);
        }

        Result::Ok(TimerScan_ {
            actions_: count,
            full_: full,
            next_millis_: next,
        })
    }

    /// 取第 `idx` 个循环的取消令牌（`0` = 读循环，`1` = 写循环）。
    ///
    /// 令牌在锁外，因此本方法**不取锁**（构造路径与 `Drop` 路径都依赖这一点）。
    pub(crate) fn loop_token_(&self, idx: usize) -> CancelToken_<A> {
        self.loops_[idx].clone()
    }

    /// 触发四个循环的取消令牌（连接关闭或失败时调用）。
    ///
    /// 令牌在锁外，因此本方法**不取锁**。
    pub(crate) fn cancel_loops_(&self) {
        for token in &self.loops_ {
            token.cancel_();
        }
    }

}

#[cfg(test)]
mod tests_ {
    use std::{collections::BTreeMap, time::Duration};

    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use crate::{
        connection::{Dock, ReserveErr_, owner_::ChannelOwner_},
        handshake::opts::BasicOpts,
    };

    use super::*;

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 测试用的异步扩展：把注册表的异步 API 收成短名
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    // 生产代码一律走**异步可取消**的取锁；用例本身也是 `async fn`（在两个真实运行时
    // 下各跑一遍），因此这里**不再用 `block_on` 把异步压成同步**，只把正式 API 的
    // 三参数形状（local/remote/cancel）收成便于断言的短名。

    use buffex::x_deps::abs_cancel::NonCancellableToken;

    /// 注册表的测试用异步快捷方法。
    trait RegistryTestExt_ {
        fn reserve_channel_t_(
            &self,
            local_dock: Dock,
            remote_dock: Dock,
        ) -> impl core::future::Future<Output = Result<ChannelOwner_<CoreAlloc>, ReserveErr_>>;
        fn release_channel_t_(
            &self,
            local_dock: Dock,
            remote_dock: Dock,
        ) -> impl core::future::Future<Output = ()>;
        fn reserve_listener_t_(
            &self,
            local_dock: Dock,
        ) -> impl core::future::Future<Output = Result<LsnOwner_<CoreAlloc>, ReserveErr_>>;
        fn reserve_listener_with_t_(
            &self,
            local_dock: Dock,
        ) -> impl core::future::Future<Output = Result<LsnOwner_<CoreAlloc>, ReserveErr_>>;
        fn reserve_telegraph_t_(
            &self,
            local_dock: Dock,
        ) -> impl core::future::Future<Output = Result<TgOwner_<CoreAlloc>, ReserveErr_>>;
        fn release_telegraph_t_(
            &self,
            local_dock: Dock,
        ) -> impl core::future::Future<Output = ()>;
        fn bind_dock_t_(
            &self,
            local_dock: Dock,
        ) -> impl core::future::Future<Output = Result<(), ReserveErr_>>;
        fn unbind_dock_t_(&self, local_dock: Dock) -> impl core::future::Future<Output = ()>;
        fn is_wait_close_t_(
            &self,
            local_dock: Dock,
            remote_dock: Dock,
        ) -> impl core::future::Future<Output = bool>;
        fn channel_owner_t_(
            &self,
            local_dock: Dock,
            remote_dock: Dock,
        ) -> impl core::future::Future<Output = Option<ChannelOwner_<CoreAlloc>>>;
        fn reserve_inbound_t_(
            &self,
            local_dock: Dock,
            remote_dock: Dock,
            report: WindowReport,
        ) -> impl core::future::Future<Output = Result<ChannelOwner_<CoreAlloc>, ReserveErr_>>;
        fn take_pending_inbound_t_(
            &self,
            local_dock: Dock,
        ) -> impl core::future::Future<Output = Option<(Dock, ChannelOwner_<CoreAlloc>)>>;
        fn take_inbound_report_t_(
            &self,
            local_dock: Dock,
            remote_dock: Dock,
        ) -> impl core::future::Future<Output = Option<WindowReport>>;
        fn max_channel_count_t_(&self) -> impl core::future::Future<Output = usize>;
        fn max_dock_chan_count_t_(&self) -> impl core::future::Future<Output = usize>;
        fn total_channels_t_(&self) -> impl core::future::Future<Output = usize>;
        fn mark_failed_t_(&self, err: &MuxError) -> impl core::future::Future<Output = ()>;
        fn failure_t_(&self) -> impl core::future::Future<Output = Option<MuxError>>;
        fn is_failed_t_(&self) -> impl core::future::Future<Output = bool>;
        fn for_each_local_of_remote_t_(
            &self,
            remote_dock: Dock,
            f: impl FnMut(Dock),
        ) -> impl core::future::Future<Output = ()>;
        fn notify_inbound_t_(&self, local_dock: Dock) -> impl core::future::Future<Output = ()>;
        fn timer_scan_t_(
            &self,
            now_millis: u64,
            pulse_millis: u64,
            timeout_millis: u64,
            actions: &mut [TimerAction_],
        ) -> impl core::future::Future<Output = TimerScan_>;
    }

    impl RegistryTestExt_ for ChannelRegistry_<CoreAlloc> {
    async fn reserve_channel_t_(
            &self,
            local_dock: Dock,
            remote_dock: Dock,
        ) -> Result<ChannelOwner_<CoreAlloc>, ReserveErr_> {
            self.reserve_channel_(local_dock, remote_dock, TEST_NOW_MILLIS_, NonCancellableToken::new())
                .await
        }

    async fn release_channel_t_(&self, local_dock: Dock, remote_dock: Dock) {
            let _ = self
                .release_channel_(local_dock, remote_dock, TEST_NOW_MILLIS_, NonCancellableToken::new())
                .await;
        }

    async fn reserve_listener_t_(&self, local_dock: Dock) -> Result<LsnOwner_<CoreAlloc>, ReserveErr_> {
            self.reserve_listener_(local_dock, NonCancellableToken::new())
                .await
        }

    async fn reserve_listener_with_t_(
            &self,
            local_dock: Dock,
        ) -> Result<LsnOwner_<CoreAlloc>, ReserveErr_> {
            self.reserve_listener_(local_dock, NonCancellableToken::new())
                .await
        }

    async fn reserve_telegraph_t_(&self, local_dock: Dock) -> Result<TgOwner_<CoreAlloc>, ReserveErr_> {
            self.reserve_telegraph_(local_dock, NonCancellableToken::new())
                .await
        }

    async fn release_telegraph_t_(&self, local_dock: Dock) {
            let _ = self
                .release_telegraph_(local_dock, NonCancellableToken::new())
                .await;
        }

    async fn bind_dock_t_(&self, local_dock: Dock) -> Result<(), ReserveErr_> {
            self.bind_dock_(local_dock, NonCancellableToken::new()).await
        }

    async fn unbind_dock_t_(&self, local_dock: Dock) {
            let _ = self
                .unbind_dock_(local_dock, NonCancellableToken::new())
                .await;
        }

    async fn is_wait_close_t_(&self, local_dock: Dock, remote_dock: Dock) -> bool {
            self.is_wait_close_(local_dock, remote_dock, TEST_NOW_MILLIS_, NonCancellableToken::new())
                .await
                .expect("测试里不该被取消")
        }

    async fn channel_owner_t_(
            &self,
            local_dock: Dock,
            remote_dock: Dock,
        ) -> Option<ChannelOwner_<CoreAlloc>> {
            self.channel_owner_(local_dock, remote_dock, NonCancellableToken::new())
                .await
                .expect("测试里不该被取消")
        }

    async fn reserve_inbound_t_(
            &self,
            local_dock: Dock,
            remote_dock: Dock,
            report: WindowReport,
        ) -> Result<ChannelOwner_<CoreAlloc>, ReserveErr_> {
            self.reserve_inbound_(
                local_dock,
                remote_dock,
                report,
                TEST_NOW_MILLIS_,
                NonCancellableToken::new(),
            )
                .await
        }

    async fn take_pending_inbound_t_(
            &self,
            local_dock: Dock,
        ) -> Option<(Dock, ChannelOwner_<CoreAlloc>)> {
            self.take_pending_inbound_(local_dock, NonCancellableToken::new())
                .await
                .expect("测试里不该被取消")
        }

    async fn take_inbound_report_t_(
            &self,
            local_dock: Dock,
            remote_dock: Dock,
        ) -> Option<WindowReport> {
            self.take_inbound_report_(local_dock, remote_dock, NonCancellableToken::new())
                .await
                .expect("测试里不该被取消")
        }

    async fn max_channel_count_t_(&self) -> usize {
            self.max_channel_count_(NonCancellableToken::new())
                .await
                .expect("测试里不该被取消")
        }

    async fn max_dock_chan_count_t_(&self) -> usize {
            self.max_dock_chan_count_(NonCancellableToken::new())
                .await
                .expect("测试里不该被取消")
        }

    async fn total_channels_t_(&self) -> usize {
            self.total_channels_(NonCancellableToken::new())
                .await
                .expect("测试里不该被取消")
        }

    async fn mark_failed_t_(&self, err: &MuxError) {
            let _ = self.mark_failed_(err, NonCancellableToken::new()).await;
        }

    async fn failure_t_(&self) -> Option<MuxError> {
            self.failure_(NonCancellableToken::new())
                .await
                .expect("测试里不该被取消")
        }

    async fn is_failed_t_(&self) -> bool {
            self.is_failed_(NonCancellableToken::new())
                .await
                .expect("测试里不该被取消")
        }

    async fn for_each_local_of_remote_t_(&self, remote_dock: Dock, f: impl FnMut(Dock)) {
            let _ = self
                .for_each_local_of_remote_(remote_dock, f, NonCancellableToken::new())
                .await;
        }

    async fn timer_scan_t_(
            &self,
            now_millis: u64,
            pulse_millis: u64,
            timeout_millis: u64,
            actions: &mut [TimerAction_],
        ) -> TimerScan_ {
            self.timer_scan_(
                now_millis,
                pulse_millis,
                timeout_millis,
                actions,
                NonCancellableToken::new(),
            )
            .await
            .expect("测试里不该被取消")
        }

    async fn notify_inbound_t_(&self, local_dock: Dock) {
            let _ = self
                .notify_inbound_(local_dock, NonCancellableToken::new())
                .await;
        }
    }

    impl ChannelRegistry_<CoreAlloc> {
        /// 只读地检查是否待决（等价于旧 `has_pending_inbound_`；测试单线程，直接取快路径）。
        fn has_pending_inbound_t_(&self, local_dock: Dock) -> bool {
            let mut session = self.inner_.acquire_session();
            let guard = session.try_read().expect("测试里不该争用");
            let (start, end) = channel_range_(local_dock);
            guard.bindings_.range((start, end)).any(|(_, binding)| {
                matches!(
                    binding,
                    BindingSlot_::Channel(ctx) if matches!(ctx.inbound_, Inbound_::Pending(_))
                )
            })
        }

        /// 持读锁执行断言（等价于旧 `with_`；测试单线程，直接取快路径）。
        fn with_t_<R>(&self, f: impl FnOnce(&RegistryInner_<CoreAlloc>) -> R) -> R {
            let mut session = self.inner_.acquire_session();
            let guard = session.try_read().expect("测试里不该争用");
            f(&guard)
        }
    }

    /// 注册表单测里的「现在」（连接内毫秒）：固定为 0。
    ///
    /// 这些用例只在**同一时刻**内验证身份表语义（宽限期取 0 ⇒ 立刻到期；取 60 秒
    /// ⇒ 不过期），因此不需要推进时间——真正需要推进时间的验收在计时循环那侧用
    /// 假时钟完成。
    const TEST_NOW_MILLIS_: u64 = 0u64;

    /// 造一份「宽限期为 0」的协商结果，便于在不睡眠的前提下测到期回收。
    ///
    /// 协议规定 `max_channel_wait_close >= 1` 秒，这里刻意取 0 是为了让
    /// `until_ == now`，下一次访问即可回收（`until_ > now` 为假）；注册表本身不
    /// 校验该范围。
    fn opts_zero_grace_() -> BasicOpts {
        BasicOpts {
            max_channel_wait_close: Duration::ZERO,
            ..BasicOpts::default()
        }
    }

    /// 断言四本索引互相一致。
    ///
    /// - 手段：持读锁统计 `bindings_` 里的活跃 channel、宽限态、每个 local 的
    ///   channel 数，以及 `remote_index_` / `wait_close_expiry_` 的规模。
    /// - 判断：`total_` = 活跃 channel 数 = `remote_index_` 规模；每个 `DockCtx_`
    ///   的 `chan_count_` 等于该 local 的活跃 channel 数；每条 `WaitClose` 都能在
    ///   到期索引里找到，且二者规模相等；每条活跃 channel 都能在反向索引里找到。
    fn assert_index_consistent_(registry: &ChannelRegistry_<CoreAlloc>) {
        registry.with_t_(|inner| {
            let mut active = 0usize;
            let mut wait_close = 0usize;
            let mut per_local: BTreeMap<Dock, usize> = BTreeMap::new();
            for (key, binding) in inner.bindings_.iter() {
                match binding {
                    BindingSlot_::Channel(_) => {
                        active += 1usize;
                        *per_local.entry(key.0).or_insert(0usize) += 1usize;
                    }
                    BindingSlot_::WaitClose(_) => {
                        wait_close += 1usize;
                        assert!(
                            inner
                                .wait_close_expiry_
                                .iter()
                                .any(|(_, local, remote)| *local == key.0 && *remote == key.1),
                            "宽限态 {:?} 必须出现在到期索引里",
                            key
                        );
                    }
                    _ => {}
                }
            }
            assert_eq!(inner.total_, active, "total_ 必须等于活跃 channel 数");
            assert_eq!(
                inner.remote_index_.len(),
                active,
                "反向索引只登记活跃 channel，规模必须相等"
            );
            assert_eq!(
                inner.wait_close_expiry_.len(),
                wait_close,
                "到期索引与宽限态必须一一对应"
            );
            for (dock, ctx) in inner.docks_.iter() {
                let actual = per_local.get(dock).copied().unwrap_or(0usize);
                assert_eq!(
                    ctx.chan_count_, actual,
                    "dock {dock:?} 的 chan_count_ 必须等于活跃 channel 数"
                );
            }
            for (key, binding) in inner.bindings_.iter() {
                if let BindingSlot_::Channel(_) = binding {
                    assert!(
                        inner.remote_index_.contains(&(key.1, key.0)),
                        "反向索引缺少 ({:?}, {:?})",
                        key.1,
                        key.0
                    );
                }
            }
        });
    }

    /// 测试 dock 级与连接级配额各自生效、释放后可重新登记。
    /// - 手段：协商值收紧为「整条连接 2 条、单 dock 1 条」；依次登记
    ///   `(1,9)`、`(1,10)`、`(2,9)`、`(3,9)`，再释放 `(1,9)` 后重新登记。
    /// - 判断：同一 dock 上的第二条报 `DockChanLimit`；连接上的第三条报
    ///   `ChanLimit`；释放后总数回落。
        async fn reserve_enforces_dock_and_connection_limits() {
        let opts = BasicOpts {
            max_channel_count: 2usize,
            max_dock_chan_count: 1usize,
            ..opts_zero_grace_()
        };
        let registry = ChannelRegistry_::new_(opts, CoreAlloc);
        assert_eq!(registry.max_channel_count_t_()
            .await, 2usize);
        assert_eq!(registry.max_dock_chan_count_t_()
            .await, 1usize);

        assert!(registry.reserve_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await.is_ok());
        assert_eq!(registry.total_channels_t_()
            .await, 1usize);

        let err = registry
            .reserve_channel_t_(Dock::new(1u32), Dock::new(10u32))
            .await
            .unwrap_err();
        assert!(matches!(err, ReserveErr_::DockChanLimit));
        assert!(registry.reserve_channel_t_(Dock::new(2u32), Dock::new(9u32))
            .await.is_ok());
        let err = registry
            .reserve_channel_t_(Dock::new(3u32), Dock::new(9u32))
            .await
            .unwrap_err();
        assert!(matches!(err, ReserveErr_::ChanLimit));

        registry.release_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await;
        assert_eq!(registry.total_channels_t_()
            .await, 1usize, "释放后活跃数应当回落");
        // 宽限期为 0，下一次访问即回收，因此同一 dock 可以重新登记。
        assert!(
            registry.reserve_channel_t_(Dock::new(1u32), Dock::new(11u32))
            .await.is_ok(),
            "释放配额后同一 dock 应当可以重新登记"
        );
        assert_eq!(registry.total_channels_t_()
            .await, 2usize);
        assert_index_consistent_(&registry);
    }
    dual_runtime_test_!(reserve_enforces_dock_and_connection_limits);
    /// 测试 telegraph 与 channel / listener 在同一个 local_dock 上互斥。
    /// - 手段：先在 dock 5 上登记 telegraph，再尝试登记 channel 与 listener；
    ///   换 dock 6 先登记 channel 再尝试 telegraph；换 dock 7 先登记 listener
    ///   再尝试 telegraph。
    /// - 判断：三次相斥的尝试都报 `ReserveErr_::DockInUse`；释放 telegraph 后
    ///   dock 5 可以登记 channel。
        async fn telegraph_is_exclusive_on_a_dock() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);

        assert!(registry.reserve_telegraph_t_(Dock::new(5u32))
            .await.is_ok());
        assert!(
            matches!(
                registry
                    .reserve_channel_t_(Dock::new(5u32), Dock::new(9u32))
            .await
                    .unwrap_err(),
                ReserveErr_::DockInUse
            ),
            "telegraph 占用的 dock 不能建 channel"
        );
        assert!(
            matches!(
                registry.reserve_listener_t_(Dock::new(5u32))
            .await.unwrap_err(),
                ReserveErr_::DockInUse
            ),
            "telegraph 占用的 dock 不能监听"
        );

        assert!(registry.reserve_channel_t_(Dock::new(6u32), Dock::new(9u32))
            .await.is_ok());
        assert!(
            matches!(
                registry.reserve_telegraph_t_(Dock::new(6u32))
            .await.unwrap_err(),
                ReserveErr_::DockInUse
            ),
            "已有 channel 的 dock 不能开 telegraph"
        );

        assert!(registry.reserve_listener_t_(Dock::new(7u32))
            .await.is_ok());
        assert!(
            matches!(
                registry.reserve_telegraph_t_(Dock::new(7u32))
            .await.unwrap_err(),
                ReserveErr_::DockInUse
            ),
            "已监听的 dock 不能开 telegraph"
        );

        registry.release_telegraph_t_(Dock::new(5u32))
            .await;
        assert!(
            registry.reserve_channel_t_(Dock::new(5u32), Dock::new(9u32))
            .await.is_ok(),
            "释放 telegraph 后该 dock 应当可用"
        );
        assert_index_consistent_(&registry);
    }
    dual_runtime_test_!(telegraph_is_exclusive_on_a_dock);
    /// 测试三种身份可以共用一张表且各自占据正确的键。
    /// - 手段：在同一 dock 与不同 dock 上分别登记 listener、telegraph、channel，
    ///   然后直接检查 `bindings_` 的键与变体。
    /// - 判断：listener 落在 `(local, wildcard)`、telegraph 落在
    ///   `(local, unspecified)`、channel 落在 `(local, 具体值)`；listener 与
    ///   channel 可以共存于同一个 local_dock。
        async fn bindings_express_three_kinds_on_one_table() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);
        assert!(registry.reserve_listener_t_(Dock::new(1u32))
            .await.is_ok());
        assert!(registry.reserve_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await.is_ok());
        assert!(registry.reserve_telegraph_t_(Dock::new(2u32))
            .await.is_ok());

        registry.with_t_(|inner| {
            assert!(matches!(
                inner.bindings_.get(&(Dock::new(1u32), Dock::wildcard())),
                Option::Some(BindingSlot_::Listener(_))
            ));
            assert!(matches!(
                inner.bindings_.get(&(Dock::new(1u32), Dock::new(9u32))),
                Option::Some(BindingSlot_::Channel(_))
            ));
            assert!(matches!(
                inner.bindings_.get(&(Dock::new(2u32), Dock::unspecified())),
                Option::Some(BindingSlot_::Telegraph(_))
            ));
            // listener 与 channel 共存，说明二者是不同键。
            assert_eq!(inner.bindings_.len(), 3usize);
        });
        assert_index_consistent_(&registry);
    }
    dual_runtime_test_!(bindings_express_three_kinds_on_one_table);
    /// 测试 dock 绑定是**独占**且**持久**的：同一 `local_dock` 第二次绑定必须报错，
    /// 解绑后可以重绑，且绑定状态不因该 dock 上子流清零而丢失。
    ///
    /// - 手段：对同一 dock 连续 `bind_dock_`；在另一个 dock 上正常绑定；再在一个
    ///   已绑定的 dock 上登记并释放一条子流；最后 `unbind_dock_` 后重绑。
    /// - 判断：第二次绑定报 `ReserveErr_::DockInUse`；不同 dock 互不影响；子流清零
    ///   后再次绑定**仍**报 `DockInUse`（绑定是持久占用）；解绑后重绑成功。
        async fn dock_binding_is_exclusive_and_persistent() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);

        // 首次绑定成功；同一 dock 第二次绑定必须失败。
        assert!(registry.bind_dock_t_(Dock::new(7u32))
            .await.is_ok());
        let err = registry.bind_dock_t_(Dock::new(7u32))
            .await.unwrap_err();
        assert!(
            matches!(err, ReserveErr_::DockInUse),
            "同一 dock 重复绑定应当报 DockInUse"
        );

        // 不同 dock 互不影响。
        assert!(registry.bind_dock_t_(Dock::new(8u32))
            .await.is_ok());

        // 绑定状态不因该 dock 上子流清零而丢失。
        assert!(
            registry
                .reserve_channel_t_(Dock::new(7u32), Dock::new(3u32))
            .await
                .is_ok()
        );
        registry.release_channel_t_(Dock::new(7u32), Dock::new(3u32))
            .await;
        let err = registry.bind_dock_t_(Dock::new(7u32))
            .await.unwrap_err();
        assert!(
            matches!(err, ReserveErr_::DockInUse),
            "子流清零不应解除绑定"
        );

        // 解绑后可重新绑定。
        registry.unbind_dock_t_(Dock::new(7u32))
            .await;
        assert!(
            registry.bind_dock_t_(Dock::new(7u32))
            .await.is_ok(),
            "解绑后应当可以重新绑定"
        );
    }
    dual_runtime_test_!(dock_binding_is_exclusive_and_persistent);
    /// 用一次「空 waker」poll 监听者的通知槽：返回它是否已就绪（顺带消费掉持久位）。
    ///
    /// 内联槽只有「登记 → 复检」两态，没有 `flume` 那样的 `try_recv`，因此测试里用
    /// 一次 `poll_wait_` 表达「现在有没有待处理的通知」。
    fn lsn_pending_(rec: &LsnOwner_<CoreAlloc>) -> bool {
        let mut cx = core::task::Context::from_waker(core::task::Waker::noop());
        rec.poll_wait_(&mut cx).is_ready()
    }

    /// 测试入向等待者只被唤醒一次，且唤醒发生在取出之后。
    /// - 手段：先在 dock 4 上登记 listener 身份，随后连续三次调用 `notify_inbound_`，
    ///   每次都用一次空 waker poll 通知槽。
    /// - 判断：第一次 poll 就绪（通知到达）；重复通知合并成一次，第二次 poll 不就绪
    ///   （持久位已被消费，不堆积）。
        async fn notify_inbound_wakes_registered_listener_once() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);
        let rx = registry
            .reserve_listener_with_t_(Dock::new(4u32))
            .await
            .expect("登记 listener 应当成功");

        registry.notify_inbound_t_(Dock::new(4u32))
            .await;
        assert!(lsn_pending_(&rx), "入向事件应当通知 listener");

        // 持久位：未被取走时重复通知合并成一次，至多留一条待处理通知。
        registry.notify_inbound_t_(Dock::new(4u32))
            .await;
        registry.notify_inbound_t_(Dock::new(4u32))
            .await;
        assert!(lsn_pending_(&rx), "仍能取到一条通知");
        assert!(!lsn_pending_(&rx), "持久位：不应堆积多条通知");
    }
    dual_runtime_test_!(notify_inbound_wakes_registered_listener_once);
    /// 测试同一 dock 对上的第二条并发子流被拒（dock 对即身份）。
    /// - 手段：在 `(1,9)` 上登记一次后重复登记；释放后再登记。
    /// - 判断：重复登记报 `ReserveErr_::Duplicate`；宽限期为 0 时释放后可重新登记。
        async fn duplicate_dock_pair_is_rejected() {
        let registry = ChannelRegistry_::new_(opts_zero_grace_(), CoreAlloc);
        assert!(registry.reserve_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await.is_ok());
        assert_eq!(
            registry.total_channels_t_()
            .await,
            1usize,
            "重复登记不应计入第二条"
        );

        let err = registry
            .reserve_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await
            .unwrap_err();
        assert!(matches!(err, ReserveErr_::Duplicate));
        assert_eq!(registry.total_channels_t_()
            .await, 1usize);

        registry.release_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await;
        assert!(
            registry.reserve_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await.is_ok(),
            "宽限期到期后同一 dock 对应当可以重新登记"
        );
        assert_index_consistent_(&registry);
    }
    dual_runtime_test_!(duplicate_dock_pair_is_rejected);
    /// 测试拆流后键进入宽限态：既保护复用，又为在途帧提供识别依据。
    /// - 手段：登记 `(1,9)` 后释放；查 `is_wait_close_`、尝试重新登记同一 dock 对、
    ///   并登记一个不同 remote 的兄弟子流。
    /// - 判断：释放后 `is_wait_close_` 为真且重新登记报 `ReserveErr_::WaitClose`；
    ///   不同 remote 不受影响（宽限是 dock 对级而非 dock 级）。
        async fn released_pair_enters_wait_close_and_blocks_reuse() {
        let opts = BasicOpts {
            max_channel_wait_close: Duration::from_secs(60u64),
            ..BasicOpts::default()
        };
        let registry = ChannelRegistry_::new_(opts, CoreAlloc);

        assert!(registry.reserve_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await.is_ok());
        registry.release_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await;

        assert!(
            registry.is_wait_close_t_(Dock::new(1u32), Dock::new(9u32))
            .await,
            "刚释放的 dock 对应当处于宽限态"
        );
        assert!(
            matches!(
                registry
                    .reserve_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await
                    .unwrap_err(),
                ReserveErr_::WaitClose
            ),
            "宽限期内不得复用同一 dock 对"
        );
        assert!(
            registry.reserve_channel_t_(Dock::new(1u32), Dock::new(10u32))
            .await.is_ok(),
            "宽限是 dock 对级：同一 local 上的其它 remote 不受影响"
        );
        assert!(
            !registry.is_wait_close_t_(Dock::new(1u32), Dock::new(10u32))
            .await,
            "活跃子流不是宽限态"
        );
        assert_index_consistent_(&registry);
    }
    dual_runtime_test_!(released_pair_enters_wait_close_and_blocks_reuse);
    /// 造一个用缺省协商结果的注册表（计时扫描的阈值由调用方显式给出，与它无关）。
    fn make_scan_registry_() -> ChannelRegistry_<CoreAlloc> {
        ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc)
    }

    /// 扫描一次计时循环，返回结果（封装 `timer_scan_t_` 的样板）。
    async fn scan_(
        registry: &ChannelRegistry_<CoreAlloc>,
        now_millis: u64,
        pulse_millis: u64,
        timeout_millis: u64,
    ) -> (usize, u64, bool, [TimerAction_; 4usize]) {
        let mut actions = [TimerAction_::Idle; 4usize];
        let scan = registry
            .timer_scan_t_(now_millis, pulse_millis, timeout_millis, &mut actions)
            .await;
        (scan.actions_(), scan.next_millis_(), scan.is_full_(), actions)
    }

    /// 测试计时扫描按「保活阈值 → 存活到顶」两级推进，且每级只产出一次动作。
    ///
    /// 本条验的是**已经裁决完毕**的子流：到点走「拆流」（`Abort`）。未裁决的建流到点
    /// 走的是另一条路（`RejectEstablish`），由下一个用例单独钉住。
    ///
    /// - 手段：登记一条子流（登记即打点 0 ms）并把它标成「建流已裁决」，以
    ///   `pulse = 500`、`timeout = 1000` 依次在 `now = 0 / 499 / 500 / 999 / 1000 /
    ///   5000` 扫描，每次记录动作条数与下一次期限。
    /// - 判断：`0` 与 `499` 无动作、期限指向 500；`500` 恰好一条 `Pulse`、期限落到
    ///   1000；`999` 无动作；`1000` 恰好一条 `Abort`；已中止之后不再参与扫描
    ///   （无动作、期限为 `u64::MAX`）。
    async fn timer_scan_walks_pulse_then_abort() {
        let registry = make_scan_registry_();
        let dock_a = Dock::new(1u32);
        let dock_b = Dock::new(9u32);
        assert!(
            registry.reserve_channel_t_(dock_a, dock_b).await.is_ok(),
            "登记子流应当成功"
        );
        // 标记「建流已裁决」：否则到点会走建流超时那条路（对端还在等裁决 ⇒ 回 REJECT）。
        let owner = registry
            .channel_owner_(dock_a, dock_b, NonCancellableToken::new())
            .await
            .expect("取状态句柄应当成功")
            .expect("刚登记的活跃子流应当有状态句柄");
        owner.set_establish_settled_();

        let (count, next, full, _) = scan_(&registry, 0u64, 500u64, 1000u64).await;
        assert_eq!(count, 0usize, "刚登记不该有动作");
        assert_eq!(next, 500u64, "下一个期限是保活阈值");
        assert!(!full);

        let (count, next, _, _) = scan_(&registry, 499u64, 500u64, 1000u64).await;
        assert_eq!(count, 0usize, "阈值前一毫秒不该发 PULSE");
        assert_eq!(next, 500u64);

        let (count, next, _, actions) = scan_(&registry, 500u64, 500u64, 1000u64).await;
        assert_eq!(count, 1usize, "到阈值应当发一条 PULSE");
        assert!(matches!(
            actions[0],
            TimerAction_::Pulse { local_dock, remote_dock, .. }
                if local_dock == dock_a && remote_dock == dock_b
        ));
        assert_eq!(next, 1000u64, "发过 PULSE 之后下一个期限是存活到顶");

        let (count, next, _, _) = scan_(&registry, 999u64, 500u64, 1000u64).await;
        assert_eq!(count, 0usize, "超时前一毫秒不该拆流");
        assert_eq!(next, 1000u64);
        assert!(
            !registry.is_wait_close_t_(dock_a, dock_b).await,
            "还没到超时，身份应当仍是活跃子流"
        );

        let (count, _, _, actions) = scan_(&registry, 1000u64, 500u64, 1000u64).await;
        assert_eq!(count, 1usize, "到达超时应当拆流");
        assert!(matches!(
            actions[0],
            TimerAction_::Abort { local_dock, remote_dock }
                if local_dock == dock_a && remote_dock == dock_b
        ));

        let (count, next, _, _) = scan_(&registry, 5000u64, 500u64, 1000u64).await;
        assert_eq!(count, 0usize, "已中止的子流不再参与扫描");
        assert_eq!(next, u64::MAX, "没有其它子流时没有需要等待的期限");
    }
    dual_runtime_test_!(timer_scan_walks_pulse_then_abort);

    /// 测试**建流尚未裁决**的子流到点走「拒绝建流」而不是「拆流」。
    ///
    /// 对端此刻正等裁决（发起方等 `ACCEPT` / `REJECT`，响应方的 `OPEN` 已到本端），
    /// 因此处置必须是「回 `REJECT` + 释放身份 + 留下原因」，而不是给一条还没有数据的
    /// 子流发 `FIN` / `RESET`。
    ///
    /// - 手段：登记一条子流后**不**置「建流已裁决」，在 `now = 1000`（`timeout = 1000`）
    ///   扫一次，再在 `now = 5000` 扫一次。
    /// - 判断：第一次恰好一条 `RejectEstablish`（指向该 dock 对）；第二次无动作——
    ///   `claim_abort_` 已经认领过，不重复投递。
    async fn timer_scan_rejects_unsettled_establish_on_timeout() {
        let registry = make_scan_registry_();
        let dock_a = Dock::new(1u32);
        let dock_b = Dock::new(9u32);
        assert!(
            registry.reserve_channel_t_(dock_a, dock_b).await.is_ok(),
            "登记子流应当成功"
        );

        let (count, _, _, actions) = scan_(&registry, 1000u64, 500u64, 1000u64).await;
        assert_eq!(count, 1usize, "未裁决的建流到点应当产出一条动作");
        assert!(matches!(
            actions[0],
            TimerAction_::RejectEstablish { local_dock, remote_dock }
                if local_dock == dock_a && remote_dock == dock_b
        ));

        let (count, next, _, _) = scan_(&registry, 5000u64, 500u64, 1000u64).await;
        assert_eq!(count, 0usize, "已经认领过的建流超时不应重复投递");
        assert_eq!(next, u64::MAX, "它已经退出扫描");
    }
    dual_runtime_test_!(timer_scan_rejects_unsettled_establish_on_timeout);

    /// 测试**收到对端 `PULSE` 不会让本端少发一条**——保活能收敛的关键。
    ///
    /// 若把「收到 `PULSE`」也算成本端的保活职责活动，两端会在同一时刻互相把对方的
    /// 职责时钟清零，于是**都不发** `PULSE`；而各自的存活时钟又从最后一次真实活动
    /// 起算，先停手的一侧必定先到 `max_channel_timeout` 被拆掉。
    ///
    /// - 手段：登记后先在 `now = 500` 扫出本端的第一条 `PULSE`；随后模拟对端也发了
    ///   一条（`touch_(500)`，只刷存活时钟），再在 `now = 750` 扫描。
    /// - 判断：`750` 处无动作，且下一个期限是 1000（本端的保活职责）而不是
    ///   1500（存活到顶）——说明 `touch_` 没有把职责时钟推后。
    async fn receiving_a_pulse_does_not_cancel_the_local_pulse_duty() {
        let registry = make_scan_registry_();
        let dock_a = Dock::new(2u32);
        let dock_b = Dock::new(7u32);
        let owner = registry
            .reserve_channel_t_(dock_a, dock_b)
            .await
            .expect("登记子流应当成功");

        let (count, next, _, _) = scan_(&registry, 500u64, 500u64, 1000u64).await;
        assert_eq!(count, 1usize, "到阈值应当发 PULSE");
        assert_eq!(next, 1000u64);

        // 对端也在保活：只刷新本端的存活时钟。
        owner.touch_(500u64);

        let (count, next, _, _) = scan_(&registry, 750u64, 500u64, 1000u64).await;
        assert_eq!(count, 0usize, "保活职责时钟在 500 刚推进过，此刻不该再发");
        assert_eq!(next, 1000u64, "下一个期限是本端的保活职责，而不是存活到顶");
        assert_eq!(owner.active_millis_(), 500u64, "收到的 PULSE 刷了存活时钟");
        assert_eq!(owner.data_millis_(), 500u64, "职责时钟由本端发 PULSE 推进");
    }
    dual_runtime_test_!(receiving_a_pulse_does_not_cancel_the_local_pulse_duty);

    /// 测试真实活动会同时推后两个时钟，且 `PULSE` 不会立刻重发。
    /// - 手段：`mark_data_(300)` 之后在 `now = 300 / 799 / 800` 扫描。
    /// - 判断：`300` 无动作且期限为 800（职责时钟 300 + 500）；`799` 无动作；
    ///   `800` 一条 `Pulse`。
    async fn data_activity_postpones_the_pulse_duty() {
        let registry = make_scan_registry_();
        let dock_a = Dock::new(3u32);
        let dock_b = Dock::new(8u32);
        let owner = registry
            .reserve_channel_t_(dock_a, dock_b)
            .await
            .expect("登记子流应当成功");

        owner.mark_data_(300u64);

        let (count, next, _, _) = scan_(&registry, 300u64, 500u64, 1000u64).await;
        assert_eq!(count, 0usize);
        assert_eq!(next, 800u64, "职责时钟被真实活动推到了 300");

        let (count, next, _, _) = scan_(&registry, 799u64, 500u64, 1000u64).await;
        assert_eq!(count, 0usize);
        assert_eq!(next, 800u64);

        let (count, _, _, actions) = scan_(&registry, 800u64, 500u64, 1000u64).await;
        assert_eq!(count, 1usize);
        assert!(matches!(actions[0], TimerAction_::Pulse { .. }));
    }
    dual_runtime_test_!(data_activity_postpones_the_pulse_duty);

    /// 测试动作批次写满时提前返回（`is_full_`），调用方据此立刻再扫一轮。
    /// - 手段：登记三条子流，`actions` 切片只给 1 格，在三条都到保活点时扫描。
    /// - 判断：本轮恰好 1 条动作且 `is_full_` 为真；随后再扫，仍能拿到动作（不丢）。
    async fn timer_scan_reports_a_full_action_batch() {
        let registry = make_scan_registry_();
        for remote in 10u32..13u32 {
            assert!(
                registry
                    .reserve_channel_t_(Dock::new(4u32), Dock::new(remote))
                    .await
                    .is_ok()
            );
        }

        // `now = 5` 已经越过保活阈值（职责时钟 0 + 1），三条子流同时到点。
        let mut actions = [TimerAction_::Idle; 1usize];
        let scan = registry
            .timer_scan_t_(5u64, 1u64, 10_000u64, &mut actions)
            .await;
        assert_eq!(scan.actions_(), 1usize, "切片只有一格，本轮至多一条动作");
        assert!(scan.is_full_(), "批次写满必须报告给调用方");

        let mut actions = [TimerAction_::Idle; 8usize];
        let scan = registry
            .timer_scan_t_(5u64, 1u64, 10_000u64, &mut actions)
            .await;
        assert!(
            scan.actions_() >= 1usize,
            "再扫一轮必须还能拿到剩下的动作（本轮 {} 条）",
            scan.actions_()
        );
    }
    dual_runtime_test_!(timer_scan_reports_a_full_action_batch);

    /// 测试监听器 / 电传端点 / 宽限态都**不**参与保活扫描。
    /// - 手段：登记一个 listener、一个 telegraph 与一条 channel，再把它释放成宽限态，
    ///   然后扫描。
    /// - 判断：扫描只对活跃 channel 产出动作（这里 timeout 极小 ⇒ 一条 `Abort`），
    ///   且不 panic、不把 listener / telegraph 当成子流。
    async fn timer_scan_ignores_non_channel_identities() {
        let registry = make_scan_registry_();
        assert!(registry.reserve_listener_t_(Dock::new(5u32)).await.is_ok());
        assert!(registry.reserve_telegraph_t_(Dock::new(6u32)).await.is_ok());
        assert!(
            registry
                .reserve_channel_t_(Dock::new(7u32), Dock::new(11u32))
                .await
                .is_ok()
        );
        registry
            .release_channel_t_(Dock::new(7u32), Dock::new(11u32))
            .await;

        let (count, _, _, _) = scan_(&registry, 10_000u64, 1u64, 10u64).await;
        assert_eq!(count, 0usize, "宽限态 / listener / telegraph 都不参与保活");
    }
    dual_runtime_test_!(timer_scan_ignores_non_channel_identities);

    /// 测试宽限期到期后宽限态被回收、键可复用。
    /// - 手段：用宽限期为 0 的配置登记并释放 `(1,9)`，随后查询一次触发回收，
    ///   再重新登记同一 dock 对。
    /// - 判断：回收后 `is_wait_close_` 为假，且重新登记成功。
        async fn wait_close_is_reaped_after_expiry() {
        let registry = ChannelRegistry_::new_(opts_zero_grace_(), CoreAlloc);
        assert!(registry.reserve_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await.is_ok());
        registry.release_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await;

        // 宽限期为 0 ⇒ `until_ <= now`，任何一次访问都会顺带回收它。
        assert!(
            !registry.is_wait_close_t_(Dock::new(1u32), Dock::new(9u32))
            .await,
            "已到期的宽限态应当被回收"
        );
        assert!(
            registry.reserve_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await.is_ok(),
            "回收后同一 dock 对应当可以复用"
        );
        assert_index_consistent_(&registry);
    }
    dual_runtime_test_!(wait_close_is_reaped_after_expiry);
    /// 测试入向请求的完整流转：登记 → 唤醒监听者 → 取走（HandedOut）→ 取回窗口通告。
    /// - 手段：先登记 dock 2 的 listener 身份；用 `reserve_inbound_` 登记 `(2,7)`
    ///   并带上对端窗口通告 `(0, 64)`；再依次调用 `has_pending_inbound_` /
    ///   `take_pending_inbound_` / `take_inbound_report_`。
    /// - 判断：登记时唤醒计数为 1；取走前 `has_pending_inbound_` 为真、取走后为假；
    ///   取回的通告与登记时给出的完全相同。
        async fn inbound_request_is_handed_out_with_report() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);
        let rx = registry
            .reserve_listener_with_t_(Dock::new(2u32))
            .await
            .expect("登记 listener 应当成功");

        let report = WindowReport::new(0u64, 64u32);
        assert!(
            registry
                .reserve_inbound_t_(Dock::new(2u32), Dock::new(7u32), report)
            .await
                .is_ok()
        );
        assert!(lsn_pending_(&rx), "登记入向请求应通知监听者");
        assert!(registry.has_pending_inbound_t_(Dock::new(2u32)));

        assert_eq!(
            registry
                .take_pending_inbound_t_(Dock::new(2u32))
                .await
                .map(|(dock, _state)| dock),
            Option::Some(Dock::new(7u32))
        );
        assert!(
            !registry.has_pending_inbound_t_(Dock::new(2u32)),
            "取走之后不应再报告有待决请求"
        );
        assert!(
            registry
                .take_pending_inbound_t_(Dock::new(2u32))
                .await
                .is_none()
        );

        assert_eq!(
            registry.take_inbound_report_t_(Dock::new(2u32), Dock::new(7u32))
            .await,
            Option::Some(report)
        );
    }
    dual_runtime_test_!(inbound_request_is_handed_out_with_report);
    /// 测试按 `local_dock` 的区间枚举只覆盖本 dock 的具体子流，且跳过哨兵行。
    ///
    /// - 手段：dock 2 上登记 listener 身份与 `(2,7)`、`(2,9)`、`(2,11)` 三条入向
    ///   请求，dock 3 上登记 `(3,7)`；随后在 dock 2 上 `take_pending_inbound_`，
    ///   并检查 `has_pending_inbound_` 对三个 dock 的回答。
    /// - 判断：dock 2 只取到本 dock 的具体子流（`remote_dock` 升序 → 先 7 后 9），
    ///   listener 身份不会被当成入向请求；dock 3 的请求不受影响。
        async fn per_local_range_covers_only_concrete_channels() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);
        let report = WindowReport::new(0u64, 64u32);
        assert!(registry.reserve_listener_t_(Dock::new(2u32))
            .await.is_ok());
        assert!(registry.reserve_listener_t_(Dock::new(3u32))
            .await.is_ok());

        assert!(registry.reserve_inbound_t_(Dock::new(2u32), Dock::new(9u32), report)
            .await.is_ok());
        assert!(registry.reserve_inbound_t_(Dock::new(2u32), Dock::new(7u32), report)
            .await.is_ok());
        assert!(registry.reserve_inbound_t_(Dock::new(2u32), Dock::new(11u32), report)
            .await.is_ok());
        assert!(registry.reserve_inbound_t_(Dock::new(3u32), Dock::new(7u32), report)
            .await.is_ok());

        assert!(registry.has_pending_inbound_t_(Dock::new(2u32)));
        assert!(registry.has_pending_inbound_t_(Dock::new(3u32)));
        // dock 1 只有 listener 身份，没有具体子流。
        assert!(registry.reserve_listener_t_(Dock::new(1u32))
            .await.is_ok());
        assert!(!registry.has_pending_inbound_t_(Dock::new(1u32)));

        assert_eq!(
            registry
                .take_pending_inbound_t_(Dock::new(2u32))
                .await
                .map(|(dock, _state)| dock),
            Option::Some(Dock::new(7u32)),
            "dock 2 上应当按 remote_dock 升序先取到 7"
        );
        assert_eq!(
            registry
                .take_pending_inbound_t_(Dock::new(2u32))
                .await
                .map(|(dock, _state)| dock),
            Option::Some(Dock::new(9u32))
        );
        assert_eq!(
            registry
                .take_pending_inbound_t_(Dock::new(3u32))
                .await
                .map(|(dock, _state)| dock),
            Option::Some(Dock::new(7u32)),
            "dock 3 自己的待决请求不应被 dock 2 的取走影响"
        );
        assert!(!registry.has_pending_inbound_t_(Dock::new(3u32)));
        assert_index_consistent_(&registry);
    }
    dual_runtime_test_!(per_local_range_covers_only_concrete_channels);
    /// 测试反向索引随活跃 channel 同步增删，且宽限态不入索引。
    ///
    /// - 手段：登记 `(1,9)`、`(2,9)`、`(1,10)`，逐次用
    ///   `for_each_local_of_remote_` 收集；随后释放 `(1,9)` 再收集一次。
    /// - 判断：`remote=9` 依次得到 `[1,2]`（升序）；`remote=10` 得到 `[1]`；
    ///   释放 `(1,9)` 后 `remote=9` 只剩 `[2]`（墓碑不入索引）；`remote=99` 为空。
        async fn remote_index_tracks_active_channels_only() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);

        assert!(registry.reserve_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await.is_ok());
        assert!(registry.reserve_channel_t_(Dock::new(2u32), Dock::new(9u32))
            .await.is_ok());
        assert!(registry.reserve_channel_t_(Dock::new(1u32), Dock::new(10u32))
            .await.is_ok());
        assert_index_consistent_(&registry);

        assert_eq!(
            collect_locals_(&registry, Dock::new(9u32)).await,
            vec![Dock::new(1u32), Dock::new(2u32)],
            "remote=9 应当按 local 升序给出两条关联"
        );
        assert_eq!(
            collect_locals_(&registry, Dock::new(10u32)).await,
            vec![Dock::new(1u32)]
        );
        assert!(collect_locals_(&registry, Dock::new(99u32)).await.is_empty());

        registry.release_channel_t_(Dock::new(1u32), Dock::new(9u32))
            .await;
        assert_eq!(
            collect_locals_(&registry, Dock::new(9u32)).await,
            vec![Dock::new(2u32)],
            "释放后反向索引必须同步摘除（墓碑不入索引）"
        );
        assert_index_consistent_(&registry);
    }
    dual_runtime_test_!(remote_index_tracks_active_channels_only);

    /// 测试连接级失败只保留首个原因，并取消两个循环的令牌。
        async fn failure_keeps_first_cause_and_cancels_loops() {
        use buffex::x_deps::abs_cancel::TrCancellationToken;

        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);
        assert!(!registry.is_failed_t_()
            .await);
        let read_loop = registry.loop_token_(0usize);
        let write_loop = registry.loop_token_(1usize);
        assert!(
            !TrCancellationToken::is_cancelled(&read_loop)
                && !TrCancellationToken::is_cancelled(&write_loop)
        );

        registry.mark_failed_t_(&MuxError::PeerClosed)
            .await;
        assert!(registry.is_failed_t_()
            .await);
        assert_eq!(registry.failure_t_()
            .await, Option::Some(MuxError::PeerClosed));
        assert!(
            TrCancellationToken::is_cancelled(&read_loop)
                && TrCancellationToken::is_cancelled(&write_loop)
        );

        registry.mark_failed_t_(&MuxError::MalformedFrame)
            .await;
        assert_eq!(
            registry.failure_t_()
            .await,
            Option::Some(MuxError::PeerClosed),
            "首个失败原因应当保留"
        );
    }
    dual_runtime_test_!(failure_keeps_first_cause_and_cancels_loops);

    /// 用 [`ChannelRegistry_::for_each_local_of_remote_`] 收集某个 remote 的 local 集合。
    /// - 手段：传入一个把元素推进 `Vec` 的回调。
    /// - 判断：返回的 `Vec` 就是该 remote 关联的全部 local（按回调顺序）。
    async fn collect_locals_(registry: &ChannelRegistry_<CoreAlloc>, remote: Dock) -> Vec<Dock> {
        let mut out: Vec<Dock> = Vec::new();
        registry.for_each_local_of_remote_t_(remote, |local| out.push(local))
            .await;
        out
    }

    /// 测试 dock 条目在「无活跃子流、未绑定」时被回收，绑定期间保留。
    ///
    /// - 手段：在 dock 20 上登记并释放一条子流；在 dock 21 上绑定后登记并释放
    ///   一条子流。
    /// - 判断：dock 20 的条目消失（宽限墓碑不需要 dock 级簿记）；dock 21 因仍处
    ///   绑定态而保留，且再次绑定仍报 `DockInUse`。
        async fn empty_dock_entry_is_pruned_unless_bound() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);

        assert!(registry.reserve_channel_t_(Dock::new(20u32), Dock::new(1u32))
            .await.is_ok());
        registry.release_channel_t_(Dock::new(20u32), Dock::new(1u32))
            .await;
        registry.with_t_(|inner| {
            assert!(
                !inner.docks_.contains_key(&Dock::new(20u32)),
                "无活跃子流、未绑定的 dock 条目应当被回收"
            );
        });

        assert!(registry.bind_dock_t_(Dock::new(21u32))
            .await.is_ok());
        assert!(registry.reserve_channel_t_(Dock::new(21u32), Dock::new(1u32))
            .await.is_ok());
        registry.release_channel_t_(Dock::new(21u32), Dock::new(1u32))
            .await;
        registry.with_t_(|inner| {
            assert!(
                inner.docks_.contains_key(&Dock::new(21u32)),
                "绑定态是持久占用，条目不可被回收"
            );
        });
        assert!(matches!(
            registry.bind_dock_t_(Dock::new(21u32))
            .await.unwrap_err(),
            ReserveErr_::DockInUse
        ));
    }
    dual_runtime_test_!(empty_dock_entry_is_pruned_unless_bound);
    /// 测试状态句柄随身份登记一起建立，且按 dock 对查回的与 `reserve` 返回的是
    /// **同一个**共享节点。
    /// - 手段：`reserve_channel_` 拿到句柄，直接查 `channel_owner_`；再在一个句柄上
    ///   置去重位、看另一个句柄是否可见。
    /// - 判断：查回的句柄存在；两者共享同一份锁外去重位（改一处、另一处可见）。
        async fn owner_is_created_with_the_identity() {
        let registry = ChannelRegistry_::new_(BasicOpts::default(), CoreAlloc);
        let reserved = registry.reserve_channel_t_(Dock::new(3u32), Dock::new(8u32))
            .await
            .expect("登记身份应当成功");

        let found = registry
            .channel_owner_t_(Dock::new(3u32), Dock::new(8u32))
            .await
            .expect("状态随身份建立，应当能查到");
        assert!(reserved.mark_tx_queued_(), "首次置位应当是 fresh");
        assert!(
            !found.mark_tx_queued_(),
            "查回的应是同一个共享句柄（去重位在共享的原子字上）"
        );
        found.clear_tx_queued_();
        assert!(reserved.mark_tx_queued_(), "清位对同一份共享节点可见");
        assert_index_consistent_(&registry);
    }
    dual_runtime_test_!(owner_is_created_with_the_identity);
}
