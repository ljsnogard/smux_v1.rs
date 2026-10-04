//! smux v1 集成冒烟测试的共享场景与辅助设施。
//!
//! 两个测试 target（tokio / compio）共用本模块，共同点是**都跑在真实 UNIX
//! domain socket 上**：
//!
//! - [`run_socket_scenario_`]：socket 侧传输装配——四个**调用方驱动的泵**把两个
//!   socket 半边接到四个**全被动环**上，再交给 [`run_smoke_scenario_`]；
//! - [`run_smoke_scenario_`]：与运行时、与传输都无关的场景主体——握手 → 拆连接 →
//!   16 个 dock × 1024 条子流收发并逐条校验载荷；
//! - [`make_passive_ring_`] / [`pump_input_`] / [`pump_output_`]：上面那套环与泵。
//!
//! 场景对运行时**不可知**：并发一律用 `futures` 的组合子（`join!` / `join_all`
//! / `select`），且**不 spawn**——compio 的 socket 半边是 `!Send`，spawn 不可用。
//!
//! # 为什么必须由调用方驱动泵（三处实测阻塞点）
//!
//! `smux` 的 `Rx` / `Tx` 只认 `abs_buff::TrBuffRead` / `TrBuffWrite`。把 socket
//! 设备直接接成这两个 trait、或在 `buffex` 里用「主动设备环」，各有实测阻塞点：
//!
//! 1. **compio 的 socket 半边是 `!Send`**（`SharedFd` 内含 `Rc`）。`buffex` 的
//!    主动设备环在构建期要求设备 `Send + Sync`，因此 compio 走不通「设备级适配 +
//!    buffex 环」这条在 tokio 上可行的路线（实测 E0277：`Rc<...>`
//!    cannot be sent between threads safely）。
//! 2. **`TrBuffWrite` 没有 flush 钩子**：段的回收函数是同步的，无法在提交时 await
//!    一次 socket 写。`abs_buff_compio_adapt::CompioWriteAsBuff` 因此把「提交」与
//!    「上网」拆开，并对外提供 `flush_async()` 由调用方收尾；而握手模块
//!    （`handshake::codec_::write_all_`）写完一帧的最后一个段后就直接转去读对端，
//!    不会再调用写侧——直接把它当作 `Tx`，首帧（INVITE 的 crc 尾字节）就会双方
//!    互相等待而死锁（实测）。
//! 3. **buffex 主动输出泵只做「提交时单次非阻塞 poll」**（见 `buffex` 模块文档
//!    「主动模式的驱动」）：对完成式 socket 写而言这一 poll 可能是 `Pending`，而
//!    「被动生产 × 主动消费」拓扑只产出生产端半部、没有 executor 驱动的泵来接住
//!    这次 `Pending`，于是尾段滞留。实测（tokio + socket）：单方向「环写 → 裸
//!    socket 读」与「裸 socket 写 → 环读」都正常，但「环写 → 对端环读」与整条
//!    握手会一直 pending。
//!
//! 结论：socket 侧必须由**调用方的 async 泵**把「设备搬运」与「环提交」显式串起来
//! （泵 future 由运行时正常 await，waker 是真 waker），环则退化为**全被动**
//! （`producer_passive().consumer_passive()`，与本 crate 单元测试同款）。这样既
//! 保留 socket 传输，又避开上面三点；代价是连接层落地后吞吐受泵分块限制。
//!
//! 设计决策与取舍同步记录在 `dev-notes/connection-20260919-1631.md`。
//!
//! 传输只有**一条**全双工 UNIX domain socket 连接：两个端点（`pair()` 的两个
//! 返回值）是这条连接的两端，不是两条连接；每端各持有自己的读 / 写半边，由四条
//! 调用方驱动的泵与场景并发推进。
//!
//! # 子流身份：每条并发子流一个临时 `local_dock`
//!
//! 帧头（`connection::frame_`）只有 `LocalDock` / `RemoteDock`，**没有 channel
//! 标识字段**；按 `src/connection/mod.rs` §4.1「dock 对即身份」，同一
//! `(local_dock, remote_dock)` 对在同一时刻**至多一条活动子流**。因此本模块的全部
//! 场景（含 1024 条的冒烟场景）都遵守这条协议规则：发起侧为每条并发子流分配一个
//! **互不相同**的临时 `local_dock`（类比 TCP 临时端口），被动侧则断言
//! `handle.local_dock()` 就是自己的监听 dock（镜像语义）、`remote_dock` 是对方的
//! 临时 dock。
//!
//! 载荷校验**不依赖 open / accept 的配对顺序**：先读 4 字节 tag，tag 里编码了发送方
//! 的 `(dock, index)`，据此推出对方的完整载荷再逐字节比对。这样即使两侧任务推进顺序
//! 不同，校验依然成立。
//!
//! > 历史：本模块曾按「同一 dock 内 FIFO 配对」在同一个 dock 对上并发 64 条子流，
//! > 那与 §4.1 冲突（第 2 条 `OPEN` 会被 `BindingError::Duplicate` 拒绝）。该冲突的裁决
//! > 是「改测试、协议不动」，见 `dev-notes/connection-20261002-0548.md` §5 Q2。
//!
#![allow(dead_code)] // 三个测试 target 各自只用到本模块的一部分。

use core::{borrow::BorrowMut, marker::PhantomData, mem::MaybeUninit};

use buffex::{
    ring::Ring,
    x_deps::abs_buff::{
        Demand, TrBuffRead, TrBuffWrite,
        buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
        error::{ReadErrTag, TrTaggedError},
        io::{TrInput, TrOutput},
    },
};
use mm_ptr::{Owned, Shared, x_deps::abs_mm::CoreAlloc};
use abs_smux::{
    chan::{
        ChannelBuffAlloc, TrChannelHalf, TrChannelHandle, TrPrepareChannelRing,
    },
    conf::TrMuxConfig,
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};
use smux_v1::{
    connection::{
        BindError, BuffAllocError, ChannelHandle, ChannelRx, ChannelTx, Dock, HandleError,
        K_STAGE_RING_CAPACITY, MuxConnection, TrConnCfg,
    },
    flow_ctrl::DefaultPolicy,
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
};

/// 连接层要求的 bound：**值化的本地作用域**（`abs_art::TrLocalScope`）。
///
/// 场景函数一律把作用域值作为第一个参数（`scope: &S`）并原样转发给
/// [`MuxConnection::new`]；两个测试目标各自给出具体后端的作用域值
/// （tokio / compio 各自的 `LocalScope`）。契约是「谁取得作用域，谁负责驱动」：
/// tokio 用 `scope.run_until(..)` 包住整段使用期，compio 由运行时自己驱动。
pub use abs_art::TrLocalScope as TrSmokeScope;

/// 一端连接对象的类型：策略固定为 [`SmokeMuxConfig`]，传输类型由策略的类型参数
/// `R` / `W` 声明；连接对外的错误类型统一为无泛型的 [`MuxError`]。
pub type SmokeConn<R, W, S> = MuxConnection<SmokeMuxConfig<W, R>, S>;

/// 测试侧对「闭包造两块缓冲」这一常见写法的适配器。
///
/// 上游 `TrPrepareChannelRing` 由环境自行实现；本测试套件为了不把每个调用点都手写
/// 一个 prepared 类型，提供一个最小的闭包包装器。
pub struct ClosurePrepare<F>(F);

impl<F> ClosurePrepare<F> {
    /// 包住一个 `FnOnce() -> (tx_buff, rx_buff)` 闭包。
    pub const fn new(inner: F) -> Self {
        ClosurePrepare(inner)
    }
}

impl<F, B> TrPrepareChannelRing<B, u8> for ClosurePrepare<F>
where
    F: FnOnce() -> (B, B),
    B: 'static + BorrowMut<[MaybeUninit<u8>]>,
{
    fn prepare(self) -> ChannelBuffAlloc<B, u8> {
        let (tx_buff, rx_buff) = (self.0)();
        ChannelBuffAlloc::new(tx_buff, rx_buff)
    }
}

/// 给真实 [`ChannelHandle`] 加一个「直接传闭包」的便捷方法，让测试场景保持可读。
///
/// 生产路径仍然按上游契约走 `accept_async(welcome, prepare)`；这里只是把闭包包进
/// [`ClosurePrepare`] 后转发。
pub trait AcceptAsyncClosureExt<C, S>: Sized
where
    C: TrConnCfg,
{
    async fn accept_async_closure<'f, W, F>(
        &'f mut self,
        welcome: &'f mut W,
        prepare: F,
    ) -> Result<(ChannelTx<C, S>, ChannelRx<C, S>), HandleError>
    where
        W: 'f + TrBuffWrite<u8>,
        F: FnOnce() -> (C::Buff, C::Buff);
}

impl<C, S> AcceptAsyncClosureExt<C, S> for ChannelHandle<C, S>
where
    C: TrConnCfg,
{
    async fn accept_async_closure<'f, W, F>(
        &'f mut self,
        welcome: &'f mut W,
        prepare: F,
    ) -> Result<(ChannelTx<C, S>, ChannelRx<C, S>), HandleError>
    where
        W: 'f + TrBuffWrite<u8>,
        F: FnOnce() -> (C::Buff, C::Buff),
    {
        self.accept_async(welcome, ClosurePrepare::new(prepare)).await
    }
}

/// 每个端点监听的 dock 数量（dock 取值 `1..=16`）。
pub const K_DOCK_COUNT: u32 = 16;

/// 每个 dock 上开通 / 接受的子流数量。
pub const K_CHANNELS_PER_DOCK: usize = 64;

/// 每端开通 / 接受的子流总数（`16 × 64 = 1024`）。
pub const K_TOTAL_CHANNELS: usize = 1024;

/// 单条子流每个方向的环容量（字节）。
pub const K_CHANNEL_CAPACITY: usize = 4096;

/// 网络侧主动设备环的容量（字节）。
pub const K_NET_BUFFER_SIZE: usize = 64usize * 1024usize;

/// 测试用复用资源策略：`mm_ptr::Owned` 存储 + `CoreAlloc` 分配器 +
/// [`DefaultPolicy`] 流控，两条传输半边由类型参数 `W` / `R` 给出。
///
/// `Clone` / `Copy` **手写**而不是 derive：`#[derive(Clone)]` 会给 `W` / `R` 加上
/// `Clone` 约束，而这里只放 `PhantomData<fn() -> _>`，本来不需要——握手里两条半边被
/// 移进 `'static` future，`W` / `R`（环端）并不 `Clone`。
#[derive(Debug)]
pub struct SmokeMuxConfig<W, R> {
    _use_w_: PhantomData<fn() -> W>,
    _use_r_: PhantomData<fn() -> R>,
}

impl<W, R> Copy for SmokeMuxConfig<W, R> {}

impl<W, R> Clone for SmokeMuxConfig<W, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<W, R> SmokeMuxConfig<W, R> {
    /// 由一个 ZST 值构造；类型由调用点的 `HandshakeDelivery` / `MuxConnection` 推断。
    pub const fn new() -> Self {
        SmokeMuxConfig {
            _use_w_: PhantomData,
            _use_r_: PhantomData,
        }
    }
}

impl<W, R> Default for SmokeMuxConfig<W, R> {
    fn default() -> Self {
        Self::new()
    }
}

/// [`DefaultPolicy`] 是 ZST；取静态引用即可满足 `TrConnCfg::policy`。
static SMOKE_POLICY: DefaultPolicy = DefaultPolicy;

impl<W, R> TrMuxConfig for SmokeMuxConfig<W, R>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    type Data = u8;
    type Dock = Dock;
    type Buff = SmokeBuff;
}

impl<W, R> TrConnCfg for SmokeMuxConfig<W, R>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    type Alloc = CoreAlloc;
    type Policy = DefaultPolicy;
    type ConnTx = W;
    type ConnRx = R;
    type StageBuff = SmokeBuff;

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &SMOKE_POLICY
    }

    fn make_ring_buffs(
        &self,
        alloc: Self::Alloc,
        capacity: usize,
    ) -> Result<(Self::Buff, Self::Buff), BuffAllocError> {
        Result::Ok((
            Owned::new_uninit_slice(capacity, alloc),
            Owned::new_uninit_slice(capacity, alloc),
        ))
    }

    fn make_stage_buffs(
        &self,
        alloc: Self::Alloc,
    ) -> Result<(Self::StageBuff, Self::StageBuff), BuffAllocError> {
        Result::Ok((
            Owned::new_uninit_slice(K_STAGE_RING_CAPACITY, alloc),
            Owned::new_uninit_slice(K_STAGE_RING_CAPACITY, alloc),
        ))
    }
}

/// 造一对**连接级**帧暂存缓冲（容量 [`K_STAGE_RING_CAPACITY`]）。
///
/// [`MuxConnection::new`] 要求调用方给出两块连接级缓冲，连接把它们建成两条帧暂存环
/// （见 `src/connection/session_.rs` 模块文档）。测试里直接按配置给出。
pub fn make_stage_buffs_() -> (SmokeBuff, SmokeBuff) {
    make_stage_buffs_with_(K_STAGE_RING_CAPACITY)
}

/// 按指定容量造一对**连接级**帧暂存缓冲。
///
/// 「极小容量」用例（容量 1 字节）用它，验收「逐字节异步解析 ⇒ 帧暂存不需要装下
/// 整帧」这条要求。
pub fn make_stage_buffs_with_(capacity: usize) -> (SmokeBuff, SmokeBuff) {
    (
        Owned::new_uninit_slice(capacity, CoreAlloc),
        Owned::new_uninit_slice(capacity, CoreAlloc),
    )
}

/// 环存储的具体类型（元素 `u8` + `CoreAlloc`），避免类型推断歧义。
///
/// 对测试目标公开：`tests/thread_safety.rs` 需要给泛化的 `connect_pair_` 标注
/// 连接配置类型，而配置类型上带着这个存储类型。
pub type SmokeBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

/// 造一块子流环存储（容量 [`K_CHANNEL_CAPACITY`]）。
///
/// 建流最终裁决（`accept_async`）要求调用方给出**本条子流**要用的两块缓冲；上游把
/// 「形式」固定为借用型切片（`&'static mut [MaybeUninit<u8>]`），**来源与容量由调用方
/// 决定**——测试里直接泄漏一块（生产代码里应当来自静态区或调用方的 arena）。
pub fn make_channel_buff_() -> SmokeBuff {
    Owned::new_uninit_slice(K_CHANNEL_CAPACITY, CoreAlloc)
}

/// 按指定容量造一块缓冲（用于「每条子流自己决定分配多少」的用例）。
pub fn make_channel_buff_with_(capacity: usize) -> SmokeBuff {
    Owned::new_uninit_slice(capacity, CoreAlloc)
}

/// 单条**全被动**环，返回 `(写端, 读端)`。
///
/// 读端交给 smux 当 `Rx`（由 [`pump_input_`] 写入），或写端交给 smux 当 `Tx`
/// （由 [`pump_output_`] 排空）。环不接任何设备，搬运完全由调用方的泵负责——原因
/// 见模块文档「为什么必须由调用方驱动泵」。
pub fn make_passive_ring_(
    capacity: usize,
) -> (
    smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
    smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
) {
    let buffer: SmokeBuff = Owned::new_uninit_slice(capacity, CoreAlloc);
    let ring = Ring::try_new(buffer).expect("环容量应当落在 buffex 允许的区间内");
    let shared = Shared::new(ring, CoreAlloc);
    // SAFETY: 这条环只被刚建出的 `Shared` 独占，且没有对应的 `Weak`（不存在升级
    // 路径），因此两个半部各持一个强引用是安全的；与 `Ring::split_unchecked` 文档
    // 要求的两条调用方保证一致。
    unsafe { Ring::split_unchecked(shared) }
}

/// 单次从 socket 搬进环的分块上限（字节）。
///
/// 设备读一返回（≥ 1 字节）就立刻提交进环，因此该值只影响单次搬运量，不影响
/// 首字节时延。
const K_PUMP_CHUNK: usize = 4096;

/// 入向泵：`TrInput`（socket 读设备）→ 全被动环。
///
/// 该环的消费端交给 smux 当 `Rx`。循环：从设备读一段（返回 0 即 EOF，退出并 drop
/// 环写端，让 smux 的 `Rx` 看到关闭）→ 把这段写进环 → drop 段提交（`advance_write`
/// 唤醒 smux 读侧）。
async fn pump_input_<I, W>(mut input: I, mut ring_tx: W)
where
    I: TrInput<u8>,
    W: TrBuffWrite<u8>,
{
    let mut chunk: Vec<MaybeUninit<u8>> =
        (0..K_PUMP_CHUNK).map(|_| MaybeUninit::uninit()).collect();
    loop {
        // 1. 从 socket 读一段（至少 1 字节，或 EOF）。
        let read = input.read_async(&mut chunk).await;
        let n = match read.pick_left() {
            Some(n) => n,
            None => panic!("入向泵：socket 读设备报错"),
        };
        if n == 0usize {
            // EOF：退出并 drop `ring_tx`，让对端读到环关闭。
            return;
        }
        // SAFETY: 设备只把已初始化的字节写进 `chunk[..n]`；`MaybeUninit<u8>` 与
        // `u8` 布局相同、对齐相同（均为 1），按已初始化字节读取是健全的。
        let bytes: &[u8] =
            unsafe { core::slice::from_raw_parts(chunk.as_ptr() as *const u8, n) };

        // 2. 把 `bytes` 全部搬进环（环可能空间不足，故分段写入）。
        let mut off = 0usize;
        while off < bytes.len() {
            let demand = Demand::at_least(1);
            let mut outcome = ring_tx.write_async(&demand).await;
            let put;
            {
                let segm = outcome.as_mut().pick_left();
                match segm {
                    Some(segm) => {
                        put = segm.as_segm_mut().clone_items_from_buff(&bytes[off..]);
                    }
                    None => panic!("入向泵：环写端不可用"),
                }
            }
            if put == 0usize {
                panic!("入向泵：环写段为空");
            }
            off += put;
            // `outcome` 在此 drop：提交写入 → advance_write → 唤醒 smux 读侧。
        }
    }
}

/// 出向泵：全被动环 → `TrOutput`（socket 写设备）。
///
/// 该环的生产端交给 smux 当 `Tx`。循环：从环读一段（`least_count` 即当前可读量）
/// → 拷进本地缓冲 → **写完 socket 之后**才 drop 段提交消费（保证「先上网、后释放
/// 缓冲」，与 `TrBuffRead` 的消费语义一致）。
async fn pump_output_<R, O>(mut ring_rx: R, mut output: O)
where
    R: TrBuffRead<u8>,
    O: TrOutput<u8>,
{
    loop {
        let demand = Demand::at_least(1);
        let mut outcome = ring_rx.read_async(&demand).await;
        {
            let segm = outcome.as_mut().pick_left();
            match segm {
                Some(segm) => {
                    let mut child = segm.as_segm_ref();
                    let n = child.least_count();
                    let mut dst: Vec<MaybeUninit<u8>> =
                        (0..n).map(|_| MaybeUninit::uninit()).collect();
                    // SAFETY: `dst` 是本函数独占的可写切片；`move_items_to_buff`
                    // 只会写入其中已初始化的前缀（返回值给出长度）。
                    let moved = unsafe { child.move_items_to_buff(&mut dst) };
                    if moved == 0usize {
                        panic!("出向泵：环读段为空");
                    }
                    // 把 `dst[..moved]` 全部写给设备（允许部分写，循环到写完）。
                    // 此时这些字节仍被环段占用，写失败不会丢数据。
                    let mut off = 0usize;
                    while off < moved {
                        let w = output.write_async(&dst[off..moved]).await;
                        match w.pick_left() {
                            Some(0usize) => panic!("出向泵：socket 写设备返回 0"),
                            Some(k) => off += k,
                            None => panic!("出向泵：socket 写设备报错"),
                        }
                    }
                    drop(child);
                }
                None => {
                    // 环被关闭（smux 侧 `Tx` 被 drop）或读错误：结束泵。
                    return;
                }
            }
        }
        // `outcome` 在此 drop：提交消费 → advance_read → 唤醒 smux 写侧。
    }
}

/// 用「socket + 调用方驱动的泵 + 全被动环」跑完 [`run_smoke_scenario_`]。
///
/// 参数依次是 A 端（握手发起方）与 B 端（等待方）的 socket 读设备（`TrInput`）与
/// 写设备（`TrOutput`）：tokio 侧由 `buffex_tokio_adapt`（设备级适配来自它依赖的
/// `abs_buff_tokio_adapt`）提供，compio 侧由 `buffex_compio_adapt` 直接提供。
///
/// 每端装配两个全被动环：
///
/// ```text
/// socket 读设备 --(pump_input_)--> ring_tx ──> ring_rx = smux 的 Rx
/// smux 的 Tx = ring_tx ──> ring_rx --(pump_output_)--> socket 写设备
/// ```
///
/// 四个泵与场景用 `select` 并发推进（同一任务内轮询，不要求任何类型 `Send`）；
/// 场景完成即丢弃泵 future，从而结束对设备（及其借用的 socket 半边）的借用。
pub async fn run_socket_scenario_<IA, OA, IB, OB, S>(
    scope: &S,
    input_a: IA,
    output_a: OA,
    input_b: IB,
    output_b: OB,
) where
    IA: TrInput<u8>,
    OA: TrOutput<u8>,
    IB: TrInput<u8>,
    OB: TrOutput<u8>,
    S: TrSmokeScope + Clone + 'static,
{
    run_socket_scenario_with_(input_a, output_a, input_b, output_b, |a_rx, a_tx, b_rx, b_tx| {
        run_smoke_scenario_::<_, _, _, _, S>(scope, a_rx, a_tx, b_rx, b_tx)
    })
    .await
}

/// 与 [`run_socket_scenario_`] 相同的传输装配，但跑**本轮验收用的小场景**
/// （2 dock × 各 2 条 channel，双向收发 + 半关闭），见 [`run_small_mux_scenario_`]。
pub async fn run_small_socket_scenario_<IA, OA, IB, OB, S>(
    scope: &S,
    input_a: IA,
    output_a: OA,
    input_b: IB,
    output_b: OB,
) where
    IA: TrInput<u8>,
    OA: TrOutput<u8>,
    IB: TrInput<u8>,
    OB: TrOutput<u8>,
    S: TrSmokeScope + Clone + 'static,
{
    run_socket_scenario_with_(
        input_a,
        output_a,
        input_b,
        output_b,
        |a_rx, a_tx, b_rx, b_tx| {
            run_small_mux_scenario_::<_, _, _, _, S>(scope, a_rx, a_tx, b_rx, b_tx)
        },
    )
    .await
}

/// 把一次场景挂在「socket + 调用方驱动的泵 + 全被动环」的传输上。
///
/// 四个泵与场景用 `select` 并发推进（同一任务内轮询，不要求任何类型 `Send`）；
/// 场景完成即丢弃泵 future，从而结束对设备（及其借用的 socket 半边）的借用。
async fn run_socket_scenario_with_<IA, OA, IB, OB, F, Fut>(
    input_a: IA,
    output_a: OA,
    input_b: IB,
    output_b: OB,
    scenario: F,
) where
    IA: TrInput<u8>,
    OA: TrOutput<u8>,
    IB: TrInput<u8>,
    OB: TrOutput<u8>,
    F: FnOnce(
        smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
    ) -> Fut,
    Fut: core::future::Future<Output = ()>,
{
    // 每端两个环：一个承载「socket → smux」（Rx），一个承载「smux → socket」（Tx）。
    let (a_rx_ring_tx, a_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (a_tx, a_tx_ring_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (b_rx_ring_tx, b_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (b_tx, b_tx_ring_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);

    let scenario_fut = scenario(a_rx, a_tx, b_rx, b_tx);
    let pumps_fut = async {
        futures::join!(
            pump_input_(input_a, a_rx_ring_tx),
            pump_output_(a_tx_ring_rx, output_a),
            pump_input_(input_b, b_rx_ring_tx),
            pump_output_(b_tx_ring_rx, output_b),
        )
    };

    futures::pin_mut!(scenario_fut);
    futures::pin_mut!(pumps_fut);
    // 左侧（场景）先被轮询：场景完成即返回；若泵全部退出（连接被提前关闭、
    // 四个泵都走到 EOF），说明场景没能跑完，判为失败。
    match futures::future::select(scenario_fut, pumps_fut).await {
        futures::future::Either::Left(((), _pumps)) => {}
        futures::future::Either::Right((_pumps, _scenario)) => {
            panic!("四个传输泵在场景完成之前全部退出")
        }
    }
}

/// 小规模验收场景：**2 个 dock × 各 2 条 channel**，双向并发收发 + 半关闭。
///
/// 即 [`run_mux_scenario_`] 在 `K_SMALL_DOCK_COUNT × K_SMALL_CHANNELS_PER_DOCK`
/// 下的实例；与 1024 条的 [`run_smoke_scenario_`] 走的是**同一份**驱动
/// （[`drive_side_`]，每条并发子流一个互不相同的临时 `local_dock`），只是规模不同。
/// 它同时是进程内直连（`tests/inmem_mux.rs`）与 socket 版
/// （[`run_small_socket_scenario_`]）快速回归的挂点。
///
/// # Panics
///
/// 任何一次 open / accept / 读写 / 半关闭校验失败都会 panic——失败即测试失败。
pub async fn run_small_mux_scenario_<RA, WA, RB, WB, S>(
    scope: &S,
    rx_a: RA,
    tx_a: WA,
    rx_b: RB,
    tx_b: WB,
) where
    RA: TrBuffRead<u8> + 'static,
    WA: TrBuffWrite<u8> + 'static,
    RB: TrBuffRead<u8> + 'static,
    WB: TrBuffWrite<u8> + 'static,
    S: TrSmokeScope + Clone + 'static,
{
    run_mux_scenario_::<_, _, _, _, S>(
        scope,
        rx_a,
        tx_a,
        rx_b,
        tx_b,
        K_SMALL_DOCK_COUNT,
        K_SMALL_CHANNELS_PER_DOCK,
    )
    .await
}

/// 与 [`run_small_mux_scenario_`] 相同，但 dock 数量与每个 dock 的子流数量可调。
///
/// 这是全部场景的唯一实现：握手 → 建两个 [`MuxConnection`]（内部各自 spawn 读 / 写
/// 循环）→ 两端并发跑「`dock_count` 个 dock × 每个 `per_dock` 条子流」的双向收发与
/// 半关闭（[`drive_side_`]）。1024 条的冒烟场景只是它的 `16 × 64` 特例。
async fn run_mux_scenario_<RA, WA, RB, WB, S>(
    scope: &S,
    rx_a: RA,
    tx_a: WA,
    rx_b: RB,
    tx_b: WB,
    dock_count: u32,
    per_dock: usize,
) where
    RA: TrBuffRead<u8> + 'static,
    WA: TrBuffWrite<u8> + 'static,
    RB: TrBuffRead<u8> + 'static,
    WB: TrBuffWrite<u8> + 'static,
    S: TrSmokeScope + Clone + 'static,
{
    let (conn_a, conn_b) =
        connect_pair_::<
            SmokeMuxConfig<WA, RA>,
            SmokeMuxConfig<WB, RB>,
            RA,
            WA,
            RB,
            WB,
            S,
        >(scope, rx_a, tx_a, rx_b, tx_b)
        .await;

    futures::join!(
        drive_side_(&conn_a, 0u32, dock_count, per_dock),
        drive_side_(&conn_b, 1u32, dock_count, per_dock),
    );
}

/// 握手（A 端发起、B 端等待）并由交付物建立两个 [`MuxConnection`]。
///
/// 抽出来给「收发场景」与「绑定独占性场景」共用，保证两者走的是**同一套**
/// 连接建立路径；`tests/thread_safety.rs` 也直接用它装配连接。
///
/// **连接配置是泛型参数** `C`：默认调用点是 [`SmokeMuxConfig`]，而「极小帧暂存」
/// 验收用例传入一个只把 `make_stage_buffs` 换成一字节缓冲的同构配置——两条子流环
/// 与其余策略完全一致，避免把「容量」以外的差异带进对照。
pub async fn connect_pair_<CA, CB, RA, WA, RB, WB, S>(
    scope: &S,
    rx_a: RA,
    tx_a: WA,
    rx_b: RB,
    tx_b: WB,
) -> (MuxConnection<CA, S>, MuxConnection<CB, S>)
where
    // 连接把 Rx / Tx 移交给 `'static` 的读写循环（`spawn_local` 要求 `'static`；
    // 本地投递**不要求** `Send`，因此 `!Send` 的传输也能直接当 `Rx` / `Tx`）。
    RA: TrBuffRead<u8> + 'static,
    WA: TrBuffWrite<u8> + 'static,
    RB: TrBuffRead<u8> + 'static,
    WB: TrBuffWrite<u8> + 'static,
    S: TrSmokeScope + Clone + 'static,
    // 两端的连接配置各自泛化：默认用例两侧都是 [`SmokeMuxConfig`]，「极小帧暂存」
    // 用例传入一对只把 `make_stage_buffs` 换成一字节缓冲的同构配置——两条子流环与
    // 其余策略完全一致，避免把「容量」以外的差异带进对照。
    CA: TrConnCfg<
            ConnRx = RA,
            ConnTx = WA,
            Alloc = CoreAlloc,
            Buff = SmokeBuff,
            StageBuff = SmokeBuff,
        > + Default
        + Clone
        + 'static,
    CB: TrConnCfg<
            ConnRx = RB,
            ConnTx = WB,
            Alloc = CoreAlloc,
            Buff = SmokeBuff,
            StageBuff = SmokeBuff,
        > + Default
        + Clone
        + 'static,
    CA::StageBuff: Send + Sync,
    CB::StageBuff: Send + Sync,
{
    let invite_opts = BasicOpts::default();
    let listen_opts = BasicOpts::default();
    let invite_fut = HandshakeAgent::new(rx_a, tx_a).invite_async(&invite_opts, AcceptAllEntries);
    let listen_fut = HandshakeAgent::new(rx_b, tx_b).listen_async(&listen_opts, AcceptAllEntries);
    let (invited, accepted) = futures::join!(async { invite_fut.await }, async { listen_fut.await });
    let delivery_a = invited.expect("发起方握手应当成功");
    let delivery_b = accepted.expect("等待方握手应当成功");

    // 两块连接级缓冲**由配置提供**（`TrConnCfg::make_stage_buffs`）：容量是连接级
    // 策略，测试装配不该在这里另写一份固定的 64 KiB。
    let config_a = CA::default();
    let config_b = CB::default();
    let (stage_ar, stage_aw) = config_a
        .make_stage_buffs(config_a.allocator())
        .expect("A 侧连接级帧暂存应当分配成功");
    let (stage_br, stage_bw) = config_b
        .make_stage_buffs(config_b.allocator())
        .expect("B 侧连接级帧暂存应当分配成功");
    (
        MuxConnection::new(scope, delivery_a, config_a, stage_ar, stage_aw),
        MuxConnection::new(scope, delivery_b, config_b, stage_br, stage_bw),
    )
}

/// 绑定独占性场景：验证 [`TrConnection::bind_async`] 对同一个 `local_dock` 拒绝
/// 第二次绑定，且丢弃 binding 后可以重绑。
///
/// 只做本地注册表行为验证，不建子流、不交换业务字节；但**必须**先完成握手并建出
/// 两个真实 `MuxConnection`，因为 binding 是连接对象上的东西。
///
/// # Panics
///
/// 握手失败、绑定出现的错误类型不是 `DockInUse`、或解绑后重绑失败都会 panic。
///
/// [`TrConnection::bind_async`]: abs_smux::conn::TrConnection::bind_async
pub async fn run_bind_exclusivity_scenario_<RA, WA, RB, WB, S>(
    scope: &S,
    rx_a: RA,
    tx_a: WA,
    rx_b: RB,
    tx_b: WB,
) where
    RA: TrBuffRead<u8> + 'static,
    WA: TrBuffWrite<u8> + 'static,
    RB: TrBuffRead<u8> + 'static,
    WB: TrBuffWrite<u8> + 'static,
    S: TrSmokeScope + Clone + 'static,
{
    let (conn_a, conn_b) =
        connect_pair_::<
            SmokeMuxConfig<WA, RA>,
            SmokeMuxConfig<WB, RB>,
            RA,
            WA,
            RB,
            WB,
            S,
        >(scope, rx_a, tx_a, rx_b, tx_b)
        .await;

    // 探测用的 dock 取值远离收发场景用的 `1..=16` 与 `0x1000..`，避免歧义。
    let dock = Dock::new(0x2000u32);
    let other = Dock::new(0x2001u32);

    // 1) 首次绑定成功；同一 dock 第二次绑定必须报 `DockInUse`，而不是又发一个
    //    binding。
    let first = conn_a.bind_async(dock).await.expect("首次绑定应当成功");
    assert!(
        matches!(
            conn_a.bind_async(dock).await,
            Result::Err(BindError::DockInUse)
        ),
        "同一 local_dock 第二次 bind_async 应当报 DockInUse"
    );

    // 2) 另一个 dock 不受影响。
    let second = conn_a
        .bind_async(other)
        .await
        .expect("不同 local_dock 应当可以绑定");

    // 3) 丢弃 binding 即解绑，之后同一个 dock 可以重绑。
    drop(first);
    let third = conn_a
        .bind_async(dock)
        .await
        .expect("解绑后应当可以重新绑定");

    // 4) 绑定是**每条连接**独立的状态：对端用同一个 dock 值不受本端影响。
    let peer_binding = conn_b
        .bind_async(dock)
        .await
        .expect("另一条连接上的同名 local_dock 应当可以绑定");

    drop((second, third, peer_binding));
}


/// 半建立句柄的收尾场景：验证「最终裁决之前丢弃句柄」在两个角色下都**不留垃圾、
/// 不悬着对端**。
///
/// - 手段：两条内存环直连并完成握手，取两个真实 `MuxConnection`；A 侧绑定一个
///   dock、B 侧在同一 dock 上 `listen_async`。然后跑两小段：
///   1. A **未 accept 就 drop** 发起方句柄，随即用**同一个 dock 对**再开一次，这次
///      两侧都正常 `accept_async`；
///   2. A 发起并 `accept_async`（会等对端裁决），同时 B 取到入向句柄后**未裁决就
///      drop**。
///
///   整个场景由 `scope.run_until` 驱动。
/// - 判断：第 1 段中第二次 `open_channel_async` 必须**立刻成功**——若丢弃发起方句柄
///   只是把它放进拆流宽限期，这里会报 `WaitClose`/`Duplicate`（发起方此刻还没发过
///   `OPEN`，对端不可能有在途帧，所以理应立刻可复用）；第 2 段中 A 的
///   `accept_async` 必须返回 `HandleError::Refused`——若丢弃响应方句柄不发 `REJECT`，
///   A 会永远悬着，测试会超时。任一不满足即 panic。
///
/// # Panics
///
/// 握手/绑定/监听失败，或上述两条判断不成立，都会 panic。
pub async fn run_unsettled_handle_scenario_<RA, WA, RB, WB, S>(
    scope: &S,
    rx_a: RA,
    tx_a: WA,
    rx_b: RB,
    tx_b: WB,
) where
    RA: TrBuffRead<u8> + 'static,
    WA: TrBuffWrite<u8> + 'static,
    RB: TrBuffRead<u8> + 'static,
    WB: TrBuffWrite<u8> + 'static,
    S: TrSmokeScope + Clone + 'static,
{
    let (conn_a, conn_b) =
        connect_pair_::<
            SmokeMuxConfig<WA, RA>,
            SmokeMuxConfig<WB, RB>,
            RA,
            WA,
            RB,
            WB,
            S,
        >(scope, rx_a, tx_a, rx_b, tx_b)
        .await;

    // 探测用的 dock 取值远离收发场景用的 `1..=16` 与 `0x1000..`，避免歧义。
    let local_a = Dock::new(0x3000u32);
    let remote_b = Dock::new(0x3001u32);

    let mut binding_a = conn_a
        .bind_async(local_a)
        .await
        .expect("A 侧绑定应当成功");
    let mut listener_b = conn_b
        .bind_async(remote_b)
        .await
        .expect("B 侧绑定应当成功")
        .listen_async()
        .await
        .expect("B 侧开始监听应当成功");

    // -- 第 1 段：发起方未裁决就丢弃 ⇒ 登记被彻底撤销，同一 dock 对立刻可复用。
    let mut message: &[u8] = &[];
    let abandoned = binding_a
        .open_channel_async(remote_b, &mut message)
        .await
        .expect("第一次发起应当成功");
    drop(abandoned);

    let mut again = binding_a
        .open_channel_async(remote_b, &mut message)
        .await
        .expect("撤销登记后，同一个 dock 对应当立刻可以再次发起");
    let mut welcome_buf: [u8; 0] = [];
    let mut welcome: &mut [u8] = &mut welcome_buf[..];
    let (opened, incoming) = futures::join!(
        async {
            again
                .accept_async_closure(&mut welcome, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
        },
        async {
            let mut handle = listener_b
                .income_async()
                .await
                .expect("B 侧应当取到第二次发起的请求");
            let mut peer_welcome_buf: [u8; 0] = [];
            let mut peer_welcome: &mut [u8] = &mut peer_welcome_buf[..];
            handle
                .accept_async_closure(&mut peer_welcome, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
        },
    );
    let (tx, rx) = opened.expect("A 侧最终裁决应当成功");
    let (peer_tx, peer_rx) = incoming.expect("B 侧最终裁决应当成功");
    drop((tx, rx, peer_tx, peer_rx));

    // -- 第 2 段：响应方未裁决就丢弃 ⇒ 主动方必须收到 `Refused`，而不是悬着。
    //
    // 换一个 dock 对：上一条子流已经拆掉，同一 dock 对会处于拆流宽限期。
    let remote_c = Dock::new(0x3002u32);
    let mut listener_c = conn_b
        .bind_async(remote_c)
        .await
        .expect("B 侧再绑定一个 dock 应当成功")
        .listen_async()
        .await
        .expect("B 侧第二个监听应当成功");
    let mut message2: &[u8] = &[];
    let mut waiter = binding_a
        .open_channel_async(remote_c, &mut message2)
        .await
        .expect("第二次发起应当成功");
    let mut welcome2_buf: [u8; 0] = [];
    let mut welcome2: &mut [u8] = &mut welcome2_buf[..];
    let (verdict, ()) = futures::join!(
        async {
            waiter
                .accept_async_closure(&mut welcome2, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
        },
        async {
            let handle = listener_c
                .income_async()
                .await
                .expect("B 侧应当取到第三次发起的请求");
            // 未裁决就丢弃：按契约应当给对端补一条 `REJECT`。
            drop(handle);
        },
    );
    assert!(
        matches!(verdict, Result::Err(HandleError::Refused)),
        "响应方丢弃待决句柄后，主动方的 accept_async 应当得到 Refused"
    );
}

/// 场景：**拒绝接受环内存**（容量不足以建环）时连接的行为。
///
/// - 目标：连接对「不合约的内存」只**拒绝接受**，不替调用方改尺寸，也不因此拆掉连接。
/// - 手段：A 发起一条子流到 `remote_b`；B 取到待决句柄后用容量 `0` 的缓冲裁决
///   （环的下限是 `1`），A 用正常容量的缓冲裁决；随后在**同一条连接**上再走一遍
///   正常的建流。
/// - 判断：B 侧裁决必须得到 [`HandleError::RingRejected`]；A 侧必须得到
///   [`HandleError::Refused`]（拒绝发生在发出任何帧之前，B 按角色补 `REJECT`）；
///   随后的那条子流必须成功——若拒绝把连接或注册表弄脏了，这一段会失败。
pub async fn run_ring_rejected_scenario_<RA, WA, RB, WB, S>(
    scope: &S,
    rx_a: RA,
    tx_a: WA,
    rx_b: RB,
    tx_b: WB,
) where
    RA: TrBuffRead<u8> + 'static,
    WA: TrBuffWrite<u8> + 'static,
    RB: TrBuffRead<u8> + 'static,
    WB: TrBuffWrite<u8> + 'static,
    S: TrSmokeScope + Clone + 'static,
{
    let (conn_a, conn_b) =
        connect_pair_::<
            SmokeMuxConfig<WA, RA>,
            SmokeMuxConfig<WB, RB>,
            RA,
            WA,
            RB,
            WB,
            S,
        >(scope, rx_a, tx_a, rx_b, tx_b)
        .await;

    let local_a = Dock::new(0x3100u32);
    let remote_b = Dock::new(0x3101u32);
    let mut binding_a = conn_a
        .bind_async(local_a)
        .await
        .expect("A 侧绑定应当成功");
    let mut listener_b = conn_b
        .bind_async(remote_b)
        .await
        .expect("B 侧绑定应当成功")
        .listen_async()
        .await
        .expect("B 侧开始监听应当成功");

    // -- 第 1 段：B 给一块容量 0 的缓冲 ⇒ 必须被拒绝。
    let mut message: &[u8] = &[];
    let mut handle_a = binding_a
        .open_channel_async(remote_b, &mut message)
        .await
        .expect("发起应当成功");
    let mut welcome_buf: [u8; 0] = [];
    let mut welcome: &mut [u8] = &mut welcome_buf[..];
    let (verdict_a, verdict_b) = futures::join!(
        async {
            handle_a
                .accept_async_closure(&mut welcome, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
        },
        async {
            let mut handle_b = listener_b
                .income_async()
                .await
                .expect("B 侧应当取到发起请求");
            let mut peer_welcome_buf: [u8; 0] = [];
            let mut peer_welcome: &mut [u8] = &mut peer_welcome_buf[..];
            handle_b
                .accept_async_closure(&mut peer_welcome, || {
                    // 容量 0 < 环下限 1 ⇒ 连接应当拒绝这两份内存。
                    (make_channel_buff_with_(0), make_channel_buff_with_(0))
                })
                .await
        },
    );
    assert!(
        matches!(verdict_b, Result::Err(HandleError::RingRejected)),
        "容量 0 不足以建环时，accept_async 必须报 RingRejected"
    );
    assert!(
        matches!(verdict_a, Result::Err(HandleError::Refused)),
        "响应方拒绝接受内存后，主动方应当收到 Refused"
    );

    // -- 第 2 段：同一条连接上正常建流仍然成功（拒绝没有弄脏连接 / 注册表）。
    let remote_c = Dock::new(0x3102u32);
    let mut listener_c = conn_b
        .bind_async(remote_c)
        .await
        .expect("B 侧再绑定一个 dock 应当成功")
        .listen_async()
        .await
        .expect("B 侧第二个监听应当成功");
    let mut message2: &[u8] = &[];
    let mut handle_a2 = binding_a
        .open_channel_async(remote_c, &mut message2)
        .await
        .expect("第二次发起应当成功");
    let (a_done, b_done) = futures::join!(
        async {
            let mut welcome2_buf: [u8; 0] = [];
            let mut welcome2: &mut [u8] = &mut welcome2_buf[..];
            let (tx, mut rx) = handle_a2
                .accept_async_closure(&mut welcome2, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
                .expect("拒绝之后，正常建流应当仍然成功");
            exchange_and_half_close_(tx, &mut rx, 0x3103u32, 0usize).await;
        },
        async {
            let mut handle_b2 = listener_c
                .income_async()
                .await
                .expect("B 侧应当取到第二次发起");
            let mut peer_welcome_buf: [u8; 0] = [];
            let mut peer_welcome: &mut [u8] = &mut peer_welcome_buf[..];
            let (tx, mut rx) = handle_b2
                .accept_async_closure(&mut peer_welcome, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
                .expect("B 侧正常建流应当成功");
            exchange_and_half_close_(tx, &mut rx, 0x3103u32, 0usize).await;
        },
    );
    let _ = (a_done, b_done);
}

/// 逐条子流自行分配场景：**同一条连接**（一种声明的环存储类型）上，两条子流各自
/// 决定「分配多少、从哪块内存来」。
///
/// - 手段：完成握手得到两个连接；两条子流的 `accept_async` 各自在闭包里现造缓冲
///   （[`leak_buff_`]），容量分别是 4096 与 8192。
/// - 判断：两条子流都建立成功、载荷逐字节相符、半关闭后读到 EOF（两条的接收窗口与
///   环大小不同，因此这也顺带验证「容量是逐条的」）。
///
/// # Panics
///
/// 握手 / 绑定 / 监听 / 交互任一环节失败都会 panic。
pub async fn run_per_channel_alloc_scenario_<RA, WA, RB, WB, S>(
    scope: &S,
    rx_a: RA,
    tx_a: WA,
    rx_b: RB,
    tx_b: WB,
) where
    RA: TrBuffRead<u8> + 'static,
    WA: TrBuffWrite<u8> + 'static,
    RB: TrBuffRead<u8> + 'static,
    WB: TrBuffWrite<u8> + 'static,
    S: TrSmokeScope + Clone + 'static,
{
    let (conn_a, conn_b) =
        connect_pair_::<
            SmokeMuxConfig<WA, RA>,
            SmokeMuxConfig<WB, RB>,
            RA,
            WA,
            RB,
            WB,
            S,
        >(scope, rx_a, tx_a, rx_b, tx_b)
        .await;

    let local_a = Dock::new(0x4000u32);
    let dock_small = Dock::new(0x4001u32);
    let dock_large = Dock::new(0x4002u32);

    let mut binding_a = conn_a
        .bind_async(local_a)
        .await
        .expect("A 侧绑定应当成功");
    let mut listener_small = conn_b
        .bind_async(dock_small)
        .await
        .expect("B 侧绑定 dock_small 应当成功")
        .listen_async()
        .await
        .expect("B 侧监听 dock_small 应当成功");
    let mut listener_large = conn_b
        .bind_async(dock_large)
        .await
        .expect("B 侧绑定 dock_large 应当成功")
        .listen_async()
        .await
        .expect("B 侧监听 dock_large 应当成功");

    // -- 第一条：两侧各自按 4096 分配。
    let small = K_CHANNEL_CAPACITY;
    let (a_done, b_done) = futures::join!(
        async {
            let mut message: &[u8] = &[];
            let mut handle = binding_a
                .open_channel_async(dock_small, &mut message)
                .await
                .expect("发起第一条子流应当成功");
            let mut welcome_buf: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome_buf[..];
            let (tx, mut rx) = handle
                .accept_async_closure(&mut welcome, move || (make_channel_buff_with_(small), make_channel_buff_with_(small)))
                .await
                .expect("第一条子流裁决应当成功");
            exchange_and_half_close_(tx, &mut rx, 0x4001u32, 0usize).await;
        },
        async {
            let mut handle = listener_small
                .income_async()
                .await
                .expect("B 侧应当取到第一条子流");
            let mut welcome_buf: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome_buf[..];
            let (tx, mut rx) = handle
                .accept_async_closure(&mut welcome, move || (make_channel_buff_with_(small), make_channel_buff_with_(small)))
                .await
                .expect("B 侧第一条子流裁决应当成功");
            exchange_and_half_close_(tx, &mut rx, 0x4001u32, 0usize).await;
        },
    );
    let _ = (a_done, b_done);

    // -- 第二条：两侧各自按 8192 分配（**另一种容量**）。
    let large = K_CHANNEL_CAPACITY * 2;
    let (a_done, b_done) = futures::join!(
        async {
            let mut message: &[u8] = &[];
            let mut handle = binding_a
                .open_channel_async(dock_large, &mut message)
                .await
                .expect("发起第二条子流应当成功");
            let mut welcome_buf: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome_buf[..];
            let (tx, mut rx) = handle
                .accept_async_closure(&mut welcome, move || (make_channel_buff_with_(large), make_channel_buff_with_(large)))
                .await
                .expect("第二条子流裁决应当成功");
            exchange_and_half_close_(tx, &mut rx, 0x4002u32, 1usize).await;
        },
        async {
            let mut handle = listener_large
                .income_async()
                .await
                .expect("B 侧应当取到第二条子流");
            let mut welcome_buf: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome_buf[..];
            let (tx, mut rx) = handle
                .accept_async_closure(&mut welcome, move || (make_channel_buff_with_(large), make_channel_buff_with_(large)))
                .await
                .expect("B 侧第二条子流裁决应当成功");
            exchange_and_half_close_(tx, &mut rx, 0x4002u32, 1usize).await;
        },
    );
    let _ = (a_done, b_done);
}

/// 小场景里的 dock 数量与每个 dock 上的子流数量（`2 × 2 = 4` 条/端）。
pub const K_SMALL_DOCK_COUNT: u32 = 2;
pub const K_SMALL_CHANNELS_PER_DOCK: usize = 2;

/// 为一端（`side` = 0/1）跑完场景：在 `1..=dock_count` 上监听，同时向对端的
/// 同名 dock 发起 `dock_count × per_dock` 条子流（每条用不同的临时 local_dock）。
///
/// 发起侧的临时 dock 取值 `0x1000 + side·0x100_0000 + dock·0x100 + index`：
///
/// - 与监听 dock（`1..=dock_count`）天然不重叠；
/// - 同侧不同 `(dock, index)` 互不相同（`per_dock ≤ 0x100` 时不会串到下一个 dock），
///   满足 §4.1「每条并发子流一个互不相同的 local_dock」；
/// - `side` 抬高一整段，保证两侧的临时 dock 区间不重叠。
///
/// # Panics
///
/// 任何一次 open / accept / 读写 / 半关闭校验失败都会 panic——失败即测试失败。
async fn drive_side_<C, S>(
    conn: &MuxConnection<C, S>,
    side: u32,
    dock_count: u32,
    per_dock: usize,
) where
    C: TrConnCfg + TrMuxConfig<Buff = SmokeBuff>,
    S: Clone,
{
    let conn_ref = conn;

    let mut accept_tasks = Vec::new();
    for dock in 1..=dock_count {
        accept_tasks.push(async move {
            let mut binding = conn_ref
                .bind_async(Dock::new(dock))
                .await
                .expect("绑定监听 dock 应当成功");
            let mut listener = binding
                .listen_async()
                .await
                .expect("在本地 dock 上建立 listener 应当成功");
            for index in 0..per_dock {
                let mut handle = listener
                    .income_async()
                    .await
                    .expect("应当取到下一条入向建流请求");
                assert_eq!(
                    handle.local_dock(),
                    Dock::new(dock),
                    "被动方的 local_dock 应当是监听 dock（镜像语义）"
                );
                let mut welcome_buf: [u8; 0] = [];
                let mut welcome: &mut [u8] = &mut welcome_buf[..];
                let (tx, mut rx) = handle
                    .accept_async_closure(&mut welcome, || {
                        (make_channel_buff_(), make_channel_buff_())
                    })
                    .await
                    .expect("accept 入向子流应当成功");
                exchange_and_half_close_(tx, &mut rx, dock, index).await;
            }
        });
    }

    let mut open_tasks = Vec::new();
    for dock in 1..=dock_count {
        for index in 0..per_dock {
            // 每条并发子流一个**互不相同**的临时 local_dock（Q2 裁决）。
            let local = Dock::new(
                0x1000u32 + side * 0x100_0000u32 + dock * 0x100u32 + index as u32,
            );
            open_tasks.push(async move {
                let mut binding = conn_ref
                    .bind_async(local)
                    .await
                    .expect("绑定发起 dock 应当成功");
                let mut message: &[u8] = &[];
                // `open_channel_async` 现在只交出**半建立**句柄：本端 `OPEN` 要
                // 等最终裁决（`accept_async`）时拿到缓冲后才发。
                let mut handle = binding
                    .open_channel_async(Dock::new(dock), &mut message)
                    .await
                    .expect("向对端 dock 发起子流应当成功");
                let mut welcome_buf: [u8; 0] = [];
                let mut welcome: &mut [u8] = &mut welcome_buf[..];
                let (tx, mut rx) = handle
                    .accept_async_closure(&mut welcome, || {
                        (make_channel_buff_(), make_channel_buff_())
                    })
                    .await
                    .expect("发起方最终裁决子流应当成功");
                exchange_and_half_close_(tx, &mut rx, dock, index).await;
            });
        }
    }

    futures::join!(
        futures::future::join_all(open_tasks),
        futures::future::join_all(accept_tasks),
    );
}

/// 一条子流上的完整交互：写本端载荷 → 读对端载荷并校验 → 丢弃发送半边（半关闭）
/// → 在接收半边等到 EOF。
///
/// 载荷校验**不依赖 open / accept 的配对顺序**：先读 4 字节 tag，tag 里编码了发送方
/// 的 `(dock, index)`，据此推出对方的完整载荷再逐字节比对。这样即使两侧的临时
/// dock 分配与 accept 顺序不同，校验依然成立。
async fn exchange_and_half_close_<T, R>(tx: T, rx: &mut R, dock: u32, index: usize)
where
    T: TrBuffWrite<u8>,
    R: TrBuffRead<u8>,
{
    let payload = make_payload_(dock, index);
    let mut tx = tx;
    write_channel_all_(&mut tx, &payload)
        .await
        .expect("子流写入本端载荷应当成功");

    let mut tag = [0u8; 4];
    read_channel_exact_(rx, &mut tag)
        .await
        .expect("子流读取对端载荷 tag 应当成功");
    let raw = u32::from_be_bytes(tag);
    let expected = make_payload_(raw >> 16, (raw & 0xFFFF) as usize);
    assert_eq!(tag, expected[..4], "对端载荷 tag 应当自洽");

    let mut rest = vec![0u8; expected.len() - 4];
    read_channel_exact_(rx, &mut rest)
        .await
        .expect("子流读取对端载荷正文应当成功");
    let got = [tag.as_slice(), rest.as_slice()].concat();
    assert_eq!(got, expected, "对端载荷应逐字节相等");

    // 半关闭：丢弃发送半边（= 发 FIN），对端应当在读尽后看到 EOF。
    drop(tx);
    expect_eof_(rx).await;
}

/// 在接收半边等到 EOF（对端 `FIN` 生效）。
///
/// 先尝试读 1 字节：写端关闭且已排空时应当返回 `ConsumerError::Closing`；若对端
/// 的 `FIN` 还没到，`read_async` 会 park 到它到达为止（这正是要验证的行为）。
///
/// 对外可见的理由同 `write_channel_all_`：分层 RPC 用例（`tests/layered_rpc.rs`）
/// 需要同一套「半关闭 → 等 EOF」收尾语义，不该在第二个文件里重写一遍。
pub async fn expect_eof_<R>(rx: &mut R)
where
    R: TrBuffRead<u8>,
{
    let demand = Demand::exactly(1usize);
    let mut outcome = rx.read_async(&demand).await;
    match outcome.as_mut().pick_left() {
        Option::Some(segm) => {
            if segm.least_count() > 0 {
                panic!("半关闭之后仍然读到了数据");
            }
        }
        Option::None => {
            let err = outcome
                .pick_right()
                .expect("IO 结果必须要么是段、要么是错误");
            let tag: ReadErrTag = err.err_tag();
            assert!(
                tag == ReadErrTag::Closing,
                "半关闭之后应当读到 Closing（EOF），实际是 {tag:?}"
            );
        }
    }
}

/// 依据 `(dock, index)` 生成确定性载荷。
///
/// 布局：4 字节大端 tag（`dock << 16 | index`）+ 变长模式体；模式体第 `i` 字节为
/// `((tag + i * 7) % 251) as u8`，长度取 `(index * 37) % 2000 + 8`，因此同一 dock
/// 内不同序号的载荷互不相同，也能覆盖「一次写段的容量边界」。
pub fn make_payload_(dock: u32, index: usize) -> Vec<u8> {
    let tag = (dock << 16) | (index as u32 & 0xFFFF);
    let body_len = (index * 37) % 2000 + 8;
    let mut out = Vec::with_capacity(4 + body_len);
    out.extend_from_slice(&tag.to_be_bytes());
    for i in 0..body_len {
        out.push(((tag as usize).wrapping_add(i * 7) % 251) as u8);
    }
    out
}

/// 把 `bytes` 全量写入子流发送半边。
///
/// 与 `abs_buff` 的段语义一致：每次按剩余长度借段、把实际写入量计入段偏移、
/// drop 段提交，直到写完。
///
/// 对外可见是为了让**分层 RPC** 用例（`tests/layered_rpc.rs`）复用同一份搬运
/// 代码：读方向必须用 `unsafe` 的 `move_items_to_buff` 才能把段搬进字节缓冲，
/// 全测试套件只该有一份这样的代码与一份 SAFETY 论证（见 `read_channel_exact_`）。
pub async fn write_channel_all_<W>(tx: &mut W, bytes: &[u8]) -> Result<(), W::Err>
where
    W: TrBuffWrite<u8>,
{
    let mut offset = 0usize;
    while offset < bytes.len() {
        let rest = bytes.len() - offset;
        let demand = Demand::exactly(rest);
        let mut outcome = tx.write_async(&demand).await;
        let put;
        {
            let segm = outcome.as_mut().pick_left();
            match segm {
                Some(segm) => {
                    put = segm.as_segm_mut().clone_items_from_buff(&bytes[offset..]);
                }
                None => {
                    return Err(outcome
                        .pick_right()
                        .expect("IO 结果必须要么是段、要么是错误"));
                }
            }
        }
        if put == 0usize {
            // 段为空说明发送方向已经关闭，继续循环只会自旋。
            break;
        }
        offset += put;
    }
    Ok(())
}

/// 从子流接收半边读满 `out`。
///
/// 与 `write_channel_all_` 对称：段可能比请求更长，只搬走需要的前缀，剩余字节在
/// 段回收时归还缓冲。可见性与 `write_channel_all_` 同理。
pub async fn read_channel_exact_<R>(rx: &mut R, out: &mut [u8]) -> Result<(), R::Err>
where
    R: TrBuffRead<u8>,
{
    let mut offset = 0usize;
    while offset < out.len() {
        let rest = out.len() - offset;
        let demand = Demand::exactly(rest);
        let mut outcome = rx.read_async(&demand).await;
        let got;
        {
            let segm = outcome.as_mut().pick_left();
            match segm {
                Some(segm) => {
                    let mut child = segm.as_segm_ref();
                    let limit = core::cmp::min(rest, child.least_count());
                    let dst = &mut out[offset..offset + limit];
                    // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同、对齐相同（均为
                    // 1），且 `dst` 是本函数独占的可写切片；`move_items_to_buff`
                    // 只会写入其中已初始化的前缀（返回值给出长度）。
                    let uninit = unsafe {
                        core::slice::from_raw_parts_mut(
                            dst.as_mut_ptr() as *mut MaybeUninit<u8>,
                            dst.len(),
                        )
                    };
                    got = unsafe { child.move_items_to_buff(uninit) };
                }
                None => {
                    return Err(outcome
                        .pick_right()
                        .expect("IO 结果必须要么是段、要么是错误"));
                }
            }
        }
        if got == 0usize {
            break;
        }
        offset += got;
    }
    Ok(())
}

/// 场景主体：握手 → 建立复用连接 → 并发驱动连接与全部子流（`16 dock × 64 条`）。
///
/// 参数是两端的 `Rx` / `Tx`（`A` 端发起握手，`B` 端等待）。连接接管 `Rx` / `Tx`
/// 后在内部自行 spawn 读写循环，本函数只是 [`run_mux_scenario_`] 在
/// `K_DOCK_COUNT × K_CHANNELS_PER_DOCK` 规模下的特例。
///
/// # Panics
///
/// 握手失败、任意一次 open / accept / 读写 / 半关闭校验失败，或读写会话在场景完成
/// 前退出时 panic——本函数是测试专用，失败即测试失败。
pub async fn run_smoke_scenario_<RA, WA, RB, WB, S>(
    scope: &S,
    rx_a: RA,
    tx_a: WA,
    rx_b: RB,
    tx_b: WB,
) where
    // 连接把 Rx / Tx 移交给 `'static` 的读写循环（`spawn_local` 要求 `'static`；
    // 本地投递不要求 `Send`）。
    RA: TrBuffRead<u8> + 'static,
    WA: TrBuffWrite<u8> + 'static,
    RB: TrBuffRead<u8> + 'static,
    WB: TrBuffWrite<u8> + 'static,
    S: TrSmokeScope + Clone + 'static,
{
    run_mux_scenario_::<_, _, _, _, S>(
        scope,
        rx_a,
        tx_a,
        rx_b,
        tx_b,
        K_DOCK_COUNT,
        K_CHANNELS_PER_DOCK,
    )
    .await
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 同一份场景在两个运行时下各跑一遍：只把「设备类型」留在各自的测试 target 里
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// **tokio** 版：建立一条已注册进运行时的 UNIX socket 对、装配好四条调用方驱动的
/// 泵，然后在给定作用域上跑 `scenario`。
///
/// 与下面的 compio 版**逐字同构**，只有「设备类型 + 适配 crate」不同；两个运行时
/// 因此共用同一份场景（[`run_smoke_scenario_`] / [`run_small_mux_scenario_`]），
/// 不再各写一个测试文件。
#[cfg(feature = "test-tokio-runtime")]
pub async fn run_socket_scenario_on_runtime_<F, Fut>(
    scope: &abs_art_tokio::LocalScope,
    scenario: F,
) where
    F: FnOnce(
        smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
    ) -> Fut,
    Fut: core::future::Future<Output = ()>,
{
    use buffex_tokio_adapt::x_deps::abs_buff_tokio_adapt::{ReadAsInput, WriteAsOutput};
    use tokio::net::UnixStream;

    let (stream_a, stream_b) = UnixStream::pair().expect("建立 tokio UNIX socket 对应当成功");
    let (mut a_read, mut a_write) = stream_a.into_split();
    let (mut b_read, mut b_write) = stream_b.into_split();

    let fut = run_socket_scenario_with_(
        ReadAsInput::new(&mut a_read),
        WriteAsOutput::new(&mut a_write),
        ReadAsInput::new(&mut b_read),
        WriteAsOutput::new(&mut b_write),
        scenario,
    );
    scope.run_until(fut).await;
}

/// **compio** 版：0.19 的 `UnixStream` 没有 `pair()`，因此先建 `std` socket 对再
/// 逐个 `from_std` 注册进当前运行时。其余与 tokio 版逐字同构。
#[cfg(not(feature = "test-tokio-runtime"))]
pub async fn run_socket_scenario_on_runtime_<F, Fut>(
    scope: &abs_art_compio::LocalScope,
    scenario: F,
) where
    F: FnOnce(
        smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedRx<SmokeBuff, CoreAlloc>,
        smux_v1::connection::BufferedTx<SmokeBuff, CoreAlloc>,
    ) -> Fut,
    Fut: core::future::Future<Output = ()>,
{
    use buffex_compio_adapt::{ReadAsInput, WriteAsOutput};

    let (std_a, std_b) =
        std::os::unix::net::UnixStream::pair().expect("建立 std UNIX socket 对应当成功");
    let stream_a =
        compio::net::UnixStream::from_std(std_a).expect("a 端应能注册到 compio 运行时");
    let stream_b =
        compio::net::UnixStream::from_std(std_b).expect("b 端应能注册到 compio 运行时");
    let (mut a_read, mut a_write) = stream_a.into_split();
    let (mut b_read, mut b_write) = stream_b.into_split();

    let fut = run_socket_scenario_with_(
        ReadAsInput::new(&mut a_read),
        WriteAsOutput::new(&mut a_write),
        ReadAsInput::new(&mut b_read),
        WriteAsOutput::new(&mut b_write),
        scenario,
    );
    scope.run_until(fut).await;
}
