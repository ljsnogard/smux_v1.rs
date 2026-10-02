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
//! > 那与 §4.1 冲突（第 2 条 `OPEN` 会被 `MuxError::Duplicate` 拒绝）。该冲突的裁决
//! > 是「改测试、协议不动」，见 `dev-notes/connection-20261002-0548.md` §6.5 Q2。
//!
#![allow(dead_code)] // 三个测试 target 各自只用到本模块的一部分。

use core::mem::MaybeUninit;

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
use abs_smux::conn::{
    TrChannelHandle, TrChannelHalf, TrChannelListener, TrConnection, TrDockBinding,
};
use smux_v1::{
    connection::{Dock, MuxConnection, MuxError, TrMuxConfig},
    flow_ctrl::DefaultPolicy,
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
};

/// 连接层要求的运行时 bound：与 `smux_v1` 的 `multi-thread` feature 保持一致。
///
/// 缺省（单线程）下 `MuxConnection::new` 要求 `Rt: TrSpawnLocal`，开启
/// `multi-thread` 后要求 `Rt: TrSpawnSend`。两个测试目标传入的具体运行时都同时
/// 具备这两种能力，因此同一份场景代码在两种 feature 配置下都能编译。
#[cfg(not(feature = "multi-thread"))]
pub use abs_art::TrSpawnLocal as TrSmokeRuntime;
#[cfg(feature = "multi-thread")]
pub use abs_art::TrSpawnSend as TrSmokeRuntime;

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
/// [`DefaultPolicy`] 流控。
pub struct SmokeMuxConfig;

/// [`DefaultPolicy`] 是 ZST；取静态引用即可满足 `TrMuxConfig::policy`。
static SMOKE_POLICY: DefaultPolicy = DefaultPolicy;

impl TrMuxConfig for SmokeMuxConfig {
    type Buff = Owned<[MaybeUninit<u8>], CoreAlloc>;
    type Alloc = CoreAlloc;
    type Policy = DefaultPolicy;

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn policy(&self) -> &Self::Policy {
        &SMOKE_POLICY
    }

    fn channel_capacity(&self) -> usize {
        K_CHANNEL_CAPACITY
    }

    fn make_buff(&self, len: usize) -> Self::Buff {
        Owned::new_uninit_slice(len, CoreAlloc)
    }
}

/// 环存储的具体类型（元素 `u8` + `CoreAlloc`），避免类型推断歧义。
type SmokeBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

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
pub async fn run_socket_scenario_<IA, OA, IB, OB, Rt>(
    input_a: IA,
    output_a: OA,
    input_b: IB,
    output_b: OB,
) where
    IA: TrInput<u8>,
    OA: TrOutput<u8>,
    IB: TrInput<u8>,
    OB: TrOutput<u8>,
    Rt: TrSmokeRuntime,
{
    run_socket_scenario_with_(input_a, output_a, input_b, output_b, |a_rx, a_tx, b_rx, b_tx| {
        run_smoke_scenario_::<_, _, _, _, Rt>(a_rx, a_tx, b_rx, b_tx)
    })
    .await
}

/// 与 [`run_socket_scenario_`] 相同的传输装配，但跑**本轮验收用的小场景**
/// （2 dock × 各 2 条 channel，双向收发 + 半关闭），见 [`run_small_mux_scenario_`]。
pub async fn run_small_socket_scenario_<IA, OA, IB, OB, Rt>(
    input_a: IA,
    output_a: OA,
    input_b: IB,
    output_b: OB,
) where
    IA: TrInput<u8>,
    OA: TrOutput<u8>,
    IB: TrInput<u8>,
    OB: TrOutput<u8>,
    Rt: TrSmokeRuntime,
{
    run_socket_scenario_with_(
        input_a,
        output_a,
        input_b,
        output_b,
        |a_rx, a_tx, b_rx, b_tx| {
            run_small_mux_scenario_::<_, _, _, _, Rt>(a_rx, a_tx, b_rx, b_tx)
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
pub async fn run_small_mux_scenario_<RA, WA, RB, WB, Rt>(
    rx_a: RA,
    tx_a: WA,
    rx_b: RB,
    tx_b: WB,
) where
    RA: TrBuffRead<u8> + Send + 'static,
    WA: TrBuffWrite<u8> + Send + 'static,
    RB: TrBuffRead<u8> + Send + 'static,
    WB: TrBuffWrite<u8> + Send + 'static,
    Rt: TrSmokeRuntime,
{
    run_mux_scenario_::<_, _, _, _, Rt>(
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
async fn run_mux_scenario_<RA, WA, RB, WB, Rt>(
    rx_a: RA,
    tx_a: WA,
    rx_b: RB,
    tx_b: WB,
    dock_count: u32,
    per_dock: usize,
) where
    RA: TrBuffRead<u8> + Send + 'static,
    WA: TrBuffWrite<u8> + Send + 'static,
    RB: TrBuffRead<u8> + Send + 'static,
    WB: TrBuffWrite<u8> + Send + 'static,
    Rt: TrSmokeRuntime,
{
    let (conn_a, conn_b) = connect_pair_::<RA, WA, RB, WB, Rt>(rx_a, tx_a, rx_b, tx_b).await;

    futures::join!(
        drive_side_(&conn_a, 0u32, dock_count, per_dock),
        drive_side_(&conn_b, 1u32, dock_count, per_dock),
    );
}

/// 握手（A 端发起、B 端等待）并由交付物建立两个 [`MuxConnection`]。
///
/// 抽出来给「收发场景」与「绑定独占性场景」共用，保证两者走的是**同一套**
/// 连接建立路径。
async fn connect_pair_<RA, WA, RB, WB, Rt>(
    rx_a: RA,
    tx_a: WA,
    rx_b: RB,
    tx_b: WB,
) -> (
    MuxConnection<RA, WA, SmokeMuxConfig, Rt>,
    MuxConnection<RB, WB, SmokeMuxConfig, Rt>,
)
where
    // 连接内部把 Rx / Tx 移交给 `'static` 的读写循环（`abs_art` 的 spawn 要求）。
    RA: TrBuffRead<u8> + Send + 'static,
    WA: TrBuffWrite<u8> + Send + 'static,
    RB: TrBuffRead<u8> + Send + 'static,
    WB: TrBuffWrite<u8> + Send + 'static,
    Rt: TrSmokeRuntime,
{
    let invite_opts = BasicOpts::default();
    let listen_opts = BasicOpts::default();
    let invite_fut = HandshakeAgent::new(rx_a, tx_a).invite_async(&invite_opts, AcceptAllEntries);
    let listen_fut = HandshakeAgent::new(rx_b, tx_b).listen_async(&listen_opts, AcceptAllEntries);
    let (invited, accepted) = futures::join!(async { invite_fut.await }, async { listen_fut.await });
    let delivery_a = invited.expect("发起方握手应当成功");
    let delivery_b = accepted.expect("等待方握手应当成功");

    (
        MuxConnection::<RA, WA, SmokeMuxConfig, Rt>::new(delivery_a, SmokeMuxConfig),
        MuxConnection::<RB, WB, SmokeMuxConfig, Rt>::new(delivery_b, SmokeMuxConfig),
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
pub async fn run_bind_exclusivity_scenario_<RA, WA, RB, WB, Rt>(
    rx_a: RA,
    tx_a: WA,
    rx_b: RB,
    tx_b: WB,
) where
    RA: TrBuffRead<u8> + Send + 'static,
    WA: TrBuffWrite<u8> + Send + 'static,
    RB: TrBuffRead<u8> + Send + 'static,
    WB: TrBuffWrite<u8> + Send + 'static,
    Rt: TrSmokeRuntime,
{
    let (conn_a, conn_b) = connect_pair_::<RA, WA, RB, WB, Rt>(rx_a, tx_a, rx_b, tx_b).await;

    // 探测用的 dock 取值远离收发场景用的 `1..=16` 与 `0x1000..`，避免歧义。
    let dock = Dock::new(0x2000u32);
    let other = Dock::new(0x2001u32);

    // 1) 首次绑定成功；同一 dock 第二次绑定必须报 `DockInUse`，而不是又发一个
    //    binding。
    let first = conn_a.bind_async(dock).await.expect("首次绑定应当成功");
    assert!(
        matches!(
            conn_a.bind_async(dock).await,
            Result::Err(MuxError::DockInUse)
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
async fn drive_side_<R, W, C, Rt>(
    conn: &MuxConnection<R, W, C, Rt>,
    side: u32,
    dock_count: u32,
    per_dock: usize,
) where
    R: TrBuffRead<u8>,
    W: TrBuffWrite<u8>,
    C: TrMuxConfig,
    Rt: TrSmokeRuntime,
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
                    .accept_async(&mut welcome)
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
                let (tx, mut rx) = binding
                    .open_channel_async(Dock::new(dock), &mut message)
                    .await
                    .expect("向对端 dock 发起子流应当成功");
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
async fn expect_eof_<R>(rx: &mut R)
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
async fn write_channel_all_<W>(tx: &mut W, bytes: &[u8]) -> Result<(), W::Err>
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
/// 段回收时归还缓冲。
async fn read_channel_exact_<R>(rx: &mut R, out: &mut [u8]) -> Result<(), R::Err>
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
pub async fn run_smoke_scenario_<RA, WA, RB, WB, Rt>(
    rx_a: RA,
    tx_a: WA,
    rx_b: RB,
    tx_b: WB,
) where
    // 连接内部把 Rx / Tx 移交给 `'static` 的读写循环（`abs_art` 的 spawn 要求）；
    // `Send` 是 `multi-thread` 配置下 `Rt::spawn` 的要求（测试用的环半部本就 `Send`）。
    RA: TrBuffRead<u8> + Send + 'static,
    WA: TrBuffWrite<u8> + Send + 'static,
    RB: TrBuffRead<u8> + Send + 'static,
    WB: TrBuffWrite<u8> + Send + 'static,
    Rt: TrSmokeRuntime,
{
    run_mux_scenario_::<_, _, _, _, Rt>(
        rx_a,
        tx_a,
        rx_b,
        tx_b,
        K_DOCK_COUNT,
        K_CHANNELS_PER_DOCK,
    )
    .await
}
