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
//! # 关于「同一 dock 上 64 条子流」的约定
//!
//! 帧头（`connection::frame_`）只有 `LocalDock` / `RemoteDock`，**没有 channel
//! 标识字段**；而本场景按需求在**同一个 dock** 上并发 64 条子流，因此同一个
//! `(local_dock, remote_dock)` 对会对应多条子流。本测试据此采用「同一 dock 内
//! 按 FIFO 配对」的约定：dock `d` 上第 `k` 条被 accept 的子流，对应发起端发往
//! dock `d` 的第 `k` 条 `open_channel_async`。载荷 tag 编码 `(d, k)`，两侧各自
//! 独立校验，因此一旦配对规则或线格式与此约定不符，测试会立刻失败——这正是本
//! 冒烟测试想要暴露的规格问题。
//!
#![allow(dead_code)] // 两个测试 target 各自只用到本模块的一部分。

use core::mem::MaybeUninit;

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef},
    io::{TrInput, TrOutput},
};
use buffex::circular_buff::builder::CircularBuffBuilder;
use mm_ptr::{Owned, x_deps::abs_mm};
use abs_mm::mem_alloc::CoreAlloc;
use abs_smux::conn::{
    TrChannelHandle, TrChannelHalf, TrChannelListener, TrConnection, TrDockBinding,
};
use smux_v1::{
    connection::{Dock, MuxConnection, TrMuxConfig},
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
}

/// buffex 构建器的具体类型别名（元素 `u8` + 缺省分配器），避免类型推断歧义。
type SmokeBuffBuilder = CircularBuffBuilder<Owned<[MaybeUninit<u8>], CoreAlloc>>;

/// 单条**全被动**环，返回 `(生产端, 消费端)`。
///
/// 消费端交给 smux 当 `Rx`（由 [`pump_input_`] 写入），或生产端交给 smux 当 `Tx`
/// （由 [`pump_output_`] 排空）。全被动模式不接任何设备，搬运完全由调用方的泵
/// 负责——原因见模块文档「为什么必须由调用方驱动泵」。
pub async fn make_passive_ring_(
    capacity: usize,
) -> (impl TrBuffWrite<u8>, impl TrBuffRead<u8>) {
    let mut ready = SmokeBuffBuilder::with_allocator(capacity, CoreAlloc)
        .expect("分配环缓冲应当成功")
        .producer_passive()
        .consumer_passive();
    let (tx, rx) = ready
        .build_async()
        .await
        .expect("构建全被动环应当成功");
    (tx, rx)
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
/// 写设备（`TrOutput`）：tokio 侧由 `abs_buff_tokio_adapt::{ReadAsInput,
/// WriteAsOutput}` 提供，compio 侧由 `abs_buff_compio_adapt::{ReadAsInput,
/// WriteAsOutput}` 提供。
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
    // 每端两个环：一个承载「socket → smux」（Rx），一个承载「smux → socket」（Tx）。
    let (a_rx_ring_tx, a_rx) = make_passive_ring_(K_NET_BUFFER_SIZE).await;
    let (a_tx, a_tx_ring_rx) = make_passive_ring_(K_NET_BUFFER_SIZE).await;
    let (b_rx_ring_tx, b_rx) = make_passive_ring_(K_NET_BUFFER_SIZE).await;
    let (b_tx, b_tx_ring_rx) = make_passive_ring_(K_NET_BUFFER_SIZE).await;

    let scenario_fut = run_smoke_scenario_::<_, _, _, _, Rt>(a_rx, a_tx, b_rx, b_tx);
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

/// 在一端（一个 `MuxConnection`）上完成「16 个 dock 监听 + 1024 条子流发起」。
///
/// - 监听侧：每个 dock 一个 `DockBinding`，`listen_async()` 一次，然后串行
///   `income_async()` → `accept_async()` 64 次；每次 accept 后先写本端载荷、
///   再读对端载荷并校验；
/// - 发起侧：每个 `(dock, index)` 一个**独立** `DockBinding`（`bind_async` 取
///   `&self`，可对同一 dock 反复调用），因为 `open_channel_async` 取
///   `&mut self`、其 future 在整个生命周期内独占该 binding；
/// - 两组任务用 `join_all` + `join!` 并发推进，从而两侧的 open 与 accept 互为
///   对方的前置条件而不会互相等待。
pub async fn drive_side_<R, W, C, Rt>(conn: &MuxConnection<R, W, C, Rt>)
where
    R: TrBuffRead<u8>,
    W: TrBuffWrite<u8>,
    C: TrMuxConfig,
    Rt: TrSmokeRuntime,
{
    let conn_ref = &conn;

    let mut accept_tasks = Vec::with_capacity(K_DOCK_COUNT as usize);
    for dock in 1..=K_DOCK_COUNT {
        accept_tasks.push(async move {
            let mut binding = conn_ref
                .bind_async(Dock::new(dock))
                .await
                .expect("绑定监听 dock 应当成功");
            let mut listener = binding
                .listen_async()
                .await
                .expect("在本地 dock 上建立 listener 应当成功");
            for index in 0..K_CHANNELS_PER_DOCK {
                let mut handle = listener
                    .income_async()
                    .await
                    .expect("应当取到下一条入向建流请求");
                assert_eq!(
                    handle.remote_dock(),
                    Dock::new(dock),
                    "入向请求的远端 dock 应与监听 dock 相同"
                );
                // 欢迎信息留空：用 `&mut &mut [u8]` 作为 `Wb`，因为生成的
                // future 要求 `Wb: Sized`，而 `[u8]` 是 unsized。
                let mut welcome_buf: [u8; 0] = [];
                let mut welcome: &mut [u8] = &mut welcome_buf[..];
                let (mut tx, mut rx) = handle
                    .accept_async(&mut welcome)
                    .await
                    .expect("accept 入向子流应当成功");
                let payload = make_payload_(dock, index);
                write_channel_all_(&mut tx, &payload)
                    .await
                    .expect("入向子流写入本端载荷应当成功");
                let mut got = vec![0u8; payload.len()];
                read_channel_exact_(&mut rx, &mut got)
                    .await
                    .expect("入向子流读取对端载荷应当成功");
                assert_eq!(
                    got, payload,
                    "dock {dock} 第 {index} 条入向子流载荷应逐字节相等"
                );
            }
        });
    }

    let mut open_tasks = Vec::with_capacity(K_TOTAL_CHANNELS);
    for dock in 1..=K_DOCK_COUNT {
        for index in 0..K_CHANNELS_PER_DOCK {
            open_tasks.push(async move {
                let mut binding = conn_ref
                    .bind_async(Dock::new(dock))
                    .await
                    .expect("绑定发起 dock 应当成功");
                let mut message: &[u8] = &[];
                let (mut tx, mut rx) = binding
                    .open_channel_async(Dock::new(dock), &mut message)
                    .await
                    .expect("向对端 dock 发起子流应当成功");
                let payload = make_payload_(dock, index);
                write_channel_all_(&mut tx, &payload)
                    .await
                    .expect("发起子流写入本端载荷应当成功");
                let mut got = vec![0u8; payload.len()];
                read_channel_exact_(&mut rx, &mut got)
                    .await
                    .expect("发起子流读取对端载荷应当成功");
                assert_eq!(
                    got, payload,
                    "dock {dock} 第 {index} 条发起子流载荷应逐字节相等"
                );
            });
        }
    }

    futures::join!(
        futures::future::join_all(open_tasks),
        futures::future::join_all(accept_tasks),
    );
}

/// 同时驱动连接的两端（每端各自并发推进 open / accept）。
pub async fn drive_both_sides_<RA, WA, RB, WB, C, Rt>(
    conn_a: &MuxConnection<RA, WA, C, Rt>,
    conn_b: &MuxConnection<RB, WB, C, Rt>,
) where
    RA: TrBuffRead<u8>,
    WA: TrBuffWrite<u8>,
    RB: TrBuffRead<u8>,
    WB: TrBuffWrite<u8>,
    C: TrMuxConfig,
    Rt: TrSmokeRuntime,
{
    futures::join!(drive_side_(conn_a), drive_side_(conn_b));
}

/// 场景主体：握手 → 建立复用连接 → 并发驱动连接与全部子流。
///
/// 参数是两端的 `Rx` / `Tx`（`A` 端发起握手，`B` 端等待）。连接接管 `Rx` / `Tx`
/// 后在内部自行 spawn 读写循环，本函数只推进业务面 [`drive_both_sides_`]。
///
/// # Panics
///
/// 握手失败、任意一次 open / accept / 读写失败，或读写会话在场景完成前退出时
/// panic——本函数是测试专用，失败即测试失败。
pub async fn run_smoke_scenario_<RA, WA, RB, WB, Rt>(
    rx_a: RA,
    tx_a: WA,
    rx_b: RB,
    tx_b: WB,
) where
    RA: TrBuffRead<u8>,
    WA: TrBuffWrite<u8>,
    RB: TrBuffRead<u8>,
    WB: TrBuffWrite<u8>,
    Rt: TrSmokeRuntime,
{
    // 1. 握手：A 端发起、B 端等待，两侧并发推进。
    let invite_opts = BasicOpts::default();
    let listen_opts = BasicOpts::default();
    let invite_fut = HandshakeAgent::new(rx_a, tx_a).invite_async(&invite_opts, AcceptAllEntries);
    let listen_fut = HandshakeAgent::new(rx_b, tx_b).listen_async(&listen_opts, AcceptAllEntries);
    let (invited, accepted) = futures::join!(
        async { invite_fut.await },
        async { listen_fut.await },
    );
    let delivery_a = invited.expect("发起方握手应当成功");
    let delivery_b = accepted.expect("等待方握手应当成功");

    // 2. 由交付物建立复用连接：`Rx` / `Tx` 由 `MuxConnection` 接管，
    //    读 / 写循环在其内部经 `abs_art` spawn，这里不再需要手动驱动。
    //    `Rt` 由测试目标给出（tokio / compio 各自的 `Runtime`）。
    let conn_a = MuxConnection::<RA, WA, SmokeMuxConfig, Rt>::new(delivery_a, SmokeMuxConfig);
    let conn_b = MuxConnection::<RB, WB, SmokeMuxConfig, Rt>::new(delivery_b, SmokeMuxConfig);

    // 3. 推进业务面（open / accept / 读写）；收发由内部任务自动进行。
    drive_both_sides_(&conn_a, &conn_b).await;
}
