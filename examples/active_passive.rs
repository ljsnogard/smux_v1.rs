//! README §2 那两段示例代码的落地：**主动端**与**被动端**各跑一遍，走完整链路。
//!
//! ```text
//! UNIX socket 对 ──┬── 主动端：invite → bind → open   → accept → write_all → 半关闭
//!                  └── 被动端：listen → bind → listen → income → accept → read_exact
//! ```
//!
//! # 本文件唯一允许感知后端的地方
//!
//! `MuxConnection` 只吃 `abs_buff` 的两条半边（`TrBuffRead` / `TrBuffWrite`），**不认识
//! 任何具体运行时**。把 tokio 的 `UnixStream` 变成这两条半边——设备级适配 + 两条全被动
//! 环 + 两条调用方驱动的泵——是后端相关的活，因此放在本文件里（`transport_` 与两个泵），
//! 与 `abs_art-demo` 的「业务库零改动、二进制负责选后端」是同一个分工。
//!
//! 运行：`cargo run --example active_passive`

use core::mem::MaybeUninit;

use abs_art::TrLocalScope;
use abs_smux::{
    chan::{ChannelBuffAlloc, TrChannelHandle, TrPrepareChannelRing},
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};
use buffex::{
    ring::{Ring, RingReader, RingWriter},
    x_deps::abs_buff::{
        Demand, TrBuffRead, TrBuffWrite,
        buffer::{TrBuffSegmMut, TrBuffSegmRef},
        io::{TrInput, TrOutput},
    },
};
use buffex_compio_adapt::{ReadAsInput, WriteAsOutput};
use mm_ptr::{Owned, Shared, x_deps::abs_mm::CoreAlloc};
use smux_v1::{
    connection::{DefaultConnCfg, Dock, MuxChanBuffOwnedBy, MuxConnection},
    flow_ctrl::DefaultPolicy,
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
    metrics::NoMetrics,
};
use compio::net::UnixStream;

/// 本示例用的**运行时值**类型：计时与时刻的来源。
///
/// 它由**配置**携带（[`TrConnCfg::Rt`]），而不是作为 `MuxConnection` 的类型参数——
/// 因此下面用 `abs_art_bridge` 的具名别名，而不是 `abs_art_tokio::Runtime`：
/// 连接建连时要经 [`smux_v1::connection::ScopeHost`] 自己取作用域，那条实现只挂在
/// bridge 的别名上（见 `src/connection/scope_host_.rs`）。
type Rt = abs_art_bridge::Runtime;
/// 本示例的连接配置：默认资源策略 + **显式传入的 tokio 运行时值** + 静默 sink。
///
/// 泛型参数顺序是 `<W, R, M, P, Rt>`：`M`（metrics 接收方）排在策略之前，本示例
/// 不需要上报，因此填 [`NoMetrics`]。
type Cfg = DefaultConnCfg<Tx, Rx, NoMetrics, DefaultPolicy, Rt>;
/// 本示例用的**本地作用域**类型：五个循环的投递点，由运行时值交出。
///
/// 与 [`Rt`] 一样取 bridge 的**裸名**——这样示例与库用的就是同一个后端解析结果，
/// smux 侧也因此**不需要**直接依赖任何后端 crate。
type Scope = abs_art_bridge::LocalScope;
/// 全被动传输环的存储类型（本示例自己用 `Shared` 建的环，与 smux 的缓冲契约无关）。
type Buf = Owned<[MaybeUninit<u8>], CoreAlloc>;
/// 交给 smux 当 `Tx` 的写半边。
type Tx = RingWriter<Shared<Ring<Buf, u8>, CoreAlloc>, Buf, u8>;
/// 交给 smux 当 `Rx` 的读半边。
type Rx = RingReader<Shared<Ring<Buf, u8>, CoreAlloc>, Buf, u8>;

/// 传输环容量：只在吞吐上有意义，取多大都不影响正确性。
const K_RING_CAP: usize = 64usize * 1024usize;
/// 单次从 socket 搬进环的分块上限。
const K_PUMP_CHUNK: usize = 4096usize;

/// 缺省后端即 **compio**（bridge 的 `default-backend-compio`），因此本示例直接用
/// compio 的运行时与 socket：`cargo run --example active_passive` 不需要任何 feature。
#[compio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // compio 0.19 的 `UnixStream` 没有 `pair()`：先建 `std` socket 对，再逐个注册进
    // 当前运行时（与 `tests/common/socket_.rs` 的 compio 版同款）。
    let (std_a, std_b) = std::os::unix::net::UnixStream::pair()?;
    let socket_a = UnixStream::from_std(std_a)?;
    let socket_b = UnixStream::from_std(std_b)?;
    // 运行时值只能从「当前运行时上下文」取得（`#[compio::main]` 满足）；作用域不再能
    // 凭空构造，只能由运行时值交出。
    let rt = abs_art_bridge::current();
    let scope = rt.local_scope();

    // -- 传输装配：每端两个全被动环，四个半部按「谁贴 socket、谁贴 smux」分派。
    //    两个泵与场景在同一个任务里轮询，因此不需要 `Send`／`'static`。
    let (mut rd_a, mut wr_a) = socket_a.into_split();
    let (mut rd_b, mut wr_b) = socket_b.into_split();
    let (a_in_tx, a_rx) = passive_ring_();
    let (a_tx, a_out_rx) = passive_ring_();
    let (b_in_tx, b_rx) = passive_ring_();
    let (b_tx, b_out_rx) = passive_ring_();

    let scenario = async {
        let (a, b) = futures::join!(
            active(&rt, &scope, a_rx, a_tx),
            passive(&rt, &scope, b_rx, b_tx)
        );
        // 两个连接交回这里保管：**drop 连接 = 拆连接**，四个循环会随之收尾，
        // 因此发送方必须把连接活到「数据真的上网」为止。
        let (conn_a, conn_b) = (a?, b?);
        drop((conn_a, conn_b));
        Result::<(), Box<dyn std::error::Error>>::Ok(())
    };
    let pumps = async {
        futures::join!(
            pump_input_(ReadAsInput::new(&mut rd_a), a_in_tx),
            pump_output_(a_out_rx, WriteAsOutput::new(&mut wr_a)),
            pump_input_(ReadAsInput::new(&mut rd_b), b_in_tx),
            pump_output_(b_out_rx, WriteAsOutput::new(&mut wr_b)),
        )
    };

    // 场景跑完就丢掉泵（相当于关闭传输）：两边都不再需要它。
    futures::pin_mut!(scenario);
    futures::pin_mut!(pumps);
    scope
        .run_until(async {
            match futures::future::select(scenario, pumps).await {
                futures::future::Either::Left((res, _pumps)) => res,
                // 四个泵全部退出 ⇒ 场景没能跑完。
                futures::future::Either::Right((_pump_outs, _scenario)) => {
                    Result::Err("传输泵在场景完成之前全部退出".into())
                }
            }
        })
        .await
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 主动端 / 被动端：与 README §2 的两段代码逐行对应
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// **主动端**：连上去，开一条子流，发消息，半关闭。
async fn active(
    rt: &Rt,
    _scope: &Scope,
    rx: Rx,
    tx: Tx,
) -> Result<MuxConnection<Cfg>, Box<dyn std::error::Error>> {
    // ① 握手（不需要运行时值，也不需要作用域）
    let delivery = HandshakeAgent::new(tx, rx)
        .invite_async(&BasicOpts::default(), AcceptAllEntries)
        .await?;

    // ② 建连接：需要环境提供运行时值（计时）与本地作用域（投递五个循环）。
    // `MuxConnection::from_delivery` 取的是**默认后端**（集成方在 Cargo.toml 里选的
    // 那个，本仓缺省 compio）的运行时值。本示例跑在 tokio 上，属于「特别的需要」，
    // 因此显式把运行时值传进配置——这正是 `DefaultConnCfg::new_with_rt` 的用途。
    let (delivery, cfg) =
        <DefaultConnCfg<Tx, Rx, NoMetrics, DefaultPolicy, Rt>>::new_with_rt(
            delivery,
            DefaultPolicy,
            rt.clone(),
        );
    let (stage_r, stage_w) = (
        MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(K_RING_CAP, CoreAlloc)),
        MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(K_RING_CAP, CoreAlloc)),
    );
    let conn = MuxConnection::new(delivery, cfg, stage_r, stage_w);

    let local_dock = Dock::new(0x2001);
    let remote_dock = Dock::new(1);
    let mut binding = conn.bind_async(local_dock).await?;

    let mut invitation: &[u8] = b"Hi, SMUX!";
    let mut ch = binding.open_channel_async(remote_dock, &mut invitation).await?;
    let mut welcome: [u8; 0] = [];
    let mut welcome: &mut [u8] = &mut welcome[..];
    let (mut tx, _rx) = ch
        .accept_async(&mut welcome, DemoRing_::new(K_RING_CAP))
        .await?;

    tx.write_all(b"hello").await?;
    drop(tx); // 半关闭：对端读到 EOF
    println!("主动端：已发出 5 字节并半关闭");
    Ok(conn)
}

/// **被动端**：等对端来找这个 dock，收到子流后读到 EOF。
async fn passive(
    rt: &Rt,
    _scope: &Scope,
    rx: Rx,
    tx: Tx,
) -> Result<MuxConnection<Cfg>, Box<dyn std::error::Error>> {
    // ① 握手（不需要运行时值，也不需要作用域）
    let delivery = HandshakeAgent::new(tx, rx)
        .listen_async(&BasicOpts::default(), AcceptAllEntries)
        .await?;

    // ② 建连接：需要环境提供运行时值（计时）与本地作用域（投递五个循环）。
    // `MuxConnection::from_delivery` 取的是**默认后端**（集成方在 Cargo.toml 里选的
    // 那个，本仓缺省 compio）的运行时值。本示例跑在 tokio 上，属于「特别的需要」，
    // 因此显式把运行时值传进配置——这正是 `DefaultConnCfg::new_with_rt` 的用途。
    let (delivery, cfg) =
        <DefaultConnCfg<Tx, Rx, NoMetrics, DefaultPolicy, Rt>>::new_with_rt(
            delivery,
            DefaultPolicy,
            rt.clone(),
        );
    let (stage_r, stage_w) = (
        MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(K_RING_CAP, CoreAlloc)),
        MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(K_RING_CAP, CoreAlloc)),
    );
    let conn = MuxConnection::new(delivery, cfg, stage_r, stage_w);

    let local_dock = Dock::new(1);
    let mut listener = conn
        .bind_async(local_dock).await?  // 绑定 dock
        .listen_async_default().await?; // 开始收建流请求

    let mut incoming = listener.income_async().await?;
    let mut welcome: [u8; 0] = [];
    let mut welcome: &mut [u8] = &mut welcome[..];
    let (_tx, mut rx) = incoming
        .accept_async(&mut welcome, DemoRing_::new(K_RING_CAP))
        .await?;

    let mut buf = [0u8; 5];
    rx.read_exact(&mut buf).await?; // 对端 drop(tx) 之后就到这里
    assert_eq!(&buf, b"hello");
    println!("被动端：收到 {:?}", core::str::from_utf8(&buf)?);
    Ok(conn)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 后端胶水：socket ↔ 全被动环 ↔ 调用方驱动的泵
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 本示例的 `accept` 环准备策略：按给定容量向 `CoreAlloc` 要两块内存，
/// 交给连接当作本条子流的发送 / 接收环。
///
/// 这正是上游 `TrPrepareChannelRing` 的用法：**类型与容量都由调用方当场决定**，
/// 配置不再规定它们（见 `smux_v1::connection::ring_` 模块文档）。
struct DemoRing_ {
    cap_: usize,
}

impl DemoRing_ {
    const fn new(cap: usize) -> Self {
        DemoRing_ { cap_: cap }
    }
}

impl TrPrepareChannelRing<MuxChanBuffOwnedBy<Buf>, u8> for DemoRing_ {
    fn prepare(self) -> ChannelBuffAlloc<MuxChanBuffOwnedBy<Buf>, u8> {
        ChannelBuffAlloc::new(
            MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(self.cap_, CoreAlloc)),
            MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(self.cap_, CoreAlloc)),
        )
    }
}

/// 造一条全被动环，切成 `(写端, 读端)`。
fn passive_ring_() -> (Tx, Rx) {
    let buffer = Owned::new_uninit_slice(K_RING_CAP, CoreAlloc);
    let ring = Ring::try_new(buffer).expect("容量合法");
    let shared = Shared::new(ring, CoreAlloc);
    // SAFETY: 这条环由刚建出的 `Shared` 独占，且不存在对应的 `Weak`，
    // 因此两个半部各持一个强引用是安全的（与 `Ring::split_unchecked` 的要求一致）。
    unsafe { Ring::split_unchecked(shared) }
}

/// 入向泵：socket 读设备 → 全被动环（环的读端即 smux 的 `Rx`）。
async fn pump_input_<I, W>(mut input: I, mut ring_tx: W)
where
    I: TrInput<u8>,
    W: TrBuffWrite<u8>,
{
    let mut chunk: Vec<MaybeUninit<u8>> =
        (0..K_PUMP_CHUNK).map(|_| MaybeUninit::uninit()).collect();
    loop {
        let read = input.read_async(&mut chunk).await;
        let n = match read.pick_left() {
            Option::Some(n) => n,
            // 设备侧结束（EOF 以 `ReadErrTag::Closing` 的形式上报）或出错：收工。
            // 丢掉 `ring_tx` 之后 smux 的 `Rx` 会看到「不再有数据」。
            Option::None => return,
        };
        if n == 0usize {
            return;
        }
        // SAFETY: 设备只把已初始化的字节写进 `chunk[..n]`；`MaybeUninit<u8>` 与 `u8`
        // 布局相同、对齐相同（均为 1），按已初始化前缀读取是健全的。
        let bytes: &[u8] = unsafe { core::slice::from_raw_parts(chunk.as_ptr() as *const u8, n) };

        let mut off = 0usize;
        while off < bytes.len() {
            let demand = Demand::at_least(1usize);
            let mut outcome = ring_tx.write_async(&demand).await;
            let put = match outcome.as_mut().pick_left() {
                Option::Some(segm) => {
                    segm.as_segm_mut().clone_items_from_buff(&bytes[off..])
                }
                // 环被拆掉：收工。
                Option::None => return,
            };
            if put == 0usize {
                return;
            }
            off += put;
            // `outcome` 在此 drop：提交写入 → 唤醒 smux 的读侧。
        }
    }
}

/// 出向泵：全被动环（环的写端即 smux 的 `Tx`）→ socket 写设备。
async fn pump_output_<R, O>(mut ring_rx: R, mut output: O)
where
    R: TrBuffRead<u8>,
    O: TrOutput<u8>,
{
    loop {
        let demand = Demand::at_least(1usize);
        let mut outcome = ring_rx.read_async(&demand).await;
        {
            let segm = outcome.as_mut().pick_left();
            let Option::Some(segm) = segm else {
                // 环被拆掉（smux 侧 `Tx` 已被 drop）：收工。
                return;
            };
            let mut child = segm.as_segm_ref();
            let n = child.least_count();
            let mut dst: Vec<MaybeUninit<u8>> =
                (0..n).map(|_| MaybeUninit::uninit()).collect();
            // SAFETY: `dst` 是本函数独占的可写切片，`move_items_to_buff` 只写入其中
            // 已初始化的前缀（返回值给出长度）。
            let moved = unsafe { child.move_items_to_buff(&mut dst) };
            if moved == 0usize {
                return;
            }
            // 先把 `dst[..moved]` 全写给设备，再 drop 段提交消费——顺序保证了
            // 「设备已收下」一定先于「环缓冲被释放」。
            let mut off = 0usize;
            while off < moved {
                let w = output.write_async(&dst[off..moved]).await;
                match w.pick_left() {
                    Option::Some(0usize) | Option::None => return,
                    Option::Some(k) => off += k,
                }
            }
            drop(child);
        }
        // `outcome` 在此 drop：提交消费 → 唤醒 smux 的写侧。
    }
}
