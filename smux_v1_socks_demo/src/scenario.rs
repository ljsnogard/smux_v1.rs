//! 与运行时、传输都无关的**场景主体**。
//!
//! 一次 [`run_phase`] = 一条 TCP 连接上的完整一段：
//!
//! ```text
//! 握手（invite 或 listen）
//!   → 建 MuxConnection（配置里带进程级 metrics sink）
//!   → 主动侧开 N 条子流 / 被动侧收 N 条子流
//!   → 每条子流双向各发一份不同的确定性载荷（先 ENVELOPE 后本体，收齐后回 RECEIPT）
//!   → 校验：接收方比对 digest 与逐字节内容；发送方比对自己声明的 digest 与回执里的值
//! ```
//!
//! # 三个必须记住的实现约束
//!
//! 1. **一条子流内部的读写必须并发推进**：先把自己几 MiB 本体写完再开始读，会在接收
//!    窗口耗尽时与对端互锁。这里用 `futures::join!`（不是 `spawn`）。
//! 2. **N 条子流之间用 `join_all` 并发**：同样不 `spawn`，因为 compio 的 socket 半边
//!    是 `!Send`。
//! 3. **回执要在半关闭之前发**：写完回执才 `drop(tx)`；两端的「等对端 EOF」因此不会
//!    互相等待（谁先 drop，谁先把自己的 `FIN` 送出去）。

use core::mem::MaybeUninit;
use std::time::Instant;

use abs_buff::{
    Demand,
    buffer::{TrBuffSegmRef, TrBuffSegmView},
};
use futures::future::join_all;
use smux_v1::x_deps::abs_buff;
use smux_v1::{
    connection::{ChannelRx, ChannelTx, Dock, DockBinding, MuxConnection, Receiver, Sender},
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
    x_deps::abs_smux::{
        chan::{TrChannelHalf, TrChannelHandle},
        conn::{TrChannelListener, TrConnection, TrDockBinding},
        dock::TrDock,
        telegraph::{
            TrDatagramRecver, TrDatagramSender, TrRecvDatagram, TrTelegraph, TrTelegraphBinding,
        },
    },
};

use crate::{
    Cfg, Rt, Rx, Tx,
    cfg::DemoCfg,
    metrics::{DemoMetrics, MetricsDelta},
    payload::{PayloadSrc, digest_of},
    proto::{Envelope, K_HEADER_LEN},
    transport::{RingPrepare, expect_eof, make_stage_buffs, read_exact, write_all},
};

/// 一侧的载荷标识：0 = 主动侧（握手 invite），1 = 被动侧（握手 listen）。
///
/// 它进载荷种子，因此**同一子流两个方向的载荷必然不同**——这正是「两个方向各走一份
/// 不同数据」的落地方式。
pub const K_SIDE_INITIATOR: u64 = 0u64;
/// 被动侧标识。
pub const K_SIDE_ACCEPTER: u64 = 1u64;

/// telegraph 端点在**主动端**（本阶段 invite 的那一侧）使用的本端 dock。
///
/// 每个阶段是**一条**连接，两端各自的 telegraph 身份必须互不相同，也不能与
/// channel / listener 的 dock 相撞：主动端的子流用 `--base-dock` 往上的临时区间，
/// 被动端用 `--service-dock` 当监听 dock，因此这里取 2 / 3 两个小值。
/// `--base-dock` 区间或 `--service-dock` 若落到这两个值上，`cli` 会在解析阶段直接报错。
pub const K_TG_DOCK_INITIATOR: u32 = 2u32;

/// telegraph 端点在**被动端**使用的本端 dock。
pub const K_TG_DOCK_ACCEPTER: u32 = 3u32;

/// telegraph 确定性载荷的序号基址。
///
/// 它把 telegraph 的报文序号与 channel 的子流序号（`0..streams`）在**同一个** `--seed`
/// 下区分开，因此两种通道各自拿到互不相同的字节流。
pub const K_TG_IDX_BASE: u64 = 1u64 << 32usize;

/// 一段（一条连接）的场景参数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhaseParams {
    /// 子流条数。
    pub streams: usize,
    /// 每个方向的本体字节数。
    pub bytes: u64,
    /// 确定性载荷的种子（两端必须一致）。
    pub seed: u64,
    /// 本体分块大小。
    pub chunk: usize,
    /// 子流环容量（= 本端接收窗口）。
    pub ring_cap: usize,
    /// 协商的 `max_packet_size`（帧总长上限）。
    pub max_packet_size: usize,
    /// 建流完成后、开始灌数据之前，先给本地队列多少个 tick（见 [`PhaseParams`] 的说明）。
    pub pre_flood_yield: usize,
    /// 发起侧临时 local dock 基址。
    pub base_dock: u32,
    /// 被动侧监听 dock。
    pub service_dock: u32,
    /// 是否在每条连接上**额外**开一条 telegraph 端点，与子流**并发**跑
    /// 「停止等待 + 应用层 ACK」闭环。
    pub mix_telegraph: bool,
    /// telegraph 每个方向的报文条数。
    pub tg_frames: usize,
    /// telegraph 每条报文的载荷字节数。
    pub tg_bytes: usize,
}

/// 一段的结论。
#[derive(Debug, Clone, Default)]
pub struct PhaseReport {
    /// 本段里本进程的角色：`initiator` 或 `accepter`。
    pub role: &'static str,
    /// 子流条数。
    pub streams: usize,
    /// 每个方向的期望字节数（判据用）。
    pub expected_per_stream: u64,
    /// 本进程写出的本体字节数。
    pub sent_bytes: u64,
    /// 本进程收到的本体字节数。
    pub recv_bytes: u64,
    /// 本进程发出的载荷中，被对端回执确认的条数。
    pub sent_acked: u64,
    /// 本进程收到的载荷中，digest 与逐字节内容都对的条数。
    pub recv_verified: u64,
    /// 出问题的子流条数。
    pub failed_channels: u64,
    /// 本段是否开启了 telegraph 混合项。
    pub mix_telegraph: bool,
    /// telegraph 每个方向的期望报文条数（判据用；未开启时为 0）。
    pub tg_frames: u64,
    /// telegraph 每条报文的载荷字节数（未开启时为 0）。
    pub tg_bytes: u64,
    /// 本端发出的 telegraph 报文中，被对端 ACK 确认的条数。
    pub tg_sent_acked: u64,
    /// 本端收到的 telegraph 报文中，**远端地址与内容**都校验通过的条数。
    pub tg_recv_verified: u64,
    /// 本段墙钟耗时（纳秒）。
    pub elapsed_ns: u64,
    /// 本段的指标增量（**数据面口径**；连接 / 子流收尾的计数可能落进下一段，见模块文档）。
    pub metrics: MetricsDelta,
    /// 逐条失败说明（为空表示本段干净）。
    pub failures: Vec<String>,
}

impl PhaseReport {
    /// 本段是否干净。
    ///
    /// 判据：没有失败项；每条子流的两个方向都被证明（`sent_acked` 与 `recv_verified`
    /// 都等于子流条数）；收发字节数都等于「条数 × 每方向字节数」。
    pub fn is_clean(&self) -> bool {
        let streams = self.streams as u64;
        let want = streams * self.expected_per_stream;
        // telegraph 混合项开启时，两个方向都必须整轮走完：自己发的每一条都被 ACK，
        // 自己收的每一条都校验通过。
        let tg_ok = !self.mix_telegraph
            || (self.tg_sent_acked == self.tg_frames && self.tg_recv_verified == self.tg_frames);
        self.failures.is_empty()
            && self.failed_channels == 0u64
            && self.sent_acked == streams
            && self.recv_verified == streams
            && self.sent_bytes == want
            && self.recv_bytes == want
            && tg_ok
    }
}

/// 一条子流的结论。
#[derive(Debug)]
struct ChannelOutcome {
    /// 子流序号。
    idx: u64,
    /// 本端写出的本体字节数。
    sent_bytes: u64,
    /// 本端收到的本体字节数。
    recv_bytes: u64,
    /// 本端发出的载荷是否被对端回执确认。
    sent_acked: bool,
    /// 本端收到的载荷是否通过 digest 与逐字节比对。
    recv_verified: bool,
    /// 失败说明。
    failure: Option<String>,
}

/// 跑完一条连接上的一段，并把**连接本身交回调用方**。
///
/// # 为什么要把连接交回来
///
/// 「`drop` 连接 = 拆连接」：已经写进环、还没上网的帧会随连接一起消失。而两个方向的
/// 收发结束时，本端最后那几条帧（回执、`FIN`）常常还躺在环里——更要命的是，**跨进程时
/// 对端不会替你驱动本端的循环**：本端一旦不再 await，本地队列上的五个循环就没人推了。
///
/// 因此收尾必须由驱动层负责：拿到交回的连接后，先让本地队列继续 tick（把环里的帧变成
/// `DATA` 帧、经写泵提交进传输适配器），再 `drop(conn)`；写端适配器在 drop 时「关生产端
/// + detach 后台泵」，尾巴由泵搬完并 `shutdown` 设备，所以丢完连接还要再 tick 一轮。
///
/// # Errors
///
/// 握手、建连接、绑定 / 监听、开流 / 收流等**结构性**失败直接返回错误；单条子流的内容
/// 不一致不算结构性失败，记进 [`PhaseReport::failures`]。
pub async fn run_phase(
    rt: &Rt,
    tx: Tx,
    rx: Rx,
    initiate: bool,
    params: &PhaseParams,
) -> Result<(PhaseReport, MuxConnection<Cfg>), Box<dyn std::error::Error>> {
    let sink = DemoMetrics::global();
    let before = sink.snapshot();
    let started = Instant::now();

    // ① 握手：主动端 invite，被动端 listen。两条连接各做一次。
    let opts = BasicOpts {
        max_packet_size: params.max_packet_size,
        ..BasicOpts::default()
    };
    let delivery = if initiate {
        HandshakeAgent::new(tx, rx)
            .invite_async(&opts, AcceptAllEntries)
            .await?
    } else {
        HandshakeAgent::new(tx, rx)
            .listen_async(&opts, AcceptAllEntries)
            .await?
    };

    crate::trace_!("握手完成：initiate={initiate}");

    // ② 建连接：运行时值进配置，因此连接内部的计时与作用域都取自这个后端。
    let cfg = DemoCfg::new(rt.clone(), DemoMetrics::global());
    let (delivery, cfg) = (delivery, cfg);
    let (stage_r, stage_w) = make_stage_buffs();
    let conn = MuxConnection::new(delivery, cfg, stage_r, stage_w);

    crate::trace_!("连接已建：initiate={initiate}");

    let mut failures: Vec<String> = Vec::new();
    let my_side = if initiate {
        K_SIDE_INITIATOR
    } else {
        K_SIDE_ACCEPTER
    };

    // ②.5 若开启 telegraph 混合项：**在 channel 建流之前**开好端点（理由见
    //      `open_telegraph_`：建流是两端共同的同步点，端点先于它开好，第 ④ 步并发
    //      开始时两边才都一定有接收方）。
    let mut tg_endpoints: Option<TgEndpoints_> = if params.mix_telegraph {
        match open_telegraph_(&conn, initiate, params).await {
            Ok(endpoints) => Some(endpoints),
            Err(note) => {
                failures.push(format!("telegraph：{note}"));
                None
            }
        }
    } else {
        None
    };

    // ③ 开 / 收 N 条子流。
    let mut channels: Vec<(u64, ChannelTx<Cfg>, ChannelRx<Cfg>)> =
        Vec::with_capacity(params.streams);

    if initiate {
        // 发起侧：每条并发子流一个**互不相同**的临时 local dock（dock 对即身份）。
        let mut bindings: Vec<DockBinding<Cfg>> = Vec::with_capacity(params.streams);
        for i in 0..params.streams {
            let local = Dock::new(params.base_dock + i as u32);
            let mut binding = conn.bind_async(local).await?;
            let mut invitation: &[u8] = &[];
            crate::trace_!("子流 {i}：开始 open");
            let mut handle = binding
                .open_channel_async(Dock::new(params.service_dock), &mut invitation)
                .await?;
            crate::trace_!("子流 {i}：open 返回，开始 accept");
            if let Some(reason) = handle.abort_reason() {
                failures.push(format!("子流 {i} 建流即被中止：{reason:?}"));
            }
            let mut empty: &mut [u8] = &mut [];
            let (tx, rx) = handle
                .accept_async(&mut empty, RingPrepare::new(params.ring_cap))
                .await?;
            crate::trace_!("子流 {i}：accept 返回（已裁决）");
            channels.push((i as u64, tx, rx));
            bindings.push(binding);
        }
        // binding 保持到本段结束（`open` 之后解绑虽然也安全，但持有它能少一条要记的规则）。
        drop(bindings);
    } else {
        // 被动侧：绑一个监听 dock，循环收建流请求；子流序号由对端的临时 dock 给出。
        let mut binding = conn.bind_async(Dock::new(params.service_dock)).await?;
        let mut listener = binding.listen_async_default().await?;
        for _ in 0..params.streams {
            let mut handle = listener.income_async().await?;
            let remote = handle.remote_dock();
            if remote.is_special() {
                failures.push("入向子流的对端 dock 是保留值".to_string());
                continue;
            }
            let raw = u64::from(remote.value());
            let base = u64::from(params.base_dock);
            if raw < base || raw >= base + params.streams as u64 {
                failures.push(format!(
                    "入向子流的对端 dock {raw} 不在发起侧临时区间 [{base}, {}) 内",
                    base + params.streams as u64
                ));
                continue;
            }
            if let Some(reason) = handle.abort_reason() {
                failures.push(format!("子流 dock={raw} 建流即被中止：{reason:?}"));
            }
            let mut empty: &mut [u8] = &mut [];
            crate::trace_!("子流 remote_dock={raw}：开始 accept");
            let (tx, rx) = handle
                .accept_async(&mut empty, RingPrepare::new(params.ring_cap))
                .await?;
            crate::trace_!("子流 remote_dock={raw}：accept 返回");
            channels.push((raw - base, tx, rx));
        }
        // listener / binding 与连接同生共死，这里显式持有到本段结束。
        drop((listener, binding));
    }

    // 灌数据之前先让位若干 tick。**不是正确性需要**（正确性由对端回执保证），而是本
    // 负载下的节奏选择：让连接循环先把建流帧送出去、把窗口带动起来，再让应用开始成批
    // 写入。实测（release，8 子流 × 每方向 8 MiB）0 tick 约 30–32 MiB/s，256 tick 约
    // 37–39 MiB/s，两次重复一致。
    if params.pre_flood_yield > 0usize {
        crate::drain_scope(&rt.local_scope(), params.pre_flood_yield).await;
    }

    // ④ 两条通道**并发**推进：
    //
    //    - 子流：每条并发跑「双向各一份不同载荷 + 回执」的闭环；
    //    - telegraph（`--mix-telegraph`）：额外一条端点，跑「停止等待 + 应用层 ACK」闭环。
    //
    //    这里用 `join!` 而不是 `spawn`：compio 的 socket 半边是 `!Send`，而本 demo 的三个
    //    后端都要能跑。两个 future 因此是**同一线程上的两个并发执行体**；选它而不是
    //    OS 线程是**必要**的——compio / smol 后端的运行时值不是 `Send`，`MuxConnection`
    //    在后两个后端下也就不能跨线程。
    let channel_fut = async {
        let futures: Vec<_> = channels
            .into_iter()
            .map(|(idx, tx, rx)| run_channel(idx, tx, rx, params, my_side))
            .collect();
        join_all(futures).await
    };
    let telegraph_fut = async {
        match tg_endpoints.as_mut() {
            Some(endpoints) => run_telegraph_(endpoints, params, my_side).await,
            None => TgOutcome_::default(),
        }
    };
    crate::trace_!(
        "子流全部就位，开始双向收发（telegraph 混合项 = {}）",
        params.mix_telegraph
    );
    let (outcomes, tg) = futures::join!(channel_fut, telegraph_fut);
    crate::trace_!("双向收发全部结束");

    // ⑤ 汇总。连接**不在这里丢弃**——收尾见函数文档。
    let mut report = PhaseReport {
        role: if initiate { "initiator" } else { "accepter" },
        streams: params.streams,
        expected_per_stream: params.bytes,
        mix_telegraph: params.mix_telegraph,
        tg_frames: if params.mix_telegraph {
            params.tg_frames as u64
        } else {
            0u64
        },
        tg_bytes: if params.mix_telegraph {
            params.tg_bytes as u64
        } else {
            0u64
        },
        failures,
        ..PhaseReport::default()
    };
    for outcome in outcomes {
        report.sent_bytes += outcome.sent_bytes;
        report.recv_bytes += outcome.recv_bytes;
        if outcome.sent_acked {
            report.sent_acked += 1u64;
        }
        if outcome.recv_verified {
            report.recv_verified += 1u64;
        }
        if let Some(note) = outcome.failure {
            report.failed_channels += 1u64;
            report
                .failures
                .push(format!("子流 {}：{note}", outcome.idx));
        }
    }
    report.tg_sent_acked = tg.sent_acked_;
    report.tg_recv_verified = tg.recv_verified_;
    if let Some(note) = tg.failure_ {
        report.failures.push(format!("telegraph：{note}"));
    }
    report.elapsed_ns = started.elapsed().as_nanos() as u64;
    report.metrics = sink.snapshot().delta(&before);
    Ok((report, conn))
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// telegraph 混合项：停止等待 + 应用层 ACK
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 本 demo 的 telegraph 报文类型（= 接收半边 `recv_async` 的产物）。
///
/// 写成别名是为了让「取远端地址」在调用点能**点名** `Cfg`：`RecvDatagram` 对**任意**
/// `C: TrConnCfg` 都实现 `TrRecvDatagram`，因此 `dg.remote_dock()` 的 `C` 无法从实参
/// 推断。
type TgDataGram<'f> = <Receiver<Cfg> as TrDatagramRecver<Cfg>>::DataGram<'f>;

/// 已经开启、尚未开始收发的 telegraph 端点（一侧）。
struct TgEndpoints_ {
    /// 发送半边。
    tx_: Sender<Cfg>,
    /// 接收半边。
    rx_: Receiver<Cfg>,
    /// 对端 telegraph 端点的 dock（逐次发送的目的地址）。
    peer_dock_: u32,
    /// 本端 dock 的绑定句柄。
    ///
    /// telegraph 自身的可用性由身份守卫保证（与两个半边同寿），这里持有它只是让
    /// 「本端 dock 已被本阶段占用」这件事在作用域里显式可见——因此它**只被持有、
    /// 不被读**。
    #[allow(dead_code)]
    binding_: DockBinding<Cfg>,
}

/// telegraph 混合项一侧的结论。
#[derive(Debug, Default, Clone)]
struct TgOutcome_ {
    /// 本端发出、且被对端 ACK 确认的报文条数。
    sent_acked_: u64,
    /// 本端收到、且**远端地址与内容**都校验通过的报文条数。
    recv_verified_: u64,
    /// 失败说明（`None` 表示这一项干净）。
    failure_: Option<String>,
}

/// 在一条已建立的连接上跑完 telegraph 的**停止等待 + 应用层 ACK**闭环。
///
/// # 协议（刻意粗糙）
///
/// 任一时刻只有**一条**报文在飞：
///
/// ```text
/// 本端   发第 n 帧（DATA：定长信封 + 确定性载荷）  ──▶  对端
/// 本端                                              ◀──  对端回第 n 帧的 ACK
/// 本端   确认 ACK 里的 digest 与自己声明的一致，才发第 n+1 帧
/// ```
///
/// ACK 就是同一套 [`Envelope`] 的 `RECEIPT` 变体，只不过它走的是**另一条 telegraph
/// 报文**而不是子流——「数据报 + 应用层确认」正是 UDP 上做可靠传输的最小形状。
///
/// # 两端对称，因此「两端都收到」可判
///
/// 每一端都既发自己的 `tg_frames` 帧、也收对方的同样多帧并逐条回 ACK。于是：
///
/// - `sent_acked` = 本端发出的每一帧都被对端证明收到；
/// - `recv_verified` = 本端收到的每一帧内容都对、且远端地址正确。
///
/// # 为什么不会死锁
///
/// 发送条件只有一个：**上一帧已被 ACK**（初始时第 0 帧立刻可发）。两端都在处理对方的
/// 入向报文，因此任何一端的「等 ACK」都对应着对端可以推进的一步；报文由底层可靠有序的
/// 字节流承载，不会丢，所以两边都能走完。真正会让它挂住的只有「接收环装不下一整帧」
/// ——那种配置在 `cli` 阶段就被拒掉了（见 `--tg-bytes` 与 `--ring-cap` 的校验）。
async fn open_telegraph_(
    conn: &MuxConnection<Cfg>,
    initiate: bool,
    params: &PhaseParams,
) -> Result<TgEndpoints_, String> {
    let (my_dock, peer_dock) = if initiate {
        (K_TG_DOCK_INITIATOR, K_TG_DOCK_ACCEPTER)
    } else {
        (K_TG_DOCK_ACCEPTER, K_TG_DOCK_INITIATOR)
    };

    let mut binding = conn
        .bind_async(Dock::new(my_dock))
        .await
        .map_err(|err| format!("dock {my_dock} 绑定失败：{err}"))?;
    let telegraph = binding
        .open_telegraph_async(RingPrepare::new(params.ring_cap))
        .await
        .map_err(|err| format!("开启端点（dock {my_dock}）失败：{err}"))?;
    let (tx_, rx_) = telegraph.split();
    crate::trace_!("telegraph 端点已就绪：local={my_dock} peer={peer_dock}");
    Ok(TgEndpoints_ {
        tx_,
        rx_,
        peer_dock_: peer_dock,
        binding_: binding,
    })
}

/// 在**已经就绪**的 telegraph 端点上跑停止等待闭环。
///
/// 端点由 [`open_telegraph_`] 在 channel 建流**之前**开好：数据报是尽力交付，报文到达
/// 一个尚未注册接收方的 dock 时会被**静默丢弃**，而本闭环没有超时重传——第一帧被丢就
/// 永远等不到 ACK。channel 建流是两端天然的同步点，把开端点放在它之前，就能保证第 ④ 步
/// 并发一开始两端的端点都已就绪。
async fn run_telegraph_(
    endpoints: &mut TgEndpoints_,
    params: &PhaseParams,
    my_side: u64,
) -> TgOutcome_ {
    let peer_dock = endpoints.peer_dock_;
    let other_side = if my_side == K_SIDE_INITIATOR {
        K_SIDE_ACCEPTER
    } else {
        K_SIDE_INITIATOR
    };

    // 停止等待主循环：`sent == acked` 才能发下一帧。
    let total = params.tg_frames as u64;
    let mut out = TgOutcome_::default();
    let mut sent = 0u64;
    let mut acked = 0u64;
    while acked < total {
        if sent == acked {
            if let Err(note) =
                send_telegram_(&mut endpoints.tx_, peer_dock, sent, params, my_side).await
            {
                out.failure_ = Some(note);
                break;
            }
            sent += 1;
        }
        // 收一条入向报文：可能是对端的数据帧，也可能是我方上一帧的 ACK。
        let (remote, frame) = match recv_telegram_(&mut endpoints.rx_).await {
            Ok(v) => v,
            Err(note) => {
                out.failure_ = Some(note);
                break;
            }
        };
        // 远端地址必须是**对端 telegraph 端点**：这一条同时钉住
        // `TrRecvDatagram::remote_dock` 的镜像语义。
        if remote != Dock::new(peer_dock) {
            out.failure_ = Some(format!(
                "telegraph 报文的远端地址是 {remote:?}，期望对端 telegraph dock {peer_dock}"
            ));
            break;
        }
        if frame.len() < K_HEADER_LEN {
            out.failure_ = Some(format!(
                "telegraph 报文只有 {} 字节，装不下 {K_HEADER_LEN} 字节的信封头",
                frame.len()
            ));
            break;
        }
        let head: &[u8; K_HEADER_LEN] = match frame[..K_HEADER_LEN].try_into() {
            Ok(head) => head,
            Err(_) => {
                out.failure_ = Some("信封头切片长度不符".to_string());
                break;
            }
        };
        let envelope = match Envelope::decode(head) {
            Ok(envelope) => envelope,
            Err(err) => {
                out.failure_ = Some(format!("信封解码失败：{err}"));
                break;
            }
        };
        if envelope.is_data() {
            let payload = &frame[K_HEADER_LEN..];
            match verify_telegram_data_(&envelope, payload, params, other_side) {
                Ok(ack) => {
                    out.recv_verified_ += 1u64;
                    if let Err(note) = send_ack_(&mut endpoints.tx_, peer_dock, ack).await {
                        out.failure_ = Some(note);
                        break;
                    }
                }
                Err(note) => {
                    out.failure_ = Some(note);
                    break;
                }
            }
        } else if envelope.is_receipt() {
            if let Err(note) = verify_ack_(&envelope, acked, params, my_side) {
                out.failure_ = Some(note);
                break;
            }
            acked += 1u64;
            crate::trace_!("telegraph：第 {} 帧已被 ACK", acked - 1u64);
        } else {
            out.failure_ = Some(format!("未知的信封种类：{}", envelope.kind));
            break;
        }
    }
    out.sent_acked_ = acked;
    crate::trace_!(
        "telegraph 闭环结束：acked={} recv_verified={} failure={:?}",
        out.sent_acked_,
        out.recv_verified_,
        out.failure_
    );
    out
}

/// 发出第 `seq` 帧：定长信封 + 确定性载荷，写完即**提交**（提交才定报文边界）。
async fn send_telegram_(
    tx: &mut Sender<Cfg>,
    peer_dock: u32,
    seq: u64,
    params: &PhaseParams,
    my_side: u64,
) -> Result<(), String> {
    let len = params.tg_bytes as u64;
    let idx = K_TG_IDX_BASE + seq;
    let digest = digest_of(params.seed, idx, my_side, len);

    let mut frame = Vec::with_capacity(K_HEADER_LEN + params.tg_bytes);
    frame.extend_from_slice(&Envelope::data(seq, len, digest).encode());
    let mut src = PayloadSrc::new(params.seed, idx, my_side, len);
    let mut buf = vec![0u8; params.tg_bytes];
    while src.remaining() > 0u64 {
        let n = src.fill(&mut buf);
        frame.extend_from_slice(&buf[..n]);
    }

    tx.write_all(&frame)
        .await
        .map_err(|err| format!("telegraph 第 {seq} 帧写入发送环失败：{err}"))?;
    let demand = Demand::at_least(frame.len());
    let submitted = tx
        .send_async(Dock::new(peer_dock), &demand)
        .await
        .map_err(|err| format!("telegraph 第 {seq} 帧提交失败：{err}"))?;
    if submitted != frame.len() {
        return Err(format!(
            "telegraph 第 {seq} 帧只提交了 {submitted} 字节，期望 {}",
            frame.len()
        ));
    }
    Ok(())
}

/// 回一条 ACK（它本身也是一条 telegraph 报文）。
async fn send_ack_(tx: &mut Sender<Cfg>, peer_dock: u32, ack: Envelope) -> Result<(), String> {
    let bytes = ack.encode();
    tx.write_all(&bytes)
        .await
        .map_err(|err| format!("ACK 写入发送环失败：{err}"))?;
    let demand = Demand::at_least(bytes.len());
    let submitted = tx
        .send_async(Dock::new(peer_dock), &demand)
        .await
        .map_err(|err| format!("ACK 提交失败：{err}"))?;
    if submitted != bytes.len() {
        return Err(format!(
            "ACK 只提交了 {submitted} 字节，期望 {}",
            bytes.len()
        ));
    }
    Ok(())
}

/// 从 telegraph 接收半边取**一条完整报文**，返回 `(远端地址, 报文原始字节)`。
///
/// 读走内容才算消费（段模型：不搬走就不会推进环的读位置），因此这里把整条报文搬进
/// 一个临时 `Vec` 再交给上层解析。
async fn recv_telegram_(rx: &mut Receiver<Cfg>) -> Result<(Dock, Vec<u8>), String> {
    let mut datagram: TgDataGram<'_> = rx
        .recv_async()
        .await
        .map_err(|err| format!("telegraph 接收失败：{err}"))?;
    let remote = <TgDataGram<'_> as TrRecvDatagram<Cfg>>::remote_dock(&datagram);
    let len = datagram.least_count();
    let mut raw = vec![MaybeUninit::<u8>::uninit(); len];
    let got = TrBuffSegmRef::move_items_to_buff(&mut datagram, &mut raw);
    if got != len {
        return Err(format!("搬出的电报载荷 {got} 字节与段长 {len} 不符"));
    }
    let mut frame = Vec::with_capacity(len);
    for slot in raw {
        // SAFETY: `move_items_to_buff` 刚刚把 `0..got`（= 全部 `len` 个槽位）初始化；
        // `u8` 的任意位模式都合法，且没有 drop 资源需要处理。
        frame.push(unsafe { slot.assume_init() });
    }
    Ok((remote, frame))
}

/// 校验一条收到的数据帧，返回应当回给对端的 ACK。
///
/// 三件事都要对：声明长度、逐字节内容、以及信封里声明的 digest。
fn verify_telegram_data_(
    envelope: &Envelope,
    payload: &[u8],
    params: &PhaseParams,
    from_side: u64,
) -> Result<Envelope, String> {
    if envelope.len != params.tg_bytes as u64 {
        return Err(format!(
            "第 {} 帧声明长度 {} 与 `--tg-bytes`（{}）不符",
            envelope.id, envelope.len, params.tg_bytes
        ));
    }
    if payload.len() != params.tg_bytes {
        return Err(format!(
            "第 {} 帧实际载荷 {} 字节与声明 {} 不符",
            envelope.id,
            payload.len(),
            envelope.len
        ));
    }
    let idx = K_TG_IDX_BASE + envelope.id;
    let mut src = PayloadSrc::new(params.seed, idx, from_side, envelope.len);
    let mut want = vec![0u8; params.tg_bytes];
    let filled = src.fill(&mut want);
    if filled != want.len() {
        return Err(format!("第 {} 帧的期望载荷提前耗尽", envelope.id));
    }
    if want != payload {
        let at = want
            .iter()
            .zip(payload.iter())
            .position(|(a, b)| a != b)
            .unwrap_or(0usize);
        return Err(format!("第 {} 帧第 {at} 字节与确定性载荷不符", envelope.id));
    }
    let actual = digest_bytes_(payload);
    if actual != envelope.digest as u32 {
        return Err(format!(
            "第 {} 帧信封声明的 digest（{:#010x}）与实算值（{actual:#010x}）不符",
            envelope.id, envelope.digest
        ));
    }
    Ok(Envelope::receipt(envelope.id, envelope.len, actual, true))
}

/// 校验对端回来的 ACK 是否确认了我方第 `expect_seq` 帧。
fn verify_ack_(
    envelope: &Envelope,
    expect_seq: u64,
    params: &PhaseParams,
    my_side: u64,
) -> Result<(), String> {
    if envelope.id != expect_seq {
        return Err(format!(
            "ACK 的序号 {} 与正在等待的帧 {expect_seq} 不符",
            envelope.id
        ));
    }
    if envelope.len != params.tg_bytes as u64 {
        return Err(format!(
            "ACK 报告的长度 {} 与 `--tg-bytes`（{}）不符",
            envelope.len, params.tg_bytes
        ));
    }
    if !envelope.receipt_agrees() {
        return Err(format!("对端报告第 {expect_seq} 帧的内容与声明不一致"));
    }
    let want = digest_of(
        params.seed,
        K_TG_IDX_BASE + expect_seq,
        my_side,
        params.tg_bytes as u64,
    );
    if envelope.digest != u64::from(want) {
        return Err(format!(
            "ACK 里的 digest（{:#010x}）与我声明的（{want:#010x}）不符",
            envelope.digest
        ));
    }
    Ok(())
}

/// 对一段字节算 CRC-32（与 [`digest_of`] 同一多项式，因此两者可直接比对）。
fn digest_bytes_(bytes: &[u8]) -> u32 {
    let crc = smux_v1::x_deps::crc::Crc::<u32>::new(&smux_v1::x_deps::crc::CRC_32_ISO_HDLC);
    let mut digest = crc.digest();
    digest.update(bytes);
    digest.finalize()
}

/// 一条子流的完整闭环：双向各发一份不同载荷，各自收齐后回一条回执。
async fn run_channel(
    idx: u64,
    mut tx: ChannelTx<Cfg>,
    mut rx: ChannelRx<Cfg>,
    params: &PhaseParams,
    my_side: u64,
) -> ChannelOutcome {
    let other_side = if my_side == K_SIDE_INITIATOR {
        K_SIDE_ACCEPTER
    } else {
        K_SIDE_INITIATOR
    };
    let my_len = params.bytes;
    let my_digest = digest_of(params.seed, idx, my_side, my_len);
    let their_digest = digest_of(params.seed, idx, other_side, my_len);

    // 两条方向同时推进：先发的那份 digest 是**将要发**的数据的 digest（先告知、后本体）。
    let (send_res, recv_res) = futures::join!(
        send_body(&mut tx, idx, my_len, my_digest, params, my_side),
        recv_body(&mut rx, idx, my_len, their_digest, params, other_side),
    );

    crate::trace_!("子流 {idx}：双向本体交换结束（发 {send_res:?} 收 {recv_res:?}）",);
    let mut failure: Option<String> = None;
    let mut sent_bytes = 0u64;
    match send_res {
        Ok(written) => sent_bytes = written,
        Err(err) => failure = Some(format!("发送失败：{err}")),
    }

    let (recv_bytes, recv_actual, recv_ok) = match recv_res {
        Ok(v) => {
            if !v.agrees {
                let note = match &v.byte_mismatch {
                    Some(msg) => format!("接收内容不一致：{msg}"),
                    None => "接收内容与对端声明的 digest 不一致".to_string(),
                };
                failure.get_or_insert(note);
            }
            (v.bytes, v.digest, v.agrees)
        }
        Err(err) => {
            failure.get_or_insert(format!("接收失败：{err}"));
            (0u64, 0u32, false)
        }
    };

    // 回执：把我**实际算出**的 digest 告诉对端（不一致时带上失败标记）。
    let receipt = Envelope::receipt(idx, my_len, recv_actual, recv_ok);
    if let Err(err) = write_all(&mut tx, &receipt.encode()).await {
        failure.get_or_insert(format!("回执发送失败：{err}"));
    }
    crate::trace_!("子流 {idx}：回执已写");
    // 先看一眼中止原因，再把发送方向半关闭（`drop(tx)` 之后就拿不到它了）。
    if let Some(reason) = tx.abort_reason() {
        failure.get_or_insert(format!("子流被连接内部中止：{reason:?}"));
    }
    drop(tx);
    crate::trace_!("子流 {idx}：已半关闭（FIN 发出）");

    // 读对端给我的回执：这是「我发的 == 对方收的」那份证据。
    let mut sent_acked = false;
    match read_receipt(&mut rx, idx, my_len, my_digest).await {
        Ok(()) => sent_acked = true,
        Err(note) => {
            failure.get_or_insert(note);
        }
    }
    crate::trace_!("子流 {idx}：对端回执已处理（acked={sent_acked}）");
    if let Err(err) = expect_eof(&mut rx).await {
        failure.get_or_insert(format!("EOF 语义不对：{err}"));
    }
    crate::trace_!("子流 {idx}：已见到 EOF");
    if let Some(reason) = rx.abort_reason() {
        failure.get_or_insert(format!("子流读侧被连接内部中止：{reason:?}"));
    }

    ChannelOutcome {
        idx,
        sent_bytes,
        recv_bytes,
        sent_acked,
        recv_verified: recv_ok,
        failure,
    }
}

/// 写一份本体：先 `ENVELOPE`（长度 + digest），再分块发本体；返回写出的字节数。
async fn send_body<W>(
    tx: &mut W,
    idx: u64,
    len: u64,
    digest: u32,
    params: &PhaseParams,
    side: u64,
) -> Result<u64, Box<dyn std::error::Error>>
where
    W: abs_buff::TrBuffWrite<u8>,
    W::Err: std::error::Error + 'static,
{
    crate::trace_!("子流 {idx}：开始写本体（声明 {len} 字节，digest {digest:#010x}）");
    write_all(tx, &Envelope::data(idx, len, digest).encode()).await?;
    crate::trace_!("子流 {idx}：信封已写，开始发本体");
    let mut src = PayloadSrc::new(params.seed, idx, side, len);
    let mut buf = vec![0u8; params.chunk];
    let mut written = 0u64;
    while src.remaining() > 0u64 {
        let n = src.fill(&mut buf);
        write_all(tx, &buf[..n]).await?;
        written += n as u64;
    }
    crate::trace_!("子流 {idx}：本体写完，共 {written} 字节");
    Ok(written)
}

/// 收一份本体：读 `ENVELOPE`，按声明长度收齐，逐字节比对并增量算 digest。
///
/// 返回的 `digest` 是**本端实际算出**的值——它随后进回执，成为对端的证据。
async fn recv_body<R>(
    rx: &mut R,
    idx: u64,
    expect_len: u64,
    expect_digest: u32,
    params: &PhaseParams,
    side: u64,
) -> Result<RecvOutcome, Box<dyn std::error::Error>>
where
    R: abs_buff::TrBuffRead<u8>,
    R::Err: std::error::Error + 'static,
{
    crate::trace_!("子流 {idx}：等待对端信封");
    let mut header = [0u8; K_HEADER_LEN];
    read_exact(rx, &mut header).await?;
    crate::trace_!("子流 {idx}：收到对端信封");
    let envelope = Envelope::decode(&header)?;
    if !envelope.is_data() {
        return Err(format!("期望 DATA 信封，收到 kind={}", envelope.kind).into());
    }
    if envelope.id != idx {
        return Err(format!("信封序号不符：期望 {idx}，收到 {}", envelope.id).into());
    }
    if envelope.len != expect_len {
        return Err(format!(
            "信封声明的长度不符：期望 {expect_len}，收到 {}",
            envelope.len
        )
        .into());
    }

    let crc = smux_v1::x_deps::crc::Crc::<u32>::new(&smux_v1::x_deps::crc::CRC_32_ISO_HDLC);
    let mut digest = crc.digest();
    let mut src = PayloadSrc::new(params.seed, idx, side, expect_len);
    let mut got_buf = vec![0u8; params.chunk];
    let mut want_buf = vec![0u8; params.chunk];
    let mut got = 0u64;
    let mut byte_mismatch: Option<String> = None;
    while got < expect_len {
        let want = core::cmp::min(params.chunk as u64, expect_len - got) as usize;
        read_exact(rx, &mut got_buf[..want]).await?;
        let filled = src.fill(&mut want_buf[..want]);
        if filled != want {
            return Err(format!("期望载荷提前耗尽：{filled} != {want}").into());
        }
        if got_buf[..want] != want_buf[..want] && byte_mismatch.is_none() {
            let at = got_buf[..want]
                .iter()
                .zip(want_buf[..want].iter())
                .position(|(a, b)| a != b)
                .unwrap_or(0usize);
            byte_mismatch = Some(format!("载荷第 {} 字节与期望不符", got + at as u64));
        }
        digest.update(&got_buf[..want]);
        got += want as u64;
    }

    let actual = digest.finalize();
    let agrees = actual == expect_digest && envelope.digest == u64::from(actual);
    Ok(RecvOutcome {
        bytes: got,
        digest: actual,
        agrees,
        byte_mismatch,
    })
}

/// 读对端回执，并与本端声明的 digest 比对。
async fn read_receipt<R>(rx: &mut R, idx: u64, my_len: u64, my_digest: u32) -> Result<(), String>
where
    R: abs_buff::TrBuffRead<u8>,
    R::Err: std::error::Error + 'static,
{
    let mut header = [0u8; K_HEADER_LEN];
    read_exact(rx, &mut header)
        .await
        .map_err(|err| format!("读对端回执失败：{err}"))?;
    let receipt = Envelope::decode(&header).map_err(|err| format!("回执解码失败：{err}"))?;
    if !receipt.is_receipt() {
        return Err(format!("期望 RECEIPT，收到 kind={}", receipt.kind));
    }
    if receipt.id != idx {
        return Err(format!("回执序号不符：期望 {idx}，收到 {}", receipt.id));
    }
    if receipt.len != my_len {
        return Err(format!("回执长度不符：期望 {my_len}，收到 {}", receipt.len));
    }
    if !receipt.receipt_agrees() {
        return Err("对端报告它收到的字节与声明的 digest 不一致".to_string());
    }
    if receipt.digest != u64::from(my_digest) {
        return Err(format!(
            "对端算出的 digest（{:#010x}）与我声明的不符（{my_digest:#010x}）",
            receipt.digest
        ));
    }
    Ok(())
}

/// 收一份本体的结论。
#[derive(Debug, Clone)]
struct RecvOutcome {
    /// 收到的字节数。
    bytes: u64,
    /// 本端实际算出的 digest。
    digest: u32,
    /// digest 与逐字节比对是否都通过。
    agrees: bool,
    /// 逐字节比对失败的说明（没有失败时为 `None`）。
    byte_mismatch: Option<String>,
}
