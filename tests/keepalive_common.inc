//! 保活（`PULSE`）与空闲超时拆流的**端到端**场景（两个运行时共用）。
//!
//! # 为什么是一份 `.inc` 而不是普通测试文件
//!
//! 计时与时刻挂在**运行时值**上：tokio 用 `abs_art_tokio::Runtime`、compio 用
//! `abs_art_compio::Runtime`（本地作用域各自从对应的运行时值经 `rt.local_scope()`
//! 取得）。两对类型都不同，而测试 target 各自是独立 crate，无法共享模块定义——按仓内
//! 既有约定（`tests/smoke_common.inc`），把场景放进本文件，由两个薄壳
//! （`keepalive.rs` / `keepalive_compio.rs`）各 `#[path]` 引入一次。
//!
//! # 时间：**虚拟**的
//!
//! 本文件的两个场景都靠 `abs_art_mock_clock` 把时间变成虚拟的：壳用
//! `ManualTime::new(运行时值, 手动时钟)` 装饰出运行时值 `RT`，再用
//! `LocalScope::block_on_advancing(&clock, body)` 驱动——驱动在「没有别的活可干」时把
//! 虚拟时钟推进到下一个到期时刻并唤醒到期任务，因此真实耗时几乎为零，而**事件顺序**
//! 与真实时间下相同（`max_channel_timeout` 仍是协议协商出的整秒值）。
//!
//! 正因如此，本文件里**不能**写 `rt.delay(1.6s)` 来「等 1.6 秒」：那会让驱动一次把时钟
//! 推到底，中间的 `PULSE` 与空闲超时判定全被跳过。等待一律走 `virtual_sleep_`——它只
//! **让出**，由驱动按定时器粒度逐格推进。
//!
//! 反例（本文件诞生时撞到的）：用一个 tokio 作用域在 `#[compio::test]` 下跑同一份场景。
//! 连接的前四个循环不碰计时器，那种写法一直「看起来能跑」；第五个循环一落地就会在
//! compio 运行时里调 `tokio::time::sleep`，直接 panic「no reactor running」。
//!
//! # 传输装配
//!
//! 与 `tests/inmem_mux.rs` 相同：两条**全被动内存环**分别承载 A→B 与 B→A，四个环半部
//! 直接交给两个 [`MuxConnection`]，中间没有任何泵。两条环由同一个构造器造出，因此两端
//! 的传输类型相同——本文件的辅助函数因此可以写成**具体类型**而不是一堆泛型。
//!
//! # 两条互补的路线
//!
//! | 用例 | 它钉住什么 |
//! | --- | --- |
//! | [`idle_channel_times_out_`] | **对端静默**时本端在 `max_channel_timeout` 后拆掉该子流并报 `IdleTimeout`；**连接本身不死**（同连接上还能再建流收发）；对端只看到一次正常关闭 |
//! | [`keepalive_pulses_`] | **两端都活着**时，`timeout/2` 的 `PULSE` 往返足以让完全空闲的子流无限期活下去 |
//! | [`establish_timeout_on_initiator_`] | **建流未裁决**的发起方：对端迟迟不裁决时在 `max_channel_timeout` 后拿到 `IdleTimeout`，不再无限等 |
//! | [`establish_timeout_on_responder_`] | **建流未裁决**的响应方：超时后连接代它向对端回 `REJECT`（对端拿到 `Refused`），它自己之后调 `accept_async` 直接拿到 `IdleTimeout` |
//! | [`cancel_accept_notifies_peer_`] | 带取消令牌的 `accept_async` 在裁决完成前被取消：调用方拿到 `Cancelled`，**同时**对端收到 `REJECT`（拿到 `Refused`），不会悬着 |

use core::{future::IntoFuture, time::Duration};

use abs_smux::{
    chan::{TrChannelHalf, TrChannelHandle},
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};
use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use futures::join;
use mm_ptr::x_deps::abs_mm::CoreAlloc;
use smux_v1::{
    connection::{
        BufferedRx, BufferedTx, ChannelRx, ChannelTx, Dock, MuxConnection, MuxError, TrConnCfg,
    },
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::{BasicOpts, HandshakeOpts},
    },
    time::TrClock,
};

use buffex::x_deps::abs_cancel::{CancelledToken, TrMayCancel};
use smux_v1::connection::HandleError;

use crate::common::{
    ClosurePrepare, K_CHANNEL_CAPACITY, K_NET_BUFFER_SIZE, SmokeBuff, SmokeConn, SmokeMuxConfig,
    TrSmokeRt, make_passive_ring_, read_channel_exact_, write_channel_all_,
};

// ── 具体类型：两条环由同一个构造器造出，因此两端传输类型相同 ────────────────

/// 内存环的发送半边。
type RingTx = BufferedTx<SmokeBuff, CoreAlloc>;

/// 内存环的接收半边。
type RingRx = BufferedRx<SmokeBuff, CoreAlloc>;

/// 本文件使用的连接类型（传输由上面的两个别名固定；第三个参数是**运行时值**）。
type Conn<RT> = SmokeConn<RingRx, RingTx, RT>;

/// 本文件使用的两个半部类型。
type Tx<RT> = ChannelTx<SmokeMuxConfig<RingTx, RingRx, RT>>;

/// 本文件使用的接收半部类型。
type Rx<RT> = ChannelRx<SmokeMuxConfig<RingTx, RingRx, RT>>;

// ── 量级 ──────────────────────────────────────────────────────────────────

/// 被验的保活超时：1 秒（协议按整秒协商，1 是最小可用值）。
const K_TIMEOUT: Duration = Duration::from_secs(1u64);

/// 「对端完全静默」用的超大超时：在本用例的时间尺度内它不会发任何 `PULSE`。
const K_SILENT_TIMEOUT: Duration = Duration::from_secs(3600u64);

// ── 建连 ──────────────────────────────────────────────────────────────────

/// 建一对连接，**两端各自的**空闲超时可不同。
///
/// `rt` 是**运行时值**（进连接的类型参数，并提供计时与时刻），`scope` 是**本地作用域**
/// （由连接自己经 [`smux_v1::connection::ScopeHost`] 取得并投递五个循环）。
///
/// # 为什么要显式改写协商结果
///
/// `max_channel_timeout` 是双方协商出来的**同一个**值，而本文件的两条路线都需要
/// 「一端活跃、一端静默」这种非对称配置：对端可能因为版本或配置不同而不做保活，
/// 这正是空闲超时要覆盖的真实情形。因此握手完成后直接改写各自的
/// [`HandshakeDelivery::opts`](smux_v1::handshake::agent::HandshakeDelivery)（字段公开）。
async fn connect_asymmetric_<RT>(
    rt: &RT,
    tx_a: RingTx,
    rx_a: RingRx,
    tx_b: RingTx,
    rx_b: RingRx,
    timeout_a: Duration,
    timeout_b: Duration,
) -> (Conn<RT>, Conn<RT>)
where
    RT: TrSmokeRt + smux_v1::connection::ScopeHost,
{
    let invite_opts = BasicOpts {
        max_channel_timeout: timeout_a,
        ..BasicOpts::default()
    };
    let listen_opts = BasicOpts {
        max_channel_timeout: timeout_b,
        ..BasicOpts::default()
    };
    let invite_fut = HandshakeAgent::new(tx_a, rx_a).invite_async(&invite_opts, AcceptAllEntries);
    let listen_fut = HandshakeAgent::new(tx_b, rx_b).listen_async(&listen_opts, AcceptAllEntries);
    let (invited, accepted) = join!(async { invite_fut.await }, async { listen_fut.await });
    let mut delivery_a = invited.expect("发起方握手应当成功");
    let mut delivery_b = accepted.expect("等待方握手应当成功");

    delivery_a.opts = HandshakeOpts {
        basic_opts: BasicOpts {
            max_channel_timeout: timeout_a,
            ..delivery_a.opts.basic_opts.clone()
        },
    };
    delivery_b.opts = HandshakeOpts {
        basic_opts: BasicOpts {
            max_channel_timeout: timeout_b,
            ..delivery_b.opts.basic_opts.clone()
        },
    };

    let config_a = SmokeMuxConfig::<RingTx, RingRx, RT>::new(rt.clone());
    let config_b = SmokeMuxConfig::<RingTx, RingRx, RT>::new(rt.clone());
    let (stage_ar, stage_aw) = config_a
        .make_stage_buffs(config_a.allocator())
        .expect("A 侧连接级帧暂存应当分配成功");
    let (stage_br, stage_bw) = config_b
        .make_stage_buffs(config_b.allocator())
        .expect("B 侧连接级帧暂存应当分配成功");
    // 运行时值与作用域都由配置解决（`TrConnCfg::runtime` + `ScopeHost`）。
    let conn_a: Conn<RT> =
        MuxConnection::new(delivery_a, config_a, stage_ar, stage_aw);
    let conn_b: Conn<RT> =
        MuxConnection::new(delivery_b, config_b, stage_br, stage_bw);
    (conn_a, conn_b)
}

/// 在 `conn_a`（临时 dock `local_a`）与 `conn_b`（监听 dock `listen_b`）之间建立
/// **一条**子流，返回两端的两个半部。
///
/// 建流阶段本身会产生帧（`OPEN` / `ACCEPT`），因此两个方向都会被 `touch`，空闲计时
/// 从这一刻起算。
async fn establish_one_channel_<RT>(
    conn_a: &Conn<RT>,
    conn_b: &Conn<RT>,
    local_a: u32,
    listen_b: u32,
) -> (Tx<RT>, Rx<RT>, Tx<RT>, Rx<RT>)
where
    RT: TrSmokeRt + smux_v1::connection::ScopeHost,
{
    let dock_a = Dock::new(local_a);
    let dock_b = Dock::new(listen_b);

    let mut listener = conn_b
        .bind_async(dock_b)
        .await
        .expect("B 侧绑定监听 dock 应当成功")
        .listen_async_default()
        .await
        .expect("B 侧建立 listener 应当成功");

    let mut message: &[u8] = &[];
    let mut handle_a = conn_a
        .bind_async(dock_a)
        .await
        .expect("A 侧绑定发起 dock 应当成功");
    let mut handle_a = handle_a
        .open_channel_async(dock_b, &mut message)
        .await
        .expect("A 侧发起子流应当成功");
    // 两端的最终裁决必须**并发**：主动方的 `OPEN` 只在它自己的 `accept_async` 里发出，
    // 而被动方的 `OPEN` + `ACCEPT` 也只在它自己的 `accept_async` 里发出（§4.2）。
    // 因此顺序写会互等：先 `income_async` 等一个还没发出的 `OPEN`，或先等 A 的
    // `accept_async` 完成（它要等 B 的 `OPEN` + `ACCEPT`）。这里用一个 `join!`：
    // 第一支发 A 的 `OPEN` 并等 B 的两条帧，第二支先取句柄、再发 B 的两条帧。
    let mut welcome_a_buf: [u8; 0] = [];
    let mut welcome_b_buf: [u8; 0] = [];
    let (accepted_a, accepted_b) = join!(
        async {
            let mut welcome_a: &mut [u8] = &mut welcome_a_buf[..];
            handle_a
                .accept_async_managed(&mut welcome_a, K_CHANNEL_CAPACITY)
                .await
                .expect("A 侧最终裁决子流应当成功")
        },
        async {
            let mut handle_b = listener
                .income_async()
                .await
                .expect("B 侧应当取到入向建流请求");
            let mut welcome_b: &mut [u8] = &mut welcome_b_buf[..];
            handle_b
                .accept_async_managed(&mut welcome_b, K_CHANNEL_CAPACITY)
                .await
                .expect("B 侧接受子流应当成功")
        },
    );
    let (tx_a, rx_a) = accepted_a;
    let (tx_b, rx_b) = accepted_b;
    (tx_a, rx_a, tx_b, rx_b)
}

/// 在一条已建立的子流上做一次单向往返：`tx` 写、`rx` 读回。
///
/// 同一条连接上的**第二条**子流也走它——这正是「空闲超时只拆子流、不拆连接」的
/// 判别动作。
async fn round_trip_<W, R>(tx: &mut W, rx: &mut R, bytes: &[u8])
where
    W: TrBuffWrite<u8>,
    R: TrBuffRead<u8>,
{
    write_channel_all_(tx, bytes)
        .await
        .expect("子流写入应当成功");
    let mut got = vec![0u8; bytes.len()];
    read_channel_exact_(rx, &mut got)
        .await
        .expect("子流读取应当成功");
    assert_eq!(got, bytes, "往返载荷应当逐字节相等");
}

// ── 虚拟时间：让出驱动，由它按定时器粒度推进时钟 ──────────────────────────

/// 让出一次执行权：自唤醒一次后返回 `Pending`。
///
/// 这是 [`virtual_sleep_`] 的步进单位。每次让出都给驱动一次「推进虚拟时钟到下一个
/// 到期时刻并驱动本地队列」的机会，因此保活 `PULSE` 与超时判定都会按真实顺序发生。
async fn yield_once_() {
    let mut first = true;
    core::future::poll_fn(move |cx| {
        if first {
            first = false;
            cx.waker().wake_by_ref();
            core::task::Poll::Pending
        } else {
            core::task::Poll::Ready(())
        }
    })
    .await;
}

/// 在**虚拟时间**上等待 `duration`：只让出，由驱动逐格推进时钟。
///
/// 判据是运行时值自己报的 `now`——它与连接内部的计时器**同源**（`TrTime: TrDelay +
/// TrClock`），因此这里等到的「1.6 秒」与连接看到的「1.6 秒」在同一条时间轴上。
async fn virtual_sleep_<RT>(rt: &RT, duration: Duration)
where
    RT: TrClock,
{
    let start = rt.now();
    let mut rounds = 0u32;
    while rt.now() - start < duration {
        rounds += 1;
        if rounds.is_multiple_of(200) {
            std::eprintln!(
                "[probe] virtual_sleep rounds={rounds} elapsed_ms={}",
                (rt.now() - start).as_millis()
            );
        }
        yield_once_().await;
    }
    std::eprintln!(
        "[probe] virtual_sleep DONE rounds={rounds} elapsed_ms={}",
        (rt.now() - start).as_millis()
    );
}

// ── 用例一：对端静默 ⇒ 拆子流、不拆连接 ────────────────────────────────────

/// 测试目标：**对端不做保活**时，本端在 `max_channel_timeout` 之后拆掉该子流并报
/// 空闲超时，而**整条连接不受影响**。
///
/// - 手段：A 侧超时 1 秒、B 侧超时 1 小时（B 因此在本用例的时间尺度内不发 `PULSE`）；
///   建立一条子流后两端都不发数据，用 [`virtual_sleep_`] 在虚拟时间上等 1.6 秒
///   （驱动会按定时器粒度推进，因此 A 侧那 500 ms 的 `PULSE` 与 1 秒处的超时判定
///   都按真实顺序发生）。
/// - 判断：
///   1. A 侧两个半部的 `abort_reason()` 都是 `Some(IdleTimeout)`（原因在共享状态里，
///      两个半部读到同一个），且环已进入关闭态；
///   2. B 侧**没有**中止原因——它对端是「收到 `CLOSE(FIN)` + `CLOSE(RESET)`」，属正常
///      关闭，这正是「保活判定」与「对端主动关闭」在应用侧可区分的判据；
///   3. 同一条连接上再建一条子流并双向往返成功 ⇒ 拆的只是那条子流，连接还活着。
pub async fn idle_channel_times_out_<RT>(rt: &RT)
where
    RT: TrSmokeRt + smux_v1::connection::ScopeHost,
{
    let (a_tx, b_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);

    std::eprintln!("[probe] idle: begin connect");
    let (conn_a, conn_b) =
        connect_asymmetric_(rt, a_tx, a_rx, b_tx, b_rx, K_TIMEOUT, K_SILENT_TIMEOUT).await;
    std::eprintln!("[probe] idle: connected; begin establish");
    let (tx_a, rx_a, tx_b, rx_b) =
        establish_one_channel_(&conn_a, &conn_b, 0x1000u32, 1u32).await;
    std::eprintln!("[probe] idle: established; begin virtual_sleep");

    // 两端都不发数据：A 侧应当在自己那 500 ms 的保活阈值上发一条 `PULSE`，
    // 但在 1 秒处判定空闲超时；B 侧的超时是 1 小时，不会主动拆。
    virtual_sleep_(rt, Duration::from_millis(1600u64)).await;
    std::eprintln!("[probe] idle: slept; asserting");

    // 1. A 侧：子流被空闲超时拆掉。
    assert_eq!(
        tx_a.abort_reason(),
        Option::Some(MuxError::IdleTimeout),
        "A 侧发送半部应当报空闲超时"
    );
    assert_eq!(
        rx_a.abort_reason(),
        Option::Some(MuxError::IdleTimeout),
        "两个半部共享同一个中止原因"
    );
    assert!(
        rx_a.is_tx_closed(),
        "被拆子流的接收方向应当读到 EOF（环写端已关闭）"
    );
    assert!(
        tx_a.is_rx_closed(),
        "被拆子流的发送方向应当看到消费端消失"
    );

    // 2. B 侧：只是收到对端的关闭帧，不是超时。
    assert_eq!(
        tx_b.abort_reason(),
        Option::None,
        "B 侧不该被自己的保活判定中止"
    );
    assert_eq!(
        rx_b.abort_reason(),
        Option::None,
        "B 侧两个半部同样没有中止原因"
    );
    assert!(
        rx_b.is_tx_closed(),
        "A 侧的 CLOSE(FIN) 应当让 B 侧读到 EOF"
    );
    // 注意：对端 `RESET` **不**关闭 B 侧发送环的消费端——协议只要求「允许放弃已
    // 提交字节」，而既有验收 `mux_recv_dropped_dual_` 钉住了「对端丢弃接收半边
    // 之前写入的字节仍然写得完」。这条不对称记在 dev-notes 的遗留里。

    // 3. 连接本身还活着：换一个临时 dock 再建一条子流并往返。
    let (mut tx_a2, mut rx_a2, mut tx_b2, mut rx_b2) =
        establish_one_channel_(&conn_a, &conn_b, 0x1001u32, 1u32).await;
    round_trip_(&mut tx_a2, &mut rx_b2, b"after-timeout-a-to-b").await;
    round_trip_(&mut tx_b2, &mut rx_a2, b"after-timeout-b-to-a").await;
}


// ── 用例二：两端都活着 ⇒ PULSE 让它活过好几个超时周期 ──────────────────────

/// 测试目标：**两端都在做保活**时，一条完全无数据的子流可以活过好几个
/// `max_channel_timeout` 周期——`PULSE` 的往返把它一直「救活」。
///
/// 这条用例同时钉住一条容易写反的语义：**本端发出 `PULSE` 本身不算活跃**。若某次
/// 实现把「我发了 PULSE」也记成活跃，本用例仍会通过（子流同样活着），但
/// [`idle_channel_times_out_`] 会失败（对端已静默，
/// 本端却永远不拆）。两条必须同时成立。
///
/// - 手段：两端超时都设 1 秒；建立一条子流后不发任何数据，用 [`virtual_sleep_`] 在
///   虚拟时间上等 2.5 个超时周期（跨过 5 个保活周期，每个都由驱动推进并执行）。
/// - 判断：两侧都没有中止原因、两个方向的环都还开着，且这条「一直空闲」的子流仍能
///   双向往返。
pub async fn keepalive_pulses_<RT>(rt: &RT)
where
    RT: TrSmokeRt + smux_v1::connection::ScopeHost,
{
    let (a_tx, b_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);

    let (conn_a, conn_b) =
        connect_asymmetric_(rt, a_tx, a_rx, b_tx, b_rx, K_TIMEOUT, K_TIMEOUT).await;
    let (mut tx_a, mut rx_a, mut tx_b, mut rx_b) =
        establish_one_channel_(&conn_a, &conn_b, 0x2000u32, 2u32).await;

    // 超过两个超时周期（PULSE 阈值是它的一半，因此这里跨过 5 个保活周期）。
    virtual_sleep_(rt, Duration::from_millis(2500u64)).await;

    assert_eq!(
        tx_a.abort_reason(),
        Option::None,
        "两端互发 PULSE ⇒ A 侧不该被判空闲超时"
    );
    assert_eq!(rx_a.abort_reason(), Option::None);
    assert_eq!(
        tx_b.abort_reason(),
        Option::None,
        "两端互发 PULSE ⇒ B 侧不该被判空闲超时"
    );
    assert_eq!(rx_b.abort_reason(), Option::None);
    assert!(!tx_a.is_rx_closed(), "子流不应被拆掉（发送方向仍可用）");
    assert!(!rx_a.is_tx_closed(), "子流不应被拆掉（接收方向仍是 EOF 之前）");

    // 「一直空闲」的子流仍然可用。
    round_trip_(&mut tx_a, &mut rx_b, b"still-alive-a-to-b").await;
    round_trip_(&mut tx_b, &mut rx_a, b"still-alive-b-to-a").await;
}


// ── 用例三：建流未裁决 —— 发起方一侧超时 ────────────────────────────────────

/// 测试目标：**发起方**发出 `OPEN` 之后对端迟迟不裁决（listener 不 accept）时，
/// `max_channel_timeout` 到点必须让它拿到明确的超时错误，而不是永久悬在等裁决上。
///
/// - 手段：A 侧超时 1 秒、B 侧 1 小时；B 只建 listener **不** accept；A 的
///   `accept_async_managed`（它内部才发 `OPEN` 并等裁决）与 `virtual_sleep_(1.6s)`
///   竞速推进。
/// - 判断：竞速必须由「建流 future 完成」取胜，且错误是
///   `HandleError::Mux(MuxError::IdleTimeout)`；句柄上的 `abort_reason()` 同样是它。
pub async fn establish_timeout_on_initiator_<RT>(rt: &RT)
where
    RT: TrSmokeRt + smux_v1::connection::ScopeHost,
{
    let (a_tx, b_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (conn_a, conn_b) =
        connect_asymmetric_(rt, a_tx, a_rx, b_tx, b_rx, K_TIMEOUT, K_SILENT_TIMEOUT).await;

    // B 只监听、不 accept：因此它不会回 `OPEN`，也不会回 `ACCEPT` / `REJECT`。
    let dock_b = Dock::new(2u32);
    let _listener_b = conn_b
        .bind_async(dock_b)
        .await
        .expect("B 侧绑定监听 dock 应当成功")
        .listen_async_default()
        .await
        .expect("B 侧建立 listener 应当成功");

    let dock_a = Dock::new(0x2100u32);
    let mut handle_a = conn_a
        .bind_async(dock_a)
        .await
        .expect("A 侧绑定发起 dock 应当成功");
    let mut message: &[u8] = &[];
    let mut handle_a = handle_a
        .open_channel_async(dock_b, &mut message)
        .await
        .expect("A 侧本地登记应当成功");

    let mut welcome_a_buf: [u8; 0] = [];
    let mut welcome_a: &mut [u8] = &mut welcome_a_buf[..];
    {
        let accept_fut = handle_a.accept_async_managed(&mut welcome_a, K_CHANNEL_CAPACITY);
        futures::pin_mut!(accept_fut);
        // 先推几轮让 `OPEN` 发出去并进入等待。
        for _ in 0..8 {
            yield_once_().await;
        }
        let sleep_fut = virtual_sleep_(rt, Duration::from_millis(1600u64));
        futures::pin_mut!(sleep_fut);
        match futures::future::select(accept_fut.as_mut(), sleep_fut).await {
            futures::future::Either::Left((outcome, _)) => match outcome {
                Result::Err(HandleError::Mux(MuxError::IdleTimeout)) => {}
                Result::Err(err) => {
                    panic!("发起方建流超时应得到 IdleTimeout，实际错误：{err:?}")
                }
                Result::Ok(_) => panic!("建流超时时不应成功拿到两个半部"),
            },
            futures::future::Either::Right(((), _)) => panic!(
                "建流超时未生效：虚拟时间已过 1.6s，发起方仍停在等裁决上"
            ),
        }
    }
    assert_eq!(
        handle_a.abort_reason(),
        Option::Some(MuxError::IdleTimeout),
        "建流超时的原因应当留在句柄上，供调用方随时查"
    );
}

// ── 用例四：建流未裁决 —— 响应方一侧超时（并代它通知对端） ──────────────────

/// 测试目标：**响应方**收到对端 `OPEN` 之后迟迟不裁决时：
/// (1) 本端超时会**代它向对端回 `REJECT`**，对端因此拿到 `Refused`（不悬着）；
/// (2) 它自己之后调 `accept_async` 直接拿到 `IdleTimeout`，而不是 `Closed`
///     或一次注定无效的成功。
///
/// - 手段：A 侧超时 1 小时、B 侧超时 1 秒；A 发 `OPEN` 并等裁决，B 取到入向句柄后
///   **不裁决**；推进虚拟时间越过 B 侧超时，再让 B 调 `accept_async_managed`。
/// - 判断：A 侧先得到 `HandleError::Refused`；B 侧随后得到
///   `HandleError::Mux(MuxError::IdleTimeout)`，且 `abort_reason()` 与之相同。
pub async fn establish_timeout_on_responder_<RT>(rt: &RT)
where
    RT: TrSmokeRt + smux_v1::connection::ScopeHost,
{
    let (a_tx, b_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    // A 侧超时 1 小时（不主动拆），B 侧超时 1 秒（要它超时）。
    let (conn_a, conn_b) =
        connect_asymmetric_(rt, a_tx, a_rx, b_tx, b_rx, K_SILENT_TIMEOUT, K_TIMEOUT).await;

    let dock_b = Dock::new(2u32);
    let mut listener_b = conn_b
        .bind_async(dock_b)
        .await
        .expect("B 侧绑定监听 dock 应当成功")
        .listen_async_default()
        .await
        .expect("B 侧建立 listener 应当成功");

    let dock_a = Dock::new(0x2200u32);
    let mut handle_a = conn_a
        .bind_async(dock_a)
        .await
        .expect("A 侧绑定发起 dock 应当成功");
    let mut message: &[u8] = &[];
    let mut handle_a = handle_a
        .open_channel_async(dock_b, &mut message)
        .await
        .expect("A 侧本地登记应当成功");

    let mut welcome_a_buf: [u8; 0] = [];
    let mut welcome_a: &mut [u8] = &mut welcome_a_buf[..];
    let accept_a = handle_a.accept_async_managed(&mut welcome_a, K_CHANNEL_CAPACITY);
    futures::pin_mut!(accept_a);

    // ① A 发 `OPEN`、B 取到入向句柄：两者竞速，B 一侧先完成。
    let mut handle_b = {
        let income_b = listener_b.income_async().into_future();
        futures::pin_mut!(income_b);
        match futures::future::select(accept_a.as_mut(), income_b).await {
            futures::future::Either::Left((_outcome, _)) => {
                panic!("B 侧还没取到入向请求，A 侧不应先完成")
            }
            futures::future::Either::Right((handle, _)) => {
                handle.expect("B 侧应当取到入向建流请求")
            }
        }
    };

    // ② B 不裁决：推进虚拟时间越过它那 1 秒超时（同时推进 A 的等待）。
    {
        let sleep_fut = virtual_sleep_(rt, Duration::from_millis(1600u64));
        futures::pin_mut!(sleep_fut);
        match futures::future::select(accept_a.as_mut(), sleep_fut).await {
            futures::future::Either::Left((outcome, _)) => match outcome {
                Result::Err(HandleError::Refused) => {}
                Result::Err(err) => panic!("A 侧应当收到 B 超时代发的 REJECT（Refused），实际：{err:?}"),
                Result::Ok(_) => panic!("A 侧不应在建流超时后拿到两个半部"),
            },
            futures::future::Either::Right(((), _)) => {
                panic!("B 侧建流超时之后没有把 REJECT 发给对端：A 侧仍在等")
            }
        }
    }

    // ③ B 之后才裁决：超时结果直接交给调用方。
    let mut welcome_b_buf: [u8; 0] = [];
    let mut welcome_b: &mut [u8] = &mut welcome_b_buf[..];
    match handle_b
        .accept_async_managed(&mut welcome_b, K_CHANNEL_CAPACITY)
        .await
    {
        Result::Err(HandleError::Mux(MuxError::IdleTimeout)) => {}
        Result::Err(err) => panic!("建流已超时的响应方 accept 应得 IdleTimeout，实际：{err:?}"),
        Result::Ok(_) => panic!("建流已超时的响应方 accept 不应成功"),
    }
    assert_eq!(
        handle_b.abort_reason(),
        Option::Some(MuxError::IdleTimeout),
        "响应方句柄上也应当读得到超时原因"
    );
}

// ── 用例五：带取消令牌的 accept 被取消 ⇒ 调用者 Cancelled、对端收到 REJECT ──

/// 测试目标：`accept_async` 带取消令牌、且在裁决真正完成之前收到取消信号时：
/// (1) 调用方得到 `HandleError::Cancelled`；
/// (2) 连接**同时**向对端宣告了 `REJECT`——对端拿到 `Refused`，不会悬在等裁决上。
///
/// - 手段：两侧超时都取 1 小时（本用例的时间尺度内不允许超时插手）；A 发 `OPEN` 并
///   等裁决，B 取到入向句柄后用**已经处于取消态**的令牌（[`CancelledToken`]）做
///   `accept_async`，两支并发推进（`join!`）。
/// - 判断：B 侧得到 `Cancelled`；A 侧得到 `Refused`。
pub async fn cancel_accept_notifies_peer_<RT>(rt: &RT)
where
    RT: TrSmokeRt + smux_v1::connection::ScopeHost,
{
    let (a_tx, b_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (conn_a, conn_b) = connect_asymmetric_(
        rt,
        a_tx,
        a_rx,
        b_tx,
        b_rx,
        K_SILENT_TIMEOUT,
        K_SILENT_TIMEOUT,
    )
    .await;

    let dock_b = Dock::new(2u32);
    let mut listener_b = conn_b
        .bind_async(dock_b)
        .await
        .expect("B 侧绑定监听 dock 应当成功")
        .listen_async_default()
        .await
        .expect("B 侧建立 listener 应当成功");

    let dock_a = Dock::new(0x2300u32);
    let mut handle_a = conn_a
        .bind_async(dock_a)
        .await
        .expect("A 侧绑定发起 dock 应当成功");
    let mut message: &[u8] = &[];
    let mut handle_a = handle_a
        .open_channel_async(dock_b, &mut message)
        .await
        .expect("A 侧本地登记应当成功");

    let mut welcome_a_buf: [u8; 0] = [];
    let mut welcome_a: &mut [u8] = &mut welcome_a_buf[..];
    let (outcome_a, outcome_b) = join!(
        async {
            // A：发 `OPEN` 并等裁决（B 取消后回 `REJECT` ⇒ 这里得到 `Refused`）。
            handle_a
                .accept_async_managed(&mut welcome_a, K_CHANNEL_CAPACITY)
                .await
        },
        async {
            // B：取到入向句柄，然后带**已经取消**的令牌做最终裁决。
            let mut handle_b = listener_b
                .income_async()
                .await
                .expect("B 侧应当取到入向建流请求");
            let mut welcome_b_buf: [u8; 0] = [];
            let mut welcome_b: &mut [u8] = &mut welcome_b_buf[..];
            let tx_buff = SmokeBuff::try_new(CoreAlloc, K_CHANNEL_CAPACITY)
                .expect("测试的子流环内存应当分配成功");
            let rx_buff = SmokeBuff::try_new(CoreAlloc, K_CHANNEL_CAPACITY)
                .expect("测试的子流环内存应当分配成功");
            let prep = ClosurePrepare::new(move || (tx_buff, rx_buff));
            let outcome = handle_b
                .accept_async(&mut welcome_b, prep)
                .may_cancel_with(CancelledToken)
                .await;
            assert_eq!(
                handle_b.abort_reason(),
                Option::None,
                "本用例靠取消收尾，不该出现超时原因"
            );
            outcome
        },
    );

    match outcome_b {
        Result::Err(HandleError::Cancelled) => {}
        Result::Err(err) => panic!("取消的 accept 应得 Cancelled，实际：{err:?}"),
        Result::Ok(_) => panic!("取消的 accept 不应成功"),
    }
    match outcome_a {
        Result::Err(HandleError::Refused) => {}
        Result::Err(err) => panic!("对端应当收到 REJECT（Refused），实际：{err:?}"),
        Result::Ok(_) => panic!("对端不应在取消场景下拿到两个半部"),
    }
}
