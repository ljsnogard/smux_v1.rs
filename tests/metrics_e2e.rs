//! `metrics` 特性的**端到端**验收：真实连接上「上报了什么」必须与场景一致。
//!
//! 本文件与 `src/metrics/tests_.rs` 分工不同——那边只钉住内置采集器的会计正确，
//! 这边要回答的是「插入点真的被触发了吗」：
//!
//! - 一条连接对、一条子流的完整生命周期（建连 → bind → open / accept → 双向收发 →
//!   半关闭）之后，**帧数、子流数、连接数、字节数**是否都真的涨了；
//! - 关闭连接时是否真的报出了「连接关闭」（该回调走核心的 `Drop` 路径）。
//!
//! # 为什么直接用内置的 `DebugMetrics`
//!
//! `TrConnCfg::Metrics` 是一个**关联类型**，因此「sink 存在哪」由配置实现决定。
//! 内置的 [`DebugMetrics`] 是 `Arc` 包住的一份**独立计数**、`Clone + Default`，
//! 因此配置只要把关联类型指向它、在结构体里放一份句柄、`metrics()` 把它借出去即可
//! ——**零静态存储、也不需要 mux 参与**（见 [`smux_v1::metrics`] 模块文档 §3）。
//!
//! 本文件**只有一个测试函数**：A / B 两侧共享同一份计数（经 `shared_sink_()` 取），
//! 多一个用例就会互相污染。
//!
//! # 运行方式
//!
//! ```bash
//! cargo test --test metrics_e2e --features metrics
//! ```

#![cfg(feature = "metrics")]

#[path = "common/mod.rs"]
mod common;

use core::marker::PhantomData;

use abs_smux::{
    chan::TrChannelHalf,
    conf::TrMuxConfig,
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};
use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};
use mm_ptr::{Owned, x_deps::abs_mm::CoreAlloc};
use smux_v1::{
    connection::{BuffAllocError, Dock, K_STAGE_RING_CAPACITY, ScopeHost, TrConnCfg},
    flow_ctrl::DefaultPolicy,
    metrics::{DebugMetrics, TrMetricsSink},
    single_runtime_test_,
};

use common::{
    AcceptAsyncClosureExt, K_NET_BUFFER_SIZE, SmokeBuff, TestConnCfg, TrLocalScope, TrSmokeRt,
    connect_pair_, expect_eof_, make_channel_buff_, make_passive_ring_, read_channel_exact_,
    write_channel_all_,
};

// 计数在 `DebugMetrics` 自己的**进程级静态**里，本文件因此不需要另备一份存储；
// 代价是它不与别的用例隔离——所以本 target 只有一个用例。

/// 编译期断言：内置采集器满足 `DefaultConnCfg` 构造器对 `M` 的要求
/// （`TrMetricsSink + Clone + Default`）。
///
/// 它把「`M` 参数真的可用」钉住——`DebugMetrics` 是 `Arc` 包住的一份独立计数，
/// 因此 `Clone` 与 `Default` 都成立（见 [`smux_v1::metrics`] 模块文档）。
const _: fn() = || {
    fn assert_usable_as_m_<M: TrMetricsSink + Clone + Default>() {}
    assert_usable_as_m_::<DebugMetrics>();
};

/// 本文件唯一的一份计数。
///
/// `TestConnCfg::new_` 只收运行时值（那是测试装配的统一契约），因此 sink 只能从一个
/// **进程级共享**的地方取；A / B 两侧拿到的是同一份计数的两个 `DebugMetrics` 句柄，
/// 于是断言覆盖的是两端之和。
fn shared_sink_() -> DebugMetrics {
    static SINK: std::sync::OnceLock<DebugMetrics> = std::sync::OnceLock::new();
    SINK.get_or_init(DebugMetrics::default).clone()
}

/// 带 metrics 的测试配置：与 `SmokeMuxConfig` 逐字同构，**只多一个 sink 字段**。
///
/// 它演示了「需求方怎么挂 sink」的最小代价：实现 `type Metrics` 与 `metrics()` 两处、
/// 并在结构里放一份句柄，其余一律照抄既有配置。
#[derive(Debug)]
struct MetricsCfg<W, R, RT> {
    _use_w_: PhantomData<fn() -> W>,
    _use_r_: PhantomData<fn() -> R>,
    rt_: RT,
    /// sink 句柄由**配置自己持有**（`metrics()` 只是借出它）。
    sink_: DebugMetrics,
}

impl<W, R, RT: Clone> Clone for MetricsCfg<W, R, RT> {
    fn clone(&self) -> Self {
        MetricsCfg {
            _use_w_: PhantomData,
            _use_r_: PhantomData,
            rt_: self.rt_.clone(),
            sink_: self.sink_.clone(),
        }
    }
}

impl<W, R, RT> MetricsCfg<W, R, RT> {
    fn new_(rt: RT, sink: DebugMetrics) -> Self {
        MetricsCfg {
            _use_w_: PhantomData,
            _use_r_: PhantomData,
            rt_: rt,
            sink_: sink,
        }
    }
}

impl<W, R, RT> TrMuxConfig for MetricsCfg<W, R, RT>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    type Data = u8;
    type Dock = Dock;
    type Buff = SmokeBuff;
}

impl<W, R, RT> TrConnCfg for MetricsCfg<W, R, RT>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
    RT: TrSmokeRt,
{
    type Rt = RT;
    type Alloc = CoreAlloc;
    type Policy = DefaultPolicy;
    type ConnTx = W;
    type ConnRx = R;
    type StageBuff = SmokeBuff;

    /// **只多这一处**：把关联类型指向内置采集器的句柄。
    type Metrics = DebugMetrics;

    fn runtime(&self) -> Self::Rt {
        self.rt_.clone()
    }

    fn allocator(&self) -> Self::Alloc {
        CoreAlloc
    }

    fn policy(&self) -> &Self::Policy {
        static POLICY: DefaultPolicy = DefaultPolicy;
        &POLICY
    }

    /// **只多这一处**：交出 sink。它是零大小的 unit struct，直接借常量即可。
    ///
    /// 注意返回类型是 `&Self::Metrics`（不是 `Option`）：「有没有 sink」是编译期事实，
    /// 运行期不需要判空——这正是它不给热路径添分支的原因。
    fn metrics(&self) -> &Self::Metrics {
        &self.sink_
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

impl<W, R, RT> TestConnCfg for MetricsCfg<W, R, RT>
where
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
    RT: TrSmokeRt,
{
    fn new_(rt: Self::Rt) -> Self {
        Self::new_(rt, shared_sink_())
    }
}

/// 测试目标：**一条子流的完整生命周期**之后，各项计数都真的被触发。
///
/// - 手段：用 `make_passive_ring_` 建两条内存环并跑当前后端；经 [`connect_pair_`]
///   用 [`MetricsCfg`] 建两条连接（sink 是零大小的 [`DebugMetrics`]，计数在进程级）；
///   在 A 侧 bind 一个临时 dock 并发起子流、B 侧 bind 监听 dock 并接受，随后
///   A 写 4 字节 / B 读 4 字节、B 写 4 字节 / A 读 4 字节，再丢弃发送半边等 EOF；
///   场景结束后丢弃两条连接（触发核心 `Drop` 里的「连接关闭」上报），最后取快照。
/// - 判断：连接数 2、子流数 2（两侧各一条）、四类帧计数与字节数非零、
///   `frame_errors` 为零、连接级原始字节非零、`conns_closed` 为 2；且发送侧
///   帧字节数不小于两侧各 4 字节的载荷（帧总长含帧头，故用下界断言）。
async fn metrics_reports_lifecycle_() {
    let (a_tx, b_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = make_passive_ring_(K_NET_BUFFER_SIZE);

    // 本 target 只有一个用例，但仍在跑之前清一次，断言只看本次增量。
    shared_sink_().reset();

    let rt = common::default_rt_();
    let scope = ScopeHost::local_scope(&rt);

    let scenario = async {
        let (conn_a, conn_b) = connect_pair_::<
            MetricsCfg<_, _, _>,
            MetricsCfg<_, _, _>,
            _,
            _,
            _,
            _,
            _,
        >(&rt, &scope, a_tx, a_rx, b_tx, b_rx)
        .await;

        let a_side = async {
            let mut binding = conn_a
                .bind_async(Dock::new(0x1001u32))
                .await
                .expect("A 侧绑定发起 dock 应当成功");
            let mut message: &[u8] = &[];
            let mut handle = binding
                .open_channel_async(Dock::new(1u32), &mut message)
                .await
                .expect("A 侧发起子流应当成功");
            let mut welcome_buf: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome_buf[..];
            let (mut tx, mut rx) = handle
                .accept_async_closure(&mut welcome, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
                .expect("A 侧最终裁决应当成功");

            write_channel_all_(&mut tx, b"ping")
                .await
                .expect("A 侧写入应当成功");
            let mut got = [0u8; 4];
            read_channel_exact_(&mut rx, &mut got)
                .await
                .expect("A 侧读取应当成功");
            assert_eq!(&got, b"pong", "A 侧应当读到 B 侧的回包");

            // 半关闭：丢掉发送半边（发 FIN），等对端收尾后读到 EOF。
            drop(tx);
            expect_eof_(&mut rx).await;
        };

        let b_side = async {
            let mut binding = conn_b
                .bind_async(Dock::new(1u32))
                .await
                .expect("B 侧绑定监听 dock 应当成功");
            let mut listener = binding
                .listen_async()
                .await
                .expect("B 侧建立 listener 应当成功");
            let mut handle = listener
                .income_async()
                .await
                .expect("B 侧应当取到入向建流请求");
            assert_eq!(
                handle.local_dock(),
                Dock::new(1u32),
                "被动方的 local_dock 应当是监听 dock（镜像语义）"
            );
            let mut welcome_buf: [u8; 0] = [];
            let mut welcome: &mut [u8] = &mut welcome_buf[..];
            let (mut tx, mut rx) = handle
                .accept_async_closure(&mut welcome, || {
                    (make_channel_buff_(), make_channel_buff_())
                })
                .await
                .expect("B 侧最终裁决应当成功");

            let mut got = [0u8; 4];
            read_channel_exact_(&mut rx, &mut got)
                .await
                .expect("B 侧读取应当成功");
            assert_eq!(&got, b"ping", "B 侧应当读到 A 侧的请求");
            write_channel_all_(&mut tx, b"pong")
                .await
                .expect("B 侧写入应当成功");

            drop(tx);
            expect_eof_(&mut rx).await;
        };

        futures::join!(a_side, b_side);

        // 两条连接在这里丢弃 ⇒ 核心 `Drop` 报出「连接关闭」。
        drop(conn_a);
        drop(conn_b);
    };

    scope.run_until(scenario).await;

    let snap = shared_sink_().snapshot();
    assert_eq!(snap.conns_opened, 2u64, "两条连接都应当报过「建立成功」");
    assert_eq!(snap.conns_closed, 2u64, "两条连接都应当报过「关闭」");
    assert_eq!(
        snap.channels_opened, 2u64,
        "两侧各建成一条子流：期望 2 次 on_channel_opened"
    );
    assert!(
        snap.frames_sent > 0u64 && snap.frames_recv > 0u64,
        "双向都应当有帧上报，实际 发送 {} / 接收 {}",
        snap.frames_sent,
        snap.frames_recv
    );
    assert!(
        snap.frame_bytes_sent >= 8u64 && snap.frame_bytes_recv >= 8u64,
        "两侧各 4 字节载荷 ⇒ 帧总长之和不应小于 8，实际 发送 {} / 接收 {}",
        snap.frame_bytes_sent,
        snap.frame_bytes_recv
    );
    // **口径断言**：同一批帧在两个方向的线上总长必须逐字节相等。它把「读侧帧头长度」
    // 钉死——读侧若只填载荷长度（曾经如此），这里必然不相等。
    assert_eq!(
        snap.frame_bytes_sent, snap.frame_bytes_recv,
        "同批帧的发送总长与接收总长必须相等：发送 {} / 接收 {}",
        snap.frame_bytes_sent, snap.frame_bytes_recv
    );
    // 分桶也要对齐：两侧的帧种类与条数完全一致。
    assert_eq!(
        snap.frames_sent, snap.frames_recv,
        "同批帧的收发条数必须相等：发送 {} / 接收 {}",
        snap.frames_sent, snap.frames_recv
    );
    assert_eq!(
        snap.frame_errors, 0u64,
        "本场景不应产生任何帧 / 协议错误"
    );
    assert!(
        snap.transport_bytes_sent > 0u64 && snap.transport_bytes_recv > 0u64,
        "连接级原始字节两个方向都应当非零"
    );
}
single_runtime_test_!(metrics_reports_lifecycle_);
