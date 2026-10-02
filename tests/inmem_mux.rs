//! 进程内（无 socket）的复用连接验收测试。
//!
//! # 为什么另起一个测试目标
//!
//! `tests/smoke_*.rs` 把连接挂在「真实 UNIX socket + 调用方驱动的泵 + 全被动环」
//! 的传输上；该装配在**并发**场景下会出现「一侧的 socket 读不再被唤醒」的问题
//! （见 dev-notes `connection-20261002-0548.md` §7 的记录），与本轮要验收的连接层
//! 行为无关。本文件改用**进程内直连**：两条全被动环分别承载 A→B 与 B→A，四个环
//! 半部直接交给两个 `MuxConnection` 当作 `Rx` / `Tx`，中间没有任何泵。
//!
//! 这样既保留了「两个真实端点、双向并发、真实运行时任务」的全部连接层行为，
//! 又把传输层的未知问题隔离在外。
//!
//! # 与 `multi-thread` feature 的关系
//!
//! 多线程配置下的连接驱动**本轮尚未实现**（`MuxConnection::new` 仍是 `todo!()`，
//! 理由见 `src/connection/channel_.rs` 中该 `impl` 的文档与 dev-notes §7），因此
//! 本文件的用例在开启该 feature 时**跳过**（`#[cfg]` 掉，而不是失败）。

mod common;

/// tokio 侧连接层使用的运行时标记类型（同时声明两种 spawn 能力，两种 feature 配置
/// 都能编译）。
#[cfg(not(feature = "multi-thread"))]
type InMemRt =
    abs_art_tokio::Runtime<{ abs_art_tokio::SPAWN_SEND | abs_art_tokio::SPAWN_LOCAL }>;

/// compio 侧连接层使用的运行时标记类型。
#[cfg(not(feature = "multi-thread"))]
type InMemRtCompio =
    abs_art_compio::Runtime<{ abs_art_compio::SPAWN_SEND | abs_art_compio::SPAWN_LOCAL }>;

/// 测试目标（**本轮验收点**）：两个端点之间 2 个 dock × 各 2 条 channel 并发通信。
///
/// - 手段：用 `make_passive_ring_` 建两条容量 64 KiB 的内存环（一条 A→B、一条
///   B→A），把四个半部直接交给 [`common::run_small_mux_scenario_`]；后者完成握手、
///   建立两个 `MuxConnection`（内部各自 spawn 读 / 写循环），并在 `1..=2` 两个 dock
///   上并发跑 4 条 open + 4 条 accept（每条子流使用互不相同的临时 local_dock），
///   双向收发后丢弃发送半边、在对端等 EOF。
/// - 判断：所有 open / accept 成功；每条子流的载荷逐字节相等；半关闭后读到
///   `Closing`（EOF）。任一不满足即 panic。
///
/// 缺省（单线程）配置下连接内部用 `Rt::spawn_local`，因此必须跑在 `LocalSet` 里。
#[cfg(not(feature = "multi-thread"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mux_small_inmem_tokio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(common::run_small_mux_scenario_::<_, _, _, _, InMemRt>(
            a_rx, a_tx, b_rx, b_tx,
        ))
        .await;
}

/// 测试目标：与 tokio 版逐字相同的验收场景，改用 **compio 运行时**。
///
/// - 手段：同样用两条内存环直连两个端点，交给
///   [`common::run_small_mux_scenario_`]；区别只是 `Rt` 换成 `abs_art_compio::Runtime`
///   并跑在 `#[compio::test]` 里（compio 的运行时本身是线程本地的，无需 `LocalSet`）。
/// - 判断：与 tokio 版相同——载荷逐字节相等、半关闭后读到 EOF。
#[cfg(not(feature = "multi-thread"))]
#[compio::test]
async fn mux_small_inmem_compio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    common::run_small_mux_scenario_::<_, _, _, _, InMemRtCompio>(a_rx, a_tx, b_rx, b_tx).await;
}

/// 测试目标：tokio 下 `bind_async` 对同一个 `local_dock` 是**独占**的——首次绑定
/// 成功，第二次绑定报 `MuxError::DockInUse`，丢弃 binding 后可重绑。
///
/// - 手段：两条内存环直连两个端点并完成握手，交给
///   [`common::run_bind_exclusivity_scenario_`]；后者在 A 侧对同一个 dock 连续
///   `bind_async`、在另一个 dock 上正常绑定、`drop` 首个 binding 后再绑定，并在
///   B 侧用同一个 dock 值绑定以证明绑定是每条连接独立的状态。缺省配置用
///   `spawn_local`，因此跑在 `LocalSet` 里。
/// - 判断：第二次绑定必须是 `MuxError::DockInUse`（不是再次成功）；不同 dock、
///   解绑后的重绑、以及对端同名 dock 的绑定都必须成功。任一不满足即 panic。
#[cfg(not(feature = "multi-thread"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mux_bind_is_exclusive_tokio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    let local = tokio::task::LocalSet::new();
    local
        .run_until(common::run_bind_exclusivity_scenario_::<_, _, _, _, InMemRt>(
            a_rx, a_tx, b_rx, b_tx,
        ))
        .await;
}

/// 测试目标：与 tokio 版逐字相同的绑定独占性验收，改用 **compio 运行时**。
///
/// - 手段：同样两条内存环直连，交给
///   [`common::run_bind_exclusivity_scenario_`]；`Rt` 换成
///   `abs_art_compio::Runtime`，无需 `LocalSet`。
/// - 判断：与 tokio 版相同——同一 dock 第二次绑定报 `DockInUse`；不同 dock、解绑后
///   重绑、对端同名 dock 绑定都成功。
#[cfg(not(feature = "multi-thread"))]
#[compio::test]
async fn mux_bind_is_exclusive_compio_() {
    let (a_tx, b_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);
    let (b_tx, a_rx) = common::make_passive_ring_(common::K_NET_BUFFER_SIZE);

    common::run_bind_exclusivity_scenario_::<_, _, _, _, InMemRtCompio>(a_rx, a_tx, b_rx, b_tx)
        .await;
}
