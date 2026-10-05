//! 冒烟主场景：`K_DOCK_COUNT` 个 dock × 各 `K_CHANNELS_PER_DOCK` 条子流双向并发收发，
//! 逐条校验载荷，最后半关闭并等 EOF。

use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};

use crate::common::{K_CHANNELS_PER_DOCK, K_DOCK_COUNT, TrSmokeScope};

use super::kit_::run_mux_scenario_;

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
    tx_a: WA,
    rx_a: RA,
    tx_b: WB,
    rx_b: RB,
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
        tx_a,
        rx_a,
        tx_b,
        rx_b,
        K_DOCK_COUNT,
        K_CHANNELS_PER_DOCK,
    )
    .await
}
