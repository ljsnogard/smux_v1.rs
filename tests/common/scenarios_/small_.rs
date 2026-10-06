//! 小规模场景：2 个 dock × 各 2 条子流。需要真实 socket 但不需规模的验收走它，
//! 跑得快、便于定位。

use buffex::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};

use crate::common::{
    K_SMALL_CHANNELS_PER_DOCK, K_SMALL_DOCK_COUNT, TrSmokeRt, TrSmokeScope,
};

use super::kit_::run_mux_scenario_;

/// 小规模验收场景：**2 个 dock × 各 2 条 channel**，双向并发收发 + 半关闭。
///
/// 即 [`run_mux_scenario_`] 在 `K_SMALL_DOCK_COUNT × K_SMALL_CHANNELS_PER_DOCK`
/// 下的实例；与 1024 条的 [`run_smoke_scenario_`] 走的是**同一份**驱动
/// （[`drive_side_`]，每条并发子流一个互不相同的临时 `local_dock`），只是规模不同。
/// 它同时是进程内直连（`tests/inmem_mux.rs`）与 socket 版
/// （[`run_small_socket_scenario_`]）快速回归的挂点。
///
/// 参数是**运行时值** `rt` 与**本地作用域** `scope`：前者进连接的类型参数并提供
/// 计时与时刻，后者交给 [`MuxConnection::new`] 投递读写循环。
///
/// # Panics
///
/// 任何一次 open / accept / 读写 / 半关闭校验失败都会 panic——失败即测试失败。
pub async fn run_small_mux_scenario_<RA, WA, RB, WB, S, RT>(
    rt: &RT,
    scope: &S,
    tx_a: WA,
    rx_a: RA,
    tx_b: WB,
    rx_b: RB,
) where
    RA: TrBuffRead<u8> + 'static,
    WA: TrBuffWrite<u8> + 'static,
    RB: TrBuffRead<u8> + 'static,
    WB: TrBuffWrite<u8> + 'static,
    S: TrSmokeScope + Clone + 'static,
    RT: TrSmokeRt,
{
    run_mux_scenario_::<_, _, _, _, S, RT>(
        rt,
        scope,
        tx_a,
        rx_a,
        tx_b,
        rx_b,
        K_SMALL_DOCK_COUNT,
        K_SMALL_CHANNELS_PER_DOCK,
    )
    .await
}
