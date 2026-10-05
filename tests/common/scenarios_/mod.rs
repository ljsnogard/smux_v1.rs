//! 场景主体：与运行时、与传输都无关。
//!
//! 每个文件一个主题，只依赖 `common` 的辅助设施；`mod.rs` 只声明子模块并**逐项**
//! 再导出。场景**不 spawn**：并发一律用 `futures` 的组合子，因此两个运行时共用同一
//! 份代码（compio 的 socket 半边 `!Send`）。

mod bind_;
mod buffers_;
mod connect_;
mod flow_ctrl_;
mod idle_write_;
mod kit_;
mod small_;
mod smoke_;

pub use bind_::{run_bind_exclusivity_scenario_, run_unsettled_handle_scenario_};
pub use buffers_::{run_per_channel_alloc_scenario_, run_ring_rejected_scenario_};
pub use connect_::connect_pair_;
pub use flow_ctrl_::{
    run_flow_ctrl_isolation_scenario_, run_flow_ctrl_socket_scenario_,
    run_recv_dropped_scenario_,
};
pub use idle_write_::run_idle_small_write_scenario_;
pub use small_::run_small_mux_scenario_;
pub use smoke_::run_smoke_scenario_;
