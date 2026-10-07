//! smux v1 集成测试共用的场景与辅助设施。
//!
//! 本目录服务于 `tests/` 下的集成用例。`mod.rs` **只做两件事**：声明子模块、逐项再
//! 导出（不使用 `*`）；所有逻辑都在子文件里，按「一件事一个文件」划分：
//!
//! | 文件 | 职责 |
//! | --- | --- |
//! | `config_` | 连接配置与用例矩阵的容量常量 |
//! | `buff_` | 帧暂存 / 子流环的缓冲构造 |
//! | `pump_` | 传输侧的调用方驱动泵与全被动环 |
//! | `socket_` | socket 装配与两个运行时的驱动入口 |
//! | `closure_` | `TrPrepareChannelRing` 的测试侧适配 |
//! | `channel_io_` | 子流半边的整段读写与 EOF 等待 |
//! | `payload_` | 确定性载荷生成 |
//! | `scenarios_` | 与运行时、传输无关的场景主体 |
//!
//! # 跨文件约定（所有场景都必须遵守）
//!
//! 1. **不 spawn**：并发一律用 `futures` 的组合子（`join!` / `join_all` / `select`）。
//!    compio 的 socket 半边是 `!Send`，spawn 不可用；两个测试目标因此共用同一份场景。
//! 2. **调用方驱动泵**：socket 只经「设备适配 + 调用方 await 的泵 + 全被动环」接入，
//!    理由是三条实测阻塞点（见 `pump_`）。
//! 3. **dock 对即身份**：帧头没有 channel 标识字段，同一 `(local_dock, remote_dock)` 在
//!    同一时刻至多一条活动子流。因此每条并发子流分配一个**互不相同**的临时
//!    `local_dock`（类比 TCP 临时端口），被动侧断言 `handle.local_dock()` 就是自己的
//!    监听 dock（镜像语义）。
//! 4. **校验不依赖配对顺序**：载荷头 4 字节编码发送方 `(dock, index)`，接收方据此复算
//!    完整载荷再逐字节比对（见 `payload_`）。
//!
//! 历史：本模块曾按「同一 dock 内 FIFO 配对」在同一 dock 对上并发 64 条子流，那与约定 3
//! 冲突（第 2 条 `OPEN` 会被 `Duplicate` 拒绝）。裁决是「改测试、协议不动」，见
//! `dev-notes/connection-20261002-0548.md` §5 Q2。
#![allow(dead_code)] // 每个测试 target 各自只用到本模块的一部分。
#![allow(unused_imports)] // 同理：本文件再导出的是**全部**共用面，单个 target 只能用到其中一部分。

mod buff_;
mod channel_io_;
mod closure_;
mod config_;
mod payload_;
mod pump_;
mod socket_;
mod scenarios_;

pub use buff_::{
    SmokeBuff,
    SmokeStageBuff,
    make_channel_buff_,
    make_channel_buff_with_,
    make_stage_buffs_,
    make_stage_buffs_with_,
};
pub use channel_io_::{expect_eof_, read_channel_exact_, write_channel_all_};
pub use scenarios_::{TestConnCfg, connect_pair_};
pub use closure_::{AcceptAsyncClosureExt, ClosurePrepare};
pub use config_::{
    DefaultRt, assert_runtime_is_, default_rt_,
    FlowCtrlConfig,
    K_CHANNELS_PER_DOCK,
    K_CHANNEL_CAPACITY,
    K_DOCK_COUNT,
    K_FLOW_CTRL_PAYLOAD_LEN,
    K_FLOW_CTRL_READ_STEP,
    K_FLOW_CTRL_RING_CAPACITY,
    K_FLOW_CTRL_SMALL_LEN,
    K_FLOW_CTRL_WATCHDOG_,
    K_NET_BUFFER_SIZE,
    K_SMALL_CHANNELS_PER_DOCK,
    K_SMALL_DOCK_COUNT,
    K_TOTAL_CHANNELS,
    SmokeConn,
    SmokeMuxConfig,
    TrLocalScope,
    TrSmokeRt,
    TrSmokeScope,
    TrTime,
};
pub use payload_::{make_flow_payload_, make_payload_};
pub use pump_::make_passive_ring_;
pub use socket_::{
    run_small_socket_scenario_,
    run_socket_scenario_,
    run_socket_scenario_on_runtime_,
};
pub use scenarios_::{
    run_bind_exclusivity_scenario_,
    run_closed_ring_spin_scenario_,
    run_conn_failed_wakes_scenario_,
    run_flow_ctrl_isolation_scenario_,
    run_flow_ctrl_socket_scenario_,
    run_frame_cap_scenario_,
    run_idle_small_write_scenario_,
    run_per_channel_alloc_scenario_,
    run_recv_dropped_scenario_,
    run_ring_rejected_scenario_,
    run_small_mux_scenario_,
    run_smoke_scenario_,
    run_unsettled_handle_scenario_,
};
