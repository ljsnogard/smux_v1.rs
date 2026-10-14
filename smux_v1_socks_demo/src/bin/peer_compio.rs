//! `peer_compio`：**compio** 装配的 peer 进程。
//!
//! ```bash
//! cargo run --no-default-features --features rt-compio --bin peer_compio -- \
//!     --runtime compio --role passive --listen 127.0.0.1:7002 --peer 127.0.0.1:7001
//! ```

/// 入口：运行时由 `#[compio::main]` 提供（执行器在运行时实例里，自己 tick 本地队列）。
#[compio::main]
async fn main() -> std::process::ExitCode {
    smux_v1_sock_demo::rt_compio::run_async().await
}
