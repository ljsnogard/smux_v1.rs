//! `peer_tokio`：**tokio** 装配的 peer 进程。
//!
//! ```bash
//! cargo run --no-default-features --features rt-tokio --bin peer_tokio -- \
//!     --runtime tokio --role active --listen 127.0.0.1:0 --peer 127.0.0.1:7002
//! ```
//!
//! 参数与两段角色交替的语义见 `--help`。

/// 入口：运行时由 `#[tokio::main]` 提供（**默认 multi-thread**），业务在库里。
///
/// 这里刻意**不**降级成 `current_thread`：本地队列虽然是线程本地的 `LocalSet`，但
/// `#[tokio::main]` 展开成 `Runtime::block_on(main_future)`，主 future 始终在**当前线程**
/// 被 poll、不会被搬到别的 worker；而本 peer 不使用全局 `spawn`，worker 线程池只承担
/// 运行时自身的调度。因此「取作用域 → 建 socket → 驱动本地队列」全在同一线程，默认
/// multi-thread 即可（三组配对与 mix 模式的实测见 `dev-notes/default-multithread-*.md`）。
#[tokio::main]
async fn main() -> std::process::ExitCode {
    smux_v1_sock_demo::rt_tokio::run_async().await
}
