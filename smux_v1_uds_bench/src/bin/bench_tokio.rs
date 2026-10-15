//! tokio 后端的 UDS 性能测试入口。
//!
//! 用法见 `bench_tokio --help`；换成另两个后端要改用 `bench_compio` / `bench_smol`，
//! 因为后端是**编译期**选定的（三个 `rt-*` feature 互斥）。
//!
//! ```bash
//! cargo run --release --no-default-features --features rt-tokio --bin bench_tokio
//! ```
//!
//! # 入口是同步的
//!
//! 每一格用例都在 [`smux_v1_uds_bench::rt_tokio::TokioEnv`] 的 `with_scope` 里
//! 自建一个 current_thread 运行时（理由见那里的注释），因此这里**不**使用
//! `#[tokio::main]`：外层再套一个运行时会撞上「不能在运行时里再起运行时」。

fn main() -> std::process::ExitCode {
    smux_v1_uds_bench::rt_tokio::run_blocking()
}
