//! compio 后端的 UDS 性能测试入口。
//!
//! ```bash
//! cargo run --release --bin bench_compio          # 缺省 feature 就是 compio
//! ```

fn main() -> std::process::ExitCode {
    smux_v1_uds_bench::rt_compio::run_blocking()
}
