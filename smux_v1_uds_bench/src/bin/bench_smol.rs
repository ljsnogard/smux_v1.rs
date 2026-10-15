//! smol 后端的 UDS 性能测试入口。
//!
//! smol 没有隐式运行时上下文，因此入口是**同步**的：内部用
//! `smol::block_on(scope.run_until(..))` 驱动本地队列。
//!
//! ```bash
//! cargo run --release --no-default-features --features rt-smol --bin bench_smol
//! ```

fn main() -> std::process::ExitCode {
    smux_v1_uds_bench::rt_smol::run_blocking()
}
