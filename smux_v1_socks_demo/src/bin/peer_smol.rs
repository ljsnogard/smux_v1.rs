//! `peer_smol`：**smol** 装配的 peer 进程。
//!
//! ```bash
//! cargo run --no-default-features --features rt-smol --bin peer_smol -- \
//!     --runtime smol --role active --listen 127.0.0.1:0 --peer 127.0.0.1:7002
//! ```
//!
//! smol 没有隐式运行时上下文，因此这里是普通的同步 `main`：驱动在
//! `smol::block_on(scope.run_until(..))` 里（见 `rt_smol` 模块文档）。

fn main() -> std::process::ExitCode {
    smux_v1_sock_demo::rt_smol::run_blocking()
}
