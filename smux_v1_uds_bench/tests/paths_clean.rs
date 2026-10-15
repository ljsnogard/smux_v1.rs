//! 端到端正确性验收：**两条路径**都要跑通，且载荷逐字节对得上。
//!
//! 本文件是**集成测试**（`tests/`），每个后端各编译一份（`required-features` 决定
//! 哪一份进本次构建）。它只验正确性：性能数字由 `bench_*` 可执行文件与 `results/`
//! 下的报告负责。
//!
//! # 为什么「正确性」也要单独测
//!
//! 性能数字很容易在「协议悄悄退化」时依然好看：少收几个字节、CRC 不核、EOF 判错，
//! 都可能让带宽读数变高。因此这里用**小载荷**把两条路径各跑一格，并断言：
//!
//! 1. 每一格都**没有失败**（`failure` 为空）；
//! 2. 每一格都真的收到了声明的字节数（用例内部已核对长度与 CRC-32）；
//! 3. 每格的墙钟与并发因子都是有限正数（用例真的跑起来了，而不是空转）。
//!
//! 三件事都成立时，「发出去的东西被完整收到」这条判据才成立。

use smux_v1_uds_bench::{CaseEnv, bench_run, cli_};

#[cfg(feature = "rt-tokio")]
use smux_v1_uds_bench::rt_tokio::TokioEnv;
#[cfg(feature = "rt-compio")]
use smux_v1_uds_bench::rt_compio::CompioEnv;
#[cfg(feature = "rt-smol")]
use smux_v1_uds_bench::rt_smol::SmolEnv;

/// 造一份小载荷的用例规格（两条路径各一格）。
fn spec_(mode: cli_::Mode, clients: usize) -> cli_::CaseSpec {
    cli_::CaseSpec {
        mode,
        clients,
        // 载荷刻意大于一个环容量（64 KiB）的若干倍：这样「跨分块拼帧」与「环满后的
        // 背压」两条路径都会被走到，而不是只搬一个块就结束。
        bytes_per_client: 320u64 * 1024u64,
        chunk: 64usize * 1024usize,
        ring_cap: 64usize * 1024usize,
        seed: 0x5EED_u64,
        verify: true,
    }
}

/// 断言一格结果干净且数值合理。
fn assert_clean_(label: &str, result: &smux_v1_uds_bench::report::CaseResult) {
    assert!(
        result.failure.is_none(),
        "{label}：用例失败 —— {}",
        result.failure.as_deref().unwrap_or("")
    );
    assert!(result.wall_ns > 0u64, "{label}：墙钟为 0");
    assert!(
        result.task_ns.len() == result.clients,
        "{label}：任务数 {} 与并发度 {} 不符",
        result.task_ns.len(),
        result.clients
    );
    assert!(
        result.bandwidth_mib_s.is_finite() && result.bandwidth_mib_s > 0.0f64,
        "{label}：带宽非正 —— {}",
        result.bandwidth_mib_s
    );
    assert!(
        result.concurrency_factor.is_finite() && result.concurrency_factor > 0.0f64,
        "{label}：并发因子非正 —— {}",
        result.concurrency_factor
    );
    // 每条流都发满了声明的字节数（用例内部核对长度与 CRC），因此总字节必须对得上。
    assert_eq!(
        result.total_bytes,
        result.bytes_per_client * result.clients as u64,
        "{label}：总字节不符"
    );
}

/// 跑「raw 与 smux 各一格」并逐格断言。
///
/// - 手段：用 `bench_run::run_case_task` 直接跑一条用例（`--runners 1` 的语义），
///   载荷 320 KiB × 4 条流，两条路径各一次。
/// - 判断：两格都 `failure` 为空、墙钟为正、任务数与并发度一致、带宽与并发因子为正，
///   且总字节等于「每客户端字节数 × 并发度」。
///
/// Runs one raw and one smux case and asserts both are clean.
/// - Method: drive each mode through `bench_run::run_case_task` with 4 streams of
///   320 KiB.
/// - Assertion: both cases report no failure, a positive wall clock, one task time per
///   client, positive bandwidth and concurrency factor, and the exact byte total.
fn assert_both_paths_clean_<E, D>(label: &str)
where
    E: CaseEnv + 'static,
    D: smux_v1_uds_bench::Drive + 'static,
{
    for (idx, mode) in [cli_::Mode::Raw, cli_::Mode::Smux].into_iter().enumerate() {
        let spec = spec_(mode, 4usize);
        let result = D::drive(bench_run::run_case_task::<E, D>(
            spec,
            1usize,
            idx,
            60u64,
        ))
        .expect("驱动用例");
        assert_clean_(&format!("{label}/{}", mode.name()), &result);
    }
}

/// 目标：tokio 后端下 raw 与 smux 两条路径都能把小载荷完整送达。
/// 手段：`assert_both_paths_clean_::<TokioEnv, DriveHere>`（载荷 320 KiB × 4 条流）。
/// 判断：两格都无失败、字节数与并发度一致、带宽与并发因子为正。
///
/// Target: both paths deliver the payload intact on the tokio backend.
/// Method: `assert_both_paths_clean_::<TokioEnv, DriveHere>` (4 × 320 KiB).
/// Assertion: both cases are clean with consistent byte totals and positive metrics.
#[cfg(feature = "rt-tokio")]
#[test]
fn tokio_both_paths_are_clean_() {
    assert_both_paths_clean_::<TokioEnv, smux_v1_uds_bench::DriveHere>("tokio");
}

/// 目标：compio 后端下 raw 与 smux 两条路径都能把小载荷完整送达。
/// 手段：与 tokio 那条逐字同形，只换壳。
/// 判断：同 tokio 那条。
///
/// Target: both paths deliver the payload intact on the compio backend.
/// Method: same as the tokio test with the compio shell.
/// Assertion: same as the tokio test.
#[cfg(feature = "rt-compio")]
#[test]
fn compio_both_paths_are_clean_() {
    assert_both_paths_clean_::<CompioEnv, smux_v1_uds_bench::DriveHere>("compio");
}

/// 目标：smol 后端下 raw 与 smux 两条路径都能把小载荷完整送达。
/// 手段：与 tokio 那条逐字同形，只换壳。
/// 判断：同 tokio 那条。
///
/// Target: both paths deliver the payload intact on the smol backend.
/// Method: same as the tokio test with the smol shell.
/// Assertion: same as the tokio test.
#[cfg(feature = "rt-smol")]
#[test]
fn smol_both_paths_are_clean_() {
    assert_both_paths_clean_::<SmolEnv, smux_v1_uds_bench::DriveHere>("smol");
}
