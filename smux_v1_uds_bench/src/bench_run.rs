//! 驱动整个用例矩阵的**共享逻辑**：三个运行时壳只提供「怎么进上下文」「socket 与适配器
//! 是什么类型」「怎么在当前线程驱动一个 future」，进来之后一律调这里。
//!
//! # 两种环境：单跑器与多跑器
//!
//! 本 bench 刻意把「用例」与「跑器线程」分开：
//!
//! ```text
//! --runners 1        全部用例依次在**同一个线程、同一个运行时**上跑完
//! --runners N > 1    N 个工作线程，各自持有自己的运行时与本地作用域，
//!                    从共享队列里取用例并列跑
//! ```
//!
//! 为什么要两种：它们回答的是不同问题。单线程给出「这套协议在最小上下文里的上限与
//! 方差」，是干净的可复现基线；多线程给出「多条连接、多个运行时在同一台机器上并存时
//! 还成不成立」——真实部署很少只开一条连接、一个运行时。
//!
//! **用例内部**在两种环境下都跑在**同一线程的同一个作用域**里：`smux_v1` 的连接内部
//! 循环是 `spawn_local` 投出去的 `!Send` 任务，一条连接只能属于一个线程。并列来自
//! 「多个用例分布到多个线程」，不是「把一个用例拆到多个线程」——后者在这套 API 下
//! 不成立，也不该成立。
//!
//! # 单格超时
//!
//! 超时用 [`futures_timer::Delay`]：它自带一个独立定时线程，**不依赖任何运行时**，
//! 因此三个后端共用一份实现。超时把「环 / 子流关不干净」这类**永久挂住**变成
//! **本格失败**，矩阵照常跑完，报告里留下位置与原因。

use std::time::{Duration, Instant};

use crate::{
    CaseEnv, Drive, Scope, cli_::CaseSpec, hub::TaskTime, report::CaseResult,
};

/// 一格用例的执行闭包：入参是「规格 + 轮次 + 用例编号」，返回可直接落盘的结果。
type CaseFn<'a> = &'a mut dyn FnMut(
    CaseSpec,
    usize,
    usize,
) -> Result<CaseResult, Box<dyn std::error::Error + Send + Sync>>;

/// 一格用例的**任务体**：跑完两条路径之一，返回可直接落盘的结果。
///
/// # 失败也要产出结果
///
/// 结构性错误（socket 建不出来、握手失败、连接建不起来）以及超时都在这里被折成
/// [`CaseResult::failure`]，而不是向上冒泡——扫矩阵时一格失败不该毁掉其余格子。
pub async fn run_case_task<E, D>(
    spec: CaseSpec,
    repeat_idx: usize,
    case_no: usize,
    timeout_secs: u64,
) -> CaseResult
where
    E: CaseEnv + 'static,
    D: Drive + 'static,
{
    let runtime_name: &'static str = crate::cli_::K_COMPILED_RUNTIME.name();
    // socket 路径在这里现造：用例体可能跨线程执行，不能借调用点的字符串。
    let addr = socket_path(case_no, repeat_idx);
    // 目录按需创建：直接调本函数（例如集成测试）时不经过 `run_matrix` 的建目录步骤。
    if let Some(parent) = addr.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::remove_file(&addr);
    let addr_text: &'static str =
        Box::leak(addr.to_string_lossy().to_string().into_boxed_str());
    // 整格用例活在一个显式作用域里：连接循环与写侧泵都要靠它被持续驱动
    // （见 `WithScope::with_scope` 的文档）。
    let spec2 = spec;
    let outcome = D::with_scope(move |_rt, scope| {
        // `env` 由**本线程**现场取：多跑器模式下每格可能在别的线程上跑，那时
        // 「当前运行时上下文」与这里的 `env` 就不是一回事了。这也是 `env` 入参
        // 只用来占位（供壳侧统一签名）的原因。
        Box::pin(async move {
            let env = E::current_thread();
            run_case_body(&env, scope, &spec2, addr_text, timeout_secs).await
        })
    })
    .await;
    // 用例结束（无论成败）都把 socket 文件清掉，别留给下一格。
    let _ = std::fs::remove_file(&addr);
    let outcome = outcome
        .map_err(|err| format!("用例执行失败：{err}"))
        .unwrap_or_else(Err);
    assemble_(runtime_name, spec, repeat_idx, outcome)
}

/// 把「跑一格」的结果折成 [`CaseResult`]。
fn assemble_(
    runtime_name: &'static str,
    spec: CaseSpec,
    repeat_idx: usize,
    outcome: Result<(u64, u64, Vec<TaskTime>), String>,
) -> CaseResult {
    match outcome {
        Ok((wall_ns, drained_ns, task_times)) => CaseResult::assemble(
            runtime_name,
            &spec,
            repeat_idx,
            wall_ns,
            Some(drained_ns),
            task_times,
        ),
        Err(note) => {
            let mut result = CaseResult::assemble(
                runtime_name,
                &spec,
                repeat_idx,
                0u64,
                None,
                vec![TaskTime::default(); spec.clients],
            );
            result.failure = Some(note);
            result
        }
    }
}

/// 把 `String` 说明折成 boxed error（本模块里大量使用）。
fn err_(note: String) -> Box<dyn std::error::Error + Send + Sync> {
    note.into()
}

/// 某一格用例的 UDS 路径（本进程私有目录，含用例编号与轮次）。
fn socket_path(case_no: usize, repeat_idx: usize) -> std::path::PathBuf {
    std::env::temp_dir()
        .join(format!("smux_v1_uds_bench_{}", std::process::id()))
        .join(format!("case{case_no}_r{repeat_idx}.sock"))
}

/// 带超时地跑一条路径；超时被转成 `Err`（本格失败）。
async fn run_case_body<E>(
    env: &E,
    scope: &Scope,
    spec: &CaseSpec,
    addr: &str,
    timeout_secs: u64,
) -> Result<(u64, u64, Vec<TaskTime>), String>
where
    E: CaseEnv,
{
    let body = async {
        match spec.mode {
            crate::cli_::Mode::Raw => crate::raw_case::run_raw_case::<E>(spec, addr).await,
            crate::cli_::Mode::Smux => {
                crate::smux_case::run_smux_case::<E>(env, scope, spec, addr).await
            }
        }
    };
    let timer = futures_timer::Delay::new(Duration::from_secs(timeout_secs));
    futures::pin_mut!(body);
    futures::pin_mut!(timer);
    match futures::future::select(body, timer).await {
        futures::future::Either::Left((outcome, _timer)) => outcome,
        futures::future::Either::Right(((), _pending)) => Err(format!("超时（>{timeout_secs}s）")),
    }
}

/// **跑器**：全部用例的共享队列 + 供多个工作线程并列领取的游标。
#[derive(Debug)]
pub struct RunnerQueue {
    /// 待跑用例（按命令行展开的顺序）：`(用例编号, 规格, 轮次)`。
    cases: std::sync::Mutex<Vec<(usize, CaseSpec, usize)>>,
    /// 下一个待领用例的下标。
    next: std::sync::atomic::AtomicUsize,
}

impl RunnerQueue {
    /// 按计划展开用例并造出共享队列。
    pub fn new(plan: &crate::cli_::RunPlan) -> Self {
        let mut cases = Vec::new();
        for (case_no, spec) in plan.expand().into_iter().enumerate() {
            for repeat_idx in 1usize..=plan.repeat {
                cases.push((case_no, spec, repeat_idx));
            }
        }
        RunnerQueue {
            cases: std::sync::Mutex::new(cases),
            next: std::sync::atomic::AtomicUsize::new(0usize),
        }
    }

    /// 取下一个待跑用例；取完返回 `None`。
    pub fn take(&self) -> Option<(usize, CaseSpec, usize)> {
        let idx = self
            .next
            .fetch_add(1usize, std::sync::atomic::Ordering::SeqCst);
        let cases = self.cases.lock().ok()?;
        cases.get(idx).copied()
    }

    /// 用例（含轮次）总数。
    pub fn total(&self) -> usize {
        self.cases.lock().map(|cases| cases.len()).unwrap_or(0usize)
    }
}

/// 跑器线程的通用循环：不停领用例、跑完、落盘，直到队列取空。
///
/// `case_fn` 由各壳给出「在当前线程的运行时上下文里跑一格」的实现（同步入口，内部
/// 自己驱动运行时）。
///
/// # Errors
///
/// 落盘失败等编排级错误返回；**单格失败不向上冒泡**（会记进报告）。
pub fn run_worker(
    queue: &RunnerQueue,
    runner: &crate::report::Runner,
    case_fn: CaseFn<'_>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    while let Some((case_no, spec, repeat_idx)) = queue.take() {
        let started = Instant::now();
        let result = case_fn(spec, repeat_idx, case_no)?;
        if result.failure.is_some() {
            eprintln!(
                "！用例 {} N={} 第 {repeat_idx} 轮未通过（已跑 {:.1}s）：{}",
                spec.mode.name(),
                spec.clients,
                started.elapsed().as_secs_f64(),
                result.failure.as_deref().unwrap_or("")
            );
        }
        runner.record(result).map_err(|err| err.to_string())?;
    }
    Ok(())
}

/// 单跑器模式的循环：**在当前运行时里**用 [`Drive::run_one`] 驱动每一格。
///
/// # Errors
///
/// 落盘失败等编排级错误返回。
async fn run_worker_one<E, D>(
    plan: &crate::cli_::RunPlan,
    queue: &RunnerQueue,
    runner: &crate::report::Runner,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    E: CaseEnv + 'static,
    D: Drive + Copy,
{
    while let Some((case_no, spec, repeat_idx)) = queue.take() {
        let started = Instant::now();
        let result = D::run_one(run_case_task::<E, D>(
            spec,
            repeat_idx,
            case_no,
            plan.timeout_secs,
        ))
        .await;
        if result.failure.is_some() {
            eprintln!(
                "！用例 {} N={} 第 {repeat_idx} 轮未通过（已跑 {:.1}s）：{}",
                spec.mode.name(),
                spec.clients,
                started.elapsed().as_secs_f64(),
                result.failure.as_deref().unwrap_or("")
            );
        }
        runner.record(result).map_err(|err| err.to_string())?;
    }
    Ok(())
}

/// 跑完整轮矩阵（单跑器与多跑器两种环境共用这一段编排）。
///
/// # 两种环境
///
/// - `--runners 1`：**当前线程、当前运行时**，依次跑完全部用例；
/// - `--runners N`：起 N 个跑器线程，各自从共享队列领用例并列跑。每个线程都在自己的
///   `run_one` 里取自己的运行时上下文（`E::current_thread()` 读的是本线程的上下文）。
///
/// # Errors
///
/// 建输出目录、落盘失败等编排级错误返回；**单格失败不向上冒泡**（会记进报告）。
pub async fn run_matrix<E, D>(
    plan: &crate::cli_::RunPlan,
    runtime_name: &'static str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    E: CaseEnv + 'static,
    D: Drive + Copy,
{
    let specs = plan.expand();
    let runner = crate::report::Runner::new(runtime_name, &plan.out_dir)
        .map_err(|err| err.to_string())?;
    runner.print_matrix(&specs, plan);

    if plan.dry_run {
        for spec in specs.iter() {
            println!(
                "  {}：N={}，每客户端 {}，分块 {}",
                spec.mode.label(),
                spec.clients,
                crate::cli_::human_bytes(spec.bytes_per_client),
                crate::cli_::human_bytes(spec.chunk as u64)
            );
        }
        println!("（--dry-run：只打印矩阵，不执行、不落盘）");
        return Ok(());
    }

    let dir = std::env::temp_dir().join(format!("smux_v1_uds_bench_{}", std::process::id()));
    crate::report::ensure_dir(&dir).map_err(|err| err.to_string())?;

    let queue = RunnerQueue::new(plan);
    let threads = plan.runners.min(queue.total().max(1usize));
    if threads <= 1usize {
        println!("== 单线程环境：1 个跑器（当前线程、当前运行时）==");
        // 用例体留在**当前**运行时里，由 `D::run_one` 驱动（`D::drive` 建的运行时
        // 就是当前这一个，不能再套一层）。
        run_worker_one::<E, D>(plan, &queue, &runner).await?;
    } else {
        println!("== 多线程环境：{threads} 个跑器线程，各持自己的运行时与本地作用域 ==");
        // `std::thread::scope` 让跑器线程借用共享队列与落盘句柄（二者本身就是
        // `Sync` 的），因此这里不需要 `Arc`。
        let joined = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(threads);
            for _ in 0..threads {
                let queue = &queue;
                let runner = &runner;
                handles.push(scope.spawn(move || {
                    // 每格在该线程自己的运行时里跑：`D::drive` 建运行时，里面的
                    // `run_worker` 负责领用例（同步闭包 → 内部驱动一格）。
                    run_worker(queue, runner, &mut |spec, repeat_idx, case_no| {
                        D::drive(run_case_task::<E, D>(
                            spec,
                            repeat_idx,
                            case_no,
                            plan.timeout_secs,
                        ))
                        .map_err(|err| err_(err.to_string()))
                    })
                }));
            }
            let mut errors = Vec::new();
            for handle in handles {
                match handle.join() {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => errors.push(err.to_string()),
                    Err(_) => errors.push("跑器线程 panic".to_string()),
                }
            }
            errors
        });
        for note in joined {
            eprintln!("！跑器线程出错：{note}");
        }
    }

    runner.finish().map_err(|err| err.to_string())?;
    let failures = runner
        .records()
        .map_err(err_)?
        .iter()
        .filter(|res| res.failure.is_some())
        .count();
    if failures > 0usize {
        return Err(format!("有 {failures} 个失败格；详见报告").into());
    }
    Ok(())
}
