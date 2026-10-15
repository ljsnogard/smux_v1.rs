//! 结果结构、统计口径落地与**报告输出**（控制台 / JSONL / CSV / Markdown）。
//!
//! # 一次用例里时间是怎么量的
//!
//! 时间轴的采集在 [`crate::hub`] 里，这里只负责把它折成统计量与报告：
//!
//! ```text
//! 墙钟   = 栅栏释放 → 服务端收完最后一条流
//! 任务时长 = 栅栏释放 → 该任务写完最后一字节
//! ```
//!
//! **平均带宽用墙钟**（保守：尾部数据还在环 / socket 缓冲里），**方差用任务时长**
//! （它才回答「N 个发送任务有没有被串起来」）。
//!
//! # 报告输出什么
//!
//! | 文件 | 内容 |
//! | --- | --- |
//! | `<out>/bench-<runtime>.jsonl` | 逐格 JSON 行（原始记录，便于二次处理） |
//! | `<out>/bench-<runtime>-summary.md` | 本次运行的汇总：逐格表、raw ↔ smux 对照、结论 |
//! | `<out>/bench-summary.csv` | 追加式的跨后端汇总（一个用例一行） |
//!
//! 三份都写是有意的：**JSONL 给机器**（方差、极差、逐轮明细一个不少）、
//! **CSV 给表格工具**、**Markdown 给人**（含结论与失败清单）。

use std::{
    collections::BTreeMap,
    fs::File,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::{
    cli_::{CaseSpec, Mode},
    hub::TaskTime,
    stats::{self, SampleStats},
};

/// 一格用例的完整结果（**统计口径只在这里落地一次**，三个运行时壳都调
/// [`CaseResult::assemble`]，因此三格的数字可比）。
#[derive(Debug, Clone)]
pub struct CaseResult {
    /// 运行时名字（`tokio` / `compio` / `smol`）。
    pub runtime: String,
    /// 被测量路径。
    pub mode: Mode,
    /// 并发客户端数。
    pub clients: usize,
    /// 本格的载荷总字节数（不含 12 字节头部）。
    pub total_bytes: u64,
    /// 每个客户端 / 每条子流的字节数。
    pub bytes_per_client: u64,
    /// 每次发送 / 接收的字节数。
    pub chunk: usize,
    /// 环容量。
    pub ring_cap: usize,
    /// 第几轮（从 1 开始）。
    pub repeat_idx: usize,
    /// 共同起点到「最后一个发送任务发完」的纳秒数（即**墙钟**，带宽取自它）。
    pub wall_ns: u64,
    /// 共同起点到「服务端收齐最后一条流」的纳秒数（**只作参考**，不进带宽）。
    ///
    /// 它与 `wall_ns` 的差就是「尾部数据从环 / socket 里排空」的时长；这个量受环容量
    /// 与收尾策略支配，把它算进带宽会让「换个大环」凭空改变带宽读数。
    pub drained_ns: Option<u64>,
    /// 每个发送任务的完成时间（纳秒，相对共同起点）。
    pub task_ns: Vec<u64>,
    /// 任务完成时间的统计量。
    pub task_stats: SampleStats,
    /// 平均带宽（MiB/s），口径是**服务端收齐**的墙钟。
    pub bandwidth_mib_s: f64,
    /// 每个任务的自身带宽（MiB/s），口径是任务自己的时长。
    pub per_task_mib_s: Vec<f64>,
    /// 真并发的证据量：`Σ 任务时长 / 墙钟`。
    pub concurrency_factor: f64,
    /// 本格是否核对了长度与 CRC-32。
    pub verified: bool,
    /// 失败说明（为空表示本格干净）。
    pub failure: Option<String>,
}

impl CaseResult {
    /// 由原始时间轴算出全部派生量。
    ///
    /// # 失败也要产出结构体
    ///
    /// 失败格照样返回一个带 `failure` 说明的结果——报告要把失败格一并落盘，
    /// 「哪一格没跑成、为什么」本身就是结论的一部分。
    pub fn assemble(
        runtime: &str,
        spec: &CaseSpec,
        repeat_idx: usize,
        wall_ns: u64,
        drained_ns: Option<u64>,
        task_times: Vec<TaskTime>,
    ) -> Self {
        let mut failure: Option<String> = None;
        let mut task_ns: Vec<u64> = Vec::with_capacity(task_times.len());
        for (idx, time) in task_times.iter().enumerate() {
            if time.failed {
                failure.get_or_insert_with(|| format!("发送任务 {idx} 失败"));
                continue;
            }
            task_ns.push(time.end_ns);
        }
        if task_ns.len() != spec.clients {
            failure.get_or_insert_with(|| {
                format!(
                    "只收齐 {} 个任务的时间（应为 {}）",
                    task_ns.len(),
                    spec.clients
                )
            });
        }
        if wall_ns == 0u64 {
            failure.get_or_insert_with(|| "墙钟为 0（用例没跑起来）".to_string());
        }

        let secs: Vec<f64> = task_ns
            .iter()
            .map(|&nanos| stats::nanos_to_secs(nanos as u128))
            .collect();
        let task_stats = SampleStats::of(&secs);
        let wall_secs = stats::nanos_to_secs(wall_ns as u128);
        let bandwidth_mib_s = stats::mib_per_sec(spec.total_bytes(), wall_secs);
        let per_task_mib_s = secs
            .iter()
            .map(|&secs| stats::mib_per_sec(spec.bytes_per_client, secs))
            .collect();
        let concurrency_factor = stats::concurrency_factor(task_stats.total(), wall_secs);

        CaseResult {
            runtime: runtime.to_string(),
            mode: spec.mode,
            clients: spec.clients,
            total_bytes: spec.total_bytes(),
            bytes_per_client: spec.bytes_per_client,
            chunk: spec.chunk,
            ring_cap: spec.ring_cap,
            repeat_idx,
            wall_ns,
            drained_ns,
            task_ns,
            task_stats,
            bandwidth_mib_s,
            per_task_mib_s,
            concurrency_factor,
            verified: spec.verify,
            failure,
        }
    }

    /// 本格是否干净（没错、且统计量算得出来）。
    pub fn is_clean(&self) -> bool {
        self.failure.is_none() && self.wall_ns > 0u64 && self.task_ns.len() == self.clients
    }

    /// 墙钟（秒）。
    pub fn wall_secs(&self) -> f64 {
        stats::nanos_to_secs(self.wall_ns as u128)
    }

    /// 一行的 JSON 表示（手写，不引 serde：字段固定、值都是标量）。
    ///
    /// # 逗号纪律
    ///
    /// 每个 `push_field_*` 的最后一个实参是「**要不要**在前面补一个逗号」：
    ///
    /// - **首字段**传 `false`（对象刚打开，还没有前一个字段）；
    /// - 其余字段一律传 `true`。
    ///
    /// 这条规则只有一个例外：`failure` 为 `None` 时手写 `"failure":null`，它自己带逗号。
    /// 测试 `json_line_starts_without_comma_` 与 `json_line_is_parseable_` 钉住了这两点
    /// （曾经出过首字段多一个逗号的回归：`{,"runtime":...}`）。
    pub fn to_json_line(&self) -> String {
        let mut out = String::with_capacity(640usize);
        out.push('{');
        // 首字段：不补逗号。
        push_field_str(&mut out, "runtime", &self.runtime, false);
        // 其余字段：一律补逗号。
        push_field_str(&mut out, "mode", self.mode.name(), true);
        push_field_num(&mut out, "clients", self.clients as f64, true);
        push_field_num(&mut out, "total_bytes", self.total_bytes as f64, true);
        push_field_num(
            &mut out,
            "bytes_per_client",
            self.bytes_per_client as f64,
            true,
        );
        push_field_num(&mut out, "chunk", self.chunk as f64, true);
        push_field_num(&mut out, "ring_cap", self.ring_cap as f64, true);
        push_field_num(&mut out, "repeat", self.repeat_idx as f64, true);
        push_field_num(&mut out, "wall_ns", self.wall_ns as f64, true);
        push_field_num(&mut out, "wall_secs", round6(self.wall_secs()), true);
        match self.drained_ns {
            Some(nanos) => push_field_num(
                &mut out,
                "drained_secs",
                round6(stats::nanos_to_secs(nanos as u128)),
                true,
            ),
            None => out.push_str(",\"drained_secs\":null"),
        }
        push_field_num(
            &mut out,
            "bandwidth_mib_s",
            round6(self.bandwidth_mib_s),
            true,
        );
        push_field_num(
            &mut out,
            "task_mean_ns",
            round6(self.task_stats.mean * 1e9f64),
            true,
        );
        push_field_num(
            &mut out,
            "task_var_ns2",
            round6(self.task_stats.variance_sample * 1e18f64),
            true,
        );
        push_field_num(
            &mut out,
            "task_stddev_ns",
            round6(self.task_stats.std_dev_sample * 1e9f64),
            true,
        );
        push_field_num(&mut out, "task_cv", round6(self.task_stats.cv), true);
        push_field_num(
            &mut out,
            "task_min_ns",
            round6(self.task_stats.min * 1e9f64),
            true,
        );
        push_field_num(
            &mut out,
            "task_max_ns",
            round6(self.task_stats.max * 1e9f64),
            true,
        );
        push_field_num(
            &mut out,
            "task_range_ns",
            round6(self.task_stats.range * 1e9f64),
            true,
        );
        push_field_num(
            &mut out,
            "concurrency_factor",
            round6(self.concurrency_factor),
            true,
        );
        push_field_bool(&mut out, "verified", self.verified, true);
        match &self.failure {
            Some(note) => push_field_str(&mut out, "failure", note, true),
            None => out.push_str(",\"failure\":null"),
        }
        out.push('}');
        out
    }

    /// 一行的 CSV 表示（列序与 [`CSV_HEADER`] 一致）。
    pub fn to_csv_line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{:.6},{},{:.6},{:.1},{:.1},{:.1},{:.6},{:.6},{:.1},{:.1},{:.1},{},{}",
            self.runtime,
            self.mode.name(),
            self.clients,
            self.total_bytes,
            self.bytes_per_client,
            self.chunk,
            self.ring_cap,
            self.wall_secs(),
            match self.drained_ns {
                Some(nanos) => format!("{:.6}", stats::nanos_to_secs(nanos as u128)),
                None => String::new(),
            },
            self.bandwidth_mib_s,
            self.task_stats.mean * 1e9f64,
            self.task_stats.variance_sample * 1e18f64,
            self.task_stats.std_dev_sample * 1e9f64,
            self.task_stats.cv,
            self.concurrency_factor,
            self.task_stats.min * 1e9f64,
            self.task_stats.max * 1e9f64,
            self.task_stats.range * 1e9f64,
            self.repeat_idx,
            self.failure.as_deref().unwrap_or(""),
        )
    }
}

/// CSV 的表头（列序必须与 [`CaseResult::to_csv_line`] 一致）。
pub const CSV_HEADER: &str = "runtime,mode,clients,total_bytes,bytes_per_client,chunk,ring_cap,\
wall_secs,drained_secs,bandwidth_mib_s,task_mean_ns,task_var_ns2,task_stddev_ns,task_cv,\
concurrency_factor,task_min_ns,task_max_ns,task_range_ns,repeat,failure";

/// 确保一个目录存在（不存在则创建）。
///
/// # Errors
///
/// 路径已存在且不是目录、或无权限创建时返回 IO / 说明错误。
pub fn ensure_dir(dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if dir.exists() && !dir.is_dir() {
        return Err(format!("{} 已存在且不是目录", dir.display()).into());
    }
    fs::create_dir_all(dir)?;
    Ok(())
}

/// 浮点取 6 位小数（只为 JSON / 报告的可读性，不改变统计本身）。
fn round6(value: f64) -> f64 {
    (value * 1_000_000.0f64).round() / 1_000_000.0f64
}

/// 追加一个字符串字段。
fn push_field_str(out: &mut String, key: &str, value: &str, comma: bool) {
    if comma {
        out.push(',');
    }
    out.push('"');
    out.push_str(key);
    out.push_str("\":\"");
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out.push('"');
}

/// 追加一个数值字段（非有限值写成 `null`）。
fn push_field_num(out: &mut String, key: &str, value: f64, comma: bool) {
    if comma {
        out.push(',');
    }
    out.push('"');
    out.push_str(key);
    out.push_str("\":");
    if value.is_finite() {
        out.push_str(&format!("{value}"));
    } else {
        out.push_str("null");
    }
}

/// 追加一个布尔字段。
fn push_field_bool(out: &mut String, key: &str, value: bool, comma: bool) {
    if comma {
        out.push(',');
    }
    out.push('"');
    out.push_str(key);
    out.push_str("\":");
    out.push_str(if value { "true" } else { "false" });
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 落盘与打印
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一次运行的落盘与打印（**可克隆的句柄**）。
///
/// # 为什么是 `Arc<Mutex<..>>`
///
/// 每格用例都被投成一个任务（见 `bench_run` 的模块文档），任务体必须是 `'static`，
/// 因此它不能借 `Runner`。这里的做法是把可变状态收进 `Arc<Mutex<..>>`：克隆一份句柄
/// 进任务，任务自己就能把结果落盘。
///
/// 本 bench 的任务都在同一个线程（`spawn_local`），`Mutex` 在这里只是为了让
/// 「`&mut self` 的落盘」在共享句柄下仍然成立，不涉及跨线程竞争。
#[derive(Debug, Clone)]
pub struct Runner {
    /// 运行时名字（不受后续写入影响，单独存一份便于无锁读取）。
    runtime: String,
    /// 输出目录。
    out_dir: PathBuf,
    /// 开始时刻（报告抬头用）。
    started: std::time::SystemTime,
    /// 可变状态：文件句柄与已采集的记录。
    shared_: Arc<Mutex<RunnerState_>>,
}

/// [`Runner`] 的可变状态（收在互斥量里）。
#[derive(Debug, Default)]
struct RunnerState_ {
    /// 逐格 JSONL 的追加句柄（惰性创建）。
    jsonl_: Option<File>,
    /// 跨后端 CSV 的追加句柄（惰性创建）。
    csv_: Option<File>,
    /// 已跑过的用例（供最后的 Markdown 汇总用）。
    records_: Vec<CaseResult>,
}

impl Runner {
    /// 准备输出目录与文件句柄。
    ///
    /// # Errors
    ///
    /// 目录创建失败、或目标路径已存在且不是目录时返回 IO / 说明错误。
    pub fn new(runtime: &str, out_dir: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let dir = PathBuf::from(out_dir);
        if dir.exists() && !dir.is_dir() {
            return Err(format!("输出路径 {out_dir} 已存在且不是目录").into());
        }
        fs::create_dir_all(&dir)?;
        Ok(Runner {
            runtime: runtime.to_string(),
            out_dir: dir,
            started: std::time::SystemTime::now(),
            shared_: Arc::new(Mutex::new(RunnerState_::default())),
        })
    }

    /// 运行时名字（`tokio` / `compio` / `smol`）。
    pub fn runtime_name(&self) -> String {
        self.runtime.clone()
    }

    /// 输出目录。
    pub fn out_dir(&self) -> &Path {
        &self.out_dir
    }

    /// 本次运行的逐格 JSONL 路径。
    pub fn jsonl_path(&self) -> PathBuf {
        self.out_dir.join(format!("bench-{}.jsonl", self.runtime))
    }

    /// 本次运行的 Markdown 汇总路径。
    pub fn summary_path(&self) -> PathBuf {
        self.out_dir
            .join(format!("bench-{}-summary.md", self.runtime))
    }

    /// 跨后端 CSV 路径。
    pub fn csv_path(&self) -> PathBuf {
        self.out_dir.join("bench-summary.csv")
    }

    /// 打印本次运行将跑的用例矩阵抬头。
    pub fn print_matrix(&self, specs: &[CaseSpec], plan: &crate::cli_::RunPlan) {
        println!(
            "== smux_v1 UDS bench：运行时 {}，共 {} 格（{} 路径 × {} 并发度 × {} 轮）==",
            self.runtime,
            specs.len() * plan.repeat,
            plan.modes.len(),
            plan.cases.len(),
            plan.repeat
        );
        println!(
            "   每客户端 {} / 分块 {} / 环 {} / 校验 {} / 单格超时 {}s",
            crate::cli_::human_bytes(plan.bytes_per_client),
            crate::cli_::human_bytes(plan.chunk as u64),
            crate::cli_::human_bytes(plan.ring_cap as u64),
            if plan.verify { "开" } else { "关" },
            plan.timeout_secs
        );
        println!(
            "{:<22} {:>3} {:>9} {:>11} {:>12} {:>13} {:>9}",
            "用例", "N", "总字节", "墙钟(s)", "带宽(MiB/s)", "任务均值(ms)", "并发因子"
        );
    }

    /// 打印并落盘一格结果。
    ///
    /// # Errors
    ///
    /// 写 JSONL / CSV 失败、或互斥量中毒时返回错误。
    pub fn record(&self, result: CaseResult) -> Result<(), Box<dyn std::error::Error>> {
        println!(
            "{:<22} {:>3} {:>9} {:>11.6} {:>12.2} {:>13.3} {:>9.2}{}",
            result.mode.label(),
            result.clients,
            crate::cli_::human_bytes(result.total_bytes),
            result.wall_secs(),
            result.bandwidth_mib_s,
            result.task_stats.mean * 1e3f64,
            result.concurrency_factor,
            match &result.failure {
                Some(note) => format!("   **失败：{note}**"),
                None => String::new(),
            }
        );

        let mut state = self
            .shared_
            .lock()
            .map_err(|_| "结果记录的互斥量中毒".to_string())?;

        if state.jsonl_.is_none() {
            state.jsonl_ = Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.jsonl_path())?,
            );
        }
        if let Some(file) = state.jsonl_.as_mut() {
            writeln!(file, "{}", result.to_json_line())?;
        }

        if state.csv_.is_none() {
            let path = self.csv_path();
            let fresh = !path.exists();
            let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
            if fresh {
                writeln!(file, "{CSV_HEADER}")?;
            }
            state.csv_ = Some(file);
        }
        if let Some(file) = state.csv_.as_mut() {
            writeln!(file, "{}", result.to_csv_line())?;
        }

        state.records_.push(result);
        Ok(())
    }

    /// 写出 Markdown 汇总，并打印三份产物的路径。
    ///
    /// # Errors
    ///
    /// 写文件失败时返回 IO 错误。
    pub fn finish(&self) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let path = self.summary_path();
        let text = self.render_markdown();
        fs::write(&path, text)?;
        println!();
        println!("== 报告已写出 ==");
        println!("  逐格 JSONL：{}", self.jsonl_path().display());
        println!("  跨后端 CSV：{}", self.csv_path().display());
        println!("  汇总 Markdown：{}", path.display());
        Ok(path)
    }

    /// 直接把一条记录塞进内存（**仅供单元测试**：绕开落盘）。
    #[cfg(test)]
    pub fn push_record_for_test_(&self, result: CaseResult) {
        if let Ok(mut state) = self.shared_.lock() {
            state.records_.push(result);
        }
    }

    /// 已采集到的记录（克隆一份；只在收尾时调用）。
    ///
    /// # Errors
    ///
    /// 互斥量中毒时返回说明。
    pub fn records(&self) -> Result<Vec<CaseResult>, String> {
        self.shared_
            .lock()
            .map(|state| state.records_.clone())
            .map_err(|_| "结果记录的互斥量中毒".to_string())
    }

    /// 渲染 Markdown 汇总。
    ///
    /// 内部先取一份记录快照，因此渲染期间不会持锁。
    pub fn render_markdown(&self) -> String {
        let records: Vec<CaseResult> = self
            .shared_
            .lock()
            .map(|state| state.records_.clone())
            .unwrap_or_default();
        self.render_markdown_of_(&records)
    }

    /// 用给定的记录快照渲染 Markdown 汇总。
    fn render_markdown_of_(&self, records: &[CaseResult]) -> String {
        let mut out = String::with_capacity(8192usize);
        out.push_str("# smux_v1 UDS 性能测试报告\n\n");
        out.push_str(&format!("- 运行时后端：`{}`\n", self.runtime));
        out.push_str(&format!(
            "- 开始时间（Unix 秒）：{}\n",
            self.started
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0u64)
        ));
        out.push_str(&format!(
            "- 逻辑 CPU 数：{}\n",
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1usize)
        ));
        out.push_str("- 被测实现：`smux_v1`（本报告由 `smux_v1_uds_bench` 生成）\n");
        out.push_str(
            "- 计时口径：墙钟 = 栅栏释放 → 服务端收完最后一条流；任务时长 = 栅栏释放 → 该任务写完最后一字节\n",
        );
        out.push_str("- 带宽单位：MiB/s（1 MiB = 1024² 字节），按墙钟折算\n\n");

        if records.is_empty() {
            out.push_str("（本次运行没有产生任何结果）\n");
            return out;
        }

        let best = best_per_case_(records);
        out.push_str("## 1. 逐格结果（跨轮取**最好**一轮）\n\n");
        out.push_str(
            "| 路径 | N | 总字节 | 墙钟 s | 服务端收齐 s | 带宽 MiB/s | 任务均值 ms | 任务方差 ms² | 标准差 ms | 极差 ms | CV | 并发因子 |\n",
        );
        out.push_str(
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
        );
        for (mode, clients, result) in best.iter() {
            let stats = &result.task_stats;
            out.push_str(&format!(
                "| {} | {} | {} | {:.6} | {} | {:.2} | {:.3} | {:.4} | {:.3} | {:.3} | {:.4} | {:.2} |\n",
                mode.label(),
                clients,
                crate::cli_::human_bytes(result.total_bytes),
                result.wall_secs(),
                match result.drained_ns {
                    Some(nanos) => format!("{:.6}", stats::nanos_to_secs(nanos as u128)),
                    None => "—".to_string(),
                },
                result.bandwidth_mib_s,
                stats.mean * 1e3f64,
                stats.variance_sample * 1e6f64,
                stats.std_dev_sample * 1e3f64,
                stats.range * 1e3f64,
                stats.cv,
                result.concurrency_factor,
            ));
        }

        out.push_str("\n## 2. raw ↔ smux 对照（同 N、同载荷）\n\n");
        out.push_str(
            "| N | raw 带宽 MiB/s | smux 带宽 MiB/s | smux/raw | raw 并发因子 | smux 并发因子 | raw 任务 CV | smux 任务 CV |\n",
        );
        out.push_str("| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
        for (clients, raw, smux) in pairs_(records) {
            let ratio = match (raw, smux) {
                (Some(r), Some(s)) if r.bandwidth_mib_s > 0.0f64 => {
                    format!("{:.3}", s.bandwidth_mib_s / r.bandwidth_mib_s)
                }
                _ => "—".to_string(),
            };
            let cell = |sel: &Option<&CaseResult>, field: fn(&CaseResult) -> String| match sel {
                Some(res) => field(res),
                None => "—".to_string(),
            };
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} |\n",
                clients,
                cell(&raw, |res| format!("{:.2}", res.bandwidth_mib_s)),
                cell(&smux, |res| format!("{:.2}", res.bandwidth_mib_s)),
                ratio,
                cell(&raw, |res| format!("{:.2}", res.concurrency_factor)),
                cell(&smux, |res| format!("{:.2}", res.concurrency_factor)),
                cell(&raw, |res| format!("{:.4}", res.task_stats.cv)),
                cell(&smux, |res| format!("{:.4}", res.task_stats.cv)),
            ));
        }

        out.push_str("\n## 3. 逐轮明细\n\n");
        out.push_str(
            "| 路径 | N | 轮 | 墙钟 s | 带宽 MiB/s | 任务均值 ms | 任务方差 ms² | 并发因子 | 校验 |\n",
        );
        out.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n");
        for result in records.iter() {
            out.push_str(&format!(
                "| {} | {} | {} | {:.6} | {:.2} | {:.3} | {:.4} | {:.2} | {} |\n",
                result.mode.label(),
                result.clients,
                result.repeat_idx,
                result.wall_secs(),
                result.bandwidth_mib_s,
                result.task_stats.mean * 1e3f64,
                result.task_stats.variance_sample * 1e6f64,
                result.concurrency_factor,
                if result.verified { "开" } else { "关" },
            ));
        }

        out.push_str("\n## 4. 结论\n\n");
        out.push_str(&render_conclusion_(records));

        let failures: Vec<&CaseResult> = records
            .iter()
            .filter(|res| res.failure.is_some())
            .collect();
        if failures.is_empty() {
            out.push_str("\n本次运行**没有失败格**。\n");
        } else {
            out.push_str(&format!(
                "\n本次运行有 **{}** 个失败格：\n\n",
                failures.len()
            ));
            for res in failures {
                out.push_str(&format!(
                    "- `{}` N={} 第 {} 轮：{}\n",
                    res.mode.name(),
                    res.clients,
                    res.repeat_idx,
                    res.failure.as_deref().unwrap_or("")
                ));
            }
        }
        out
    }

}

/// 每个「路径 × 并发度」取跨轮最好的一轮（**按带宽**取）。
fn best_per_case_(records: &[CaseResult]) -> Vec<(Mode, usize, &CaseResult)> {
        let mut best: BTreeMap<(u8, usize), &CaseResult> = BTreeMap::new();
        for result in records.iter() {
            if result.failure.is_some() {
                continue;
            }
            let key = (mode_rank(result.mode), result.clients);
            match best.get(&key) {
                Some(prev) if prev.bandwidth_mib_s >= result.bandwidth_mib_s => {}
                _ => {
                    best.insert(key, result);
                }
            }
        }
        best.into_iter()
            .map(|((_rank, clients), result)| (result.mode, clients, result))
            .collect()
    }

/// 配对出 `(N, raw 最好, smux 最好)`（供报告与测试使用）。
fn pairs_(records: &[CaseResult]) -> Vec<(usize, Option<&CaseResult>, Option<&CaseResult>)> {
        let mut raw: BTreeMap<usize, &CaseResult> = BTreeMap::new();
        let mut smux: BTreeMap<usize, &CaseResult> = BTreeMap::new();
        for result in records.iter() {
            if result.failure.is_some() {
                continue;
            }
            let table = match result.mode {
                Mode::Raw => &mut raw,
                Mode::Smux => &mut smux,
            };
            match table.get(&result.clients) {
                Some(prev) if prev.bandwidth_mib_s >= result.bandwidth_mib_s => {}
                _ => {
                    table.insert(result.clients, result);
                }
            }
        }
        let mut keys: Vec<usize> = raw.keys().chain(smux.keys()).copied().collect();
        keys.sort_unstable();
        keys.dedup();
        keys.into_iter()
            .map(|clients| {
                (
                    clients,
                    raw.get(&clients).copied(),
                    smux.get(&clients).copied(),
                )
            })
            .collect()
    }

/// 结论文本：把「带宽」与「真并发」两件事分开说。
fn render_conclusion_(records: &[CaseResult]) -> String {
        let mut out = String::new();
        let best = best_per_case_(records);
        if best.is_empty() {
            return "（没有可用的结果）\n".to_string();
        }

        out.push_str("**真并发判据**：`并发因子 = Σ(任务时长) / 墙钟`。\n\n");
        out.push_str("- 串行执行（一个接一个完成）时它趋近 1.0；\n");
        out.push_str("- N 个任务真同时推进时它趋近 N；\n");
        out.push_str("- 落在中间说明部分重叠——典型情形是尾部数据还在环 / socket 缓冲里排队，\n");
        out.push_str("  墙钟因此比任务时长之和大得不多。\n\n");

        for (mode, clients, result) in best.iter() {
            let factor = result.concurrency_factor;
            let share = if *clients > 0usize {
                factor / *clients as f64
            } else {
                0.0f64
            };
            out.push_str(&format!(
                "- `{}` N={}：并发因子 **{:.2}**（理论串行 1.00 / 理论并发 {:.2}），\
                 任务时长 CV **{:.4}**，极差 **{:.3} ms**。{}\n",
                mode.name(),
                clients,
                factor,
                clients,
                result.task_stats.cv,
                result.task_stats.range * 1e3f64,
                if share >= 0.7f64 {
                    "⇒ 任务确实在同一段时间里互相交错推进。"
                } else if share >= 0.4f64 {
                    "⇒ 部分重叠：存在明显的排队 / 收尾拖尾。"
                } else {
                    "⇒ **接近串行**，需要检查这条路径上有没有把任务串起来的环节。"
                }
            ));
        }

        out.push_str("\n**raw ↔ smux 的读法**：raw 是「N 条真 socket」的基准；smux 在**同一条**\n");
        out.push_str("socket 上复用 N 条子流，因此它的带宽上限受这一条 socket 与一趟编解码限制。\n");
        out.push_str("两者并发因子都应当随 N 上升；若 smux 的并发因子明显低于 raw，说明复用路径上\n");
        out.push_str("存在把子流串起来的环节。\n");
        out
}

/// 路径在表格里的排序权重（raw 在前，与命令行展开顺序一致）。
fn mode_rank(mode: Mode) -> u8 {
    match mode {
        Mode::Raw => 0u8,
        Mode::Smux => 1u8,
    }
}

/// 报告模块的单元测试：统计装配、JSON / CSV 形状、跨轮取最好。
#[cfg(test)]
mod tests {
    use super::*;

    /// 造一格用于测试的用例规格。
    fn spec_of(mode: Mode, clients: usize) -> CaseSpec {
        CaseSpec {
            mode,
            clients,
            bytes_per_client: 1024u64,
            chunk: 256usize,
            ring_cap: 1024usize,
            seed: 7u64,
            verify: true,
        }
    }

    /// 造一个完成时刻均匀分布的任务时间轴。
    fn times_of(clients: usize) -> Vec<TaskTime> {
        (1usize..=clients)
            .map(|i| TaskTime {
                end_ns: i as u64 * 100_000_000u64,
                ..TaskTime::default()
            })
            .collect()
    }

    /// 测试结果装配给出与手算一致的任务统计与并发因子。
    /// - 手段：4 个任务、完成时刻 100/200/300/400 ms，墙钟 0.5 s，载荷 4 KiB。
    /// - 判断：墙钟 0.5 s、任务均值 0.25 s、并发因子 = 1.0/0.5 = 2.0、总字节 4096、
    ///   带宽等于 `4 KiB / 0.5 s`；`drained_ns` 原样保留为参考量。
    ///
    /// Checks the assembled result against a hand computation.
    /// - Method: four tasks finishing at 100/200/300/400 ms with a 0.5 s wall.
    /// - Assertion: mean 0.25 s, concurrency factor 2.0, total bytes 4096 and the
    ///   bandwidth equals `4 KiB / 0.5 s`.
    #[test]
    fn assemble_matches_hand_computation_() {
        let spec = spec_of(Mode::Smux, 4usize);
        let result = CaseResult::assemble(
            "test",
            &spec,
            1usize,
            500_000_000u64,
            Some(520_000_000u64),
            times_of(4usize),
        );
        assert!(result.is_clean());
        assert_eq!(result.total_bytes, 4096u64);
        assert!((result.wall_secs() - 0.5f64).abs() < 1e-12f64);
        assert_eq!(result.drained_ns, Some(520_000_000u64));
        assert!((result.task_stats.mean - 0.25f64).abs() < 1e-12f64);
        assert!((result.concurrency_factor - 2.0f64).abs() < 1e-9f64);
        let want = (4096.0f64 / (1024.0f64 * 1024.0f64)) / 0.5f64;
        assert!((result.bandwidth_mib_s - want).abs() < 1e-12f64);
        assert_eq!(result.per_task_mib_s.len(), 4usize);
    }

    /// 测试失败格也产出可落盘的结构体，且 `failure` 说明了原因。
    /// - 手段：墙钟为 0 且有一个任务标记失败。
    /// - 判断：`is_clean()` 为假、`failure` 非空、JSON 行里含 `"failure":"`。
    ///
    /// Checks a failed case still produces a serialisable result.
    /// - Method: zero wall clock plus one failed task.
    /// - Assertion: `is_clean` is false, `failure` is set and the JSON line carries it.
    #[test]
    fn failed_case_still_serialises_() {
        let spec = spec_of(Mode::Smux, 2usize);
        let times = vec![
            TaskTime {
                end_ns: 10u64,
                failed: true,
            },
            TaskTime {
                end_ns: 20u64,
                ..TaskTime::default()
            },
        ];
        let result = CaseResult::assemble("test", &spec, 1usize, 0u64, None, times);
        assert!(!result.is_clean());
        assert!(result.failure.is_some());
        let json = result.to_json_line();
        assert!(json.contains("\"failure\":\""), "JSON：{json}");
        assert!(json.starts_with("{\"runtime\":\"test\""), "JSON 首字段：{json}");
        assert!(!json.contains("{,"), "JSON 里有前导逗号：{json}");
        // 字段之间必须有逗号：用最朴素的「成对引号 + 冒号」计数无法判定，这里直接
        // 检查几个已知相邻字段之间的分隔符（曾经的回归：`"tokio""mode"`）。
        assert!(json.contains("\"runtime\":\"test\",\"mode\":"), "JSON 分隔：{json}");
        assert!(json.contains("\"drained_secs\":null"), "JSON：{json}");
        assert!(json.contains("\"mode\":\"smux\""));
    }

    /// 测试 CSV 行与表头的字段数一致（防止两处口径漂移）。
    /// - 手段：造一格干净结果，切 CSV 行的逗号分隔字段并与表头列数比对。
    /// - 判断：表头 20 列、数据行 20 个字段，且失败列为空串。
    ///
    /// Checks the CSV row shape matches the header.
    /// - Method: one clean result, split on commas, compare with the header columns.
    /// - Assertion: 19 header columns and 19 fields, with an empty failure column.
    #[test]
    fn csv_row_shape_matches_header_() {
        assert_eq!(CSV_HEADER.split(',').count(), 20usize);
        let spec = spec_of(Mode::Raw, 2usize);
        let result = CaseResult::assemble("test", &spec, 1usize, 1_000_000u64, Some(1_000_000u64), times_of(2));
        let line = result.to_csv_line();
        assert_eq!(line.split(',').count(), 20usize);
        assert!(line.ends_with(','));
    }

    /// 测试「跨轮取最好」按带宽挑，并且 raw / smux 各挑各的。
    /// - 手段：手工灌入同一格（raw N=2）三轮不同带宽，再灌 smux N=2 一轮。
    /// - 判断：表格里 raw N=2 的带宽是三轮里的最大值；raw 与 smux 各自成行。
    ///
    /// Checks the cross-round "best" selection picks the fastest round per mode.
    /// - Method: feed three raw rounds with different bandwidth plus one smux round.
    /// - Assertion: the raw row carries the maximum bandwidth and both modes appear.
    #[test]
    fn best_per_case_picks_fastest_round_() {
        let runner = Runner::new("test", std::env::temp_dir().to_str().unwrap_or("."))
            .expect("准备输出目录");
        let spec_raw = spec_of(Mode::Raw, 2usize);
        let spec_smux = spec_of(Mode::Smux, 2usize);
        for (round, wall) in [(1usize, 1_000_000u64), (2usize, 500_000u64), (3usize, 800_000u64)] {
            let result =
                CaseResult::assemble("test", &spec_raw, round, wall, Some(wall), times_of(2usize));
            runner.push_record_for_test_(result);
        }
        runner.push_record_for_test_(CaseResult::assemble(
            "test",
            &spec_smux,
            1usize,
            700_000u64,
            Some(700_000u64),
            times_of(2usize),
        ));

        let records = runner.records().expect("取记录");
        let best = best_per_case_(&records);
        assert_eq!(best.len(), 2usize);
        let raw = best
            .iter()
            .find(|(mode, _, _)| *mode == Mode::Raw)
            .expect("应当有 raw 行");
        assert_eq!(raw.2.repeat_idx, 2usize);
        assert_eq!(raw.2.wall_ns, 500_000u64);
        assert!(best.iter().any(|(mode, _, _)| *mode == Mode::Smux));
    }

    /// 测试 Markdown 汇总把两张表与结论段都渲染出来。
    /// - 手段：灌两格（raw / smux 各一）后渲染。
    /// - 判断：文本含四节标题、含路径名，且含「并发因子」判据说明。
    ///
    /// Checks the Markdown summary renders both tables and the conclusion.
    /// - Method: record one raw and one smux case, then render.
    /// - Assertion: the text contains the four section headings, both mode names and
    ///   the concurrency-factor criterion.
    #[test]
    fn markdown_contains_tables_and_conclusion_() {
        let runner = Runner::new("test", std::env::temp_dir().to_str().unwrap_or("."))
            .expect("准备输出目录");
        runner.push_record_for_test_(CaseResult::assemble(
            "test",
            &spec_of(Mode::Raw, 2usize),
            1usize,
            500_000u64,
            Some(500_000u64),
            times_of(2usize),
        ));
        runner.push_record_for_test_(CaseResult::assemble(
            "test",
            &spec_of(Mode::Smux, 2usize),
            1usize,
            600_000u64,
            Some(600_000u64),
            times_of(2usize),
        ));
        let text = runner.render_markdown();
        for needle in [
            "## 1. 逐格结果",
            "## 2. raw ↔ smux 对照",
            "## 3. 逐轮明细",
            "## 4. 结论",
            "`raw`",
            "`smux`",
            "并发因子",
            "没有失败格",
        ] {
            assert!(text.contains(needle), "报告里缺少 `{needle}`");
        }
    }
}
