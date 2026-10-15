//! 命令行解析：对端 / 编排器共用的一份可选参数。
//!
//! # 为什么不引第三方 CLI 库
//!
//! 本 bench 的参数只有十来个、全是「标量 + 一组并发度」，而三个后端各要编一次。
//! 手写解析换来的是**零依赖**：`clap` 会让三格构建多编一遍依赖树，与本项目
//! 「只依赖 smux_v1 与三个适配器」的取向相悖。
//!
//! # 用法
//!
//! ```text
//! bench_compio --runtime compio --cases 2,4,8,16 --bytes 64M --chunk 64K
//!             --modes raw,smux --repeat 3 --out results
//! ```
//!
//! `--help` 会打印完整说明（[`HELP`]）。所有数值都接受 `K` / `M` / `G` 后缀
//! （1024 进制），见 [`parse_size`]。

use crate::{K_DEFAULT_CASES, payload};

/// 运行时节标识（用于「命令行声明的后端」与「编译期后端」对账，以及报告里的名字）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeSel {
    /// tokio 后端。
    Tokio,
    /// compio 后端。
    Compio,
    /// smol 后端。
    Smol,
}

impl RuntimeSel {
    /// 小写名字（进报告、命令行、文件名的那个）。
    pub const fn name(self) -> &'static str {
        match self {
            RuntimeSel::Tokio => "tokio",
            RuntimeSel::Compio => "compio",
            RuntimeSel::Smol => "smol",
        }
    }

    /// 本后端对应的可执行文件名。
    pub const fn bin(self) -> &'static str {
        match self {
            RuntimeSel::Tokio => "bench_tokio",
            RuntimeSel::Compio => "bench_compio",
            RuntimeSel::Smol => "bench_smol",
        }
    }

    /// 转成 `abs_art` 的运行时节标识（用于与运行时值自报的身份比对）。
    pub fn tag(self) -> smux_v1::x_deps::RuntimeTag {
        use smux_v1::x_deps::RuntimeTag;
        match self {
            RuntimeSel::Tokio => RuntimeTag::Tokio,
            RuntimeSel::Compio => RuntimeTag::Compio,
            RuntimeSel::Smol => RuntimeTag::Smol,
        }
    }

    /// 由名字解析（大小写不敏感）。
    pub fn parse(text: &str) -> Result<Self, String> {
        match text.to_ascii_lowercase().as_str() {
            "tokio" => Ok(RuntimeSel::Tokio),
            "compio" => Ok(RuntimeSel::Compio),
            "smol" => Ok(RuntimeSel::Smol),
            other => Err(format!(
                "未知运行时 `{other}`；可选 tokio / compio / smol"
            )),
        }
    }
}

/// **编译期选定的后端**（由 `rt-*` feature 唯一决定）。
///
/// 它与 `--runtime` 是两件事：前者是「这个可执行文件是按哪个后端编出来的」，
/// 后者是「调用方声称自己在跑哪个」。两者不一致就直接失败退出（见 [`crate::assert_runtime`]）。
#[cfg(feature = "rt-tokio")]
pub const K_COMPILED_RUNTIME: RuntimeSel = RuntimeSel::Tokio;

/// 编译期选定的后端。
#[cfg(feature = "rt-compio")]
pub const K_COMPILED_RUNTIME: RuntimeSel = RuntimeSel::Compio;

/// 编译期选定的后端。
#[cfg(feature = "rt-smol")]
pub const K_COMPILED_RUNTIME: RuntimeSel = RuntimeSel::Smol;

/// 被测量的路径。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// **基准组**：每个客户端一条**真实** UNIX domain socket 连接。
    Raw,
    /// **被测组**：一条 UNIX domain socket 上开 N 条 `smux_v1` 复用子流。
    Smux,
}

impl Mode {
    /// 小写名字。
    pub const fn name(self) -> &'static str {
        match self {
            Mode::Raw => "raw",
            Mode::Smux => "smux",
        }
    }

    /// 中文别名（控制台表格用）。
    pub const fn label(self) -> &'static str {
        match self {
            Mode::Raw => "raw(真 UDS 多连接)",
            Mode::Smux => "smux(1 连接 N 子流)",
        }
    }

    /// 由名字解析（大小写不敏感）。
    pub fn parse(text: &str) -> Result<Self, String> {
        match text.to_ascii_lowercase().as_str() {
            "raw" => Ok(Mode::Raw),
            "smux" => Ok(Mode::Smux),
            other => Err(format!("未知路径 `{other}`；可选 raw / smux")),
        }
    }

    /// 本路径的报告文件名前缀。
    pub const fn file_stem(self) -> &'static str {
        self.name()
    }
}

/// 一格用例的规格（与运行时无关）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaseSpec {
    /// 被测量路径。
    pub mode: Mode,
    /// 并发客户端数（= 并发发送任务数）。
    ///
    /// - raw：N 条真实 UDS 连接；
    /// - smux：1 条真实连接上的 N 条复用子流。
    pub clients: usize,
    /// **每个客户端 / 每条子流**发送的字节数（不含 12 字节头部）。
    pub bytes_per_client: u64,
    /// 每次发送 / 接收的字节数。
    pub chunk: usize,
    /// 环容量（字节）：smux 子流环与适配器传输环都用它。
    pub ring_cap: usize,
    /// 载荷种子（两条路径必须一致，否则「内容相同」无从谈起）。
    pub seed: u64,
    /// 是否逐帧核对长度与 CRC-32。
    pub verify: bool,
}

impl CaseSpec {
    /// 本格的载荷总字节数（不含头部）。
    pub fn total_bytes(&self) -> u64 {
        self.bytes_per_client.saturating_mul(self.clients as u64)
    }

    /// 本格的短标识（报告表格与文件名用）。
    pub fn key(&self) -> String {
        format!(
            "{}-n{}-{}-c{}",
            self.mode.name(),
            self.clients,
            human_bytes(self.bytes_per_client),
            human_bytes(self.chunk as u64)
        )
    }
}

/// 跑完整个矩阵所需的全部参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunPlan {
    /// 命令行声明的运行时（要与编译期后端对账）。
    pub runtime: RuntimeSel,
    /// 要测的并发度序列（依次跑）。
    pub cases: Vec<usize>,
    /// 要测的路径。
    pub modes: Vec<Mode>,
    /// **每个客户端 / 每条子流**发送的字节数。
    pub bytes_per_client: u64,
    /// 每次发送 / 接收的字节数。
    pub chunk: usize,
    /// 环容量。
    pub ring_cap: usize,
    /// 每个用例重复多少轮（报告同时给出逐轮明细与跨轮汇总）。
    pub repeat: usize,
    /// **跑器线程数**：并列执行用例的工作线程数。
    ///
    /// - `1`（缺省）：单线程环境——全部用例依次在同一个线程、同一个运行时上跑完；
    /// - `N > 1`：多线程环境——N 个工作线程各自持有**自己的运行时与本地作用域**，
    ///   从共享队列里取用例并列执行。
    ///
    /// 两个环境都要测，是因为它们回答的是不同问题：单线程给出「这套协议在最小上下文
    /// 里的上限」，多线程给出「多个连接、多个运行时并存时还成不成立」。
    pub runners: usize,
    /// 载荷种子。
    pub seed: u64,
    /// 是否逐帧核对长度与 CRC-32。
    pub verify: bool,
    /// 报告输出目录。
    pub out_dir: String,
    /// 单个用例的墙钟上限（秒）；超时由运行时壳中止并报失败。
    pub timeout_secs: u64,
    /// 只打印，不落盘。
    pub dry_run: bool,
}

impl Default for RunPlan {
    fn default() -> Self {
        RunPlan {
            // 缺省的「声称后端」= **编译期选定的后端**：这样裸跑 `bench_tokio` 不会因为
            // 「默认声称 compio」而被自检拦下，同时 `assert_runtime` 的自检仍然有效
            // （拿错二进制去跑时，声称值与 `rt.about()` 依然会对不上）。
            runtime: K_COMPILED_RUNTIME,
            cases: K_DEFAULT_CASES.to_vec(),
            modes: vec![Mode::Raw, Mode::Smux],
            bytes_per_client: payload::K_DEFAULT_BYTES_PER_CLIENT,
            chunk: payload::K_DEFAULT_CHUNK,
            ring_cap: payload::K_DEFAULT_RING_CAP,
            repeat: 3usize,
            runners: 1usize,
            seed: 0x5EED_1234_5678_9ABC_u64,
            verify: true,
            out_dir: "results".to_string(),
            timeout_secs: 120u64,
            dry_run: false,
        }
    }
}

impl RunPlan {
    /// 把「并发度 × 路径」展开成待跑的用例序列。
    ///
    /// 顺序是**先按路径分组、再按并发度递增**：报告里因此总是能看到
    /// `2 → 4 → 8 → 16` 的单调走势，便于人眼读趋势。
    pub fn expand(&self) -> Vec<CaseSpec> {
        let mut out = Vec::with_capacity(self.cases.len() * self.modes.len());
        for &mode in self.modes.iter() {
            for &clients in self.cases.iter() {
                out.push(CaseSpec {
                    mode,
                    clients,
                    bytes_per_client: self.bytes_per_client,
                    chunk: self.chunk,
                    ring_cap: self.ring_cap,
                    seed: self.seed,
                    verify: self.verify,
                });
            }
        }
        out
    }

    /// 校验取值自洽（构造 [`RunPlan`] 之后、动手之前调一次）。
    ///
    /// # Errors
    ///
    /// 并发度为 0、分块为 0、环容量为 0、载荷为 0、重复轮数为 0 时返回说明。
    pub fn validate(&self) -> Result<(), String> {
        if self.cases.contains(&0usize) {
            return Err("并发度不能为 0".to_string());
        }
        if self.cases.is_empty() {
            return Err("至少需要一个并发度".to_string());
        }
        if self.modes.is_empty() {
            return Err("至少需要一条被测路径（raw / smux）".to_string());
        }
        if self.chunk == 0usize {
            return Err("分块大小不能为 0".to_string());
        }
        if self.ring_cap < self.chunk {
            return Err(format!(
                "环容量（{}）必须不小于分块（{}）：否则子流环一次放不下一个分块，\
                 测量的将是「等待环腾空」而不是传输本身",
                self.ring_cap, self.chunk
            ));
        }
        if self.bytes_per_client == 0u64 {
            return Err("每个客户端的字节数不能为 0".to_string());
        }
        if self.repeat == 0usize {
            return Err("重复轮数不能为 0".to_string());
        }
        if self.runners == 0usize {
            return Err("跑器线程数不能为 0（单线程环境写 1）".to_string());
        }
        Ok(())
    }
}

/// `--help` 打印的完整说明。
pub const HELP: &str = "\
smux_v1 UDS 性能测试（三条运行时后端各一个可执行文件）

用法：
  bench_<runtime> [--runtime <名字>] [选项...]

选项：
  --runtime <名字>     声明本进程的运行时（tokio / compio / smol）。
                       它是**自检**：与编译期后端不一致就立刻失败退出。
  --cases <列表>       并发客户端数，逗号分隔。缺省 2,4,8,16。
  --modes <列表>       被测路径，逗号分隔：raw（真 UDS 多连接）/
                       smux（1 条 UDS + N 条复用子流）。缺省 raw,smux。
  --bytes <大小>       每个客户端 / 每条子流发送的字节数。缺省 64M。
  --chunk <大小>       每次发送 / 接收的字节数。缺省 64K。
  --ring-cap <大小>    环容量（smux 子流环与适配器传输环）。缺省 64K。
                       必须不小于 --chunk。
  --repeat <次数>      每个用例重复轮数。缺省 3。
  --runners <线程数>   并列执行用例的工作线程数。缺省 1（单线程环境）；
                       写 N > 1 时每个线程各持自己的运行时与本地作用域，
                       从共享队列里取用例并列跑（多线程环境）。
  --seed <u64>         载荷种子。缺省 0x5EED123456789ABC。
  --no-verify          关闭逐帧长度 / CRC-32 核对（只影响正确性判据，不影响计时口径）。
  --out <目录>         报告输出目录（不存在则创建）。缺省 results。
  --timeout <秒>       单个用例的墙钟上限。缺省 120。
  --dry-run            只打印将跑的用例矩阵，不落盘、不执行。
  -h, --help           打印本说明。

大小后缀：K = 1024，M = 1024²，G = 1024³（大小写均可）。

输出：
  <out>/bench-<runtime>.jsonl          逐格 JSON 行（原始记录，便于二次处理）
  <out>/bench-<runtime>-summary.md     本次运行的汇总报告（表格 + 结论）
  <out>/bench-summary.csv              追加式的跨后端汇总（一个用例一行）
";

/// 解析带 `K` / `M` / `G` 后缀的大小。
///
/// # Errors
///
/// 空串、非数字、未知后缀、或换算后超出 `u64` 时返回说明。
pub fn parse_size(text: &str) -> Result<u64, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("大小不能为空".to_string());
    }
    let (digits, mult) = match trimmed.chars().last().map(|c| c.to_ascii_uppercase()) {
        Some('K') => (&trimmed[..trimmed.len() - 1], 1024u64),
        Some('M') => (&trimmed[..trimmed.len() - 1], 1024u64 * 1024u64),
        Some('G') => (&trimmed[..trimmed.len() - 1], 1024u64 * 1024u64 * 1024u64),
        _ => (trimmed, 1u64),
    };
    let value: u64 = digits
        .trim()
        .parse()
        .map_err(|err| format!("`{text}` 不是合法大小（{err}）"))?;
    value
        .checked_mul(mult)
        .ok_or_else(|| format!("`{text}` 换算后超出 u64"))
}

/// 把字节数renders成人读的短形式（`64K` / `64M` / `1.5G`）。
pub fn human_bytes(bytes: u64) -> String {
    const K: u64 = 1024u64;
    const M: u64 = K * 1024u64;
    const G: u64 = M * 1024u64;
    if bytes >= G && bytes.is_multiple_of(G) {
        format!("{}G", bytes / G)
    } else if bytes >= M && bytes.is_multiple_of(M) {
        format!("{}M", bytes / M)
    } else if bytes >= K && bytes.is_multiple_of(K) {
        format!("{}K", bytes / K)
    } else {
        format!("{bytes}")
    }
}

/// 解析结果：要么得到一份计划，要么「已经处理完了」（打印帮助 / 报错）。
#[derive(Debug)]
pub enum Parsed {
    /// 得到一份可执行的计划。
    Plan(Box<RunPlan>),
    /// 已经打印过帮助或错误，退出码由调用方直接返回。
    Exit(std::process::ExitCode),
}

/// 解析 `std::env::args()`（跳过 argv[0]）。
pub fn parse_env_args() -> Parsed {
    parse_args(std::env::args().skip(1usize))
}

/// 解析一串参数。
///
/// # Errors（经 [`Parsed::Exit`]）
///
/// 未知选项、取值非法、`--help` 时分别打印提示并给出退出码。
pub fn parse_args<I, S>(args: I) -> Parsed
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut plan = RunPlan::default();
    let mut it = args.into_iter().map(|s| s.as_ref().to_string());
    while let Some(arg) = it.next() {
        let (name, inline) = match arg.split_once('=') {
            Some((name, value)) => (name.to_string(), Some(value.to_string())),
            None => (arg.clone(), None),
        };
        // 取一个「有值参数」：既支持 `--x v`，也支持 `--x=v`。
        let mut take_value = |what: &str| -> Result<String, String> {
            match inline.clone() {
                Some(value) => Ok(value),
                None => it
                    .next()
                    .ok_or_else(|| format!("`{what}` 后面缺少取值")),
            }
        };
        match name.as_str() {
            "-h" | "--help" => {
                println!("{HELP}");
                return Parsed::Exit(std::process::ExitCode::SUCCESS);
            }
            "--runtime" => match take_value("--runtime") {
                Ok(value) => match RuntimeSel::parse(&value) {
                    Ok(sel) => plan.runtime = sel,
                    Err(note) => return fail(&note),
                },
                Err(note) => return fail(&note),
            },
            "--cases" => match take_value("--cases") {
                Ok(value) => match parse_usize_list(&value) {
                    Ok(list) => plan.cases = list,
                    Err(note) => return fail(&note),
                },
                Err(note) => return fail(&note),
            },
            "--modes" => match take_value("--modes") {
                Ok(value) => match parse_modes(&value) {
                    Ok(list) => plan.modes = list,
                    Err(note) => return fail(&note),
                },
                Err(note) => return fail(&note),
            },
            "--bytes" => match take_value("--bytes").and_then(|v| parse_size(&v)) {
                Ok(value) => plan.bytes_per_client = value,
                Err(note) => return fail(&note),
            },
            "--chunk" => match take_value("--chunk").and_then(|v| parse_size(&v)) {
                Ok(value) => plan.chunk = value as usize,
                Err(note) => return fail(&note),
            },
            "--ring-cap" => match take_value("--ring-cap").and_then(|v| parse_size(&v)) {
                Ok(value) => plan.ring_cap = value as usize,
                Err(note) => return fail(&note),
            },
            "--repeat" => match take_value("--repeat").and_then(|v| parse_usize(&v)) {
                Ok(value) => plan.repeat = value,
                Err(note) => return fail(&note),
            },
            "--runners" => match take_value("--runners").and_then(|v| parse_usize(&v)) {
                Ok(value) => plan.runners = value,
                Err(note) => return fail(&note),
            },
            "--seed" => match take_value("--seed").and_then(|v| parse_u64(&v)) {
                Ok(value) => plan.seed = value,
                Err(note) => return fail(&note),
            },
            "--timeout" => match take_value("--timeout").and_then(|v| parse_u64(&v)) {
                Ok(value) => plan.timeout_secs = value,
                Err(note) => return fail(&note),
            },
            "--out" => match take_value("--out") {
                Ok(value) => plan.out_dir = value,
                Err(note) => return fail(&note),
            },
            "--no-verify" => plan.verify = false,
            "--dry-run" => plan.dry_run = true,
            other => return fail(&format!("未知选项 `{other}`；试 `--help`")),
        }
    }
    if let Err(note) = plan.validate() {
        return fail(&note);
    }
    Parsed::Plan(Box::new(plan))
}

/// 打印错误并给出失败退出码。
fn fail(note: &str) -> Parsed {
    eprintln!("参数错误：{note}");
    Parsed::Exit(std::process::ExitCode::FAILURE)
}

/// 解析 `2,4,8,16` 形式的整数列表。
fn parse_usize_list(text: &str) -> Result<Vec<usize>, String> {
    let mut out = Vec::new();
    for part in text.split(',') {
        if part.trim().is_empty() {
            continue;
        }
        out.push(parse_usize(part)?);
    }
    if out.is_empty() {
        return Err(format!("`{text}` 里没有有效的整数"));
    }
    Ok(out)
}

/// 解析一个 `usize`。
fn parse_usize(text: &str) -> Result<usize, String> {
    text.trim()
        .parse()
        .map_err(|err| format!("`{text}` 不是合法整数（{err}）"))
}

/// 解析一个 `u64`（支持 `0x` 前缀）。
fn parse_u64(text: &str) -> Result<u64, String> {
    let trimmed = text.trim();
    let parsed = if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        u64::from_str_radix(hex, 16u32)
    } else {
        trimmed.parse::<u64>()
    };
    parsed.map_err(|err| format!("`{text}` 不是合法整数（{err}）"))
}

/// 解析 `raw,smux` 形式的路径列表。
fn parse_modes(text: &str) -> Result<Vec<Mode>, String> {
    let mut out = Vec::new();
    for part in text.split(',') {
        if part.trim().is_empty() {
            continue;
        }
        out.push(Mode::parse(part)?);
    }
    if out.is_empty() {
        return Err(format!("`{text}` 里没有有效的路径"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试默认计划与用例展开顺序。
    /// - 手段：用缺省计划展开，检查用例数与「先 raw 后 smux、并发度递增」的顺序。
    /// - 判断：用例数为 `路径数 × 并发度数`；前 4 格是 raw 的 2/4/8/16，后 4 格是 smux 的。
    ///
    /// Checks the default plan expansion order.
    /// - Method: expand the default plan.
    /// - Assertion: the case count is modes × concurrency, with raw first and the
    ///   concurrency increasing monotonically inside each mode group.
    #[test]
    fn default_plan_expands_modes_then_cases_() {
        let plan = RunPlan::default();
        assert!(plan.validate().is_ok());
        let cases = plan.expand();
        assert_eq!(cases.len(), 8usize);
        for (idx, spec) in cases.iter().enumerate() {
            let want_mode = if idx < 4usize { Mode::Raw } else { Mode::Smux };
            assert_eq!(spec.mode, want_mode);
            assert_eq!(spec.clients, K_DEFAULT_CASES[idx % 4usize]);
        }
        assert_eq!(cases[3usize].total_bytes(), 16u64 * plan.bytes_per_client);
    }

    /// 测试大小后缀解析与非法取值。
    /// - 手段：解析 `64K` / `1M` / `2G` / 纯数字 / `0x10`，再解析几个非法串。
    /// - 判断：合法值等于对应的 1024 进制换算；非法串返回 `Err`。
    ///
    /// Checks the size suffixes and the rejections.
    /// - Method: parse valid suffixes and malformed inputs.
    /// - Assertion: valid inputs match the 1024-based conversion; malformed ones err.
    #[test]
    fn size_suffixes_parse_and_reject_() {
        assert_eq!(parse_size("64K").unwrap(), 64u64 * 1024u64);
        assert_eq!(parse_size("1M").unwrap(), 1024u64 * 1024u64);
        assert_eq!(parse_size("2G").unwrap(), 2u64 * 1024u64 * 1024u64 * 1024u64);
        assert_eq!(parse_size("4096").unwrap(), 4096u64);
        assert!(parse_size("").is_err());
        assert!(parse_size("12X").is_err());
        assert!(parse_size("abc").is_err());
    }

    /// 测试 `--x=v` 与 `--x v` 两种写法等价，且非法组合被拒。
    /// - 手段：分别用两种写法解析同一组参数；再解析 `--chunk 128K --ring-cap 64K`。
    /// - 判断：两种写法得到完全相同的计划；环容量小于分块时校验失败。
    ///
    /// Checks both option spellings and the ring-capacity invariant.
    /// - Method: parse the same options in both spellings, then a broken ring cap.
    /// - Assertion: the two plans are equal; the broken one fails validation.
    #[test]
    fn option_spellings_and_ring_cap_invariant_() {
        let spaced = parse_args([
            "--runtime",
            "tokio",
            "--cases",
            "2,4",
            "--bytes",
            "1M",
        ]);
        let inlined = parse_args(["--runtime=tokio", "--cases=2,4", "--bytes=1M"]);
        let plan_of = |parsed: Parsed| match parsed {
            Parsed::Plan(plan) => *plan,
            Parsed::Exit(_) => panic!("应当解析成功"),
        };
        let a = plan_of(spaced);
        let b = plan_of(inlined);
        assert_eq!(a, b);
        assert_eq!(a.runtime, RuntimeSel::Tokio);
        assert_eq!(a.cases, vec![2usize, 4usize]);
        assert_eq!(a.bytes_per_client, 1024u64 * 1024u64);
        assert_eq!(a.runners, 1usize);

        match parse_args(["--chunk", "128K", "--ring-cap", "64K"]) {
            Parsed::Exit(_) => {}
            Parsed::Plan(_) => panic!("环容量小于分块时应当被拒"),
        }
    }

    /// 测试未知选项与 `--help` 都走「已处理完」分支。
    /// - 手段：解析 `--nope` 与 `--help`。
    /// - 判断：两者都返回 [`Parsed::Exit`]，不产生计划。
    ///
    /// Checks unknown options and `--help` both exit early.
    /// - Method: parse `--nope` and `--help`.
    /// - Assertion: both return `Parsed::Exit` instead of a plan.
    #[test]
    fn unknown_option_and_help_exit_() {
        assert!(matches!(parse_args(["--nope"]), Parsed::Exit(_)));
        assert!(matches!(parse_args(["--help"]), Parsed::Exit(_)));
    }

    /// 测试 `--runners` 的解析与 0 值拒绝。
    /// - 手段：解析 `--runners 4`，再解析 `--runners 0`。
    /// - 判断：前者得到 4；后者被校验拒绝（退回 `Parsed::Exit`）。
    ///
    /// Checks the `--runners` option and rejects zero.
    /// - Method: parse `--runners 4` and then `--runners 0`.
    /// - Assertion: the first yields 4 and the second is rejected.
    #[test]
    fn runners_option_parses_and_rejects_zero_() {
        match parse_args(["--runners", "4"]) {
            Parsed::Plan(plan) => assert_eq!(plan.runners, 4usize),
            Parsed::Exit(_) => panic!("应当解析成功"),
        }
        assert!(matches!(parse_args(["--runners", "0"]), Parsed::Exit(_)));
    }

    /// 测试路径解析与用例标识。
    /// - 手段：解析 `smux`、`RAW`（大小写）并构造一格用例。
    /// - 判断：两种写法归一到同一个枚举；`key()` 同时含路径名与并发数。
    ///
    /// Checks mode parsing and the case key.
    /// - Method: parse `smux` and `RAW`, then build a case.
    /// - Assertion: both spellings normalise to one variant and the key names the
    ///   mode and the client count.
    #[test]
    fn mode_parsing_and_key_() {
        assert_eq!(Mode::parse("RAW").unwrap(), Mode::Raw);
        assert_eq!(Mode::parse("smux").unwrap(), Mode::Smux);
        assert!(Mode::parse("bogus").is_err());
        let spec = CaseSpec {
            mode: Mode::Smux,
            clients: 8usize,
            bytes_per_client: 64u64 * 1024u64 * 1024u64,
            chunk: 64u64 as usize * 1024usize,
            ring_cap: 64usize * 1024usize,
            seed: 1u64,
            verify: true,
        };
        assert!(spec.key().starts_with("smux-n8-64M"));
    }
}
