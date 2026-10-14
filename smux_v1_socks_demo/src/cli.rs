//! 命令行解析：**手写**，不引 `clap`。
//!
//! 本 demo 的参数面很小（运行时、角色、两个地址、负载四件套），手写解析少一个依赖，
//! 也让单元测试可以直接构造参数切片来调 [`parse_peer_args`]。
//!
//! 支持 `--key value` 与 `--key=value` 两种写法。

use std::{net::SocketAddr, process::ExitCode};

use smux_v1::x_deps::RuntimeTag;

use crate::{
    proto::K_HEADER_LEN,
    scenario::{K_TG_DOCK_ACCEPTER, K_TG_DOCK_INITIATOR},
};

/// `smux_v1` 帧头的**最坏编码长度**。
///
/// 上游的 `K_MAX_FRAME_HEADER` 是 `pub(crate)`，demo 拿不到，因此这里按同一数值复述
/// 一遍——只为在**参数解析**阶段就判掉「telegraph 单帧必然超过协商帧长上限」这种配置，
/// 而不是等运行期让发送侧把它整条丢弃、停止等待永远等不到 ACK。
pub const K_FRAME_HEAD_MAX: usize = 64usize;

/// `--help` 的哨兵：解析函数用它区分「要看帮助」与「参数写错了」。
pub const PEER_HELP: &str = "\
用法：peer_<runtime> [选项]

  --runtime <tokio|compio|smol>  本进程的异步运行时（**必须与编译期后端一致**）
  --role <active|passive>        本轮谁先当主动端（阶段 1 的 dialer）
  --listen <ip:port>             本端 TCP 监听地址（可写 0 让内核选端口）
  --peer <ip:port>               对端 TCP 监听地址
  --streams <n>                  每条连接开多少条子流（默认 8）
  --bytes <n>                    每个方向发送的本体字节数（默认 8 MiB）
  --seed <n>                     确定性载荷的种子（两端必须一致，默认 20261007）
  --chunk <n>                    本体分块大小（默认 16 KiB）
  --ring-cap <n>                 子流环容量，也就是本端接收窗口（默认 256 KiB）
  --max-packet-size <n>          协商的帧总长上限（默认 4096，与协议缺省一致）
  --pre-flood-yield <n>          建流完成后先给本地队列 n 个 tick 再灌数据（默认 256）。
                                 不是正确性需要，而是本负载下的节奏选择：实测 release
                                 档 8 子流 × 8 MiB 时，0 约 30-32 MiB/s、256 约 37-39 MiB/s
  --base-dock <n>                发起侧临时 local dock 基址（默认 4096）
  --service-dock <n>             被动侧监听 dock（默认 1）
  --mix-telegraph                **开关**（无值）：在每条连接上额外开一条 telegraph 端点，
                                 与子流并发跑「发一帧 → 等对端 ACK → 再发下一帧」的
                                 停止等待闭环（默认关闭）。开启时两端都要带这个开关
  --tg-frames <n>                telegraph 每个方向的报文条数（默认 16）
  --tg-bytes <n>                 telegraph 每条报文的载荷字节数（默认 256）
  --tag <s>                      结果行里的标签，便于编排器区分（默认空）
  --help                         打印本帮助

一次运行包含**两个阶段**：阶段 1 由 `--role active` 的一端拨号并当主动端，另一端当
被动端；阶段 2 反过来。因此两个进程都必须给出同一个 `--peer` 语义下的对端地址。
";

/// 三种异步运行时。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeSel {
    /// tokio。
    Tokio,
    /// compio。
    Compio,
    /// smol。
    Smol,
}

impl RuntimeSel {
    /// 命令行里的名字（也是报告里的名字）。
    pub const fn name(self) -> &'static str {
        match self {
            RuntimeSel::Tokio => "tokio",
            RuntimeSel::Compio => "compio",
            RuntimeSel::Smol => "smol",
        }
    }

    /// 对应的 peer 可执行文件名。
    pub const fn bin(self) -> &'static str {
        match self {
            RuntimeSel::Tokio => "peer_tokio",
            RuntimeSel::Compio => "peer_compio",
            RuntimeSel::Smol => "peer_smol",
        }
    }

    /// 编译期后端自报身份用的标签。
    pub const fn tag(self) -> RuntimeTag {
        match self {
            RuntimeSel::Tokio => RuntimeTag::Tokio,
            RuntimeSel::Compio => RuntimeTag::Compio,
            RuntimeSel::Smol => RuntimeTag::Smol,
        }
    }

    /// 解析名字。
    ///
    /// # Errors
    ///
    /// 名字不在 `tokio` / `compio` / `smol` 之内时返回错误。
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "tokio" => Ok(RuntimeSel::Tokio),
            "compio" => Ok(RuntimeSel::Compio),
            "smol" => Ok(RuntimeSel::Smol),
            other => Err(format!(
                "无法识别的运行时 `{other}`（可用 tokio / compio / smol）"
            )),
        }
    }
}

/// 本轮最先当主动端的那一侧。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// 阶段 1 dial 对端并当主动端，阶段 2 当被动端。
    Active,
    /// 阶段 1 当被动端，阶段 2 dial 对端并当主动端。
    Passive,
}

impl Role {
    /// 命令行里的名字（也是报告里的名字）。
    pub const fn name(self) -> &'static str {
        match self {
            Role::Active => "active",
            Role::Passive => "passive",
        }
    }

    /// 解析名字。
    ///
    /// # Errors
    ///
    /// 名字不是 `active` / `passive` 时返回错误。
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "active" => Ok(Role::Active),
            "passive" => Ok(Role::Passive),
            other => Err(format!("无法识别的角色 `{other}`（可用 active / passive）")),
        }
    }
}

/// 一个 peer 进程的全部参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerOptions {
    /// 本进程的运行时（必须与编译期后端一致）。
    pub runtime: RuntimeSel,
    /// 本轮谁先当主动端。
    pub role: Role,
    /// 本端 TCP 监听地址。
    pub listen: SocketAddr,
    /// 对端 TCP 监听地址。
    pub peer: SocketAddr,
    /// 每条连接的子流条数。
    pub streams: usize,
    /// 每个方向发送的本体字节数。
    pub bytes: u64,
    /// 确定性载荷的种子（两端必须一致）。
    pub seed: u64,
    /// 本体分块大小。
    pub chunk: usize,
    /// 子流环容量（= 本端接收窗口）。
    pub ring_cap: usize,
    /// 协商的 `max_packet_size`（帧总长上限，含帧头）。
    pub max_packet_size: usize,
    /// 建流完成后、开始灌数据之前，先给本地队列多少个 tick。
    pub pre_flood_yield: usize,
    /// 发起侧临时 local dock 基址。
    pub base_dock: u32,
    /// 被动侧监听 dock。
    pub service_dock: u32,
    /// 是否在每条连接上额外开一条 telegraph 端点（与子流并发）。
    pub mix_telegraph: bool,
    /// telegraph 每个方向的报文条数。
    pub tg_frames: usize,
    /// telegraph 每条报文的载荷字节数。
    pub tg_bytes: usize,
    /// 结果行里的标签。
    pub tag: String,
}

impl Default for PeerOptions {
    fn default() -> Self {
        PeerOptions {
            runtime: RuntimeSel::Compio,
            role: Role::Active,
            listen: "127.0.0.1:0".parse().expect("字面量地址合法"),
            peer: "127.0.0.1:0".parse().expect("字面量地址合法"),
            streams: 8,
            bytes: 8 * 1024 * 1024,
            seed: 20_261_007,
            chunk: 16 * 1024,
            ring_cap: 256 * 1024,
            max_packet_size: 4096,
            pre_flood_yield: 256,
            base_dock: 4096,
            service_dock: 1,
            mix_telegraph: false,
            tg_frames: 16,
            tg_bytes: 256,
            tag: String::new(),
        }
    }
}

/// 解析结果：要么拿到参数，要么已经决定退出（帮助 / 参数错误）。
#[derive(Debug)]
pub enum ParsedArgs {
    /// 参数齐备。
    Options(Box<PeerOptions>),
    /// 打印过帮助或错误信息，按该退出码退出。
    Exit(ExitCode),
}

/// 从 `std::env::args()` 解析参数，并把帮助 / 错误信息直接打在对应的流上。
///
/// 参数错误返回 [`ExitCode::from(2)`]，`--help` 返回 [`ExitCode::SUCCESS`]。
pub fn parse_env_args() -> ParsedArgs {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match parse_peer_args(&args) {
        Ok(opts) => ParsedArgs::Options(Box::new(opts)),
        Err(msg) if msg == PEER_HELP => {
            println!("{PEER_HELP}");
            ParsedArgs::Exit(ExitCode::SUCCESS)
        }
        Err(msg) => {
            eprintln!("参数错误：{msg}\n");
            eprintln!("{PEER_HELP}");
            ParsedArgs::Exit(ExitCode::from(2))
        }
    }
}

/// 解析 peer 的命令行参数（不含 `argv[0]`）。
///
/// # Errors
///
/// - 遇到 `--help`：返回 [`PEER_HELP`]（调用方据此打印帮助）；
/// - 未知选项、缺值、值不合法、越界：返回人类可读的错误说明。
pub fn parse_peer_args(args: &[String]) -> Result<PeerOptions, String> {
    let mut opts = PeerOptions::default();
    let mut seen_runtime = false;
    let mut seen_role = false;
    let mut seen_listen = false;
    let mut seen_peer = false;

    let mut i = 0usize;
    while i < args.len() {
        let raw = args[i].as_str();
        if raw == "--help" || raw == "-h" {
            return Err(PEER_HELP.to_string());
        }
        let (key, inline) = match raw.split_once('=') {
            Some((k, v)) => (k, Some(v.to_string())),
            None => (raw, None),
        };
        // **无值开关**必须在「把下一个参数当作取值」之前落定，否则 `--mix-telegraph`
        // 会把紧随其后的那个选项吞成自己的值。
        if key == "--mix-telegraph" {
            opts.mix_telegraph = match inline {
                Some(v) => parse_bool_(&v, key)?,
                None => true,
            };
            i += 1;
            continue;
        }
        let value = match inline {
            Some(v) => v,
            None => {
                i += 1;
                args.get(i)
                    .cloned()
                    .ok_or_else(|| format!("`{key}` 缺少取值"))?
            }
        };
        match key {
            "--runtime" => {
                opts.runtime = RuntimeSel::parse(&value)?;
                seen_runtime = true;
            }
            "--role" => {
                opts.role = Role::parse(&value)?;
                seen_role = true;
            }
            "--listen" => {
                opts.listen = parse_addr(&value, "--listen")?;
                seen_listen = true;
            }
            "--peer" => {
                opts.peer = parse_addr(&value, "--peer")?;
                seen_peer = true;
            }
            "--streams" => opts.streams = parse_num(&value, "--streams")?,
            "--bytes" => opts.bytes = parse_num(&value, "--bytes")?,
            "--seed" => opts.seed = parse_num(&value, "--seed")?,
            "--chunk" => opts.chunk = parse_num(&value, "--chunk")?,
            "--ring-cap" => opts.ring_cap = parse_num(&value, "--ring-cap")?,
            "--max-packet-size" => opts.max_packet_size = parse_num(&value, "--max-packet-size")?,
            "--pre-flood-yield" => opts.pre_flood_yield = parse_num(&value, "--pre-flood-yield")?,
            "--base-dock" => opts.base_dock = parse_num(&value, "--base-dock")?,
            "--service-dock" => opts.service_dock = parse_num(&value, "--service-dock")?,
            "--tg-frames" => opts.tg_frames = parse_num(&value, "--tg-frames")?,
            "--tg-bytes" => opts.tg_bytes = parse_num(&value, "--tg-bytes")?,
            "--tag" => opts.tag = value,
            other => return Err(format!("无法识别的选项 `{other}`")),
        }
        i += 1;
    }

    if !seen_runtime {
        return Err("`--runtime` 是必填项".to_string());
    }
    if !seen_role {
        return Err("`--role` 是必填项".to_string());
    }
    if !seen_listen {
        return Err("`--listen` 是必填项".to_string());
    }
    if !seen_peer {
        return Err("`--peer` 是必填项".to_string());
    }
    if opts.streams == 0usize {
        return Err("`--streams` 至少为 1".to_string());
    }
    if opts.bytes == 0u64 {
        return Err("`--bytes` 至少为 1".to_string());
    }
    if opts.chunk == 0usize {
        return Err("`--chunk` 至少为 1".to_string());
    }
    if opts.max_packet_size < 64usize {
        return Err("`--max-packet-size` 太小（至少 64）".to_string());
    }
    if opts.ring_cap < opts.chunk {
        return Err(format!(
            "`--ring-cap`（{}）必须不小于 `--chunk`（{}），否则环永远装不下一块",
            opts.ring_cap, opts.chunk
        ));
    }
    let dock_span = u64::from(opts.base_dock) + opts.streams as u64;
    if dock_span > u64::from(u32::MAX) {
        return Err("`--base-dock + --streams` 超出 u32 范围".to_string());
    }
    let service = u64::from(opts.service_dock);
    if service >= u64::from(opts.base_dock) && service < dock_span {
        return Err(format!(
            "`--service-dock`（{}）落在发起侧临时 dock 区间 [{}, {}) 之内，会撞身份",
            opts.service_dock, opts.base_dock, dock_span
        ));
    }
    if opts.tg_frames == 0usize {
        return Err("`--tg-frames` 至少为 1".to_string());
    }
    if opts.tg_bytes == 0usize {
        return Err("`--tg-bytes` 至少为 1".to_string());
    }
    if opts.mix_telegraph {
        // 一条 telegraph 报文的线上长度 = 信封（定长头）+ 载荷。
        let frame = K_HEADER_LEN + opts.tg_bytes;
        if frame + K_FRAME_HEAD_MAX > opts.max_packet_size {
            return Err(format!(
                "telegraph 单帧（信封 {K_HEADER_LEN} + 载荷 {}）加上帧头上限 {K_FRAME_HEAD_MAX} \
                 超过 `--max-packet-size`（{}）：发送侧会把这一条整帧丢弃",
                opts.tg_bytes, opts.max_packet_size
            ));
        }
        if frame > opts.ring_cap {
            return Err(format!(
                "telegraph 单帧（{frame}）超过 `--ring-cap`（{}）：接收环装不下一整条报文，\
                 停止等待会永远等不到 ACK",
                opts.ring_cap
            ));
        }
        for dock in [K_TG_DOCK_INITIATOR, K_TG_DOCK_ACCEPTER] {
            if dock == opts.service_dock {
                return Err(format!(
                    "telegraph 固定使用 dock {dock}，与 `--service-dock`（{}）相撞",
                    opts.service_dock
                ));
            }
            let d = u64::from(dock);
            if d >= u64::from(opts.base_dock) && d < dock_span {
                return Err(format!(
                    "telegraph 固定使用 dock {dock}，落在发起侧临时 dock 区间 [{}, {}) 之内",
                    opts.base_dock, dock_span
                ));
            }
        }
    }
    Ok(opts)
}

/// 解析开关取值（只在显式写成 `--key=value` 时用到）。
fn parse_bool_(value: &str, key: &str) -> Result<bool, String> {
    match value {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(format!(
            "`{key}` 的值 `{value}` 不是合法开关（true / false）"
        )),
    }
}

/// 解析 `<ip:port>`。
fn parse_addr(value: &str, key: &str) -> Result<SocketAddr, String> {
    value
        .parse::<SocketAddr>()
        .map_err(|e| format!("`{key}` 的值 `{value}` 不是合法的 <ip:port>：{e}"))
}

/// 解析十进制正整数。
fn parse_num<T>(value: &str, key: &str) -> Result<T, String>
where
    T: std::str::FromStr,
{
    value
        .parse::<T>()
        .map_err(|_| format!("`{key}` 的值 `{value}` 不是合法的正整数"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 校验：完整的一行参数能被解析成期望的字段。
    ///
    /// - 手段：构造带 `--key value` 与 `--key=value` 两种写法的参数切片，调用
    ///   [`parse_peer_args`]。
    /// - 判断：返回 `Ok`，且运行时、角色、地址、负载参数与写入的值逐一相等。
    #[test]
    fn parses_full_command_line() {
        let args: Vec<String> = [
            "--runtime",
            "smol",
            "--role=passive",
            "--listen",
            "0.0.0.0:7001",
            "--peer=192.168.7.100:7002",
            "--streams",
            "4",
            "--bytes=65536",
            "--seed",
            "7",
            "--chunk",
            "8192",
            "--ring-cap",
            "65536",
            "--base-dock",
            "8192",
            "--service-dock",
            "1",
            "--mix-telegraph",
            "--tg-frames",
            "4",
            "--tg-bytes=64",
            "--tag",
            "case-a",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let opts = parse_peer_args(&args).expect("应当解析成功");
        assert_eq!(opts.runtime, RuntimeSel::Smol);
        assert_eq!(opts.role, Role::Passive);
        assert_eq!(opts.listen.to_string(), "0.0.0.0:7001");
        assert_eq!(opts.peer.to_string(), "192.168.7.100:7002");
        assert_eq!(opts.streams, 4);
        assert_eq!(opts.bytes, 65536);
        assert_eq!(opts.seed, 7);
        assert_eq!(opts.chunk, 8192);
        assert_eq!(opts.ring_cap, 65536);
        assert_eq!(opts.base_dock, 8192);
        assert!(opts.mix_telegraph);
        assert_eq!(opts.tg_frames, 4);
        assert_eq!(opts.tg_bytes, 64);
        assert_eq!(opts.tag, "case-a");
    }

    /// 校验：`--mix-telegraph` 是**无值开关**——它不会把紧随其后的选项吞成自己的取值，
    /// 而 `--mix-telegraph=false` 这种显式写法能把它关掉。
    ///
    /// - 手段：构造 `--mix-telegraph --tg-frames 4` 与 `--mix-telegraph=false` 两组参数，
    ///   分别解析。
    /// - 判断：前者 `mix_telegraph == true` **且** `tg_frames == 4`（说明没吞参数）；
    ///   后者 `mix_telegraph == false`。
    #[test]
    fn mix_telegraph_is_a_valueless_switch() {
        let base = [
            "--runtime",
            "compio",
            "--role",
            "active",
            "--listen",
            "127.0.0.1:0",
            "--peer",
            "127.0.0.1:0",
        ];

        let mut on: Vec<String> = base.iter().map(|s| s.to_string()).collect();
        on.extend(
            ["--mix-telegraph", "--tg-frames", "4"]
                .iter()
                .map(|s| s.to_string()),
        );
        let opts = parse_peer_args(&on).expect("应当解析成功");
        assert!(opts.mix_telegraph);
        assert_eq!(opts.tg_frames, 4, "开关后面的 `--tg-frames` 不能被吞掉");

        let mut off: Vec<String> = base.iter().map(|s| s.to_string()).collect();
        off.push("--mix-telegraph=false".to_string());
        let opts = parse_peer_args(&off).expect("应当解析成功");
        assert!(!opts.mix_telegraph);
    }

    /// 校验：telegraph 的三类**非法配置**都在解析阶段被拒。
    ///
    /// - 手段：在一条合法命令行的基础上分别制造「单帧超过 `--max-packet-size`」「单帧
    ///   超过 `--ring-cap`」「telegraph 的固定 dock 与 `--service-dock` 相撞」三种情形。
    /// - 判断：三次都返回 `Err`，且错误文案分别点名对应的那个选项。
    #[test]
    fn rejects_bad_telegraph_config() {
        let base = [
            "--runtime",
            "compio",
            "--role",
            "active",
            "--listen",
            "127.0.0.1:0",
            "--peer",
            "127.0.0.1:0",
            "--mix-telegraph",
            "--tg-bytes",
            "256",
        ];

        let mut too_long: Vec<String> = base.iter().map(|s| s.to_string()).collect();
        too_long.extend(["--max-packet-size", "256"].iter().map(|s| s.to_string()));
        assert!(
            parse_peer_args(&too_long)
                .unwrap_err()
                .contains("--max-packet-size")
        );

        let mut tiny_ring: Vec<String> = base.iter().map(|s| s.to_string()).collect();
        tiny_ring.extend(
            ["--ring-cap", "64", "--chunk", "64"]
                .iter()
                .map(|s| s.to_string()),
        );
        assert!(
            parse_peer_args(&tiny_ring)
                .unwrap_err()
                .contains("--ring-cap")
        );

        let mut dock_clash: Vec<String> = base.iter().map(|s| s.to_string()).collect();
        dock_clash.push("--service-dock".to_string());
        dock_clash.push("3".to_string());
        assert!(
            parse_peer_args(&dock_clash)
                .unwrap_err()
                .contains("--service-dock")
        );
    }

    /// 校验：必填项缺失、选项写错、数值不合法、dock 区间撞身份这四类输入都被拒绝。
    ///
    /// - 手段：分别构造只缺一项 / 含未知选项 / 数值写错 / `--service-dock` 落进
    ///   `--base-dock` 区间的参数切片。
    /// - 判断：四次调用都返回 `Err`，且错误文案里带上出问题的那个选项名。
    #[test]
    fn rejects_bad_command_lines() {
        let base = [
            "--runtime",
            "tokio",
            "--role",
            "active",
            "--listen",
            "127.0.0.1:0",
            "--peer",
            "127.0.0.1:0",
        ];
        let missing_peer: Vec<String> = base[..base.len() - 2]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(
            parse_peer_args(&missing_peer)
                .unwrap_err()
                .contains("--peer")
        );

        let mut unknown: Vec<String> = base.iter().map(|s| s.to_string()).collect();
        unknown.push("--nope".to_string());
        unknown.push("1".to_string());
        assert!(parse_peer_args(&unknown).unwrap_err().contains("--nope"));

        let mut bad_num: Vec<String> = base.iter().map(|s| s.to_string()).collect();
        bad_num.push("--streams".to_string());
        bad_num.push("zero".to_string());
        assert!(parse_peer_args(&bad_num).unwrap_err().contains("--streams"));

        let mut clash: Vec<String> = base.iter().map(|s| s.to_string()).collect();
        clash.extend(
            [
                "--base-dock",
                "4096",
                "--streams",
                "8",
                "--service-dock",
                "4098",
            ]
            .iter()
            .map(|s| s.to_string()),
        );
        assert!(
            parse_peer_args(&clash)
                .unwrap_err()
                .contains("--service-dock")
        );
    }

    /// 校验：`--help` 会以 [`PEER_HELP`] 作为错误文案返回，供调用方打印帮助。
    ///
    /// - 手段：只传 `--help`。
    /// - 判断：返回值等于 [`PEER_HELP`] 常量。
    #[test]
    fn help_uses_sentinel() {
        let args = vec!["--help".to_string()];
        assert_eq!(parse_peer_args(&args).unwrap_err(), PEER_HELP);
    }
}
