//! 结果行：一行 JSON（给编排器读）+ 控制台摘要（给人看）。
//!
//! 刻意不引 `serde`：要编码的结构是固定的小结构，手写一遍比多一个依赖更划算，也让
//! 「编排器到底读哪几个字段」这件事在源码里一眼可见。

use std::{net::SocketAddr, process::ExitCode};

use crate::{
    cli::{PeerOptions, Role, RuntimeSel},
    metrics::MetricsDelta,
    scenario::PhaseReport,
};

/// 一个进程跑完一次实验之后的全部结论。
#[derive(Debug, Clone)]
pub struct PeerResult {
    /// 本进程的运行时（编译期后端）。
    pub runtime: RuntimeSel,
    /// 本轮最先当主动端的那一侧。
    pub role: Role,
    /// 本端监听地址。
    pub listen: SocketAddr,
    /// 对端监听地址。
    pub peer: SocketAddr,
    /// 编排器给的标签。
    pub tag: String,
    /// 全部判据是否通过。
    pub ok: bool,
    /// 两个阶段的结论（阶段 1、阶段 2）。
    pub phases: Vec<PhaseReport>,
    /// 进程级指标增量（含连接 / 子流收尾）。
    pub metrics: MetricsDelta,
    /// 从解析参数到打印结果的总墙钟（纳秒）。
    pub total_elapsed_ns: u64,
    /// 进程级失败说明。
    pub failures: Vec<String>,
}

impl PeerResult {
    /// 编码成一行 JSON。
    pub fn to_json(&self) -> String {
        let phases: Vec<String> = self.phases.iter().map(phase_json_).collect();
        format!(
            "{{\"event\":\"result\",\"runtime\":\"{}\",\"role\":\"{}\",\"listen\":\"{}\",\
             \"peer\":\"{}\",\"tag\":\"{}\",\"ok\":{},\"total_elapsed_ms\":{:.3},\
             \"phases\":[{}],\"metrics\":{},\"failures\":{}}}",
            self.runtime.name(),
            self.role.name(),
            self.listen,
            self.peer,
            esc_(&self.tag),
            self.ok,
            self.total_elapsed_ns as f64 / 1_000_000.0,
            phases.join(","),
            metrics_json_(&self.metrics),
            str_array_json_(&self.failures),
        )
    }

    /// 往 stdout 打一份人看的摘要。
    pub fn print_console(&self) {
        println!(
            "== {} / role={} tag={} ==",
            self.runtime.name(),
            self.role.name(),
            if self.tag.is_empty() { "-" } else { &self.tag }
        );
        println!(
            "   listen={} peer={} 结论={}",
            self.listen,
            self.peer,
            if self.ok { "通过" } else { "失败" }
        );
        for phase in &self.phases {
            println!(
                "   [{}] 子流 {} 条：发出 {} B / 收到 {} B，回执确认 {}，接收校验 {}，\
                 失败子流 {}，耗时 {:.3} ms，吞吐(发) {:.2} MiB/s",
                phase.role,
                phase.streams,
                phase.sent_bytes,
                phase.recv_bytes,
                phase.sent_acked,
                phase.recv_verified,
                phase.failed_channels,
                phase.elapsed_ns as f64 / 1_000_000.0,
                mib_per_s_(phase.sent_bytes, phase.elapsed_ns),
            );
            if phase.mix_telegraph {
                println!(
                    "        telegraph：报文 {}/{} 条被 ACK，接收校验 {}/{} 条（每帧 {} B）",
                    phase.tg_sent_acked,
                    phase.tg_frames,
                    phase.tg_recv_verified,
                    phase.tg_frames,
                    phase.tg_bytes,
                );
            }
            for note in &phase.failures {
                println!("        ! {note}");
            }
        }
        let m = &self.metrics;
        println!(
            "   metrics：连接 {} 开 / {} 关，子流 {} 开 / {} 关，帧 {} 发 / {} 收，\
             DATA {} 发 / {} 收，WINDOW_UPDATE {} 发 / {} 收，帧字节 {} 发 / {} 收，\
             传输字节 {} 发 / {} 收，流控阻塞 {} 次 / {} ms，帧错误 {}，RESET {} 本地 / {} 对端",
            m.conns_opened,
            m.conns_closed,
            m.channels_opened,
            m.channels_closed,
            m.frames_sent,
            m.frames_recv,
            m.data_frames_sent,
            m.data_frames_recv,
            m.window_updates_sent,
            m.window_updates_recv,
            m.frame_bytes_sent,
            m.frame_bytes_recv,
            m.transport_bytes_sent,
            m.transport_bytes_recv,
            m.flow_stalled,
            m.flow_stall_ms,
            m.frame_errors,
            m.resets_by_local,
            m.resets_by_peer,
        );
        println!(
            "   子流寿命：之和 {} ms，最大 {} ms，分桶[<10ms,<100ms,<1s,<10s,>=10s]={:?}",
            m.channel_lifetime_sum_ms, m.channel_lifetime_max_ms, m.channel_lifetime_buckets
        );
        for note in &self.failures {
            println!("   ! {note}");
        }
    }
}

/// 打印「已监听」事件行，让编排器知道可以放对端起来了。
///
/// 这一行必须在**绑定成功之后、拨号之前**打印，并立刻 flush，否则编排器可能抢跑。
pub fn print_ready_event(
    runtime: RuntimeSel,
    role: Role,
    listen: SocketAddr,
    actual: SocketAddr,
    tag: &str,
) {
    use std::io::Write;
    println!(
        "{{\"event\":\"ready\",\"runtime\":\"{}\",\"role\":\"{}\",\"listen\":\"{}\",\
         \"actual\":\"{}\",\"tag\":\"{}\"}}",
        runtime.name(),
        role.name(),
        listen,
        actual,
        esc_(tag),
    );
    let _ = std::io::stdout().flush();
}

/// 打印结果并给出退出码（三个运行时壳共用）。
pub fn finish(result: Result<PeerResult, Box<dyn std::error::Error>>) -> ExitCode {
    match result {
        Ok(res) => {
            res.print_console();
            println!("{}", res.to_json());
            if res.ok {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(err) => {
            eprintln!("运行失败：{err}");
            ExitCode::FAILURE
        }
    }
}

/// 由参数与两个阶段的结论拼出 [`PeerResult`]。
pub fn assemble(
    opts: &PeerOptions,
    listen: SocketAddr,
    phases: Vec<PhaseReport>,
    metrics: MetricsDelta,
    total_elapsed_ns: u64,
) -> PeerResult {
    let mut failures: Vec<String> = Vec::new();
    if phases.len() != 2usize {
        failures.push(format!("阶段数不是 2（实际 {}）", phases.len()));
    }
    // 两个阶段必须一个当主动端、一个当被动端——这是本实验对「角色交替」的直接核对。
    let initiator = phases.iter().filter(|p| p.role == "initiator").count();
    let accepter = phases.iter().filter(|p| p.role == "accepter").count();
    if initiator != 1usize || accepter != 1usize {
        failures.push(format!(
            "角色交替不成立：initiator={initiator}，accepter={accepter}"
        ));
    }
    for phase in &phases {
        if !phase.is_clean() {
            failures.push(format!("阶段 {} 未通过判据", phase.role));
        }
    }
    if metrics.frame_errors > 0u64 {
        failures.push(format!("出现 {} 次帧 / 协议错误", metrics.frame_errors));
        if let Some(note) = crate::metrics::DemoMetrics::global().frame_error_note() {
            failures.push(format!("第一条帧 / 协议错误详情：{note}"));
        }
    }
    PeerResult {
        runtime: opts.runtime,
        role: opts.role,
        listen,
        peer: opts.peer,
        tag: opts.tag.clone(),
        ok: failures.is_empty(),
        phases,
        metrics,
        total_elapsed_ns,
        failures,
    }
}

/// 每秒 MiB（`bytes` 除以纳秒耗时）。
fn mib_per_s_(bytes: u64, elapsed_ns: u64) -> f64 {
    if elapsed_ns == 0u64 {
        return 0.0f64;
    }
    (bytes as f64) / (elapsed_ns as f64 / 1_000_000_000.0) / (1024.0f64 * 1024.0f64)
}

/// JSON 字符串转义（只处理必须处理的那几个字符）。
fn esc_(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20u32 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// 编码一个字符串数组。
fn str_array_json_(items: &[String]) -> String {
    let encoded: Vec<String> = items.iter().map(|s| format!("\"{}\"", esc_(s))).collect();
    format!("[{}]", encoded.join(","))
}

/// 编码一个阶段的结论。
fn phase_json_(p: &PhaseReport) -> String {
    format!(
        "{{\"role\":\"{}\",\"streams\":{},\"expected_per_stream\":{},\"sent_bytes\":{},\
         \"recv_bytes\":{},\"sent_acked\":{},\"recv_verified\":{},\"failed_channels\":{},\
         \"mix_telegraph\":{},\"tg_frames\":{},\"tg_bytes\":{},\"tg_sent_acked\":{},\
         \"tg_recv_verified\":{},\
         \"elapsed_ms\":{:.3},\"mib_per_s\":{:.3},\"metrics\":{},\"failures\":{}}}",
        p.role,
        p.streams,
        p.expected_per_stream,
        p.sent_bytes,
        p.recv_bytes,
        p.sent_acked,
        p.recv_verified,
        p.failed_channels,
        p.mix_telegraph,
        p.tg_frames,
        p.tg_bytes,
        p.tg_sent_acked,
        p.tg_recv_verified,
        p.elapsed_ns as f64 / 1_000_000.0,
        mib_per_s_(p.sent_bytes, p.elapsed_ns),
        metrics_json_(&p.metrics),
        str_array_json_(&p.failures),
    )
}

/// 编码一份指标增量。
fn metrics_json_(m: &MetricsDelta) -> String {
    let buckets: Vec<String> = m
        .channel_lifetime_buckets
        .iter()
        .map(|v| v.to_string())
        .collect();
    format!(
        "{{\"conns_opened\":{},\"conns_closed\":{},\"channels_opened\":{},\"channels_closed\":{},\
         \"frames_sent\":{},\"frames_recv\":{},\"data_frames_sent\":{},\"data_frames_recv\":{},\
         \"window_updates_sent\":{},\"window_updates_recv\":{},\"pulse_sent\":{},\"pulse_recv\":{},\
         \"close_sent\":{},\"close_recv\":{},\"frame_bytes_sent\":{},\"frame_bytes_recv\":{},\
         \"transport_bytes_sent\":{},\"transport_bytes_recv\":{},\"resets_by_local\":{},\
         \"resets_by_peer\":{},\"flow_stalled\":{},\"flow_stall_ms\":{},\"frame_errors\":{},\
         \"channel_lifetime_sum_ms\":{},\"channel_lifetime_max_ms\":{},\"lifetime_buckets\":[{}]}}",
        m.conns_opened,
        m.conns_closed,
        m.channels_opened,
        m.channels_closed,
        m.frames_sent,
        m.frames_recv,
        m.data_frames_sent,
        m.data_frames_recv,
        m.window_updates_sent,
        m.window_updates_recv,
        m.pulse_sent,
        m.pulse_recv,
        m.close_sent,
        m.close_recv,
        m.frame_bytes_sent,
        m.frame_bytes_recv,
        m.transport_bytes_sent,
        m.transport_bytes_recv,
        m.resets_by_local,
        m.resets_by_peer,
        m.flow_stalled,
        m.flow_stall_ms,
        m.frame_errors,
        m.channel_lifetime_sum_ms,
        m.channel_lifetime_max_ms,
        buckets.join(","),
    )
}
