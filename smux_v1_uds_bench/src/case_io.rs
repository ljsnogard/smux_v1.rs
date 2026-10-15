//! 两条被测路径（raw 与 smux）共用的**用例级动作**。
//!
//! 两条路径的差别只在「传输是什么」：
//!
//! - **raw**：每个客户端一条真实 UNIX domain socket，服务端 `accept` N 条；
//! - **smux**：一条真实 UNIX domain socket，上面开 N 条复用子流。
//!
//! 一旦把传输抽象成 `abs_buff` 的两条半边（`TrBuffWrite` / `TrBuffRead`），
//! 「等栅栏同时出发」「发一份带头部与 CRC 的载荷」「收一份并核对」「记下完成时刻」
//! 这几件事在两条路径上**逐字相同**。本模块是它们的唯一实现，因此不可能出现
//! 「raw 用了一套协议、smux 用了另一套」的偏差——那正是本 bench 最不能出的错。
//!
//! # 时间是怎么记的
//!
//! [`run_send_task`] 的顺序是**取时刻 → 报告发完 → 丢弃发送半边**。时刻必须在丢弃
//! 之前取：丢弃会触发半关闭（收尾动作），不该算进「发送任务完成」。报告发完这一步
//! 也不能漏——墙钟终点靠它落地（见 [`CaseHub::send_finished_one`]）。

use std::time::Instant;

use smux_v1::x_deps::abs_buff::{TrBuffRead, TrBuffWrite};

use crate::{
    barrier::Release,
    hub::CaseHub,
    proto::{K_HEADER_LEN, decode_header, digest_and_chunks, encode_header},
    transport::{read_exact, read_frame_checked, write_all},
};

/// 让一个发送任务在栅栏上等「同时出发」，返回共同起点。
///
/// 先 [`CaseHub::mark_ready`] 再等：这样即使栅栏随后被判定失败，编排层也能从
/// 「已就位数」看出到底有几个任务真的到了。
///
/// # Errors
///
/// 栅栏被判定失败（有任务提前出错）时返回说明。
pub async fn wait_for_start(hub: &CaseHub) -> Result<Instant, String> {
    hub.mark_ready();
    match hub.barrier().wait().await {
        Release::Go => {
            let start = Instant::now();
            hub.set_start(start);
            Ok(start)
        }
        Release::Failed => Err("栅栏被判定失败：有发送任务在出发前就出错了".to_string()),
    }
}

/// 一个发送任务的完整动作：等栅栏 → 发头部 → 发本体 → 记时刻 → 报告发完。
///
/// # Errors
///
/// 栅栏失败、写失败、或记录时刻失败时返回说明。任何失败都会在 [`CaseHub`] 上把该
/// 任务标记为失败（好让本格被判定为失败格），并且**照样报告发完**——否则墙钟终点
/// 永远落不下来，用例会一直等到超时。
pub async fn run_send_task<W>(
    hub: &CaseHub,
    idx: usize,
    tx: &mut W,
    payload: &[u8],
    chunk: usize,
) -> Result<(), String>
where
    W: TrBuffWrite<u8>,
    W::Err: std::error::Error + 'static,
{
    let mut failure: Option<String> = None;
    if let Err(note) = wait_for_start(hub).await {
        failure = Some(note);
    }

    if failure.is_none() {
        let attempt = async {
            let (crc, parts) = digest_and_chunks(payload, chunk);
            let header = encode_header(payload.len() as u64, crc);
            write_all(tx, &header)
                .await
                .map_err(|err| format!("写头部失败：{err}"))?;
            for part in parts {
                write_all(tx, part)
                    .await
                    .map_err(|err| format!("写本体失败：{err}"))?;
            }
            Ok::<(), String>(())
        };
        if let Err(note) = attempt.await {
            failure = Some(note);
        }
    }

    // 时刻必须在丢弃发送半边**之前**取：丢弃会触发半关闭（收尾动作），
    // 它不属于「发送任务完成」。
    let end_ns = hub
        .start()
        .map(|start| start.elapsed().as_nanos() as u64)
        .unwrap_or(0u64);

    if failure.is_none() {
        if let Err(note) = hub.finish_task(idx, end_ns) {
            let _ = hub.fail_task(idx);
            hub.send_finished_one();
            return Err(note);
        }
    } else {
        let _ = hub.fail_task(idx);
    }
    hub.send_finished_one();
    match failure {
        Some(note) => Err(note),
        None => Ok(()),
    }
}

/// 一个服务端读任务的完整动作：收一份带头部与 CRC 的载荷、核对、报告收完。
///
/// # 失败也要报告「收完了」
///
/// 无论成功还是失败，结束时都会调 [`CaseHub::drain_finished_one`]——它是报告里的
/// 参考量（服务端收齐时刻）；漏掉这一步会让这一格读不出排空时刻。
///
/// # Errors
///
/// 读失败、长度或校验和不一致时返回说明（调用方负责把它折成本格失败）。
pub async fn run_recv_task<R>(
    hub: &CaseHub,
    rx: &mut R,
    chunk: usize,
    verify: bool,
) -> Result<u64, String>
where
    R: TrBuffRead<u8>,
    R::Err: std::error::Error + 'static,
{
    let outcome = if verify {
        read_frame_checked(rx, chunk)
            .await
            .map_err(|err| format!("收流失败：{err}"))
    } else {
        read_frame_len_only(rx, chunk)
            .await
            .map_err(|err| format!("收流失败：{err}"))
    };
    hub.drain_finished_one();
    outcome
}

/// 不核对 CRC 的收流：只按头部声明的长度收满。
///
/// `--no-verify` 时用。计时口径与核对版**完全相同**（都是「收满声明的字节数」），
/// 因此两格的墙钟可比——差别只有「收完有没有多算一遍 CRC」。
///
/// # Errors
///
/// 读失败或收不满时返回错误。
pub async fn read_frame_len_only<R>(
    rx: &mut R,
    chunk: usize,
) -> Result<u64, Box<dyn std::error::Error>>
where
    R: TrBuffRead<u8>,
    R::Err: std::error::Error + 'static,
{
    let mut header = [0u8; K_HEADER_LEN];
    read_exact(rx, &mut header).await?;
    let (declared_len, _declared_crc) = decode_header(&header);

    let mut buf = vec![0u8; chunk.max(1usize)];
    let mut got = 0u64;
    while got < declared_len {
        let want = core::cmp::min(declared_len - got, buf.len() as u64) as usize;
        read_exact(rx, &mut buf[..want]).await?;
        got += want as u64;
    }
    Ok(got)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_::Mode;

    /// 测试栅栏失败时 `wait_for_start` 会如实报错（否则用例会永久挂住）。
    /// - 手段：造一个 2 方 [`CaseHub`]、先 `abort` 栅栏，再 `block_on` 等出发。
    /// - 判断：返回 `Err` 且说明里含「栅栏」；同时「先 mark_ready 再等」这条顺序
    ///   是可观测的——即使栅栏已失败，就位数也计上了。
    ///
    /// Checks an aborted barrier is reported instead of hanging.
    /// - Method: abort a two-party hub barrier, then await `wait_for_start`.
    /// - Assertion: it returns an error mentioning the barrier and the ready counter
    ///   still advanced.
    #[test]
    fn wait_for_start_reports_aborted_barrier_() {
        let hub = CaseHub::new(2usize);
        hub.barrier().abort();
        let err = futures::executor::block_on(wait_for_start(&hub)).expect_err("栅栏已失败");
        assert!(err.contains("栅栏"), "错误说明：{err}");
        assert_eq!(hub.ready(), 1usize);
    }

    /// 测试两条路径的用例键互不相同（报告里不会把两格写成同一行）。
    /// - 手段：同一并发度分别造 raw 与 smux 的规格，比较 `key()`。
    /// - 判断：两个键各以自己的路径名开头且互不相等。
    ///
    /// Checks the two paths produce distinct case keys.
    /// - Method: build one raw and one smux spec with the same client count.
    /// - Assertion: the keys differ and start with the mode name.
    #[test]
    fn case_keys_are_distinct_per_mode_() {
        let base = crate::cli_::CaseSpec {
            mode: Mode::Raw,
            clients: 4usize,
            bytes_per_client: 1024u64,
            chunk: 64usize,
            ring_cap: 128usize,
            seed: 1u64,
            verify: true,
        };
        let mut smux = base;
        smux.mode = Mode::Smux;
        assert!(base.key().starts_with("raw-"), "键：{}", base.key());
        assert!(smux.key().starts_with("smux-"), "键：{}", smux.key());
        assert_ne!(base.key(), smux.key());
    }
}
