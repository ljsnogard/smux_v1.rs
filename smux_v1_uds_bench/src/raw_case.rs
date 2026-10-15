//! **基准/对照组**：N 条**真实** UNIX domain socket 连接。
//!
//! ```text
//! 客户端任务 0 ──[真实 UDS 连接 0]──▶ 服务端读任务 0
//! 客户端任务 1 ──[真实 UDS 连接 1]──▶ 服务端读任务 1
//!                        …                        （N 条独立连接，无复用）
//! ```
//!
//! # 为什么它也经适配器
//!
//! 每个客户端本可以直接读写 socket 的 `AsyncRead` / `AsyncWrite`。本 bench 刻意不这么
//! 做：那样「raw 与 smux」的差值会同时包含「环形缓冲 vs 直连设备」与「真 socket vs
//! 复用子流」两项，无法归因。让两边都经 `buffex_*_adapt` 的环，**唯一的差别就只剩
//! 传输形态**——这正是本 bench 想回答的问题。
//!
//! # 服务端先接满再收
//!
//! 服务端把 N 条连接**全部 accept 完**之后才开始读。这不是随手的选择：环默认只有
//! 64 KiB，若「接到一条就开始读」与「客户端在栅栏后一起发」交错，客户端会先被环的
//! 背压挡住——那测的就是另一件事了。接满之后再并行读，N 个读任务因此总是同时在推进，
//! 与 smux 侧的 N 个子流读任务逐字同构。
//!
//! # socket 路径为什么是 `'static`
//!
//! 客户端与服务端**每条连接都是一个独立任务**（用 `join_all` 并发跑），因此
//! [`CaseEnv`] 的 socket 工厂返回的 future 必须 `'static`，它们的入参也只能是
//! `'static` 的字符串。本模块用 [`Box::leak`] 把路径升格到 `'static`：一格的路径只有
//! 几十字节，泄漏它换来的是「三个后端共用同一份用例逻辑」，这个交换是划算的。

use futures::future::join_all;

use crate::{CaseEnv, case_io, cli_::CaseSpec, hub::CaseHub, hub::TaskTime, payload};

/// 跑完一格 raw 用例：返回 `(墙钟纳秒, 服务端收齐纳秒, 逐任务时间)`。
///
/// `addr` 是服务端绑定 / 客户端拨号的 UDS 路径（由运行时壳给出：路径里含进程号与
/// 用例序号，避免多格之间互相踩）。
///
/// # Errors
///
/// 建监听、accept、适配器构造、拨号、读流失败时返回说明（调用方折成本格失败）。
pub async fn run_raw_case<E>(
    spec: &CaseSpec,
    addr: &str,
) -> Result<(u64, u64, Vec<TaskTime>), String>
where
    E: CaseEnv,
{
    let payload = payload::payload_of(spec.seed, 0u64, spec.bytes_per_client);
    let hub = CaseHub::new(spec.clients);
    // 升格到 `'static`：连接与 accept 各自被包成独立任务，入参不能借本函数栈。
    let addr: &'static str = Box::leak(addr.to_string().into_boxed_str());

    let listener = E::raw_listen(addr)
        .await
        .map_err(|err| format!("绑定 UDS 监听 {addr} 失败：{err}"))?;

    // 服务端与客户端**并发**推进：服务端要 accept N 条、客户端要拨号 N 条，
    // 任何一侧单独 await 都会与另一侧互等。listener 按值进服务端任务。
    let server = run_raw_server::<E>(listener, &hub, spec);
    let client = run_raw_clients::<E>(addr, &hub, spec, &payload);
    let (accepted, sent) = futures::join!(server, client);

    // 先把失败**都**收上来：一侧失败时另一侧也可能是失败，报第一个更具体的原因。
    let mut failure = accepted.err();
    if failure.is_none() {
        failure = sent.err();
    }

    let wall_ns = hub
        .wall_ns()
        .ok_or_else(|| "发送侧没有报出墙钟终点（用例提前结束）".to_string())?;
    let drained_ns = hub.drained_ns().unwrap_or(0u64);
    let times = hub.task_times()?;
    match failure {
        Some(note) => Err(note),
        None => Ok((wall_ns, drained_ns, times)),
    }
}

/// 服务端一侧：accept N 条连接 → 每条一个读任务 → 全部读完。
async fn run_raw_server<E>(
    mut listener: E::RawListener,
    hub: &CaseHub,
    spec: &CaseSpec,
) -> Result<(), String>
where
    E: CaseEnv,
{
    let mut halves = Vec::with_capacity(spec.clients);
    for idx in 0..spec.clients {
        // 关联函数：listener 按值进、按值出，因此返回的 future 是 `'static`，
        // 可以被下面的读任务与 `join_all` 一起驱动。
        let (back, mut rx, tx) = E::raw_accept(listener, spec.ring_cap)
            .await
            .map_err(|err| format!("accept 第 {idx} 条连接失败：{err}"))?;
        listener = back;
        // raw 路径上服务端不回数据，写半边当场丢弃（它的析构会放掉对应的一半设备）。
        drop(tx);
        halves.push(async move {
            let got = case_io::run_recv_task(hub, &mut rx, spec.chunk, spec.verify).await?;
            if got != spec.bytes_per_client {
                return Err(format!(
                    "第 {idx} 条连接收到 {got} 字节，应为 {}",
                    spec.bytes_per_client
                ));
            }
            Ok::<(), String>(())
        });
    }
    for outcome in join_all(halves).await {
        outcome?;
    }
    Ok(())
}

/// 客户端一侧：N 个发送任务**同时出发**（栅栏），各自一条连接。
async fn run_raw_clients<E>(
    addr: &'static str,
    hub: &CaseHub,
    spec: &CaseSpec,
    payload: &[u8],
) -> Result<(), String>
where
    E: CaseEnv,
{
    let tasks = (0usize..spec.clients).map(|idx| async move {
        let (rx, mut tx) = E::raw_connect(addr, spec.ring_cap).await.map_err(|err| {
            hub.barrier().abort();
            format!("第 {idx} 个客户端拨号失败：{err}")
        })?;
        // raw 路径上服务端不回数据，读半边当场丢弃。
        drop(rx);
        let outcome = case_io::run_send_task(hub, idx, &mut tx, payload, spec.chunk).await;
        // 收尾：丢弃发送半边 = 半关闭，服务端由此读到 EOF。
        drop(tx);
        outcome
    });
    let mut failure: Option<String> = None;
    for outcome in join_all(tasks).await {
        if let Err(note) = outcome {
            // 一个任务失败就把其余仍在等栅栏的任务唤醒成失败，避免它们永远等下去。
            hub.barrier().abort();
            failure.get_or_insert(note);
        }
    }
    match failure {
        Some(note) => Err(note),
        None => Ok(()),
    }
}
