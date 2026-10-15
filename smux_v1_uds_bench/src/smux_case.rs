//! **被测组**：一条真实 UNIX domain socket 上开 N 条 `smux_v1` 复用子流。
//!
//! ```text
//! 客户端任务 0 ─┐
//! 客户端任务 1 ─┤──[1 条真实 UDS + 握手 + 复用]──▶ 服务端子流 0..N
//!        …      │
//! 客户端任务 N ─┘
//! ```
//!
//! 与 [`crate::raw_case`] 的差别**只有传输形态**：载荷、头部、分块、环容量、并发度、
//! 计时口径全部逐字相同（都调 [`crate::case_io`] 的同一批函数）。
//!
//! # 两端各自建一条连接，是同一条吗
//!
//! 不是——这与 `smux_v1_socks_demo` 的单进程装配不同。本 bench 的**服务端也用一份
//! socket**：`smux_listen` 自己 bind / accept 一条 UDS，客户端用 `smux_connect` 拨号
//! 到那个路径。两端因此是**真正经 UDS 传输**的（而不是内存里对折的一条连接），
//! 这正是「用 smux 的复用 channel 代替真实 unix domain stream」要测的东西。
//!
//! # 顺序为什么不能颠倒
//!
//! 两端各自要完成「握手 → 建连接 → 绑 dock」才算就绪：主动侧（客户端）在
//! `smux_connect` 里 invite，被动侧（服务端）在 `smux_listen` 里 listen。两端**并发**
//! 推这件事，谁先谁后由协议自己收敛；先建好一端、再 await 另一端也成立，只是没必要
//! 串起来——服务端先起监听、客户端再去拨号，两者在同一个 `join!` 里推进即可。

use futures::future::join_all;
use smux_v1::x_deps::abs_art::TrLocalScope;

use crate::{CaseEnv, case_io, cli_::CaseSpec, hub::CaseHub, hub::TaskTime, payload};

/// 跑完一格 smux 用例：返回 `(墙钟纳秒, 逐任务时间)`。
///
/// `addr` 是服务端绑定 / 客户端拨号的 UDS 路径。
///
/// # Errors
///
/// 建连接、握手、绑 dock、建子流、读流失败时返回说明（调用方折成本格失败）。
pub async fn run_smux_case<E>(
    env: &E,
    scope: &crate::Scope,
    spec: &CaseSpec,
    addr: &str,
) -> Result<(u64, u64, Vec<TaskTime>), String>
where
    E: CaseEnv,
{
    let payload = payload::payload_of(spec.seed, 0u64, spec.bytes_per_client);
    let hub = CaseHub::new(spec.clients);
    // 服务端在 `async move` 块里跑，因此给它一份句柄克隆（内部是共享计时槽）。
    let hub2 = hub.clone();

    // 服务端这一侧要先把「监听 dock」建起来，客户端的 invite 才有落点。
    crate::trace_!("smux：开始建监听 {addr}");
    // 服务端与客户端必须**并发**推进：服务端在 `smux_listen` 里 accept 一条拨号，
    // 客户端在 `smux_connect` 里拨号 + 握手。任何一侧单独 await 都会与另一侧互等
    // （这正是本模块文档里那条「顺序不能颠倒」的落地）。
    //
    // `async move` 让 `env` 的不可变借用只活到 `smux_listen` 完成；之后服务端循环
    // 只借 `&mut listener`，客户端那侧照常用 `&E`。
    let server = async move {
                let mut listener = scope
            .run_until(env.smux_listen(addr, spec.ring_cap))
            .await
            .map_err(|err| format!("服务端建立复用监听失败：{err}"))?;
        crate::trace_!("smux：监听就绪");
        run_smux_server(env, scope, &mut listener, &hub2, spec).await
    };

    let client = async {
        let mut client = scope
            .run_until(env.smux_connect(addr, spec.ring_cap))
            .await
            .map_err(|err| format!("客户端建立复用连接失败：{err}"))?;
                run_smux_clients(env, scope, &mut client, &hub, spec, &payload).await
    };

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

/// 服务端一侧：收 N 条入向子流 → 每条一个读任务 → 全部读完。
async fn run_smux_server<E>(
    env: &E,
    scope: &crate::Scope,
    listener: &mut smux_v1::connection::ChannelListener<crate::Cfg>,
    hub: &CaseHub,
    spec: &CaseSpec,
) -> Result<(), String>
where
    E: CaseEnv,
{
    let mut halves = Vec::with_capacity(spec.clients);
    for idx in 0..spec.clients {
        crate::trace_!("smux 服务端：等待第 {idx} 条入向子流");
        let (mut rx, tx) = scope
            .run_until(env.smux_accept(listener, spec.ring_cap))
            .await
            .map_err(|err| format!("接收第 {idx} 条入向子流失败：{err}"))?;
                // 服务端不回数据：发送半边当场丢弃（= 对该子流半关闭）。
        drop(tx);
        halves.push(async move {
            let got = case_io::run_recv_task(hub, &mut rx, spec.chunk, spec.verify).await?;
            if got != spec.bytes_per_client {
                return Err(format!(
                    "第 {idx} 条子流收到 {got} 字节，应为 {}",
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

/// 客户端一侧：N 个发送任务**同时出发**（栅栏），各自一条复用子流。
///
/// 子流由**每个任务自己**在栅栏之前开好（`smux_open`），这样「开流」的开销不计入
/// 发送时长；开流失败的先置位失败标记，其余任务仍会被唤醒成失败而不是挂住。
async fn run_smux_clients<E>(
    env: &E,
    scope: &crate::Scope,
    client: &mut E::SmuxClient,
    hub: &CaseHub,
    spec: &CaseSpec,
    payload: &[u8],
) -> Result<(), String>
where
    E: CaseEnv,
{
    let mut halves: Vec<(usize, crate::ChanTx, crate::ChanRx)> = Vec::with_capacity(spec.clients);
    for idx in 0..spec.clients {
        crate::trace_!("smux 客户端：开第 {idx} 条子流");
        match scope
            .run_until(env.smux_open(client, idx, spec.ring_cap))
            .await
        {
            Ok((rx, tx)) => halves.push((idx, tx, rx)),
            Err(note) => {
                let _ = hub.fail_task(idx);
                hub.barrier().abort();
                // 失败计数也要补上，否则墙钟终点落不下来（见 `CaseHub` 的文档）。
                hub.send_finished_one();
                return Err(format!("开第 {idx} 条子流失败：{note}"));
            }
        }
    }

    let tasks = halves.into_iter().map(|(idx, mut tx, rx)| async move {
        // smux 路径上服务端不回数据，读半边当场丢弃。
        drop(rx);
        let outcome = case_io::run_send_task(hub, idx, &mut tx, payload, spec.chunk).await;
        // 收尾：丢弃发送半边 = 对该子流半关闭，服务端由此读到 EOF。
        drop(tx);
        outcome
    });

    let mut failure: Option<String> = None;
    for outcome in join_all(tasks).await {
        if let Err(note) = outcome {
            hub.barrier().abort();
            failure.get_or_insert(note);
        }
    }
    match failure {
        Some(note) => Err(note),
        None => Ok(()),
    }
}
