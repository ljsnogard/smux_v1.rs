//! **tokio** 装配：真实 UDS socket + `buffex_tokio_adapt` 的读写适配器。
//!
//! # 这一层只做后端相关的四件事
//!
//! 1. 建 UDS 监听 / 拨号（`tokio::net::UnixListener` / `UnixStream`）；
//! 2. 造两条适配器半边——**写侧走自驱动形态**（`try_new_local`，它把「环 → 设备」的
//!    泵挂在本线程的本地队列上，因此 smux 的复用循环把帧提交进环就一定会上网）；
//! 3. 建 smux 连接（握手 + `MuxConnection` + 绑 dock）；
//! 4. 收尾：给本地队列若干 tick，把环里残余的帧搬上网。
//!
//! 协议、载荷、统计全在库的其余模块里，与运行时无关。
//!
//! # 为什么可以直接跑默认的 multi-thread
//!
//! 本地队列是**线程本地**的 `LocalSet`（`abs_art-tokio` 的实现）。本 bench 不使用
//! 全局 `spawn`，所有并发都用 `join!` / `join_all` 表达，因此主 future 始终在当前线程
//! 被 poll，不会被搬到别的 worker 线程上；worker 线程池只承担运行时自身的调度。

use std::process::ExitCode;

use tokio::net::{UnixListener, UnixStream};

use buffex_tokio_adapt::DefaultAllocConfig;
use smux_v1::{
    connection::{ChannelListener, Dock, DockBinding, MuxConnection},
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
    x_deps::{
        abs_art::TrLocalScope,
        abs_smux::{
            chan::TrChannelHandle,
            conn::{TrChannelListener, TrConnection, TrDockBinding},
        },
    },
};

use crate::{
    CaseEnv, Drive, Cfg, ChanRx, ChanTx, K_DRAIN_TICKS, Rt, Rx, Scope, Tx, cli_,
    transport::RingPrepare,
};

/// 客户端子流的**本地 dock 基址**。
///
/// smux 用「本地 dock + 对端 dock」这一对当子流身份，N 条子流必须各有各的本地 dock。
/// 取一个不与保留值相撞的基址（保留值与 `smux_v1` 的 `Dock` 约定一致，见 `socks_demo`
/// 里 1 / 2 / 3 的用法），往上依次分配。
const K_BASE_DOCK: u32 = 0x2001u32;

/// 服务端监听的 dock。
const K_SERVICE_DOCK: u32 = 1u32;

/// tokio 装配的 [`CaseEnv`]。
#[derive(Clone)]
pub struct TokioEnv {
    /// 运行时值（建连接时进配置）。
    rt_: Rt,
    /// 本地作用域（连接内部循环的投递点）。
    scope_: Scope,
}

impl TokioEnv {
    /// 取当前运行时值与作用域。
    ///
    /// 必须在 tokio 运行时上下文内调用（`local_scope` 取的是本线程的 `LocalSet`）。
    pub fn current() -> Self {
        let rt: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
        let scope = rt.local_scope();
        TokioEnv { rt_: rt, scope_: scope }
    }
}

impl CaseEnv for TokioEnv {
    type RawListener = UnixListener;

    type SmuxClient = TokioSmuxClient;

    fn current_thread() -> Self {
        // `Self::current()` 读的就是本线程的运行时上下文（作用域与身份都由它给出）。
        Self::current()
    }

    fn rt(&self) -> Rt {
        self.rt_.clone()
    }

    fn scope(&self) -> &Scope {
        &self.scope_
    }

    async fn raw_listen(addr: &str) -> Result<Self::RawListener, String> {
        // 上一轮遗留的同名 socket 文件会让 bind 失败（`EADDRINUSE`）。
        crate::trace_!("tokio：bind {addr}");
        let _ = std::fs::remove_file(addr);
        UnixListener::bind(addr).map_err(|err| err.to_string())
    }

    async fn raw_accept(
        listener: Self::RawListener,
        ring_cap: usize,
    ) -> Result<(Self::RawListener, Rx, Tx), String> {
        crate::trace_!("tokio：accept 等待拨号");
        let (stream, _peer) = listener.accept().await.map_err(|err| err.to_string())?;
        crate::trace_!("tokio：已 accept，构造适配器");
        // 适配器构造必须在本地驱动栈内（写侧要 `spawn_local` 泵），因此整段放进
        // `run_until`；它对本后端是「驱动本线程 `LocalSet` 直到 future 完成」。
        let scope = runtime_scope_();
        let (rx, tx) = scope
            .run_until(async move { adapt_tokio_(stream, ring_cap) })
            .await?;
        Ok((listener, rx, tx))
    }

    async fn raw_connect(addr: &'static str, ring_cap: usize) -> Result<(Rx, Tx), String> {
        let stream = connect_retry(addr).await?;
        let scope = runtime_scope_();
        scope
            .run_until(async move { adapt_tokio_(stream, ring_cap) })
            .await
    }

    async fn smux_connect<'a>(
        &'a self,
        addr: &'a str,
        ring_cap: usize,
    ) -> Result<Self::SmuxClient, String> {
        crate::trace_!("tokio：客户端开始拨号 {addr}");
        let stream = connect_retry(addr).await?;
                // 这段跑在 `WithScope::with_scope` 给的作用域里，因此适配器与连接循环
        // 都挂在本格自己的运行时上下文上。
        let scope = runtime_scope_();
        scope
            .run_until(async move {
                let (rx, tx) = adapt_tokio_with_(stream, ring_cap, true)?;
                                let opts = BasicOpts::default();
                let delivery = HandshakeAgent::new(tx, rx)
                    .invite_async(&opts, AcceptAllEntries)
                    .await
                    .map_err(|err| format!("握手失败：{err}"))?;
                                let rt_value: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
                let conn = MuxConnection::new(delivery, crate::cfg::BenchCfg::new(rt_value));
                                Ok(TokioSmuxClient {
                    conn,
                    bindings: Vec::new(),
                })
            })
            .await
    }

    async fn smux_open<'a>(
        &'a self,
        client: &'a mut Self::SmuxClient,
        idx: usize,
        ring_cap: usize,
    ) -> Result<(ChanRx, ChanTx), String> {
        let scope = runtime_scope_();
        scope
            .run_until(async move { open_channel_(client, idx, ring_cap).await })
            .await
    }

    async fn smux_accept<'a>(
        &'a self,
        listener: &'a mut ChannelListener<Cfg>,
        ring_cap: usize,
    ) -> Result<(ChanRx, ChanTx), String> {
        let scope = runtime_scope_();
        scope
            .run_until(async move { accept_channel_(listener, ring_cap).await })
            .await
    }

    async fn smux_listen<'a>(
        &'a self,
        addr: &'a str,
        ring_cap: usize,
    ) -> Result<ChannelListener<Cfg>, String> {
        let _ = std::fs::remove_file(addr);
        crate::trace_!("tokio：服务端 bind {addr}");
        let listener = UnixListener::bind(addr).map_err(|err| err.to_string())?;
        crate::trace_!("tokio：服务端 accept 等待拨号");
        let (stream, _peer) = listener.accept().await.map_err(|err| err.to_string())?;
        crate::trace_!("tokio：服务端已 accept，开始握手");
        let scope = runtime_scope_();
        scope
            .run_until(async move {
                let (rx, tx) = adapt_tokio_with_(stream, ring_cap, true)?;
                                let opts = BasicOpts::default();
                let delivery = HandshakeAgent::new(tx, rx)
                    .listen_async(&opts, AcceptAllEntries)
                    .await
                    .map_err(|err| format!("握手失败：{err}"))?;
                                let rt_value: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
                let conn = MuxConnection::new(delivery, crate::cfg::BenchCfg::new(rt_value));
                let mut binding = conn
                    .bind_async(Dock::new(K_SERVICE_DOCK))
                    .await
                    .map_err(|err| format!("绑定监听 dock 失败：{err}"))?;
                let listener = binding
                    .listen_async_default()
                    .await
                    .map_err(|err| format!("开始监听失败：{err}"))?;
                // 连接与 binding 由 listener 内部各持一份智能指针，因此可以随
                // `listener` 一起交出去；`conn` 就地丢弃不影响它们（核心是共享的）。
                drop((conn, binding));
                Ok(listener)
            })
            .await
    }

    async fn settle(&self) {
        // 给本地队列若干 tick：smux 的复用循环是投递在它上面的任务，收尾（FIN 与
        // 尾部数据帧）要靠它们搬进适配器的环，再由自驱动泵上网。
        crate::drain_scope(&self.scope_, K_DRAIN_TICKS).await;
    }

    fn spawn<F>(
        &self,
        future: F,
    ) -> impl core::future::Future<Output = Result<F::Output, Box<dyn std::error::Error + Send + Sync>>>
    where
        F: core::future::Future + 'static,
        F::Output: 'static,
    {
        // 投到本线程的 `LocalSet`（tokio 的 `spawn` 要求 `Send`，这里用不了）。
        let handle = self.scope_.spawn_local(future);
        async move {
            // `abs_art` 的句柄本身就是一个 `Future<Output = Result<T, JoinErr>>`，
            // 三个后端的错误类型不同，这里统一折成 boxed error。
            handle
                .await
                .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> { Box::new(err) })
        }
    }
}

/// tokio 装配下的 smux 客户端：一条复用连接 + 各子流的 dock 绑定。
///
/// 两个字段都是 `pub`（本类型是 [`CaseEnv`] 的关联类型，必须可被库侧使用），
/// 因此按项目约定不带下划线后缀。
pub struct TokioSmuxClient {
    /// 复用连接的智能指针句柄。
    pub conn: MuxConnection<Cfg>,
    /// 各子流 local dock 的绑定；随客户端一起活到用例结束。
    pub bindings: Vec<DockBinding<Cfg>>,
}

/// 发起一条子流并裁决（在本地驱动栈内跑）。
async fn open_channel_(
    client: &mut TokioSmuxClient,
    idx: usize,
    ring_cap: usize,
) -> Result<(ChanRx, ChanTx), String> {
    // 每条子流一个互不相同的临时 local dock（dock 对即身份）。
    let local = Dock::new(K_BASE_DOCK + idx as u32);
    let binding = client
        .conn
        .bind_async(local)
        .await
        .map_err(|err| format!("绑定子流 dock 失败：{err}"))?;
    // binding 必须活到本子流用完（「本地 dock 被绑定」这个事实要在注册表里成立）。
    client.bindings.push(binding);
    let binding = client
        .bindings
        .last_mut()
        .ok_or_else(|| "子流 binding 集合为空（内部错误）".to_string())?;
    let mut invitation: &[u8] = &[];
    let mut handle = binding
        .open_channel_async(Dock::new(K_SERVICE_DOCK), &mut invitation)
        .await
        .map_err(|err| format!("发起子流失败：{err}"))?;
    if let Some(reason) = handle.abort_reason() {
        return Err(format!("子流建流即被中止：{reason:?}"));
    }
    let mut empty: &mut [u8] = &mut [];
    let (tx, rx): (ChanTx, ChanRx) = handle
        .accept_async(&mut empty, RingPrepare::new(ring_cap))
        .await
        .map_err(|err| format!("裁决子流失败：{err}"))?;
    Ok((rx, tx))
}

/// 收一条入向子流并裁决（在本地驱动栈内跑）。
async fn accept_channel_(
    listener: &mut ChannelListener<Cfg>,
    ring_cap: usize,
) -> Result<(ChanRx, ChanTx), String> {
    let mut handle = listener
        .income_async()
        .await
        .map_err(|err| format!("等待入向子流失败：{err}"))?;
    if let Some(reason) = handle.abort_reason() {
        return Err(format!("入向子流建流即被中止：{reason:?}"));
    }
    let mut empty: &mut [u8] = &mut [];
    let (tx, rx): (ChanTx, ChanRx) = handle
        .accept_async(&mut empty, RingPrepare::new(ring_cap))
        .await
        .map_err(|err| format!("裁决入向子流失败：{err}"))?;
    Ok((rx, tx))
}

/// 取本线程的本地作用域（关联函数里没有 `self`，因此直接向运行时值要）。
fn runtime_scope_() -> Scope {
    let rt: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
    rt.local_scope()
}

/// 带重试地拨号（服务端可能还没 bind）。
async fn connect_retry(addr: &str) -> Result<UnixStream, String> {
    let mut last = String::new();
    for _ in 0..100usize {
        match UnixStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(err) => {
                last = err.to_string();
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
    Err(format!("拨号 {addr} 重试 100 次仍失败：{last}"))
}

/// 把一条 UDS 连接拆成两条适配器半边（外面套 `run_until` 的调用点见各方）。
///
/// `self_driving` 选写侧形态：
///
/// - `true`：`try_new_local`——构造时把「环 → 设备」的泵投到本线程 `LocalSet` 上。
///   适合**长期驱动**的场景（`smux_v1_socks_demo` 把整段场景放进一个 `run_until`）；
/// - `false`：`try_new`——按需驱动，由调用方的 `write_async` / `flush_async` 顺带推。
///   本 bench 的 smux 用例**跨多个 `run_until`**（连接建立、开子流、数据面各一段），
///   自驱动泵的生命周期与那些段的边界不一致，因此这里走按需形态。
fn adapt_tokio_(stream: UnixStream, ring_cap: usize) -> Result<(Rx, Tx), String> {
    adapt_tokio_with_(stream, ring_cap, true)
}

/// 与 [`adapt_tokio_`] 同体，由 `self_driving` 选择写侧形态。
fn adapt_tokio_with_(
    stream: UnixStream,
    ring_cap: usize,
    self_driving: bool,
) -> Result<(Rx, Tx), String> {
    let (rd, wr) = stream.into_split();
    let rx = Rx::try_new(rd, ring_cap, DefaultAllocConfig).map_err(|err| err.to_string())?;
    // 两个构造入口的错误类型不同，因此各自 `map_err` 之后再统一成 `Result<Tx, String>`。
    let tx: Tx = if self_driving {
        Tx::try_new_local(wr, ring_cap, DefaultAllocConfig).map_err(|err| err.to_string())?
    } else {
        Tx::try_new(wr, ring_cap, DefaultAllocConfig).map_err(|err| err.to_string())?
    };
    Ok((rx, tx))
}

/// bin 的入口（**同步**）。
///
/// # 外层为什么不起运行时
///
/// 入口是同步的：外壳只做参数解析与结果汇总，运行时按需创建。
///
/// 汇总层用一个 current_thread 运行时 + `LocalSet`（用例是 `spawn_local` 投出来的）；
/// 自驱动写泵要的「本地队列持续被驱动」由每格用例自己的运行时保证（见
/// `WithScope` 的实现）。
pub fn run_blocking() -> ExitCode {
    let plan = match cli_::parse_env_args() {
        cli_::Parsed::Plan(plan) => plan,
        cli_::Parsed::Exit(code) => return code,
    };
    let runtime = cli_::K_COMPILED_RUNTIME;
    // 自检要在后端上下文里做，因此借 `Drive::drive` 跑一遍。
    let checked = crate::DriveHere::drive(async {
        let rt: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
        crate::assert_runtime(&rt, runtime)?;
        crate::assert_runtime(&rt, plan.runtime)?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let result = match checked {
        Ok(Ok(())) => <crate::DriveHere as Drive>::drive(
            crate::bench_run::run_matrix::<TokioEnv, crate::DriveHere>(&plan, runtime.name()),
        ),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(err),
    };
    match result.and_then(|inner| inner) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("运行失败：{err}");
            ExitCode::FAILURE
        }
    }

}

impl Drive for crate::DriveHere {
    async fn with_scope<F, R>(
        body: F,
    ) -> Result<R, Box<dyn std::error::Error + Send + Sync>>
    where
        F: for<'s> FnOnce(
            &'s Rt,
            &'s Scope,
        )
            -> core::pin::Pin<Box<dyn core::future::Future<Output = R> + 's>>,
        R: 'static,
    {
        // **不再建运行时**：当前线程已经由 `drive` 建好了运行时与 `LocalSet`，
        // 这里只把用例体交给本线程的 `run_until`（它同时驱动本地队列与用例）。
        let rt_value: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
        let scope = rt_value.local_scope();
        Ok(scope.run_until(body(&rt_value, &scope)).await)
    }

    fn drive<F>(future: F) -> Result<F::Output, Box<dyn std::error::Error + Send + Sync>>
    where
        F: core::future::Future,
    {
        // 每个跑器线程一个 current_thread 运行时 + 一个 `LocalSet`：连接的循环与自驱动
        // 写泵都投在这个 `LocalSet` 上，由它一直驱动到用例完成。
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let local = tokio::task::LocalSet::new();
        Ok(local.block_on(&rt, future))
    }

    async fn run_one<F>(future: F) -> F::Output
    where
        F: core::future::Future + 'static,
    {
        // 汇总层已经在 `drive` 建的运行时里，这里只把用例投到本线程的 `LocalSet` 上，
        // 由它一直驱动到用例完成（连接循环与自驱动写泵都在它上面）。
        let local = tokio::task::LocalSet::new();
        local.run_until(future).await
    }
}

#[cfg(test)]
mod tests {
    use crate::Drive;

    /// 测试 tokio 壳的 [`Drive::drive`] 能把一个 future 跑到完成并取回输出。
    /// - 手段：用 `DriveHere::drive` 跑一个 `async { 6 * 7 }`。
    /// - 判断：返回 `Ok(42)`。
    ///
    /// Checks the tokio shell can drive a future to completion.
    /// - Method: run `async { 6 * 7 }` through `DriveHere::drive`.
    /// - Assertion: it yields `Ok(42)`.
    #[test]
    fn drive_runs_future_to_completion_() {
        let got = <crate::DriveHere as Drive>::drive(async { 6u32 * 7u32 });
        assert_eq!(got.expect("驱动应当成功"), 42u32);
    }

    /// 测试 [`Drive::with_scope`] 在 [`Drive::drive`] 建好的运行时上下文里给出作用域与
    /// 运行时值。
    /// - 手段：把 `with_scope` **套在** `drive` 里跑（`with_scope` 自身不建运行时，
    ///   它依赖调用方已经在后端上下文内），闭包里取一次 `rt.about()`。
    /// - 判断：返回的后端标识与本壳一致。
    ///
    /// Checks `with_scope` receives the runtime value inside the driven context.
    /// - Method: nest `with_scope` inside `drive` and read `rt.about()` in the closure.
    /// - Assertion: the reported backend matches this shell.
    #[test]
    fn with_scope_exposes_the_runtime_() {
        let got = <crate::DriveHere as Drive>::drive(
            <crate::DriveHere as Drive>::with_scope(|rt, _scope| {
                Box::pin(async move {
                    smux_v1::x_deps::abs_art::TrAsyncRuntime::about(rt)
                })
            }),
        );
        let tag = got
            .expect("驱动应当成功")
            .expect("应当给出运行时值");
        assert_eq!(tag, smux_v1::x_deps::RuntimeTag::Tokio);
    }
}
