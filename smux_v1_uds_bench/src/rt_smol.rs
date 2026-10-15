//! **smol** 装配：与另两个壳同构，差别在「谁来驱动本地队列」。
//!
//! # smol 没有隐式运行时上下文
//!
//! 本地队列在本线程的 `thread_local!` 里（`Rc<LocalExecutor<'static>>`，
//! 见 `abs_art-smol` 的 `local_scope.rs`），必须由调用方驱动。因此：
//!
//! - 入口是**同步**函数 [`run_blocking`]，整段实验挂在
//!   `smol::block_on(scope.run_until(..))` 上；
//! - 运行时值与作用域在**进入 `block_on` 之前**取好（与 `smux_v1/tests/smoke_smol.rs`
//!   的写法一致）；
//! - 写侧适配器用 `BuffWrite::try_new_local`——它把泵投进本线程的本地队列，
//!   由外层的 `run_until` 顺带推进。
//!
//! # 读写半边
//!
//! `smol::net::UnixStream` 就是 `async_net::unix::UnixStream`，它没有 `into_split`；
//! 用 `smol::io::split` 拆（内部是一把短临界区的锁，两个半边仍指向同一条全双工连接）。

use std::process::ExitCode;

use buffex_smol_adapt::DefaultAllocConfig;
use async_net::unix::{UnixListener, UnixStream};
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
    CaseEnv, Drive, Cfg, ChanRx, ChanTx, K_DRAIN_TICKS, Rt, Rx, Scope, Tx, cli_, transport::RingPrepare,
};

/// 客户端子流的**本地 dock 基址**（理由见 [`crate::rt_tokio`] 的同名常量）。
const K_BASE_DOCK: u32 = 0x2001u32;

/// 服务端监听的 dock。
const K_SERVICE_DOCK: u32 = 1u32;

/// smol 装配的 [`CaseEnv`]。
#[derive(Clone)]
pub struct SmolEnv {
    /// 运行时值（建连接时进配置）。
    rt_: Rt,
    /// 本地作用域（连接内部循环的投递点）。
    scope_: Scope,
}

impl SmolEnv {
    /// 取当前运行时值与作用域。
    pub fn current() -> Self {
        let rt: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
        let scope = rt.local_scope();
        SmolEnv { rt_: rt, scope_: scope }
    }
}

impl CaseEnv for SmolEnv {
    type RawListener = UnixListener;

    type SmuxClient = SmolSmuxClient;

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
        let _ = std::fs::remove_file(addr);
        UnixListener::bind(addr).map_err(|err| err.to_string())
    }

    async fn raw_accept(
        listener: Self::RawListener,
        ring_cap: usize,
    ) -> Result<(Self::RawListener, Rx, Tx), String> {
        let (stream, _peer) = listener.accept().await.map_err(|err| err.to_string())?;
        let (rx, tx) = adapt_smol_(stream, ring_cap)?;
        Ok((listener, rx, tx))
    }

    async fn raw_connect(addr: &'static str, ring_cap: usize) -> Result<(Rx, Tx), String> {
        let stream = connect_retry(addr).await?;
        adapt_smol_(stream, ring_cap)
    }

    async fn smux_connect<'a>(
        &'a self,
        addr: &'a str,
        ring_cap: usize,
    ) -> Result<Self::SmuxClient, String> {
        let stream = connect_retry(addr).await?;
        let rt = self.rt_.clone();
        let scope = self.scope_.clone();
        scope
            .run_until(async move {
                let (rx, tx) = adapt_smol_(stream, ring_cap)?;
                let opts = BasicOpts::default();
                let delivery = HandshakeAgent::new(tx, rx)
                    .invite_async(&opts, AcceptAllEntries)
                    .await
                    .map_err(|err| format!("握手失败：{err}"))?;
                let conn = MuxConnection::new(delivery, crate::cfg::BenchCfg::new(rt));
                Ok(SmolSmuxClient {
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
        let local = Dock::new(K_BASE_DOCK + idx as u32);
        let binding = client.conn
            .bind_async(local)
            .await
            .map_err(|err| format!("绑定子流 dock 失败：{err}"))?;
        // binding 必须活到本子流用完（「本地 dock 已被绑定」这个事实要在注册表里成立）。
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

    async fn smux_accept<'a>(
        &'a self,
        listener: &'a mut ChannelListener<Cfg>,
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

    async fn smux_listen<'a>(
        &'a self,
        addr: &'a str,
        ring_cap: usize,
    ) -> Result<ChannelListener<Cfg>, String> {
        let _ = std::fs::remove_file(addr);
        let listener = UnixListener::bind(addr).map_err(|err| err.to_string())?;
        let (stream, _peer) = listener.accept().await.map_err(|err| err.to_string())?;
        let rt = self.rt_.clone();
        let scope = self.scope_.clone();
        scope
            .run_until(async move {
                let (rx, tx) = adapt_smol_(stream, ring_cap)?;
                let opts = BasicOpts::default();
                let delivery = HandshakeAgent::new(tx, rx)
                    .listen_async(&opts, AcceptAllEntries)
                    .await
                    .map_err(|err| format!("握手失败：{err}"))?;
                let conn = MuxConnection::new(delivery, crate::cfg::BenchCfg::new(rt));
                let mut binding = conn
                    .bind_async(Dock::new(K_SERVICE_DOCK))
                    .await
                    .map_err(|err| format!("绑定监听 dock 失败：{err}"))?;
                let listener = binding
                    .listen_async_default()
                    .await
                    .map_err(|err| format!("开始监听失败：{err}"))?;
                // 连接与 binding 的智能指针各被 listener 持有一份，因此这里可以就地丢弃。
                drop((conn, binding));
                Ok(listener)
            })
            .await
    }

    async fn settle(&self) {
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
        // smol 的本地队列是本线程 `thread_local!` 里的执行器，由外层 `run_until` 驱动。
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

/// smol 装配下的 smux 客户端：一条复用连接 + 各子流的 dock 绑定。
pub struct SmolSmuxClient {
    /// 复用连接的智能指针句柄。
    pub conn: MuxConnection<Cfg>,
    /// 各子流 local dock 的绑定；随客户端一起活到用例结束。
    pub bindings: Vec<DockBinding<Cfg>>,
}

/// 带重试地拨号（服务端可能还没 bind）。
async fn connect_retry(addr: &str) -> Result<UnixStream, String> {
    let mut last = String::new();
    for _ in 0..100usize {
        match UnixStream::connect(addr).await {
            Ok(stream) => return Ok(stream),
            Err(err) => {
                last = err.to_string();
                smol::Timer::after(std::time::Duration::from_millis(20)).await;
            }
        }
    }
    Err(format!("拨号 {addr} 重试 100 次仍失败：{last}"))
}

/// 把一条 UDS 流拆成两条适配器半边。
///
/// 写侧走**自驱动**形态（`try_new_local`）：本 bench 的每一格用例都活在
/// [`crate::CaseEnv::with_scope`] 给的一个显式作用域里，自驱动泵的生命周期因此与用例
/// 同寿——这正是它需要的条件。
fn adapt_smol_(stream: UnixStream, ring_cap: usize) -> Result<(Rx, Tx), String> {
    let (rd, wr) = smol::io::split(stream);
    let rx = Rx::try_new(rd, ring_cap, DefaultAllocConfig).map_err(|err| err.to_string())?;
    let tx = Tx::try_new_local(wr, ring_cap, DefaultAllocConfig).map_err(|err| err.to_string())?;
    Ok((rx, tx))
}

/// bin 的入口（**同步**：`block_on` 自己就是驱动源）。
pub fn run_blocking() -> ExitCode {
    let plan = match cli_::parse_env_args() {
        cli_::Parsed::Plan(plan) => plan,
        cli_::Parsed::Exit(code) => return code,
    };
    let runtime = cli_::K_COMPILED_RUNTIME;
    // 自检要在后端上下文里做，因此借 `Drive::drive` 跑一遍。
    let checked = <crate::DriveHere as Drive>::drive(async {
        let rt: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
        crate::assert_runtime(&rt, runtime)?;
        crate::assert_runtime(&rt, plan.runtime)?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let result = match checked {
        Ok(Ok(())) => <crate::DriveHere as Drive>::drive(
            crate::bench_run::run_matrix::<SmolEnv, crate::DriveHere>(&plan, runtime.name()),
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
        // **不再建运行时**：smol 的本地队列是本线程 `thread_local!` 里的执行器，
        // 由 `drive`（`block_on`）驱动；这里只把用例体交给 `run_until`。
        let rt_value: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
        let scope = rt_value.local_scope();
        Ok(scope.run_until(body(&rt_value, &scope)).await)
    }

    fn drive<F>(future: F) -> Result<F::Output, Box<dyn std::error::Error + Send + Sync>>
    where
        F: core::future::Future,
    {
        // 每个跑器线程自己的本地队列；`block_on` 期间 `run_until` 把队列一起推进。
        let rt: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
        let scope = rt.local_scope();
        Ok(smol::block_on(scope.run_until(future)))
    }

    async fn run_one<F>(future: F) -> F::Output
    where
        F: core::future::Future + 'static,
    {
        // 汇总层已在 `drive` 的 `block_on` 里，这里用本线程的 `run_until` 驱动用例。
        let rt: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
        let scope = rt.local_scope();
        scope.run_until(future).await
    }
}

#[cfg(test)]
mod tests {
    use crate::Drive;

    /// 测试 smol 壳的 [`Drive::drive`] 能把一个 future 跑到完成并取回输出。
    /// - 手段：用 `DriveHere::drive` 跑一个 `async { 21 * 2 }`。
    /// - 判断：返回 `Ok(42)`。
    ///
    /// Checks the smol shell can drive a future to completion.
    /// - Method: run `async { 21 * 2 }` through `DriveHere::drive`.
    /// - Assertion: it yields `Ok(42)`.
    #[test]
    fn drive_runs_future_to_completion_() {
        let got = <crate::DriveHere as Drive>::drive(async { 21u32 * 2u32 });
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
        assert_eq!(tag, smux_v1::x_deps::RuntimeTag::Smol);
    }
}
