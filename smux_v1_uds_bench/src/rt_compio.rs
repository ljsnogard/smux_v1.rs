//! **compio** 装配：与 [`crate::rt_tokio`] 逐字同构，只有设备类型与适配 crate 不同。
//!
//! # 一处结构性差别：泵是后台的
//!
//! compio 的 `BuffWrite::try_new` **本来就**用 `compio::runtime::spawn` 挂了后台泵
//! （它的执行体是线程本地的，不要求 `Send`），因此：
//!
//! - 适配器构造不必挤进 `run_until`（compio 的 `run_until` 也只是原样返回 future）；
//! - 写侧「环 → 设备」的搬运由那条泵持续进行，收尾时连接丢弃会让适配器
//!   「关生产端 + detach 泵」，尾巴由泵搬完并 `shutdown` 设备。
//!
//! # 读写半边怎么来
//!
//! compio 的 UDS 流是全双工的**同一个对象**，因此各要一份：读侧用 `stream.clone()`，
//! 写侧用原件（`compio::net::UnixStream: Clone` 共用同一个 fd）。这与 tokio 的
//! `into_split`、smol 的 `smol::io::split` 是同一件事的三种表达。

use std::process::ExitCode;

use compio::net::{UnixListener, UnixStream};

use buffex_compio_adapt::DefaultAllocConfig;
use smux_v1::{
    connection::{ChannelListener, Dock, DockBinding, MuxConnection},
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
    x_deps::{
        abs_art::{TrBlockOn, TrLocalScope},
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

/// 客户端子流的**本地 dock 基址**（理由见 [`crate::rt_tokio`] 的同名常量）。
const K_BASE_DOCK: u32 = 0x2001u32;

/// 服务端监听的 dock。
const K_SERVICE_DOCK: u32 = 1u32;

/// compio 装配的 [`CaseEnv`]。
#[derive(Clone)]
pub struct CompioEnv {
    /// 运行时值（建连接时进配置）。
    rt_: Rt,
    /// 本地作用域（连接内部循环的投递点）。
    scope_: Scope,
}

impl CompioEnv {
    /// 取当前运行时值与作用域（必须在 compio 上下文内调用）。
    pub fn current() -> Self {
        let rt: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
        let scope = rt.local_scope();
        CompioEnv { rt_: rt, scope_: scope }
    }
}

impl CaseEnv for CompioEnv {
    type RawListener = UnixListener;

    type SmuxClient = CompioSmuxClient;

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
        let _ = std::fs::remove_file(addr);
        UnixListener::bind(addr).await.map_err(|err| err.to_string())
    }

    async fn raw_accept(
        listener: Self::RawListener,
        ring_cap: usize,
    ) -> Result<(Self::RawListener, Rx, Tx), String> {
        let (stream, _peer) = listener.accept().await.map_err(|err| err.to_string())?;
        let (rx, tx) = adapt_compio_(stream, ring_cap)?;
        Ok((listener, rx, tx))
    }

    async fn raw_connect(addr: &'static str, ring_cap: usize) -> Result<(Rx, Tx), String> {
        let stream = UnixStream::connect(addr)
            .await
            .map_err(|err| err.to_string())?;
        adapt_compio_(stream, ring_cap)
    }

    async fn smux_connect<'a>(
        &'a self,
        addr: &'a str,
        ring_cap: usize,
    ) -> Result<Self::SmuxClient, String> {
        // 重试拨号：本 bench 的两端在同一进程里并发起步，客户端可能比服务端的
        // `bind` 早一步（此时拿到 `ENOENT`）。真实 C/S 里也有同样的先后竞态，
        // 因此这里用「重试 + 退避」而不是假设对端一定先起来。
        let stream = connect_retry(addr).await?;
                let rt = self.rt_.clone();
        let scope = self.scope_.clone();
        scope
            .run_until(async move {
                let (rx, tx) = adapt_compio_(stream, ring_cap)?;
                let opts = BasicOpts::default();
                                let delivery = HandshakeAgent::new(tx, rx)
                    .invite_async(&opts, AcceptAllEntries)
                    .await
                    .map_err(|err| format!("握手失败：{err}"))?;
                                let conn = MuxConnection::new(delivery, crate::cfg::BenchCfg::new(rt));
                Ok(CompioSmuxClient {
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
        crate::trace_!("compio：bind {addr}");
        let listener = UnixListener::bind(addr).await.map_err(|err| err.to_string())?;
        crate::trace_!("compio：accept 等待拨号");
        let (stream, _peer) = listener.accept().await.map_err(|err| err.to_string())?;
        crate::trace_!("compio：已 accept，开始握手");
        let rt = self.rt_.clone();
        let scope = self.scope_.clone();
        scope
            .run_until(async move {
                                let (rx, tx) = adapt_compio_(stream, ring_cap)?;
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
                crate::trace_!("compio：监听 dock 就绪");
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
        &'static self,
        future: F,
    ) -> impl core::future::Future<Output = Result<F::Output, Box<dyn std::error::Error + Send + Sync>>>
    where
        F: core::future::Future + 'static,
        F::Output: 'static,
    {
        // compio 的执行体是线程本地的，`spawn` 不要求 `Send`。
        let handle = self.scope_.spawn_local(future);
        async move {
            // `abs_art` 的句柄本身就是一个 `Future<Output = Result<T, JoinErr>>`，
            // 三个后端的错误类型不同，这里统一折成 boxed error。
            handle
                .await
                .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> {
                    format!("用例任务失败：{err:?}").into()
                })
        }
    }
}

/// compio 装配下的 smux 客户端：一条复用连接 + 各子流的 dock 绑定。
pub struct CompioSmuxClient {
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
                compio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
    Err(format!("拨号 {addr} 重试 100 次仍失败：{last}"))
}

/// 把一条 UDS 流拆成两条适配器半边。
fn adapt_compio_(stream: UnixStream, ring_cap: usize) -> Result<(Rx, Tx), String> {
    // compio 的 `into_split` 就是 `(self.clone(), self)`：两个半边共用同一个 fd，
    // 各自都能独立读写（环本身就是「一次只借给一个人」的 SPSC）。
    let (writer, reader) = stream.into_split();
    let rx = Rx::try_new(reader, ring_cap, DefaultAllocConfig).map_err(|err| err.to_string())?;
    let tx = Tx::try_new(writer, ring_cap, DefaultAllocConfig).map_err(|err| err.to_string())?;
    Ok((rx, tx))
}

/// bin 的入口。
pub fn run_blocking() -> ExitCode {
    let plan = match cli_::parse_env_args() {
        cli_::Parsed::Plan(plan) => plan,
        cli_::Parsed::Exit(code) => return code,
    };
    let runtime = cli_::K_COMPILED_RUNTIME;
    // 自检要在后端上下文里做，因此借 `Drive::drive` 跑一遍（它会建一个 compio 运行时）。
    let checked = <crate::DriveHere as Drive>::drive(async {
        let rt: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
        crate::assert_runtime(&rt, runtime)?;
        crate::assert_runtime(&rt, plan.runtime)?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let result = match checked {
        Ok(Ok(())) => <crate::DriveHere as Drive>::drive(
            crate::bench_run::run_matrix::<CompioEnv, crate::DriveHere>(&plan, runtime.name()),
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
        // **不再建运行时**：`drive` 已经建好 compio 运行时（thread-local 的
        // `CURRENT_RUNTIME`），这里只把用例体交给它（`run_until` 对本后端是原样返回）。
        let rt_value: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
        let scope = rt_value.local_scope();
        Ok(scope.run_until(body(&rt_value, &scope)).await)
    }

    fn drive<F>(future: F) -> Result<F::Output, Box<dyn std::error::Error + Send + Sync>>
    where
        F: core::future::Future,
    {
        // 每个跑器线程一个 compio 运行时实例（线程本地执行体 + proactor）。
        // compio 的「当前运行时」是一个 thread-local，必须在取运行时值**之前**建好；
        // 本方法因此自己建一个运行时实例（每个跑器线程一个）。
        let driver = compio::runtime::Runtime::new()
            .map_err(|err| format!("建 compio 运行时失败：{err}"))?;
        Ok(driver.block_on(async {
            let rt: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
            // `block_on` 来自 `TrBlockOn`（与 `TrLocalScope` 一样由 `abs_art` 提供）。
            TrBlockOn::block_on(&rt, future)
        }))
    }

    async fn run_one<F>(future: F) -> F::Output
    where
        F: core::future::Future + 'static,
    {
        // 汇总层已在 `drive` 建的 compio 运行时里；本后端的 `run_until` 是「原样返回
        // future」，直接 await 即可（运行时自己 tick 队列与 proactor）。
        let rt: Rt = smux_v1::x_deps::abs_art_facade::default_rt_();
        let scope = rt.local_scope();
        scope.run_until(future).await
    }
}

#[cfg(test)]
mod tests {
    use crate::Drive;

    /// 测试 compio 壳的 [`Drive::drive`] 能把一个 future 跑到完成并取回输出。
    /// - 手段：用 `DriveHere::drive` 跑一个 `async { 40 + 2 }`。
    /// - 判断：返回 `Ok(42)`。
    ///
    /// Checks the compio shell can drive a future to completion.
    /// - Method: run `async { 40 + 2 }` through `DriveHere::drive`.
    /// - Assertion: it yields `Ok(42)`.
    #[test]
    fn drive_runs_future_to_completion_() {
        let got = <crate::DriveHere as Drive>::drive(async { 40u32 + 2u32 });
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
        assert_eq!(tag, smux_v1::x_deps::RuntimeTag::Compio);
    }
}
