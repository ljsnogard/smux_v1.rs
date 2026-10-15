//! **tokio** 装配下 `SmuxListener` 的端到端验收：TCP 与 UNIX 域各一遍。
//!
//! # 这个文件验证什么
//!
//! listener 的职责是「接受连接 → 完成 smux 握手 → 交出成形连接」。本文件因此在
//! **真实 socket** 上把两侧都跑起来：
//!
//! 1. 服务端绑定监听套接字、构造 [`SmuxListener`]；
//! 2. 客户端用 [`HandshakeAgent::invite_async`] 主动发起握手——这正是 listener 要自动
//!    化掉的那一步；
//! 3. 从 listener 拉取连接，并**在交付的连接上真的做一次操作**（`bind_async` 认领一个
//!    dock）：只断言「拿到了一个对象」不足以证明握手真的完成了。
//!
//! # 与另外两个运行时的关系
//!
//! 本文件是 tokio 的壳；compio / smol 各有一份同形的壳。三份**不共用**场景代码，
//! 因为 socket 类型、作用域类型与 `#[…::test]` 宏都不同——共用的代价大于收益。

#![cfg(feature = "rt-tokio")]

use abs_art_tokio::Runtime as TokioRuntime;
use abs_async_iter::TrAsyncIterator;
use buffex_tokio_adapt::{BuffRead, BuffWrite, DefaultAllocConfig};
use smux_v1::{
    connection::{Dock, TrConnCfg},
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
    x_deps::{
        abs_art::TrLocalScope,
        abs_cancel::{CancelledToken, NonCancellableToken, TrMayCancel},
        abs_smux::conn::TrConnection,
    },
};
use smux_v1_listener::{
    SmuxListenerError,
    config::{TrInbound, TrListenerConfig, TrNegotiatorFactory},
    listening::SmuxListener,
    runtime::{K_DEFAULT_RING_CAPACITY, TcpConnCfg, TcpTransport, UdsConnCfg, UdsTransport},
};

/// 协商策略：接受对端提出的一切条件。
///
/// 它就是 `smux_v1` 自带的 [`AcceptAllEntries`]——无状态，因此工厂返回 `Self`。
#[derive(Clone, Copy)]
struct AcceptAll;

impl TrNegotiatorFactory for AcceptAll {
    type Negotiator = AcceptAllEntries;

    fn make(&self) -> Self::Negotiator {
        AcceptAllEntries
    }
}

/// 测试用的 listener 配置：握手条件取协议缺省值，连接策略用 [`CurrentConnCfg`]。
#[derive(Clone)]
struct TestCfg<C> {
    /// 运行时值：提供「现在几点」与「怎么等」。
    rt_: TokioRuntime,

    /// 每条连接的资源策略。
    conn_: C,
}

impl<C> TrListenerConfig for TestCfg<C>
where
    C: TrConnCfg<Rt = TokioRuntime> + Clone + 'static,
{
    type Rt = TokioRuntime;
    type Conn = C;

    fn runtime(&self) -> Self::Rt {
        self.rt_.clone()
    }

    fn local_opts(&self) -> BasicOpts {
        BasicOpts::default()
    }

    fn conn_cfg(&self) -> Self::Conn {
        self.conn_.clone()
    }

}

/// 验收：单次等待可取消且不影响 listener；`close_async` 能打断卡在 accept 上的槽位、
/// 等它收尾，之后不再交付。
/// - 手段：构造 listener 后先用一个**已取消**的令牌调一次 `next_async`（它已经补出槽位
///   并卡在 accept 上）；接着在本地队列上起一个客户端完成一次正常握手并再取一次连接；
///   最后调用 `close_async`。
/// - 判断：第一次等待得到 `Err(Cancelled)`；第二次拿到连接且能 `bind_async`（证明取消
///   没有弄坏 listener）；`close_async` 返回后 `active_slots_()` 为 0，且 `next_async`
///   一律得到 `Err(Closed)`。

#[tokio::test]
async fn tokio_next_async_cancellable_and_close_async_() {
    let rt = TokioRuntime::current();
    let scope = rt.local_scope();

    let bound = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 TCP 监听套接字");
    let addr = bound.local_addr().expect("取本地地址");

    let cfg = TestCfg {
        rt_: rt.clone(),
        conn_: TcpConnCfg::make_with_rt(smux_v1::flow_ctrl::DefaultPolicy, rt.clone()),
    };
    let mut listener = SmuxListener::new_(TcpTransport::new(bound), cfg, AcceptAll, scope.clone());

    scope
        .run_until(async {
            // ① 本次等待被取消：只结束这一次调用，listener 本身不受影响。
            let cancelled = listener
                .next_async()
                .may_cancel_with(CancelledToken::new())
                .await;
            assert!(
                matches!(cancelled, Result::Err(SmuxListenerError::Cancelled)),
                "施加已取消的令牌应当只中止这一次等待"
            );

            // ② listener 仍然可用：正常交付一条连接。
            let client = scope.spawn_local(async move {
                let stream = tokio::net::TcpStream::connect(addr)
                    .await
                    .expect("客户端连接");
                stream.set_nodelay(true).expect("设置 TCP_NODELAY");
                let (rd, wr) = tokio::io::split(stream);
                let rx = BuffRead::<_, DefaultAllocConfig>::try_new(
                    rd,
                    K_DEFAULT_RING_CAPACITY,
                    DefaultAllocConfig,
                )
                .expect("客户端读环容量合法");
                let tx = BuffWrite::<_, DefaultAllocConfig>::try_new_local(
                    wr,
                    K_DEFAULT_RING_CAPACITY,
                    DefaultAllocConfig,
                )
                .expect("客户端写环容量合法");
                HandshakeAgent::new(tx, rx)
                    .invite_async(&BasicOpts::default(), AcceptAllEntries)
                    .may_cancel_with(NonCancellableToken::new())
                    .await
            });

            let (accepted, invited) = futures::join!(async { listener.next_async().await }, client);

            let invited = invited.expect("客户端任务完成");
            assert!(invited.is_ok(), "客户端握手应当成功：{:?}", invited.err());
            let conn = accepted.expect("取消过一次之后仍应交付连接");
            assert!(
                TrConnection::bind_async(&conn, Dock::unspecified())
                    .await
                    .is_ok(),
                "交付的连接应当可以认领 dock"
            );

            // ③ 优雅关闭：打断卡在 accept 上的槽位、等它们收尾，然后不再交付。
            listener.close_async().await;
            assert_eq!(
                listener.active_slots_(),
                0usize,
                "close_async 返回时不应还有存活的接受槽位"
            );
            // 幂等：再关一次不会有任何副作用。
            listener.close_async().await;
            let after = listener.next_async().await;
            assert!(
                matches!(after, Result::Err(SmuxListenerError::Closed)),
                "关闭之后应当一律回报 Closed"
            );
        })
        .await;
}

/// 编译期自查：`next_async` 的每一项约束在这里被单独钉住。
///
/// 它比「在测试里用一次」更有价值：约束一旦被将来某次改动破坏，这里会精确指出**是
/// 哪一条**不再成立，而不是只说「找不到方法 `next_async`」。
const _: fn() = || {
    fn assert_next_async_available_<Cfg, A, N, U>()
    where
        Cfg: TrListenerConfig + Clone + 'static,
        A: TrInbound<ConnCfg = Cfg::Conn> + 'static,
        N: TrNegotiatorFactory + Clone + 'static,
        U: smux_v1::x_deps::abs_art::TrLocalScope + Clone + 'static,
    {
        fn uses_next_async_<Cfg, A, N, U>(listener: &mut SmuxListener<Cfg, A, N, U>)
        where
            Cfg: TrListenerConfig + Clone + 'static,
            A: TrInbound<ConnCfg = Cfg::Conn> + 'static,
            N: TrNegotiatorFactory + Clone + 'static,
            U: smux_v1::x_deps::abs_art::TrLocalScope + Clone + 'static,
        {
            drop(TrAsyncIterator::next_async(listener));
        }
        let _ = uses_next_async_::<Cfg, A, N, U>;
    }

    let _ = assert_next_async_available_::<
        TestCfg<TcpConnCfg>,
        TcpTransport,
        AcceptAll,
        abs_art_tokio::LocalScope,
    >;
    let _ = assert_next_async_available_::<
        TestCfg<UdsConnCfg>,
        UdsTransport,
        AcceptAll,
        abs_art_tokio::LocalScope,
    >;
};

/// 验收：TCP 上能交付一条已完成握手的连接，且该连接可以真的认领 dock。
/// - 手段：服务端在 `127.0.0.1:0` 上绑定并构造 listener；客户端在**本线程本地队列**上
///   连上去并用 `HandshakeAgent::invite_async` 主动完成一次握手；两侧在同一 `join!` 里推进。
/// - 判断：listener 交出 `Ok(conn)`，且 `conn.bind_async(unspecified)` 也返回 `Ok`
///   ——后者证明连接内部的五个循环确实跑起来了，而不只是一个空句柄。
#[tokio::test]
async fn tokio_tcp_delivers_handshaken_connection_() {
    let rt = TokioRuntime::current();
    let scope = rt.local_scope();

    let bound = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 TCP 监听套接字");
    let addr = bound.local_addr().expect("取本地地址");

    let cfg = TestCfg {
        rt_: rt.clone(),
        conn_: TcpConnCfg::make_with_rt(smux_v1::flow_ctrl::DefaultPolicy, rt.clone()),
    };
    let mut listener = SmuxListener::new_(TcpTransport::new(bound), cfg, AcceptAll, scope.clone());

    scope
        .run_until(async {
            // 客户端也投到本地队列：它的写半边走「自驱动」形态，泵必须由同一线程驱动。
            let client = scope.spawn_local(async move {
                let stream = tokio::net::TcpStream::connect(addr)
                    .await
                    .expect("客户端连接");
                stream.set_nodelay(true).expect("设置 TCP_NODELAY");
                let (rd, wr) = tokio::io::split(stream);
                let rx = BuffRead::<_, DefaultAllocConfig>::try_new(
                    rd,
                    K_DEFAULT_RING_CAPACITY,
                    DefaultAllocConfig,
                )
                .expect("客户端读环容量合法");
                let tx = BuffWrite::<_, DefaultAllocConfig>::try_new_local(
                    wr,
                    K_DEFAULT_RING_CAPACITY,
                    DefaultAllocConfig,
                )
                .expect("客户端写环容量合法");
                HandshakeAgent::new(tx, rx)
                    .invite_async(&BasicOpts::default(), AcceptAllEntries)
                    .may_cancel_with(NonCancellableToken::new())
                    .await
            });

            let (accepted, invited) = futures::join!(async { listener.next_async().await }, client);

            let invited = invited.expect("客户端任务完成");
            assert!(invited.is_ok(), "客户端握手应当成功：{:?}", invited.err());
            let conn = accepted.expect("listener 应当交付一条连接");
            assert!(
                TrConnection::bind_async(&conn, Dock::unspecified())
                    .await
                    .is_ok(),
                "交付的连接应当可以认领 dock（证明内部循环已跑起来）"
            );
        })
        .await;
}

/// 验收：UNIX 域上同样能交付一条已完成握手的连接。
/// - 手段：与 TCP 那条逐字同形，只把监听套接字与客户端连接换成 `UnixListener` /
///   `UnixStream`，路径取进程私有临时目录下的一个文件。
/// - 判断：同 TCP 那条——`Ok(conn)` 且 `bind_async` 成功。
#[tokio::test]
async fn tokio_uds_delivers_handshaken_connection_() {
    let rt = TokioRuntime::current();
    let scope = rt.local_scope();

    let path = std::env::temp_dir().join(format!(
        "smux-v1-listener-tokio-{}.sock",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let bound = tokio::net::UnixListener::bind(&path).expect("绑定 UNIX 域监听套接字");
    let connect_path = path.clone();

    let cfg = TestCfg {
        rt_: rt.clone(),
        conn_: UdsConnCfg::make_with_rt(smux_v1::flow_ctrl::DefaultPolicy, rt.clone()),
    };
    let mut listener = SmuxListener::new_(UdsTransport::new(bound), cfg, AcceptAll, scope.clone());

    scope
        .run_until(async {
            let client = scope.spawn_local(async move {
                let stream = tokio::net::UnixStream::connect(connect_path)
                    .await
                    .expect("客户端连接");
                let (rd, wr) = tokio::io::split(stream);
                let rx = BuffRead::<_, DefaultAllocConfig>::try_new(
                    rd,
                    K_DEFAULT_RING_CAPACITY,
                    DefaultAllocConfig,
                )
                .expect("客户端读环容量合法");
                let tx = BuffWrite::<_, DefaultAllocConfig>::try_new_local(
                    wr,
                    K_DEFAULT_RING_CAPACITY,
                    DefaultAllocConfig,
                )
                .expect("客户端写环容量合法");
                HandshakeAgent::new(tx, rx)
                    .invite_async(&BasicOpts::default(), AcceptAllEntries)
                    .may_cancel_with(NonCancellableToken::new())
                    .await
            });

            let (accepted, invited) = futures::join!(async { listener.next_async().await }, client);

            let invited = invited.expect("客户端任务完成");
            assert!(invited.is_ok(), "客户端握手应当成功：{:?}", invited.err());
            let conn = accepted.expect("listener 应当交付一条连接");
            assert!(
                TrConnection::bind_async(&conn, Dock::unspecified())
                    .await
                    .is_ok(),
                "交付的连接应当可以认领 dock"
            );
        })
        .await;

    let _ = std::fs::remove_file(&path);
}

/// 编译期自查：**交付的连接**必须是 `Send + Sync`，而配置本身也是。
///
/// 使用者拿到 `MuxConnection` 之后通常要把它分发给别的线程（每条连接一个 worker），
/// 因此那两个 auto trait 是契约的一部分。一旦某个内部字段（例如连接配置携带的运行时
/// 值）不再 `Send`，这里会立刻失败，而不是等到使用者的代码里才暴露。
///
/// # `SmuxListener` 自己**不是** `Send`，这是刻意的
///
/// 它持有本地作用域（各后端的作用域都是线程本地的 `Rc` 句柄），因此必须由创建它的那条
/// 线程驱动——这正是 `TrLocalScope` 的语义。`SmuxListener` 的 `U` 参数上也写着
/// `TrLocalScope`，编译器本来就不允许把它搬到别的线程。
const _: fn() = || {
    fn assert_send_sync_<T: Send + Sync>() {}

    assert_send_sync_::<TestCfg<TcpConnCfg>>();
    assert_send_sync_::<smux_v1::connection::MuxConnection<TcpConnCfg>>();
};
