# smux_v1

`smux` 流复用协议的第一版实现，实现 [`abs_smux`](../abs_smux) 定义的那套接口。
握手与复用已经端到端跑通：真实 UNIX domain socket 上，tokio 与 compio 各跑一遍。

**本文 §2 的两段代码逐字取自 [`examples/active_passive.rs`](examples/active_passive.rs)
的两个角色函数**，那个 example 可以直接跑：

```bash
cargo run --example active_passive
# 主动端：已发出 5 字节并半关闭
# 被动端：收到 "hello"
```

## 1. 为什么值得用

- **一条连接，上万条子流**：TCP / UNIX socket / QUIC stream 进来，出去是互不干扰的
  channel。
- **两端对称**：没有 client / server 之分——各自在 dock 上绑定，一侧 `open`、
  一侧 `listen`。
- **半关闭是真语义**：丢掉发送半边就是对端的 EOF，协议保证**先排空再发 FIN**。
- **零拷贝**：子流两端直接就是 `abs_buff` 的段接口，没有中间缓冲，也不按帧长分配。
- **极小内存也能跑**：帧头与握手都是 sans-IO 逐字节状态机，环容量 1 字节也够。
- **不绑定运行时**，`no_std` 友好，每一处堆分配都走调用方注入的分配器。

## 2. 怎么用起来

一次连接建立是**两段**，两段用到的概念在代码里都是明的：

| 段 | 做什么 | 要作用域值吗 |
| --- | --- | --- |
| **握手** | `HandshakeAgent` 在两条半边上来回协商，产出 `HandshakeDelivery`（协商结果 + 归还的收发通道） | **不要**——它不投递任何任务 |
| **建连接** | 交付物 + 资源策略 + 帧暂存 → `MuxConnection`，四个收发循环投递到本地队列 | 要 |

`rx` / `tx` 是**已经接成 [`abs_buff`](../abs_buff) 两条半边**的传输——即实现
`TrBuffRead<u8>` / `TrBuffWrite<u8>` 的一对读写端，怎么从 socket 接出来见 §3。
`scope` 是运行时给的本地作用域值（`abs_art_tokio::LocalScope` 之类）。

**主动端**——连上去，开一条子流，发消息，半关闭：

```rust,ignore
// ① 握手
let delivery = HandshakeAgent::new(tx, rx)
    .invite_async(&BasicOpts::default(), AcceptAllEntries)
    .await?;

// ② 建连接：需要环境提供一个 LocalScope。
let conn = MuxConnection::from_delivery(scope, delivery)?;

let local_dock = Dock::new(0x2001);
let remote_dock = Dock::new(1);
let mut binding = conn.bind_async(local_dock).await?;

let mut invitation: &[u8] = b"Hi, SMUX!";
let mut ch = binding.open_channel_async(remote_dock, &mut invitation).await?;
let (mut tx, _rx) = ch.accept_async_default().await?;

tx.write_all(b"hello").await?;
drop(tx); // 半关闭：对端读到 EOF
```

**被动端**——等对端来找这个 dock，收到子流后读到 EOF：

```rust,ignore
// ① 握手
let delivery = HandshakeAgent::new(tx, rx)
    .listen_async(&BasicOpts::default(), AcceptAllEntries)
    .await?;

// ② 建连接：需要环境提供一个 LocalScope。
let conn = MuxConnection::from_delivery(scope, delivery)?;

let local_dock = Dock::new(1);
let mut listener = conn
    .bind_async(local_dock).await? // 绑定 dock
    .listen_async().await?;        // 开始收建流请求

let mut incoming = listener.income_async().await?;
let (_tx, mut rx) = incoming.accept_async_default().await?;

let mut buf = [0u8; 5];
rx.read_exact(&mut buf).await?; // 对端 drop(tx) 之后就到这里
```

握手的协商条目、拒绝策略、取消都由 `HandshakeAgent` 的参数控制，需要时直接改；
`MuxConnection::from_delivery` 只是「默认配置 + 建帧暂存 + `MuxConnection::new`」三步的
省事写法（**同步**方法），要自选分配器 / 流控 / 环存储 / 暂存容量就走
[`MuxConnection::new`](src/connection/mux_connection/conn_.rs)。`accept_async_default` 是
「空欢迎消息 + 缺省环容量」，`write_all` / `read_exact` 是段级循环之上的省事封装。

## 3. 唯一还要自己接的：传输

`MuxConnection` 只吃 `abs_buff` 的两条半边，**刻意不认识任何具体运行时**（tokio 与
compio 的 socket 半边类型不同，compio 的还是 `!Send`）。所以「socket →
`TrBuffRead` / `TrBuffWrite`」这一步留在调用方：

```text
socket 读半边 --(入向泵)--> 全被动环 ──> Rx 交给 smux
                         smux 的 Tx = 全被动环 --(出向泵)--> socket 写半边
```

照抄 [`examples/active_passive.rs`](examples/active_passive.rs) 里的
`passive_ring_` / `pump_input_` / `pump_output_` 三个函数即可（约 80 行）。
**为什么必须自己驱动泵**（三处实测阻塞点：compio 半边 `!Send`、`TrBuffWrite` 没有
flush 钩子、`buffex` 主动泵只做单次 poll）写在
[`tests/common/mod.rs`](tests/common/mod.rs) 模块文档里。

## 4. 怎么快速验证

```bash
cd smux_v1
cargo run --example active_passive           # 本文 §2 的两段代码，可运行版本

cargo test --test inmem_mux                  # 内存环直连：握手 → 建流 → 收发 → 半关闭
cargo test --test smoke_tokio small_socket   # 真实 UNIX socket，2 dock × 2 子流，秒级
cargo test --test smoke_tokio smoke_socket   # 真实 UNIX socket，16 个 dock 共 1024 条子流
cargo test --test inmem_mux mux_single_byte_transport   # 传输环只给 1 字节也照样跑通
just test                                    # 全量：两套冒烟 + 文档测试 + feature 组合
```

单条用例 `just test-one <名字片段>`；单个目标 `just test-target inmem_mux`。

## 5. 四条使用须知

1. **dock 对即身份**：同一 `(local_dock, remote_dock)` 对同一时刻至多一条活动子流。
   发起侧要为每条并发子流分配**互不相同**的临时 `local_dock`（类比 TCP 临时端口），
   否则第二次 `open` 会被 `BindingError::Duplicate` 拒绝。
2. **半关闭是协议义务**：`drop(tx)` 只是「不再写」，已写入的数据仍会被搬运出去，
   对端读空之后才拿到 `Closing`。`drop(tx)` 本身**不等待**送达。
3. **`drop` 连接 = 拆连接**：四个收发循环随连接最后一个强引用一起收尾。发送方必须把
   连接**活到数据真的上网**为止——`examples/active_passive.rs` 正是因为这一点，
   把连接交回 `main` 保管，而不是在角色函数里就地丢弃。
4. **内存由调用方说了算**：每一处堆分配都走注入的分配器（`allocator_api`），不用全局
   分配器，也不靠 `Vec` 之类的临时堆结构绕开设计。

## 6. 延伸阅读

- 协议线格式与设计（帧形状、dock 语义、流控、关闭流程、缓冲区）：
  [`src/connection/mod.rs`](src/connection/mod.rs) 模块文档 §1–§6；
- 握手线格式（magic、自描述条目、增量 CRC、一票否决）：
  [`src/handshake/mod.rs`](src/handshake/mod.rs) 模块文档 §4–§12；
- 决策与踩坑记录：[`dev-notes/`](dev-notes)。
