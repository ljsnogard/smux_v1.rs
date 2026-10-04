# smux_v1

`smux` 流复用协议的第一版实现，实现 [`abs_smux`](../abs_smux) 定义的那套接口。
握手与复用已经端到端跑通：真实 UNIX domain socket 上，tokio 与 compio 各跑一遍。

> ⚠️ 面向使用者的高层入口还没做。§2 是**目标形态**（样子代码，尚不能编译），
> §3 说明今天要自己接哪几处。想先跑起来，直接跳到 §4。

## 1. 为什么值得用

- **一条连接，上万条子流**：TCP / UNIX socket / QUIC stream 进来，出去是互不干扰的
  channel。
- **两端对称**：没有 client / server 之分——各自在 dock 上绑定，一侧 `open`、
  一侧 `listen`。
- **半关闭是真语义**：丢掉发送半边就是对端的 EOF，协议保证**先排空再发 FIN**。
- **零拷贝**：子流两端直接就是 `abs_buff` 的段接口，没有中间缓冲，也不按帧长分配。
- **极小内存也能跑**：帧头与握手都是 sans-IO 逐字节状态机，环容量 1 字节也够。
- **不绑定运行时**，`no_std` 友好，每一处堆分配都走调用方注入的分配器。

## 2. 目标形态（样子代码，尚不能编译）

**主动端**——连上去，开一条子流，发消息，半关闭：

```rust,ignore
let conn = MuxConnection::invite_async(socket, BasicOpts::default()).await?;
let mut binding = conn.bind_async(Dock::new(0x2001)).await?;

let mut ch = binding.open_channel_async(Dock::new(1)).await?;
let (mut tx, _rx) = ch.accept_async().await?;

tx.write_all(b"hello").await?;
drop(tx);                             // 半关闭：对端读到 EOF
```

**被动端**——等对端来找这个 dock，收到子流后读到 EOF：

```rust,ignore
let conn = MuxConnection::listen_async(socket, BasicOpts::default()).await?;

let mut listener = conn.bind_async(Dock::new(1)).await?   // 绑定 dock
    .listen_async().await?;                               // 开始收建流请求
let mut incoming = listener.income_async().await?;
let (_tx, mut rx) = incoming.accept_async().await?;

let mut buf = [0u8; 5];
rx.read_exact(&mut buf).await?;       // 对端 drop(tx) 之后就到这里
```

`bind_async` / `listen_async` / `open_channel_async` / `income_async` 这些名字取自
[`abs_smux`](../abs_smux) 现有的 trait，**今天就已经由本 crate 实现**。还缺的是
「一行接上 socket」的 `invite_async` / `listen_async` 入口，以及默认欢迎消息、
无参 `accept_async`、`write_all` / `read_exact` 这类省事的糖。

## 3. 今天要自己接的四件事

1. **传输**：`ConnRx` / `ConnTx` 要自己从 socket 接出来——设备级适配 + 全被动环 +
   4 条调用方驱动的泵。**为什么必须这么绕**（三处实测阻塞点）见
   [`tests/common/mod.rs`](tests/common/mod.rs) 模块文档。
2. **握手**：先 `HandshakeAgent::new(rx, tx).invite_async(&opts, AcceptAllEntries)`
   拿到 `HandshakeDelivery`，再 `MuxConnection::new(&scope, delivery, cfg, stage_r, stage_w)`。
3. **配置**：要自己写一个 `TrConnCfg`（约 50 行）。照抄
   [`tests/common/mod.rs`](tests/common/mod.rs) 的 `SmokeMuxConfig` 就行——crate 自带的
   `DefaultConnCfg` 现在**用不了**：`derive(Clone)` 顺手给 `W` / `R` 加了 `Clone`
   （环半部并不 `Clone`），而 `StageBuff = MuxChanBuff` 没有 `Send` / `Sync`。
4. **数据面**：今天直接对着 `try_write(&Demand)` / `read_async(&Demand)` 写段级循环，
   现成写法见同文件的 `write_channel_all_` / `read_channel_exact_`。

能跑的完整版本就是上面这些拼起来的，见 `tests/common/mod.rs` 的 `connect_pair_`
与其后的场景函数。

## 4. 怎么快速验证

```bash
cd smux_v1

cargo test --test inmem_mux              # 内存环直连：握手 → 建流 → 收发 → 半关闭
cargo test --test smoke_tokio small_socket   # 真实 UNIX socket，2 dock × 2 子流，秒级
cargo test --test smoke_tokio smoke_socket   # 真实 UNIX socket，16 个 dock 共 1024 条子流
cargo test --test inmem_mux mux_single_byte_transport   # 传输环只给 1 字节也照样跑通
just test                                # 全量：两套冒烟 + 文档测试 + feature 组合
```

单条用例 `just test-one <名字片段>`；单个目标 `just test-target inmem_mux`。

## 5. 三条使用须知

1. **dock 对即身份**：同一 `(local_dock, remote_dock)` 对同一时刻至多一条活动子流。
   发起侧要为每条并发子流分配**互不相同**的临时 `local_dock`（类比 TCP 临时端口），
   否则第二次 `open` 会被 `BindingError::Duplicate` 拒绝。
2. **半关闭是协议义务**：`drop(tx)` 只是「不再写」，已写入的数据仍会被搬运出去，
   对端读空之后才拿到 `Closing`。`drop(tx)` 本身**不等待**送达。
3. **内存由调用方说了算**：每一处堆分配都走注入的分配器（`allocator_api`），不用全局
   分配器，也不靠 `Vec` 之类的临时堆结构绕开设计。

## 6. 延伸阅读

- 协议线格式与设计（帧形状、dock 语义、流控、关闭流程、缓冲区）：
  [`src/connection/mod.rs`](src/connection/mod.rs) 模块文档 §1–§6；
- 握手线格式（magic、自描述条目、增量 CRC、一票否决）：
  [`src/handshake/mod.rs`](src/handshake/mod.rs) 模块文档 §4–§12；
- 决策与踩坑记录：[`dev-notes/`](dev-notes)。
