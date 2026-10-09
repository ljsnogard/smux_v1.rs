# smux_v1

`smux` 流复用协议的第一版实现，实现 [`abs_smux`](../abs_smux) 定义的那套接口。
握手与复用已经端到端跑通：真实 UNIX domain socket 上，tokio 与 compio 各跑一遍。

**本文 §2 的两段代码取自 [`examples/active_passive.rs`](examples/active_passive.rs)
的两个角色函数**（差别只在：example 里把默认后端与连接级缓冲的构造写全了），
那个 example 可以直接跑：

```bash
cargo run --example active_passive
# 主动端：已发出 5 字节并半关闭
# 被动端：收到 "hello"
```

## 1. 为什么值得用

- **一条连接复用成多条并行子流**：可以基于 TCP / UNIX socket / QUIC stream 等连接复用成互不干扰的
  channel。
- **不绑定运行时**，且 `no_std` 友好
- **两端对称**：没有 client / server 之分，两边都支持绑定垛口（Dock）后监听建立子流的请求。
- **零拷贝**：子流两端直接就是 `abs_buff` 的段接口，没有中间缓冲，也不按帧长分配。

## 2. 怎么用起来

一次连接建立是**两段**，两段用到的概念在代码里都是明的：

| 段 | 做什么 | 要自己准备运行时吗 |
| --- | --- | --- |
| **握手** | `HandshakeAgent` 在两条半边上来回协商，产出 `HandshakeDelivery`（协商结果 + 归还的收发通道） | **不要**——它不投递任何任务 |
| **建连接** | 交付物 + 资源策略 + 帧暂存 → `MuxConnection`，五个循环投递到本地队列 | **不要**——运行时值与本地作用域都由连接自己取 |

`rx` / `tx` 是**已经接成 [`abs_buff`](../abs_buff) 两条半边**的传输——即实现
`TrBuffRead<u8>` / `TrBuffWrite<u8>` 的一对读写端，怎么从 socket 接出来见 §3。

**运行时值与本地作用域都不再是入参**：后端由集成方在 `Cargo.toml` 里经
`abs_art-bridge` 选定（本仓缺省 **compio**），连接建连时取该后端的运行时值、再由它
交出本线程的本地作用域。因此 `MuxConnection` 的类型参数只有**配置**一个，调用方一行
`from_delivery` 就够。要**自己挑后端**（例如默认 compio 但这条连接要跑在 tokio 上）
就用 §2.1 的显式写法。

**主动端**——连上去，开一条子流，发消息，半关闭：

```rust,ignore
// ① 握手
let delivery = HandshakeAgent::new(tx, rx)
    .invite_async(&BasicOpts::default(), AcceptAllEntries)
    .await?;

// ② 建连接：运行时值与本地作用域由连接自己取（后端 = 默认后端）。
let conn = MuxConnection::from_delivery(delivery)?;

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

// ② 建连接：同上。
let conn = MuxConnection::from_delivery(delivery)?;

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

### 2.1 想自己挑后端：显式传入运行时值

`from_delivery` 用的是**默认后端**。要在别的后端上建连接（或要接虚拟时钟），把运行时值
显式传进配置即可——`MuxConnection` 依然只有一个类型参数，后端的选择落在配置的类型上：

```rust,ignore
use smux_v1::connection::{DefaultConnCfg, MuxConnection, TrConnCfg};
use smux_v1::flow_ctrl::DefaultPolicy;
use smux_v1::metrics::NoMetrics;

// 用自己的运行时值类型（这里用 bridge 的具名别名指到 tokio）。
type Rt = smux_v1::x_deps::abs_art_bridge::TokioRuntime;
// 泛型参数顺序是 <W, R, M, P, Rt>：M 是 metrics 接收方，不上报就填 NoMetrics。
type Cfg = DefaultConnCfg<Tx, Rx, NoMetrics, DefaultPolicy, Rt>;

let rt = <Rt>::current();                   // 在 tokio 上下文内取
let (delivery, cfg) = <Cfg>::new_with_rt(delivery, DefaultPolicy, rt);
let (stage_r, stage_w) = cfg.make_stage_buffs(cfg.allocator())?;
let conn = MuxConnection::new(delivery, cfg, stage_r, stage_w);
```

**smux 本身不直接依赖任何后端 crate**：`Runtime` / `LocalScope` / `current` 都从
`abs_art-bridge` 取，后端由 feature 选定（缺省 compio）。因此下游要么直接用 bridge 的
裸名（与缺省后端一致），要么像上面这样用具名别名挑一个别的后端。

连接内部**读取「现在」的时刻来源**就是配置里的这个值，因此「时刻」与「计时器」必然
同源——换成 `abs_art_mock_clock::ManualTime` 就能把整条连接跑在虚拟时间上。

## 3. 唯一还要自己接的：传输

`MuxConnection` 只吃 `abs_buff` 的两条半边，**不认识具体的 socket 类型**（tokio 与
compio 的 socket 半边类型不同，compio 的还是 `!Send`）。所以「socket →
`TrBuffRead` / `TrBuffWrite`」这一步留在调用方：

```text
socket 读半边 --(入向泵)--> 全被动环 ──> Rx 交给 smux
                         smux 的 Tx = 全被动环 --(出向泵)--> socket 写半边
```

照抄 [`examples/active_passive.rs`](examples/active_passive.rs) 里的
`passive_ring_` / `pump_input_` / `pump_output_` 三个函数即可（约 80 行；那个示例用
**缺省后端 compio** 的 socket 与适配，因此 `cargo run --example active_passive`
不需要任何 feature）。
**为什么必须自己驱动泵**（三处实测阻塞点：compio 半边 `!Send`、`TrBuffWrite` 没有
flush 钩子、`buffex` 主动泵只做单次 poll）写在
[`tests/common/mod.rs`](tests/common/mod.rs) 模块文档里。

## 4. 怎么快速验证

```bash
cd smux_v1
cargo run --example active_passive           # 本文 §2 的两段代码，可运行版本
cargo run --example connect_single_thread    # 单线程起连接：DefaultConnCfg + 内存环直连
cargo run --example connect_multi_thread     # 多线程起连接：CurrentConnCfg + 句柄跨线程

cargo test --test inmem_mux                  # 内存环直连：握手 → 建流 → 收发 → 半关闭
cargo test --test smoke_compio small_socket  # 真实 UNIX socket，2 dock × 2 子流，秒级（缺省装配）
cargo test --test smoke_compio smoke_socket  # 真实 UNIX socket，16 个 dock 共 1024 条子流
cargo test --test inmem_mux mux_single_byte_transport   # 传输环只给 1 字节也照样跑通
just test                                    # 全量：两套冒烟 + 文档测试 + feature 组合
```

单条用例 `just test-one <名字片段>`；单个目标 `just test-target inmem_mux`。

后两个示例是**同一件事的两种装配**，都只演示「起连接」（内存环直连，不碰 socket）：

| 示例 | 配置 | 连接跨线程 |
| --- | --- | --- |
| [`connect_single_thread`](examples/connect_single_thread.rs) | `DefaultConnCfg`：运行时值**存进**配置 | compio 装配下 `!Send`（单线程下最省事） |
| [`connect_multi_thread`](examples/connect_multi_thread.rs) | `CurrentConnCfg`：运行时值**不存**，每次从当前上下文取 | `Send + Sync`：worker 线程自建一个运行时上下文，就能拿连接句柄 `bind_async` / `open_channel_async` |

## 5. 六条使用须知

1. **dock 对即身份**：同一 `(local_dock, remote_dock)` 对同一时刻至多一条活动子流。
   发起侧要为每条并发子流分配**互不相同**的临时 `local_dock`（类比 TCP 临时端口），
   否则第二次 `open` 会被 `BindingError::Duplicate` 拒绝。
2. **半关闭是协议义务**：`drop(tx)` 只是「不再写」，已写入的数据仍会被搬运出去，
   对端读空之后才拿到 `Closing`。`drop(tx)` 本身**不等待**送达。
   两个方向正交：`drop(rx)` 发 `CLOSE(RESET)`（「我不再接收」），只让对端停止**发送**，
   不影响本端仍在排空的发送方向；反过来，本端收到对端的 `RESET` 时**允许直接放弃**
   发送环里还没上网的字节——那是唯一一处「已提交的字节可以不送达」的例外。
3. **`drop` 连接 = 拆连接**：四个收发循环随连接最后一个强引用一起收尾。发送方必须把
   连接**活到数据真的上网**为止——`examples/active_passive.rs` 正是因为这一点，
   把连接交回 `main` 保管，而不是在角色函数里就地丢弃。
4. **内存由调用方说了算**：每一处堆分配都走注入的分配器（`allocator_api`），不用全局
   分配器，也不靠 `Vec` 之类的临时堆结构绕开设计。
5. **后端选择在 `Cargo.toml`，且默认是 compio**：经 `abs_art-bridge` 选定；测试下用
   `--no-default-features --features test-tokio-runtime` 换 tokio。连接是
   `Send + Sync` **当且仅当**配置里的运行时值是——compio 的运行时值
   （线程本地的执行器）不是，tokio 的 `Handle` 把手是。
6. **指标上报是可选特性**：`--features metrics` 才启用（**零依赖**）。它只在
   `TrConnCfg::Metrics` 上做**静态分派**：缺省是零大小的 `NoMetrics`，`metrics()` 返回
   的是**确定的引用**（不是 `Option`，因此热路径上没有判空分支），上报调用点随之被
   优化掉（实测无 `callq`）——不开 feature 不付代价。要接自己的采集器，实现
   `type Metrics` 与 `metrics()` 两处即可（不上报的写法是一行 `NoMetrics::silent()`）；
   mux **不**替你做 `Arc` / `dyn` 的适配，sink 存在哪、怎么共享全由你决定
   （`tests/metrics_e2e.rs` 演示了「放 `static`」这一种）。指标清单的裁剪（重传与心跳
   RTT 在本协议下不成立，水位不报）与全部插入点见
   [`dev-notes/metrics-20261006-2253.md`](dev-notes/metrics-20261006-2253.md)。

## 6. 延伸阅读

- 协议线格式与设计（帧形状、dock 语义、流控、关闭流程、缓冲区）：
  [`src/connection/mod.rs`](src/connection/mod.rs) 模块文档 §1–§6；
- 握手线格式（magic、自描述条目、增量 CRC、一票否决）：
  [`src/handshake/mod.rs`](src/handshake/mod.rs) 模块文档 §4–§12；
- 决策与踩坑记录：[`dev-notes/`](dev-notes)；其中
  [`runtime-adoption-20261006-1352.md`](dev-notes/runtime-adoption-20261006-1352.md)
  与
  [`timer-mock-clock-and-generic-drop-20261006-1625.md`](dev-notes/timer-mock-clock-and-generic-drop-20261006-1625.md)
  记着「为什么运行时值进配置、为什么 `MuxConnection` 只剩一个参数」。
