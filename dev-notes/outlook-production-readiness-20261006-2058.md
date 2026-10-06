# smux_v1 的实战可用性：缺口清单与跨进程验证实验（展望）

> 本文是**展望**：上半部分盘点「距离实战可用还差什么」，下半部分给出一个可执行的
> 验证实验设计。它不是设计确认，也不是路线承诺。
>
> 约定：所有「现状」判断都标注代码 / 文档位置；所有**没有实测量支撑**的判断都显式
> 标成「待实测」，不写成结论。

---

## 1. 已经在位的机制

不是从零开始，下面这些都有测试钉住，是后续讨论的地基：

| 机制 | 位置 |
| --- | --- |
| 帧协议（`OPEN` / `ACCEPT` / `REJECT` / `CLOSE` / `DATA` / `WINDOW_UPDATE` / `PULSE`），Sans-IO 逐字节解析 | `src/connection/frame_.rs`、`frame_parser_.rs` |
| 三次握手、增量 CRC、一票否决 | `src/handshake/` |
| dock 对即身份、建流最终裁决（唯一提交点） | `src/connection/mod.rs` §4.1–§4.2 |
| 每子流接收窗口 + 水位通告 + 背压 | `src/flow_ctrl/`、`src/connection/session_.rs` |
| 半关闭：`FIN` / `RESET` 两个正交方向 | `src/connection/mod.rs` §7.0 |
| 保活 `PULSE`、空闲超时拆流、拆流宽限期 | `src/connection/timer_.rs`、`mux_connection/registry_.rs` |
| 配额（连接级 / dock 级子流数）、`max_packet_size` 校验 | `src/handshake/opts.rs`、`registry_.rs` |
| 零拷贝借用 + 调用方注入分配器（`allocator_api`） | `src/connection/ring_.rs`、`tests/alloc_count.rs` |
| 取消令牌收尾：`drop` 连接 ⇒ 五个循环退出 | `src/connection/mux_connection/core_.rs` |
| tokio 装配下连接 `Send + Sync` | `tests/thread_safety.rs` |

---

## 2. 缺口清单（按对实战落地的阻碍排序）

### 2.1 传输与会话骨架要使用者自己写（最大的缺口）

- **现象**：接一条真实 socket 需要调用方自己实现 `TrBuffRead` / `TrBuffWrite` 适配、
  两条「调用方驱动的泵」、两条全被动环，并理解三条实测阻塞点。
- **依据**：README §3 的原话是「唯一还要自己接的：传输」；现成实现只在
  `tests/common/pump_.rs`（测试专用）与 `dev-dependencies` 的
  `buffex_tokio_adapt` / `buffex_compio_adapt` 里。
- **影响**：每个使用方都要重写一遍这段代码，而它恰好是踩坑最密的一段
  （compio 半边 `!Send`、`TrBuffWrite` 没有 flush 钩子、buffex 主动泵只做一次
  非阻塞 poll）。
- **处置方向**：把它从 `tests/` 提炼成可复用的一层（例如 `smux_v1::transport::uds`），
  并补一个「监听 + accept 多条连接 + 每条建 `MuxConnection`」的服务端骨架；
  `examples/active_passive.rs` 是它的雏形。

### 2.2 smol 后端实际不可用

- **现象**：`abs_art-bridge` 有 `backend-smol`，`ScopeHost` 也只实现了一处
  （对裸名 `Runtime`，`src/connection/scope_host_.rs:67`），但 `smux_v1` 侧
  **没有 smol 的 feature、没有传输适配、零测试覆盖**。
- **依据**：`Cargo.toml` 的 `[features]` 只有 `test-tokio-runtime` /
  `test-compio-runtime`；`tests/` 下没有 smol 目标。
- **额外坑**：smol 的 `LocalExecutor` 与 tokio 的 `LocalSet` 一样**必须由调用方驱动**
  （`run_until`）——`layered_rpc` 刚在 tokio 上因为漏了这一步而静默挂起
  （见 `timer-mock-clock-and-generic-drop-20261006-1625.md` §10.6）。
- **处置方向**：加 feature（转发 bridge 的 `backend-smol`）+ 一个 smol 的 UDS 薄适配
  （`async-net::UnixStream` 包成 `TrInput` / `TrOutput`）+ 一个冒烟 target；
  驱动方式照 `tests/inmem_mux.rs`（`scope.run_until(scenario)`）。
- **进展（2026-10-06）**：本条处置方向的三件（Bridge feature / UDS 适配 / 冒烟
  target）已落地，四个冒烟用例在三个装配下全绿（当时的记录文档随后按「文档先清理」
  删掉了，实测结论就是这一句：三个装配都能在真实 UDS 上跑通冒烟）。本节其余判断
  （§2.1 的骨架提炼、§3 的跨进程编排）仍然成立；smol 侧在 §3 的实验里改走 **TCP**。

### 2.3 没有优雅关闭

- **现象**：`drop(连接) = 拆连接`。已提交但还没上网的字节，会因为连接被丢弃而消失。
- **依据**：README §5 第 3 条把「发送方必须把连接活到数据真的上网为止」写成使用纪律；
  `MuxCore::drop` 直接触发取消令牌（`mux_connection/core_.rs`）。
- **影响**：实战里「发完就 drop」是最自然的写法，而它会丢数据——现在只能靠调用方
  自己保管连接生命期，没有 API 兜底。
- **处置方向**：一个显式的 `close_async()`：停止接受新子流 → 等已提交数据上网 /
  子流排空（带上限）→ 再拆；超时后走现在的强制路径。

### 2.4 没有连接级流控（待实测）

- **现象**：窗口是**每子流**的；连接写环（stage ring）是**共享的一条**。
- **依据**：`src/connection/session_.rs:154` 明说「循环侧不再需要任何连接级窗口快照」；
  `flow_ctrl/` 只有子流窗口与水位。
- **待实测**：一条慢子流把连接写环占满时，其它子流是否被一起拖住（队头阻塞）。
  这是**推测**，需要用 §3 的 R2 轮去量，不宜先改设计。
- **处置方向**：先测；若确有影响，再讨论「连接级窗口」或「按子流限额」的取舍。

### 2.5 协议特性未落地：`DATAGRAM` / telegraph

- **现象**：`FrameKind::Datagram` 收到即忽略（`session_.rs:1017`），写路径未接
  （`frame_.rs:562`）；telegraph 端点无实现。
- **处置方向**：要么按需实现，要么明确把「数据报 / 代理」移出 v1 范围——两条路都行，
  但不能停留在「帧类型在、语义没有」的中间态。

### 2.6 没有连接建立超时

- **现象**：握手本身无超时；对端 accept 之后不发言，发起方会无限等在
  `wait_establish_`（`channel_handle/handle_.rs:421`）。空闲超时只管**已登记**的子流。
- **处置方向**：按本端策略加一个建流超时（与 `timer-mock-clock-and-generic-drop-…`
  §10.5 的裁决一致：本端自行决定，不进协议）。
- **进展（2026-10-06）**：**子流建流**这一段已落地——超时沿用
  `max_channel_timeout`（本端策略、不进协议），到点由连接代替调用方向对端回
  `REJECT`，并把 `IdleTimeout` 直接交给调用方（`accept` / `reject` 返回它，或经
  `ChannelHandle::abort_reason()` 查询）；裁决被取消时同样会向对端回 `REJECT`。
  因果与实测见 `establish-timeout-20261006-2231.md`。**本条前半句仍未解决**：
  连接级握手（`HandshakeAgent` 的 invite / listen）依旧没有超时。

### 2.7 默认参数的保守面

- `max_packet_size = 4096`（**帧总长**上限）、`max_channel_timeout = 30s`、
  `max_channel_wait_close = 5s`、`max_channel_count = 1 << 28`
  （`src/handshake/opts.rs:121-125`）。
- **影响**：4 KiB 的帧意味着大文件要靠海量小帧；吞吐与公平性都还没有数据。
- **处置方向**：用 §3 的 R4 轮量出「帧大小 vs 吞吐」，再决定默认值是否调整。

### 2.8 可运维性

- 无 tracing / metrics / 连接与子流统计；无故障注入、无压力测试、无长时间 soak。
- §3 的实验先补「跨进程 + 并发 + 大文件」这一块，其余仍属空白。

---

## 3. 验证实验：两进程 × 真实 TCP socket × 三运行时两两配对 × digest 交叉验证

> **2026-10-07 修订（取代原先的「三进程 × Unix domain socket × 环形配对」）**：
> 测试的过程与方式是——**两个进程前后交替扮演主动端和被动端**，各自建立**多条子流**发送
> 数据，然后验证数据收发是否正确。传输基础设施**不是 UDS 而是真实 TCP socket**，最终要
> 在**真实局域网的两台机器**上跑；发送侧**先告知对方数据 digest、再发数据本体**，接收侧
> 收到之后**再扮演主动端把数据本体送回**，于是**原先的主动方也能证明「我发的 == 对方收的」**；
> 传输过程用 **metrics** 衡量性能。
> UDS 环回在这种新口径下只配当「本地正确性预演」，不再是判据的一部分。

### 3.1 它要回答什么

1. **接收可靠性**：多条并发子流上搬运大块数据，字节与 digest 是否**一字不差**；
2. **跨后端互通**：compio / smol / tokio 三种装配**两两配对**时，协议与流控是否一致；
3. **跨进程、跨机器真实性**：不走内存环、不走 UDS，走**真实 TCP socket**、真实网卡与内核缓冲；
4. **性能可观测**：传输过程用 **metrics** 衡量（口径见 §3.5），把「感觉够快」换成数字；
5. **回归网**：把 §2.4 / §2.7 的待测项变成可重复的数字。

### 3.2 拓扑与角色交替

两个进程 `P_a` / `P_b`，各自绑定自己的 TCP 监听地址（`--listen`），并且都知道对端地址
（`--peer`）。一次测试里**前后两个阶段**：

```text
阶段 1：P_a ──dial──▶ P_b
        P_a = 主动端（握手 invite、开子流、发本体）     连接 A
        P_b = 被动端（握手 listen、accept 子流、收本体）

阶段 2（阶段 1 全部收发与校验完成之后）：P_b ──dial──▶ P_a
        P_b = 主动端（握手 invite、开子流、发本体）     连接 B
        P_a =被动端（握手 listen、accept 子流、收本体）
```

- 两个阶段**前后串行**，不是并发：「前后交替扮演主动端和被动端」于是由**阶段顺序 +
  拓扑**直接保证，而不是靠约定；
- 每个进程都跑**同一份场景代码**：差别只有编译期选定的运行时、地址参数，以及
  `--role`（本轮谁先当主动端，用于打破启动时的双向等待）；
- **主动端/被动端**按 README §2 的定义：主动端 = 握手 `invite_async`，被动端 = 握手
  `listen_async`。两条连接各自独立完成一次握手；
- 三种运行时**两两配对**共 3 组：(tokio, compio)、(tokio, smol)、(compio, smol)，
  每组跑一次；每组里两个运行时都先当主动端、再当被动端。

**为什么「选运行时」是编译期的事**：后端由 `abs_art-bridge` 的 feature 选定，而 `smux_v1`
的三个 `test-*-runtime` 是**互斥三选一**（同时启用多个 backend 且没有显式声明默认后端时，
会撞 bridge 的 `compile_error!`）。因此 demo 是**同一份源码按 feature 编译出的三个 peer
可执行文件**；命令行 `--runtime` 只用于**自检与编排**（与编译期后端不一致就立刻报错退出），
不是进程内切换运行时。这一点在下面的判据里要显式核对。

### 3.3 负载与「先 digest、后本体、再回执」的闭环

每条连接上，**主动端**为每条子流分配互不相同的临时 local dock（dock 对即身份的纪律，
见 §5.1），开 `N` 条子流（默认 8）；每条子流的**两个方向都各走一份不同的确定性数据**，
线格式是一个极小的信封协议：

```text
方向 d1（主动端 → 被动端）：ENVELOPE{ id, len, digest } + len 字节本体 + RECEIPT{ id, digest' }
方向 d2（被动端 → 主动端）：ENVELOPE{ id, len, digest } + len 字节本体 + RECEIPT{ id, digest' }
```

- **先 digest、后本体**：`ENVELOPE` 里带的是发送方**将要发**的那份数据的 digest；
- **回执就是「回送」的证明**：`RECEIPT.digest'` 是接收方**实际算出来的**、刚收到的那个
  方向的 digest。于是：
  - 接收方自己就判定得了「收得对不对」；
  - 发起方拿到 `RECEIPT` 之后比对 `digest' == digest`，**原先的主动方也就证明了
    「我发送的数据 == 对方收到的数据」**——这正是本实验要的那条因果链；
- 两个方向都带 `RECEIPT`，所以两个方向都被**双向**证明，而不是只靠一侧自报；
- 同一连接的两个方向走**不同**数据块（不是把收到的字节原样回显）：这样每条连接的两个
  方向、以及两条连接，一共覆盖四份互不相同的载荷，顺带压到「同一连接上两个方向同时有
  流量」这条路径；
- 载荷用**确定性 PRNG** 生成（默认每方向 8 MiB，`--bytes` 可改），避免磁盘 IO 成为变量；
  digest 复用已有 `crc` 依赖（CRC-32），不引新依赖。

**实现约束（实测得来，必须写进代码）**：每条子流的写侧与读侧必须 `join!` 并发推进——
「先把自己 8 MiB 写完再开始读」会在接收窗口耗尽时和对端互锁；同理，一条连接上 `N` 条
子流要用与运行时无关的组合子（`join_all`）并发推进，不依赖 `spawn`（compio 半边是 `!Send`）。

**收尾纪律**：每个方向都在 `RECEIPT` 到达之后才认为「已上网」，因此不需要靠 sleep 兜底；
对端 `FIN` 之后读到 `Closing` 才丢弃连接（§2.3 的使用纪律）。

### 3.4 「可靠」的判据

| # | 判据 |
| --- | --- |
| 1 | 每条子流、每个方向收到的字节数 == `ENVELOPE` 声明的 `len` |
| 2 | 接收方算出的 digest == 发送方在 `ENVELOPE` 里声明的 digest |
| 3 | 发起方收到的 `RECEIPT.digest'` == 自己声明的 digest（跨进程、跨运行时、跨机器都成立） |
| 4 | 子流 `abort_reason()` 为 `None`；对端 `FIN` 之后读到 `Closing`（EOF 语义正确） |
| 5 | 两个进程退出码全 0；编排脚本总超时（默认 180 s）反证「无挂起」 |
| 6 | 3 组配对全部通过，且每组两个进程都自报「我在连接 A 是主动端、在连接 B 是被动端」，自报运行时与编译期后端一致 |

### 3.5 metrics 口径

每个进程在场景结束时落一行 JSON（供编排器汇总），字段与口径：

| 指标 | 来源 | 口径说明 |
| --- | --- | --- |
| 本体字节 / 墙钟 = MiB/s | 自有计时 | 分方向、分阶段报告；`DebugMetrics` 不带时间源，吞吐必须自己测 |
| 帧数（按方向 / 帧种） | `frames_*_by_kind` | 能看出 `Data` / `WindowUpdate` / `Close` / `Pulse` 的比例 |
| 帧字节 | `frame_bytes_*` | **不含**握手与泵开销 |
| 传输字节 | `transport_bytes_*` | 两泵口径，与帧字节的**差值**就是握手 + 传输层开销 |
| 连接数 / 子流数 | `conns_*` / `channels_*` | 活跃数由 sink 自己相减 |
| 窗口更新 / RESET / 流控阻塞 / 帧错误 | `sent_of(WindowUpdate)`、`resets`、`flow_stalled`、`frame_errors` | 流控是否真的在工作 |
| 子流寿命、首次字节时间 | demo 自写 sink | 内置 `DebugMetrics` **丢弃** `on_channel_closed` 的 `lifetime_millis`，要这两项必须自己实现 `TrMetricsSink`（组合内置采集器即可） |

明确**不报**的三项（设计上就不成立，不要为它们改协议）：重传次数（协议跑在可靠有序
字节流上，恒 0）、`PULSE` RTT（帧协议上没有请求/应答语义）、缓冲区水位（`metrics` 模块
的既定裁剪）。

### 3.6 实施顺序：先环回，再局域网

1. **本地环回预演**（`127.0.0.1`，同一台机器两个进程，3 组配对全跑）：验的是**装置本身
   与协议闭环**，不产生跨机结论；
2. **真实局域网**（两台机器，各自绑 `0.0.0.0:<port>`，`--peer` 指向对方的 `<ip>:<port>`）：
   复跑同样的 3 组，得到跨机器的正确性与吞吐数字；
3. **R2–R5 那类压测**（背压、`drop(rx)` 触发 RESET、帧大小对吞吐的影响、静默触发空闲
   超时）等前两步稳下来再谈，本轮不做。

### 3.7 交付物

同级的 `smux_v1_sock_demo/`（**独立 crate，不并入任何 workspace**）：一份与运行时无关的
场景代码 + 三个按 feature 编译的 peer 可执行文件（tokio / compio / smol，各自挂
`smux_v1/metrics`）+ 一个编排器（本机环回时起两个进程、收 JSON 结果行、判定并汇总成
控制台表格 / markdown / jsonl）；跨机时用同样的参数经 ssh 起两个进程、回收两边结果行。
传输适配（TCP → `TrBuffRead` / `TrBuffWrite` 的两条半边 + 两条调用方驱动的泵）按
`examples/active_passive.rs` 与 `tests/common/` 的既有写法各写一份，**不动 `smux_v1` 的公开面**。


## 4. 与现有验收的分工

| 现有验收 | 覆盖 | 本实验补的面 |
| --- | --- | --- |
| `inmem_mux.rs` | 多子流、流控、半关闭（内存环） | 真实 TCP + 跨进程跨机器 + 大数据量 |
| `smoke_tokio/compio/smol.rs` | 端到端冒烟（真实 UDS） | 两进程前后交替角色、三运行时两两互通、digest 交叉验证 |
| `metrics_e2e.rs` | 一条子流的计数上报 | 多连接多子流下的 metrics 汇总与吞吐口径 |
| `keepalive*.rs` | 保活与空闲超时（虚拟时间） | 传输进行中的保活行为 |
| `layered_rpc.rs` | 公开 API 的可分层性 | 真实部署形态下的分层 |
| `alloc_count.rs` | 分配预算 | 大流量下的分配增长 |
