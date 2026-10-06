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
  target）已落地，四个冒烟用例在三个装配下全绿；因果与实测见
  `smoke-smol-20261006-2152.md`。本节其余判断（§2.1 的骨架提炼、§3 的跨进程编排）
  仍然成立。

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

## 3. 验证实验：三进程 × Unix domain socket × 并发子流传文件

### 3.1 它要回答什么

1. **接收可靠性**：多并发子流上搬运大块数据，字节与校验和是否**一字不差**；
2. **跨后端互通**：compio / smol / tokio 三种装配两两配对时，协议与流控是否一致；
3. **跨进程真实性**：不再用内存环，走真实 UDS 与真实内核缓冲；
4. **回归网**：把 §2.4 / §2.7 的待测项变成可重复的数字。

### 3.2 拓扑（环形配对，3 条连接、6 个端点方向）

```text
P_compio ──A→B──▶ P_smol
    ▲                │
    └──C→A── P_tokio ◀┘
```

每个进程：监听自己的 `<name>.sock`，同时主动连接另外两个；因此它既是两条连接的
发起方、又是两条连接的接收方，**三种后端两两之间都真的跑过**。
`ScopeHost` 由「当前默认后端」唯一决定，所以三个进程就是同一份代码按 feature
编译出的三个可执行文件。

### 3.3 负载

- 每条连接上每侧各开 `N = 8` 条子流；临时 dock 按 `0x1000 + i` 错开
  （dock 对即身份，重复会被 `Duplicate` 拒）。
- 一半子流**双向**（两侧各发不同文件）、一半**单向**。
- 「文件」用确定性 PRNG 生成 8 MiB（可选 `--from-file` 读真实文件），避免磁盘 IO
  成为变量。
- 结束时必须在**最后一条子流读到 EOF 之后**才丢弃连接——否则会踩到 §2.3 那条纪律。

### 3.4 「可靠」的判据

| # | 判据 |
| --- | --- |
| 1 | 每条子流收到的字节数 == 源长度 |
| 2 | 每条子流的 CRC32 == 源 CRC32（复用已有 `crc` 依赖，不引新依赖） |
| 3 | 该子流 `abort_reason()` 为 `None`（没有被误拆） |
| 4 | EOF 语义正确：对端 `FIN` 之后读到 `Closing` |
| 5 | 三条连接全程存活（证明保活与流控协同工作） |
| 6 | 三个进程退出码全 0；脚本总超时（如 120 s）反证「无挂起」 |

### 3.5 分轮压测

| 轮次 | 变量 | 想看什么 |
| --- | --- | --- |
| R1 | 基线：8 子流 × 8 MiB | 正确性 + 吞吐基线 |
| R2 | 接收端每 64 KiB 停 10 ms | 窗口耗尽、`WINDOW_UPDATE`、背压是否活；顺带量 §2.4 的队头阻塞 |
| R3 | 某条子流接收端 `drop(rx)` | 对端应拿到 `RESET`，而不是挂起或连接级失败 |
| R4 | `max_packet_size` 4 KiB ↔ 64 KiB | 帧大小对吞吐与公平性的影响（§2.7） |
| R5 | 传输中途对端静默 2 个 `timeout` 周期 | 空闲超时只拆该子流、连接不死 |

### 3.6 实现要点与前置缺口

- **硬前置**：§2.2 的 smol 装配（feature + UDS 薄适配）。tokio / compio 两端复用现成的
  `buffex_*_adapt` 与调用方泵。
- **三个可执行**：同一份场景代码，按 feature 编译三次
  （`test-tokio-runtime` / `test-compio-runtime` / `test-smol-runtime`）；角色、监听路径、
  对端列表走命令行参数。
- **编排脚本**（just 配方或 shell）：生成文件 → 起三个进程 → 收退出码 →
  汇总每流的长度 / CRC / 吞吐 → 判定并打印失败明细。
- **常见坑**：dock 对唯一；连接活到数据上网；tokio / smol 侧记得 `scope.run_until`。

### 3.7 实施顺序

1. **先 compio + tokio 两端跑通**：这两端的适配与 `ScopeHost` 都现成，不需要新 feature，
   先把实验装置本身验对；
2. **再补 smol 装配**接上第三端（这一步动公开面：新增 feature 与一个适配模块）；
3. **最后加 R2–R5**：把待测项变成数字，并沉淀成可重复的验收目标。

---

## 4. 与现有验收的分工

| 现有验收 | 覆盖 | 本实验补的面 |
| --- | --- | --- |
| `inmem_mux.rs` | 多子流、流控、半关闭（内存环） | 真实 UDS + 跨进程 + 大数据量 |
| `smoke_tokio/compio.rs` | 端到端冒烟（真实 socket） | 多连接、多后端两两互通 |
| `keepalive*.rs` | 保活与空闲超时（虚拟时间） | 传输进行中的保活行为 |
| `layered_rpc.rs` | 公开 API 的可分层性 | 真实部署形态下的分层 |
| `alloc_count.rs` | 分配预算 | 大流量下的分配增长 |
