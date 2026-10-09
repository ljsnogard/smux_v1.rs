# 建流裁决的超时与取消：从「拆流」到「标记 + 告知对端 + 告知调用者」

> 本文记录本轮按人类裁决补上的两条行为：**建流未裁决时的超时**，以及**裁决被取消时
> 向对端主动告知**。它是 `outlook-production-readiness-20261006-2058.md` §2.6 与
> `timer-mock-clock-and-generic-drop-20261006-1625.md` §10.5 那条裁决的落地。

---

## 1. 人类裁决（本轮需求原文）

1. **超时**：「accept 或者 reject 都有超时失败的可能性，对端建流请求到达本端时，就应该
   开始启动一个计时，超过这个计时没有 accept 或者 reject 就应该标记对应的
   `ChannelHandle` 为超时，等真正 accept 或者 reject 的时候就可以直接将这个错误结果
   告知调用者。」
2. **取消**：「accept 或者 reject 是带着 cancel token 进行，并且 cancel token 在
   accept 或者 reject 真正完成之前就收到 cancellation signal，这种情况下调用者显然
   是得到 `Cancelled` 错误的同时，MuxConn 也应该向对端主动通知为超时或者拒绝。」

三个待定项当场拍板：**超时到点即向对端发 `REJECT` 并释放身份**（不留悬空对端）；
**时长沿用 `max_channel_timeout`**（本端策略，不进协议）；**发起方与响应方两侧都覆盖**。

## 2. 动手前的现状（与需求的差距）

| 事实 | 位置 |
| --- | --- |
| 计时扫描**已经**覆盖未裁决的子流（`BindingSlot_::Channel` 全扫），到点走 `claim_abort_(IdleTimeout)` | `registry_.rs` 的 `timer_scan_` |
| 但到点动作是 **`Abort`**：发 `CLOSE(FIN)` + `CLOSE(RESET)`、进拆流宽限——对端此刻还在等裁决，收到的是「一条已经建好的流被关掉」 | `timer_.rs` 的 `deliver_action_` |
| `wait_establish_` 只有三个出口：取消令牌、连接级失败、`establish_outcome_`（`ACCEPT`/`REJECT` 帧）——**没有超时出口** | `owner_.rs` 的 `wait_establish_` |
| `set_establish_outcome_` 全仓只有一处在写（收到 `ACCEPT`/`REJECT` 帧） | `session_.rs` |
| `ChannelHandle` 读不到中止原因（只有两个半部有 `abort_reason()`） | `channel_handle/handle_.rs` |
| `accept` / `reject` 的取消路径**什么都不发**：响应方取消后对端永久停在等裁决上；发起方取消还会让对端以为建流成功 | `handle_.rs` 的 `mux_accept_async_` / `mux_reject_async_` |

实测（本轮开工前的临时探针，跑完即删）：A 侧 `max_channel_timeout = 1 s`、B 侧 1 h、
B 只监听不 accept，A 的 `accept_async` 在推过 **3.2 s** 虚拟时间后仍 `Pending`。

## 3. 设计

一条存活时钟（`active_millis_`）、一个 `max_channel_timeout`，按**是否已裁决**分两种到点：

```text
存活时钟到顶
├─ 建流未裁决（ESTABLISH_SETTLED = 0）
│    ⇒ TimerAction_::RejectEstablish
│       ① 向对端发 REJECT（把它从等裁决里放走）
│       ② release_channel_（进拆流宽限，在途帧静默丢弃）
│       ③ claim_abort_(IdleTimeout) 已经写下的原因留给调用方
│       ④ 唤醒建流等待方（wait_establish_ 见到 is_aborted_ ⇒ IdleTimeout）
└─ 已裁决（ESTABLISH_SETTLED = 1）
     ⇒ TimerAction_::Abort（现状不变：FIN + RESET 拆流）
```

「是否已裁决」用 owner 上新增的一个位（`ESTABLISH_SETTLED`，`flags_` 的 `1 << 13`）表示，
在 `accept` / `reject` 收尾处置位。

取消的处置与超时同构：**只要本端参与过这条子流的建流，就必须向对端把话说清楚**。

| 场景 | 对端知道这条流吗 | 本端动作 | 调用方得到 |
| --- | --- | --- | --- |
| 发起方取消，且**还没发** `OPEN` | 不知道 | `unreserve_channel_`（撤销预留） | `Cancelled` |
| 发起方取消，**已发** `OPEN` | 知道（可能已回 `OPEN`/`ACCEPT`） | 新增 `reject_after_open_`：发 `REJECT` + `ReleaseChannel` | `Cancelled` |
| 响应方取消（`OPEN` 必然已到） | 正在等裁决 | `abort_pending_`：发 `REJECT` + `ReleaseChannel` | `Cancelled` |
| `reject` 途中取消 | 正在等裁决 | 照发 `REJECT`（理由载荷取已读到的部分） | `Cancelled` |
| 建流已超时 | 已收到 `REJECT` | 不再发帧（计时循环发过了） | `IdleTimeout` |

## 4. 实现清单

| 文件 | 改动 |
| --- | --- |
| `src/connection/owner_.rs` | 新增 `ESTABLISH_SETTLED` 位与 `set_establish_settled_()` / `establish_settled_()`；`wait_establish_` 增加「已中止 ⇒ `MuxError::IdleTimeout`」出口 |
| `src/connection/mux_connection/registry_.rs` | `timer_scan_` 的存活到顶分支按 `establish_settled_` 分流（未裁决 ⇒ 新动作） |
| `src/connection/timer_.rs` | 新增 `TimerAction_::RejectEstablish` 与其投递（发 `REJECT` + `release_channel_` + 置「已裁决」+ `notify_establish_`） |
| `src/connection/channel_handle/handle_.rs` | 新增公开 `ChannelHandle::abort_reason()`；`mux_accept_async_` 补「入口超时检查」「入口取消检查」「响应方发 `ACCEPT` 前的取消检查」「取消出口回 `REJECT`」；`mux_reject_async_` 补「入口超时检查」「取消也照发 `REJECT`」；新增 `reject_after_open_` |
| `src/connection/error_.rs` | `MuxError::IdleTimeout` 的文档扩为「两个阶段」 |
| `tests/common/keepalive_common.rs` + 两个壳 | 新增三条端到端用例（见下） |

**错误类型复用 `MuxError::IdleTimeout`**，不新增公开变体：调用点本身就能区分阶段
（`accept`/`reject` 返回的它 = 建流阶段；两个半部 `abort_reason()` 里的它 = 活跃阶段）。

## 5. 新用例

| 用例 | 它钉住什么 |
| --- | --- |
| `establish_timeout_on_initiator_` | 发起方发出 `OPEN` 后对端不裁决：1.6 s 虚拟时间内 `accept_async` 必须返回 `IdleTimeout`，且句柄 `abort_reason()` 同值（就是本轮开工前实测会永久挂住的那条路径） |
| `establish_timeout_on_responder_` | 响应方收到 `OPEN` 后不裁决：连接代它回 `REJECT` ⇒ 对端拿到 `Refused`；它自己随后调 `accept_async` 拿到 `IdleTimeout` |
| `cancel_accept_notifies_peer_` | 带**已取消**令牌的 `accept_async`：调用方拿 `Cancelled`，**同时**对端拿 `Refused`（证明 `REJECT` 真的发出去了） |

两个壳（`tests/keepalive.rs` / `tests/keepalive_compio.rs`）各自新增这三条。

## 6. 实测

| 装配 | 结果 |
| --- | --- |
| compio 侧（`--no-default-features --features test-compio-runtime --test keepalive_compio`） | 5 passed（含三条新用例） |
| tokio 侧（`--no-default-features --features test-tokio-runtime --test keepalive`） | 5 passed（含三条新用例） |
| 单元测试（`cargo test --lib`） | 201 passed（新增 `timer_scan_rejects_unsettled_establish_on_timeout`；原 `timer_scan_walks_pulse_then_abort` 改为显式标记「已裁决」后仍验拆流） |
| 缺省全量（`cargo test --all-targets`） | 见提交说明 |

## 7. 遗留

1. **连接级握手仍无超时**：`HandshakeAgent` 的 invite / listen 等待循环只看取消令牌。
   outlook §2.6 的前半句（「握手本身无超时」）仍然成立，本轮只解决了**子流建流**这一段。
2. `REJECT` 的理由载荷在超时 / 取消路径上是空的（正常 reject 路径仍带调用方给的理由）；
   协议消息表里 `REJECT` 的载荷目前也没有消费者（见 `connection` 模块文档 §2.11）。
3. 建流超时与活跃子流空闲超时**共用** `max_channel_timeout`。若将来需要分别配置，
   再按 §10.5 的同一条原则（本端策略、不进协议）加一个本端字段即可。
