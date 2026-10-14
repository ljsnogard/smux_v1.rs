# smux_v1_listener

Provide cross-runtime smux_v1 listeners implementation.

Currently it support `compio`, `smol`, and `tokio`.

## 它做什么

一次服务端连接建立是**两段**：在一条已建立的字节流上完成 smux v1 握手，然后把协商结果
与两条半边接成一条复用连接。本 crate 把这两段对**每一条**连入的连接自动跑一遍，并处理
「这条连接根本不是 smux 客户端」的情况——调用方拿到的是一条条**已经握手完成、可以直接
使用**的连接。

## 用法

需要自己准备三样东西，然后把它们与监听套接字一起交给
[`SmuxListener`](src/listening/listener_.rs)：

| 要准备的 | 契约 | 回答什么 |
| --- | --- | --- |
| 连接装配策略 | `TrListenerConfig` | 握手条件、连接用哪套资源策略、帧暂存缓冲从哪来 |
| 入向适配 | `TrInbound` | 从哪个 socket 接受连接、怎么把它接上传输环 |
| 协商策略工厂 | `TrNegotiatorFactory` | 每次握手交给谁去裁决 |

入向适配不必自己写：`runtime` 模块为三套运行时 × 两种传输各给了一个现成实现。

```rust,ignore
use smux_v1_listener::{config::TrListenerConfig, listening::SmuxListener, runtime};

// ① 握手条件 + 连接策略 + 缓冲工厂
let cfg = MyCfg::new(/* … */);
// ② 协商策略：`smux_v1` 自带的 `AcceptAllEntries` 就可以直接用
let neg = AcceptAll;
// ③ 选传输：`runtime::TcpTransport` / `runtime::UdsTransport`
let listener = SmuxListener::new_(runtime::TcpTransport::new(tcp_listener), cfg, neg, scope);

while let Ok(conn) = listener.next_async().await {
    // `conn` 已经是握手完成的 `MuxConnection`
}
```

## 交付模型

- **握手并发进行**：每个「接受槽位」拿到连接后立刻把握手交给一条独立任务，自己回去
  接受下一条；
- **按完成顺序交付**：握完的连接排进一条 FIFO 队列，`next_async` 每次取队首。于是
  一条慢握手不会挡住后面已经握完的连接；
- **惰性**：接受槽位由 `next_async` 按需投递——调用方不拉取，就没有人在接受连接；
- **背压**：同时在途的条数由 `TrListenerConfig::max_inflight` 界定（缺省 16），取满
  之后不再接受新连接，内核的 accept 队列替本端挡着。

## 出错时的行为

| 情形 | 行为 |
| --- | --- |
| 不是 smux 帧、CRC 不匹配、协商被拒、对端半途关闭 | **静默丢弃**这条连接，继续接受下一条 |
| 从监听套接字接受失败（如 `EMFILE`） | 以 `Err(SmuxListenerError::Accept)` 交给调用方——listener 自己已经产不出连接了 |
| 本次等待被取消 | 以 `Err(SmuxListenerError::Cancelled)` 结束**这一次**等待；listener 仍可用，已排队的连接不丢 |

## feature

`rt-tokio` / `rt-compio` / `rt-smol` 各自点亮一套设备适配（缺省 `rt-compio`）。
**运行时选择不由 feature 决定连接行为**：连接的运行时值与本地作用域都由
`TrListenerConfig` 交出，feature 只决定「哪几套适配参与编译」。
