# 写侧收尾契约：从「必须自驱动」到 `TrBuffFlush`

日期：2026-10-15（承接 `uds-bench-20261015-2240.md` 的 §3.1）

## 1. 起因

`smux_v1_uds_bench` 在 tokio 上跑不通握手（静默挂起：两端各自 park 在自己的读上、
无错误、无超时）。根因不是协议逻辑，而是**传输写侧的驱动模型错配**：

```text
写半边 = BuffWrite::try_new（按需驱动）──> 环 ──?──> 设备
                                            ↑
                              这一跳只由调用方的 write_async / flush_async 顺带推进
```

`smux_v1` 的复用循环把帧提交进环就走、从不替适配器调收尾入口，于是**最后一段永远留在
环里**。当时（`uds-bench` 那一轮）的绕法是"写侧必须改用 `try_new_local`（自驱动）"：
让适配器自己带一个后台泵。

这个绕法把正确性压在**构造入口的选择**上，属于"使用者责任"，而不是可检查的契约。

## 2. 决策：把"收尾"抬进抽象

上游 `abs_buff` 新增两个 trait（`abs_buff/src/writer_.rs`）：

```rust
pub trait TrBuffTryFlush<T = u8>: TrBuffTryWrite<T> {
    fn try_flush(&mut self) -> Result<(), Self::Err>;
    /// 幂等：告诉对端"写端已封口，不会再有数据"。
    fn close(&mut self) -> Result<(), Self::Err>;
}

pub trait TrBuffFlush<T = u8>: TrBuffWrite<T> + TrBuffTryFlush<T> {
    type FlushAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<(), Self::Err>>;
    type CloseAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<(), Self::Err>>;
    fn flush_async(&mut self) -> Self::FlushAsync<'_>;
    fn close_async(&mut self) -> Self::CloseAsync<'_>;
}
```

**修法**：`smux_v1` 把连接写半边（`TrConnCfg::ConnTx`）与握手入口的约束从
`TrBuffWrite<u8>` 抬到 `TrBuffWrite<u8> + TrBuffFlush<u8>`，并**自己**在正确时机收尾。
于是"必须选自驱动形态"这条要求从文档里消失：两种入口都可以。

### 2.1 每个实现的语义（"下游是谁"决定实现）

| 实现 | `try_flush` | `close` |
| --- | --- | --- |
| `buffex_tokio_adapt` / `buffex_smol_adapt`（按需 `try_new`） | 同步冲一轮；环已空 `Ok`，设备收不下 `Stuffed`，出错 `Closing` | 置 `PRODUCER_CLOSED` + 唤醒下游排空方 |
| 同上（自驱动 `try_new_local`） | 不推进也不等待：环空 `Ok`，否则 `Stuffed` | 同上（后台泵据此排空并 `shutdown`） |
| `buffex_compio_adapt`（构造即 spawn 泵） | 不推进不等待，同上 | 同上 |
| `buffex::ring::RingWriter`（全被动环） | 恒等 `Ok`（环本身就是下游） | `RingWriter::close`：**唤醒 park 在环读端上的调用方排空泵** |
| `abs_buff` 的 `&mut [u8]`（测试用字节汇） | 恒等 `Ok` | 把借用长度推进到 0 |

`flush_async` / `close_async` 一律复用各适配器**已有**的异步收尾入口（保持
`may_cancel_with` 语义），只是把它们暴露到 trait 上。

### 2.2 `smux_v1` 的三处改动

1. **握手自己推进**（`handshake/agent.rs`）：每写出一帧就 `flush_frame_async_`。
   握手是"写完就等对端回执"的形状，不 flush 就必然停在第一步（`INVITE`）。
   这一处保证了**按需形态也能建连**。
2. **连接退出封口**（`connection/session_.rs`）：复用循环的传输写半边套
   `TxSealGuard_`，退出时 `try_flush` + `close`。用守卫而不是在每条退出路径手写：
   循环有 5+ 条 `return`，靠人工记得补不可靠。这同时**修好了旧版记录的缺口**
   （"写半边退出只是 drop，全被动环不会唤醒排空方"）。
3. **约束抬到 `TrConnCfg`**（`connection/config_.rs`）+ `MuxConnection::from_delivery`
   的便捷 impl。

## 3. 验收

| 项 | 结果 |
| --- | --- |
| `cargo test -p smux_v1`（compio） | 302 单元 + 全部集成目标全绿 |
| `--features test-tokio-runtime` | 全绿（含 19 条 socket 场景、5 条 smoke） |
| `--features test-smol-runtime` | 全绿 |
| `smux_v1_listener`（rt-compio / rt-tokio / rt-smol） | 全绿 |
| `buffex_tokio_adapt` 新契约测试 `tests/flush_contract_.rs` | 4/4 |
| `buffex_compio_adapt` 新契约测试 `tests/flush_contract_.rs` | 3/3 |
| `smux_v1` 新回归 `tests/handshake_manual_form_.rs` | 1/1；**反证**：临时把 `flush_frame_async_` 改成空实现后该用例在看门狗处失败（10 s 超时） |
| `smux_v1_uds_bench` tokio 冒烟（2 格 × 1 MiB） | 通过，报告正常写出 |
| `smux_v1_socks_demo` tokio 构建 | 通过 |

新回归用例 `handshake_manual_form_.rs` 是这条契约的"哨兵"：两侧都用
`BuffWrite::try_new`（**没有任何泵**）+ 真实 UDS，握手必须在 10 s 看门狗内完成。

## 4. ⚠️ 本轮的临时设施（**必须清理**）

`abs_buff` 的这两个 trait 与三个适配器里的实现**尚未推到 gitee**，而 `smux_v1` 的依赖
写的是 gitee git 源，因此本轮在四个 workspace 根各加了一段临时 `[patch]`：

- `smux_v1/Cargo.toml`（同时覆盖 `smux_v1` 与 `smux_v1_listener`）
- `smux_v1_uds_bench/Cargo.toml`
- `smux_v1_socks_demo/Cargo.toml`

它们把 `abs_buff` / `buffex` / `buffex_*_adapt` 指向 `../buffex.rs`（相对路径按各自
workspace 深度写，分别是 `../` 与 `../../`）。

**上游推送后**：删除这三处 `[patch]` 段，然后

```bash
cargo update -p abs_buff -p buffex -p buffex_tokio_adapt -p buffex_smol_adapt -p buffex_compio_adapt
```

（三个 workspace 各跑一次；`smux_v1` 的 workspace 根覆盖前两个 crate。）

## 5. 遗留

- `smux_v1_uds_bench` 的写侧**仍**用 `try_new_local` + `Drive::with_scope`。契约生效后
  它不再是正确性前提，可以进一步简化成 `try_new`（数据面靠 `write_async` 顺带推、
  尾部靠退出封口），但这属于 bench 自身重构，未在本轮做。
- `smux_v1_listener` 的 `make_halves_` 同样仍写死 `try_new_local`；其文档中"按需形态下
  最后一段会永远留在环里"的措辞需要按新契约复核。
- `TrBuffTryFlush::close` 目前只保证"唤醒下游 / 关生产端"，**不保证设备已 `shutdown`**
  （那是 `close_async` 的事）。若将来需要"同步封口且设备已关"，需要新的同步入口。
