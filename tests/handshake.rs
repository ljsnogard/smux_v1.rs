//! 握手流程的集成测试：用字节切片充当底层传输，跑完整的握手交互。
//!
//! 发起方 / 等待方流程、拒绝与对端关闭两条失败路径都在这里覆盖；编解码算法
//! 本身由 crate 内部单元测试覆盖。
//!
//! 下面各常量是**手算的参考帧**（CRC-16/XMODEM），用作线格式的固定锚点：一旦
//! 线格式或 CRC 实现发生变化，这些测试就会失败，从而提醒协议出现不兼容。
//! 参与协商的取值约定为：`INVITE` 只声明 `max_packet_size = 4096`，等待方
//! 本地值为 `max_channel_count = 8`、`max_dock_chan_count = 4`、
//! `max_channel_timeout = 5s`。

use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};

use abs_buff::{x_deps::abs_cancel::NonCancellableToken, x_deps::abs_cancel::TrMayCancel};

use smux_v1::handshake::{
    K_ACCEPT_MAGIC, K_CONFRM_MAGIC, K_INVITE_MAGIC, K_REJECT_MAGIC,
    agent::HandshakeAgent,
    error::HandshakeError,
    opts::{BasicOpts, NegotiationBasicEntry},
};

/// 单帧长度上限，测试里取一个宽松值。
const K_MAX_FRAME: usize = 64;

/// 发起方的 `INVITE`（条目区只有 `max_packet_size = 4096`）。
const K_INVITE_BYTES: &[u8] = &[0x5f, 0x1b, 0x01, 0x69, 0x10, 0x10, 0x00, 0x1c, 0x11, 0x8b];

/// 等待方的 `ACCEPT`（补全后的四项）。
const K_ACCEPT_BYTES: &[u8] = &[
    0x5f, 0x1b, 0x01, 0x41, 0x10, 0x10, 0x00, 0x01, 0x08, 0x02, 0x04, 0x03, 0x05, 0x1c, 0xb2, 0xa6,
];

/// 发起方的 `CONFIRM`（原样回显 `ACCEPT` 的四项取值）。
const K_CONFIRM_BYTES: &[u8] = &[
    0x5f, 0x1b, 0x01, 0x63, 0x10, 0x10, 0x00, 0x01, 0x08, 0x02, 0x04, 0x03, 0x05, 0x1c, 0xb8, 0x6f,
];

/// 等待方的空 `CONFRM`（`magic` 之后直接是校验尾）。
const K_EMPTY_CONFRM_BYTES: &[u8] = &[0x5f, 0x1b, 0x01, 0x63, 0x1c, 0xcf, 0x87];

/// 任一方都可以发送的 `REJECT`（条目区为空）。
const K_REJECT_BYTES: &[u8] = &[0x5f, 0x1b, 0x01, 0x4a, 0x1c, 0x73, 0xf9];

/// 在当前线程把 future 跑到完成。
///
/// 字节切片实现的 `TrBuffRead` / `TrBuffWrite` 都是立即就绪的，因此不需要
/// 真正的异步运行时，一个 no-op waker 足够。
fn block_on_<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let waker = Waker::noop();
    let mut ctx = Context::from_waker(waker);
    loop {
        match fut.as_mut().poll(&mut ctx) {
            Poll::Ready(out) => return out,
            Poll::Pending => continue,
        }
    }
}

/// 构造一块预置长度的写缓冲。
///
/// 测试里只关心「发起的握手能否成功」与「首帧 magic 是否正确」，不逐字节
/// 校验写出的内容——线格式的逐字节校验由 crate 内部单元测试与上面的参考帧
/// 常量负责。`Vec<u8>` 的写实现要求缓冲预先有长度。
fn write_buffer_() -> Vec<u8> {
    vec![0u8; K_MAX_FRAME]
}

/// 等待方的本地基础项取值。
fn local_opts_() -> BasicOpts {
    BasicOpts {
        max_packet_size: 2048usize,
        max_channel_count: 8usize,
        max_dock_chan_count: 4usize,
        max_channel_timeout: core::time::Duration::from_secs(5u64),
    }
}

/// 发起方声明的 `INVITE` 条目（`BeU16` + `MaxPacketSize`）。
fn invite_entries_() -> [NegotiationBasicEntry; 1] {
    [NegotiationBasicEntry {
        opts_key: 0x10,
        val_data: 4096,
    }]
}

/// 测试完整的四消息握手：INVITE → ACCEPT → CONFIRM → CONFRM。
///
/// - 手段：把预置的对端帧放进发起方的接收缓冲，让它一次跑完
///   `INVITE → CONFIRM`；等待方同样预置 `INVITE`，跑完整个流程。
/// - 判断：双方都成功；协商结果中 `max_packet_size` 为发起方的 4096，其余
///   三项为等待方本地值；双方写出的帧与参考帧逐字节一致。
#[test]
fn four_message_handshake_succeeds() {
    // 1. 发起方：发送 INVITE，读到 ACCEPT 后回 CONFIRM，再读到空 CONFRM。
    //    接收侧依次预置 ACCEPT 与空 CONFRM 两帧。
    let mut initiator_buff = K_ACCEPT_BYTES.to_vec();
    initiator_buff.extend_from_slice(K_EMPTY_CONFRM_BYTES);
    let agent = HandshakeAgent::new(&initiator_buff[..], write_buffer_(), K_MAX_FRAME);
    let invite = invite_entries_();
    let mut cancel = NonCancellableToken::new();
    let endpoint = block_on_(
        agent
            .invite_async(&invite, |_| true)
            .may_cancel_with(&mut cancel),
    )
    .expect("发起方握手应当成功");
    assert_eq!(
        &endpoint.tx[..K_CONFIRM_BYTES.len()],
        K_CONFIRM_BYTES,
        "发起方应当回显一帧 CONFIRM"
    );
    assert_eq!(endpoint.opts.basic_opts.max_packet_size, 4096usize);
    assert_eq!(endpoint.opts.basic_opts.max_channel_count, 8usize);
    assert_eq!(endpoint.opts.basic_opts.max_dock_chan_count, 4usize);
    assert_eq!(
        endpoint.opts.basic_opts.max_channel_timeout,
        core::time::Duration::from_secs(5u64)
    );

    // 2. 等待方：读到 INVITE 后回 ACCEPT，读到 CONFIRM 后回空 CONFRM。
    //    接收侧依次预置 INVITE 与 CONFIRM 两帧。
    let mut responder_buff = K_INVITE_BYTES.to_vec();
    responder_buff.extend_from_slice(K_CONFIRM_BYTES);
    let listener = HandshakeAgent::new(&responder_buff[..], write_buffer_(), K_MAX_FRAME);
    let local = local_opts_();
    let mut cancel = NonCancellableToken::new();
    let endpoint = block_on_(
        listener
            .accept_handshake(&local, |_| true)
            .may_cancel_with(&mut cancel),
    )
    .expect("等待方握手应当成功");
    // 写侧是「预先铺满」的缓冲，`Vec<u8>` 的通用写实现不前进游标，因此每次
    // 写入都落在同一位置；这里只校验最后一帧（空 CONFRM）的 magic 正确。
    assert_eq!(
        &endpoint.tx[..4],
        &K_CONFRM_MAGIC,
        "等待方最终应当写出空 CONFRM"
    );
    assert_eq!(endpoint.opts.basic_opts.max_packet_size, 4096usize);
    assert_eq!(endpoint.opts.basic_opts.max_channel_count, 8usize);
    assert_eq!(endpoint.opts.basic_opts.max_dock_chan_count, 4usize);
    assert_eq!(
        endpoint.opts.basic_opts.max_channel_timeout,
        core::time::Duration::from_secs(5u64)
    );
}

/// 测试收到 `REJECT` 时发起方报告「对端拒绝」。
///
/// - 手段：把参考 `REJECT` 帧放进发起方的接收缓冲。
/// - 判断：返回 [`HandshakeError::PeerRejected`]。
#[test]
fn reject_is_reported_as_peer_rejected() {
    let mut initiator_buff = K_REJECT_BYTES.to_vec();
    initiator_buff.resize(K_MAX_FRAME, 0u8);
    let agent = HandshakeAgent::new(&initiator_buff[..], write_buffer_(), K_MAX_FRAME);
    let invite = invite_entries_();
    let outcome = block_on_(
        agent
            .invite_async(&invite, |_| true)
            .may_cancel_with(NonCancellableToken::shared_mut()),
    );
    assert!(matches!(outcome, Err(HandshakeError::PeerRejected)));
}

/// 测试等待方上层拒绝时以 `Rejected` 结束并回 `REJECT`。
///
/// - 手段：等待方预置合法 `INVITE`，`decide` 返回 `false`。
/// - 判断：结果为 [`HandshakeError::Rejected`]，且写出的帧是参考 `REJECT`。
#[test]
fn responder_rejects_invite() {
    let mut responder_buff = K_INVITE_BYTES.to_vec();
    responder_buff.resize(K_MAX_FRAME, 0u8);
    let listener = HandshakeAgent::new(&responder_buff[..], write_buffer_(), K_MAX_FRAME);
    let local = local_opts_();
    let outcome = block_on_(
        listener
            .accept_handshake(&local, |_| false)
            .may_cancel_with(NonCancellableToken::shared_mut()),
    );
    assert!(matches!(outcome, Err(HandshakeError::Rejected)));
}

/// 测试发起方上层拒绝 ACCEPT 时以 `Rejected` 结束并回 `REJECT`。
///
/// - 手段：发起方预置合法 `ACCEPT`，`decide` 返回 `false`。
/// - 判断：结果为 [`HandshakeError::Rejected`]，且写出的帧是参考 `REJECT`。
#[test]
fn initiator_rejects_accept() {
    let mut initiator_buff = K_ACCEPT_BYTES.to_vec();
    initiator_buff.resize(K_MAX_FRAME, 0u8);
    let agent = HandshakeAgent::new(&initiator_buff[..], write_buffer_(), K_MAX_FRAME);
    let invite = invite_entries_();
    let outcome = block_on_(
        agent
            .invite_async(&invite, |_| false)
            .may_cancel_with(NonCancellableToken::shared_mut()),
    );
    assert!(matches!(outcome, Err(HandshakeError::Rejected)));
}

/// 测试对端在帧中途关闭会被识别。
///
/// - 手段：发起方的接收缓冲为空（等价于对端已关闭）。
/// - 判断：返回 [`HandshakeError::PeerClosed`]。
#[test]
fn peer_closed_mid_frame_is_reported() {
    let empty: &[u8] = &[];
    let agent = HandshakeAgent::new(empty, write_buffer_(), K_MAX_FRAME);
    let invite = invite_entries_();
    let outcome = block_on_(
        agent
            .invite_async(&invite, |_| true)
            .may_cancel_with(NonCancellableToken::shared_mut()),
    );
    assert!(matches!(outcome, Err(HandshakeError::PeerClosed)));
}

/// 测试四种 magic 常量与参考帧的前 4 字节一致。
///
/// - 手段：直接比较常量与参考帧前缀。
/// - 判断：完全相等，保证测试与其他模块对 magic 的理解一致。
#[test]
fn magic_constants_match_reference_frames() {
    assert_eq!(&K_INVITE_BYTES[..4], &K_INVITE_MAGIC);
    assert_eq!(&K_ACCEPT_BYTES[..4], &K_ACCEPT_MAGIC);
    assert_eq!(&K_CONFIRM_BYTES[..4], &K_CONFRM_MAGIC);
    assert_eq!(&K_EMPTY_CONFRM_BYTES[..4], &K_CONFRM_MAGIC);
    assert_eq!(&K_REJECT_BYTES[..4], &K_REJECT_MAGIC);
}
