//! 建流期 message ring：每个**未裁决** channel 一对小容量普通 ring。
//!
//! `open` 的 message、`accept` 的 welcome、`reject` 的 reason 都要在「调用方缓冲」与
//! 「帧」之间搬运。现状用 `ControlFrame_::payload_: Vec<u8>` /
//! `ChannelHandle::message_: Vec<u8>` 缓存，每次建流 / 拒绝一次临时堆分配。本模块把它们
//! 换成**每个 pending channel 一对 ring**：
//!
//! - app 侧 [`PendingMsgApp_`]：写 outgoing（`OPEN` message / welcome / reason）、
//!   读 incoming（对端 `OPEN` message / `ACCEPT` welcome / `REJECT` reason）；
//! - main 侧 [`PendingMsgMain_`]：镜像的两半，由 `main_loop` 独占。
//!
//! ring 是单向的，所以「收发 message」实际是**两个方向各一条** ring，而不是一条双向
//! ring。每条 message 用**分块格式**（与公共子流 / `abs_mptp` 的分块体一致）：
//!
//! ```text
//! message    = *chunk last-chunk
//! chunk      = chunk-size(2 字节大端 u16) chunk-data
//! last-chunk = 0x0000
//! ```
//!
//! 内容对 smux 不透明，由调用方自行解释；smux 只负责切块（写入侧）与重组（
//! `read_message_async` 逐块写进调用方的 sink）。每个 pending channel 的每个方向
//! **至多一条**消息，因此 ring 容量按「`cap` 字节内容 + 每块 2 字节前缀 + 2 字节终止
//! 块」的最大值算（见 [`PublicChunkIo_::framed_message_len_`]）。
//!
//! # 容量
//!
//! `cap` 是 message **内容**上限（字节），由 `TrConnCfg::pending_msg_capacity()` 决定
//! （默认 [`K_PENDING_MSG_CAP_DEFAULT`]）。实际 ring 容量是
//! `framed_message_len_(cap)`：`cap == 0` 时为 2（只需要一个 `0x0000`）。
//!
//! # 为什么不用一条共享 ring
//!
//! `Listener` 若让所有入向请求共享一条 ring，容量就重新变成「按并发请求数争用」——
//! 这正是本方案要消掉的问题。因此每个 pending channel 建一对，`Listener` 只携带配置。

// 过渡：P0 阶段只落原语、不接线；接入建流请求后删除本行。
#![allow(dead_code)]

use core::{
    alloc::AllocatorClone,
    task::{Context, Poll},
};

use abs_buff::Demand;
use abs_cancel::{NonCancellableToken, TrMayCancel};

use mm_ptr::Owned;

use crate::{
    connection::{
        public_channel_::{ChunkReadErr_, ChunkReadState_, ChunkWriteErr_, PublicChunkIo_},
        ring_::{BufferedRx, BufferedTx, RingBuildErr, new_buffered_channel},
    },
    x_deps::{abs_buff, abs_cancel, mm_ptr},
};

/// 每个方向的 message 载荷默认上限（字节）。
pub(crate) const K_PENDING_MSG_CAP_DEFAULT: usize = 512usize;

/// 应用侧持有的两半：写 outgoing、读 incoming。
pub(crate) struct PendingMsgApp_ {
    /// app 写 -> main 读（`OPEN` message / welcome / reason）。
    tx_: BufferedTx,

    /// main 写 -> app 读（对端 message / welcome / reason）。
    rx_: BufferedRx,
}

/// 主循环持有的两半（与 [`PendingMsgApp_`] 共享同一对 ring）。
pub(crate) struct PendingMsgMain_ {
    /// 读 app 写的 outgoing。
    tx_: BufferedRx,

    /// 写 incoming 给 app。
    rx_: BufferedTx,
}

/// 为一个 pending channel 建一对 message ring。
///
/// 返回 `(app 侧, main 侧)`；两条 ring 各 `max(2, 2 + cap)` 字节。建环失败时已建好的
/// 那条随返回值一起释放，调用方按「身份登记失败 / 回 REJECT」处理，不让整条连接失败。
///
/// # Errors
///
/// 容量低于 ring 原语下限（[`RingBuildErr::Capacity`]）或分配失败
/// （[`RingBuildErr::Alloc`]）。
pub(crate) fn new_pending_msg_<A>(
    cap: usize,
    alloc: A,
) -> Result<(PendingMsgApp_, PendingMsgMain_), RingBuildErr>
where
    A: AllocatorClone + Send + Sync,
{
    let ring_cap = PublicChunkIo_::framed_message_len_(cap).max(2usize);
    let (out_tx, out_rx) = new_buffered_channel(Owned::new_uninit_slice(ring_cap, alloc.clone()))?;
    let (in_tx, in_rx) = new_buffered_channel(Owned::new_uninit_slice(ring_cap, alloc))?;
    Result::Ok((
        PendingMsgApp_ {
            tx_: out_tx,
            rx_: in_rx,
        },
        PendingMsgMain_ {
            tx_: out_rx,
            rx_: in_tx,
        },
    ))
}

impl PendingMsgApp_ {
    /// 写 outgoing message（`open` 的 message / `accept` 的 welcome / `reject` 的 reason）。
    ///
    /// 内容是调用方自己的格式；本函数只做「内容 → 分块」（`*chunk + 0x0000`）。
    /// 返回 [`ChunkWriteErr_::WouldBlock`] 表示 ring 里已有的一条消息还没被 main
    /// 取走；正常协议下每个方向至多一条，不会发生。
    pub(crate) fn write_outgoing_(
        &mut self,
        message: &[u8],
        cap: usize,
    ) -> Result<(), ChunkWriteErr_> {
        PublicChunkIo_::try_write_chunked_message_(&mut self.tx_, message, cap)
    }

    /// 读 incoming message 的长度前缀；`Some(len)` 表示下一块有 `len` 字节，
    /// `Some(0)` 是**终止块**（整条 message 结束）。
    pub(crate) fn read_prefix_incoming_(
        &mut self,
        state: &mut ChunkReadState_,
        cap: usize,
    ) -> Result<Option<usize>, ChunkReadErr_> {
        PublicChunkIo_::try_read_prefix_(&mut self.rx_, state, cap)
    }

    /// 等 incoming ring 出现可读字节（不消费）。供 `read_message_async` 的 park 点用。
    pub(crate) fn poll_incoming_readable_(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let demand = Demand::at_least(1usize);
        let fut = self
            .rx_
            .read_async(&demand)
            .may_cancel_with(NonCancellableToken::new());
        let mut fut = core::pin::pin!(fut);
        match core::future::Future::poll(fut.as_mut(), cx) {
            Poll::Ready(_) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }

    /// 把 incoming message 的内容读进 `dst`（最多 `state.left_()` 字节）。
    pub(crate) fn read_body_incoming_(
        &mut self,
        state: &mut ChunkReadState_,
        dst: &mut [u8],
    ) -> Result<usize, ChunkReadErr_> {
        PublicChunkIo_::try_read_payload_into_(&mut self.rx_, state, dst)
    }
}

impl PendingMsgMain_ {
    /// app 写好的**分块编码**在 outgoing ring 里占多少字节（0 = 没有消息）。
    ///
    /// app 侧一次写完、main 只在之后才读，因此 ring 的 `data_size()` 就是整条
    /// 分块 message 的长度。
    pub(crate) fn outgoing_framed_len_(&self) -> usize {
        self.tx_.ring_state().data_size()
    }

    /// 把 app 写好的分块 message **原样**读进 `dst`（不解析内容）。
    pub(crate) fn read_raw_outgoing_into_(
        &mut self,
        dst: &mut [u8],
    ) -> Result<usize, ChunkReadErr_> {
        PublicChunkIo_::try_read_available_into_(&mut self.tx_, dst)
    }

    /// 把对端 message 的**分块编码原样**写进 incoming ring（main 不理解内容）。
    pub(crate) fn write_incoming_(
        &mut self,
        framed: &[u8],
        cap: usize,
    ) -> Result<(), ChunkWriteErr_> {
        PublicChunkIo_::try_write_raw_(
            &mut self.rx_,
            framed,
            PublicChunkIo_::framed_message_len_(cap),
        )
    }
}

#[cfg(test)]
mod tests_ {
    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use crate::connection::public_channel_::K_MAX_CHUNK_PAYLOAD;

    use super::*;

    /// 读一整条 incoming 分块 message 的内容：循环到终止块，逐块取内容。
    fn drain_incoming_(app: &mut PendingMsgApp_, cap: usize) -> std::vec::Vec<u8> {
        let mut state = ChunkReadState_::new_message_();
        let mut out = std::vec::Vec::new();
        loop {
            let len = app
                .read_prefix_incoming_(&mut state, cap)
                .expect("读前缀")
                .expect("整条 message 已写入，前缀应当可读");
            if len == 0usize {
                break;
            }
            let mut chunk = std::vec![0u8; len];
            let mut filled = 0usize;
            while filled < len {
                let n = app
                    .read_body_incoming_(&mut state, &mut chunk[filled..])
                    .expect("读内容");
                assert!(n > 0usize, "整条 message 已写入，块读不应空转");
                filled += n;
            }
            out.extend_from_slice(&chunk);
        }
        out
    }

    /// 测试 app -> main：内容按 `*chunk + 0x0000` 编码，main 原样读出分块字节。
    ///
    /// - 手段：`cap=8` 建一对 ring；app 写 `b"hi"`；main 读分块长度与原样字节。
    /// - 判断：分块长度为 `6`（`2 前缀 + 2 内容 + 2 终止`），字节为
    ///   `00 02 h i 00 00`；读空之后 ring 长度归零。
    ///
    /// Test goal: app-to-main content is encoded as `*chunk + 0x0000` and copied
    /// verbatim by `main`.
    /// Method: build a pair with `cap=8`; app writes `b"hi"`; main reads the framed
    /// length and the raw framed bytes.
    /// Assertion: framed length is 6 and bytes are `00 02 h i 00 00`; after the read
    /// the ring is empty.
    #[test]
    fn pending_msg_outgoing_is_chunked_() {
        let (mut app, mut main) = new_pending_msg_(8usize, CoreAlloc).expect("建环");

        app.write_outgoing_(b"hi", 8usize).expect("写 outgoing");
        assert_eq!(main.outgoing_framed_len_(), 6usize);
        let mut framed = [0u8; 6usize];
        let mut filled = 0usize;
        while filled < framed.len() {
            let n = main
                .read_raw_outgoing_into_(&mut framed[filled..])
                .expect("读分块");
            assert!(n > 0usize, "分块已写入，读不应空转");
            filled += n;
        }
        assert_eq!(&framed, &[0u8, 2u8, b'h', b'i', 0u8, 0u8]);
        assert_eq!(main.outgoing_framed_len_(), 0usize, "读空之后 ring 为空");
    }

    /// 测试 main -> app：分块字节原样写入 incoming，app 重组出原内容。
    ///
    /// - 手段：`cap=8`；main 写 `00 02 y o 00 00`；app 逐块读到终止块。
    /// - 判断：重组结果为 `b"yo"`。
    ///
    /// Test goal: framed bytes written verbatim by `main` are reassembled by the app.
    /// Method: with `cap=8`, main writes `00 02 y o 00 00`; the app reads chunks until
    /// the terminator.
    /// Assertion: the reassembled content is `b"yo"`.
    #[test]
    fn pending_msg_incoming_reassembles_() {
        let (mut app, mut main) = new_pending_msg_(8usize, CoreAlloc).expect("建环");

        main.write_incoming_(&[0u8, 2u8, b'y', b'o', 0u8, 0u8], 8usize)
            .expect("写 incoming");
        assert_eq!(drain_incoming_(&mut app, 8usize), b"yo");
    }

    /// 测试空 message：分块编码只有终止块 `0x0000`，重组结果为空。
    ///
    /// - 手段：`cap=8`；app 写空切片；main 读分块；再原样写回 incoming 让 app 重组。
    /// - 判断：分块长度为 2、字节为 `00 00`；app 读到终止块即结束，内容为空。
    ///
    /// Test goal: an empty message is exactly the terminator block and reassembles to
    /// empty.
    /// Method: with `cap=8`, app writes an empty slice; main reads the framed bytes and
    /// writes them back to incoming.
    /// Assertion: framed length is 2 with bytes `00 00`; the app reads the terminator
    /// immediately and yields empty content.
    #[test]
    fn pending_msg_empty_is_just_terminator_() {
        let (mut app, mut main) = new_pending_msg_(8usize, CoreAlloc).expect("建环");

        app.write_outgoing_(&[], 8usize).expect("写空 message");
        assert_eq!(main.outgoing_framed_len_(), 2usize);
        let mut framed = [0u8; 2usize];
        let mut filled = 0usize;
        while filled < framed.len() {
            filled += main
                .read_raw_outgoing_into_(&mut framed[filled..])
                .expect("读分块");
        }
        assert_eq!(framed, [0u8, 0u8]);

        main.write_incoming_(&framed, 8usize).expect("写 incoming");
        assert!(drain_incoming_(&mut app, 8usize).is_empty());
    }

    /// 测试超过 `cap` 的内容被拒绝，且不写入任何字节。
    ///
    /// - 手段：`cap=4`，app 写 5 字节。
    /// - 判断：返回 `TooLarge`；outgoing ring 仍为空。
    ///
    /// Test goal: content longer than `cap` is rejected without writing anything.
    /// Method: with `cap=4`, app writes 5 bytes.
    /// Assertion: `TooLarge`; the outgoing ring stays empty.
    #[test]
    fn pending_msg_rejects_too_long_() {
        let (mut app, mut main) = new_pending_msg_(4usize, CoreAlloc).expect("建环");

        assert_eq!(
            app.write_outgoing_(&[1u8; 5usize], 4usize)
                .expect_err("超过 cap"),
            ChunkWriteErr_::TooLarge
        );
        assert_eq!(main.outgoing_framed_len_(), 0usize);
    }

    /// 测试 `cap == 0` 时只允许空 message。
    ///
    /// - 手段：`cap=0` 建对（ring 容量 2）；写空消息成功；再写 1 字节。
    /// - 判断：空消息分块长度为 2；1 字节消息因超过 `cap` 返回 `TooLarge`。
    ///
    /// Test goal: `cap == 0` admits only the empty message.
    /// Method: build a pair with `cap=0` (ring capacity 2); write empty, then 1 byte.
    /// Assertion: empty framed length is 2; the 1-byte write is `TooLarge`.
    #[test]
    fn pending_msg_zero_cap_only_empty_() {
        let (mut app, mut main) = new_pending_msg_(0usize, CoreAlloc).expect("建环");

        app.write_outgoing_(&[], 0usize).expect("空 message");
        assert_eq!(main.outgoing_framed_len_(), 2usize);
        assert_eq!(
            app.write_outgoing_(&[1u8], 0usize)
                .expect_err("cap=0 只允许空 message"),
            ChunkWriteErr_::TooLarge
        );
    }

    /// 测试内容超过单块上限时自动切成多块，app 逐块重组得到原内容。
    ///
    /// - 手段：`cap = K_MAX_CHUNK_PAYLOAD + 7`；写满该长度的 `0x5A`；main 原样读出
    ///   整条分块并写回 incoming；app 逐块重组。
    /// - 判断：分块长度 = `2 + 65535 + 2 + 7 + 2`；重组结果逐字节等于原内容。
    ///
    /// Test goal: content longer than one chunk is split automatically and reassembles
    /// byte-for-byte.
    /// Method: `cap = K_MAX_CHUNK_PAYLOAD + 7`; write that many `0x5A`; main copies the
    /// framed bytes to incoming; the app reassembles chunk by chunk.
    /// Assertion: framed length is `2 + 65535 + 2 + 7 + 2`; content matches exactly.
    #[test]
    fn pending_msg_splits_into_multiple_chunks_() {
        const CONTENT: usize = K_MAX_CHUNK_PAYLOAD + 7usize;
        let (mut app, mut main) = new_pending_msg_(CONTENT, CoreAlloc).expect("建环");
        let body = std::vec![0x5Au8; CONTENT];
        app.write_outgoing_(&body, CONTENT).expect("写多块 message");

        let framed_len = 2usize + K_MAX_CHUNK_PAYLOAD + 2usize + 7usize + 2usize;
        assert_eq!(main.outgoing_framed_len_(), framed_len);
        let mut framed = std::vec![0u8; framed_len];
        let mut filled = 0usize;
        while filled < framed.len() {
            filled += main
                .read_raw_outgoing_into_(&mut framed[filled..])
                .expect("读分块");
        }
        assert_eq!(&framed[..2], &(K_MAX_CHUNK_PAYLOAD as u16).to_be_bytes());
        assert_eq!(&framed[2usize + K_MAX_CHUNK_PAYLOAD..][..2], &7u16.to_be_bytes());
        assert_eq!(&framed[framed_len - 2usize..], &[0u8, 0u8]);

        main.write_incoming_(&framed, CONTENT).expect("写 incoming");
        assert_eq!(drain_incoming_(&mut app, CONTENT), body);
    }
}
