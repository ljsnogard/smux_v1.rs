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
//! ring。协议与公共子流相同（`[u16 大端长度][message bytes]`），只是 `0x0000` 表示
//! **空 message**（不是错误）。每个 pending channel 的每个方向**至多一条**消息，因此
//! ring 容量只需 `2 + cap`。
//!
//! # 容量
//!
//! `cap` 是 message 载荷上限（字节），由 `TrConnCfg::pending_msg_capacity()` 决定
//! （默认 [`K_PENDING_MSG_CAP_DEFAULT`]）。实际 ring 容量是 `max(2, 2 + cap)`：
//! `cap == 0` 时仍能写一个空的 `0x0000`。
//!
//! # 为什么不用一条共享 ring
//!
//! `Listener` 若让所有入向请求共享一条 ring，容量就重新变成「按并发请求数争用」——
//! 这正是本方案要消掉的问题。因此每个 pending channel 建一对，`Listener` 只携带配置。

// 过渡：P0 阶段只落原语、不接线；接入建流请求后删除本行。
#![allow(dead_code)]

use core::alloc::AllocatorClone;

use mm_ptr::Owned;

use crate::{
    connection::{
        public_channel_::{ChunkReadErr_, ChunkReadState_, ChunkWriteErr_, PublicChunkIo_},
        ring_::{BufferedRx, BufferedTx, RingBuildErr, new_buffered_channel},
    },
    x_deps::mm_ptr,
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
    let ring_cap = 2usize.saturating_add(cap).max(2usize);
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
    /// 空 message 写 `0x0000`。返回 [`ChunkWriteErr_::WouldBlock`] 表示 ring 里已有
    /// 一条尚未被 main 取走的消息；正常协议下每个方向至多一条，不会发生。
    pub(crate) fn write_outgoing_(
        &mut self,
        message: &[u8],
        cap: usize,
    ) -> Result<(), ChunkWriteErr_> {
        PublicChunkIo_::try_write_message_(&mut self.tx_, message, cap)
    }

    /// 读 incoming message 的长度前缀；`Some(len)` 表示下一条消息有 `len` 字节
    /// （`len == 0` 是空 message，调用方无需再读 body）。
    pub(crate) fn read_prefix_incoming_(
        &mut self,
        state: &mut ChunkReadState_,
        cap: usize,
    ) -> Result<Option<usize>, ChunkReadErr_> {
        PublicChunkIo_::try_read_prefix_(&mut self.rx_, state, cap)
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
    /// 读 app 写的 outgoing message 长度前缀。
    pub(crate) fn read_prefix_outgoing_(
        &mut self,
        state: &mut ChunkReadState_,
        cap: usize,
    ) -> Result<Option<usize>, ChunkReadErr_> {
        PublicChunkIo_::try_read_prefix_(&mut self.tx_, state, cap)
    }

    /// 把 app 写的 outgoing message 内容读进 `dst`。
    pub(crate) fn read_body_outgoing_(
        &mut self,
        state: &mut ChunkReadState_,
        dst: &mut [u8],
    ) -> Result<usize, ChunkReadErr_> {
        PublicChunkIo_::try_read_payload_into_(&mut self.tx_, state, dst)
    }

    /// 写 incoming message 给 app（对端 `OPEN` message / `ACCEPT` welcome / `REJECT` reason）。
    pub(crate) fn write_incoming_(
        &mut self,
        message: &[u8],
        cap: usize,
    ) -> Result<(), ChunkWriteErr_> {
        PublicChunkIo_::try_write_message_(&mut self.rx_, message, cap)
    }
}

#[cfg(test)]
mod tests_ {
    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use super::*;

    /// 测试 app -> main 方向的一条消息能原样读回。
    /// - 手段：`cap=8` 建一对 ring；app 写 `b"hi"`；main 读前缀与内容。
    /// - 判断：前缀为 `Some(2)`；内容为 `b"hi"`；state 回到 idle。
    ///
    /// Test goal: an app-to-main message round-trips.
    /// Method: build a pair with `cap=8`; app writes `b"hi"`; main reads prefix and body.
    /// Assertion: prefix is `Some(2)`; body equals `b"hi"`; state is idle.
    #[test]
    fn pending_msg_outgoing_round_trip_() {
        let (mut app, mut main) = new_pending_msg_(8usize, CoreAlloc).expect("建环");
        let mut state = ChunkReadState_::new_message_();

        app.write_outgoing_(b"hi", 8usize).expect("写 outgoing");
        assert_eq!(
            main.read_prefix_outgoing_(&mut state, 8usize)
                .expect("读前缀"),
            Option::Some(2usize)
        );
        let mut out = [0u8; 2usize];
        let n = main
            .read_body_outgoing_(&mut state, &mut out)
            .expect("读内容");
        assert_eq!(n, 2usize);
        assert_eq!(&out, b"hi");
        assert!(state.is_idle_());
    }

    /// 测试 main -> app 方向的一条消息能原样读回。
    /// - 手段：`cap=8` 建一对 ring；main 写 `b"yo"`；app 读前缀与内容。
    /// - 判断：前缀为 `Some(2)`；内容为 `b"yo"`。
    ///
    /// Test goal: a main-to-app message round-trips.
    /// Method: build a pair with `cap=8`; main writes `b"yo"`; app reads prefix and body.
    /// Assertion: prefix is `Some(2)`; body equals `b"yo"`.
    #[test]
    fn pending_msg_incoming_round_trip_() {
        let (mut app, mut main) = new_pending_msg_(8usize, CoreAlloc).expect("建环");
        let mut state = ChunkReadState_::new_message_();

        main.write_incoming_(b"yo", 8usize).expect("写 incoming");
        assert_eq!(
            app.read_prefix_incoming_(&mut state, 8usize)
                .expect("读前缀"),
            Option::Some(2usize)
        );
        let mut out = [0u8; 2usize];
        let n = app
            .read_body_incoming_(&mut state, &mut out)
            .expect("读内容");
        assert_eq!(n, 2usize);
        assert_eq!(&out, b"yo");
    }

    /// 测试空 message 编码为 `0x0000` 且可被识别。
    /// - 手段：`cap=8`，app 写空切片；main 读前缀。
    /// - 判断：前缀为 `Some(0)`；state 保持 idle；不需要读 body。
    ///
    /// Test goal: an empty message is encoded as `0x0000` and recognized.
    /// Method: with `cap=8`, app writes an empty slice; main reads the prefix.
    /// Assertion: prefix is `Some(0)`; state stays idle; no body read is needed.
    #[test]
    fn pending_msg_empty_message_() {
        let (mut app, mut main) = new_pending_msg_(8usize, CoreAlloc).expect("建环");
        let mut state = ChunkReadState_::new_message_();

        app.write_outgoing_(&[], 8usize).expect("写空 message");
        assert_eq!(
            main.read_prefix_outgoing_(&mut state, 8usize)
                .expect("读前缀"),
            Option::Some(0usize)
        );
        assert!(state.is_idle_());
    }

    /// 测试超过 `cap` 的 message 被拒绝。
    /// - 手段：`cap=4`，app 写 5 字节。
    /// - 判断：返回 `TooLarge`，ring 里没有写入任何消息。
    ///
    /// Test goal: a message longer than `cap` is rejected.
    /// Method: with `cap=4`, app writes 5 bytes.
    /// Assertion: `TooLarge`; no message is written.
    #[test]
    fn pending_msg_rejects_too_long_() {
        let (mut app, mut main) = new_pending_msg_(4usize, CoreAlloc).expect("建环");
        let mut state = ChunkReadState_::new_message_();

        assert_eq!(
            app.write_outgoing_(&[1u8; 5usize], 4usize)
                .expect_err("超过 cap"),
            ChunkWriteErr_::TooLarge
        );
        assert!(
            main.read_prefix_outgoing_(&mut state, 4usize)
                .expect("读前缀")
                .is_none()
        );
    }

    /// 测试 `cap == 0` 时只允许空 message。
    /// - 手段：`cap=0` 建对（ring 容量 2）；写空消息成功；写 1 字节消息。
    /// - 判断：空消息读回 `Some(0)`；1 字节消息因超过 `cap` 返回 `TooLarge`。
    ///
    /// Test goal: `cap == 0` admits only the empty message.
    /// Method: build a pair with `cap=0` (ring capacity 2); write empty successfully;
    /// then write a 1-byte message.
    /// Assertion: empty message reads back as `Some(0)`; the 1-byte write is
    /// `TooLarge` because it exceeds `cap`.
    #[test]
    fn pending_msg_zero_cap_only_empty_() {
        let (mut app, mut main) = new_pending_msg_(0usize, CoreAlloc).expect("建环");
        let mut state = ChunkReadState_::new_message_();

        app.write_outgoing_(&[], 0usize).expect("空 message");
        assert_eq!(
            main.read_prefix_outgoing_(&mut state, 0usize)
                .expect("读前缀"),
            Option::Some(0usize)
        );
        assert_eq!(
            app.write_outgoing_(&[1u8], 0usize)
                .expect_err("cap=0 只允许空 message"),
            ChunkWriteErr_::TooLarge
        );
    }
}
