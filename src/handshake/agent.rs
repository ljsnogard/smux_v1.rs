//! 握手 IO 层：成帧读写、发起方 / 等待方对象与状态机。
//!
//! 帧的线格式与协商规则见 [`crate::handshake`] 模块文档；纯编解码见私有模块
//! `codec`。本模块对外只暴露两个对象：
//!
//! - [`HandshakeAgent`]：拥有收发通道，执行完整的发起方流程
//!   （`INVITE → ACCEPT → CONFIRM → CONFRM`）。
//! - [`HandshakeListener`]：拥有收发通道，执行完整的等待方流程
//!   （`INVITE → ACCEPT → CONFIRM → CONFRM`）。
//!
//! 两者成功后都向调用者交付 [`HandshakeEndpoint`]：协商好的规格
//! （[`HandshakeOpts`]）以及一对 `Tx` / `Rx`，供后续连接层继续使用。
//!
//! # 调用约定
//!
//! 两个对象的握手方法都会**消耗**对象自身，返回
//! `gen_may_cancel_future` 生成的 future；future 完成后把收发通道归还。
//! 返回的 future 可以直接 `.await`（不可取消），也可以先
//! `.may_cancel_with(cancel)` 再 `.await`。
//!
//! # 取消
//!
//! 取消发生在帧中途时，本次握手判定失败并返回
//! [`HandshakeError::Cancelled`]；调用方必须关闭底层传输，v1 不支持断点续读。

use abs_buff::{
    TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite, gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;

use crate::handshake::{
    HandshakeChecksum, K_ACCEPT_MAGIC, K_CONFRM_MAGIC, K_INVITE_MAGIC, K_REJECT_MAGIC,
    codec_::{
        ParsedFrame, complete_invite_, complete_values_, confirm_matches_, read_frame_,
        values_to_basic_, write_frame_,
    },
    error::{HandshakeError, from_read_frame_err_, from_write_frame_err_},
    opts::{BasicOpts, HandshakeOpts, NegotiationBasicEntry},
};

/// 握手帧默认使用的校验算法：CRC-16/XMODEM。
///
/// 校验算法逐帧自声明（模块文档 §3），需要别的算法时由调用方在
/// [`write_frame_`] 一层指定。
const K_DEFAULT_CHECKSUM: HandshakeChecksum =
    HandshakeChecksum::Crc16(crc::Crc::<u16>::new(&crc::CRC_16_XMODEM));

/// 基础项个数。
const K_BASIC_COUNT: usize = 4;

/// 握手成功后交付给调用者的内容。
///
/// `opts` 是双方协商一致的连接规格；`tx` / `rx` 是握手期间使用的收发通道，
/// 由本对象归还，供后续连接层继续使用。
#[derive(Debug, Clone)]
pub struct HandshakeEndpoint<Tx, Rx> {
    /// 协商好的连接规格。
    pub opts: HandshakeOpts,

    /// 发送半边。
    pub tx: Tx,

    /// 接收半边。
    pub rx: Rx,
}

/// 握手发起方对象。
///
/// 由调用方构造并持有收发通道与单帧长度上限；调用
/// [`HandshakeAgent::invite_async`] 执行完整流程。
pub struct HandshakeAgent<Rx, Tx> {
    rx_: Rx,
    tx_: Tx,
    max_size_: usize,
}

impl<Rx, Tx> HandshakeAgent<Rx, Tx>
where
    Rx: TrBuffRead<u8> + TrBuffTryRead<u8>,
    Tx: TrBuffWrite<u8> + TrBuffTryWrite<u8>,
{
    /// 用收发通道与单帧长度上限构造。
    ///
    /// `max_size` 表示单帧总字节数上限（含 magic 与校验尾），推荐取本端
    /// [`BasicOpts::max_packet_size`]。
    pub fn new(rx: Rx, tx: Tx, max_size: usize) -> Self {
        HandshakeAgent {
            rx_: rx,
            tx_: tx,
            max_size_: max_size,
        }
    }

    /// 执行完整的发起方握手：发送 `INVITE`、等待 `ACCEPT`、交由 `decide`
    /// 决定、发送 `CONFIRM`、等待对端 `CONFRM`。
    ///
    /// - `invite` 是发起方希望声明的基础项，可只列出一部分；键必须唯一、
    ///   取值不得为 0。
    /// - `decide` 在收到合法的 `ACCEPT` 后被调用一次；返回 `false` 时向对端
    ///   发送 `REJECT` 并终止。
    ///
    /// 返回的 future 输出 `Result<HandshakeEndpoint<Tx, Rx>, HandshakeError>`；
    /// 成功后由 [`HandshakeEndpoint`] 交付协商规格与归还的收发通道。
    pub fn invite_async<'f, D>(
        self,
        invite: &'f [NegotiationBasicEntry],
        decide: D,
    ) -> HandshakeInviteAsync<'f, Rx, Tx, D>
    where
        Rx: 'f,
        Tx: 'f,
        D: FnMut(&BasicOpts) -> bool + 'f,
    {
        HandshakeInviteAsync(self.rx_, self.tx_, invite, self.max_size_, decide)
    }

    /// 执行完整的等待方握手：等待 `INVITE`、补全条件、交由 `decide` 决定、
    /// 发送 `ACCEPT`、等待并校验 `CONFIRM`、发送空 `CONFRM`。
    ///
    /// - `local` 是本端的基础项取值，用于补全 `INVITE` 未提及的项。
    /// - `decide` 在收到合法的 `INVITE`、生成完整结果后被调用一次；返回
    ///   `false` 时向对端发送 `REJECT` 并终止。
    ///
    /// 返回的 future 输出 `Result<HandshakeEndpoint<Tx, Rx>, HandshakeError>`。
    pub fn accept_handshake<'f, D>(
        self,
        local: &'f BasicOpts,
        decide: D,
    ) -> AcceptHandshakeAsync<'f, Rx, Tx, D>
    where
        Rx: 'f,
        Tx: 'f,
        D: FnMut(&BasicOpts) -> bool + 'f,
    {
        AcceptHandshakeAsync(self.rx_, self.tx_, local, self.max_size_, decide)
    }
}

/// 读取一个完整握手帧。
///
/// 帧没有长度字段，读取由 [`read_frame_`] 顺序推进；累计帧长超过 `max_size`
/// 时返回 [`HandshakeError::FrameTooLarge`]。
async fn read_frame_async_<'f, R, K, WE>(
    rx: &'f mut R,
    max_size: usize,
    cancel: &'f mut K,
) -> Result<ParsedFrame, HandshakeError<R::Err, WE>>
where
    R: TrBuffRead<u8> + TrBuffTryRead<u8>,
    K: TrCancellationToken + Clone,
{
    read_frame_(rx, max_size, cancel)
        .await
        .map_err(from_read_frame_err_)
}

/// 编码并写出一个握手帧（校验尾固定 CRC-16/XMODEM）。
async fn write_frame_async_<'f, W, K, RE>(
    tx: &'f mut W,
    magic: [u8; 4],
    values: &'f [Option<usize>; K_BASIC_COUNT],
    cancel: &'f mut K,
) -> Result<(), HandshakeError<RE, W::Err>>
where
    W: TrBuffWrite<u8> + TrBuffTryWrite<u8>,
    K: TrCancellationToken + Clone,
{
    write_frame_(tx, magic, values, K_DEFAULT_CHECKSUM, cancel)
        .await
        .map_err(from_write_frame_err_)
}

/// 发起方状态机实现；对外经 [`HandshakeAgent::invite_async`] 使用。
#[gen_may_cancel_future(HandshakeInvite)]
async fn handshake_invite_async_<'f, R, W, D, K>(
    mut rx: R,
    mut tx: W,
    invite: &'f [NegotiationBasicEntry],
    max_size: usize,
    mut decide: D,
    cancel: &'f mut K,
) -> Result<HandshakeEndpoint<W, R>, HandshakeError<R::Err, W::Err>>
where
    R: TrBuffRead<u8> + TrBuffTryRead<u8> + 'f,
    W: TrBuffWrite<u8> + TrBuffTryWrite<u8> + 'f,
    D: FnMut(&BasicOpts) -> bool + 'f,
    K: TrCancellationToken + Clone,
{
    let empty: [Option<usize>; K_BASIC_COUNT] =
        [Option::None, Option::None, Option::None, Option::None];

    // 1. 校验并发送 INVITE。
    let mut proposed: [Option<usize>; K_BASIC_COUNT] =
        [Option::None, Option::None, Option::None, Option::None];
    let mut seen = 0u8;
    for entry in invite {
        let key = entry.opts_key & 0x0F;
        if key as usize >= K_BASIC_COUNT {
            return Result::Err(HandshakeError::UnsupportedOption);
        }
        let bit = 1u8 << key;
        if seen & bit != 0 || entry.val_data == 0 {
            return Result::Err(HandshakeError::MalformedBody);
        }
        seen |= bit;
        proposed[key as usize] = Option::Some(entry.val_data);
    }
    write_frame_async_(&mut tx, K_INVITE_MAGIC, &proposed, cancel).await?;

    // 2. 等待 ACCEPT / REJECT。
    let frame = read_frame_async_(&mut rx, max_size, cancel).await?;
    if frame.magic == K_REJECT_MAGIC {
        return Result::Err(HandshakeError::PeerRejected);
    }
    if frame.magic != K_ACCEPT_MAGIC {
        return Result::Err(HandshakeError::InvalidMagic);
    }

    // 3. ACCEPT 必须补全全部 4 项。
    let Option::Some(accepted) = complete_values_(&frame.entries) else {
        let _ = write_frame_async_::<W, K, R::Err>(&mut tx, K_REJECT_MAGIC, &empty, cancel).await;
        return Result::Err(HandshakeError::Inconsistent);
    };
    let result = values_to_basic_(&accepted);

    // 4. 交由上层决定；拒绝则回 REJECT。
    if !decide(&result) {
        let _ = write_frame_async_::<W, K, R::Err>(&mut tx, K_REJECT_MAGIC, &empty, cancel).await;
        return Result::Err(HandshakeError::Rejected);
    }

    // 5. 回显同一组数值作为 CONFIRM。
    write_frame_async_(&mut tx, K_CONFRM_MAGIC, &accepted, cancel).await?;

    // 6. 等待等待方的空 CONFRM。
    let done = read_frame_async_(&mut rx, max_size, cancel).await?;
    if done.magic != K_CONFRM_MAGIC {
        return Result::Err(HandshakeError::InvalidMagic);
    }
    if done.entries.iter().any(Option::is_some) {
        return Result::Err(HandshakeError::Inconsistent);
    }
    Result::Ok(HandshakeEndpoint {
        opts: HandshakeOpts { basic_opts: result },
        tx,
        rx,
    })
}

/// 等待方状态机实现；对外经 [`HandshakeListener::accept_handshake`] 使用。
#[gen_may_cancel_future(AcceptHandshake)]
async fn accept_handshake_async_<'f, R, W, D, K>(
    mut rx: R,
    mut tx: W,
    local: &'f BasicOpts,
    max_size: usize,
    mut decide: D,
    cancel: &'f mut K,
) -> Result<HandshakeEndpoint<W, R>, HandshakeError<R::Err, W::Err>>
where
    R: TrBuffRead<u8> + TrBuffTryRead<u8> + 'f,
    W: TrBuffWrite<u8> + TrBuffTryWrite<u8> + 'f,
    D: FnMut(&BasicOpts) -> bool + 'f,
    K: TrCancellationToken + Clone,
{
    let empty: [Option<usize>; K_BASIC_COUNT] =
        [Option::None, Option::None, Option::None, Option::None];

    // 1. 等待 INVITE。
    let frame = read_frame_async_(&mut rx, max_size, cancel).await?;
    if frame.magic != K_INVITE_MAGIC {
        return Result::Err(HandshakeError::InvalidMagic);
    }

    // 2. 逐字重复已提及项，未提及项用本地值补全。
    let values = complete_invite_(local, &frame.entries);
    let result = values_to_basic_(&values);

    // 3. 交由上层决定；拒绝则回 REJECT。
    if !decide(&result) {
        let _ = write_frame_async_::<W, K, R::Err>(&mut tx, K_REJECT_MAGIC, &empty, cancel).await;
        return Result::Err(HandshakeError::Rejected);
    }

    // 4. 发送 ACCEPT。
    write_frame_async_(&mut tx, K_ACCEPT_MAGIC, &values, cancel).await?;

    // 5. 等待并校验 CONFIRM。
    let confirm = read_frame_async_(&mut rx, max_size, cancel).await?;
    if confirm.magic != K_CONFRM_MAGIC {
        let _ = write_frame_async_::<W, K, R::Err>(&mut tx, K_REJECT_MAGIC, &empty, cancel).await;
        return Result::Err(HandshakeError::InvalidMagic);
    }
    let consistent = confirm_matches_(&confirm.entries, &values);
    if !consistent {
        let _ = write_frame_async_::<W, K, R::Err>(&mut tx, K_REJECT_MAGIC, &empty, cancel).await;
        return Result::Err(HandshakeError::Inconsistent);
    }

    // 6. 发送空 CONFRM。
    write_frame_async_(&mut tx, K_CONFRM_MAGIC, &empty, cancel).await?;
    Result::Ok(HandshakeEndpoint {
        opts: HandshakeOpts { basic_opts: result },
        tx,
        rx,
    })
}
