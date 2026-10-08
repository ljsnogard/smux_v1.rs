//! 数据报的**接收半边**：把到达的整条报文交给应用。
//!
//! # 它做什么
//!
//! 解复用循环已经把「帧头 + 载荷」的**整帧原始字节**写进接收环、并把**帧总长**记进
//! 身份节点内联的接收队列（见 [`super::recv_ctx_`]）。本半边只负责：
//!
//! 1. 等到队列里出现下一条报文的帧总长；
//! 2. 从接收环借出这一整帧；
//! 3. **只读**解析帧头，拆出远端地址与帧头长度；
//! 4. 把帧头那一段**消费掉**——消费量记在同一个段的偏移上，于是剩下的部分恰好就是
//!    载荷；
//! 5. 把「载荷段 + 远端地址」包成 [`RecvDatagram`] 交给应用。
//!
//! # 为什么在接收侧拆
//!
//! 上游把「收一条报文」定义成一个自描述对象（载荷段 + 远端地址）。让解复用循环只
//! 负责**投整帧**、由接收侧**自拆**，是为了让「数据报的内部结构」只在一个地方被理解：
//! 中心循环因此完全不碰 `DATAGRAM` 的字段语义，丢弃也天然以**整帧**为单位。
//!
//! # 借段与 `Demand` 的存放位置
//!
//! `buffex::ring` 的 `try_read` 签名要求「环半部」与「`Demand`」**同寿**，而返回的段
//! 要活到 `&mut Receiver` 的整个借用周期。因此本次借段用的 `Demand` 不能是
//! `recv_async` 的局部量——它作为 [`Receiver::recv_demand_`] 与环半部同处一个结构体，
//! 借出时两个字段被**分别借用**。

use core::{
    future::poll_fn,
    mem::MaybeUninit,
};

use abs_buff::{
    Demand, gen_may_cancel_future,
    buffer::{TrBuffSegmRef, TrBuffSegmView},
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use abs_smux::telegraph::TrDatagramRecver;
use buffex::{
    ring::RingSegmRef,
    x_deps::abs_buff,
};

use crate::{
    connection::{
        Dock, MuxChanBuff_, MuxConnection, MuxError, TrConnCfg,
        frame_::{K_MAX_FRAME_HEADER},
        frame_parser_::{Consume_, FrameHeaderParser},
        ring_::BufferedRx,
        session_::race_cancel_,
    },
};
use super::{
    ctx_::TelegraphCtxHolder_,
    datagram_::RecvDatagram,
    endpoint_::TelegraphError,
};

/// 数据报的**接收半边**。
///
/// 自身不实现任何缓冲读写 trait：取一条报文 = [`recv_async`](TrDatagramRecver::recv_async)
/// 返回一个 [`RecvDatagram`]，它自己就是载荷的段引用。
pub struct Receiver<C>
where
    C: TrConnCfg,
{
    /// 接收环的**消费端**（解复用循环写、应用读）。
    ring_rx_: BufferedRx,

    /// 连接智能指针（保活 + 连接级失败查询）。
    mux_conn_: MuxConnection<C>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 身份守卫（与发送半边各持一份）。
    ctx_holder_: TelegraphCtxHolder_<C>,

    /// 本次 `recv_async` 借出整帧时使用的 `Demand`。
    ///
    /// **必须**是结构体字段而不是 future 的局部量：`RingReader::try_read` 要求
    /// `&'f mut ring` 与 `&'f Demand<usize>` 同寿，而返回的段要活到
    /// `&mut Receiver` 的整个借用周期（见模块文档）。
    recv_demand_: Demand<usize>,
}

impl<C> Receiver<C>
where
    C: TrConnCfg,
{
    /// 由接收环消费端、连接、dock 与身份守卫构造（只允许 `Telegraph::split` 调用）。
    pub(super) fn new_(
        ring: BufferedRx,
        conn: MuxConnection<C>,
        local_dock: Dock,
        identity: TelegraphCtxHolder_<C>,
    ) -> Self {
        Receiver {
            ring_rx_: ring,
            mux_conn_: conn,
            local_dock_: local_dock,
            ctx_holder_: identity,
            recv_demand_: Demand::exactly(0usize),
        }
    }
}

impl<C> TrDatagramRecver<C> for Receiver<C>
where
    C: TrConnCfg,
{
    type DataGram<'f>
        = RecvDatagram<'f, RingSegmRef<'f, MuxChanBuff_, u8>>
    where
        Self: 'f;

    type Err = TelegraphError;

    type RecvAsync<'f>
        = MuxRecvTgAsync<'f, 'f, C>
    where
        Self: 'f;

    fn local_dock(&self) -> C::Dock {
        self.local_dock_
    }

    fn recv_async(&mut self) -> Self::RecvAsync<'_> {
        MuxRecvTgAsync::new(self)
    }
}

/// [`TrDatagramRecver::recv_async`] 的 step 函数。
///
/// 等到「接收队列里有一条帧总长」之后**一次性**借出整帧并返回——借出本身不可失败
/// （写侧先写环、再入队，因此队列里出现的长度必然对应环上已就位的整帧）。若极端的
/// 内部不一致导致借不出，如实报成接收方向已关闭，而不是无限等一条永远不会来的报文。
#[gen_may_cancel_future(MuxRecvTg, pub, new(pub(crate)))]
async fn mux_recv_tg_async_<'f, C, K>(
    rx: &'f mut Receiver<C>,
    cancel: K,
) -> Result<RecvDatagram<'f, RingSegmRef<'f, MuxChanBuff_, u8>>, TelegraphError>
where
    C: TrConnCfg + 'f,
    K: TrCancellationToken,
{
    loop {
        // 1. 队列里有下一条报文的帧总长吗？有就说明解复用循环**已经**把整帧写进了
        //    接收环（先写环、再入队），因此下面这次借段一定拿得到完整的一帧。
        if let Option::Some((_, frame_len)) = rx.ctx_holder_.in_().try_pop_() {
            // 队列腾出了一个槽位：解复用循环可能正等它（它按「队列满则整帧丢弃」自保，
            // 但腾位通知仍然要发，否则要等到下一次才有机会）。
            rx.ctx_holder_.in_().notify_();
            return take_datagram_(rx, frame_len);
        }
        // 2. 连接级失败 / 接收环已关：如实报错，而不是无限等一条不会来的报文。
        if let Option::Some(reason) = rx.mux_conn_.core_().reg_().fail_kind_() {
            return Result::Err(TelegraphError::Mux(MuxError::ConnFailed(reason)));
        }
        if rx.ring_rx_.ring_state().is_producer_closed() {
            return Result::Err(TelegraphError::RxClosed);
        }
        // 3. 等一次「队列非空」通知（可取消：等待期间调用方可以发主动取消信号）。
        let wait = poll_fn(|cx| rx.ctx_holder_.in_().poll_wait_(cx));
        match race_cancel_(&cancel, wait).await {
            Option::Some(()) => {}
            Option::None => return Result::Err(TelegraphError::Mux(MuxError::Cancelled)),
        }
    }
}

/// 借出环上的一整帧，自拆出载荷与远端地址。
///
/// # 步骤
///
/// 1. 把本次借段的 `Demand` 写进 [`Receiver::recv_demand_`]（它是借出段的寿命依据）；
/// 2. 从接收环借出**整帧**（帧头 + 载荷）；
/// 3. 只读解析帧头得到 `(帧头长度, 远端地址)`；
/// 4. 把帧头那一段**搬走并丢弃**——注意只 `take_segm_ref` 取出而不搬走**不会**推进
///    消费量（见实现内注释），因此这里真的把帧头字节搬进一个定长临时缓冲；搬走之后
///    父段剩下的部分恰好就是载荷；
/// 5. 把「载荷段 + 远端地址」包成 [`RecvDatagram`]。
///
/// # Errors
///
/// 环已关闭 / 借不出段，或环里的字节解析不出合法帧头（内部不一致）时返回错误。
fn take_datagram_<'f, C>(
    rx: &'f mut Receiver<C>,
    frame_len: usize,
) -> Result<RecvDatagram<'f, RingSegmRef<'f, MuxChanBuff_, u8>>, TelegraphError>
where
    C: TrConnCfg,
{
    rx.recv_demand_ = Demand::exactly(frame_len);
    // 两个字段被**分别借用**：环半部要 `&mut`，`Demand` 只要共享。
    let mut whole = match rx.ring_rx_.try_read(&rx.recv_demand_).pick_left() {
        Option::Some(segm) => segm,
        // 队列里已有长度却借不出整帧：环已关闭或内部记账不一致，如实收尾。
        Option::None => return Result::Err(TelegraphError::RxClosed),
    };

    // 只读解析帧头：`iter_slices` 不消费任何字节。
    let (head_len, remote_dock) = parse_head_(&whole).map_err(TelegraphError::Mux)?;

    // 消费掉帧头。**注意**：`take_segm_ref` 只交出「可视」的子段——真正的消费发生在
    // 把内容**搬走**之时，取出即丢弃并不会推进父段的偏移。因此这里把帧头字节搬进一个
    // 定长临时缓冲并丢弃：父段的偏移随之恰好前进 `head_len`，剩下的部分就是载荷
    // （空载荷时它自然退化为空段，不需要任何特判）。
    if head_len > K_MAX_FRAME_HEADER {
        // 帧头不可能超过上界（`frame_parser_` 的字段总和上限）；内部不一致。
        return Result::Err(TelegraphError::Mux(MuxError::MalformedFrame));
    }
    if head_len > 0 {
        let mut junk = [MaybeUninit::<u8>::uninit(); K_MAX_FRAME_HEADER];
        if let Option::Some(mut head) = whole.take_segm_ref(&Demand::exactly(head_len)) {
            let moved = TrBuffSegmRef::move_items_to_buff(&mut head, &mut junk[..head_len]);
            debug_assert_eq!(moved, head_len, "帧头段必须恰好搬出 head_len 字节");
            drop(head);
        }
    }

    Result::Ok(RecvDatagram::new_(remote_dock, whole))
}

/// **只读**解析一段原始帧字节的帧头，返回 `(帧头长度, 远端地址)`。
///
/// 「远端地址」取帧头的 `LocalDock`：帧头是**镜像**语义（发送方写自己的 dock 在
/// `LocalDock`、写目的地址在 `RemoteDock`），所以对接收方而言远端 = `LocalDock`。
///
/// 复用中心循环同一个 sans-IO 状态机，因此「接收侧自拆」与「发送侧编码」共用一套
/// 字段规则，不存在第二份协议理解。
///
/// # Errors
///
/// 段里没有完整 / 合法的帧头时返回错误（对接收侧而言是内部不一致）。
fn parse_head_<S>(segm: &S) -> Result<(usize, Dock), MuxError>
where
    S: TrBuffSegmView<Item = u8>,
{
    let mut parser = FrameHeaderParser::new();
    for slice in segm.iter_slices() {
        for &byte in slice {
            match parser.consume_byte_(byte) {
                Consume_::Pending => {}
                Consume_::Done(header) => {
                    // 帧头是**镜像**语义：发送方把自己的 dock 写在 `LocalDock`，
                    // 把目的地址写在 `RemoteDock`（见 `session_` 的派发注释）。
                    // 因此对本接收方而言，「这条报文来自哪个远端垛口」是
                    // `LocalDock`，而 `RemoteDock` 只是本端自己的地址。
                    return Result::Ok((parser.consumed_len_(), header.local_dock()));
                }
                Consume_::Failed(err) => return Result::Err(err),
            }
        }
    }
    Result::Err(MuxError::MalformedFrame)
}
