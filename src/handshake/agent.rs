//! 握手 IO 层：成帧读写、发起方 / 等待方对象与状态机。
//!
//! 帧的线格式与协商规则见 [`crate::handshake`] 模块文档；纯编解码见私有模块
//! `codec_`。本模块对外只暴露两个对象：
//!
//! - [`HandshakeAgent`]：拥有收发通道。
//! - [`HandshakeAgent::listen_async`]：同一个对象执行完整的等待方流程。
//!
//! 两者成功后都向调用者交付 [`HandshakeDelivery`]：协商好的规格
//! （[`HandshakeOpts`]）以及一对 `Tx` / `Rx`，供后续连接层继续使用。
//!
//! # 边接收边协商
//!
//! 收到的那一帧（发起方收 `ACCEPT`、等待方收 `INVITE`）**不是**先整帧读完再
//! 交给上层：读侧状态机每解析出一个条目就立刻把它交给 [`TrNegotiator`]。一旦
//! 协商器拒绝，本端**不再读剩余条目**，立即回 `REJECT` 并终止握手
//! （模块文档 §7.5、§9；`dev-notes.md` D4）。
//!
//! CRC 与协商**彻底分离**：协商器只看得到条目，不看校验值；协商走完之后若已
//! 读完整帧，则 `crc` 比对一票否决——校验失败时整次协商作废（`dev-notes.md`
//! D1）。协商器提前接受、没有把条目区读到底时，本端会继续读完并照常做校验。
//!
//! # 调用约定
//!
//! 两个握手方法都会**消耗**对象自身，返回 `gen_may_cancel_future` 生成的
//! future；future 完成后把收发通道归还。返回的 future 可以直接 `.await`
//! （不可取消），也可以先 `.may_cancel_with(cancel)` 再 `.await`。
//!
//! # 取消
//!
//! 取消发生在帧中途时，本次握手判定失败并返回
//! [`HandshakeError::Cancelled`]；调用方必须关闭底层传输，v1 不支持断点续读。

use core::marker::PhantomData;

use abs_async_iter::TrAsyncIterator;
use abs_buff::{TrBuffRead, TrBuffWrite, gen_may_cancel_future};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use buffex::x_deps::{abs_buff, abs_cancel};

use crate::handshake::{
    K_ACCEPT_MAGIC, K_CONFRM_MAGIC, K_INVITE_MAGIC, K_REJECT_MAGIC,
    codec_::{
        FrameReader, K_DEFAULT_CHECKSUM, basic_to_values_, basic_values_are_valid_,
        complete_invite_, complete_values_, confirm_matches_, values_to_basic_, write_frame_,
    },
    error::{HandshakeError, from_read_frame_err_, from_write_frame_err_},
    opts::{BasicOpts, HandshakeOpts, K_BASIC_KEY_COUNT, NegotiationEntry, NegotiationKey},
};

/// 协商策略：逐条判断对端提出的协商条目是否可接受。
///
/// 实现者拿到的是一个**异步条目流**；每条条目在被解析出来的**那一刻**就交给
/// `should_accept_async`，因此不必等整帧收完。实现应当：
///
/// 1. 循环调用 `entries.next_async().await`，每拿到一条就立刻判断；
/// 2. 全部条目都接受时返回 `true`；
/// 3. 遇到不可接受的条目时**立即**返回 `false`——调用方随即发送 `REJECT`，
///    并且**不会**再读剩余条目（模块文档 §7.5）。
///
/// 返回的 future 支持通过 [`TrMayCancel`] 主动取消。
///
/// # 条目类型
///
/// [`TrNegotiator::Entry`] 的生命周期与 `self` 的借用生命周期相互独立，因此
/// 实现可以返回借用读缓冲的条目类型；本 crate 内部把它实例化为
/// [`NegotiationEntry`]。
pub trait TrNegotiator {
    /// 交给协商器的条目类型。
    ///
    /// 本 crate 的握手实现要求它是 [`NegotiationEntry`]，见
    /// [`HandshakeAgent::invite_async`] 的约束。
    type Entry<'f>;

    /// `should_accept_async` 返回的 future。
    ///
    /// 条目流的类型 `I` 必须出现在这里：驱动 `I` 的 future 会持有它，而
    /// associated type 无法捕获方法自身的泛型参数。
    type ShouldAcceptAsync<'f, I>: TrMayCancel<'f, MayCancelOutput = bool>
    where
        Self: 'f,
        I: TrAsyncIterator<Item = Self::Entry<'f>> + 'f;

    /// 边收边判：消费 `entries`，返回是否接受。
    ///
    /// 语义见 trait 文档。实现应当把 `entries` 读到 `None` 再返回 `true`；
    /// 提前返回 `true` 时调用方仍会替它把剩余条目读完并做 CRC 校验。
    fn should_accept_async<'f, I>(&'f mut self, entries: I) -> Self::ShouldAcceptAsync<'f, I>
    where
        I: TrAsyncIterator<Item = Self::Entry<'f>> + 'f;
}

/// 接受一切条目的协商器。
///
/// 它会把条目流读到结束，因此同时充当「流式协商能跑通」的最小实现与测试替身。
///
/// 条目流以 `Err` 表示终止（读完或读侧失败），这里对两者都返回「不再拒绝」；若终止
/// 源于失败，调用方会从读状态机取回它，并优先按失败分类（不会变成 `REJECT`）。
pub struct AcceptAllEntries;

/// `AcceptAllEntry` 的协商逻辑。
///
/// 生成条目需放宽到 `pub`：`AcceptAllEntries` 本身是公开类型，其
/// [`TrNegotiator::ShouldAcceptAsync`] 若引用私有类型会触发 E0446。
#[gen_may_cancel_future(AcceptAll, pub)]
async fn accept_all_async_<'f, I, C>(
    _: &'f mut AcceptAllEntries,
    mut entries: I,
    cancel: C,
) -> bool
where
    I: TrAsyncIterator<Item = NegotiationEntry<'f>> + 'f,
    C: TrCancellationToken,
{
    loop {
        if cancel.is_cancelled() {
            return false;
        }
        let next = entries
            .next_async()
            .may_cancel_with(cancel.child_token())
            .await;
        match next {
            // 还有下一条：继续读，边收边判。
            Result::Ok(_) => continue,
            // 流已终止（读完，或读侧失败）：都按「接受」返回。失败原因不在这里
            // 区分，由调用方经 `FrameReader::take_error_` 取回后分类处置
            // （见 `StreamEnd_` 的文档）。
            Result::Err(_) => return true,
        }
    }
}

impl TrNegotiator for AcceptAllEntries {
    type Entry<'f> = NegotiationEntry<'f>;

    type ShouldAcceptAsync<'f, I> = AcceptAllAsync<'f, 'f, I>
    where
        Self: 'f,
        I: TrAsyncIterator<Item = NegotiationEntry<'f>> + 'f;

    fn should_accept_async<'f, I>(&'f mut self, entries: I) -> Self::ShouldAcceptAsync<'f, I>
    where
        I: TrAsyncIterator<Item = NegotiationEntry<'f>> + 'f,
    {
        AcceptAllAsync::new(self, entries)
    }
}

/// 握手成功后交付给调用者的内容。
///
/// `opts` 是双方协商一致的连接规格；`tx` / `rx` 是握手期间使用的收发通道，
/// 由本对象归还，供后续连接层继续使用。
#[derive(Debug, Clone)]
pub struct HandshakeDelivery<Tx, Rx> {
    /// 协商好的连接规格。
    pub opts: HandshakeOpts,

    /// 发送半边。
    pub tx: Tx,

    /// 接收半边。
    pub rx: Rx,
}

/// 握手对象，同时承载发起方与等待方两种角色。
///
/// 由调用方构造并持有收发通道；调用 [`HandshakeAgent::invite_async`] 执行发起
/// 方流程，或调用 [`HandshakeAgent::listen_async`] 执行等待方流程。
pub struct HandshakeAgent<Tx, Rx> {
    tx_: Tx,
    rx_: Rx,
}

impl<Tx, Rx> HandshakeAgent<Tx, Rx>
where
    Tx: TrBuffWrite<u8>,
    Rx: TrBuffRead<u8>,
{
    /// 用收发通道构造。
    ///
    /// 不接收「单帧长度上限」参数：条目数量不设上限（协议层面），防御无界连接
    /// 由调用方的取消令牌负责，见模块文档 §3、§10。本端对**单个条目长度**的
    /// 内部上限属于实现策略，不由构造参数给出。
    ///
    /// 参数顺序与全 crate 一致：**成对的收发一律 `(tx, rx)`**——与
    /// [`HandshakeDelivery`] 的字段顺序、`accept_async` 的返回值、
    /// [`BufferedChannel`](crate::connection::BufferedChannel) 等同一约定。
    pub fn new(tx: Tx, rx: Rx) -> Self {
        HandshakeAgent { tx_: tx, rx_: rx }
    }

    /// 执行完整的发起方握手：发送 `INVITE`、等待并**边收边判** `ACCEPT`、
    /// 发送 `CONFIRM`、等待对端空 `CONFRM`。
    ///
    /// - `entries` 是发起方希望声明的基础项，可只列出一部分；键必须唯一、
    ///   取值不得为 0。它们会被逐条写出，**不会**先在内存里成形整帧。
    /// - `negotiator` 在接收 `ACCEPT` 时被调用一次；返回 `false` 时向对端发送
    ///   `REJECT` 并立即终止（不再读完剩余条目）。
    ///
    /// 返回的 future 输出 `Result<HandshakeDelivery<Tx, Rx>, HandshakeError>`；
    /// 成功后由 [`HandshakeDelivery`] 交付协商规格与归还的收发通道。
    pub fn invite_async<'f, I, D>(
        self,
        entries: I,
        negotiator: D,
    ) -> HandshakeInviteAsync<'f, 'f, Tx, Rx, I, D>
    where
        Tx: 'f,
        Rx: 'f,
        I: IntoIterator<Item = NegotiationEntry<'f>> + 'f,
        D: for<'x> TrNegotiator<Entry<'x> = NegotiationEntry<'x>> + 'f,
    {
        HandshakeInviteAsync::new(
            PhantomData,
            self.tx_,
            self.rx_,
            entries,
            negotiator,
        )
    }

    /// 执行完整的等待方握手：等待并**边收边判** `INVITE`、补全条件、
    /// 发送 `ACCEPT`、等待并校验 `CONFIRM`、发送空 `CONFRM`。
    ///
    /// - `local` 是本端的基础项取值，用于补全 `INVITE` 未提及的项。
    /// - `negotiator` 在接收 `INVITE` 时被调用一次；返回 `false` 时向对端发送
    ///   `REJECT` 并立即终止（不再读完剩余条目）。
    ///
    /// 返回的 future 输出 `Result<HandshakeDelivery<Tx, Rx>, HandshakeError>`。
    pub fn listen_async<'f, D>(
        self,
        local: &'f BasicOpts,
        negotiator: D,
    ) -> ListenHandshakeAsync<'f, 'f, Tx, Rx, D>
    where
        Tx: 'f,
        Rx: 'f,
        D: for<'x> TrNegotiator<Entry<'x> = NegotiationEntry<'x>> + 'f,
    {
        ListenHandshakeAsync::new(self.tx_, self.rx_, local, negotiator)
    }
}

/// 把条目流交给协商器并等待结论。
///
/// 条目流由 [`FrameReader`] 的 [`TrAsyncIterator`] 实现提供：每产出一条就交给
/// 协商器判断，因此条目是**边收边判**的。
///
/// 取消由 `cancel` 统一施加（符合 AGENTS.md 纪律 4）；读侧的取消另由读状态机
/// 内部持有的令牌克隆在每次 `read_async` 上生效。
async fn negotiate_async<'m, D, I, K>(
    negotiator: &'m mut D,
    entries: I,
    cancel: K,
) -> bool
where
    D: TrNegotiator,
    I: TrAsyncIterator<Item = D::Entry<'m>> + 'm,
    K: TrCancellationToken + 'm,
{
    negotiator
        .should_accept_async(entries)
        .may_cancel_with(cancel)
        .await
}

/// 编码并写出一个握手帧（默认 CRC-16/XMODEM）。
///
/// 写侧逐字段推进、边写边算校验，**不预成形整帧**。
async fn write_frame_async_<W, K, RE>(
    tx: &mut W,
    magic: [u8; 4],
    values: &[Option<usize>; K_BASIC_KEY_COUNT],
    cancel: K,
) -> Result<(), HandshakeError<RE, W::Err>>
where
    W: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    write_frame_(tx, magic, values, &K_DEFAULT_CHECKSUM, cancel.child_token())
        .await
        .map_err(from_write_frame_err_)
}

/// 发起方状态机实现；对外经 [`HandshakeAgent::invite_async`] 使用。
#[gen_may_cancel_future(HandshakeInvite, pub)]
async fn handshake_invite_async_<'f, W, R, I, D, K>(
    _: PhantomData<&'f ()>,
    mut tx: W,
    mut rx: R,
    entries: I,
    mut negotiator: D,
    cancel: K,
) -> Result<HandshakeDelivery<W, R>, HandshakeError<R::Err, W::Err>>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    I: IntoIterator<Item = NegotiationEntry<'f>> + 'f,
    D: for<'x> TrNegotiator<Entry<'x> = NegotiationEntry<'x>> + 'f,
    K: TrCancellationToken,
{
    let empty: [Option<usize>; K_BASIC_KEY_COUNT] = [Option::None; K_BASIC_KEY_COUNT];

    // 1. 校验入参并发送 INVITE（逐条写出，不预留整帧缓冲）。
    let mut proposed: [Option<usize>; K_BASIC_KEY_COUNT] = [Option::None; K_BASIC_KEY_COUNT];
    let mut seen = 0u8;
    for entry in entries.into_iter() {
        let NegotiationEntry::Basic(entry) = entry else {
            return Result::Err(HandshakeError::UnsupportedOption);
        };
        let key = (entry.opts_key & NegotiationKey::MASK) as usize;
        if key >= K_BASIC_KEY_COUNT {
            return Result::Err(HandshakeError::UnsupportedOption);
        }
        let bit = 1u8 << key;
        if seen & bit != 0 || entry.val_data == 0 {
            return Result::Err(HandshakeError::MalformedBody);
        }
        seen |= bit;
        proposed[key] = Option::Some(entry.val_data);
    }
    write_frame_async_(
        &mut tx,
        K_INVITE_MAGIC,
        &proposed,
        cancel.child_token(),
    ).await?;

    // 2. 等待 ACCEPT：先判 magic，再**边收边判**条目。
    let mut reader = FrameReader::
        begin_async_(&mut rx, cancel.child_token())
        .await
        .map_err(from_read_frame_err_)?;
    if reader.magic_() == K_REJECT_MAGIC {
        return Result::Err(HandshakeError::PeerRejected);
    }
    if reader.magic_() != K_ACCEPT_MAGIC {
        return Result::Err(HandshakeError::InvalidMagic);
    }
    let accepted = negotiate_async(
        &mut negotiator,
        &mut reader,
        cancel.child_token(),
    ).await;
    // 读侧失败（CRC 不匹配、保留键、对端关闭……）**优先**分类：它既不能当成
    // 「本端主动拒绝」（按模块文档 §9，这类失败不得回 REJECT），也不能因为协商器
    // 在条目流终止时返回「不再拒绝」而被吞掉——[`TrAsyncIterator`] 用 `Err` 同时
    // 表达「读完」与「失败」，两者的区分只在 `take_error_` 里。
    if let Option::Some(err) = reader.take_error_() {
        return Result::Err(from_read_frame_err_(err));
    }
    if !accepted {
        // D4：协商中途拒绝 → 立即回 REJECT，不再读剩余条目。
        let _ = write_frame_async_::<W, K, R::Err>(&mut tx, K_REJECT_MAGIC, &empty, cancel).await;
        return Result::Err(HandshakeError::Rejected);
    }
    // 协商器可能在条目区读完前就接受；剩余条目仍要读完，CRC 才有一票否决的机会。
    if !reader.is_finished_() {
        reader.drain_async_().await.map_err(from_read_frame_err_)?;
    }

    // 3. ACCEPT 必须补全全部 5 项。
    let Option::Some(accepted_values) = complete_values_(reader.basics_()) else {
        // 该分支随即返回，`cancel` 之后不再使用，因此直接按值移交即可。
        let _ = write_frame_async_::<W, K, R::Err>(&mut tx, K_REJECT_MAGIC, &empty, cancel).await;
        return Result::Err(HandshakeError::Inconsistent);
    };
    let result = values_to_basic_(&accepted_values);

    // 4. 回显同一组数值作为 CONFIRM；下面还要读 CONFRM，故只交出子令牌。
    write_frame_async_(&mut tx, K_CONFRM_MAGIC, &accepted_values, cancel.child_token()).await?;

    // 5. 等待等待方的空 CONFRM。
    let mut done = FrameReader::begin_async_(&mut rx, cancel.child_token())
        .await
        .map_err(from_read_frame_err_)?;
    if done.magic_() != K_CONFRM_MAGIC {
        return Result::Err(HandshakeError::InvalidMagic);
    }
    done.drain_async_().await.map_err(from_read_frame_err_)?;
    if done.basics_().iter().any(Option::is_some) {
        return Result::Err(HandshakeError::Inconsistent);
    }

    Result::Ok(HandshakeDelivery {
        opts: HandshakeOpts { basic_opts: result },
        tx,
        rx,
    })
}

/// 等待方状态机实现；对外经 [`HandshakeAgent::listen_async`] 使用。
#[gen_may_cancel_future(ListenHandshake, pub)]
async fn listen_handshake_async_<'f, W, R, D, K>(
    mut tx: W,
    mut rx: R,
    local: &'f BasicOpts,
    mut negotiator: D,
    cancel: K,
) -> Result<HandshakeDelivery<W, R>, HandshakeError<R::Err, W::Err>>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    D: for<'x> TrNegotiator<Entry<'x> = NegotiationEntry<'x>> + 'f,
    K: TrCancellationToken,
{
    let empty: [Option<usize>; K_BASIC_KEY_COUNT] = [Option::None; K_BASIC_KEY_COUNT];

    // 0. 本端基础项先过一遍：补全 `INVITE` 未提及的项时会把它们写进 `ACCEPT`
    //    （`complete_invite_`），而 `0` 在 v1 里非法（`0` 表示「未提供」：秒为单位
    //    的项因此最小是 1 秒，`Duration::from_millis(500).as_secs() == 0`）。
    //
    //    在这里判、而不是等编码咽喉 `write_frame_` 判：本检查发生在**读写任何
    //    字节之前**，失败时线上没有半条帧——与发起方 `invite_async` 对入参的早判
    //    对称（那一条见 `handshake_invite_async_` 的入参循环）。
    if !basic_values_are_valid_(&basic_to_values_(local)) {
        return Result::Err(HandshakeError::MalformedBody);
    }

    // 1. 等待 INVITE，并**边收边判**。
    let mut reader = FrameReader::begin_async_(&mut rx, cancel.child_token())
        .await
        .map_err(from_read_frame_err_)?;
    if reader.magic_() != K_INVITE_MAGIC {
        return Result::Err(HandshakeError::InvalidMagic);
    }
    let accepted = negotiate_async(&mut negotiator, &mut reader, cancel.child_token()).await;
    // 与 `listen` 路径同理：读侧失败优先分类（见上一处注释）。
    if let Option::Some(err) = reader.take_error_() {
        return Result::Err(from_read_frame_err_(err));
    }
    if !accepted {
        // D4：协商中途拒绝 → 立即回 REJECT，不再读剩余条目。
        let _ = write_frame_async_::<W, K, R::Err>(&mut tx, K_REJECT_MAGIC, &empty, cancel).await;
        return Result::Err(HandshakeError::Rejected);
    }
    if !reader.is_finished_() {
        reader.drain_async_().await.map_err(from_read_frame_err_)?;
    }

    // 2. 逐字重复已提及项，未提及项用本地值补全。
    let values = complete_invite_(local, reader.basics_());
    let result = values_to_basic_(&values);

    // 3. 发送 ACCEPT；后面还要继续读 CONFIRM，故只交出子令牌。
    write_frame_async_(&mut tx, K_ACCEPT_MAGIC, &values, cancel.child_token()).await?;

    // 4. 等待并校验 CONFIRM（协议层逐值比较，不再走协商器）。
    let mut confirm = FrameReader::begin_async_(&mut rx, cancel.child_token())
        .await
        .map_err(from_read_frame_err_)?;
    if confirm.magic_() != K_CONFRM_MAGIC {
        let _ = write_frame_async_::<W, K, R::Err>(&mut tx, K_REJECT_MAGIC, &empty, cancel).await;
        return Result::Err(HandshakeError::InvalidMagic);
    }
    confirm.drain_async_().await.map_err(from_read_frame_err_)?;
    if !confirm_matches_(confirm.basics_(), &values) {
        let _ = write_frame_async_::<W, K, R::Err>(&mut tx, K_REJECT_MAGIC, &empty, cancel).await;
        return Result::Err(HandshakeError::Inconsistent);
    }

    // 5. 发送空 CONFRM。
    write_frame_async_(&mut tx, K_CONFRM_MAGIC, &empty, cancel).await?;
    Result::Ok(HandshakeDelivery {
        opts: HandshakeOpts { basic_opts: result },
        tx,
        rx,
    })
}

#[cfg(test)]
mod tests_ {
    use abs_cancel::NonCancellableToken;

    use crate::connection::ring_::test_support_::make_test_channel_;

    use super::*;

    /// 只接受 `max_packet_size` 不超过给定上限的协商器。
    struct CapPacketSize {
        /// 允许的最大报文长度。
        cap_: usize,

        /// 已经处理过的条目数量，便于断言「边收边判」确实逐条发生。
        seen_: usize,
    }

    /// `CapPacketSize` 的协商逻辑；future 类型由 [`gen_may_cancel_future`] 生成。
    #[gen_may_cancel_future(CapPacketSizeAccept, pub)]
    async fn cap_packet_size_accept_async_<'f, I, C>(
        this: &'f mut CapPacketSize,
        mut entries: I,
        _: C,
    ) -> bool
    where
        I: TrAsyncIterator<Item = NegotiationEntry<'f>> + 'f,
        C: TrCancellationToken,
    {
        loop {
            let Ok(entry) = entries.next_async().await else {
                // 流已终止：按「接受」返回，失败原因由调用方取回。
                return true;
            };
            this.seen_ += 1;
            let NegotiationEntry::Basic(basic) = entry else {
                return false;
            };
            let key = basic.opts_key & NegotiationKey::MASK;
            if key == NegotiationKey::MaxPacketSize as u8 && basic.val_data > this.cap_ {
                // 立即拒绝：调用方不会再读剩余条目。
                return false;
            }
        }
    }

    impl TrNegotiator for CapPacketSize {
        type Entry<'f> = NegotiationEntry<'f>;

        type ShouldAcceptAsync<'f, I> = CapPacketSizeAcceptAsync<'f, 'f, I>
        where
            Self: 'f,
            I: TrAsyncIterator<Item = NegotiationEntry<'f>> + 'f;

        fn should_accept_async<'f, I>(&'f mut self, entries: I) -> Self::ShouldAcceptAsync<'f, I>
        where
            I: TrAsyncIterator<Item = NegotiationEntry<'f>> + 'f,
        {
            CapPacketSizeAcceptAsync::new(self, entries)
        }
    }

    /// 建一对互联的环形缓冲，返回两个已经连好通道的握手对象。
    async fn make_pair_(
        buff_size: usize,
    ) -> (
        HandshakeAgent<impl TrBuffWrite<u8>, impl TrBuffRead<u8>>,
        HandshakeAgent<impl TrBuffWrite<u8>, impl TrBuffRead<u8>>,
    ) {
        let (a_tx, b_rx) = make_test_channel_(buff_size);
        let (b_tx, a_rx) = make_test_channel_(buff_size);
        (
            HandshakeAgent::new(a_tx, a_rx),
            HandshakeAgent::new(b_tx, b_rx),
        )
    }

    /// 测试握手能在一对环形缓冲上完整走通，且协商结果等于双方声明的缺省值。
    /// - 手段：在同一任务里用 `futures::join!` 并发推进等待方与发起方；发起方声明
    ///   [`BasicOpts::DEFAULT`]，等待方本地值同样是缺省值，协商器接受一切。
    /// - 判断：两侧 future 都返回 `Ok`，且各自拿到的 `max_packet_size` 都是
    ///   4096（`BasicOpts::DEFAULT`）。
    async fn handshake_through_ring_test_() {
        let (a, b) = make_pair_(16usize).await;

        let local_opts = BasicOpts::default();
        let proposed = BasicOpts::default();
        let (accepted, invited) = futures::join!(
            async move { a.listen_async(&local_opts, AcceptAllEntries).await },
            async move { b.invite_async(&proposed, AcceptAllEntries).await },
        );

        assert!(accepted.is_ok(), "等待方握手应当成功");
        assert!(invited.is_ok(), "发起方握手应当成功");
        let accepted = accepted.unwrap();
        let invited = invited.unwrap();
        assert_eq!(accepted.opts.basic_opts.max_packet_size, 4096usize);
        assert_eq!(invited.opts.basic_opts.max_packet_size, 4096usize);
    }
    dual_runtime_test_!(handshake_through_ring_test_);

    /// 目的：验证发起方对本端声明的入参做**同一条** `>= 1` 早判（与等待方对称）。
    ///
    /// 手段：把截断为 0 的 `max_channel_timeout` 所在的 [`BasicOpts`] 当入参
    /// （`&BasicOpts` 实现了 `IntoIterator<Item = NegotiationEntry>`），接收侧给一段
    /// **空**字节流，直接调用 `invite_async`。
    ///
    /// 判断：返回 `MalformedBody`——校验发生在写 `INVITE` 与读 `ACCEPT` 之前，空接收侧
    /// 因此绝不会被读到（读到会先得到读错误）。这条早判原先就存在，这里把它钉住，
    /// 免得将来只剩编码咽喉那一处兜底。
    async fn invite_rejects_zero_valued_entry() {
        let proposed = BasicOpts {
            max_channel_timeout: core::time::Duration::from_millis(500u64),
            ..BasicOpts::default()
        };
        let rx: &[u8] = &[];
        let mut sink = [0u8; 64];
        let agent = HandshakeAgent::new(&mut sink[..], rx);
        let res = agent.invite_async(&proposed, AcceptAllEntries).await;
        assert!(
            matches!(res, Result::Err(HandshakeError::MalformedBody)),
            "本端入参里截断为 0 的秒级项必须在写 INVITE 之前被拒"
        );
    }
    dual_runtime_test_!(invite_rejects_zero_valued_entry);

    /// 目的：验证等待方的**本端**基础项取 0 时，在读写任何字节之前就失败。
    ///
    /// 手段：`max_channel_timeout` 取 500 ms（`as_secs()` 截断为 0）作为 `local`，
    /// 接收侧给一段**空**字节流，直接调用 `listen_async`。
    ///
    /// 判断：返回 `MalformedBody`——接收侧是空的，若校验发生在读之后，这里会先得到
    /// 读错误；返回它即证明该检查在这些读写之前。这条与发起方对入参的早判对称，
    /// 补上的是等待方原先唯一的缺口（`local` 经 `complete_invite_` 补进 `ACCEPT`）。
    async fn listen_rejects_sub_second_local_timeout() {
        let local = BasicOpts {
            max_channel_timeout: core::time::Duration::from_millis(500u64),
            ..BasicOpts::default()
        };
        let rx: &[u8] = &[];
        let mut sink = [0u8; 64];
        let agent = HandshakeAgent::new(&mut sink[..], rx);
        let res = agent.listen_async(&local, AcceptAllEntries).await;
        assert!(
            matches!(res, Result::Err(HandshakeError::MalformedBody)),
            "本端秒级项截断为 0 必须在写 ACCEPT 之前被拒"
        );
    }
    dual_runtime_test_!(listen_rejects_sub_second_local_timeout);

    /// 测试读侧校验失败不会被误报成「本端主动拒绝」。
    /// - 手段：手工构造一个 CRC 被翻转的 `INVITE` 字节流交给等待方读取；协商器
    ///   在条目流上只能看到「终止」，详细原因由读状态机记录。
    /// - 判断：等待方返回 `ChecksumErr` 而不是 `Rejected`——按模块文档 §9，
    ///   校验失败不得回 `REJECT`。
    async fn checksum_failure_is_not_reported_as_rejected() {
        let values = [
            Option::Some(4096usize),
            Option::Some(1usize << 28),
            Option::Some(64usize),
            Option::Some(30usize),
            Option::Some(5usize),
        ];
        let mut buf = [0u8; 64];
        let capacity = buf.len();
        let len = {
            let mut cursor: &mut [u8] = &mut buf;
            write_frame_(
                &mut cursor,
                K_INVITE_MAGIC,
                &values,
                &K_DEFAULT_CHECKSUM,
                NonCancellableToken::new(),
            )
            .await
            .expect("写帧应当成功");
            capacity - cursor.len()
        };
        // 翻转 CRC 的最后一个字节。
        buf[len - 1] ^= 0xFF;

        let rx: &[u8] = &buf[..len];
        let mut sink = [0u8; 64];
        let agent = HandshakeAgent::new(&mut sink[..], rx);
        let local = BasicOpts::default();
        let res = agent.listen_async(&local, AcceptAllEntries).await;
        assert!(
            matches!(res, Result::Err(HandshakeError::ChecksumErr)),
            "校验失败必须按其本身分类，而不是 Rejected"
        );
    }
    dual_runtime_test_!(checksum_failure_is_not_reported_as_rejected);

    /// 测试协商器拒绝时握手立即失败（发起方收到 `PeerRejected`）。
    /// - 手段：等待方的协商器把 `max_packet_size` 上限压到 1，而发起方声明
    ///   缺省值 4096，于是等待方在读到该条目的**那一刻**就拒绝。
    /// - 判断：等待方返回 `Rejected`，发起方返回 `PeerRejected`，即拒绝路径
    ///   确实立即回了 `REJECT`，而不是等整帧收完。
    ///
    /// 环容量必须**大于**「首条目之后剩余的全部 INVITE 字节」：等待方在读到
    /// `MaxPacketSize` 后就不再读取，而发起方仍会把剩余条目与校验尾写完；环放不下
    /// 就会让发起方阻塞在写上、双方互等。基础协商项每多一项，这个下界就抬高一截，
    /// 因此这里留出足够余量（64 字节）而不是贴着算。
    async fn negotiator_rejects_immediately() {
        let (a, b) = make_pair_(64usize).await;

        let local_opts = BasicOpts::default();
        let proposed = BasicOpts::default();
        let (accepted, invited) = futures::join!(
            async move {
                a.listen_async(
                    &local_opts,
                    CapPacketSize {
                        cap_: 1usize,
                        seen_: 0usize,
                    },
                )
                .await
            },
            async move { b.invite_async(&proposed, AcceptAllEntries).await },
        );

        assert!(matches!(accepted, Result::Err(HandshakeError::Rejected)));
        assert!(matches!(invited, Result::Err(HandshakeError::PeerRejected)));
    }
    dual_runtime_test_!(negotiator_rejects_immediately);
}
