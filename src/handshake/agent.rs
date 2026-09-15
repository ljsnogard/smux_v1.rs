//! 握手 IO 层：成帧读写、发起方 / 等待方对象与状态机。
//!
//! 帧的线格式与协商规则见 [`crate::handshake`] 模块文档；纯编解码见私有模块
//! `codec_`。本模块对外只暴露两个对象：
//!
//! - [`HandshakeAgent`]：拥有收发通道。
//! - [`HandshakeAgent::listen_async`]：同一个对象执行完整的等待方流程。
//!
//! 两者成功后都向调用者交付 [`HandshakeEndpoint`]：协商好的规格
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
use abs_buff::{TrBuffRead, TrBuffWrite, gen_may_cancel_future, x_deps::abs_cancel};
use abs_cancel::{TrCancellationToken, TrMayCancel};

use crate::handshake::{
    K_ACCEPT_MAGIC, K_CONFRM_MAGIC, K_INVITE_MAGIC, K_REJECT_MAGIC,
    codec_::{
        FrameReader, K_DEFAULT_CHECKSUM, complete_invite_, complete_values_, confirm_matches_,
        values_to_basic_, write_frame_,
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
#[cfg(test)]
pub(crate) struct AcceptAllEntry;

/// `AcceptAllEntry` 的协商逻辑。
///
/// 用 [`gen_may_cancel_future`] 生成可取消的 future 类型：direct `.await` 与
/// `.may_cancel_with(token)` 两条路径由宏统一提供，不必手写 `TrMayCancel`。
#[cfg(test)]
#[gen_may_cancel_future(AcceptAll)]
async fn accept_all_async_<'f, I, C>(
    _this: &'f mut AcceptAllEntry,
    mut entries: I,
    _cancel: &'f mut C,
) -> bool
where
    I: TrAsyncIterator<Item = NegotiationEntry<'f>> + 'f,
    C: TrCancellationToken + Clone,
{
    loop {
        // abs_cancel 已把 `IntoFuture::Output` 钉到 `MayCancelOutput`，
        // 因此这里可以直接 `.await` 得到 `Result<..>`，无需自造令牌。
        match entries.next_async().await {
            Result::Ok(Option::Some(_)) => continue,
            Result::Ok(Option::None) => return true,
            Result::Err(_) => return false,
        }
    }
}

#[cfg(test)]
impl TrNegotiator for AcceptAllEntry {
    type Entry<'f> = NegotiationEntry<'f>;

    type ShouldAcceptAsync<'f, I> = AcceptAllAsync<'f, I>
    where
        Self: 'f,
        I: TrAsyncIterator<Item = NegotiationEntry<'f>> + 'f;

    fn should_accept_async<'f, I>(&'f mut self, entries: I) -> Self::ShouldAcceptAsync<'f, I>
    where
        I: TrAsyncIterator<Item = NegotiationEntry<'f>> + 'f,
    {
        AcceptAllAsync(self, entries)
    }
}

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

/// 握手对象，同时承载发起方与等待方两种角色。
///
/// 由调用方构造并持有收发通道；调用 [`HandshakeAgent::invite_async`] 执行发起
/// 方流程，或调用 [`HandshakeAgent::listen_async`] 执行等待方流程。
pub struct HandshakeAgent<Rx, Tx> {
    rx_: Rx,
    tx_: Tx,
}

impl<Rx, Tx> HandshakeAgent<Rx, Tx>
where
    Rx: TrBuffRead<u8>,
    Tx: TrBuffWrite<u8>,
{
    /// 用收发通道构造。
    ///
    /// 不接收「单帧长度上限」参数：条目数量不设上限（协议层面），防御无界连接
    /// 由调用方的取消令牌负责，见模块文档 §3、§10。本端对**单个条目长度**的
    /// 内部上限属于实现策略，不由构造参数给出。
    pub fn new(rx: Rx, tx: Tx) -> Self {
        HandshakeAgent { rx_: rx, tx_: tx }
    }

    /// 执行完整的发起方握手：发送 `INVITE`、等待并**边收边判** `ACCEPT`、
    /// 发送 `CONFIRM`、等待对端空 `CONFRM`。
    ///
    /// - `entries` 是发起方希望声明的基础项，可只列出一部分；键必须唯一、
    ///   取值不得为 0。它们会被逐条写出，**不会**先在内存里成形整帧。
    /// - `negotiator` 在接收 `ACCEPT` 时被调用一次；返回 `false` 时向对端发送
    ///   `REJECT` 并立即终止（不再读完剩余条目）。
    ///
    /// 返回的 future 输出 `Result<HandshakeEndpoint<Tx, Rx>, HandshakeError>`；
    /// 成功后由 [`HandshakeEndpoint`] 交付协商规格与归还的收发通道。
    pub fn invite_async<'f, I, D>(
        self,
        entries: I,
        negotiator: D,
    ) -> HandshakeInviteAsync<'f, Rx, Tx, I, D>
    where
        Rx: 'f,
        Tx: 'f,
        I: IntoIterator<Item = NegotiationEntry<'f>> + 'f,
        D: for<'x> TrNegotiator<Entry<'x> = NegotiationEntry<'x>> + 'f,
    {
        HandshakeInviteAsync(self.rx_, self.tx_, entries, negotiator, PhantomData)
    }

    /// 执行完整的等待方握手：等待并**边收边判** `INVITE`、补全条件、
    /// 发送 `ACCEPT`、等待并校验 `CONFIRM`、发送空 `CONFRM`。
    ///
    /// - `local` 是本端的基础项取值，用于补全 `INVITE` 未提及的项。
    /// - `negotiator` 在接收 `INVITE` 时被调用一次；返回 `false` 时向对端发送
    ///   `REJECT` 并立即终止（不再读完剩余条目）。
    ///
    /// 返回的 future 输出 `Result<HandshakeEndpoint<Tx, Rx>, HandshakeError>`。
    pub fn listen_async<'f, D>(
        self,
        local: &'f BasicOpts,
        negotiator: D,
    ) -> ListenHandshakeAsync<'f, Rx, Tx, D>
    where
        Rx: 'f,
        Tx: 'f,
        D: for<'x> TrNegotiator<Entry<'x> = NegotiationEntry<'x>> + 'f,
    {
        ListenHandshakeAsync(self.rx_, self.tx_, local, negotiator)
    }
}

/// 把条目流交给协商器并等待结论。
///
/// 条目流由 [`FrameReader`] 的 [`TrAsyncIterator`] 实现提供：每产出一条就交给
/// 协商器判断，因此条目是**边收边判**的。
///
/// 取消由 `cancel` 统一施加（符合 AGENTS.md 纪律 4）；读侧的取消另由读状态机
/// 内部持有的令牌克隆在每次 `read_async` 上生效。
async fn negotiate_async<'m, D, I, K>(negotiator: &'m mut D, entries: I, cancel: &'m mut K) -> bool
where
    D: TrNegotiator,
    I: TrAsyncIterator<Item = D::Entry<'m>> + 'm,
    K: TrCancellationToken + Clone,
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
    cancel: &mut K,
) -> Result<(), HandshakeError<RE, W::Err>>
where
    W: TrBuffWrite<u8>,
    K: TrCancellationToken + Clone,
{
    write_frame_(tx, magic, values, &K_DEFAULT_CHECKSUM, cancel)
        .await
        .map_err(from_write_frame_err_)
}

/// 发起方状态机实现；对外经 [`HandshakeAgent::invite_async`] 使用。
#[gen_may_cancel_future(HandshakeInvite)]
async fn handshake_invite_async_<'f, R, W, I, D, K>(
    mut rx: R,
    mut tx: W,
    entries: I,
    mut negotiator: D,
    cancel: &'f mut K,
) -> Result<HandshakeEndpoint<W, R>, HandshakeError<R::Err, W::Err>>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    I: IntoIterator<Item = NegotiationEntry<'f>> + 'f,
    D: for<'x> TrNegotiator<Entry<'x> = NegotiationEntry<'x>> + 'f,
    K: TrCancellationToken + Clone,
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
    write_frame_async_(&mut tx, K_INVITE_MAGIC, &proposed, cancel).await?;

    // 2. 等待 ACCEPT：先判 magic，再**边收边判**条目。
    let mut reader = FrameReader::begin_async_(&mut rx, &mut *cancel)
        .await
        .map_err(from_read_frame_err_)?;
    if reader.magic_() == K_REJECT_MAGIC {
        return Result::Err(HandshakeError::PeerRejected);
    }
    if reader.magic_() != K_ACCEPT_MAGIC {
        return Result::Err(HandshakeError::InvalidMagic);
    }
    let accepted = negotiate_async(&mut negotiator, &mut reader, &mut *cancel).await;
    if !accepted {
        // 读侧失败（CRC 不匹配、保留键、对端关闭……）不能当成「本端主动拒绝」：
        // 按模块文档 §9，这类失败不得回 REJECT。
        if let Option::Some(err) = reader.take_error_() {
            return Result::Err(from_read_frame_err_(err));
        }
        // D4：协商中途拒绝 → 立即回 REJECT，不再读剩余条目。
        let _ = write_frame_async_::<W, K, R::Err>(&mut tx, K_REJECT_MAGIC, &empty, cancel).await;
        return Result::Err(HandshakeError::Rejected);
    }
    // 协商器可能在条目区读完前就接受；剩余条目仍要读完，CRC 才有一票否决的机会。
    if !reader.is_finished_() {
        reader.drain_async_().await.map_err(from_read_frame_err_)?;
    }

    // 3. ACCEPT 必须补全全部 4 项。
    let Option::Some(accepted_values) = complete_values_(reader.basics_()) else {
        let _ = write_frame_async_::<W, K, R::Err>(&mut tx, K_REJECT_MAGIC, &empty, cancel).await;
        return Result::Err(HandshakeError::Inconsistent);
    };
    let result = values_to_basic_(&accepted_values);

    // 4. 回显同一组数值作为 CONFIRM。
    write_frame_async_(&mut tx, K_CONFRM_MAGIC, &accepted_values, cancel).await?;

    // 5. 等待等待方的空 CONFRM。
    let mut done = FrameReader::begin_async_(&mut rx, &mut *cancel)
        .await
        .map_err(from_read_frame_err_)?;
    if done.magic_() != K_CONFRM_MAGIC {
        return Result::Err(HandshakeError::InvalidMagic);
    }
    done.drain_async_().await.map_err(from_read_frame_err_)?;
    if done.basics_().iter().any(Option::is_some) {
        return Result::Err(HandshakeError::Inconsistent);
    }

    Result::Ok(HandshakeEndpoint {
        opts: HandshakeOpts { basic_opts: result },
        tx,
        rx,
    })
}

/// 等待方状态机实现；对外经 [`HandshakeAgent::listen_async`] 使用。
#[gen_may_cancel_future(ListenHandshake)]
async fn listen_handshake_async_<'f, R, W, D, K>(
    mut rx: R,
    mut tx: W,
    local: &'f BasicOpts,
    mut negotiator: D,
    cancel: &'f mut K,
) -> Result<HandshakeEndpoint<W, R>, HandshakeError<R::Err, W::Err>>
where
    R: TrBuffRead<u8> + 'f,
    W: TrBuffWrite<u8> + 'f,
    D: for<'x> TrNegotiator<Entry<'x> = NegotiationEntry<'x>> + 'f,
    K: TrCancellationToken + Clone,
{
    let empty: [Option<usize>; K_BASIC_KEY_COUNT] = [Option::None; K_BASIC_KEY_COUNT];

    // 1. 等待 INVITE，并**边收边判**。
    let mut reader = FrameReader::begin_async_(&mut rx, &mut *cancel)
        .await
        .map_err(from_read_frame_err_)?;
    if reader.magic_() != K_INVITE_MAGIC {
        return Result::Err(HandshakeError::InvalidMagic);
    }
    let accepted = negotiate_async(&mut negotiator, &mut reader, &mut *cancel).await;
    if !accepted {
        // 读侧失败不能当成「本端主动拒绝」：按模块文档 §9，这类失败不得回 REJECT。
        if let Option::Some(err) = reader.take_error_() {
            return Result::Err(from_read_frame_err_(err));
        }
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

    // 3. 发送 ACCEPT。
    write_frame_async_(&mut tx, K_ACCEPT_MAGIC, &values, cancel).await?;

    // 4. 等待并校验 CONFIRM（协议层逐值比较，不再走协商器）。
    let mut confirm = FrameReader::begin_async_(&mut rx, &mut *cancel)
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
    Result::Ok(HandshakeEndpoint {
        opts: HandshakeOpts { basic_opts: result },
        tx,
        rx,
    })
}

#[cfg(test)]
mod tests_ {
    use core::mem::MaybeUninit;

    use abs_art_bridge::Runtime;
    use abs_cancel::{NonCancellableToken, TrMayCancel};
    use abs_mm::mem_alloc::CoreAlloc;
    use buffex::circular_buff::builder::CircularBuffBuilder;
    use mm_ptr::{Owned, x_deps::abs_mm};

    use super::*;

    /// 只接受 `max_packet_size` 不超过给定上限的协商器。
    struct CapPacketSize {
        /// 允许的最大报文长度。
        cap_: usize,

        /// 已经处理过的条目数量，便于断言「边收边判」确实逐条发生。
        seen_: usize,
    }

    /// `CapPacketSize` 的协商逻辑；future 类型由 [`gen_may_cancel_future`] 生成。
    #[gen_may_cancel_future(CapPacketSizeAccept)]
    async fn cap_packet_size_accept_async_<'f, I, C>(
        this: &'f mut CapPacketSize,
        mut entries: I,
        _cancel: &'f mut C,
    ) -> bool
    where
        I: TrAsyncIterator<Item = NegotiationEntry<'f>> + 'f,
        C: TrCancellationToken + Clone,
    {
        loop {
            let Ok(maybe) = entries.next_async().await else {
                return false;
            };
            let Option::Some(entry) = maybe else {
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

        type ShouldAcceptAsync<'f, I> = CapPacketSizeAcceptAsync<'f, I>
        where
            Self: 'f,
            I: TrAsyncIterator<Item = NegotiationEntry<'f>> + 'f;

        fn should_accept_async<'f, I>(&'f mut self, entries: I) -> Self::ShouldAcceptAsync<'f, I>
        where
            I: TrAsyncIterator<Item = NegotiationEntry<'f>> + 'f,
        {
            CapPacketSizeAcceptAsync(self, entries)
        }
    }

    /// 建一对互联的环形缓冲，返回两个已经连好通道的握手对象。
    async fn make_pair_(
        buff_size: usize,
    ) -> (
        HandshakeAgent<impl TrBuffRead<u8>, impl TrBuffWrite<u8>>,
        HandshakeAgent<impl TrBuffRead<u8>, impl TrBuffWrite<u8>>,
    ) {
        let (a_tx, b_rx) = CircularBuffBuilder::
            try_with_buffer(
                Owned::<[MaybeUninit<u8>], _>::new_uninit_slice(buff_size, CoreAlloc),
                CoreAlloc,
            )
            .expect("分配 a→b 缓冲")
            .consumer_passive()
            .producer_passive()
            .build_async()
            .await
            .expect("构建 a→b 缓冲");
        let (b_tx, a_rx) = CircularBuffBuilder::
            try_with_buffer(
                Owned::<[MaybeUninit<u8>], _>::new_uninit_slice(buff_size, CoreAlloc),
                CoreAlloc,
            )
            .expect("分配 b→a 缓冲")
            .consumer_passive()
            .producer_passive()
            .build_async()
            .await
            .expect("构建 b→a 缓冲");
        (
            HandshakeAgent::new(a_rx, a_tx),
            HandshakeAgent::new(b_rx, b_tx),
        )
    }

    /// 测试握手能在一对环形缓冲上完整走通，且协商结果等于双方声明的缺省值。
    /// - 手段：把等待方与发起方分别投递到 compio 运行时并发执行；发起方声明
    ///   [`BasicOpts::DEFAULT`]，等待方本地值同样是缺省值，协商器接受一切。
    /// - 判断：两侧 future 都返回 `Ok`，且各自拿到的 `max_packet_size` 都是
    ///   4096（`BasicOpts::DEFAULT`）。
    #[compio::test]
    async fn handshake_through_circ_buff_test_() {
        let (a, b) = make_pair_(16usize).await;

        let a_accept = Runtime::spawn_local(async move {
            let local_opts = BasicOpts::default();
            a.listen_async(&local_opts, AcceptAllEntry)
                .may_cancel_with(NonCancellableToken::shared_mut())
                .await
        });
        let b_invite = Runtime::spawn_local(async move {
            let proposed = BasicOpts::default();
            b.invite_async(&proposed, AcceptAllEntry)
                .may_cancel_with(NonCancellableToken::shared_mut())
                .await
        });

        let accepted = a_accept.await.expect("等待方任务不应 panic");
        let invited = b_invite.await.expect("发起方任务不应 panic");
        assert!(accepted.is_ok(), "等待方握手应当成功");
        assert!(invited.is_ok(), "发起方握手应当成功");
        let accepted = accepted.unwrap();
        let invited = invited.unwrap();
        assert_eq!(accepted.opts.basic_opts.max_packet_size, 4096usize);
        assert_eq!(invited.opts.basic_opts.max_packet_size, 4096usize);
    }

    /// 测试读侧校验失败不会被误报成「本端主动拒绝」。
    /// - 手段：手工构造一个 CRC 被翻转的 `INVITE` 字节流交给等待方读取；协商器
    ///   在条目流上只能看到「终止」，详细原因由读状态机记录。
    /// - 判断：等待方返回 `ChecksumErr` 而不是 `Rejected`——按模块文档 §9，
    ///   校验失败不得回 `REJECT`。
    #[compio::test]
    async fn checksum_failure_is_not_reported_as_rejected() {
        let values = [
            Option::Some(4096usize),
            Option::Some(1usize << 28),
            Option::Some(64usize),
            Option::Some(30usize),
        ];
        let mut buf = [0u8; 64];
        let capacity = buf.len();
        let len = {
            let mut cursor: &mut [u8] = &mut buf;
            let mut token = NonCancellableToken::new();
            write_frame_(
                &mut cursor,
                K_INVITE_MAGIC,
                &values,
                &K_DEFAULT_CHECKSUM,
                &mut token,
            )
            .await
            .expect("写帧应当成功");
            capacity - cursor.len()
        };
        // 翻转 CRC 的最后一个字节。
        buf[len - 1] ^= 0xFF;

        let rx: &[u8] = &buf[..len];
        let mut sink = [0u8; 64];
        let agent = HandshakeAgent::new(rx, &mut sink[..]);
        let local = BasicOpts::default();
        let res = agent
            .listen_async(&local, AcceptAllEntry)
            .may_cancel_with(NonCancellableToken::shared_mut())
            .await;
        assert!(
            matches!(res, Result::Err(HandshakeError::ChecksumErr)),
            "校验失败必须按其本身分类，而不是 Rejected"
        );
    }

    /// 测试协商器拒绝时握手立即失败（发起方收到 `PeerRejected`）。
    /// - 手段：等待方的协商器把 `max_packet_size` 上限压到 1，而发起方声明
    ///   缺省值 4096，于是等待方在读到该条目的**那一刻**就拒绝。
    /// - 判断：等待方返回 `Rejected`，发起方返回 `PeerRejected`，即拒绝路径
    ///   确实立即回了 `REJECT`，而不是等整帧收完。
    #[compio::test]
    async fn negotiator_rejects_immediately() {
        let (a, b) = make_pair_(16usize).await;

        let a_accept = Runtime::spawn_local(async move {
            let local_opts = BasicOpts::default();
            a.listen_async(
                &local_opts,
                CapPacketSize {
                    cap_: 1usize,
                    seen_: 0usize,
                },
            )
            .may_cancel_with(NonCancellableToken::shared_mut())
            .await
        });
        let b_invite = Runtime::spawn_local(async move {
            let proposed = BasicOpts::default();
            b.invite_async(&proposed, AcceptAllEntry)
                .may_cancel_with(NonCancellableToken::shared_mut())
                .await
        });

        let accepted = a_accept.await.expect("等待方任务不应 panic");
        let invited = b_invite.await.expect("发起方任务不应 panic");
        assert!(matches!(accepted, Result::Err(HandshakeError::Rejected)));
        assert!(matches!(invited, Result::Err(HandshakeError::PeerRejected)));
    }
}
