//! 数据报端点：[`Telegraph`] 与它的两个半边 [`TelegraphTx`] / [`TelegraphRx`]。
//!
//! 数据报（telegraph）是类似 UDP 的短消息通道：**不需要建流握手**，但按
//! `abs_smux` 的约定，`channel` 与 `telegraph` **不得共用同一个 local_dock**，
//! 因此它在 `local_dock` 上与 channel / listener 互斥（登记在统一身份表的
//! `(local, wildcard)` / `(local, 具体值)` 之外，占 `(local, unspecified)`，
//! 见 `mux_connection::registry_`）。
//!
//! # 语义（本轮定稿）
//!
//! - **一次提交 = 一条报文 = 一条 `DATAGRAM` 帧**。应用先把载荷写进发送环
//!   （[`TelegraphTx`] 实现 `TrBuffWrite`），再用
//!   [`send_async`](TrTelegraphTx::send_async) 提交；提交量取「环里已写、尚未提交的
//!   字节数」，返回的就是这条报文的载荷长度。
//! - **长度在发送前已知**：报文长度由提交那一刻确定，帧头的 `PayloadLen` 直接照搬，
//!   因此接收端不需要任何额外的长度前缀。
//! - **跨段也能给**：发送环容量可以小于 `max_packet_size`，连接按环当前给出的段分块
//!   推进（与 channel 的数据帧走同一条入环路径）。
//! - **不参与流控**：没有接收窗口、没有 `WINDOW_UPDATE`、没有信用回补，也不靠保活
//!   `PULSE` 维持（计时循环不为 telegraph 建任何时钟）。
//! - **接收整条丢弃**：接收环剩余空间装不下整条报文时**丢弃该条**（不截断、不重传），
//!   并计入 metrics；应用会继续等**下一条**，不会拿到半条。
//!
//! # 两侧的「报文边界」记在哪
//!
//! 环是**字节**缓冲，本身不知道「哪几个字节是一条报文」。因此两个方向各有一份
//! **条目队列**（内联在身份节点 `TgRec_` 里，零堆分配）：提交时把
//! **`(目的 dock, 载荷长度)`** 一起入队（发送方向），收下一条报文时把长度入队
//! （接收方向）；消费侧按 FIFO 取得条目再从环里精确取走。环里因此始终只有**载荷
//! 字节**，没有长度前缀。
//!
//! 发送方向之所以连**目的地址**一起入队：它是逐次发送的实参，同一个端点的不同报文
//! 可以发往不同 dock，因此它必须与长度一起按序记下来，循环才可能为每条报文取到正确
//! 地址（见 `TgRec_::out_` 的文档）。
//!
//! # 身份的寿命由两个半边共同决定
//!
//! telegraph 的 `local_dock` 是**一个**身份，却由 tx / rx 两个对象使用。任一半边先被
//! 丢弃都不该释放身份（另一半还在用），因此两半各持一份 [`TgIdentityGuard_`]：其中
//! 一份发现自己已经是最后一份强引用时才投递
//! [`SessionEvent_::ReleaseTelegraph`](crate::connection::signal_::SessionEvent_)。

use core::{
    alloc::AllocatorClone,
    future::poll_fn,
    mem::MaybeUninit,
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::TrCancellationToken;
use abs_mm::res_man::TrUnique;
use abs_smux::{
    chan::RingBuffAlloc,
    conn::{TrTelegraph, TrTelegraphRx, TrTelegraphTx},
};
use buffex::{
    ring::{ConsumerError, ProducerError},
    x_deps::{abs_buff, anylr::SomeOf},
};
use mm_ptr::x_deps::abs_mm;

use crate::{
    connection::{
        Dock, MuxConnection, MuxError, TrConnCfg,
        owner_::{TgLenQueue_, TgOwner_},
        ring_::{BufferedRx, BufferedTx, RingBuildErr, new_buffered_channel},
        session_::race_cancel_,
        signal_::{SessionEvent_, TrEventSender_, WriteEvent_},
    },
};

/// 数据报端点的错误类型（[`TrTelegraphTx::send_async`] /
/// [`TrTelegraphRx::recv_async`] 共用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TelegraphError {
    /// 待发报文的**长度队列**已满（容量 [`K_TG_LEN_QUEUE`] 条）。
    ///
    /// 它表示「应用提交得比网络送得快」。数据报是尽力交付，因此这里如实报错让应用自己
    /// 决定重试 / 丢弃，而不是让连接替它排队到无限。
    #[error("待发数据报的长度队列已满")]
    OutboxFull,

    /// 调用方给出的环内存建不出环（容量不在 `buffex::ring` 允许区间内，或块分配失败）。
    #[error("调用方给出的数据报环内存不合用")]
    RingRejected,

    /// 发送环已关闭（发送半边被丢弃，或连接已拆）。
    #[error("数据报发送环已关闭")]
    TxClosed,

    /// 接收环已关闭（接收半边被丢弃，或连接已拆）。
    #[error("数据报接收环已关闭")]
    RxClosed,

    /// 本次提交的长度不满足 `demand` 给出的区间。
    ///
    /// 连接不替调用方补齐、也不截断：长度由调用方通过 `Demand` 自己表达。
    #[error("数据报长度不满足 Demand：{0}")]
    Demand(DemandErr),

    /// 连接级失败：`Display` 用内层文案，`source` 指回内层（`?` 亦直通）。
    #[error("{0}")]
    Mux(#[from] MuxError),
}

/// [`TelegraphError::Demand`] 的两档成因。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DemandErr {
    /// 环里可提交的字节数**不足** `demand` 的下界。
    #[error("环里可提交的字节数不足下界")]
    Unfulfilled,

    /// 环里可提交的字节数**超过** `demand` 的上界。
    #[error("环里可提交的字节数超过上界")]
    TooLong,
}

/// 数据报端点工厂。
///
/// **不借用连接**：自己持有一份 [`MuxConnection`] 克隆，因此生命周期参数从公开类型上
/// 消失，可以存进结构体、可以从函数返回。用 [`split`](TrTelegraph::split) 取出收发两个
/// 半边之后即可分别使用。
pub struct Telegraph<C>
where
    C: TrConnCfg,
{
    /// 连接智能指针：两个半边各克隆一份（通知循环 + 收尾时释放身份）。
    conn_: MuxConnection<C>,

    /// 本端 dock（两个半边共享同一个身份）。
    local_dock_: Dock,

    /// 身份守卫（split 时克隆给两个半边）。
    identity_: TgIdentityGuard_<C>,

    /// 应用侧发送环生产端；`split` 时交出。
    tx_w_: Option<BufferedTx>,

    /// 应用侧接收环消费端；`split` 时交出。
    rx_r_: Option<BufferedRx>,

    /// 身份守卫的第二份：`split` 时交给接收半边。
    ///
    /// 工厂期就持两份而不是 `split` 时才克隆：这样「开放后直接丢弃、从不 split」也走
    /// 与「两个半边都被丢弃」完全相同的释放路径（两份守卫一起放下 → 最后一份释放身份）。
    rx_identity_: TgIdentityGuard_<C>,
}

impl<C> Telegraph<C>
where
    C: TrConnCfg,
{
    /// 由连接、`local_dock`、身份句柄与**应用侧两个环半部**构造
    /// （只允许 `open_telegraph_async` 调用）。
    ///
    /// `tx_w` 是发送环的生产端（应用写），`rx_r` 是接收环的消费端（应用读）；对应的
    /// 另一半已经交给两个中心循环（见 `binding_::mux_open_telegraph_async_`）。
    pub(crate) fn new_(
        conn: MuxConnection<C>,
        local_dock: Dock,
        rec: TgOwner_<C::Alloc>,
        tx_w: BufferedTx,
        rx_r: BufferedRx,
    ) -> Self {
        let guard = TgIdentityGuard_::new_(rec, conn.clone(), local_dock);
        let rx_guard = guard.clone();
        // 工厂把两半都持有起来，`split` 时再分出去；这样「开放后不 split 直接丢弃」
        // 也走同一条释放路径（两份守卫一起被丢弃）。
        Telegraph {
            conn_: conn,
            local_dock_: local_dock,
            identity_: guard,
            tx_w_: Option::Some(tx_w),
            rx_r_: Option::Some(rx_r),
            rx_identity_: rx_guard,
        }
    }
}

impl<C> TrTelegraph<C> for Telegraph<C>
where
    C: TrConnCfg,
{
    type Err = TelegraphError;

    type Tx = TelegraphTx<C>;
    type Rx = TelegraphRx<C>;

    fn local_dock(&self) -> C::Dock {
        self.local_dock_
    }

    fn split(mut self) -> (Self::Tx, Self::Rx) {
        // `Option::take` 而非按字段 move：本类型实现了 `Drop`（未 split 就丢弃时要释放
        // 身份），因此不能把字段整体移出。
        let tx_w = self.tx_w_.take().expect("端点工厂只能 split 一次");
        let rx_r = self.rx_r_.take().expect("端点工厂只能 split 一次");
        let rx_guard = self.rx_identity_.clone();
        let tx_guard = self.identity_.clone();
        let conn = self.conn_.clone();
        let local = self.local_dock_;
        (
            TelegraphTx::new_(tx_w, conn.clone(), local, tx_guard),
            TelegraphRx::new_(rx_r, conn, local, rx_guard),
        )
    }
}

impl<C> Drop for Telegraph<C>
where
    C: TrConnCfg,
{
    /// 未 `split` 就丢弃工厂：两个半部（以及身份守卫的两份克隆）都随之释放。
    ///
    /// 这里**不需要**额外动作——`tx_w_` / `rx_r_` 被 drop 会各自把环的对应端放下，
    /// 身份则由 [`TgIdentityGuard_`] 的最后一份负责释放。
    fn drop(&mut self) {}
}

/// 数据报的**发送半边**。
///
/// 实现 [`TrBuffWrite`]（应用把载荷写进本地发送环），并用
/// [`send_async`](TrTelegraphTx::send_async) 提交一条报文。真正的上网由复用循环完成
/// ——它持有一条**发送环的读端**（经 `WriteEvent_::TgAttach` 交接）。
///
/// # Drop
///
/// 丢弃发送半边只收尾**发送方向**：关掉发送环的生产端（唤醒可能正 park 在该环上的复用
/// 循环）并通知循环摘掉本地表项、丢弃队列里尚未送出的项。接收半边是否还在与本对象无关
/// ——身份由两个半边的守卫共同持有。
pub struct TelegraphTx<C>
where
    C: TrConnCfg,
{
    /// 发送环的**生产端**（应用写、复用循环读）。
    ring_: BufferedTx,

    /// 连接智能指针（提交时通知复用循环）。
    conn_: MuxConnection<C>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 身份守卫（与接收半边各持一份）。
    identity_: TgIdentityGuard_<C>,
}

impl<C> TelegraphTx<C>
where
    C: TrConnCfg,
{
    /// 由发送环生产端、连接、dock 与身份守卫构造。
    pub(crate) fn new_(
        ring: BufferedTx,
        conn: MuxConnection<C>,
        local_dock: Dock,
        identity: TgIdentityGuard_<C>,
    ) -> Self {
        TelegraphTx {
            ring_: ring,
            conn_: conn,
            local_dock_: local_dock,
            identity_: identity,
        }
    }

    /// 把 `bytes` 全部写进发送环（**不**提交）。
    ///
    /// 段级循环的省事封装：借一段、能写多少写多少、段 drop 时按写入量提交，直到写完。
    /// 每次只按「至少 1 字节」索要，因此**与环容量无关**——写多大的报文都行，环小就多
    /// 借几次。
    ///
    /// # Errors
    ///
    /// 发送方向已关闭（[`ProducerError::Closing`]）或底层写失败时返回错误；已经写出去
    /// 的部分**不会回滚**（它仍会在下一次提交时被算进报文）。
    ///
    /// # Examples
    ///
    /// ```ignore
    /// tx.write_all(b"hello").await?;
    /// let len = tx.send_async(&Demand::exactly(5)).await?;
    /// assert_eq!(len, 5);
    /// ```
    pub async fn write_all(&mut self, bytes: &[u8]) -> Result<(), ProducerError<usize>> {
        let mut offset = 0usize;
        while offset < bytes.len() {
            let demand = Demand::at_least(1usize);
            let mut outcome = TrBuffWrite::write_async(self, &demand).await;
            let put = match outcome.as_mut().pick_left() {
                Option::Some(segm) => {
                    segm.as_segm_mut().clone_items_from_buff(&bytes[offset..])
                }
                Option::None => {
                    return Result::Err(match outcome.pick_right() {
                        Option::Some(err) => err,
                        Option::None => ProducerError::Closing,
                    });
                }
            };
            debug_assert!(put > 0usize, "借出的空段不可能写进 0 字节");
            if put == 0usize {
                return Result::Err(ProducerError::Closing);
            }
            offset += put;
        }
        Result::Ok(())
    }
}

impl<C> Drop for TelegraphTx<C>
where
    C: TrConnCfg,
{
    /// 丢弃发送半边：关掉发送环的生产端，并通知复用循环摘表。
    ///
    /// 不在这里释放身份——接收半边可能还在用同一个 `local_dock`（见模块文档「身份的寿命
    /// 由两个半边共同决定」）。
    fn drop(&mut self) {
        // 显式关闭：`buffex` 的环半部被 drop **不会**置位关闭标记，少了这一步，正 park
        // 在该环上的复用循环永远醒不过来。
        self.ring_.close();
        let _ = self
            .conn_
            .core_()
            .w_events_()
            .try_send_event_(WriteEvent_::TgTxClosed {
                local_dock: self.local_dock_,
            });
    }
}

impl<C> TrBuffTryWrite<u8> for TelegraphTx<C>
where
    C: TrConnCfg,
{
    type SegmMut<'f>
        = <BufferedTx as TrBuffTryWrite<u8>>::SegmMut<'f>
    where
        Self: 'f;

    type Err = <BufferedTx as TrBuffTryWrite<u8>>::Err;

    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        self.ring_.try_write(demand)
    }
}

impl<C> TrBuffWrite<u8> for TelegraphTx<C>
where
    C: TrConnCfg,
{
    type WriteAsync<'f> = <BufferedTx as TrBuffWrite<u8>>::WriteAsync<'f>
    where
        Self: 'f;

    fn write_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::WriteAsync<'f> {
        self.ring_.write_async(demand)
    }
}

impl<C> TrTelegraphTx<C> for TelegraphTx<C>
where
    C: TrConnCfg,
{
    type Err = TelegraphError;

    type SendAsync<'f> = MuxSendTgAsync<'f, 'f, C>
    where
        Self: 'f;

    fn local_dock(&self) -> C::Dock {
        self.local_dock_
    }

    fn send_async<'f>(
        &'f mut self,
        remote_dock: C::Dock,
        demand: &'f Demand<usize>,
    ) -> Self::SendAsync<'f> {
        MuxSendTgAsync::new(self, remote_dock, demand)
    }
}

/// [`TrTelegraphTx::send_async`] 的 step 函数。
///
/// 它**同步完成**（没有 park 点）：把「环里已写、尚未提交的字节数」记进身份节点的发送
/// 长度队列，并通知复用循环。真正的分块搬运与成帧都在循环里做。
#[gen_may_cancel_future(MuxSendTg, pub, new(pub(crate)))]
async fn mux_send_tg_async_<'f, C, K>(
    tx: &'f mut TelegraphTx<C>,
    remote_dock: C::Dock,
    demand: &'f Demand<usize>,
    _cancel: K,
) -> Result<usize, TelegraphError>
where
    C: TrConnCfg + 'f,
    K: TrCancellationToken,
{
    // 1. 「环里已写、尚未提交」的字节数 = **环内可读量 − 队列里已提交的字节数**。
    //
    //    关键是两项**同步递减**：复用循环每成功送出一条报文就 `try_pop_`，于是
    //    `bytes_` 与它从环里取走的量同时减少，差值（= 本次可提交量）不受消费影响。
    //    反过来，若循环「先取走环里的字节、之后才 pop 长度记录」，这个差值会在两次
    //    操作之间**暂时变小**，提交量因此被算少、报文内容随之错位——所以
    //    `send_one_datagram_` 把 `try_pop_` 放在成功写出之后**紧邻**的位置。
    let pending = tx
        .ring_
        .ring_state()
        .data_size()
        .saturating_sub(tx.identity_.out_().bytes_());

    // 2. 按 `demand` 校验：下界不足 / 上界超出都由调用方自己表达，连接不补齐、不截断。
    if let Option::Some(min) = demand.min()
        && pending < min
    {
        return Result::Err(TelegraphError::Demand(DemandErr::Unfulfilled));
    }
    if let Option::Some(max) = demand.max()
        && pending > max
    {
        return Result::Err(TelegraphError::Demand(DemandErr::TooLong));
    }

    // 3. 入队 + 通知复用循环。队列满是**应用该看到的背压**（提交得比网络送得快），因此
    //    如实报错而不是在这里 park。
    //
    //    长度为 0 的提交也入队：它是一条**空报文**，与「什么都没提交」不同。
    tx.identity_
        .out_()
        .try_push_(remote_dock, pending)
        .map_err(|_| TelegraphError::OutboxFull)?;

    // 唤醒复用循环：它可能正 park 在事件通道上（也可能正在排空主循环里别的端点）。
    // 投递失败表示循环已经不在了（连接正在收尾），此时本次提交不会被搬运——但**不必**
    // 在这里报错：接收侧的 `recv_async` / 下一次提交会经环的关闭态看到连接已收尾。
    let _ = tx
        .conn_
        .core_()
        .w_events_()
        .try_send_event_(WriteEvent_::TgReady {
            local_dock: tx.local_dock_,
        });
    Result::Ok(pending)
}

/// 数据报的**接收半边**。
///
/// 实现 [`TrBuffRead`]：解复用循环把整条报文写进接收环、长度入队；
/// [`recv_async`](TrTelegraphRx::recv_async) 取出下一条的长度，应用再按该长度从本对象
/// 读走内容。
pub struct TelegraphRx<C>
where
    C: TrConnCfg,
{
    /// 接收环的**消费端**（解复用循环写、应用读）。
    ring_: BufferedRx,

    /// 连接智能指针（保活 + 连接级失败查询）。
    conn_: MuxConnection<C>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 身份守卫（与发送半边各持一份）。
    identity_: TgIdentityGuard_<C>,
}

impl<C> TelegraphRx<C>
where
    C: TrConnCfg,
{
    /// 由接收环消费端、连接、dock 与身份守卫构造。
    pub(crate) fn new_(
        ring: BufferedRx,
        conn: MuxConnection<C>,
        local_dock: Dock,
        identity: TgIdentityGuard_<C>,
    ) -> Self {
        TelegraphRx {
            ring_: ring,
            conn_: conn,
            local_dock_: local_dock,
            identity_: identity,
        }
    }

    /// 从接收环读满 `out`（**恰好** `out.len()` 字节）。
    ///
    /// 配合 [`recv_async`](TrTelegraphRx::recv_async) 使用：先拿到本条报文的长度，再按该
    /// 长度调用本方法。段级循环，每次只按「至少 1 字节」索要，因此与环容量无关。
    ///
    /// # Errors
    ///
    /// 接收方向已关闭（读到 EOF）或底层读失败时返回错误；已经读到的部分不会回滚。
    pub async fn read_exact(&mut self, out: &mut [u8]) -> Result<(), ConsumerError<usize>> {
        let mut offset = 0usize;
        while offset < out.len() {
            let rest = out.len() - offset;
            let demand = Demand::at_least(1usize);
            let mut outcome = TrBuffRead::read_async(self, &demand).await;
            let got = match outcome.as_mut().pick_left() {
                Option::Some(segm) => {
                    let mut child = segm.as_segm_ref();
                    let limit = core::cmp::min(rest, child.least_count());
                    let dst = &mut out[offset..offset + limit];
                    // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同（同尺寸、同对齐、无
                    // niche），且 `dst` 是本函数独占的可写切片；`move_items_to_buff` 只
                    // 写入其中已初始化的前缀并返回写入长度，因此不会读到未初始化内存，
                    // 也不会越界。
                    let uninit = unsafe {
                        core::slice::from_raw_parts_mut(
                            dst.as_mut_ptr() as *mut MaybeUninit<u8>,
                            dst.len(),
                        )
                    };
                    unsafe { child.move_items_to_buff(uninit) }
                }
                Option::None => {
                    return Result::Err(match outcome.pick_right() {
                        Option::Some(err) => err,
                        Option::None => ConsumerError::Closing,
                    });
                }
            };
            debug_assert!(got > 0usize, "有数据的段不可能搬出 0 字节");
            if got == 0usize {
                return Result::Err(ConsumerError::Closing);
            }
            offset += got;
        }
        Result::Ok(())
    }
}

impl<C> TrBuffTryRead<u8> for TelegraphRx<C>
where
    C: TrConnCfg,
{
    type SegmRef<'f>
        = <BufferedRx as TrBuffTryRead<u8>>::SegmRef<'f>
    where
        Self: 'f;

    type Err = <BufferedRx as TrBuffTryRead<u8>>::Err;

    fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        self.ring_.try_read(demand)
    }
}

impl<C> TrBuffRead<u8> for TelegraphRx<C>
where
    C: TrConnCfg,
{
    type ReadAsync<'f>
        = <BufferedRx as TrBuffRead<u8>>::ReadAsync<'f>
    where
        Self: 'f;

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        self.ring_.read_async(demand)
    }
}

impl<C> TrTelegraphRx<C> for TelegraphRx<C>
where
    C: TrConnCfg,
{
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

/// [`TrTelegraphRx::recv_async`] 的 step 函数。
#[gen_may_cancel_future(MuxRecvTg, pub, new(pub(crate)))]
async fn mux_recv_tg_async_<'f, C, K>(
    rx: &'f mut TelegraphRx<C>,
    cancel: K,
) -> Result<usize, TelegraphError>
where
    C: TrConnCfg + 'f,

    K: TrCancellationToken,
{
    loop {
        // 1. 队列里有下一条报文的长度吗？有就说明解复用循环**已经**把整条载荷写进了接收
        //    环（先写环、再入队），因此调用者随后的 `read_exact(len)` 一定读得到。
        if let Option::Some((_, len)) = rx.identity_.in_().try_pop_() {
            // 队列腾出了一个槽位：解复用循环可能正等它（它按「队列满则整条丢弃」自保，
            // 但腾位通知仍然要发，否则要等到下一次才有机会）。
            rx.identity_.in_().notify_();
            return Result::Ok(len);
        }
        // 2. 连接级失败 / 接收环已关：如实报错，而不是无限等一条不会来的报文。
        if let Option::Some(reason) = rx.conn_.core_().reg_().fail_kind_() {
            return Result::Err(TelegraphError::Mux(MuxError::ConnFailed(reason)));
        }
        if rx.ring_.ring_state().is_producer_closed() {
            return Result::Err(TelegraphError::RxClosed);
        }
        // 3. 等一次「队列非空」通知（可取消：等待期间调用方可以发主动取消信号）。
        let wait = poll_fn(|cx| rx.identity_.in_().poll_wait_(cx));
        match race_cancel_(&cancel, wait).await {
            Option::Some(()) => {}
            Option::None => return Result::Err(TelegraphError::Mux(MuxError::Cancelled)),
        }
    }
}

/// telegraph 端点身份的**共享守卫**：两份克隆（tx / rx）都丢掉时才释放身份。
///
/// # 为什么需要它
///
/// telegraph 的 `local_dock` 是**一个**身份，却由 tx / rx 两个对象使用。任一半边先被
/// 丢弃都不该释放身份（另一半还在用同一个 dock），所以两半各持一份本守卫；每份在被丢弃
/// 时先看「现在还剩几份强引用」——`<= 1` 说明自己是最后一份，由它投递
/// [`SessionEvent_::ReleaseTelegraph`](crate::connection::signal_::SessionEvent_)。
///
/// # `Drop` 不取锁
///
/// 释放只投一条消息（与 `ChannelTx` / `ChannelRx` 的 `Drop` 同一条纪律）：真正的身份表
/// 改动由核心执行者在异步上下文里落实。
pub(crate) struct TgIdentityGuard_<C>
where
    C: TrConnCfg,
{
    /// 身份节点句柄（最后一份被丢弃时节点随之释放）。
    owner_: TgOwner_<C::Alloc>,

    /// 连接智能指针（投递释放消息用）。
    conn_: MuxConnection<C>,

    /// 本端 dock。
    local_dock_: Dock,
}

impl<C> TgIdentityGuard_<C>
where
    C: TrConnCfg,
{
    /// 由身份句柄、连接与 dock 构造。
    fn new_(owner: TgOwner_<C::Alloc>, conn: MuxConnection<C>, local_dock: Dock) -> Self {
        TgIdentityGuard_ {
            owner_: owner,
            conn_: conn,
            local_dock_: local_dock,
        }
    }

    /// 发送方向的长度队列（身份节点内联）。
    fn out_(&self) -> &TgLenQueue_ {
        self.owner_.out_()
    }

    /// 接收方向的长度队列（身份节点内联）。
    fn in_(&self) -> &TgLenQueue_ {
        self.owner_.in_()
    }
}

impl<C> Clone for TgIdentityGuard_<C>
where
    C: TrConnCfg,
{
    fn clone(&self) -> Self {
        TgIdentityGuard_ {
            owner_: self.owner_.clone(),
            conn_: self.conn_.clone(),
            local_dock_: self.local_dock_,
        }
    }
}

impl<C> Drop for TgIdentityGuard_<C>
where
    C: TrConnCfg,
{
    fn drop(&mut self) {
        // 此刻本份仍持有 `owner_`，因此 `<= 1` 表示本份就是最后一份强引用。先问再放下，
        // 顺序不可反：放下之后节点可能已经被回收。
        if !self.owner_.is_last_strong_ref_() {
            return;
        }
        let _ = self
            .conn_
            .core_()
            .reg_()
            .post_session_event_(SessionEvent_::ReleaseTelegraph {
                local_dock: self.local_dock_,
            });
    }
}

/// 建好的两条环：`(发送环对, 接收环对)`，每对是 `(写端, 读端)`。
///
/// 它把「调用方交出的智能指针 `B`」从后续类型里彻底擦除：`open_telegraph_async` 同步
/// 建环之后，future 只持有本类型（与 channel 裁决路径交给 future 的产物同形）。
pub(crate) type TelegraphRings_ = ((BufferedTx, BufferedRx), (BufferedTx, BufferedRx));

/// 由调用方交出的两块环内存建出 `(发送环写端, 发送环读端)` 与
/// `(接收环写端, 接收环读端)`。
///
/// 这就是 `open_telegraph_async` 的**纯本地**部分：不取锁、不碰注册表，只建环。失败时
/// 把 [`RingBuildErr`] 如实映射成 [`TelegraphError`]。
///
/// # Errors
///
/// 容量不在 `buffex::ring` 允许区间内、或块分配失败 → [`TelegraphError::RingRejected`]
/// （对调用方而言都是「这块内存不合用」）。
pub(crate) fn build_telegraph_rings_<B>(
    buffs: RingBuffAlloc<B, u8>,
) -> Result<TelegraphRings_, TelegraphError>
where
    B: 'static + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone> + Send + Sync,
{
    let (tx_buff, rx_buff) = buffs.destruct();
    let tx_pair = new_buffered_channel(tx_buff).map_err(map_ring_err_)?;
    let rx_pair = new_buffered_channel(rx_buff).map_err(map_ring_err_)?;
    Result::Ok((tx_pair, rx_pair))
}

/// 把建环失败映射为 telegraph 的错误档位。
fn map_ring_err_(err: RingBuildErr) -> TelegraphError {
    match err {
        RingBuildErr::Capacity(_) | RingBuildErr::Alloc => TelegraphError::RingRejected,
    }
}
