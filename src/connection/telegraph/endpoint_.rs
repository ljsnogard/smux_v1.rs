//! 数据报端点：[`Telegraph`] 与它的两个半边 [`Sender`] / [`Receiver`]。
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
//!   （[`Sender`] 实现 `TrBuffWrite`），再用
//!   [`send_async`](abs_smux::telegraph::TrDatagramSender::send_async) 提交；提交量取
//!   「环里已写、尚未提交的字节数」，返回的就是这条报文的载荷长度。目的地址是
//!   **逐次**发送的实参（同 UDP 的 `sendto`），不是端点的固有属性。
//! - **长度在发送前已知**：报文长度由提交那一刻确定，帧头的 `PayloadLen` 直接照搬，
//!   因此接收端不需要任何额外的长度前缀。
//! - **跨段也能给**：发送环容量可以小于 `max_packet_size`，连接按环当前给出的段分块
//!   推进（与 channel 的数据帧走同一条入环路径）。
//! - **不参与流控**：没有接收窗口、没有 `WINDOW_UPDATE`、没有信用回补，也不靠保活
//!   `PULSE` 维持（计时循环不为 telegraph 建任何时钟）。
//! - **接收整帧丢弃**：接收环剩余空间装不下**整帧**（帧头 + 载荷）时丢弃该条
//!   （不截断、不重传），并计入 metrics；应用会继续等**下一条**，不会拿到半条。
//!
//! # 两侧的「报文边界」记在哪
//!
//! 环是**字节**缓冲，本身不知道「哪几个字节是一条报文」。因此两个方向各有一份
//! **条目队列**（内联在身份节点 `TgRec_` 里，零堆分配）；消费侧按 FIFO 取得条目，
//! 再从环里精确取走。两个方向存的**内容**不同：
//!
//! - **发送方向**：提交时把 **`(目的 dock, 载荷长度)`** 一起入队，环里只放**载荷
//!   字节**。目的地址之所以连长度一起记：它是逐次发送的实参，同一个端点的不同报文
//!   可以发往不同 dock，因此它必须与长度按同一个 FIFO 顺序配好（见 `TgRec_::out_`
//!   的文档）。
//! - **接收方向**：解复用循环把**整帧原始字节**（帧头 + 载荷）写进环，队列里记的
//!   是**帧总长**。远端地址与载荷由接收侧从帧头里**自行拆出**（见
//!   [`super::datagram_`] 与 [`super::recver_`]），因此队列槽位不承载地址。
//!
//! # 身份的寿命由两个半边共同决定
//!
//! telegraph 的 `local_dock` 是**一个**身份，却由 tx / rx 两个对象使用。任一半边先被
//! 丢弃都不该释放身份（另一半还在用），因此两半各持一份 [`TelegraphCtxHolder_`]：其中
//! 一份发现自己已经是最后一份强引用时才投递
//! [`SessionEvent_::ReleaseTelegraph`](crate::connection::signal_::SessionEvent_)。

use core::{
    alloc::AllocatorClone,
    mem::MaybeUninit,
};

use abs_buff::gen_may_cancel_future;
use abs_cancel::TrCancellationToken;
use abs_mm::res_man::TrUnique;
use abs_smux::{
    chan::{RingBuffAlloc, TrPrepareRing},
    telegraph::{TrTelegraph, TrTelegraphBinding},
};
use buffex::x_deps::{abs_buff, abs_cancel};
use mm_ptr::x_deps::abs_mm;

use crate::{
    connection::{
        BindingError, Dock, DockBinding, MuxConnection, MuxError, TrConnCfg,
        dock_binding::map_reserve_err_,
        owner_::TgOwner_,
        ring_::{BufferedRx, BufferedTx, RingBuildErr, new_buffered_channel},
        signal_::{ReadEvent_, SessionEvent_, TrEventSender_, WriteEvent_},
    },

};
use super::{
    ctx_::TelegraphCtxHolder_,
    recver_::Receiver,
    sender_::Sender,
};

/// 数据报端点的错误类型（`TrDatagramSender::send_async` 与
/// `TrDatagramRecver::recv_async` 共用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TelegraphError {
    /// 待发报文的**长度队列**已满（容量 `K_TG_LEN_QUEUE` 条）。
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

impl<C> TrTelegraphBinding<C> for DockBinding<C>
where
    C: TrConnCfg,
{
    type Telegraph = crate::connection::Telegraph<C>;

    type OpenTelegraphAsync<'f, B, P> = MuxOpenTelegraphAsync<'f, 'f, C>
    where
        Self: 'f,
        B: 'static + TrUnique<Item = [MaybeUninit<C::Data>], Alloc: AllocatorClone> + Send + Sync,
        P: TrPrepareRing<B, C::Data>;

    fn open_telegraph_async<'f, B, P>(
        &'f mut self,
        prepare: P,
    ) -> Self::OpenTelegraphAsync<'f, B, P>
    where
        B: 'static + TrUnique<Item = [MaybeUninit<C::Data>], Alloc: AllocatorClone> + Send + Sync,
        P: TrPrepareRing<B, C::Data>,
    {
        // **先同步建环**：`P` 与 `B` 因此都不进 future 的类型（与 channel 的
        // `accept_async` 同一形状）。建环失败在这里就定局，step 只处理结果。
        let rings = crate::connection::telegraph::build_telegraph_rings_(prepare.prepare());
        MuxOpenTelegraphAsync::new(self, rings)
    }
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
    identity_: TelegraphCtxHolder_<C>,

    /// 应用侧发送环生产端；`split` 时交出。
    tx_w_: Option<BufferedTx>,

    /// 应用侧接收环消费端；`split` 时交出。
    rx_r_: Option<BufferedRx>,

    /// 身份守卫的第二份：`split` 时交给接收半边。
    ///
    /// 工厂期就持两份而不是 `split` 时才克隆：这样「开放后直接丢弃、从不 split」也走
    /// 与「两个半边都被丢弃」完全相同的释放路径（两份守卫一起放下 → 最后一份释放身份）。
    rx_identity_: TelegraphCtxHolder_<C>,
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
        let guard = TelegraphCtxHolder_::new_(rec, conn.clone(), local_dock);
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

    type Sender = Sender<C>;
    type Recver = Receiver<C>;

    fn local_dock(&self) -> C::Dock {
        self.local_dock_
    }

    fn split(mut self) -> (Self::Sender, Self::Recver) {
        // `Option::take` 而非按字段 move：本类型实现了 `Drop`（未 split 就丢弃时要释放
        // 身份），因此不能把字段整体移出。
        let tx_w = self.tx_w_.take().expect("端点工厂只能 split 一次");
        let rx_r = self.rx_r_.take().expect("端点工厂只能 split 一次");
        let rx_guard = self.rx_identity_.clone();
        let tx_guard = self.identity_.clone();
        let conn = self.conn_.clone();
        let local = self.local_dock_;
        (
            Sender::new_(tx_w, conn.clone(), local, tx_guard),
            Receiver::new_(rx_r, conn, local, rx_guard),
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
    /// 身份则由 `TelegraphCtxHolder_` 的最后一份负责释放。
    fn drop(&mut self) {}
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


/// [`TrDockBinding::open_telegraph_async`] 的 step 函数。
///
/// 与 channel 的最终裁决同形：**ring 内存由调用方当场交出**（`prepare`），本函数把两块
/// 内存各建一条环，然后
///
/// 1. 登记 telegraph 身份（**独占**该 `local_dock`）；
/// 2. 把**会话侧**两个半部经事件通道交给对应的中心循环
///    （发送环读端 → 复用循环，接收环写端 → 解复用循环）；
/// 3. 把**应用侧**两个半部与身份句柄包成端点工厂交给调用方。
///
/// 数据报没有建流握手，因此这里**不发任何帧**：身份与环一就位即可收发。
///
/// # 失败路径
///
/// - 建环失败（容量非法 / 分配失败）→ [`BindingError::RingRejected`]，身份**尚未**登记，
///   无需撤销；
/// - 身份被占（`local_dock` 已作 telegraph / listener / channel）→
///   [`BindingError::DockInUse`]（由 `reserve_telegraph_` 判定）；
/// - 事件投递失败（连接正在收尾）→ [`BindingError::Closed`]，并投一条
///   `ReleaseTelegraph` 撤销刚登记的身份。
#[gen_may_cancel_future(MuxOpenTelegraph, pub, new(pub(crate)))]
async fn mux_open_telegraph_async_<'f, C, K>(
    binding: &'f mut DockBinding<C>,
    rings: Result<
        crate::connection::telegraph::TelegraphRings_,
        crate::connection::TelegraphError,
    >,
    cancel: K,
) -> Result<crate::connection::Telegraph<C>, BindingError>
where
    C: TrConnCfg + 'f,

    K: TrCancellationToken,
{
    let conn = binding.conn_();
    let local = binding.local_dock_();

    // 1. 纯本地：建两条环（结果在 `open_telegraph_async` 里已经算出）。
    let ((tx_w, tx_r), (rx_w, rx_r)) = rings.map_err(map_telegraph_err_)?;

    // 2. 先落实积压的释放消息，再认领身份（与 `listen_async` 同一纪律）。
    conn.core_()
        .drain_session_events_(cancel.child_token())
        .await
        .map_err(map_reserve_err_)?;
    let rec = conn
        .core_()
        .reserve_telegraph_(local, cancel.child_token())
        .await
        .map_err(map_reserve_err_)?;

    // 3. 把会话侧两个半部交给对应的中心循环。失败（循环已不在 = 连接正在收尾）时必须
    //    撤销身份：否则该 `local_dock` 会被一个永远不能收发、也没有使用者的身份占住。
    let attached = conn
        .core_()
        .w_events_()
        .try_send_event_(WriteEvent_::TgAttach {
            local_dock: local,
            owner: rec.clone(),
            reader_: tx_r,
        })
        && conn
            .core_()
            .r_events_()
            .try_send_event_(ReadEvent_::TgAttach {
                local_dock: local,
                owner: rec.clone(),
                writer_: rx_w,
            });
    if !attached && !cfg!(test) {
        let _ = conn
            .core_()
            .reg_()
            .post_session_event_(SessionEvent_::ReleaseTelegraph { local_dock: local });
        return Result::Err(BindingError::Closed);
    }

    // 4. 应用侧两个半部 + 身份句柄 → 端点工厂。
    Result::Ok(Telegraph::new_(conn, local, rec, tx_w, rx_r))
}

/// 把 telegraph 的建环错误映射为 binding 面的错误。
fn map_telegraph_err_(err: crate::connection::TelegraphError) -> BindingError {
    match err {
        crate::connection::TelegraphError::RingRejected => BindingError::RingRejected,
        _ => BindingError::Closed,
    }
}
