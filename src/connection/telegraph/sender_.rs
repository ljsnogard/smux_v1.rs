use abs_buff::{
    Demand, TrBuffTryWrite, TrBuffWrite,
    gen_may_cancel_future,
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::TrCancellationToken;
use abs_smux::telegraph::TrDatagramSender;
use anylr::SomeOf;
use buffex::{
    ring::ProducerError,
    x_deps::abs_buff,
};

use crate::{
    connection::{
        Dock, MuxConnection, TrConnCfg,
        ring_::BufferedTx,
        signal_::{TrEventSender_, WriteEvent_},
    },
};
use super::{
    ctx_::TelegraphCtxHolder_,
    endpoint_::{DemandErr, TelegraphError},
};


/// 数据报的**发送半边**。
///
/// 实现 [`TrBuffWrite`]（应用把载荷写进本地发送环），并用
/// [`send_async`](abs_smux::telegraph::TrDatagramSender::send_async) 提交一条报文。真正的上网由复用循环完成
/// ——它持有一条**发送环的读端**（经 `WriteEvent_::TgAttach` 交接）。
///
/// # Drop
///
/// 丢弃发送半边只收尾**发送方向**：关掉发送环的生产端（唤醒可能正 park 在该环上的复用
/// 循环）并通知循环摘掉本地表项、丢弃队列里尚未送出的项。接收半边是否还在与本对象无关
/// ——身份由两个半边的守卫共同持有。
pub struct Sender<C>
where
    C: TrConnCfg,
{
    /// 发送环的**生产端**（应用写、复用循环读）。
    ring_tx_: BufferedTx,

    /// 连接智能指针（提交时通知复用循环）。
    mux_conn_: MuxConnection<C>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 身份守卫（与接收半边各持一份）。
    ctx_holder_: TelegraphCtxHolder_<C>,
}

impl<C> Sender<C>
where
    C: TrConnCfg,
{
    /// 由发送环生产端、连接、dock 与身份守卫构造。
    pub(super) fn new_(
        ring: BufferedTx,
        conn: MuxConnection<C>,
        local_dock: Dock,
        ctx_holder: TelegraphCtxHolder_<C>,
    ) -> Self {
        Sender {
            ring_tx_: ring,
            mux_conn_: conn,
            local_dock_: local_dock,
            ctx_holder_: ctx_holder,
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

impl<C> Drop for Sender<C>
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
        self.ring_tx_.close();
        let _ = self
            .mux_conn_
            .core_()
            .w_events_()
            .try_send_event_(WriteEvent_::TgTxClosed {
                local_dock: self.local_dock_,
            });
    }
}

impl<C> TrBuffTryWrite<u8> for Sender<C>
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
        self.ring_tx_.try_write(demand)
    }
}

/// 应用侧把载荷写进发送环（[`TrBuffWrite`]）。
///
/// 写入本身**不**提交报文：提交由 [`send_async`](TrDatagramSender::send_async) 完成。
/// 因此这里不需要通知复用循环——环里有未提交字节时，循环即使被叫醒也不会成帧
/// （报文长度只由提交动作写进条目队列）。
impl<C> TrBuffWrite<u8> for Sender<C>
where
    C: TrConnCfg,
{
    type WriteAsync<'f>
        = <BufferedTx as TrBuffWrite<u8>>::WriteAsync<'f>
    where
        Self: 'f;

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        self.ring_tx_.write_async(demand)
    }
}

impl<C> TrDatagramSender<C> for Sender<C>
where
    C: TrConnCfg,
{
    type Err = TelegraphError;
    type SendAsync<'f> = MuxSendTgAsync<'f, 'f, C> where Self: 'f;

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

/// [`TrDatagramSender::send_async`](abs_smux::telegraph::TrDatagramSender::send_async) 的 step 函数。
///
/// 它**同步完成**（没有 park 点）：把「环里已写、尚未提交的字节数」记进身份节点的发送
/// 长度队列，并通知复用循环。真正的分块搬运与成帧都在循环里做。
#[gen_may_cancel_future(MuxSendTg, pub, new(pub(crate)))]
async fn mux_send_tg_async_<'f, C, K>(
    tx: &'f mut Sender<C>,
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
        .ring_tx_
        .ring_state()
        .data_size()
        .saturating_sub(tx.ctx_holder_.out_().bytes_());

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
    tx.ctx_holder_
        .out_()
        .try_push_(remote_dock, pending)
        .map_err(|_| TelegraphError::OutboxFull)?;

    // 唤醒复用循环：它可能正 park 在事件通道上（也可能正在排空主循环里别的端点）。
    // 投递失败表示循环已经不在了（连接正在收尾），此时本次提交不会被搬运——但**不必**
    // 在这里报错：接收侧的 `recv_async` / 下一次提交会经环的关闭态看到连接已收尾。
    let _ = tx
        .mux_conn_
        .core_()
        .w_events_()
        .try_send_event_(WriteEvent_::TgReady {
            local_dock: tx.local_dock_,
        });
    Result::Ok(pending)
}
