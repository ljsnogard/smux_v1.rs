//! 子流的两个应用侧半部：[`ChannelTx`] / [`ChannelRx`]。
//!
//! 对应 `abs_smux` 的 `TrChannelTx` / `TrChannelRx` / `TrChannelHalf`；数据面则
//! 直接实现 `abs_buff` 的 `TrBuffTryWrite` / `TrBuffWrite` / `TrBuffRead`，发送半边
//! 另外实现 `TrBuffFlush`（冲 / 封口的可等待形态）。收尾的完成点与唤醒来源见
//! `crate::connection` 模块文档 §7 与 `owner_::ChannelState_`。
//!
//! # 关闭态：直接读环的两个端
//!
//! `TrChannelHalf::is_tx_closed` / `is_rx_closed` 不额外维护标志位，而是读
//! `buffex::ring` 环本身的**两端关闭态**（半部经 `ring_state()` 取
//! `is_producer_closed` / `is_consumer_closed`）：
//!
//! - 发送环的应用端是 [`ChannelTx`]（生产端），另一端由中心循环的写路径持有；
//!   接收环的会话端是生产端，应用端是 [`ChannelRx`]（消费端）；
//! - `ChannelTx::is_tx_closed()` 因此是「应用端已关闭发送环的生产端」——丢弃
//!   [`ChannelTx`] 即置位；`ChannelTx::is_rx_closed()` 是「中心循环已关闭发送环的
//!   消费端」，即连接已经拆掉这条子流；
//! - 接收方向对称：`ChannelRx::is_tx_closed()` 表示会话（生产端）已关闭接收环，
//!   即对端不再发送（EOF）；`ChannelRx::is_rx_closed()` 表示应用端（消费端）已关闭。
//!
//! 好处是**关闭态不需要再引入一份共享状态**：环本身就是两个端共享的那点状态。
//!
//! # 为什么没有内层包装类型
//!
//! 早先的实现在 [`ChannelTx`] 之内还套了一层 `TxRing_` / `RxRing_`，并把
//! `Drop` 与 `TrChannelHalf` 落在内层上。那一层存在的唯一理由是 E0366：Rust
//! 不允许为泛型结构体的**某个具体实例化**单独实现 `Drop`，而当时 `ChannelTx<H>`
//! 对「环半部类型」泛型，`Drop` 只能落到 `ChannelTx<TxRing_<..>>` 上。
//!
//! 现在两个半部直接以 `<C>` 参数化（环半部类型由 `C` 决定，是**唯一**
//! 的），E0366 不再适用，因此内层包装被删除：两个半部**各自就是那个具名、可写进
//! 结构体字段的具体类型**（旧模型下内层包装未导出，下游根本写不出已建立 channel
//! 的类型，见 `dev-notes` §16.2 F5）。

use core::{
    future::poll_fn,
    task::{Context, Poll},
};

use abs_buff::{
    Demand,
    TrBuffFlush, TrBuffTryFlush,
    TrBuffRead, TrBuffTryRead,
    TrBuffWrite, TrBuffTryWrite,
    buffer::{TrConsumerState, TrProducerState},
};
use abs_cancel::TrCancellationToken;
use abs_smux::chan::{TrChannelHalf, TrChannelRx, TrChannelTx};
use anylr::SomeOf;
use buffex::ring::{ConsumerError, ProducerError};
use gen_mcf2::gen_may_cancel_future;

use crate::{
    connection::{
        BufferedRx, BufferedTx, Dock, MuxConnection, MuxError, TrConnCfg,
        // config_::TrMuxAllocConfig,
        owner_::ChannelOwner_,
        session_::race_cancel_,
        signal_::{
            HINT_W_TX_CLOSED,
        },
    },
    flow_ctrl::Credit,
    x_deps::{abs_buff, abs_cancel, abs_smux, anylr, buffex, gen_mcf2},
};

/// 子流发送半边（应用侧**生产端**）。
///
/// 实现 `TrBuffTryWrite<u8>`，因此应用侧写数据是**非阻塞**的：环满即返回
/// `WriteErrTag::Stuffed`，由应用决定等待还是丢弃。真正把数据推上网络的是
/// 内部写循环。
///
/// 除环的生产端外，它还持有：
///
/// - 该子流的共享状态（用于「已入队」位去重与关闭记账）；
/// - **一份连接智能指针克隆**——既用于通知写循环（`WriteEvent_`），也保证
///   「应用还拿着半部」期间连接不会被回收（最后一个强引用消失才会关闭连接）。
///
/// # 关闭语义（半关闭）
///
/// 本类型**按值独占**生产端半部，因此**丢弃它即关闭发送方向**（`buffex` 在
/// 半部 drop 时置位本端关闭标志）；其 `Drop` 同时通知写循环「排空后发
/// `CLOSE(FIN)`」。两个方向互不影响，关闭态直接取自环本身（见模块文档
/// 「关闭态」一节）。
///
/// 也可以**显式**收尾（`abs_smux` 的契约）：
///
/// - [`TrBuffTryFlush::try_flush`]：提醒写循环去取数据（同步，不等结果）；
/// - [`TrBuffTryFlush::close`]：立刻封口（就是 `drop` 做的那件事，幂等）；
/// - [`TrBuffFlush::flush_async`]：等到发送环**排空**（已提交字节全部交给传输）；
/// - [`TrBuffFlush::close_async`]：封口并等到 `CLOSE(FIN)` **上线**。
///
/// `drop` 与显式 `close` 复用**同一套**收尾（[`ChannelTx::seal_tx_`]），因此两者
/// 语义一致，重复调用也不会重复投递拆流事件。
pub struct ChannelTx<C>
where
    C: TrConnCfg<Data = u8>,
{
    /// `buffex` 生产端半部（[`BufferedTx`] 的实例）。
    ring_: BufferedTx,

    /// 该子流的共享状态（「已入队」去重与关闭记账）。
    owner_: ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,

    /// 连接智能指针：通知写循环 + 保活。
    conn_: MuxConnection<C>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,

    /// **发送环的临界水位**：环内积压超过它（即剩余空间不足
    /// `容量 × 1/N`）就算进入「临界区」。
    ///
    /// 与接收侧的临界区同源——都取自
    /// [`TrFlowCtrlPolicy::critical_denominator`]，只是这里量的是「待发积压」而
    /// 不是「剩余接收容量」。两侧对称的含义是一致的：**剩余不足 1/N 就每变必报**。
    backlog_: Credit,

    /// 最近一次通知写循环时，发送环是否已经处于临界区。
    ///
    /// 通知（`TxReady`）只在**临界区里**频繁发：越接近「发不出去」，越要保证写循环
    /// 拿得到取数机会；积压不严重时不必每次写入都喊一遍（那会把事件通道灌满，反过来
    /// 挤占写循环的排空段——本仓库真实 socket 用例撞到过）。
    backlogged_: bool,
}

impl<C> ChannelTx<C>
where
    C: TrConnCfg<Data = u8>,
{
    /// 由环生产端、共享状态、连接与 dock 对构造。
    ///
    /// `backlog` 是**发送环的临界水位**（见 [`ChannelTx`] 的同名字段文档）。
    ///
    /// 只供连接内部（建流路径）与单元测试使用：对外部使用者而言，这两个半边只应由
    /// `abs_smux` 的 trait 产出。
    pub(crate) fn new_(
        ring: BufferedTx,
        owner: ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,
        conn: MuxConnection<C>,
        local_dock: Dock,
        remote_dock: Dock,
        backlog: Credit,
    ) -> Self {
        ChannelTx {
            ring_: ring,
            owner_: owner,
            conn_: conn,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
            backlog_: backlog,
            backlogged_: false}
    }

    /// 通知写循环「这条子流的发送环可能有数据」。
    ///
    /// # 什么时候必须通知
    ///
    /// 通知是**提醒**而不是数据：写循环每轮本来就全表扫描，一次提示只让它「回顶部重
    /// 扫」。积压不严重时每次写入都喊一遍没有信息量，只会让合并提示位反复置位；因此
    /// 规则与接收侧对称：**待发积压超过 `容量 × 3/4`（即剩余不足 `容量 × 1/4`）时任何
    /// 变化都通知**；积压不严重时只在「刚从积压状态回落」的边沿补一次。
    ///
    /// 另有一条**不可省**的补充：**进入本函数时环是空的**（本次写入要让环由空变非空）
    /// 也必须通知。少了它有一个确定性的丢唤醒：一条安静的连接上，某条子流第一次写入
    /// 若**低于临界水位**、也不构成边沿，则合并提示不会被置位；写循环此时可能正停在
    /// park 上，环里的数据就此无人搬运（`tests/alloc_count.rs` 的基线用例把这条路径
    /// 稳定复现出来：单条安静子流写 512 B < 临界水位 1024 B 直接挂死）。
    ///
    /// R13 起「通知」就是一次合并提示位 [`HINT_W_TX_READY`]（不再是事件通道）：「已入队」
    /// 去重位（`mark_tx_queued_`）保证同一条子流至多一次待处理提示，写循环取走掩码后按
    /// 全表扫描、排空时清去重位。因此「空环也通知」不会把任何队列灌满。
    ///
    /// [`HINT_W_TX_READY`]: crate::connection::signal_::HINT_W_TX_READY
    fn notify_tx_ready_(&mut self) {
        let queued = self.ring_.ring_state().data_size() as u64;
        let congested = queued > self.backlog_ as u64;
        // 边沿也要通知一次（进临界区 / 出临界区），避免写循环漏掉状态切换。
        let edge = congested != self.backlogged_;
        // 空环也通知：见上方文档（丢唤醒）。
        if congested || edge || queued == 0u64 {
            #[cfg(feature = "test-loop-probe")]
            crate::connection::loop_probe_::event_tx_ready_attempt_(
                queued == 0u64,
                congested,
                edge,
            );
            self.backlogged_ = congested;
            // 去重位的第一置位者 raise 合并提示：写循环据此回顶部做**全表**排空。
            if self.owner_.mark_tx_queued_() {
                self.conn_
                    .core_()
                    .reg_()
                    .raise_write_hint_(crate::connection::signal_::HINT_W_TX_READY);
                #[cfg(feature = "test-loop-probe")]
                crate::connection::loop_probe_::event_tx_ready_result_(true);
            } else {
                #[cfg(feature = "test-loop-probe")]
                crate::connection::loop_probe_::event_tx_ready_dedup_();
            }
        }
    }

    /// 把 `bytes` 全部写进发送环。
    ///
    /// 段级循环的省事封装：借一段、能写多少写多少、段 drop 时按写入量提交，直到写完。
    /// 每次只按「至少 1 字节」索要，因此**与子流环容量无关**——写多大的字节串都行，
    /// 环小就多借几次。
    ///
    /// # 取消
    ///
    /// 这是一个普通 `async fn`：**丢弃它就等于取消**。环的 park future 在 drop 时撤回
    /// 登记，因此不会留下悬挂的等待者。
    ///
    /// # Errors
    ///
    /// 发送方向已关闭（[_Closing_](ProducerError::Closing)）或底层写失败时返回错误；
    /// 已经写出去的部分**不会回滚**。
    ///
    /// # Examples
    ///
    /// ```ignore
    /// tx.write_all(b"hello").await?;
    /// drop(tx);   // 半关闭：对端读到 EOF
    /// ```
    pub async fn write_all(&mut self, bytes: &[u8]) -> Result<(), ProducerError<usize>> {
        let mut offset = 0usize;
        while offset < bytes.len() {
            // 下限只用 1：环容量再小也满足（见 `wire_io_` 同款说明）。
            let demand = Demand::at_least(1usize);
            let mut outcome = TrBuffWrite::write_async(self, &demand).await;
            let put = match outcome.as_mut().pick_left() {
                Option::Some(segm) => {
                    segm.as_segm_mut().clone_items_from_buff(&bytes[offset..])
                }
                Option::None => {
                    return Result::Err(match outcome.pick_right() {
                        Option::Some(err) => err,
                        // 既没有段也没有错误：环已不可用。
                        Option::None => ProducerError::Closing,
                    });
                }
            };
            // 借出的段至少有一格空闲，而 `bytes[offset..]` 非空，因此 `put >= 1`。
            debug_assert!(put > 0usize, "借出的空段不可能写进 0 字节");
            if put == 0usize {
                return Result::Err(ProducerError::Closing);
            }
            offset += put;
        }
        Result::Ok(())
    }

    /// 本条子流**为什么停下来**（若它不是被对端正常关闭的）。
    ///
    /// # 什么时候会有值
    ///
    /// 两档，且**返回的就是最终结论**（只增不减、优先级单调）：
    ///
    /// - [`MuxError::ConnFailed`]：**连接级失败**牵连。连接判定不可恢复之后，所有在册
    ///   子流一并终结；这一档**压过**此前可能已经记下的子流级原因（连接正式判死之前
    ///   往往已经发生过大面积子流级错误，应用需要的结论是「连接没了」）。载荷只带
    ///   类别，具体原因（首个 [`MuxError`]）留在连接级。
    /// - [`MuxError::IdleTimeout`]：`max_channel_timeout` 内既无数据、也无任何方向的
    ///   保活往来（**含建流未裁决**那一档），由计时循环判定。
    ///
    /// 两种情形下，本半部与对侧半部的环都进入关闭态（`is_tx_closed` / `is_rx_closed`
    /// 为真，读写返回 `Closing`）——**环那一层与「对端正常半关闭」不可区分**，因此
    /// 应用被唤醒后的固定读取顺序是：
    ///
    /// ```text
    /// 环错误 Closing（现象）  →  abort_reason()（本子流为什么停）  →  连接级查询（细节）
    /// ```
    ///
    /// # 唤醒从哪来
    ///
    /// 连接级失败时，取消令牌会让两个内侧循环退出；它们退出前**显式 `close()`** 手里
    /// 的环半部（`buffex` 的环半部 drop 不置关闭位、也不唤醒对端，所以这一步不可省），
    /// 应用的读写等待者因此被唤醒并读到本方法的原因。
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // 应用侧被唤醒之后，区分「对端关的」「空闲超时拆的」「连接整体挂了」。
    /// match tx.abort_reason() {
    ///     Some(MuxError::IdleTimeout) => { /* 保活判定拆流：可以按自己的策略重连 */ }
    ///     Some(MuxError::ConnFailed(kind)) => { /* 连接级失败：整条连接不可用 */ }
    ///     Some(_) | None => { /* 对端正常半关闭，或其它 */ }
    /// }
    /// ```
    pub fn abort_reason(&self) -> Option<MuxError> {
        self.owner_.abort_reason_()
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 收尾：封口与等待判据
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// **封口发送方向**：关掉发送环的生产端，并（仅首次）通知写循环「排空后发
    /// `CLOSE(FIN)`」。
    ///
    /// # 为什么 `close` 与 `drop` 共用它
    ///
    /// 两者在协议上是**同一件事**：应用不再写。差别只在调用方是否还在等结果——
    /// [`close_async`](TrBuffFlush::close_async) 等到 `FIN` 上线，`drop` 不等。把收尾
    /// 收敛到一个函数，是为了让「丢弃发送半边」与「显式关闭」不可能漂移出两套语义。
    ///
    /// # 幂等
    ///
    /// [`BufferedTx`] 的 `close` 本身幂等；协议侧那一次「排空 + 发 `FIN`」的**投递**
    /// 用 `ChannelState_::set_app_tx_closed_` 的首次置位去重。少了这层去重，第二条
    /// 收尾提示会让写循环对已经收尾 / 摘表的条目再做一轮 `FIN` 探测。
    fn seal_tx_(&mut self) {
        self.ring_.close();
        if self.owner_.set_app_tx_closed_() {
            // 收尾事实已经在状态字上；这里只唤醒写循环，不再逐条投事件。
            self.conn_
                .core_()
                .reg_()
                .raise_write_hint_(HINT_W_TX_CLOSED);
        }
    }

    /// 发送环是否已经**排空**（应用提交的字节全部被写循环取走）。
    ///
    /// 判据取环内积压为 0，而**不是**「`FIN` 已发」：`flush_async` 的语义是「已提交
    /// 的字节都交给了下游」，它比封口更早成立。
    fn tx_drained_(&self) -> bool {
        self.ring_.ring_state().data_size() == 0usize
    }

    /// 发送方向是否已经**不可能再前进**：对端宣告不再接收、本端被拆流 / 空闲超时 /
    /// 连接级失败，或发送环的消费端（写循环）已经关闭。
    ///
    /// 它是两个等待判据的**共同终局**：走到这里，`flush_async` 与 `close_async` 都
    /// 必须返回，而不是继续 park 在一条不会有进展的环上。
    fn tx_terminated_(&self) -> bool {
        self.owner_.is_peer_reset_()
            || self.owner_.abort_reason_().is_some()
            || self.ring_.ring_state().is_consumer_closed()
    }

    /// `flush_async` 的就绪判据；`None` = 还要等。
    ///
    /// - 环已排空 ⇒ `Ok`：已提交的字节都交给了下游；
    /// - 环不空、但发送方向已终止 ⇒ `Err(Closing)`：这些字节再也不会被送出去
    ///   （对端 `RESET` 授权放弃已提交字节，拆流 / 连接级失败同理）。
    fn tx_flush_ready_(&self) -> Option<Result<(), ProducerError<usize>>> {
        if self.tx_drained_() {
            return Option::Some(Result::Ok(()));
        }
        if self.tx_terminated_() {
            return Option::Some(Result::Err(ProducerError::Closing));
        }
        Option::None
    }

    /// `close_async` 的就绪判据；`None` = 还要等。
    ///
    /// - `FIN` 已上线 ⇒ `Ok`：封口已送达对端（它蕴含发送环当时已排空）；
    /// - 对端宣告不再接收 ⇒ `Ok`：无论这条 `FIN` 发不发，对端都已不再收数据，封口的
    ///   目的已经达成；
    /// - 发送方向已终止而 `FIN` 没能发出 ⇒ `Err(Closing)`。
    fn tx_close_ready_(&self) -> Option<Result<(), ProducerError<usize>>> {
        if self.owner_.is_local_fin_sent_() || self.owner_.is_peer_reset_() {
            return Option::Some(Result::Ok(()));
        }
        if self.owner_.abort_reason_().is_some() || self.ring_.ring_state().is_consumer_closed() {
            return Option::Some(Result::Err(ProducerError::Closing));
        }
        Option::None
    }
}

impl<C> Drop for ChannelTx<C>
where
    C: TrConnCfg,
{
    /// 丢弃发送半边 = 半关闭：与显式 `close` / `close_async` **同一套**收尾
    /// （[`ChannelTx::seal_tx_`]）——既关发送环的生产端，也通知写循环排空后发 `FIN`。
    ///
    /// 应用侧的收尾只在**原子状态字**上记一次（无锁、同步）：循环侧的收尾判据据此
    /// 起算，不依赖事件何时被处理，也不依赖 `drop` 与显式关闭谁先到。
    fn drop(&mut self) {
        self.seal_tx_();
    }
}

impl<C> TrBuffTryWrite<u8> for ChannelTx<C>
where
    C: TrConnCfg,
{
    type SegmMut<'f> = <BufferedTx as TrBuffTryWrite<u8>>::SegmMut<'f>
    where
        Self: 'f;

    type Err = <BufferedTx as TrBuffTryWrite<u8>>::Err;

    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        self.notify_tx_ready_();
        self.ring_.try_write(demand)
    }
}

impl<C> TrBuffWrite<u8> for ChannelTx<C>
where
    C: TrConnCfg,
{
    type WriteAsync<'f>
        = <BufferedTx as TrBuffWrite<u8>>::WriteAsync<'f>
    where
        Self: 'f;

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        self.notify_tx_ready_();
        self.ring_.write_async(demand)
    }
}

impl<C> TrChannelHalf<C> for ChannelTx<C>
where
    C: TrConnCfg,
{
    fn local_dock(&self) -> C::Dock {
        self.local_dock_
    }

    fn remote_dock(&self) -> C::Dock {
        self.remote_dock_
    }

    /// 发送方向是否已关闭：本端（应用）已不再发送，或会话已停止排空本环。
    fn is_tx_closed(&self) -> bool {
        self.ring_.ring_state().is_producer_closed()
    }

    /// 接收方向是否已关闭：环的消费端（由写循环持有）已关闭，即整条子流已被
    /// 连接拆掉。
    fn is_rx_closed(&self) -> bool {
        self.ring_.ring_state().is_consumer_closed()
    }
}

impl<C> TrProducerState for ChannelTx<C>
where
    C: TrConnCfg,
{
    #[inline]
    fn producer_state(&self) -> Option<(usize, bool)> {
        self.ring_.producer_state()
    }
}

impl<C> TrBuffTryFlush<u8> for ChannelTx<C>
where
    C: TrConnCfg,
{
    /// **尽力推一轮**：`ChannelTx` 的下游是连接的写循环，而这是一个同步入口——
    /// 能做的只有把「发送环里可能有数据」提醒给写循环（`TxReady`）。真正的排空用
    /// [`flush_async`](TrBuffFlush::flush_async) 等。
    ///
    /// 返回恒为 `Ok`：这里没有任何会发生 IO 失败的动作，环本身也不会失败（与
    /// `buffex::ring::RingWriter` 对纯内存环的实现同款）。
    #[inline]
    fn try_flush(&mut self) -> Result<(), Self::Err> {
        self.notify_tx_ready_();
        Result::Ok(())
    }

    /// **同步封口**：等价于 [`ChannelTx::seal_tx_`]（关生产端 + 首次置位并提示写循环）。
    /// 幂等，因此调用者不必区分「已经 `drop` 过」还是「第一次 `close`」。
    #[inline]
    fn close(&mut self) -> Result<(), Self::Err> {
        self.seal_tx_();
        Result::Ok(())
    }
}

impl<C> TrBuffFlush<u8> for ChannelTx<C>
where
    C: TrConnCfg,
{
    type FlushAsync<'f> = ChannelTxFlushAsync<'f, 'f, C> where Self: 'f;

    /// 等**发送环排空**（应用提交的字节全部被写循环取走）：
    /// [`channel_tx_flush_async_`]。
    #[inline]
    fn flush_async(&mut self) -> Self::FlushAsync<'_> {
        ChannelTxFlushAsync::new(self)
    }

    type CloseAsync<'f>
        = ChannelTxCloseAsync<'f, 'f, C>
    where
        Self: 'f;

    /// 封口并等 **`CLOSE(FIN)` 上线**：[`channel_tx_close_async_`]。
    #[inline]
    fn close_async(&mut self) -> Self::CloseAsync<'_> {
        ChannelTxCloseAsync::new(self)
    }
}

impl<C> TrChannelTx<C> for ChannelTx<C>
where
    C: TrConnCfg,
{}

/// 子流接收半边（应用侧**消费端**）。
///
/// 实现 `TrBuffTryRead<u8>`；环空即返回 `ReadErrTag::Drained`。数据由
/// 内部读循环从网络解复用后写入。
///
/// 每次应用发起读之前，先**通知解复用循环**「水位可能变了」——注意是**裸通知**，
/// 不带消费增量：增量由持有接收环**写端**的解复用循环按「自己记账的累计已收 − 环内
/// 实际积压」重算，原因见 `notify_consumed_` 的文档。
///
/// # 关闭语义（半关闭）
///
/// 丢弃本类型即关闭接收方向；写端关闭后先把残留数据读走，再 `try_read` 才会
/// 报 `Closing`（EOF 语义，见模块文档「关闭态」一节）。
///
/// 也可以**显式**关闭：[`TrChannelRx::close_async`] 关闭接收方向并等到
/// `CLOSE(RESET)` **上线**；它与 `drop` 复用同一套收尾（[`ChannelRx::seal_rx_`]），
/// 因此返回之后本端不再接收，而对端在收到那条 `RESET` 之前发出的帧被静默丢弃。
pub struct ChannelRx<C>
where
    C: TrConnCfg,
{
    /// `buffex` 消费端半部（[`BufferedRx`] 的实例）。
    ring_: BufferedRx,

    /// 该子流的共享状态（「已消费」去重位在锁外，见
    /// [`ChannelOwner_::mark_rx_consumed_`]）。
    owner_: ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,

    /// 连接智能指针：通知写循环 + 保活。
    conn_: MuxConnection<C>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,
}

impl<C> ChannelRx<C>
where
    C: TrConnCfg,
{
    /// 由环消费端、共享状态、连接与 dock 对构造；可见性同 [`ChannelTx::new_`]。
    pub(crate) fn new_(
        ring: BufferedRx,
        owner: ChannelOwner_<crate::connection::ConnChanOwnerAlloc_<C>>,
        conn: MuxConnection<C>,
        local_dock: Dock,
        remote_dock: Dock,
    ) -> Self {
        ChannelRx {
            ring_: ring,
            owner_: owner,
            conn_: conn,
            local_dock_: local_dock,
            remote_dock_: remote_dock}
    }

    /// 通知**解复用循环**「应用刚取走了数据，接收窗口的账面水位可能变了」。
    ///
    /// # 为什么是裸通知，而不是「本次消费了多少字节」
    ///
    /// 应用侧只能**采样**环的 `data_size` 再求差，而 `data_size` 是**净**水位：只要
    /// 两次采样之间既有读出又有写入，差值就退化成 0，那一次消费被永久漏记。稳态恰好
    /// 是这个形状——对端每拿到一份额度就写回等量字节，正好把应用刚读掉的顶回去。
    /// 漏记累积到一个整窗口之后，接收环已经读空、通告窗口却仍是 0，两端互等
    /// （实测终局与因果链见 `dev-notes/flow-ctrl-20261005-0115.md` §3）。
    ///
    /// 解复用循环持有接收环**写端**，它在自己的任务里读到的「环内积压」与它自己记账的
    /// 「累计已收」是同一时刻的一致快照，二者之差即**精确**的累计已消费量，因此判定
    /// 权归它。
    ///
    /// # 去重
    ///
    /// 与发送侧的「已入队」位同构：每条子流至多一条待处理通知。消费方必须**先清位、
    /// 再读环内积压**（见 [`ChannelOwner_::clear_rx_consumed_`]），这样清位之后发生的
    /// 消费会重新置位并投递，不会丢。
    fn notify_consumed_(&mut self) {
        #[cfg(feature = "test-loop-probe")]
        crate::connection::loop_probe_::event_rx_consumed_attempt_();
        if !self.owner_.mark_rx_consumed_() {
            #[cfg(feature = "test-loop-probe")]
            crate::connection::loop_probe_::event_rx_consumed_dedup_();
            return;
        }
        // 位化：只 raise 合并提示，读循环按全表重算水位（清位顺序保证不丢）。
        self.conn_
            .core_()
            .reg_()
            .raise_read_hint_(crate::connection::signal_::HINT_R_RX_CONSUMED);
        #[cfg(feature = "test-loop-probe")]
        crate::connection::loop_probe_::event_rx_consumed_result_(true);
    }

    /// 从接收环读满 `out`。
    ///
    /// 与 [`ChannelTx::write_all`] 对称：段级循环的省事封装，每次只按「至少 1 字节」
    /// 索要，因此**与子流环容量无关**——要读多大都行，环小就多借几次。
    ///
    /// # 取消
    ///
    /// 同 [`ChannelTx::write_all`]：丢弃 future 即取消。
    ///
    /// # Errors
    ///
    /// 对端已半关闭（[_Closing_](ConsumerError::Closing)，即读到 EOF）或底层读失败时
    /// 返回错误；**已经读到的部分不会回滚**，因此调用方可以按已填充的前缀处理。
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
                    // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同（同尺寸、同对齐、
                    // 无 niche），且 `dst` 是本函数独占的可写切片；
                    // `move_items_to_buff` 只写入其中已初始化的前缀并返回写入长度，
                    // 因此不会读到未初始化内存，也不会越界。
                    let uninit = unsafe {
                        core::slice::from_raw_parts_mut(
                            dst.as_mut_ptr() as *mut core::mem::MaybeUninit<u8>,
                            dst.len(),
                        )
                    };
                    unsafe { child.move_items_to_buff(uninit) }
                }
                Option::None => {
                    return Result::Err(match outcome.pick_right() {
                        Option::Some(err) => err,
                        // 既没有段也没有错误：环已不可用。
                        Option::None => ConsumerError::Closing,
                    });
                }
            };
            // 借出的段至少有 1 个字节可读，因此 `got >= 1`。
            debug_assert!(got > 0usize, "有数据的段不可能搬出 0 字节");
            if got == 0usize {
                return Result::Err(ConsumerError::Closing);
            }
            offset += got;
        }
        Result::Ok(())
    }

    /// 本条子流被**连接内部主动中止**的原因（若发生过）。
    ///
    /// 与 [`ChannelTx::abort_reason`] 同源、同语义：一条子流的两个半部读到**同一个**
    /// 原因（它在共享状态里，不在任一半部里）。见该方法的说明。
    pub fn abort_reason(&self) -> Option<MuxError> {
        self.owner_.abort_reason_()
    }

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 收尾：封口与等待判据
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// **关闭接收方向**：关掉接收环的消费端，并（仅首次）通知写循环发
    /// `CLOSE(RESET)`。
    ///
    /// 与 [`ChannelTx::seal_tx_`] 对称：`drop` 与显式
    /// [`close_async`](TrChannelRx::close_async) 走**同一套**收尾，幂等由环的关闭位与
    /// `ChannelState_::set_app_rx_closed_` 的首次置位共同保证。
    fn seal_rx_(&mut self) {
        self.ring_.close();
        if self.owner_.set_app_rx_closed_() {
            // 位是事实；main 扫描到 `APP_RX_CLOSED` 后组 `CLOSE(RESET)`。
            self.conn_
                .core_()
                .control_()
                .raise_main_hint_(crate::connection::HINT_MAIN_RESET);
        }
    }

    /// [`close_async`](TrChannelRx::close_async) 的就绪判据；`None` = 还要等。
    ///
    /// - 本端 `RESET` 已**上线** ⇒ `Ok`：对端已被明确告知本端不再接收；
    /// - 对端宣告不再发送（`FIN`），或接收环的写端已经关闭 ⇒ `Ok`：接收方向本来就
    ///   到头了，不存在「还要通知对端停止发送」这件事；
    /// - 连接级失败 / 拆流 / 空闲超时 ⇒ `Err(Closing)`：这条 `RESET` 不可能再送达。
    fn rx_close_ready_(&self) -> Option<Result<(), ConsumerError<usize>>> {
        if self.owner_.is_local_reset_flushed_() || self.owner_.is_peer_fin_() {
            return Option::Some(Result::Ok(()));
        }
        if self.owner_.abort_reason_().is_some() {
            return Option::Some(Result::Err(ConsumerError::Closing));
        }
        if self.ring_.ring_state().is_producer_closed() {
            return Option::Some(Result::Ok(()));
        }
        Option::None
    }
}

impl<C> Drop for ChannelRx<C>
where
    C: TrConnCfg,
{
    /// 丢弃接收半边：与显式 [`close_async`](TrChannelRx::close_async) **同一套**收尾
    /// （[`ChannelRx::seal_rx_`]）——既关接收环的消费端，也通知写循环发 `RESET`。
    ///
    /// 应用侧的收尾只在**原子状态字**上记一次（无锁、同步）：解复用循环据此把在途
    /// 数据静默丢弃，而不是写进一条消费端已关闭的环、把连接判成传输错误。
    fn drop(&mut self) {
        self.seal_rx_();
    }
}

impl<C> TrBuffTryRead<u8> for ChannelRx<C>
where
    C: TrConnCfg,
{
    type SegmRef<'f> = <BufferedRx as TrBuffTryRead<u8>>::SegmRef<'f>
    where
        Self: 'f;

    type Err = <BufferedRx as TrBuffTryRead<u8>>::Err;

    fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        self.notify_consumed_();
        self.ring_.try_read(demand)
    }
}

impl<C> TrBuffRead<u8> for ChannelRx<C>
where
    C: TrConnCfg,
{
    type ReadAsync<'f>
        = <BufferedRx as TrBuffRead<u8>>::ReadAsync<'f>
    where
        Self: 'f;

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        self.notify_consumed_();
        self.ring_.read_async(demand)
    }
}

impl<C> TrChannelHalf<C> for ChannelRx<C>
where
    C: TrConnCfg,
{
    fn local_dock(&self) -> C::Dock {
        self.local_dock_
    }

    fn remote_dock(&self) -> C::Dock {
        self.remote_dock_
    }

    /// 发送方向是否已关闭：环的生产端（由读循环持有）已关闭，即对端不再发送
    /// （EOF）或整条子流已被连接拆掉。
    fn is_tx_closed(&self) -> bool {
        self.ring_.ring_state().is_producer_closed()
    }

    /// 接收方向是否已关闭：本端（应用）已不再接收。
    fn is_rx_closed(&self) -> bool {
        self.ring_.ring_state().is_consumer_closed()
    }
}

impl<C> TrConsumerState for ChannelRx<C>
where
    C: TrConnCfg,
{
    fn consumer_state(&self) -> Option<(usize, bool)> {
        self.ring_.consumer_state()
    }
}

impl<C> TrChannelRx<C> for ChannelRx<C>
where
    C: TrConnCfg,
{
    type CloseAsync<'f> = ChannelRxCloseAsync<'f, 'f, C> where Self: 'f;

    /// 关闭接收方向并等 `CLOSE(RESET)` 上线：[`channel_rx_close_async_`]。
    #[inline]
    fn close_async(&mut self) -> Self::CloseAsync<'_> {
        ChannelRxCloseAsync::new(self)
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 收尾等待的 step 函数
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// [`TrBuffFlush::flush_async`] 的 step 函数：等「发送环已排空」。
///
/// # 完成判据
///
/// 环内积压归零（[`ChannelTx::tx_drained_`]），或发送方向进入终局
/// （[`ChannelTx::tx_terminated_`]）——后者按 `Err(Closing)` 返回，因为那时剩余的
/// 字节已经不可能再被送出去。
///
/// # 取消
///
/// 等待期间可在任意 `await` 点被取消令牌打断，返回
/// [`ProducerError::Cancelled`]。取消**不**回滚已经封口 / 已经排空的部分：本函数
/// 只观察进度，不改协议状态。
#[gen_may_cancel_future(ChannelTxFlush, pub, new(pub(super)))]
async fn channel_tx_flush_async_<'f, C, K>(
    tx: &'f mut ChannelTx<C>,
    cancel: K,
) -> Result<(), ProducerError<usize>>
where
    C: TrConnCfg + 'f,
    K: TrCancellationToken,
{
    // 先补一次「发送环可能有数据」的提醒：本函数**不写入任何字节**，而写循环完全
    // 可能正 park 在事件通道上（应用上一次提交之后它已经排空并睡下）。
    tx.notify_tx_ready_();
    wait_tx_progress_(tx, &cancel, ChannelTx::<C>::tx_flush_ready_).await
}

/// [`TrBuffFlush::close_async`] 的 step 函数：封口并等 `CLOSE(FIN)` 上线。
///
/// 封口动作本身是同步且幂等的（[`ChannelTx::seal_tx_`]），因此它与 `drop` 走的是
/// 完全相同的一步；本函数额外的价值只有一个——**等到那条 `FIN` 真的写进传输**
/// （或等到它不可能再上线）。
///
/// # 取消
///
/// 等待期间被取消时返回 [`ProducerError::Cancelled`]；**封口本身已经完成**，不会
/// 因为调用方取消而撤销（这与「丢弃 future 即取消一次读 / 写」不同：封口是状态迁移，
/// 不是一次可回滚的提交）。
#[gen_may_cancel_future(ChannelTxClose, pub, new(pub(super)))]
async fn channel_tx_close_async_<'f, C, K>(
    tx: &'f mut ChannelTx<C>,
    cancel: K,
) -> Result<(), ProducerError<usize>>
where
    C: TrConnCfg + 'f,
    K: TrCancellationToken,
{
    tx.seal_tx_();
    wait_tx_progress_(tx, &cancel, ChannelTx::<C>::tx_close_ready_).await
}

/// [`TrChannelRx::close_async`] 的 step 函数：关闭接收方向并等 `CLOSE(RESET)` 上线。
///
/// # 完成判据
///
/// 本端 `RESET` 已写进传输（`ChannelState_::is_local_reset_flushed_`）、对端已
/// 宣告不再发送、或接收环写端已经关闭；连接级终局按 `Err(Closing)` 返回。
///
/// # 取消
///
/// 与 [`channel_tx_close_async_`] 同款：关闭动作已经完成，取消只影响这次**等待**。
#[gen_may_cancel_future(ChannelRxClose, pub, new(pub(crate)))]
async fn channel_rx_close_async_<'f, C, K>(
    rx: &'f mut ChannelRx<C>,
    cancel: K,
) -> Result<(), ConsumerError<usize>>
where
    C: TrConnCfg + 'f,
    K: TrCancellationToken,
{
    rx.seal_rx_();
    wait_rx_progress_(rx, &cancel, ChannelRx::<C>::rx_close_ready_).await
}

/// 等发送方向的下一次进展，直到 `ready` 给出结论。
///
/// # 为什么用 `poll_fn` 而不是「先查状态、再 await」
///
/// 「查状态」与「登记等待」之间必须没有缝：通知可能恰好**落在这两者之间**。等待槽
/// 的 `pending_` 位是**持久**的（`ChannelState_::tx_progress_poll_wait_` 先消费
/// 它、再登记、登记后复检），因此循环写成「复检 → 登记 → 复检」就不会丢唤醒。
///
/// # 取消
///
/// 每一次挂起都与调用方的取消令牌竞争（[`race_cancel_`]）：调用方取消时返回
/// [`ProducerError::Cancelled`]，而不是把等待留在槽里。
async fn wait_tx_progress_<C, K, F>(
    tx: &ChannelTx<C>,
    cancel: &K,
    ready: F,
) -> Result<(), ProducerError<usize>>
where
    C: TrConnCfg,
    K: TrCancellationToken,
    F: Fn(&ChannelTx<C>) -> Option<Result<(), ProducerError<usize>>>,
{
    let wait = poll_fn(|cx: &mut Context<'_>| loop {
        if let Option::Some(result) = ready(tx) {
            return Poll::Ready(result);
        }
        match tx.owner_.tx_progress_poll_wait_(cx) {
            // 有进展：回到循环顶部按最新状态复检。
            Poll::Ready(()) => continue,
            Poll::Pending => return Poll::Pending,
        }
    });
    match race_cancel_(cancel, wait).await {
        Option::Some(result) => result,
        Option::None => Result::Err(ProducerError::Cancelled),
    }
}

/// 等接收方向的下一次进展；语义与 [`wait_tx_progress_`] 完全对称。
async fn wait_rx_progress_<C, K, F>(
    rx: &ChannelRx<C>,
    cancel: &K,
    ready: F,
) -> Result<(), ConsumerError<usize>>
where
    C: TrConnCfg,
    K: TrCancellationToken,
    F: Fn(&ChannelRx<C>) -> Option<Result<(), ConsumerError<usize>>>,
{
    let wait = poll_fn(|cx: &mut Context<'_>| loop {
        if let Option::Some(result) = ready(rx) {
            return Poll::Ready(result);
        }
        match rx.owner_.rx_progress_poll_wait_(cx) {
            Poll::Ready(()) => continue,
            Poll::Pending => return Poll::Pending,
        }
    });
    match race_cancel_(cancel, wait).await {
        Option::Some(result) => result,
        Option::None => Result::Err(ConsumerError::Cancelled),
    }
}

#[cfg(test)]
mod tests_ {
    use core::{
        mem::MaybeUninit,
        task::{Context, Poll, Waker},
    };

    use abs_buff::{
        Demand,
        buffer::{TrBuffSegmMut, TrBuffSegmRef}};
    use abs_cancel::{CancelledToken, TrMayCancel};
    use buffex::ring::ConsumerError;
    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use crate::{
        connection::{
            owner_::{AbortCode_, new_owner_},
            ring_::test_support_::make_test_channel_,
            test_support_::{TestMuxConfig_, make_test_conn_},
        },
        flow_ctrl::DefaultPolicy,
    };

    use super::*;

    /// 测试用的子流半边类型（连接未经握手、不含任何循环，只用于检查本地行为）。
    type TestTx = ChannelTx<TestMuxConfig_>;
    type TestRx = ChannelRx<TestMuxConfig_>;

    /// 构造一对包在**内存环**上的子流半边（容量 64，dock 对 `(3, 7)`）。
    /// - 手段：先建一个「无循环连接」（[`make_test_conn_`]，只提供事件发送端与
    ///   保活），再用 `buffex::ring` 建一条容量 64 的环并切成读写两端，为它建一份
    ///   共享状态，最后把两个半部各自包成 [`ChannelTx`] / [`ChannelRx`]。
    /// - 判断：返回的 `(Tx, Rx)` 即被测对象；构建失败即测试失败。
    fn make_halves_() -> (TestTx, TestRx) {
        let (half_tx, half_rx) = make_test_channel_(64usize);
        let owner = new_owner_(CoreAlloc);
        owner.install_(&DefaultPolicy, 64usize);
        let conn = make_test_conn_();
        let local = Dock::new(3u32);
        let remote = Dock::new(7u32);
        (
            ChannelTx::new_(
                half_tx,
                owner.clone(),
                conn.clone(),
                local,
                remote,
                // 测试环容量 64 ⇒ 临界水位 64/4 = 16。
                16u32,
            ),
            ChannelRx::new_(half_rx, owner, conn, local, remote),
        )
    }

    /// 通过非阻塞接口写入全部字节。
    /// - 手段：按剩余长度要段、把实际写入量计入偏移、drop 段提交。
    /// - 判断：返回 `Ok` 表示写完；环满或关闭即返回错误。
    async fn try_write_all_<W>(tx: &mut W, bytes: &[u8]) -> Result<(), W::Err>
    where
        W: TrBuffTryWrite<u8>,
    {
        let mut offset = 0usize;
        while offset < bytes.len() {
            let rest = bytes.len() - offset;
            let demand = Demand::exactly(rest);
            let mut outcome = tx.try_write(&demand);
            let put = match outcome.as_mut().pick_left() {
                Option::Some(segm) => {
                    segm.as_segm_mut().clone_items_from_buff(&bytes[offset..])
                }
                Option::None => {
                    return Result::Err(
                        outcome.pick_right().expect("IO 结果必须要么是段、要么是错误"),
                    );
                }
            };
            if put == 0usize {
                break;
            }
            offset += put;
        }
        Result::Ok(())
    }

    /// 通过非阻塞接口读出恰好 `out.len()` 字节。
    /// - 手段：与 `try_write_all_` 对称，只搬走请求长度的前缀。
    /// - 判断：返回 `Ok` 表示读满；环空或关闭即返回错误。
    async fn try_read_exact_<R>(rx: &mut R, out: &mut [u8]) -> Result<(), R::Err>
    where
        R: TrBuffTryRead<u8>,
    {
        let mut offset = 0usize;
        while offset < out.len() {
            let rest = out.len() - offset;
            let demand = Demand::exactly(rest);
            let mut outcome = rx.try_read(&demand);
            let got = match outcome.as_mut().pick_left() {
                Option::Some(segm) => {
                    let mut child = segm.as_segm_ref();
                    let limit = core::cmp::min(rest, child.least_count());
                    let dst = &mut out[offset..offset + limit];
                    // SAFETY: `MaybeUninit<u8>` 与 `u8` 布局相同，且 `dst` 是本地
                    // 独占的可写切片；`move_items_to_buff` 只写入已初始化前缀。
                    let uninit = unsafe {
                        core::slice::from_raw_parts_mut(
                            dst.as_mut_ptr() as *mut MaybeUninit<u8>,
                            dst.len(),
                        )
                    };
                    unsafe { child.move_items_to_buff(uninit) }
                }
                Option::None => {
                    return Result::Err(
                        outcome.pick_right().expect("IO 结果必须要么是段、要么是错误"),
                    );
                }
            };
            if got == 0usize {
                break;
            }
            offset += got;
        }
        Result::Ok(())
    }

    /// 测试两个半边如实报告 dock 对，并把非阻塞读写转发给底下的环。
    /// - 手段：在内存环上构造 `(Tx, Rx)`（dock 对 `(3, 7)`），先断言四个 dock
    ///   取值，再用 `try_write` 写入 5 字节、用 `try_read` 读出并比对。
    /// - 判断：dock 与写入值完全一致；读回的字节与写入逐字节相等——说明包装层
    ///   没有吞掉或改写数据。
    async fn halves_report_docks_and_delegate_try_io() {
        let (mut tx, mut rx) = make_halves_();

        assert_eq!(tx.local_dock(), Dock::new(3u32));
        assert_eq!(tx.remote_dock(), Dock::new(7u32));
        assert_eq!(rx.local_dock(), Dock::new(3u32));
        assert_eq!(rx.remote_dock(), Dock::new(7u32));

        let payload = [1u8, 2, 3, 4, 5];
        try_write_all_(&mut tx, &payload)
            .await
            .expect("写入内存环应当成功");
        let mut got = [0u8; 5];
        try_read_exact_(&mut rx, &mut got)
            .await
            .expect("从内存环读出应当成功");
        assert_eq!(got, payload);
    }
    multi_art_test_!(halves_report_docks_and_delegate_try_io);

    /// 测试四个关闭标志分别对应环的两端，且两端互相可见。
    /// - 手段：新建的环上先断言四个标志全为假；然后关闭发送端（`ChannelTx`
    ///   底下的生产端），再关闭接收端（`ChannelRx` 底下的消费端），每次都读四个
    ///   标志。
    /// - 判断：关闭生产端后两个半边的 `is_tx_closed` 都变为真，而 `is_rx_closed`
    ///   仍为假；关闭消费端后两个半边的 `is_rx_closed` 也变为真——证明两个方向
    ///   互不影响、且状态由环共享。
    async fn close_flags_track_both_ends_independently() {
        let (mut tx, mut rx) = make_halves_();

        assert!(!tx.is_tx_closed());
        assert!(!tx.is_rx_closed());
        assert!(!rx.is_tx_closed());
        assert!(!rx.is_rx_closed());

        tx.ring_.close();
        assert!(tx.is_tx_closed(), "关闭生产端后发送方向应视为已关闭");
        assert!(rx.is_tx_closed(), "发送方向的关闭应对接收半边可见");
        assert!(!tx.is_rx_closed(), "接收方向不应受影响");
        assert!(!rx.is_rx_closed(), "接收方向不应受影响");

        rx.ring_.close();
        assert!(rx.is_rx_closed(), "关闭消费端后接收方向应视为已关闭");
        assert!(tx.is_rx_closed(), "接收方向的关闭应对发送半边可见");
    }
    multi_art_test_!(close_flags_track_both_ends_independently);

    /// 测试半关闭后的 EOF 语义：写端关闭不丢数据，排空后才报关闭。
    /// - 手段：写入 3 字节后关闭发送端；先把 3 字节读走，再尝试读 1 字节。
    /// - 判断：关闭后仍能读回全部残留数据；排空后再读返回
    ///   [`ConsumerError::Closing`]——即「先读完再 EOF」。
    async fn send_close_keeps_buffered_data_then_eof() {
        let (mut tx, mut rx) = make_halves_();

        let payload = [9u8, 8, 7];
        try_write_all_(&mut tx, &payload)
            .await
            .expect("写入内存环应当成功");
        tx.ring_.close();
        assert!(rx.is_tx_closed(), "写端关闭应立即可见");

        let mut got = [0u8; 3];
        try_read_exact_(&mut rx, &mut got)
            .await
            .expect("关闭写端不应丢弃已缓存的数据");
        assert_eq!(got, payload);

        let mut one = [0u8; 1];
        let err = try_read_exact_(&mut rx, &mut one)
            .await
            .expect_err("排空后继续读应当报错而不是空转");
        assert!(
            matches!(err, ConsumerError::Closing),
            "排空且写端已关闭后应报告 Closing"
        );
    }
    multi_art_test_!(send_close_keeps_buffered_data_then_eof);

    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // 收尾等待语义（`TrBuffFlush` / `TrChannelRx::close_async`）
    //-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// 用**空 waker** 轮询一次给定 future；`None` 表示仍然挂起。
    ///
    /// 手工 `poll` 是**故意**的：这些用例验的是等待槽的「登记 → 复检 → 唤醒」协议
    /// 本身，而不是某个运行时的调度行为。
    fn poll_once_<F>(fut: core::pin::Pin<&mut F>) -> Option<F::Output>
    where
        F: core::future::Future,
    {
        let mut cx = Context::from_waker(Waker::noop());
        match fut.poll(&mut cx) {
            Poll::Ready(out) => Option::Some(out),
            Poll::Pending => Option::None,
        }
    }

    /// 测试目标：`flush_async` 的完成点是**发送环排空**，而不是「已经封口」。
    /// - 手段：在内存环上建一对半部，写入 3 字节后开始 `flush_async` 并轮询一次；
    ///   随后用接收半边把这 3 字节读走，再模拟写循环报告一次「发送方向有进展」，
    ///   最后再轮询一次。
    /// - 判断：第一次轮询为挂起（环里还有数据、且没有任何执行者会取走它）；读空并
    ///   通知之后得到 `Ok(())`——若实现把完成点写成「`FIN` 已发」，第一次轮询就会
    ///   错误地就绪，或第二次轮询仍挂起，两种情况都会让本用例失败。
    async fn tx_flush_async_waits_for_ring_to_drain() {
        let (mut tx, mut rx) = make_halves_();
        let payload = [1u8, 2, 3];
        try_write_all_(&mut tx, &payload)
            .await
            .expect("写入内存环应当成功");

        let mut flush = core::pin::pin!(core::future::IntoFuture::into_future(TrBuffFlush::flush_async(&mut tx)));
        assert!(
            poll_once_(flush.as_mut()).is_none(),
            "发送环未排空时 flush_async 不应就绪"
        );

        let mut got = [0u8; 3];
        try_read_exact_(&mut rx, &mut got)
            .await
            .expect("把发送环读空应当成功");
        assert_eq!(got, payload);
        // 模拟写循环：取走数据之后报告一次进展（生产代码里由 `drain_one_` 调用）。
        rx.owner_.notify_tx_progress_();

        match poll_once_(flush.as_mut()) {
            Option::Some(Result::Ok(())) => {}
            other => panic!("排空之后 flush_async 应当就绪，实际 {other:?}"),
        }
    }
    multi_art_test_!(tx_flush_async_waits_for_ring_to_drain);

    /// 测试目标：对端宣告不再接收（`RESET`）之后，未排空的发送方向必须让
    /// `flush_async` **报错返回**，而不是永久挂起。
    /// - 手段：写入 1 字节（无人取走），开始 `flush_async` 并轮询一次；随后在共享
    ///   状态上置 `PEER_RESET` 并唤醒发送方向的等待者，再轮询一次。
    /// - 判断：第一次挂起；置位并唤醒之后得到 `Err(Closing)`——这些字节已经被对端
    ///   授权放弃，报「关闭」是与协议一致的终局。
    async fn tx_flush_async_fails_after_peer_reset() {
        let (mut tx, rx) = make_halves_();
        try_write_all_(&mut tx, &[7u8])
            .await
            .expect("写入内存环应当成功");

        let mut flush = core::pin::pin!(core::future::IntoFuture::into_future(TrBuffFlush::flush_async(&mut tx)));
        assert!(poll_once_(flush.as_mut()).is_none(), "初始应当挂起");

        rx.owner_.set_peer_reset_();
        rx.owner_.notify_tx_progress_();

        match poll_once_(flush.as_mut()) {
            Option::Some(Result::Err(ProducerError::Closing)) => {}
            other => panic!("对端 RESET 之后 flush_async 应当报 Closing，实际 {other:?}"),
        }
    }
    multi_art_test_!(tx_flush_async_fails_after_peer_reset);

    /// 测试目标：`close_async` 先**同步封口**，再等 `FIN` 上线。
    /// - 手段：开始 `close_async` 并轮询一次；断言此时发送环的生产端已经关闭；
    ///   随后置「本端 `FIN` 已发」位并唤醒等待者，再轮询一次。
    /// - 判断：第一次挂起但**封口动作已经生效**（`is_tx_closed` 为真）；置位并唤醒
    ///   之后得到 `Ok(())`。
    async fn tx_close_async_seals_then_waits_for_fin() {
        let (mut tx, rx) = make_halves_();

        let mut close = core::pin::pin!(core::future::IntoFuture::into_future(TrBuffFlush::close_async(&mut tx)));
        assert!(
            poll_once_(close.as_mut()).is_none(),
            "FIN 未上线时 close_async 不应就绪"
        );
        assert!(
            rx.is_tx_closed(),
            "close_async 的首次推进就应当关掉发送环的生产端"
        );

        rx.owner_.set_local_fin_sent_();
        rx.owner_.notify_tx_progress_();

        match poll_once_(close.as_mut()) {
            Option::Some(Result::Ok(())) => {}
            other => panic!("FIN 上线之后 close_async 应当就绪，实际 {other:?}"),
        }
    }
    multi_art_test_!(tx_close_async_seals_then_waits_for_fin);

    /// 测试目标：封口**幂等**——`close` 与 `drop` 复用同一套收尾时不会重复生效。
    /// - 手段：先调同步 `close`，读出状态字里的首置位（应当已经置上）；再调一次
    ///   `close`，最后丢弃发送半边。
    /// - 判断：第二次读首置位为 `false`（说明第一次已经置位、且不会重复投递
    ///   收尾提示）；重复 `close` 与随后的 `drop` 都不失败。
    async fn tx_close_is_idempotent_across_drop() {
        let (mut tx, _rx) = make_halves_();

        TrBuffTryFlush::close(&mut tx).expect("首次封口不应失败");
        assert!(
            !tx.owner_.set_app_tx_closed_(),
            "封口位应当已经由首次 close 置上"
        );
        TrBuffTryFlush::close(&mut tx).expect("重复封口应当幂等成功");
        // `drop` 走同一套收尾：这里只需不 panic，且不再产生第二条收尾提示。
        drop(tx);
    }
    multi_art_test_!(tx_close_is_idempotent_across_drop);

    /// 测试目标：`TrChannelRx::close_async` 等的是**本端 `RESET` 上线**。
    /// - 手段：开始 `close_async` 并轮询一次；断言接收环的消费端已经关闭；随后置
    ///   「`RESET` 已上线」位并唤醒接收方向的等待者，再轮询一次。
    /// - 判断：第一次挂起且接收方向已关闭（`is_rx_closed` 为真）；置位并唤醒之后
    ///   得到 `Ok(())`。
    async fn rx_close_async_waits_for_reset_to_flush() {
        let (tx, mut rx) = make_halves_();

        let mut close = core::pin::pin!(core::future::IntoFuture::into_future(TrChannelRx::close_async(&mut rx)));
        assert!(
            poll_once_(close.as_mut()).is_none(),
            "RESET 未上线时 close_async 不应就绪"
        );
        assert!(
            tx.is_rx_closed(),
            "close_async 的首次推进就应当关掉接收环的消费端"
        );

        tx.owner_.set_local_reset_flushed_();
        tx.owner_.notify_rx_progress_();

        match poll_once_(close.as_mut()) {
            Option::Some(Result::Ok(())) => {}
            other => panic!("RESET 上线之后 close_async 应当就绪，实际 {other:?}"),
        }
    }
    multi_art_test_!(rx_close_async_waits_for_reset_to_flush);

    /// 测试目标：对端已经宣告不再发送（`FIN`）时，关闭接收方向无需再等 `RESET`。
    /// - 手段：先在共享状态上置 `PEER_FIN`，再开始 `close_async` 并轮询。
    /// - 判断：首次轮询即得到 `Ok(())`——接收方向本来就到头了，不存在「还要通知
    ///   对端停止发送」这件事。
    async fn rx_close_async_completes_when_peer_fin() {
        let (tx, mut rx) = make_halves_();
        tx.owner_.set_peer_fin_();
        tx.owner_.notify_rx_progress_();

        let mut close = core::pin::pin!(core::future::IntoFuture::into_future(TrChannelRx::close_async(&mut rx)));
        match poll_once_(close.as_mut()) {
            Option::Some(Result::Ok(())) => {}
            other => panic!("对端 FIN 之后 close_async 应当立即就绪，实际 {other:?}"),
        }
    }
    multi_art_test_!(rx_close_async_completes_when_peer_fin);

    /// 测试目标：连接级终局（空闲超时）之下，`close_async` 必须**报错返回**。
    /// - 手段：开始 `close_async` 并轮询一次确认挂起；随后认领一次 `IdleTimeout`
    ///   中止（它会发布不可忽略通知）并唤醒两侧等待者，再轮询一次。
    /// - 判断：第一次挂起；中止之后得到 `Err(Closing)`——那条 `RESET` 不可能再送达。
    async fn rx_close_async_fails_on_abort() {
        let (_tx, mut rx) = make_halves_();
        // future 借走 `rx`，因此先克隆共享状态句柄：中止与唤醒都作用在它上面。
        let owner = rx.owner_.clone();

        let mut close = core::pin::pin!(core::future::IntoFuture::into_future(TrChannelRx::close_async(&mut rx)));
        assert!(poll_once_(close.as_mut()).is_none(), "初始应当挂起");

        assert!(
            owner.claim_abort_(AbortCode_::IdleTimeout),
            "首次认领中止应当成功"
        );
        owner.notify_both_progress_();

        match poll_once_(close.as_mut()) {
            Option::Some(Result::Err(ConsumerError::Closing)) => {}
            other => panic!("空闲超时之后 close_async 应当报 Closing，实际 {other:?}"),
        }
    }
    multi_art_test_!(rx_close_async_fails_on_abort);

    /// 测试目标：等待可以被调用方的取消令牌打断，且**已经完成的封口不回滚**。
    /// - 手段：用一个**已取消**的令牌包住发送半边的 `close_async` 并 await。
    /// - 判断：得到 `Err(Cancelled)`；同时发送环的生产端已经关闭（封口是状态迁移，
    ///   不是一次可回滚的提交）。
    async fn close_async_cancels_but_keeps_seal() {
        let (mut tx, _rx) = make_halves_();

        let out = TrMayCancel::may_cancel_with(
            TrBuffFlush::close_async(&mut tx),
            CancelledToken::new(),
        )
        .await;
        match out {
            Result::Err(ProducerError::Cancelled) => {}
            other => panic!("取消之后应当报 Cancelled，实际 {other:?}"),
        }
        assert!(
            tx.is_tx_closed(),
            "取消只结束这次等待，封口本身已经完成"
        );
    }
    multi_art_test_!(close_async_cancels_but_keeps_seal);
}
