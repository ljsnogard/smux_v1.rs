//! 子流的两个应用侧半部：[`ChannelTx`] / [`ChannelRx`]。
//!
//! 对应 `abs_smux` 的 `TrChannelTx` / `TrChannelRx` / `TrChannelHalf`；数据面则
//! 直接实现 `abs_buff` 的 `TrBuffWrite` / `TrBuffRead`。
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
//! 现在两个半部直接以 `<W, R, S, C>` 参数化（环半部类型由 `C` 决定，是**唯一**
//! 的），E0366 不再适用，因此内层包装被删除：两个半部**各自就是那个具名、可写进
//! 结构体字段的具体类型**（旧模型下内层包装未导出，下游根本写不出已建立 channel
//! 的类型，见 `dev-notes` §16.2 F5）。

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    x_deps::anylr,
};
use abs_smux::chan::{TrChannelHalf, TrChannelRx, TrChannelTx};
use anylr::SomeOf;
use buffex::x_deps::abs_buff;

use crate::{
    connection::{
        BufferedRx, BufferedTx, Dock, MuxConnection, TrMuxConfig,
        // config_::TrMuxAllocConfig,
        owner_::ChannelOwner_,
        signal_::{TrEventSender_, WriteEvent_},
    },
    flow_ctrl::Credit,
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
pub struct ChannelTx<W, R, S, C>
where
    C: TrMuxConfig,
{
    /// `buffex` 生产端半部（[`BufferedTx`] 的实例）。
    ring_: BufferedTx<C::Buff, C::Alloc>,

    /// 该子流的共享状态（「已入队」去重与关闭记账）。
    owner_: ChannelOwner_<C::Alloc>,

    /// 连接智能指针：通知写循环 + 保活。
    conn_: MuxConnection<W, R, S, C>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock}

impl<W, R, S, C> ChannelTx<W, R, S, C>
where
    C: TrMuxConfig,
{
    /// 由环生产端、共享状态、连接与 dock 对构造。
    ///
    /// 只供连接内部（建流路径）与单元测试使用：对外部使用者而言，这两个半边只应由
    /// `abs_smux` 的 trait 产出。
    pub(crate) fn new_(
        ring: BufferedTx<C::Buff, C::Alloc>,
        owner: ChannelOwner_<C::Alloc>,
        conn: MuxConnection<W, R, S, C>,
        local_dock: Dock,
        remote_dock: Dock,
    ) -> Self {
        ChannelTx {
            ring_: ring,
            owner_: owner,
            conn_: conn,
            local_dock_: local_dock,
            remote_dock_: remote_dock}
    }

    /// 通知写循环「这条子流的发送环可能有数据」。
    ///
    /// 每条子流至多一条待处理事件：只有 `tx_queued_` 由假变真时才真正投递
    /// （顺序与去重协议见 `dev-notes` §11.4）。
    fn notify_tx_ready_(&self) {
        let fresh = self.owner_.with_mut_(|state| {
            let fresh = !state.tx_queued_();
            state.set_tx_queued_(true);
            fresh
        });
        if fresh {
            let _ = self
                .conn_
                .core_()
                .w_events_()
                .try_send_event_(WriteEvent_::TxReady {
                    local_dock: self.local_dock_,
                    remote_dock: self.remote_dock_});
        }
    }
}

impl<W, R, S, C> Drop for ChannelTx<W, R, S, C>
where
    C: TrMuxConfig,
{
    /// 丢弃发送半边 = 半关闭：显式关闭环的生产端，并通知写循环排空后发 `FIN`。
    fn drop(&mut self) {
        self.ring_.close();
        let _ = self
            .conn_
            .core_()
            .w_events_()
            .try_send_event_(WriteEvent_::TxClosed {
                local_dock: self.local_dock_,
                remote_dock: self.remote_dock_});
    }
}

impl<W, R, S, C> TrBuffTryWrite<u8> for ChannelTx<W, R, S, C>
where
    C: TrMuxConfig,
{
    type SegmMut<'f>
        = <BufferedTx<C::Buff, C::Alloc> as TrBuffTryWrite<u8>>::SegmMut<'f>
    where
        Self: 'f;

    type Err = <BufferedTx<C::Buff, C::Alloc> as TrBuffTryWrite<u8>>::Err;

    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        self.notify_tx_ready_();
        self.ring_.try_write(demand)
    }
}

impl<W, R, S, C> TrBuffWrite<u8> for ChannelTx<W, R, S, C>
where
    C: TrMuxConfig,
{
    type WriteAsync<'f>
        = <BufferedTx<C::Buff, C::Alloc> as TrBuffWrite<u8>>::WriteAsync<'f>
    where
        Self: 'f;

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        self.notify_tx_ready_();
        self.ring_.write_async(demand)
    }
}

impl<W, R, S, C> TrChannelHalf for ChannelTx<W, R, S, C>
where
    C: TrMuxConfig,
{
    type Data = u8;
    type Dock = Dock;

    fn local_dock(&self) -> Self::Dock {
        self.local_dock_
    }

    fn remote_dock(&self) -> Self::Dock {
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

impl<W, R, S, C> TrChannelTx for ChannelTx<W, R, S, C> where C: TrMuxConfig {}

/// 子流接收半边（应用侧**消费端**）。
///
/// 实现 `TrBuffTryRead<u8>`；环空即返回 `ReadErrTag::Drained`。数据由
/// 内部读循环从网络解复用后写入。
///
/// 每次应用发起读之前，先按环的 `data_size` 变化算出**已提交消费量**并通知写循环
/// 回补窗口。用增量而不是「本次借出的段长」是为了不超前通告：段在被 drop 之前
/// 仍占用环空间。
///
/// # 关闭语义（半关闭）
///
/// 丢弃本类型即关闭接收方向；写端关闭后先把残留数据读走，再 `try_read` 才会
/// 报 `Closing`（EOF 语义，见模块文档「关闭态」一节）。
pub struct ChannelRx<W, R, S, C>
where
    C: TrMuxConfig,
{
    /// `buffex` 消费端半部（[`BufferedRx`] 的实例）。
    ring_: BufferedRx<C::Buff, C::Alloc>,

    /// 连接智能指针：通知写循环 + 保活。
    conn_: MuxConnection<W, R, S, C>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,

    /// 上一次观察到的环内数据量（用于算已提交消费的增量）。
    last_data_: usize}

impl<W, R, S, C> ChannelRx<W, R, S, C>
where
    C: TrMuxConfig,
{
    /// 由环消费端、连接与 dock 对构造；可见性同 [`ChannelTx::new_`]。
    pub(crate) fn new_(
        ring: BufferedRx<C::Buff, C::Alloc>,
        conn: MuxConnection<W, R, S, C>,
        local_dock: Dock,
        remote_dock: Dock,
    ) -> Self {
        ChannelRx {
            ring_: ring,
            conn_: conn,
            local_dock_: local_dock,
            remote_dock_: remote_dock,
            last_data_: 0usize}
    }

    /// 观察 `data_size` 的下降量（= 上次调用之后已提交的消费量）并上报。
    fn note_consumed_(&mut self) {
        let now = self.ring_.ring_state().data_size();
        let delta = self.last_data_.saturating_sub(now);
        self.last_data_ = now;
        if delta > 0 {
            let amount = Credit::try_from(delta).unwrap_or(Credit::MAX);
            let _ = self
                .conn_
                .core_()
                .w_events_()
                .try_send_event_(WriteEvent_::RxConsumed {
                    local_dock: self.local_dock_,
                    remote_dock: self.remote_dock_,
                    amount_: amount});
        }
    }
}

impl<W, R, S, C> Drop for ChannelRx<W, R, S, C>
where
    C: TrMuxConfig,
{
    /// 丢弃接收半边：关掉环的消费端，并通知写循环发 `RESET` 拆掉该方向。
    fn drop(&mut self) {
        self.ring_.close();
        let _ = self
            .conn_
            .core_()
            .w_events_()
            .try_send_event_(WriteEvent_::RxClosed {
                local_dock: self.local_dock_,
                remote_dock: self.remote_dock_});
    }
}

impl<W, R, S, C> TrBuffTryRead<u8> for ChannelRx<W, R, S, C>
where
    C: TrMuxConfig,
{
    type SegmRef<'f>
        = <BufferedRx<C::Buff, C::Alloc> as TrBuffTryRead<u8>>::SegmRef<'f>
    where
        Self: 'f;

    type Err = <BufferedRx<C::Buff, C::Alloc> as TrBuffTryRead<u8>>::Err;

    fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        self.note_consumed_();
        self.ring_.try_read(demand)
    }
}

impl<W, R, S, C> TrBuffRead<u8> for ChannelRx<W, R, S, C>
where
    C: TrMuxConfig,
{
    type ReadAsync<'f>
        = <BufferedRx<C::Buff, C::Alloc> as TrBuffRead<u8>>::ReadAsync<'f>
    where
        Self: 'f;

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        self.note_consumed_();
        self.ring_.read_async(demand)
    }
}

impl<W, R, S, C> TrChannelHalf for ChannelRx<W, R, S, C>
where
    C: TrMuxConfig,
{
    type Data = u8;
    type Dock = Dock;

    fn local_dock(&self) -> Self::Dock {
        self.local_dock_
    }

    fn remote_dock(&self) -> Self::Dock {
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

impl<W, R, S, C> TrChannelRx for ChannelRx<W, R, S, C> where C: TrMuxConfig {}

#[cfg(test)]
mod tests_ {
    use core::mem::MaybeUninit;

    use abs_buff::{
        Demand,
        buffer::{TrBuffSegmMut, TrBuffSegmRef}};
    use buffex::ring::ConsumerError;
    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use crate::{
        connection::{
            owner_::ChannelState_,
            ring_::test_support_::make_test_channel_,
            test_support_::{NullScope_, TestMuxConfig_, TestWireRx_, TestWireTx_, make_test_conn_},
        },
        flow_ctrl::{DefaultPolicy, FlowCtrl, ReportThresholds_},
    };

    use super::*;

    /// 测试用的子流半边类型（连接未经握手、不含任何循环，只用于检查本地行为）。
    type TestTx = ChannelTx<TestWireTx_, TestWireRx_, NullScope_, TestMuxConfig_>;
    type TestRx = ChannelRx<TestWireTx_, TestWireRx_, NullScope_, TestMuxConfig_>;

    /// 构造一对包在**内存环**上的子流半边（容量 64，dock 对 `(3, 7)`）。
    /// - 手段：先建一个「无循环连接」（[`make_test_conn_`]，只提供事件发送端与
    ///   保活），再用 `buffex::ring` 建一条容量 64 的环并切成读写两端，为它建一份
    ///   共享状态，最后把两个半部各自包成 [`ChannelTx`] / [`ChannelRx`]。
    /// - 判断：返回的 `(Tx, Rx)` 即被测对象；构建失败即测试失败。
    fn make_halves_() -> (TestTx, TestRx) {
        let (half_tx, half_rx) = make_test_channel_(64usize);
        let flow = FlowCtrl::new(&DefaultPolicy, 64usize);
        let owner = ChannelOwner_::new_(
            ChannelState_::new_(
                flow,
                ReportThresholds_::new_(&DefaultPolicy, 64u32),
            ),
            CoreAlloc,
        );
        let conn = make_test_conn_();
        let local = Dock::new(3u32);
        let remote = Dock::new(7u32);
        (
            ChannelTx::new_(
                half_tx,
                owner,
                conn.clone(),
                local,
                remote,
            ),
            ChannelRx::new_(half_rx, conn, local, remote),
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
    #[compio::test]
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

    /// 测试四个关闭标志分别对应环的两端，且两端互相可见。
    /// - 手段：新建的环上先断言四个标志全为假；然后关闭发送端（`ChannelTx`
    ///   底下的生产端），再关闭接收端（`ChannelRx` 底下的消费端），每次都读四个
    ///   标志。
    /// - 判断：关闭生产端后两个半边的 `is_tx_closed` 都变为真，而 `is_rx_closed`
    ///   仍为假；关闭消费端后两个半边的 `is_rx_closed` 也变为真——证明两个方向
    ///   互不影响、且状态由环共享。
    #[compio::test]
    async fn close_flags_track_both_ends_independently() {
        let (tx, rx) = make_halves_();

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

    /// 测试半关闭后的 EOF 语义：写端关闭不丢数据，排空后才报关闭。
    /// - 手段：写入 3 字节后关闭发送端；先把 3 字节读走，再尝试读 1 字节。
    /// - 判断：关闭后仍能读回全部残留数据；排空后再读返回
    ///   [`ConsumerError::Closing`]——即「先读完再 EOF」。
    #[compio::test]
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
}
