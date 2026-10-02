use core::{
    alloc::AllocatorClone,
    borrow::BorrowMut,
    mem::MaybeUninit,
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    x_deps::anylr,
};
use abs_smux::conn::{TrChannelHalf, TrChannelRx, TrChannelTx};
use anylr::SomeOf;
use buffex::x_deps::abs_buff;

use crate::{
    connection::{
        BufferedRx, BufferedTx, Dock,
        owner_::ChannelOwner_,
        signal_::{EventSender_, TrEventSender_, WriteEvent_},
    },
    flow_ctrl::Credit,
};

/// 子流发送半边：包一个 `buffex` 生产端半部。
///
/// 实现 `TrBuffTryWrite<u8>`，因此应用侧写数据是**非阻塞**的：环满即返回
/// `WriteErrTag::Stuffed`，由应用决定等待还是丢弃。真正把数据推上网络的是
/// 内部写循环。
///
/// # 关闭语义（半关闭）
///
/// 本类型**按值独占**生产端半部，因此**丢弃它即关闭发送方向**（`buffex` 在
/// 半部 drop 时置位本端关闭标志）；会话据此得知「应用不再发送」并发出
/// `CLOSE(FIN)`。两个方向互不影响，关闭态直接取自环本身（见模块文档
/// 「关闭态」一节）。
pub struct ChannelTx<H> {
    /// `buffex` 生产端半部（[`BufferedTx`] 的实例）。
    half_: H,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock}

impl<H> ChannelTx<H> {
    /// 由 `buffex` 生产端半部与 dock 对构造。
    ///
    /// 只供连接内部（中心循环、建流路径）与单元测试使用：对外部使用者而言，
    /// 这两个半边只应由 `abs_smux` 的 trait 产出。
    pub(crate) fn new_(half: H, local_dock: Dock, remote_dock: Dock) -> Self {
        ChannelTx {
            half_: half,
            local_dock_: local_dock,
            remote_dock_: remote_dock}
    }
}

/// 子流接收半边：包一个 `buffex` 消费端半部。
///
/// 实现 `TrBuffTryRead<u8>`；环空即返回 `ReadErrTag::Drained`。数据由
/// 内部读循环从网络解复用后写入。
///
/// # 关闭语义（半关闭）
///
/// 丢弃本类型即关闭接收方向；写端关闭后先把残留数据读走，再 `try_read` 才会
/// 报 `Closing`（EOF 语义，见模块文档「关闭态」一节）。
pub struct ChannelRx<H> {
    /// `buffex` 消费端半部（[`BufferedRx`] 的实例）。
    half_: H,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock}

impl<H> ChannelRx<H> {
    /// 由 `buffex` 消费端半部与 dock 对构造；可见性同 [`ChannelTx::new_`]。
    pub(crate) fn new_(half: H, local_dock: Dock, remote_dock: Dock) -> Self {
        ChannelRx {
            half_: half,
            local_dock_: local_dock,
            remote_dock_: remote_dock}
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 应用侧环半部包装：环端 + 事件发送端 +（发送侧的）共享状态
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 应用侧**发送**环半部。
///
/// 除了 `buffex` 的写端，它还持有该子流的共享状态，因此能在应用开始写数据的那一刻
/// 通知写循环。
///
/// > **目标形状（本轮确定，迁移中）**：通知方式由「往事件通道投一条消息」改为
/// > **直接调用 `MuxCore::note_tx_ready_`**（同步、锁内改状态、出锁唤醒写循环），
/// > 见 `dev-notes` §17.3、§17.5。
/// >
/// > 同时**本类型改为对外导出**并取一个正常名字（如 `ChannelTxHalf<C, Rt>`），
/// > `ChannelTx` / `ChannelRx` 直接以 `<C, Rt>` 参数化。旧设计把内层包装标成模块
/// > 私有、只为保住 `ChannelTx` 的泛型 arity，那个理由在新设计下不再成立；不导出
/// > 的后果是下游**无法命名**已建立的 channel 类型（`dev-notes` §16.2 F5），
/// > 连「把半部存进结构体字段」都做不到。
pub struct TxRing_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
{
    /// 应用侧写端。
    ring_: BufferedTx<B, A>,

    /// 该子流的共享状态（用于「已入队」去重与关闭记账）。
    owner_: ChannelOwner_<A>,

    /// 写事件发送端。
    events_: EventSender_<WriteEvent_<B, A>>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock}

impl<B, A> TxRing_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
{
    /// 构造。
    pub(crate) fn new_(
        ring: BufferedTx<B, A>,
        owner: ChannelOwner_<A>,
        events: EventSender_<WriteEvent_<B, A>>,
        local_dock: Dock,
        remote_dock: Dock,
    ) -> Self {
        TxRing_ {
            ring_: ring,
            owner_: owner,
            events_: events,
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
            let _ = self.events_.try_send_event_(WriteEvent_::TxReady {
                local_dock: self.local_dock_,
                remote_dock: self.remote_dock_});
        }
    }
}

impl<B, A> Drop for TxRing_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
{
    /// 丢弃发送半边 = 半关闭：显式关闭环的生产端，并通知写循环排空后发 `FIN`。
    ///
    /// `Drop` 落在环包装类型上而不是 [`ChannelTx`] 上：Rust 不允许为泛型结构体的
    /// 某个具体实例化单独实现 `Drop`（E0366），而 `ChannelTx<H>` 是公开的泛型
    /// 结构体。
    fn drop(&mut self) {
        self.ring_.close();
        let _ = self.events_.try_send_event_(WriteEvent_::TxClosed {
            local_dock: self.local_dock_,
            remote_dock: self.remote_dock_});
    }
}

impl<B, A> TrBuffTryWrite<u8> for TxRing_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
{
    type SegmMut<'f>
        = <BufferedTx<B, A> as TrBuffTryWrite<u8>>::SegmMut<'f>
    where
        Self: 'f;

    type Err = <BufferedTx<B, A> as TrBuffTryWrite<u8>>::Err;

    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        self.notify_tx_ready_();
        self.ring_.try_write(demand)
    }
}

impl<B, A> TrBuffWrite<u8> for TxRing_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
{
    type WriteAsync<'f>
        = <BufferedTx<B, A> as TrBuffWrite<u8>>::WriteAsync<'f>
    where
        Self: 'f;

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        self.notify_tx_ready_();
        self.ring_.write_async(demand)
    }
}

/// 应用侧**接收**环半部。
///
/// 每次应用发起读之前，先按环的 `data_size` 变化算出**已提交消费量**并通知写循环
/// 回补窗口。用增量而不是「本次借出的段长」是为了不超前通告：段在被 drop 之前
/// 仍占用环空间。
/// 可见性理由同 [`TxRing_`]。
pub struct RxRing_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
{
    /// 应用侧读端。
    ring_: BufferedRx<B, A>,

    /// 写事件发送端。
    events_: EventSender_<WriteEvent_<B, A>>,

    /// 本端 dock。
    local_dock_: Dock,

    /// 对端 dock。
    remote_dock_: Dock,

    /// 上一次观察到的环内数据量（用于算已提交消费的增量）。
    last_data_: usize}

impl<B, A> RxRing_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
{
    /// 构造。
    pub(crate) fn new_(
        ring: BufferedRx<B, A>,
        events: EventSender_<WriteEvent_<B, A>>,
        local_dock: Dock,
        remote_dock: Dock,
    ) -> Self {
        RxRing_ {
            ring_: ring,
            events_: events,
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
            let _ = self.events_.try_send_event_(WriteEvent_::RxConsumed {
                local_dock: self.local_dock_,
                remote_dock: self.remote_dock_,
                amount_: amount});
        }
    }
}

impl<B, A> Drop for RxRing_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
{
    /// 丢弃接收半边：关掉环的消费端，并通知写循环发 `RESET` 拆掉该方向。
    fn drop(&mut self) {
        self.ring_.close();
        let _ = self.events_.try_send_event_(WriteEvent_::RxClosed {
            local_dock: self.local_dock_,
            remote_dock: self.remote_dock_});
    }
}

impl<B, A> TrBuffTryRead<u8> for RxRing_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
{
    type SegmRef<'f>
        = <BufferedRx<B, A> as TrBuffTryRead<u8>>::SegmRef<'f>
    where
        Self: 'f;

    type Err = <BufferedRx<B, A> as TrBuffTryRead<u8>>::Err;

    fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        self.note_consumed_();
        self.ring_.try_read(demand)
    }
}

impl<B, A> TrBuffRead<u8> for RxRing_<B, A>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
{
    type ReadAsync<'f>
        = <BufferedRx<B, A> as TrBuffRead<u8>>::ReadAsync<'f>
    where
        Self: 'f;

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        self.note_consumed_();
        self.ring_.read_async(demand)
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// TrConnection
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----


//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 子流发送半边
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 只有真正的 `buffex` 生产端才能回答「两个方向各自的关闭态」，因此本 impl 落在
/// 具体半部类型上（泛型的 `TrBuffTryWrite` 转发 impl 仍然对任意 `H` 成立）。
impl<B, A> TrChannelHalf for ChannelTx<TxRing_<B, A>>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
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
        self.half_.ring_.ring_state().is_producer_closed()
    }

    /// 接收方向是否已关闭：环的消费端（由写循环持有）已关闭，即整条子流已被
    /// 连接拆掉。
    fn is_rx_closed(&self) -> bool {
        self.half_.ring_.ring_state().is_consumer_closed()
    }
}

impl<H> TrBuffTryWrite<u8> for ChannelTx<H>
where
    H: TrBuffTryWrite<u8>,
{
    type SegmMut<'f>
        = H::SegmMut<'f>
    where
        Self: 'f;

    type Err = H::Err;

    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        self.half_.try_write(demand)
    }
}

impl<H> TrBuffWrite<u8> for ChannelTx<H>
where
    H: TrBuffWrite<u8>,
{
    type WriteAsync<'f>
        = H::WriteAsync<'f>
    where
        Self: 'f;

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        self.half_.write_async(demand)
    }
}

impl<B, A> TrChannelTx for ChannelTx<TxRing_<B, A>>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
{}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 子流接收半边
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl<B, A> TrChannelHalf for ChannelRx<RxRing_<B, A>>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
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
        self.half_.ring_.ring_state().is_producer_closed()
    }

    /// 接收方向是否已关闭：本端（应用）已不再接收。
    fn is_rx_closed(&self) -> bool {
        self.half_.ring_.ring_state().is_consumer_closed()
    }
}

impl<H> TrBuffTryRead<u8> for ChannelRx<H>
where
    H: TrBuffTryRead<u8>,
{
    type SegmRef<'f>
        = H::SegmRef<'f>
    where
        Self: 'f;

    type Err = H::Err;

    fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        self.half_.try_read(demand)
    }
}

impl<H> TrBuffRead<u8> for ChannelRx<H>
where
    H: TrBuffRead<u8>,
{
    type ReadAsync<'f>
        = H::ReadAsync<'f>
    where
        Self: 'f;

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        self.half_.read_async(demand)
    }
}

impl<B, A> TrChannelRx for ChannelRx<RxRing_<B, A>>
where
    B: BorrowMut<[MaybeUninit<u8>]> + Send + Sync,
    A: AllocatorClone + Send + Sync,
{}


#[cfg(test)]
mod tests_ {
    use abs_buff::{
        Demand,
        buffer::{TrBuffSegmMut, TrBuffSegmRef}};
    use buffex::ring::ConsumerError;
    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use crate::{
        connection::{
            owner_::ChannelState_,
            ring_::test_support_::{TestBuff, make_test_channel_},
            signal_::event_channel_,
        },
        flow_ctrl::{DefaultPolicy, FlowCtrl},
    };

    use super::*;

    /// 测试用的子流半边类型。
    type TestTx = ChannelTx<TxRing_<TestBuff, CoreAlloc>>;
    type TestRx = ChannelRx<RxRing_<TestBuff, CoreAlloc>>;

    /// 构造一对包在**内存环**上的子流半边（容量 64，dock 对 `(3, 7)`）。
    /// - 手段：用 `buffex::ring` 建一条容量 64 的环并切成读写两端，再为它建一份
    ///   共享状态与一对（无人消费的）事件通道，最后把两个半部包进
    ///   [`ChannelTx`] / [`ChannelRx`]。
    /// - 判断：返回的 `(Tx, Rx)` 即被测对象；构建失败即测试失败。
    fn make_halves_() -> (TestTx, TestRx) {
        let (half_tx, half_rx) = make_test_channel_(64usize);
        let flow = FlowCtrl::new(&DefaultPolicy, 64usize);
        let owner = ChannelOwner_::new_(ChannelState_::new_(flow), CoreAlloc);
        let (events, _events_rx) = event_channel_::<WriteEvent_<TestBuff, CoreAlloc>>();
        let local = Dock::new(3u32);
        let remote = Dock::new(7u32);
        (
            ChannelTx::new_(
                TxRing_::new_(half_tx, owner, events.clone(), local, remote),
                local,
                remote,
            ),
            ChannelRx::new_(RxRing_::new_(half_rx, events, local, remote), local, remote),
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

        tx.half_.ring_.close();
        assert!(tx.is_tx_closed(), "关闭生产端后发送方向应视为已关闭");
        assert!(rx.is_tx_closed(), "发送方向的关闭应对接收半边可见");
        assert!(!tx.is_rx_closed(), "接收方向不应受影响");
        assert!(!rx.is_rx_closed(), "接收方向不应受影响");

        rx.half_.ring_.close();
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
        tx.half_.ring_.close();
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
