//! telegraph 的**接收上下文**：解复用循环持有的「接收环写端 + 身份句柄」。
//!
//! # 职责边界
//!
//! 中心循环**不认识数据报的内部结构**：它只把「帧头 + 载荷」原样交给本单元
//! （[`TgRecvCtx_::deliver_frame_`]），由本单元负责：
//!
//! 1. 把帧头按本端的编码规则**重新成字节**（解析侧与编码侧共用 `frame_` 的规则，
//!    因此环里得到的是一条**自描述**的原始帧）；
//! 2. 把「帧头字节 + 载荷」作为**整帧**写进该端点的接收环；
//! 3. 把**帧总长**记进身份节点内联的接收队列并唤醒应用侧。
//!
//! 于是接收环里存的是**原始帧字节**，「这条报文从哪里开始、到哪里结束」不再需要
//! 中心循环理解——应用侧取到一段就同时拿到了远端地址（帧头）与载荷（帧头的后半）。
//! 远端地址与载荷的**拆分**因此发生在接收侧（见 [`super::datagram_`]），而不是在
//! 解复用循环里。
//!
//! # 丢弃语义
//!
//! 数据报是**尽力交付**：接收环剩余空间装不下整帧、或接收方向的条目队列已满时
//! **整帧丢弃**——不写半条、不入队、不重传，并计入 metrics
//! （[`TrMetricsSink::on_datagram_dropped`]，口径是**载荷**字节数）。
//!
//! # 没有任何接收者
//!
//! 本单元**存在**本身就是「有接收方」的判据：解复用循环的本地表里没有对应
//! `local_dock` 的条目时，整个帧直接丢弃（连丢弃计数都不记——对端可能按
//! `wildcard` 发来，那不是违例，也无人可归因）。
//!
//! [`TrMetricsSink::on_datagram_dropped`]: crate::metrics::TrMetricsSink::on_datagram_dropped

use core::alloc::AllocatorClone;

use abs_buff::Demand;
use buffex::x_deps::abs_buff;

use crate::{
    connection::{
        Dock, FrameHeader,
        frame_::encode_header_,
        owner_::TgOwner_,
        ring_::BufferedTx,
    },
    metrics::TrMetricsSink,
};

/// 解复用循环持有的一条 telegraph 接收上下文。
///
/// 字段与 `owner_.rs` 的身份节点体系同源：身份句柄给出**报文边界队列**
/// （`TgRec_::in_`），接收环写端给出**载荷字节落点**。
pub(crate) struct TgRecvCtx_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 本端 dock（丢弃上报用）。
    local_dock_: Dock,

    /// 该端点的身份节点句柄（含接收方向的条目队列）。
    owner_: TgOwner_<A>,

    /// 会话侧接收环写端（本循环写、应用读）。
    writer_: BufferedTx,
}

impl<A> TgRecvCtx_<A>
where
    A: AllocatorClone + Send + Sync,
{
    /// 由本端 dock、身份句柄与接收环写端构造（只允许 `ReadEvent_::TgAttach`
    /// 的落地处调用）。
    pub(crate) fn new_(local_dock: Dock, owner: TgOwner_<A>, writer: BufferedTx) -> Self {
        TgRecvCtx_ {
            local_dock_: local_dock,
            owner_: owner,
            writer_: writer,
        }
    }

    /// 关闭接收环写端。
    ///
    /// 收尾时**必须**显式调用：`buffex` 的环半部被 drop 不会置位关闭标记，少了这一步
    /// 应用侧正 park 的 `recv_async` 永远醒不过来（与 channel 的接收环同一纪律）。
    pub(crate) fn close_(&mut self) {
        self.writer_.close();
    }

    /// 把一条 `DATAGRAM` 的**整帧**投进接收环。
    ///
    /// `header` 是中心循环已解析出的帧头，`payload` 是紧随其后的载荷字节。本方法
    /// 自行从 `header` 拆出「编码后的帧头字节」，与 `payload` 拼成整帧落进环里。
    ///
    /// 装不下（环剩余空间不足 / 条目队列已满）时**整帧丢弃**并计入 metrics；环被
    /// 关闭时同样丢弃——写进去的字节没有长度记录，应用侧永远不会读到它们。
    pub(crate) fn deliver_frame_<M>(&mut self, header: &FrameHeader, payload: &[u8], metrics: &M)
    where
        M: TrMetricsSink,
    {
        // 0. 帧头重新成字节。失败只可能是内部矛盾（字段组合不合法），如实丢弃。
        let encoded = encode_header_(header);
        let Ok((head_buf, head_len)) = encoded else {
            self.report_dropped_(header, payload, metrics);
            return;
        };
        let frame_len = head_len + payload.len();

        // 1. 两项容量**先判**，再动手：这样「写进环」与「入条目队列」之间不会出现
        //    「一半成功、一半失败」的中间态，也就不需要任何撤回操作。
        //
        //    - 条目队列满：应用侧没在取（它只是尽力交付的接收方），整帧丢弃；
        //    - 环剩余空间不足：**整帧丢弃**，绝不留半条——半条会破坏应用侧
        //      「一次收取就是一条报文」的契约。
        if self.owner_.in_().is_full_()
            || self.writer_.ring_state().free_size() < frame_len
        {
            self.report_dropped_(header, payload, metrics);
            return;
        }

        // 2. 整帧落环：先帧头、后载荷。段级循环，因此与环容量无关（上面的容量检查
        //    与本循环之间没有 `await`，所以它一定能写完）。
        if !write_all_(&mut self.writer_, &head_buf[..head_len])
            || !write_all_(&mut self.writer_, payload)
        {
            // 容量检查之后环被关闭（连接正在收尾）：如实丢弃。
            self.report_dropped_(header, payload, metrics);
            return;
        }

        // 3. 整帧已就位：入队**帧总长**（含空载荷），再唤醒应用侧。
        //
        //    接收方向的队列槽位不承载远端地址——地址就在环里的帧头字节中，由接收侧
        //    自行拆出（这正是「投整帧」的意义）。
        self.owner_
            .in_()
            .try_push_(Dock::unspecified(), frame_len)
            .expect("容量已判过");
        self.owner_.in_().notify_();
    }

    /// 记一次「整条丢弃」（口径是**载荷**字节数，不含帧头）。
    fn report_dropped_<M>(&self, header: &FrameHeader, payload: &[u8], metrics: &M)
    where
        M: TrMetricsSink,
    {
        metrics.on_datagram_dropped(
            self.local_dock_,
            header.remote_dock(),
            u32::try_from(payload.len()).unwrap_or(u32::MAX),
        );
    }
}

/// 把 `bytes` 全部写进环写端。
///
/// 段级循环的省事封装：借一段、能写多少写多少、段 drop 时按写入量提交。每次只按
/// 「至少 1 字节」索要，因此与环容量无关。返回 `false` 表示环已关闭或借不出段。
fn write_all_(writer: &mut BufferedTx, mut bytes: &[u8]) -> bool {
    while !bytes.is_empty() {
        let demand = Demand::at_least(1usize);
        let mut outcome = writer.try_write(&demand);
        let put = match outcome.as_mut().pick_left() {
            Option::Some(segm) => {
                let mut child = segm.as_segm_mut();
                child.clone_items_from_buff(bytes)
            }
            Option::None => 0usize,
        };
        if put == 0usize {
            return false;
        }
        bytes = &bytes[put..];
    }
    true
}
