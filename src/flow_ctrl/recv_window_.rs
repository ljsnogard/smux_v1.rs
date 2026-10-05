use crate::flow_ctrl::{Credit, FlowCtrlError, RecvTotal, TrFlowCtrlPolicy, WindowReport};

/// 提醒的**分区**：只分「临界」与「非临界」，判据一律看**剩余容量**。
///
/// ```text
/// | 区间                   | 行为                                   |
/// | ---------------------- | -------------------------------------- |
/// | 剩余 ≤ 容量/N（临界区） | 任何变化都提醒（不受频率门限约束）      |
/// | 剩余 > 容量/N          | 累计变动量 ≥ min_advance 才提醒（防抖） |
/// ```
///
/// 边界 `容量/N` 由 [`TrFlowCtrlPolicy::critical_denominator`] 推出。
///
/// # 为什么没有「充裕区不提醒」这一档
///
/// 曾经在 `容量×(1−1/N)` 以上再加一档「不提醒」，理由是「离瘫痪还远，不必产生控制
/// 帧」。这个理由站不住：一旦本端不再更新通告，对端的可用额度就永远停在旧快照上，
/// 连「本端已经消费掉、额度其实变大了」也传不过去；对端于是拿着 `available == 0`
/// 空转，而本端明明空着额度（真实 socket 的流控验收用例撞到过）。
///
/// 现在的口径是**政策不随水位放松**：临界区每变必报；非临界区按变动量门限（缺省即
/// `容量/N`）报，**上不封顶**。通告因此始终在推进，代价只是离瘫痪越远、控制帧越稀。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Zone_ {
    /// 非临界区：剩余容量尚可，按变动量门限提醒。
    Normal,

    /// 临界区：剩余 `≤ 容量/N`，任何变化都提醒。
    Critical,
}


/// 接收窗口：本端**还愿意接收**多少字节，以及提醒所需的状态。
///
/// 三个量各自累积：累计已收 `R`、累计已消费 `C`、以及最近一次通告出去的快照
/// `(R₀, W₀)`。当前可通告的窗口是物理剩余
///
/// ```text
/// W = cap − (R − C)
/// ```
///
/// 而越权判定用的是**已通告**的额度：对端最多只能发到 `R₀ + W₀`。
pub struct RecvWindow {
    /// 接收窗口容量（由策略从环容量推导，同时也是本端通告的上界）。
    capacity_: Credit,

    /// 累计已收字节数（`R`）。
    received_: RecvTotal,

    /// 累计已消费（交给应用）字节数（`C`）。
    consumed_: RecvTotal,

    /// 最近一次通告的快照 `(R₀, W₀)`；`None` 表示还没通告过。
    reported_: Option<(RecvTotal, Credit)>,



    /// 自上次提醒以来累计的**变动量**（字节）：收到多少 + 消费多少。
    ///
    /// 只在**非临界区**的频率门限里用到。量字节而不量帧数，是为了让「哪一侧在动」都
    /// 能推进它（理由见 [`TrFlowCtrlPolicy::min_advance_between_reports`]）。
    activity_since_report_: RecvTotal,

    /// 判定分区所需的策略快照；由 `new_` 展开一次。
    ///
    /// 存下来是为了让**不持有策略**的入口（[`RecvWindow::should_report`] /
    /// [`RecvWindow::report`]）也能自己判断，不必把调用方的 `C`/`P` 带进读写循环。
    thresholds_: ReportThresholds_,

    /// 当前 epoch 的起点（绝对累计量）：每次重置后推进到当时的累计已收量。
    epoch_base_: RecvTotal,

    /// 本端累计量的 epoch 规格：涨到它就重置。
    epoch_limit_: RecvTotal,
}


impl RecvWindow {
    /// 以策略算出的参数构造。
    pub(crate) fn new_<P>(policy: &P, ring_capacity: usize) -> Self
    where
        P: TrFlowCtrlPolicy,
    {
        let initial = policy.initial_window(ring_capacity);
        let thresholds_ = ReportThresholds_::new_(policy, initial);
        RecvWindow {
            capacity_: initial,
            thresholds_,
            received_: 0u64,
            consumed_: 0u64,
            reported_: Option::None,
            activity_since_report_: 0u64,
            epoch_base_: 0u64,
            epoch_limit_: policy.recv_total_epoch(),
        }
    }

    /// 本端承诺的最大可接收字节数（初始窗口 = `OPEN` 通告的值）。
    pub const fn capacity(&self) -> Credit {
        self.capacity_
    }

    /// 当前可通告的接收窗口 `W`（物理剩余）。
    pub fn window(&self) -> Credit {
        let buffered = self.received_.saturating_sub(self.consumed_);
        let buffered = Credit::try_from(buffered).unwrap_or(Credit::MAX);
        self.capacity_.saturating_sub(buffered)
    }

    /// **绝对**累计已收字节数（自子流建立以来的总量）。
    pub const fn recv_total(&self) -> RecvTotal {
        self.received_
    }

    /// 当前 epoch 内、将要写进线格式的累计已收量（= 绝对量 − epoch 起点）。
    pub const fn encoded_recv_total(&self) -> RecvTotal {
        self.received_.saturating_sub(self.epoch_base_)
    }

    /// 累计量是否已经涨到当前 epoch 规格，必须在下次通告里宣告重置。
    pub const fn reset_due(&self) -> bool {
        self.encoded_recv_total() >= self.epoch_limit_
    }

    /// 累计已消费字节数（`C`）。
    pub const fn consumed_total(&self) -> RecvTotal {
        self.consumed_
    }

    /// 最近一次通告出去的窗口（诊断用）。
    pub const fn reported_window(&self) -> Option<Credit> {
        match self.reported_ {
            Option::Some((_, w)) => Option::Some(w),
            Option::None => Option::None,
        }
    }


    /// 对端又发来 `amount` 字节（一次调用 = 一个数据帧），计入在途并推进帧计数。
    ///
    /// # Errors
    ///
    /// 超过**已通告**额度（`R₀ + W₀`）时返回 [`FlowCtrlError::PeerViolation`]——
    /// 这是协议违例，不是「丢弃即可」。
    pub fn on_data(&mut self, amount: Credit) -> Result<(), FlowCtrlError> {
        let authorized = match self.reported_ {
            Option::Some((r0, w0)) => r0.saturating_add(w0 as u64),
            // 还没通告过任何窗口：对端本来就不该发数据。
            Option::None => 0u64,
        };
        let next = self.received_.saturating_add(amount as u64);
        if next > authorized {
            return Result::Err(FlowCtrlError::PeerViolation);
        }
        self.received_ = next;
        // 只有**被接受的**数据才算变动量：违例帧（上面已返回）不算，否则对端可以靠
        // 灌违例帧把门限刷开。零字节帧自然也不贡献变动量。
        self.activity_since_report_ = self.activity_since_report_.saturating_add(amount as u64);
        Result::Ok(())
    }

    /// 应用又消费（取走）了 `amount` 字节，窗口相应变大。
    pub fn on_consumed(&mut self, amount: Credit) {
        self.consumed_ = self.consumed_.saturating_add(amount as u64);
        // 消费同样是「真实进展」，同样推进非临界区的频率门限（见
        // [`TrFlowCtrlPolicy::min_advance_between_reports`] 的口径说明）。
        self.activity_since_report_ = self.activity_since_report_.saturating_add(amount as u64);
    }

    /// 当前剩余容量落在哪个分区（用预先展开好的阈值快照判）。
    pub(crate) fn zone_with_(&self, thresholds: &ReportThresholds_) -> Zone_ {
        let free = self.window();
        if free <= thresholds.critical_ {
            Zone_::Critical
        } else {
            Zone_::Normal
        }
    }

    /// 是否应当发一条提醒。
    ///
    /// 判据只有两条：
    ///
    /// 1. **临界区**（剩余 `≤ 容量/N`）——只要窗口与上次通告不同就发，**不看变动量
    ///    门限**。越接近瘫痪越要保证发送方拿得到新额度；
    /// 2. **非临界区**（剩余 `> 容量/N`）——窗口确实变了、且自上次提醒以来累计的
    ///    **变动量**达到 [`TrFlowCtrlPolicy::min_advance_between_reports`] 才发（防抖）。
    ///
    /// 「非临界区」**没有上限档**：剩余再多也照样按同一政策推进通告。曾经有过一档
    /// 「剩余 `> 容量×(1−1/N)` 就不提醒」，结果是本端停止更新通告之后，对端的额度
    /// 永远停在旧快照上，连本端消费掉多少都传不过去（理由见 `Zone_` 的文档）。
    ///
    /// 「窗口与上次通告一模一样」时一律不发——任何一档都没有新信息可携带。
    ///
    /// 与更早的刻度列实现相比少了两个特例：旧实现要给「归零 / 从 0 抬起」单独开免
    /// 门限的后门，否则会死锁。现在归零落在**临界区**（0 ≤ 容量/N 恒成立），临界区
    /// 本来就每变必报，后门因此不再需要。
    ///
    /// 这里只做判断、不改状态；真正发提醒用 [`RecvWindow::report`]。
    pub fn should_report<P>(&self, _policy: &P) -> bool
    where
        P: TrFlowCtrlPolicy,
    {
        self.should_report_with_(&self.thresholds_)
    }

    /// 与 [`RecvWindow::should_report`] 同判据，但用**预先展开好的阈值快照**。
    ///
    /// 读写循环不持有调用方的 `C`/`P`，因此在连接建立时把策略展开成
    /// [`ReportThresholds_`] 交给它们即可（见 `connection/session_.rs`）。
    pub(crate) fn should_report_with_(&self, thresholds: &ReportThresholds_) -> bool {
        let Option::Some((_, last)) = self.reported_ else {
            return true;
        };
        // 重置是**编码前提**（窄规格要放不下了），不受任何门限约束。
        if self.reset_due() {
            return true;
        }
        // 「窗口与上次通告相同」即没有新信息：无论哪个分区都不发。反向也成立——窗口
        // 变了就必然越过一次分区边界或留在原分区，两种情形下面各自处理。
        //
        // （不需要再比一次「上次所在分区」：分区是当前窗口的函数，窗口相同则分区必然
        // 相同；而窗口不同时，「进入临界区」本身就是一次变化，会被下面第 1 条无条件
        // 放行。）
        if self.window() == last {
            return false;
        }
        // 1. 临界区：任何变化都提醒（含「刚跌进临界区」那一次）。
        if self.zone_with_(thresholds) == Zone_::Critical {
            return true;
        }
        // 2. 非临界区：变化 + 变动量门限（防抖），上不封顶。
        self.activity_since_report_ >= thresholds.min_advance_ as u64
    }

    /// 生成一份通告快照并把它记为「已通告」（此后越权判定以它为准、变动量清零）。
    ///
    /// 若累计量已经涨到当前 epoch 规格（[`RecvWindow::reset_due`]），这里会自动产出
    /// **重置变体**：携带重置前的绝对累计量并把 epoch 起点推进到它，其后的通告里
    /// 累计量从 `0` 重新计数。
    ///
    /// 保活 `PULSE` 无条件用它取当前窗口；按阈值通告则由
    /// [`RecvWindow::should_report`] 先判断。
    pub fn report(&mut self) -> WindowReport {
        let report = if self.reset_due() {
            let reset = WindowReport::new_reset(self.received_, self.window());
            self.epoch_base_ = self.received_;
            reset
        } else {
            WindowReport::new(self.encoded_recv_total(), self.window())
        };
        // 越权判定始终按**绝对**量记账：通告发出时对端最多能发到「当前已收 + W」。
        self.reported_ = Option::Some((self.received_, report.window()));
        self.activity_since_report_ = 0u64;
        report
    }
}


/// 通告判定所需的策略快照。
///
/// 读写循环运行在 `abs_art` spawn 出来的任务里（`'static`），既拿不到
/// [`TrMuxConfig`](crate::connection::TrMuxConfig) 也借不到 `TrFlowCtrlPolicy`；
/// 因此在连接建立时把「是否该通告」用到的两个量**展开一次**，此后判定只用它。
/// 两个量都只依赖策略与初始窗口，而初始窗口由环容量唯一确定，所以整条连接共享
/// 一份即可。
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReportThresholds_ {
    /// 非临界区两次提醒之间至少要积累的变动量（字节）。
    min_advance_: Credit,

    /// **临界区**的上界：剩余 `≤ critical_` 即进入临界区（任何变化都提醒）。
    critical_: Credit,
}


impl ReportThresholds_ {
    /// 从策略与初始窗口展开。
    pub(crate) fn new_<P>(policy: &P, initial: Credit) -> Self
    where
        P: TrFlowCtrlPolicy,
    {
        let denominator = policy.critical_denominator().max(1u32);
        let critical_ = initial / denominator;
        // 变动量门限（非临界区防抖）由策略给出；缺省实现取「一档临界容量」，因此
        // 非临界区每积累大约 1/N 容量的变动就提醒一次，与临界区粒度同量级。
        let min_advance_ = policy.min_advance_between_reports(initial);
        ReportThresholds_ {
            min_advance_,
            critical_,
        }
    }
}
