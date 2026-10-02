use crate::flow_ctrl::{Credit, FlowCtrlError, RecvTotal, TrFlowCtrlPolicy, WindowReport};

/// 通告水位的档位数（收缩侧与扩张侧各一档）。
pub const K_REPORT_LEVEL_COUNT: usize = 3;


/// 接收窗口：本端**还愿意接收**多少字节，以及通告所需的状态。
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

    /// 初始窗口；阈值水位按它计算。
    initial_: Credit,

    /// 累计已收字节数（`R`）。
    received_: RecvTotal,

    /// 累计已消费（交给应用）字节数（`C`）。
    consumed_: RecvTotal,

    /// 最近一次通告的快照 `(R₀, W₀)`；`None` 表示还没通告过。
    reported_: Option<(RecvTotal, Credit)>,

    /// 自上次通告以来收到的数据帧数（频率限制用）。
    frames_since_report_: usize,

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
        RecvWindow {
            capacity_: initial,
            initial_: initial,
            received_: 0u64,
            consumed_: 0u64,
            reported_: Option::None,
            frames_since_report_: 0usize,
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
        self.frames_since_report_ = self.frames_since_report_.saturating_add(1);
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
        Result::Ok(())
    }

    /// 应用又消费（取走）了 `amount` 字节，窗口相应变大。
    pub fn on_consumed(&mut self, amount: Credit) {
        self.consumed_ = self.consumed_.saturating_add(amount as u64);
    }

    /// 是否应当按策略通告当前窗口。
    ///
    /// 判据：**上次通告值**与当前值之间跨过了某个方向对应的水位
    /// （收缩看 [`TrFlowCtrlPolicy::shrink_levels`]，扩张看
    /// [`TrFlowCtrlPolicy::expand_levels`]），**且**自上次通告以来的数据帧数已达
    /// [`TrFlowCtrlPolicy::min_frames_between_reports`]。从未通告过时总是为真
    /// （建流时的那次通告）。
    ///
    /// 这里只做判断、不改状态；真正通告用 [`RecvWindow::report`]。
    pub fn should_report<P>(&self, policy: &P) -> bool
    where
        P: TrFlowCtrlPolicy,
    {
        self.should_report_with_(&ReportThresholds_::new_(policy, self.initial_))
    }

    /// 与 [`RecvWindow::should_report`] 同判据，但用**预先展开好的阈值快照**。
    ///
    /// 读写循环不持有调用方的 `C`/`P`，因此在连接建立时把策略展开成
    /// [`ReportThresholds_`] 交给它们即可（见 `connection/session_.rs`）。
    pub(crate) fn should_report_with_(&self, thresholds: &ReportThresholds_) -> bool {
        let Option::Some((_, last)) = self.reported_ else {
            return true;
        };
        // 重置是**编码前提**（窄规格要放不下了），不受频率限制约束。
        if self.reset_due() {
            return true;
        }
        if self.frames_since_report_ < thresholds.min_frames_ {
            return false;
        }
        let current = self.window();
        if current < last {
            return thresholds
                .shrink_
                .iter()
                .any(|level| current <= *level && *level < last);
        }
        if current > last {
            return thresholds
                .expand_
                .iter()
                .any(|level| current >= *level && *level > last);
        }
        false
    }

    /// 生成一份通告快照并把它记为「已通告」（此后越权判定以它为准、帧计数清零）。
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
        self.frames_since_report_ = 0usize;
        report
    }
}


/// 通告判定所需的策略快照。
///
/// 读写循环运行在 `abs_art` spawn 出来的任务里（`'static`），既拿不到
/// [`TrMuxConfig`](crate::connection::TrMuxConfig) 也借不到 `TrFlowCtrlPolicy`；
/// 因此在连接建立时把「是否该通告」用到的三个量**展开一次**，此后判定只用它。
/// 三个量都只依赖策略与初始窗口，而初始窗口由环容量唯一确定，所以整条连接共享
/// 一份即可。
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReportThresholds_ {
    /// 两次通告之间至少经过的数据帧数。
    min_frames_: usize,

    /// 收缩水位（跌破即通告）。
    shrink_: [Credit; K_REPORT_LEVEL_COUNT],

    /// 扩张水位（升过即通告）。
    expand_: [Credit; K_REPORT_LEVEL_COUNT],
}


impl ReportThresholds_ {
    /// 从策略与初始窗口展开。
    pub(crate) fn new_<P>(policy: &P, initial: Credit) -> Self
    where
        P: TrFlowCtrlPolicy,
    {
        ReportThresholds_ {
            min_frames_: policy.min_frames_between_reports(),
            shrink_: policy.shrink_levels(initial),
            expand_: policy.expand_levels(initial),
        }
    }
}
