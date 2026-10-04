use crate::flow_ctrl::{Credit, FlowCtrlError, RecvTotal, TrFlowCtrlPolicy, WindowReport};

/// 提醒的**分区**（越接近瘫痪越密报）。
///
/// 判据一律看**剩余容量**，分区边界由
/// [`TrFlowCtrlPolicy::critical_denominator`] 推出：
///
/// ```text
/// | 区间                        | 行为                     |
/// | --------------------------- | ------------------------ |
/// | 剩余 ≤ 容量/N（临界区）      | 任何变化都提醒            |
/// | 容量/N < 剩余 ≤ 容量×(1−1/N) | 按 min_frames 提醒（中间区）|
/// | 剩余 > 容量×(1−1/N)          | 不提醒                    |
/// ```
///
/// 设计意图：**越接近瘫痪的子流，越要给它改变状态的机会**。1/4 以下每变必报，保证
/// 发送方总能拿到新额度；3/4 以上干脆不产生控制帧，把开销集中在真正需要的子流上。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Zone_ {
    /// 剩余充裕：不提醒。
    Relaxed,

    /// 中间区：按帧数门限提醒。
    Moderate,

    /// 临界区：任何变化都提醒。
    Critical,
}

impl Zone_ {
    /// 是不是**比 `other` 更紧张**的分区（剩余更少）。
    ///
    /// 用于「进入更紧的分区必须立刻报一次」——那是发送方最需要知道的信息，
    /// 不能等帧数门限。
    pub(crate) const fn tighter_than_(self, other: Zone_) -> bool {
        matches!(
            (self, other),
            (Zone_::Critical, Zone_::Moderate | Zone_::Relaxed)
                | (Zone_::Moderate, Zone_::Relaxed)
        )
    }
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
    /// 只在**中间区**的频率门限里用到。量字节而不量帧数，是为了让「哪一侧在动」都
    /// 能推进它（理由见 [`TrFlowCtrlPolicy::min_advance_between_reports`]）。
    activity_since_report_: RecvTotal,

    /// 判定分区所需的策略快照；由 `new_` 展开一次。
    ///
    /// 存下来的唯一理由是让**不持有策略**的 [`RecvWindow::report`] 也能自己算出当前
    /// 分区：否则调用方一旦用了 policy-free 的那个入口，`last_level_` 就会停在旧值上，
    /// 下一次 `should_report` 会把「同一分区」误判成「换了分区」。
    thresholds_: ReportThresholds_,

    /// 最近一次通告（或建流）时所在的分区。
    ///
    /// **分区切换本身就带滞回**：只有「离开当前分区」才值得再看一眼帧数门限，这既
    /// 是实现「3/4 以上不报」的地方，也是「临界区里每变必报」的判据。
    last_level_: Zone_,

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
            last_level_: Zone_::Relaxed,
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
        // 消费同样是「真实进展」，同样推进中间区的频率门限（见
        // [`TrFlowCtrlPolicy::min_advance_between_reports`] 的口径说明）。
        self.activity_since_report_ = self.activity_since_report_.saturating_add(amount as u64);
    }

    /// 当前剩余容量落在哪个分区（用预先展开好的阈值快照判）。
    pub(crate) fn zone_with_(&self, thresholds: &ReportThresholds_) -> Zone_ {
        let free = self.window();
        if free <= thresholds.critical_ {
            Zone_::Critical
        } else if free > thresholds.relaxed_above_ {
            Zone_::Relaxed
        } else {
            Zone_::Moderate
        }
    }

    /// 是否应当发一条提醒。
    ///
    /// 判据是**分区 + 变化**，不再是刻度列：
    ///
    /// 1. **进入更紧的分区**（剩余变少、跨过边界）——无条件发。这是发送方最需要
    ///    知道的信息，不能等帧数门限；
    /// 2. **临界区**（剩余 `≤ 容量/N`）——只要窗口与上次通告不同就发，不看帧数。
    ///    越接近瘫痪，越要保证发送方拿得到新额度；
    /// 3. **充裕区**（剩余 `> 容量×(1−1/N)`）——只有「离开充裕区」（跌进中间区）
    ///    那一次才发；其余一概不发，离瘫痪还远，不必产生控制帧；
    /// 4. **中间区内部的变化**——按
    ///    [`TrFlowCtrlPolicy::min_advance_between_reports`] 限频（防抖）。
    ///
    /// 「窗口与上次通告一模一样」时一律不发（没有新信息可携带）。
    ///
    /// 与旧实现相比少了两个特例：旧实现要给「归零 / 从 0 抬起」单独开免门限的后门，
    /// 否则会死锁。现在归零落在**临界区**（0 ≤ 容量/N 恒成立），临界区本来就每变必
    /// 报，后门因此不再需要——这正是分区机制相对刻度列的价值。
    ///
    /// 这里只做判断、不改状态；真正发提醒用 [`RecvWindow::report_in_zone_`]。
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
        let current = self.window();
        let zone = self.zone_with_(thresholds);
        let unchanged = current == last && zone == self.last_level_;

        // 1. 进入更紧的分区：无条件（这是「越接近瘫痪越早告知」的核心）。
        if zone.tighter_than_(self.last_level_) {
            return true;
        }
        // 2. 临界区：任何变化都提醒。
        if zone == Zone_::Critical {
            return !unchanged;
        }
        // 3. 充裕区：只有「刚离开充裕区」那一次值得发（上面第 1 条已覆盖更紧的分区，
        //    这里补「从充裕区跌到中间区」——它同时也是 `tighter_than_` 的一种，故实际
        //    不会走到；保留分支是为了让「充裕区不发」这条规则显式可见）。
        if zone == Zone_::Relaxed {
            return zone != self.last_level_;
        }
        // 4. 中间区内部：变化 + 变动量门限（防抖）。
        !unchanged && self.activity_since_report_ >= thresholds.min_advance_ as u64
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
        let zone = self.zone_with_(&self.thresholds_);
        self.report_in_zone_(zone)
    }

    /// 与 [`RecvWindow::report`] 相同，但顺带记录**本次通告时的分区**，供下一次
    /// [`RecvWindow::should_report_with_`] 判「是否离开了当前分区」。
    ///
    /// 调用方必须传入「调用 `report()` 之前」用 `zone_with_` 算出的分区——`report()`
    /// 本身会把 `reported_` 推进到当前值，之后就算不出旧分区了。
    pub(crate) fn report_in_zone_(&mut self, zone: Zone_) -> WindowReport {
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
        self.last_level_ = zone;
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
    /// 中间区两次提醒之间至少要积累的变动量（字节）。
    min_advance_: Credit,

    /// **临界区**的上界：剩余 `≤ critical_` 即进入临界区（任何变化都提醒）。
    critical_: Credit,

    /// **充裕区**的下界：剩余 `> relaxed_above_` 即回充裕区（不提醒）。
    relaxed_above_: Credit,
}


impl ReportThresholds_ {
    /// 从策略与初始窗口展开。
    pub(crate) fn new_<P>(policy: &P, initial: Credit) -> Self
    where
        P: TrFlowCtrlPolicy,
    {
        let denominator = policy.critical_denominator().max(1u32);
        let critical_ = initial / denominator;
        // 帧数门限有个下限：小窗口（例如 512 B）在中间区一次也就放行几十~一两百字节，
        // 按「阈值 / 数据帧长」折算出来的帧数小得离谱，门限要么恒真、要么恒假。取下限
        // 之后，门限衡量的仍然是「对端还在持续发数据」。
        let min_advance_ = policy.min_advance_between_reports(initial);
        // 充裕区下界 = 容量 × (1 − 1/N)，由同一个旋钮推出，避免两个参数互相矛盾。
        let relaxed_above_ = initial.saturating_sub(critical_);
        ReportThresholds_ {
            min_advance_,
            critical_,
            relaxed_above_,
        }
    }
}
