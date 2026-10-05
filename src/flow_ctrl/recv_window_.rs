use core::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

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
///
/// # 原子化（2026-10 改造）
///
/// 字段改为原子、方法改取 `&self`，好让本类型能放进一个**可克隆的共享节点**里。
/// 并发上比发送侧更简单：**生产路径上本类型只有解复用循环一个任务访问**
/// （建流期由应用线程 `install_` 写一次，早于任何帧）；没有任何字段需要跨任务打包，
/// 逐字段原子即足够。
pub struct RecvWindow {
    /// 接收窗口容量（由策略从环容量推导，同时也是本端通告的上界）。
    capacity_: AtomicU32,

    /// 累计已收字节数（`R`）。
    received_: AtomicU64,

    /// 累计已消费（交给应用）字节数（`C`）。
    consumed_: AtomicU64,

    /// 最近一次通告的快照 `R₀`；有效性与 `reported_window_` 一起由
    /// `reported_valid_` 决定。
    reported_: AtomicU64,

    /// 最近一次通告的快照 `W₀`。
    reported_window_: AtomicU32,

    /// 是否已经通告过（`reported_` / `reported_window_` 是否有效）。
    reported_valid_: AtomicBool,

    /// 自上次提醒以来累计的**变动量**（字节）：收到多少 + 消费多少。
    ///
    /// 只在**非临界区**的频率门限里用到。量字节而不量帧数，是为了让「哪一侧在动」都
    /// 能推进它（理由见 [`TrFlowCtrlPolicy::min_advance_between_reports`]）。
    activity_since_report_: AtomicU64,

    /// 判定分区所需的策略快照（`install_` 时展开）。
    ///
    /// 存下来是为了让**不持有策略**的入口（[`RecvWindow::should_report`] /
    /// [`RecvWindow::report`]）也能自己判断，不必把调用方的 `C`/`P` 带进读写循环。
    critical_: AtomicU32,

    /// 非临界区两次提醒之间至少要积累的变动量（字节）。
    min_advance_: AtomicU32,

    /// 当前 epoch 的起点（绝对累计量）：每次重置后推进到当时的累计已收量。
    epoch_base_: AtomicU64,

    /// 本端累计量的 epoch 规格：涨到它就重置。
    epoch_limit_: AtomicU64,
}

impl RecvWindow {
    /// 建一条**尚未安装**的接收窗口：容量 0、还没有通告。
    pub(crate) const fn new_empty_() -> Self {
        RecvWindow {
            capacity_: AtomicU32::new(0u32),
            received_: AtomicU64::new(0u64),
            consumed_: AtomicU64::new(0u64),
            reported_: AtomicU64::new(0u64),
            reported_window_: AtomicU32::new(0u32),
            reported_valid_: AtomicBool::new(false),
            activity_since_report_: AtomicU64::new(0u64),
            critical_: AtomicU32::new(0u32),
            min_advance_: AtomicU32::new(0u32),
            epoch_base_: AtomicU64::new(0u64),
            epoch_limit_: AtomicU64::new(u64::MAX),
        }
    }

    /// 以策略算出的参数安装容量与阈值。
    pub(crate) fn install_<P>(&self, policy: &P, ring_capacity: usize)
    where
        P: TrFlowCtrlPolicy,
    {
        let initial = policy.initial_window(ring_capacity);
        let thresholds = ReportThresholds_::new_(policy, initial);
        self.capacity_.store(initial, Ordering::Release);
        self.critical_.store(thresholds.critical_, Ordering::Release);
        self.min_advance_
            .store(thresholds.min_advance_, Ordering::Release);
        self.epoch_limit_
            .store(policy.recv_total_epoch(), Ordering::Release);
    }

    #[cfg(test)]
    /// 以策略算出的参数构造。
    pub(crate) fn new_<P>(policy: &P, ring_capacity: usize) -> Self
    where
        P: TrFlowCtrlPolicy,
    {
        let window = Self::new_empty_();
        window.install_(policy, ring_capacity);
        window
    }

    /// 本端承诺的最大可接收字节数（初始窗口 = `OPEN` 通告的值）。
    pub fn capacity(&self) -> Credit {
        self.capacity_.load(Ordering::Acquire)
    }

    /// 当前可通告的接收窗口 `W`（物理剩余）。
    pub fn window(&self) -> Credit {
        let buffered = self
            .received_
            .load(Ordering::Acquire)
            .saturating_sub(self.consumed_.load(Ordering::Acquire));
        let buffered = Credit::try_from(buffered).unwrap_or(Credit::MAX);
        self.capacity().saturating_sub(buffered)
    }

    /// **绝对**累计已收字节数（自子流建立以来的总量）。
    pub fn recv_total(&self) -> RecvTotal {
        self.received_.load(Ordering::Acquire)
    }

    /// 当前 epoch 内、将要写进线格式的累计已收量（= 绝对量 − epoch 起点）。
    pub fn encoded_recv_total(&self) -> RecvTotal {
        self.recv_total()
            .saturating_sub(self.epoch_base_.load(Ordering::Acquire))
    }

    /// 累计量是否已经涨到当前 epoch 规格，必须在下次通告里宣告重置。
    pub fn reset_due(&self) -> bool {
        self.encoded_recv_total() >= self.epoch_limit_.load(Ordering::Acquire)
    }

    /// 累计已消费字节数（`C`）。
    pub fn consumed_total(&self) -> RecvTotal {
        self.consumed_.load(Ordering::Acquire)
    }

    /// 最近一次通告出去的窗口（诊断用）。
    pub fn reported_window(&self) -> Option<Credit> {
        if self.reported_valid_.load(Ordering::Acquire) {
            Option::Some(self.reported_window_.load(Ordering::Acquire))
        } else {
            Option::None
        }
    }

    /// 对端又发来 `amount` 字节（一次调用 = 一个数据帧），计入在途并推进帧计数。
    ///
    /// # Errors
    ///
    /// 超过**已通告**额度（`R₀ + W₀`）时返回 [`FlowCtrlError::PeerViolation`]——
    /// 这是协议违例，不是「丢弃即可」。
    pub fn on_data(&self, amount: Credit) -> Result<(), FlowCtrlError> {
        let authorized = if self.reported_valid_.load(Ordering::Acquire) {
            self.reported_
                .load(Ordering::Acquire)
                .saturating_add(self.reported_window_.load(Ordering::Acquire) as u64)
        } else {
            // 还没通告过任何窗口：对端本来就不该发数据。
            0u64
        };
        let next = self
            .received_
            .load(Ordering::Acquire)
            .saturating_add(amount as u64);
        if next > authorized {
            return Result::Err(FlowCtrlError::PeerViolation);
        }
        self.received_.store(next, Ordering::Release);
        // 只有**被接受的**数据才算变动量：违例帧（上面已返回）不算，否则对端可以靠
        // 灌违例帧把门限刷开。零字节帧自然也不贡献变动量。
        self.activity_since_report_
            .fetch_add(amount as u64, Ordering::AcqRel);
        Result::Ok(())
    }

    /// 应用又消费（取走）了 `amount` 字节，窗口相应变大。
    pub fn on_consumed(&self, amount: Credit) {
        self.consumed_.fetch_add(amount as u64, Ordering::AcqRel);
        // 消费同样是「真实进展」，同样推进非临界区的频率门限（见
        // [`TrFlowCtrlPolicy::min_advance_between_reports`] 的口径说明）。
        self.activity_since_report_
            .fetch_add(amount as u64, Ordering::AcqRel);
    }

    /// 当前剩余容量落在哪个分区（用给定的阈值快照判）。
    pub(crate) fn zone_with_(&self, thresholds: &ReportThresholds_) -> Zone_ {
        if self.window() <= thresholds.critical_ {
            Zone_::Critical
        } else {
            Zone_::Normal
        }
    }

    /// 当前剩余容量落在哪个分区（用安装时存下的阈值判）。
    fn zone_(&self) -> Zone_ {
        self.zone_with_(&self.thresholds_())
    }

    /// 安装时存下的阈值快照（重建为值类型，供分区判定使用）。
    fn thresholds_(&self) -> ReportThresholds_ {
        ReportThresholds_ {
            min_advance_: self.min_advance_.load(Ordering::Acquire),
            critical_: self.critical_.load(Ordering::Acquire),
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
        self.should_report_()
    }

    /// 与 [`RecvWindow::should_report`] 同判据；读写循环不持有策略，因此用它。
    pub(crate) fn should_report_(&self) -> bool {
        if !self.reported_valid_.load(Ordering::Acquire) {
            return true;
        }
        // 重置是**编码前提**（窄规格要放不下了），不受任何门限约束。
        if self.reset_due() {
            return true;
        }
        // 「窗口与上次通告相同」即没有新信息：无论哪个分区都不发。
        //
        // （不需要再比一次「上次所在分区」：分区是当前窗口的函数，窗口相同则分区必然
        // 相同；而窗口不同时，「进入临界区」本身就是一次变化，会被下面第 1 条无条件
        // 放行。）
        if self.window() == self.reported_window_.load(Ordering::Acquire) {
            return false;
        }
        // 1. 临界区：任何变化都提醒（含「刚跌进临界区」那一次）。
        if self.zone_() == Zone_::Critical {
            return true;
        }
        // 2. 非临界区：变化 + 变动量门限（防抖），上不封顶。
        self.activity_since_report_.load(Ordering::Acquire)
            >= self.min_advance_.load(Ordering::Acquire) as u64
    }

    /// 生成一份通告快照并把它记为「已通告」（此后越权判定以它为准、变动量清零）。
    ///
    /// 若累计量已经涨到当前 epoch 规格（[`RecvWindow::reset_due`]），这里会自动产出
    /// **重置变体**：携带重置前的绝对累计量并把 epoch 起点推进到它，其后的通告里
    /// 累计量从 `0` 重新计数。
    ///
    /// 保活 `PULSE` 无条件用它取当前窗口；按阈值通告则由
    /// [`RecvWindow::should_report`] 先判断。
    pub fn report(&self) -> WindowReport {
        let received = self.received_.load(Ordering::Acquire);
        let report = if self.reset_due() {
            let reset = WindowReport::new_reset(received, self.window());
            self.epoch_base_.store(received, Ordering::Release);
            reset
        } else {
            WindowReport::new(
                received.saturating_sub(self.epoch_base_.load(Ordering::Acquire)),
                self.window(),
            )
        };
        // 越权判定始终按**绝对**量记账：通告发出时对端最多能发到「当前已收 + W」。
        self.reported_.store(received, Ordering::Release);
        self.reported_window_
            .store(report.window(), Ordering::Release);
        self.reported_valid_.store(true, Ordering::Release);
        self.activity_since_report_.store(0u64, Ordering::Release);
        report
    }
}

/// 通告判定所需的策略快照。
///
/// 读写循环运行在 `abs_art` spawn 出来的任务里（`'static`），既拿不到
/// [`TrMuxConfig`](crate::connection::TrMuxConfig) 也借不到 `TrFlowCtrlPolicy`；
/// 因此在连接建立时把「是否该通告」用到的量**展开一次**，此后判定只用它。整条连接
/// 共享一份即可（见 `connection/session_.rs`）。
///
/// 本类型是**建流期的值**：`install_` 把它展开进 [`RecvWindow`] 的原子字段，此后判定
/// 不再需要它。用例仍用它构造出与窗口一致的分区阈值做断言。
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReportThresholds_ {
    /// 非临界区两次提醒之间至少要积累的变动量（字节）。
    pub(crate) min_advance_: Credit,

    /// **临界区**的上界：剩余 `≤ critical_` 即进入临界区（任何变化都提醒）。
    pub(crate) critical_: Credit,
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
