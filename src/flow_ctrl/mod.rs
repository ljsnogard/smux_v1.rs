//! # 流控（flow control）
//!
//! 本模块实现**与 IO 无关**的字节窗口算法，供 [`crate::connection`] 的中心循环与子流
//! 两侧调用。它不碰任何缓冲、网络或异步原语，因此可以单独测试，也可以被将来别的
//! 复用协议复用。
//!
//! ## 为什么需要它
//!
//! 复用层把若干条子流复用到同一条字节流上。若发送方可以无限制地把某条子流的
//! 数据推入网络，而接收方来不及把它交给应用，就会出现**一条慢子流拖垮整条
//! 连接**（head-of-line blocking 的内存版本）。因此每条子流的**每个方向**都
//! 各自维护一个窗口：
//!
//! - **接收窗口**（[`RecvWindow`]）：本端承诺还能接收多少字节。它只由两个事件
//!   驱动——「对端又发来 N 字节」与「应用又消费了 N 字节」；后者让窗口可以重新变大，
//!   并由连接层择机通告给对端。
//! - **发送窗口**（[`SendWindow`]）：本端还能向对端发送多少字节。它由两个事件驱动
//!   ——「本端又发了 N 字节」与「收到对端的窗口通告」。
//!
//! 两个窗口是**独立**的：一条子流的 `Tx` 与 `Rx` 可以分别阻塞，互不影响，也不
//! 影响别的子流。
//!
//! ## 方案：绝对量快照 `(R, W)` + 阈值通告
//!
//! 1. 子流建立时两端**互相通告**接收窗口：主动方在 `OPEN` 里带上，被动方在自己那条
//!    `OPEN` 里带上（建流三步见 [`crate::connection`] 模块文档 §4.2）。窗口以对端
//!    通告的值为准；本端实际能收多少仍由本端接收环的容量决定。
//! 2. 发送方在可用额度内切分数据帧；窗口用尽即**阻塞该子流**（不是丢弃、不是报错）。
//! 3. 通告是**绝对量快照** `(R, W)`：`R` 是通告发出时的累计已收字节数，`W` 是当时的
//!    剩余窗口。发送方据此算 `可用 = W − (已发 − R)`，从而精确扣掉在途数据；只发 `W`
//!    会把在途量重复计入（推导见 [`WindowReport`]）。
//! 4. 通告**什么时候发**由阈值 + 频率限制决定（[`RecvWindow::should_report`]）：
//!    建流时一次（通告最大窗口），此后**收缩**跌破 `1/2` / `1/4` / `0`、**扩张**升过
//!    `1/2` / `3/4` / 满时各发一次；两次通告之间至少隔若干个数据帧，避免窗口在阈值
//!    附近抖动时反复发同一条事件。因为通告是快照，**延迟通告不会导致越权**。
//! 5. 绝对上限由 [`TrFlowCtrlPolicy::max_window`] 钳制，防止两端来回加码导致窗口
//!    无限增长。
//!
//! ## 与帧的关系
//!
//! 通告作为 `OPEN` / `PULSE` / `WINDOW_UPDATE` 的字段出现（线格式上是
//! `RecvTotal` + `RecvWindow` 两个字段，见 `connection::frame_`）。本模块只负责窗口
//! 记账与「该不该发」的判断，不关心它被编进哪个帧、什么时候真正写出去。
//!
//! ## 违例处理
//!
//! 对端在**已通告**额度之外继续发数据属于**协议违例**，由
//! [`FlowCtrlError::PeerViolation`] 表达，调用方应当终止该子流（并可按需终止整条
//! 连接）。本端自己算错、溢出等属于内部错误，用 [`FlowCtrlError::Overflow`] 表达。

/// 以**字节**为单位的信用量（窗口大小）。
///
/// 窗口计数用 `u32`：单条子流的窗口不可能超过 [`TrFlowCtrlPolicy::max_window`]
/// 的量级，而 `u32` 足以覆盖
/// [`crate::handshake::opts::BasicOpts::DEFAULT`] 中的 `max_packet_size`。
/// 与 `usize` 的换算在调用方完成：环容量是 `usize`，通告到线格式时收窄为
/// `u32`，收窄失败按 [`FlowCtrlError::Overflow`] 处理。
pub type Credit = u32;

/// **累计**字节数（已经收到 / 已经发出去的总量）。
///
/// 通告必须携带「窗口对应的累计已收字节数」，否则发送方无法扣掉在途数据——推导
/// 见 [`WindowReport`]。累计量单调不减，用 `u64` 以免在长寿命子流上回绕。
pub type RecvTotal = u64;

/// 通告水位的档位数（收缩侧与扩张侧各一档）。
pub const K_REPORT_LEVEL_COUNT: usize = 3;

/// 流控策略：把「缓冲能力」翻译成窗口参数与通告时机。
///
/// 策略由调用方注入（见 [`crate::connection`] 的分配器 / 策略注入约定），
/// 因此同一份流控算法可以配不同的内存预算与通告频率。缺省实现见 [`DefaultPolicy`]。
pub trait TrFlowCtrlPolicy {
    /// 由接收环容量推导**初始（也是最大）接收窗口**。
    ///
    /// 建流时 `OPEN` 通告的就是它；缺省策略取「环容量」本身：本端最多愿意缓存
    /// 这么多字节，正好等于缓冲能力。推荐实现里这个值应当**截断到 `Credit`**，
    /// 因为它同时决定后续通告的上界。
    fn initial_window(&self, ring_capacity: usize) -> Credit;

    /// **收缩方向**的通告水位（相对初始窗口而言的绝对值），按任意顺序返回。
    ///
    /// 本端接收窗口 `W` 跌破其中任一水位（含恰好落在水位上）、且该水位严格低于
    /// 「上次通告值」时发一次通告。水位列里带 `0` 是为了表达「窗口降到 0」这个
    /// 触发点。缺省为 `[1/2, 1/4, 0]`。
    fn shrink_levels(&self, initial: Credit) -> [Credit; K_REPORT_LEVEL_COUNT];

    /// **扩张方向**的通告水位，语义与 [`TrFlowCtrlPolicy::shrink_levels`] 对称。
    ///
    /// 缺省为 `[1/2, 3/4, 初始值]`。
    fn expand_levels(&self, initial: Credit) -> [Credit; K_REPORT_LEVEL_COUNT];

    /// 两次通告之间**至少**要经过多少个数据帧；`0` 表示不做频率限制。
    ///
    /// 目的是避免窗口在阈值附近来回抖动时反复发同一条事件。因为通告携带的是
    /// 「截至某累计已收字节数的窗口」快照，**延迟通告不会导致越权**（发送方按上一次
    /// 通告算出的额度本身就受限于那次的真实窗口），所以这里可以放心限制频率。
    fn min_frames_between_reports(&self) -> usize;

    /// 窗口的**绝对上限**。
    ///
    /// 发送窗口在收到对端通告时不得超过它（对端同样受本端实现约束，这里做一次
    /// 防御性钳制）。
    fn max_window(&self) -> Credit;

    /// 本端通告里累计已收量的 **epoch 规格**：累计量涨到它之前就要重置一次。
    ///
    /// 累计量在线格式上允许 2 / 4 / 8 字节（见
    /// [`crate::connection::FieldId::RecvTotal`]）。选 2 字节的 epoch
    /// （`u16::MAX`）让控制帧最紧凑、但重置最频繁；选 8 字节
    /// （`u64::MAX`）实际上永不重置。重置本身通过带
    /// [`crate::connection::flags::K_TOTAL_RESET`] 的通告宣告，携带**重置前**的
    /// 累计量，对端据此 rebase。
    fn recv_total_epoch(&self) -> RecvTotal;
}

/// 缺省流控策略；各项取值的推导见 [`TrFlowCtrlPolicy`] 的方法文档。
///
/// # Examples
///
/// ```
/// use smux_v1::flow_ctrl::DefaultPolicy;
///
/// // 策略是 ZST：不含状态，按值传递即可。
/// let policy = DefaultPolicy::new();
/// let _copy = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultPolicy;

impl DefaultPolicy {
    /// 构造缺省策略。
    pub const fn new() -> Self {
        DefaultPolicy
    }

    /// 缺省的最小通告间隔（数据帧数）。
    pub const K_MIN_FRAMES_BETWEEN_REPORTS: usize = 4;
}

impl TrFlowCtrlPolicy for DefaultPolicy {
    fn initial_window(&self, ring_capacity: usize) -> Credit {
        // 环容量是 `usize`、窗口是 `u32`：超出即**饱和**为上界，不做截断（截断会
        // 得到一个错误的小窗口）。
        ring_capacity.min(Credit::MAX as usize) as Credit
    }

    fn shrink_levels(&self, initial: Credit) -> [Credit; K_REPORT_LEVEL_COUNT] {
        [initial / 2u32, initial / 4u32, 0u32]
    }

    fn expand_levels(&self, initial: Credit) -> [Credit; K_REPORT_LEVEL_COUNT] {
        [initial / 2u32, initial / 4u32 * 3u32, initial]
    }

    fn min_frames_between_reports(&self) -> usize {
        Self::K_MIN_FRAMES_BETWEEN_REPORTS
    }

    fn max_window(&self) -> Credit {
        // 取 `u32::MAX` 的一半：远大于任何真实环容量，又留出余量。
        Credit::MAX / 2u32
    }

    fn recv_total_epoch(&self) -> RecvTotal {
        // 4 字节规格：`R` 在 `u32` 范围内都不必重置，同时字段很少需要用到 8 字节。
        u32::MAX as RecvTotal
    }
}

/// 一份**窗口通告**：截至累计已收 `recv_total` 字节时，本端还能再收 `window` 字节。
///
/// # 为什么必须带上「累计已收字节数」
///
/// 设本端容量 `cap`、累计已消费 `C`、累计已收 `R`，则本端物理剩余
///
/// ```text
/// W = cap − (R − C)
/// ```
///
/// 对端在途字节数是 `已发 S − 已收 R`。对端**正确的可用额度**应为
///
/// ```text
/// 可用 = cap + C − S = W − (S − R)
/// ```
///
/// 也就是说：只发 `W` 时，对端只能取「可用 = W」，会把已经在途、本端还没计入 `R`
/// 的那 `S − R` 个字节重复算一遍，于是越权发送、接收环可能溢出。带上 `R` 才能精确
/// 扣掉在途量——这也是 TCP 的窗口通告必须和 ACK 一起发的原因。
///
/// # Examples
///
/// ```
/// use smux_v1::flow_ctrl::WindowReport;
///
/// let report = WindowReport::new(1024, 4096);
/// assert_eq!(report.recv_total(), 1024);
/// assert_eq!(report.window(), 4096);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowReport {
    recv_total_: RecvTotal,
    window_: Credit,
    reset_: bool,
}

impl WindowReport {
    /// 以「累计已收字节数 + 当前接收窗口」构造（普通通告）；构造方是**接收侧**。
    pub const fn new(recv_total: RecvTotal, window: Credit) -> Self {
        WindowReport {
            recv_total_: recv_total,
            window_: window,
            reset_: false,
        }
    }

    /// 构造**重置变体**：`pre_reset_total` 是重置**之前**的累计已收量（绝对总量），
    /// 收到它的一方据此 rebase，其后的通告里累计量从 `0` 重新计数。
    ///
    /// 本端在累计量即将超出 [`TrFlowCtrlPolicy::recv_total_epoch`] 时发它。
    pub const fn new_reset(pre_reset_total: RecvTotal, window: Credit) -> Self {
        WindowReport {
            recv_total_: pre_reset_total,
            window_: window,
            reset_: true,
        }
    }

    /// 本通告对应的累计已收字节数（`R`）。
    pub const fn recv_total(&self) -> RecvTotal {
        self.recv_total_
    }

    /// 截至上述累计已收字节数时，本端还能再收的字节数（`W`）。
    pub const fn window(&self) -> Credit {
        self.window_
    }

    /// 是否为重置变体：是则 [`WindowReport::recv_total`] 是**重置前**的绝对累计量。
    pub const fn is_reset(&self) -> bool {
        self.reset_
    }
}

/// 发送窗口：本端在子流的某个方向上**还能发送**多少字节。
///
/// 只暴露「查询剩余」「预扣」「归还」「接收通告」四类操作，窗口的加减全部集中
/// 在这里，避免散落在帧调度代码里。
pub struct SendWindow {
    /// 累计**已发送**字节数（`S`）。
    sent_: RecvTotal,

    /// 最近一次收到的通告折算成**绝对**累计已收量后的快照 `(R₀, W₀)`；`None` 表示
    /// 尚未收到（对端的 `OPEN` 还没到）。
    reported_: Option<(RecvTotal, Credit)>,

    /// 对端累计量的当前 epoch 起点（绝对总量）：对端每次宣告重置时更新。
    peer_epoch_base_: RecvTotal,

    /// 上限，用于对通告做防御性钳制。
    max_: Credit,
}

impl SendWindow {
    /// 以「对端窗口上限」构造；**初始可用额度为 0**，要等收到对端那条 `OPEN`
    /// 携带的通告才生效——建流三步保证数据帧不会早于它。
    pub(crate) const fn new_(max: Credit) -> Self {
        SendWindow {
            sent_: 0u64,
            reported_: Option::None,
            peer_epoch_base_: 0u64,
            max_: max,
        }
    }

    /// 剩余可发送字节数：`W₀ − (S − R₀)`，下钳 `0`。
    pub fn available(&self) -> Credit {
        let Option::Some((r0, w0)) = self.reported_ else {
            return 0u32;
        };
        let inflight = self.sent_.saturating_sub(r0);
        // 在途量理论上不会超过一个窗口（`u32` 量级）；超出即视为额度耗尽。
        let inflight = Credit::try_from(inflight).unwrap_or(Credit::MAX);
        w0.saturating_sub(inflight)
    }

    /// 窗口是否已经用尽（发送方应当阻塞该子流，而不是报错）。
    pub fn is_exhausted(&self) -> bool {
        self.available() == 0u32
    }

    /// 预扣 `want` 字节，返回本次**实际获批**的字节数（`0..=want`）。
    ///
    /// 调用方据此决定本次帧调度能取多少数据；返回 0 表示窗口用尽。
    /// 获批的字节在真正写出前就已经从窗口扣除，写失败时用
    /// [`SendWindow::refund`] 归还。
    pub fn reserve(&mut self, want: Credit) -> Credit {
        let granted = want.min(self.available());
        self.sent_ = self.sent_.saturating_add(granted as u64);
        granted
    }

    /// 归还之前 [`SendWindow::reserve`] 预扣、但最终未写出的额度。
    pub fn refund(&mut self, amount: Credit) {
        self.sent_ = self.sent_.saturating_sub(amount as u64);
    }

    /// 收到对端的窗口通告。
    ///
    /// 重置变体（[`WindowReport::is_reset`]）会先把对端的 epoch 起点推进到它携带的
    /// 「重置前累计量」，因此两种变体都能折算成同一个**绝对**累计已收量再比较。
    /// 过期或重复的通告（折算后的 `R` 不比已记录的更大）被忽略，因此**幂等**：保活
    /// `PULSE` 可以放心地重复携带同一份快照。
    ///
    /// # Errors
    ///
    /// 通告的窗口超过本端上限时返回 [`FlowCtrlError::Overflow`]（本端实现防御，
    /// 正常对端不会触发）。
    pub fn on_report(&mut self, report: WindowReport) -> Result<(), FlowCtrlError> {
        // 折算成绝对累计已收量：重置变体携带的就是「重置前」的绝对总量。
        let reset = report.is_reset();
        let absolute = if reset {
            report.recv_total()
        } else {
            self.peer_epoch_base_.saturating_add(report.recv_total())
        };

        if let Option::Some((r0, _)) = self.reported_
            && absolute <= r0
        {
            // 旧快照：不覆盖、不推进 epoch 起点，也不报错。
            return Result::Ok(());
        }
        if report.window() > self.max_ {
            return Result::Err(FlowCtrlError::Overflow);
        }
        if reset {
            // epoch 起点只在通告被接受后推进。
            self.peer_epoch_base_ = report.recv_total();
        }
        self.reported_ = Option::Some((absolute, report.window()));
        Result::Ok(())
    }

    /// 对端累计量的当前 epoch 起点（绝对总量；诊断用）。
    pub const fn peer_epoch_base(&self) -> RecvTotal {
        self.peer_epoch_base_
    }
}

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

/// 一条子流**双向**流控状态的聚合。///
/// 子流对象持有它，`Tx` 侧只访问 [`FlowCtrl::send_window`]，`Rx` 侧只访问
/// [`FlowCtrl::recv_window`]，两个方向互不干扰。
pub struct FlowCtrl {
    send_: SendWindow,
    recv_: RecvWindow,
}

impl FlowCtrl {
    /// 按策略与接收环容量为一条子流建立双向窗口。
    ///
    /// 接收侧的初始窗口由 `OPEN` 通告出去；发送侧**此时还没有额度**，要等收到对端
    /// 那条 `OPEN` 里的通告（[`WindowReport`]）才生效。
    pub fn new<P>(policy: &P, ring_capacity: usize) -> Self
    where
        P: TrFlowCtrlPolicy,
    {
        let max = policy.max_window();
        FlowCtrl {
            send_: SendWindow::new_(max),
            recv_: RecvWindow::new_(policy, ring_capacity),
        }
    }

    /// 发送窗口（`Tx` 侧使用）。
    pub const fn send_window(&self) -> &SendWindow {
        &self.send_
    }

    /// 发送窗口（`Tx` 侧使用，可变）。
    pub fn send_window_mut(&mut self) -> &mut SendWindow {
        &mut self.send_
    }

    /// 接收窗口（`Rx` 侧使用）。
    pub const fn recv_window(&self) -> &RecvWindow {
        &self.recv_
    }

    /// 接收窗口（`Rx` 侧使用，可变）。
    pub fn recv_window_mut(&mut self) -> &mut RecvWindow {
        &mut self.recv_
    }
}

/// 流控失败。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowCtrlError {
    /// 对端发送的数据超过了本端**已通告**的接收窗口。
    ///
    /// 属于协议违例：调用方应当终止该子流，并可按需终止整条连接。
    PeerViolation,

    /// 窗口计数溢出，或窗口 / 容量无法收窄为 [`Credit`]。
    ///
    /// 属于本端内部或配置错误，正常运行的协议不会触发。
    Overflow,
}

impl core::fmt::Display for FlowCtrlError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            FlowCtrlError::PeerViolation => f.write_str("对端发送数据超过已通告的接收窗口"),
            FlowCtrlError::Overflow => f.write_str("流控窗口计数溢出"),
        }
    }
}

impl core::error::Error for FlowCtrlError {}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 缺省策略：初始窗口跟随环容量（超出 `u32` 时饱和），水位为 1/2、1/4、0 与
    /// 1/2、3/4、满，频率限制为 4 个数据帧。
    /// - 手段：对容量 0 / 4096 / `usize::MAX` 求初窗，并对初窗 4096 求两组水位。
    /// - 判断：初窗分别饱和为 0 / 4096 / `u32::MAX`；水位与频率限制与设计一致。
    #[test]
    fn default_policy_window_and_levels() {
        let p = DefaultPolicy::new();
        assert_eq!(p.initial_window(0usize), 0u32);
        assert_eq!(p.initial_window(4096usize), 4096u32);
        assert_eq!(p.initial_window(usize::MAX), Credit::MAX);

        assert_eq!(p.shrink_levels(4096u32), [2048u32, 1024u32, 0u32]);
        assert_eq!(p.expand_levels(4096u32), [2048u32, 3072u32, 4096u32]);
        assert_eq!(p.min_frames_between_reports(), 4usize);
        assert_eq!(p.max_window(), Credit::MAX / 2u32);
    }

    /// 测试「建流时先通告一次」以及收缩 / 扩张两侧的阈值触发点。
    /// - 手段：初窗 4096；先 `report()` 一次（模拟 `OPEN` 通告）。随后按
    ///   「收 4 个数据帧」为一批推进窗口，逐档检查 `should_report`：
    ///   5696 - 2400 = 1696（跌破 1/2）→ 再收 800 → 896（跌破 1/4）→ 再收到 0；
    ///   然后应用消费使窗口升过 1/2、超过 3/4、回到满。
    /// - 判断：每次跨过水位时 `should_report` 为真；未跨水位（如从 4096 只降到
    ///   3000）时为假；`report()` 之后再次判断为假（已通告同一值）。
    #[test]
    fn recv_window_reports_on_threshold_crossings() {
        let p = DefaultPolicy::new();
        let mut w = RecvWindow::new_(&p, 4096usize);
        assert!(w.should_report(&p), "还没通告过：必须先通告一次");
        let first = w.report();
        assert_eq!(first.recv_total(), 0u64);
        assert_eq!(first.window(), 4096u32, "开场通告的是最大接收窗口");
        assert!(!w.should_report(&p), "刚通告过同一值：不该重复发");

        // 只降到 3000（未跨 1/2=2048）：不触发。
        for _ in 0..4 {
            w.on_data(274u32).expect("在额度内");
        }
        assert_eq!(w.window(), 4096u32 - 4u32 * 274u32);
        assert!(!w.should_report(&p), "没有跨过任何水位");

        // 继续降到 1696（跌破 1/2）：触发。
        for _ in 0..4 {
            w.on_data(326u32).expect("在额度内");
        }
        assert_eq!(w.window(), 1696u32);
        assert!(w.should_report(&p), "跌破 1/2 应当通告");
        w.report();

        // 降到 896（跌破 1/4=1024）：触发。
        for _ in 0..4 {
            w.on_data(200u32).expect("在额度内");
        }
        assert_eq!(w.window(), 896u32);
        assert!(w.should_report(&p), "跌破 1/4 应当通告");
        w.report();

        // 降到 0：触发（水位列里的 0 专门表达这个点）。
        for _ in 0..4 {
            w.on_data(100u32).expect("在额度内");
        }
        assert_eq!(w.window(), 496u32);
        for _ in 0..4 {
            w.on_data(124u32).expect("在额度内");
        }
        assert_eq!(w.window(), 0u32);
        assert!(w.should_report(&p), "窗口降到 0 应当通告");
        w.report();

        // 扩张：消费 2100 → 窗口 2100（升过 1/2=2048）：触发。
        for _ in 0..4 {
            w.on_data(0u32).expect("零字节也算一个数据帧");
        }
        w.on_consumed(2100u32);
        assert_eq!(w.window(), 2100u32);
        assert!(w.should_report(&p), "升过 1/2 应当通告");
        w.report();

        // 扩张到 3200（超过 3/4=3072）：触发。
        for _ in 0..4 {
            w.on_data(0u32).expect("零字节也算一个数据帧");
        }
        w.on_consumed(1100u32);
        assert_eq!(w.window(), 3200u32);
        assert!(w.should_report(&p), "升过 3/4 应当通告");
        w.report();

        // 回到满：触发。
        for _ in 0..4 {
            w.on_data(0u32).expect("零字节也算一个数据帧");
        }
        w.on_consumed(896u32);
        assert_eq!(w.window(), 4096u32);
        assert!(w.should_report(&p), "回到满应当通告");
    }

    /// 测试频率限制：跨过水位但数据帧数不够时不发，攒够帧数后补发。
    /// - 手段：初窗 4096，先 `report()`；随后 3 个数据帧就把窗口压到 1/2 以下。
    /// - 判断：第 3 帧后 `should_report` 仍为假（未达 4 帧）；第 4 帧后为真。
    #[test]
    fn recv_window_rate_limits_reports() {
        let p = DefaultPolicy::new();
        let mut w = RecvWindow::new_(&p, 4096usize);
        w.report();

        w.on_data(1400u32).expect("在额度内");
        assert_eq!(w.window(), 2696u32, "还没跌破 1/2");

        w.on_data(1400u32).expect("在额度内");
        assert!(w.window() < 2048u32, "已经跌破 1/2");
        assert!(!w.should_report(&p), "不足最小帧数：先攒着");

        w.on_data(0u32).expect("第 3 帧");
        assert!(!w.should_report(&p), "第 3 帧仍不够");

        w.on_data(0u32).expect("第 4 帧");
        assert!(
            w.should_report(&p),
            "攒够帧数后应当补发（水位条件一直成立）"
        );
    }

    /// 测试「未通告」与「超出已通告额度」都判为对端违例。
    /// - 手段：新建接收窗口（尚未通告）时 `on_data(1)`；`report()` 出 `(0, 4096)`
    ///   后再 `on_data(4097)`。
    /// - 判断：两次都返回 [`FlowCtrlError::PeerViolation`]，且已收字节数不前进。
    #[test]
    fn recv_window_flags_peer_violation() {
        let p = DefaultPolicy::new();
        let mut w = RecvWindow::new_(&p, 4096usize);

        assert_eq!(
            w.on_data(1u32),
            Result::Err(FlowCtrlError::PeerViolation),
            "还没通告过窗口，对端不该发数据"
        );
        assert_eq!(w.recv_total(), 0u64);

        let report = w.report();
        assert_eq!(report.window(), 4096u32);
        assert!(w.on_data(4096u32).is_ok(), "刚好用满已通告额度是合法的");
        assert_eq!(w.recv_total(), 4096u64);
        assert_eq!(
            w.on_data(1u32),
            Result::Err(FlowCtrlError::PeerViolation)
        );
        assert_eq!(w.recv_total(), 4096u64, "违例不应推进已收计数");
    }

    /// 测试发送窗口按 `可用 = W₀ − (S − R₀)` 计算，并忽略过期通告。
    /// - 手段：初窗 100（`(0, 100)`）→ 预扣 30 → 收到新通告 `(30, 50)` → 再喂一份
    ///   过期的 `(10, 999)`。
    /// - 判断：预扣后可用 70；新通告后可用 50（在途被精确扣掉）；过期通告不生效。
    #[test]
    fn send_window_subtracts_inflight_and_ignores_stale_reports() {
        let mut w = SendWindow::new_(1024u32);
        assert_eq!(w.available(), 0u32, "未收到对端 OPEN 通告前没有额度");
        assert!(w.is_exhausted());

        w.on_report(WindowReport::new(0u64, 100u32))
            .expect("通告在上限内");
        assert_eq!(w.available(), 100u32);

        assert_eq!(w.reserve(30u32), 30u32);
        assert_eq!(w.available(), 70u32, "已发送 30 字节");

        w.on_report(WindowReport::new(30u64, 50u32))
            .expect("新通告");
        assert_eq!(w.available(), 50u32, "在途为 30−30=0，可用即通告值");

        w.on_report(WindowReport::new(10u64, 999u32))
            .expect("过期通告不是错误");
        assert_eq!(w.available(), 50u32, "过期通告不覆盖较新的快照");
    }

    /// 测试发送窗口的预扣、部分获批、用尽与归还。
    /// - 手段：通告 `(0, 10)` 后依次 `reserve(4)`、`reserve(100)`、`refund(3)`。
    /// - 判断：两次获批分别为 4 与 6；用尽后 `is_exhausted` 为真；归还后可用 3。
    #[test]
    fn send_window_reserve_exhaust_and_refund() {
        let mut w = SendWindow::new_(1024u32);
        w.on_report(WindowReport::new(0u64, 10u32)).expect("通告");
        assert_eq!(w.reserve(4u32), 4u32);
        assert_eq!(w.available(), 6u32);
        assert!(!w.is_exhausted());
        assert_eq!(w.reserve(100u32), 6u32);
        assert_eq!(w.available(), 0u32);
        assert!(w.is_exhausted());
        w.refund(3u32);
        assert_eq!(w.available(), 3u32);
    }

    /// 测试对端通告超过本端上限时判为溢出。
    /// - 手段：上限收窄到 12，先接受 `(0, 12)`，再喂 `(1, 13)`。
    /// - 判断：第二次返回 [`FlowCtrlError::Overflow`]，且原快照仍然生效。
    #[test]
    fn send_window_rejects_report_beyond_cap() {
        let mut w = SendWindow::new_(12u32);
        w.on_report(WindowReport::new(0u64, 12u32)).expect("刚好到上限");
        assert_eq!(w.available(), 12u32);
        assert_eq!(
            w.on_report(WindowReport::new(1u64, 13u32)),
            Result::Err(FlowCtrlError::Overflow)
        );
        assert_eq!(w.available(), 12u32, "被拒的通告不改状态");
    }

    /// 测试 [`FlowCtrl::new`] 同时接好收发两个方向：接收侧立即可通告初窗，发送侧在
    /// 收到对端通告前没有额度。
    /// - 手段：以环容量 100 使用缺省策略构造，读取两个方向；随后喂一份通告。
    /// - 判断：接收容量与窗口都是 100、发送可用为 0；喂 `(0, 40)` 后发送可用 40；
    ///   接收方向收满 100 后再收 1 字节返回 `PeerViolation`。
    #[test]
    fn flow_ctrl_new_wires_both_directions() {
        let p = DefaultPolicy::new();
        let mut ctrl = FlowCtrl::new(&p, 100usize);
        assert_eq!(ctrl.recv_window().capacity(), 100u32);
        assert_eq!(ctrl.recv_window().window(), 100u32);
        assert_eq!(ctrl.send_window().available(), 0u32);

        ctrl.send_window_mut()
            .on_report(WindowReport::new(0u64, 40u32))
            .expect("通告");
        assert_eq!(ctrl.send_window().available(), 40u32);

        ctrl.recv_window_mut().report();
        assert!(ctrl.recv_window_mut().on_data(100u32).is_ok());
        assert_eq!(
            ctrl.recv_window_mut().on_data(1u32),
            Result::Err(FlowCtrlError::PeerViolation)
        );
    }

    /// 测试 `report()` 是幂等的快照：重复调用取到同一个值，且会清零帧计数。
    /// - 手段：初窗 4096 上连续 `report()` 两次，并在中间读 `reported_window`。
    /// - 判断：两次快照都是 `(0, 4096)`；`reported_window` 为 `Some(4096)`；
    ///   第一次 `report` 后的 `should_report` 为假。
    #[test]
    fn report_is_idempotent_snapshot() {
        let p = DefaultPolicy::new();
        let mut w = RecvWindow::new_(&p, 4096usize);
        let a = w.report();
        let b = w.report();
        assert_eq!(a, b);
        assert_eq!(w.reported_window(), Option::Some(4096u32));
        assert_eq!(w.recv_total(), 0u64);
        assert!(!w.should_report(&p));
    }
}

#[cfg(test)]
mod reset_tests_ {
    use super::*;

    /// 2 字节 epoch 的策略：累计量涨到 `u16::MAX` 就重置，用于验证重置路径。
    struct TinyEpochPolicy;

    impl TrFlowCtrlPolicy for TinyEpochPolicy {
        fn initial_window(&self, ring_capacity: usize) -> Credit {
            ring_capacity.min(Credit::MAX as usize) as Credit
        }

        fn shrink_levels(&self, initial: Credit) -> [Credit; K_REPORT_LEVEL_COUNT] {
            [initial / 2u32, initial / 4u32, 0u32]
        }

        fn expand_levels(&self, initial: Credit) -> [Credit; K_REPORT_LEVEL_COUNT] {
            [initial / 2u32, initial / 4u32 * 3u32, initial]
        }

        fn min_frames_between_reports(&self) -> usize {
            4usize
        }

        fn max_window(&self) -> Credit {
            Credit::MAX / 2u32
        }

        fn recv_total_epoch(&self) -> RecvTotal {
            8u64
        }
    }

    /// 测试累计量涨到 epoch 规格时产出重置变体，且携带的是**重置前**的绝对量。
    /// - 手段：用 2 字节 epoch 策略（阈值为 8）建立接收窗口，通告一次后收 9 字节。
    /// - 判断：`reset_due` 为真；`should_report` 为真（不受频率限制）；`report()`
    ///   返回重置变体，其 `recv_total()` 是重置前的绝对量 9；重置后
    ///   `encoded_recv_total()` 归零，而绝对量仍是 9。
    #[test]
    fn recv_window_reset_carries_pre_reset_total() {
        let p = TinyEpochPolicy;
        let mut w = RecvWindow::new_(&p, 4096usize);
        assert!(!w.reset_due());
        w.report();

        for _ in 0..4 {
            w.on_data(3u32).expect("在额度内");
        }
        assert_eq!(w.recv_total(), 12u64);
        assert!(w.reset_due(), "已经超过 epoch 规格 8");

        // 频率限制已满足，但即使没满足，重置也必须能发出去。
        let report = w.report();
        assert!(report.is_reset(), "应当产出重置变体");
        assert_eq!(report.recv_total(), 12u64, "携带重置前的绝对累计量");
        assert_eq!(report.window(), 4096u32 - 12u32);

        assert_eq!(w.recv_total(), 12u64, "绝对量继续单调");
        assert_eq!(w.encoded_recv_total(), 0u64, "新 epoch 从 0 起算");
        assert!(!w.reset_due());

        // 之后的普通通告携带的是 epoch 内的小值。
        for _ in 0..4 {
            w.on_data(1u32).expect("在额度内");
        }
        let next = w.report();
        assert!(!next.is_reset());
        assert_eq!(next.recv_total(), 4u64, "epoch 内累计量");
    }

    /// 测试重置不受频率限制影响（频率限制只约束阈值通告）。
    /// - 手段：2 字节 epoch 策略下通告一次，收 1 个数据帧就把累计量推过规格。
    /// - 判断：帧数远小于 `min_frames_between_reports` 时 `should_report` 仍为真，
    ///   且 `report()` 产出重置变体。
    #[test]
    fn reset_bypasses_report_rate_limit() {
        let p = TinyEpochPolicy;
        let mut w = RecvWindow::new_(&p, 64usize);
        w.report();

        w.on_data(9u32).expect("在额度内");
        assert_eq!(w.recv_total(), 9u64);
        assert!(
            w.should_report(&p),
            "重置是编码前提，不受最小帧间隔约束"
        );
        assert!(w.report().is_reset());
    }

    /// 测试发送窗口在收到重置变体后按「重置前累计量」rebase。
    /// - 手段：接通告 `(0, 100)`、预扣 30；再收重置变体 `(30, 50)`；随后预扣 10
    ///   并收普通通告 `(5, 40)`（epoch 内 5 → 绝对 35）。
    /// - 判断：重置后可用 = 50（在途 0）；普通通告后可用 = 40 − (40 − 35) = 35。
    #[test]
    fn send_window_rebases_on_reset_report() {
        let mut w = SendWindow::new_(1024u32);
        w.on_report(WindowReport::new(0u64, 100u32)).expect("通告");
        assert_eq!(w.reserve(30u32), 30u32);
        assert_eq!(w.available(), 70u32);

        w.on_report(WindowReport::new_reset(30u64, 50u32))
            .expect("重置变体");
        assert_eq!(w.peer_epoch_base(), 30u64);
        assert_eq!(w.available(), 50u32, "在途为 30 − 30 = 0");

        assert_eq!(w.reserve(10u32), 10u32);
        assert_eq!(w.available(), 40u32);
        w.on_report(WindowReport::new(5u64, 40u32))
            .expect("epoch 内的普通通告");
        assert_eq!(
            w.available(),
            35u32,
            "绝对已收 = 30 + 5 = 35，在途 = 40 − 35 = 5"
        );

        // 重置变体之后，比它更旧的重置快照被忽略。
        w.on_report(WindowReport::new_reset(10u64, 999u32))
            .expect("更旧的重置快照不是错误");
        assert_eq!(w.peer_epoch_base(), 30u64, "epoch 起点不回退");
    }
}
