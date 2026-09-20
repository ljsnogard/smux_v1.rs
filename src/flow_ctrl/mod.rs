//! # 流控（flow control）
//!
//! 本模块实现**与 IO 无关**的字节信用窗口算法，供 [`crate::connection`] 的读写
//! 会话与子流两侧调用。它不碰任何缓冲、网络或异步原语，因此可以单独测试，
//! 也可以被将来别的复用协议复用。
//!
//! ## 为什么需要它
//!
//! 复用层把若干条子流复用到同一条字节流上。若发送方可以无限制地把某条子流的
//! 数据推入网络，而接收方来不及把它交给应用，就会出现**一条慢子流拖垮整条
//! 连接**（head-of-line blocking 的内存版本）。因此每条子流的**每个方向**都
//! 各自维护一个窗口：
//!
//! - **接收窗口**（[`RecvWindow`]）：本端承诺还能接收多少字节。它只由两个事件
//!   驱动——「对端又发来 N 字节」与「应用又消费了 N 字节」；后者让窗口可以
//!   重新变大，并通过 [`WindowUpdate`] 公告给对端。
//! - **发送窗口**（[`SendWindow`]）：本端还能向对端发送多少字节。它只由两个
//!   事件驱动——「本端又发了 N 字节」与「收到对端的窗口更新 N 字节」。
//!
//! 两个窗口是**独立**的：一条子流的 `Tx` 与 `Rx` 可以分别阻塞，互不影响，也
//! 不影响别的子流。这正是「各业务逻辑各持自己的 session」在数据面上的体现。
//!
//! ## 方案：字节信用 + 阈值回补
//!
//! 采用**字节信用窗口**（QUIC / HTTP-2 `MAX_STREAM_DATA` 风格），而非 TCP 式
//! 累积确认：
//!
//! 1. 子流建立时，两端**互相通告**自己的接收窗口：主动方在 `OPEN` 里带上，被动方在
//!    自己那条 `OPEN` 里带上（建流三步见 [`crate::connection`] 模块文档 §4.2）。
//!    窗口因此以对端通告的值为准，不再假定「两端用同一条规则算出相同初窗」；
//!    本端实际能收多少，仍由本端接收环的容量决定。
//! 2. 发送方在可用额度内切分数据帧；窗口用尽即**阻塞该子流**（不是丢弃、不是报错）。
//! 3. 接收方每消费一定字节就又有可公告的窗口，公告的**触发时机**由连接层掌握
//!    （应用读过接收环后经上行通知通道提示，见 [`crate::connection`] 模块文档 §2.1），
//!    因此天然合批，控制帧数量不会随消费次数线性增长。
//! 4. 公告**绝对值化**（`OPEN` / `PULSE` / `WINDOW_UPDATE` 共用一个字段：见
//!    [`crate::connection::FieldId::RecvWindow`]）已经定稿在 `dev-notes` §11.2；
//!    本模块当前仍是**增量**形态（[`WindowUpdate`] 表示增量），会随 §11.6 第 3 步
//!    一起改。绝对上限由 [`TrFlowCtrlPolicy::max_window`] 钳制，防止两端来回加码
//!    导致窗口无限增长。
//!
//! 之所以不用累积确认：复用层没有重传语义（底层字节流已经可靠有序），
//! 只需要「背压 + 回补」，累积序号反而要额外维护序号空间与乱序处理。
//!
//! ## 与帧的关系
//!
//! [`WindowUpdate`] 是窗口公告的**当前（增量）**形态，最终会被绝对值形态取代
//! （见上一条）；线格式见 `connection::frame_` 的 [`FieldId::RecvWindow`]。
//! 本模块只负责窗口记账，不关心公告被编进哪个帧、什么时候真正写出去。
//!
//! [`FieldId::RecvWindow`]: crate::connection::FieldId::RecvWindow
//!
//! ## 违例处理
//!
//! 对端在窗口之外继续发数据属于**协议违例**，由 [`FlowCtrlError::PeerViolation`]
//! 表达，调用方应当终止该子流（并可按需终止整条连接）。本端自己算错、
//! 溢出等属于内部错误，用 [`FlowCtrlError::Overflow`] 表达。

/// 以**字节**为单位的信用量。
///
/// 窗口计数统一用 `u32`：单条子流的窗口不可能超过
/// [`TrFlowCtrlPolicy::max_window`] 的量级，而 `u32` 足以覆盖
/// [`crate::handshake::opts::BasicOpts::DEFAULT`] 中的 `max_packet_size`。
/// 与 `usize` 的换算在调用方完成：环容量是 `usize`，公告到线格式时收窄为
/// `u32`，收窄失败按 [`FlowCtrlError::Overflow`] 处理。
pub type Credit = u32;

/// 流控策略：把「缓冲能力」翻译成窗口参数。
///
/// 策略由调用方注入（见 [`crate::connection`] 的分配器 / 策略注入约定），
/// 因此同一份流控算法可以配不同的内存预算。缺省实现见 [`DefaultPolicy`]。
pub trait TrFlowCtrlPolicy {
    /// 由接收环容量推导**初始窗口**。
    ///
    /// 缺省策略取「环容量」本身：本端最多愿意缓存这么多字节，正好等于缓冲
    /// 能力，既不会浪费内存，也不会让对端过早阻塞。返回值必须是正数。
    fn initial_window(&self, ring_capacity: usize) -> Credit;

    /// 已消费字节攒到多少时回发一次窗口更新。
    ///
    /// 缺省策略取当前窗口的一半：太小会让控制帧过密，太大则回补滞后、
    /// 发送方可能出现不必要的空档。
    fn update_threshold(&self, window: Credit) -> Credit;

    /// 窗口的**绝对上限**。
    ///
    /// 接收窗口在回补时不得超过它；发送窗口在收到对端的窗口更新时也不得
    /// 超过它（对端同样受本端实现约束，这里做一次防御性钳制）。
    fn max_window(&self) -> Credit;
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
}

impl TrFlowCtrlPolicy for DefaultPolicy {
    fn initial_window(&self, ring_capacity: usize) -> Credit {
        // 环容量是 `usize`、窗口是 `u32`：超出即**饱和**为上界，不做截断（截断会
        // 得到一个错误的小窗口）。
        ring_capacity.min(Credit::MAX as usize) as Credit
    }

    fn update_threshold(&self, window: Credit) -> Credit {
        // 半窗，但不小于 1：阈值为 0 会让每次消费都产生一个控制帧。
        (window / 2u32).max(1u32)
    }

    fn max_window(&self) -> Credit {
        // 取 `u32::MAX` 的一半：远大于任何真实环容量，又给增量累加留出余量，
        // 使 `available_ + delta` 不会轻易溢出。
        Credit::MAX / 2u32
    }
}

/// 待公告的**接收窗口增量**。
///
/// 作为 `WINDOW_UPDATE` 控制帧的载荷发送；发送之后由
/// [`RecvWindow::take_update`] 取走并清空。增量为 0 时**不应**产生更新，
/// 因此本类型只在增量非 0 时被构造。
///
/// # Examples
///
/// ```
/// use smux_v1::flow_ctrl::WindowUpdate;
///
/// let update = WindowUpdate::new(1024u32);
/// assert_eq!(update.delta(), 1024u32);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowUpdate {
    delta_: Credit,
}

impl WindowUpdate {
    /// 以增量构造。
    pub const fn new(delta: Credit) -> Self {
        WindowUpdate { delta_: delta }
    }

    /// 本更新公告的字节数。
    pub const fn delta(&self) -> Credit {
        self.delta_
    }
}

/// 发送窗口：本端在子流某个方向上**还能发送**的字节数。
///
/// 只暴露「查询剩余」「预扣」「回补」三类操作，窗口的加减全部集中在这里，
/// 避免散落在帧调度代码里。
pub struct SendWindow {
    /// 当前剩余可发送字节数。
    available_: Credit,

    /// 上限，用于对窗口更新做防御性钳制。
    max_: Credit,
}

impl SendWindow {
    /// 以初始信用与上限构造；只供 `connection` 建立子流时使用。
    pub(crate) const fn new_(initial: Credit, max: Credit) -> Self {
        SendWindow {
            available_: initial,
            max_: max,
        }
    }

    /// 剩余可发送字节数。
    pub const fn available(&self) -> Credit {
        self.available_
    }

    /// 窗口是否已经用尽（发送方应当阻塞该子流，而不是报错）。
    pub const fn is_exhausted(&self) -> bool {
        self.available_ == 0u32
    }

    /// 预扣 `want` 字节，返回本次**实际获批**的字节数（`0..=want`）。
    ///
    /// 调用方据此决定本次帧调度能取多少数据；返回 0 表示窗口用尽。
    /// 获批的字节在真正写出前就已经从窗口扣除，写失败时用
    /// [`SendWindow::refund`] 归还。
    pub fn reserve(&mut self, want: Credit) -> Credit {
        let granted = want.min(self.available_);
        self.available_ -= granted;
        granted
    }

    /// 归还之前 [`SendWindow::reserve`] 预扣、但最终未写出的额度。
    pub fn refund(&mut self, amount: Credit) {
        self.available_ = self.available_.saturating_add(amount).min(self.max_);
    }

    /// 收到对端的窗口更新，把发送窗口调大。
    ///
    /// # Errors
    ///
    /// 增量导致窗口超过上限时返回 [`FlowCtrlError::Overflow`]（本端实现防御，
    /// 正常对端不会触发）。
    pub fn on_update(&mut self, update: WindowUpdate) -> Result<(), FlowCtrlError> {
        let grown = self
            .available_
            .checked_add(update.delta())
            .ok_or(FlowCtrlError::Overflow)?;
        if grown > self.max_ {
            return Result::Err(FlowCtrlError::Overflow);
        }
        self.available_ = grown;
        Result::Ok(())
    }
}

/// 接收窗口：本端**还愿意接收**的字节数，以及「已消费、待公告」的累计量。
///
/// 它同时承担两个职责：
///
/// 1. 作为**入向配额**：读路径每收到 N 字节就 [`RecvWindow::on_data`]，
///    越过配额即对端违例；
/// 2. 作为**回补来源**：应用每消费 N 字节就 [`RecvWindow::on_consumed`]，
///    累计量越过阈值后由 [`RecvWindow::take_update`] 产出一次公告。
pub struct RecvWindow {
    /// 本端承诺的最大可接收字节数（初始窗口）。
    capacity_: Credit,

    /// 回补阈值。
    threshold_: Credit,

    /// 窗口上限。
    max_: Credit,

    /// 当前**尚未用掉**的接收额度：对端还可以再发送这么多字节。
    available_: Credit,

    /// 应用已消费、但尚未公告的字节数（回补来源）。
    consumed_pending_: Credit,
}

impl RecvWindow {
    /// 以策略算出的参数构造；只供 `connection` 建立子流时使用。
    pub(crate) const fn new_(capacity: Credit, threshold: Credit, max: Credit) -> Self {
        RecvWindow {
            capacity_: capacity,
            threshold_: threshold,
            max_: max,
            available_: capacity,
            consumed_pending_: 0u32,
        }
    }

    /// 本端承诺的最大可接收字节数。
    pub const fn capacity(&self) -> Credit {
        self.capacity_
    }

    /// 当前还允许对端发送的字节数（仅用于诊断与测试，不作为唯一判据）。
    pub const fn available(&self) -> Credit {
        self.available_
    }

    /// 对端又发来 `amount` 字节，计入在途。
    ///
    /// # Errors
    ///
    /// 超过已公告额度时返回 [`FlowCtrlError::PeerViolation`]。
    pub fn on_data(&mut self, amount: Credit) -> Result<(), FlowCtrlError> {
        // 额度不足即对端发超了本端公告的窗口——协议违例，不是「丢弃即可」。
        let rest = self
            .available_
            .checked_sub(amount)
            .ok_or(FlowCtrlError::PeerViolation)?;
        self.available_ = rest;
        Result::Ok(())
    }

    /// 应用又消费（取走）了 `amount` 字节，窗口相应可以回补。
    pub fn on_consumed(&mut self, amount: Credit) {
        // 消费量只会把额度还回来；累计量以 `max_` 封顶，避免长期不公告时无意义地增长。
        self.consumed_pending_ = self.consumed_pending_.saturating_add(amount).min(self.max_);
    }

    /// 若已攒够阈值则产出待公告的窗口更新，否则返回 `None`。
    ///
    /// 取走后累计量清零；调用方应当把产出的更新交给写路径编进
    /// `WINDOW_UPDATE` 控制帧。
    pub fn take_update(&mut self) -> Option<WindowUpdate> {
        if !self.has_pending_update() {
            return Option::None;
        }
        // 增量不能把窗口推过上限；没公告完的部分留到下一次。
        let room = self.max_.saturating_sub(self.available_);
        let delta = self.consumed_pending_.min(room);
        if delta == 0u32 {
            // 窗口已经在上限：本次没有可公告的空间。必须清空累计量，否则
            // `has_pending_update` 会永远为真。
            self.consumed_pending_ = 0u32;
            return Option::None;
        }
        self.consumed_pending_ -= delta;
        self.available_ = self.available_.saturating_add(delta);
        Option::Some(WindowUpdate::new(delta))
    }

    /// 是否已经攒够阈值、应当公告窗口更新。
    pub const fn has_pending_update(&self) -> bool {
        // 攒够阈值就公告；此外**额度耗尽**时必须公告，哪怕不足阈值——否则应用只
        // 消费了少量字节、对端却因为额度为 0 永久停发，形成死锁。
        self.consumed_pending_ > 0u32
            && (self.consumed_pending_ >= self.threshold_ || self.available_ == 0u32)
    }
}

/// 一条子流**双向**流控状态的聚合。
///
/// 子流对象持有它，`Tx` 侧只访问 [`FlowCtrl::send_window`]，`Rx` 侧只访问
/// [`FlowCtrl::recv_window`]，两个方向互不干扰。
pub struct FlowCtrl {
    send_: SendWindow,
    recv_: RecvWindow,
}

impl FlowCtrl {
    /// 按策略与接收环容量为一条子流建立双向窗口。
    ///
    /// `ring_capacity` 是**本端接收环**的容量，初始窗口由
    /// [`TrFlowCtrlPolicy::initial_window`] 从它推导；发送窗口的初始值取同一条
    /// 规则（两端规则一致，因此不必在帧里交换窗口大小）。
    pub fn new<P>(policy: &P, ring_capacity: usize) -> Self
    where
        P: TrFlowCtrlPolicy,
    {
        let initial = policy.initial_window(ring_capacity);
        // 上限必须不低于初窗，否则初窗一建出来就越界。
        let max = policy.max_window().max(initial);
        let threshold = policy.update_threshold(initial).max(1u32);
        FlowCtrl {
            send_: SendWindow::new_(initial, max),
            recv_: RecvWindow::new_(initial, threshold, max),
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
    /// 对端发送的数据超过了本端公告的接收窗口。
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
            FlowCtrlError::PeerViolation => f.write_str("对端发送数据超过已公告的接收窗口"),
            FlowCtrlError::Overflow => f.write_str("流控窗口计数溢出"),
        }
    }
}

impl core::error::Error for FlowCtrlError {}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试缺省策略把环容量直接翻译成初始窗口，并在超出 `u32` 时饱和。
    /// - 手段：对容量 0、4096 与 `usize::MAX` 调用
    ///   [`DefaultPolicy::initial_window`]。
    /// - 判断：小于 `u32::MAX` 的容量原样返回；`usize::MAX` 饱和为 `u32::MAX`
    ///   （不截断成错误的小窗口）。
    #[test]
    fn default_policy_initial_window_tracks_capacity() {
        let p = DefaultPolicy::new();
        assert_eq!(p.initial_window(0usize), 0u32);
        assert_eq!(p.initial_window(4096usize), 4096u32);
        assert_eq!(p.initial_window(usize::MAX), Credit::MAX);
    }

    /// 测试缺省策略的阈值与上限取值。
    /// - 手段：对 0 / 1 / 8 / 4096 求阈值，并读取上限。
    /// - 判断：阈值为「半窗且不小于 1」，上限恰好是 `u32::MAX / 2`。
    #[test]
    fn default_policy_threshold_and_cap() {
        let p = DefaultPolicy::new();
        assert_eq!(p.update_threshold(0u32), 1u32);
        assert_eq!(p.update_threshold(1u32), 1u32);
        assert_eq!(p.update_threshold(8u32), 4u32);
        assert_eq!(p.update_threshold(4096u32), 2048u32);
        assert_eq!(p.max_window(), Credit::MAX / 2u32);
    }

    /// 测试发送窗口的预扣、部分获批、用尽与归还。
    /// - 手段：初窗 10，先 [`SendWindow::reserve`]`(4)`，再 `reserve(100)`，
    ///   最后 `refund(3)`。
    /// - 判断：两次获批分别为 4 与 6；用尽后 [`SendWindow::is_exhausted`] 为真；
    ///   归还后可用量为 3。
    #[test]
    fn send_window_reserve_exhaust_and_refund() {
        let mut w = SendWindow::new_(10u32, 1024u32);
        assert_eq!(w.reserve(4u32), 4u32);
        assert_eq!(w.available(), 6u32);
        assert!(!w.is_exhausted());
        assert_eq!(w.reserve(100u32), 6u32);
        assert_eq!(w.available(), 0u32);
        assert!(w.is_exhausted());
        w.refund(3u32);
        assert_eq!(w.available(), 3u32);
    }

    /// 测试发送窗口拒绝会把窗口推过上限的更新，也拒绝溢出。
    /// - 手段：初窗 10、上限 12；依次 [`SendWindow::on_update`] `+1`、`+5`、
    ///   `u32::MAX`。
    /// - 判断：`+1` 成功且可用量 11；`+5` 与 `u32::MAX` 都返回
    ///   `Err(FlowCtrlError::Overflow)`，且可用量保持 11。
    #[test]
    fn send_window_rejects_update_beyond_cap() {
        let mut w = SendWindow::new_(10u32, 12u32);
        assert!(w.on_update(WindowUpdate::new(1u32)).is_ok());
        assert_eq!(w.available(), 11u32);
        assert_eq!(
            w.on_update(WindowUpdate::new(5u32)),
            Result::Err(FlowCtrlError::Overflow)
        );
        assert_eq!(
            w.on_update(WindowUpdate::new(Credit::MAX)),
            Result::Err(FlowCtrlError::Overflow)
        );
        assert_eq!(w.available(), 11u32);
    }

    /// 测试接收窗口把超额数据判为对端违例。
    /// - 手段：容量 8 的接收窗口先 [`RecvWindow::on_data`]`(8)`（刚好用尽），
    ///   再 `on_data(1)`。
    /// - 判断：第一次 `Ok` 且可用量 0；第二次返回
    ///   `Err(FlowCtrlError::PeerViolation)`，且可用量保持 0（不产生负额度）。
    #[test]
    fn recv_window_flags_peer_violation() {
        let mut w = RecvWindow::new_(8u32, 4u32, 64u32);
        assert!(w.on_data(8u32).is_ok());
        assert_eq!(w.available(), 0u32);
        assert_eq!(
            w.on_data(1u32),
            Result::Err(FlowCtrlError::PeerViolation)
        );
        assert_eq!(w.available(), 0u32);
    }

    /// 测试接收窗口在消费攒够阈值时产出一次窗口更新并恢复额度。
    /// - 手段：容量 8、阈值 4；收满 8 后消费 4，再 [`RecvWindow::take_update`]。
    /// - 判断：未消费时无更新；消费后产出增量 4 的更新，可用量回到 4，且再次
    ///   调用返回 `None`（累计量已清空）。
    #[test]
    fn recv_window_emits_update_at_threshold() {
        let mut w = RecvWindow::new_(8u32, 4u32, 64u32);
        assert!(w.on_data(8u32).is_ok());
        assert!(!w.has_pending_update());
        w.on_consumed(4u32);
        assert!(w.has_pending_update());
        let update = w.take_update().expect("攒够阈值应当产出窗口更新");
        assert_eq!(update.delta(), 4u32);
        assert_eq!(w.available(), 4u32);
        assert!(!w.has_pending_update());
        assert!(w.take_update().is_none());
    }

    /// 测试额度耗尽时即使不足阈值也必须公告，避免对端永久停发。
    /// - 手段：容量 8、阈值 8（直接构造）；收满 8 后只消费 1。
    /// - 判断：[`RecvWindow::has_pending_update`] 为真，`take_update` 返回增量 1，
    ///   可用量回到 1——证明「额度为 0」本身就会触发公告。
    #[test]
    fn recv_window_emits_when_exhausted_below_threshold() {
        let mut w = RecvWindow::new_(8u32, 8u32, 64u32);
        assert!(w.on_data(8u32).is_ok());
        assert!(!w.has_pending_update());
        w.on_consumed(1u32);
        assert!(w.has_pending_update());
        assert_eq!(w.take_update().map(|u| u.delta()), Option::Some(1u32));
        assert_eq!(w.available(), 1u32);
        assert!(!w.has_pending_update());
    }

    /// 测试 [`FlowCtrl::new`] 同时接好收发两个方向。
    /// - 手段：以环容量 100 使用缺省策略构造，然后读取两个方向并在接收方向收满。
    /// - 判断：发送窗口可用量与接收窗口容量都是 100；收满 100 后再收 1 字节返回
    ///   `Err(FlowCtrlError::PeerViolation)`——说明接收方向也接上了。
    #[test]
    fn flow_ctrl_new_wires_both_directions() {
        let mut ctrl = FlowCtrl::new(&DefaultPolicy::new(), 100usize);
        assert_eq!(ctrl.send_window().available(), 100u32);
        assert_eq!(ctrl.recv_window().capacity(), 100u32);
        assert!(ctrl.recv_window_mut().on_data(100u32).is_ok());
        assert_eq!(
            ctrl.recv_window_mut().on_data(1u32),
            Result::Err(FlowCtrlError::PeerViolation)
        );
    }
}
