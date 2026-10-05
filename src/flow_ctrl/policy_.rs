use crate::flow_ctrl::{Credit, RecvTotal};

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

    /// **临界区**的容量分母：剩余容量不足 `容量 / N` 时进入「临界区」。
    ///
    /// 临界区里**任何变化都提醒**——越接近瘫痪的子流，越要给它改变状态的机会。
    /// 缺省 `N = 4`（即剩余不足 1/4）。
    ///
    /// 本值**只**决定这一个边界。临界区之外没有第二档「剩余够多就不提醒」的上界：
    /// 那种做法会让本端停止更新通告，对端的可用额度永远停在旧快照上（实施后果见
    /// [`crate::flow_ctrl::RecvWindow::should_report`] 的文档）。临界区之外的提醒
    /// 频率一律由 [`TrFlowCtrlPolicy::min_advance_between_reports`] 限制。
    fn critical_denominator(&self) -> Credit;

    /// **临界区之外**两次提醒之间，至少要积累多少字节的**变动量**；`0` 表示不做
    /// 频率限制。
    ///
    /// 临界区之外的子流离瘫痪还远，提醒的价值低，这里用一个变动量下限把频率压住
    /// ——这正是「防抖」该待的地方。临界区不受它约束（那里每变必报）。
    ///
    /// # 为什么量的是**字节**而不是**帧数**
    ///
    /// 曾经量的是「自上次提醒以来收到多少个数据帧」。那个口径有个致命偏斜：**它只能
    /// 靠收到数据帧推进**，因此窗口一旦归零（发送方停摆、一帧也发不出来），或者应用
    /// 只消费而发送方还没跟上时，「变动量」明明在涨、帧计数却纹丝不动，提醒被永久
    /// 压住——双方互等。字节口径对「哪一侧在动」不敏感，任何真实进展都能推进它。
    ///
    /// 缺省实现取「临界区上界的大小」与一个下限中的较大者（见 [`DefaultPolicy`]）；
    /// 这样临界区之外每积累大约一档临界容量（缺省即 1/N 容量）的变动才提醒一次，
    /// 与分区粒度同量级。
    ///
    /// 因为提醒携带的是「截至某累计已收字节数的窗口」快照，**延迟提醒不会导致越权**
    /// （发送方按上一次通告算出的额度本身就受限于那次的真实窗口），所以这里可以放心
    /// 限制频率。
    fn min_advance_between_reports(&self, initial: Credit) -> Credit;

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

    /// 缺省的最小通告间隔的**绝对下限**（字节）。
    pub const K_MIN_ADVANCE_FLOOR: Credit = 16u32;
}


impl TrFlowCtrlPolicy for DefaultPolicy {
    fn initial_window(&self, ring_capacity: usize) -> Credit {
        // 环容量是 `usize`、窗口是 `u32`：超出即**饱和**为上界，不做截断（截断会
        // 得到一个错误的小窗口）。
        ring_capacity.min(Credit::MAX as usize) as Credit
    }

    fn critical_denominator(&self) -> Credit {
        4u32
    }

    fn min_advance_between_reports(&self, initial: Credit) -> Credit {
        // 与临界区同粒度：中间区每积累「一档临界容量」的变动就值得提醒一次。
        // 再压一个绝对下限，避免极小窗口下门限退化成 0（= 不限频）。
        let critical = initial / self.critical_denominator().max(1u32);
        critical.max(Self::K_MIN_ADVANCE_FLOOR)
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
