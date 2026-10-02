use crate::flow_ctrl::{Credit, K_REPORT_LEVEL_COUNT, RecvTotal};

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
