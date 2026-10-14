use crate::flow_ctrl::{Credit, RecvTotal};

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
    /// 本端在累计量即将超出 [`TrFlowCtrlPolicy::recv_total_epoch`](crate::flow_ctrl::TrFlowCtrlPolicy::recv_total_epoch) 时发它。
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
