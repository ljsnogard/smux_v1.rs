use core::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use crate::flow_ctrl::{Credit, FlowCtrlError, RecvTotal, WindowReport};

/// 发送窗口：本端在子流的某个方向上**还能发送**多少字节。
///
/// 只暴露「查询剩余」「预扣」「归还」「接收通告」四类操作，窗口的加减全部集中
/// 在这里，避免散落在帧调度代码里。
///
/// # 原子化（2026-10 改造）
///
/// 本类型过去是 `&mut self` 的值类型。为了让「每条子流的共享状态」能放进一个**可克隆
/// 的共享节点**里、被复用循环与解复用循环**无锁**访问，字段改为原子、方法改取
/// `&self`。并发模型由字段的**写者归属**决定，不需要任何锁：
///
/// | 字段 | 写者 | 读者 |
/// | --- | --- | --- |
/// | `sent_` | 复用循环（`reserve` / `refund`） | 复用循环（`available`） |
/// | `reported_pair_` | 解复用循环（`on_report`） | **复用循环**（`available`） |
/// | `reported_abs_` | 解复用循环 | 解复用循环 |
/// | `peer_epoch_base_` | 解复用循环 | 解复用循环 |
/// | `max_` | 建流期写一次 | 解复用循环 |
///
/// 于是**唯一一处跨任务的多字段读**是 `available()` 里的 `(r0, w0)` 快照，把它打进
/// 一个 `u64` 就得到一致视图；其余字段各自只有一个写者、且读者与写者同任务。
pub struct SendWindow {
    /// 累计**已发送**字节数（`S`）。
    sent_: AtomicU64,

    /// 最近一次被接受的通告里「`r0` 的低 32 位」与「`w0`」打包成的快照。
    ///
    /// 只有 [`SendWindow::available`] 会读它，而那是与写者**不同**的任务，因此必须
    /// 打包成一个字做原子载入；否则可能读到一个通告的 `r0` 与另一个通告的 `w0`。
    reported_pair_: AtomicU64,

    /// 最近一次被接受的通告折算成**绝对**累计已收量后的快照 `R₀`。
    ///
    /// 只服务接受判据（新旧快照比较），写者与读者都是解复用循环，因此不参与打包。
    reported_abs_: AtomicU64,

    /// `reported_pair_` / `reported_abs_` 是否有效（是否已经收到过通告）。
    reported_valid_: AtomicBool,

    /// 对端累计量的当前 epoch 起点（绝对总量）：对端每次宣告重置时更新。
    peer_epoch_base_: AtomicU64,

    /// 上限，用于对通告做防御性钳制。
    max_: AtomicU32,
}

impl SendWindow {
    /// 建一条**尚未安装**的发送窗口：上限 0、没有任何通告。
    ///
    /// 建流路径先建出共享状态节点，等调用方在最终裁决（`accept_async`）给出环容量
    /// 时再 [`SendWindow::install_`]。
    pub(crate) const fn new_empty_() -> Self {
        SendWindow {
            sent_: AtomicU64::new(0u64),
            reported_pair_: AtomicU64::new(0u64),
            reported_abs_: AtomicU64::new(0u64),
            reported_valid_: AtomicBool::new(false),
            peer_epoch_base_: AtomicU64::new(0u64),
            max_: AtomicU32::new(0u32),
        }
    }

    /// 安装对端窗口上限。
    pub(crate) fn install_(&self, max: Credit) {
        self.max_.store(max, Ordering::Release);
    }

    #[cfg(test)]
    /// 以「对端窗口上限」构造；**初始可用额度为 0**，要等收到对端那条 `OPEN`
    /// 携带的通告才生效——建流三步保证数据帧不会早于它。
    pub(crate) fn new_(max: Credit) -> Self {
        let window = Self::new_empty_();
        window.install_(max);
        window
    }

    /// 剩余可发送字节数：`W₀ − (S − R₀)`，下钳 `0`。
    ///
    /// # 为什么用低 32 位做回绕减法
    ///
    /// 真实在途量 `S − R₀` 不可能超过窗口上限（协议上限是 `u32` 量级，缺省策略取
    /// `u32::MAX / 2`），因此它必然 `< 2³²`。于是把 `S` 与 `R₀` 各自截到低 32 位再
    /// 做模 `2³²` 的减法，得到的正是真实在途量——这样 `(R₀, W₀)` 只需要 64 位即可
    /// 一次原子载入（`R₀` 是 `u64`，但它与 `S` 的差总是 `u32` 量级）。
    pub fn available(&self) -> Credit {
        if !self.reported_valid_.load(Ordering::Acquire) {
            return 0u32;
        }
        let pair = self.reported_pair_.load(Ordering::Acquire);
        let r0_low = (pair >> 32) as u32;
        let w0 = pair as u32;
        let sent_low = self.sent_.load(Ordering::Acquire) as u32;
        w0.saturating_sub(sent_low.wrapping_sub(r0_low))
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
    ///
    /// # 为什么不需要 CAS
    ///
    /// `sent_` 的写者只有复用循环一个。即便并发地有一条新通告到达（解复用循环），
    /// 它也只会让 `available()` **变大或不变**（接受判据本身要求快照严格前进），
    /// 因此「读到旧快照 ⇒ 少批」是安全方向，不会越权。
    pub fn reserve(&self, want: Credit) -> Credit {
        let granted = want.min(self.available());
        if granted > 0u32 {
            self.sent_.fetch_add(granted as u64, Ordering::AcqRel);
        }
        granted
    }

    /// 归还之前 [`SendWindow::reserve`] 预扣、但最终未写出的额度。
    pub fn refund(&self, amount: Credit) {
        if amount == 0u32 {
            return;
        }
        // `sent_` 只有复用循环一个写者（与 `reserve` 同任务），因此一次载入 + 一次
        // 存储即可；不需要 CAS，也不会与 `available()` 的读构成越权方向。
        let sent = self.sent_.load(Ordering::Acquire);
        self.sent_
            .store(sent.saturating_sub(amount as u64), Ordering::Release);
    }

    /// 收到对端的窗口通告。
    ///
    /// 重置变体（[`WindowReport::is_reset`]）会先把对端的 epoch 起点推进到它携带的
    /// 「重置前累计量」，因此两种变体都能折算成同一个**绝对**累计已收量再比较。
    ///
    /// # 接受与忽略的判据（**不是**「`R` 必须变大」）
    ///
    /// 新快照严格优于旧快照的判据是
    ///
    /// ```text
    /// absolute > r0  ||  (absolute == r0 && window > w0)
    /// ```
    ///
    /// 后半条不可省。接收方**消费**数据会让窗口回补，而它此时可能还没有再收到新数据
    /// ——于是通告是「`R` 不变、`W` 变大」。窗口跌到 0 之后的第一条回补通告恰好总是
    /// 这种形状，因此这不是边角情形。若把它当重复通告丢掉，发送方会永远停在
    /// `available() == 0`，而接收方明明空着额度：**这条子流就此死锁**（本仓库
    /// `send_window_` 的 `report_with_same_recv_total_but_larger_window_is_accepted_`
    /// 钉住了这一点）。
    ///
    /// 反过来，`absolute < r0`、或「`R` 与 `W` 都没变」的快照确实是过期 / 重复的，
    /// 忽略它们让本方法保持**幂等**：保活 `PULSE` 可以放心地重复携带同一份快照。
    ///
    /// # 单写者前提
    ///
    /// 本方法由**解复用循环**调用（被动方还有一次出现在最终裁决的应用线程上，按协议
    /// 一定早于对端收到本端 `OPEN` 之后才会发的任何通告）。多字段的读改写因此不设
    /// CAS；协议建流顺序保证不会有两个写者同时进入。
    ///
    /// # Errors
    ///
    /// 通告的窗口超过本端上限时返回 [`FlowCtrlError::Overflow`]（本端实现防御，
    /// 正常对端不会触发）。
    pub fn on_report(&self, report: WindowReport) -> Result<(), FlowCtrlError> {
        // 折算成绝对累计已收量：重置变体携带的就是「重置前」的绝对总量。
        let reset = report.is_reset();
        let absolute = if reset {
            report.recv_total()
        } else {
            self.peer_epoch_base_
                .load(Ordering::Acquire)
                .saturating_add(report.recv_total())
        };

        if self.reported_valid_.load(Ordering::Acquire) {
            let r0 = self.reported_abs_.load(Ordering::Acquire);
            let w0 = self.reported_pair_.load(Ordering::Acquire) as u32;
            if absolute < r0 || (absolute == r0 && report.window() <= w0) {
                // 过期或重复的快照：不覆盖、不推进 epoch 起点，也不报错。
                return Result::Ok(());
            }
        }
        if report.window() > self.max_.load(Ordering::Acquire) {
            return Result::Err(FlowCtrlError::Overflow);
        }
        if reset {
            // epoch 起点只在通告被接受后推进。
            self.peer_epoch_base_
                .store(report.recv_total(), Ordering::Release);
        }
        self.reported_abs_.store(absolute, Ordering::Release);
        self.reported_pair_.store(
            (((absolute as u32) as u64) << 32) | report.window() as u64,
            Ordering::Release,
        );
        self.reported_valid_.store(true, Ordering::Release);
        Result::Ok(())
    }

    /// 对端累计量的当前 epoch 起点（绝对总量；诊断用）。
    pub fn peer_epoch_base(&self) -> RecvTotal {
        self.peer_epoch_base_.load(Ordering::Acquire)
    }
}
