use crate::flow_ctrl::{Credit, FlowCtrlError, RecvTotal, WindowReport};

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

        if let Option::Some((r0, w0)) = self.reported_
            && (absolute < r0 || (absolute == r0 && report.window() <= w0))
        {
            // 过期或重复的快照：不覆盖、不推进 epoch 起点，也不报错。
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
