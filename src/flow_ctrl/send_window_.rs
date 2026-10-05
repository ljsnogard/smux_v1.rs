use abs_sync::may_break::TrMayBreak;
use atomic_sync::mutex::preemptive::SpinningMutexOwned;
use atomic_sync::x_deps::abs_sync;

use crate::flow_ctrl::{Credit, FlowCtrlError, RecvTotal, WindowReport};

/// 发送窗口的**内部状态**。
///
/// 全部字段都是普通值：它们只在 [`SendWindow`] 内那把锁的临界区里被访问，因此不需要
/// 原子、也不需要把 `(r0, w0)` 打包成一个字（见 §4.1 的历史：原子版本必须那么做，
/// 因为那时读 `available()` 与写 `on_report` 是两个任务直接读同一份内存）。
struct SendInner {
    /// 累计**已发送**字节数（`S`）。
    sent_: RecvTotal,

    /// 最近一次被接受的通告折算成**绝对**累计已收量后的快照 `(R₀, W₀)`；`None` 表示
    /// 尚未收到（对端的 `OPEN` 还没到）。
    reported_: Option<(RecvTotal, Credit)>,

    /// 对端累计量的当前 epoch 起点（绝对总量）：对端每次宣告重置时更新。
    peer_epoch_base_: RecvTotal,

    /// 上限，用于对通告做防御性钳制。
    max_: Credit,
}

impl SendInner {
    /// 空状态：上限 0、没有任何通告。
    const fn new_() -> Self {
        SendInner {
            sent_: 0u64,
            reported_: Option::None,
            peer_epoch_base_: 0u64,
            max_: 0u32,
        }
    }

    /// 剩余可发送字节数：`W₀ − (S − R₀)`，下钳 `0`。
    fn available_(&self) -> Credit {
        let Option::Some((r0, w0)) = self.reported_ else {
            return 0u32;
        };
        let inflight = self.sent_.saturating_sub(r0);
        // 在途量理论上不会超过一个窗口（`u32` 量级）；超出即视为额度耗尽。
        let inflight = Credit::try_from(inflight).unwrap_or(Credit::MAX);
        w0.saturating_sub(inflight)
    }

    /// 预扣 `want` 字节，返回本次**实际获批**的字节数（`0..=want`）。
    fn reserve_(&mut self, want: Credit) -> Credit {
        let granted = want.min(self.available_());
        self.sent_ = self.sent_.saturating_add(granted as u64);
        granted
    }

    /// 归还预扣但未写出的额度。
    fn refund_(&mut self, amount: Credit) {
        self.sent_ = self.sent_.saturating_sub(amount as u64);
    }

    /// 收到对端的窗口通告（判据与理由见 [`SendWindow::on_report`]）。
    fn on_report_(&mut self, report: WindowReport) -> Result<(), FlowCtrlError> {
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
}

/// 发送窗口：本端在子流的某个方向上**还能发送**多少字节。
///
/// 只暴露「查询剩余」「预扣」「归还」「接收通告」四类操作，窗口的加减全部集中
/// 在这里，避免散落在帧调度代码里。
///
/// # 并发：一把零分配自旋锁，对外全是 `&self`
///
/// 子流的共享状态节点被多个任务经 `Shared` 以 `&self` 持有，因此本类型的方法一律取
/// `&self`；**内部状态 `SendInner` 则整块放进一把
/// [`SpinningMutexOwned`]**：
///
/// - 一次操作 = 一次临界区。`available()` 的读、`reserve()` 的「读剩余 + 扣减」、
///   `on_report()` 的「比较 + 改写 epoch + 覆盖快照」都在同一段临界区里完成，
///   不存在半新半旧的中间态；
/// - 锁是 `preemptive`（自旋）而不是 `cooperative`：后者每条实例内部持一个
///   `Arc<RwCore>`，等于给每条子流添一次**全局**分配，与本仓「分配一律经注入分配器」
///   的纪律冲突；
/// - 两个内侧循环投在同一个本地作用域上（同线程），应用线程只碰状态字、不碰窗口，
///   因此这把锁在生产路径上**不会真的自旋**，无争用代价就是一次 CAS；
/// - **临界区里不得 `await`、不得回调进 `ChannelState_`**：本类型方法都是同步的，
///   拿到守卫后纯算完即释放。
///
/// 唯一不能阻塞的地方是复用循环 park 里的 `poll`：那里用
/// `available_try_`（`try_lock`，失败按「没额度」处理）。
pub struct SendWindow {
    /// 内部状态 + 自旋锁（零内部堆分配）。
    inner_: SpinningMutexOwned<SendInner>,
}

impl SendWindow {
    /// 在锁下访问内部状态（不可取消的自旋等待）。
    ///
    /// `wait_or` 的失败分支只可能来自**可取消**的等待；这里用的是不可取消令牌，
    /// 因此该分支不可达（上游 `TrMayBreak::wait` 即 `may_break_with(NonCancellable)`）。
    #[inline]
    fn with_inner_<R>(&self, f: impl FnOnce(&mut SendInner) -> R) -> R {
        let mut session = self.inner_.lock_session();
        let mut inner = session
            .lock()
            .wait_or(|| unreachable!("不可取消的自旋锁不会以取消告终"));
        f(&mut inner)
    }

    /// 在锁下访问内部状态；锁当场不可用时返回 `None`（**只在 `poll` 里用**）。
    #[inline]
    fn try_with_inner_<R>(&self, f: impl FnOnce(&mut SendInner) -> R) -> Option<R> {
        let mut session = self.inner_.lock_session();
        match session.try_lock() {
            Result::Ok(mut inner) => Option::Some(f(&mut inner)),
            Result::Err(_) => Option::None,
        }
    }

    /// 建一条**尚未安装**的发送窗口：上限 0、没有任何通告。
    ///
    /// 建流路径先建出共享状态节点，等调用方在最终裁决（`accept_async`）给出环容量
    /// 时再 [`SendWindow::install_`]。
    pub(crate) fn new_empty_() -> Self {
        SendWindow {
            inner_: SpinningMutexOwned::new_owned(SendInner::new_()),
        }
    }

    /// 安装对端窗口上限。
    pub(crate) fn install_(&self, max: Credit) {
        self.with_inner_(|inner| inner.max_ = max);
    }

    /// 以「对端窗口上限」构造；**初始可用额度为 0**，要等收到对端那条 `OPEN`
    /// 携带的通告才生效——建流三步保证数据帧不会早于它。
    #[cfg(test)]
    pub(crate) fn new_(max: Credit) -> Self {
        let window = Self::new_empty_();
        window.install_(max);
        window
    }

    /// 剩余可发送字节数：`W₀ − (S − R₀)`，下钳 `0`。
    pub fn available(&self) -> Credit {
        self.with_inner_(|inner| inner.available_())
    }

    /// [`SendWindow::available`] 的**非阻塞**形式：`try_lock` 失败返回 `None`。
    ///
    /// 供复用循环 park 的 `poll` 使用：那里既不能等锁，也不该因为「读不到状态」就
    /// 唤醒自己（那会变成忙等）。**失败即按「没有额度」处理**是安全方向：唯一的额度
    /// 来源是解复用循环收到窗口通告，而那条路径一定会投一条事件上来把 park 打断
    /// （即旧实现里 `try_with_` 的同一契约）。
    pub(crate) fn available_try_(&self) -> Option<Credit> {
        self.try_with_inner_(|inner| inner.available_())
    }

    /// 窗口是否已经用尽（发送方应当阻塞该子流，而不是报错）。
    pub fn is_exhausted(&self) -> bool {
        self.with_inner_(|inner| inner.available_() == 0u32)
    }

    /// 预扣 `want` 字节，返回本次**实际获批**的字节数（`0..=want`）。
    ///
    /// 调用方据此决定本次帧调度能取多少数据；返回 0 表示窗口用尽。
    /// 获批的字节在真正写出前就已经从窗口扣除，写失败时用
    /// [`SendWindow::refund`] 归还。「读剩余 + 扣减」在同一段临界区里完成。
    pub fn reserve(&self, want: Credit) -> Credit {
        self.with_inner_(|inner| inner.reserve_(want))
    }

    /// 归还之前 [`SendWindow::reserve`] 预扣、但最终未写出的额度。
    pub fn refund(&self, amount: Credit) {
        self.with_inner_(|inner| inner.refund_(amount));
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
    pub fn on_report(&self, report: WindowReport) -> Result<(), FlowCtrlError> {
        self.with_inner_(|inner| inner.on_report_(report))
    }

    /// 对端累计量的当前 epoch 起点（绝对总量；诊断用）。
    pub fn peer_epoch_base(&self) -> RecvTotal {
        self.with_inner_(|inner| inner.peer_epoch_base_)
    }
}
