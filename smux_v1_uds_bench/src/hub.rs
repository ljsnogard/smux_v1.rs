//! 一格用例的**共享时间轴**：把「N 个任务同时出发」与「大家都发完了」串起来。
//!
//! # 时间怎么取
//!
//! ```text
//! 栅栏释放（t0＝所有发送任务共同的起点）
//!    ├─ 任务 0 写完最后一字节 → t_end(0)
//!    ├─ 任务 1 写完最后一字节 → t_end(1)
//!    │      …
//!    └─ 最后一个任务写完最后一字节 → t_wall（＝墙钟终点）
//!                                    ↓ 尾部数据经环 / socket 排空
//!                                    服务端收齐（t_drained，只作参考）
//! ```
//!
//! - **每任务时长** `t_end(i) − t0`：任务完成时间的方差取自它；
//! - **墙钟** `t_wall − t0`：平均带宽取自它；
//! - `t_drained` **不进带宽**，只作为参考量写进报告——它受环容量、`--chunk`、
//!   适配器的排空策略支配，把它算进带宽会让「同一份代码换个大环」这种事凭空改变
//!   带宽读数。两条路径的墙钟口径因此严格一致：**都是「所有发送任务发完」**。
//!
//! # 为什么用「最后一个完成的人负责落表」
//!
//! N 个任务谁最后完成不确定。这里用一个递减计数器：**减到 0 的那一次调用**顺带记
//! 时刻。于是不需要任何「收集 N 个时刻再取最大」的协调，也不怕某一格提前结束时漏记。
//!
//! # 为什么可以是 `Rc` 与 `Arc` 混用
//!
//! 三个后端下全部发送任务都跑在同一个线程（compio 的 socket 半边是 `!Send`，
//! `spawn` 用不了），因此 [`StartBarrier`] 用单线程的 `Rc<RefCell<..>>`；
//! 而「计数器 + 时间槽」用 `Arc` 是为了让同一份代码在将来真的出现多线程任务时仍然成立。

use std::{
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use crate::barrier::StartBarrier;

/// 一格用例里，某个发送任务的时间轴（相对共同起点）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskTime {
    /// 任务写完最后一字节的相对纳秒数。
    pub end_ns: u64,
    /// 该任务是否失败（失败的任务不进统计）。
    pub failed: bool,
}

/// 一格用例的共享上下文。
#[derive(Debug, Clone)]
pub struct CaseHub {
    /// 让 N 个发送任务同时出发的栅栏。
    barrier: StartBarrier,
    /// 共同起点（栅栏释放的那一刻）。
    start_: Arc<OnceLock<Instant>>,
    /// 逐任务的写入完成时刻。
    slots_: Arc<Mutex<Vec<TaskTime>>>,
    /// 还没报「发完了」的发送任务数。
    pending_sends_: Arc<AtomicUsize>,
    /// 墙钟终点（最后一个发送任务完成时落表）。
    wall_ns_: Arc<Mutex<Option<u64>>>,
    /// 服务端收齐的相对时刻（只作参考，不进带宽）。
    drained_ns_: Arc<Mutex<Option<u64>>>,
    /// 还没排空完的服务端读任务数。
    pending_drains_: Arc<AtomicUsize>,
    /// 已就位的发送任务数。
    ready_: Arc<AtomicUsize>,
}

impl CaseHub {
    /// 造一个需要 `parties` 个发送任务就位的上下文。
    pub fn new(parties: usize) -> Self {
        CaseHub {
            barrier: StartBarrier::new(parties),
            start_: Arc::new(OnceLock::new()),
            slots_: Arc::new(Mutex::new(vec![TaskTime::default(); parties])),
            pending_sends_: Arc::new(AtomicUsize::new(parties)),
            wall_ns_: Arc::new(Mutex::new(None)),
            drained_ns_: Arc::new(Mutex::new(None)),
            pending_drains_: Arc::new(AtomicUsize::new(parties)),
            ready_: Arc::new(AtomicUsize::new(0usize)),
        }
    }

    /// 栅栏句柄（发送任务拿它等「同时出发」）。
    pub fn barrier(&self) -> &StartBarrier {
        &self.barrier
    }

    /// 设置共同起点（幂等：重复设置只保留第一次）。
    pub fn set_start(&self, start: Instant) {
        let _ = self.start_.set(start);
    }

    /// 共同起点；还没设置时返回 `None`。
    pub fn start(&self) -> Option<Instant> {
        self.start_.get().copied()
    }

    /// 记录第 `idx` 个发送任务「写完最后一字节」的相对纳秒数。
    ///
    /// # Errors
    ///
    /// 序号越界或互斥量中毒时返回说明。
    pub fn finish_task(&self, idx: usize, end_ns: u64) -> Result<(), String> {
        let mut slots = self
            .slots_
            .lock()
            .map_err(|_| "任务时间槽的互斥量中毒".to_string())?;
        let total = slots.len();
        let slot = slots
            .get_mut(idx)
            .ok_or_else(|| format!("任务序号 {idx} 越界（共 {total} 个）"))?;
        slot.end_ns = end_ns;
        Ok(())
    }

    /// 记录第 `idx` 个发送任务失败。
    ///
    /// # Errors
    ///
    /// 序号越界或互斥量中毒时返回说明。
    pub fn fail_task(&self, idx: usize) -> Result<(), String> {
        let mut slots = self
            .slots_
            .lock()
            .map_err(|_| "任务时间槽的互斥量中毒".to_string())?;
        let total = slots.len();
        let slot = slots
            .get_mut(idx)
            .ok_or_else(|| format!("任务序号 {idx} 越界（共 {total} 个）"))?;
        slot.failed = true;
        Ok(())
    }

    /// 记下一个发送任务已经就位。
    pub fn mark_ready(&self) {
        self.ready_.fetch_add(1usize, Ordering::SeqCst);
    }

    /// 已就位的发送任务数（诊断用）。
    pub fn ready(&self) -> usize {
        self.ready_.load(Ordering::SeqCst)
    }

    /// 发送侧报告「我又发完了一个」。
    ///
    /// 计数归零的那一次调用顺带落**墙钟终点**。失败的任务也要调它——否则用例会一直
    /// 等一个永远不会到来的终点，直到超时。
    pub fn send_finished_one(&self) {
        let left = self.pending_sends_.fetch_sub(1usize, Ordering::SeqCst);
        if left == 1usize
            && let Some(start) = self.start_.get().copied()
            && let Ok(mut slot) = self.wall_ns_.lock()
        {
            *slot = Some(start.elapsed().as_nanos() as u64);
        }
    }

    /// 墙钟终点（相对共同起点的纳秒数）；还没有落表时返回 `None`。
    pub fn wall_ns(&self) -> Option<u64> {
        self.wall_ns_.lock().ok().and_then(|guard| *guard)
    }

    /// 服务端报告「我又收完了一条」。
    ///
    /// 只记**最后一个**收完的时刻，作为参考量（不进带宽，理由见模块文档）。
    pub fn drain_finished_one(&self) {
        let left = self.pending_drains_.fetch_sub(1usize, Ordering::SeqCst);
        if left == 1usize
            && let Some(start) = self.start_.get().copied()
            && let Ok(mut slot) = self.drained_ns_.lock()
        {
            *slot = Some(start.elapsed().as_nanos() as u64);
        }
    }

    /// 服务端收齐的相对纳秒数；还没有收齐时返回 `None`。
    pub fn drained_ns(&self) -> Option<u64> {
        self.drained_ns_.lock().ok().and_then(|guard| *guard)
    }

    /// 取出逐任务时间（按任务序号）。
    ///
    /// # Errors
    ///
    /// 互斥量中毒时返回说明。
    pub fn task_times(&self) -> Result<Vec<TaskTime>, String> {
        self.slots_
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| "任务时间槽的互斥量中毒".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试「最后一个发送任务完成时落墙钟终点」这条规则。
    /// - 手段：3 个任务的上下文里依次调 `send_finished_one`，中间各查一次。
    /// - 判断：前两次 `wall_ns()` 为空，第三次落表且值为 `Some`。
    ///
    /// Checks only the last sender stamps the wall-clock end.
    /// - Method: call `send_finished_one` three times, inspecting in between.
    /// - Assertion: the first two leave the stamp empty and the last one sets it.
    #[test]
    fn only_last_sender_stamps_wall_() {
        let hub = CaseHub::new(3usize);
        hub.set_start(Instant::now());
        hub.send_finished_one();
        assert!(hub.wall_ns().is_none());
        hub.send_finished_one();
        assert!(hub.wall_ns().is_none());
        hub.send_finished_one();
        assert!(hub.wall_ns().is_some());
    }

    /// 测试服务端排空时刻与墙钟终点是**两个独立的量**。
    /// - 手段：只让发送侧完成，再让服务端完成。
    /// - 判断：墙钟先落表、`drained_ns()` 后落表，且两者都非空时墙钟不大于排空时刻
    ///   （排空在墙钟之后发生）。
    ///
    /// Checks the drained instant is tracked separately from the wall clock.
    /// - Method: finish both sides and compare the two stamps.
    /// - Assertion: both exist and the drained instant is not earlier than the wall.
    #[test]
    fn drained_instant_is_separate_from_wall_() {
        let hub = CaseHub::new(1usize);
        hub.set_start(Instant::now());
        hub.send_finished_one();
        assert!(hub.wall_ns().is_some());
        assert!(hub.drained_ns().is_none());
        hub.drain_finished_one();
        let wall = hub.wall_ns().expect("墙钟");
        let drained = hub.drained_ns().expect("排空时刻");
        assert!(drained >= wall, "排空 {drained} 早于墙钟 {wall}");
    }

    /// 测试任务时间槽的越界与失败标记。
    /// - 手段：2 个任务的上下文里写第 0 号、标记第 1 号失败，再越界写第 9 号。
    /// - 判断：前两次 `Ok`；越界返回 `Err`；`task_times` 里第 1 号带 `failed` 标记。
    ///
    /// Checks the task slots and their bounds checking.
    /// - Method: write slot 0, fail slot 1, then write slot 9.
    /// - Assertion: the first two succeed, the out-of-range write errs, and the
    ///   failure flag is visible in `task_times`.
    #[test]
    fn task_slots_report_failure_and_reject_out_of_range_() {
        let hub = CaseHub::new(2usize);
        assert!(hub.finish_task(0usize, 123u64).is_ok());
        assert!(hub.fail_task(1usize).is_ok());
        assert!(hub.finish_task(9usize, 1u64).is_err());
        let times = hub.task_times().expect("取任务时间");
        assert_eq!(times[0usize].end_ns, 123u64);
        assert!(!times[0usize].failed);
        assert!(times[1usize].failed);
    }

    /// 测试共同起点只设置一次（幂等）。
    /// - 手段：连续设置两个不同的 `Instant`。
    /// - 判断：`start()` 返回的是**第一次**那个。
    ///
    /// Checks the common start is set only once.
    /// - Method: set two different instants in a row.
    /// - Assertion: `start()` yields the first one.
    #[test]
    fn common_start_is_set_once_() {
        let hub = CaseHub::new(1usize);
        let first = Instant::now();
        hub.set_start(first);
        hub.set_start(Instant::now());
        assert_eq!(hub.start(), Some(first));
    }
}
