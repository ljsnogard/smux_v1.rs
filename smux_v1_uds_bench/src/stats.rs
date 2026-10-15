//! 与运行时、与传输都无关的**统计口径**：均值、方差、标准差与「真并发」的证据量。
//!
//! # 为什么方差是这个 bench 的一等指标
//!
//! 「N 个客户端同时发」有两种截然不同的实现质量：
//!
//! - **真并发**：N 个发送任务在同一段时间里互相交错推进，于是它们的完成时间**彼此接近**，
//!   方差小；同时因为串行化了等待（环满、socket 不可写）的时间，`Σ 任务时长` 会明显
//!   大于墙钟。
//! - **假并发（先后完成）**：某个环节把 N 个任务串成了队列（共享一把锁、一条串行通道、
//!   一次只服务一个），于是它们**一个接一个**完成，完成时间被拉开，方差随 N 线性增长；
//!   而 `Σ 任务时长 ≈ 墙钟`。
//!
//! 因此本模块给两个量：
//!
//! 1. [`SampleStats`]：任务完成时间的**样本方差 / 总体方差 / 标准差 / 极差 / 变异系数**；
//! 2. [`concurrency_factor`]：`Σ 任务时长 / 墙钟`。它不依赖「任务时长的分布形状」，
//!    是判断「有没有真的同时跑」的独立证据——**串行时趋近 1.0，并发时趋近 N**。
//!
//! 两者一起看才不会误判：只报方差会漏掉「全部任务都很慢且完全同步」这种情形，
//! 只报并发因子会漏掉「确实同时在跑、但某个任务被拖尾」这种情形。

/// 一组样本的统计量。
///
/// 只依赖 `f64`，因此本模块与运行时、与传输无关，可单独单元测试。
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SampleStats {
    /// 样本个数。
    pub count: usize,
    /// 算术平均。
    pub mean: f64,
    /// **样本方差**（除以 `n − 1`，无偏估计；`n < 2` 时为 0）。
    pub variance_sample: f64,
    /// **总体方差**（除以 `n`；`n = 0` 时为 0）。
    pub variance_population: f64,
    /// 样本方差的平方根。
    pub std_dev_sample: f64,
    /// 总体方差的平方根。
    pub std_dev_population: f64,
    /// 最小值（`n = 0` 时为 0）。
    pub min: f64,
    /// 最大值（`n = 0` 时为 0）。
    pub max: f64,
    /// 极差（`max − min`）。
    ///
    /// 对「有没有被串行化」比方差更直观：串行队列下它会随 N 线性放大。
    pub range: f64,
    /// 变异系数（`std_dev_sample / mean`；`mean = 0` 时为 0）。
    ///
    /// 无量纲，因此可以跨用例（不同 N、不同载荷）横向比较离散程度。
    pub cv: f64,
}

impl SampleStats {
    /// 由一组样本算出全部统计量。
    ///
    /// # 数值稳定性
    ///
    /// 方差用 **Welford 在线算法**累加，不做 `Σx² − (Σx)²/n` 那种「两个大数相减」
    /// 的写法——本 bench 的时长量级在 `1e-3` 秒上下，朴素写法的灾难性抵消足以把
    /// 方差算成负数。这也是本函数不接受「先求和再平方」的原因。
    pub fn of(samples: &[f64]) -> Self {
        let count = samples.len();
        if count == 0usize {
            return SampleStats::default();
        }

        let mut mean = 0.0f64;
        // Welford 的 `m2`：当 n 足够大时就是 Σ(xᵢ − meanₙ)²。
        let mut m2 = 0.0f64;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;
        for (idx, &x) in samples.iter().enumerate() {
            let n = (idx + 1usize) as f64;
            let delta = x - mean;
            mean += delta / n;
            m2 += delta * (x - mean);
            if x < min {
                min = x;
            }
            if x > max {
                max = x;
            }
        }

        let variance_population = m2 / count as f64;
        let variance_sample = if count > 1usize {
            m2 / (count - 1usize) as f64
        } else {
            0.0f64
        };
        let std_dev_sample = variance_sample.sqrt();
        let cv = if mean != 0.0f64 {
            std_dev_sample / mean
        } else {
            0.0f64
        };

        SampleStats {
            count,
            mean,
            variance_sample,
            variance_population,
            std_dev_sample,
            std_dev_population: variance_population.sqrt(),
            min,
            max,
            range: max - min,
            cv,
        }
    }

    /// 样本和（`mean × count`）。
    pub fn total(&self) -> f64 {
        self.mean * self.count as f64
    }
}

/// **真并发**的证据量：`Σ 任务时长 / 墙钟`。
///
/// - `≈ 1.0`：任务是一个接一个完成的（串行，或者每个任务都被上游串行化）；
/// - `≈ N`：N 个任务在同一段时间里同时推进；
/// - 落在中间：部分重叠（典型）。
///
/// # 参数
///
/// - `task_total`：[`SampleStats::total`]，即 N 个任务时长的和；
/// - `wall`：整段墙钟。**必须**与任务时长用同一个时间原点和同一把时钟。
///
/// `wall <= 0` 时返回 `0.0`（无意义的输入不产生 `inf` / `NaN`）。
pub fn concurrency_factor(task_total: f64, wall: f64) -> f64 {
    // `wall` 是「半开区间」上的判据：0 与负数、NaN 都视为无意义输入，返回 0 而不是
    // `inf` / `NaN`。用 `partial_cmp` 明示「先比大小再取值」，避免取反比较的歧义。
    match wall.partial_cmp(&0.0f64) {
        Some(core::cmp::Ordering::Greater) => task_total / wall,
        _ => 0.0f64,
    }
}

/// 把纳秒整数转成秒（本 bench 内部一律用「纳秒整数」存时间，避免 `Duration` 序列化时
/// 丢精度；只有进统计与报告时才转 `f64`）。
pub fn nanos_to_secs(nanos: u128) -> f64 {
    nanos as f64 / 1_000_000_000.0f64
}

/// 带宽（MiB/s，1 MiB = 1024² 字节）。
///
/// `secs <= 0` 时返回 0，不产生 `inf`。
pub fn mib_per_sec(bytes: u64, secs: f64) -> f64 {
    match secs.partial_cmp(&0.0f64) {
        Some(core::cmp::Ordering::Greater) => (bytes as f64 / (1024.0f64 * 1024.0f64)) / secs,
        _ => 0.0f64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试统计口径对一组已知样本给出标准答案。
    /// - 手段：输入 `[1, 2, 3, 4]`（均值 2.5），手算出总体方差 1.25、样本方差 5/3。
    /// - 判断：各统计量与手算值在 `1e-12` 内一致，且极差 / 变异系数与定义相符。
    ///
    /// Verifies the statistics against a hand-computed sample.
    /// - Method: feed `[1, 2, 3, 4]` (mean 2.5) whose population variance is 1.25
    ///   and sample variance is 5/3.
    /// - Assertion: every statistic matches the hand computation within `1e-12`.
    #[test]
    fn stats_of_known_sample_matches_hand_computation_() {
        let s = SampleStats::of(&[1.0f64, 2.0f64, 3.0f64, 4.0f64]);
        assert_eq!(s.count, 4usize);
        assert!((s.mean - 2.5f64).abs() < 1e-12f64);
        assert!((s.variance_population - 1.25f64).abs() < 1e-12f64);
        assert!((s.variance_sample - 5.0f64 / 3.0f64).abs() < 1e-12f64);
        assert!((s.std_dev_population - 1.25f64.sqrt()).abs() < 1e-12f64);
        assert_eq!(s.min, 1.0f64);
        assert_eq!(s.max, 4.0f64);
        assert!((s.range - 3.0f64).abs() < 1e-12f64);
        assert!((s.cv - (5.0f64 / 3.0f64).sqrt() / 2.5f64).abs() < 1e-12f64);
        assert!((s.total() - 10.0f64).abs() < 1e-12f64);
    }

    /// 测试退化输入不产生 `NaN` / `inf`。
    /// - 手段：空样本、单样本、全零样本各算一遍；再算一次 `wall = 0` 的并发因子。
    /// - 判断：空样本全零；单样本方差为 0 而均值为该样本；并发因子为 0 而不是 `inf`。
    ///
    /// Checks the degenerate inputs never produce `NaN` or `inf`.
    /// - Method: empty, single-element and all-zero samples, plus `wall = 0`.
    /// - Assertion: empty yields zeros, a single sample has zero variance, and the
    ///   concurrency factor is `0.0` instead of `inf`.
    #[test]
    fn stats_are_nan_free_on_degenerate_input_() {
        let empty = SampleStats::of(&[]);
        assert_eq!(empty.count, 0usize);
        assert_eq!(empty.mean, 0.0f64);

        let one = SampleStats::of(&[7.0f64]);
        assert_eq!(one.mean, 7.0f64);
        assert_eq!(one.variance_sample, 0.0f64);
        assert_eq!(one.variance_population, 0.0f64);

        let zeros = SampleStats::of(&[0.0f64; 4usize]);
        assert_eq!(zeros.cv, 0.0f64);

        assert_eq!(concurrency_factor(4.0f64, 0.0f64), 0.0f64);
        assert!(!concurrency_factor(4.0f64, f64::NAN).is_nan());
    }

    /// 测试并发因子能区分「串行」与「并发」两种形态。
    /// - 手段：构造 4 个任务、每个 1 秒的两组数据——串行（墙钟 4 秒）与并发（墙钟 1 秒）。
    /// - 判断：串行时因子 ≈ 1.0，并发时因子 ≈ 4.0。
    ///
    /// Checks the concurrency factor separates serial from concurrent execution.
    /// - Method: four one-second tasks with a 4 s wall (serial) and a 1 s wall
    ///   (concurrent).
    /// - Assertion: the factor is ~1.0 and ~4.0 respectively.
    #[test]
    fn concurrency_factor_separates_serial_from_parallel_() {
        let tasks = SampleStats::of(&[1.0f64; 4usize]);
        assert!((concurrency_factor(tasks.total(), 4.0f64) - 1.0f64).abs() < 1e-12f64);
        assert!((concurrency_factor(tasks.total(), 1.0f64) - 4.0f64).abs() < 1e-12f64);
    }

    /// 测试带宽换算用的是 1024² 口径。
    /// - 手段：1 MiB 恰好用 1 秒发完，以及 0 秒的退化输入。
    /// - 判断：前者为 1.0 MiB/s，后者为 0（不是 `inf`）。
    ///
    /// Checks the bandwidth helper uses the 1024² definition.
    /// - Method: 1 MiB in exactly 1 s, and a zero-duration input.
    /// - Assertion: 1.0 MiB/s and 0.0 respectively.
    #[test]
    fn bandwidth_uses_binary_mebibyte_() {
        assert!((mib_per_sec(1024usize as u64 * 1024usize as u64, 1.0f64) - 1.0f64).abs() < 1e-12f64);
        assert_eq!(mib_per_sec(1024usize as u64, 0.0f64), 0.0f64);
    }
}
