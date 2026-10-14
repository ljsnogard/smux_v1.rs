use super::recv_window_::{ReportThresholds_, Zone_};
use super::*;

/// 把非临界区的**变动量门限**固定成 `容量/4` 的同构策略（用于验门限本身）。
struct GatedPolicy;

impl TrFlowCtrlPolicy for GatedPolicy {
    fn initial_window(&self, ring_capacity: usize) -> Credit {
        ring_capacity.min(Credit::MAX as usize) as Credit
    }

    fn critical_denominator(&self) -> Credit {
        4u32
    }

    fn min_advance_between_reports(&self, initial: Credit) -> Credit {
        (initial / 4u32).max(1u32)
    }

    fn max_window(&self) -> Credit {
        Credit::MAX / 2u32
    }

    fn recv_total_epoch(&self) -> RecvTotal {
        u32::MAX as RecvTotal
    }
}

/// 缺省策略：初始窗口跟随环容量（超出 `u32` 时饱和），临界区取容量的 1/4，
/// 非临界区的变动量门限同样取一档临界容量（并有绝对下限）。
/// - 手段：对容量 0 / 4096 / `usize::MAX` 求初窗，并读取分区旋钮。
/// - 判断：初窗分别饱和为 0 / 4096 / `u32::MAX`；临界分母为 4、变动量门限为
///   1024（= 4096/4）与绝对下限 16。
#[test]
fn default_policy_window_and_zone() {
    let p = DefaultPolicy::new();
    assert_eq!(p.initial_window(0usize), 0u32);
    assert_eq!(p.initial_window(4096usize), 4096u32);
    assert_eq!(p.initial_window(usize::MAX), Credit::MAX);

    assert_eq!(p.critical_denominator(), 4u32);
    // 非临界区门限 = 一档临界容量（4096/4 = 1024），远高于绝对下限。
    assert_eq!(p.min_advance_between_reports(4096u32), 1024u32);
    // 极小窗口退到绝对下限，不会变成 0（0 意味着不限频）。
    assert_eq!(p.min_advance_between_reports(8u32), 16u32);
    assert_eq!(p.max_window(), Credit::MAX / 2u32);
}

/// 测试分区边界：容量 512 时临界区 `≤128`、非临界区 `>128`（**没有第二个上界**）。
///
/// - 手段：建窗并通告一次（初始剩余 = 512）；随后收 128 字节把剩余压到 384、
///   再收 256 压到 128（临界区上界本身）、最后收 128 归零。
/// - 判断：`zone_with_` 在四个点上依次给出 `Normal / Normal / Critical / Critical`。
#[test]
fn recv_window_zone_boundaries() {
    let p = DefaultPolicy::new();
    let w = RecvWindow::new_(&p, 512usize);
    let thresholds = ReportThresholds_::new_(&p, 512u32);
    assert_eq!(w.window(), 512u32);
    assert_eq!(w.zone_with_(&thresholds), Zone_::Normal, "满窗口属非临界区");

    // 通告一次（模拟建流那次 `OPEN`）：`on_data` 的越权判定以已通告额度为准。
    w.report();
    // 剩余 384：仍在非临界区（判据只看「是否 ≤ 容量/4 = 128」）。
    w.on_data(128u32).expect("在额度内");
    assert_eq!(w.window(), 384u32);
    assert_eq!(w.zone_with_(&thresholds), Zone_::Normal);

    // 压到 128 = 临界区上界：属临界区（判据是「≤ 上界」）。
    w.on_data(256u32).expect("在额度内");
    assert_eq!(w.window(), 128u32);
    assert_eq!(w.zone_with_(&thresholds), Zone_::Critical, "恰在上界算临界区");

    // 归零：仍在临界区。
    w.on_data(128u32).expect("在额度内");
    assert_eq!(w.window(), 0u32);
    assert_eq!(w.zone_with_(&thresholds), Zone_::Critical);
}

/// 测试**充裕区不发提醒**，以及「跌出充裕区」这一次要发。
///
/// - 手段：初窗 512，通告一次；收 64（剩余 448，仍充裕）后看 `should_report`；
///   再收 64（剩余 384，落到中间区下界）后再看。
/// - 判断：448 时为假（充裕区，离瘫痪还远）；384 时为真（分区切换本身要报一次，
///   否则发送方会一直以为这边还空着）。
///
/// 测试**临界区每变必报**（不被非临界区的变动量门限压住）。
///
/// - 手段：容量 1024（临界上界 256）；通告一次后收 800，剩余 224 进临界区；消费 1 字节。
/// - 判断：一进入临界区就判为应当发；此后仅消费 1 字节（变动量远小于门限）仍为真。
#[test]
fn recv_window_reports_every_change_in_critical_zone() {
    let p = DefaultPolicy::new();
    // 容量 1024 ⇒ 临界上界 256。
    let w = RecvWindow::new_(&p, 1024usize);
    w.report();

    // 收 800：剩余 224 ≤ 256，进临界区。
    w.on_data(800u32).expect("在额度内");
    assert_eq!(w.window(), 224u32, "224 ≤ 256：临界区");
    assert!(w.should_report(&p), "刚进入临界区应当发");
    w.report();

    // 应用只消费 1 字节：临界区里任何变化都要报，哪怕变动量远小于门限。
    w.on_consumed(1u32);
    assert_eq!(w.window(), 225u32);
    assert!(
        w.should_report(&p),
        "临界区里任何变化都提醒（不被变动量门限压住）"
    );
}

/// 测试**非临界区的变化受变动量门限约束**（防抖）。
///
/// 构造的关键：窗口要落在非临界区（`> 容量/4`），且那一次变化**先**发过通告
/// （否则「刚跌进临界区」会无条件放行，验不到门限）。
///
/// - 手段：用一个门限明确的同构策略 + 容量 2048（临界上界 512、变动量门限 512）；
///   通告一次；收 1000 使剩余 1048（非临界区，且变动量 1000 已达标，先报一次）；
///   此后只消费 128 一档一档地推进，窗口单调变大。
/// - 判断：累计 128 / 256 / 384 时**仍不发**（门限 512 未达）；累计到 512 时为真。
#[test]
fn recv_window_rate_limits_outside_critical_zone() {
    let p = GatedPolicy;
    let w = RecvWindow::new_(&p, 2048usize);
    w.report();

    // 剩余 1048：非临界区（临界上界 512）。
    w.on_data(1000u32).expect("在额度内");
    assert_eq!(w.window(), 1048u32, "应当落在非临界区");
    assert!(w.should_report(&p), "变动量 1000 已超过门限 512，应当发");
    w.report();

    // 非临界区内部：**只消费**（窗口单调变大，保证「窗口确实变了」始终成立）。
    // 门限是「变动量达到 2048/4 = 512 字节」，因此每次消费 128、探一次：
    // 前 3 次累计 128/256/384 均不足，第 4 次累计 512 达标。
    let mut reported = false;
    for i in 0..4 {
        w.on_consumed(128u32);
        let ok = w.should_report(&p);
        if i < 3 {
            assert!(
                !ok,
                "非临界区：累计 {} 字节仍不足门限，不该发",
                (i + 1) * 128
            );
        } else {
            reported = ok;
        }
    }
    assert!(reported, "累计变动量达到门限且窗口确实变了：应当发");
}

/// 测试**没有「剩余 > 3/4 就不提醒」的上限档**：非临界区一律按变动量门限推进。
///
/// 这是本轮修掉的那条政策——旧实现在剩余高于 `容量 × 3/4` 时完全不发通告，导致对端
/// 的可用额度永远停在旧快照上。
///
/// - 手段：初窗 512（临界上界 = 变动量门限 = 128）；通告一次；收 96（剩余 416，
///   仍高于 3/4 水位 384），此时变动量 96 < 128；再消费 32（剩余 448）。
/// - 判断：变动量不足门限时为假（防抖仍在）；累计到 128 且窗口确实变了时为真
///   ——即便剩余仍高于 3/4。
#[test]
fn recv_window_reports_above_three_quarters_once_advance_reached() {
    let p = DefaultPolicy::new();
    let w = RecvWindow::new_(&p, 512usize);
    w.report();

    w.on_data(96u32).expect("在额度内");
    assert_eq!(w.window(), 416u32, "剩余 416 > 3/4 容量（384）");
    assert!(!w.should_report(&p), "变动量 96 < 门限 128：仍要防抖");

    w.on_consumed(32u32);
    assert_eq!(w.window(), 448u32);
    assert!(
        w.should_report(&p),
        "变动量累计到 128：即便剩余仍高于 3/4，也必须推进通告"
    );
}

/// 测试「窗口与上次通告相同」时一律不发（没有新信息）。
///
/// - 手段：初窗 512、通告一次后原样再判两次。
/// - 判断：均为假。
#[test]
fn recv_window_silent_when_unchanged() {
    let p = DefaultPolicy::new();
    let w = RecvWindow::new_(&p, 512usize);
    w.report();
    assert!(!w.should_report(&p), "窗口没变：不发");
    assert_eq!(w.report().window(), 512u32, "重复 report 仍是同一快照");
    assert!(!w.should_report(&p), "再次判断仍不发");
}

/// 测试窗口归零之后「从 0 抬起」不再需要任何特例：归零与抬起都落在**临界区**，
/// 而临界区每变必报。
///
/// - 手段：初窗 512；收满 512 归零并通告；应用消费 64（剩余 64，仍临界区）。
/// - 判断：归零后 `should_report` 为真（临界区每变必报）；消费后仍为真，
///   且变动量为 0（没有任何新数据帧）也照样为真。
#[test]
fn recv_window_zero_and_refill_need_no_special_case() {
    let p = DefaultPolicy::new();
    let w = RecvWindow::new_(&p, 512usize);
    w.report();

    w.on_data(512u32).expect("刚好用满已通告额度");
    assert_eq!(w.window(), 0u32);
    assert!(w.should_report(&p), "归零应当发");
    w.report();

    // 没有新数据帧，只有应用消费：临界区里照样要发。
    w.on_consumed(64u32);
    assert_eq!(w.window(), 64u32);
    assert!(
        w.should_report(&p),
        "从 0 抬起落在临界区：无需任何免门限特例"
    );
    assert_eq!(w.report().window(), 64u32);
}

/// 测试发送窗口按 `可用 = W₀ − (S − R₀)` 计算，并忽略过期通告。
/// - 手段：初窗 100（`(0, 100)`）→ 预扣 30 → 收到新通告 `(30, 50)` → 再喂一份
///   过期的 `(10, 999)`。
/// - 判断：预扣后可用 70；新通告后可用 50（在途被精确扣掉）；过期通告不生效。
#[test]
fn send_window_subtracts_inflight_and_ignores_stale_reports() {
    let w = SendWindow::new_(1024u32);
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

/// 测试「累计已收 `R` 不变、只有窗口 `W` 变大」的回补通告会被**接受**。
///
/// 这是窗口跌到 0 之后接收方消费数据时发出的**唯一**形状：它还没再收到新数据，
/// 所以 `R` 与上次通告相同，只有 `W` 变大。若发送方把它当重复通告丢掉，这条子流
/// 会永久停在 `available() == 0` 而死锁。
///
/// - 手段：上限 4096；喂 `(0, 4096)` 通告后预扣满 4096（窗口用尽、可用为 0）；
///   随后依次喂「`R` 不变但 `W` 递增」的 `(4096, 512)` → `(4096, 1024)`。
/// - 判断：两份通告都被接受，可用额度分别变为 512 与 1024（而不是停在 0）；
///   再喂一份 `R` 不变、`W` 更小的 `(4096, 256)` 时**不覆盖**较新的快照。
#[test]
fn send_window_accepts_window_growth_with_same_recv_total() {
    let w = SendWindow::new_(4096u32);
    w.on_report(WindowReport::new(0u64, 4096u32))
        .expect("开场通告");

    // 把额度用尽：接收方此刻会通告 `(4096, 0)`。
    assert_eq!(w.reserve(4096u32), 4096u32);
    w.on_report(WindowReport::new(4096u64, 0u32)).expect("窗口降到 0");
    assert_eq!(w.available(), 0u32, "窗口降到 0 后没有额度");
    assert!(w.is_exhausted());

    // 接收方应用消费了 512 字节：`R` 不变、`W` 升到 512。
    w.on_report(WindowReport::new(4096u64, 512u32))
        .expect("R 不变但 W 变大：必须接受");
    assert_eq!(w.available(), 512u32, "回补的额度必须立即可用");

    // 再消费 512 字节。
    w.on_report(WindowReport::new(4096u64, 1024u32))
        .expect("继续回补");
    assert_eq!(w.available(), 1024u32);

    // 同 `R` 但窗口更小：过期快照，不覆盖。
    w.on_report(WindowReport::new(4096u64, 256u32))
        .expect("更旧的快照不是错误");
    assert_eq!(w.available(), 1024u32, "更小的同 R 快照不应覆盖较新快照");
}

/// 测试发送窗口的预扣、部分获批、用尽与归还。
/// - 手段：通告 `(0, 10)` 后依次 `reserve(4)`、`reserve(100)`、`refund(3)`。
/// - 判断：两次获批分别为 4 与 6；用尽后 `is_exhausted` 为真；归还后可用 3。
#[test]
fn send_window_reserve_exhaust_and_refund() {
    let w = SendWindow::new_(1024u32);
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
    let w = SendWindow::new_(12u32);
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
    let ctrl = FlowCtrl::new(&p, 100usize);
    assert_eq!(ctrl.recv_window().capacity(), 100u32);
    assert_eq!(ctrl.recv_window().window(), 100u32);
    assert_eq!(ctrl.send_window().available(), 0u32);

    ctrl.send_window()
        .on_report(WindowReport::new(0u64, 40u32))
        .expect("通告");
    assert_eq!(ctrl.send_window().available(), 40u32);

    ctrl.recv_window().report();
    assert!(ctrl.recv_window().on_data(100u32).is_ok());
    assert_eq!(
        ctrl.recv_window().on_data(1u32),
        Result::Err(FlowCtrlError::PeerViolation)
    );
}

/// 测试 `report()` 是幂等的快照：重复调用取到同一个值，且会清零变动量。
/// - 手段：初窗 4096 上连续 `report()` 两次，并在中间读 `reported_window`。
/// - 判断：两次快照都是 `(0, 4096)`；`reported_window` 为 `Some(4096)`；
///   第一次 `report` 后的 `should_report` 为假。
#[test]
fn report_is_idempotent_snapshot() {
    let p = DefaultPolicy::new();
    let w = RecvWindow::new_(&p, 4096usize);
    let a = w.report();
    let b = w.report();
    assert_eq!(a, b);
    assert_eq!(w.reported_window(), Option::Some(4096u32));
    assert_eq!(w.recv_total(), 0u64);
    assert!(!w.should_report(&p));
}
