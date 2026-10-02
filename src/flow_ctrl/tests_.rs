use super::*;

/// 缺省策略：初始窗口跟随环容量（超出 `u32` 时饱和），水位为 1/2、1/4、0 与
/// 1/2、3/4、满，频率限制为 4 个数据帧。
/// - 手段：对容量 0 / 4096 / `usize::MAX` 求初窗，并对初窗 4096 求两组水位。
/// - 判断：初窗分别饱和为 0 / 4096 / `u32::MAX`；水位与频率限制与设计一致。
#[test]
fn default_policy_window_and_levels() {
    let p = DefaultPolicy::new();
    assert_eq!(p.initial_window(0usize), 0u32);
    assert_eq!(p.initial_window(4096usize), 4096u32);
    assert_eq!(p.initial_window(usize::MAX), Credit::MAX);

    assert_eq!(p.shrink_levels(4096u32), [2048u32, 1024u32, 0u32]);
    assert_eq!(p.expand_levels(4096u32), [2048u32, 3072u32, 4096u32]);
    assert_eq!(p.min_frames_between_reports(), 4usize);
    assert_eq!(p.max_window(), Credit::MAX / 2u32);
}

/// 测试「建流时先通告一次」以及收缩 / 扩张两侧的阈值触发点。
/// - 手段：初窗 4096；先 `report()` 一次（模拟 `OPEN` 通告）。随后按
///   「收 4 个数据帧」为一批推进窗口，逐档检查 `should_report`：
///   5696 - 2400 = 1696（跌破 1/2）→ 再收 800 → 896（跌破 1/4）→ 再收到 0；
///   然后应用消费使窗口升过 1/2、超过 3/4、回到满。
/// - 判断：每次跨过水位时 `should_report` 为真；未跨水位（如从 4096 只降到
///   3000）时为假；`report()` 之后再次判断为假（已通告同一值）。
#[test]
fn recv_window_reports_on_threshold_crossings() {
    let p = DefaultPolicy::new();
    let mut w = RecvWindow::new_(&p, 4096usize);
    assert!(w.should_report(&p), "还没通告过：必须先通告一次");
    let first = w.report();
    assert_eq!(first.recv_total(), 0u64);
    assert_eq!(first.window(), 4096u32, "开场通告的是最大接收窗口");
    assert!(!w.should_report(&p), "刚通告过同一值：不该重复发");

    // 只降到 3000（未跨 1/2=2048）：不触发。
    for _ in 0..4 {
        w.on_data(274u32).expect("在额度内");
    }
    assert_eq!(w.window(), 4096u32 - 4u32 * 274u32);
    assert!(!w.should_report(&p), "没有跨过任何水位");

    // 继续降到 1696（跌破 1/2）：触发。
    for _ in 0..4 {
        w.on_data(326u32).expect("在额度内");
    }
    assert_eq!(w.window(), 1696u32);
    assert!(w.should_report(&p), "跌破 1/2 应当通告");
    w.report();

    // 降到 896（跌破 1/4=1024）：触发。
    for _ in 0..4 {
        w.on_data(200u32).expect("在额度内");
    }
    assert_eq!(w.window(), 896u32);
    assert!(w.should_report(&p), "跌破 1/4 应当通告");
    w.report();

    // 降到 0：触发（水位列里的 0 专门表达这个点）。
    for _ in 0..4 {
        w.on_data(100u32).expect("在额度内");
    }
    assert_eq!(w.window(), 496u32);
    for _ in 0..4 {
        w.on_data(124u32).expect("在额度内");
    }
    assert_eq!(w.window(), 0u32);
    assert!(w.should_report(&p), "窗口降到 0 应当通告");
    w.report();

    // 扩张：消费 2100 → 窗口 2100（升过 1/2=2048）：触发。
    for _ in 0..4 {
        w.on_data(0u32).expect("零字节也算一个数据帧");
    }
    w.on_consumed(2100u32);
    assert_eq!(w.window(), 2100u32);
    assert!(w.should_report(&p), "升过 1/2 应当通告");
    w.report();

    // 扩张到 3200（超过 3/4=3072）：触发。
    for _ in 0..4 {
        w.on_data(0u32).expect("零字节也算一个数据帧");
    }
    w.on_consumed(1100u32);
    assert_eq!(w.window(), 3200u32);
    assert!(w.should_report(&p), "升过 3/4 应当通告");
    w.report();

    // 回到满：触发。
    for _ in 0..4 {
        w.on_data(0u32).expect("零字节也算一个数据帧");
    }
    w.on_consumed(896u32);
    assert_eq!(w.window(), 4096u32);
    assert!(w.should_report(&p), "回到满应当通告");
}

/// 测试频率限制：跨过水位但数据帧数不够时不发，攒够帧数后补发。
/// - 手段：初窗 4096，先 `report()`；随后 3 个数据帧就把窗口压到 1/2 以下。
/// - 判断：第 3 帧后 `should_report` 仍为假（未达 4 帧）；第 4 帧后为真。
#[test]
fn recv_window_rate_limits_reports() {
    let p = DefaultPolicy::new();
    let mut w = RecvWindow::new_(&p, 4096usize);
    w.report();

    w.on_data(1400u32).expect("在额度内");
    assert_eq!(w.window(), 2696u32, "还没跌破 1/2");

    w.on_data(1400u32).expect("在额度内");
    assert!(w.window() < 2048u32, "已经跌破 1/2");
    assert!(!w.should_report(&p), "不足最小帧数：先攒着");

    w.on_data(0u32).expect("第 3 帧");
    assert!(!w.should_report(&p), "第 3 帧仍不够");

    w.on_data(0u32).expect("第 4 帧");
    assert!(
        w.should_report(&p),
        "攒够帧数后应当补发（水位条件一直成立）"
    );
}

/// 测试「未通告」与「超出已通告额度」都判为对端违例。
/// - 手段：新建接收窗口（尚未通告）时 `on_data(1)`；`report()` 出 `(0, 4096)`
///   后再 `on_data(4097)`。
/// - 判断：两次都返回 [`FlowCtrlError::PeerViolation`]，且已收字节数不前进。
#[test]
fn recv_window_flags_peer_violation() {
    let p = DefaultPolicy::new();
    let mut w = RecvWindow::new_(&p, 4096usize);

    assert_eq!(
        w.on_data(1u32),
        Result::Err(FlowCtrlError::PeerViolation),
        "还没通告过窗口，对端不该发数据"
    );
    assert_eq!(w.recv_total(), 0u64);

    let report = w.report();
    assert_eq!(report.window(), 4096u32);
    assert!(w.on_data(4096u32).is_ok(), "刚好用满已通告额度是合法的");
    assert_eq!(w.recv_total(), 4096u64);
    assert_eq!(
        w.on_data(1u32),
        Result::Err(FlowCtrlError::PeerViolation)
    );
    assert_eq!(w.recv_total(), 4096u64, "违例不应推进已收计数");
}

/// 测试发送窗口按 `可用 = W₀ − (S − R₀)` 计算，并忽略过期通告。
/// - 手段：初窗 100（`(0, 100)`）→ 预扣 30 → 收到新通告 `(30, 50)` → 再喂一份
///   过期的 `(10, 999)`。
/// - 判断：预扣后可用 70；新通告后可用 50（在途被精确扣掉）；过期通告不生效。
#[test]
fn send_window_subtracts_inflight_and_ignores_stale_reports() {
    let mut w = SendWindow::new_(1024u32);
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

/// 测试发送窗口的预扣、部分获批、用尽与归还。
/// - 手段：通告 `(0, 10)` 后依次 `reserve(4)`、`reserve(100)`、`refund(3)`。
/// - 判断：两次获批分别为 4 与 6；用尽后 `is_exhausted` 为真；归还后可用 3。
#[test]
fn send_window_reserve_exhaust_and_refund() {
    let mut w = SendWindow::new_(1024u32);
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
    let mut w = SendWindow::new_(12u32);
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
    let mut ctrl = FlowCtrl::new(&p, 100usize);
    assert_eq!(ctrl.recv_window().capacity(), 100u32);
    assert_eq!(ctrl.recv_window().window(), 100u32);
    assert_eq!(ctrl.send_window().available(), 0u32);

    ctrl.send_window_mut()
        .on_report(WindowReport::new(0u64, 40u32))
        .expect("通告");
    assert_eq!(ctrl.send_window().available(), 40u32);

    ctrl.recv_window_mut().report();
    assert!(ctrl.recv_window_mut().on_data(100u32).is_ok());
    assert_eq!(
        ctrl.recv_window_mut().on_data(1u32),
        Result::Err(FlowCtrlError::PeerViolation)
    );
}

/// 测试 `report()` 是幂等的快照：重复调用取到同一个值，且会清零帧计数。
/// - 手段：初窗 4096 上连续 `report()` 两次，并在中间读 `reported_window`。
/// - 判断：两次快照都是 `(0, 4096)`；`reported_window` 为 `Some(4096)`；
///   第一次 `report` 后的 `should_report` 为假。
#[test]
fn report_is_idempotent_snapshot() {
    let p = DefaultPolicy::new();
    let mut w = RecvWindow::new_(&p, 4096usize);
    let a = w.report();
    let b = w.report();
    assert_eq!(a, b);
    assert_eq!(w.reported_window(), Option::Some(4096u32));
    assert_eq!(w.recv_total(), 0u64);
    assert!(!w.should_report(&p));
}
