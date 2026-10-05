use super::*;

/// 2 字节 epoch 的策略：累计量涨到 `u16::MAX` 就重置，用于验证重置路径。
struct TinyEpochPolicy;

impl TrFlowCtrlPolicy for TinyEpochPolicy {
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
        8u64
    }
}

/// 测试累计量涨到 epoch 规格时产出重置变体，且携带的是**重置前**的绝对量。
/// - 手段：用 2 字节 epoch 策略（阈值为 8）建立接收窗口，通告一次后收 9 字节。
/// - 判断：`reset_due` 为真；`should_report` 为真（不受频率限制）；`report()`
///   返回重置变体，其 `recv_total()` 是重置前的绝对量 9；重置后
///   `encoded_recv_total()` 归零，而绝对量仍是 9。
#[test]
fn recv_window_reset_carries_pre_reset_total() {
    let p = TinyEpochPolicy;
    let mut w = RecvWindow::new_(&p, 4096usize);
    assert!(!w.reset_due());
    w.report();

    for _ in 0..4 {
        w.on_data(3u32).expect("在额度内");
    }
    assert_eq!(w.recv_total(), 12u64);
    assert!(w.reset_due(), "已经超过 epoch 规格 8");

    // 频率限制已满足，但即使没满足，重置也必须能发出去。
    let report = w.report();
    assert!(report.is_reset(), "应当产出重置变体");
    assert_eq!(report.recv_total(), 12u64, "携带重置前的绝对累计量");
    assert_eq!(report.window(), 4096u32 - 12u32);

    assert_eq!(w.recv_total(), 12u64, "绝对量继续单调");
    assert_eq!(w.encoded_recv_total(), 0u64, "新 epoch 从 0 起算");
    assert!(!w.reset_due());

    // 之后的普通通告携带的是 epoch 内的小值。
    for _ in 0..4 {
        w.on_data(1u32).expect("在额度内");
    }
    let next = w.report();
    assert!(!next.is_reset());
    assert_eq!(next.recv_total(), 4u64, "epoch 内累计量");
}

/// 测试重置不受频率限制影响（频率限制只约束按分区的阈值通告）。
/// - 手段：2 字节 epoch 策略下通告一次，收 1 个数据帧就把累计量推过规格。
/// - 判断：变动量远小于非临界区门限时 `should_report` 仍为真（重置是编码前提），
///   且 `report()` 产出重置变体。
#[test]
fn reset_bypasses_report_rate_limit() {
    let p = TinyEpochPolicy;
    let mut w = RecvWindow::new_(&p, 64usize);
    w.report();

    w.on_data(9u32).expect("在额度内");
    assert_eq!(w.recv_total(), 9u64);
    assert!(
        w.should_report(&p),
        "重置是编码前提，不受最小帧间隔约束"
    );
    assert!(w.report().is_reset());
}

/// 测试发送窗口在收到重置变体后按「重置前累计量」rebase。
/// - 手段：接通告 `(0, 100)`、预扣 30；再收重置变体 `(30, 50)`；随后预扣 10
///   并收普通通告 `(5, 40)`（epoch 内 5 → 绝对 35）。
/// - 判断：重置后可用 = 50（在途 0）；普通通告后可用 = 40 − (40 − 35) = 35。
#[test]
fn send_window_rebases_on_reset_report() {
    let mut w = SendWindow::new_(1024u32);
    w.on_report(WindowReport::new(0u64, 100u32)).expect("通告");
    assert_eq!(w.reserve(30u32), 30u32);
    assert_eq!(w.available(), 70u32);

    w.on_report(WindowReport::new_reset(30u64, 50u32))
        .expect("重置变体");
    assert_eq!(w.peer_epoch_base(), 30u64);
    assert_eq!(w.available(), 50u32, "在途为 30 − 30 = 0");

    assert_eq!(w.reserve(10u32), 10u32);
    assert_eq!(w.available(), 40u32);
    w.on_report(WindowReport::new(5u64, 40u32))
        .expect("epoch 内的普通通告");
    assert_eq!(
        w.available(),
        35u32,
        "绝对已收 = 30 + 5 = 35，在途 = 40 − 35 = 5"
    );

    // 重置变体之后，比它更旧的重置快照被忽略。
    w.on_report(WindowReport::new_reset(10u64, 999u32))
        .expect("更旧的重置快照不是错误");
    assert_eq!(w.peer_epoch_base(), 30u64, "epoch 起点不回退");
}
