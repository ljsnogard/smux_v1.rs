use crate::flow_ctrl::{RecvWindow, SendWindow, TrFlowCtrlPolicy};

/// 一条子流**双向**流控状态的聚合。///
/// 子流对象持有它，`Tx` 侧只访问 [`FlowCtrl::send_window`]，`Rx` 侧只访问
/// [`FlowCtrl::recv_window`]，两个方向互不干扰。
pub struct FlowCtrl {
    send_: SendWindow,
    recv_: RecvWindow,
}


impl FlowCtrl {
    /// 按策略与接收环容量为一条子流建立双向窗口。
    ///
    /// 接收侧的初始窗口由 `OPEN` 通告出去；发送侧**此时还没有额度**，要等收到对端
    /// 那条 `OPEN` 里的通告（[`WindowReport`](crate::flow_ctrl::WindowReport)）才生效。
    pub fn new<P>(policy: &P, ring_capacity: usize) -> Self
    where
        P: TrFlowCtrlPolicy,
    {
        let max = policy.max_window();
        FlowCtrl {
            send_: SendWindow::new_(max),
            recv_: RecvWindow::new_(policy, ring_capacity),
        }
    }

    /// 发送窗口（`Tx` 侧使用）。
    pub const fn send_window(&self) -> &SendWindow {
        &self.send_
    }

    /// 发送窗口（`Tx` 侧使用，可变）。
    pub fn send_window_mut(&mut self) -> &mut SendWindow {
        &mut self.send_
    }

    /// 接收窗口（`Rx` 侧使用）。
    pub const fn recv_window(&self) -> &RecvWindow {
        &self.recv_
    }

    /// 接收窗口（`Rx` 侧使用，可变）。
    pub fn recv_window_mut(&mut self) -> &mut RecvWindow {
        &mut self.recv_
    }
}
