use crate::flow_ctrl::{RecvWindow, SendWindow, TrFlowCtrlPolicy};

/// 一条子流**双向**流控状态的聚合。
///
/// 子流对象持有它，`Tx` 侧只访问 [`FlowCtrl::send_window`]，`Rx` 侧只访问
/// [`FlowCtrl::recv_window`]，两个方向互不干扰。
///
/// # 生命周期：`new_empty_` → `install_`
///
/// 子流的**身份记录**先于「环容量」出现：读循环在收到对端 `OPEN` 时就要登记身份，
/// 而接收窗口由调用方在最终裁决（`accept_async`）给出的接收缓冲容量决定。因此共享
/// 状态节点建立时 [`FlowCtrl`] 是**空**的（`new_empty_`），等拿到容量再 `install_`。
/// 两个入口都是 `&self`：字段本身是原子，安装只是一次存储。
///
/// 这也解释了本类型为什么不再有 `send_window_mut` / `recv_window_mut`：可变借出与
/// 「状态被多个任务经共享句柄持有」不相容，而原子访问不需要可变借用。
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
        let ctrl = Self::new_empty_();
        ctrl.install_(policy, ring_capacity);
        ctrl
    }

    /// 建一条**尚未安装**的双向窗口（容量/阈值待 `install_`）。
    pub(crate) const fn new_empty_() -> Self {
        FlowCtrl {
            send_: SendWindow::new_empty_(),
            recv_: RecvWindow::new_empty_(),
        }
    }

    /// 安装窗口参数：发送侧的上限与接收侧的容量/阈值/epoch 规格。
    pub(crate) fn install_<P>(&self, policy: &P, ring_capacity: usize)
    where
        P: TrFlowCtrlPolicy,
    {
        self.send_.install_(policy.max_window());
        self.recv_.install_(policy, ring_capacity);
    }

    /// 发送窗口（`Tx` 侧使用）。
    pub const fn send_window(&self) -> &SendWindow {
        &self.send_
    }

    /// 接收窗口（`Rx` 侧使用）。
    pub const fn recv_window(&self) -> &RecvWindow {
        &self.recv_
    }
}
