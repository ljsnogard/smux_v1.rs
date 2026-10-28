//! 控制面共享状态：定容 MPMC 请求通道 + `main_loop` 的合并提示位。
//!
//! API future 把一次非荷载请求（bind / listen / reserve …）连同完成源塞进
//! [`ControlState_::submit_async_`]；`main_loop` 从另一端取走并处理。
//!
//! # 为什么用 `mpmc_chan` 而不是自己维护等待者
//!
//! 请求队列是**多生产者、单消费者**：生产者是任意线程上的 API future，消费者是
//! `main_loop`。并发建流时可能有远超队列容量的生产者在等空位（cross_thread 场景
//! 实测 256 并发），因此等待者集合**不能有固定上界**。
//!
//! `mpmc_chan` 的等待队列是 chunked `SlabList`：逻辑上无上界，物理上按 chunk
//! （这里取 4 KiB）批量申请、槽位复用，取消/丢弃时节点归还 slab 而不是全局分配器。
//! 于是「无上界等待」与「长期运行不产生碎片」同时成立，也不需要给调用方引入
//! `Busy` 重试契约。
//!
//! 通道的外层 `Arc` 与等待 chunk 都用 [`crate::connection::TrConnAllocCfg::MpmcAlloc`]（经
//! [`MuxChanCfg_`]），不落到全局堆：每个连接只有这一条请求通道，分配次数固定、
//! 不随并发增长。
//!
//! 提示位与请求分开：`HINT_MAIN_*` 只表示「某类事实可能变了」，`main_loop` 一次取走
//! 整个掩码后自己决定扫描什么（见设计稿 §6.1 / §8.4）。

use core::task::{Context, Poll};

use abs_cancel::{TrCancellationToken, TrMayCancel};
use mpmc_chan::{MpmcBuf, MpmcChan, RecvAsync, SendError, TryRecvError};
use mm_ptr::Shared;

use crate::{
    connection::{
        MpmcChanCfg_, TrConnAllocCfg,
        main_loop::MainLoopRequest_,
        slot_::{HintSlot_, PushErr_},
    },
    x_deps::{abs_cancel, mm_ptr},
};

/// 请求队列容量（设计稿 §10.2：64 条）。
pub(crate) const K_MAIN_REQ_CAP: usize = 64usize;

/// 「有身份被 `Drop` 置了 retired 位」：`main_loop` 扫描并释放。
pub(crate) const HINT_MAIN_RETIRED: u64 = 1u64 << 0usize;

/// 请求通道的缓冲区类型别名（`MpmcBuf` 由 `MpmcChan` 持有）。
type ControlBuf_<C> = MpmcBuf<MainLoopRequest_<C>, K_MAIN_REQ_CAP>;

/// 请求通道的发送端类型。
type ControlTx_<C> = mpmc_chan::Sender<
    'static,
    MainLoopRequest_<C>,
    K_MAIN_REQ_CAP,
    ControlBuf_<C>,
    MpmcChanCfg_<<C as TrConnAllocCfg>::MpmcAlloc<'static>>,
>;

/// 请求通道的接收端类型。
type ControlRx_<C> = mpmc_chan::Receiver<
    'static,
    MainLoopRequest_<C>,
    K_MAIN_REQ_CAP,
    ControlBuf_<C>,
    MpmcChanCfg_<<C as TrConnAllocCfg>::MpmcAlloc<'static>>,
>;

/// `main_loop` 的请求等待 future（`RecvAsync` 是参数载体，`into_future` 之后才登记唤醒）。
pub(crate) type ControlRecvAsync_<'f, C> = RecvAsync<
    'static,
    'f,
    'f,
    MainLoopRequest_<C>,
    ControlBuf_<C>,
    MpmcChanCfg_<<C as TrConnAllocCfg>::MpmcAlloc<'static>>,
    K_MAIN_REQ_CAP,
>;

/// 控制面共享状态句柄（`MuxCore` 与 `main_loop` 各持一份）。
pub(crate) struct ControlState_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 共享本体；一次分配，句柄可克隆。
    inner_: Shared<ControlStateInner_<C>, C::RegistryAlloc>,
}

impl<C> Clone for ControlState_<C>
where
    C: TrConnAllocCfg + 'static,
{
    fn clone(&self) -> Self {
        ControlState_ {
            inner_: self.inner_.clone(),
        }
    }
}

/// 共享本体的内容。
struct ControlStateInner_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 请求通道的发送端（多生产者共享；`Sender` 是 `Arc` 句柄，克隆廉价）。
    tx_: ControlTx_<C>,

    /// 请求通道的接收端（`main_loop` 独占）。
    rx_: ControlRx_<C>,

    /// `main_loop` 的合并提示位（`HINT_MAIN_*`）。
    main_hint_: HintSlot_,
}

impl<C> ControlState_<C>
where
    C: TrConnAllocCfg + 'static,
{
    /// 建一条空的请求通道与提示位（一次 `Shared` 分配）。
    ///
    /// 通道的外层 `Arc` 与缓存用 `alloc` 分配；等待 chunk 在首次 park 时申请。
    pub(crate) fn new_(cfg: C) -> Self {
        let (tx_, rx_) = MpmcChan::<
            MainLoopRequest_<C>,
            K_MAIN_REQ_CAP,
            _,
            MpmcChanCfg_<<C as TrConnAllocCfg>::MpmcAlloc<'static>>,
        >::new_in(ControlBuf_::<C>::new(), cfg.mpmc_alloc());
        ControlState_ {
            inner_: Shared::new(
                ControlStateInner_ {
                    tx_,
                    rx_,
                    main_hint_: HintSlot_::new_(),
                },
                cfg.registry_alloc(),
            ),
        }
    }

    /// 提交一个请求；通道满时进入等待队列（可被取消）。
    ///
    /// # Errors
    ///
    /// - [`PushErr_::Closed`]：`main_loop` 已退出（连接正在收尾）；
    /// - [`PushErr_::Cancelled`]：等空位期间被取消；请求原样带回。
    pub(crate) async fn submit_async_<K>(
        &self,
        request: MainLoopRequest_<C>,
        cancel: K,
    ) -> Result<(), PushErr_<MainLoopRequest_<C>>>
    where
        K: TrCancellationToken,
    {
        match self
            .inner_
            .tx_
            .send_async(request)
            .may_cancel_with(cancel)
            .await
        {
            Result::Ok(()) => Result::Ok(()),
            Result::Err(SendError::Closed(item)) => Result::Err(PushErr_::Closed(item)),
            Result::Err(SendError::Cancelled(item)) => Result::Err(PushErr_::Cancelled(item)),
        }
    }

    /// 非阻塞取一个请求（`main_loop` 的批处理用）。
    ///
    /// 通道关闭后仍可 drain 已入队元素；空且关闭时返回 `None`。
    pub(crate) fn try_pop_request_(&self) -> Option<MainLoopRequest_<C>> {
        match self.inner_.rx_.try_recv() {
            Result::Ok(request) => Option::Some(request),
            Result::Err(TryRecvError::Empty) | Result::Err(TryRecvError::Closed) => Option::None,
        }
    }

    /// `main_loop` 的请求等待 future。
    ///
    /// 返回的 [`RecvAsync`] 是**参数载体**（实现 `IntoFuture`），调用方必须先
    /// `into_future` 再 `pin`，并**跨 poll 持有**它——每次 poll 新建会丢唤醒登记。
    pub(crate) fn recv_request_async_<'f>(&'f self) -> ControlRecvAsync_<'f, C> {
        self.inner_.rx_.recv_async()
    }

    /// 关闭请求通道：此后 [`Self::submit_async_`] 一律返回 [`PushErr_::Closed`]，
    /// 消费者仍能 drain 已入队元素，空后 [`Self::try_pop_request_`] 返回 `None`。
    pub(crate) fn close_requests_(&self) {
        self.inner_.rx_.close();
    }

    /// 置位若干 `main_loop` 提示并唤醒它；幂等、零分配。
    ///
    /// 与请求通道无关：提示位仍是定容合并掩码，不参与队列背压。
    pub(crate) fn raise_main_hint_(&self, bits: u64) {
        self.inner_.main_hint_.raise_(bits);
    }

    /// 取走并清除当前全部提示位。
    pub(crate) fn take_main_hint_(&self) -> u64 {
        self.inner_.main_hint_.take_()
    }

    /// `main_loop` 的 park 点：有提示时返回掩码。
    pub(crate) fn poll_main_hint_(&self, cx: &mut Context<'_>) -> Poll<u64> {
        self.inner_.main_hint_.poll_wait_(cx)
    }
}

#[cfg(test)]
mod tests_ {
    use abs_cancel::NonCancellableToken;
    use super::*;
    use crate::connection::{Dock, SingleConnAllocCfg, Tcs_};

    /// 测试请求通道的 FIFO 与关闭语义。
    /// - 手段：依次提交两条 `Bind`（dock 1 / 2），逐条 `try_pop_request_`；随后
    ///   `close_requests_()` 再提交第三条。
    /// - 判断：两次 pop 的 dock 依次为 1 / 2；关闭后的提交返回 `PushErr_::Closed`。
    ///
    /// Test goal: the request channel is FIFO and rejects submissions after close.
    /// Method: submit two `Bind` requests (docks 1 and 2) and pop them one by one;
    /// then `close_requests_()` and submit a third.
    /// Assertion: pops yield docks 1 then 2; the third submit is `PushErr_::Closed`.
    #[test]
    fn control_state_is_fifo_and_closes_() {
        let cfg = SingleConnAllocCfg::default();
        let scope = crate::connection::new_test_scope_();
        let ctl = ControlState_::<SingleConnAllocCfg>::new_(cfg);
        for dock in [1u32, 2u32] {
            let reply = futures::executor::block_on(Tcs_::new_(&scope)).expect("测试域可用");
            assert!(
                futures::executor::block_on(ctl.submit_async_(
                    MainLoopRequest_::Bind {
                        local_dock: Dock::new(dock),
                        reply,
                    },
                    NonCancellableToken::new(),
                ))
                .is_ok(),
                "空通道应当立刻入队"
            );
        }
        for expected in [1u32, 2u32] {
            match ctl.try_pop_request_() {
                Option::Some(MainLoopRequest_::Bind { local_dock, .. }) => {
                    assert_eq!(local_dock, Dock::new(expected));
                }
                _ => panic!("应当取到第 {expected} 条 Bind 请求"),
            }
        }
        assert!(ctl.try_pop_request_().is_none());

        ctl.close_requests_();
        let reply = futures::executor::block_on(Tcs_::new_(&scope)).expect("测试域可用");
        let err = futures::executor::block_on(ctl.submit_async_(
            MainLoopRequest_::Bind {
                local_dock: Dock::new(3u32),
                reply,
            },
            NonCancellableToken::new(),
        ))
        .expect_err("关闭后应当拒绝入队");
        assert!(matches!(err, PushErr_::Closed(_)));
    }
}
