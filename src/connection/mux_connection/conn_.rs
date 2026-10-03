use abs_buff::gen_may_cancel_future;
use abs_art::{TrJoinHandle, TrLocalScope};
use abs_cancel::TrCancellationToken;
use abs_smux::{conn::TrConnection, dock::TrDock};
use buffex::x_deps::{abs_buff, abs_cancel};
use mm_ptr::{Owned, Shared};

use crate::{
    connection::{
        Dock, MuxError, TrConnCfg,
        dock_binding::DockBinding,
        error_::face_error_impls,
        session_::{LoopShared_, read_loop_async_, write_loop_async_},
        signal_::{EventReceiver_, ReadEvent_, WriteEvent_, event_channel_},
    },
    handshake::{agent::HandshakeDelivery, opts::HandshakeOpts},
};

use super::{core_::MuxCore, registry_::ChannelRegistry_};


/// [`TrConnection`](abs_smux::conn::TrConnection) 的错误类型：目前只有 `bind_async`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindError {
    /// 要绑定的 dock 是协议保留值（`unspecified` / `wildcard`），不能当身份用。
    ReservedDock,

    /// 该 local_dock 已被占用。
    DockInUse,

    /// 连接级失败。
    Mux(MuxError),
}

face_error_impls!(
    BindError,
    BindError::ReservedDock => "要绑定的 dock 是协议保留值，不能作为身份",
    BindError::DockInUse => "该 local_dock 已被占用",
);

/// 复用连接：**对一个 `MuxCore` 的智能指针的薄封装**，同时实现
/// [`TrConnection`]。
///
/// 泛型参数：
///
/// - `C`：资源策略，见 [`TrConnCfg`]；
/// - `S`：调用方注入的本地作用域类型（`abs_art::TrLocalScope` 的实现值）。
///
/// 收发半边类型由 `C::ConnTx` / `C::ConnRx` 给出，不再出现在公开类型上。
pub struct MuxConnection<C, S>
where
    C: TrConnCfg,
{
    /// 指向演员核心的强引用；最后一个强引用消失时核心析构（并触发收尾）。
    core_: Shared<MuxCore<C, S>, C::Alloc>,
}

impl<C, S> Clone for MuxConnection<C, S>
where
    C: TrConnCfg,
{
    /// 克隆即多一个强引用：核心不复制，句柄因此可以与任意多个对象共享同一连接。
    fn clone(&self) -> Self {
        MuxConnection {
            core_: self.core_.clone(),
        }
    }
}

impl<C, S> MuxConnection<C, S>
where
    C: TrConnCfg,
    S: TrLocalScope + Clone,
{
    /// 由一次成功的握手交付物、调用方的本地作用域与资源策略构造连接：接管
    /// `C::ConnRx` / `C::ConnTx`，并**经作用域 `spawn_local`** 投递读 / 写两个循环。
    pub fn new(
        scope: &S,
        delivery: HandshakeDelivery<C::ConnTx, C::ConnRx>,
        config: C,
    ) -> Self
    where
        C: 'static,
        C::Alloc: 'static,
        S: 'static,
    {
        let HandshakeDelivery { opts, tx, rx } = delivery;
        let bundle = CoreBundle_::build_(scope, opts, config);
        let CoreBundle_ {
            core_,
            shared_,
            w_receiver_,
            r_receiver_,
            alloc_,
        } = bundle;
        let max_packet_size = core_.opts_().basic_opts.max_packet_size;
        // 投递也走核心里的那一份作用域：调用方给出的值只用来克隆保活。
        let scope = core_.scope_();

        // 帧暂存一律走调用方注入的分配器（`mm_ptr::Owned`），不再落到全局分配器。
        let read_fut = read_loop_async_::<C, _>(
            rx,
            shared_.clone(),
            r_receiver_,
            core_.w_events_().clone(),
            Owned::new_slice(
                max_packet_size,
                |_idx, slot| {
                    slot.write(0u8);
                },
                alloc_.clone(),
            ),
            core_.loop_token_(0usize),
        );
        scope.spawn_local(read_fut).detach();

        let write_fut = write_loop_async_::<C, _>(
            tx,
            shared_,
            w_receiver_,
            core_.r_events_().clone(),
            core_.loop_token_(1usize),
        );
        scope.spawn_local(write_fut).detach();

        MuxConnection { core_ }
    }
}

impl<C, S> MuxConnection<C, S>
where
    C: TrConnCfg,
{
    /// 演员核心（crate 内部句柄与两个循环都经它访问共享状态）。
    pub(crate) fn core_(&self) -> &MuxCore<C, S> {
        &self.core_
    }
}

/// 建连的中间产物：核心 + 两个循环共享的一份量 + 两条事件接收端 + 分配器。
struct CoreBundle_<C, S>
where
    C: TrConnCfg,
{
    core_: Shared<MuxCore<C, S>, C::Alloc>,
    shared_: LoopShared_<C::Alloc>,
    w_receiver_: EventReceiver_<WriteEvent_<C::Buff, C::Alloc>>,
    r_receiver_: EventReceiver_<ReadEvent_<C::Buff, C::Alloc>>,
    alloc_: C::Alloc,
}

impl<C, S> CoreBundle_<C, S>
where
    C: TrConnCfg,
{
    /// 展开策略、建注册表与两条事件通道，并把全部共享状态装进核心。
    fn build_(scope: &S, opts: HandshakeOpts, config: C) -> Self
    where
        S: Clone,
    {
        let max_packet_size = opts.basic_opts.max_packet_size;
        let alloc = config.allocator();

        let reg = ChannelRegistry_::new_(opts.basic_opts.clone(), alloc.clone());
        let (w_events, w_receiver) = event_channel_();
        let (r_events, r_receiver) = event_channel_();

        let shared = LoopShared_::new_(reg.clone(), max_packet_size);

        // 核心自持一份两个循环的取消令牌：`MuxCore::drop` 因此不必去注册表取锁
        // （那会阻塞，而 `Drop` 可能在任意线程上发生）。
        let loops = [reg.loop_token_(0usize), reg.loop_token_(1usize)];

        let core = Shared::new(
            MuxCore::new_(
                config,
                opts,
                scope.clone(),
                reg,
                w_events,
                r_events,
                loops,
            ),
            alloc.clone(),
        );

        CoreBundle_ {
            core_: core,
            shared_: shared,
            w_receiver_: w_receiver,
            r_receiver_: r_receiver,
            alloc_: alloc,
        }
    }
}

#[cfg(test)]
impl<C, S> MuxConnection<C, S>
where
    C: TrConnCfg,
{
    /// 测试专用构造：只建核心与两条事件通道，**不 spawn 任何循环**。
    pub(crate) fn new_test_(scope: &S, opts: HandshakeOpts, config: C) -> Self
    where
        S: Clone,
    {
        MuxConnection {
            core_: CoreBundle_::build_(scope, opts, config).core_,
        }
    }
}

impl<C, S> TrConnection<C> for MuxConnection<C, S>
where
    C: TrConnCfg,
{
    type Err = BindError;

    type DockBinding = DockBinding<C, S>;

    type BindAsync<'f> = MuxBindAsync<'f, 'f, C, S>
    where
        Self: 'f;

    fn bind_async<'f>(&'f self, local_dock: C::Dock) -> Self::BindAsync<'f> {
        MuxBindAsync::new(self, local_dock)
    }
}

/// [`TrConnection::bind_async`] 的 step 函数。
#[gen_may_cancel_future(MuxBind, pub, new(pub(crate)))]
async fn mux_bind_async_<'f, C, S, K>(
    conn: &'f MuxConnection<C, S>,
    local_dock: Dock,
    _cancel: K,
) -> Result<DockBinding<C, S>, BindError>
where
    C: TrConnCfg + 'f,
    S: 'f,
    K: TrCancellationToken,
{
    if local_dock.is_special() {
        return Result::Err(BindError::ReservedDock);
    }
    // 先清空释放邮箱：上一个 `DockBinding` 可能刚被丢弃（`Drop` 只投消息），
    // 不先落实就会把「已解绑」误判成 `DockInUse`——这是「丢弃后立刻重绑」这条
    // 既有约定的确定性来源。
    conn.core_().reg_().drain_session_events_();
    // 独占绑定：同一 local_dock 在任意时刻至多一个 `DockBinding`。
    conn.core_()
        .reg_()
        .bind_dock_(local_dock)
        .map_err(|_| BindError::DockInUse)?;
    Result::Ok(DockBinding::new_(conn.clone(), local_dock))
}
