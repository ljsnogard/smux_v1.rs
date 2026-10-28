//! 连接级临时对象域（`sw_scope::Scope`）。
//!
//! 这里只承载**生命周期短于子流 / Telegraph、且不需要 `Allocator` 接口**的对象：
//! 请求完成源（`Tcs_`）与循环暂存（`[u8]`）。其余分配用途仍走
//! [`TrConnAllocCfg`](crate::connection::TrConnAllocCfg)——理由见
//! `dev-notes/sw-scope-20261028-2130.md` §4.2。
//!
//! # 所有权
//!
//! 每个连接一个 [`Scope`]，由注册表持有（[`ChannelRegistry_`] 的直接字段），
//! 随连接关闭显式 `close`。域内对象用 `Retain<T>` 作为句柄；访问时临时
//! `try_owning()` / `try_sharing()` 出借用视图。
//!
//! # 分配器
//!
//! 域的**池分配器**取连接配置的 `RegistryAlloc`（见 [`new_conn_scope_`]），因此对象
//! 实际从注入分配器上分配。域的**实体**与默认 root 的 B+ 树节点由 sw-scope 的默认
//! root 承担，那部分固定走全局 `Global`——每连接一次，不是每请求。
//!
//! # 页大小
//!
//! 取 64 KiB：循环暂存的上限是 `max_packet_size`（部署建议 ≤ 64 KiB），页大小取
//! 同量级可以让一次暂存落在同一个强池里，避免一条连接频繁开池。

extern crate alloc;

use core::alloc::{Allocator, Layout};

use crate::x_deps::sw_scope;
use sw_scope::{IntoEmplace, Retain, Scope, ScopeError, TrScope, TrScopeInnerConfig};

/// 连接级 Scope 的域配置。
///
/// `PoolAlloc` 只用于满足 [`TrScopeInnerConfig`] 的类型约束；真正的池分配器是
/// [`new_conn_scope_`] 传进去的那个具体实例（可以不是 `Global`）。`LockAlloc`
/// 在当前 sw-scope 实现里尚未接入（锁固定用 `&'static dyn Allocator`，指向 root
/// 的分配器），这里填 `Global` 只为占位。
struct ConnScopeCfg;

impl TrScopeInnerConfig for ConnScopeCfg {
    type PoolAlloc = alloc::alloc::Global;
    type LockAlloc = alloc::alloc::Global;
    type PageSize = sw_scope::page_size::PageSize64KB;
}

/// 用调用方注入的分配器建一个连接级 Scope。
///
/// `alloc` 是**每域一份**的池分配器，会被内联进域实体尾部，之后本域所有对象都从它
/// 分配。调用方通常传 `cfg.registry_alloc()`。
///
/// # Panics
///
/// sw-scope 的默认 root 无法发布、或域实体分配失败时 panic。前者只在进程级
/// 分配器耗尽时发生，与标准库容器 OOM 的行为一致。
pub(crate) fn new_conn_scope_<A>(alloc: A) -> Scope
where
    A: Allocator + Send + Sync + 'static,
{
    Scope::new_with_alloc_value::<A, ConnScopeCfg>(alloc)
}

/// 在连接级 Scope 里放一块**零初始化**的 `[u8]`。
///
/// 这是循环暂存的分配入口：调用方拿到 `Retain<[u8]>` 后，用
/// [`try_owning`](Retain::try_owning) 借出 `Owning<[u8]>`，再把 `&mut [u8]` 交给
/// 搬运逻辑。零初始化让“只写不读”的暂存也能安全地按 `[u8]`（而不是
/// `[MaybeUninit<u8>]`）使用。
///
/// # Errors
///
/// `len` 对应的布局溢出，或域已关闭 / 池分配失败时返回 [`ScopeError`]。
pub(crate) async fn alloc_zeroed_bytes_async_(
    scope: &Scope,
    len: usize,
) -> Result<Retain<[u8]>, ScopeError> {
    let layout = Layout::array::<u8>(len).map_err(|_| ScopeError::MallocFailed)?;
    let emplace = IntoEmplace::<_, [u8]>::new(
        |_layout, place: *mut [u8]| {
            // SAFETY: `place` 指向 arena 按 `layout` 划出、尚未初始化的 `len` 字节
            // 数据区；`write_bytes` 恰好写满这块区域，不越过布局边界。
            unsafe { core::ptr::write_bytes(place.cast::<u8>(), 0u8, len) };
        },
        len,
    );
    // SAFETY: 上面的闭包写满布局指定的区域，`[u8]` 没有析构需求，满足
    // `TrScope::emplace_async` 的安全契约。
    let fut = unsafe { scope.emplace_async(layout, emplace) };
    fut.await
}

/// 显式关闭连接级 Scope：强制清盘本域对象，随后由视图释放路径收尾。
///
/// 消费的是一个**克隆句柄**，注册表里的原句柄不受影响；关闭标记对本域所有句柄
/// 可见。清盘会跳过仍存活的 `Owning` / `Sharing`，等它们释放后重试。
pub(crate) fn close_conn_scope_(scope: &Scope) {
    scope.clone().close();
}
