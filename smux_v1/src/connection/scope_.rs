//! crate 级 `RootScope` 与连接级临时对象域（`sw_scope::Scope`）。
//!
//! 这里只承载**生命周期短于子流 / Telegraph、且不需要 `Allocator` 接口**的对象：
//! 请求完成源（`Tcs_`）与循环暂存（`[u8]`）。其余分配用途仍走
//! [`TrConnAllocCfg`](crate::connection::TrConnAllocCfg)——理由见
//! `dev-notes/sw-scope-20261028-2130.md` §4.2。
//!
//! # 两级结构：crate 级 root + 每连接一个域
//!
//! ```text
//! CONN_ROOT（OnceLock<&'static RootScope>，进程内唯一）
//!   └── Scope（每连接一个，池分配器 = RegistryAlloc）
//!         ├── TcsInner_<T>
//!         └── [u8] 循环暂存
//! ```
//!
//! root 由本模块显式创建并发布到 `Box::leak` 出来的静态槽位，**不**使用 sw-scope
//! 内部的默认 root（新 API 已不再有隐式默认 root）。这样 root 的存储分配器与页大小
//! 都由本 crate 决定；连接关闭只 `close` 自己的域，root 留在进程生命期，不参与收尾。
//!
//! # 分配器
//!
//! - **root 的存储与实体池**：`Global`。root 是进程级单例，没有连接配置可注入；
//!   每个连接域实体从 root 的实体池按 64 KiB 页分配，因此全局堆调用是**按池**而不是
//!   按请求。
//! - **域内对象**：调用方注入的 `RegistryAlloc`（见 [`new_conn_scope_`]）。
//!
//! # 页大小
//!
//! 域页大小取 64 KiB：循环暂存的上限是 `max_packet_size`（部署建议 ≤ 64 KiB），
//! 同量级的页可以让一次暂存落在同一个强池里，避免一条连接频繁开池。root 的实体池
//! 页大小取同一个值，供多条连接的域实体共享池块。

extern crate alloc;

use core::{
    alloc::{Allocator, Layout},
    ptr,
    sync::atomic::AtomicPtr,
};
use std::sync::OnceLock;

use sw_scope::{IntoEmplace, Retain, RootScope, Scope, ScopeError, TrScope, TrScopeInnerConfig};

use crate::x_deps::sw_scope;

/// root 实体池与域页的缺省大小。两者取同一量级，理由见模块文档「页大小」。
const K_SCOPE_PAGE_SIZE: usize = 64usize * 1024usize;

/// crate 级 `RootScope` 的缓存。
///
/// `OnceLock` 保证初始化闭包只执行一次；并发调用会等先到者发布完成后再读缓存，
/// 因此不存在多个线程同时拿到 `&'static mut` 发布槽位的问题。
static CONN_ROOT: OnceLock<&'static RootScope> = OnceLock::new();

/// 取 crate 级 `RootScope`，首次调用时创建。
///
/// root 的发布槽位由 `Box::leak` 提供（一个 `AtomicPtr`，进程生命期）；root 自身
/// 用 `Global` 分配，也不在本 crate 里 `close`，因此不需要回收路径。
///
/// # Panics
///
/// root 存储或类型表分配失败时 panic。root 是整棵树的根，没有可恢复的调用方；
/// 这与标准库容器在 OOM 时的行为一致。
fn conn_root_() -> &'static RootScope {
    CONN_ROOT.get_or_init(|| {
        // 发布槽位要求 `&'static mut`。`OnceLock` 保证本闭包只执行一次，因此这个
        // 可变引用在进程内唯一，不会与其它线程的引用别名。
        let slot: &'static mut AtomicPtr<RootScope> =
            alloc::boxed::Box::leak(alloc::boxed::Box::new(AtomicPtr::new(ptr::null_mut())));
        match RootScope::try_init_in(slot, K_SCOPE_PAGE_SIZE, alloc::alloc::Global) {
            Result::Ok(root) | Result::Err(root) => root,
        }
    })
}

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

/// 用调用方注入的分配器在 crate 级 root 下建一个连接级 Scope。
///
/// `alloc` 是**每域一份**的池分配器，会被内联进域实体尾部，之后本域所有对象都从它
/// 分配。调用方通常传 `cfg.registry_alloc()`。
///
/// # Errors
///
/// root 已关闭（本 crate 不会关闭它）或域实体分配失败时返回 [`ScopeError`]。
pub(crate) fn new_conn_scope_<A>(alloc: A) -> Result<Scope, ScopeError>
where
    A: Allocator + Send + Sync + 'static,
{
    Scope::new_in_with_alloc_value::<A, ConnScopeCfg>(conn_root_(), alloc)
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
///
/// 这里只关域，**不关 root**：crate 级 root 供所有连接共享，随进程结束。
pub(crate) fn close_conn_scope_(scope: &Scope) {
    scope.clone().close();
}

/// 测试用：在 crate 级 root 下建一个默认配置的域。
///
/// 单元测试共享 crate 级 root（与生产路径同构），但每个用例拿自己的域，因此
/// `close` 一个域不会影响别的用例。
#[cfg(test)]
pub(crate) fn new_test_scope_() -> Scope {
    match Scope::new_in(conn_root_()) {
        Result::Ok(scope) => scope,
        Result::Err(_) => panic!("测试域创建失败"),
    }
}
