//! 连接内索引树的**单 chunk 大小**配置。
//!
//! 上游 `bplus_tree_cmp` 用 [`TrConfig::CHUNK_SIZE`] 控制 `SlabList` 每次分配的
//! chunk 字节数。连接级索引表（身份表、dock 表、反向索引、宽限到期索引、数据面
//! 本地表）都是「条目少、churn 高」的小表：上游默认 64 KiB chunk 会在小连接上把
//! 固定内存放大到数百 KiB。
//!
//! 因此本 crate **暂时统一取 2 KiB**（[`K_DOCK_TREE_CHUNK_SIZE`]）。这是连接级
//! 常量，不是公开配置；需要按连接调整时再经 `TrConnCfg` 暴露（Q16）。
//!
//! `BPlusTree` / `BTree` 的最后一个泛型参数就是本模块的 [`DockTreeCfg_`]：
//!
//! ```text
//! BPlusTree<K_DOCK_TREE_DEGREE, OrdComparer<(Dock, Dock)>, (Dock, Dock), V, DockTreeCfg_<A>>
//! ```
//!
//! 具体树的类型别名在 P1.5 的 `dock_index_` 里定义；本模块只负责「chunk 大小 +
//! 分配器」这一个类型级事实。

// 过渡：P1.5 建 `dock_index_` 时使用；此前先让配置与常量为后续阶段就位。
#![allow(dead_code)]

use bplustree_cmp::{CHUNK_SIZE_2K, SlabListConfig};

/// 本 crate 索引树单 chunk 的默认字节数（2 KiB）。
///
/// 取值理由见模块文档：小表 + 高 churn 下，2 KiB 比上游默认 64 KiB 的固定开销小
/// 两个数量级，chunk 内槽位仍然复用，不会退化成逐条分配。
pub(crate) const K_DOCK_TREE_CHUNK_SIZE: usize = CHUNK_SIZE_2K;

/// 索引树的类型级配置：2 KiB chunk + 调用方注入的分配器。
///
/// `A` 是 `TrConnCfg::Alloc`，因此每棵树的 `SlabList` 都走同一个注入分配器，
/// 不会隐式落到全局分配器。
pub(crate) type DockTreeCfg_<A> = SlabListConfig<K_DOCK_TREE_CHUNK_SIZE, A>;

#[cfg(test)]
mod tests_ {
    use core::alloc::Allocator;

    use bplustree_cmp::{BPlusTree, OrdComparer, TrConfig};
    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use super::*;

    /// 测试本 crate 的索引树配置确实是 2 KiB chunk。
    /// - 手段：读取 `DockTreeCfg_<CoreAlloc>` 的 `CHUNK_SIZE` 关联常量。
    /// - 判断：等于 2 * 1024，且不等于上游默认的 64 KiB。
    ///
    /// Test goal: the crate's tree config really uses a 2 KiB chunk.
    /// Method: read `<DockTreeCfg_<CoreAlloc> as TrConfig>::CHUNK_SIZE`.
    /// Assertion: it equals 2 * 1024 and differs from the upstream 64 KiB default.
    #[test]
    fn dock_tree_cfg_uses_2k_chunk_() {
        assert_eq!(
            <DockTreeCfg_<CoreAlloc> as TrConfig>::CHUNK_SIZE,
            2usize * 1024usize
        );
        assert_ne!(
            <DockTreeCfg_<CoreAlloc> as TrConfig>::CHUNK_SIZE,
            bplustree_cmp::DEFAULT_CHUNK_BYTES
        );
    }

    /// 测试用 2 KiB 配置建出的 `BPlusTree` 能正常插入与有序迭代。
    /// - 手段：`BPlusTree::<4, OrdComparer<u64>, u64, u64, DockTreeCfg_<CoreAlloc>>::new_in`
    ///   建树，插入 1..=64，回收迭代结果。
    /// - 判断：迭代结果严格升序且数量为 64（chunk 小不影响树语义）。
    ///
    /// Test goal: a `BPlusTree` built with the 2 KiB config still inserts and iterates
    /// in order.
    /// Method: build via `new_in` with `DockTreeCfg_<CoreAlloc>`, insert 1..=64, collect.
    /// Assertion: exactly 64 keys in strictly increasing order.
    #[test]
    fn dock_tree_cfg_builds_working_bplus_tree_() {
        let mut tree: BPlusTree<4, OrdComparer<u64>, u64, u64, DockTreeCfg_<CoreAlloc>> =
            BPlusTree::new_in(CoreAlloc).expect("2 KiB chunk 足以容纳一个节点");
        for key in 1u64..=64u64 {
            tree.insert(key, key);
        }
        let keys: std::vec::Vec<u64> = tree.iter().map(|(key, _value)| *key).collect();
        assert_eq!(keys, (1u64..=64u64).collect::<std::vec::Vec<u64>>());
    }

    /// 测试分配器类型由配置决定（不是全局分配器）。
    /// - 手段：把 `DockTreeCfg_<CoreAlloc>` 的 `Allocator` 关联类型绑定到 `CoreAlloc`
    ///   实例上（类型不同则编译失败），再按 `Allocator` 约束消费。
    /// - 判断：编译通过（关联类型确实是 `CoreAlloc`），且 `CoreAlloc: Allocator` 成立。
    ///
    /// Test goal: the config's allocator is the injected one, not `Global`.
    /// Method: bind the associated `Allocator` type to a `CoreAlloc` value (a type
    /// mismatch would fail to compile), then consume it under `A: Allocator`.
    /// Assertion: it compiles (the type is `CoreAlloc`) and `CoreAlloc: Allocator`.
    #[test]
    fn dock_tree_cfg_keeps_injected_allocator_() {
        let alloc: <DockTreeCfg_<CoreAlloc> as TrConfig>::Allocator = CoreAlloc;
        fn assert_allocator_<A: Allocator>(_value: A) {}
        assert_allocator_(alloc);
    }
}
