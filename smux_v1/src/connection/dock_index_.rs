//! 连接内索引树的**类型别名与 degree**（2 KiB chunk，见 `tree_config_`）。
//!
//! 身份表与数据面本地表最终都用这里的别名建树；本模块只负责「degree + comparer +
//! chunk 配置」这一层，不含具体表的构造逻辑（P4 迁移时按表落地）。
//!
//! `BPlusTree` 的 `(K, V)` 地址在删除前稳定，适合值里带环半部的本地表；
//! `BTree` 单 arena、按值存放，适合 dock 级簿记与纯键集合。
//!
//! 与 `BTreeMap` 的两处 API 差异（迁移时每处都要改）：
//!
//! - `get` / `get_mut` 交出 `(&K, &V)` / `(&K, &mut V)`，不是单独的 `&V`；
//! - `new_in` 返回 `Result<_, TreeError>`。

// 过渡：P4 迁移各表时使用；此前先让别名与 degree 就位。
#![allow(dead_code)]

use bplustree_cmp::{BPlusTree, BTree, OrdComparer};

use crate::connection::tree_config_::DockTreeCfg_;

/// 连接内 B+ 树 / B 树的节点最大项数。
pub(crate) const K_DOCK_TREE_DEGREE: usize = 16usize;

/// 值地址稳定、适合携带环半部的 B+ 树（身份表 / 数据面本地表）。
pub(crate) type DockBPlusTree_<A, K, V> =
    BPlusTree<K_DOCK_TREE_DEGREE, OrdComparer<K>, K, V, DockTreeCfg_<A>>;

/// 单 arena、纯键集合或按值簿记的 B 树（dock 表 / 反向索引 / 宽限到期索引）。
pub(crate) type DockSet_<A, K> = BTree<K_DOCK_TREE_DEGREE, OrdComparer<K>, K, (), DockTreeCfg_<A>>;

#[cfg(test)]
mod tests_ {
    use alloc::{collections::BTreeMap, vec::Vec};

    use bplustree_cmp::TrConfig;
    use mm_ptr::x_deps::abs_mm::CoreAlloc;

    use super::*;
    use crate::connection::Dock;

    /// 测试 2 KiB B+ 树在 get / insert / 迭代 / range / remove 上与 `BTreeMap` 等价。
    /// - 手段：同一组 64 个 `(Dock,Dock)` 键同时插入 `DockBPlusTree_` 与 `BTreeMap`，
    ///   逐项比较 get、全序迭代、半开区间 range、偶数键删除后的 len。
    /// - 判断：每一步两边结果完全相同；`DockTreeCfg_` 的 `CHUNK_SIZE` 为 2 KiB。
    ///
    /// Test goal: the 2 KiB B+ tree behaves like `BTreeMap` for the operations the
    /// connection uses.
    /// Method: insert the same 64 `(Dock,Dock)` keys into both containers, then
    /// compare get, ordered iteration, a half-open range, and len after removing
    /// every even key.
    /// Assertion: every step matches; `DockTreeCfg_` uses a 2 KiB chunk.
    #[test]
    fn dock_tree_matches_btreemap_() {
        let mut tree: DockBPlusTree_<CoreAlloc, (Dock, Dock), u32> =
            DockBPlusTree_::new_in(CoreAlloc).expect("2 KiB chunk 足以容纳一个节点");
        let mut map: BTreeMap<(Dock, Dock), u32> = BTreeMap::new();
        for i in 0u32..64u32 {
            let key = (Dock::new(i), Dock::new(i + 1u32));
            tree.insert(key, i);
            map.insert(key, i);
        }
        for i in 0u32..64u32 {
            let key = (Dock::new(i), Dock::new(i + 1u32));
            assert_eq!(
                tree.get(&key).map(|(_, value)| *value),
                map.get(&key).copied()
            );
        }
        let tree_pairs: Vec<_> = tree.iter().map(|(key, value)| (*key, *value)).collect();
        let map_pairs: Vec<_> = map.iter().map(|(key, value)| (*key, *value)).collect();
        assert_eq!(tree_pairs, map_pairs);

        let lo = (Dock::new(10u32), Dock::new(11u32));
        let hi = (Dock::new(20u32), Dock::new(21u32));
        let tree_range: Vec<_> = tree
            .range(lo..hi)
            .map(|(key, value)| (*key, *value))
            .collect();
        let map_range: Vec<_> = map
            .range(lo..hi)
            .map(|(key, value)| (*key, *value))
            .collect();
        assert_eq!(tree_range, map_range);

        for i in (0u32..64u32).step_by(2usize) {
            let key = (Dock::new(i), Dock::new(i + 1u32));
            assert_eq!(tree.remove(&key), map.remove(&key));
        }
        assert_eq!(tree.len(), map.len());
        assert_eq!(
            <DockTreeCfg_<CoreAlloc> as TrConfig>::CHUNK_SIZE,
            2usize * 1024usize
        );
    }
}
