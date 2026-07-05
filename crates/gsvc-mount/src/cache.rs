//! Client-side caches for the mount core.
//!
//! Everything cached here is keyed by `(commit, path)` or `(commit, path, block)`, and commits are
//! immutable — so entries never invalidate, only evict. Negative lookups are cached the same way
//! (a path absent at a commit is absent forever), which is what makes stat-storm workloads cheap
//! after first touch.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;

use crate::client::{FileStat, TreeEntry};

/// A dependency-free LRU: `HashMap` for lookup plus a `BTreeMap<stamp, key>` recency index.
/// O(log n) per operation, exact LRU eviction. `weight` lets byte-budgeted caches (blocks) share
/// the implementation with count-budgeted ones (metadata, weight 1).
struct Lru<K: Clone + Eq + Hash, V> {
    map: HashMap<K, (V, u64, u64)>, // value, stamp, weight
    order: BTreeMap<u64, K>,
    budget: u64,
    used: u64,
    next_stamp: u64,
}

impl<K: Clone + Eq + Hash, V: Clone> Lru<K, V> {
    fn new(budget: u64) -> Self {
        Lru {
            map: HashMap::new(),
            order: BTreeMap::new(),
            budget: budget.max(1),
            used: 0,
            next_stamp: 0,
        }
    }

    fn get(&mut self, key: &K) -> Option<V> {
        let stamp = self.next_stamp;
        let (value, old_stamp, _) = self.map.get_mut(key)?;
        let value = value.clone();
        self.order.remove(&std::mem::replace(old_stamp, stamp));
        self.order.insert(stamp, key.clone());
        self.next_stamp += 1;
        Some(value)
    }

    fn insert(&mut self, key: K, value: V, weight: u64) {
        let weight = weight.max(1);
        if let Some((_, stamp, old_weight)) = self.map.remove(&key) {
            self.order.remove(&stamp);
            self.used -= old_weight;
        }
        let stamp = self.next_stamp;
        self.next_stamp += 1;
        self.map.insert(key.clone(), (value, stamp, weight));
        self.order.insert(stamp, key);
        self.used += weight;
        while self.used > self.budget && self.order.len() > 1 {
            let Some((&oldest, _)) = self.order.iter().next() else {
                break;
            };
            let key = self.order.remove(&oldest).expect("indexed key");
            if let Some((_, _, w)) = self.map.remove(&key) {
                self.used -= w;
            }
        }
    }
}

/// A cached lookup result: found metadata, or a definitive absence at this commit.
#[derive(Clone, Debug)]
pub enum StatOutcome {
    Present(FileStat),
    Absent,
}

/// Cache sizing knobs, all per-mount.
#[derive(Clone, Copy, Debug)]
pub struct CacheConfig {
    /// Directory pages held (count of directories, fully-listed).
    pub dir_entries: u64,
    /// Stat/negative entries held (count).
    pub stat_entries: u64,
    /// Byte budget for file content blocks.
    pub content_bytes: u64,
    /// Content block size; reads are served from block-aligned fetches.
    pub block_bytes: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            dir_entries: 4096,
            stat_entries: 65536,
            content_bytes: 256 * 1024 * 1024,
            block_bytes: 1024 * 1024,
        }
    }
}

/// Cache key: `(commit hex, repo-relative path)`.
type PathKey = (Arc<str>, String);
/// Cache key for one content block: `(content identity, block index)`. The identity is the blob
/// oid — content-addressed, so identical files share blocks across commits, paths, and renames —
/// with a `commit:path` fallback for the rare node whose oid is unknown.
type BlockKey = (String, u64);

/// The mount's immutable-content caches. Metadata keys carry the commit hex (a branch-following
/// refresh needs no invalidation: new lookups miss under the new commit and old entries age
/// out); content blocks key by blob oid, which is immutable by construction.
pub struct MountCaches {
    dirs: Mutex<Lru<PathKey, Arc<Vec<TreeEntry>>>>,
    stats: Mutex<Lru<PathKey, StatOutcome>>,
    blocks: Mutex<Lru<BlockKey, Bytes>>,
    pub config: CacheConfig,
}

impl MountCaches {
    pub fn new(config: CacheConfig) -> Self {
        MountCaches {
            dirs: Mutex::new(Lru::new(config.dir_entries)),
            stats: Mutex::new(Lru::new(config.stat_entries)),
            blocks: Mutex::new(Lru::new(config.content_bytes)),
            config,
        }
    }

    pub fn dir(&self, commit: &Arc<str>, path: &str) -> Option<Arc<Vec<TreeEntry>>> {
        self.dirs
            .lock()
            .unwrap()
            .get(&(commit.clone(), path.to_string()))
    }

    pub fn put_dir(&self, commit: &Arc<str>, path: &str, entries: Arc<Vec<TreeEntry>>) {
        self.dirs
            .lock()
            .unwrap()
            .insert((commit.clone(), path.to_string()), entries, 1);
    }

    pub fn stat(&self, commit: &Arc<str>, path: &str) -> Option<StatOutcome> {
        self.stats
            .lock()
            .unwrap()
            .get(&(commit.clone(), path.to_string()))
    }

    pub fn put_stat(&self, commit: &Arc<str>, path: &str, outcome: StatOutcome) {
        self.stats
            .lock()
            .unwrap()
            .insert((commit.clone(), path.to_string()), outcome, 1);
    }

    pub fn block(&self, ident: &str, block: u64) -> Option<Bytes> {
        self.blocks.lock().unwrap().get(&(ident.to_string(), block))
    }

    pub fn put_block(&self, ident: &str, block: u64, bytes: Bytes) {
        let weight = bytes.len() as u64;
        self.blocks
            .lock()
            .unwrap()
            .insert((ident.to_string(), block), bytes, weight);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_evicts_least_recently_used_by_weight() {
        let mut lru: Lru<u32, u32> = Lru::new(3);
        lru.insert(1, 10, 1);
        lru.insert(2, 20, 1);
        lru.insert(3, 30, 1);
        // Touch 1 so 2 becomes the eviction candidate.
        assert_eq!(lru.get(&1), Some(10));
        lru.insert(4, 40, 1);
        assert_eq!(lru.get(&2), None, "least-recently-used entry evicted");
        assert_eq!(lru.get(&1), Some(10));
        assert_eq!(lru.get(&3), Some(30));
        assert_eq!(lru.get(&4), Some(40));
    }

    #[test]
    fn lru_weighted_eviction_frees_enough_for_large_entries() {
        let mut lru: Lru<u32, u32> = Lru::new(10);
        lru.insert(1, 1, 4);
        lru.insert(2, 2, 4);
        lru.insert(3, 3, 8); // over budget: evicts 1 and 2
        assert_eq!(lru.get(&1), None);
        assert_eq!(lru.get(&2), None);
        assert_eq!(lru.get(&3), Some(3));
    }

    #[test]
    fn lru_reinsert_replaces_weight() {
        let mut lru: Lru<u32, u32> = Lru::new(4);
        lru.insert(1, 1, 4);
        lru.insert(1, 2, 2);
        lru.insert(2, 3, 2);
        assert_eq!(lru.get(&1), Some(2));
        assert_eq!(lru.get(&2), Some(3));
    }
}
