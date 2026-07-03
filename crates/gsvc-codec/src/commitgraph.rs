//! Commit-graph with generation numbers (design §7) — accelerates reachability without parsing
//! commits.
//!
//! A reachability walk normally parses each commit object to find its parents. A commit-graph
//! precomputes, per commit, its **parents** and its **generation number** — `gen(c) = 1 +
//! max(gen(parent))`, with parentless commits at generation 1. Generation numbers give a cheap
//! reachability cutoff: every commit reachable from `c` has generation `≤ gen(c)`, so when walking
//! down from a set of tips to decide whether they reach a target `t`, any commit with generation
//! `< gen(t)` can be skipped — it cannot be `t` nor reach it. The graph is generated at compaction
//! time (off the hot path) and stored alongside the pack as its `.graph`.

use std::collections::HashMap;

use crate::{CodecError, Oid};

const GRAPH_MAGIC: &[u8; 4] = b"GCGF";
const GRAPH_VERSION: u32 = 1;

/// One commit's node: its root tree, parents, and generation number. Storing the root tree lets a
/// reachability walk descend into a commit's content (its tree) without inflating the commit object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitNode {
    pub oid: Oid,
    pub generation: u32,
    pub root_tree: Oid,
    pub parents: Vec<Oid>,
}

/// A commit-graph: the parents + generation number of every commit in a pack. Entries are kept
/// sorted by oid for a compact, deterministic encoding; an index supports O(1) lookup.
#[derive(Clone, Debug, Default)]
pub struct CommitGraph {
    entries: Vec<CommitNode>,
    index: HashMap<Oid, usize>,
}

impl PartialEq for CommitGraph {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}
impl Eq for CommitGraph {}

impl CommitGraph {
    /// Build a commit-graph from a `commit oid -> (root tree, parent oids)` map. Generation numbers
    /// are computed iteratively (no recursion, so arbitrarily deep histories are safe); a parent
    /// absent from the map (e.g. a boundary/shallow commit) contributes generation 0.
    pub fn build(commits: &HashMap<Oid, (Oid, Vec<Oid>)>) -> CommitGraph {
        let mut generation: HashMap<Oid, u32> = HashMap::with_capacity(commits.len());
        // Iterative post-order: for each commit, resolve all in-graph parents' generations first.
        let mut stack: Vec<Oid> = commits.keys().copied().collect();
        let mut on_path: std::collections::HashSet<Oid> = std::collections::HashSet::new();
        while let Some(oid) = stack.last().copied() {
            if generation.contains_key(&oid) {
                stack.pop();
                continue;
            }
            let parents = match commits.get(&oid) {
                Some((_, p)) => p,
                None => {
                    // Not actually a graph commit (shouldn't happen for a stack seeded from keys).
                    stack.pop();
                    continue;
                }
            };
            // Find an in-graph parent whose generation isn't known yet; resolve it first.
            let mut pending: Option<Oid> = None;
            for p in parents {
                if commits.contains_key(p) && !generation.contains_key(p) && !on_path.contains(p) {
                    pending = Some(*p);
                    break;
                }
            }
            match pending {
                Some(p) => {
                    on_path.insert(oid);
                    stack.push(p);
                }
                None => {
                    // All in-graph parents resolved (or cyclic/boundary): gen = 1 + max(parent gen).
                    let g = parents
                        .iter()
                        .map(|p| generation.get(p).copied().unwrap_or(0))
                        .max()
                        .unwrap_or(0)
                        + 1;
                    generation.insert(oid, g);
                    on_path.remove(&oid);
                    stack.pop();
                }
            }
        }

        let mut entries: Vec<CommitNode> = commits
            .iter()
            .map(|(oid, (root_tree, parents))| CommitNode {
                oid: *oid,
                generation: generation.get(oid).copied().unwrap_or(1),
                root_tree: *root_tree,
                parents: parents.clone(),
            })
            .collect();
        entries.sort_by(|a, b| a.oid.as_bytes().cmp(b.oid.as_bytes()));
        let index = entries
            .iter()
            .enumerate()
            .map(|(i, n)| (n.oid, i))
            .collect();
        CommitGraph { entries, index }
    }

    /// Number of commits in the graph.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the graph holds no commits.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether `oid` is a commit in this graph.
    pub fn contains(&self, oid: &Oid) -> bool {
        self.index.contains_key(oid)
    }

    /// The generation number of `oid`, or `None` if it isn't in the graph.
    pub fn generation(&self, oid: &Oid) -> Option<u32> {
        self.index.get(oid).map(|&i| self.entries[i].generation)
    }

    /// The parents of `oid`, or `None` if it isn't in the graph.
    pub fn parents(&self, oid: &Oid) -> Option<&[Oid]> {
        self.index
            .get(oid)
            .map(|&i| self.entries[i].parents.as_slice())
    }

    /// The root tree of commit `oid`, or `None` if it isn't in the graph.
    pub fn root_tree(&self, oid: &Oid) -> Option<Oid> {
        self.index.get(oid).map(|&i| self.entries[i].root_tree)
    }

    /// All nodes, sorted by oid.
    pub fn nodes(&self) -> &[CommitNode] {
        &self.entries
    }

    /// Serialize to a compact, zlib-compressed byte blob (stored as the pack's `.graph`).
    pub fn encode(&self) -> Result<Vec<u8>, CodecError> {
        use std::io::Write;
        let mut raw = Vec::new();
        raw.extend_from_slice(GRAPH_MAGIC);
        raw.extend_from_slice(&GRAPH_VERSION.to_be_bytes());
        raw.extend_from_slice(&(self.entries.len() as u32).to_be_bytes());
        for n in &self.entries {
            raw.extend_from_slice(n.oid.as_bytes());
            raw.extend_from_slice(&n.generation.to_be_bytes());
            raw.extend_from_slice(n.root_tree.as_bytes());
            raw.extend_from_slice(&(n.parents.len() as u16).to_be_bytes());
            for p in &n.parents {
                raw.extend_from_slice(p.as_bytes());
            }
        }
        let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(&raw)
            .map_err(|e| CodecError::Io(e.to_string()))?;
        enc.finish().map_err(|e| CodecError::Io(e.to_string()))
    }

    /// Parse a blob produced by [`encode`](Self::encode).
    pub fn decode(bytes: &[u8]) -> Result<CommitGraph, CodecError> {
        use std::io::Read;
        let mut raw = Vec::new();
        flate2::read::ZlibDecoder::new(bytes)
            .read_to_end(&mut raw)
            .map_err(|e| CodecError::Io(e.to_string()))?;

        let mut r = GraphCursor { buf: &raw, pos: 0 };
        if r.take(4)? != GRAPH_MAGIC {
            return Err(CodecError::BadObject("bad commit-graph magic".into()));
        }
        let version = u32::from_be_bytes(r.take(4)?.try_into().unwrap());
        if version != GRAPH_VERSION {
            return Err(CodecError::BadObject(format!(
                "commit-graph version {version}"
            )));
        }
        let count = u32::from_be_bytes(r.take(4)?.try_into().unwrap()) as usize;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let oid = Oid::from_bytes(r.take(20)?)?;
            let generation = u32::from_be_bytes(r.take(4)?.try_into().unwrap());
            let root_tree = Oid::from_bytes(r.take(20)?)?;
            let np = u16::from_be_bytes(r.take(2)?.try_into().unwrap()) as usize;
            let mut parents = Vec::with_capacity(np);
            for _ in 0..np {
                parents.push(Oid::from_bytes(r.take(20)?)?);
            }
            entries.push(CommitNode {
                oid,
                generation,
                root_tree,
                parents,
            });
        }
        let index = entries
            .iter()
            .enumerate()
            .map(|(i, n)| (n.oid, i))
            .collect();
        Ok(CommitGraph { entries, index })
    }
}

struct GraphCursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> GraphCursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        let s = self
            .buf
            .get(self.pos..self.pos + n)
            .ok_or_else(|| CodecError::BadObject("truncated commit-graph".into()))?;
        self.pos += n;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(b: u8) -> Oid {
        Oid::from_array([b; 20])
    }

    // A distinct dummy root-tree oid per commit (its value is irrelevant to generation numbers).
    fn tree(b: u8) -> Oid {
        Oid::from_array([0x80 | b; 20])
    }

    #[test]
    fn generations_follow_longest_path() {
        // c0 ← c1 ← c2, and a side branch c0 ← c3, merge c4 = {c2, c3}.
        let (c0, c1, c2, c3, c4) = (oid(0), oid(1), oid(2), oid(3), oid(4));
        let mut m = HashMap::new();
        m.insert(c0, (tree(0), vec![]));
        m.insert(c1, (tree(1), vec![c0]));
        m.insert(c2, (tree(2), vec![c1]));
        m.insert(c3, (tree(3), vec![c0]));
        m.insert(c4, (tree(4), vec![c2, c3]));
        let g = CommitGraph::build(&m);
        assert_eq!(g.generation(&c0), Some(1));
        assert_eq!(g.generation(&c1), Some(2));
        assert_eq!(g.generation(&c2), Some(3));
        assert_eq!(g.generation(&c3), Some(2));
        // Merge generation = 1 + max(gen(c2)=3, gen(c3)=2) = 4.
        assert_eq!(g.generation(&c4), Some(4));
        assert_eq!(g.parents(&c4).unwrap(), &[c2, c3]);
        assert_eq!(g.root_tree(&c2), Some(tree(2)));
        assert!(g.contains(&c1));
        assert!(!g.contains(&oid(9)));
    }

    #[test]
    fn boundary_parents_count_as_zero() {
        // c1's parent c0 is NOT in the graph (a boundary/shallow commit).
        let (c0, c1) = (oid(10), oid(11));
        let mut m = HashMap::new();
        m.insert(c1, (tree(11), vec![c0]));
        let g = CommitGraph::build(&m);
        assert_eq!(g.generation(&c1), Some(1)); // 1 + max(0) = 1
        assert!(!g.contains(&c0));
    }

    #[test]
    fn encode_decode_roundtrip() {
        let (c0, c1, c2) = (oid(1), oid(2), oid(3));
        let mut m = HashMap::new();
        m.insert(c0, (tree(1), vec![]));
        m.insert(c1, (tree(2), vec![c0]));
        m.insert(c2, (tree(3), vec![c1, c0]));
        let g = CommitGraph::build(&m);
        let bytes = g.encode().unwrap();
        let g2 = CommitGraph::decode(&bytes).unwrap();
        assert_eq!(g, g2);
        assert_eq!(g2.generation(&c2), Some(3));
        assert_eq!(g2.root_tree(&c2), Some(tree(3)));
        assert_eq!(g2.parents(&c2).unwrap(), &[c1, c0]);
    }

    #[test]
    fn deep_linear_history_does_not_overflow() {
        // A long chain would blow a recursive implementation's stack; the iterative build must not.
        let mut m = HashMap::new();
        let mut prev: Option<Oid> = None;
        let mut last = oid(0);
        for i in 0..20_000u32 {
            let mut b = [0u8; 20];
            b[0..4].copy_from_slice(&i.to_be_bytes());
            let o = Oid::from_array(b);
            m.insert(o, (tree(0), prev.into_iter().collect()));
            prev = Some(o);
            last = o;
        }
        let g = CommitGraph::build(&m);
        assert_eq!(g.generation(&last), Some(20_000));
    }
}
