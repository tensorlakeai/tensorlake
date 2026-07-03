//! Reachability parsing: pulling the outbound oid references out of commit, tree, and tag objects.
//!
//! This is the minimum git object grammar needed to walk the object graph for `upload-pack`
//! (which objects does a clone need?) and for GC mark-sweep (which objects are still reachable?).
//! It is deliberately tolerant — it extracts the links it understands and ignores the rest — and
//! pure (no I/O), so the traversal driver lives in the server/GC layers that own object lookup.

use crate::{CodecError, Kind, Object, Oid};

/// One entry in a tree object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeEntry {
    /// The git file mode, parsed from its octal ASCII (e.g. `0o100644`, `0o40000` for a subtree).
    pub mode: u32,
    pub name: Vec<u8>,
    pub oid: Oid,
}

impl TreeEntry {
    /// Whether this entry points at a subtree (mode `040000`).
    pub fn is_tree(&self) -> bool {
        self.mode == 0o040000
    }
    /// Whether this entry is a gitlink/submodule (mode `160000`) — a commit in *another* repo,
    /// never present in this repo's object set, so reachability must not follow it.
    pub fn is_gitlink(&self) -> bool {
        self.mode == 0o160000
    }
    /// Whether this entry is a blob (a regular file, executable, or symlink).
    pub fn is_blob(&self) -> bool {
        !self.is_tree() && !self.is_gitlink()
    }
}

/// What a single object points at, for reachability. The traversal driver folds these into a
/// work-list, classifying each referent so it can apply partial-clone filters (e.g. skip blobs).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Links {
    /// Commits referenced (a commit's parents, or a tag's commit target).
    pub commits: Vec<Oid>,
    /// Trees referenced (a commit's root tree, or a tree's subtrees).
    pub trees: Vec<Oid>,
    /// Blobs referenced (a tree's file entries).
    pub blobs: Vec<Oid>,
    /// Annotated-tag targets whose kind isn't statically known from the referrer (rare nested
    /// tags); the driver resolves their kind when it looks them up.
    pub tags: Vec<Oid>,
}

/// The root tree and parent commits of a commit object.
pub fn commit_links(data: &[u8]) -> Result<(Oid, Vec<Oid>), CodecError> {
    let mut tree: Option<Oid> = None;
    let mut parents = Vec::new();
    for line in data.split(|&b| b == b'\n') {
        if line.is_empty() {
            break; // blank line terminates the header; the message follows.
        }
        if let Some(rest) = line.strip_prefix(b"tree ") {
            tree = Some(parse_hex_oid(rest)?);
        } else if let Some(rest) = line.strip_prefix(b"parent ") {
            parents.push(parse_hex_oid(rest)?);
        } else if !starts_with_known_commit_header(line) {
            // `author`/`committer`/`gpgsig`/continuation lines — not references; keep scanning.
        }
    }
    let tree = tree.ok_or_else(|| CodecError::BadObject("commit has no tree".into()))?;
    Ok((tree, parents))
}

/// The target object of an annotated tag, plus its declared type if present.
pub fn tag_target(data: &[u8]) -> Result<(Oid, Option<Kind>), CodecError> {
    let mut target: Option<Oid> = None;
    let mut kind: Option<Kind> = None;
    for line in data.split(|&b| b == b'\n') {
        if line.is_empty() {
            break;
        }
        if let Some(rest) = line.strip_prefix(b"object ") {
            target = Some(parse_hex_oid(rest)?);
        } else if let Some(rest) = line.strip_prefix(b"type ") {
            kind = Kind::from_str(&String::from_utf8_lossy(rest)).ok();
        }
    }
    let target = target.ok_or_else(|| CodecError::BadObject("tag has no object".into()))?;
    Ok((target, kind))
}

/// Parse a tree object's entries: a sequence of `<octal-mode> <name>\0<20-byte-oid>`.
pub fn tree_entries(data: &[u8]) -> Result<Vec<TreeEntry>, CodecError> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos < data.len() {
        // mode (octal ASCII) up to the space.
        let sp = data[pos..]
            .iter()
            .position(|&b| b == b' ')
            .ok_or_else(|| CodecError::BadObject("tree entry missing mode separator".into()))?;
        let mode = parse_octal(&data[pos..pos + sp])?;
        pos += sp + 1;
        // name up to the NUL.
        let nul = data[pos..]
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| CodecError::BadObject("tree entry missing name terminator".into()))?;
        let name = data[pos..pos + nul].to_vec();
        pos += nul + 1;
        // 20 raw oid bytes.
        if pos + 20 > data.len() {
            return Err(CodecError::BadObject("tree entry truncated oid".into()));
        }
        let oid = Oid::from_bytes(&data[pos..pos + 20])?;
        pos += 20;
        out.push(TreeEntry { mode, name, oid });
    }
    Ok(out)
}

/// Extract the outbound links of any object. Blobs have none.
pub fn links_of(obj: &Object) -> Result<Links, CodecError> {
    let mut links = Links::default();
    match obj.kind {
        Kind::Blob => {}
        Kind::Commit => {
            let (tree, parents) = commit_links(&obj.data)?;
            links.trees.push(tree);
            links.commits.extend(parents);
        }
        Kind::Tag => {
            let (target, kind) = tag_target(&obj.data)?;
            match kind {
                Some(Kind::Commit) => links.commits.push(target),
                Some(Kind::Tree) => links.trees.push(target),
                Some(Kind::Blob) => links.blobs.push(target),
                Some(Kind::Tag) | None => links.tags.push(target),
            }
        }
        Kind::Tree => {
            for e in tree_entries(&obj.data)? {
                if e.is_tree() {
                    links.trees.push(e.oid);
                } else if e.is_blob() {
                    links.blobs.push(e.oid);
                }
                // gitlinks are intentionally dropped: the referent lives in another repo.
            }
        }
    }
    Ok(links)
}

fn starts_with_known_commit_header(line: &[u8]) -> bool {
    line.starts_with(b"author ")
        || line.starts_with(b"committer ")
        || line.starts_with(b"gpgsig")
        || line.starts_with(b" ")
        || line.starts_with(b"encoding ")
        || line.starts_with(b"mergetag ")
}

fn parse_hex_oid(b: &[u8]) -> Result<Oid, CodecError> {
    // Take exactly the leading 40 hex chars (some lines carry a trailing label, e.g. parent oids
    // never do, but be defensive).
    let hex = &b[..b.len().min(40)];
    Oid::from_hex(&String::from_utf8_lossy(hex))
}

fn parse_octal(b: &[u8]) -> Result<u32, CodecError> {
    let s =
        std::str::from_utf8(b).map_err(|_| CodecError::BadObject("non-utf8 tree mode".into()))?;
    u32::from_str_radix(s, 8).map_err(|_| CodecError::BadObject(format!("bad tree mode {s:?}")))
}

/// Serialize tree entries into git's canonical on-disk form.
pub fn encode_tree(entries: &[TreeEntry]) -> Vec<u8> {
    let mut items: Vec<&TreeEntry> = entries.iter().collect();
    items.sort_by(|a, b| tree_entry_cmp(a, b));
    let mut out = Vec::new();
    for e in items {
        out.extend_from_slice(format!("{:o}", e.mode).as_bytes());
        out.push(b' ');
        out.extend_from_slice(&e.name);
        out.push(0);
        out.extend_from_slice(e.oid.as_bytes());
    }
    out
}

fn tree_entry_cmp(a: &TreeEntry, b: &TreeEntry) -> std::cmp::Ordering {
    let alen = a.name.len() + a.is_tree() as usize;
    let blen = b.name.len() + b.is_tree() as usize;
    for i in 0..alen.min(blen) {
        let ca = if i < a.name.len() { a.name[i] } else { b'/' };
        let cb = if i < b.name.len() { b.name[i] } else { b'/' };
        if ca != cb {
            return ca.cmp(&cb);
        }
    }
    alen.cmp(&blen)
}

/// Assemble a commit object with canonical commit headers.
pub fn encode_commit(
    tree: Oid,
    parents: &[Oid],
    author: (&str, &str),
    committer: (&str, &str),
    message: &str,
    when_secs: u64,
) -> Object {
    let mut body = Vec::new();
    body.extend_from_slice(format!("tree {}\n", tree.to_hex()).as_bytes());
    for p in parents {
        body.extend_from_slice(format!("parent {}\n", p.to_hex()).as_bytes());
    }
    body.extend_from_slice(
        format!("author {} <{}> {when_secs} +0000\n", author.0, author.1).as_bytes(),
    );
    body.extend_from_slice(
        format!(
            "committer {} <{}> {when_secs} +0000\n",
            committer.0, committer.1
        )
        .as_bytes(),
    );
    body.push(b'\n');
    body.extend_from_slice(message.as_bytes());
    if !message.ends_with('\n') {
        body.push(b'\n');
    }
    Object::new(Kind::Commit, body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(b: u8) -> Oid {
        Oid::from_array([b; 20])
    }

    #[test]
    fn encode_tree_round_trips_through_tree_entries_in_git_order() {
        let entries = vec![
            TreeEntry {
                mode: 0o100644,
                name: b"file.txt".to_vec(),
                oid: oid(0x11),
            },
            TreeEntry {
                mode: 0o040000,
                name: b"dir".to_vec(),
                oid: oid(0x22),
            },
            TreeEntry {
                mode: 0o100755,
                name: b"dir.txt".to_vec(),
                oid: oid(0x33),
            },
        ];
        let encoded = encode_tree(&entries);
        let decoded = tree_entries(&encoded).unwrap();
        let names: Vec<&[u8]> = decoded.iter().map(|e| e.name.as_slice()).collect();
        assert_eq!(names, vec![&b"dir.txt"[..], &b"dir"[..], &b"file.txt"[..]]);
        assert_eq!(encode_tree(&decoded), encoded);
    }

    #[test]
    fn encode_commit_round_trips_through_commit_links() {
        let tree = oid(0xaa);
        let parents = vec![oid(0xb1), oid(0xb2)];
        let obj = encode_commit(
            tree,
            &parents,
            ("Ada", "ada@x"),
            ("Bob", "bob@x"),
            "hello",
            1_700_000_000,
        );
        assert_eq!(obj.kind, Kind::Commit);
        let (got_tree, got_parents) = commit_links(&obj.data).unwrap();
        assert_eq!(got_tree, tree);
        assert_eq!(got_parents, parents);
        assert!(obj.data.ends_with(b"\nhello\n"));
    }

    #[test]
    fn parses_commit_tree_and_parents() {
        let t = oid(0x11);
        let p1 = oid(0x22);
        let p2 = oid(0x33);
        let body = format!(
            "tree {}\nparent {}\nparent {}\nauthor A <a@x> 1 +0000\ncommitter A <a@x> 1 +0000\n\nmsg\n",
            t.to_hex(),
            p1.to_hex(),
            p2.to_hex()
        );
        let (tree, parents) = commit_links(body.as_bytes()).unwrap();
        assert_eq!(tree, t);
        assert_eq!(parents, vec![p1, p2]);
    }

    #[test]
    fn root_commit_has_no_parents() {
        let body = format!(
            "tree {}\nauthor A <a@x> 1 +0000\ncommitter A <a@x> 1 +0000\n\nroot\n",
            oid(0x11).to_hex()
        );
        let (tree, parents) = commit_links(body.as_bytes()).unwrap();
        assert_eq!(tree, oid(0x11));
        assert!(parents.is_empty());
    }

    #[test]
    fn parses_tree_entries_with_modes() {
        // Build `100644 file\0<oid>` + `40000 sub\0<oid>` + `160000 mod\0<oid>`.
        let mut data = Vec::new();
        let push = |data: &mut Vec<u8>, mode: &str, name: &str, o: Oid| {
            data.extend_from_slice(mode.as_bytes());
            data.push(b' ');
            data.extend_from_slice(name.as_bytes());
            data.push(0);
            data.extend_from_slice(o.as_bytes());
        };
        push(&mut data, "100644", "file", oid(0xaa));
        push(&mut data, "40000", "sub", oid(0xbb));
        push(&mut data, "160000", "mod", oid(0xcc));
        let entries = tree_entries(&data).unwrap();
        assert_eq!(entries.len(), 3);
        assert!(entries[0].is_blob());
        assert_eq!(entries[0].oid, oid(0xaa));
        assert!(entries[1].is_tree());
        assert!(entries[2].is_gitlink());

        // Reachability folds the blob + subtree but drops the gitlink.
        let obj = Object::new(Kind::Tree, data);
        let links = links_of(&obj).unwrap();
        assert_eq!(links.trees, vec![oid(0xbb)]);
        assert_eq!(links.blobs, vec![oid(0xaa)]);
    }

    #[test]
    fn parses_tag_target() {
        let body = format!(
            "object {}\ntype commit\ntag v1\ntagger A <a@x> 1 +0000\n\nrelease\n",
            oid(0x44).to_hex()
        );
        let (target, kind) = tag_target(body.as_bytes()).unwrap();
        assert_eq!(target, oid(0x44));
        assert_eq!(kind, Some(Kind::Commit));
        let links = links_of(&Object::new(Kind::Tag, body.into_bytes())).unwrap();
        assert_eq!(links.commits, vec![oid(0x44)]);
    }
}
