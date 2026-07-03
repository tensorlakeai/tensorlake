//! Interop tests against the reference `git` implementation. These are the ground-truth oracle:
//! a pack/idx we emit must be accepted by `git index-pack`/`git verify-pack`, and a pack git
//! emits must parse cleanly with our reader.
//!
//! Skipped automatically if `git` is not on PATH.

use std::path::Path;
use std::process::Command;

use gsvc_codec::{build_pack, build_pack_delta, parse_pack, write_idx_v2, Kind, Object};

fn git_available() -> bool {
    Command::new("git")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn run(dir: &Path, args: &[&str]) -> std::process::Output {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .expect("spawn git");
    out
}

fn sample_objects() -> Vec<Object> {
    vec![
        Object::new(Kind::Blob, &b"hello, git interop\n"[..]),
        Object::new(Kind::Blob, &b""[..]),
        Object::new(Kind::Blob, vec![0x5a_u8; 70_000]), // multi-byte size header
        Object::new(Kind::Tree, &b""[..]),
        Object::new(
            Kind::Blob,
            &b"the quick brown fox jumps over the lazy dog"[..],
        ),
    ]
}

#[test]
fn git_index_pack_accepts_our_pack() {
    if !git_available() {
        eprintln!("skipping: git not available");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    // `git index-pack` needs a repository (object dir) to resolve against.
    assert!(run(dir.path(), &["init", "--bare", "-q"]).status.success());

    let built = build_pack(&sample_objects()).unwrap();
    let pack_path = dir.path().join("test.pack");
    std::fs::write(&pack_path, &built.data).unwrap();

    // index-pack fully validates: it re-inflates every object, checks the trailer, and (re)builds
    // the index. If our pack were malformed, this fails.
    let out = run(
        dir.path(),
        &["index-pack", "-v", pack_path.to_str().unwrap()],
    );
    assert!(
        out.status.success(),
        "git index-pack rejected our pack:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

#[test]
fn git_index_pack_resolves_our_delta_pack() {
    if !git_available() {
        eprintln!("skipping: git not available");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    assert!(run(dir.path(), &["init", "--bare", "-q"]).status.success());

    // Near-identical blobs so the builder emits OFS_DELTA entries; git must resolve the chain.
    let base: Vec<u8> = (0..30_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();
    let mut objects = vec![Object::new(Kind::Blob, base.clone())];
    for k in 1..6u8 {
        let mut v = base.clone();
        let len = v.len();
        for j in 0..40usize {
            v[(j * 521) % len] = k.wrapping_add(j as u8);
        }
        objects.push(Object::new(Kind::Blob, v));
    }
    let built = build_pack_delta(&objects).unwrap();
    // Sanity: this pack really is smaller than the undeltified one (i.e. deltas were emitted).
    assert!(built.data.len() < build_pack(&objects).unwrap().data.len());

    let pack_path = dir.path().join("delta.pack");
    let idx_path = dir.path().join("delta.idx");
    std::fs::write(&pack_path, &built.data).unwrap();

    // index-pack --strict fully resolves every delta and validates the object graph; it fails on a
    // malformed delta or bad offset.
    let out = run(
        dir.path(),
        &[
            "index-pack",
            "--strict",
            "-o",
            idx_path.to_str().unwrap(),
            pack_path.to_str().unwrap(),
        ],
    );
    assert!(
        out.status.success(),
        "git index-pack rejected our delta pack:\nstderr: {}",
        String::from_utf8_lossy(&out.stderr),
    );

    // verify-pack lists every resolved object (oid + type + delta chain). All our oids must appear,
    // proving git's delta resolution produced exactly our objects; and at least one is a real delta.
    let verify = run(
        dir.path(),
        &["verify-pack", "-v", idx_path.to_str().unwrap()],
    );
    assert!(verify.status.success(), "verify-pack failed");
    let listing = String::from_utf8_lossy(&verify.stdout);
    for obj in &objects {
        assert!(
            listing.contains(&obj.id().to_hex()),
            "git did not resolve object {}",
            obj.id().to_hex()
        );
    }
    // verify-pack's histogram prints a "chain length = N" line only when the pack contains deltas.
    assert!(
        listing.contains("chain length"),
        "expected git to report a delta chain in the pack:\n{listing}"
    );
}

#[test]
fn git_verify_pack_accepts_our_idx() {
    if !git_available() {
        eprintln!("skipping: git not available");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let built = build_pack(&sample_objects()).unwrap();
    let idx = write_idx_v2(&built.entries, built.pack_hash);

    // git verify-pack wants the pair named pack-<hash>.{pack,idx}.
    let stem = format!("pack-{}", built.pack_hash.to_hex());
    let pack_path = dir.path().join(format!("{stem}.pack"));
    let idx_path = dir.path().join(format!("{stem}.idx"));
    std::fs::write(&pack_path, &built.data).unwrap();
    std::fs::write(&idx_path, &idx).unwrap();

    let out = run(
        dir.path(),
        &["verify-pack", "-v", idx_path.to_str().unwrap()],
    );
    assert!(
        out.status.success(),
        "git verify-pack rejected our idx:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    // Every object id we wrote should appear in verify-pack's listing.
    let listing = String::from_utf8_lossy(&out.stdout);
    for obj in sample_objects() {
        assert!(
            listing.contains(&obj.id().to_hex()),
            "verify-pack listing missing {}",
            obj.id()
        );
    }
}

#[test]
fn we_parse_a_pack_git_produced() {
    if !git_available() {
        eprintln!("skipping: git not available");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    assert!(run(dir.path(), &["init", "-q"]).status.success());
    // Make git produce real objects (blobs, a tree, a commit) and pack them.
    std::fs::write(dir.path().join("a.txt"), b"first file\n").unwrap();
    std::fs::write(dir.path().join("b.txt"), vec![b'x'; 40_000]).unwrap();
    run(dir.path(), &["add", "."]);
    run(
        dir.path(),
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-q",
            "-m",
            "initial",
        ],
    );

    // Build a pack of all reachable objects on stdout.
    let rev = run(dir.path(), &["rev-list", "--objects", "--all"]);
    assert!(rev.status.success());
    let pack_out = Command::new("git")
        .args(["pack-objects", "--stdout"])
        .current_dir(dir.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .map(|mut child| {
            use std::io::Write;
            child.stdin.take().unwrap().write_all(&rev.stdout).unwrap();
            child.wait_with_output().unwrap()
        })
        .unwrap();
    assert!(pack_out.status.success());
    let pack_bytes = pack_out.stdout;
    assert!(pack_bytes.len() > 12);

    // git may emit deltas; our parser resolves them against in-pack bases.
    let parsed = parse_pack(&pack_bytes, |_| None).expect("parse git's pack");
    assert!(!parsed.objects.is_empty());

    // Cross-check: every oid we computed should be a real object git can cat-file.
    for (oid, _obj) in &parsed.objects {
        let out = run(dir.path(), &["cat-file", "-t", &oid.to_hex()]);
        assert!(
            out.status.success(),
            "git does not recognize parsed oid {oid}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

/// The fix-thin replacement for a received thin pack must be a fully valid standalone Git pack:
/// `git index-pack --strict` re-inflates every object, resolves the in-pack REF_DELTA against the
/// appended base, and verifies the recomputed trailer.
#[test]
fn git_index_pack_accepts_fix_thin_output() {
    use gsvc_codec::{
        encode_trivial_delta, ExternalBase, Kind, Object, PackResolveLogContext, ReceivePackSpooler,
    };
    use sha1::{Digest as _, Sha1};
    use std::collections::HashMap;
    use std::io::Write as _;

    if !git_available() {
        eprintln!("skipping: git not available");
        return;
    }

    let base = Object::new(Kind::Blob, &b"the original base content for fix thin"[..]);
    let target = Object::new(
        Kind::Blob,
        &b"the original base content for fix thin, now extended further"[..],
    );
    let delta = encode_trivial_delta(&base.data, &target.data);

    // Thin pack: one REF_DELTA entry whose base is external.
    let mut data = Vec::new();
    data.extend_from_slice(b"PACK");
    data.extend_from_slice(&2u32.to_be_bytes());
    data.extend_from_slice(&1u32.to_be_bytes());
    // Entry header: type 7 (REF_DELTA), size = delta.len() (< 16 keeps this simple? use varint).
    let mut size = delta.len() as u64;
    let mut byte = (7u8 << 4) | (size & 0x0f) as u8;
    size >>= 4;
    while size > 0 {
        data.push(byte | 0x80);
        byte = (size & 0x7f) as u8;
        size >>= 7;
    }
    data.push(byte);
    data.extend_from_slice(base.id().as_bytes());
    let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(&delta).unwrap();
    data.extend_from_slice(&enc.finish().unwrap());
    let mut h = Sha1::new();
    h.update(&data);
    let digest: [u8; 20] = h.finalize().into();
    data.extend_from_slice(&digest);

    let mut spooler = ReceivePackSpooler::new(true);
    spooler.push(&data).unwrap();
    let scanned = spooler.finish().unwrap().expect("thin scan");

    let mut journal = tempfile::NamedTempFile::new().unwrap();
    journal.write_all(&data).unwrap();
    journal.as_file_mut().flush().unwrap();
    let mut base_file = tempfile::NamedTempFile::new().unwrap();
    base_file.write_all(&base.data).unwrap();
    base_file.as_file_mut().flush().unwrap();
    let mut external = HashMap::new();
    external.insert(
        base.id(),
        ExternalBase {
            kind: base.kind,
            size: base.data.len() as u64,
            path: base_file.path().to_path_buf(),
        },
    );

    let resolved = gsvc_codec::resolve_scanned_pack_with_external_bases(
        journal.as_file(),
        scanned,
        &external,
        PackResolveLogContext::default(),
    )
    .unwrap()
    .expect("resolved thin pack");
    let fixed = resolved
        .fixed_thin_pack
        .as_ref()
        .expect("thin pack must produce a fix-thin replacement");
    assert_eq!(resolved.pack_hash, fixed.pack_hash);
    assert_eq!(resolved.object_count, 2, "delta target + appended base");
    assert!(
        resolved.oids.contains(&base.id()) && resolved.oids.contains(&target.id()),
        "both objects must be indexed"
    );
    assert!(resolved.external_refs.is_empty());

    let dir = tempfile::tempdir().unwrap();
    assert!(run(dir.path(), &["init", "--bare", "-q"]).status.success());
    let pack_path = dir.path().join("fixed.pack");
    std::fs::copy(fixed.file.path(), &pack_path).unwrap();
    assert_eq!(
        std::fs::metadata(&pack_path).unwrap().len(),
        fixed.len,
        "reported fixed pack length must match the file"
    );
    let out = run(
        dir.path(),
        &["index-pack", "--strict", "-v", pack_path.to_str().unwrap()],
    );
    assert!(
        out.status.success(),
        "git index-pack rejected the fix-thin pack:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}
