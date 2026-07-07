#!/usr/bin/env python3
"""POSIX conformance battery for a mounted `tl fs` workspace.

Ported from the artifact_storage issue-#24 overlay spike (which ratified the mount
architecture with 29 PASS / 0 FAIL on kernel overlayfs; see the issue thread). This version is
**implementation-agnostic**: it exercises only POSIX-visible behavior of a writable mounted
workspace, so it conforms whatever overlay implementation backs `tl fs mount` — today's
userspace overlay or a future kernel-overlayfs relayering. The spike's kernel-specific checks
(upperdir whiteout/opaque/redirect decoding) were design validation, not conformance, and are
deliberately not here.

Usage: battery.py <mounted-workspace-dir> [--seeded]

`--seeded` asserts the "lower layer" checks too: the orchestrator seeds fixture files, takes a
snapshot, unmounts, and reattaches, so the fixture content is served from the versioned lower
side of the overlay rather than local dirty state. Without it, only the upper-born checks run.

Output contract (one line per check):
    PASS <test>            requirement holds
    FAIL <test> -- detail  requirement broken
    INFO <test> -- detail  recorded behavior; not pass/fail (semantics legitimately vary)
Exit code = number of FAILs.
"""

import ctypes
import ctypes.util
import fcntl
import hashlib
import mmap
import multiprocessing
import os
import sqlite3
import stat
import struct
import sys
import time

RESULTS = {"PASS": 0, "FAIL": 0, "INFO": 0}


def report(kind, test, detail=""):
    RESULTS[kind] += 1
    line = f"{kind} {test}"
    if detail:
        line += f" -- {detail}"
    print(line, flush=True)


def run(test):
    def wrap(fn):
        try:
            fn()
        except Exception as e:  # noqa: BLE001 - battery must keep going
            report("FAIL", test, f"{type(e).__name__}: {e}")

    return wrap


libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
FALLOC_FL_KEEP_SIZE = 0x1
FALLOC_FL_PUNCH_HOLE = 0x2
SEEK_DATA = 3
SEEK_HOLE = 4
F_OFD_GETLK = getattr(fcntl, "F_OFD_GETLK", 36)
F_OFD_SETLK = getattr(fcntl, "F_OFD_SETLK", 37)


def fallocate(fd, mode, offset, length):
    if libc.fallocate(fd, mode, ctypes.c_long(offset), ctypes.c_long(length)) != 0:
        raise OSError(ctypes.get_errno(), "fallocate")


# ── SQLite under multi-process concurrency ───────────────────────────────────


def sqlite_writer(path, n, barrier):
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA busy_timeout=15000")
    barrier.wait()
    for i in range(n):
        db.execute("INSERT INTO t(v) VALUES (?)", (f"w{i}",))
        if i % 10 == 0:
            db.commit()
    db.commit()
    db.close()


def sqlite_reader(path, n, barrier, out):
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA busy_timeout=15000")
    barrier.wait()
    seen = 0
    for _ in range(n):
        seen = max(seen, db.execute("SELECT count(*) FROM t").fetchone()[0])
        time.sleep(0.002)
    out.put(seen)
    db.close()


def sqlite_concurrent(test, path, journal_mode):
    db = sqlite3.connect(path, timeout=30)
    mode = db.execute(f"PRAGMA journal_mode={journal_mode}").fetchone()[0]
    if mode.lower() != journal_mode.lower():
        report("FAIL", test, f"journal_mode={mode}, wanted {journal_mode}")
        return
    db.execute("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)")
    db.commit()

    barrier = multiprocessing.Barrier(3)
    out = multiprocessing.Queue()
    w = multiprocessing.Process(target=sqlite_writer, args=(path, 200, barrier))
    r = multiprocessing.Process(target=sqlite_reader, args=(path, 50, barrier, out))
    w.start()
    r.start()
    barrier.wait()
    w.join(60)
    r.join(60)
    if w.exitcode != 0 or r.exitcode != 0:
        report("FAIL", test, f"writer exit {w.exitcode}, reader exit {r.exitcode}")
        return
    seen = out.get()
    ok = db.execute("PRAGMA integrity_check").fetchone()[0]
    count = db.execute("SELECT count(*) FROM t").fetchone()[0]
    db.close()
    if ok != "ok" or count < 200:
        report("FAIL", test, f"integrity={ok} rows={count}")
        return
    report("PASS", test, f"rows={count} reader_saw={seen}")


# ── lock families ────────────────────────────────────────────────────────────


def ofd_lock_holder(path, ready, release):
    fd = os.open(path, os.O_RDWR)
    flk = struct.pack("hhqqi", fcntl.F_WRLCK, os.SEEK_SET, 0, 100, 0)
    fcntl.fcntl(fd, F_OFD_SETLK, flk)
    ready.set()
    release.wait(30)
    os.close(fd)


def posix_lock_holder(path, ready, release):
    fd = os.open(path, os.O_RDWR)
    fcntl.lockf(fd, fcntl.LOCK_EX, 100, 0, 0)
    ready.set()
    release.wait(30)
    os.close(fd)


def flock_holder(path, ready, release):
    fd = os.open(path, os.O_RDWR)
    fcntl.flock(fd, fcntl.LOCK_EX)
    ready.set()
    release.wait(30)
    os.close(fd)


def contended_lock(test, path, holder, probe):
    ready = multiprocessing.Event()
    release = multiprocessing.Event()
    p = multiprocessing.Process(target=holder, args=(path, ready, release))
    p.start()
    try:
        if not ready.wait(15):
            report("FAIL", test, "holder never took the lock")
            return
        conflicted = probe(path)
        release.set()
        p.join(15)
        granted = not probe(path)
        if conflicted and granted:
            report("PASS", test)
        else:
            report(
                "FAIL", test, f"conflict_detected={conflicted} grantable_after={granted}"
            )
    finally:
        release.set()
        if p.is_alive():
            p.terminate()


def probe_ofd(path):
    fd = os.open(path, os.O_RDWR)
    try:
        flk = struct.pack("hhqqi", fcntl.F_WRLCK, os.SEEK_SET, 0, 100, 0)
        try:
            fcntl.fcntl(fd, F_OFD_SETLK, flk)
            return False
        except OSError:
            return True
    finally:
        os.close(fd)


def probe_posix(path):
    def child(path, out):
        fd = os.open(path, os.O_RDWR)
        try:
            fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 100, 0, 0)
            out.put(False)
        except OSError:
            out.put(True)
        finally:
            os.close(fd)

    out = multiprocessing.Queue()
    p = multiprocessing.Process(target=child, args=(path, out))
    p.start()
    p.join(15)
    return out.get()


def probe_flock(path):
    fd = os.open(path, os.O_RDWR)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return False
    except OSError:
        return True
    finally:
        os.close(fd)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


# Fixture layout the orchestrator seeds before snapshot+reattach (kept in sync with
# run_conformance.sh). SEED_SHA files carry deterministic content so integrity is checkable
# without a side channel.
SEEDED = {
    "readme": "seed/README.md",  # "hello conformance\n"
    "script": "seed/bin/run.sh",  # echoes conformance-ok, mode 755
    "link": "seed/link",  # symlink -> README.md
    "files": [f"seed/dir/f{i}.txt" for i in range(1, 6)],  # "seed file <i>\n"
    "renameme": "seed/renameme",  # dir with content
    "db": "seed/seed.db",  # sqlite, 50 rows
    "blob": "seed/blob.bin",  # 4 MiB of i%251 bytes
    "blob2": "seed/blob2.bin",  # 1 MiB of (i*7)%251 bytes
}


def pattern(n, mult=1):
    return bytes(((i * mult) % 251) for i in range(n))


def main():
    argv = [a for a in sys.argv[1:] if a != "--seeded"]
    seeded = "--seeded" in sys.argv
    ws = argv[0]

    @run("c01_sqlite_wal_upper_born")
    def _():
        d = os.path.join(ws, "sq")
        os.makedirs(d, exist_ok=True)
        sqlite_concurrent("c01_sqlite_wal_upper_born", os.path.join(d, "wal.db"), "wal")

    @run("c02_sqlite_rollback_journal")
    def _():
        d = os.path.join(ws, "sq")
        os.makedirs(d, exist_ok=True)
        sqlite_concurrent("c02_sqlite_rollback_journal", os.path.join(d, "rb.db"), "delete")

    @run("c03_ofd_locks")
    def _():
        p = os.path.join(ws, "locks.bin")
        with open(p, "wb") as f:
            f.write(b"x" * 4096)
        contended_lock("c03_ofd_locks", p, ofd_lock_holder, probe_ofd)

    @run("c04_posix_locks")
    def _():
        contended_lock(
            "c04_posix_locks", os.path.join(ws, "locks.bin"), posix_lock_holder, probe_posix
        )

    @run("c05_flock")
    def _():
        contended_lock("c05_flock", os.path.join(ws, "locks.bin"), flock_holder, probe_flock)

    @run("c06_fsync_file_and_dir")
    def _():
        d = os.path.join(ws, "fsync")
        os.makedirs(d, exist_ok=True)
        p = os.path.join(d, "f")
        fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o644)
        os.write(fd, b"data")
        os.fsync(fd)
        os.fdatasync(fd)
        os.close(fd)
        dfd = os.open(d, os.O_RDONLY)
        os.fsync(dfd)
        os.close(dfd)
        report("PASS", "c06_fsync_file_and_dir")

    @run("c07_rename_atomic_over_existing")
    def _():
        a, b = os.path.join(ws, "rn_a"), os.path.join(ws, "rn_b")
        for p, c in ((a, b"A"), (b, b"B")):
            with open(p, "wb") as f:
                f.write(c)
        os.rename(a, b)
        with open(b, "rb") as f:
            assert f.read() == b"A"
        report("PASS", "c07_rename_atomic_over_existing")

    @run("c08_unlinked_but_open")
    def _():
        p = os.path.join(ws, "ubo_new")
        with open(p, "wb") as f:
            f.write(b"seed")
        fd = os.open(p, os.O_RDWR)
        os.unlink(p)
        os.pwrite(fd, b"after-unlink", 0)
        got = os.pread(fd, 12, 0)
        nlink = os.fstat(fd).st_nlink
        os.close(fd)
        if got == b"after-unlink" and nlink == 0:
            report("PASS", "c08_unlinked_but_open")
        else:
            report("FAIL", "c08_unlinked_but_open", f"read={got!r} nlink={nlink}")

    @run("c09_o_excl_and_mkdir_lock")
    def _():
        p = os.path.join(ws, "excl.lock")
        fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        os.close(fd)
        try:
            os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            report("FAIL", "c09_o_excl_and_mkdir_lock", "second O_EXCL create succeeded")
            return
        except FileExistsError:
            pass
        d = os.path.join(ws, "mkdir.lock")
        os.mkdir(d)
        try:
            os.mkdir(d)
            report("FAIL", "c09_o_excl_and_mkdir_lock", "second mkdir succeeded")
        except FileExistsError:
            report("PASS", "c09_o_excl_and_mkdir_lock")

    @run("c10_punch_hole_seek_hole")
    def _():
        p = os.path.join(ws, "holes.bin")
        size = 8 << 20
        with open(p, "wb") as f:
            f.write(b"\xab" * size)
        fd = os.open(p, os.O_RDWR)
        os.fsync(fd)
        before = os.fstat(fd).st_blocks
        try:
            fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, 1 << 20, 4 << 20)
        except OSError as e:
            os.close(fd)
            # Sidecar hole capture degrades to dense storage without this; record, don't fail.
            report("INFO", "c10_punch_hole_seek_hole", f"fallocate unsupported: {e}")
            return
        hole = os.lseek(fd, 0, SEEK_HOLE)
        data = os.lseek(fd, hole, SEEK_DATA)
        after = os.fstat(fd).st_blocks
        os.close(fd)
        if hole < size and data > hole and after < before:
            report(
                "PASS",
                "c10_punch_hole_seek_hole",
                f"hole@{hole} data@{data} blocks {before}->{after}",
            )
        else:
            report(
                "FAIL",
                "c10_punch_hole_seek_hole",
                f"hole@{hole} data@{data} blocks {before}->{after}",
            )

    @run("c11_hardlinks")
    def _():
        d = os.path.join(ws, "hl")
        os.makedirs(d, exist_ok=True)
        a, b = os.path.join(d, "a"), os.path.join(d, "b")
        with open(a, "wb") as f:
            f.write(b"one")
        os.link(a, b)
        with open(a, "ab") as f:
            f.write(b"two")
        with open(b, "rb") as f:
            through = f.read()
        sa, sb = os.stat(a), os.stat(b)
        if through == b"onetwo" and sa.st_ino == sb.st_ino and sa.st_nlink == 2:
            report("PASS", "c11_hardlinks")
        else:
            report(
                "FAIL", "c11_hardlinks", f"read={through!r} inos={sa.st_ino},{sb.st_ino}"
            )
        farm = os.path.join(ws, "farm")
        os.makedirs(farm, exist_ok=True)
        src = os.path.join(farm, "src")
        with open(src, "wb") as f:
            f.write(b"farm")
        for i in range(100):
            os.link(src, os.path.join(farm, f"l{i}"))
        if os.stat(src).st_nlink == 101:
            report("PASS", "c11b_link_farm_100")
        else:
            report("FAIL", "c11b_link_farm_100", f"nlink={os.stat(src).st_nlink}")

    @run("c12_mmap_shared")
    def _():
        p = os.path.join(ws, "mmap_new.bin")
        with open(p, "wb") as f:
            f.write(b"\0" * 4096)
        fd = os.open(p, os.O_RDWR)
        mm = mmap.mmap(fd, 4096, mmap.MAP_SHARED)
        mm[0:4] = b"MMAP"
        mm.flush()
        mm.close()
        got = os.pread(fd, 4, 0)
        os.close(fd)
        if got == b"MMAP":
            report("PASS", "c12_mmap_shared")
        else:
            report("FAIL", "c12_mmap_shared", f"read back {got!r}")

    @run("c13_symlinks")
    def _():
        with open(os.path.join(ws, "sl_target"), "wb") as f:
            f.write(b"pointed-at")
        os.symlink("sl_target", os.path.join(ws, "sl_link"))
        target = os.readlink(os.path.join(ws, "sl_link"))
        with open(os.path.join(ws, "sl_link"), "rb") as f:
            data = f.read()
        if target == "sl_target" and data == b"pointed-at":
            report("PASS", "c13_symlinks")
        else:
            report("FAIL", "c13_symlinks", f"target={target} data={data!r}")

    @run("c14_rename_dir_tree")
    def _():
        d = os.path.join(ws, "updir")
        os.makedirs(os.path.join(d, "x"), exist_ok=True)
        with open(os.path.join(d, "x", "f"), "wb") as f:
            f.write(b"v")
        os.rename(d, os.path.join(ws, "updir2"))
        assert os.path.exists(os.path.join(ws, "updir2", "x", "f"))
        report("PASS", "c14_rename_dir_tree")

    if not seeded:
        print(
            f"SUMMARY pass={RESULTS['PASS']} fail={RESULTS['FAIL']} info={RESULTS['INFO']} (seeded checks skipped)",
            flush=True,
        )
        sys.exit(1 if RESULTS["FAIL"] else 0)

    # ── seeded (lower-layer) checks: fixture content served from the last snapshot ──

    @run("s01_seeded_content_integrity")
    def _():
        p = os.path.join(ws, SEEDED["blob"])
        got = sha256_file(p)
        want = hashlib.sha256(pattern(4 << 20)).hexdigest()
        if got == want:
            report("PASS", "s01_seeded_content_integrity")
        else:
            report("FAIL", "s01_seeded_content_integrity", "blob.bin diverged after reattach")

    @run("s02_seeded_modify_preserves_content")
    def _():
        p = os.path.join(ws, SEEDED["blob2"])
        with open(p, "ab") as f:
            f.write(b"!")
        with open(p, "rb") as f:
            data = f.read()
        if data[:-1] == pattern(1 << 20, 7) and data[-1:] == b"!":
            report("PASS", "s02_seeded_modify_preserves_content")
        else:
            report("FAIL", "s02_seeded_modify_preserves_content", "copy-up corrupted bytes")

    @run("s03_sqlite_wal_on_seeded_db")
    def _():
        sqlite_concurrent(
            "s03_sqlite_wal_on_seeded_db", os.path.join(ws, SEEDED["db"]), "wal"
        )

    @run("s04_exec_seeded_script")
    def _():
        import subprocess

        out = subprocess.run(
            [os.path.join(ws, SEEDED["script"])], capture_output=True, text=True, timeout=30
        )
        if out.returncode == 0 and out.stdout.strip() == "conformance-ok":
            report("PASS", "s04_exec_seeded_script")
        else:
            report("FAIL", "s04_exec_seeded_script", f"rc={out.returncode} out={out.stdout!r}")

    @run("s05_seeded_symlink")
    def _():
        target = os.readlink(os.path.join(ws, SEEDED["link"]))
        if target == "README.md":
            report("PASS", "s05_seeded_symlink")
        else:
            report("FAIL", "s05_seeded_symlink", f"target={target}")

    @run("s06_delete_seeded_then_excl_create")
    def _():
        p = os.path.join(ws, SEEDED["files"][0])
        os.unlink(p)
        if os.path.exists(p):
            report("FAIL", "s06_delete_seeded_then_excl_create", "unlink did not hide the path")
            return
        fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        os.close(fd)
        report("PASS", "s06_delete_seeded_then_excl_create")

    @run("s07_unlinked_but_open_seeded")
    def _():
        p = os.path.join(ws, SEEDED["files"][1])
        fd = os.open(p, os.O_RDWR)
        os.unlink(p)
        os.pwrite(fd, b"after-unlink", 0)
        got = os.pread(fd, 12, 0)
        os.close(fd)
        if got == b"after-unlink":
            report("PASS", "s07_unlinked_but_open_seeded")
        else:
            report("FAIL", "s07_unlinked_but_open_seeded", f"read={got!r}")

    @run("s08_link_of_seeded_file")
    def _():
        a = os.path.join(ws, SEEDED["files"][2])
        b = a + ".link"
        os.link(a, b)
        s1, s2 = os.stat(a), os.stat(b)
        if s1.st_ino == s2.st_ino and s1.st_nlink == 2:
            report("PASS", "s08_link_of_seeded_file")
        else:
            report(
                "FAIL",
                "s08_link_of_seeded_file",
                f"inos={s1.st_ino},{s2.st_ino} nlink={s1.st_nlink}",
            )

    @run("s09_mmap_shared_seeded")
    def _():
        p = os.path.join(ws, SEEDED["files"][3])
        fd = os.open(p, os.O_RDWR)
        size = os.fstat(fd).st_size
        mm = mmap.mmap(fd, size, mmap.MAP_SHARED)
        mm[0:4] = b"MMAP"
        mm.flush()
        mm.close()
        got = os.pread(fd, 4, 0)
        os.close(fd)
        if got == b"MMAP":
            report("PASS", "s09_mmap_shared_seeded")
        else:
            report("FAIL", "s09_mmap_shared_seeded", f"read back {got!r}")

    @run("s10_rename_seeded_dir")
    def _():
        src = os.path.join(ws, SEEDED["renameme"])
        dst = src + ".renamed"
        try:
            os.rename(src, dst)
        except OSError as e:
            import errno as _errno

            if e.errno == _errno.EXDEV:
                # Legitimate under kernel overlayfs without redirect_dir (and in userns);
                # tools fall back to copy+delete. Recorded, not failed.
                report("INFO", "s10_rename_seeded_dir", "EXDEV (copy+delete territory)")
                return
            raise
        assert os.path.exists(os.path.join(dst, "keep.txt"))
        report("PASS", "s10_rename_seeded_dir", "atomic rename of snapshot-backed dir")

    @run("s11_concurrent_seeded_reads")
    def _():
        import concurrent.futures

        p = os.path.join(ws, SEEDED["readme"])
        # blob.bin may have been touched by s01 only via read; use it for ranged reads.
        blob = os.path.join(ws, SEEDED["blob"])
        size = os.path.getsize(blob)

        def read_range(i):
            with open(blob, "rb") as f:
                f.seek((i * 137) % max(size - 4096, 1))
                return len(f.read(4096))

        with concurrent.futures.ThreadPoolExecutor(16) as ex:
            got = list(ex.map(read_range, range(256)))
        with open(p, "rb") as f:
            head = f.read(5)
        if all(n > 0 for n in got) and head == b"hello":
            report("PASS", "s11_concurrent_seeded_reads")
        else:
            report("FAIL", "s11_concurrent_seeded_reads", f"head={head!r}")

    @run("s12_ino_stability_across_modify")
    def _():
        p = os.path.join(ws, SEEDED["files"][4])
        before = os.stat(p).st_ino
        with open(p, "ab") as f:
            f.write(b"!")
        after = os.stat(p).st_ino
        report(
            "INFO",
            "s12_ino_stability_across_modify",
            f"ino {before} -> {after} ({'stable' if before == after else 'CHANGED'})",
        )

    print(
        f"SUMMARY pass={RESULTS['PASS']} fail={RESULTS['FAIL']} info={RESULTS['INFO']}",
        flush=True,
    )
    sys.exit(1 if RESULTS["FAIL"] else 0)


if __name__ == "__main__":
    multiprocessing.set_start_method("fork")
    main()
