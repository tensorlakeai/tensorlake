#!/usr/bin/env bash
# POSIX conformance run against a real `tl fs` mount (Linux only).
#
# Two phases against the production mount path, whatever overlay implementation backs it:
#   1. fresh writable workspace  -> upper-born checks (battery.py)
#   2. seed fixture -> snapshot -> unmount -> reattach (--workspace) -> seeded checks
#      (fixture content is now served from the versioned lower side, which is the
#      snapshot/restore path users actually resume through)
#
# Requirements: linux, fuse3, python3, a `tl` binary (TL_BIN or on PATH), and working server
# credentials (`tl login`, or TENSORLAKE_GIT_TOKEN against a dev server). Exit code = FAIL count.
#
# Provenance: ported from the artifact_storage issue-#24 overlay spike (29 PASS / 0 FAIL on
# kernel overlayfs 7.0.7); kernel-overlayfs-specific upperdir checks were dropped as
# implementation details. This suite is the acceptance bar for any mount relayering.

set -uo pipefail

if [ "$(uname -s)" != "Linux" ]; then
    echo "fs-posix-conformance is Linux-only (FUSE mounts)" >&2
    exit 1
fi

HERE=$(cd "$(dirname "$0")" && pwd)
TL=${TL_BIN:-tl}
FS="conformance-$(date +%s)"
RUN=$(mktemp -d /tmp/fs-conformance.XXXXXX)
MNT1=$RUN/mnt1
MNT2=$RUN/mnt2
LOG=$RUN/conformance.log
mkdir -p "$MNT1" "$MNT2"

say() { echo "$@" | tee -a "$LOG"; }
FAILS=0

cleanup() {
    set +e
    mountpoint -q "$MNT2" && "$TL" fs unmount "$MNT2" --delete >>"$LOG" 2>&1
    mountpoint -q "$MNT1" && "$TL" fs unmount "$MNT1" --delete >>"$LOG" 2>&1
    "$TL" fs rm "$FS" >>"$LOG" 2>&1
}
trap cleanup EXIT

say "== conformance run: fs=$FS tl=$("$TL" --version 2>/dev/null | head -1) kernel=$(uname -r) =="

"$TL" fs create "$FS" >>"$LOG" 2>&1 || { say "FAIL create_fs -- see $LOG"; exit 1; }
MOUNT_OUT=$("$TL" fs mount "$FS" "$MNT1" 2>&1 | tee -a "$LOG")
mountpoint -q "$MNT1" || { say "FAIL mount -- $MOUNT_OUT"; exit 1; }
WS=$(echo "$MOUNT_OUT" | sed -n 's/.*workspace \([a-zA-Z0-9_-]*\)).*/\1/p' | head -1)
[ -n "$WS" ] || { say "FAIL parse_workspace_id -- $MOUNT_OUT"; exit 1; }
say "workspace: $WS"

say "== phase 1: upper-born checks =="
python3 "$HERE/battery.py" "$MNT1" 2>&1 | tee -a "$LOG"
FAILS=$((FAILS + $(grep -c '^FAIL ' "$LOG" || true) - FAILS))

say "== seeding fixture =="
(
    set -e
    cd "$MNT1"
    mkdir -p seed/bin seed/dir seed/renameme
    printf 'hello conformance\n' > seed/README.md
    printf '#!/bin/sh\necho conformance-ok\n' > seed/bin/run.sh
    chmod 755 seed/bin/run.sh
    ln -s README.md seed/link
    for i in 1 2 3 4 5; do printf 'seed file %s\n' "$i" > "seed/dir/f$i.txt"; done
    printf 'kept\n' > seed/renameme/keep.txt
    python3 - <<'PYEOF'
import sqlite3

db = sqlite3.connect("seed/seed.db")
db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
db.executemany("INSERT INTO t(v) VALUES (?)", [(f"seed{i}",) for i in range(50)])
db.commit()
db.close()

def pattern(n, mult=1):
    return bytes(((i * mult) % 251) for i in range(n))

open("seed/blob.bin", "wb").write(pattern(4 << 20))
open("seed/blob2.bin", "wb").write(pattern(1 << 20, 7))
PYEOF
) >>"$LOG" 2>&1 || { say "FAIL seed_fixture -- see $LOG"; exit 1; }

"$TL" fs snapshot "$MNT1" -m "conformance seed" >>"$LOG" 2>&1 \
    || { say "FAIL snapshot -- see $LOG"; exit 1; }
"$TL" fs unmount "$MNT1" >>"$LOG" 2>&1 || { say "FAIL unmount -- see $LOG"; exit 1; }

say "== phase 2: reattach; fixture served from the snapshot (lower) =="
"$TL" fs mount "$FS" --workspace "$WS" "$MNT2" >>"$LOG" 2>&1
mountpoint -q "$MNT2" || { say "FAIL reattach -- see $LOG"; exit 1; }
python3 "$HERE/battery.py" "$MNT2" --seeded 2>&1 | tee -a "$LOG"

PASSES=$(grep -c '^PASS ' "$LOG" || true)
FAILS=$(grep -c '^FAIL ' "$LOG" || true)
INFOS=$(grep -c '^INFO ' "$LOG" || true)
say "== RESULT pass=$PASSES fail=$FAILS info=$INFOS log=$LOG kernel=$(uname -r) =="
grep '^FAIL ' "$LOG" || true
exit "$FAILS"
