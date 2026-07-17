# Durable local snapshot state

Status: accepted and implemented by the native `tl fs` client.

## Decision

`tl fs` uses one embedded redb database in each mount or tracked-directory state directory as
the local authority for mutation discovery and snapshot lifecycle state. redb was selected over
RocksDB, SQLite, and a purpose-built WAL because it provides:

- atomic multi-table transactions and ordered key ranges
- an explicit `Immediate` durability boundary
- process-level exclusive writer locking and read-only inspection
- corruption/open failures that can be surfaced fail-closed
- a pure-Rust build on macOS and Linux without a native database toolchain
- bounded, predictable memory use suitable for small sandboxes

The database stores metadata only. File bytes remain in the overlay, immutable generation
captures, generation-owned preparation files, and remote blob storage. Every capture/staging
artifact has an ownership row before creation, and retirement/reset removes the row in the same
transaction that retires its generation. Startup removes only unowned artifacts; it never guesses
ownership from a filename.

## Durability contract

Every first mutation of a path in an open generation records a durable intent before the overlay
operation is allowed to start. A crash between the journal commit and the filesystem operation
therefore leaves a conservative false positive; a completed filesystem operation cannot be
forgotten.

Repeated writes to an already covered path do not write another durable row until a generation
freeze rearms the path. Namespace operations record all participating paths before changing the
overlay.

Freezing is atomic in the local database:

1. generation `N` changes from `Open` to `Frozen`
2. the exact resolved namespace and immutable source capture for `N` is stored
3. generation `N+1` is created as `Open`

For a managed overlay, the capture uses reflinks where available and otherwise hardlinks the
upper file into the generation-owned capture. The VFS breaks every shared upper inode under the
mutation fence before a later write or truncate, so the captured bytes remain immutable without
copying a multi-gigabyte generation during freeze. An unmanaged tracked directory cannot enforce
that copy-on-write rule and therefore uses reflink-or-copy.

New writes can continue in `N+1` while the background worker reads, hashes, compresses, uploads,
and prepares `N`. Up to four content-only generations may be frozen or prepared at once, with
strictly ordered publication and retirement. A lower-backed directory rename changes the lower
namespace coordinate system, so freezing a later generation pauses until that rename is adopted;
this is a correctness fence, not a return to tree walking.

A snapshot request persists its operation ID before the first network call and publishes only the
prepared candidate. The request path never falls back to reading file content or walking the
overlay. Managed mounts, tracked directories, and the initial cold import all feed the same
durable generation records and common publication/rebase/adoption state machine. They differ only
at discovery and preparation: the first arbitrary-directory import must walk, and a tracked
directory without an authoritative watcher must reconcile metadata before each freeze.

Publication adoption, sealed-baseline installation, and generation retirement are one local
transaction. Completed request receipts are retained in a bounded table so a lost daemon reply
can be answered idempotently after the active generation rows have been retired.

## Recovery

Normal daemon startup opens the database before serving the mount and reconstructs the in-memory
dirty cache from live generation rows. It does not recursively walk `upper/` or `wh/`.

Frozen, prepared, and publish-requested generations resume with their original base and operation
IDs. Preparation and publication use separate durable operation IDs; status lookup is scoped to
the preparation operation rather than a short-lived upload session, so reminting credentials
after response loss cannot hide a pending result. Permanent client/protocol failures are
dead-lettered with their reason and stop automatic retries. Missing, corrupt, newer-schema, or
identity-mismatched state fails closed and preserves the overlay for `tl fs doctor`.

The only normal full walks are:

- the unavoidable first import of an arbitrary directory
- strict reconciliation of a plain directory without an authoritative watcher history
- an explicit conservative repair/import
- the one-time local cutover that protects unsaved bytes from a pre-database development mount

There is no server-side migration or dual-read period. This storage engine has not reached
production, so disposable development server state may be recreated. The local cutover exists
only to avoid discarding unsnapshotted user bytes in `upper/` and `wh/`; after it commits, legacy
prepared/request/sealed JSON files are removed as authorities.

## Identity and sensitive data

The database identity contains the project, filesystem, workspace, and a random local store UUID.
Opening it for another identity fails closed. Credentials, API keys, bearer tokens, presigned
URLs, and reusable upload authorization are never persisted. Server operation/session identifiers
may be stored because retries remint credentials and upload targets.

## Clear and restore

`snapshot --clear` is generation-scoped. It removes only retained upper/whiteout entries proven
to belong to the published generation, skips paths dirtied in a later generation, and retains
ignored or local-only files.

Native restore is one adoption operation: after the server selects the restored snapshot, the
daemon clears the old overlay and transactionally resets the local generation engine to that
snapshot. It does not enter a separate reindex state or rebuild the journal by walking.

Human `tl fs status` is a local diagnostic. It reads cached immutable attachment facts, the local
journal, and bounded daemon inspection calls without requiring authentication or platform
availability. If a live daemon stops answering between its health probe and dirty-state query,
status reports the daemon and local change state as unknown instead of hanging or claiming the
mount is clean. `tl fs status --json` retains its explicit live-server record contract.

The restore request ID is committed before the server call. A permanent failure moves it to a
diagnostic dead-letter row and releases the write fence; an ambiguous/retryable result keeps the
fence and all local evidence until recovery can prove an outcome.

## Operational interface

`tl fs doctor <mount-or-binding> [--json]` reads the live daemon state or opens a detached
database read-only. It reports store identity and size, generation states, dirt and rename counts,
ordered mutation counts, prepared candidates, pending or failed request IDs, restore state,
generation-owned staging bytes, oldest unretired generation, and sealed baselines.
The daemon's structured mutation-journal event reports group size, queue/commit time, and rolling
p50/p95/p99 end-to-end durability latency over the latest 256 first-dirty records.

Retirement deletes mutation, capture, prepared, request, and artifact-ownership rows in the same
transaction that advances `last_retired_generation`. redb reuses freed pages, so repeated saves do
not append one database history forever; the file may retain its largest historical allocation as
a reusable high-water mark. An explicit offline doctor repair uses copy/build/swap and produces a
minimal database when physical shrinking is required. Full redb compaction is intentionally not
run in the live daemon because it requires exclusive ownership and has an unbounded stop-the-world
phase.

ENOSPC is fail-closed at both durability boundaries. If the immediate journal commit fails, the
corresponding filesystem mutation fails before changing the overlay. If capture or preparation
staging fills the volume, the frozen generation and all ownership evidence remain retryable while
new writes continue in later generations; the four-generation limit then applies backpressure
instead of allowing unbounded local growth.

Use `just test-fs-journal` as the authoritative local validation target. It runs the complete
private/full-feature CLI suite because the state engine crosses the VFS, daemon, plain-directory,
and CLI control paths.

## Rollback

Rollback to a client that does not understand the redb local-state format is unsupported. Store
identity and schema validation prevent a newer state directory from being silently interpreted as
clean by the new client. Older development clients must use an explicit discard/remount workflow;
they must not be used to mutate a converted state directory.
