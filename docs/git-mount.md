# Repository mounts

`tl git mount` exposes a repository lazily without cloning it. Reads fetch only the objects a
process opens. A writable mount journals edits locally and autosaves them as durable, non-commit
WAL checkpoints on the server. An explicit snapshot materializes the current checkpoint as a
commit on the private workspace line; only `promote` (or an explicit snapshot from a `--publish`
mount) changes a branch.

## Sources and subtrees

The target grammar is:

```text
REPOSITORY[:BRANCH|TAG|FULL_COMMIT][//SUBTREE]
```

Examples:

```bash
tl git mount monorepo /code
tl git mount monorepo:feature/auth /code
tl git mount monorepo:refs/tags/v2 /code --ro
tl git mount monorepo:0123456789abcdef0123456789abcdef01234567 /code --ro
tl git mount monorepo:main//services/auth /code
```

A branch or tag mount follows that exact canonical ref. Full 40-hex commits are pinned. A subtree mount makes
the selected directory appear as the mount root: `/code/src/lib.rs` represents
`services/auth/src/lib.rs` in the repository. The server validates that the source exists and the
subtree is a directory. Snapshot upserts, deletes, and renames are prefixed back into the complete
repository tree, so siblings outside the subtree remain untouched.

## Writable workspaces and read-only views

A plain mount is writable, but mounting alone does not allocate a server workspace. The first
autosaved checkpoint after a write creates the durable private workspace lazily. Resume that
workspace, including its unsnapshotted WAL, from any machine with:

```bash
tl git mount monorepo /code --workspace WORKSPACE_ID
```

`--ro` creates no workspace, ref, or GC root. It records only an expiring, principal-bound presence
row so the control plane can show live mounts; a clean unmount removes the row immediately and a
crashed sandbox disappears at expiry.

## Workflow

```bash
tl git status [/code]
tl git snapshot [/code] --message "checkpoint"
tl git sync [/code] [BRANCH|TAG|FULL_COMMIT]
tl git rebase [/code] BRANCH|TAG|FULL_COMMIT
tl git promote [/code] BRANCH [--merge]
tl git log [/code|REPOSITORY]
tl git smartlog [/code|REPOSITORY] [--project]
```

Writes first enter the crash-safe local journal. The mount continuously prepares and uploads them,
then records ordered, idempotent checkpoints in the workspace WAL. These checkpoints make edits
durable and resumable, but they are not repository commits and do not update a branch. `snapshot`
flushes the current WAL state and materializes it as one immutable commit on the workspace's private
line.

`sync` refreshes the current source while carrying the unsnapshotted WAL forward. With a target it
switches a stateless read-only view or a workspace with no materialized snapshots without rewriting
history. Once a workspace has snapshots, changing its base is an explicit `rebase`. Rebase runs
server-side and replays both materialized snapshots and the unsnapshotted WAL tail; conflicts
materialize as normal diff3 markers unless `--fail-on-conflict` is requested. A rebase can touch the
full repository: conflicts inside a mounted subtree are shown with subtree-relative paths, while
conflicts outside it are reported as repository paths and must be resolved by resuming the same
workspace in a view that contains them.

A `--publish` workspace continuously serves its fixed target branch. Its explicit snapshots are
reconciled onto that branch server-side, so `sync` refreshes the target but cannot retarget it and
`rebase` is intentionally rejected: rebasing only the private workspace ref would make the result
invisible in the live branch view. Create a normal workspace when explicit rebase/retarget control
is required.

Before replacing a snapshotted chain, rebase retains its prior tip under a recovery ref. `log`,
`smartlog`, and `status` expose these retained chains. They remain recoverable and GC-rooted until
the workspace is explicitly deleted; deleting the workspace atomically releases its active and
retained refs.

`promote` autosaves outstanding edits, materializes the current state as a workspace snapshot, and
deliberately lands it onto a real branch. The default is a squash landing; `--merge` creates a true
merge when the branch moved and publishes nothing if conflicts remain.

## How this differs from `tl fs`

`tl fs` is a continuously published drive, not a private commit workflow. A writable filesystem
mount autosaves its latest state to the shared drive and retains recent saves as an ephemeral WAL.
`tl fs snapshot [PATH] -m "message"` records a new autosaved generation as a permanent, billed
snapshot; it is a quiet no-op when there is no new generation. `tl fs push DIR FILESYSTEM` follows
the same rule: without `-m` it is an autosave, while `-m` creates a permanent snapshot.

Filesystem snapshots are path-addressed rather than repository-style named commits. `PATH` is a
mounted or tracked directory and defaults to the attachment containing the current directory. A
filesystem gets its name at `tl fs create`; there is no `tl fs name` command or snapshot-name
argument. `-m`/`--message` is descriptive metadata. Use `tl fs history [FILESYSTEM|PATH]` to see
permanent snapshots first and the visibly ephemeral recent autosave WAL second. Its JSON output
keeps these as distinct `snapshots` and `autosaves` arrays. `tl fs status [PATH]` reports local
changes, the last autosave time, and the permanent snapshot count without walking filesystem
history.

Deleting a filesystem snapshot removes that permanent retention point; it does not immediately
delete content that another snapshot, autosave, or live filesystem head still references.
Unreferenced bytes are reclaimed asynchronously.
