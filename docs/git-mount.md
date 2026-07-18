# Repository mounts

`tl git mount` exposes a repository lazily without cloning it. Reads fetch only the objects a
process opens. Paths written through a writable mount stay in its local overlay until an explicit
snapshot; the durable workspace and every snapshot live on the server.

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

A plain mount is writable and creates a durable private workspace. Resume it from any machine with:

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

`sync` refreshes the current source. With a target it switches a stateless read-only view or a
pristine workspace without rewriting history. Once a workspace has snapshots, changing its base is
an explicit `rebase`. Rebase runs server-side; conflicts materialize as normal diff3 markers unless
`--fail-on-conflict` is requested. A rebase can touch the full repository: conflicts inside a
mounted subtree are shown with subtree-relative paths, while conflicts outside it are reported as
repository paths and must be resolved by resuming the same workspace in a view that contains them.

A `--publish` workspace continuously serves its fixed target branch. Its explicit snapshots are
reconciled onto that branch server-side, so `sync` refreshes the target but cannot retarget it and
`rebase` is intentionally rejected: rebasing only the private workspace ref would make the result
invisible in the live branch view. Create a normal workspace when explicit rebase/retarget control
is required.

Before replacing a snapshotted chain, rebase retains its prior tip under a recovery ref. `log`,
`smartlog`, and `status` expose these retained chains. They remain recoverable and GC-rooted until
the workspace is explicitly deleted; deleting the workspace atomically releases its active and
retained refs.

`promote` deliberately lands the workspace onto a real branch. The default is a squash landing;
`--merge` creates a true merge when the branch moved and publishes nothing if conflicts remain.
