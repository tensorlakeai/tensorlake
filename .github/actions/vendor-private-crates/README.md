# vendor-private-crates

Composite action that injects the private filesystem client, mount core, and packfile codec so CI
can build the official CLI with `--features mount,git-clone`. They live in the private
`tensorlakeai/artifact_storage` repo and are **never** committed into this public tree. The three
directories under `crates/gsvc-*` are resolution placeholders. This action fetches the real sources
for the duration of the job.

## What it does

1. Mints a short-lived token from a GitHub App scoped to `tensorlakeai/artifact_storage`.
2. Sparse-checks-out the three private crate source trees into `.mount-core/`.
3. Resolves that checkout to an exact commit; the optional FSKit companion checkout is pinned to
   the same revision instead of resolving a moving branch a second time.
4. Copies the real sources over the placeholders, keeping the placeholders' workspace-adapted
   manifests.
5. When `include-macos-fskit: "true"`, separately stages the private FSKit `Sources` and
   `Resources` for consumers that need them. Linux and Windows callers leave the
   input disabled and do not fetch or compile Swift sources. The FSKit `build.sh` remains confined
   to the dedicated TLFS.app release job.

The calling job must run `actions/checkout` for this repo first, then call this action before the
build step, and add `--features git-clone` (plus `mount` on mount-capable targets; or
`mount,git-clone` for official standalone CLI builds) to that build.
CI and releases use the reviewed 40-character `artifact_storage` commit SHA in
`crates/ARTIFACT_STORAGE_REVISION`. Update that file to a merged AS commit when consuming private
source changes. Release workflows resolve the pin once and pass it to every binary and TLFS.app
job. Ordinary test workflows omit `source-ref` to use the same pin; stacked changes may pass a
different full commit SHA explicitly. Moving branch names are rejected.

## One-time setup

Create a **GitHub App** (org-owned) with **Repository permissions → Contents: Read-only**, install
it on `tensorlakeai/artifact_storage`, and add these repository (or org) secrets to the public repo:

| Secret | Value |
| --- | --- |
| `ARTIFACT_STORAGE_APP_ID` | The GitHub App's App ID. |
| `ARTIFACT_STORAGE_APP_PRIVATE_KEY` | A generated private key (PEM) for the App. |

A GitHub App is used (instead of a PAT or deploy key) so access is short-lived, auditable, and
scoped to exactly one repo.

## Fork PRs

Pull requests from forks cannot read these secrets, so mount jobs are gated with
`if: ... pull_request.head.repo.full_name == github.repository` and are skipped for forks. The
default (no-mount) lanes still run for them, so external contributors' PRs build and test normally.

## Consumers

- `.github/workflows/tests.yaml` — full-feature Rust workspace tests, macOS private client tests, and
  the Windows CLI build with `git-clone` enabled.
- `.github/workflows/publish_cli.yaml` — release `tensorlake` binaries (Linux, macOS, Windows).

Local equivalent (no App needed, uses a sibling artifact_storage checkout): `just build-cli-full`.
Check out the recorded AS revision in that sibling to reproduce CI; the local recipe deliberately
uses the sibling's current source tree to support development of uncommitted private changes.
On macOS, `build-cli-full` and `test-cli-full` stage the matching private FSKit `Sources` and
`Resources` for the command and restore the public one-line TLFS directory afterward, including on
failure.
