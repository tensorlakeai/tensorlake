# vendor-mount-core

Composite action that swaps the **private** `gsvc-mount` mount core into `crates/gsvc-mount` so a
CI job can build the CLI with `--features mount` (the `tl fs mount` stack). The mount core lives in
the private `tensorlakeai/artifact_storage` repo and is **never** committed into this public tree —
`crates/gsvc-mount` here is only a resolution placeholder (see its `Cargo.toml`). This action
fetches the real source onto the runner for the duration of the job.

## What it does

1. Mints a short-lived token from a GitHub App scoped to `tensorlakeai/artifact_storage`.
2. Sparse-checks-out `crates/gsvc-mount/src` from that repo into `.mount-core/`.
3. Copies the real source over the placeholder, keeping the placeholder's tensorlake-adapted
   `Cargo.toml`.

The calling job must run `actions/checkout` for this repo first, then call this action before the
build step, and add `--features mount` (or `TENSORLAKE_CLI_FEATURES=mount` for the npm build) to
that build.

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

- `.github/workflows/tests.yaml` — `rust_mount_feature` (build + test with mount).
- `.github/workflows/publish_cli.yaml` — release `tensorlake` binaries (Linux + macOS).
- `.github/workflows/publish_npm.yaml` — `build-cli-binaries` `tl` binaries (Linux + macOS).

Local equivalent (no App needed, uses a sibling artifact_storage checkout): `just build-cli-mount`.
