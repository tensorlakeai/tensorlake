# tlfs — macOS FSKit module for `tl fs mount` (source is private)

The production FSKit file-system extension behind `tl fs mount` on macOS is not part of
this public repository. Like the `gsvc-mount` / `gsvc-codec` / `gsvc-fs-client` mount
core (see `crates/gsvc-mount/src/lib.rs`), its source lives in the private
`tensorlakeai/artifact_storage` repo, under the same `platform/macos/tlfs/` path.

What ships publicly is the compiled, Developer-ID-signed, notarized `TLFS.app` bundle:

- On release, the `tlfs-app` job in `.github/workflows/publish_cli.yaml` sparse-checks-
  out `platform/macos/tlfs/` from artifact_storage (same GitHub App credentials as the
  private-crate vendoring), builds/signs/notarizes it, and attaches
  `TLFS-<version>.app.zip` to the `cli-v<version>` GitHub release.
- The darwin CLI build embeds that same zip (`TLFS_APP_ZIP` →
  `crates/gsvc-fs-client/build.rs`), so `tl fs setup` on an official binary installs the
  extension offline. Source builds fall back to downloading the release asset matching
  the CLI version, or `tl fs setup --from <path-or-url>`.

Building the app yourself requires the private source plus an Apple provisioning
profile with the restricted FSKit Module capability. With an artifact_storage checkout
as a sibling of this repo (or `ARTIFACT_STORAGE_DIR` pointing at one):

```sh
just build-tlfs-app                        # dev build (device-limited profile)
just build-tlfs-app --release --notarize   # distribution build
```

The `fskit-hello` prototype (a from-scratch FSKit walkthrough) lives in
artifact_storage as well, at `platform/macos/fskit-hello/`.
