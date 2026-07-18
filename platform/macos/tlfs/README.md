# tlfs — production FSKit module for `tl fs mount` on macOS

The macOS kernel path for `tl fs` mounts. No kernel extension, no macFUSE, no sudo: an
FSKit file-system extension (`FSUnaryFileSystem`) hosted in a minimal app bundle. The
Rust daemon (`tl fs mount`) owns all real state — gsvc-mount `MountCore`, the overlay,
lease heartbeats, credential rotation — and serves a small VFS wire protocol on
localhost TCP; this extension is a thin translator between FSKit's `FSVolume`
operations and that protocol.

```
kernel VFS ── fskitd ── TLFSModule.appex (this dir, Swift)
                             │  tlfs://127.0.0.1:<port>/<secret>
                             ▼
                tl fs daemon (Rust): vfsserver ── OverlayFs ── MountCore ── artifact-storage
```

Why a TCP proxy instead of linking the Rust stack into the appex: the extension runs
sandboxed under fskitd's lifecycle — it cannot read the CLI's state directories or hold
long-lived credentials, and shipping the whole mount stack as a C ABI inside an appex
makes iteration and crash isolation worse. The daemon binds 127.0.0.1 only and requires
a per-mount 32-byte hex secret in the HELLO frame (it is carried in the mount URL), so
another local user cannot attach to the volume's backend.

## Layout

- `Sources/Extension/TLFS.swift` — the whole module: wire codec, blocking socket pool,
  `TLFSFileSystem` (probe/load), `TLFSVolume` (lookup/enumerate/create/remove/rename/
  read/write/setattr/symlink + open-close discipline).
- `Resources/Appex-Info.plist` — extension point `com.apple.fskit.fsmodule`, principal
  class `TLFSFileSystem`, `FSShortName tlfs`, URL scheme `tlfs`.
- `Resources/Appex.entitlements` — `com.apple.developer.fskit.fsmodule`, app sandbox,
  `com.apple.security.network.client` (the TCP leg), application/team identifiers.
- `Resources/App-Info.plist` — the host app (`ai.tensorlake.tlfs`); it exists only to
  carry the appex in `Contents/Extensions/`.
- `build.sh` — swiftc-only build (no Xcode project), codesign with the provisioning
  profile, registration steps.

## Wire protocol (v1)

Frames are `u32-le length | u8 opcode | payload`; responses `u32-le length | i32-le
errno | payload`. Strings/bytes are u32-length-prefixed; attrs are packed
`u64 ino | u8 kind | u64 size | u16 perm | u8 upper | u64 mtime_sec | u32 mtime_nsec |
u64 content_version`. Opcodes `HELLO(0)…STATFS(20)` mirror the overlay's op set. The
canonical definition lives in the injected private `gsvc-fs-client/src/vfsserver.rs`; keep both
sides in lockstep — version 3 is enforced by the HELLO check and has no downgrade negotiation.

## Build and install (development)

Requirements: macOS 26 SDK (Xcode 26.x), a provisioning profile for the explicit App ID
`ai.tensorlake.tlfs.fsmodule` (team `9DQWQ9K87W`) with the **FSKit Module** capability
(restricted entitlement), Apple Development signing identity.

```sh
TLFS_PROVISION_PROFILE=~/Downloads/tlfsfsmoduledev.provisionprofile ./build.sh
```

A dev build launches **only on Macs whose UDID is in the development profile** — AMFI
validates the restricted entitlement against the embedded profile, and development
profiles carry a device allowlist. That is fine for iteration and useless for users;
see Distribution below.

The script builds `build/TLFS.app` with the appex embedded, then registers it
(`lsregister`, `pluginkit -a`, `pluginkit -e use`). If the module does not appear in
`fskit_agent`'s enabled set, see the registration triple-gate in
`platform/macos/fskit-hello/README.md` — that prototype README is the canonical list of
FSKit mechanics and gotchas (NSExtensionMain entry point, enabledModules.plist,
fskitd's per-URL EBUSY caching, the one-copy-per-bundle-id rule). Do not rediscover
those; they are all still true.

The daemon mounts with `/sbin/mount -F -t tlfs tlfs://127.0.0.1:<port>/<secret> <dir>`
(`daemon.rs` does this automatically on macOS) and unmounts with plain `umount`.

## Distribution (end users)

Any Mac, outside the App Store: Developer ID signing + notarization. The `tlfs-app` job
in `.github/workflows/publish_cli.yaml` builds this on release, attaches
`TLFS-<version>.app.zip` to the `cli-v<version>` GitHub release, and **embeds the same
zip into the darwin CLI binary** (`TLFS_APP_ZIP` + `crates/cli/build.rs`), so
**`tl fs setup`** on an official build installs offline: it ditto-installs the embedded
app to `/Applications`, launches it once to register the extension, and walks the System
Settings toggle. Source builds have nothing embedded and fall back to downloading the
release asset matching the CLI version; `--from <path-or-url>` overrides either path.
`tl fs setup --check` diagnoses an install. Users rarely run setup by hand: `tl fs
mount` pre-flights the extension and auto-runs the install when it is missing, stopping
or when its stamped version differs from the CLI, stopping only for the Settings toggle (the one
step Apple reserves for the user). Embedding (and failing that, the shared
release tag) is the wire-protocol-skew defense: the VFS protocol has no version
negotiation beyond HELLO.

One-time Apple portal prerequisites (team `9DQWQ9K87W`):

1. **Developer ID Application certificate** — export as .p12 for CI.
2. **Developer ID provisioning profile** for App ID `ai.tensorlake.tlfs.fsmodule` with
   the FSKit Module capability. Unlike the development profile there is no device list,
   so the appex launches on any Mac. If the FSKit capability is not offered when
   creating a Developer ID profile, request it from Apple developer support.
3. **App Store Connect API key** (Developer role) for `notarytool`.

CI secrets: `TLFS_SIGNING_CERT_P12` + `TLFS_SIGNING_CERT_PASSWORD`,
`TLFS_PROVISION_PROFILE` (base64), `TLFS_NOTARY_KEY` (base64 .p8) +
`TLFS_NOTARY_KEY_ID` + `TLFS_NOTARY_ISSUER`. The job is `continue-on-error` until these
exist; flip that once they do.

Local release build (same thing CI runs):

```sh
TLFS_PROVISION_PROFILE=~/profiles/tlfs-developer-id.provisionprofile \
TLFS_NOTARY_PROFILE=tlfs-notary \
  ./build.sh --release --notarize    # emits build/TLFS.app.zip, stapled
```

Signing chain notes: release mode signs with hardened runtime and a secure timestamp
(`--timestamp`; dev mode uses `--timestamp=none`, which notarization rejects), embeds
the Developer ID profile, notarizes the zipped app, and staples the ticket into the
bundle so Gatekeeper passes offline. The embedded profile's validity is tied to the
Developer ID cert — renewing the cert means re-signing and re-shipping the app, or
mounts on user machines start failing with AMFI launch denials.

Target floor: `arm64-apple-macos26.0` (Apple Silicon, macOS 26+). The cache-coherence
behavior below was measured on macOS 26.5 lifs, and the generic-URL-resource FSKit
surface is 26-era; do not silently lower `LSMinimumSystemVersion`.

## Semantics notes

- Item lifetime follows Apple's passthrough sample: `lookupItem` counts a lookup debt,
  `reclaimItem` releases the daemon file handle and sends FORGET for the owed count.
- `enumerateDirectory` is stateless per call: OPENDIR/READDIR pages/RELEASEDIR, with
  `.`/`..` synthesized at cookies 0/1 and real entries from cookie 2. When attributes
  are requested it does a per-entry LOOKUP followed by an immediate FORGET.
- Open modes upgrade by opening a new daemon handle before releasing the old one;
  `closeItem` fsyncs writable handles before RELEASE.
- Writes are chunked at 2 MiB per WRITE frame.

## Cache coherence after restore/snapshot

`tl fs restore` and `tl fs snapshot` mutate overlay state behind the kernel's back, and
FSKit has no invalidation-push API — so coherence is engineered from what the kernel
does honor (all measured on macOS 26.5 lifs):

- **Real content timestamps.** The wire attr carries an mtime: upper-backed items report
  the real file mtime; lower-backed items report when the mount first served their
  pinned commit. It moves exactly when content can have changed, which is what the
  kernel's revalidation compares. (Never report `time(nil)`: constant-now mtimes defeat
  caching and still leave minutes-long stale windows.)
- **Opaque content versions guard retained FSItems.** FSKit may keep one `FSItem` alive across
  independent opens. The private v3 localhost wire therefore carries an identity derived from the
  pinned snapshot (or upper inode/ctime); a new read open replaces any handle whose identity is
  stale even when size and mtime happen to match.
- **Directory verifier from the dir's mtime.** A constant `FSDirectoryVerifier` makes
  the kernel treat cached listing pages and resume cookies as forever-valid (`ls` shows
  deleted files, or a stale empty tail). Deriving it from the directory's wire mtime
  makes it re-enumerate exactly when needed.
- **Every packed entry needs a real `itemID`.** Plain enumeration (`getdirentries64`,
  no attributes requested) surfaces it as `d_ino`; the kernel silently drops entries
  packed with `.invalid` — `ls` (getattrlistbulk) worked while `readdir(3)` returned
  nothing.
- **The CLI converges the kernel view before returning** (`converge_kernel_view` in
  `fs.rs`): after restore it opens every changed path (open revalidates close-to-open
  and cuts through stale positive *and* negative name-cache entries; plain `stat(2)`
  serves the cache until a ~30s TTL), purges cached pages of changed files via
  `msync(MS_INVALIDATE)` on a shared mapping (attribute changes alone make the kernel
  adopt a new size but never refetch pages — a file that grew behind the kernel keeps a
  zero-filled tail forever otherwise), and breaks pinned negative entries with an
  `O_CREAT|O_EXCL` / `mkdir` probe that the overlay answers with `EEXIST`.

## Extended attributes

The native snapshot format does not currently promise general xattr persistence. The FSKit module
advertises limited, mount-lifetime support only for macOS bookkeeping attributes (`provenance`,
`quarantine`, and `lastuseddate`) so routine copies do not materialize transient `._*` AppleDouble
files as filesystem content. Meaningful metadata such as resource forks, Finder info, tags, and
comments remains unsupported; the module does not accept and silently discard it.

Measured result: restore returns in ~0.1s with `open`/`read` and directory listings
fully coherent, both directions, repeatedly. Residual caveat: a bare `stat(2)`/`lstat`
of a changed path that is never re-opened can serve cached attributes for up to the
kernel's TTL (~30s); `open(2)` always sees truth.

The daemon-side halves of this: the overlay enforces `EEXIST` on create/mkdir/symlink
itself (the kernel skips LOOKUP when it trusts a stale negative entry), whiteouts hide
whole subtrees for inode-based ops (`whited_out` walks ancestors), and nodes born in
the upper lazily re-bind to the advanced lower after a snapshot seals them
(`lower_binding`), so cached kernel inodes keep working across the swap.

## Iteration gotchas (beyond fskit-hello's README)

- Rebuilding/re-registering the appex while a volume is mounted can make `fskit_agent`
  prune the module from `~/Library/Group Containers/group.com.apple.fskit.settings/`
  `enabledModules.plist` on the next LaunchServices database change — the running
  module process is killed ("Module Death") and later mounts fail with `mount: Unable
  to invoke task`. Re-add the id with PlistBuddy, re-run the pluginkit gate, and kill
  `fskit_agent` (`kill -9`; plain `pkill` may not take).
- A mountpoint directory whose volume died uncleanly can wedge (`rmdir` hangs); make a
  fresh mountpoint and let a reboot reap the zombie.
