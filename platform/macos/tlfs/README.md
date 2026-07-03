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
`u64 ino | u8 kind | u64 size | u16 perm | u8 upper`. Opcodes `HELLO(0)…STATFS(20)`
mirror the overlay's op set. The canonical definition lives in the daemon at
`crates/cli/src/commands/fs/vfsserver.rs`; keep both sides in lockstep — there is no
version negotiation beyond the HELLO check yet.

## Build and install

Requirements: macOS 15.4+ (FSKit), a provisioning profile for an explicit App ID with
the **FSKit Module** capability (restricted entitlement), Apple Development signing
identity. Current profile: App ID `ai.tensorlake.tlfs.fsmodule`, team `9DQWQ9K87W`.

```sh
TLFS_PROVISION_PROFILE=~/Downloads/tlfsfsmoduledev.provisionprofile ./build.sh
```

The script builds `build/TLFS.app` with the appex embedded, then registers it
(`lsregister`, `pluginkit -a`, `pluginkit -e use`). If the module does not appear in
`fskit_agent`'s enabled set, see the registration triple-gate in
`platform/macos/fskit-hello/README.md` — that prototype README is the canonical list of
FSKit mechanics and gotchas (NSExtensionMain entry point, enabledModules.plist,
fskitd's per-URL EBUSY caching, the one-copy-per-bundle-id rule). Do not rediscover
those; they are all still true.

The daemon mounts with `/sbin/mount -F -t tlfs tlfs://127.0.0.1:<port>/<secret> <dir>`
(`daemon.rs` does this automatically on macOS) and unmounts with plain `umount`.

## Semantics notes

- Item lifetime follows Apple's passthrough sample: `lookupItem` counts a lookup debt,
  `reclaimItem` releases the daemon file handle and sends FORGET for the owed count.
- `enumerateDirectory` is stateless per call: OPENDIR/READDIR pages/RELEASEDIR, with
  `.`/`..` synthesized at cookies 0/1 and real entries from cookie 2. When attributes
  are requested it does a per-entry LOOKUP followed by an immediate FORGET.
- Open modes upgrade by opening a new daemon handle before releasing the old one;
  `closeItem` fsyncs writable handles before RELEASE.
- Writes are chunked at 2 MiB per WRITE frame.

## Known issue: post-restore/snapshot cache staleness

`tl fs restore` and `tl fs snapshot` mutate overlay state behind the kernel's back.
The kernel's UBC may serve cached file content for a short window (observed seconds,
occasionally longer) until it revalidates attributes and notices the size/mtime
change; directory listings recover faster than `open`+`read` of a cached file. A
`stat(2)` of the file forces revalidation. Fix candidates: FSKit invalidation pushes
(if/when the API allows) or attribute-validity hints tuned down for upper-backed items.
