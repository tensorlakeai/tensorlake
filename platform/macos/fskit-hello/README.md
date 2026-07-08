# FSKit hello-world module (prototype)

The de-risk prototype for the `tl fs` macOS mount binding: a minimal FSKit
`FSUnaryFileSystem` (one read-only `hello.txt`), compiled headlessly with
`swiftc` (no xcodeproj), hand-assembled bundles, signed, registered, and
enabled. macFUSE is abandoned (its mount returns EPERM on macOS 26 even with
the kext loaded); FSKit is the sanctioned kextless path (macOS 26+; same
approach as Archil).

## Status: FULLY WORKING

Validated end to end on macOS 26.5 (2026-07-03): builds headlessly, signs with
the team's development identity + provisioning profile, registers, enables,
launches, **mounts, and serves reads through the kernel** — no sudo, no kext:

    $ mount -F -t tlfshello 'tlfshello://x' /tmp/hellomnt
    $ cat /tmp/hellomnt/hello.txt
    hello from tlfs

Two requirements beyond the original notes:

1. `com.apple.developer.fskit.fsmodule` is a restricted entitlement — the appex
   embeds a macOS development provisioning profile for the explicit App ID
   `ai.tensorlake.tlfs.fsmodule` (FSKit Module capability enabled in the
   portal) as `Contents/embedded.provisionprofile`, and the signing
   entitlements include application-identifier + team-identifier matching it.
   `build.sh` reads the profile path from `TLFS_PROVISION_PROFILE`
   (default: ~/Downloads/tlfsfsmoduledev.provisionprofile).
2. `EXExtensionPrincipalClass` is NOT optional in practice: without it the
   extension launches and even creates its FSMachPort, but fskit_agent cannot
   fetch the listener endpoint (NSCocoaErrorDomain 4099) and fskitd terminates
   the instance. Point it at the FSUnaryFileSystem subclass and pin the ObjC
   name with `@objc(...)` on the Swift class.

(Original pre-profile notes follow.) The earlier blocker was that
`com.apple.developer.fskit.fsmodule` is a **restricted entitlement**: the
appex needs a macOS provisioning profile with the "FSKit Module" capability
for team 9DQWQ9K87W, embedded as `Contents/embedded.provisionprofile`, then
re-signed. Without it AMFI kills the launch
(`taskgated-helper: Disallowing … no eligible provisioning profiles found`).
A control build *without* the entitlement launches and FSKit accepts the
delegate but cannot obtain the kernel volume port — the entitlement is
functionally mandatory.

## Hard-won mechanics (do not rediscover)

- Entry point (validated config, mirrors Apple's msdos/exfat/ftp modules): NO
  Swift `@main`. Link the appex binary with `-parse-as-library -Xlinker -e
  -Xlinker _NSExtensionMain` and set `EXExtensionPrincipalClass` to the
  `FSUnaryFileSystem` subclass, pinned with `@objc(ClassName)`. The
  `@main … UnaryFileSystemExtension` path launches and creates its FSMachPort
  but the process exits before fskit_agent can fetch the listener endpoint
  (NSCocoaErrorDomain 4099 → "Couldn't communicate with a helper application").
- Appex embeds under `App.app/Contents/Extensions/*.appex` (ExtensionKit, not PlugIns).
- Registration: `lsregister -f App.app` FIRST, then `pluginkit -a <appex>`,
  `pluginkit -e use -i <bundle id>`. Re-run after every rebuild.
- FSKit has a third enablement gate beyond pluginkit: fskit_agent's
  `~/Library/Group Containers/group.com.apple.fskit.settings/enabledModules.plist`
  (what the System Settings "File System Extensions" toggle writes). CLI
  substitute: `plutil -insert` the bundle id + `pkill -9 -x fskit_agent`.
- Mount: `mount -F -t tlfshello 'tlfshello://x' /tmp/hellomnt` (`-F` = FSKit
  module, documented in mount(8) on macOS 26). `fsck_fskit`/`newfs_fskit`
  exist; there is no `mount_fskit`.
- `FSVolume.Operations` requires createItem/rename/remove/setAttributes/
  readSymbolicLink even for a read-only fs — stub with POSIX EROFS/EINVAL.
  `ReadWriteOperations` requires both read and write.
- Swift renames: `FSProbeResult.notRecognized`, `FSError.Code.*`,
  `FSVolume.OpenModes`, `closeItem(_:modes:replyHandler:)`.

## Next (the real binding)

Extract `tlfs-core` (overlay + daemon core) from the CLI, expose a C ABI
(`tlfs-ffi` staticlib), and implement the production appex as a thin Swift
translation of `FSVolume` operations onto it — same daemon, control socket,
and `tl fs` UX as the Linux fuser binding.
