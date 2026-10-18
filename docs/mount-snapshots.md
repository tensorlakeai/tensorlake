# Snapshot a sandbox workspace for a build

Finish the checkout or write transaction, then retain a version of the running mount:

```sh
tl sbx fs snapshot SANDBOX --path /work -m 'build input' --json
```

Inside a compatible sandbox runtime:

```sh
tl fs status /work --json
tl fs doctor /work --json
tl fs snapshot /work -m 'build input' --json
```

Guest commands use the runtime's local socket and existing mount authority. They do
not need a platform credential or interactive login. The guest user must own the
mount or be root. Runtime discovery takes priority over the local CLI registry,
including root's registry. Only an explicit unmanaged-path response falls back to
an ordinary CLI mount; authorization and connection errors are reported directly.
The controller command uses normal sandbox authentication.

Use the returned `snapshot_id` as the explicit snapshot argument to your filesystem
fork. Do not reread head: other writers and background saves can change it. A retained
snapshot is billed like other permanent snapshots and remains until explicitly deleted.
The CLI prints its request UUID and mount ID to stderr before starting. After a lost
response or a retryable error, rerun with `--request-id UUID --mount-id ORIGINAL_MOUNT_ID`
and the same message to recover the same captured tree. Both replay flags are required;
a replacement mount or copied sandbox rejects the old identity. A fresh operation in a
copied sandbox has its own identity, even when the caller reuses a request UUID.
Messages must contain 1–1024 UTF-8 bytes without control characters.
A failed capture must be started again with a new UUID; it is never silently recaptured
under the old request. The CLI waits with progress for at most about five minutes.

Controllers can use `POST {sandbox_proxy_url}/api/v1/filesystems/control` directly:

```json
{"operation":"status","mount_path":"/work"}
```

Take `mount_id` from that response, then start:

```json
{"operation":"snapshot_start","mount_path":"/work","mount_id":"MOUNT_ID","request_id":"REQUEST_UUID","message":"build input"}
```

Poll with `operation: "snapshot_status"`, the same mount path, mount ID, and request
ID. Only `phase: "completed"` with a `snapshot_id` is a successful boundary.
`failed` is terminal; `retryable` resumes through `snapshot_start`.

Status includes observed pending writes, the last saved version and the last retained
snapshot. Unknown is represented as null, not as clean. Status describes an instant;
it is not a synchronization primitive. The application must finish a multi-file
transaction before requesting its snapshot. Kernel flush does not flush application
buffers that have never been written to the filesystem.

This requires the matching Artifact Storage and guest runtime release. Updating only
`tl` does not add support to an existing runtime. Completed request IDs remain
replayable for the lifetime of the local mount state. A helper restart resumes a
durably captured cut within the original five-minute deadline; it fails explicitly
if no cut was captured or the deadline expired.
