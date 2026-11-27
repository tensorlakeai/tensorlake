# Recovering a failed sandbox resume

A server that supports recoverable resume failures can report `resume_failed`.
The sandbox still exists and retains its original `snapshot_id`. Its optional
`error_details` contains the server's diagnosis, which may be a string or other
JSON. `get` and `list` preserve both fields. This state is neither termination
nor a polling timeout.

Readiness and resume waits stop promptly with `SandboxResumeError` (a
`SandboxError` subclass). Python exposes `sandbox_id`, `status`, `snapshot_id`,
and `error_details`; TypeScript exposes `sandboxId`, `status`, `snapshotId`, and
`errorDetails`. Diagnosis data is preserved unchanged, including unknown fields.
A failed wait does not delete the sandbox, even with `cancel_on_timeout=True` /
`cancelOnTimeout: true`. Those options still apply to a pending timeout.

Retry only when the caller chooses to, through the existing resume API:

```python
from tensorlake.sandbox import SandboxClient, SandboxResumeError, SandboxStatus

client = SandboxClient.for_cloud()
sandbox_id = "your_sandbox_id"
info = client.get(sandbox_id)
print(info.status, info.error_details, info.snapshot_id)
if info.status == SandboxStatus.RESUME_FAILED:
    try:
        client.resume(sandbox_id, timeout=120)
    except SandboxResumeError as error:
        print(error.sandbox_id, error.error_details, error.snapshot_id)
print(client.get(sandbox_id).status)
```

`AsyncSandboxClient` uses the same imports, fields and semantics with `await`.
Connected `Sandbox.resume()` and `AsyncSandbox.resume()` also surface the error.

```typescript
import {
  SandboxClient, SandboxResumeError, SandboxStatus,
} from "@tensorlakeai/tensorlake";

const client = SandboxClient.forCloud();
const sandboxId = "your_sandbox_id";
const info = await client.get(sandboxId);
console.log(info.status, info.errorDetails, info.snapshotId);
if (info.status === SandboxStatus.RESUME_FAILED) {
  try {
    await client.resume(sandboxId, { timeout: 120 });
  } catch (error) {
    if (!(error instanceof SandboxResumeError)) throw error;
    console.log(error.sandboxId, error.errorDetails, error.snapshotId);
  }
}
console.log((await client.get(sandboxId)).status);
```

Resume waits by default in Python/TypeScript. `wait=False` / `{ wait: false }`
submits a single request and returns without waiting for its outcome. A successful
retry reaches `running` and clears the old diagnosis. A new failure requires
another explicit caller decision; `connect`, `get_or_create` / `getOrCreate`,
readiness polling and proxy traffic do not silently retry a failed resume.

In Rust, `SandboxesClient::resume_and_wait` accepts empty HTTP 200 (returns
`true`) and 202 (`false`, still in progress) responses. A 409 followed by a GET
that confirms `resume_failed` produces `SdkError::SandboxResume`, carrying
`SandboxResumeError` with `sandbox_id`, `snapshot_id`, `error_details` and
`status() == "resume_failed"`. Unrecognized conflicts retain their original
HTTP error. `wait_until_settled` keeps its existing observation API: it returns
the failed `SandboxInfo` immediately, without deleting or retrying it.

The CLI also stops waiting on a failed resume and prints its diagnosis and
checkpoint. Inspect the state, explicitly retry once, then wait separately:

```sh
tl sbx describe your_sandbox_id
tl sbx resume your_sandbox_id --no-wait
tl sbx wait your_sandbox_id --timeout 120
```

`tl sbx resume ID` waits by default. Failed resumes remain visible in the default
sandbox listing. The suspended-only filter remains specific to `suspended`.

An uncertain resident resume remains `pending` (internally `resume_uncertain`),
because execution may still be continuing. A diagnostic mentioning a resume
failure does not make it safe to retry; only `resume_failed` has the recoverable
contract. Fresh creation failures retain their existing terminal behavior.
Preserved checkpoints remain available to the existing copy/export APIs.

This depends on [compute-engine-internal #3069](https://github.com/tensorlakeai/compute-engine-internal/pull/3069).
Backend deployment is separate from SDK availability. Older servers, including
empty lifecycle success bodies and diagnostic-only conflicts, remain supported.
Public documentation: [docs #434](https://github.com/tensorlakeai/docs/pull/434).
