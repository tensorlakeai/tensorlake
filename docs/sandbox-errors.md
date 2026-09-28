# Sandbox failure diagnostics

Sandbox failures carry a server reason and, when available, an actionable
diagnostic. `ConfigurationError` means the server identified an invalid sandbox
configuration, such as a conflicting filesystem mount or a root disk smaller
than the captured filesystem. Internal and unknown failures retain their own
reasons; clients do not infer responsibility from diagnostic text.

Blocking create and claim failures raise the existing `RemoteAPIError`. The
exception's text includes the sandbox ID, reason, and diagnostic. The exception
also exposes those fields for programmatic use:

```python
from tensorlake.sandbox import RemoteAPIError, SandboxClient

client = SandboxClient.for_cloud()
try:
    sandbox = client.create_and_connect(image="my-image")
except RemoteAPIError as error:
    print(str(error))
    if error.reason == "ConfigurationError":
        print(error.sandbox_id, error.error_details)
    raise
```

The same fields are available with `AsyncSandboxClient`.

```typescript
import { RemoteAPIError, SandboxClient } from "tensorlake";

const client = SandboxClient.forCloud();
try {
  await client.createAndConnect({ image: "my-image" });
} catch (error) {
  if (error instanceof RemoteAPIError) {
    console.error(error.message);
    if (error.reason === "ConfigurationError") {
      console.error(error.sandboxId, error.errorDetails);
    }
  }
  throw error;
}
```

The original response body remains available as Python `error.message` or
TypeScript `error.responseMessage`; the HTTP status remains `status_code` or
`statusCode`. The new fields are absent (`None` in Python) for unrelated API
errors. Reasons are strings so future server reasons remain readable. Older
servers that omit diagnostics still produce a useful reason or their original
error message. Diagnostic values may be strings or legacy JSON objects/arrays.

When a create operation returns a pending sandbox, the readiness helper includes
the eventual termination reason and diagnostic in its `SandboxError`. To inspect
a sandbox directly, use `client.get(...)`: the returned object has
`termination_reason` and `error_details` in Python, or `terminationReason` and
`errorDetails` in TypeScript. Copy operations can return partial failures; inspect
each entry's `status`, `reason`, and diagnostic in the returned `sandboxes` list.

`tl sbx create` and `tl sbx copy` print the reason and diagnostic on failure.
`tl sbx describe <id>` shows an **Error details** line for terminated sandboxes,
including archived sandboxes. Diagnoses remain available when the terminated
sandbox no longer has an SSH endpoint.

These clients display the information supplied by the server. Detailed
configuration diagnoses require the server/dataplane changes in
[compute-engine-internal #2040](https://github.com/tensorlakeai/compute-engine-internal/pull/2040)
to be deployed. Updating the client alone does not add details to historic
failures.
