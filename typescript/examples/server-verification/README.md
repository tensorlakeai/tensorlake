# TypeScript server verification

This fixture uses the public `tl deploy <PATH>` workflow to deploy a TypeScript application to an already-running Tensorlake server and verify the function executor end to end.

It covers:

- Node 24 executor initialization and health checks
- JSON application input and output
- durable child calls, fan-out, delayed futures, and retries
- nested durable calls and concurrent mixed success/failure fan-out
- terminal failure delivery after retry exhaustion
- sequential reduce calls, empty reduce input, and reducer failure
- request headers, request state, metrics, and progress
- tail calls
- direct `File` input and output
- `RequestError` propagation

The JSON applications use the concise `registerFunction(name, handler)` and
`registerApplication(name, handler)` forms. The file application keeps explicit
schemas because raw `File` input and output require content-aware decoding.

## Prerequisites

- Node.js 24 or newer
- the standalone `tl` CLI
- a running server reachable through `TENSORLAKE_API_URL`
- credentials and project scope accepted by that server
- working sandbox-image builds

Set the same environment used by `tl`:

```bash
export TENSORLAKE_API_URL="http://localhost:8900"
export TENSORLAKE_API_KEY="..."       # or TENSORLAKE_PAT
export INDEXIFY_NAMESPACE="default"

# Required for PAT/project-scoped installations:
export TENSORLAKE_ORGANIZATION_ID="..."
export TENSORLAKE_PROJECT_ID="..."
```

This repository fixture deliberately uses `"tensorlake": "file:../.."`, so it tests the SDK and executor from this checkout without publishing either one. Build the local SDK, install the example dependency, and deploy from the repository root:

```bash
npm --prefix typescript install
npm --prefix typescript run build:sdk
npm --prefix typescript/examples/server-verification install
tl deploy typescript/examples/server-verification/application.ts
node typescript/examples/server-verification/verify.mjs
```

To build and exercise the CLI from this repository instead, run these commands from the repository root:

```bash
cargo build -p tensorlake-cli --bin tl
npm --prefix typescript install
npm --prefix typescript run build:sdk
npm --prefix typescript/examples/server-verification install
./target/debug/tl deploy typescript/examples/server-verification/application.ts
node typescript/examples/server-verification/verify.mjs
```

The SDK build creates a hashed executor capsule next to the local package. Rolldown records the exact `tensorlake/applications` package it resolves, and `tl deploy` validates that package's capsule, adds it directly to the image-build context, and installs it from a local tarball. The Tensorlake package itself never needs to be present in an npm registry; npm is only used by the image builder to install the capsule's locked third-party dependencies.

`tl` bundles `application.ts` as ESM with its embedded Rust Rolldown implementation, initializes the bundle once with Node 24 to discover the registered functions, builds the function image, and uploads the application manifests and code ZIP to the configured server.

When deploying from the example directory, execute all checks with:

```bash
node verify.mjs
```

Set `TENSORLAKE_VERIFY_CLEANUP=1` to delete the eight applications after verification. Set `TENSORLAKE_VERIFY_TIMEOUT_SEC` to increase the default 180-second request timeout. A timeout includes the last request metadata response so the full function-run and allocation graph is available in CI output.

## Manual invocation

The applications can also be exercised with `curl`. These commands use `jq` to extract request IDs:

```bash
API_URL="${TENSORLAKE_API_URL%/}"
NAMESPACE="${INDEXIFY_NAMESPACE:-default}"
APPLICATIONS_URL="$API_URL/v1/namespaces/$NAMESPACE/applications"
AUTHORIZATION="Authorization: Bearer ${TENSORLAKE_API_KEY:-$TENSORLAKE_PAT}"
```

Invoke the main runtime check:

```bash
REQUEST_ID=$(curl -fsS \
  "$APPLICATIONS_URL/typescript_runtime_verification" \
  -H "$AUTHORIZATION" \
  -H "X-Tensorlake-Verification: header-propagated" \
  --json '{"label":"server-check","values":[1,2,5]}' | jq -r .request_id)

curl -fsS \
  "$APPLICATIONS_URL/typescript_runtime_verification/requests/$REQUEST_ID" \
  -H "$AUTHORIZATION" | jq

curl -fsS \
  "$APPLICATIONS_URL/typescript_runtime_verification/requests/$REQUEST_ID/output" \
  -H "$AUTHORIZATION" | jq
```

The output should contain `[2,4,10]`, `header-propagated`, and the same request ID. If the metadata still has `"outcome": null`, repeat the metadata request until it becomes `"success"`.

Verify tail calls:

```bash
REQUEST_ID=$(curl -fsS \
  "$APPLICATIONS_URL/typescript_tail_call_verification" \
  -H "$AUTHORIZATION" \
  --json '41' | jq -r .request_id)

curl -fsS \
  "$APPLICATIONS_URL/typescript_tail_call_verification/requests/$REQUEST_ID/output" \
  -H "$AUTHORIZATION"
# Expected after completion: 42
```

Verify direct `File` transport:

```bash
printf 'typescript file boundary' > /tmp/tensorlake-typescript-input.txt
REQUEST_ID=$(curl -fsS \
  "$APPLICATIONS_URL/typescript_file_verification" \
  -H "$AUTHORIZATION" \
  -H 'Content-Type: text/plain' \
  --data-binary @/tmp/tensorlake-typescript-input.txt | jq -r .request_id)

curl -fsS \
  "$APPLICATIONS_URL/typescript_file_verification/requests/$REQUEST_ID/output" \
  -H "$AUTHORIZATION"
# Expected after completion: TYPESCRIPT FILE BOUNDARY
```

Verify `RequestError` propagation:

```bash
REQUEST_ID=$(curl -fsS \
  "$APPLICATIONS_URL/typescript_request_error_verification" \
  -H "$AUTHORIZATION" \
  --json '"intentional TypeScript request error"' | jq -r .request_id)

curl -fsS \
  "$APPLICATIONS_URL/typescript_request_error_verification/requests/$REQUEST_ID" \
  -H "$AUTHORIZATION" | jq
# Expected: failure_reason=request_error and the supplied message in request_error.message
```

For PAT-based installations, add the project scope headers to each `curl` request:

```bash
-H "X-Forwarded-Organization-Id: $TENSORLAKE_ORGANIZATION_ID" \
-H "X-Forwarded-Project-Id: $TENSORLAKE_PROJECT_ID"
```

## What to inspect on the server

The executor's `GetInfo` response reports `sdk_language=typescript` and the Node version in `sdk_language_version`. Server logs should also show the executor initialization events and the intentional failures for `typescript_verification_retry_once`, `typescript_verification_mixed_failure`, `typescript_verification_always_fails`, and the negative-input reduce call. The verification script checks retry allocation counts, nested function-run presence, expected request failure and cancellation after a mapped child fails, retry exhaustion, and sequential reduce behavior. Child function errors are request-fatal on the server; the failure checks therefore assert terminal request metadata rather than expecting the parent application to recover and return an output.

If deployment succeeds but initialization fails, inspect the function-container logs first. Common causes are an older function-executor protobuf or an SDK release whose function image does not contain the Node 24 executor.
