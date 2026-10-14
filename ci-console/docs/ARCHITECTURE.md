# Label-driven runner architecture

## User contract

The user installs the GitHub App once and changes one workflow field:

```yaml
runs-on: tensorlake
```

Tensorlake CI never scans or edits repositories. Optional labels select larger resource profiles.

## Live sequence

```text
User                 GitHub                 Tensorlake CI              Tensorlake
 | install App          |                         |                         |
 |--------------------->|--- installation ------>|                         |
 | change runs-on       |                         |                         |
 | push workflow ------>|                         |                         |
 |                      |-- workflow_job queued ->| verify HMAC             |
 |                      |                         | match tensorlake label  |
 |                      |<-- request JIT config --|                         |
 |                      |--- one-job config ----->|                         |
 |                      |                         |--- create sandbox ------>|
 |                      |                         |--- start runner -------->|
 |                      |<======= runner claims queued job =================|
 |<========================= normal Actions status and logs =================|
 |                      |                         |--- terminate sandbox --->|
```

The webhook payload supplies the GitHub App installation ID. The service exchanges its App JWT for an installation token, generates an organization-scoped JIT configuration, and passes that configuration directly to the runner process.

## GitHub permissions

- Metadata: read
- Actions: read
- Organization self-hosted runners: write

Contents, Workflows, and Pull Requests permissions are intentionally absent. The control plane never receives a repository token; GitHub's runner application obtains the one-job credentials it needs through the encoded JIT configuration.

## Label profiles

Runner profiles are an explicit allowlist in `backend/runners.py`. A label controls CPU and memory but cannot request arbitrary resources. The default `tensorlake` label maps to 2 vCPU and 8 GB on Ubuntu 24.04.

## Components

- `server.py` serves the install page and webhook endpoint.
- `backend/controller.py` verifies delivery intent, deduplicates webhook deliveries, and dispatches only supported labels.
- `backend/github_app.py` signs App JWTs, obtains installation tokens, verifies setup callbacks, and generates JIT configurations.
- `backend/runners.py` maps labels to resource profiles and owns sandbox lifecycle.
- `backend/state.py` holds the preview's in-memory webhook and run state.

## Production boundary

Before operating the service for multiple organizations, add:

- Durable webhook delivery IDs and run records.
- A retryable provisioning queue with idempotency, backoff, and dead-letter handling.
- Tenant-aware concurrency and spend limits.
- A cleanup reconciler for orphaned sandboxes and runners.
- Authentication and authorization on run/log inspection endpoints.
- Encrypted secrets or workload identity for GitHub App and Tensorlake credentials.
- Structured logs, traces, metrics, alerts, and audit events.
- A release pipeline for runner images and GitHub Actions Runner upgrades.
