# Golden-path architecture

## Live sequence

```text
Browser                  Tensorlake CI              GitHub                 Tensorlake
  | POST /github/connect      |                        |                         |
  |-------------------------->| create CSRF state      |                         |
  |<--------------------------| installation URL       |                         |
  |------------------------------ install app -------->|                         |
  |<-------- callback with installation_id ------------|                         |
  | GET /repositories        | installation token      |                         |
  |-------------------------->|----------------------->|                         |
  |<--------------------------| repository metadata    |                         |
  | POST /migration/plan     | read workflow YAML      |                         |
  |-------------------------->|----------------------->|                         |
  |<--------------------------| exact unified diff     |                         |
  | POST /pull-request       | branch + commits + PR   |                         |
  |-------------------------->|----------------------->|                         |
  |                           |<-- workflow_job queued-|                         |
  |                           | verify HMAC signature  |                         |
  |                           | request JIT config --->|                         |
  |                           | create sandbox --------------------------------->|
  |                           | start run.sh --jitconfig ------------------------>|
  | GET /runs/:id (poll)     |<---------------- process output + sandbox state --|
  |<--------------------------| live steps, logs, SSH command                    |
  |                           |<-- workflow_job complete -------------------------|
  |                           | terminate sandbox ------------------------------->|
```

## Components

- `server.py` serves the application and the control-plane HTTP API.
- `backend/controller.py` enforces the golden-path state transitions and selects demo or live providers.
- `backend/github_app.py` signs GitHub App JWTs, obtains installation tokens, discovers workflows, produces diffs, creates branches/commits/PRs, and generates JIT runner configuration.
- `backend/runners.py` provisions either deterministic demo jobs or real GitHub JIT runners inside Tensorlake sandboxes.
- `backend/state.py` owns synchronized single-workspace state and correlates a UI-created waiting run with the subsequent GitHub `workflow_job` delivery.

## API contract

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/health` | Liveness and provider mode |
| `GET` | `/api/session` | Restore the current onboarding state |
| `POST` | `/api/github/connect` | Connect demo GitHub or obtain the live App installation URL |
| `GET` | `/api/github/callback` | Validate installation state and bind the installation |
| `POST` | `/api/github/webhook` | Verify and accept GitHub webhook deliveries |
| `GET` | `/api/repositories` | List repositories available to the installation |
| `POST` | `/api/migration/plan` | Detect workflow files and return an exact runner-label diff |
| `POST` | `/api/migration/pull-request` | Create migration branches, commits, smoke workflows, and PRs |
| `POST` | `/api/runs/smoke` | Create the waiting run shown in onboarding |
| `GET` | `/api/runs/:id` | Poll steps, logs, conclusion, sandbox, and SSH information |
| `POST` | `/api/runs/:id/retain` | Keep a sandbox for debugging after failure |

## Safety model

Core CI provisioning needs organization self-hosted-runner administration and `workflow_job` events. It never sends a repository token into the sandbox; the GitHub Actions runner receives a one-job JIT configuration generated for the installation. The sandbox is ephemeral and is terminated after the runner exits unless the user explicitly retains a failed run.

Migration is a separate, user-invoked capability. It needs workflow/content write permission only to create a branch and pull request. It does not merge the PR or bypass branch protections.

## Required production hardening

The local server intentionally keeps state in memory to remain dependency-free and reviewable. A hosted service must add:

- Durable installations, repositories, migration plans, runs, and webhook delivery IDs.
- Authenticated browser sessions tied to GitHub identity and organization membership.
- Encrypted secret storage or workload identity for GitHub App and Tensorlake credentials.
- A durable provisioning queue with retries, idempotency keys, dead-letter handling, and capacity controls.
- A cleanup reconciler that terminates orphaned sandboxes after process or service failure.
- Server-sent events or WebSockets in place of onboarding polling.
- Audit logs, rate limits, organization policies, regional placement, and billing metering.
- A release pipeline for validating and publishing runner images before changing the pinned runner version.
