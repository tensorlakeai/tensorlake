# Tensorlake CI

Serverless GitHub Actions runners with one repository-side change:

```diff
-    runs-on: ubuntu-latest
+    runs-on: tensorlake
```

Install the Tensorlake GitHub App once, use the label, and push. A signed `workflow_job` webhook provisions a fresh Tensorlake sandbox, registers a one-job GitHub JIT runner, and terminates the sandbox after the job.

There is no repository picker, workflow migration bot, agent installation, or Tensorlake secret to add to the repository.

## Try it locally

```bash
cd /workspace/tensorlake/ci-console
python3 server.py
```

Open `http://127.0.0.1:4173`. Demo mode shows the same label-first onboarding and can dispatch a simulated webhook-to-sandbox run without credentials.

Run the test suite with:

```bash
python3 -m unittest discover -s tests -v
```

## Runner labels

| Label | CPU | Memory | Image |
|---|---:|---:|---|
| `tensorlake` | 2 vCPU | 8 GB | Ubuntu 24.04 |
| `tensorlake-2vcpu-ubuntu-2404` | 2 vCPU | 8 GB | Ubuntu 24.04 |
| `tensorlake-4vcpu-ubuntu-2404` | 4 vCPU | 16 GB | Ubuntu 24.04 |
| `tensorlake-8vcpu-ubuntu-2404` | 8 vCPU | 32 GB | Ubuntu 24.04 |

Unknown labels are ignored instead of provisioning unbounded compute.

## Run in live mode

### 1. Publish the runner image

Authenticate the `tl` CLI, then build the included image:

```bash
tl sbx image create ./runner-image/Dockerfile \
  --registered-name tensorlake-ci-runner-ubuntu-2404
```

The image contains GitHub Actions Runner `2.337.0` under `/opt/actions-runner`.

### 2. Configure the GitHub App

Set the App's setup URL to:

```text
https://YOUR_CI_HOST/api/github/callback
```

Set its webhook URL to:

```text
https://YOUR_CI_HOST/api/github/webhook
```

Subscribe to the `workflow_job` event. Installation events are delivered automatically.

The App needs only:

- Repository metadata: read (required for every GitHub App)
- Actions: read (to receive `workflow_job` events)
- Organization self-hosted runners: write (to create one-job JIT configurations)

It does not need Contents, Workflows, or Pull Request write access. Tensorlake CI never edits a repository.

### 3. Start the webhook service

```bash
export TENSORLAKE_CI_MODE=live
export TENSORLAKE_API_KEY="tl_your_api_key"
export TENSORLAKE_CI_RUNNER_IMAGE="tensorlake-ci-runner-ubuntu-2404"
export GITHUB_APP_ID="123456"
export GITHUB_APP_SLUG="your-tensorlake-ci-app"
export GITHUB_APP_PRIVATE_KEY_PATH="/run/secrets/github-app.pem"
export GITHUB_WEBHOOK_SECRET="replace-with-a-random-secret"
export GITHUB_RUNNER_GROUP_ID="1"

python3 server.py --host 0.0.0.0 --port 4173 --mode live
```

The browser's install button points directly to the GitHub App installation page. The setup callback verifies the installation and immediately tells the user to add `runs-on: tensorlake`.

## HTTP surface

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/health` | Service health and mode |
| `GET` | `/api/config` | Install URL and supported labels |
| `GET` | `/api/github/callback` | Verify an App installation and return to setup |
| `POST` | `/api/github/webhook` | Accept signed `workflow_job` events |
| `GET` | `/api/runs` | Inspect recent local runs |
| `GET` | `/api/runs/:id` | Inspect a run and its sandbox logs |
| `POST` | `/api/demo/jobs` | Dispatch a simulated job in demo mode |

## Production boundary

The reference service keeps webhook delivery IDs and run state in memory. A hosted service still needs durable idempotency, a retryable provisioning queue, tenant-aware quotas, a cleanup reconciler, authentication for the run-inspection API, and production telemetry. See [Architecture](./docs/ARCHITECTURE.md).
