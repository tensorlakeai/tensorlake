# Tensorlake CI

An end-to-end golden-path implementation for running GitHub Actions jobs on ephemeral Tensorlake sandboxes.

The product guides a user through:

1. Installing the Tensorlake GitHub App.
2. Selecting repositories and discovering workflow files.
3. Reviewing the exact `runs-on` migration diff.
4. Opening migration pull requests.
5. Watching GitHub queue a smoke job and Tensorlake provision a JIT runner.
6. Streaming runner logs, inspecting the sandbox, and copying its SSH command.

The frontend and backend have no third-party runtime dependencies. Demo and live mode use the same HTTP contract.

## Run the complete demo

```bash
cd /workspace/tensorlake/ci-console
python3 server.py
```

Open [http://127.0.0.1:4173](http://127.0.0.1:4173) and complete all four steps. The server returns deterministic repositories, creates a demo migration PR, and emits the same runner states and logs that the live provider uses.

Do not use `python3 -m http.server`; the onboarding UI now depends on the control-plane API served by `server.py`.

## Tests

```bash
python3 -m unittest discover -s tests -v
```

The suite covers workflow rewriting, safe handling of unsupported runner expressions, onboarding-order enforcement, waiting-job correlation, the runner lifecycle, and the complete HTTP flow.

## Live mode

Live mode requires a GitHub App, a Tensorlake API key, and a registered Tensorlake runner image.

### 1. Build the runner image

Authenticate the `tl` CLI, then build and register the included image:

```bash
tl sbx image create ./runner-image/Dockerfile \
  --registered-name tensorlake-ci-runner-ubuntu-2404
```

The image contains GitHub Actions Runner `2.337.0` under `/opt/actions-runner`. Update `RUNNER_VERSION` deliberately as new runner releases are validated.

### 2. Configure a GitHub App

Set its setup callback URL to:

```text
https://YOUR_CI_HOST/api/github/callback
```

Set its webhook URL to:

```text
https://YOUR_CI_HOST/api/github/webhook
```

Subscribe to `workflow_job`, `installation`, and `installation_repositories` events. The golden path currently needs these permissions:

- Repository metadata: read
- Actions: read and write
- Contents: read and write
- Workflows: read and write
- Pull requests: read and write
- Organization self-hosted runners: read and write

The UI shows the workflow diff before any write. Migration changes are delivered through a pull request and remain subject to the repository's normal reviews and branch protections.

### 3. Set environment variables

Copy `.env.example` into your secret manager or runtime environment. Do not commit the GitHub private key, webhook secret, or Tensorlake API key.

```bash
export TENSORLAKE_CI_MODE=live
export TENSORLAKE_API_KEY=tl_your_api_key
export TENSORLAKE_CI_RUNNER_IMAGE=tensorlake-ci-runner-ubuntu-2404
export GITHUB_APP_ID=123456
export GITHUB_APP_SLUG=your-tensorlake-ci-app
export GITHUB_APP_PRIVATE_KEY_PATH=/absolute/path/to/github-app.private-key.pem
export GITHUB_APP_CALLBACK_URL=https://YOUR_CI_HOST/api/github/callback
export GITHUB_WEBHOOK_SECRET=replace-with-a-random-secret
export GITHUB_RUNNER_GROUP_ID=1

python3 server.py --host 0.0.0.0 --port 4173 --mode live
```

In live mode, GitHub App JWTs are signed locally with OpenSSL. Installation tokens are short-lived and cached only in memory. Incoming webhook bodies are verified with `X-Hub-Signature-256` before they can provision compute.

## Production boundary

This directory implements and validates the complete single-workspace golden path. Before exposing it as a multi-tenant service, replace the in-memory state with durable storage, add user/session authentication and workspace authorization, move provisioning work to a durable queue, and add webhook delivery persistence. See [Architecture](./docs/ARCHITECTURE.md) for the exact boundary.
