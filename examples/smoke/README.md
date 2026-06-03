# Smoke Tests

These smoke scripts target the highest-signal ingress-endpoint paths from this PR:

- Python cloud SDK `create_and_connect()` plus reconnect-by-name
- TypeScript cloud SDK `createAndConnect()`
- CLI sandbox-image build path

## Prereqs

Set cloud credentials first:

```bash
export TENSORLAKE_API_KEY=...
export TENSORLAKE_ORGANIZATION_ID=...
export TENSORLAKE_PROJECT_ID=...
```

`TENSORLAKE_ORGANIZATION_ID` and `TENSORLAKE_PROJECT_ID` are optional if your account context does not require them.

## Python named reconnect

Build the local Rust extension, then run the Python SDK from source:

```bash
cd /Users/sumit2/workspace/tensorlake
make build_cloud_sdk
PYTHONPATH=src python3 examples/smoke/python_named_reconnect.py
```

Optional flags:

```bash
PYTHONPATH=src python3 examples/smoke/python_named_reconnect.py \
  --image tensorlake/ubuntu-minimal \
  --name my-smoke-sandbox \
  --keep
```

## TypeScript createAndConnect

Build the local TypeScript SDK bundle, then run the Node smoke script:

```bash
cd /Users/sumit2/workspace/tensorlake/typescript
npm ci
npm run build:sdk
cd /Users/sumit2/workspace/tensorlake
node examples/smoke/ts_create_and_connect.mjs
```

You can pass a different image as the first positional argument:

```bash
node examples/smoke/ts_create_and_connect.mjs tensorlake/ubuntu-minimal
```

## CLI image build

This uses the source CLI directly via `cargo run` and builds a throwaway image name by default:

```bash
cd /Users/sumit2/workspace/tensorlake
bash examples/smoke/cli_image_build.sh
```

Or specify the Dockerfile and registered image name explicitly:

```bash
bash examples/smoke/cli_image_build.sh \
  /Users/sumit2/workspace/tensorlake/examples/smoke/Dockerfile.smoke \
  codex-smoke-image-manual
```

Note: the CLI currently exposes `create`, `ls`, and `describe` for sandbox images, but not delete. Use throwaway image names for smoke runs.
