#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DOCKERFILE_PATH="${1:-$ROOT_DIR/examples/smoke/Dockerfile.smoke}"
IMAGE_NAME="${2:-codex-smoke-image-$(date +%s)}"

if [[ -z "${TENSORLAKE_API_KEY:-}" && -z "${TENSORLAKE_PAT:-}" ]]; then
  echo "Set TENSORLAKE_API_KEY or TENSORLAKE_PAT before running this script." >&2
  exit 2
fi

echo "Building sandbox image from ${DOCKERFILE_PATH}"
echo "Registered name: ${IMAGE_NAME}"

cd "$ROOT_DIR"

cargo run -p tensorlake-cli --bin tl -- \
  sbx image create "$DOCKERFILE_PATH" \
  --registered-name "$IMAGE_NAME" \
  --json

echo
echo "Describing registered image ${IMAGE_NAME}"

cargo run -p tensorlake-cli --bin tl -- \
  sbx image describe "$IMAGE_NAME"
