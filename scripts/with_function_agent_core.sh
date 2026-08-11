#!/usr/bin/env bash

set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: $0 <command> [args...]" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tensorlake_root="$(cd "${script_dir}/.." && pwd)"
destination_crate="${tensorlake_root}/crates/function-agent-core"
python_binding="${tensorlake_root}/crates/rust-cloud-sdk-py/src/function_agent.rs"
node_binding="${tensorlake_root}/crates/rust-cloud-sdk-node/src/function_agent.rs"
stage_marker="${tensorlake_root}/.function-agent-core-staged"
core_placeholder="Function Agent core source has not been staged"
python_placeholder="Function Agent Python binding source has not been staged"
node_placeholder="Function Agent Node binding source has not been staged"

# The CI action exports the exact SHA after staging. Local nested build commands inherit the same
# variable from this wrapper. Absence of this explicit provenance never implies that arbitrary
# non-placeholder files are safe to compile.
staged_sha="${FUNCTION_AGENT_CORE_STAGED_SHA:-}"
if [[ -n "${staged_sha}" ]]; then
  marker_sha=""
  if [[ -f "${stage_marker}" ]]; then
    marker_sha="$(tr -d '[:space:]' <"${stage_marker}")"
  fi
  if [[ ! "${staged_sha}" =~ ^[0-9a-f]{40}$ || "${marker_sha}" != "${staged_sha}" ]]; then
    echo "error: Function Agent staging marker is missing or does not match ${staged_sha}" >&2
    exit 1
  fi
  exec "$@"
fi

if [[ -e "${stage_marker}" ]] \
  || ! grep -Fq "${core_placeholder}" "${destination_crate}/src/lib.rs" \
  || ! grep -Fq "${python_placeholder}" "${python_binding}" \
  || ! grep -Fq "${node_placeholder}" "${node_binding}"; then
  echo "error: Tensorlake contains unverified or stale Function Agent source" >&2
  echo "Restore the Function Agent placeholders before running a staged build." >&2
  exit 1
fi

compute_engine_root="${COMPUTE_ENGINE_INTERNAL_DIR:-${tensorlake_root}/../compute-engine-internal}"
if [[ ! -f "${compute_engine_root}/function-service/agent-core/src/lib.rs" ]]; then
  echo "error: Function Agent core not found under ${compute_engine_root}" >&2
  echo "Set COMPUTE_ENGINE_INTERNAL_DIR to a CEI checkout." >&2
  exit 1
fi
source_sha="$(git -C "${compute_engine_root}" rev-parse HEAD)"
if [[ ! "${source_sha}" =~ ^[0-9a-f]{40}$ ]]; then
  echo "error: CEI checkout did not resolve to a full commit SHA" >&2
  exit 1
fi

backup_dir="$(mktemp -d "${TMPDIR:-/tmp}/tensorlake-function-agent-core.XXXXXX")"
cp -R "${destination_crate}" "${backup_dir}/core"
cp "${python_binding}" "${backup_dir}/python-function-agent.rs"
cp "${node_binding}" "${backup_dir}/node-function-agent.rs"

restore_placeholders() {
  rm -rf "${destination_crate}"
  cp -R "${backup_dir}/core" "${destination_crate}"
  cp "${backup_dir}/python-function-agent.rs" "${python_binding}"
  cp "${backup_dir}/node-function-agent.rs" "${node_binding}"
  rm -f "${stage_marker}"
  rm -rf "${backup_dir}"
}
trap restore_placeholders EXIT

"${tensorlake_root}/.github/scripts/stage_function_agent_core.sh" \
  "${compute_engine_root}" "${tensorlake_root}" "${source_sha}"

export FUNCTION_AGENT_CORE_STAGED_SHA="${source_sha}"
"$@"
