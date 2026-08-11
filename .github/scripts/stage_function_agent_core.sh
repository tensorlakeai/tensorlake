#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <compute-engine-repo-dir> <tensorlake-repo-dir> <compute-engine-sha>" >&2
  exit 2
fi

compute_engine_root="$(cd "$1" && pwd)"
tensorlake_root="$(cd "$2" && pwd)"
source_sha="$3"
upstream_crate="${compute_engine_root}/function-service/agent-core"
destination_crate="${tensorlake_root}/crates/function-agent-core"
python_binding_source="${compute_engine_root}/function-service/bindings/python/src/function_agent.rs"
node_binding_source="${compute_engine_root}/function-service/bindings/node/src/function_agent.rs"
python_binding_destination="${tensorlake_root}/crates/rust-cloud-sdk-py/src/function_agent.rs"
node_binding_destination="${tensorlake_root}/crates/rust-cloud-sdk-node/src/function_agent.rs"
stage_marker="${tensorlake_root}/.function-agent-core-staged"

if [[ ! "${source_sha}" =~ ^[0-9a-f]{40}$ ]]; then
  echo "error: compute-engine SHA must contain exactly 40 lowercase hex characters" >&2
  exit 1
fi

if [[ ! -f "${upstream_crate}/Cargo.toml" || ! -f "${upstream_crate}/src/lib.rs" ]]; then
  echo "error: ${upstream_crate} is not a Function Agent core crate" >&2
  exit 1
fi

if [[ ! -f "${python_binding_source}" || ! -f "${node_binding_source}" ]]; then
  echo "error: canonical Function Agent language bindings are missing from CEI" >&2
  exit 1
fi

if [[ ! -f "${destination_crate}/Cargo.toml" || ! -d "${destination_crate}" ]]; then
  echo "error: ${destination_crate} is not the Tensorlake Function Agent placeholder" >&2
  exit 1
fi

if ! tr -d '\r' <"${upstream_crate}/Cargo.toml" \
  | grep -Eq '^name = "tensorlake-function-agent-core"$'; then
  echo "error: upstream Cargo.toml has the wrong package name" >&2
  exit 1
fi

# Tensorlake intentionally selects ring by default for portable SDK binaries; CEI selects
# aws-lc for the service workspace. Every other manifest line must remain identical so a new
# dependency or feature cannot be silently omitted from the public workspace placeholder.
normalized_upstream_manifest="$(mktemp "${TMPDIR:-/tmp}/function-agent-core-upstream.XXXXXX")"
normalized_destination_manifest="$(mktemp "${TMPDIR:-/tmp}/function-agent-core-destination.XXXXXX")"
trap 'rm -f "${normalized_upstream_manifest}" "${normalized_destination_manifest}"' EXIT
tr -d '\r' <"${upstream_crate}/Cargo.toml" \
  | sed 's/^default = \["tls-aws-lc"\]$/default = ["tls-ring"]/' \
    >"${normalized_upstream_manifest}"
tr -d '\r' <"${destination_crate}/Cargo.toml" >"${normalized_destination_manifest}"
if ! diff -u "${normalized_destination_manifest}" "${normalized_upstream_manifest}"; then
  echo "error: Tensorlake's Function Agent placeholder manifest has drifted from CEI" >&2
  exit 1
fi

# Preserve only Tensorlake's workspace-adapted manifest and immutable source pin. Copy every
# other top-level CEI crate input, including build.rs, examples, tests, benches, and assets. This
# avoids silently producing a different crate when CEI adds compile-time files outside src/.
find "${destination_crate}" -mindepth 1 -maxdepth 1 \
  ! -name Cargo.toml ! -name CEI_REVISION -exec rm -rf {} +
while IFS= read -r -d '' entry; do
  name="$(basename "${entry}")"
  case "${name}" in
    Cargo.toml | target | .git)
      continue
      ;;
  esac
  cp -R "${entry}" "${destination_crate}/"
done < <(find "${upstream_crate}" -mindepth 1 -maxdepth 1 -print0)

while IFS= read -r -d '' entry; do
  name="$(basename "${entry}")"
  case "${name}" in
    Cargo.toml | target | .git)
      continue
      ;;
  esac
  if ! diff -qr "${entry}" "${destination_crate}/${name}"; then
    echo "error: staged Function Agent crate input ${name} differs from CEI" >&2
    exit 1
  fi
done < <(find "${upstream_crate}" -mindepth 1 -maxdepth 1 -print0)

cp "${python_binding_source}" "${python_binding_destination}"
cp "${node_binding_source}" "${node_binding_destination}"
diff -q "${python_binding_source}" "${python_binding_destination}" >/dev/null
diff -q "${node_binding_source}" "${node_binding_destination}" >/dev/null

printf '%s\n' "${source_sha}" >"${stage_marker}"
echo "Staged Function Agent core and language bindings from compute-engine-internal@${source_sha}"
