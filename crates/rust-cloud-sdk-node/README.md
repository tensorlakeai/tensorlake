# @tensorlake/native

napi-rs bindings for the Tensorlake Rust cloud SDK, consumed by the TypeScript
SDK (`typescript/`). Mirrors the surface that `crates/rust-cloud-sdk-py`
exposes to Python, so both language SDKs delegate to the same Rust core.

## What's here

Today this exposes `buildSandboxImage` — the sandbox-image build pipeline that
parses a Dockerfile, provisions a builder sandbox, drives `tl-rootfs-build`,
and registers the resulting snapshot as a sandbox template. Additional
functions will be added here as more TS code paths are consolidated.

## Local development

```bash
# From this directory:
npm install
npm run build          # release build → tensorlake-node.<triple>.node
npm run build:debug    # debug build (faster compile, slower runtime)
```

The build artifact lands next to `index.js`. The loader in `index.js` prefers
the local file when present; otherwise it falls back to
`@tensorlake/native-<triple>` from npm.

## Publication

CI is expected to run `napi prepublish -t npm` to materialize the
per-platform subpackages under `npm/` and publish them alongside this
package. See `package.json#napi.triples` for the target list.
