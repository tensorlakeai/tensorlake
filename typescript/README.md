# Tensorlake TypeScript SDK

The TypeScript SDK supports Tensorlake sandboxes and durable applications on Node.js 24 or newer. Application handlers are async-only and values crossing a function boundary must be JSON values or a direct `File`.

```ts
import { registerApplication, registerFunction, schema } from "tensorlake/applications";

const square = registerFunction(
  async (value: number) => value * value,
  {
    parameters: [schema.parameter("value", schema.number())] as const,
    returns: schema.number(),
  },
);

export const squares = registerApplication(
  async (values: number[]) => square.map(values),
  {
    parameters: [schema.parameter("values", schema.array(schema.number()))] as const,
    returns: schema.array(schema.number()),
  },
);
```

Run locally or deploy the same file:

```ts
import { runLocal } from "tensorlake/applications";

const request = await runLocal(squares, [1, 2, 3]);
console.log(await request.output());
```

```bash
npm install tensorlake
tl deploy app.ts
```

`tl` recognizes ESM `.ts`, `.mts`, `.js`, and `.mjs` entrypoints. The Rust CLI bundles the application for Node 24 with Rolldown, initializes the resulting ESM bundle once with Node to discover its registered functions, builds the function images, and uploads the deployment. CommonJS application modules and bundler configuration files are not supported.

The SDK build also produces a hashed executor capsule containing the Node 24 ESM executor, its protobufs, and an npm shrinkwrap derived from the checked-in SDK lockfile. During deployment, `tl` uses the capsule from the exact SDK package Rolldown resolved and adds it directly to the image-build context. This supports local dependencies such as `"tensorlake": "file:../tensorlake/typescript"`; build that SDK checkout with `npm run build:sdk` before deploying. The local Tensorlake package does not need to be published.

Functions expose `future`, `map`, `reduce`, and `tailCall`. Awaiting a registered function inside a running application creates a durable remote function call. `RequestContext.get()` provides async request state, metrics, progress, headers, and an abort signal.

TypeScript and Python definitions cannot be mixed in one deployed application bundle. Class-based function semantics are not supported in the TypeScript runtime.

## Function executor correctness

Run the executor transport and protocol-state regression suites with:

```bash
npm run test:function-executor
```

These tests use bounded fake-server event sequences to cover terminal-result cardinality, retry exhaustion, reordered and duplicate events, strict replay divergence, non-advancing event pages, tail calls, cross-bundle request errors, state reconciliation, and BLOB transport. `npm run build:sdk` additionally builds the exact executor capsule used during deployment.

## Live server verification

The [server-verification example](examples/server-verification/README.md) uses the normal `tl deploy <PATH>` workflow to deploy eight reference applications and verify durable calls, retries and retry exhaustion, concurrent fan-out, sequential reduce, nested calls, request context, tail calls, files, and request errors against an already-running server.
