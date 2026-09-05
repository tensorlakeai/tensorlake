# Tensorlake TypeScript SDK

The TypeScript SDK supports Tensorlake sandboxes on Node.js 22 or newer. Deploying and running durable TypeScript applications requires Node.js 24 or newer. Application handlers are async-only and values crossing a function boundary must be JSON values or a direct `File`. Application entrypoints can also receive an exact raw request body with `HttpBody`.

Sandbox handles retain connection configuration without loading TLS certificates.
Their first HTTP operation initializes the native transport on a background thread;
concurrent operations share that initialization and proxies reuse its connection pool.
Initialization failures reject the operation and can be retried by a later operation.
Creating a new client picks up changes to the system's trusted certificates.

On Linux, native binding selection reads the Node executable's ELF interpreter
without generating diagnostic reports. For a nonstandard or statically linked Node
executable, set `TENSORLAKE_NODE_LIBC=gnu` or `TENSORLAKE_NODE_LIBC=musl` before using
the SDK.

```ts
import { registerApplication, registerFunction } from "tensorlake/applications";

const square = registerFunction(
  "square",
  async (value: number) => value * value,
);

export const squares = registerApplication(
  "squares",
  async (values: number[]) => square.map(values),
);
```

This concise form infers the TypeScript call signature and uses permissive JSON
schemas. JavaScript default parameters are inferred as optional, and the
explicit name is stable across bundling. Use the schema-rich form when you need
runtime validation, API metadata, optional parameters without a JavaScript
default, rest parameters, `File` inputs and outputs, or an `HttpBody` input:

```ts
import { registerApplication, schema } from "tensorlake/applications";

export const validatedSquare = registerApplication(
  async (value: number) => value * value,
  {
    name: "validated_square",
    parameters: [schema.parameter("value", schema.number())] as const,
    returns: schema.number(),
  },
);
```

Run locally or deploy the same file:

```ts
import { runLocal } from "tensorlake/applications";

const request = await runLocal(squares, [1, 2, 3]);
console.log(await request.output());
```

Use `runRemote` for a deployed application. Plain client options remain the
last argument when every application argument is supplied. Wrap options with
`remoteOptions` when omitting JavaScript default arguments so an object input
can never be mistaken for client configuration:

```ts
import { registerApplication, remoteOptions, runRemote } from "tensorlake/applications";

const greeting = registerApplication(
  "greeting",
  async (name = "world") => `Hello, ${name}!`,
);

const request = await runRemote(
  greeting,
  remoteOptions({ apiKey: process.env.TENSORLAKE_API_KEY }),
);
console.log(await request.output());
```

```bash
npm install tensorlake
tl deploy app.ts
```

`tl` recognizes ESM `.ts`, `.mts`, `.js`, and `.mjs` entrypoints. The Rust CLI bundles the application for Node 24 with Rolldown, initializes the resulting ESM bundle once with Node to discover its registered functions, builds the function images, and uploads the deployment. CommonJS application modules and bundler configuration files are not supported.

The SDK build also produces a hashed executor capsule containing the Node 24 ESM executor, its protobufs, and an npm shrinkwrap derived from the checked-in SDK lockfile. During deployment, `tl` uses the capsule from the exact SDK package Rolldown resolved and adds it directly to the image-build context. This supports local dependencies such as `"tensorlake": "file:../tensorlake/typescript"`; build that SDK checkout with `npm run build:sdk` before deploying. The local Tensorlake package does not need to be published.

Functions expose `future`, `map`, `reduce`, and `tailCall`. `map` and `reduce`
accept either an iterable or a Promise/Future that produces one, and their
iterables may contain Promise/Future values. A reduce initial value is optional;
without one, the first item becomes the accumulator and an empty collection is
an error. Awaiting a registered function inside a running application creates a
durable remote function call. `RequestContext.get()` provides async request
state, metrics, progress, and an abort signal. Local requests expose `cancel()`,
which aborts that signal and rejects `output()`.

TypeScript and Python definitions cannot be mixed in one deployed application bundle. Class-based function semantics are not supported in the TypeScript runtime.

For a raw request body, declare a direct application parameter with
`schema.httpBody()`. `HttpBody.content` contains the unchanged bytes,
`contentType` preserves the request MIME type, and `text()` / `json()` provide
explicit decoding helpers. `RequestContext.get().headers` exposes sanitized,
case-insensitive invocation headers and preserves duplicate values through
`getAll()`. `HttpBody` is not supported in nested schemas, durable function
parameters, or return values. See the
[raw webhook example](examples/http-body/README.md).

Applications that accept public HTTP requests can set
`allow: ["unauthenticated_requests"]` in their registration options. The
capability is emitted in the deployment manifest, and deployments preserve the
application's stable public endpoint identifier across updates.

## Function executor correctness

Run the executor transport and protocol-state regression suites with:

```bash
npm run test:function-executor
```

These tests use bounded fake-server event sequences to cover terminal-result cardinality, retry exhaustion, deterministic concurrent fan-out and strict replay, function-call reduce chains, malformed user input, non-advancing event pages, tail calls, cross-bundle request errors, state reconciliation, and ranged BLOB transport. `npm run build:sdk` additionally builds the exact executor capsule used during deployment.

## Live server verification

The [server-verification example](examples/server-verification/README.md) uses the normal `tl deploy <PATH>` workflow to deploy eight reference applications and verify durable calls, retries and retry exhaustion, concurrent fan-out, server-orchestrated reduce, nested calls, request context, tail calls, files, and request errors against an already-running server.
