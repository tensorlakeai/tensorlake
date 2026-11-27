# Tensorlake TypeScript SDK

The TypeScript SDK supports Tensorlake sandboxes on Node.js 22 or newer.

Sandbox, repository and filesystem handles retain configuration on the calling thread.
The first native operation starts a persistent Node worker that loads the addon and
owns the native clients. Concurrent calls use the same worker; sandbox proxies share
their lifecycle client's connection pool. Addon loading and TLS initialization keep
the application's event loop responsive, including on the first call. Initialization
failures reject the operation. A later operation can retry transport initialization;
creating a new client picks up changes to the system's trusted certificates.

Streaming consumers acknowledge each event before Rust reads the next one. Breaking
iteration, aborting its signal, or closing the sandbox connection cancels the native
HTTP stream. Closing one handle leaves sibling handles usable. Garbage collection
also releases native handles, and an idle worker does not keep Node running. If a
worker fails, outstanding calls reject without automatic replay; subsequent calls
can start a fresh worker. Byte arguments are copied across the worker boundary;
caller-owned buffers are never detached. File-path upload APIs avoid that copy.

Both ESM and CommonJS packages include the worker entrypoint. Projects using a
bundler should keep `@tensorlakeai/tensorlake` external so its worker and optional native packages
remain available at runtime.

On Linux, native binding selection reads the Node executable's ELF interpreter
without generating diagnostic reports. For a nonstandard or statically linked Node
executable, set `TENSORLAKE_NODE_LIBC=gnu` or `TENSORLAKE_NODE_LIBC=musl` before using
the SDK.

For agent commands, `maxOutputBytes` keeps only the last N bytes of output:

```ts
const result = await sandbox.run("rg", {
  args: ["pattern", "."],
  maxOutputBytes: 1024 * 1024,
  timeout: 30,
});
// result.stdout and result.stderr contain the retained tail.
// result.outputTruncated is true if older output was discarded.
```

The limit is a shared UTF-8 byte budget across stdout and stderr in arrival
order, including the newlines between output events. A character cut at the
start of the tail is omitted, so the result can be slightly smaller than the
budget. `0` retains only exit metadata; omitting the option preserves unlimited
capture. Truncation is reported separately from the command's exit status.

Either stream can consume the entire shared budget. Received order can differ
from when the command wrote each stream: buffered stdout can arrive after a
stderr diagnostic and evict it.

The native client evicts old output as it reads the stream, before constructing
the result passed to JavaScript. It continues reading until the command exits;
the cap does not stop the command, reduce network traffic, or limit the size of
an individual incoming event. Use `timeout` to bound command execution as well.
Run `npm run test:native-command-output-limit` after building the SDK and native
addon to verify a 148.4 MiB stream is retained as a 1 MiB tail.

## Expose ports when creating a sandbox

Set `exposedPorts` in the creation options to configure HTTP access through the
sandbox proxy in the initial request:

```ts
import { Sandbox } from "@tensorlakeai/tensorlake";

const sandbox = await Sandbox.create({
  exposedPorts: [3000, 8080],
  allowUnauthenticatedAccess: true,
});
```

Set `allowUnauthenticatedAccess: true` only when the ports should be publicly
accessible. Omitting it keeps authentication required for a fresh sandbox.
Ports must be integers from 1 to 65535, excluding management port 9501;
duplicates are removed. These options also work with `SandboxClient.create()`,
`createAndConnect()`, and `wait: false`, without a follow-up update request.
Snapshot restores inherit both settings when omitted. Pass `exposedPorts: []`
to clear inherited ports, or `allowUnauthenticatedAccess: false` to require
authentication.

## Native transport and application preloads

Sandbox, repository, usage, and filesystem HTTP operations use the Rust SDK
through an internal worker. Keep the platform-specific optional native package
installed. Importing Tensorlake does not replace the application's HTTP dispatcher.

TCP port forwarding also uses this worker and the shared Rust tunnel transport,
used by the CLI and Python's synchronous and async interfaces. Rust owns the
listener, WebSocket/TLS connection, backpressure, keepalive, and shutdown. Public
tunnel APIs and the server protocol are unchanged. The shared regression tests
are in `crates/cloud-sdk/src/sandboxes/tunnel/tests.rs`.

The worker starts with an empty `execArgv` and a copied environment with
`NODE_OPTIONS` cleared, so application `--require`, `--import`, and loader hooks
do not run inside it. Other environment variables are copied when it starts.
The application's own startup flags and environment remain intact. Native HTTPS
uses the Rust SDK's bundled Mozilla certificate roots.

After building the SDK and native addon, run `npm run test:native-cloud` to check
sandbox and image-build requests against a local HTTP server.
Run `npm run test:native-tunnel` to check TCP forwarding through the worker and
native addon against a local WebSocket server.
