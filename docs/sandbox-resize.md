# Resize a running sandbox

Running Cloud Hypervisor sandboxes support live CPU, memory, and root-disk
updates. Resource arguments use the same names, numeric types, and units as
sandbox creation. Omitted dimensions keep their current allocation. Resource
updates must be standalone: send name, proxy, and network changes separately.

## CLI

`tl sbx update` accepts create's `-c` / `--cpus`, `-m` / `--memory`, and
`--disk_mb` flags. Memory and disk values are MiB, as on create.

```sh
tl sbx update my-sandbox -c 2 -m 4096 --disk_mb 20480
tl sbx update my-sandbox --memory 6144 --timeout 120
```

The command prints the admitted generation and status, then waits for that
generation to succeed or fail. The default wait budget is 300 seconds. A
successful result reports the confirmed allocation; admission alone is not
completion. A failure or timeout exits nonzero and includes the generation,
server diagnostic when available, and last confirmed allocation.

```sh
# Return when admitted, without waiting for completion.
tl sbx update my-sandbox --memory 8192 --no-wait

# Inspect the latest generation, requested resources, status, and error.
tl sbx describe my-sandbox
```

Timeout does not cancel a resize. Inspect it or use an SDK generation wait to
continue observing the same operation; do not submit another resize just to wait.
An update whose targets all equal the confirmed allocation makes no PATCH and
reports no resource changes. SDKs return the fresh sandbox observation in this
case; its resize metadata may describe an earlier operation, since no new
generation was admitted.

## Python, synchronous and asynchronous

The same arguments work on `Sandbox.update()`, `AsyncSandbox.update()`,
`SandboxClient.update_sandbox()`, and `AsyncSandboxClient.update_sandbox()`:
`cpus: float | None`, `memory_mb: int | None`, and `disk_mb: int | None`.

```python
from tensorlake.sandbox import Sandbox, SandboxResizeError

sandbox = Sandbox.connect("my-sandbox")
info = sandbox.update(cpus=2.0, memory_mb=4096, disk_mb=20480)
print(info.resource_resize.status, info.resources)

admitted = sandbox.update(memory_mb=6144, wait=False)
generation = admitted.resource_resize.generation
try:
    completed = sandbox.wait_for_resource_resize(generation, timeout=120)
    print(completed.resources)
except SandboxResizeError as error:
    print(error.generation, error.reason, error.info.resources if error.info else None)
    # A timeout leaves the resize running. Wait for error.generation again.
```

Use `await sandbox.update(...)` and
`await sandbox.wait_for_resource_resize(...)` on `AsyncSandbox`. Client-level
waits take the sandbox ID/name followed by the generation. `timeout` defaults to
300 seconds and `poll_interval` to 1 second; both apply to generation polling.
`wait=False` returns the admission response. Existing network/proxy updates keep
their behavior.

## TypeScript

Both `Sandbox.update()` and `SandboxClient.update()` accept create's `cpus`,
`memoryMb`, and `diskMb` option names.

```ts
import { Sandbox } from "tensorlake";

const sandbox = await Sandbox.connect({ sandboxId: "my-sandbox" });
const completed = await sandbox.update({ cpus: 2, memoryMb: 4096, diskMb: 20480 });
console.log(completed.resourceResize?.status, completed.resources);

const admitted = await sandbox.update({ memoryMb: 6144, wait: false });
if (admitted.resourceResize) {
  await sandbox.waitForResourceResize(admitted.resourceResize.generation, {
    timeout: 120,
    pollInterval: 1,
  });
}
```

`SandboxResizeError` exposes `generation`, `reason`, and `info` with the last
confirmed allocation. The client-level `waitForResourceResize()` additionally
takes the sandbox ID/name as its first argument.

## Rust

`UpdateSandboxRequest.resources` takes `ResizeSandboxResources` with optional
`cpus: f64`, `memory_mb: i64`, and `disk_mb: u64` values, matching create's field
names, nesting, and numeric types. `SandboxesClient::update()` waits by default.
Use `update_with_options()` with `ResizeOptions { wait: false, ..Default::default() }`
to return admission, then `wait_for_resource_resize(id, generation, timeout,
poll_interval)` to wait for that exact generation. Wait durations use `Duration`.
`SdkError::SandboxResize` carries the generation, reason, and latest `SandboxInfo`.

## Validation and confirmed allocation

- CPU targets must be finite positive whole-vCPU counts. `2.0` is accepted;
  `1.5` is rejected without rounding. The server owns host-specific CPU limits.
- Memory targets must be positive integer MiB values. CH's immutable boot-memory
  floor and hotplug window are not exposed by the public API. The SDK sends the
  exact request and preserves driver rejection details, including after full
  snapshot restore or suspend/resume. The driver adjusts hotplug memory in
  128 MiB blocks; a successful confirmed allocation can differ from the request.
- Root disk targets must be positive integer MiB values and cannot shrink below
  the current confirmed disk size. Live updates do not use creation-time disk
  minimums or snapshot admission rules. Success follows device/filesystem growth.
- General CPU/memory caps, memory-per-CPU policy, quota, billing, concurrent
  capture, pinned memory, and VMM state remain server/driver decisions.

`resources` always means confirmed allocation. `resource_resize.requested`
(`resourceResize.requested` in TypeScript) is the generation's target. Failed
resizes can partially converge, so inspect confirmed allocation on failure.
Generation waits never treat a newer successful resize as success for an older
one; they report that the older result was superseded. A missing admission
generation is reported as an incompatible response, not completion.

A sandbox must be Running. Full-snapshot-restored or suspended-then-resumed CH
sandboxes may resize once Running. This API adds no GPU, pool-template,
attached-volume, suspended/paused, Firecracker, or gVisor resize functionality.
