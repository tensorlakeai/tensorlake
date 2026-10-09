# Resize a running sandbox

Resize CPU, memory, or the root disk of a running CPU-only CAS sandbox.
Running GPU CAS sandboxes support root-disk growth only; CPU and memory changes
are rejected before submitting an update.
Non-CAS sandboxes do not support live resize.
Use `tl sbx describe <id>` to check its current allocation. Memory
and disk values are MiB, with the same resource names and units as create.
Omitted dimensions stay unchanged; send name, proxy, and network updates separately.

## Resize and wait

```sh
tl sbx update my-sandbox -c 2 -m 4096 --disk_mb 20480
```

For a GPU CAS sandbox, supply only the disk target:

```sh
tl sbx update my-gpu-sandbox --disk_mb 20480
```

The CLI prints the requested target, shows a spinner while waiting, and reports
the confirmed allocation when the resize completes. It waits up to 300 seconds.
Use `--wait-timeout 120` to change that budget. This does not change the sandbox's
lifetime; create's `--timeout` still means lifetime and is not an update flag.

Status goes to stderr. When piped, stdout contains only the sandbox ID after a
successful command. A failed resize exits nonzero and reports the server's
reason and last confirmed allocation. An unchanged target makes no update
request and reports “No resource changes”.

## Resize now and wait later

```sh
tl sbx update my-sandbox --memory 6144 --no-wait
# Reports the accepted resize generation, for example 7, and the wait command.
tl sbx wait my-sandbox --resize 7 --timeout 300
```

`--no-wait` (also `-n`) returns once the update is accepted. The printed generation
identifies the operation to wait for. `sbx wait --resize` only reads its status;
it never submits another update. Without `--resize`, `sbx wait` retains its
existing behavior of waiting for the sandbox to be running.

## Recover from a timeout

```sh
tl sbx update my-sandbox --memory 8192 --wait-timeout 30
# If the wait times out, the error gives the exact command to continue, such as:
tl sbx wait my-sandbox --resize 8 --timeout 300
# Inspect the latest request, status, and confirmed allocation at any time:
tl sbx describe my-sandbox
```

The hint includes `--timeout 300`, matching update's default wait budget.
`--timeout 0` on a resize wait (or `--wait-timeout 0` on update) checks status once
without polling again. The client's request timeout applies to that check.
It succeeds if the resize is already complete and otherwise returns the observed
failure or a timeout with the latest allocation.

Timeout ends the wait, not the resize. Copy the generation from the timeout
error and keep waiting for it; do not resubmit the resource update to wait.
`--no-wait` and `--wait-timeout` cannot be combined, and neither is accepted on a
network-only update.

## Python

These examples use a connected sandbox. The same resource and wait arguments
work on `Sandbox.update()`, `AsyncSandbox.update()`,
`SandboxClient.update_sandbox()`, and `AsyncSandboxClient.update_sandbox()`.
Use `await` with either asynchronous interface.

```python
from tensorlake.sandbox import Sandbox

sandbox = Sandbox.connect("my-sandbox")
info = sandbox.update(cpus=2.0, memory_mb=4096, disk_mb=20480)
print(info.resources)  # Confirmed allocation after waiting.
```

If every target already matches, this is a **no-op**: the call returns current
resources without submitting an update. `info.resource_resize` can then be
`None` or describe an earlier operation, even a failed one. Do not use its status
as the result of this call. The example above works in all these cases.

For an update that admits a new resize, keep its returned generation to wait later:

```python
from tensorlake.sandbox import ResizeStatus

admitted = sandbox.update(memory_mb=6144, wait=False)
resize = admitted.resource_resize
if resize is not None and resize.status == ResizeStatus.PENDING:
    completed = sandbox.wait_for_resource_resize(resize.generation, timeout=120)
    print(completed.resources)
else:
    print(admitted.resources)
```

The no-op rule also applies with `wait=False`: existing metadata does not imply
that this call started a resize. Only wait on a generation you intend to observe.
Client-level waits take the sandbox ID/name before the generation.

Recover from a timeout without replaying the update:

```python
from tensorlake.sandbox import ResizeErrorReason, SandboxResizeError

try:
    info = sandbox.update(memory_mb=8192, timeout=30)
except SandboxResizeError as error:
    print(error.confirmed_resources)
    if error.reason != ResizeErrorReason.TIMEOUT:
        raise
    info = sandbox.wait_for_resource_resize(error.generation, timeout=300)
print(info.resources)
```

`timeout` defaults to 300 seconds and `poll_interval` to 1 second. A zero timeout
checks status once. These options control polling, not sandbox lifetime, and
have no effect with `wait=False`. All wait options are ignored on updates without
resource targets. CPU values must be
positive whole numbers. Memory/disk accept positive integers and integer-valued
floats such as `2048.0`, matching create's numeric coercion; fractional values,
booleans, and numeric strings are rejected without rounding.

## TypeScript

`Sandbox.update()` and `SandboxClient.update()` use create's `cpus`, `memoryMb`,
and `diskMb` option names:

```ts
import { Sandbox } from "@tensorlakeai/tensorlake";

const sandbox = await Sandbox.connect({ sandboxId: "my-sandbox" });
const info = await sandbox.update({ cpus: 2, memoryMb: 4096, diskMb: 20480 });
console.log(info.resources);
```

A no-op returns current resources without submitting an update. Its
`resourceResize` can be absent or refer to an earlier generation, including a
failed one; use confirmed resources as above to report the result.

```ts
import { ResizeStatus } from "@tensorlakeai/tensorlake";

const admitted = await sandbox.update({ memoryMb: 6144, wait: false });
const resize = admitted.resourceResize;
if (resize?.status === ResizeStatus.PENDING) {
  const completed = await sandbox.waitForResourceResize(resize.generation, {
    timeout: 120,
    pollInterval: 1,
  });
  console.log(completed.resources);
}
```

As in Python, a no-op does not create a generation: only wait on one you intend
to observe. To recover from a timeout, catch `SandboxResizeError`, compare
`error.reason` with `ResizeErrorReason.TIMEOUT`, and call
`waitForResourceResize(error.generation)`. `error.confirmedResources` contains
the last confirmed allocation when available. Client-level waits take the
sandbox ID/name first. Wait options are ignored on non-resource updates.

## Rust

`UpdateSandboxRequest.resources` takes `ResizeSandboxResources` with optional
`cpus: f64`, `memory_mb: i64`, and `disk_mb: u64` values, matching create's names,
nesting, and numeric types. `SandboxesClient::update()` waits by default.

Use `update_with_options()` with
`ResizeOptions { wait: false, ..Default::default() }` to return once accepted,
then `wait_for_resource_resize(id, generation, timeout, poll_interval)` to wait
for that operation. Durations use `Duration`. `SdkError::SandboxResize` carries
the generation, reason, and last `SandboxInfo`.

For an explicit distinction between a no-op and a new resize,
`update_with_result()` returns `info` and `resize_generation`. The latter is
`None` when no resize was submitted, regardless of earlier metadata in `info`.
This also lets the CLI report no-ops without a second preflight GET.

## Generations and resource limits

Each accepted resize has a generation. `resources` is the confirmed allocation;
`resource_resize.requested` (`resourceResize.requested` in TypeScript) is the
operation's target. Status is `pending`, `succeeded`, or `failed`, exposed as
`ResizeStatus` in Python and TypeScript. Unknown future statuses remain strings
so the sandbox can still be fetched and inspected. A failed resize can partially converge,
so inspect confirmed resources on failure.

Waiting for an older generation never treats a newer successful resize as its
result. Instead it reports `ResizeErrorReason.SUPERSEDED`; the earlier result is
no longer available. A missing generation in an update response is an
incompatible response, not confirmation of completion.

- CPU targets must be finite positive whole-vCPU counts. `2.0` is accepted;
  `1.5` is rejected without rounding. Host-specific limits remain server decisions.
- CPU-only CAS sandboxes' boot-memory floor and hotplug window are not exposed by
  the public API. The SDK preserves driver rejection details, including after
  full snapshot restore or suspend/resume. The driver adjusts hotplug memory
  in 128 MiB blocks, so confirmed memory can differ from the requested target.
- Root disk targets cannot shrink below the current confirmed size. Live resize
  does not apply create-time disk minimums or snapshot admission rules. Success
  follows device/filesystem growth.
- General CPU/memory caps, memory-per-CPU policy, quotas, billing, concurrent
  capture, pinned memory, and virtual machine state remain server/driver decisions.

A sandbox must be running. Full-snapshot-restored or suspended-then-resumed
CPU-only CAS sandboxes may resize once running. GPU CAS sandboxes support only root-disk
growth. Unchanged CPU and memory targets are omitted from the update, so they may
be supplied alongside a larger disk target without requesting a CPU or memory
resize. GPU allocation, pool templates, and attached volumes cannot be resized
through this API. Suspended, paused, and non-CAS sandboxes do not support live resize.
