# Function Agent Python bridge

## Controlled delayed-core comparison

The Task09 harness compares the exact global-lock `ProtocolWriter` behavior from
SDK `317e9d5a620bbab4420648bb46e374980b163eb4` with the current per-attempt writer
in the same Python process. Both receive identical failure messages for 16
independent attempts per wave, eight waves per mode, three interleaved pairs.
Each fake native submission waits 10 ms on the actual asyncio loop. The test
requires 128 completed acknowledgments in each mode, maximum native concurrency
one for the control and 16 for the current writer, and no retained ordering keys.

Timing starts after all worker threads reach the start gate and ends when all
writes return from their fake native acknowledgment. Reported CPU includes the
whole mode, including thread setup and gate preparation. Peak RSS is the process
high-water mark, not incremental memory or a per-mode allocation measurement.
Each wave and comparison has an explicit timeout and prints progress; failed
checks fail the command. These figures characterize language-bridge blocking,
not real native fsync throughput, executed Python functions, Function Service
throughput, or a 15k-invocation/s capacity result. Real WAL durability/group-commit
evidence remains in CEI's reviewed Task06 record.

Commands, from an existing Poetry environment with a freshly built native
extension staged from `51577dc4a8b9fbbec7d5c5120f9cb6afa8de64b0`:

```sh
just test-function-agent-python
just check-function-agent-python
just bench-function-agent-python
```

## Linux CI evidence — 2026-09-10 UTC

Executable SDK revision: `d99cbaa708a0de80607ecbb7715837596aebdd88`.
The [Linux lint/test job](https://github.com/tensorlakeai/tensorlake/actions/runs/34426030186/job/102711346464)
completed successfully at that exact revision. Its staging log confirms CEI
`51577dc4a8b9fbbec7d5c5120f9cb6afa8de64b0` for both the core and native binding
modules. The manifest matches that core except for the documented SDK `tls-ring`
default. This was a fresh GitHub Actions checkout with its own Cargo target and
the normal restored Cargo cache, not the shared SSH worktree target.

Runner: `ubuntu-latest-xlarge-1000184526`, runner group `image-builder`, label
`ubuntu-latest-xlarge`; Ubuntu 24.04.4 x86_64, image `ubuntu-24.04` version
`20260831.293.1`; CPython 3.10.21, Poetry 2.0.0, maturin 1.12.6, rustc 1.98.1
(`48a229cea`, 2026-09-01), just 1.58.0. CPU model/count, installed RAM and kernel
version were not emitted by this job and are unknown; the label is not a
substitute measurement. All six samples ran on this same runner/process.

The job ran `poetry install --with=dev && make build_cloud_sdk && make check`,
building and installing the actual abi3 Python >=3.10 extension. It then ran
`PYTHONPATH=src poetry run python -m unittest` over the existing sandbox/image
modules plus `tests.function_agent.test_runner`: **309 tests passed in 2.383 s**,
including all 16 runner test methods.
This includes both forced-timeout benchmark cleanup cases, queued/active canceled
writer waits, independent progress, same-attempt native errors, initialization,
shutdown, malformed identities, repeated-key reclamation and real PyO3 validation
errors. The expected stopped-agent logs come from tests targeting an unavailable
local registration endpoint. Focused Black/isort checks also passed.

Actual comparison command:

```sh
just check-function-agent-python
just bench-function-agent-python | tee function-agent-python-benchmark.log
```

| Pair | Writer | All-ACK time (s) | Bridge outputs/s | User CPU (s) | System CPU (s) |
|---|---|---:|---:|---:|---:|
| 1 | Prior global lock | 1.339231 | 95.577 | 0.043287 | 0.022273 |
| 1 | Per-attempt lock | 0.111625 | 1146.693 | 0.031938 | 0.013973 |
| 2 | Prior global lock | 1.346184 | 95.084 | 0.052274 | 0.013909 |
| 2 | Per-attempt lock | 0.109676 | 1167.070 | 0.030922 | 0.013793 |
| 3 | Prior global lock | 1.333646 | 95.977 | 0.047285 | 0.014244 |
| 3 | Per-attempt lock | 0.107690 | 1188.598 | 0.027343 | 0.014958 |

Every sample completed exactly 128 outputs; maximum fake-native in-flight count
was one for the control and 16 for the candidate. Process peak RSS was 62,468 KiB
for every sample. Median bridge rate changed from 95.577 to 1167.070 outputs/s
under the imposed 10-ms fake-native delay. This isolates removal of Python's
cross-attempt serialization; it is not native fsync throughput or end-to-end
invocation capacity. No measured speedup is inferred for other workloads.

Raw output is retained in
[artifact 10132716481](https://github.com/tensorlakeai/tensorlake/actions/runs/34426030186/artifacts/10132716481),
named `function-agent-python-bridge-d99cbaa708a0de80607ecbb7715837596aebdd88`.
The locally collected copy is
`/tmp/perf09-ci-evidence.2wvGQu/function-agent-python-benchmark.log`.

At the same SDK head, the [TypeScript unit job](https://github.com/tensorlakeai/tensorlake/actions/runs/34426032237/job/102711349747)
passed type checking and 532 tests across 25 files in 8.25 s, including three
Function Agent runner tests (113 ms).
The [native Function Agent binding smoke job](https://github.com/tensorlakeai/tensorlake/actions/runs/34426032237/job/102711349894)
also passed: glibc native build with verified GLIBC_2.28 floor, musl native build
without glibc symbol dependencies, runner capsule/package compatibility, native
stream lifetime and cancellation, and packed SDK worker exit checks. The entire
[TypeScript workflow](https://github.com/tensorlakeai/tensorlake/actions/runs/34426032237)
passed, including its existing integration and Python/TypeScript compatibility
jobs; those are separate coverage, not the timing workload above.

The [Rust workspace job](https://github.com/tensorlakeai/tensorlake/actions/runs/34426030186/job/102711346460)
passed its existing full-feature build, HTTP transport lint and workspace tests.
The staged native Function Agent core ran **98 passed, one ignored in 1.43 s**,
including WAL replay/group commit, reserved-completion backpressure and canceled
waiter tests. The ignored test is the separate native WAL benchmark. Existing
Sandbox integration CI also passed. No manually launched cloud acceptance run or
production deployment was performed. The Windows build job remains outside this
Linux evidence record until its result is available.
