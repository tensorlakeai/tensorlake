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

The Linux SDK workflow runs the runner regression suite with the actual PyO3
extension, then runs this comparison and uploads its output under an exact-head
artifact name. Validation results and exact CI links are pending; no measured
speedup is claimed before those results are collected. No macOS test, build, or
benchmark is required or used for this change.
