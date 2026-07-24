# Function executor compatibility harness

This harness launches the real Python and TypeScript function executors and
drives both through the Python gRPC bindings generated from the shared protocol
in `proto/tensorlake/function_executor/proto`.

Each executor receives an equivalent application archive and logical
allocation. The driver acts as the orchestration server: it supplies inputs,
persists output BLOBs, acknowledges execution-log batches, returns durable
event-log entries, services request-state operations, and validates the
terminal allocation result. Language-specific serialization is decoded and
normalized before the externally observable Python and TypeScript results are
compared.

The TypeScript fixture uses schema-free JSON registrations for every applicable
scenario, so the matrix also verifies that inferred descriptors drive the same
executor protocol outcomes as the explicit Python definitions. File scenarios
remain schema-rich because they cross the protocol as raw bytes.

## Parity matrix

| Scenario | Contract checked |
| --- | --- |
| `parity_value` | JSON input/output and a successful terminal result without durable calls |
| `parity_multipart` | Multiple JSON arguments encoded as file-like multipart parts |
| `parity_child` | Blocking durable child call, watcher result, and successful strict replay of the captured history |
| `parity_wait_first_failure_after_success` | `Future.wait(FIRST_FAILURE)` continues past an already-successful future until every remaining future succeeds |
| `parity_wait_first_failure_after_success_and_failure` | `Future.wait(FIRST_FAILURE)` keeps a previously successful future in `done` when a later future fails |
| `parity_wait_causal_replay` | `Future.wait(FIRST_COMPLETED)` and strict replay preserve the original watcher-result position across a later durable call |
| `parity_wait_batched_results` | `Future.wait(FIRST_COMPLETED)` uses the first completion boundary even when multiple watcher results arrive in one event-log page |
| `parity_map` | Remote fan-out, deterministic call order, ordered results, and strict replay for three inputs |
| `parity_reduce` | Server-orchestrated reduction with an explicit initial value, dependency-chain validation, and strict replay |
| `parity_reduce_no_initial` | Reduce without an initial value, including first-item accumulator semantics and strict replay |
| `parity_map_reduce` | A remote map result used directly as reduce input, covering Promise/collection composition and strict replay |
| `parity_tail_call` | Durable tail call with no result watcher |
| `parity_handled_child_failure` | A failed child watcher becomes a catchable `FunctionError` |
| `parity_handled_child_request_error` | A child request-error watcher becomes a catchable cross-bundle `RequestError` |
| `parity_handled_creation_failure` | A rejected child creation becomes a catchable `FunctionError` |
| `parity_watcher_creation_failure` | A rejected watcher creation terminates immediately instead of waiting forever for a result event |
| `parity_request_error` | Terminal request-error classification and exact UTF-8 error payload |
| `parity_function_error` | Terminal function-error classification |
| `parity_file` | Raw `File` output bytes and content type |
| `parity_json_file` | A JSON-MIME `File` retains raw file identity and cross-bundle `instanceof` behavior across a durable child call |
| `parity_state` | Missing-state default, prepare-write/commit-write/read round trip, request ID, and progress update |
| `parity_replay_mismatch` | Strict replay divergence terminates with the replay-history-mismatch reason |

For every scenario, the harness also rejects deliberately false application
archive size and SHA-256 values and requires both runtimes to return the same
structured `InitializeResponse` failure. It then runs a different application
archive while requesting a manifest-only function that the module does not
register, verifies the same structured post-import failure, and retries the
valid function from the current archive on the same executor. This catches
leaked module, registry, or import-path state that would otherwise poison
initialization retries. The harness also requires exact `INVALID_ARGUMENT`
statuses for mismatched argument/BLOB counts, a missing request-error BLOB, a
missing argument manifest, a missing or oversized metadata size, and a BLOB
chunk without a URI, and verifies that none of those requests retain an
allocation. Before initialization begins, it also requires `create_allocation`
to return the exact `FAILED_PRECONDITION` lifecycle status. It then checks
protocol version reporting, post-initialization
health, allocation listing, exactly one terminal event, acknowledgement of
every execution-log batch, an empty batch after completion, allocation
deletion, output hashes and BLOB bounds.

Once per harness run, it also starts allocations whose HTTP input download and
output upload never respond, sends the executor process its termination signal,
and requires both runtimes to exit within five seconds. It verifies the exact
exit mode in both cases. The TypeScript executor must complete graceful
shutdown with exit code zero and deliver one terminal function-error event for
the cancelled allocation; the Python reference executor currently exits from
`SIGTERM` and the pending execution-log RPC must close with `UNAVAILABLE`.
This verifies that pending BLOB I/O cannot trap executor shutdown without
misrepresenting the runtimes' distinct process-level behavior as parity.

Python and TypeScript both emit one logical server-orchestrated reduce. Python
uses its splitter-call metadata, while TypeScript emits ordinary `FunctionCall`
operations whose accumulator arguments reference the preceding step. The
harness rejects the deprecated protocol `ReduceOp` and normalizes both current
encodings before asserting the same reducer, ordered collection, initial value,
watcher, strict replay behavior, and final result.

## Run it

Install the Python and TypeScript dependencies, then run the full matrix:

```sh
poetry install --with=dev
npm --prefix typescript ci
make test_function_executor_compatibility
```

The Make target rebuilds the TypeScript SDK and executor before launching
either runtime, so it cannot accidentally use stale generated JavaScript.

To iterate on one or more scenarios after building:

```sh
npm --prefix typescript run build:sdk
PYTHONPATH=src poetry run python tests/function_executor_compatibility/run.py \
  --scenario parity_reduce \
  --scenario parity_replay_mismatch
```

The harness covers the common, locally observable function-executor protocol
and process-shutdown cancellation during BLOB I/O. Per-allocation
cancellation, retries, scheduling, application deployment, and final request
aggregation are owned by the orchestration server and remain covered by server
verification rather than this local harness.
