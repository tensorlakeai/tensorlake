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
compared. Before decoding an executor-emitted typed child-call argument, the
driver also applies the production server's content-type-to-encoding round trip
and rejects metadata that would change encoding when used as the child
allocation input.

The TypeScript fixture uses schema-free JSON registrations for every applicable
scenario, so the matrix also verifies that inferred descriptors drive the same
executor protocol outcomes as the explicit Python definitions. File scenarios
and the `HttpBody` scenario remain schema-rich because they cross the protocol
as raw bytes.

## Parity matrix

| Scenario | Contract checked |
| --- | --- |
| `parity_value` | JSON input/output and a successful terminal result without durable calls |
| `parity_multipart` | Multiple JSON arguments encoded as file-like multipart parts |
| `parity_child` | Blocking durable child call, watcher result, and successful strict replay of the captured history |
| `parity_wait_first_failure_after_success` | `Future.wait(FIRST_FAILURE)` continues past an already-successful future until every remaining future succeeds |
| `parity_wait_first_failure_after_success_and_failure` | `Future.wait(FIRST_FAILURE)` keeps a previously successful future in `done` when a later future fails |
| `parity_wait_causal_replay` | `Future.wait(FIRST_COMPLETED)` and strict replay preserve the original watcher-result position across asynchronous application work and a later durable call |
| `parity_wait_batched_results` | `Future.wait(FIRST_COMPLETED)` uses the first completion boundary even when multiple watcher results arrive in one event-log page |
| `parity_map` | Remote fan-out, deterministic call order, ordered results, and strict replay for three inputs |
| `parity_reduce` | Server-orchestrated reduction with an explicit initial value, dependency-chain validation, and strict replay |
| `parity_reduce_large` | Reduction across the TypeScript executor's 512-call plan boundary, including cross-plan accumulator references and strict replay |
| `parity_reduce_no_initial` | Reduce without an initial value, including first-item accumulator semantics and strict replay |
| `parity_map_reduce` | A remote map result used directly as reduce input, covering Promise/collection composition and strict replay |
| `parity_tail_call` | Durable tail call with no result watcher |
| `parity_handled_child_failure` | A failed child watcher becomes a catchable `FunctionError` |
| `parity_handled_child_request_error` | A child request-error watcher becomes a catchable cross-bundle `RequestError` |
| `parity_handled_child_timeout` | A timed-out child watcher becomes a catchable cross-bundle `TimeoutError` |
| `parity_handled_creation_failure` | A rejected child creation becomes a catchable `FunctionError` |
| `parity_watcher_creation_failure` | A rejected watcher creation terminates immediately instead of waiting forever for a result event |
| `parity_request_error` | Terminal request-error classification and exact UTF-8 error payload |
| `parity_function_error` | Terminal function-error classification |
| `parity_file` | Raw `File` output bytes and content type |
| `parity_json_file` | A JSON-MIME `File` retains raw file identity and cross-bundle `instanceof` behavior across a durable child call |
| `parity_http_body` | Raw application bytes, MIME type, text/JSON helpers, sanitized duplicate request headers, and cross-bundle `HttpBody` identity |
| `parity_state` | Missing-state default, prepare-write/commit-write/read round trip, request ID, and progress update |
| `parity_progress_validation` | Invalid progress counters, messages, and attribute containers are rejected consistently without publishing progress |
| `parity_context_validation` | Invalid request-state keys and metric names/values raise `SDKUsageError` before publishing state operations or events |
| `parity_replay_mismatch` | Strict replay divergence terminates with the replay-history-mismatch reason |
| `parity_http_envelope` | A forwarded `message/http` public invocation preserves its JSON body and request-context header |
| `parity_http_envelope_default` | An empty forwarded HTTP request supplies zero arguments so language defaults apply |
| `parity_file_input` | Direct binary `File` input preserves exact bytes and MIME type |
| `parity_multipart_http_body` | Multipart raw `HttpBody` and JSON metadata parameters deserialize together |
| `parity_empty_http_body` | An empty `HttpBody` preserves an absent content type instead of inventing a JSON MIME type |
| `parity_malformed_json` | Malformed JSON application input terminates as a function error |
| `parity_chunked_http_body` | A multi-chunk 256 KiB input is reassembled without truncation or corruption |
| `parity_wait_all_completed` | `Future.wait(ALL_COMPLETED)` returns successful and failed futures together |
| `parity_wait_timeout` | A timed-out watcher is represented as a completed failed future |
| `parity_run_later` | Delayed execution emits a scheduled durable call and returns its result |
| `parity_detached_future` | Starting a future without consuming its result does not keep the parent allocation alive |
| `parity_future_reuse` | Re-reading one future returns the same result without creating another child call |
| `parity_map_empty` | Empty map returns an empty collection, allowing language-specific internal call plans |
| `parity_reduce_empty_initial` | Empty reduce with an initial value returns that value, allowing language-specific internal call plans |
| `parity_map_failure` | A failed fan-out terminates the graph once; concurrent TypeScript watcher installation is intentionally not asserted |
| `parity_reduce_failure` | A failed reducer propagates a terminal graph function error through strict replay |
| `parity_unhandled_child_failure` | An unhandled child failure propagates as a terminal function error through strict replay |
| `parity_unhandled_child_request_error` | An unhandled child request error preserves its terminal classification and payload through strict replay |
| `parity_context_events` | State overwrite/read order, counter/timer CloudEvents, full progress payloads, and replayed context effects |
| `parity_state_failure` | A request-state service failure propagates as a terminal function error through strict replay |

Before executor scenarios run, separate Python and TypeScript manifest probes
generate a public `HttpBody` application and a resource-configured child
function. The normalized manifests must agree on application name,
description, tags, `allow=["unauthenticated_requests"]`, entrypoint
serializers, raw-body parameter type, function resources, GPU shape, retry
policies, regions, secrets, image, timeouts, and container scaling options.

For every scenario, the harness rejects an empty function-reference field and
deliberately false application archive size and SHA-256 values, requiring both
runtimes to return the same structured `InitializeResponse` failure. It then runs a different application
archive while requesting a manifest-only function that the module does not
register, verifies the same structured post-import failure, and retries the
valid function from the current archive on the same executor. This catches
leaked module, registry, or import-path state that would otherwise poison
initialization retries. A second initialization after success must also return
the same structured function-error response. The harness requires exact `INVALID_ARGUMENT`
statuses for an empty required ID, mismatched argument/BLOB counts, a missing request-error BLOB, a
missing argument manifest, a missing or oversized metadata size, and a BLOB
chunk without a URI. It also rejects sizes outside the shared safe-integer
range and verifies that none of those requests retain an allocation. Before
initialization begins, it requires `create_allocation`
to return the exact `FAILED_PRECONDITION` lifecycle status. It then checks
protocol version reporting, post-initialization
health, allocation listing, exactly one terminal event, acknowledgement of
every execution-log batch, an empty batch after completion, allocation
deletion, output hashes and BLOB bounds.

Once per harness run, the server also rejects an output-BLOB request with a
non-OK status and no BLOB payload, as required by the protocol. Both executors
must associate that ID-less response with the outstanding request, emit exactly
one terminal internal-error event, finish the allocation, and allow it to be
deleted.

The harness also sends both executors an event-log page whose entry and page
clocks fail to advance. Live execution must terminate exactly once with an
internal error, while strict replay must terminate exactly once with a replay
history mismatch. This checks that neither runtime continues polling malformed
history and that both classify the same protocol corruption consistently.
It also captures one valid child-call history and corrupts it three ways:
an unknown event, a watcher result before watcher creation, and a duplicate
watcher result. Every corruption must terminate exactly once as a strict replay
history mismatch without emitting new durable operations.

Once per harness run, it starts allocations whose HTTP input download and
output upload never respond and another allocation with an unacknowledged
child-call execution batch, then sends the executor process its termination
signal. Both runtimes must exit within five seconds with exit code zero and
deliver exactly one terminal function-error event. In the queued-batch case,
the harness acknowledges the older batch after shutdown begins and verifies
that the terminal batch behind it remains available. The complete shutdown
result is compared across languages, so a closed execution-log RPC, stranded
terminal batch, or language-specific exit mode is a parity failure.
While the input download is blocked, duplicate allocation creation must return
`ALREADY_EXISTS` and deletion of the active allocation must return
`FAILED_PRECONDITION`.

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

The harness covers the common, locally observable function-executor and
application-manifest contracts. It deliberately compares expected outcomes
instead of requiring Python and TypeScript to use identical future or graph
internals. Public HTTP routing beyond the forwarded executor input, server
retry policy, application deployment, scheduling execution, and final request
aggregation are owned by the orchestration server and remain covered by server
verification rather than this local harness.
