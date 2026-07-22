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
| `parity_map` | Remote fan-out, deterministic call order, ordered results, and strict replay for three inputs |
| `parity_reduce` | Server-orchestrated reduction with an explicit initial value, dependency-chain validation, and strict replay |
| `parity_tail_call` | Durable tail call with no result watcher |
| `parity_handled_child_failure` | A failed child watcher becomes a catchable `FunctionError` |
| `parity_handled_creation_failure` | A rejected child creation becomes a catchable `FunctionError` |
| `parity_request_error` | Terminal request-error classification and exact UTF-8 error payload |
| `parity_function_error` | Terminal function-error classification |
| `parity_file` | Raw `File` output bytes and content type |
| `parity_json_file` | A JSON-MIME `File` retains raw file identity across a durable child call |
| `parity_state` | Missing-state default, prepare-write/commit-write/read round trip, request ID, and progress update |
| `parity_replay_mismatch` | Strict replay divergence terminates with the replay-history-mismatch reason |

For every scenario, the harness also checks protocol version reporting,
initialization, post-initialization health, allocation listing, exactly one
terminal event, acknowledgement of every execution-log batch, an empty batch
after completion, allocation deletion, output hashes and BLOB bounds.

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

The harness covers the common, locally observable function-executor protocol.
Retries, cancellation, scheduling, application deployment, and final request
aggregation are owned by the orchestration server and remain covered by server
verification rather than this local harness.
