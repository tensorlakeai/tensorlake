# Function Executor — Execution and Replay

**Status:** Implemented · **Date:** 2026-07-25 · **Owner:** Tensorlake · **Scope:** Function Executor

This page specifies allocation execution after service admission. It covers user-code boundaries,
durable operations, replay, terminal outcomes, and cleanup.

---

## Responsibilities

1. Download and deserialize function arguments.
2. Run the registered handler inside allocation-scoped runtime and request contexts.
3. Convert SDK operations into durable execution events.
4. Wait for server acknowledgements and watcher results.
5. serialize one terminal value, tail call, request error, or failure.
6. Settle all protocol work when execution terminates.

The runner does not decide server retry policy. A watcher result represents the server's eventual
success or terminal failure after retry handling.

---

## Execution flow

```text
validate inputs
      |
download + deserialize
      |
install allocation/request context
      |
invoke handler
      |
      +--> durable call/map/reduce --> execution event --> server event --> resume
      |
classify result or exception
      |
upload output or request-error payload
      |
queue exactly one finish event
      |
abort and settle remaining protocol work
```

Input and output boundaries validate declared lengths, metadata ranges, SHA-256 digests, encoding,
and language-level value constraints. A direct `File` crosses as raw bytes with content type.

---

## Durable function calls

A durable child call emits:

1. a `create_function_call` execution event containing an execution plan and optional argument BLOB;
2. an event-log wait for `function_call_created`;
3. a `create_function_call_watcher` event when the caller needs the result;
4. an event-log wait for `function_call_watcher_created`; and
5. an event-log wait for the terminal watcher result.

Creation and watcher failures are catchable application errors after their protocol response.
Transport or malformed-protocol failures latch an allocation-level protocol error so user code
cannot catch the local rejection and later report success.

Concurrent calls reserve deterministic call and watcher emission turns. Input promises are resolved
before durable IDs are assigned for map fan-out. The result array retains input order.

---

## Reduce and tail calls

Reduce is server-orchestrated as a chain of ordinary function calls. Each step receives the prior
step by function-call reference and the next item by value. TypeScript emits plans of at most 512
calls, linking the first accumulator of each later plan to the preceding plan. Python emits its
current splitter-call representation. The deprecated protobuf `ReduceOp` is not emitted.

Without an explicit initial value, the first collection item becomes the accumulator. An empty
collection without an initial value is an SDK usage error. An empty collection with an initial
value returns that value without a remote reducer call.

A tail call finishes successfully with `tail_call_durable_id` and no watcher for that call.
TypeScript uses `registeredFunction.tailCall(...)`; Python recognizes the returned function future
as a tail-call result.

---

## Error and terminal classification

| Condition | Allocation outcome | Failure reason |
|---|---|---|
| Value serialized and uploaded | success | none |
| Tail call created | success | none |
| Unhandled `RequestError` | failure | request error |
| User handler or serialization failure | failure | function error |
| External cancellation or shutdown | failure | function error |
| Strict replay divergence | failure | replay event history mismatch |
| Malformed protocol, unexpected watcher outcome, or internal invariant | failure | internal error |

Request-error payload text is preserved across the BLOB boundary. Child request errors,
function errors, and timeouts become catchable typed SDK errors. Unknown watcher statuses or
outcomes are protocol failures.

Only the first terminal batch is queued. Later normal execution events are rejected after terminal
state.

---

## Strict replay

Strict replay loads event-log pages from clock zero before running the handler. The replay history
tracks expected call creation, watcher creation, and watcher results by deterministic durable ID.

Replay conforms when:

- the same durable operations are emitted in the same logical order;
- each recorded result is consumed at the same causal boundary;
- every expected replay entry is consumed; and
- no result is duplicated or associated with a different operation.

TypeScript tracks promise causality so `Promise.all`, `Promise.race`, `Promise.any`, explicit
chaining, already-resolved wrappers, and mixed ordinary/durable contenders cannot reveal a result
before its recorded history position. A blocked replay boundary terminates as a mismatch rather
than waiting indefinitely.

Replay resources use weak references for promises and release async-hook state after completion.
Executor-owned async resources do not inherit application causality.

---

## Cancellation and terminal cleanup

Allocation cancellation aborts BLOB HTTP I/O, request-context waits, state operations, event-log
reads, and durable event waiters. Normal completion performs the same protocol cleanup for detached
work that the handler did not await.

After the abort signal is set:

- no new output-BLOB request is registered;
- no new state operation is registered;
- no new durable-event waiter is registered;
- existing request maps and published request lists are cleared; and
- execution-log readers receive remaining queued batches followed by an empty batch.

The TypeScript process waits up to ten seconds for graceful service shutdown. The service gives
terminal execution batches a bounded observation window before gRPC shutdown.

---

## Implementation layout

```text
src/tensorlake/function_executor/allocation_runner/
  allocation_runner.py
  event_loop/
  strict_mode_replayer.py
  execution_log_buffer.py
  blob_manager.py
typescript/src/function-executor/
  allocation.ts
  blob.ts
  async-queue.ts
```

---

## Assumptions and open questions

**Assumptions**

- A terminal watcher result already reflects server retry exhaustion.
- The event log is durable and ordered by its logical clock.

**Decisions**

- *Reduce encoding.* **Chains of ordinary function calls.** This keeps dependency semantics in the
  active execution-plan model and avoids the deprecated `ReduceOp`.
- *Replay failure.* **Mismatch is terminal.** Re-executing after a strict divergence would violate
  the caller's deterministic-history request.
- *Cleanup.* **Completion aborts detached protocol work.** An allocation cannot retain waiters or
  create new external work after its terminal result.

**Open questions**

(None at this stage.)
