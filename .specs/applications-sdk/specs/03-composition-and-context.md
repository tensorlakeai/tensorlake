# Applications SDK — Composition and Context

**Status:** Implemented · **Date:** 2026-07-25 · **Owner:** Tensorlake · **Scope:** Applications SDK

This page specifies calls, futures, fan-out, reduction, tail calls, retries, request context, File,
and typed failure behavior across local and deployed runtimes.

---

## Function invocation

A registered function invoked outside a deployed runtime executes through the local handler path.
Inside a Function Executor allocation, the same invocation creates a durable child call through the
runtime hook.

Python sync functions block and return their result. Python async functions return a coroutine.
TypeScript handlers and registered callables are Promise-based.

Arguments and results cross a serialization boundary on every function call, including local calls.
This prevents object mutation from leaking across retries or between calls.

---

## Future contract

A Future starts through `run`, delayed run, result access, or await. Starting an already-running
Future raises `SDKUsageError`. A delayed start requires a non-negative finite delay in seconds and
rejects invalid input without starting or mutating the Future. Failures remain available on the
Future and intentionally unawaited failures do not become process-level unhandled rejections.

| Capability | Python | TypeScript |
|---|---|---|
| Immediate start | `future.run()` | `future.run()` |
| Delayed start | `run_later(seconds)` | `runLater(seconds)` |
| Result | blocking `result(timeout)` or await | Promise `result()` or await |
| Completion | `done()` and `exception` | `done` and `exception` |
| Wait | `Future.wait(futures, timeout, RETURN_WHEN)` | `Future.wait(futures, options)` |

Wait modes are all completed, first completed, and first failure. First failure waits past prior
successes until a failure occurs or every remaining Future succeeds. Returned done/not-done
partitions reflect the deterministic completion cutoff, including several results delivered in one
event page. Python wait accepts an iterable of Futures and materializes it exactly once; TypeScript
accepts an array. Both validate the wait mode and non-negative finite timeout before starting any
Future.

---

## Map

Map accepts an iterable or an SDK Future that resolves to an iterable; TypeScript additionally
accepts Promise-like collections and items. Python accepts Tensorlake Futures and the coroutine
wrappers produced by async Tensorlake functions. Inputs resolve before durable call IDs are
assigned, all child calls fan out remotely, and results preserve input order.

An empty collection returns an empty result without child calls. A child failure follows normal
typed child error behavior.

---

## Reduce

Reduce accepts the same language-specific collection forms as map and an optional initial value.
Items and the initial value can be durable results.

With an initial value, every item is reduced. Without one, the first item becomes the accumulator.
Empty input without an initial value raises `SDKUsageError`; empty input with an initial value
returns the initial value.

Local execution applies the reducer sequentially. Deployed execution asks the Function Executor to
emit the server-orchestrated dependency chain specified in the Function Executor contract.

---

## Tail calls and retries

Returning a tail call transfers the allocation result to a durable child without creating a watcher.
TypeScript exposes `registeredFunction.tailCall(...)`. Python runtime output conversion recognizes
the returned function Future as a tail call.

Local retry execution uses the function retry override or application default. Each attempt receives
a fresh serialized copy of its arguments. `RequestError` is not retried. Server retry policy owns
deployed attempts, and the parent watcher receives one eventual success or terminal failure.

---

## Request context

`RequestContext.get()` is valid only within a running Tensorlake function.

### State

State keys must be strings. TypeScript state values must be JSON values. Local state uses serialized
copies; deployed state uses prepare-read, prepare-write, and commit-write protocol operations.
Missing reads return the provided default.

### Progress

Progress requires non-negative finite `current` and `total`. An optional message must be a string.
Attributes must be a plain mapping/object containing only string keys and string values. Invalid
updates raise `SDKUsageError` before publishing state.

### Metrics

Counter names are strings and counter values are integers. TypeScript requires safe integers.
Timer names are strings and timer values are finite numbers. Invalid metrics are rejected before
emitting events.

### Cancellation

TypeScript request context exposes an `AbortSignal`. Cancelling a local request aborts the signal
and rejects output. Deployed executor shutdown and allocation cancellation use the same signal.
Python request context does not expose a public cancellation signal.

---

## File and error boundaries

File values retain exact bytes and content type. TypeScript accepts a File only when the direct
parameter or return schema is `schema.file()`. JSON MIME types do not convert a File into a JSON
value.

Cross-bundle TypeScript brands preserve `instanceof` for File, `RequestError`, `TimeoutError`,
`FunctionError`, and `SDKUsageError`.

| Error | Meaning |
|---|---|
| `SDKUsageError` | Invalid SDK call, registration, schema, or context input |
| `SerializationError` | Value cannot cross the selected boundary |
| `DeserializationError` | Boundary bytes cannot produce the expected value |
| `FunctionError` | Child or handler failed normally |
| `TimeoutError` | Child watcher or explicit wait timed out |
| `RequestError` | User-directed request failure with preserved payload |
| `RequestNotFinished` | Python remote request output was requested before completion |
| `RequestFailed` | Python remote or local application request failed |
| `RemoteAPIError` | Python cloud API request failed at the HTTP boundary |
| `ReplayMismatchError` | TypeScript strict durable history diverged |
| `InternalError` | Python SDK or executor invariant failed |

`TensorlakeException`/`TensorlakeError` in Python and `TensorlakeApplicationError` in TypeScript are
the public base classes for their respective SDK errors.

---

## Assumptions and open questions

**Assumptions**

- The orchestration server preserves the watcher route across retryable child attempts.
- Request-state operations are scoped to one application request.

**Decisions**

- *Map ordering.* **Resolve inputs before assigning durable IDs.** Promise timing cannot reorder
  durable identity under replay.
- *Reduce location.* **Sequential locally, dependency chain when deployed.** Both paths expose the
  same result semantics while deployed work can fan across allocations.
- *Context validation.* **Reject before publication.** Invalid progress, metrics, or state cannot
  create malformed external state.

**Open questions**

(None at this stage.)
