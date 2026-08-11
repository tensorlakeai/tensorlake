# Function Executor — Domain Model

**Status:** Implemented · **Date:** 2026-07-25 · **Owner:** Tensorlake · **Scope:** Function Executor

This page defines the entities managed by a Function Executor. Their semantic shapes are formalized
in [canonical-types.schema.json](canonical-types.schema.json); protobuf remains the authoritative
wire encoding.

---

## Identifier scheme

Identifiers are opaque non-empty strings. Their scope is defined by the producing component.

| Identifier | Producer | Required scope |
|---|---|---|
| `request_id` | orchestration server | Application request |
| `function_call_id` | orchestration server or SDK durable hash | Request |
| `allocation_id` | orchestration server | Executor process |
| output BLOB request ID | allocation runner | Allocation |
| state operation ID | allocation runner | Allocation |

TypeScript durable function-call IDs are SHA-256 hashes of the parent call ID, previous durable ID,
operation kind, and target function name. Python durable IDs remain language-runtime values but
must be deterministic under strict replay.

---

## Entities

### Function reference

A function reference carries namespace, application name, application version, and function name.
Initialization binds the process to one reference.

### Serialized object

A serialized object consists of a manifest and bytes. The manifest declares encoding, size,
metadata size, SHA-256 digest, optional content type, and optional source function-call ID.
`SerializedObjectInsideBLOB` adds an offset into a context-provided BLOB.

### BLOB

A BLOB has an allocation-scoped ID and ordered chunks. Each chunk carries a URI and a declared
size. Downloaders validate ranges, manifests, and hashes; uploaders do not exceed the supplied
chunk capacity.

### Allocation

An allocation carries its three IDs, function inputs, optional request-error BLOB, SDK metadata,
and replay mode. The runner attaches transient protocol state but does not rewrite the input
identity.

### Allocation state

Allocation state is a level-triggered snapshot. It contains current progress, outstanding output
BLOB requests, outstanding request-state operations, and a SHA-256 hash of the snapshot content.
Python also exposes legacy function-call and watcher lists required by its runtime path.

### Execution event and batch

Execution events are executor output. Active event kinds create a function call, create a watcher,
record a creation failure, or finish the allocation. A batch remains current until the server
advances it.

### Allocation event and page

Allocation events are server input. They acknowledge call creation, acknowledge watcher creation,
or deliver a watcher result. Events carry monotonically increasing logical clocks and arrive in
pages bounded by `max_entries`.

---

## Relationships

```text
FunctionRef 1 ---- 1 ExecutorProcess
ExecutorProcess 1 ---- * Allocation
Allocation 1 ---- 1 AllocationState
Allocation 1 ---- * ExecutionBatch ---- * ExecutionEvent
Allocation 1 ---- * EventLogPage  ------ * AllocationEvent
Allocation 1 ---- * BLOB
Allocation 1 ---- * StateOperation
FunctionCall 1 ---- 0..1 Watcher
```

---

## Lifecycle state machines

### Executor service

```text
uninitialized -- initialize success --> initialized -- shutdown --> stopping
      |                  |
      +-- failure -------+--> uninitialized and retryable
```

Initialization failure rolls back imported modules or registries and releases partial runtime
resources. A successful initialization cannot be replaced in the same process.

### Allocation

```text
admission -> running -> terminal batch queued -> terminal batch observed -> deletable
                |                ^
                +-- cancel ------+
                +-- failure -----+
```

Exactly one terminal event is queued. Deletion is rejected while the runner is active. Shutdown
cancels active I/O and protocol waiters and gives the server a bounded window to observe the
terminal batch. TypeScript waits for allocation runners during graceful shutdown; Python queues the
terminal result before transport shutdown and retains a forced process-exit bound because an
arbitrary Python thread cannot be interrupted safely.

### Protocol request

```text
queued -> published -> matched response -> removed and settled
   |                                     
   +-- allocation abort ----------------> rejected and removed
```

Responses are correlated by ID. An output-BLOB error without a BLOB payload is matched to the
oldest outstanding output request because the response contains no BLOB ID.

---

## Required query patterns

| Query | Access pattern |
|---|---|
| Allocation by ID | In-process allocation map |
| Current allocations | Snapshot of the allocation map |
| Outstanding BLOB request by ID | Allocation-local map |
| Outstanding state operation by ID | Allocation-local map |
| Current execution batch | FIFO queue head |
| Durable event by predicate | Replay history or live waiter/backlog |

---

## Assumptions and open questions

**Assumptions**

- IDs are unguessable only when their producer requires that property; the protocol treats them as opaque.
- The server sends at most one active response for each correlation ID.

**Decisions**

- *Allocation state.* **Level-triggered snapshots.** A reconnect receives all outstanding work, so
  backpressured streams can coalesce intermediate states.
- *Execution log.* **Acknowledged FIFO batches.** This prevents a slow server from losing durable
  intent and preserves terminal-event ordering.
- *Event ordering.* **Logical clocks are strict within each delivered page.** Invalid or ambiguous
  pages fail the allocation instead of permitting indefinite polling.

**Open questions**

(None at this stage.)
