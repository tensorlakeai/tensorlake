# Function Executor — Protocol and Service

**Status:** Implemented · **Date:** 2026-07-25 · **Owner:** Tensorlake · **Scope:** Function Executor

This page specifies the gRPC service implemented from
`proto/tensorlake/function_executor/proto/function_executor.proto`. Message field presence,
oneofs, and enum numbers are defined by that protobuf; this page defines service behavior.

---

## Responsibilities

1. Report runtime identity and health.
2. Initialize one function from a validated application archive.
3. Admit, list, observe, update, and delete allocations.
4. Exchange execution-log batches and event-log read pages.
5. Reject malformed or lifecycle-invalid requests with stable gRPC status classes.

The service does not schedule retries, create durable child allocations, or persist logs.

---

## RPC surface

| RPC | Contract |
|---|---|
| `initialize` | Validate and load one function; return a structured success or failure response |
| `list_allocations` | Return the allocations retained by this process |
| `create_allocation` | Validate, register, and start an allocation |
| `watch_allocation_state` | Stream complete state snapshots until allocation completion |
| `delete_allocation` | Delete only a finished allocation |
| `send_allocation_update` | Deliver an output-BLOB or request-state response |
| `get_allocation_execution_log_batch` | Block until the current batch or terminal empty batch is available |
| `advance_allocation_execution_log_batch` | Acknowledge and remove the current batch |
| `watch_allocation_event_log_reads` | Stream allocation-generated read requests |
| `send_allocation_event_log_read_response` | Deliver the page for the outstanding read |
| `check_health` | Succeed only after initialization |
| `get_info` | Report executor protocol, SDK, language, and language-runtime versions |

The active version `0.1.3` durable path uses execution-log and event-log RPCs. Deprecated
function-call result and legacy output-BLOB fields remain in the protobuf for wire migration but
are not part of the TypeScript service update contract.

---

## Initialization contract

An initialization request requires a complete function reference and an application-code serialized
object. The archive manifest, declared size, metadata size, SHA-256 digest, runtime kind, module,
requested function, and requested application are validated before publishing runtime state.

Python imports the requested module from the code ZIP and resolves its registry entry. TypeScript
extracts the validated ZIP to a temporary directory, imports the ESM module, and calls
`__tensorlakeGetFunction` for both function and application definitions. TypeScript application
archives require format version 2 and Node.js 24 compatibility.

On failure, the service:

1. returns `INITIALIZATION_OUTCOME_CODE_FAILURE`;
2. classifies customer archive or import failures as function errors;
3. restores registry and module state;
4. removes temporary archive resources;
5. releases partially created runtime services; and
6. remains eligible for a new initialization attempt.

Concurrent initialization is serialized. Allocation admission waits while initialization is in
progress and then rechecks deadline, cancellation, shutdown, and initialization success.

---

## Allocation admission and ownership

Admission validates:

- all allocation IDs and inputs required by the runtime;
- equal argument-manifest and argument-BLOB counts;
- required manifests and metadata sizes;
- request-error BLOB presence where required;
- non-empty chunk URIs and safe numeric sizes; and
- absence of a duplicate allocation ID.

Validation failure returns `INVALID_ARGUMENT` and does not retain an allocation. Admission before
initialization returns `FAILED_PRECONDITION`. Duplicate IDs return `ALREADY_EXISTS`. Deadlines,
cancelled calls, and shutdown return their corresponding transport status before the runner starts.

Once registered, the runner stays addressable until a successful `delete_allocation`. Deleting an
active runner returns `FAILED_PRECONDITION`; deleting an unknown ID returns `NOT_FOUND`.

---

## State, BLOB, and request-state reconciliation

State streams receive immutable snapshots. A TypeScript stream that returns backpressure coalesces
later state publications to the newest snapshot and flushes it on `drain`. A failing or closed
stream is removed without failing the allocation.

`send_allocation_update` accepts one recognized active update. Output-BLOB responses correlate by
BLOB ID; a non-OK response without a BLOB correlates to the oldest outstanding request. State
operation results require a non-empty `operation_id`. Missing correlation fields are protocol
failures rather than permanent waits.

Request-state writes use a prepare-write response to acquire a writable BLOB, upload the JSON value,
and then commit that BLOB. Reads return `NOT_FOUND` to select the caller's default or provide a
readable BLOB for JSON deserialization.

---

## Execution and event logs

Execution-log batches are FIFO and acknowledgement-controlled. The current batch is returned
repeatedly until advanced. After completion and acknowledgement of all batches, reads return an
empty batch. Only one finish event is accepted.

The event-log read stream is executor-initiated. Each allocation has at most one outstanding read.
The current read is published to every connected stream and is republished when a stream reconnects.
Late or duplicate responses without an outstanding read are ignored.
The effective page clock is `last_clock` when present and otherwise the requested `after_clock`.
An omitted `has_more` is false, following protobuf scalar defaults.
Pages must:

- contain an array of entries;
- carry a safe, non-negative page clock;
- never move backward;
- advance when entries or `has_more` are present;
- carry one recognized payload per entry; and
- carry strictly increasing entry clocks no greater than the page clock.

Strict replay classifies malformed history as replay-history mismatch. Live protocol corruption is
an internal allocation failure. Neither path continues polling an invalid page.

---

## Transport status matrix

| Condition | Status or response |
|---|---|
| Missing or malformed allocation fields | `INVALID_ARGUMENT` |
| Allocation before initialization | `FAILED_PRECONDITION` |
| Allocation while initialization later fails | `FAILED_PRECONDITION` |
| Duplicate allocation | `ALREADY_EXISTS` |
| Unknown allocation | `NOT_FOUND` |
| Delete active allocation | `FAILED_PRECONDITION` |
| Update finished allocation | `FAILED_PRECONDITION` |
| Unsupported active allocation update | `INVALID_ARGUMENT` |
| Admission RPC cancelled | `CANCELLED` |
| Admission deadline elapsed | `DEADLINE_EXCEEDED` |
| Service stopping | `UNAVAILABLE` |
| Customer initialization error | Structured initialization failure response |

---

## Implementation layout

```text
proto/tensorlake/function_executor/proto/function_executor.proto
src/tensorlake/function_executor/
  service.py
  message_validators.py
  server.py
typescript/src/function-executor/
  service.ts
  main.ts
  allocation.ts
typescript/scripts/check-function-executor-proto.mjs
```

---

## Assumptions and open questions

**Assumptions**

- The gRPC client obeys protobuf oneof semantics.
- Output-BLOB responses are reconciled in request-list order when failure omits a BLOB ID.

**Decisions**

- *Initialization errors.* **Customer failures use a normal response.** This preserves a structured,
  customer-visible error while transport failures remain service concerns.
- *Failed initialization.* **Rollback is complete and retryable.** A bad archive cannot poison the
  next archive loaded by the process.
- *Protocol corruption.* **Fail terminally.** Silent ignore is limited to late unmatched responses;
  malformed correlated input cannot strand user code.

**Open questions**

(None at this stage.)
