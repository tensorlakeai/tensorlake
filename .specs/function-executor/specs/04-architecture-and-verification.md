# Function Executor — Architecture and Verification

**Status:** Implemented · **Date:** 2026-07-25 · **Owner:** Tensorlake · **Scope:** Function Executor

This page defines implementation boundaries, observability, security assumptions, and the
verification evidence required for Function Executor changes.

---

## Architecture boundaries

```text
shared protobuf
      |
      +--------------------------+
      |                          |
Python gRPC service       TypeScript gRPC service
      |                          |
allocation runner         AllocationRunner
      |                          |
Python Applications SDK   TypeScript Applications SDK
      |                          |
customer function         customer async handler
```

The shared protobuf contains no Python or TypeScript SDK object. Language runtimes translate their
own values to serialized-object and BLOB messages at the boundary.

### Python layout

| Path | Role |
|---|---|
| `service.py` | Initialization, allocation registry, RPC implementation, runtime hooks |
| `server.py` / `main.py` | gRPC process composition and signal handling |
| `allocation_runner/allocation_runner.py` | Allocation execution and contexts |
| `allocation_runner/event_loop/` | Durable operation event loop |
| `allocation_runner/blob_manager.py` | BLOB request reconciliation and transport |
| `allocation_runner/execution_log_buffer.py` | Acknowledged execution batches |
| `allocation_runner/strict_mode_replayer.py` | Strict replay |

### TypeScript layout

| Path | Role |
|---|---|
| `service.ts` | Initialization, allocation registry, and RPC implementation |
| `main.ts` | Shared-proto loading, gRPC composition, and signals |
| `allocation.ts` | Allocation runtime, durable operations, replay, state, and cleanup |
| `blob.ts` | Serialized-object and BLOB transport |
| `safe-output.ts` | Structured output resilient to customer stream replacement |
| `user-events.ts` | Customer-visible CloudEvents |

---

## Concurrency and isolation

The client controls allocation concurrency. Each runner has independent state, waiters, logs, and
abort control. Process-global registries and hooks are installed only during successful
initialization and are restored on failure.

Customer code is untrusted. The executor receives no credentials that grant access to resources
outside the customer's authority. The surrounding platform owns process and VM isolation.

TypeScript user code can replace `process.stdout.write`, `process.stderr.write`, or global SDK
objects. Executor logs and lifecycle events retain captured output writers and use process-global
symbols for cross-bundle error, File, runtime, and request-context identity.

---

## Observability contract

Executor diagnostic logs are structured JSON on stderr and include allocation, request, function
call, application, and function fields when available. Lifecycle logs cover initialization,
admission, BLOB transport, state reconciliation, durable events, replay, terminal classification,
and shutdown.

Customer-visible events use CloudEvents-like JSON and include:

- initialization started, finished, or failed;
- allocations started and finished; and
- user function call failed.

User-provided exception text is excluded from diagnostic initialization logs where it could expose
customer data, while the structured initialization response and customer event retain an error
message.

---

## Verification layers

| Layer | Command or location | Obligation |
|---|---|---|
| TypeScript unit/protocol | `npm --prefix typescript test` | SDK, transport, replay, races, and terminal cardinality |
| TypeScript static | `typecheck`, `lint`, `check:proto` | Types, ESLint, and single proto source |
| Python unit | `tests/function_executor/`, `tests/applications/` | Python service, runner, SDK, and helpers |
| Cross-language parity | `make test_function_executor_compatibility` | Shared externally observable scenarios |
| Live server verification | `typescript/examples/server-verification/` | Scheduling, retries, deployment, and request aggregation |

The parity harness launches both real executors against the same shared Python gRPC bindings. It
drives initialization, allocation state, BLOB storage, execution batches, event pages, request
state, malformed requests, replay, and shutdown. Language-specific serialization is decoded before
comparison.

The harness covers the scenarios indexed in
`tests/function_executor_compatibility/README.md`, including value, multipart, child, Future wait,
map, reduce, tail call, handled and terminal failures, File, request context, strict mismatch,
initialization rollback, malformed admission, BLOB failures, and shutdown.

---

## Change obligations

A Function Executor protocol change is complete only when:

1. the canonical protobuf changes once under `proto/`;
2. Python bindings and TypeScript loading still resolve that source;
3. both executors define the same active behavior or the language-specific difference is specified;
4. bounded unit tests cover failure and termination paths;
5. the parity matrix covers shared behavior; and
6. live verification covers behavior owned by the orchestration server.

No validation case relies on an unbounded wait. Tests use deadlines and assert exactly one terminal
event.

---

## Assumptions and open questions

**Assumptions**

- CI supplies Python and Node dependencies required by both real executor processes.
- Live verification targets a server version compatible with protocol `0.1.3`.

**Decisions**

- *Verification split.* **Local parity plus live orchestration checks.** Local tests control every
  protocol edge; server checks cover scheduling and aggregation that the harness does not own.
- *Logging.* **Captured structured writers.** Executor diagnostics remain available when customer
  code mutates normal process output APIs.
- *Cross-language comparison.* **Normalize serialization, compare behavior.** Python pickle and
  TypeScript JSON differ internally while terminal semantics remain comparable.

**Open questions**

(None at this stage.)
