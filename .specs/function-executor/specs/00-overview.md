# Function Executor — Design Overview

**Status:** Implemented · **Date:** 2026-07-25 · **Owner:** Tensorlake · **Scope:** Function Executor

The Function Executor is the per-function process that loads one deployed customer function and
runs allocations for it. Python and TypeScript executors expose the same gRPC service from the
shared protobuf definition.

---

## Problem

Application functions need an isolated runtime that can load customer code once, execute concurrent
allocations, exchange large values through BLOB storage, and suspend on durable child operations.
The orchestration server must control allocation admission, external work, state reconciliation,
event history, and lifecycle cleanup without importing a language SDK.

The executor separates those responsibilities. It runs language-specific customer code while the
server remains the authority for scheduling, retries, request aggregation, and durable event
persistence.

---

## Goals

1. Load exactly one function definition and share its process resources across allocations.
2. Validate initialization and allocation inputs before retaining runtime state.
3. Run user code until it produces one terminal success or failure event.
4. Express child calls, fan-out, reduce chains, watchers, and tail calls through the shared protocol.
5. Replay durable behavior deterministically from the allocation event log.
6. Reconcile BLOB requests, request state, progress, and metrics without blocking unrelated allocations.
7. Shut down active allocations with bounded cleanup and observable terminal results.

## Non-goals

- Scheduling allocations or function retries.
- Persisting the request event log or execution log.
- Aggregating the final application request outcome.
- Granting platform credentials to customer code.
- Mixing Python and TypeScript definitions in one deployed application archive.

---

## System shape

```text
deployment archive
       |
       v
+----------------------+       shared gRPC protocol       +----------------------+
| Function Executor    | <------------------------------> | orchestration server |
|                      |                                  |                      |
| service lifecycle    | -- execution-log batches ------> | scheduling / retry   |
| allocation runners   | <-- event-log pages ------------ | durable persistence  |
| language SDK runtime | -- state and BLOB requests ----> | state / BLOB broker  |
+----------+-----------+                                  +----------------------+
           |
           v
     customer handler
```

The service owns initialization and the allocation registry. Each allocation runner owns one user
handler execution, its protocol waiters, its state snapshots, and its terminal event. The
orchestration server consumes execution-log batches and supplies event-log and state responses.

---

## Detail pages

| Page | Topic |
|---|---|
| [01-domain-model.md](01-domain-model.md) | Entities, identifiers, relationships, and state machines |
| [02-protocol-and-service.md](02-protocol-and-service.md) | RPC semantics and validation |
| [03-execution-and-replay.md](03-execution-and-replay.md) | Allocation execution and replay |
| [04-architecture-and-verification.md](04-architecture-and-verification.md) | Code layout, observability, and tests |
| [canonical-types.schema.json](canonical-types.schema.json) | Semantic wire entity shapes |

---

## Scope summary

| Area | Implementation | Notes |
|---|---|---|
| Protocol source | `proto/tensorlake/function_executor/proto/` | Language-specific `.proto` copies are rejected |
| Python executor | `src/tensorlake/function_executor/` | Supports Python SDK functions and classes |
| TypeScript executor | `typescript/src/function-executor/` | Requires Node.js 24 or newer |
| Durable protocol | Execution-log output plus event-log input | Protocol version `0.1.3` |
| Replay | Strict and non-replay execution | Adaptive replay is present in the enum but not an executor contract |
| Verification | Unit, protocol, parity, and live-server checks | Parity normalizes language serialization |

---

## Assumptions and open questions

**Assumptions**

- The executor process runs inside an isolation boundary supplied by the surrounding platform.
- The orchestration server preserves execution-log batches until they are acknowledged.

**Decisions**

- *Process ownership.* **One initialized function per executor process.** This permits loaded models
  and other customer resources to be shared without cross-function registry ambiguity.
- *Orchestration split.* **The server owns scheduling and durable persistence.** The executor emits
  intent and waits for authoritative protocol events.
- *Protocol source.* **One repository-level protobuf definition.** Both languages compile or load
  the same messages and RPC service.

**Open questions**

(None at this stage.)
