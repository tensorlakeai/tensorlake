# Applications SDK — Design Overview

**Status:** Implemented · **Date:** 2026-07-25 · **Owner:** Tensorlake · **Scope:** Applications SDK

The Applications SDKs let users define orchestration applications and functions, run them locally
or remotely, and compose durable calls inside the Function Executor. Python is the established
decorator-based SDK; TypeScript provides an async, schema-aware surface with matching durable
semantics.

---

## Problem

Application authors need one programming model for local iteration, deployment, durable child
calls, fan-out, reduction, request-scoped state, and typed failures. Values and control operations
must retain their meaning when execution moves from an in-process local runner to a distributed
Function Executor.

Python and TypeScript have different reflection and serialization capabilities. The SDK contract
defines shared behavior while retaining deliberate language-specific APIs where exact syntax or
value models cannot be identical.

---

## Goals

1. Define application entrypoints and reusable functions with resource and retry configuration.
2. Preserve function-call behavior across local, remote, and deployed execution.
3. Compose durable function calls with futures, map, reduce, waits, and tail calls.
4. Expose request state, progress, metrics, request ID, and cancellation where the language runtime supports it.
5. Preserve direct File values and typed Tensorlake errors across package and deployment boundaries.
6. Generate deployment manifests that the server and Function Executor can validate.
7. Maintain shared behavioral parity through the real-executor compatibility harness.

## Non-goals

- Mixing Python and TypeScript functions in one deployed application bundle.
- Providing Python class-based function semantics in TypeScript.
- Supporting arbitrary Python pickle values in TypeScript.
- Treating nested Files as JSON values.
- Defining orchestration-server scheduling or retry execution.

---

## System shape

```text
user module
   |
   +--> definition registry --> manifest --> deployment archive
   |
   +--> local runner -------> in-process function runtime
   |
   +--> remote runner ------> application API
   |
   +--> deployed handler ---> Function Executor ---> durable protocol
```

Definitions are registered at module load. Local runners execute the same public callable through
an in-memory runtime. Deployment tooling converts definitions into manifests and bundles. The
Function Executor supplies a durable runtime implementation through request-local hooks or
async-local storage.

---

## Detail pages

| Page | Topic |
|---|---|
| [01-domain-model.md](01-domain-model.md) | Public entities and lifecycles |
| [02-definition-and-registration.md](02-definition-and-registration.md) | Definitions, validation, schemas, and manifests |
| [03-composition-and-context.md](03-composition-and-context.md) | Runtime composition and request context |
| [04-architecture-and-verification.md](04-architecture-and-verification.md) | Implementations and verification |
| [canonical-types.schema.json](canonical-types.schema.json) | Canonical public entity shapes |

---

## Scope summary

| Area | Python | TypeScript |
|---|---|---|
| Definition syntax | `@application()`, `@function()`, `@cls()` | `registerApplication`, `registerFunction` |
| Handler kind | Sync or async | Async only |
| Value model | Serializer-selected Python values and `File` | JSON values or direct `File` |
| Runtime schemas | Python type hints and serializers | Optional JSON Schema descriptors |
| Durable composition | Calls, Future, wait, map, reduce, returned-Future tail call | Calls, Future, wait, map, reduce, explicit `tailCall` |
| Request context | ID, state, progress, metrics | ID, state, progress, metrics, abort signal |
| Local and remote | Synchronous `Request.output()` | Promise-based request output; local cancellation |
| Class methods | Supported | Not supported |

---

## Assumptions and open questions

**Assumptions**

- Users import public Applications APIs from the documented package entrypoints.
- Deployment tooling validates definitions before creating server manifests.

**Decisions**

- *Parity boundary.* **Behavioral parity, not syntax identity.** The SDKs share durable and error
  semantics while respecting language reflection and serialization models.
- *TypeScript value boundary.* **JSON or direct File.** This provides deterministic validation and
  a deployment-safe representation without JavaScript object serialization.
- *Python classes.* **Python-only capability.** The TypeScript runtime does not emulate Python
  instance initialization or method descriptors.

**Open questions**

(None at this stage.)
