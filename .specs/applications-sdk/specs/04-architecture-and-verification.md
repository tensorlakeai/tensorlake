# Applications SDK — Architecture and Verification

**Status:** Implemented · **Date:** 2026-07-25 · **Owner:** Tensorlake · **Scope:** Applications SDK

This page defines the local, remote, and deployed adapters for each language and the verification
required to keep their shared behavior aligned.

---

## Architecture

```text
                         public definition
                          /      |       \
                         v       v        v
                   local run   manifest  remote request
                       |          |          |
                       v          v          v
                 local runtime  deploy CLI  cloud API
                       |
                       +------------------+
                                          |
                                          v
                                  Function Executor runtime
```

Public definition objects are independent of an execution adapter. Runtime hooks select local
execution or durable Function Executor behavior. Remote invocation addresses an application API and
returns a request handle.

---

## Python layout

| Path | Role |
|---|---|
| `applications/interface/` | Public decorators, Function, Future, Request, context, File, errors |
| `applications/local/` | Local runner, Future execution, state, and values |
| `applications/remote/` | API client, deployment, manifests, code archive, and remote requests |
| `applications/function/` | Function-call metadata, introspection, serializers, type hints |
| `applications/runtime_hooks.py` | Function Executor runtime integration |
| `applications/validation/` | Pre-deployment validation |

Python's interface package imports only public interface modules and `Image`. Internal helpers use
leading underscores when circular dependency avoidance requires co-location.

---

## TypeScript layout

| Path | Role |
|---|---|
| `applications/index.ts` | Public export surface |
| `applications/function.ts` | Definitions, Future, composition, runtime hooks, Promise instrumentation |
| `applications/schema.ts` | JSON Schema and direct File descriptors |
| `applications/context.ts` | Context interfaces, validation, and in-memory implementation |
| `applications/local.ts` | Local runtime and cancellable request |
| `applications/remote.ts` | Cloud request adapter and explicit remote options |
| `applications/manifest.ts` | Deployment and code manifests |
| `applications/registry.ts` | Definition registry and rollback snapshots |

Process-global symbols connect the executor's SDK copy with the deployed application's SDK copy
without making private runtime objects public.

---

## Public export inventory

Python exports its supported interface through `tensorlake.applications`: `application`, `function`,
`cls`, `Function`, `Future`, `RETURN_WHEN`, `Retries`, `File`, `Image`, `Request`,
`RequestContext`, `RequestState`, `FunctionProgress`, `Logger`, local and remote application
runners, and the public Tensorlake exception hierarchy. `RequestNotFinished`, `RequestFailed`, and
`RemoteAPIError` describe remote request lifecycle and transport failures.

TypeScript exports registration and retry helpers, `Future`, registration option and result types,
the schema builder and schema types, `File`, `Image`, `SDK_VERSION`, `RequestContext`, local and
remote request adapters, remote options, the public error hierarchy, deployment manifest helpers
and types, and read-only registry lookup functions. Registry access and manifest construction are
introspection/deployment APIs; they do not execute a handler.

Symbols intentionally exported from internal source modules but omitted from each language's public
package entrypoint are implementation details and carry no compatibility guarantee.

---

## Local and remote behavior

Local execution applies registration validation, JSON or serializer boundaries, retry rules,
request context, and typed failures. It is an execution adapter, not a protocol emulator.

Remote execution submits the application name and serialized arguments through the cloud client.
TypeScript `remoteOptions(...)` disambiguates omitted JavaScript default arguments from a final
object-valued input. Python uses its API client context and synchronous Request interface.

Deployment creates one-language archives. The CLI bundles TypeScript ESM for Node.js 24 and embeds
the executor capsule resolved from the exact SDK package. CommonJS entrypoints and mixed-language
bundles are unsupported.

---

## Shared parity contract

The shared contract includes:

- JSON and multipart argument/result boundaries;
- durable child calls and typed child failures;
- Future wait cutoffs;
- map ordering and fan-out;
- reduce semantics and dependency chains;
- tail calls;
- File bytes and content type;
- request error payloads;
- request state, progress, and metrics validation;
- strict replay mismatch;
- initialization rollback, malformed admission, BLOB failure, and shutdown.

The parity harness fixture uses concise TypeScript JSON registrations wherever File schemas are not
required. This ensures inferred descriptors exercise the same executor behavior as Python
definitions.

---

## Intentional language differences

| Capability | Reason |
|---|---|
| Python sync handlers and blocking result access | Native Python application model |
| Python classes and instance initialization | Python descriptor and class semantics |
| Python serializer-selected arbitrary values | Existing Python type-hint and pickle support |
| TypeScript Promise-like map/reduce inputs | Native JavaScript asynchronous collection composition |
| TypeScript JSON Schema validation | JavaScript lacks runtime parameter type hints |
| TypeScript async-only handlers | Durable Promise causality and Node runtime model |
| TypeScript explicit tail-call control value | A returned Promise cannot safely double as a control value |
| TypeScript public cancellation signal | AbortSignal is the Node cancellation primitive |

These differences are specified and are not parity failures unless they alter a shared scenario.

---

## Verification obligations

| Change | Required evidence |
|---|---|
| Public API or defaults | Language unit tests and manifest assertions |
| Serialization or File | Local boundary tests plus real-executor parity |
| Future, map, reduce, wait, tail call | Local tests, protocol tests, strict replay, parity |
| Request context | Local and deployed validation tests plus parity |
| Cross-bundle identity | Separate SDK-copy tests |
| Deployment manifest | Manifest unit tests and server verification |
| Shared semantics | Compatibility harness scenario in both fixtures |

The compatibility matrix and run instructions live in
`tests/function_executor_compatibility/README.md`. TypeScript server verification lives in
`typescript/examples/server-verification/`.

---

## Assumptions and open questions

**Assumptions**

- The CLI bundles the same TypeScript SDK package whose manifests it reads.
- Python and TypeScript tests run against the repository's shared protobuf source.

**Decisions**

- *Adapter boundary.* **One public definition, multiple execution adapters.** Local and deployed
  calls share validation and composition semantics.
- *Cross-bundle state.* **Process-global symbols for private identity.** Separate SDK copies can
  exchange branded values and contexts without exposing executor internals.
- *Parity evidence.* **Real executors, not mocked SDK calls.** The harness validates the complete
  boundary where most cross-language defects occur.

**Open questions**

(None at this stage.)
