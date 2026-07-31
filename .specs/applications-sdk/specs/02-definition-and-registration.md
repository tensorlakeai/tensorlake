# Applications SDK — Definition and Registration

**Status:** Implemented · **Date:** 2026-07-25 · **Owner:** Tensorlake · **Scope:** Applications SDK

This page specifies how user handlers become application and function definitions, how configuration
is normalized, and how deployment manifests preserve the definition contract.

---

## Responsibilities

1. Attach stable names and application/function roles to handlers.
2. Normalize resource, retry, region, image, secret, and container settings.
3. Validate language-specific handler and value descriptors.
4. Register definitions without ambiguous duplicates.
5. Generate deployment manifests and code manifests from registration snapshots.

Registration does not schedule or execute server allocations.

---

## Python definition API

```python
@application(tags={}, retries=Retries(), region=None)
@function(
    description="",
    cpu=1.0,
    memory=1.0,
    ephemeral_disk=10.0,
    gpu=None,
    timeout=300,
    image=None,
    secrets=[],
    retries=None,
    region=None,
    warm_containers=None,
    min_containers=None,
    max_containers=None,
)
def handler(...): ...
```

`@application()` marks a Function as an entrypoint and assigns tags, default retries, region, and a
generated alphanumeric version. `@function()` assigns function metadata and resources. A function
retry setting overrides the application retry policy.

`@cls(init_timeout=...)` records Python class initialization metadata, replaces normal construction
with an empty constructor for call sites, and lets the executor create one real instance for the
loaded function process. Registered method names use their qualified class and method name.

Python preserves handler reflection metadata so frameworks that inspect functions, annotations,
docs, and coroutine status continue to recognize wrapped functions. Definition consistency and
type-hint/serializer compatibility are validated before deployment.

---

## TypeScript definition API

### Concise registration

```ts
registerFunction("stable_name", async (...args) => result, options?)
registerApplication("stable_name", async (...args) => result, options?)
```

Concise registration infers permissive JSON parameter schemas from the handler declaration and uses
`schema.json()` for the return. JavaScript default parameters become optional descriptors. Rest
parameters, native or bound functions, ambiguous source forms, and missing stable names require the
explicit-schema form.

### Explicit-schema registration

```ts
registerFunction(async (...args) => result, {
  name,
  parameters: [schema.parameter(...)],
  returns: schema.number(),
  ...resources,
})
```

Applications accept the same function settings plus `tags` and `applicationRetries`. Both
`parameters` and `returns` must be supplied together. Duplicate parameter names are rejected.
Handlers must return a Promise.

The legacy explicit signatures remain supported while concise registration reduces configuration
for JSON-only handlers.

---

## Resource and retry configuration

| Setting | Default | Validation |
|---|---:|---|
| CPU | 1 | finite and greater than zero |
| Memory GB | 1 | finite and greater than zero |
| Ephemeral disk GB | 10 | finite and greater than zero |
| Timeout seconds | 300 | integer from 1 through 86400 |
| GPU | none | model or model list; model strings are non-empty and optional `:COUNT` is positive |
| Secrets | empty | non-empty strings |
| Retry max | 0 at application | integer from 0 through 10 |
| Region | none | `us-east-1` or `eu-west-1` |
| Warm/min/max containers | unset | non-negative integers; minimum and warm do not exceed maximum |

Python validation occurs in its deployment validation layer and reports all configuration errors
with their function source details. TypeScript validates immediately, then normalizes and snapshots
mutable options at registration so later caller mutation cannot change execution or manifests.
Application tags require non-empty string keys and string values.

---

## TypeScript schema contract

The schema builder supports JSON, string, number, integer, boolean, null, literals, enums, arrays,
tuples, records, objects, unions, nullable values, custom JSON Schema, direct File, and named
parameters.

Schemas and defaults are deep snapshots. Cycles, sparse arrays, non-finite numbers, symbols,
functions, unsupported prototypes, invalid custom schemas, and schema/value mismatches raise
`SDKUsageError`.

Generated schemas use JSON Schema 2020-12. Custom schemas that explicitly identify draft-07 use a
draft-07 validator. File schemas are valid only as direct parameter or return descriptors and
cannot be nested in JSON schema composition.

---

## Registry and manifest contract

Each definition registry distinguishes functions from application entrypoints. Duplicate names are
rejected. Snapshot and restore operations isolate failed module discovery and executor
initialization attempts.

TypeScript manifests carry:

- descriptor format `tensorlake.typescript.json-schema.v1`;
- function parameter and return descriptors;
- normalized resource and retry settings;
- application tags, retries, version, and function membership; and
- a code manifest with format version 2, runtime `typescript`, minimum Node major, module path, and
  registered function names.

Deployment discovery imports the generated bundle and rejects functions or applications whose runtime
definition names do not match the manifest request.

---

## Implementation layout

```text
src/tensorlake/applications/interface/
  decorators.py
  function.py
  retries.py
src/tensorlake/applications/validation/
src/tensorlake/applications/remote/manifests/
typescript/src/applications/
  function.ts
  schema.ts
  registry.ts
  manifest.ts
```

---

## Assumptions and open questions

**Assumptions**

- Bundlers can rewrite JavaScript function names, so concise TypeScript registration names are
  supplied by the user.
- Python deployment validation runs before the code archive is accepted by the server.

**Decisions**

- *Registration defaults.* **Concise JSON registration plus explicit schema registration.** Common
  handlers require a stable name and code; richer API metadata remains opt-in.
- *Configuration mutation.* **Registration snapshots mutable options.** Execution and deployment
  read the same immutable definition.
- *Schema dialect.* **2020-12 by default with explicit draft-07 compatibility.** Tuple validation
  matches emitted `prefixItems` while existing custom schemas remain valid.

**Open questions**

(None at this stage.)
