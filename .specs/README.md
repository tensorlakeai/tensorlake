# Tensorlake Canonical Specifications

**Status:** Implemented · **Date:** 2026-07-25 · **Owner:** Tensorlake · **Scope:** Repo-wide

This directory indexes the canonical design specifications for the Function Executor and
Applications SDKs. The specifications describe the behavior implemented in the current branch.

## Global specifications

| File | Purpose |
|---|---|
| [canonical-types.schema.json](canonical-types.schema.json) | Shared identifier and digest shapes |

## Per-package specifications

| Package | File | Topic |
|---|---|---|
| Function Executor | [index](function-executor/README.md) | Package reading order |
| Function Executor | [overview](function-executor/specs/00-overview.md) | Goals, system shape, and scope |
| Function Executor | [domain model](function-executor/specs/01-domain-model.md) | Allocations, BLOBs, logs, and lifecycle |
| Function Executor | [protocol and service](function-executor/specs/02-protocol-and-service.md) | RPC and validation contract |
| Function Executor | [execution and replay](function-executor/specs/03-execution-and-replay.md) | Durable runtime and termination |
| Function Executor | [architecture and verification](function-executor/specs/04-architecture-and-verification.md) | Layout, observability, and evidence |
| Function Executor | [canonical types](function-executor/specs/canonical-types.schema.json) | Executor semantic shapes |
| Applications SDK | [index](applications-sdk/README.md) | Package reading order |
| Applications SDK | [overview](applications-sdk/specs/00-overview.md) | Goals, system shape, and language scope |
| Applications SDK | [domain model](applications-sdk/specs/01-domain-model.md) | Public entities and lifecycle |
| Applications SDK | [definition and registration](applications-sdk/specs/02-definition-and-registration.md) | Definition and manifest contract |
| Applications SDK | [composition and context](applications-sdk/specs/03-composition-and-context.md) | Runtime operations and request context |
| Applications SDK | [architecture and verification](applications-sdk/specs/04-architecture-and-verification.md) | Adapters, parity, and evidence |
| Applications SDK | [canonical types](applications-sdk/specs/canonical-types.schema.json) | SDK semantic shapes |

## Reviews

Conformance reviews are recorded under `reviews/` when a canonical spec set is checked against
the implementation.

| File | Purpose |
|---|---|
| [Function Executor and Applications SDK conformance review](reviews/2026-07-25-function-executor-applications-sdk-conformance.md) | Bidirectional R2 review, remediations, and verification evidence |

## Assumptions and open questions

**Assumptions**

- The canonical specification is revised with any public or protocol behavior change.

**Decisions**

- *Specification layering.* **Shared types are global; executor and SDK behavior is per-package.**
  This keeps cross-language identifiers consistent without mixing two distinct lifecycles.

**Open questions**

(None at this stage.)
