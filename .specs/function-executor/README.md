# Function Executor Specifications

**Status:** Implemented · **Date:** 2026-07-25 · **Owner:** Tensorlake · **Scope:** Function Executor

Read the [global specification index](../README.md) first. The files below define the
language-independent Function Executor contract and the Python and TypeScript implementations.

## Specification set

| File | Topic |
|---|---|
| [00-overview.md](specs/00-overview.md) | Goals, system shape, and scope |
| [01-domain-model.md](specs/01-domain-model.md) | Allocations, BLOBs, logs, and lifecycles |
| [02-protocol-and-service.md](specs/02-protocol-and-service.md) | gRPC contract, admission, state, and validation |
| [03-execution-and-replay.md](specs/03-execution-and-replay.md) | User-code execution, durable operations, replay, and termination |
| [04-architecture-and-verification.md](specs/04-architecture-and-verification.md) | Implementation boundaries, observability, and verification |
| [canonical-types.schema.json](specs/canonical-types.schema.json) | Canonical executor entity shapes |

## Assumptions and open questions

**Assumptions**

- The global specification index remains the entrypoint for package discovery.

**Decisions**

- *Package boundary.* **Protocol and runtime behavior share one spec set.** Both executor languages
  implement the same service even where their private event loops differ.

**Open questions**

(None at this stage.)
