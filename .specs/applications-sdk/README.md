# Applications SDK Specifications

**Status:** Implemented · **Date:** 2026-07-25 · **Owner:** Tensorlake · **Scope:** Applications SDK

Read the [global specification index](../README.md) first. The files below define shared
Applications semantics and the Python and TypeScript public surfaces.

## Specification set

| File | Topic |
|---|---|
| [00-overview.md](specs/00-overview.md) | Goals, system shape, and language scope |
| [01-domain-model.md](specs/01-domain-model.md) | Applications, functions, futures, requests, context, and errors |
| [02-definition-and-registration.md](specs/02-definition-and-registration.md) | Decorators, registration, schemas, resources, and manifests |
| [03-composition-and-context.md](specs/03-composition-and-context.md) | Calls, Future wait, map, reduce, tail calls, File, and request context |
| [04-architecture-and-verification.md](specs/04-architecture-and-verification.md) | Local, remote, deployed runtimes and parity obligations |
| [canonical-types.schema.json](specs/canonical-types.schema.json) | Canonical SDK entity shapes |

## Assumptions and open questions

**Assumptions**

- Shared behavior is specified independently from language-specific syntax.

**Decisions**

- *Package boundary.* **Python and TypeScript use one SDK spec set.** This makes parity obligations
  visible beside intentional language differences.

**Open questions**

(None at this stage.)
