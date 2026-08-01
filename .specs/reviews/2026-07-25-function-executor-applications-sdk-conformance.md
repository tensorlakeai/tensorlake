# Function Executor and Applications SDK Conformance Review

**Status:** Implemented · **Date:** 2026-07-25 · **Owner:** Tensorlake · **Scope:** Function Executor and Applications SDKs

This review applies the canonical-spec-versus-code R2 procedure to both package specifications
after remediation. Line anchors identify the reviewed implementation; verification evidence records
the checks run against the final source state.

## Premises

P1: The spec pages `.specs/function-executor/specs/00-overview.md` through `04-architecture-and-verification.md` and `.specs/applications-sdk/specs/00-overview.md` through `04-architecture-and-verification.md` claim the concrete executor protocol, lifecycle, validation, execution, replay, SDK definition, registration, composition, context, adapter, error, and parity contracts listed below.
P2: The implementing code lives in `proto/tensorlake/function_executor/proto/`, `src/tensorlake/function_executor/`, `src/tensorlake/applications/`, `typescript/src/function-executor/`, `typescript/src/applications/`, and their tests.
P3: The spec rule under test: the body describes only what exists in the current branch (missing code = divergence, not "deferred").

## Claim resolution — forward pass

### Function Executor

CLAIM 1: Python and TypeScript use one repository-level protobuf definition and protocol version
`0.1.3`.

1. Is the named protocol source present? Yes: the service and messages are defined at
   `proto/tensorlake/function_executor/proto/function_executor.proto:303` and `:570`.
2. Shape comparison: Python imports the generated module from
   `src/tensorlake/function_executor/proto/`; TypeScript locates the shared source at
   `typescript/src/function-executor/main.ts:48` and reports `0.1.3` at
   `typescript/src/function-executor/service.ts:690`.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 2: One successful initialization binds an executor process to one complete function
reference; failed initialization rolls back and remains retryable.

1. Is there a matching lifecycle in the named services? Yes:
   `src/tensorlake/function_executor/service.py:183` and
   `typescript/src/function-executor/service.ts:270`.
2. Shape comparison: both serialize attempts, reject replacement after success, restore registry
   state on failure, and clear partial service/definition state.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 3: TypeScript initialization validates a format-version-2 Node.js application bundle,
integrity, requested definitions, and path safety before publishing initialized state.

1. Is there a matching loader? Yes: `typescript/src/function-executor/service.ts:126` and `:282`.
2. Shape comparison: safe paths, exact size, metadata size, SHA-256, runtime, Node major, module,
   function, and application definition are checked before assignments at `:387`.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 4: Python initialization imports a ZIP module, resolves its registry entry, creates
process-scoped class state where applicable, and removes failed-import module and registry state.

1. Is there a matching loader? Yes: `src/tensorlake/function_executor/service.py:183`.
2. Shape comparison: archive-loaded modules are identified and restored by
   `src/tensorlake/function_executor/service.py:104`; runtime hooks and partial services are
   rolled back by the initialization failure path.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 5: Allocation admission validates required non-empty IDs, inputs, manifest/BLOB pairing,
metadata ranges, non-empty chunk URIs, safe numeric sizes, request-error storage, and duplicate IDs.

1. Is there a validator in the named modules? Yes:
   `src/tensorlake/function_executor/message_validators.py`,
   `src/tensorlake/function_executor/proto/message_validator.py:14`, and
   `typescript/src/function-executor/service.ts:42`.
2. Shape comparison: the Python and TypeScript validators enforce the same safe-integer and
   required-field rules before registry insertion.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 6: Admission waits across an in-progress initialization and rechecks cancellation, deadline,
shutdown, and final initialization state.

1. Is there a matching admission guard? Yes:
   `src/tensorlake/function_executor/service.py:471` and
   `typescript/src/function-executor/service.ts:433`.
2. Shape comparison: Python uses a condition with deadline/cancellation polling; TypeScript races
   the initialization promise against the RPC lifecycle and then rechecks admission.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 7: The service implements initialize, list, create, state watch, update, delete, execution-log,
event-log, health, and info RPCs with the specified status classes.

1. Is the RPC surface defined and implemented? Yes:
   `proto/tensorlake/function_executor/proto/function_executor.proto:570`,
   `src/tensorlake/function_executor/service.py:183`, and
   `typescript/src/function-executor/service.ts:162`.
2. Shape comparison: unknown IDs, duplicate IDs, active deletion, finished updates, malformed
   updates, inactive admission, and stopping state map to the stated gRPC classes.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 8: Allocation state is a complete level-triggered snapshot with progress, outstanding BLOB
and request-state operations, and a content hash.

1. Is there a matching state object? Yes:
   `src/tensorlake/function_executor/allocation_runner/allocation_state_wrapper.py` and
   `typescript/src/function-executor/allocation.ts:1088`.
2. Shape comparison: both publish complete current work; TypeScript coalesces backpressured stream
   writes and removes closed streams.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 9: Output-BLOB requests correlate by ID, BLOB-less failures correlate to the oldest request,
and all matched success or failure responses settle their waiter.

1. Is there a matching BLOB manager? Yes:
   `src/tensorlake/function_executor/allocation_runner/blob_manager.py:28` and
   `typescript/src/function-executor/allocation.ts:2359`.
2. Shape comparison: insertion order supplies fallback correlation, request maps are cleared before
   wakeup, and response errors reject rather than wait permanently.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 10: BLOB downloads validate declared ranges, sizes, metadata ranges, and digests; uploads stay
within supplied chunk capacity and honor cancellation.

1. Is there a matching transport boundary? Yes:
   `src/tensorlake/applications/blob_store.py`,
   `src/tensorlake/function_executor/allocation_runner/allocation_runner.py:79`, and
   `typescript/src/function-executor/blob.ts:223`.
2. Shape comparison: manifests and hashes are checked before deserialization, upload slices are
   bounded by chunk capacity, and TypeScript passes the allocation abort signal to transport I/O.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 11: Request-state reads and prepare-write/commit-write operations correlate by operation ID;
missing reads select the default and malformed correlation cannot strand user code.

1. Is there a matching request-state protocol? Yes:
   `src/tensorlake/function_executor/allocation_runner/request_state.py` and
   `typescript/src/function-executor/allocation.ts:2003`.
2. Shape comparison: both publish operation snapshots, require correlation IDs, upload JSON writes,
   and treat `NOT_FOUND` reads as default selection.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 12: Execution-log batches are acknowledged FIFO entries, repeat until advanced, accept one
terminal event, and end with an empty batch after terminal acknowledgement.

1. Is there a matching execution buffer? Yes:
   `src/tensorlake/function_executor/allocation_runner/execution_log_buffer.py:7` and
   `typescript/src/function-executor/allocation.ts:1088`.
2. Shape comparison: both retain the head until advance, latch terminal insertion, and unblock
   terminal consumers.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 13: Each allocation has at most one pending event-log read; the request is visible to
connected and replacement streams; late duplicate responses are ignored.

1. Is there a matching reader? Yes:
   `src/tensorlake/function_executor/allocation_runner/event_log_reader.py:112` and
   `typescript/src/function-executor/allocation.ts:2291`.
2. Shape comparison: both retain a current request, republish it on stream connection, settle it
   once, and ignore responses when no read remains.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 14: Event-log pages use a safe non-negative effective clock, never move backward, advance
when they contain entries or `has_more`, and contain exactly one recognized payload with strictly
increasing entry clocks.

1. Is there a matching validator? Yes:
   `src/tensorlake/function_executor/allocation_runner/event_log_reader.py:33` and
   `typescript/src/function-executor/allocation.ts:75`.
2. Shape comparison: omitted `last_clock` uses the requested clock, omitted `has_more` is false,
   and entry clocks must not exceed the page clock.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 15: Malformed event history is a replay-history mismatch in strict replay and an internal
terminal failure during live execution.

1. Is there matching classification? Yes:
   `src/tensorlake/function_executor/allocation_runner/strict_mode_replayer.py:167`,
   `src/tensorlake/function_executor/allocation_runner/allocation_runner.py:237`, and
   `typescript/src/function-executor/allocation.ts:1924`.
2. Shape comparison: neither language resumes polling after an invalid page.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 16: User handlers execute inside allocation/request contexts; ordinary values and direct
Files cross validated serialization boundaries.

1. Is there a matching execution boundary? Yes:
   `src/tensorlake/function_executor/allocation_runner/event_loop/event_loop.py` and
   `typescript/src/function-executor/allocation.ts:1775`.
2. Shape comparison: both install runtime hooks/context, deserialize before invocation, validate
   handler output, and serialize the terminal value or File.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 17: Durable child calls emit a call followed by a watcher; fan-out assigns IDs in input order
and waits independently; retries remain server-owned.

1. Is there a matching durable runtime? Yes:
   `src/tensorlake/function_executor/allocation_runner/event_loop/event_loop.py` and
   `typescript/src/function-executor/allocation.ts:1337`.
2. Shape comparison: emitted child metadata includes target, arguments, delay, and retries; watcher
   results expose eventual server success or terminal failure.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 18: Deployed reduce emits left-associated dependency chains, splits large TypeScript plans at
512 calls, watches only the final call, and replays at plan boundaries.

1. Is there a matching reduce implementation? Yes:
   Python reducer splitters are in
   `src/tensorlake/applications/local/future_run/function_call_future_run.py:181`; TypeScript is at
   `typescript/src/function-executor/allocation.ts:1264`.
2. Shape comparison: TypeScript uses `MAX_REDUCE_CALLS_PER_PLAN = 512`, carries prior durable IDs as
   arguments, emits plan boundaries, and watches the final ID.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 19: Tail calls emit a child call without a watcher and finish with
`tail_call_durable_id`.

1. Is there a matching tail path? Yes:
   `src/tensorlake/function_executor/allocation_runner/event_loop/` and
   `typescript/src/function-executor/allocation.ts:1740`.
2. Shape comparison: terminal conversion records the child ID and omits a result watcher.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 20: Terminal outcomes distinguish value success, tail-call success, request error, function
error, internal error, and replay mismatch and queue exactly one finish event.

1. Is there a matching outcome converter? Yes:
   `src/tensorlake/function_executor/allocation_runner/finish_event_helper.py`,
   `src/tensorlake/function_executor/allocation_runner/execution_log_buffer.py:21`, and
   `typescript/src/function-executor/allocation.ts:1924`.
2. Shape comparison: normal errors, typed request errors, protocol failures, replay failures, and
   cancellation use the specified failure reasons; terminal latches prevent duplicates.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 21: Strict replay matches ordered call/watcher creation, handles watcher results without
changing their recorded visibility, detects blocked causal boundaries, and releases replay state.

1. Is there a matching replayer? Yes:
   `src/tensorlake/function_executor/allocation_runner/strict_mode_replayer.py:124` and
   `typescript/src/function-executor/allocation.ts` (`ReplayHistory` and causal tracking).
2. Shape comparison: ordered events, unordered result availability, mismatch classification,
   Promise combinators, weak promise references, and cleanup are covered by protocol tests.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 22: Cancellation settles BLOB, state, event-log, and durable waiters, leaves one terminal
batch, and provides bounded process shutdown.

1. Is there matching cleanup? Yes:
   `src/tensorlake/function_executor/service.py:155`,
   `src/tensorlake/function_executor/server.py:12`,
   `typescript/src/function-executor/allocation.ts:1184`, and
   `typescript/src/function-executor/main.ts:16`.
2. Shape comparison: Python queues the terminal result and retains a four-second forced exit;
   TypeScript aborts/awaits allocation runners and retains a ten-second forced exit. Both provide a
   one-second terminal-observation window.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 23: Lifecycle and failure logs carry runtime and allocation correlation fields and remain
writable if user code replaces process output writers.

1. Is there matching observability? Yes:
   `src/tensorlake/function_executor/user_events.py`,
   `typescript/src/function-executor/user-events.ts`, and
   `typescript/src/function-executor/safe-output.ts`.
2. Shape comparison: initialization, allocation, user failure, shutdown, BLOB, state, replay, and
   protocol events include the available reference and ID fields.

→ FOUND; shape matches the spec: CONFORMS.

### Applications SDKs

CLAIM 24: Python exposes `@application`, `@function`, `@cls`, Function/Future, context, File,
request adapters, Image, Retries, logging, and the public exception hierarchy.

1. Is the public entrypoint present? Yes:
   `src/tensorlake/applications/interface/__init__.py:4`.
2. Shape comparison: `__all__` at `:37` includes every inventory item described by the spec.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 25: Python decorators preserve callable metadata, register sync/async functions and class
methods, and carry defaults/resources/retries into definitions for deployment validation.

1. Is there a matching definition API? Yes:
   `src/tensorlake/applications/interface/decorators.py:86`, `:180`, and `:264`.
2. Shape comparison: function defaults match the table; class decoration retains the original
   initializer and qualifies method names; validation inspects source and serializers.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 26: TypeScript supports concise stable-name registration and the legacy explicit-schema
signatures for functions and applications.

1. Is there a matching overload set? Yes:
   `typescript/src/applications/function.ts:1827` and `:1878`.
2. Shape comparison: concise registration infers JSON descriptors; explicit registration requires
   parameter and return schemas; both produce the same RegisteredFunction surface.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 27: Concise TypeScript inference handles defaults and supported source forms and rejects
anonymous, native/bound, rest, or ambiguous handlers that require explicit schemas.

1. Is there a matching parser? Yes:
   `typescript/src/applications/function.ts:1453` through `:1786`.
2. Shape comparison: comments, strings, templates, regex literals, classic/arrow functions, default
   expressions, and unsupported forms are explicitly parsed or rejected.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 28: Resource defaults and validation are aligned: positive finite CPU/memory/disk, timeout
1–86400, non-empty secrets, valid GPU counts, supported regions, bounded retries, and valid
container relationships.

1. Is there a matching validator? Yes:
   `src/tensorlake/applications/validation/validate.py:418` and
   `typescript/src/applications/function.ts:1198`.
2. Shape comparison: Python emits deployment validation messages; TypeScript raises
   `SDKUsageError` at registration. Both enforce retry maximum 10 and the stated defaults.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 29: TypeScript registration snapshots mutable options, schemas, and defaults; malformed
runtime registration shapes raise `SDKUsageError`.

1. Is there a matching snapshot/validation boundary? Yes:
   `typescript/src/applications/schema.ts:42`,
   `typescript/src/applications/function.ts:1199`, and `:1403`.
2. Shape comparison: JSON structures are recursively copied/frozen, File defaults are copied,
   arrays/maps are copied into definitions, and invalid secrets/tags/retries/explicit descriptors
   receive SDK errors.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 30: The TypeScript schema builder emits and validates JSON Schema 2020-12, preserves explicit
draft-07 custom schemas, rejects invalid/sparse/cyclic/non-JSON values, and permits File only as a
direct descriptor.

1. Is there a matching builder? Yes: `typescript/src/applications/schema.ts:42` and `:168`.
2. Shape comparison: tuple `prefixItems`, dialect selection, Ajv compilation, File-composition
   guards, and deep snapshots implement the stated contract.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 31: Definition registries distinguish functions/applications, reject duplicate names,
support rollback snapshots, and generate descriptor/application/code manifests.

1. Is there a matching registry and manifest layer? Yes:
   `typescript/src/applications/registry.ts:22` and
   `typescript/src/applications/manifest.ts:139`.
2. Shape comparison: registry snapshots isolate failed imports; manifests carry format, resources,
   retries, tags, membership, Node version, and registered names.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 32: Calls choose local or deployed runtime hooks and cross a serialization boundary for
arguments and results.

1. Is there a matching adapter selection? Yes:
   `src/tensorlake/applications/runtime_hooks.py`,
   `src/tensorlake/applications/local/runner.py`,
   `typescript/src/applications/function.ts:1321`, and
   `typescript/src/applications/local.ts:40`.
2. Shape comparison: local adapters make boundary copies and deployed hooks emit durable protocol
   work.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 33: Future start, delayed start, result, completion, wait modes, cutoffs, and validation agree
across the languages, including non-mutating failure of invalid controls.

1. Is there a matching Future? Yes:
   `src/tensorlake/applications/interface/futures.py:53` and
   `typescript/src/applications/function.ts:248`.
2. Shape comparison: invalid delay/timeout/mode validation happens before start, Python iterable
   input is materialized once, first-failure waits through prior successes, and done/not-done
   cutoffs are deterministic.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 34: Map resolves collection/items before durable identity, fans out all child calls, preserves
input order, and handles empty input without calls.

1. Is there a matching composition API? Yes:
   `src/tensorlake/applications/interface/function.py:125` and
   `typescript/src/applications/function.ts:1333`.
2. Shape comparison: Python accepts its Future/coroutine wrappers; TypeScript additionally accepts
   Promise-like collections/items; both preserve result order.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 35: Reduce supports optional initial values and the specified empty-input behavior, runs
sequentially locally, and delegates a dependency chain when deployed.

1. Is there a matching reduce API? Yes:
   `src/tensorlake/applications/interface/function.py:147`,
   `src/tensorlake/applications/local/future_run/function_call_future_run.py:190`, and
   `typescript/src/applications/function.ts:1350`.
2. Shape comparison: both languages distinguish omitted initial from an explicit value, apply every
   item when initial exists, and reject empty input only when initial is absent.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 36: Local retries use function overrides or application defaults, do not retry request errors,
and start every attempt with a fresh argument boundary copy.

1. Is there a matching retry loop? Yes:
   `src/tensorlake/applications/local/future_run/function_call_future_run.py:90` and
   `typescript/src/applications/local.ts:105`.
2. Shape comparison: both retain pristine inputs, copy them per attempt, stop on typed request
   errors, and apply the normalized retry count.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 37: Request state, progress, and metrics validate before publication and local state preserves
the deployed JSON boundary.

1. Is there matching context validation? Yes:
   `src/tensorlake/applications/request_context/`,
   `src/tensorlake/applications/interface/request_context.py:7`, and
   `typescript/src/applications/context.ts:24`.
2. Shape comparison: state keys, progress numbers/messages/attributes, counter integers, timer
   finiteness, and TypeScript safe integers are checked before mutation or transport.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 38: TypeScript local cancellation uses the request AbortSignal; Python exposes no public
cancellation signal.

1. Is there a matching language-specific context? Yes:
   `typescript/src/applications/context.ts:120`,
   `typescript/src/applications/local.ts:24`, and
   `src/tensorlake/applications/interface/request_context.py:77`.
2. Shape comparison: TypeScript cancellation aborts output and handler waits; the Python interface
   has state/progress/metrics without an AbortSignal API.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 39: File retains exact bytes/content type, requires a direct TypeScript File schema, and
cross-bundle brands preserve the specified TypeScript identities.

1. Is there a matching File/error boundary? Yes:
   `src/tensorlake/applications/interface/file.py:1`,
   `typescript/src/applications/file.ts`, and
   `typescript/src/applications/errors.ts:1`.
2. Shape comparison: File is copied at boundaries; nested/schema-less File is rejected; the five
   listed cross-bundle identities use `Symbol.for` brands and `Symbol.hasInstance`.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 40: Local and remote adapters expose typed request handles, and TypeScript remote options
disambiguate omitted defaults from object-valued application inputs.

1. Is there a matching adapter layer? Yes:
   `src/tensorlake/applications/interface/run.py`,
   `typescript/src/applications/local.ts:152`, and
   `typescript/src/applications/remote.ts:23`.
2. Shape comparison: local and remote request output types, cloud failure mapping, File/JSON output
   decoding, and branded `remoteOptions` match the spec.

→ FOUND; shape matches the spec: CONFORMS.

CLAIM 41: Shared behavior is exercised through real Python and TypeScript executors for value,
child, wait, map, reduce, tail, File, request error, state, progress, metrics, replay,
initialization, malformed admission/event pages, BLOB failure, and shutdown.

1. Is there a matching harness? Yes: `tests/function_executor_compatibility/run.py:161` and
   `tests/function_executor_compatibility/README.md`.
2. Shape comparison: the runner starts both executors against one driver, normalizes serialization,
   compares 26 scenarios, and runs lifecycle/failure probes outside the scenario table.

→ FOUND; shape matches the spec: CONFORMS.

## Coverage resolution — reverse pass

### Function Executor symbols

SYMBOL `proto/tensorlake/function_executor/proto/function_executor.proto:570`:
`FunctionExecutor` RPC service
→ described at Function Executor `02-protocol-and-service.md` → RPC surface: SPEC'D.

SYMBOL `src/tensorlake/function_executor/service.py:130`: `Service`
→ described at Function Executor `00-overview.md` → System shape and
`02-protocol-and-service.md` → Responsibilities: SPEC'D.

SYMBOL `typescript/src/function-executor/service.ts:153`: `FunctionExecutorService`
→ described at Function Executor `02-protocol-and-service.md` → Initialization contract and RPC
surface: SPEC'D.

SYMBOL `src/tensorlake/function_executor/allocation_runner/allocation_runner.py:79` and
`typescript/src/function-executor/allocation.ts:1088`: `AllocationRunner`
→ described at Function Executor `03-execution-and-replay.md` → Execution flow: SPEC'D.

SYMBOL `src/tensorlake/function_executor/allocation_runner/event_log_reader.py:33`:
`validate_event_log_response`
→ described at Function Executor `02-protocol-and-service.md` → Execution and event logs: SPEC'D.

SYMBOL `src/tensorlake/function_executor/allocation_runner/strict_mode_replayer.py:124`:
`AllocationStrictModeReplayer`
→ described at Function Executor `03-execution-and-replay.md` → Strict replay: SPEC'D.

SYMBOL `src/tensorlake/function_executor/allocation_runner/blob_manager.py:28`:
`AllocationBLOBManager`
→ described at Function Executor `02-protocol-and-service.md` → State, BLOB, and request-state
reconciliation: SPEC'D.

SYMBOL `src/tensorlake/function_executor/allocation_runner/execution_log_buffer.py:7`:
`ExecutionLogBuffer`
→ described at Function Executor `01-domain-model.md` → Execution event and batch: SPEC'D.

SYMBOL `src/tensorlake/function_executor/proto/message_validator.py:14`: `MessageValidator`
→ described at Function Executor `02-protocol-and-service.md` → Allocation admission and ownership:
SPEC'D.

SYMBOL `src/tensorlake/function_executor/server.py:20`: `Server` and
`typescript/src/function-executor/main.ts:65`: `main`
→ described at Function Executor `03-execution-and-replay.md` → Cancellation and terminal cleanup:
SPEC'D.

SYMBOL `typescript/src/function-executor/blob.ts:223`: BLOB download/upload functions
→ described at Function Executor `01-domain-model.md` → BLOB and
`03-execution-and-replay.md` → Execution flow: SPEC'D.

SYMBOL `src/tensorlake/function_executor/user_events.py` and
`typescript/src/function-executor/user-events.ts`: lifecycle event emitters
→ described at Function Executor `04-architecture-and-verification.md` → Observability: SPEC'D.

### Python Applications SDK symbols

SYMBOL `src/tensorlake/applications/interface/decorators.py:86`, `:180`, `:264`:
`application`, `function`, `cls`
→ described at Applications SDK `02-definition-and-registration.md` → Python definition API:
SPEC'D.

SYMBOL `src/tensorlake/applications/interface/function.py:58`: `Function`, map, reduce, future
factory
→ described at Applications SDK `01-domain-model.md` → Registered function and
`03-composition-and-context.md`: SPEC'D.

SYMBOL `src/tensorlake/applications/interface/futures.py:45` and `:53`: `RETURN_WHEN`, `Future`
→ described at Applications SDK `03-composition-and-context.md` → Future contract: SPEC'D.

SYMBOL `src/tensorlake/applications/interface/retries.py:1`: `Retries`
→ described at Applications SDK `02-definition-and-registration.md` → Resource and retry
configuration: SPEC'D.

SYMBOL `src/tensorlake/applications/interface/file.py:1`: `File`
→ described at Applications SDK `01-domain-model.md` → File and
`03-composition-and-context.md` → File and error boundaries: SPEC'D.

SYMBOL `src/tensorlake/applications/interface/request.py:6`: `Request`
→ described at Applications SDK `01-domain-model.md` → Application request: SPEC'D.

SYMBOL `src/tensorlake/applications/interface/request_context.py:7`, `:52`, `:77`:
`RequestState`, `FunctionProgress`, `RequestContext`
→ described at Applications SDK `03-composition-and-context.md` → Request context: SPEC'D.

SYMBOL `src/tensorlake/applications/interface/logger.py`: `Logger`
→ described at Applications SDK `04-architecture-and-verification.md` → Public export inventory:
SPEC'D.

SYMBOL `src/tensorlake/applications/interface/exceptions.py:1`:
public Tensorlake exception hierarchy
→ described at Applications SDK `03-composition-and-context.md` → File and error boundaries:
SPEC'D.

SYMBOL `src/tensorlake/applications/interface/run.py`: local and remote application runners
→ described at Applications SDK `04-architecture-and-verification.md` → Local and remote behavior:
SPEC'D.

SYMBOL `tensorlake.image.Image`, re-exported at
`src/tensorlake/applications/interface/__init__.py:4`
→ described at Applications SDK `04-architecture-and-verification.md` → Public export inventory:
SPEC'D.

### TypeScript Applications SDK symbols

SYMBOL `typescript/src/applications/index.ts:1`: registration, retry, Future, and definition types
→ described at Applications SDK `02-definition-and-registration.md` → TypeScript definition API and
`04-architecture-and-verification.md` → Public export inventory: SPEC'D.

SYMBOL `typescript/src/applications/index.ts:15`: `schema` and schema types
→ described at Applications SDK `02-definition-and-registration.md` → TypeScript schema contract:
SPEC'D.

SYMBOL `typescript/src/applications/index.ts:16`: `File`, `Image`, `SDK_VERSION`
→ described at Applications SDK `04-architecture-and-verification.md` → Public export inventory:
SPEC'D.

SYMBOL `typescript/src/applications/index.ts:19`: `RequestContext`
→ described at Applications SDK `03-composition-and-context.md` → Request context: SPEC'D.

SYMBOL `typescript/src/applications/index.ts:20`: `runLocal`, `LocalRequest`
→ described at Applications SDK `04-architecture-and-verification.md` → Local and remote behavior:
SPEC'D.

SYMBOL `typescript/src/applications/index.ts:21`: `runRemote`, `remoteOptions`, `RemoteRequest`
→ described at Applications SDK `04-architecture-and-verification.md` → Local and remote behavior:
SPEC'D.

SYMBOL `typescript/src/applications/index.ts:27`: public TypeScript error hierarchy
→ described at Applications SDK `03-composition-and-context.md` → File and error boundaries:
SPEC'D.

SYMBOL `typescript/src/applications/index.ts:37`: manifest builders and manifest types
→ described at Applications SDK `02-definition-and-registration.md` → Registry and manifest
contract: SPEC'D.

SYMBOL `typescript/src/applications/index.ts:46`: read-only registry lookups
→ described at Applications SDK `04-architecture-and-verification.md` → Public export inventory:
SPEC'D.

No significant public package entrypoint symbol is absent from the canonical bodies.

## Divergence classification and remediation

DIVERGENCE: Python accepted event-log pages without validating cross-language clock and payload
invariants.

- TYPE: MISSING impl.
- SOURCE OF TRUTH: the shared protocol and TypeScript behavior; malformed history must terminate
  deterministically.
- SUFFICIENCY TEST: yes for this claim; the other findings remain independent.
- REMEDY: implemented page validation in
  `src/tensorlake/function_executor/allocation_runner/event_log_reader.py`, live internal-failure
  handling, strict replay mismatch handling, unit cases, and parity probes.

DIVERGENCE: Python event-log read delivery could strand a request when the stream that consumed a
single queue entry disconnected.

- TYPE: INCORRECT impl.
- SOURCE OF TRUTH: the service lifecycle contract and TypeScript reconnect behavior; a replacement
  stream must see the pending read.
- SUFFICIENCY TEST: yes for this claim.
- REMEDY: replaced the single-consumer queue with a pending-read broadcaster, added single-settlement
  handling for late responses, and added reconnect transport tests.

DIVERGENCE: Python allocation manifests accepted integers beyond JavaScript's exact range and
present-but-empty required identifiers.

- TYPE: INCORRECT impl.
- SOURCE OF TRUTH: the shared Python/TypeScript wire contract; both runtimes must interpret sizes
  and identifiers identically.
- SUFFICIENCY TEST: yes for this claim.
- REMEDY: added safe-integer checks for manifests, offsets, chunks, and encoding versions; required
  string fields now reject empty values; the parity malformed-admission probe covers both cases.

DIVERGENCE: TypeScript initialization accepted empty function-reference strings.

- TYPE: INCORRECT impl.
- SOURCE OF TRUTH: protobuf presence alone is insufficient for opaque non-empty identifiers.
- SUFFICIENCY TEST: yes for this claim.
- REMEDY: validate namespace, application, version, and function strings before archive work and
  compare the structured initialization failure in the compatibility harness.

DIVERGENCE: Python deployment validation did not enforce the shared resource, region, container,
GPU, tag, and retry limits.

- TYPE: MISSING impl.
- SOURCE OF TRUTH: the existing TypeScript/API limits and manifest contract.
- SUFFICIENCY TEST: yes for this claim.
- REMEDY: added aggregated Python validation messages and focused configuration tests; strengthened
  TypeScript GPU and tag runtime validation.

DIVERGENCE: Python Future wait consumed generator input before the runtime hook and invalid wait
controls could start work before rejection.

- TYPE: INCORRECT impl.
- SOURCE OF TRUTH: the public Future contract; iterable input is consumed once and validation is
  atomic with respect to starting.
- SUFFICIENCY TEST: yes for this claim.
- REMEDY: materialize once, validate delay/timeout/mode before mutation or start, and add Python and
  TypeScript regression tests.

DIVERGENCE: Python local retry attempts reused values mutated by a prior failed attempt.

- TYPE: INCORRECT impl.
- SOURCE OF TRUTH: deployed attempts begin from serialized call inputs and TypeScript local retries
  already preserve that boundary.
- SUFFICIENCY TEST: yes for this claim.
- REMEDY: retain pristine resolved inputs, copy them for each attempt, preserve the process-scoped
  class instance, and add an isolated local retry boundary test.

DIVERGENCE: malformed TypeScript resource or explicit-schema objects could escape as native
`TypeError` values.

- TYPE: INCORRECT impl.
- SOURCE OF TRUTH: invalid SDK registration is a typed `SDKUsageError`.
- SUFFICIENCY TEST: yes for this claim.
- REMEDY: validate retry objects, secrets arrays, plain tag records, parameter arrays, return
  schemas, and parameter descriptor shapes before normalization.

DIVERGENCE: replay verification fixtures restarted live clocks after replay or used separate clocks
for recreated watcher and callback events.

- TYPE: INCORRECT impl in verification fixtures.
- SOURCE OF TRUTH: event clocks remain monotonic across replay-to-live transition.
- SUFFICIENCY TEST: yes for the fixtures.
- REMEDY: make recreated and live events share the replay cursor in complex and sequential replay
  suites.

DIVERGENCE: the invalid-configuration regression fixture registered malformed application state at
module import time and could contaminate unrelated local-run tests in the same process.

- TYPE: INCORRECT impl in verification fixtures.
- SOURCE OF TRUTH: application registration is process-global, so a test that intentionally adds
  invalid definitions must restore the registry it changes.
- SUFFICIENCY TEST: yes for the fixture.
- REMEDY: register the malformed function in `setUp`, restore the registry snapshot in `tearDown`,
  and run the configuration and local-retry cases together.

DIVERGENCE: the first canonical draft described TypeScript archive extraction as size-bounded and
described both languages as waiting for runner completion during shutdown.

- TYPE: INCORRECT impl relative to an overclaimed spec body.
- SOURCE OF TRUTH: current code; archive size limits are external to this executor source, and
  Python cannot safely interrupt an arbitrary user thread.
- SUFFICIENCY TEST: yes for these statements.
- REMEDY: the canonical body now says the ZIP is validated and documents the distinct bounded
  Python and TypeScript shutdown mechanisms.

All classified divergences are remediated in the reviewed source state.

## Edge cases

All claims are verifiable from the provided context. Generated protobuf artifacts are covered by
the shared-source check, cross-bundle behavior is exercised by separate-copy tests, and deployed
protocol behavior is exercised by real executor processes in the compatibility harness.

## Verification evidence

- `22` Python Function Executor test files: pass.
- TypeScript Vitest suite: `18` files and `388` tests pass.
- Function Executor compatibility harness: Python and TypeScript pass `26` shared parity scenarios
  plus initialization, malformed-admission, BLOB-failure, malformed-event-log, replay, and shutdown
  probes.
- TypeScript typecheck, ESLint, SDK/capsule build, package compatibility, and shared-proto source
  check: pass.
- Focused Python event-log, message-validation, Future-validation, resource-validation, and local
  retry tests pass together: `12` tests and `20` subtests.
- Python formatting/import checks, canonical JSON parsing, local-link resolution, and diff
  whitespace checks are part of the final conformance gate.

VERDICT: CONFORMS
CONFIDENCE: high
SUMMARY: Every canonical body claim maps to the remediated implementation, every significant public symbol is specified, and real-executor parity plus language tests cover the shared contract.
DIVERGENCES:
- None after remediation.

## Assumptions and open questions

**Assumptions**

- The orchestration server continues to enforce deployment upload limits outside the executor
  process.
- Generated Python protobuf files are refreshed through the repository's existing generation
  workflow when the shared definition changes.

**Decisions**

- *Review scope.* **Public SDK entrypoints and protocol-significant executor symbols receive the
  reverse coverage pass.** Private helpers are covered through their public behavior and tests.
- *Verdict timing.* **The verdict describes the post-remediation source state.** Initial
  divergences remain recorded above for auditability.

**Open questions**

(None at this stage.)
