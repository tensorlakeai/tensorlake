import { AsyncResource, createHook, executionAsyncId } from "node:async_hooks";
import { createHash, randomUUID } from "node:crypto";
import type { RegisteredDefinition, FunctionRuntime, FunctionFuture, TailCall } from "../applications/function.js";
import {
  DURABLE_PROMISE_INTERNAL_THEN,
  DURABLE_PROMISE_OBSERVER,
  executeHandlerResult,
  installPromiseInstrumentation,
  isDetachedDurableThenable,
  isTailCall,
  registerDurableAssimilation,
  registerDurableThenable,
  runWithFunctionRuntime,
} from "../applications/function.js";
import { File } from "../applications/file.js";
import { Headers } from "../applications/headers.js";
import { HttpBody } from "../applications/http-body.js";
import {
  DeserializationError,
  FunctionError,
  ReplayMismatchError,
  RequestError,
  SDKUsageError,
  TimeoutError,
  isRequestError,
} from "../applications/errors.js";
import type { RequestContextValue } from "../applications/context.js";
import {
  runWithRequestContext,
  serializeRequestStateValue,
  validateCounterMetric,
  validateProgressUpdate,
  validateRequestStateKey,
  validateTimerMetric,
  waitWithAbortSignal,
} from "../applications/context.js";
import { deserializeJSON } from "../applications/serialization.js";
import { deferred, type Deferred } from "./async-queue.js";
import {
  deserializeValueFromProtocol,
  downloadBlob as downloadBlobData,
  downloadSerializedObject as downloadSerializedObjectData,
  joinPrepared,
  prepareSerializedObject,
  prepareTextObject,
  uploadBlob as uploadBlobData,
  type BlobValue,
  type PreparedSerializedObject,
  type SerializedObjectInsideBlobValue,
} from "./blob.js";
import { writeStructuredOutput } from "./safe-output.js";
import { printCloudEvent } from "./user-events.js";

// The executor imports this module before deployed application modules and
// their dependencies, so module-level captures retain the instrumented
// Promise implementations used during allocation execution.
installPromiseInstrumentation();

type Message = Record<string, any>;
type StreamCall = { write(value: Message): boolean; end(): void; on(event: string, listener: () => void): void };
type LogLevel = "debug" | "info" | "warn" | "error";

const OK = 0;
const NOT_FOUND = 5;
const REPLAY_STRICT = new Set(["REPLAY_MODE_STRICT", 1]);
const OUTCOME_SUCCESS = new Set(["ALLOCATION_OUTCOME_CODE_SUCCESS", 1]);
const OUTCOME_FAILURE = new Set(["ALLOCATION_OUTCOME_CODE_FAILURE", 2]);
const WATCHER_TIMED_OUT = new Set(["FUNCTION_CALL_WATCHER_STATUS_TIMEDOUT", 2]);
const MAX_REDUCE_CALLS_PER_PLAN = 512;
const nativeStructuredClone = globalThis.structuredClone;

function statusError(status: Message | undefined, fallback: string): Error | undefined {
  if (status == null || (status.code ?? OK) === OK) return undefined;
  return new FunctionError(status.message || fallback);
}

function validatedEventLogClock<E extends Error>(
  response: Message,
  requestedAfterClock: number,
  createError: (message: string) => E,
): number {
  if (response.entries != null && !Array.isArray(response.entries)) {
    throw createError("Allocation event log returned a non-array entries field");
  }
  const responseClock = Number(response.lastClock ?? requestedAfterClock);
  if (
    !Number.isFinite(responseClock)
    || !Number.isSafeInteger(responseClock)
    || responseClock < 0
  ) {
    throw createError(
      `Allocation event log returned invalid clock '${String(response.lastClock)}'`,
    );
  }
  if (responseClock < requestedAfterClock) {
    throw createError(
      `Allocation event log moved backwards from clock ${requestedAfterClock} to ${responseClock}`,
    );
  }
  if (response.hasMore && responseClock === requestedAfterClock) {
    throw createError(
      `Allocation event log reported more entries without advancing clock ${requestedAfterClock}`,
    );
  }
  if ((response.entries?.length ?? 0) > 0 && responseClock === requestedAfterClock) {
    throw createError(
      `Allocation event log returned entries without advancing clock ${requestedAfterClock}`,
    );
  }
  const eventFields = [
    "functionCallCreated",
    "functionCallWatcherCreated",
    "functionCallWatcherResult",
  ];
  let previousEntryClock = requestedAfterClock;
  for (const [index, entry] of (response.entries ?? []).entries()) {
    if (entry == null || typeof entry !== "object") {
      throw createError(`Allocation event log entry ${index} is not an object`);
    }
    const presentFields = eventFields.filter((field) => entry[field] != null);
    if (presentFields.length !== 1) {
      throw createError(
        `Allocation event log entry ${index} has ${presentFields.length} recognized payloads; expected exactly one`,
      );
    }
    const entryClock = Number(entry.clock);
    if (
      !Number.isSafeInteger(entryClock)
      || entryClock <= previousEntryClock
      || entryClock > responseClock
    ) {
      throw createError(
        `Allocation event log entry ${index} has invalid clock '${String(entry.clock)}'`
        + ` after ${previousEntryClock} with page clock ${responseClock}`,
      );
    }
    previousEntryClock = entryClock;
  }
  return responseClock;
}

function durableHash(values: string[]): string {
  const hash = createHash("sha256");
  for (const value of values) {
    const bytes = Buffer.from(value, "utf8");
    const length = Buffer.alloc(8);
    length.writeBigUInt64BE(BigInt(bytes.byteLength));
    hash.update(length);
    hash.update(bytes);
    hash.update("|");
  }
  return hash.digest("hex");
}

function protocolTimestamp(date: Date): { seconds: number; nanos: number } {
  const milliseconds = date.getTime();
  return { seconds: Math.floor(milliseconds / 1000), nanos: (milliseconds % 1000) * 1_000_000 };
}

function parseHTTPMessage(data: Uint8Array): { headers: Record<string, string>; body: Uint8Array } {
  const marker = Buffer.from("\r\n\r\n");
  const bytes = Buffer.from(data);
  const split = bytes.indexOf(marker);
  if (split < 0) throw new DeserializationError("Invalid message/http application input");
  const lines = bytes.subarray(0, split).toString("utf8").split("\r\n");
  lines.shift();
  const headers: Record<string, string> = {};
  for (const line of lines) {
    const colon = line.indexOf(":");
    if (colon > 0) headers[line.slice(0, colon).trim().toLowerCase()] = line.slice(colon + 1).trim();
  }
  return { headers, body: new Uint8Array(bytes.subarray(split + marker.byteLength)) };
}

export async function deserializeApplicationArguments(
  definition: RegisteredDefinition,
  payload: { data: Uint8Array; contentType?: string; encoding?: string | number },
): Promise<{ args: unknown[] }> {
  let data = payload.data;
  let suppliedContentType = payload.contentType;
  let hasExplicitContentType = payload.contentType != null;
  if (suppliedContentType?.toLowerCase().startsWith("message/http")) {
    const parsed = parseHTTPMessage(data);
    data = parsed.body;
    const bodyContentType = parsed.headers["content-type"];
    hasExplicitContentType = bodyContentType != null;
    suppliedContentType = bodyContentType;
  }
  if (definition.parameters.length === 0) return { args: [] };
  const firstParameter = definition.parameters[0];
  if (
    definition.parameters.length === 1
    && firstParameter?.schema._httpBody
  ) {
    return { args: [new HttpBody(data, suppliedContentType)] };
  }
  const contentType = suppliedContentType ?? "application/json";
  if (contentType.toLowerCase().startsWith("multipart/form-data")) {
    let form: FormData;
    try {
      form = await new Response(Uint8Array.from(data).buffer, {
        headers: { "content-type": contentType },
      }).formData();
    } catch (error) {
      throw new DeserializationError("Failed to deserialize multipart application input", { cause: error });
    }
    const args: unknown[] = [];
    for (const [index, parameter] of definition.parameters.entries()) {
      const part = form.get(parameter.name) ?? form.get(String(index));
      if (part == null) {
        args.push(undefined);
      } else if (parameter.schema._file || parameter.schema._httpBody) {
        const bytes = typeof part === "string"
          ? new TextEncoder().encode(part)
          : new Uint8Array(await part.arrayBuffer());
        const partContentType = typeof part === "string"
          ? "application/octet-stream"
          : part.type || "application/octet-stream";
        args.push(parameter.schema._httpBody
          ? new HttpBody(bytes, partContentType)
          : new File(bytes, partContentType));
      } else {
        try {
          const bytes = typeof part === "string"
            ? new TextEncoder().encode(part)
            : new Uint8Array(await part.arrayBuffer());
          args.push(deserializeJSON(bytes));
        } catch (error) {
          if (error instanceof DeserializationError) throw error;
          throw new DeserializationError(
            `Failed to deserialize multipart argument '${parameter.name}' as JSON`,
            { cause: error },
          );
        }
      }
    }
    return { args };
  }
  // An invocation without a request body supplies no arguments. This lets the
  // normal argument validator apply JavaScript defaults (or report a required
  // argument) instead of attempting to parse an empty JSON document. An
  // explicitly typed empty file remains a real File input.
  if (data.byteLength === 0 && (!firstParameter?.schema._file || !hasExplicitContentType)) {
    return { args: [] };
  }
  if (firstParameter?.schema._file) return { args: [new File(data, contentType)] };
  if (!contentType.toLowerCase().includes("json")) {
    throw new DeserializationError(
      `Expected application/json input for '${firstParameter?.name ?? "argument"}', got ${contentType}`,
    );
  }
  return { args: [deserializeJSON(data)] };
}

type ReplayEntryKind = "call" | "watcher" | "result" | "unknown";

interface ReplayEntry {
  readonly kind: ReplayEntryKind;
  readonly event: Message;
  consumed: boolean;
}

// Handles that can remain alive without representing a pending callback which
// could resume application code. Request resources, timers, immediates, and
// promises are deliberately not in this set.
const PASSIVE_ASYNC_RESOURCE_TYPES = new Set([
  "PIPEWRAP",
  "PIPESERVERWRAP",
  "SIGNALWRAP",
  "TCPSERVERWRAP",
  "TCPWRAP",
  "TLSWRAP",
  "TTYWRAP",
  "UDPWRAP",
]);

interface TrackedAsyncResource {
  readonly type: string;
  readonly triggerAsyncId: number;
  // Promise destroy hooks are GC-driven. A strong reference here would keep
  // every Promise alive until the allocation ends and prevent that cleanup
  // hook from ever running.
  readonly promise?: WeakRef<object>;
  pending: boolean;
}

/**
 * Tracks asynchronous work which is causally capable of producing the next
 * durable event during strict replay.
 *
 * Promise frames created by async functions are not producers by themselves:
 * they can be waiting on another durable result and form a replay cycle.
 * Timers and I/O request resources are producers. Promise ownership is still
 * propagated when one promise is resolved by a causal continuation, which
 * carries causality through Promise.all/race and pre-created await reactions.
 */
export class ReplayCausality {
  private readonly hook: ReturnType<typeof createHook>;
  private readonly children = new Map<number, Set<number>>();
  private readonly resources = new Map<number, TrackedAsyncResource>();
  private readonly owners = new Map<number, Set<number>>();
  private readonly provenance = new Map<number, Set<number>>();
  private readonly tokenResources = new Map<number, Set<number>>();
  private readonly tokenProducers = new Map<number, Set<number>>();
  private readonly promiseIds = new WeakMap<object, number>();
  private readonly excludedRoots = new Set<number>();
  private readonly durableAssimilations = new Map<number, number>();
  private readonly nativeDurableAssimilations = new Set<number>();
  private readonly explicitDurableAssimilations = new Set<number>();
  private readonly promiseAggregateAssimilationScopes: Set<number>[] = [];
  private readonly promiseSettlementObservers = new Map<number, Set<() => void>>();
  private readonly mixedPromiseProducerGroups = new Set<Set<number>>();
  private nextToken = 1;
  private checkScheduled = false;
  private checkHandle?: NodeJS.Immediate;
  private suppressOwnership = false;
  private stopped = false;

  constructor(private readonly onQuiescent: () => void) {
    this.hook = createHook({
      init: (asyncId, type, triggerAsyncId, resource) => {
        if (this.stopped) return;
        const triggerPromise = this.resources.get(triggerAsyncId)?.promise?.deref();
        const resumesDetachedDurable =
          type === "PROMISE"
          && triggerPromise != null
          && isDetachedDurableThenable(triggerPromise);
        const children = this.children.get(triggerAsyncId) ?? new Set<number>();
        children.add(asyncId);
        this.children.set(triggerAsyncId, children);
        if (type === "PROMISE" && typeof resource === "object" && resource != null) {
          this.promiseIds.set(resource, asyncId);
        }
        const triggerOwners = new Set(this.owners.get(triggerAsyncId) ?? []);
        this.resources.set(asyncId, {
          type,
          triggerAsyncId,
          promise: type === "PROMISE" && typeof resource === "object" && resource != null
            ? new WeakRef(resource)
            : undefined,
          pending: true,
        });
        if (resumesDetachedDurable) {
          this.nativeDurableAssimilations.add(asyncId);
          this.promiseAggregateAssimilationScopes.at(-1)?.add(asyncId);
          this.suspendDurableAssimilation(asyncId);
        }
        if (this.suppressOwnership) return;
        const inherited = new Set([
          ...(this.owners.get(executionAsyncId()) ?? []),
          ...triggerOwners,
        ]);
        for (const token of inherited) this.addTokenToSubtree(asyncId, token);
      },
      promiseResolve: (asyncId) => {
        if (this.stopped) return;
        this.notifyPromiseSettlement(asyncId);
        if (this.nativeDurableAssimilations.delete(asyncId)) {
          this.resumeDurableAssimilation(asyncId);
        }
        if (this.explicitDurableAssimilations.delete(asyncId)) {
          this.resumeDurableAssimilation(asyncId);
        }
        this.settleMixedPromiseProducerGroups(asyncId);
        const currentOwners = this.owners.get(executionAsyncId());
        if (currentOwners != null) {
          for (const token of currentOwners) this.addTokenToSubtree(asyncId, token);
        }
        this.setResourcePending(asyncId, false);
        this.scheduleCheck();
      },
      destroy: (asyncId) => {
        if (this.stopped) return;
        this.notifyPromiseSettlement(asyncId);
        if (this.nativeDurableAssimilations.delete(asyncId)) {
          this.detachDurableAssimilation(asyncId);
        }
        if (this.explicitDurableAssimilations.delete(asyncId)) {
          this.detachDurableAssimilation(asyncId);
        }
        this.settleMixedPromiseProducerGroups(asyncId);
        this.setResourcePending(asyncId, false);
        this.releaseDestroyedResource(asyncId);
        this.scheduleCheck();
      },
    });
    this.hook.enable();
  }

  runRoot<T>(callback: () => Promise<T>): Promise<T> {
    const resource = new AsyncResource("TensorlakeReplayCausality");
    let result!: Promise<T>;
    resource.runInAsyncScope(() => {
      this.beginCurrentContinuation();
      result = callback();
      // The async handler's own result promise is pending for the handler's
      // entire lifetime, so it cannot prove that application code can advance
      // replay. Concrete promises created by the handler remain tracked.
      this.excludePromise(result);
    });
    resource.emitDestroy();
    return result;
  }

  beginCurrentContinuation(): void {
    if (this.stopped) return;
    const asyncId = executionAsyncId();
    this.excludedRoots.delete(asyncId);
    this.addTokenToSubtree(asyncId, this.nextToken);
    this.nextToken += 1;
    this.scheduleCheck();
  }

  excludePromise(promise: Promise<unknown>): void {
    const asyncId = this.promiseIds.get(promise);
    if (asyncId == null) return;
    this.excludedRoots.add(asyncId);
    this.removeOwnersFromSubtree(asyncId);
    this.scheduleCheck();
  }

  suspendDurableAssimilation(asyncId: number): boolean {
    if (this.stopped) return false;
    const count = this.durableAssimilations.get(asyncId) ?? 0;
    this.durableAssimilations.set(asyncId, count + 1);
    if (count === 0) {
      this.excludedRoots.add(asyncId);
      this.removeOwnersFromSubtree(asyncId);
    }
    this.scheduleCheck();
    return true;
  }

  resumeDurableAssimilation(asyncId: number): void {
    if (this.stopped) return;
    const count = this.durableAssimilations.get(asyncId);
    if (count == null) return;
    if (count > 1) {
      this.durableAssimilations.set(asyncId, count - 1);
      return;
    }
    this.durableAssimilations.delete(asyncId);
    this.excludedRoots.delete(asyncId);
    const currentOwners = this.owners.get(executionAsyncId());
    if (currentOwners != null) {
      for (const token of currentOwners) this.addTokenToSubtree(asyncId, token);
    }
  }

  detachDurableAssimilation(asyncId: number): void {
    if (this.stopped) return;
    const count = this.durableAssimilations.get(asyncId);
    if (count == null) return;
    if (count > 1) {
      this.durableAssimilations.set(asyncId, count - 1);
      return;
    }
    this.durableAssimilations.delete(asyncId);
    this.excludedRoots.delete(asyncId);
    // This reaction belongs to aggregate construction rather than an
    // application-level await. Do not restore its old ownership: a pending
    // durable input would otherwise mask a genuinely blocked durable await
    // created when user code later consumes the aggregate.
    this.scheduleCheck();
  }

  hasPendingDurableAssimilation(): boolean {
    return this.durableAssimilations.size > 0;
  }

  hasPendingProducer(): boolean {
    if (this.mixedPromiseProducerGroups.size > 0) return true;
    for (const producers of this.tokenProducers.values()) {
      if (producers.size > 0) return true;
    }
    return false;
  }

  requestCheck(): void {
    this.scheduleCheck();
  }

  trackMixedPromiseRace(
    promise: Promise<unknown>,
    producers: readonly Promise<unknown>[] = [promise],
  ): void {
    if (this.stopped) return;
    const producerIds = new Set<number>();
    for (const producer of producers) {
      const asyncId = this.promiseIds.get(producer);
      if (asyncId == null) continue;
      // A producer in this race has already settled, so the "first producer
      // settles" lifetime has already ended and the other producers must not
      // keep replay alive.
      if (this.resources.get(asyncId)?.pending !== true) {
        this.scheduleCheck();
        return;
      }
      producerIds.add(asyncId);
    }
    if (producerIds.size > 0) this.mixedPromiseProducerGroups.add(producerIds);
    this.scheduleCheck();
  }

  trackPromiseSettlement(
    promise: Promise<unknown>,
    onSettled: () => void,
  ): boolean {
    if (this.stopped) return false;
    const asyncId = this.promiseIds.get(promise);
    if (asyncId == null) return false;
    if (this.resources.get(asyncId)?.pending !== true) {
      onSettled();
      return true;
    }
    const observers = this.promiseSettlementObservers.get(asyncId) ?? new Set<() => void>();
    observers.add(onSettled);
    this.promiseSettlementObservers.set(asyncId, observers);
    return true;
  }

  trackDurablePromiseConsumption(promise: Promise<unknown>): void {
    if (this.stopped) return;
    const asyncId = this.promiseIds.get(promise);
    if (
      asyncId == null
      || this.resources.get(asyncId)?.pending !== true
      || this.nativeDurableAssimilations.has(asyncId)
      || this.explicitDurableAssimilations.has(asyncId)
    ) {
      return;
    }
    this.explicitDurableAssimilations.add(asyncId);
    this.promiseAggregateAssimilationScopes.at(-1)?.add(asyncId);
    this.suspendDurableAssimilation(asyncId);
  }

  runPromiseAggregateConstruction<T>(callback: () => T): T {
    const aggregateAssimilations = new Set<number>();
    this.promiseAggregateAssimilationScopes.push(aggregateAssimilations);
    try {
      return callback();
    } finally {
      this.promiseAggregateAssimilationScopes.pop();
      // Promise combinators install internal reactions while constructing the
      // aggregate. Those reactions do not prove that application code awaits
      // the aggregate; a later user reaction on the returned promise does.
      this.detachPromiseAggregateAssimilations(aggregateAssimilations);
    }
  }

  runWithoutOwnership<T>(callback: () => T): T {
    if (this.stopped) return callback();
    const previous = this.suppressOwnership;
    this.suppressOwnership = true;
    try {
      return callback();
    } finally {
      this.suppressOwnership = previous;
    }
  }

  stop(): void {
    if (this.stopped) return;
    this.stopped = true;
    this.hook.disable();
    if (this.checkHandle != null) clearImmediate(this.checkHandle);
    this.checkHandle = undefined;
    this.children.clear();
    this.resources.clear();
    this.owners.clear();
    this.provenance.clear();
    this.tokenResources.clear();
    this.tokenProducers.clear();
    this.durableAssimilations.clear();
    this.nativeDurableAssimilations.clear();
    this.explicitDurableAssimilations.clear();
    this.promiseAggregateAssimilationScopes.length = 0;
    this.promiseSettlementObservers.clear();
    this.mixedPromiseProducerGroups.clear();
  }

  private notifyPromiseSettlement(asyncId: number): void {
    const observers = this.promiseSettlementObservers.get(asyncId);
    if (observers == null) return;
    this.promiseSettlementObservers.delete(asyncId);
    for (const observer of observers) observer();
  }

  private settleMixedPromiseProducerGroups(asyncId: number): void {
    for (const group of [...this.mixedPromiseProducerGroups]) {
      if (group.has(asyncId)) this.mixedPromiseProducerGroups.delete(group);
    }
  }

  private detachPromiseAggregateAssimilations(assimilations: Set<number>): void {
    for (const asyncId of assimilations) {
      if (this.nativeDurableAssimilations.delete(asyncId)) {
        this.detachDurableAssimilation(asyncId);
      }
      if (this.explicitDurableAssimilations.delete(asyncId)) {
        this.detachDurableAssimilation(asyncId);
      }
    }
    assimilations.clear();
  }

  private addTokenToSubtree(asyncId: number, token: number): void {
    if (this.excludedRoots.has(asyncId)) return;
    const owners = this.owners.get(asyncId) ?? new Set<number>();
    if (!owners.has(token)) {
      owners.add(token);
      this.owners.set(asyncId, owners);
      const provenance = this.provenance.get(asyncId) ?? new Set<number>();
      provenance.add(token);
      this.provenance.set(asyncId, provenance);
      const tokenResources = this.tokenResources.get(token) ?? new Set<number>();
      tokenResources.add(asyncId);
      this.tokenResources.set(token, tokenResources);
      const resource = this.resources.get(asyncId);
      if (resource?.pending && this.isProducer(resource, token)) {
        const producers = this.tokenProducers.get(token) ?? new Set<number>();
        producers.add(asyncId);
        this.tokenProducers.set(token, producers);
      }
    }
    for (const child of this.children.get(asyncId) ?? []) {
      this.addTokenToSubtree(child, token);
    }
  }

  private isProducer(resource: TrackedAsyncResource, token: number): boolean {
    if (resource.type === "PROMISE") {
      return !(this.provenance.get(resource.triggerAsyncId)?.has(token) ?? false);
    }
    return !PASSIVE_ASYNC_RESOURCE_TYPES.has(resource.type);
  }

  private setResourcePending(asyncId: number, pending: boolean): void {
    const resource = this.resources.get(asyncId);
    if (resource == null || resource.pending === pending) return;
    resource.pending = pending;
    for (const token of this.owners.get(asyncId) ?? []) {
      const producers = this.tokenProducers.get(token);
      if (pending && this.isProducer(resource, token)) {
        const next = producers ?? new Set<number>();
        next.add(asyncId);
        this.tokenProducers.set(token, next);
      } else {
        producers?.delete(asyncId);
      }
    }
  }

  private removeOwnersFromSubtree(asyncId: number): void {
    this.removeResourceOwnership(asyncId);
    for (const child of this.children.get(asyncId) ?? []) {
      this.removeOwnersFromSubtree(child);
    }
  }

  private removeResourceOwnership(asyncId: number): void {
    const owners = this.owners.get(asyncId);
    if (owners == null) return;
    for (const token of owners) {
      const resources = this.tokenResources.get(token);
      resources?.delete(asyncId);
      if (resources?.size === 0) this.tokenResources.delete(token);
      const producers = this.tokenProducers.get(token);
      producers?.delete(asyncId);
      if (producers?.size === 0) this.tokenProducers.delete(token);
    }
    this.owners.delete(asyncId);
  }

  private releaseDestroyedResource(asyncId: number): void {
    const resource = this.resources.get(asyncId);
    if (resource == null) return;
    this.resources.delete(asyncId);
    this.excludedRoots.delete(asyncId);
    this.removeResourceOwnership(asyncId);
    this.provenance.delete(asyncId);
    const siblings = this.children.get(resource.triggerAsyncId);
    siblings?.delete(asyncId);
    if (siblings?.size === 0) this.children.delete(resource.triggerAsyncId);
    // A destroy hook is Node's guarantee that this resource cannot create
    // further descendants. Existing children already own independent copies
    // of their provenance and ownership sets.
    this.children.delete(asyncId);
  }

  private scheduleCheck(): void {
    if (this.stopped || this.checkScheduled) return;
    this.checkScheduled = true;
    const previous = this.suppressOwnership;
    this.suppressOwnership = true;
    try {
      this.checkHandle = setImmediate(() => {
        this.checkScheduled = false;
        this.checkHandle = undefined;
        this.onQuiescent();
      });
    } finally {
      this.suppressOwnership = previous;
    }
  }
}

class ReplayAwarePromise<T> extends Promise<T> {
  private readonly causality: ReplayCausality;

  constructor(source: Promise<T>, causality: ReplayCausality) {
    let bridge: Promise<void> | undefined;
    super((resolve, reject) => {
      bridge = source.then(resolve, reject);
    });
    this.causality = causality;
    registerDurableThenable(this);
    causality.excludePromise(source);
    causality.excludePromise(this);
    if (bridge != null) causality.excludePromise(bridge);
  }

  static get [Symbol.species](): PromiseConstructor {
    return Promise;
  }

  override then<TResult1 = T, TResult2 = never>(
    onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
  ): Promise<TResult1 | TResult2> {
    const assimilationId = executionAsyncId();
    const assimilationSuspended =
      this.causality.suspendDurableAssimilation(assimilationId);
    let assimilationDetached = false;
    const aggregateAssimilation = registerDurableAssimilation(this, () => {
      if (!assimilationSuspended || assimilationDetached) return;
      assimilationDetached = true;
      this.causality.detachDurableAssimilation(assimilationId);
    });
    const continuation = super.then(
      (value) => {
        this.causality.beginCurrentContinuation();
        aggregateAssimilation.finish();
        if (assimilationSuspended && !assimilationDetached) {
          this.causality.resumeDurableAssimilation(assimilationId);
        }
        if (onfulfilled == null) return value as unknown as TResult1;
        return onfulfilled(value);
      },
      (reason) => {
        this.causality.beginCurrentContinuation();
        aggregateAssimilation.finish();
        if (assimilationSuspended && !assimilationDetached) {
          this.causality.resumeDurableAssimilation(assimilationId);
        }
        if (onrejected == null) throw reason;
        return onrejected(reason);
      },
    );
    aggregateAssimilation.link(continuation);
    this.causality.excludePromise(continuation);
    return continuation;
  }

  [DURABLE_PROMISE_INTERNAL_THEN]<TResult1 = T, TResult2 = never>(
    onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
  ): Promise<TResult1 | TResult2> {
    const continuation = super.then(onfulfilled, onrejected);
    registerDurableThenable(continuation, this);
    this.causality.excludePromise(continuation);
    return continuation;
  }

  [DURABLE_PROMISE_OBSERVER](
    onfulfilled: (value: T) => void,
    onrejected: (reason: unknown) => void,
  ): void {
    const observation = super.then(onfulfilled, onrejected);
    this.causality.excludePromise(observation);
    void observation.catch(() => undefined);
  }
}

class ReplayHistory {
  private readonly entries: ReplayEntry[];
  private readonly calls: ReplayEntry[] = [];
  private readonly watchers: ReplayEntry[] = [];
  private readonly availableResults = new Map<string, Message[]>();
  private readonly resultWaiters = new Map<string, Deferred<Message | undefined>[]>();
  private readonly endWaiters: Deferred<void>[] = [];
  private readonly createdWatcherIds = new Set<string>();
  private readonly releasedResultIds = new Set<string>();
  private readonly expectedCallIds = new Set<string>();
  private readonly expectedWatcherIds = new Set<string>();
  private readonly causality: ReplayCausality;
  // A replayed result may still be downloading before it can resume user code.
  private activeResultResolutions = 0;
  private callCursor = 0;
  private watcherCursor = 0;
  private prefixCursor = 0;
  private mismatch?: ReplayMismatchError;

  constructor(entries: Message[]) {
    this.causality = new ReplayCausality(() => this.failBlockedResultWithoutProducer());
    this.entries = entries.map((entry) => {
      if (entry.functionCallWatcherResult != null) {
        return { kind: "result", event: entry.functionCallWatcherResult, consumed: false };
      }
      if (entry.functionCallCreated != null) {
        const replayEntry: ReplayEntry = {
          kind: "call",
          event: entry.functionCallCreated,
          consumed: false,
        };
        this.calls.push(replayEntry);
        return replayEntry;
      }
      if (entry.functionCallWatcherCreated != null) {
        const replayEntry: ReplayEntry = {
          kind: "watcher",
          event: entry.functionCallWatcherCreated,
          consumed: false,
        };
        this.watchers.push(replayEntry);
        return replayEntry;
      }
      return { kind: "unknown", event: entry, consumed: false };
    });
  }

  async takeOrdered(kind: "call" | "watcher", id: string): Promise<Message | undefined> {
    this.advancePrefix();
    const entries = kind === "call" ? this.calls : this.watchers;
    const cursor = kind === "call" ? this.callCursor : this.watcherCursor;
    const expectedIds = kind === "call" ? this.expectedCallIds : this.expectedWatcherIds;
    if (cursor >= entries.length) {
      expectedIds.delete(id);
      if (this.prefixCursor === this.entries.length) return undefined;
      const blocked = this.entries[this.prefixCursor];
      // A new call may reach the live boundary before the watcher belonging to
      // an already-replayed call has finished being created. That watcher is a
      // known producer of replay progress, so wait for it without imposing a
      // wall-clock deadline. Every other cross-kind boundary is impossible for
      // the current execution order and must fail instead of hanging forever.
      if (
        kind !== "call"
        || blocked.kind !== "watcher"
        || !this.expectedWatcherIds.has(String(blocked.event.functionCallId ?? ""))
      ) {
        this.fail(
          `Unexpected ${kind} event for ${id} before replay ${blocked.kind}`
          + ` event ${String(blocked.event.functionCallId ?? "")}`,
        );
      }
      const waiter = deferred<void>();
      this.endWaiters.push(waiter);
      await waiter.promise;
      this.assertNoMismatch();
      return undefined;
    }
    const entry = entries[cursor];
    if (kind === "call") this.callCursor += 1;
    else this.watcherCursor += 1;
    if (entry.event.functionCallId !== id) {
      this.fail(`Expected replay ${kind} event for ${id}`);
    }
    expectedIds.delete(id);
    entry.consumed = true;
    if (kind === "watcher") {
      this.createdWatcherIds.add(id);
    }
    this.advancePrefix();
    return entry.event;
  }

  async takeResult(id: string): Promise<Message | undefined> {
    this.advancePrefix();
    const available = this.availableResults.get(id);
    if (available != null && available.length > 0) {
      const result = available.shift();
      if (result != null) this.activeResultResolutions += 1;
      return result;
    }
    if (this.prefixCursor === this.entries.length) return undefined;
    const blocked = this.entries[this.prefixCursor];
    const blockedId = String(blocked.event.functionCallId ?? "");
    const hasProducer = blocked.kind === "call"
      ? this.expectedCallIds.has(blockedId)
      : blocked.kind === "watcher"
        ? this.expectedWatcherIds.has(blockedId)
        : false;
    const waiter = deferred<Message | undefined>();
    const waiters = this.resultWaiters.get(id) ?? [];
    waiters.push(waiter);
    this.resultWaiters.set(id, waiters);
    if (!hasProducer) this.causality.requestCheck();
    const result = await waiter.promise;
    if (result != null) this.activeResultResolutions += 1;
    return result;
  }

  completeResultResolution(): void {
    if (this.activeResultResolutions === 0) return;
    this.activeResultResolutions -= 1;
    this.causality.requestCheck();
  }

  runCausalHandler<T>(callback: () => Promise<T>): Promise<T> {
    return this.causality.runRoot(callback);
  }

  wrapDurableResult<T>(source: Promise<T>): Promise<T> {
    return new ReplayAwarePromise(source, this.causality);
  }

  trackMixedPromiseRace(
    promise: Promise<unknown>,
    producers?: readonly Promise<unknown>[],
  ): void {
    this.causality.trackMixedPromiseRace(promise, producers);
  }

  trackPromiseSettlement(
    promise: Promise<unknown>,
    onSettled: () => void,
  ): boolean {
    return this.causality.trackPromiseSettlement(promise, onSettled);
  }

  trackDurablePromiseConsumption(promise: Promise<unknown>): void {
    this.causality.trackDurablePromiseConsumption(promise);
  }

  runPromiseAggregateConstruction<T>(callback: () => T): T {
    return this.causality.runPromiseAggregateConstruction(callback);
  }

  runWithoutCausalOwnership<T>(callback: () => T): T {
    return this.causality.runWithoutOwnership(callback);
  }

  dispose(): void {
    this.causality.stop();
  }

  expectCall(id: string): void {
    this.expectedCallIds.add(id);
  }

  expectWatcher(id: string): void {
    this.expectedWatcherIds.add(id);
  }

  abandonCall(id: string): void {
    this.abandonOrdered("call", id);
  }

  abandonWatcher(id: string): void {
    this.abandonOrdered("watcher", id);
  }

  private abandonOrdered(kind: "call" | "watcher", id: string): void {
    const expectedIds = kind === "call" ? this.expectedCallIds : this.expectedWatcherIds;
    expectedIds.delete(id);
    const hasWaiters = this.endWaiters.length > 0
      || [...this.resultWaiters.values()].some((waiters) => waiters.length > 0);
    if (this.prefixCursor >= this.entries.length || !hasWaiters) return;
    const blocked = this.entries[this.prefixCursor];
    if (blocked.kind === kind && blocked.event.functionCallId === id) {
      this.fail(`Replay ${kind} event for ${id} can no longer be created`);
    }
  }

  private failBlockedResultWithoutProducer(): void {
    if (this.activeResultResolutions > 0) return;
    // Quiescence is only evidence of a replay cycle while application code is
    // actually assimilating a durable result. Outside that state the handler
    // may legitimately be waiting for a callback owned by a module-initialized
    // timer, socket, or client which predates this allocation and is therefore
    // absent from the causal async-resource tree.
    if (!this.causality.hasPendingDurableAssimilation()) return;
    if (this.causality.hasPendingProducer()) return;
    if (this.prefixCursor >= this.entries.length) return;
    if (![...this.resultWaiters.values()].some((waiters) => waiters.length > 0)) return;
    const blocked = this.entries[this.prefixCursor];
    const blockedId = String(blocked.event.functionCallId ?? "");
    const hasProducer = blocked.kind === "call"
      ? this.expectedCallIds.has(blockedId)
      : blocked.kind === "watcher"
        ? this.expectedWatcherIds.has(blockedId)
        : false;
    if (!hasProducer) {
      this.recordMismatch(
        `Replay result is blocked behind unreachable ${blocked.kind}`
        + ` event ${blockedId}`,
      );
    }
  }

  hasAvailableResult(id: string): boolean {
    return (this.availableResults.get(id)?.length ?? 0) > 0;
  }

  assertNoMismatch(): void {
    if (this.mismatch != null) throw this.mismatch;
  }

  assertConsumed(): void {
    this.assertNoMismatch();
    this.advancePrefix();
    if (this.prefixCursor !== this.entries.length) {
      throw new ReplayMismatchError("Function completed before its durable event history was consumed");
    }
  }

  private advancePrefix(): void {
    this.assertNoMismatch();
    while (this.prefixCursor < this.entries.length) {
      const entry = this.entries[this.prefixCursor];
      if (entry.kind === "unknown") {
        this.fail("Replay history contains an unknown allocation event");
      }
      if (entry.kind === "call" || entry.kind === "watcher") {
        if (!entry.consumed) return;
        this.prefixCursor += 1;
        continue;
      }
      entry.consumed = true;
      this.prefixCursor += 1;
      this.releaseResult(entry.event);
    }
    for (const waiter of this.endWaiters.splice(0)) waiter.resolve();
    for (const waiters of this.resultWaiters.values()) {
      for (const waiter of waiters) waiter.resolve(undefined);
    }
    this.resultWaiters.clear();
  }

  private releaseResult(event: Message): void {
    const id = String(event.functionCallId ?? "");
    if (!this.createdWatcherIds.has(id)) {
      this.fail(`Replay watcher result for ${id} appeared before its watcher was created`);
    }
    if (this.releasedResultIds.has(id)) {
      this.fail(`Replay history contains duplicate watcher results for ${id}`);
    }
    this.releasedResultIds.add(id);
    const waiter = this.resultWaiters.get(id)?.shift();
    if (waiter != null) {
      waiter.resolve(event);
      if (this.resultWaiters.get(id)?.length === 0) this.resultWaiters.delete(id);
      return;
    }
    const available = this.availableResults.get(id) ?? [];
    available.push(event);
    this.availableResults.set(id, available);
  }

  private fail(message: string): never {
    throw this.recordMismatch(message);
  }

  private recordMismatch(message: string): ReplayMismatchError {
    this.mismatch ??= new ReplayMismatchError(message);
    this.causality.stop();
    for (const waiter of this.endWaiters.splice(0)) waiter.reject(this.mismatch);
    for (const waiters of this.resultWaiters.values()) {
      for (const waiter of waiters) waiter.reject(this.mismatch);
    }
    this.resultWaiters.clear();
    return this.mismatch;
  }
}

interface EventWaiter {
  predicate(event: Message): boolean;
  result: Deferred<Message>;
}

interface EmissionTurn {
  wait: Promise<void>;
  release(): void;
}

interface StateStreamFlow {
  backpressured: boolean;
  pending?: Message;
}

class AllocationProtocolError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "AllocationProtocolError";
  }
}

export class AllocationRunner implements FunctionRuntime {
  readonly allocation: Message;
  private readonly functionRef: Message;
  private readonly definition: RegisteredDefinition;
  private readonly applicationDefinition: RegisteredDefinition;
  private readonly stateStreams = new Set<StreamCall>();
  private readonly stateStreamFlows = new Map<StreamCall, StateStreamFlow>();
  private readonly eventReadStreams = new Set<StreamCall>();
  private readonly outputBlobRequests = new Map<string, Deferred<Message>>();
  private readonly stateOperationRequests = new Map<string, Deferred<Message>>();
  private readonly executionBatches: Message[][] = [];
  private readonly executionBatchWaiters: Array<(events: Message[]) => void> = [];
  private readonly liveBacklog: Message[] = [];
  private readonly liveWaiters: EventWaiter[] = [];
  private readonly controller = new AbortController();
  private currentRead?: { request: Message; response: Deferred<Message> };
  private replay?: ReplayHistory;
  private protocolError?: AllocationProtocolError;
  private durableCallEmissionTail: Promise<void> = Promise.resolve();
  private durableWatcherEmissionTail: Promise<void> = Promise.resolve();
  private lastEventClock = 0;
  private previousDurableId: string;
  private livePump?: Promise<void>;
  private completion?: Promise<void>;
  private started = false;
  private terminalBatchQueued = false;
  private terminalBatchObserved = false;
  private readonly terminalBatchObservedWaiters: Array<() => void> = [];
  private finished = false;
  private state: Message = { outputBlobRequests: [], requestStateOperations: [] };
  private readonly startedAt = Date.now();

  constructor(
    allocation: Message,
    functionRef: Message,
    definition: RegisteredDefinition,
    applicationDefinition: RegisteredDefinition = definition,
  ) {
    this.allocation = allocation;
    this.functionRef = functionRef;
    this.definition = definition;
    this.applicationDefinition = applicationDefinition;
    this.previousDurableId = String(allocation.functionCallId);
    this.updateStateHash();
    this.log("debug", "allocation runner created", {
      replay_mode: allocation.replayMode,
      input_count: allocation.inputs?.args?.length ?? 0,
      input_blob_count: allocation.inputs?.argBlobs?.length ?? 0,
    });
  }

  start(): void {
    if (this.started) {
      this.log("warn", "duplicate allocation runner start ignored");
      return;
    }
    this.started = true;
    this.log("info", "allocation runner scheduled");
    this.completion = this.run().catch((error) => {
      this.log("error", "allocation runner promise rejected", {}, error);
      this.queueTerminalBatch({
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_INTERNAL_ERROR",
      });
      this.finish();
    });
  }

  waitForCompletion(): Promise<void> {
    return this.completion ?? Promise.resolve();
  }

  async waitForTerminalBatchObserved(timeoutMs: number): Promise<boolean> {
    if (this.terminalBatchObserved) return true;
    return new Promise<boolean>((resolve) => {
      let settled = false;
      const observed = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve(true);
      };
      const timer = setTimeout(() => {
        if (settled) return;
        settled = true;
        const index = this.terminalBatchObservedWaiters.indexOf(observed);
        if (index >= 0) this.terminalBatchObservedWaiters.splice(index, 1);
        resolve(false);
      }, Math.max(timeoutMs, 0));
      this.terminalBatchObservedWaiters.push(observed);
      // Close the race where observation happens between the first check and
      // registering this waiter.
      if (this.terminalBatchObserved) observed();
    });
  }

  cancel(reason: unknown = new FunctionError("Allocation was cancelled")): void {
    if (this.controller.signal.aborted) return;
    this.log("info", "allocation cancellation requested", {
      reason: reason instanceof Error ? reason.message : String(reason),
    });
    this.abortExecution(reason);
  }

  private abortExecution(reason: unknown): void {
    if (!this.controller.signal.aborted) this.controller.abort(reason);
    const rejection = this.controller.signal.reason ?? reason;
    const outputBlobRequestCount = this.outputBlobRequests.size;
    const stateOperationRequestCount = this.stateOperationRequests.size;
    const liveWaiterCount = this.liveWaiters.length;
    const liveBacklogCount = this.liveBacklog.length;
    const hadCurrentRead = this.currentRead != null;
    for (const request of this.outputBlobRequests.values()) request.reject(rejection);
    for (const request of this.stateOperationRequests.values()) request.reject(rejection);
    for (const waiter of this.liveWaiters.splice(0)) waiter.result.reject(rejection);
    this.currentRead?.response.reject(rejection);
    this.outputBlobRequests.clear();
    this.stateOperationRequests.clear();
    this.liveBacklog.splice(0);
    this.currentRead = undefined;
    this.state.outputBlobRequests = [];
    this.state.requestStateOperations = [];
    this.log("debug", "pending allocation protocol work settled", {
      output_blob_requests: outputBlobRequestCount,
      state_operation_requests: stateOperationRequestCount,
      durable_event_waiters: liveWaiterCount,
      durable_event_backlog_entries: liveBacklogCount,
      event_log_read: hadCurrentRead,
    });
  }

  invoke<T>(definition: RegisteredDefinition, args: readonly unknown[]): Promise<T> {
    this.log("debug", "durable function invocation requested", {
      target_function: definition.name,
      argument_count: args.length,
    });
    const result = this.runChild<T>(definition, args, 0);
    return this.replay?.wrapDurableResult(result) ?? result;
  }

  runFuture<T>(future: FunctionFuture<T>): Promise<T> {
    this.log("debug", "durable future execution requested", {
      target_function: future.definition.name,
      argument_count: future.args.length,
      delay_seconds: future.delaySeconds,
    });
    const result = this.runChild<T>(future.definition, future.args, future.delaySeconds);
    return this.replay?.wrapDurableResult(result) ?? result;
  }

  wrapFutureWait<T>(promise: Promise<T>): Promise<T> {
    return this.replay?.wrapDurableResult(promise) ?? promise;
  }

  trackMixedPromiseRace(
    promise: Promise<unknown>,
    producers?: readonly Promise<unknown>[],
  ): void {
    this.replay?.trackMixedPromiseRace(promise, producers);
  }

  trackPromiseSettlement(
    promise: Promise<unknown>,
    onSettled: () => void,
  ): boolean {
    return this.replay?.trackPromiseSettlement(promise, onSettled) ?? false;
  }

  trackDurablePromiseConsumption(promise: Promise<unknown>): void {
    this.replay?.trackDurablePromiseConsumption(promise);
  }

  runPromiseAggregateConstruction<T>(callback: () => T): T {
    return this.replay?.runPromiseAggregateConstruction(callback) ?? callback();
  }

  reduce<T>(
    definition: RegisteredDefinition,
    items: readonly unknown[],
    initial: unknown,
    hasInitial: boolean,
  ): Promise<T> {
    const result = this.runReduce<T>(definition, items, initial, hasInitial);
    return this.replay?.wrapDurableResult(result) ?? result;
  }

  private async runReduce<T>(
    definition: RegisteredDefinition,
    items: readonly unknown[],
    initial: unknown,
    hasInitial: boolean,
  ): Promise<T> {
    let reduceItems = items;
    let reduceInitial = initial;
    if (!hasInitial) {
      if (items.length === 0) {
        throw new SDKUsageError("reduce of empty iterable with no initial value");
      }
      [reduceInitial, ...reduceItems] = items;
    }
    if (reduceItems.length === 0) return reduceInitial as T;
    const callTurn = this.reserveEmissionTurn("call");
    const watcherTurn = this.reserveEmissionTurn("watcher");
    const ids = reduceItems.map(() => this.nextDurableId(definition, "ReduceStep"));
    const id = ids[ids.length - 1];
    const startedAt = Date.now();
    this.log("info", "durable reduce starting", {
      durable_id: id,
      target_function: definition.name,
      collection_size: reduceItems.length,
      plan_count: Math.ceil(reduceItems.length / MAX_REDUCE_CALLS_PER_PLAN),
    });
    const replayCallIds = ids.filter((_id, index) =>
      (index + 1) % MAX_REDUCE_CALLS_PER_PLAN === 0 || index === ids.length - 1
    );
    for (const replayCallId of replayCallIds) this.replay?.expectCall(replayCallId);
    this.replay?.expectWatcher(id);
    try {
      try {
        await this.createReduceChain(ids, definition, reduceItems, reduceInitial, callTurn);
      } catch (error) {
        for (const replayCallId of replayCallIds) this.replay?.abandonCall(replayCallId);
        this.replay?.abandonWatcher(id);
        throw error;
      }
      await this.createWatcher(id, watcherTurn);
    } finally {
      callTurn.release();
      watcherTurn.release();
    }
    const replayedResult = this.replay == null ? undefined : await this.replay.takeResult(id);
    const result = replayedResult ?? await this.waitForLiveEvent(
      (entry) => entry.functionCallWatcherResult?.functionCallId === id,
    ).then((entry) => entry.functionCallWatcherResult);
    let resolved: T;
    try {
      resolved = await this.resolveWatcherResult<T>(result);
    } finally {
      if (replayedResult != null) this.replay?.completeResultResolution();
    }
    this.log("info", "durable reduce completed", {
      durable_id: id,
      target_function: definition.name,
      collection_size: reduceItems.length,
      duration_ms: Date.now() - startedAt,
    });
    return resolved;
  }

  private nextDurableId(definition: RegisteredDefinition, operation = "FunctionCall"): string {
    const id = durableHash([
      String(this.allocation.functionCallId),
      this.previousDurableId,
      operation,
      definition.name,
    ]);
    this.previousDurableId = id;
    return id;
  }

  private async runChild<T>(
    definition: RegisteredDefinition,
    args: readonly unknown[],
    delaySeconds: number,
  ): Promise<T> {
    const callTurn = this.reserveEmissionTurn("call");
    const watcherTurn = this.reserveEmissionTurn("watcher");
    const id = this.nextDurableId(definition);
    const startedAt = Date.now();
    const maxRetries = definition.options.retries?.maxRetries
      ?? this.applicationDefinition.application?.retries.maxRetries
      ?? 0;
    this.log("info", "durable function call starting", {
      durable_id: id,
      target_function: definition.name,
      argument_count: args.length,
      delay_seconds: delaySeconds,
      max_retries: maxRetries,
    });
    this.replay?.expectCall(id);
    this.replay?.expectWatcher(id);
    try {
      try {
        await this.createChildCall(id, definition, args, delaySeconds, callTurn);
      } catch (error) {
        this.replay?.abandonCall(id);
        this.replay?.abandonWatcher(id);
        throw error;
      }
      await this.createWatcher(id, watcherTurn);
    } finally {
      callTurn.release();
      watcherTurn.release();
    }
    this.log("debug", "waiting for terminal durable function result", {
      durable_id: id,
      target_function: definition.name,
      max_retries: maxRetries,
      replay_result_available: this.replay?.hasAvailableResult(id) ?? false,
    });
    // The server owns allocation retry policy and retains the watcher route
    // across retryable attempt failures. It emits exactly one result here:
    // the eventual success or terminal failure.
    const replayedResult = this.replay == null ? undefined : await this.replay.takeResult(id);
    const result = replayedResult ?? await this.waitForLiveEvent(
      (entry) => entry.functionCallWatcherResult?.functionCallId === id,
    ).then((entry) => entry.functionCallWatcherResult);
    let resolved: T;
    try {
      resolved = await this.resolveWatcherResult<T>(result);
    } finally {
      if (replayedResult != null) this.replay?.completeResultResolution();
    }
    this.log("info", "durable function call completed", {
      durable_id: id,
      target_function: definition.name,
      max_retries: maxRetries,
      duration_ms: Date.now() - startedAt,
    });
    return resolved;
  }

  private async createChildCall(
    id: string,
    definition: RegisteredDefinition,
    args: readonly unknown[],
    delaySeconds: number,
    turn: EmissionTurn,
  ): Promise<void> {
    try {
      let turnAcquired = false;
      if (this.replay != null) {
        await turn.wait;
        turnAcquired = true;
      }
      const replayed = await this.replay?.takeOrdered("call", id);
      if (replayed != null) {
        this.log("debug", "replaying durable function creation", { durable_id: id });
        const error = statusError(replayed.status, "Failed to start function call");
        if (error != null) throw error;
        return;
      }
      const prepared: PreparedSerializedObject[] = [];
      let offset = 0;
      for (const arg of args) {
        const item = prepareSerializedObject(arg, offset);
        prepared.push(item);
        offset += item.bytes.byteLength;
      }
      let argsBlob: BlobValue | undefined;
      if (prepared.length > 0) {
        this.log("debug", "requesting durable function arguments blob", {
          durable_id: id,
          argument_count: args.length,
          serialized_bytes: offset,
        });
        try {
          argsBlob = await this.requestOutputBlob(offset);
          argsBlob = await this.uploadBlob(
            argsBlob,
            joinPrepared(prepared),
            "durable function arguments",
            { durable_id: id },
          );
        } catch (error) {
          if (this.controller.signal.aborted) throw this.controller.signal.reason;
          throw this.latchProtocolError(
            `Failed to materialize durable function arguments for '${id}'`,
            error,
          );
        }
      }
      const metadata = Buffer.from(JSON.stringify({
        format: "tensorlake.typescript.function-call.v1",
        id,
        functionName: definition.name,
        argumentCount: args.length,
      }), "utf8");
      const updates: Message = {
        updates: [{ functionCall: {
          id,
          target: {
            namespace: this.functionRef.namespace,
            applicationName: this.functionRef.applicationName,
            applicationVersion: this.functionRef.applicationVersion,
            functionName: definition.name,
          },
          args: prepared.map((item) => ({ value: item.object })),
          callMetadata: metadata,
        } }],
        rootFunctionCallId: id,
      };
      if (delaySeconds > 0) updates.startAt = protocolTimestamp(new Date(Date.now() + delaySeconds * 1000));
      if (!turnAcquired) await turn.wait;
      this.log("debug", "queueing durable function creation event", {
        durable_id: id,
        target_function: definition.name,
        delay_seconds: delaySeconds,
      });
      this.addBatch([{ createFunctionCall: { updates, argsBlob } }]);
      this.log("debug", "waiting for durable function creation acknowledgement", { durable_id: id });
      const createdPromise = this.waitForLiveEvent((entry) => entry.functionCallCreated?.functionCallId === id);
      turn.release();
      const created = await createdPromise;
      const error = statusError(created.functionCallCreated?.status, "Failed to start function call");
      if (error != null) throw error;
    } finally {
      turn.release();
    }
  }

  private async createReduceChain(
    ids: readonly string[],
    definition: RegisteredDefinition,
    items: readonly unknown[],
    initial: unknown,
    turn: EmissionTurn,
  ): Promise<void> {
    try {
      let turnAcquired = false;
      if (this.replay != null) {
        await turn.wait;
        turnAcquired = true;
      }
      const livePlans: Message[] = [];
      const liveRoots: string[] = [];
      for (let start = 0; start < items.length; start += MAX_REDUCE_CALLS_PER_PLAN) {
        const end = Math.min(items.length, start + MAX_REDUCE_CALLS_PER_PLAN);
        const rootId = ids[end - 1];
        const replayed = await this.replay?.takeOrdered("call", rootId);
        if (replayed != null) {
          this.log("debug", "replaying durable reduce plan creation", {
            durable_id: rootId,
            reduce_step_start: start,
            reduce_step_end: end,
          });
          const error = statusError(replayed.status, "Failed to start reduce operation");
          if (error != null) throw error;
          continue;
        }

        const prepared: PreparedSerializedObject[] = [];
        let offset = 0;
        if (start === 0) {
          const value = prepareSerializedObject(initial, offset);
          prepared.push(value);
          offset += value.bytes.byteLength;
        }
        for (let index = start; index < end; index += 1) {
          const value = prepareSerializedObject(items[index], offset);
          prepared.push(value);
          offset += value.bytes.byteLength;
        }
        let argsBlob: BlobValue;
        try {
          const requested = await this.requestOutputBlob(offset);
          argsBlob = await this.uploadBlob(
            requested,
            joinPrepared(prepared),
            "durable reduce arguments",
            { durable_id: rootId, reduce_step_start: start, reduce_step_end: end },
          );
        } catch (error) {
          if (this.controller.signal.aborted) throw this.controller.signal.reason;
          throw this.latchProtocolError(
            `Failed to materialize durable reduce arguments for '${rootId}'`,
            error,
          );
        }
        const itemOffset = start === 0 ? 1 : 0;
        const updates = [];
        for (let index = start; index < end; index += 1) {
          const id = ids[index];
          const accumulator = index === 0
            ? { value: prepared[0].object }
            : { functionCallId: ids[index - 1] };
          const item = prepared[itemOffset + index - start];
          const metadata = Buffer.from(JSON.stringify({
            format: "tensorlake.typescript.function-call.v1",
            id,
            functionName: definition.name,
            argumentCount: 2,
            operation: "reduce",
            reduceRootId: ids[ids.length - 1],
            reduceStep: index,
            reduceStepCount: items.length,
          }), "utf8");
          updates.push({ functionCall: {
            id,
            target: {
              namespace: this.functionRef.namespace,
              applicationName: this.functionRef.applicationName,
              applicationVersion: this.functionRef.applicationVersion,
              functionName: definition.name,
            },
            args: [accumulator, { value: item.object }],
            callMetadata: metadata,
          } });
        }
        livePlans.push({
          createFunctionCall: {
            updates: { updates, rootFunctionCallId: rootId },
            argsBlob,
          },
        });
        liveRoots.push(rootId);
      }

      if (livePlans.length === 0) return;
      if (!turnAcquired) await turn.wait;
      this.log("debug", "queueing durable reduce function-call chains", {
        durable_id: ids[ids.length - 1],
        target_function: definition.name,
        reduce_step_count: items.length,
        plan_count: livePlans.length,
      });
      this.addBatch(livePlans);
      const creationWaiters = liveRoots.map((rootId) => {
        const predicate = (entry: Message) =>
          entry.functionCallCreated?.functionCallId === rootId;
        return {
          predicate,
          promise: this.waitForLiveEvent(predicate),
        };
      });
      const createdPromises = creationWaiters.map(async ({ promise }) => {
        const created = await promise;
        const error = statusError(
          created.functionCallCreated?.status,
          "Failed to start reduce operation",
        );
        if (error != null) throw error;
      });
      turn.release();
      try {
        await Promise.all(createdPromises);
      } catch (error) {
        // One failed plan is terminal for the whole reduce. Remove and reject
        // sibling plan waiters immediately: the server is not required to
        // acknowledge plans after it has rejected the batch.
        for (const { predicate } of creationWaiters) {
          const index = this.liveWaiters.findIndex(
            (waiter) => waiter.predicate === predicate,
          );
          if (index >= 0) this.liveWaiters.splice(index, 1)[0].result.reject(error);
        }
        throw error;
      }
    } finally {
      turn.release();
    }
  }

  private async createWatcher(id: string, turn: EmissionTurn): Promise<void> {
    try {
      await turn.wait;
      const replayed = await this.replay?.takeOrdered("watcher", id);
      if (replayed != null) {
        this.log("debug", "replaying durable function watcher creation", { durable_id: id });
        const error = statusError(replayed.status, "Failed to create function call watcher");
        if (error != null) throw error;
        return;
      }
      this.log("debug", "queueing durable function watcher event", { durable_id: id });
      this.addBatch([{ createFunctionCallWatcher: { functionCallId: id } }]);
      this.log("debug", "waiting for durable function watcher acknowledgement", { durable_id: id });
      const createdPromise = this.waitForLiveEvent(
        (entry) => entry.functionCallWatcherCreated?.functionCallId === id,
      );
      turn.release();
      const created = await createdPromise;
      const error = statusError(created.functionCallWatcherCreated?.status, "Failed to create function call watcher");
      if (error != null) throw error;
    } finally {
      turn.release();
    }
  }

  private async resolveWatcherResult<T>(event: Message): Promise<T> {
    this.log("debug", "resolving durable function watcher result", {
      durable_id: event.functionCallId,
      watcher_status: event.watcherStatus,
      outcome_code: event.outcomeCode,
      has_value_output: event.valueOutput != null,
      has_request_error_output: event.requestErrorOutput != null,
    });
    if (WATCHER_TIMED_OUT.has(event.watcherStatus)) throw new TimeoutError();
    if (OUTCOME_SUCCESS.has(event.outcomeCode)) {
      try {
        const downloaded = await this.downloadSerializedObject(
          event.valueOutput,
          event.valueBlob,
          "durable function result",
          { durable_id: event.functionCallId },
        );
        return deserializeValueFromProtocol(downloaded) as T;
      } catch (error) {
        if (this.controller.signal.aborted) throw this.controller.signal.reason;
        throw this.latchProtocolError(
          `Invalid successful function call watcher payload for '${String(event.functionCallId)}'`,
          error,
        );
      }
    }
    if (OUTCOME_FAILURE.has(event.outcomeCode)) {
      if (event.requestErrorOutput != null) {
        let message: string;
        try {
          const downloaded = await this.downloadSerializedObject(
            event.requestErrorOutput,
            event.requestErrorBlob,
            "durable function request error",
            { durable_id: event.functionCallId },
          );
          message = new TextDecoder("utf-8", { fatal: true }).decode(downloaded.data);
        } catch (error) {
          if (this.controller.signal.aborted) throw this.controller.signal.reason;
          throw this.latchProtocolError(
            `Invalid request-error function call watcher payload for '${String(event.functionCallId)}'`,
            error,
          );
        }
        throw new RequestError(message);
      }
      throw new FunctionError("Function call failed");
    }
    throw this.latchProtocolError(
      `Unexpected function call watcher outcome '${String(event.outcomeCode)}'`,
    );
  }

  private latchProtocolError(
    message: string | AllocationProtocolError,
    cause?: unknown,
  ): AllocationProtocolError {
    const protocolError = this.protocolError
      ?? (message instanceof AllocationProtocolError
        ? message
        : new AllocationProtocolError(
          message,
          cause === undefined ? undefined : { cause },
        ));
    // Protocol failures are executor invariants, not child-function failures.
    // Latch the first one so application code cannot turn corrupt event data
    // into a successful allocation or hang indefinitely after catching the
    // rejected child promise.
    this.protocolError = protocolError;
    this.abortExecution(protocolError);
    return protocolError;
  }

  private async startTailCall(tailCall: TailCall<unknown>): Promise<string> {
    const callTurn = this.reserveEmissionTurn("call");
    const id = this.nextDurableId(tailCall.definition);
    this.log("info", "tail call starting", {
      durable_id: id,
      target_function: tailCall.definition.name,
      argument_count: tailCall.args.length,
    });
    this.replay?.expectCall(id);
    try {
      await this.createChildCall(id, tailCall.definition, tailCall.args, 0, callTurn);
    } catch (error) {
      this.replay?.abandonCall(id);
      throw error;
    }
    return id;
  }

  private reserveEmissionTurn(kind: "call" | "watcher"): EmissionTurn {
    const previous = kind === "call" ? this.durableCallEmissionTail : this.durableWatcherEmissionTail;
    let signalRelease!: () => void;
    const released = new Promise<void>((resolve) => { signalRelease = resolve; });
    const tail = previous.then(() => released);
    if (kind === "call") this.durableCallEmissionTail = tail;
    else this.durableWatcherEmissionTail = tail;
    let didRelease = false;
    return {
      wait: previous,
      release: () => {
        if (didRelease) return;
        didRelease = true;
        signalRelease();
      },
    };
  }

  private async run(): Promise<void> {
    let handlerArgs: unknown[];
    this.log("info", "allocation execution started", {
      replay_mode: this.allocation.replayMode,
      function: this.functionRef.functionName,
    });
    this.emitLifecycleEvent("allocations_started", "Starting allocations");
    try {
      const inputs = this.allocation.inputs ?? {};
      const objects = inputs.args ?? [];
      const blobs = inputs.argBlobs ?? [];
      this.log("debug", "allocation inputs validated", {
        object_count: objects.length,
        blob_count: blobs.length,
        function_call_metadata_bytes: inputs.functionCallMetadata?.length ?? 0,
      });
      if (objects.length !== blobs.length) throw new Error("Function argument and BLOB counts differ");
      const values = await Promise.all(objects.map(
        (object: SerializedObjectInsideBlobValue, index: number) => this.downloadSerializedObject(
          object,
          blobs[index],
          "function input",
          { input_index: index },
        ),
      ));
      this.log("info", "all function inputs downloaded", {
        input_count: values.length,
        total_data_bytes: values.reduce((total, value) => total + value.data.byteLength, 0),
      });
      const metadata = Buffer.from(inputs.functionCallMetadata ?? []);
      if (metadata.byteLength === 0) {
        this.log("debug", "deserializing application invocation input", {
          content_type: values[0]?.contentType,
        });
        if (values.length !== 1) throw new Error("Application calls require exactly one input payload");
        try {
          const parsed = await deserializeApplicationArguments(this.definition, values[0]);
          handlerArgs = parsed.args;
        } catch (error) {
          this.log("error", "application invocation input deserialization failed", {}, error);
          await this.finishUserError(error);
          return;
        }
        this.log("debug", "application invocation input deserialized", {
          handler_argument_count: handlerArgs.length,
        });
      } else {
        this.log("debug", "deserializing internal function invocation inputs", {
          metadata_bytes: metadata.byteLength,
        });
        const call = JSON.parse(metadata.toString("utf8")) as Message;
        if (call.format !== "tensorlake.typescript.function-call.v1") {
          throw new Error("Function call metadata was produced by a different SDK language");
        }
        if (call.argumentCount !== values.length) throw new Error("Function call metadata argument count differs");
        try {
          handlerArgs = values.map(deserializeValueFromProtocol);
        } catch (error) {
          this.log("error", "internal function argument deserialization failed", {}, error);
          await this.finishUserError(error);
          return;
        }
        this.log("debug", "internal function invocation inputs deserialized", {
          handler_argument_count: handlerArgs.length,
          metadata_function: call.functionName,
        });
      }

      if (REPLAY_STRICT.has(this.allocation.replayMode)) {
        this.log("info", "strict replay history loading");
        await this.loadReplayHistory();
      } else {
        this.log("debug", "strict replay disabled");
      }
      const requestContext = this.createRequestContext();
      let result: unknown | TailCall<unknown>;
      const handlerStartedAt = Date.now();
      this.log("info", "user function handler starting", {
        handler_argument_count: handlerArgs.length,
      });
      try {
        const execute = () => executeHandlerResult(this.definition, handlerArgs);
        result = await runWithRequestContext(requestContext, () =>
          runWithFunctionRuntime(this, () => waitWithAbortSignal(
            this.replay?.runCausalHandler(execute) ?? execute(),
            this.controller.signal,
          )),
        );
      } catch (error) {
        if (error instanceof ReplayMismatchError || error instanceof AllocationProtocolError) {
          throw error;
        }
        // Promise combinators such as Promise.any can wrap the replay waiter's
        // ReplayMismatchError (for example in an AggregateError). Preserve the
        // allocation-level replay classification even when user-level promise
        // machinery changes the surfaced error object.
        this.replay?.assertNoMismatch();
        this.log("error", "user function handler failed", {
          duration_ms: Date.now() - handlerStartedAt,
        }, error);
        await this.finishUserError(error);
        return;
      }
      this.log("info", "user function handler completed", {
        duration_ms: Date.now() - handlerStartedAt,
        result_kind: isTailCall(result)
          ? "tail_call"
          : "value",
        result_type: result == null ? String(result) : typeof result,
      });
      if (this.protocolError != null) throw this.protocolError;
      this.replay?.assertNoMismatch();
      if (isTailCall(result)) {
        const id = await this.startTailCall(result);
        this.replay?.assertConsumed();
        this.controller.signal.throwIfAborted();
        this.log("info", "queueing successful tail-call allocation finish", { tail_call_durable_id: id });
        this.queueTerminalBatch({
          outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
          tailCallDurableId: id,
        });
      } else {
        this.replay?.assertConsumed();
        let prepared: PreparedSerializedObject;
        try {
          prepared = prepareSerializedObject(result, 0, String(this.allocation.functionCallId));
        } catch (error) {
          this.log("error", "function output serialization failed", {}, error);
          await this.finishUserError(error);
          return;
        }
        this.log("debug", "function output serialized", {
          output_bytes: prepared.bytes.byteLength,
          output_encoding: prepared.object.manifest?.encoding,
          output_content_type: prepared.object.manifest?.contentType,
        });
        let outputBlob = await this.requestOutputBlob(prepared.bytes.byteLength);
        outputBlob = await this.uploadBlob(outputBlob, prepared.bytes, "function output");
        this.controller.signal.throwIfAborted();
        this.log("info", "queueing successful value allocation finish", {
          output_blob_id: outputBlob.id,
          output_bytes: prepared.bytes.byteLength,
        });
        this.queueTerminalBatch({
          outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
          value: prepared.object,
          uploadedFunctionOutputsBlob: outputBlob,
        });
      }
    } catch (error) {
      const replayMismatch = error instanceof ReplayMismatchError;
      const protocolFailure =
        error instanceof AllocationProtocolError || this.protocolError != null;
      const cancelled = this.controller.signal.aborted && !protocolFailure;
      this.log(
        cancelled ? "info" : "error",
        cancelled
          ? "allocation stopped after cancellation"
          : replayMismatch
            ? "durable replay mismatch"
            : "allocation internal failure",
        {},
        error,
      );
      this.queueTerminalBatch({
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: replayMismatch
          ? "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH"
          : protocolFailure
          ? "ALLOCATION_FAILURE_REASON_INTERNAL_ERROR"
          : cancelled
          ? "ALLOCATION_FAILURE_REASON_FUNCTION_ERROR"
          : "ALLOCATION_FAILURE_REASON_INTERNAL_ERROR",
      });
    } finally {
      this.log("info", "allocation execution stopping", {
        duration_ms: Date.now() - this.startedAt,
        pending_output_blob_requests: this.outputBlobRequests.size,
        pending_state_operations: this.stateOperationRequests.size,
        pending_event_waiters: this.liveWaiters.length,
      });
      this.emitLifecycleEvent("allocations_finished", "Allocations completed");
      this.finish();
      this.replay?.dispose();
    }
  }

  private async finishUserError(error: unknown): Promise<void> {
    const rendered = error instanceof Error ? error.message : String(error);
    this.runWithoutReplayOwnership(() => {
      printCloudEvent({
        level: "error",
        event: "function_call_failed",
        message: rendered,
        namespace: this.functionRef.namespace,
        application: this.functionRef.applicationName,
        application_version: this.functionRef.applicationVersion,
        function: this.functionRef.functionName,
        request_id: this.allocation.requestId,
        function_call_id: this.allocation.functionCallId,
        allocation_id: this.allocation.allocationId,
        error: error instanceof Error ? error.stack?.split("\n") ?? [rendered] : [rendered],
      });
    });
    if (isRequestError(error)) {
      const output = prepareTextObject(error.message);
      const requestErrorBlob = this.allocation.inputs?.requestErrorBlob as BlobValue | undefined;
      if (requestErrorBlob == null) throw new Error("Allocation has no request error BLOB");
      const uploaded = await this.uploadBlob(requestErrorBlob, output.bytes, "request error output");
      this.log("info", "queueing request-error allocation finish", {
        request_error_bytes: output.bytes.byteLength,
        request_error_blob_id: uploaded.id,
      });
      this.queueTerminalBatch({
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_REQUEST_ERROR",
        requestErrorOutput: output.object,
        uploadedRequestErrorBlob: uploaded,
      });
      return;
    }
    this.log("info", "queueing function-error allocation finish", {}, error);
    this.queueTerminalBatch({
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_FUNCTION_ERROR",
    });
  }

  private createRequestContext(): RequestContextValue {
    return {
      requestId: String(this.allocation.requestId),
      headers: new Headers(
        (this.allocation.inputs?.requestContext?.headers ?? []).map(
          (header: Message) => [
            String(header.name ?? ""),
            String(header.value ?? ""),
          ],
        ),
      ),
      signal: this.controller.signal,
      state: {
        get: async <T>(key: string, defaultValue?: T) => {
          validateRequestStateKey(key);
          const operationId = randomUUID();
          this.log("debug", "request state read starting", {
            state_key: key,
            operation_id: operationId,
            has_default: defaultValue !== undefined,
          });
          const result = await this.requestStateOperation({ operationId, stateKey: key, prepareRead: {} });
          if (result.status?.code === NOT_FOUND) {
            this.log("debug", "request state key not found", { state_key: key, operation_id: operationId });
            return defaultValue;
          }
          const error = statusError(result.status, `Failed to read request state '${key}'`);
          if (error != null) throw error;
          const data = await this.downloadBlob(result.prepareRead?.blob, "request state value", {
            state_key: key,
            operation_id: operationId,
          });
          const value = deserializeJSON(data) as T;
          this.log("debug", "request state read completed", {
            state_key: key,
            operation_id: operationId,
            value_bytes: data.byteLength,
          });
          return value;
        },
        set: async (key: string, value: unknown) => {
          validateRequestStateKey(key);
          const serialized = serializeRequestStateValue(value);
          const prepareOperationId = randomUUID();
          this.log("debug", "request state write preparation starting", {
            state_key: key,
            operation_id: prepareOperationId,
            value_bytes: serialized.byteLength,
          });
          const prepared = await this.requestStateOperation({
            operationId: prepareOperationId,
            stateKey: key,
            prepareWrite: { size: serialized.byteLength },
          });
          const error = statusError(prepared.status, `Failed to prepare request state '${key}'`);
          if (error != null) throw error;
          const blob = await this.uploadBlob(prepared.prepareWrite?.blob, serialized, "request state value", {
            state_key: key,
            operation_id: prepareOperationId,
          });
          const commitOperationId = randomUUID();
          this.log("debug", "request state write commit starting", {
            state_key: key,
            operation_id: commitOperationId,
            blob_id: blob.id,
          });
          const committed = await this.requestStateOperation({
            operationId: commitOperationId,
            stateKey: key,
            commitWrite: { blob },
          });
          const commitError = statusError(committed.status, `Failed to commit request state '${key}'`);
          if (commitError != null) throw commitError;
          this.log("debug", "request state write completed", {
            state_key: key,
            operation_id: commitOperationId,
            value_bytes: serialized.byteLength,
          });
        },
      },
      metrics: {
        counter: async (name, value = 1) => {
          validateCounterMetric(name, value);
          this.emitUserEvent("ai.tensorlake.metric.counter.inc", {
            counter_name: name,
            counter_inc: value,
          });
        },
        timer: async (name, value) => {
          validateTimerMetric(name, value);
          this.emitUserEvent("ai.tensorlake.metric.timer", {
            timer_name: name,
            timer_value: value,
          });
        },
      },
      progress: {
        update: async (current, total, options) => {
          validateProgressUpdate(current, total, options);
          this.log("debug", "request progress update", {
            current,
            total,
            has_custom_message: options?.message != null,
            attribute_count: Object.keys(options?.attributes ?? {}).length,
          });
          this.state.progress = { current, total };
          this.publishState();
          const message = options?.message ?? `${this.functionRef.functionName}: executing step ${current} of ${total}`;
          this.runWithoutReplayOwnership(() => {
            printCloudEvent({ RequestProgressUpdated: {
              request_id: this.allocation.requestId,
              function_name: this.functionRef.functionName,
              function_run_id: this.allocation.functionCallId,
              allocation_id: this.allocation.allocationId,
              message,
              step: current,
              total,
              attributes: options?.attributes,
              created_at: new Date().toISOString(),
            } }, {
              type: "ai.tensorlake.progress_update",
              source: "/tensorlake/applications/progress",
              message,
            });
          });
        },
      },
    };
  }

  private emitUserEvent(type: string, data: Message): void {
    this.log("debug", "user metric event emitted", {
      event_type: type,
      metric_name: data.counter_name ?? data.timer_name,
    });
    this.runWithoutReplayOwnership(() => {
      printCloudEvent({
        request_id: this.allocation.requestId,
        function_name: this.functionRef.functionName,
        function_run_id: this.allocation.functionCallId,
        allocation_id: this.allocation.allocationId,
        ...data,
      }, { type, source: "/tensorlake/applications/metrics" });
    });
  }

  private emitLifecycleEvent(event: string, message: string): void {
    this.runWithoutReplayOwnership(() => {
      printCloudEvent({
        event,
        message,
        allocations: [{
          namespace: this.functionRef.namespace,
          application: this.functionRef.applicationName,
          application_version: this.functionRef.applicationVersion,
          function: this.functionRef.functionName,
          request_id: this.allocation.requestId,
          function_call_id: this.allocation.functionCallId,
          allocation_id: this.allocation.allocationId,
        }],
      });
    });
  }

  private async loadReplayHistory(): Promise<void> {
    const entries: Message[] = [];
    let afterClock = 0;
    let page = 0;
    while (true) {
      page += 1;
      this.log("debug", "requesting replay history page", { page, after_clock: afterClock });
      const requestedAfterClock = afterClock;
      const response = await this.readEvents(afterClock);
      entries.push(...(response.entries ?? []));
      this.log("debug", "replay history page received", {
        page,
        entry_count: response.entries?.length ?? 0,
        last_clock: response.lastClock,
        has_more: response.hasMore,
      });
      const responseClock = validatedEventLogClock(
        response,
        requestedAfterClock,
        (message) => new ReplayMismatchError(message),
      );
      afterClock = responseClock;
      if (!response.hasMore) break;
    }
    this.lastEventClock = afterClock;
    this.replay = new ReplayHistory(entries);
    this.log("info", "strict replay history loaded", {
      page_count: page,
      entry_count: entries.length,
      last_clock: afterClock,
    });
  }

  private waitForLiveEvent(predicate: (event: Message) => boolean): Promise<Message> {
    if (this.controller.signal.aborted) {
      return Promise.reject(this.controller.signal.reason);
    }
    const index = this.liveBacklog.findIndex(predicate);
    if (index >= 0) {
      this.log("debug", "durable event satisfied from backlog", {
        backlog_size: this.liveBacklog.length,
        pending_waiters: this.liveWaiters.length,
      });
      return Promise.resolve(this.liveBacklog.splice(index, 1)[0]);
    }
    const result = deferred<Message>();
    this.liveWaiters.push({ predicate, result });
    this.log("debug", "waiting for durable event", {
      backlog_size: this.liveBacklog.length,
      pending_waiters: this.liveWaiters.length,
    });
    this.ensureLivePump();
    return result.promise;
  }

  private ensureLivePump(): void {
    if (this.livePump != null) {
      this.log("debug", "durable event live pump already running");
      return;
    }
    this.log("info", "durable event live pump starting", { after_clock: this.lastEventClock });
    const pump = (async () => {
      // Only issue event-log reads while user code is blocked on a durable
      // event. This leaves no orphaned long poll after the final watcher result
      // and makes stream shutdown deterministic.
      while (!this.finished && this.liveWaiters.length > 0) {
        const requestedAfterClock = this.lastEventClock;
        const response = await this.readEvents(this.lastEventClock);
        this.log("debug", "durable event log page received", {
          requested_after_clock: requestedAfterClock,
          entry_count: response.entries?.length ?? 0,
          last_clock: response.lastClock,
          has_more: response.hasMore,
        });
        const responseClock = validatedEventLogClock(
          response,
          requestedAfterClock,
          (message) => new AllocationProtocolError(message),
        );
        this.lastEventClock = responseClock;
        for (const entry of response.entries ?? []) this.dispatchLiveEvent(entry);
      }
    })();
    this.livePump = pump;
    void pump.then(() => {
      if (this.livePump !== pump) return;
      this.livePump = undefined;
      this.log("debug", "durable event live pump stopped", {
        finished: this.finished,
        pending_waiters: this.liveWaiters.length,
        after_clock: this.lastEventClock,
      });
      // A waiter can be installed between the loop condition and this
      // continuation. Restart in that case so it cannot be stranded.
      if (!this.finished && this.liveWaiters.length > 0) this.ensureLivePump();
    }, (error) => {
      if (this.livePump === pump) this.livePump = undefined;
      if (!this.finished && !this.controller.signal.aborted) {
        const protocolError = error instanceof AllocationProtocolError
          ? error
          : new AllocationProtocolError(
            error instanceof Error ? error.message : String(error),
            { cause: error },
          );
        this.latchProtocolError(protocolError);
        this.log("error", "durable event live pump failed", {
          pending_waiters: this.liveWaiters.length,
        }, protocolError);
      }
    });
  }

  private dispatchLiveEvent(entry: Message): void {
    const index = this.liveWaiters.findIndex((waiter) => waiter.predicate(entry));
    const eventKind = Object.keys(entry).find((key) => key !== "clock") ?? "unknown";
    if (index < 0) {
      this.liveBacklog.push(entry);
      this.log("debug", "durable event added to backlog", {
        event_kind: eventKind,
        event_clock: entry.clock,
        backlog_size: this.liveBacklog.length,
      });
    } else {
      this.liveWaiters.splice(index, 1)[0].result.resolve(entry);
      this.log("debug", "durable event delivered to waiter", {
        event_kind: eventKind,
        event_clock: entry.clock,
        pending_waiters: this.liveWaiters.length,
      });
    }
  }

  private readEvents(afterClock: number): Promise<Message> {
    if (this.currentRead != null) {
      throw new AllocationProtocolError("Allocation event log already has a pending read");
    }
    const response = deferred<Message>();
    const request = {
      allocationId: this.allocation.allocationId,
      afterClock,
      maxEntries: 100,
    };
    this.currentRead = { request, response };
    this.log("debug", "event log read requested", {
      after_clock: afterClock,
      max_entries: request.maxEntries,
      connected_streams: this.eventReadStreams.size,
    });
    for (const stream of [...this.eventReadStreams]) {
      try {
        stream.write(request);
      } catch (error) {
        this.dropEventReadStream(stream, "write_failed", error);
      }
    }
    return response.promise;
  }

  watchEventLogReads(stream: StreamCall): void {
    this.eventReadStreams.add(stream);
    this.log("info", "event log read stream connected", {
      connected_streams: this.eventReadStreams.size,
      has_pending_read: this.currentRead != null,
    });
    try {
      stream.on("cancelled", () => this.dropEventReadStream(stream, "cancelled"));
      stream.on("close", () => this.dropEventReadStream(stream, "closed"));
      if (this.currentRead != null) {
        this.log("debug", "sending pending event log read to new stream", {
          after_clock: this.currentRead.request.afterClock,
          max_entries: this.currentRead.request.maxEntries,
        });
        stream.write(this.currentRead.request);
      }
      if (this.finished && this.currentRead == null) stream.end();
    } catch (error) {
      this.dropEventReadStream(stream, "setup_failed", error);
      throw error;
    }
  }

  deliverEventLogResponse(response: Message): void {
    if (this.currentRead == null) {
      this.log("warn", "unexpected event log response ignored", {
        entry_count: response.entries?.length ?? 0,
        last_clock: response.lastClock,
      });
      return;
    }
    const pending = this.currentRead;
    this.currentRead = undefined;
    this.log("debug", "event log response delivered", {
      requested_after_clock: pending.request.afterClock,
      entry_count: response.entries?.length ?? 0,
      last_clock: response.lastClock,
      has_more: response.hasMore,
    });
    pending.response.resolve(response);
  }

  private requestOutputBlob(size: number): Promise<BlobValue> {
    if (this.controller.signal.aborted) {
      return Promise.reject(this.controller.signal.reason);
    }
    const id = randomUUID();
    const request = deferred<Message>();
    this.outputBlobRequests.set(id, request);
    this.state.outputBlobRequests.push({ id, size });
    this.log("debug", "output blob requested", {
      blob_id: id,
      requested_bytes: size,
      pending_output_blob_requests: this.outputBlobRequests.size,
    });
    this.publishState();
    return request.promise.then((output) => {
      this.log("debug", "output blob response received", {
        blob_id: id,
        status_code: output.status?.code,
        chunk_count: output.blob?.chunks?.length ?? 0,
      });
      const error = statusError(output.status, "Failed to create output BLOB");
      if (error != null) throw error;
      return output.blob as BlobValue;
    });
  }

  private requestStateOperation(operation: Message): Promise<Message> {
    if (this.controller.signal.aborted) {
      return Promise.reject(this.controller.signal.reason);
    }
    const request = deferred<Message>();
    this.stateOperationRequests.set(operation.operationId, request);
    this.state.requestStateOperations.push(operation);
    this.log("debug", "request state operation queued", {
      operation_id: operation.operationId,
      state_key: operation.stateKey,
      operation_kind: this.stateOperationKind(operation),
      requested_bytes: operation.prepareWrite?.size,
      pending_state_operations: this.stateOperationRequests.size,
    });
    this.publishState();
    return request.promise;
  }

  deliverUpdate(update: Message): void {
    if (update.outputBlob != null) {
      let id = String(update.outputBlob.blob?.id ?? "");
      let matchedByOrder = false;
      if (!id) {
        // Error responses do not contain a BLOB, and therefore carry no BLOB ID.
        // The dataplane reconciles state requests and sends their responses in
        // state-list order, so match a BLOB-less response to the oldest request.
        id = String(this.state.outputBlobRequests[0]?.id ?? "");
        matchedByOrder = id !== "";
      }
      const request = this.outputBlobRequests.get(id);
      if (request == null) {
        this.log("warn", "unmatched output blob update ignored", {
          blob_id: id || undefined,
          status_code: update.outputBlob.status?.code,
          pending_output_blob_requests: this.outputBlobRequests.size,
        });
        return;
      }
      this.outputBlobRequests.delete(id);
      this.state.outputBlobRequests = this.state.outputBlobRequests.filter((item: Message) => item.id !== id);
      this.log("debug", "output blob update matched", {
        blob_id: id,
        status_code: update.outputBlob.status?.code,
        chunk_count: update.outputBlob.blob?.chunks?.length ?? 0,
        matched_by_order: matchedByOrder,
        pending_output_blob_requests: this.outputBlobRequests.size,
      });
      // Once a response is matched, settle its waiter before notifying state
      // streams. A broken stream writer must not strand the allocation after
      // the dataplane has already delivered the BLOB response.
      request.resolve(update.outputBlob);
      this.publishState();
      return;
    }
    const result = update.requestStateOperationResult;
    if (result != null) {
      if (typeof result.operationId !== "string" || result.operationId.length === 0) {
        throw this.latchProtocolError(
          "Allocation received a request state operation result without an operation ID",
        );
      }
      const id = result.operationId;
      const request = this.stateOperationRequests.get(id);
      if (request == null) {
        this.log("warn", "unmatched request state operation result ignored", {
          operation_id: id,
          status_code: result.status?.code,
          pending_state_operations: this.stateOperationRequests.size,
        });
        return;
      }
      const operation = this.state.requestStateOperations.find((item: Message) => item.operationId === id);
      this.stateOperationRequests.delete(id);
      this.state.requestStateOperations = this.state.requestStateOperations.filter(
        (item: Message) => item.operationId !== id,
      );
      this.log("debug", "request state operation result matched", {
        operation_id: id,
        state_key: operation?.stateKey,
        operation_kind: this.stateOperationKind(operation),
        status_code: result.status?.code,
        pending_state_operations: this.stateOperationRequests.size,
      });
      // State publication is downstream bookkeeping. Resolve the operation
      // first so a failing stream cannot turn a delivered result into a hang.
      request.resolve(result);
      this.publishState();
      return;
    }
    this.log("warn", "allocation update contained no recognized payload");
  }

  watchState(stream: StreamCall): void {
    this.stateStreams.add(stream);
    this.stateStreamFlows.set(stream, { backpressured: false });
    this.log("info", "allocation state stream connected", {
      connected_streams: this.stateStreams.size,
      state_hash: this.state.sha256Hash,
      pending_output_blob_requests: this.outputBlobRequests.size,
      pending_state_operations: this.stateOperationRequests.size,
    });
    try {
      stream.on("cancelled", () => this.dropStateStream(stream, "cancelled"));
      stream.on("close", () => this.dropStateStream(stream, "closed"));
      stream.on("drain", () => this.drainStateStream(stream));
      this.writeStateSnapshot(stream, this.snapshotState());
      if (this.finished) stream.end();
    } catch (error) {
      this.dropStateStream(stream, "setup_failed", error);
      throw error;
    }
  }

  private updateStateHash(): void {
    const content = JSON.stringify({
      progress: this.state.progress,
      outputBlobRequests: this.state.outputBlobRequests,
      requestStateOperations: this.state.requestStateOperations,
    });
    this.state.sha256Hash = createHash("sha256").update(content).digest("hex");
  }

  private snapshotState(): Message {
    return nativeStructuredClone(this.state) as Message;
  }

  private publishState(): void {
    this.updateStateHash();
    this.log("debug", "allocation state published", {
      state_hash: this.state.sha256Hash,
      connected_streams: this.stateStreams.size,
      pending_output_blob_requests: this.outputBlobRequests.size,
      pending_state_operations: this.stateOperationRequests.size,
      progress_current: this.state.progress?.current,
      progress_total: this.state.progress?.total,
    });
    for (const stream of [...this.stateStreams]) {
      // Allocation state is level-triggered: one current snapshot contains all
      // outstanding requests. Coalesce intermediate snapshots while grpc-js is
      // backpressured so progress-heavy functions cannot grow the transport
      // buffer without bound.
      this.writeStateSnapshot(stream, this.snapshotState());
    }
  }

  private writeStateSnapshot(stream: StreamCall, snapshot: Message): void {
    const flow = this.stateStreamFlows.get(stream);
    if (flow == null) return;
    if (flow.backpressured) {
      flow.pending = snapshot;
      return;
    }
    try {
      // grpc-js streams are object-mode writers and may retain a message
      // reference while backpressured. Every caller supplies a deep snapshot
      // so later reconciliation cannot rewrite transport-owned state.
      flow.backpressured = !stream.write(snapshot);
    } catch (error) {
      // A reconnect receives the complete current state. Drop a broken writer
      // so it cannot block healthy streams or allocation waiters.
      this.dropStateStream(stream, "write_failed", error);
    }
  }

  private drainStateStream(stream: StreamCall): void {
    const flow = this.stateStreamFlows.get(stream);
    if (flow == null) return;
    flow.backpressured = false;
    const pending = flow.pending;
    flow.pending = undefined;
    if (pending != null) this.writeStateSnapshot(stream, pending);
  }

  private addBatch(events: Message[]): void {
    if (this.terminalBatchQueued && events.every((event) => event.finishAllocation == null)) {
      this.log("error", "execution event rejected after terminal batch", {
        event_count: events.length,
        event_kinds: events.map((event) => Object.keys(event)[0] ?? "unknown"),
      });
      return;
    }
    this.executionBatches.push(events);
    this.log("debug", "execution log batch queued", {
      event_count: events.length,
      event_kinds: events.map((event) => Object.keys(event)[0] ?? "unknown"),
      queued_batches: this.executionBatches.length,
      waiting_consumers: this.executionBatchWaiters.length,
    });
    for (const waiter of this.executionBatchWaiters.splice(0)) {
      this.markTerminalBatchObserved(events);
      waiter(events);
    }
  }

  private queueTerminalBatch(finishAllocation: Message): void {
    if (this.terminalBatchQueued) {
      this.log("error", "duplicate terminal allocation batch rejected", {
        outcome_code: finishAllocation.outcomeCode,
        failure_reason: finishAllocation.failureReason,
      });
      return;
    }
    this.terminalBatchQueued = true;
    this.addBatch([{ finishAllocation }]);
  }

  async getExecutionBatch(): Promise<Message[]> {
    if (this.executionBatches.length > 0) {
      this.log("debug", "returning queued execution log batch", {
        event_count: this.executionBatches[0].length,
        queued_batches: this.executionBatches.length,
      });
      const events = this.executionBatches[0];
      this.markTerminalBatchObserved(events);
      return events;
    }
    if (this.finished) {
      this.log("debug", "returning empty execution log batch for finished allocation");
      return [];
    }
    this.log("debug", "execution log batch consumer waiting", {
      waiting_consumers: this.executionBatchWaiters.length + 1,
    });
    return new Promise((resolve) => this.executionBatchWaiters.push(resolve));
  }

  advanceExecutionBatch(): void {
    const advanced = this.executionBatches.shift();
    this.log("debug", "execution log batch advanced", {
      advanced_event_count: advanced?.length ?? 0,
      queued_batches: this.executionBatches.length,
    });
  }

  private markTerminalBatchObserved(events: Message[]): void {
    if (
      this.terminalBatchObserved
      || !events.some((event) => event.finishAllocation != null)
    ) {
      return;
    }
    this.terminalBatchObserved = true;
    this.log("debug", "terminal execution log batch observed by consumer");
    for (const waiter of this.terminalBatchObservedWaiters.splice(0)) waiter();
  }

  get isFinished(): boolean {
    return this.finished;
  }

  private finish(): void {
    if (this.finished) {
      this.log("debug", "allocation finish ignored because runner is already finished");
      return;
    }
    this.finished = true;
    this.abortExecution(new FunctionError("Allocation finished"));
    this.log("info", "allocation runner finished", {
      duration_ms: Date.now() - this.startedAt,
      queued_execution_batches: this.executionBatches.length,
      state_streams: this.stateStreams.size,
      event_log_streams: this.eventReadStreams.size,
    });
    for (const stream of [...this.stateStreams]) {
      try {
        stream.end();
      } catch (error) {
        this.dropStateStream(stream, "end_failed", error);
      }
    }
    for (const stream of [...this.eventReadStreams]) {
      try {
        stream.end();
      } catch (error) {
        this.dropEventReadStream(stream, "end_failed", error);
      }
    }
    for (const waiter of this.executionBatchWaiters.splice(0)) waiter([]);
  }

  private dropStateStream(
    stream: StreamCall,
    reason: string,
    error?: unknown,
  ): void {
    if (!this.stateStreams.delete(stream)) return;
    this.stateStreamFlows.delete(stream);
    this.log("info", "allocation state stream disconnected", {
      reason,
      connected_streams: this.stateStreams.size,
    }, error);
  }

  private dropEventReadStream(
    stream: StreamCall,
    reason: string,
    error?: unknown,
  ): void {
    if (!this.eventReadStreams.delete(stream)) return;
    this.log("info", "event log read stream disconnected", {
      reason,
      connected_streams: this.eventReadStreams.size,
      has_pending_read: this.currentRead != null,
    }, error);
  }

  private stateOperationKind(operation: Message | undefined): string {
    if (operation?.prepareRead != null) return "prepare_read";
    if (operation?.prepareWrite != null) return "prepare_write";
    if (operation?.commitWrite != null) return "commit_write";
    return "unknown";
  }

  private blobLogFields(blob: BlobValue | undefined): Message {
    const chunks = blob?.chunks ?? [];
    return {
      blob_id: blob?.id,
      chunk_count: chunks.length,
      declared_bytes: chunks.reduce((total, chunk) => total + (chunk.size ?? 0), 0),
    };
  }

  private async downloadBlob(
    blob: BlobValue | undefined,
    purpose: string,
    fields: Message = {},
  ): Promise<Uint8Array> {
    if (blob == null) throw new Error(`Missing BLOB for ${purpose}`);
    const startedAt = Date.now();
    this.log("debug", "blob download starting", {
      purpose,
      ...fields,
      ...this.blobLogFields(blob),
    });
    try {
      const data = await downloadBlobData(
        blob,
        (message, details) => this.log("debug", `blob ${message}`, {
          purpose,
          ...fields,
          ...details,
        }),
        this.controller.signal,
      );
      this.log("debug", "blob download completed", {
        purpose,
        ...fields,
        ...this.blobLogFields(blob),
        downloaded_bytes: data.byteLength,
        duration_ms: Date.now() - startedAt,
      });
      return data;
    } catch (error) {
      this.log("error", "blob download failed", {
        purpose,
        ...fields,
        ...this.blobLogFields(blob),
        duration_ms: Date.now() - startedAt,
      }, error);
      throw error;
    }
  }

  private async downloadSerializedObject(
    object: SerializedObjectInsideBlobValue | undefined,
    blob: BlobValue | undefined,
    purpose: string,
    fields: Message = {},
  ): Promise<{ data: Uint8Array; metadata: Uint8Array; contentType?: string; encoding?: string | number }> {
    if (object == null) throw new Error(`Missing serialized object for ${purpose}`);
    if (blob == null) throw new Error(`Missing BLOB for ${purpose}`);
    const startedAt = Date.now();
    this.log("debug", "serialized object download starting", {
      purpose,
      ...fields,
      ...this.blobLogFields(blob),
      object_offset: object.offset ?? 0,
      object_bytes: object.manifest?.size,
      metadata_bytes: object.manifest?.metadataSize,
      content_type: object.manifest?.contentType,
      encoding: object.manifest?.encoding,
    });
    try {
      const value = await downloadSerializedObjectData(
        object,
        blob,
        (message, details) => this.log(
          "debug",
          `blob ${message}`,
          { purpose, ...fields, ...details },
        ),
        this.controller.signal,
      );
      this.log("debug", "serialized object download completed", {
        purpose,
        ...fields,
        ...this.blobLogFields(blob),
        object_bytes: value.data.byteLength + value.metadata.byteLength,
        data_bytes: value.data.byteLength,
        metadata_bytes: value.metadata.byteLength,
        duration_ms: Date.now() - startedAt,
      });
      return value;
    } catch (error) {
      this.log("error", "serialized object download failed", {
        purpose,
        ...fields,
        ...this.blobLogFields(blob),
        duration_ms: Date.now() - startedAt,
      }, error);
      throw error;
    }
  }

  private async uploadBlob(
    blob: BlobValue | undefined,
    data: Uint8Array,
    purpose: string,
    fields: Message = {},
  ): Promise<BlobValue> {
    if (blob == null) throw new Error(`Missing BLOB for ${purpose}`);
    const startedAt = Date.now();
    this.log("debug", "blob upload starting", {
      purpose,
      ...fields,
      ...this.blobLogFields(blob),
      upload_bytes: data.byteLength,
    });
    try {
      const uploaded = await uploadBlobData(
        blob,
        data,
        (message, details) => this.log("debug", `blob ${message}`, {
          purpose,
          ...fields,
          ...details,
        }),
        this.controller.signal,
      );
      this.log("debug", "blob upload completed", {
        purpose,
        ...fields,
        ...this.blobLogFields(uploaded),
        upload_bytes: data.byteLength,
        duration_ms: Date.now() - startedAt,
      });
      return uploaded;
    } catch (error) {
      this.log("error", "blob upload failed", {
        purpose,
        ...fields,
        ...this.blobLogFields(blob),
        upload_bytes: data.byteLength,
        duration_ms: Date.now() - startedAt,
      }, error);
      throw error;
    }
  }

  private log(level: LogLevel, message: string, fields: Message = {}, error?: unknown): void {
    this.runWithoutReplayOwnership(() => {
      writeStructuredOutput("stderr", () => {
        const rendered = error instanceof Error
          ? error.stack?.split("\n") ?? [`${error.name}: ${error.message}`]
          : error == null ? undefined : [String(error)];
        return {
          timestamp: new Date().toISOString(),
          level,
          component: "typescript_function_executor_allocation",
          message,
          allocation_id: this.allocation.allocationId,
          request_id: this.allocation.requestId,
          function_call_id: this.allocation.functionCallId,
          namespace: this.functionRef.namespace,
          application: this.functionRef.applicationName,
          application_version: this.functionRef.applicationVersion,
          function: this.functionRef.functionName,
          elapsed_ms: Date.now() - this.startedAt,
          ...fields,
          ...(rendered == null ? {} : { error: rendered }),
        };
      });
    });
  }

  private runWithoutReplayOwnership<T>(callback: () => T): T {
    return this.replay?.runWithoutCausalOwnership(callback) ?? callback();
  }
}
