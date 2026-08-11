import { AsyncLocalStorage, executionAsyncId } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import type { Image } from "../image.js";
import { SDKUsageError } from "./errors.js";
import type { Parameter, ParameterValues, Schema } from "./schema.js";
import { schema, validateWithSchema } from "./schema.js";
import { registerDefinition } from "./registry.js";

export interface Retries {
  maxRetries: number;
}

export function retries(options: Retries): Retries {
  if (
    typeof options !== "object"
    || options == null
    || !Number.isInteger(options.maxRetries)
    || options.maxRetries < 0
    || options.maxRetries > 10
  ) {
    throw new SDKUsageError("maxRetries must be an integer between 0 and 10");
  }
  return Object.freeze({ ...options });
}

export type Region = "us-east-1" | "eu-west-1";

export type ApplicationCapability = "unauthenticated_requests";

export interface FunctionOptions<
  P extends readonly Parameter<unknown>[],
  R,
> {
  name?: string;
  parameters: P;
  returns: Schema<R>;
  description?: string;
  cpu?: number;
  memory?: number;
  ephemeralDisk?: number;
  gpu?: string | string[] | null;
  timeout?: number;
  image?: Image;
  secrets?: string[];
  retries?: Retries;
  region?: Region;
  warmContainers?: number;
  minContainers?: number;
  maxContainers?: number;
}

export interface ApplicationOptions<
  P extends readonly Parameter<unknown>[],
  R,
> extends FunctionOptions<P, R> {
  tags?: Record<string, string>;
  applicationRetries?: Retries;
  allow?: readonly ApplicationCapability[];
}

/** Options available when parameter and return schemas use the JSON defaults. */
export type SimpleFunctionOptions = Omit<
  FunctionOptions<readonly Parameter<unknown>[], unknown>,
  "name" | "parameters" | "returns"
>;

/** Application options available when parameter and return schemas use the JSON defaults. */
export type SimpleApplicationOptions = Omit<
  ApplicationOptions<readonly Parameter<unknown>[], unknown>,
  "name" | "parameters" | "returns"
>;

export interface NormalizedFunctionOptions {
  description: string;
  cpu: number;
  memory: number;
  ephemeralDisk: number;
  gpu: string | string[] | null;
  timeout: number;
  image?: Image;
  secrets: string[];
  retries?: Retries;
  region?: Region;
  warmContainers?: number;
  minContainers?: number;
  maxContainers?: number;
}

export interface ApplicationConfiguration {
  tags: Record<string, string>;
  retries: Retries;
  allow: ApplicationCapability[];
  version: string;
}

export interface RegisteredDefinition {
  readonly name: string;
  readonly handler: (...args: unknown[]) => Promise<unknown>;
  readonly parameters: readonly Parameter<unknown>[];
  readonly returns: Schema<unknown>;
  readonly options: NormalizedFunctionOptions;
  readonly application?: ApplicationConfiguration;
}

// A string discriminator alone is valid user JSON and cannot safely identify
// an SDK control-flow value. Symbol.for keeps the brand stable when the user
// application and executor load separate copies of the SDK in one process.
const TAIL_CALL_BRAND: unique symbol = Symbol.for(
  "tensorlake.applications.tail-call.v1",
);

export interface TailCall<T> {
  readonly [TAIL_CALL_BRAND]: true;
  readonly kind: "tensorlake-tail-call";
  readonly definition: RegisteredDefinition;
  readonly args: readonly unknown[];
  readonly _result?: T;
}

export interface WaitOptions {
  timeout?: number;
  returnWhen?: "all_completed" | "first_completed" | "first_failure";
}

export interface WaitResult<T> {
  done: FunctionFuture<T>[];
  notDone: FunctionFuture<T>[];
}

export interface FunctionRuntime {
  invoke<T>(definition: RegisteredDefinition, args: readonly unknown[]): Promise<T>;
  runFuture<T>(future: FunctionFuture<T>): Promise<T>;
  wrapFutureWait?<T>(promise: Promise<T>): Promise<T>;
  trackMixedPromiseRace?(
    promise: Promise<unknown>,
    producers?: readonly Promise<unknown>[],
  ): void;
  trackPromiseSettlement?(
    promise: Promise<unknown>,
    onSettled: () => void,
  ): boolean;
  trackDurablePromiseConsumption?(promise: Promise<unknown>): void;
  runPromiseAggregateConstruction?<T>(callback: () => T): T;
  reduce<T>(
    definition: RegisteredDefinition,
    items: readonly unknown[],
    initial: unknown,
    hasInitial: boolean,
  ): Promise<T>;
}

// FunctionFuture observes durable promises to update its public completion
// fields. That bookkeeping must not look like an application-level await to
// the strict replay causality tracker. Symbol.for keeps the private observer
// available when the application and executor use separate SDK bundles.
export const DURABLE_PROMISE_OBSERVER = Symbol.for(
  "tensorlake.applications.durable-promise-observer.v1",
);

// SDK combinators must be able to compose durable promises without making
// their private bookkeeping look like a user continuation. The aggregate
// returned to user code is wrapped separately.
export const DURABLE_PROMISE_INTERNAL_THEN = Symbol.for(
  "tensorlake.applications.durable-promise-internal-then.v1",
);

interface ObservableDurablePromise<T> extends Promise<T> {
  [DURABLE_PROMISE_OBSERVER]?: (
    onfulfilled: (value: T) => void,
    onrejected: (reason: unknown) => void,
  ) => void;
  [DURABLE_PROMISE_INTERNAL_THEN]?: <TResult1 = T, TResult2 = never>(
    onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
  ) => Promise<TResult1 | TResult2>;
}

// The user application and function executor are separate bundles loaded into
// one Node process. Store the runtime on globalThis so durable calls made by the
// application SDK copy reach the executor SDK copy that installed the runtime.
const FUNCTION_RUNTIME_STORAGE_KEY = Symbol.for(
  "tensorlake.applications.function-runtime-storage.v1",
);

const PROMISE_INSTRUMENTED_KEY = Symbol.for(
  "tensorlake.applications.promise-instrumented.v3",
);

const PROMISE_THEN_INSTRUMENTED_KEY = Symbol.for(
  "tensorlake.applications.promise-then-instrumented.v1",
);

const DURABLE_THENABLE_REGISTRY_KEY = Symbol.for(
  "tensorlake.applications.durable-thenable-registry.v2",
);

const INTERNAL_PROMISE_AGGREGATE_DEPTH_KEY = Symbol.for(
  "tensorlake.applications.internal-promise-aggregate-depth.v1",
);

interface DurableAssimilationRegistration {
  active: boolean;
  readonly detach: () => void;
}

interface DurableThenableMetadata {
  readonly aggregateExpectations: DurableAggregateExpectation[];
  readonly provenanceExpectations: DurableProvenanceExpectation[];
}

interface DurableThenableProvenance {
  readonly metadata: DurableThenableMetadata;
  readonly assimilation?: DurableAssimilationRegistration;
  readonly detached?: true;
}

interface DurableAggregateScope {
  settled: boolean;
  readonly metadata: Set<DurableThenableMetadata>;
}

interface DurableAggregateExpectation {
  readonly scope: DurableAggregateScope;
  readonly creatorAsyncId: number;
}

interface DurableProvenanceExpectation {
  readonly value: object;
  readonly creatorAsyncId: number;
}

export interface DurableAssimilation {
  finish(): void;
  link<T extends object>(value: T): T;
}

let futureCompletionCounter = 0;

function runtimeStorage(): AsyncLocalStorage<FunctionRuntime> {
  const target = globalThis as typeof globalThis & {
    [FUNCTION_RUNTIME_STORAGE_KEY]?: AsyncLocalStorage<FunctionRuntime>;
  };
  target[FUNCTION_RUNTIME_STORAGE_KEY] ??= new AsyncLocalStorage<FunctionRuntime>();
  return target[FUNCTION_RUNTIME_STORAGE_KEY];
}

export function currentFunctionRuntime(): FunctionRuntime | undefined {
  return runtimeStorage().getStore();
}

export function runWithFunctionRuntime<T>(
  runtime: FunctionRuntime,
  callback: () => Promise<T>,
): Promise<T> {
  return runtimeStorage().run(runtime, callback);
}

export class FunctionFuture<T> implements PromiseLike<T> {
  readonly id = randomUUID();
  readonly definition: RegisteredDefinition;
  readonly args: readonly unknown[];
  private promise?: Promise<T>;
  private startDelaySeconds = 0;
  private completionOrder?: number;
  private failed = false;
  exception?: unknown;
  done = false;

  constructor(definition: RegisteredDefinition, args: readonly unknown[]) {
    this.definition = definition;
    this.args = args;
    registerDurableThenable(this);
  }

  run(): this {
    if (this.promise != null) throw new SDKUsageError(`Future ${this.id} is already running`);
    const runtime = currentFunctionRuntime();
    const promise = runtime == null
      ? executeHandler<T>(this.definition, this.args)
      : runtime.runFuture(this);
    // The future and its runtime promise are two views of the same durable
    // operation. Sharing provenance lets Promise.resolve/fan-in wrappers keep
    // the exact durable contender identity.
    registerDurableThenable(promise, this);
    this.promise = promise;
    const onfulfilled = (value: T) => {
      this.done = true;
      this.completionOrder = futureCompletionCounter;
      futureCompletionCounter += 1;
      return value;
    };
    const onrejected = (error: unknown) => {
      this.done = true;
      this.failed = true;
      this.exception = error;
      this.completionOrder = futureCompletionCounter;
      futureCompletionCounter += 1;
    };
    const observer = (promise as ObservableDurablePromise<T>)[DURABLE_PROMISE_OBSERVER];
    if (observer != null) {
      observer.call(promise, onfulfilled, onrejected);
    } else {
      void promise.then(
        (value) => {
          onfulfilled(value);
        },
        (error) => {
          onrejected(error);
        },
      );
    }
    // The completion observer handles rejection without changing `this.promise`,
    // so result()/await still receive the original rejection while Node does not
    // treat an intentionally unawaited future as an unhandled rejection.
    return this;
  }

  runLater(delaySeconds: number): this {
    if (!Number.isFinite(delaySeconds) || delaySeconds < 0) {
      throw new SDKUsageError("Future delay must be a non-negative finite number");
    }
    if (this.promise != null) {
      throw new SDKUsageError(`Future ${this.id} is already running`);
    }
    this.startDelaySeconds = delaySeconds;
    return this.run();
  }

  get delaySeconds(): number {
    return this.startDelaySeconds;
  }

  result(): Promise<T> {
    if (this.promise == null) this.run();
    return this.promise as Promise<T>;
  }

  then<TResult1 = T, TResult2 = never>(
    onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
  ): PromiseLike<TResult1 | TResult2> {
    return this.result().then(onfulfilled, onrejected);
  }

  static wait<T>(futures: FunctionFuture<T>[], options: WaitOptions = {}): Promise<WaitResult<T>> {
    const completion = FunctionFuture.waitForFutures(futures, options);
    // Returning the runtime wrapper lets strict replay observe when user code
    // awaits this aggregate promise. Merely creating a Future.wait remains
    // fire-and-forget and must not trigger a mismatch.
    return currentFunctionRuntime()?.wrapFutureWait?.(completion) ?? completion;
  }

  private static async waitForFutures<T>(
    futures: FunctionFuture<T>[],
    options: WaitOptions,
  ): Promise<WaitResult<T>> {
    const returnWhen = options.returnWhen ?? "all_completed";
    if (!["all_completed", "first_completed", "first_failure"].includes(returnWhen)) {
      throw new SDKUsageError(`Unsupported Future.wait returnWhen value '${returnWhen}'`);
    }
    if (options.timeout != null && (!Number.isFinite(options.timeout) || options.timeout < 0)) {
      throw new SDKUsageError("Future.wait timeout must be a non-negative finite number");
    }
    futures.forEach((future) => {
      if (future.promise == null) future.run();
    });
    const alreadyDone = futures.filter((future) => future.done);
    if (
      alreadyDone.length > 0
      && (
        returnWhen === "first_completed"
        || (
          returnWhen === "first_failure"
          && alreadyDone.some((future) => future.failed)
        )
      )
    ) {
      return {
        done: alreadyDone,
        notDone: futures.filter((future) => !future.done),
      };
    }
    const pending = futures.filter((future) => !future.done);
    if (pending.length === 0) {
      return { done: [...futures], notDone: [] };
    }
    let wait: Promise<FutureWaitOutcome<T>>;
    if (returnWhen === "all_completed") {
      wait = internalPromiseAll(
        pending.map((future) => internalDurableThen(future.result(),
          () => undefined,
          () => undefined,
        )),
      ).then(() => ({ kind: "all_completed" }));
    } else if (returnWhen === "first_completed") {
      wait = internalPromiseRace(
        pending.map((future) => internalDurableThen(future.result(),
          () => ({ kind: "cutoff", future } as const),
          () => ({ kind: "cutoff", future } as const),
        )),
      );
    } else {
      wait = new Promise((resolve) => {
        let remaining = pending.length;
        for (const future of pending) {
          void internalDurableThen(
            future.result(),
            () => {
              remaining -= 1;
              if (remaining === 0) resolve({ kind: "all_completed" });
            },
            () => resolve({ kind: "cutoff", future }),
          );
        }
      });
    }
    const outcome = await waitUntilOrTimeout(wait, options.timeout);
    if (outcome.kind === "cutoff") {
      const completionCutoff = outcome.future.completionOrder;
      if (completionCutoff == null) {
        throw new SDKUsageError("Future.wait winner has no completion order");
      }
      const doneSet = new Set(alreadyDone);
      for (const future of pending) {
        if (
          future.done
          && future.completionOrder != null
          && future.completionOrder <= completionCutoff
        ) {
          doneSet.add(future);
        }
      }
      return {
        done: futures.filter((future) => doneSet.has(future)),
        notDone: futures.filter((future) => !doneSet.has(future)),
      };
    }
    return {
      done: futures.filter((future) => future.done),
      notDone: futures.filter((future) => !future.done),
    };
  }
}

type FutureWaitOutcome<T> =
  | { kind: "all_completed" }
  | { kind: "cutoff"; future: FunctionFuture<T> };

async function waitUntilOrTimeout<T>(
  promise: Promise<T>,
  timeoutSeconds?: number,
): Promise<T | { kind: "timeout" }> {
  if (timeoutSeconds == null) {
    return promise;
  }
  let timer: NodeJS.Timeout | undefined;
  try {
    return await internalPromiseRace<T | { kind: "timeout" }>([
      promise,
      new Promise<{ kind: "timeout" }>((resolve) => {
        timer = setTimeout(() => resolve({ kind: "timeout" }), timeoutSeconds * 1000);
      }),
    ]);
  } finally {
    if (timer != null) clearTimeout(timer);
  }
}

type Awaitable<T> = T | PromiseLike<T>;
type AwaitableIterable<T> = Iterable<Awaitable<T>> | PromiseLike<Iterable<Awaitable<T>>>;

function isDurableThenable(value: unknown): boolean {
  if ((typeof value !== "object" && typeof value !== "function") || value == null) {
    return false;
  }
  return durableThenableRegistry().has(value);
}

export function isDetachedDurableThenable(value: unknown): boolean {
  const provenance = provenanceForDurableThenable(value);
  return provenance?.detached === true
    || (provenance?.assimilation != null && !provenance.assimilation.active);
}

function durableThenableRegistry(): WeakMap<object, DurableThenableProvenance> {
  const target = globalThis as typeof globalThis & {
    [DURABLE_THENABLE_REGISTRY_KEY]?: WeakMap<object, DurableThenableProvenance>;
  };
  target[DURABLE_THENABLE_REGISTRY_KEY] ??=
    new WeakMap<object, DurableThenableProvenance>();
  return target[DURABLE_THENABLE_REGISTRY_KEY];
}

function newDurableThenableMetadata(): DurableThenableMetadata {
  return {
    aggregateExpectations: [],
    provenanceExpectations: [],
  };
}

function metadataForDurableThenable(value: unknown): DurableThenableMetadata | undefined {
  return provenanceForDurableThenable(value)?.metadata;
}

function provenanceForDurableThenable(
  value: unknown,
): DurableThenableProvenance | undefined {
  if ((typeof value !== "object" && typeof value !== "function") || value == null) {
    return undefined;
  }
  return durableThenableRegistry().get(value);
}

export function registerDurableThenable<T extends object>(
  value: T,
  source?: object,
): T {
  const registry = durableThenableRegistry();
  const metadata = source == null
    ? registry.get(value)?.metadata ?? newDurableThenableMetadata()
    : registry.get(source)?.metadata ?? newDurableThenableMetadata();
  registry.set(value, { metadata });
  if (source != null && !registry.has(source)) registry.set(source, { metadata });
  return value;
}

function registerDetachedDurableThenable<T extends object>(value: T): T {
  const registry = durableThenableRegistry();
  registry.set(value, {
    metadata: registry.get(value)?.metadata ?? newDurableThenableMetadata(),
    detached: true,
  });
  return value;
}

function inheritDurableThenable<T extends object>(value: T, source: object): T {
  const provenance = durableThenableRegistry().get(source);
  if (provenance == null) return value;
  durableThenableRegistry().set(value, provenance);
  if (provenance.assimilation == null) {
    provenance.metadata.provenanceExpectations.push({
      value,
      creatorAsyncId: executionAsyncId(),
    });
  }
  return value;
}

/**
 * Associates a replay-aware durable assimilation with its durable provenance.
 *
 * The returned handle marks normal settlement and links derived promises to
 * this exact assimilation. Promise aggregate construction can detach the
 * aggregate's internal reaction so it is not mistaken for application code
 * awaiting the returned aggregate.
 */
export function registerDurableAssimilation(
  value: object,
  detach: () => void,
): DurableAssimilation {
  const metadata = metadataForDurableThenable(value);
  if (metadata == null) {
    return {
      finish: () => undefined,
      link: (linked) => linked,
    };
  }
  const registration: DurableAssimilationRegistration = {
    active: true,
    detach,
  };
  const provenanceIndex = metadata.provenanceExpectations.findIndex((expectation) =>
    expectation.creatorAsyncId !== executionAsyncId()
  );
  if (provenanceIndex >= 0) {
    const [expectation] = metadata.provenanceExpectations.splice(provenanceIndex, 1);
    durableThenableRegistry().set(expectation.value, {
      metadata,
      assimilation: registration,
    });
  }
  const expectationIndex = metadata.aggregateExpectations.findIndex((expectation) =>
    !expectation.scope.settled
    && expectation.creatorAsyncId !== executionAsyncId()
  );
  if (expectationIndex >= 0) {
    const [expectation] = metadata.aggregateExpectations.splice(expectationIndex, 1);
    associateAssimilationWithAggregate(registration, expectation.scope);
  }
  return {
    finish: () => finishDurableAssimilation(registration),
    link: <T extends object>(linked: T): T => {
      durableThenableRegistry().set(linked, {
        metadata,
        assimilation: registration,
      });
      return linked;
    },
  };
}

function associateAssimilationWithAggregate(
  registration: DurableAssimilationRegistration,
  scope: DurableAggregateScope,
): void {
  if (!registration.active || scope.settled) return;
  finishDurableAssimilation(registration);
  registration.detach();
}

function finishDurableAssimilation(registration: DurableAssimilationRegistration): void {
  if (!registration.active) return;
  registration.active = false;
}

function trackDurableAggregate(
  runtime: FunctionRuntime,
  result: Promise<unknown>,
  durableContenders: readonly unknown[],
  markResult: boolean,
): void {
  const scope: DurableAggregateScope = {
    settled: false,
    metadata: new Set<DurableThenableMetadata>(),
  };
  if (markResult) {
    // Constructing an aggregate attaches internal reactions to its durable
    // inputs, but that does not mean application code is awaiting the result.
    // Mark the aggregate itself as detached so a later user reaction becomes
    // the durable assimilation observed by strict replay.
    registerDetachedDurableThenable(result);
  }
  const creatorAsyncId = executionAsyncId();
  for (const contender of durableContenders) {
    const provenance = provenanceForDurableThenable(contender);
    if (provenance == null) continue;
    if (provenance.assimilation != null) {
      associateAssimilationWithAggregate(provenance.assimilation, scope);
    } else {
      provenance.metadata.aggregateExpectations.push({
        scope,
        creatorAsyncId,
      });
    }
    if (scope.metadata.has(provenance.metadata)) continue;
    const { metadata } = provenance;
    scope.metadata.add(metadata);
  }
  const settle = () => {
    if (scope.settled) return;
    scope.settled = true;
    for (const metadata of scope.metadata) {
      for (let index = metadata.aggregateExpectations.length - 1; index >= 0; index -= 1) {
        if (metadata.aggregateExpectations[index].scope === scope) {
          metadata.aggregateExpectations.splice(index, 1);
        }
      }
    }
    scope.metadata.clear();
  };
  // Promise.prototype.then still performs a user-observable Symbol.species
  // lookup even when invoked directly. The executor observes the native
  // promiseResolve async hook instead, so bookkeeping cannot call user code or
  // construct an extra subclass promise.
  if (runtime.trackPromiseSettlement?.(result, settle) !== true) settle();
}

interface PromiseAggregateObservation {
  readonly ordinaryLifetime: Promise<void>;
  allOrdinaryValuesObservable: boolean;
  observeFulfillment(): void;
  observeRejection(): void;
  finishIteration(contenderCount: number): void;
}

function promiseAggregateObservation(
  kind: "all" | "all_settled" | "any",
): PromiseAggregateObservation {
  let settleLifetime!: () => void;
  const ordinaryLifetime = new Promise<void>((resolve) => {
    settleLifetime = resolve;
  });
  let contenderCount: number | undefined;
  let fulfillmentCount = 0;
  let rejectionCount = 0;
  let settled = false;
  const maybeSettle = () => {
    if (settled) return;
    const canNoLongerAdvance = kind === "any"
      ? contenderCount != null && rejectionCount === contenderCount
      : kind === "all"
        ? rejectionCount > 0
          || (contenderCount != null && fulfillmentCount === contenderCount)
        : contenderCount != null
          && fulfillmentCount + rejectionCount === contenderCount;
    if (!canNoLongerAdvance) return;
    settled = true;
    settleLifetime();
  };
  return {
    ordinaryLifetime,
    allOrdinaryValuesObservable: true,
    observeFulfillment: () => {
      fulfillmentCount += 1;
      maybeSettle();
    },
    observeRejection: () => {
      rejectionCount += 1;
      maybeSettle();
    },
    finishIteration: (count) => {
      contenderCount = count;
      maybeSettle();
    },
  };
}

function observePromiseAggregateContender(
  normalized: unknown,
  observation: PromiseAggregateObservation,
): unknown {
  if (
    (typeof normalized !== "object" && typeof normalized !== "function")
    || normalized == null
  ) {
    observation.allOrdinaryValuesObservable = false;
    return normalized;
  }
  let observedSettled = false;
  const observed = Object.create(null) as object;
  Object.defineProperty(observed, "then", {
    configurable: true,
    get: () => {
      const then = Reflect.get(normalized, "then", normalized) as unknown;
      if (typeof then !== "function") return then;
      return function observedThen(
        this: unknown,
        onfulfilled: unknown,
        onrejected: unknown,
      ): unknown {
        return Reflect.apply(then, normalized, [
          function observedFulfillment(this: unknown, value: unknown): unknown {
            if (!observedSettled) {
              observedSettled = true;
              observation.observeFulfillment();
            }
            return typeof onfulfilled === "function"
              ? Reflect.apply(onfulfilled, this, [value])
              : undefined;
          },
          function observedRejection(this: unknown, reason: unknown): unknown {
            if (!observedSettled) {
              observedSettled = true;
              observation.observeRejection();
            }
            if (typeof onrejected === "function") {
              return Reflect.apply(onrejected, this, [reason]);
            }
            throw reason;
          },
        ]);
      };
    },
  });
  return observed;
}

/**
 * Lets a native Promise aggregate algorithm retain control of iteration and
 * settlement while exposing each constructor-normalized contender.
 *
 * The facade constructs the real Promise subclass and delegates its `resolve`
 * lookup and calls back to that subclass. This preserves custom static resolve
 * behavior and the public result type without resolving an input twice.
 */
function observedPromiseConstructor(
  constructor: PromiseConstructor,
  observeNormalized: (normalized: unknown) => unknown,
): PromiseConstructor {
  const facade = function PromiseAggregateObservedConstructor(
    executor: (
      resolve: (value: unknown) => void,
      reject: (reason?: unknown) => void,
    ) => void,
  ): Promise<unknown> {
    return Reflect.construct(
      constructor,
      [executor],
      constructor,
    ) as Promise<unknown>;
  } as unknown as PromiseConstructor;
  Object.defineProperty(facade, "resolve", {
    configurable: true,
    get: () => {
      const promiseResolve = Reflect.get(constructor, "resolve", constructor) as unknown;
      if (typeof promiseResolve !== "function") return promiseResolve;
      return (value: unknown): unknown => {
        const normalized = Reflect.apply(promiseResolve, constructor, [value]) as unknown;
        return observeNormalized(normalized);
      };
    },
  });
  return facade;
}

function internalDurableThen<T, TResult1 = T, TResult2 = never>(
  promise: Promise<T>,
  onfulfilled?: ((value: T) => TResult1 | PromiseLike<TResult1>) | null,
  onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
): Promise<TResult1 | TResult2> {
  const internalThen = (promise as ObservableDurablePromise<T>)[DURABLE_PROMISE_INTERNAL_THEN];
  if (internalThen != null) {
    return internalThen.call(
      promise,
      onfulfilled,
      onrejected,
    ) as Promise<TResult1 | TResult2>;
  }
  return promise.then(onfulfilled, onrejected);
}

function internalAwaitable<T>(value: Awaitable<T>): Promise<T> {
  if ((typeof value === "object" || typeof value === "function") && value != null) {
    const internalThen = (
      value as Partial<ObservableDurablePromise<T>>
    )[DURABLE_PROMISE_INTERNAL_THEN];
    if (internalThen != null) {
      return internalThen.call(
        value,
        (resolved) => resolved,
        (error) => {
          throw error;
        },
      ) as Promise<T>;
    }
  }
  return Promise.resolve(value);
}

async function resolveAwaitableIterable<T>(
  items: AwaitableIterable<T>,
): Promise<T[]> {
  const iterable = await internalAwaitable(items);
  return internalPromiseAll([...iterable].map((item) => internalAwaitable(item)));
}

function wrapDurableAggregate<T>(completion: Promise<T>): Promise<T> {
  return currentFunctionRuntime()?.wrapFutureWait?.(completion) ?? completion;
}

function runInternalPromiseAggregate<T>(callback: () => T): T {
  const target = globalThis as typeof globalThis & {
    [INTERNAL_PROMISE_AGGREGATE_DEPTH_KEY]?: number;
  };
  target[INTERNAL_PROMISE_AGGREGATE_DEPTH_KEY] =
    (target[INTERNAL_PROMISE_AGGREGATE_DEPTH_KEY] ?? 0) + 1;
  try {
    return callback();
  } finally {
    const remaining = (target[INTERNAL_PROMISE_AGGREGATE_DEPTH_KEY] ?? 1) - 1;
    if (remaining === 0) delete target[INTERNAL_PROMISE_AGGREGATE_DEPTH_KEY];
    else target[INTERNAL_PROMISE_AGGREGATE_DEPTH_KEY] = remaining;
  }
}

function isInternalPromiseAggregate(): boolean {
  const target = globalThis as typeof globalThis & {
    [INTERNAL_PROMISE_AGGREGATE_DEPTH_KEY]?: number;
  };
  return (target[INTERNAL_PROMISE_AGGREGATE_DEPTH_KEY] ?? 0) > 0;
}

function internalPromiseAll<T>(
  values: Iterable<T | PromiseLike<T>>,
): Promise<Awaited<T>[]> {
  return runInternalPromiseAggregate(() => Promise.all(values));
}

function internalPromiseRace<T>(
  values: Iterable<T | PromiseLike<T>>,
): Promise<Awaited<T>> {
  return runInternalPromiseAggregate(() => Promise.race(values));
}

export function installPromiseInstrumentation(): void {
  const target = globalThis as typeof globalThis & {
    [PROMISE_INSTRUMENTED_KEY]?: true;
    [PROMISE_THEN_INSTRUMENTED_KEY]?: true;
  };
  if (target[PROMISE_THEN_INSTRUMENTED_KEY] !== true) {
    const originalThen = Promise.prototype.then;
    Object.defineProperty(Promise.prototype, "then", {
      ...Object.getOwnPropertyDescriptor(Promise.prototype, "then"),
      value: function then(
        this: Promise<unknown>,
        onfulfilled?: ((value: unknown) => unknown) | null,
        onrejected?: ((reason: unknown) => unknown) | null,
      ): Promise<unknown> {
        const result = Reflect.apply(
          originalThen,
          this,
          [onfulfilled, onrejected],
        ) as Promise<unknown>;
        if (isDetachedDurableThenable(this)) {
          currentFunctionRuntime()?.trackDurablePromiseConsumption?.(result);
        }
        return result;
      },
    });
    target[PROMISE_THEN_INSTRUMENTED_KEY] = true;
  }
  if (target[PROMISE_INSTRUMENTED_KEY] === true) return;
  const originalRace = Promise.race;
  const originalAny = Promise.any;
  const originalAll = Promise.all;
  const originalAllSettled = Promise.allSettled;
  const originalResolve = Promise.resolve;

  // Promise.resolve normally hides a durable Promise subclass inside a native
  // Promise. Preserve the durable provenance on that wrapper so a subsequent
  // all/allSettled/race/any can still distinguish it from an independent
  // contender.
  Object.defineProperty(Promise, "resolve", {
    ...Object.getOwnPropertyDescriptor(Promise, "resolve"),
    value: function resolve<T>(
      this: PromiseConstructor,
      value: T | PromiseLike<T>,
    ): Promise<Awaited<T>> {
      const result = Reflect.apply(originalResolve, this, [value]) as Promise<Awaited<T>>;
      if (isDurableThenable(value)) inheritDurableThenable(result, value as object);
      return result;
    },
  });

  Object.defineProperty(Promise, "race", {
    ...Object.getOwnPropertyDescriptor(Promise, "race"),
    value: function race<T>(
      this: PromiseConstructor,
      values: Iterable<T | PromiseLike<T>>,
    ): Promise<Awaited<T>> {
      const runtime = currentFunctionRuntime();
      if (runtime?.trackMixedPromiseRace == null) {
        return Reflect.apply(originalRace, this, [values]) as Promise<Awaited<T>>;
      }
      const durableContenders: unknown[] = [];
      let ordinaryContenderCount = 0;
      const trackedValues = {
        *[Symbol.iterator](): Iterator<T | PromiseLike<T>> {
          for (const value of values) {
            yield value;
          }
        },
      };
      const observedConstructor = observedPromiseConstructor(
        this,
        (normalized) => {
          if (isDurableThenable(normalized)) {
            durableContenders.push(normalized);
          } else {
            ordinaryContenderCount += 1;
          }
          return normalized;
        },
      );
      const construct = () =>
        Reflect.apply(
          originalRace,
          observedConstructor,
          [trackedValues],
        ) as Promise<Awaited<T>>;
      const result = runtime.runPromiseAggregateConstruction?.(construct) ?? construct();
      if (durableContenders.length > 0) {
        trackDurableAggregate(
          runtime,
          result,
          durableContenders,
          !isInternalPromiseAggregate(),
        );
      }
      if (
        durableContenders.length > 0
        && ordinaryContenderCount > 0
      ) {
        runtime.trackMixedPromiseRace(result);
      }
      return result;
    },
  });

  Object.defineProperty(Promise, "any", {
    ...Object.getOwnPropertyDescriptor(Promise, "any"),
    value: function any<T>(
      this: PromiseConstructor,
      values: Iterable<T | PromiseLike<T>>,
    ): Promise<Awaited<T>> {
      const runtime = currentFunctionRuntime();
      if (runtime?.trackMixedPromiseRace == null) {
        return Reflect.apply(originalAny, this, [values]) as Promise<Awaited<T>>;
      }
      const durableContenders: unknown[] = [];
      let ordinaryContenderCount = 0;
      const observation = promiseAggregateObservation("any");
      const trackedValues = {
        *[Symbol.iterator](): Iterator<T | PromiseLike<T>> {
          for (const value of values) {
            yield value;
          }
        },
      };
      const observedConstructor = observedPromiseConstructor(
        this,
        (normalized) => {
          if (isDurableThenable(normalized)) {
            durableContenders.push(normalized);
            return normalized;
          }
          ordinaryContenderCount += 1;
          return observePromiseAggregateContender(normalized, observation);
        },
      );
      const construct = () =>
        Reflect.apply(
          originalAny,
          observedConstructor,
          [trackedValues],
        ) as Promise<Awaited<T>>;
      const result = runtime.runPromiseAggregateConstruction?.(construct) ?? construct();
      observation.finishIteration(ordinaryContenderCount);
      if (durableContenders.length > 0) {
        trackDurableAggregate(
          runtime,
          result,
          durableContenders,
          !isInternalPromiseAggregate(),
        );
      }
      if (durableContenders.length > 0 && ordinaryContenderCount > 0) {
        if (
          observation.allOrdinaryValuesObservable
        ) {
          // Once every ordinary contender rejects, only a durable contender
          // can advance Promise.any. A fulfillment may itself be a pending
          // promise that the public aggregate adopts, so the ordinary gate
          // must not expire merely because its fulfillment callback ran.
          // Stop treating the ordinary path as a producer when either it or
          // the public aggregate settles. The latter covers a durable winner
          // as well as fulfilled-value adoption and abrupt iterator failures.
          runtime.trackMixedPromiseRace(result, [observation.ordinaryLifetime, result]);
        } else {
          // A non-object custom resolve result makes native Promise.any reject
          // the aggregate. Retain that aggregate as the producer until the
          // rejection becomes observable.
          runtime.trackMixedPromiseRace(result);
        }
      }
      return result;
    },
  });

  Object.defineProperty(Promise, "all", {
    ...Object.getOwnPropertyDescriptor(Promise, "all"),
    value: function all<T>(
      this: PromiseConstructor,
      values: Iterable<T | PromiseLike<T>>,
    ): Promise<Awaited<T>[]> {
      const runtime = currentFunctionRuntime();
      if (runtime?.trackMixedPromiseRace == null) {
        return Reflect.apply(originalAll, this, [values]) as Promise<Awaited<T>[]>;
      }
      const durableContenders: unknown[] = [];
      let ordinaryContenderCount = 0;
      const observation = promiseAggregateObservation("all");
      const trackedValues = {
        *[Symbol.iterator](): Iterator<T | PromiseLike<T>> {
          for (const value of values) {
            yield value;
          }
        },
      };
      const observedConstructor = observedPromiseConstructor(
        this,
        (normalized) => {
          if (isDurableThenable(normalized)) {
            durableContenders.push(normalized);
            return normalized;
          }
          ordinaryContenderCount += 1;
          return observePromiseAggregateContender(normalized, observation);
        },
      );
      const construct = () =>
        Reflect.apply(
          originalAll,
          observedConstructor,
          [trackedValues],
        ) as Promise<Awaited<T>[]>;
      const result = runtime.runPromiseAggregateConstruction?.(construct) ?? construct();
      observation.finishIteration(ordinaryContenderCount);
      if (durableContenders.length > 0) {
        trackDurableAggregate(
          runtime,
          result,
          durableContenders,
          !isInternalPromiseAggregate(),
        );
      }
      if (durableContenders.length > 0 && ordinaryContenderCount > 0) {
        if (
          observation.allOrdinaryValuesObservable
        ) {
          // Ordinary inputs can advance Promise.all until one rejects or all
          // fulfill. The non-rejecting lifetime gate models that boundary
          // without observing the public result or invoking a thenable twice.
          runtime.trackMixedPromiseRace(result, [observation.ordinaryLifetime, result]);
        } else {
          // A custom resolve result that is not thenable makes the public
          // aggregate reject. Its own settlement is the only observable
          // producer boundary available.
          runtime.trackMixedPromiseRace(result);
        }
      }
      return result;
    },
  });

  Object.defineProperty(Promise, "allSettled", {
    ...Object.getOwnPropertyDescriptor(Promise, "allSettled"),
    value: function allSettled<T>(
      this: PromiseConstructor,
      values: Iterable<T | PromiseLike<T>>,
    ): Promise<PromiseSettledResult<Awaited<T>>[]> {
      const runtime = currentFunctionRuntime();
      if (runtime?.trackMixedPromiseRace == null) {
        return Reflect.apply(
          originalAllSettled,
          this,
          [values],
        ) as Promise<PromiseSettledResult<Awaited<T>>[]>;
      }
      const durableContenders: unknown[] = [];
      let ordinaryContenderCount = 0;
      const observation = promiseAggregateObservation("all_settled");
      const trackedValues = {
        *[Symbol.iterator](): Iterator<T | PromiseLike<T>> {
          for (const value of values) yield value;
        },
      };
      const observedConstructor = observedPromiseConstructor(
        this,
        (normalized) => {
          if (isDurableThenable(normalized)) {
            durableContenders.push(normalized);
            return normalized;
          }
          ordinaryContenderCount += 1;
          return observePromiseAggregateContender(normalized, observation);
        },
      );
      const construct = () =>
        Reflect.apply(
          originalAllSettled,
          observedConstructor,
          [trackedValues],
        ) as Promise<PromiseSettledResult<Awaited<T>>[]>;
      const result = runtime.runPromiseAggregateConstruction?.(construct) ?? construct();
      observation.finishIteration(ordinaryContenderCount);
      if (durableContenders.length > 0) {
        trackDurableAggregate(
          runtime,
          result,
          durableContenders,
          !isInternalPromiseAggregate(),
        );
      }
      if (durableContenders.length > 0 && ordinaryContenderCount > 0) {
        if (observation.allOrdinaryValuesObservable) {
          // Ordinary inputs can advance Promise.allSettled until every one has
          // settled. The aggregate itself covers abrupt iterator/resolve
          // failures and durable completion.
          runtime.trackMixedPromiseRace(result, [observation.ordinaryLifetime, result]);
        } else {
          runtime.trackMixedPromiseRace(result);
        }
      }
      return result;
    },
  });
  target[PROMISE_INSTRUMENTED_KEY] = true;
}

export interface RegisteredFunction<Args extends readonly unknown[], Result> {
  (...args: Args): Promise<Result>;
  readonly definition: RegisteredDefinition;
  future(...args: Args): FunctionFuture<Result>;
  map(items: AwaitableIterable<Args[0]>): Promise<Result[]>;
  reduce(
    items: AwaitableIterable<Args[1]>,
    initial?: Args[0],
  ): Promise<Result>;
  tailCall(...args: Args): TailCall<Result>;
}

function normalizeOptions(options: FunctionOptions<readonly Parameter<unknown>[], unknown>): NormalizedFunctionOptions {
  validateGPUOption(options.gpu);
  if (options.secrets != null && !Array.isArray(options.secrets)) {
    throw new SDKUsageError("secrets must be an array of non-empty strings");
  }
  const normalizedRetries = options.retries == null
    ? undefined
    : retries(options.retries);
  const result: NormalizedFunctionOptions = {
    description: options.description ?? "",
    cpu: options.cpu ?? 1,
    memory: options.memory ?? 1,
    ephemeralDisk: options.ephemeralDisk ?? 10,
    gpu: Array.isArray(options.gpu) ? [...options.gpu] : options.gpu ?? null,
    timeout: options.timeout ?? 300,
    image: options.image,
    secrets: [...(options.secrets ?? [])],
    retries: normalizedRetries,
    region: options.region,
    warmContainers: options.warmContainers,
    minContainers: options.minContainers,
    maxContainers: options.maxContainers,
  };
  if (!Number.isFinite(result.cpu) || result.cpu <= 0) throw new SDKUsageError("cpu must be greater than zero");
  if (!Number.isFinite(result.memory) || result.memory <= 0) throw new SDKUsageError("memory must be greater than zero");
  if (!Number.isFinite(result.ephemeralDisk) || result.ephemeralDisk <= 0) throw new SDKUsageError("ephemeralDisk must be greater than zero");
  if (!Number.isInteger(result.timeout) || result.timeout <= 0 || result.timeout > 86_400) {
    throw new SDKUsageError("timeout must be an integer between 1 and 86400 seconds");
  }
  for (const [label, value] of [
    ["warmContainers", result.warmContainers],
    ["minContainers", result.minContainers],
    ["maxContainers", result.maxContainers],
  ] as const) {
    if (value != null && (!Number.isInteger(value) || value < 0)) {
      throw new SDKUsageError(`${label} must be a non-negative integer`);
    }
  }
  if (result.minContainers != null && result.maxContainers != null && result.minContainers > result.maxContainers) {
    throw new SDKUsageError("minContainers cannot exceed maxContainers");
  }
  if (result.warmContainers != null && result.maxContainers != null && result.warmContainers > result.maxContainers) {
    throw new SDKUsageError("warmContainers cannot exceed maxContainers");
  }
  if (result.region != null && !["us-east-1", "eu-west-1"].includes(result.region)) {
    throw new SDKUsageError(`Unsupported region '${result.region}'`);
  }
  if (result.secrets.some((secret) => typeof secret !== "string" || secret.length === 0)) {
    throw new SDKUsageError("secrets must contain non-empty strings");
  }
  return result;
}

function validateGPUOption(value: unknown): void {
  if (value == null) return;
  const values = Array.isArray(value) ? value : [value];
  for (const setting of values) {
    if (typeof setting !== "string") {
      throw new SDKUsageError("gpu must be a GPU model or an array of GPU models");
    }
    const [model, countText, ...extra] = setting.split(":");
    const count = countText == null ? 1 : Number(countText);
    if (
      !model
      || extra.length > 0
      || !Number.isInteger(count)
      || count <= 0
      || (countText != null && String(count) !== countText)
    ) {
      throw new SDKUsageError(`Invalid GPU setting '${setting}'`);
    }
  }
}

function validateArguments(definition: RegisteredDefinition, supplied: readonly unknown[]): unknown[] {
  const args = [...supplied];
  if (args.length > definition.parameters.length) {
    throw new SDKUsageError(`${definition.name} expected at most ${definition.parameters.length} arguments, received ${args.length}`);
  }
  definition.parameters.forEach((parameter, index) => {
    if (index >= args.length || args[index] === undefined) {
      if (parameter.hasDefault) args[index] = parameter.defaultValue;
      else if (parameter.required) throw new SDKUsageError(`${definition.name} is missing argument '${parameter.name}'`);
      return;
    }
    validateWithSchema(parameter.schema, args[index], `${definition.name}.${parameter.name}`);
  });
  return args;
}

export async function executeHandler<T>(
  definition: RegisteredDefinition,
  suppliedArgs: readonly unknown[],
): Promise<T> {
  const output = await executeHandlerResult<T>(definition, suppliedArgs);
  if (isTailCall(output)) {
    const runtime = currentFunctionRuntime();
    if (runtime == null) return executeHandler<T>(output.definition, output.args);
    return runtime.invoke<T>(output.definition, output.args);
  }
  return output;
}

/** Runs one handler frame without resolving a tail call. Used by language executors. */
export async function executeHandlerResult<T>(
  definition: RegisteredDefinition,
  suppliedArgs: readonly unknown[],
): Promise<T | TailCall<T>> {
  const args = validateArguments(definition, suppliedArgs);
  const result = definition.handler(...args);
  if (result == null || typeof (result as PromiseLike<unknown>).then !== "function") {
    throw new SDKUsageError(`Tensorlake handler '${definition.name}' must be async and return a Promise`);
  }
  const output = await result;
  if (isTailCall(output)) {
    return output as TailCall<T>;
  }
  validateWithSchema(definition.returns, output, `${definition.name} return value`);
  return output as T;
}

export function isTailCall(value: unknown): value is TailCall<unknown> {
  if (typeof value !== "object" || value == null) return false;
  const candidate = value as Partial<TailCall<unknown>>;
  return candidate[TAIL_CALL_BRAND] === true
    && candidate.kind === "tensorlake-tail-call"
    && typeof candidate.definition === "object"
    && candidate.definition != null
    && Array.isArray(candidate.args);
}

function createRegisteredFunction<Args extends readonly unknown[], Result>(
  definition: RegisteredDefinition,
): RegisteredFunction<Args, Result> {
  const callable = ((...args: Args): Promise<Result> => {
    const runtime = currentFunctionRuntime();
    return runtime == null
      ? executeHandler<Result>(definition, args)
      : runtime.invoke<Result>(definition, args);
  }) as RegisteredFunction<Args, Result>;
  Object.defineProperties(callable, {
    definition: { value: definition, enumerable: true },
    future: { value: (...args: Args) => new FunctionFuture<Result>(definition, args) },
    map: {
      value: (items: AwaitableIterable<Args[0]>) => {
        // Resolve inputs before assigning durable IDs. Awaiting each input inside
        // the invocation loop would make call order depend on promise timing,
        // allowing strict replay to associate a recorded result with a different
        // input while still matching the durable event sequence.
        const completion = (async () => {
          const resolvedItems = await resolveAwaitableIterable(items);
          return internalPromiseAll(
            resolvedItems.map((item) =>
              internalAwaitable(callable(...([item] as unknown as Args)))
            ),
          );
        })();
        return wrapDurableAggregate(completion);
      },
    },
    reduce: {
      value: (
        items: AwaitableIterable<Args[1]>,
        ...initialValues: [] | [Args[0]]
      ) => {
        const completion = (async () => {
          let values = await resolveAwaitableIterable(items);
          const hasInitial = initialValues.length > 0;
          let initial: unknown = initialValues[0];
          const runtime = currentFunctionRuntime();
          if (runtime != null) {
            return internalAwaitable(
              runtime.reduce<Result>(definition, values, initial, hasInitial),
            );
          }
          if (!hasInitial) {
            if (values.length === 0) {
              throw new SDKUsageError("reduce of empty iterable with no initial value");
            }
            [initial, ...values] = values;
          }
          let accumulator = initial as Result | Args[0];
          for (const item of values) {
            accumulator = await internalAwaitable(
              callable(...([accumulator, item] as unknown as Args)),
            );
          }
          return accumulator as Result;
        })();
        return wrapDurableAggregate(completion);
      },
    },
    tailCall: {
      value: (...args: Args): TailCall<Result> => ({
        [TAIL_CALL_BRAND]: true,
        kind: "tensorlake-tail-call",
        definition,
        args,
      }),
    },
  });
  return callable;
}

function register<
  const P extends readonly Parameter<unknown>[],
  Result,
>(
  handler: (...args: ParameterValues<P>) => Promise<Result | TailCall<Result>>,
  options: FunctionOptions<P, Result> | ApplicationOptions<P, Result>,
  application: boolean,
): RegisteredFunction<ParameterValues<P>, Result> {
  const name = options.name ?? handler.name;
  if (!name) throw new SDKUsageError("Anonymous Tensorlake handlers require an explicit name");
  if (!/^[A-Za-z_$][\w$]*$/.test(name)) throw new SDKUsageError(`Invalid Tensorlake function name '${name}'`);
  let applicationRetries: Retries | undefined;
  if (application) {
    const appOptions = options as ApplicationOptions<P, Result>;
    applicationRetries = retries(appOptions.applicationRetries ?? { maxRetries: 0 });
    if (
      appOptions.allow != null
      && (
        !Array.isArray(appOptions.allow)
        || appOptions.allow.some((capability) =>
          capability !== "unauthenticated_requests"
        )
      )
    ) {
      throw new SDKUsageError(
        "Application allow must contain only 'unauthenticated_requests'",
      );
    }
    const tags = appOptions.tags;
    const tagsPrototype = tags == null || typeof tags !== "object"
      ? undefined
      : Object.getPrototypeOf(tags);
    if (
      tags != null
      && (
        typeof tags !== "object"
        || Array.isArray(tags)
        || (tagsPrototype !== Object.prototype && tagsPrototype !== null)
        || Reflect.ownKeys(tags).some((key) =>
          typeof key !== "string"
          || !key
          || typeof (tags as Record<PropertyKey, unknown>)[key] !== "string"
        )
      )
    ) {
      throw new SDKUsageError("Application tags require non-empty string keys and string values");
    }
  }
  if (!Array.isArray(options.parameters)) {
    throw new SDKUsageError("Tensorlake registration parameters must be an array");
  }
  if (
    typeof options.returns !== "object"
    || options.returns == null
    || typeof options.returns.jsonSchema !== "object"
    || options.returns.jsonSchema == null
  ) {
    throw new SDKUsageError("Tensorlake registration returns must be a schema");
  }
  const parameterNames = new Set<string>();
  for (const parameter of options.parameters) {
    if (
      typeof parameter !== "object"
      || parameter == null
      || typeof parameter.name !== "string"
      || typeof parameter.schema !== "object"
      || parameter.schema == null
      || typeof parameter.schema.jsonSchema !== "object"
      || parameter.schema.jsonSchema == null
      || typeof parameter.required !== "boolean"
      || typeof parameter.hasDefault !== "boolean"
    ) {
      throw new SDKUsageError(
        "Tensorlake registration parameters must be created with schema.parameter()",
      );
    }
    if (parameterNames.has(parameter.name)) {
      throw new SDKUsageError(`Duplicate Tensorlake parameter name '${parameter.name}'`);
    }
    parameterNames.add(parameter.name);
  }
  const httpBodyParameter = options.parameters.find(
    (parameter) => parameter.schema._httpBody,
  );
  if (httpBodyParameter != null && !application) {
    throw new SDKUsageError(
      "HttpBody is only supported for application parameters",
    );
  }
  if (options.returns._httpBody) {
    throw new SDKUsageError(
      "HttpBody is only supported for application parameters; "
      + "use schema.file() or a JSON-compatible return schema instead",
    );
  }
  const definition: RegisteredDefinition = {
    name,
    handler: handler as (...args: unknown[]) => Promise<unknown>,
    parameters: [...options.parameters],
    returns: options.returns as Schema<unknown>,
    options: normalizeOptions(options as FunctionOptions<readonly Parameter<unknown>[], unknown>),
    application: application
      ? {
          tags: { ...((options as ApplicationOptions<P, Result>).tags ?? {}) },
          retries: applicationRetries as Retries,
          allow: [...((options as ApplicationOptions<P, Result>).allow ?? [])],
          version: randomUUID().replaceAll("-", ""),
        }
      : undefined,
  };
  registerDefinition(definition, application);
  return createRegisteredFunction(definition);
}

type SimpleResult<Output> = Output extends TailCall<infer Result> ? Result : Output;
type RuntimeHandler = (...args: never[]) => Promise<unknown>;

function hasExplicitSchemas(
  value: unknown,
): value is FunctionOptions<readonly Parameter<unknown>[], unknown> {
  if (typeof value !== "object" || value == null) return false;
  const hasParameters = Object.prototype.hasOwnProperty.call(value, "parameters");
  const hasReturns = Object.prototype.hasOwnProperty.call(value, "returns");
  if (hasParameters !== hasReturns) {
    throw new SDKUsageError("Tensorlake registrations must specify both parameters and returns");
  }
  return hasParameters;
}

function skipJavaScriptLiteral(source: string, start: number): number {
  const quote = source[start];
  let escaped = false;
  for (let index = start + 1; index < source.length; index += 1) {
    const character = source[index];
    if (escaped) {
      escaped = false;
    } else if (character === "\\") {
      escaped = true;
    } else if (character === quote) {
      return index;
    }
  }
  return source.length - 1;
}

function skipJavaScriptComment(source: string, start: number): number | undefined {
  if (source[start] !== "/") return undefined;
  if (source[start + 1] === "/") {
    const newline = source.indexOf("\n", start + 2);
    return newline < 0 ? source.length - 1 : newline;
  }
  if (source[start + 1] === "*") {
    const end = source.indexOf("*/", start + 2);
    return end < 0 ? source.length - 1 : end + 1;
  }
  return undefined;
}

function skipJavaScriptTrivia(source: string, start: number): number {
  let index = start;
  while (index < source.length) {
    while (index < source.length && /\s/.test(source[index])) index += 1;
    const commentEnd = skipJavaScriptComment(source, index);
    if (commentEnd == null) break;
    index = commentEnd + 1;
  }
  return index;
}

const REGEX_PREFIX_KEYWORDS = new Set([
  "await",
  "case",
  "delete",
  "in",
  "instanceof",
  "new",
  "return",
  "throw",
  "typeof",
  "void",
  "yield",
]);

function regexLiteralStartsAt(source: string, start: number): boolean {
  let expectsExpression = true;
  for (let index = 0; index < start; index += 1) {
    const character = source[index];
    if (/\s/.test(character)) continue;
    if (character === "\"" || character === "'" || character === "`") {
      index = skipJavaScriptLiteral(source, index);
      expectsExpression = false;
      continue;
    }
    const commentEnd = skipJavaScriptComment(source, index);
    if (commentEnd != null) {
      index = commentEnd;
      continue;
    }
    if (/[A-Za-z_$]/.test(character)) {
      let tokenEnd = index + 1;
      while (tokenEnd < start && /[A-Za-z0-9_$]/.test(source[tokenEnd])) tokenEnd += 1;
      expectsExpression = REGEX_PREFIX_KEYWORDS.has(source.slice(index, tokenEnd));
      index = tokenEnd - 1;
      continue;
    }
    if (/[0-9]/.test(character) || (character === "." && /[0-9]/.test(source[index + 1]))) {
      while (index + 1 < start && /[A-Za-z0-9_.]/.test(source[index + 1])) index += 1;
      expectsExpression = false;
      continue;
    }
    if (character === "/") {
      if (expectsExpression) {
        index = skipJavaScriptRegex(source, index);
        expectsExpression = false;
      } else {
        if (source[index + 1] === "=") index += 1;
        expectsExpression = true;
      }
      continue;
    }
    if (character === ")" || character === "]" || character === "}") {
      expectsExpression = false;
      continue;
    }
    if (character === ".") {
      if (source.slice(index, index + 3) === "...") {
        index += 2;
        expectsExpression = true;
      } else {
        expectsExpression = false;
      }
      continue;
    }
    if ((character === "+" || character === "-") && source[index + 1] === character) {
      index += 1;
      continue;
    }
    expectsExpression = true;
  }
  return expectsExpression;
}

function skipJavaScriptRegex(source: string, start: number): number {
  let escaped = false;
  let characterClass = false;
  for (let index = start + 1; index < source.length; index += 1) {
    const character = source[index];
    if (escaped) {
      escaped = false;
    } else if (character === "\\") {
      escaped = true;
    } else if (character === "[") {
      characterClass = true;
    } else if (character === "]") {
      characterClass = false;
    } else if (character === "/" && !characterClass) {
      while (index + 1 < source.length && /[A-Za-z]/.test(source[index + 1])) index += 1;
      return index;
    }
  }
  return source.length - 1;
}

function skipJavaScriptValue(source: string, index: number): number | undefined {
  const character = source[index];
  if (character === "\"" || character === "'" || character === "`") {
    return skipJavaScriptLiteral(source, index);
  }
  const commentEnd = skipJavaScriptComment(source, index);
  if (commentEnd != null) return commentEnd;
  if (character === "/" && regexLiteralStartsAt(source, index)) {
    return skipJavaScriptRegex(source, index);
  }
  return undefined;
}

function findTopLevelArrow(source: string): number {
  let parentheses = 0;
  let brackets = 0;
  let braces = 0;
  for (let index = 0; index < source.length - 1; index += 1) {
    const character = source[index];
    const valueEnd = skipJavaScriptValue(source, index);
    if (valueEnd != null) {
      index = valueEnd;
      continue;
    }
    if (character === "(") parentheses += 1;
    else if (character === ")") parentheses -= 1;
    else if (character === "[") brackets += 1;
    else if (character === "]") brackets -= 1;
    else if (character === "{") {
      // A top-level block before an arrow is a classic function or method
      // body. Its nested arrows cannot describe the handler parameters.
      if (parentheses === 0 && brackets === 0 && braces === 0) return -1;
      braces += 1;
    }
    else if (character === "}") braces -= 1;
    else if (character === "=" && source[index + 1] === ">" && parentheses === 0 && brackets === 0 && braces === 0) {
      return index;
    }
  }
  return -1;
}

function findClosingParenthesis(source: string, open: number): number {
  let depth = 0;
  for (let index = open; index < source.length; index += 1) {
    const character = source[index];
    const valueEnd = skipJavaScriptValue(source, index);
    if (valueEnd != null) {
      index = valueEnd;
      continue;
    }
    if (character === "(") depth += 1;
    else if (character === ")" && --depth === 0) return index;
  }
  return -1;
}

function findOpeningParenthesis(source: string): number {
  for (let index = 0; index < source.length; index += 1) {
    const valueEnd = skipJavaScriptValue(source, index);
    if (valueEnd != null) {
      index = valueEnd;
      continue;
    }
    if (source[index] === "(") return index;
  }
  return -1;
}

function splitTopLevelParameters(source: string): string[] {
  if (source.trim() === "") return [];
  const result: string[] = [];
  let start = 0;
  let parentheses = 0;
  let brackets = 0;
  let braces = 0;
  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];
    const valueEnd = skipJavaScriptValue(source, index);
    if (valueEnd != null) {
      index = valueEnd;
      continue;
    }
    if (character === "(") parentheses += 1;
    else if (character === ")") parentheses -= 1;
    else if (character === "[") brackets += 1;
    else if (character === "]") brackets -= 1;
    else if (character === "{") braces += 1;
    else if (character === "}") braces -= 1;
    else if (character === "," && parentheses === 0 && brackets === 0 && braces === 0) {
      result.push(source.slice(start, index).trim());
      start = index + 1;
    }
  }
  const final = source.slice(start).trim();
  if (final !== "") result.push(final);
  return result;
}

function hasTopLevelDefault(source: string): boolean {
  let parentheses = 0;
  let brackets = 0;
  let braces = 0;
  for (let index = 0; index < source.length; index += 1) {
    const character = source[index];
    const valueEnd = skipJavaScriptValue(source, index);
    if (valueEnd != null) {
      index = valueEnd;
      continue;
    }
    if (character === "(") parentheses += 1;
    else if (character === ")") parentheses -= 1;
    else if (character === "[") brackets += 1;
    else if (character === "]") brackets -= 1;
    else if (character === "{") braces += 1;
    else if (character === "}") braces -= 1;
    else if (character === "=" && parentheses === 0 && brackets === 0 && braces === 0) return true;
  }
  return false;
}

function declaredHandlerParameters(handler: RuntimeHandler): string[] {
  const source = Function.prototype.toString.call(handler).trim();
  if (source.includes("[native code]")) {
    throw new SDKUsageError("Schema-free registration cannot inspect a native or bound handler; specify parameters and returns");
  }
  const arrow = findTopLevelArrow(source);
  if (arrow >= 0) {
    let header = source.slice(0, arrow).trim();
    if (
      header.startsWith("async")
      && !/[A-Za-z0-9_$]/.test(header["async".length] ?? "")
    ) {
      header = header.slice(skipJavaScriptTrivia(header, "async".length));
    }
    if (!header.startsWith("(")) return [header];
    const close = findClosingParenthesis(header, 0);
    if (close < 0) throw new SDKUsageError("Schema-free registration could not inspect handler parameters");
    return splitTopLevelParameters(header.slice(1, close));
  }
  const open = findOpeningParenthesis(source);
  const close = open < 0 ? -1 : findClosingParenthesis(source, open);
  if (open < 0 || close < 0) {
    throw new SDKUsageError("Schema-free registration could not inspect handler parameters");
  }
  return splitTopLevelParameters(source.slice(open + 1, close));
}

function inferredJSONParameters(handler: RuntimeHandler): readonly Parameter<unknown>[] {
  const parameters = declaredHandlerParameters(handler);
  if (parameters.some((parameter) => {
    let index = 0;
    while (index < parameter.length) {
      while (index < parameter.length && /\s/.test(parameter[index])) index += 1;
      const commentEnd = skipJavaScriptComment(parameter, index);
      if (commentEnd == null) break;
      index = commentEnd + 1;
    }
    return parameter.slice(index).startsWith("...");
  })) {
    throw new SDKUsageError(
      "Schema-free registration does not support rest parameters; specify explicit parameters and returns",
    );
  }
  return parameters.map((parameter, index) => schema.parameter(
    parameters.length === 1 ? "input" : `arg${index}`,
    schema.json(),
    hasTopLevelDefault(parameter) ? { optional: true } : {},
  ));
}

function registerWithDefaults(
  name: string,
  handler: RuntimeHandler,
  options: SimpleFunctionOptions | SimpleApplicationOptions,
  application: boolean,
): RegisteredFunction<readonly unknown[], unknown> {
  return register(
    handler as (...args: readonly unknown[]) => Promise<unknown>,
    {
      ...options,
      name,
      parameters: inferredJSONParameters(handler),
      returns: schema.json(),
    },
    application,
  );
}

export function registerFunction<
  const P extends readonly Parameter<unknown>[],
  Result,
>(
  handler: (...args: ParameterValues<P>) => Promise<Result | TailCall<Result>>,
  options: FunctionOptions<P, Result>,
): RegisteredFunction<ParameterValues<P>, Result>;

export function registerFunction<
  const Args extends readonly unknown[],
  Output,
>(
  name: string,
  handler: (...args: Args) => Promise<Output>,
  options?: SimpleFunctionOptions,
): RegisteredFunction<Args, SimpleResult<Output>>;
export function registerFunction(
  handlerOrName: unknown,
  handlerOrOptions?: unknown,
  options: SimpleFunctionOptions = {},
): RegisteredFunction<readonly unknown[], unknown> {
  if (typeof handlerOrName === "string") {
    if (typeof handlerOrOptions !== "function") {
      throw new SDKUsageError("registerFunction(name, handler) requires an async handler");
    }
    return registerWithDefaults(
      handlerOrName,
      handlerOrOptions as RuntimeHandler,
      options,
      false,
    );
  }
  if (typeof handlerOrName !== "function") {
    throw new SDKUsageError("registerFunction requires an async handler");
  }
  const handler = handlerOrName as RuntimeHandler;
  if (typeof handlerOrOptions === "function") {
    throw new SDKUsageError("registerFunction received an unexpected second handler");
  }
  if (hasExplicitSchemas(handlerOrOptions)) {
    return register(
      handler as (...args: readonly unknown[]) => Promise<unknown>,
      handlerOrOptions,
      false,
    );
  }
  throw new SDKUsageError(
    "Schema-free functions require an explicit stable name: registerFunction(name, handler)",
  );
}

export function registerApplication<
  const P extends readonly Parameter<unknown>[],
  Result,
>(
  handler: (...args: ParameterValues<P>) => Promise<Result | TailCall<Result>>,
  options: ApplicationOptions<P, Result>,
): RegisteredFunction<ParameterValues<P>, Result>;
export function registerApplication<
  const Args extends readonly unknown[],
  Output,
>(
  name: string,
  handler: (...args: Args) => Promise<Output>,
  options?: SimpleApplicationOptions,
): RegisteredFunction<Args, SimpleResult<Output>>;
export function registerApplication(
  handlerOrName: unknown,
  handlerOrOptions?: unknown,
  options: SimpleApplicationOptions = {},
): RegisteredFunction<readonly unknown[], unknown> {
  if (typeof handlerOrName === "string") {
    if (typeof handlerOrOptions !== "function") {
      throw new SDKUsageError("registerApplication(name, handler) requires an async handler");
    }
    return registerWithDefaults(
      handlerOrName,
      handlerOrOptions as RuntimeHandler,
      options,
      true,
    );
  }
  if (typeof handlerOrName !== "function") {
    throw new SDKUsageError("registerApplication requires an async handler");
  }
  const handler = handlerOrName as RuntimeHandler;
  if (typeof handlerOrOptions === "function") {
    throw new SDKUsageError("registerApplication received an unexpected second handler");
  }
  if (hasExplicitSchemas(handlerOrOptions)) {
    return register(
      handler as (...args: readonly unknown[]) => Promise<unknown>,
      handlerOrOptions,
      true,
    );
  }
  throw new SDKUsageError(
    "Schema-free applications require an explicit stable name: registerApplication(name, handler)",
  );
}
