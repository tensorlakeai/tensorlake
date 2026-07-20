import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import type { Image } from "../image.js";
import { FunctionError, SDKUsageError } from "./errors.js";
import type { Parameter, ParameterValues, Schema } from "./schema.js";
import { validateWithSchema } from "./schema.js";
import { registerDefinition } from "./registry.js";

export interface Retries {
  maxRetries: number;
}

export function retries(options: Retries): Retries {
  if (!Number.isInteger(options.maxRetries) || options.maxRetries < 0 || options.maxRetries > 10) {
    throw new SDKUsageError("maxRetries must be an integer between 0 and 10");
  }
  return Object.freeze({ ...options });
}

export type Region = "us-east-1" | "eu-west-1";

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
}

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

export interface TailCall<T> {
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
}

// The user application and function executor are separate bundles loaded into
// one Node process. Store the runtime on globalThis so durable calls made by the
// application SDK copy reach the executor SDK copy that installed the runtime.
const FUNCTION_RUNTIME_STORAGE_KEY = Symbol.for(
  "tensorlake.applications.function-runtime-storage.v1",
);

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
  exception?: unknown;
  done = false;

  constructor(definition: RegisteredDefinition, args: readonly unknown[]) {
    this.definition = definition;
    this.args = args;
  }

  run(): this {
    if (this.promise != null) throw new SDKUsageError(`Future ${this.id} is already running`);
    const runtime = currentFunctionRuntime();
    this.promise = (runtime == null
      ? executeHandler<T>(this.definition, this.args)
      : runtime.runFuture(this)
    ).then(
      (value) => {
        this.done = true;
        return value;
      },
      (error) => {
        this.done = true;
        this.exception = error;
        throw error;
      },
    );
    return this;
  }

  runLater(delaySeconds: number): this {
    if (!Number.isFinite(delaySeconds) || delaySeconds < 0) {
      throw new SDKUsageError("Future delay must be a non-negative finite number");
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

  static async wait<T>(futures: FunctionFuture<T>[], options: WaitOptions = {}): Promise<WaitResult<T>> {
    const returnWhen = options.returnWhen ?? "all_completed";
    if (!["all_completed", "first_completed", "first_failure"].includes(returnWhen)) {
      throw new SDKUsageError(`Unsupported Future.wait returnWhen value '${returnWhen}'`);
    }
    if (options.timeout != null && (!Number.isFinite(options.timeout) || options.timeout < 0)) {
      throw new SDKUsageError("Future.wait timeout must be a non-negative finite number");
    }
    const pending = futures.filter((future) => !future.done);
    if (pending.length === 0) {
      return { done: [...futures], notDone: [] };
    }
    pending.forEach((future) => {
      if (future.promise == null) future.run();
    });
    const settled = pending.map((future) => future.result().then(
      () => ({ future }),
      () => ({ future }),
    ));
    let wait: Promise<unknown>;
    if (returnWhen === "all_completed") {
      wait = Promise.all(settled);
    } else if (returnWhen === "first_completed") {
      wait = Promise.race(settled);
    } else {
      const firstFailure = pending.map((future) => future.result().then(
        () => new Promise<never>(() => undefined),
        () => ({ future }),
      ));
      wait = Promise.race([Promise.all(settled), ...firstFailure]);
    }
    await waitUntilOrTimeout(wait, options.timeout);
    return {
      done: futures.filter((future) => future.done),
      notDone: futures.filter((future) => !future.done),
    };
  }
}

async function waitUntilOrTimeout(promise: Promise<unknown>, timeoutSeconds?: number): Promise<void> {
  if (timeoutSeconds == null) {
    await promise;
    return;
  }
  let timer: NodeJS.Timeout | undefined;
  await Promise.race([
    promise,
    new Promise<void>((resolve) => {
      timer = setTimeout(resolve, timeoutSeconds * 1000);
    }),
  ]);
  if (timer != null) clearTimeout(timer);
}

export interface RegisteredFunction<Args extends readonly unknown[], Result> {
  (...args: Args): Promise<Result>;
  readonly definition: RegisteredDefinition;
  future(...args: Args): FunctionFuture<Result>;
  map(items: Iterable<Args[0]>): Promise<Result[]>;
  reduce(items: Iterable<Args[1]>, initial: Args[0]): Promise<Result>;
  tailCall(...args: Args): TailCall<Result>;
}

function normalizeOptions(options: FunctionOptions<readonly Parameter<unknown>[], unknown>): NormalizedFunctionOptions {
  const result: NormalizedFunctionOptions = {
    description: options.description ?? "",
    cpu: options.cpu ?? 1,
    memory: options.memory ?? 1,
    ephemeralDisk: options.ephemeralDisk ?? 10,
    gpu: options.gpu ?? null,
    timeout: options.timeout ?? 300,
    image: options.image,
    secrets: [...(options.secrets ?? [])],
    retries: options.retries,
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
  if (result.retries != null) retries(result.retries);
  if (result.region != null && !["us-east-1", "eu-west-1"].includes(result.region)) {
    throw new SDKUsageError(`Unsupported region '${result.region}'`);
  }
  if (result.secrets.some((secret) => typeof secret !== "string" || secret.length === 0)) {
    throw new SDKUsageError("secrets must contain non-empty strings");
  }
  return result;
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
  return typeof value === "object" && value != null && (value as TailCall<unknown>).kind === "tensorlake-tail-call";
}

function createRegisteredFunction<Args extends readonly unknown[], Result>(
  definition: RegisteredDefinition,
): RegisteredFunction<Args, Result> {
  const callable = (async (...args: Args): Promise<Result> => {
    const runtime = currentFunctionRuntime();
    return runtime == null
      ? executeHandler<Result>(definition, args)
      : runtime.invoke<Result>(definition, args);
  }) as RegisteredFunction<Args, Result>;
  Object.defineProperties(callable, {
    definition: { value: definition, enumerable: true },
    future: { value: (...args: Args) => new FunctionFuture<Result>(definition, args) },
    map: {
      value: async (items: Iterable<Args[0]>) =>
        Promise.all([...items].map((item) => callable(...([item] as unknown as Args)))),
    },
    reduce: {
      value: async (items: Iterable<Args[1]>, initial: Args[0]) => {
        let accumulator: Result | Args[0] = initial;
        for (const item of items) {
          accumulator = await callable(...([accumulator, item] as unknown as Args));
        }
        return accumulator as Result;
      },
    },
    tailCall: {
      value: (...args: Args): TailCall<Result> => ({
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
  if (application) {
    const appOptions = options as ApplicationOptions<P, Result>;
    retries(appOptions.applicationRetries ?? { maxRetries: 0 });
    for (const [key, value] of Object.entries(appOptions.tags ?? {})) {
      if (!key || typeof value !== "string") throw new SDKUsageError("Application tags require non-empty string keys and string values");
    }
  }
  const definition: RegisteredDefinition = {
    name,
    handler: handler as (...args: unknown[]) => Promise<unknown>,
    parameters: options.parameters,
    returns: options.returns as Schema<unknown>,
    options: normalizeOptions(options as FunctionOptions<readonly Parameter<unknown>[], unknown>),
    application: application
      ? {
          tags: { ...((options as ApplicationOptions<P, Result>).tags ?? {}) },
          retries: (options as ApplicationOptions<P, Result>).applicationRetries ?? retries({ maxRetries: 0 }),
          version: randomUUID().replaceAll("-", ""),
        }
      : undefined,
  };
  registerDefinition(definition, application);
  return createRegisteredFunction(definition);
}

export function registerFunction<
  const P extends readonly Parameter<unknown>[],
  Result,
>(
  handler: (...args: ParameterValues<P>) => Promise<Result | TailCall<Result>>,
  options: FunctionOptions<P, Result>,
): RegisteredFunction<ParameterValues<P>, Result> {
  return register(handler, options, false);
}

export function registerApplication<
  const P extends readonly Parameter<unknown>[],
  Result,
>(
  handler: (...args: ParameterValues<P>) => Promise<Result | TailCall<Result>>,
  options: ApplicationOptions<P, Result>,
): RegisteredFunction<ParameterValues<P>, Result> {
  return register(handler, options, true);
}
