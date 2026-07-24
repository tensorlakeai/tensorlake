import { randomUUID } from "node:crypto";
import {
  executeHandler,
  type FunctionFuture,
  type FunctionRuntime,
  type RegisteredDefinition,
  type RegisteredFunction,
  runWithFunctionRuntime,
} from "./function.js";
import { FunctionError, SDKUsageError, isRequestError } from "./errors.js";
import {
  MemoryRequestContext,
  runWithRequestContext,
  waitWithAbortSignal,
} from "./context.js";
import { File, isFile } from "./file.js";
import { jsonRoundTrip } from "./serialization.js";

export class LocalRequest<T> {
  constructor(
    readonly id: string,
    private readonly value: Promise<T>,
    private readonly cancelRequest: (reason?: unknown) => void,
  ) {}

  output(): Promise<T> {
    return this.value;
  }

  cancel(reason?: unknown): void {
    this.cancelRequest(reason);
  }
}

function boundaryCopy<T>(value: T): T {
  if (isFile(value)) return new File(value.content.slice(), value.contentType) as T;
  return jsonRoundTrip(value);
}

export class LocalRuntime implements FunctionRuntime {
  readonly requestContext: MemoryRequestContext;
  private readonly application: RegisteredDefinition;
  private readonly controller = new AbortController();

  constructor(application: RegisteredDefinition, requestId = `local-${randomUUID()}`) {
    this.application = application;
    this.requestContext = new MemoryRequestContext(requestId, {
      signal: this.controller.signal,
    });
  }

  cancel(reason: unknown = new FunctionError("Local request was cancelled")): void {
    if (!this.controller.signal.aborted) this.controller.abort(reason);
  }

  invoke<T>(definition: RegisteredDefinition, args: readonly unknown[]): Promise<T> {
    return this.execute<T>(definition, args);
  }

  async runFuture<T>(future: FunctionFuture<T>): Promise<T> {
    if (future.delaySeconds > 0) {
      let timer: NodeJS.Timeout | undefined;
      try {
        await waitWithAbortSignal(
          new Promise<void>((resolve) => {
            timer = setTimeout(resolve, future.delaySeconds * 1000);
          }),
          this.requestContext.signal,
        );
      } finally {
        if (timer != null) clearTimeout(timer);
      }
    }
    return this.execute<T>(future.definition, future.args);
  }

  async reduce<T>(
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
    let accumulator = boundaryCopy(reduceInitial);
    for (const item of reduceItems) {
      accumulator = await this.execute<T>(definition, [accumulator, item]);
    }
    return accumulator as T;
  }

  private async execute<T>(definition: RegisteredDefinition, rawArgs: readonly unknown[]): Promise<T> {
    const originalArgs = rawArgs.map(boundaryCopy);
    const maxRetries = definition.options.retries?.maxRetries
      ?? this.application.application?.retries.maxRetries
      ?? 0;
    let lastError: unknown;
    for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
      const args = originalArgs.map(boundaryCopy);
      try {
        const value = await runWithFunctionRuntime(this, () =>
          runWithRequestContext(this.requestContext, () =>
            waitWithAbortSignal(
              this.withTimeout(executeHandler<T>(definition, args), definition.options.timeout),
              this.controller.signal,
            ),
          ),
        );
        return boundaryCopy(value);
      } catch (error) {
        if (isRequestError(error)) throw error;
        if (this.controller.signal.aborted) throw this.controller.signal.reason;
        lastError = error;
      }
    }
    throw lastError instanceof Error
      ? new FunctionError(`${definition.name} failed: ${lastError.message}`, { cause: lastError })
      : new FunctionError(`${definition.name} failed`);
  }

  private async withTimeout<T>(promise: Promise<T>, timeoutSeconds: number): Promise<T> {
    let timer: NodeJS.Timeout | undefined;
    try {
      return await Promise.race([
        promise,
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () => reject(new FunctionError(`Function timed out after ${timeoutSeconds} seconds`)),
            timeoutSeconds * 1000,
          );
        }),
      ]);
    } finally {
      if (timer != null) clearTimeout(timer);
    }
  }
}

export async function runLocal<Args extends readonly unknown[], Result>(
  application: RegisteredFunction<Args, Result>,
  ...args: Args
): Promise<LocalRequest<Result>> {
  if (application.definition.application == null) {
    throw new SDKUsageError(`${application.definition.name} is not a Tensorlake application`);
  }
  const runtime = new LocalRuntime(application.definition);
  const output = runtime.invoke<Result>(application.definition, args);
  return new LocalRequest(
    runtime.requestContext.requestId,
    output,
    (reason) => runtime.cancel(reason),
  );
}
