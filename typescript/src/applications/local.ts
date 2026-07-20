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
import { MemoryRequestContext, runWithRequestContext } from "./context.js";
import { File, isFile } from "./file.js";
import { jsonRoundTrip } from "./serialization.js";

export class LocalRequest<T> {
  constructor(readonly id: string, private readonly value: Promise<T>) {}

  output(): Promise<T> {
    return this.value;
  }
}

function boundaryCopy<T>(value: T): T {
  if (isFile(value)) return new File(value.content.slice(), value.contentType) as T;
  return jsonRoundTrip(value);
}

export class LocalRuntime implements FunctionRuntime {
  readonly requestContext: MemoryRequestContext;
  private readonly application: RegisteredDefinition;

  constructor(application: RegisteredDefinition, requestId = `local-${randomUUID()}`) {
    this.application = application;
    this.requestContext = new MemoryRequestContext(requestId);
  }

  invoke<T>(definition: RegisteredDefinition, args: readonly unknown[]): Promise<T> {
    return this.execute<T>(definition, args);
  }

  async runFuture<T>(future: FunctionFuture<T>): Promise<T> {
    if (future.delaySeconds > 0) {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, future.delaySeconds * 1000);
        this.requestContext.signal.addEventListener("abort", () => {
          clearTimeout(timer);
          reject(this.requestContext.signal.reason);
        }, { once: true });
      });
    }
    return this.execute<T>(future.definition, future.args);
  }

  private async execute<T>(definition: RegisteredDefinition, rawArgs: readonly unknown[]): Promise<T> {
    const args = rawArgs.map(boundaryCopy);
    const maxRetries = definition.options.retries?.maxRetries
      ?? this.application.application?.retries.maxRetries
      ?? 0;
    let lastError: unknown;
    for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
      try {
        const value = await runWithFunctionRuntime(this, () =>
          runWithRequestContext(this.requestContext, () =>
            this.withTimeout(executeHandler<T>(definition, args), definition.options.timeout),
          ),
        );
        return boundaryCopy(value);
      } catch (error) {
        if (isRequestError(error)) throw error;
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
  return new LocalRequest(runtime.requestContext.requestId, output);
}
