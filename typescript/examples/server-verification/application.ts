import {
  File,
  Future,
  Image,
  RequestContext,
  RequestError,
  registerApplication,
  registerFunction,
  retries,
  schema,
} from "tensorlake/applications";

const verificationImage = new Image({
  name: "typescript-server-verification-runtime",
  baseImage: "node:24-bookworm-slim",
});

const double = registerFunction(
  async (value: number) => value * 2,
  {
    name: "typescript_verification_double",
    parameters: [schema.parameter("value", schema.number())] as const,
    returns: schema.number(),
    image: verificationImage,
  },
);

const retryOnce = registerFunction(
  async (value: number) => {
    const context = RequestContext.get();
    const key = `retry-once:${value}`;
    const attempt = await context.state.get<number>(key, 0) ?? 0;
    await context.state.set(key, attempt + 1);
    if (attempt === 0) throw new Error("intentional first-attempt failure");
    return value;
  },
  {
    name: "typescript_verification_retry_once",
    parameters: [schema.parameter("value", schema.number())] as const,
    returns: schema.number(),
    retries: retries({ maxRetries: 1 }),
    image: verificationImage,
  },
);

const verificationInput = schema.object({
  label: schema.string(),
  values: schema.array(schema.number()),
});

const verificationOutput = schema.object({
  requestId: schema.string(),
  invocationHeader: schema.string(),
  rememberedLabel: schema.string(),
  doubled: schema.array(schema.number()),
  delayed: schema.number(),
  retried: schema.number(),
});

export const runtimeVerification = registerApplication(
  async (input: { label: string; values: number[] }) => {
    const context = RequestContext.get();
    await context.progress.update(1, 3, {
      message: "TypeScript verification started",
      attributes: { runtime: "typescript" },
    });
    await context.metrics.counter("typescript_verification_requests");
    await context.state.set("verification-label", input.label);

    const delayed = double.future(input.values[0] ?? 0).runLater(0.05);
    const doubled = await double.map(input.values);
    const retried = await retryOnce(input.label.length);
    const waited = await Future.wait([delayed]);
    const delayedResult = await waited.done[0].result();
    const rememberedLabel = await context.state.get<string>("verification-label", "");

    await context.metrics.timer("typescript_verification_values", input.values.length);
    await context.progress.update(3, 3, { message: "TypeScript verification completed" });
    return {
      requestId: context.requestId,
      invocationHeader: context.headers["x-tensorlake-verification"] ?? "",
      rememberedLabel: rememberedLabel ?? "",
      doubled,
      delayed: delayedResult,
      retried,
    };
  },
  {
    name: "typescript_runtime_verification",
    description: "Exercises the TypeScript function runtime against a live server",
    parameters: [schema.parameter("input", verificationInput)] as const,
    returns: verificationOutput,
    image: verificationImage,
    tags: { example: "server-verification", runtime: "typescript" },
  },
);

const increment = registerFunction(
  async (value: number) => value + 1,
  {
    name: "typescript_verification_increment",
    parameters: [schema.parameter("value", schema.number())] as const,
    returns: schema.number(),
    image: verificationImage,
  },
);

export const tailCallVerification = registerApplication(
  async (value: number) => increment.tailCall(value),
  {
    name: "typescript_tail_call_verification",
    parameters: [schema.parameter("value", schema.number())] as const,
    returns: schema.number(),
    image: verificationImage,
    tags: { example: "server-verification", feature: "tail-call" },
  },
);

export const fileVerification = registerApplication(
  async (file: File) => {
    const upper = new TextDecoder().decode(file.content).toUpperCase();
    return new File(new TextEncoder().encode(upper), file.contentType);
  },
  {
    name: "typescript_file_verification",
    parameters: [schema.parameter("file", schema.file())] as const,
    returns: schema.file(),
    image: verificationImage,
    tags: { example: "server-verification", feature: "file" },
  },
);

export const requestErrorVerification = registerApplication(
  async (message: string) => {
    throw new RequestError(message);
  },
  {
    name: "typescript_request_error_verification",
    parameters: [schema.parameter("message", schema.string())] as const,
    returns: schema.null(),
    image: verificationImage,
    tags: { example: "server-verification", feature: "request-error" },
  },
);

const nestedLeaf = registerFunction(
  async (value: number) => value + 1,
  {
    name: "typescript_verification_nested_leaf",
    parameters: [schema.parameter("value", schema.number())] as const,
    returns: schema.number(),
    image: verificationImage,
  },
);

const nestedMiddle = registerFunction(
  async (value: number) => (await nestedLeaf(value)) * 2,
  {
    name: "typescript_verification_nested_middle",
    parameters: [schema.parameter("value", schema.number())] as const,
    returns: schema.number(),
    image: verificationImage,
  },
);

export const nestedVerification = registerApplication(
  async (value: number) => nestedMiddle(value),
  {
    name: "typescript_nested_verification",
    parameters: [schema.parameter("value", schema.number())] as const,
    returns: schema.number(),
    image: verificationImage,
    tags: { example: "server-verification", feature: "nested-durable-calls" },
  },
);

const mixedFailure = registerFunction(
  async (value: number) => {
    if (value % 2 !== 0) throw new Error(`intentional mixed failure for ${value}`);
    return value * 10;
  },
  {
    name: "typescript_verification_mixed_failure",
    parameters: [schema.parameter("value", schema.number())] as const,
    returns: schema.number(),
    image: verificationImage,
  },
);

const alwaysFails = registerFunction(
  async () => {
    throw new Error("intentional exhausted retry failure");
  },
  {
    name: "typescript_verification_always_fails",
    parameters: [] as const,
    returns: schema.null(),
    retries: retries({ maxRetries: 2 }),
    image: verificationImage,
  },
);

export const failureVerification = registerApplication(
  async (values: number[]) => mixedFailure.map(values),
  {
    name: "typescript_failure_verification",
    parameters: [schema.parameter("values", schema.array(schema.number()))] as const,
    returns: schema.array(schema.number()),
    image: verificationImage,
    tags: { example: "server-verification", feature: "map-terminal-failure" },
  },
);

export const retryExhaustionVerification = registerApplication(
  async () => alwaysFails(),
  {
    name: "typescript_retry_exhaustion_verification",
    parameters: [] as const,
    returns: schema.null(),
    image: verificationImage,
    tags: { example: "server-verification", feature: "retry-exhaustion" },
  },
);

const reduceSum = registerFunction(
  async (accumulator: number, value: number) => {
    if (value < 0) throw new Error(`intentional reduce failure for ${value}`);
    return accumulator + value;
  },
  {
    name: "typescript_verification_reduce_sum",
    parameters: [
      schema.parameter("accumulator", schema.number()),
      schema.parameter("value", schema.number()),
    ] as const,
    returns: schema.number(),
    image: verificationImage,
  },
);

const reduceInput = schema.object({
  initial: schema.number(),
  values: schema.array(schema.number()),
});

const reduceOutput = schema.object({
  total: schema.number(),
  empty: schema.number(),
});

export const reduceVerification = registerApplication(
  async (input: { initial: number; values: number[] }) => ({
    total: await reduceSum.reduce(input.values, input.initial),
    empty: await reduceSum.reduce([], input.initial),
  }),
  {
    name: "typescript_reduce_verification",
    parameters: [schema.parameter("input", reduceInput)] as const,
    returns: reduceOutput,
    image: verificationImage,
    tags: { example: "server-verification", feature: "reduce" },
  },
);
