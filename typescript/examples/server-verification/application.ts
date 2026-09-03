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
  baseImage: "node:24-trixie",
});

const double = registerFunction(
  "typescript_verification_double",
  async (value: number) => value * 2,
  {
    image: verificationImage,
  },
);

const retryOnce = registerFunction(
  "typescript_verification_retry_once",
  async (value: number) => {
    const context = RequestContext.get();
    const key = `retry-once:${value}`;
    const attempt = await context.state.get<number>(key, 0) ?? 0;
    await context.state.set(key, attempt + 1);
    if (attempt === 0) throw new Error("intentional first-attempt failure");
    return value;
  },
  {
    retries: retries({ maxRetries: 1 }),
    image: verificationImage,
  },
);

export const runtimeVerification = registerApplication(
  "typescript_runtime_verification",
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
      rememberedLabel: rememberedLabel ?? "",
      doubled,
      delayed: delayedResult,
      retried,
    };
  },
  {
    description: "Exercises the TypeScript function runtime against a live server",
    image: verificationImage,
    tags: { example: "server-verification", runtime: "typescript" },
  },
);

const increment = registerFunction(
  "typescript_verification_increment",
  async (value: number) => value + 1,
  {
    image: verificationImage,
  },
);

export const tailCallVerification = registerApplication(
  "typescript_tail_call_verification",
  async (value: number) => increment.tailCall(value),
  {
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
  "typescript_request_error_verification",
  async (message: string) => {
    throw new RequestError(message);
  },
  {
    image: verificationImage,
    tags: { example: "server-verification", feature: "request-error" },
  },
);

const nestedLeaf = registerFunction(
  "typescript_verification_nested_leaf",
  async (value: number) => value + 1,
  {
    image: verificationImage,
  },
);

const nestedMiddle = registerFunction(
  "typescript_verification_nested_middle",
  async (value: number) => (await nestedLeaf(value)) * 2,
  {
    image: verificationImage,
  },
);

export const nestedVerification = registerApplication(
  "typescript_nested_verification",
  async (value: number) => nestedMiddle(value),
  {
    image: verificationImage,
    tags: { example: "server-verification", feature: "nested-durable-calls" },
  },
);

const mixedFailure = registerFunction(
  "typescript_verification_mixed_failure",
  async (value: number) => {
    if (value % 2 !== 0) throw new Error(`intentional mixed failure for ${value}`);
    return value * 10;
  },
  {
    image: verificationImage,
  },
);

const alwaysFails = registerFunction(
  "typescript_verification_always_fails",
  async () => {
    throw new Error("intentional exhausted retry failure");
  },
  {
    retries: retries({ maxRetries: 2 }),
    image: verificationImage,
  },
);

export const failureVerification = registerApplication(
  "typescript_failure_verification",
  async (values: number[]) => mixedFailure.map(values),
  {
    image: verificationImage,
    tags: { example: "server-verification", feature: "map-terminal-failure" },
  },
);

export const retryExhaustionVerification = registerApplication(
  "typescript_retry_exhaustion_verification",
  async () => alwaysFails(),
  {
    image: verificationImage,
    tags: { example: "server-verification", feature: "retry-exhaustion" },
  },
);

const reduceSum = registerFunction(
  "typescript_verification_reduce_sum",
  async (accumulator: number, value: number) => {
    if (value < 0) throw new Error(`intentional reduce failure for ${value}`);
    return accumulator + value;
  },
  {
    image: verificationImage,
  },
);

export const reduceVerification = registerApplication(
  "typescript_reduce_verification",
  async (input: { initial: number; values: number[] }) => ({
    total: await reduceSum.reduce(input.values, input.initial),
    empty: await reduceSum.reduce([], input.initial),
  }),
  {
    image: verificationImage,
    tags: { example: "server-verification", feature: "reduce" },
  },
);
