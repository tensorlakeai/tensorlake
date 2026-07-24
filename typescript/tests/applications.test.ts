import { describe, expect, it } from "vitest";
import {
  File,
  FunctionError,
  Future,
  RequestContext,
  RequestError,
  createApplicationManifest,
  registerApplication,
  registerFunction,
  runLocal,
  schema,
} from "../src/applications/index.js";
import { clearRegistryForTest } from "../src/applications/registry.js";
import { isRequestError } from "../src/applications/errors.js";
import { isFile } from "../src/applications/file.js";
import { validateWithSchema } from "../src/applications/schema.js";
import { serializeValue } from "../src/applications/serialization.js";
import { MemoryRequestContext } from "../src/applications/context.js";

describe("TypeScript applications", () => {
  it("runs async fan-out locally across JSON boundaries", async () => {
    clearRegistryForTest();
    const double = registerFunction("double", async (value: number) => value * 2);
    const app = registerApplication(
      "double_all",
      async (values: number[]) => double.map(values),
    );

    const request = await runLocal(app, [1, 2, 3]);
    expect(await request.output()).toEqual([2, 4, 6]);
    expect(double.definition.parameters.map((parameter) => parameter.name)).toEqual(["input"]);
    expect(app.definition.parameters.map((parameter) => parameter.name)).toEqual(["input"]);
    expect(app.definition.returns.jsonSchema).toEqual({});
    expect(createApplicationManifest(app.definition).functions.double_all).toMatchObject({
      parameters: [{ name: "input", data_type: { title: "input" }, required: true }],
      return_type: { title: "Return value" },
    });
  });

  it("reduces sequentially from an explicit initial value and handles empty input", async () => {
    clearRegistryForTest();
    const subtract = registerFunction(
      "subtract",
      async (accumulator: number, value: number) => accumulator - value,
    );
    const app = registerApplication(
      "subtract_all",
      async (values: number[]) => ({
        populated: await subtract.reduce(values, 10),
        empty: await subtract.reduce([], 10),
      }),
    );

    const request = await runLocal(app, [1, 2, 3]);
    expect(await request.output()).toEqual({ populated: 4, empty: 10 });
    expect(subtract.definition.parameters.map((parameter) => parameter.name)).toEqual([
      "arg0",
      "arg1",
    ]);
  });

  it("reduces without an initial value and accepts asynchronously produced collections", async () => {
    clearRegistryForTest();
    const double = registerFunction("reduce_input_double", async (value: number) => value * 2);
    const add = registerFunction(
      "reduce_without_initial_add",
      async (accumulator: number, value: number) => accumulator + value,
    );
    const app = registerApplication(
      "reduce_without_initial",
      async (values: number[]) => ({
        mapped: await double.map(Promise.resolve(values)),
        reduced: await add.reduce(double.map(values)),
        futureItems: await add.reduce([
          Promise.resolve(1),
          double.future(2),
          Promise.resolve(3),
        ], 0),
        singleton: await add.reduce([7]),
      }),
    );

    const request = await runLocal(app, [1, 2, 3]);
    expect(await request.output()).toEqual({
      mapped: [2, 4, 6],
      reduced: 12,
      futureItems: 8,
      singleton: 7,
    });
    await expect(add.reduce([])).rejects.toThrow("reduce of empty iterable with no initial value");
  });

  it("keeps runtime options and inferred types in the simple form", async () => {
    clearRegistryForTest();
    const increment = registerFunction(
      "increment",
      async (value: number) => value + 1,
      { cpu: 2, timeout: 60 },
    );
    const app = registerApplication(
      "named_application",
      async (value: number) => increment(value),
    );

    expect(increment.definition.name).toBe("increment");
    expect(increment.definition.options.cpu).toBe(2);
    expect(increment.definition.options.timeout).toBe(60);
    expect(app.definition.name).toBe("named_application");
    await expect(runLocal(app, 41).then((request) => request.output())).resolves.toBe(42);
  });

  it("infers optional descriptors for default parameters in schema-free registrations", async () => {
    clearRegistryForTest();
    const greet = registerFunction(
      "defaulted_greeting",
      async (name = "world", punctuation = "!") => `Hello, ${name}${punctuation}`,
    );
    const app = registerApplication(
      "defaulted_application",
      async (name = "world") => greet(name),
    );

    expect(greet.definition.parameters.map((parameter) => parameter.required)).toEqual([false, false]);
    expect(app.definition.parameters).toHaveLength(1);
    expect(app.definition.parameters[0].required).toBe(false);
    await expect(greet()).resolves.toBe("Hello, world!");
    await expect(runLocal(app).then((request) => request.output())).resolves.toBe("Hello, world!");
  });

  it("parses commas inside regex-literal defaults", async () => {
    clearRegistryForTest();
    const matches = registerFunction(
      "regex_default",
      async (pattern = /a,b/) => pattern.test("a,b"),
    );

    expect(matches.definition.parameters).toHaveLength(1);
    expect(matches.definition.parameters[0].required).toBe(false);
    await expect(matches()).resolves.toBe(true);
  });

  it("distinguishes regex literals from division in parameter defaults", async () => {
    clearRegistryForTest();
    let numerator = 4;
    const denominator = 2;
    const calculate = registerFunction(
      "division_default",
      async (value = numerator++ / denominator, increment = 1) => value + increment,
    );
    const divisionThenRegexHandler = (0, eval)(
      "(async (value = 1 / /a,b/.test('a,b'), increment = 1) => value + increment)",
    ) as (value?: number, increment?: number) => Promise<number>;
    const divideByRegexResult = registerFunction(
      "division_then_regex_default",
      divisionThenRegexHandler,
    );
    const commentedRegex = (0, eval)(
      "(async (pattern = // pattern default\n /a,b/, fallback = false) => pattern.test('a,b') || fallback)",
    ) as (pattern?: RegExp, fallback?: boolean) => Promise<boolean>;
    const matches = registerFunction("commented_regex_default", commentedRegex);

    expect(calculate.definition.parameters.map((parameter) => parameter.required))
      .toEqual([false, false]);
    expect(divideByRegexResult.definition.parameters.map((parameter) => parameter.required))
      .toEqual([false, false]);
    expect(matches.definition.parameters.map((parameter) => parameter.required))
      .toEqual([false, false]);
    await expect(calculate()).resolves.toBe(3);
    await expect(divideByRegexResult()).resolves.toBe(2);
    await expect(matches()).resolves.toBe(true);
  });

  it("does not inspect classic function bodies for handler arrows", async () => {
    clearRegistryForTest();
    const classicHandler = (0, eval)(`(async function(value) {
      if (true) /}/.test("}");
      const nested = item => item;
      return nested(value);
    })`) as (value: number) => Promise<number>;
    const classic = registerFunction("classic_handler", classicHandler);

    expect(classic.definition.parameters).toHaveLength(1);
    expect(classic.definition.parameters[0].required).toBe(true);
    await expect(classic(21)).resolves.toBe(21);
  });

  it("ignores parentheses in comments before classic function parameters", async () => {
    clearRegistryForTest();
    const commentedHandler = (0, eval)(
      "(async function /* misleading ( */ (value) { return value; })",
    ) as (value: number) => Promise<number>;
    const classic = registerFunction("commented_classic_handler", commentedHandler);

    expect(classic.definition.parameters).toHaveLength(1);
    expect(classic.definition.parameters[0].required).toBe(true);
    await expect(classic(21)).resolves.toBe(21);
  });

  it("does not confuse user JSON with the SDK tail-call control value", async () => {
    clearRegistryForTest();
    const increment = registerFunction("tail_call_increment", async (value: number) => value + 1);
    const collision = registerApplication(
      "tail_call_tag_collision",
      async () => ({ kind: "tensorlake-tail-call", payload: 21 }),
    );
    const genuine = registerApplication(
      "branded_tail_call",
      async () => increment.tailCall(41),
    );

    await expect(runLocal(collision).then((request) => request.output())).resolves.toEqual({
      kind: "tensorlake-tail-call",
      payload: 21,
    });
    await expect(runLocal(genuine).then((request) => request.output())).resolves.toBe(42);
  });

  it("requires explicit schemas for rest-parameter handlers", () => {
    clearRegistryForTest();
    expect(() => registerFunction(
      "rest_function",
      async (...values: number[]) => values.reduce((total, value) => total + value, 0),
    )).toThrow("does not support rest parameters");
    expect(() => registerApplication(
      "rest_application",
      async (...values: number[]) => values,
    )).toThrow("does not support rest parameters");

    const commentedRest = (0, eval)(
      "(async (/* values */ ...values) => values)",
    ) as (...values: number[]) => Promise<number[]>;
    expect(() => registerFunction("commented_rest_function", commentedRest))
      .toThrow("does not support rest parameters");
  });

  it("requires async handlers", async () => {
    clearRegistryForTest();
    const app = registerApplication(((value: string) => value) as never, {
      name: "not_async",
      parameters: [schema.parameter("value", schema.string())] as const,
      returns: schema.string(),
    });
    const request = await runLocal(app, "x");
    await expect(request.output()).rejects.toThrow("must be async");
  });

  it("does not retry request errors", async () => {
    clearRegistryForTest();
    let calls = 0;
    const app = registerApplication(async () => {
      calls += 1;
      throw new RequestError("stop");
    }, {
      name: "request_error",
      parameters: [] as const,
      returns: schema.null(),
      applicationRetries: { maxRetries: 3 },
    });
    const request = await runLocal(app);
    await expect(request.output()).rejects.toThrow("stop");
    expect(calls).toBe(1);
  });

  it("starts every local retry with a fresh boundary copy of its arguments", async () => {
    clearRegistryForTest();
    let attempts = 0;
    const app = registerApplication(async (input: { count: number }) => {
      input.count += 1;
      attempts += 1;
      if (attempts === 1) throw new Error("retry me");
      return input.count;
    }, {
      name: "isolated_retry_arguments",
      parameters: [schema.parameter("input", schema.object({ count: schema.number() }))] as const,
      returns: schema.number(),
      applicationRetries: { maxRetries: 1 },
    });
    const input = { count: 0 };

    const request = await runLocal(app, input);
    await expect(request.output()).resolves.toBe(1);
    expect(input).toEqual({ count: 0 });
    expect(attempts).toBe(2);
  });

  it("keeps local request state behind the same JSON boundary as deployed state", async () => {
    const context = new MemoryRequestContext("local-state");
    const source = { nested: { value: 1 } };
    await context.state.set("value", source);
    source.nested.value = 2;

    const first = await context.state.get<typeof source>("value");
    expect(first).toEqual({ nested: { value: 1 } });
    first!.nested.value = 3;
    await expect(context.state.get("value")).resolves.toEqual({ nested: { value: 1 } });
    await expect(context.state.set(
      "file",
      new File(new Uint8Array([1]), "application/octet-stream"),
    )).rejects.toThrow("JSON values");
  });

  it("emits an existing-server-compatible manifest", () => {
    clearRegistryForTest();
    const app = registerApplication(async (value: string) => value, {
      name: "echo",
      parameters: [schema.parameter("value", schema.string())] as const,
      returns: schema.string(),
      region: "eu-west-1",
    });
    const manifest = createApplicationManifest(app.definition);
    expect(manifest.entrypoint.input_serializer).toBe("json");
    expect(manifest.functions.echo.parameters[0].data_type.type).toBe("string");
    expect(manifest.functions.echo.placement_constraints.filter_expressions).toEqual(["region==eu-west-1"]);
  });

  it("supports direct File values but rejects nested files", () => {
    expect(serializeValue(new File(new Uint8Array([1, 2]), "application/octet-stream")).encoding).toBe("raw");
    expect(() => serializeValue({ file: new File(new Uint8Array([1]), "application/octet-stream") }))
      .toThrow("nested File");
  });

  it("supports File instanceof checks across SDK bundles", () => {
    const foreignFile = {
      [Symbol.for("tensorlake.applications.file.v1")]: true,
      content: new Uint8Array([1, 2, 3]),
      contentType: "text/plain",
    };

    expect(foreignFile).toBeInstanceOf(File);
    expect(isFile(foreignFile)).toBe(true);
    expect(validateWithSchema(schema.file(), foreignFile, "foreign file")).toBe(foreignFile);
    expect(serializeValue(foreignFile)).toEqual({
      data: foreignFile.content,
      contentType: "text/plain",
      encoding: "raw",
    });

    class SpecializedFile extends File {}
    expect(new File(new Uint8Array([1]), "text/plain"))
      .not.toBeInstanceOf(SpecializedFile);
    expect(new SpecializedFile(new Uint8Array([1]), "text/plain"))
      .toBeInstanceOf(SpecializedFile);
  });

  it("supports RequestError instanceof checks across SDK bundles", () => {
    const foreignError = Object.assign(new Error("stop across bundle boundary"), {
      [Symbol.for("tensorlake.applications.request-error.v1")]: true,
    });

    expect(foreignError).toBeInstanceOf(RequestError);
    expect(isRequestError(foreignError)).toBe(true);

    class SpecializedRequestError extends RequestError {}
    expect(new RequestError("base request error"))
      .not.toBeInstanceOf(SpecializedRequestError);
    expect(new SpecializedRequestError("specialized request error"))
      .toBeInstanceOf(SpecializedRequestError);
  });

  it("supports catching FunctionError values created by another SDK bundle", () => {
    const foreignError = Object.assign(new Error("child failed across bundle boundary"), {
      [Symbol.for("tensorlake.applications.function-error.v1")]: true,
    });

    expect(foreignError).toBeInstanceOf(FunctionError);

    class SpecializedFunctionError extends FunctionError {}
    expect(new FunctionError("base function error"))
      .not.toBeInstanceOf(SpecializedFunctionError);
    expect(new SpecializedFunctionError("specialized function error"))
      .toBeInstanceOf(SpecializedFunctionError);
  });

  it("waits for a failure in first_failure mode", async () => {
    clearRegistryForTest();
    const task = registerFunction(async (value: number) => {
      if (value === 2) throw new Error("boom");
      await new Promise((resolve) => setTimeout(resolve, value === 1 ? 5 : 30));
      return value;
    }, {
      name: "task",
      parameters: [schema.parameter("value", schema.number())] as const,
      returns: schema.number(),
    });
    const futures = [task.future(1), task.future(2), task.future(3)];
    const result = await Future.wait(futures, { returnWhen: "first_failure" });
    expect(result.done.some((future) => future.exception != null)).toBe(true);
    await Promise.allSettled(futures.map((future) => future.result()));
    await expect(Future.wait(futures, { returnWhen: "first_completed" })).resolves.toEqual({
      done: futures,
      notDone: [],
    });
  });

  it("returns immediately from first-completion waits when a future is already settled", async () => {
    clearRegistryForTest();
    const task = registerFunction("settled_wait_task", async (value: number) => {
      if (value > 1) await new Promise((resolve) => setTimeout(resolve, 30));
      return value;
    });
    const completed = task.future(1).run();
    await completed.result();
    const pending = task.future(2);

    await expect(Future.wait([completed, pending], {
      returnWhen: "first_completed",
    })).resolves.toEqual({
      done: [completed],
      notDone: [pending],
    });
    await pending.result();
  });

  it("waits for all remaining futures after an already successful future in first_failure mode", async () => {
    clearRegistryForTest();
    const task = registerFunction("settled_first_failure_task", async (value: number) => {
      if (value > 1) await new Promise((resolve) => setTimeout(resolve, 30));
      return value;
    });
    const completed = task.future(1).run();
    await completed.result();
    const pending = task.future(2);

    const waiting = Future.wait([completed, pending], {
      returnWhen: "first_failure",
    });
    await Promise.resolve();
    expect(pending.done).toBe(false);
    await expect(waiting).resolves.toEqual({
      done: [completed, pending],
      notDone: [],
    });
  });

  it("keeps deterministic wait cutoffs when several futures settle in one turn", async () => {
    clearRegistryForTest();
    const completions = new Map<number, {
      resolve(value: number): void;
      reject(error: Error): void;
    }>();
    const task = registerFunction("same_turn_wait_task", async (value: number) =>
      new Promise<number>((resolve, reject) => {
        completions.set(value, { resolve, reject });
      })
    );

    const first = task.future(1).run();
    const second = task.future(2).run();
    const firstCompleted = Future.wait([first, second], {
      returnWhen: "first_completed",
    });
    completions.get(1)?.resolve(1);
    completions.get(2)?.resolve(2);

    await expect(firstCompleted).resolves.toEqual({
      done: [first],
      notDone: [second],
    });
    expect(second.done).toBe(true);

    const successful = task.future(3).run();
    const failing = task.future(4).run();
    const later = task.future(5).run();
    const firstFailure = Future.wait([successful, failing, later], {
      returnWhen: "first_failure",
    });
    completions.get(3)?.resolve(3);
    completions.get(4)?.reject(new Error("expected failure"));
    completions.get(5)?.resolve(5);

    await expect(firstFailure).resolves.toEqual({
      done: [successful, failing],
      notDone: [later],
    });
    expect(later.done).toBe(true);
  });

  it("keeps detached future failures available without an unhandled rejection", async () => {
    clearRegistryForTest();
    const fail = registerFunction("detached_future_failure", async () => {
      throw new Error("detached boom");
    });
    const future = fail.future().run();

    await new Promise((resolve) => setTimeout(resolve, 10));
    await expect(future.result()).rejects.toThrow("detached boom");
    expect(future.done).toBe(true);
    expect(future.exception).toBeInstanceOf(Error);
  });

  it("cancels a local request through its RequestContext signal", async () => {
    clearRegistryForTest();
    let observedSignal: AbortSignal | undefined;
    const app = registerApplication("cancellable_local_request", async () => {
      observedSignal = RequestContext.get().signal;
      await new Promise<never>(() => undefined);
    });
    const request = await runLocal(app);
    const reason = new FunctionError("stop local request");

    request.cancel(reason);

    await expect(request.output()).rejects.toBe(reason);
    expect(observedSignal?.aborted).toBe(true);
    expect(observedSignal?.reason).toBe(reason);
  });
});
