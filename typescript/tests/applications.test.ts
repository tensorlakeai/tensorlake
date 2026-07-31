import { describe, expect, it } from "vitest";
import {
  File,
  FunctionError,
  Future,
  Headers,
  HttpBody,
  RequestContext,
  RequestError,
  SDKUsageError,
  TimeoutError,
  createApplicationManifest,
  registerApplication,
  registerFunction,
  retries,
  runLocal,
  schema,
} from "../src/applications/index.js";
import { clearRegistryForTest } from "../src/applications/registry.js";
import { isRequestError } from "../src/applications/errors.js";
import { isFile } from "../src/applications/file.js";
import { isHttpBody } from "../src/applications/http-body.js";
import { isHeaders } from "../src/applications/headers.js";
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

  it("snapshots mutable registration options before execution and manifest generation", async () => {
    clearRegistryForTest();
    const functionRetries = { maxRetries: 0 };
    const applicationRetries = { maxRetries: 0 };
    const allow = ["unauthenticated_requests"] as const;
    const mutableAllow = [...allow];
    const gpu = ["A10"];
    const tags = { environment: "original" };
    const secrets = ["ORIGINAL_SECRET"];
    const parameters = [schema.parameter("value", schema.number())];
    let attempts = 0;
    const app = registerApplication(async (_value: number) => {
      attempts += 1;
      throw new Error("do not retry");
    }, {
      name: "stable_registration_options",
      parameters,
      returns: schema.null(),
      retries: functionRetries,
      applicationRetries,
      allow: mutableAllow,
      gpu,
      tags,
      secrets,
    });

    functionRetries.maxRetries = 1;
    applicationRetries.maxRetries = 1;
    mutableAllow.length = 0;
    gpu.push("T4");
    tags.environment = "mutated";
    secrets.push("MUTATED_SECRET");
    parameters.push(schema.parameter("mutated", schema.number()));

    const request = await runLocal(app, 1);
    await expect(request.output()).rejects.toThrow("do not retry");
    expect(attempts).toBe(1);
    expect(app.definition.options.retries).toEqual({ maxRetries: 0 });
    expect(app.definition.application?.retries).toEqual({ maxRetries: 0 });
    expect(app.definition.application?.allow).toEqual([
      "unauthenticated_requests",
    ]);
    expect(app.definition.options.gpu).toEqual(["A10"]);
    expect(app.definition.options.secrets).toEqual(["ORIGINAL_SECRET"]);
    expect(app.definition.parameters.map((parameter) => parameter.name))
      .toEqual(["value"]);
    expect(createApplicationManifest(app.definition)).toMatchObject({
      tags: { environment: "original" },
      allow: ["unauthenticated_requests"],
      functions: {
        stable_registration_options: {
          retry_policy: { max_retries: 0 },
          resources: {
            gpus: [{ count: 1, model: "A10" }],
          },
          secret_names: ["ORIGINAL_SECRET"],
          parameters: [{ name: "value" }],
        },
      },
    });
  });

  it("rejects duplicate explicit parameter names before producing an ambiguous manifest", () => {
    clearRegistryForTest();
    expect(() => registerFunction(async (left: number, right: number) => left + right, {
      name: "duplicate_explicit_parameters",
      parameters: [
        schema.parameter("value", schema.number()),
        schema.parameter("value", schema.number()),
      ] as const,
      returns: schema.number(),
    })).toThrow("Duplicate Tensorlake parameter name 'value'");
  });

  it("rejects malformed GPU and application tag options at registration", () => {
    clearRegistryForTest();
    for (const gpu of ["", "H100:0", "H100:1.5", "H100:2:extra"]) {
      expect(() => registerFunction(
        "invalid_gpu",
        async () => null,
        { gpu: gpu as string },
      )).toThrow(/gpu|GPU/);
    }
    expect(() => registerApplication(
      "invalid_tags",
      async () => null,
      { tags: ["not-a-record"] as never },
    )).toThrow(
      "Application tags require non-empty string keys and string values",
    );
    expect(() => registerApplication(
      "non_plain_tags",
      async () => null,
      { tags: new Date() as unknown as Record<string, string> },
    )).toThrow(SDKUsageError);
    expect(() => registerApplication(
      "invalid_allow_shape",
      async () => null,
      { allow: "unauthenticated_requests" as never },
    )).toThrow(
      "Application allow must contain only 'unauthenticated_requests'",
    );
    expect(() => registerApplication(
      "invalid_allow_capability",
      async () => null,
      { allow: ["unknown_capability"] as never },
    )).toThrow(
      "Application allow must contain only 'unauthenticated_requests'",
    );
    expect(() => registerFunction(
      "invalid_secrets",
      async () => null,
      { secrets: "TOKEN" as unknown as string[] },
    )).toThrow(SDKUsageError);
    expect(() =>
      retries(null as unknown as { maxRetries: number })
    ).toThrow(SDKUsageError);
  });

  it("reports malformed explicit schemas as SDK usage errors", () => {
    clearRegistryForTest();
    expect(() => registerFunction(async () => null, {
      name: "missing_parameters",
      parameters: undefined,
      returns: schema.json(),
    } as never)).toThrow(SDKUsageError);
    expect(() => registerFunction(async () => null, {
      name: "missing_return_schema",
      parameters: [],
      returns: undefined,
    } as never)).toThrow(SDKUsageError);
    expect(() => registerFunction(async () => null, {
      name: "invalid_parameter",
      parameters: [{}],
      returns: schema.json(),
    } as never)).toThrow(SDKUsageError);
  });

  it("snapshots nested schemas and defaults while preserving typed builder invariants", async () => {
    clearRegistryForTest();
    const jsonSchema = {
      type: "object",
      properties: {
        greeting: { type: "string" },
      },
      required: ["greeting"],
      additionalProperties: false,
    };
    const defaultValue = { greeting: "hello" };
    const input = schema.custom<{ greeting: string }>(jsonSchema);
    const parameter = schema.parameter("input", input, { default: defaultValue });
    const app = registerApplication(async (value: { greeting: string }) => {
      const greeting = value.greeting;
      value.greeting = "changed by handler";
      return greeting;
    }, {
      name: "stable_nested_schema",
      parameters: [parameter],
      returns: schema.string(),
    });

    jsonSchema.properties.greeting.type = "number";
    jsonSchema.required.push("mutated");
    defaultValue.greeting = "mutated";

    await expect(app()).resolves.toBe("hello");
    await expect(app()).resolves.toBe("hello");
    const exposedDefault = parameter.defaultValue;
    exposedDefault!.greeting = "changed through definition";
    expect(parameter.defaultValue).toEqual({ greeting: "hello" });
    expect(() => validateWithSchema(input, { greeting: 1 }, "input"))
      .toThrow("must be string");
    expect(createApplicationManifest(app.definition).functions.stable_nested_schema)
      .toMatchObject({
        parameters: [{
          data_type: {
            type: "object",
            properties: { greeting: { type: "string" } },
            required: ["greeting"],
            default: { greeting: "hello" },
          },
        }],
      });

    const forcedString = schema.string({ type: "number" } as never);
    const forcedArray = schema.array(schema.string(), {
      type: "object",
      items: schema.number().jsonSchema,
    } as never);
    const fileBytes = Buffer.from([1, 2, 3]);
    const fileParameter = schema.parameter(
      "file",
      schema.file(),
      { default: new File(fileBytes, "application/octet-stream") },
    );
    fileBytes[0] = 9;
    const exposedFileDefault = fileParameter.defaultValue;
    exposedFileDefault!.content[1] = 9;
    expect(forcedString.jsonSchema.type).toBe("string");
    expect(forcedArray.jsonSchema).toMatchObject({
      type: "array",
      items: { type: "string" },
    });
    expect(fileParameter.defaultValue?.content).toEqual(new Uint8Array([1, 2, 3]));
  });

  it("rejects sparse arrays instead of changing their values at a JSON boundary", () => {
    const sparse = new Array<number>(1);

    expect(() => serializeValue(sparse)).toThrow("sparse");
    expect(() => schema.parameter(
      "items",
      schema.json<number[]>(),
      { default: sparse },
    )).toThrow("sparse");
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

  it("parses comments between async and parenthesized arrow parameters", async () => {
    clearRegistryForTest();
    const commentedArrow = (0, eval)(
      "(async /* comment between async and parameters */ (left, right) => left + right)",
    ) as (left: number, right: number) => Promise<number>;
    const add = registerFunction("commented_async_arrow", commentedArrow);

    expect(add.definition.parameters).toHaveLength(2);
    expect(add.definition.parameters.map((parameter) => parameter.required))
      .toEqual([true, true]);
    await expect(add(20, 22)).resolves.toBe(42);
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
    await expect(context.state.get(42 as unknown as string))
      .rejects.toThrow("State key must be a string");
    await expect(context.state.set(42 as unknown as string, "value"))
      .rejects.toThrow("State key must be a string");
  });

  it("validates local metric names and values", async () => {
    const context = new MemoryRequestContext("local-metrics");

    await expect(context.metrics.counter(42 as unknown as string))
      .rejects.toThrow("Counter name must be a string");
    await expect(context.metrics.counter("counter", 1.5))
      .rejects.toThrow("Counter value must be an int");
    await expect(context.metrics.counter("counter", true as unknown as number))
      .rejects.toThrow("Counter value must be an int");
    await expect(context.metrics.counter("counter", Number.MAX_SAFE_INTEGER + 1))
      .rejects.toThrow("Counter value must be a safe int");
    await expect(context.metrics.timer(42 as unknown as string, 1))
      .rejects.toThrow("Timer name must be a string");
    await expect(context.metrics.timer("timer", "invalid" as unknown as number))
      .rejects.toThrow("Timer value must be a finite number");
    await expect(context.metrics.timer("timer", true as unknown as number))
      .rejects.toThrow("Timer value must be a finite number");
    await expect(context.metrics.timer("timer", Number.POSITIVE_INFINITY))
      .rejects.toThrow("Timer value must be a finite number");
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
    expect(manifest.allow).toEqual([]);
    expect(manifest.entrypoint.input_serializer).toBe("json");
    expect(manifest.functions.echo.parameters[0].data_type.type).toBe("string");
    expect(manifest.functions.echo.placement_constraints.filter_expressions).toEqual(["region==eu-west-1"]);
  });

  it("supports direct File values but rejects nested files", () => {
    expect(serializeValue(new File(new Uint8Array([1, 2]), "application/octet-stream")).encoding).toBe("raw");
    expect(() => serializeValue({ file: new File(new Uint8Array([1]), "application/octet-stream") }))
      .toThrow("nested File");
  });

  it("exposes raw HTTP body accessors and serializes exact bytes", () => {
    const content = new TextEncoder().encode(' { "event": "created" }\n');
    const body = new HttpBody(
      content,
      "application/cloudevents+json; charset=utf-8",
    );

    expect(body.content).toBe(content);
    expect(body.contentType).toBe(
      "application/cloudevents+json; charset=utf-8",
    );
    expect(body.text()).toBe(' { "event": "created" }\n');
    expect(body.json<{ event: string }>()).toEqual({ event: "created" });
    expect(serializeValue(body)).toEqual({
      data: content,
      contentType: "application/cloudevents+json; charset=utf-8",
      encoding: "raw",
    });
    expect(new HttpBody(new Uint8Array()).contentType).toBeUndefined();
  });

  it("exposes immutable case-insensitive request headers with duplicate values", () => {
    const headers = new Headers([
      ["X-Tensorlake-Test", "first"],
      ["x-tensorlake-test", "second"],
    ]);

    expect(headers.get("X-TENSORLAKE-TEST")).toBe("second");
    expect(headers.getAll("x-tensorlake-test")).toEqual(["first", "second"]);
    expect(headers.has("X-Tensorlake-Test")).toBe(true);
    expect([...headers]).toEqual([
      ["X-Tensorlake-Test", "first"],
      ["x-tensorlake-test", "second"],
    ]);
    expect(Object.isFrozen(headers.getAll("x-tensorlake-test"))).toBe(true);
    expect(new MemoryRequestContext("local").headers.getAll("missing")).toEqual([]);
  });

  it("supports Headers identity across SDK bundles", () => {
    const foreignHeaders = {
      [Symbol.for("tensorlake.applications.headers.v1")]: true,
      get: () => "second",
      getAll: () => ["first", "second"],
      has: () => true,
      *[Symbol.iterator]() {
        yield ["X-Test", "first"] as const;
        yield ["x-test", "second"] as const;
      },
    };

    expect(foreignHeaders).toBeInstanceOf(Headers);
    expect(isHeaders(foreignHeaders)).toBe(true);
    expect(new Headers(foreignHeaders).getAll("x-test")).toEqual([
      "first",
      "second",
    ]);
  });

  it("marks HttpBody application inputs in manifests", () => {
    clearRegistryForTest();
    const application = registerApplication(async (body: HttpBody) => ({
      contentType: body.contentType,
      size: body.content.byteLength,
    }), {
      name: "raw_http_body",
      parameters: [
        schema.parameter("body", schema.httpBody()),
      ] as const,
      returns: schema.object({
        contentType: schema.string(),
        size: schema.integer(),
      }),
    });

    expect(
      createApplicationManifest(application.definition)
        .functions.raw_http_body.parameters[0].data_type,
    ).toMatchObject({
      title: "body",
      type: "tensorlake_http_body",
    });
  });

  it("copies HttpBody bytes across local execution boundaries", async () => {
    clearRegistryForTest();
    const application = registerApplication(async (body: HttpBody) => {
      const firstByte = body.content[0];
      body.content[0] = 9;
      return firstByte;
    }, {
      name: "http_body_boundary",
      parameters: [
        schema.parameter("body", schema.httpBody()),
      ] as const,
      returns: schema.integer(),
    });
    const content = Buffer.from([1, 2, 3]);

    await expect(runLocal(
      application,
      new HttpBody(content, "application/octet-stream"),
    ).then((request) => request.output())).resolves.toBe(1);
    expect(content).toEqual(Buffer.from([1, 2, 3]));
  });

  it("limits HttpBody schemas to direct application parameters", () => {
    expect(() => schema.array(schema.httpBody())).toThrow(
      "HttpBody is only supported as a direct application parameter",
    );
    expect(() => schema.object({ body: schema.httpBody() })).toThrow(
      "HttpBody is only supported as a direct application parameter",
    );
    expect(() => registerFunction(async (_body: HttpBody) => null, {
      name: "function_http_body_input",
      parameters: [
        schema.parameter("body", schema.httpBody()),
      ] as const,
      returns: schema.null(),
    })).toThrow("HttpBody is only supported for application parameters");
    expect(() => registerApplication(async () =>
      new HttpBody(new Uint8Array()), {
      name: "application_http_body_output",
      parameters: [] as const,
      returns: schema.httpBody(),
    })).toThrow("HttpBody is only supported for application parameters");
    expect(() => serializeValue({
      body: new HttpBody(new Uint8Array([1]), "application/octet-stream"),
    })).toThrow("nested HttpBody");
  });

  it("copies Buffer-backed Files across local execution boundaries", async () => {
    clearRegistryForTest();
    const application = registerApplication(async (file: File) => {
      const firstByte = file.content[0];
      file.content[0] = 9;
      return firstByte;
    }, {
      name: "buffer_file_boundary",
      parameters: [schema.parameter("file", schema.file())] as const,
      returns: schema.number(),
    });
    const bytes = Buffer.from([1, 2, 3]);

    await expect(runLocal(
      application,
      new File(bytes, "application/octet-stream"),
    ).then((request) => request.output())).resolves.toBe(1);
    expect(bytes).toEqual(Buffer.from([1, 2, 3]));
  });

  it("requires schema.file for File values instead of accepting them as JSON", async () => {
    clearRegistryForTest();
    const file = new File(new Uint8Array([1, 2, 3]), "application/octet-stream");
    const echo = registerFunction("schema_free_file_echo", async (value: unknown) => value);
    const application = registerApplication(
      "schema_free_file_output",
      async () => file,
    );

    await expect(echo(file)).rejects.toThrow("must use schema.file()");
    const request = await runLocal(application);
    await expect(request.output()).rejects.toThrow("must use schema.file()");
  });

  it("rejects File schemas inside JSON schema combinators", () => {
    expect(() => schema.array(schema.file())).toThrow("File is only supported as a direct");
    expect(() => schema.tuple([schema.string(), schema.file()])).toThrow(
      "File is only supported as a direct",
    );
    expect(() => schema.record(schema.file())).toThrow("File is only supported as a direct");
    expect(() => schema.object({ document: schema.file() })).toThrow(
      "File is only supported as a direct",
    );
    expect(() => schema.union(schema.file(), schema.null())).toThrow(
      "File is only supported as a direct",
    );
    expect(() => schema.nullable(schema.file())).toThrow("File is only supported as a direct");
  });

  it("validates every schema.tuple position using its emitted prefixItems", async () => {
    clearRegistryForTest();
    const pair = schema.tuple([schema.string(), schema.integer()]);
    const echo = registerFunction(async (value: [string, number]) => value, {
      name: "tuple_schema_echo",
      parameters: [schema.parameter("value", pair)] as const,
      returns: pair,
    });

    await expect(echo(
      [1, "wrong"] as unknown as [string, number],
    )).rejects.toThrow("tuple_schema_echo.value does not match its schema");
    await expect(echo(["correct", 2])).resolves.toEqual(["correct", 2]);
  });

  it("preserves explicitly declared draft-07 custom schema validation", () => {
    const legacy = schema.custom<string>({
      $schema: "http://json-schema.org/draft-07/schema#",
      type: "string",
      minLength: 2,
    });

    expect(validateWithSchema(legacy, "ok", "legacy input")).toBe("ok");
    expect(() => validateWithSchema(legacy, "x", "legacy input"))
      .toThrow("must NOT have fewer than 2 characters");
  });

  it("reports invalid custom JSON schemas as SDK usage errors", () => {
    const invalid = schema.custom<unknown>({ type: "not-a-json-schema-type" });

    expect(() => validateWithSchema(invalid, "value", "custom input"))
      .toThrow(SDKUsageError);
    expect(() => validateWithSchema(invalid, "value", "custom input"))
      .toThrow("custom input uses an invalid JSON schema");
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

  it("supports HttpBody instanceof checks across SDK bundles", () => {
    const foreignBody = {
      [Symbol.for("tensorlake.applications.http-body.v1")]: true,
      content: new Uint8Array([1, 2, 3]),
      contentType: "application/json",
    };

    expect(foreignBody).toBeInstanceOf(HttpBody);
    expect(isHttpBody(foreignBody)).toBe(true);
    expect(validateWithSchema(
      schema.httpBody(),
      foreignBody,
      "foreign body",
    )).toBe(foreignBody);
    expect(serializeValue(foreignBody)).toEqual({
      data: foreignBody.content,
      contentType: "application/json",
      encoding: "raw",
    });

    class SpecializedHttpBody extends HttpBody {}
    expect(new HttpBody(new Uint8Array([1])))
      .not.toBeInstanceOf(SpecializedHttpBody);
    expect(new SpecializedHttpBody(new Uint8Array([1])))
      .toBeInstanceOf(SpecializedHttpBody);
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

  it("supports TimeoutError instanceof checks across SDK bundles", () => {
    const foreignError = Object.assign(new Error("Timed out."), {
      [Symbol.for("tensorlake.applications.timeout-error.v1")]: true,
    });

    expect(foreignError).toBeInstanceOf(TimeoutError);

    class SpecializedTimeoutError extends TimeoutError {}
    expect(new TimeoutError()).not.toBeInstanceOf(SpecializedTimeoutError);
    expect(new SpecializedTimeoutError()).toBeInstanceOf(SpecializedTimeoutError);
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

  it("supports catching SDKUsageError values created by another SDK bundle", () => {
    const foreignError = Object.assign(new Error("invalid usage across bundle boundary"), {
      [Symbol.for("tensorlake.applications.sdk-usage-error.v1")]: true,
    });

    expect(foreignError).toBeInstanceOf(SDKUsageError);

    class SpecializedSDKUsageError extends SDKUsageError {}
    expect(new SDKUsageError("base usage error"))
      .not.toBeInstanceOf(SpecializedSDKUsageError);
    expect(new SpecializedSDKUsageError("specialized usage error"))
      .toBeInstanceOf(SpecializedSDKUsageError);
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

  it("recognizes nullish promise rejections as already completed failures", async () => {
    clearRegistryForTest();
    const task = registerFunction(
      "nullish_future_failure",
      async (mode: "undefined" | "null" | "pending") => {
        if (mode === "undefined") throw undefined;
        if (mode === "null") throw null;
        return new Promise<never>(() => undefined);
      },
    );
    const undefinedFailure = task.future("undefined").run();
    const nullFailure = task.future("null").run();
    await Promise.allSettled([undefinedFailure.result(), nullFailure.result()]);
    const pending = task.future("pending");
    let timer: NodeJS.Timeout | undefined;

    try {
      const result = await Promise.race([
        Future.wait([undefinedFailure, nullFailure, pending], {
          returnWhen: "first_failure",
        }),
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(
            () => reject(new Error("FIRST_FAILURE did not observe nullish rejections")),
            100,
          );
        }),
      ]);
      expect(result).toEqual({
        done: [undefinedFailure, nullFailure],
        notDone: [pending],
      });
    } finally {
      if (timer != null) clearTimeout(timer);
    }
  });

  it("does not mutate an already-running future when runLater is rejected", async () => {
    clearRegistryForTest();
    const task = registerFunction("already_running_future", async () => 42);
    const future = task.future().run();

    expect(() => future.runLater(10)).toThrow("already running");
    expect(future.delaySeconds).toBe(0);
    await expect(future.result()).resolves.toBe(42);
  });

  it("rejects invalid runLater delays without starting the future", () => {
    clearRegistryForTest();
    const task = registerFunction("invalid_future_delay", async () => 42);

    for (const delay of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
      const future = task.future();
      expect(() => future.runLater(delay)).toThrow(
        "Future delay must be a non-negative finite number",
      );
      expect(future.done).toBe(false);
      expect(future.delaySeconds).toBe(0);
    }
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
