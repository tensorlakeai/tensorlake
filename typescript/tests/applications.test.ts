import { describe, expect, it } from "vitest";
import {
  File,
  FunctionError,
  Future,
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

  it("recognizes File values created by another SDK bundle", () => {
    const foreignFile = {
      [Symbol.for("tensorlake.applications.file.v1")]: true,
      content: new Uint8Array([1, 2, 3]),
      contentType: "text/plain",
    };

    expect(foreignFile).not.toBeInstanceOf(File);
    expect(isFile(foreignFile)).toBe(true);
    expect(validateWithSchema(schema.file(), foreignFile, "foreign file")).toBe(foreignFile);
    expect(serializeValue(foreignFile)).toEqual({
      data: foreignFile.content,
      contentType: "text/plain",
      encoding: "raw",
    });
  });

  it("recognizes RequestError values created by another SDK bundle", () => {
    const foreignError = Object.assign(new Error("stop across bundle boundary"), {
      [Symbol.for("tensorlake.applications.request-error.v1")]: true,
    });

    expect(foreignError).not.toBeInstanceOf(RequestError);
    expect(isRequestError(foreignError)).toBe(true);
  });

  it("supports catching FunctionError values created by another SDK bundle", () => {
    const foreignError = Object.assign(new Error("child failed across bundle boundary"), {
      [Symbol.for("tensorlake.applications.function-error.v1")]: true,
    });

    expect(foreignError).toBeInstanceOf(FunctionError);
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
});
