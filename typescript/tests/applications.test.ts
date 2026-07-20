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
    const double = registerFunction(async (value: number) => value * 2, {
      name: "double",
      parameters: [schema.parameter("value", schema.number())] as const,
      returns: schema.number(),
    });
    const app = registerApplication(async (values: number[]) => double.map(values), {
      name: "double_all",
      parameters: [schema.parameter("values", schema.array(schema.number()))] as const,
      returns: schema.array(schema.number()),
    });

    const request = await runLocal(app, [1, 2, 3]);
    expect(await request.output()).toEqual([2, 4, 6]);
  });

  it("reduces sequentially from an explicit initial value and handles empty input", async () => {
    clearRegistryForTest();
    const subtract = registerFunction(async (accumulator: number, value: number) => accumulator - value, {
      name: "subtract",
      parameters: [
        schema.parameter("accumulator", schema.number()),
        schema.parameter("value", schema.number()),
      ] as const,
      returns: schema.number(),
    });
    const app = registerApplication(async (values: number[]) => ({
      populated: await subtract.reduce(values, 10),
      empty: await subtract.reduce([], 10),
    }), {
      name: "subtract_all",
      parameters: [schema.parameter("values", schema.array(schema.number()))] as const,
      returns: schema.object({ populated: schema.number(), empty: schema.number() }),
    });

    const request = await runLocal(app, [1, 2, 3]);
    expect(await request.output()).toEqual({ populated: 4, empty: 10 });
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
