import { describe, expect, it, vi } from "vitest";
import type { RequestContextValue } from "../src/applications/context.js";
import type { FunctionRuntime } from "../src/applications/function.js";

describe.sequential("application runtime context", () => {
  it("shares request and function context across separate SDK module instances", async () => {
    const executorContext = await import("../src/applications/context.js");
    const executorFunction = await import("../src/applications/function.js");

    // A deployed runtime.mjs and the executor capsule each bundle an independent
    // SDK module graph. Resetting Vitest's module registry models those two
    // physical module instances in one Node process.
    vi.resetModules();
    const applicationContext = await import("../src/applications/context.js");
    const applicationFunction = await import("../src/applications/function.js");

    const context: RequestContextValue = {
      requestId: "request-from-executor",
      headers: Object.freeze({ verification: "shared" }),
      signal: new AbortController().signal,
      state: { get: async () => undefined, set: async () => undefined },
      metrics: { counter: async () => undefined, timer: async () => undefined },
      progress: { update: async () => undefined },
    };
    const runtime = {
      invoke: async () => "invoked",
      runFuture: async () => "future",
    } as FunctionRuntime;

    await executorContext.runWithRequestContext(context, async () => {
      expect(applicationContext.RequestContext.get()).toBe(context);
    });
    await executorFunction.runWithFunctionRuntime(runtime, async () => {
      expect(applicationFunction.currentFunctionRuntime()).toBe(runtime);
    });
  });
});
