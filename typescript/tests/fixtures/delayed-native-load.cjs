const { isMainThread, getEnvironmentData } = require("node:worker_threads");

// Inherited only by the SDK worker in the native regression. Hold addon loading
// until the main thread proves it can advance while this worker is blocked.
// The timeout catches a blocked main thread without relying on elapsed tick counts.
if (!isMainThread) {
  const dlopen = process.dlopen;
  process.dlopen = function (...args) {
    const state = new Int32Array(getEnvironmentData("tensorlake-test-native-loads"));
    Atomics.add(state, 0, 1); // Addon loads.
    Atomics.store(state, 1, 1); // Loading is now blocked.
    if (Atomics.wait(state, 2, 0, 5_000) === "timed-out") {
      throw new Error("Main event loop did not release the blocked addon load within 5s");
    }
    return Reflect.apply(dlopen, this, args);
  };
  process.report.getReport = () => { throw new Error("Worker must not generate diagnostic reports"); };
}
