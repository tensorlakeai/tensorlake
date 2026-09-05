const { isMainThread, getEnvironmentData } = require("node:worker_threads");

// Inherited only by the SDK worker in the native regression. Make addon
// loading exceed the production watchdog threshold, without delaying startup
// of the test driver or depending on the host's disk cache.
if (!isMainThread) {
  const dlopen = process.dlopen;
  process.dlopen = function (...args) {
    const state = getEnvironmentData("tensorlake-test-native-loads");
    if (state) Atomics.add(new Int32Array(state), 0, 1);
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 750);
    return Reflect.apply(dlopen, this, args);
  };
  process.report.getReport = () => { throw new Error("Worker must not generate diagnostic reports"); };
}
