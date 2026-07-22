import assert from "node:assert/strict";

const apiUrl = (process.env.TENSORLAKE_API_URL ?? "https://api.tensorlake.ai").replace(/\/$/, "");
const namespace = process.env.INDEXIFY_NAMESPACE ?? "default";
const credential = process.env.TENSORLAKE_API_KEY ?? process.env.TENSORLAKE_PAT;
const timeoutMs = Number(process.env.TENSORLAKE_VERIFY_TIMEOUT_SEC ?? "180") * 1000;
const cleanup = ["1", "true", "yes"].includes(
  (process.env.TENSORLAKE_VERIFY_CLEANUP ?? "").toLowerCase(),
);
const applications = [
  "typescript_runtime_verification",
  "typescript_tail_call_verification",
  "typescript_file_verification",
  "typescript_request_error_verification",
  "typescript_nested_verification",
  "typescript_failure_verification",
  "typescript_retry_exhaustion_verification",
  "typescript_reduce_verification",
];

if (!credential) throw new Error("Set TENSORLAKE_API_KEY or TENSORLAKE_PAT before verifying");

const commonHeaders = { Authorization: `Bearer ${credential}` };
if (process.env.TENSORLAKE_ORGANIZATION_ID) {
  commonHeaders["X-Forwarded-Organization-Id"] = process.env.TENSORLAKE_ORGANIZATION_ID;
}
if (process.env.TENSORLAKE_PROJECT_ID) {
  commonHeaders["X-Forwarded-Project-Id"] = process.env.TENSORLAKE_PROJECT_ID;
}

function applicationURL(application, suffix = "") {
  return `${apiUrl}/v1/namespaces/${encodeURIComponent(namespace)}/applications/` +
    `${encodeURIComponent(application)}${suffix}`;
}

async function checkedFetch(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: { ...commonHeaders, ...(options.headers ?? {}) },
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`${options.method ?? "GET"} ${url} returned ${response.status}: ${body}`);
  }
  return response;
}

async function invoke(application, body, contentType, headers = {}) {
  const response = await checkedFetch(applicationURL(application), {
    method: "POST",
    headers: { Accept: "application/json", "Content-Type": contentType, ...headers },
    body,
  });
  const payload = await response.json();
  assert.equal(typeof payload.request_id, "string", `No request_id returned for ${application}`);
  process.stdout.write(`${application}: request ${payload.request_id}\n`);
  return payload.request_id;
}

async function waitForRequest(application, requestId) {
  const deadline = Date.now() + timeoutMs;
  let lastMetadata;
  while (Date.now() < deadline) {
    const response = await checkedFetch(applicationURL(
      application,
      `/requests/${encodeURIComponent(requestId)}`,
    ));
    const metadata = await response.json();
    lastMetadata = metadata;
    if (metadata.outcome != null) return metadata;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(
    `${application} request ${requestId} did not finish within ${timeoutMs / 1000}s; ` +
    `last metadata: ${JSON.stringify(lastMetadata)}`,
  );
}

async function output(application, requestId) {
  return checkedFetch(applicationURL(
    application,
    `/requests/${encodeURIComponent(requestId)}/output`,
  ));
}

async function verifyRuntime() {
  const input = { label: "server-check", values: [1, 2, 5] };
  const requestId = await invoke(
    "typescript_runtime_verification",
    JSON.stringify(input),
    "application/json",
  );
  const metadata = await waitForRequest("typescript_runtime_verification", requestId);
  assert.equal(metadata.outcome, "success", JSON.stringify(metadata));
  const value = await (await output("typescript_runtime_verification", requestId)).json();
  assert.equal(value.requestId, requestId);
  assert.equal(value.rememberedLabel, input.label);
  assert.deepEqual(value.doubled, [2, 4, 10]);
  assert.equal(value.delayed, 2);
  assert.equal(value.retried, input.label.length);

  const names = new Set((metadata.function_runs ?? []).map((run) => run.name));
  assert(names.has("typescript_verification_double"), "durable double calls were not recorded");
  assert(names.has("typescript_verification_retry_once"), "retry function was not recorded");
  const retryRun = (metadata.function_runs ?? []).find(
    (run) => run.name === "typescript_verification_retry_once",
  );
  assert((retryRun?.allocations?.length ?? 0) >= 2, "retry function did not receive a second allocation");
}

async function verifyTailCall() {
  const requestId = await invoke(
    "typescript_tail_call_verification",
    "41",
    "application/json",
  );
  const metadata = await waitForRequest("typescript_tail_call_verification", requestId);
  assert.equal(metadata.outcome, "success", JSON.stringify(metadata));
  assert.equal(await (await output("typescript_tail_call_verification", requestId)).json(), 42);
  const names = new Set((metadata.function_runs ?? []).map((run) => run.name));
  assert(names.has("typescript_verification_increment"), "tail-call target was not recorded");
}

async function verifyFile() {
  const requestId = await invoke(
    "typescript_file_verification",
    new TextEncoder().encode("typescript file boundary"),
    "text/plain",
  );
  const metadata = await waitForRequest("typescript_file_verification", requestId);
  assert.equal(metadata.outcome, "success", JSON.stringify(metadata));
  const response = await output("typescript_file_verification", requestId);
  assert.match(response.headers.get("content-type") ?? "", /^text\/plain/);
  assert.equal(await response.text(), "TYPESCRIPT FILE BOUNDARY");
}

async function verifyRequestError() {
  const message = "intentional TypeScript request error";
  const requestId = await invoke(
    "typescript_request_error_verification",
    JSON.stringify(message),
    "application/json",
  );
  const metadata = await waitForRequest("typescript_request_error_verification", requestId);
  assert.equal(metadata.failure_reason, "request_error", JSON.stringify(metadata));
  assert.equal(metadata.request_error?.message, message, JSON.stringify(metadata));
  assert.equal(metadata.request_error?.function_name, "typescript_request_error_verification");
}

async function verifyNestedCalls() {
  const requestId = await invoke(
    "typescript_nested_verification",
    "20",
    "application/json",
  );
  const metadata = await waitForRequest("typescript_nested_verification", requestId);
  assert.equal(metadata.outcome, "success", JSON.stringify(metadata));
  assert.equal(await (await output("typescript_nested_verification", requestId)).json(), 42);
  const names = new Set((metadata.function_runs ?? []).map((run) => run.name));
  assert(names.has("typescript_verification_nested_middle"), "nested middle function was not recorded");
  assert(names.has("typescript_verification_nested_leaf"), "nested leaf function was not recorded");
}

async function verifyTerminalFailures() {
  const requestId = await invoke(
    "typescript_failure_verification",
    JSON.stringify([1, 2, 3, 4]),
    "application/json",
  );
  const metadata = await waitForRequest("typescript_failure_verification", requestId);
  assert.equal(metadata.failure_reason, "function_error", JSON.stringify(metadata));
  const runs = metadata.function_runs ?? [];
  const mixedRuns = runs.filter((run) => run.name === "typescript_verification_mixed_failure");
  assert.equal(mixedRuns.length, 4, `expected four mixed fan-out runs: ${JSON.stringify(metadata)}`);
  assert(
    mixedRuns.some((run) => run.failure_reason === "function_error"),
    `expected a mapped child function error: ${JSON.stringify(metadata)}`,
  );
  assert(
    mixedRuns.every((run) => run.status === "completed"),
    `all mapped runs should be terminal: ${JSON.stringify(metadata)}`,
  );
}

async function verifyRetryExhaustion() {
  const requestId = await invoke(
    "typescript_retry_exhaustion_verification",
    "null",
    "application/json",
  );
  const metadata = await waitForRequest("typescript_retry_exhaustion_verification", requestId);
  assert.equal(metadata.failure_reason, "function_error", JSON.stringify(metadata));
  const runs = metadata.function_runs ?? [];
  const exhaustedRun = runs.find((run) => run.name === "typescript_verification_always_fails");
  assert(exhaustedRun != null, "exhausted-retry function was not recorded");
  assert.equal(exhaustedRun.outcome, "failure");
  assert(
    (exhaustedRun.allocations?.length ?? 0) >= 3,
    `expected the initial allocation and two retries: ${JSON.stringify(exhaustedRun)}`,
  );
}

async function verifyReduce() {
  const application = "typescript_reduce_verification";
  const input = { initial: 10, values: [1, 2, 3] };
  const requestId = await invoke(application, JSON.stringify(input), "application/json");
  const metadata = await waitForRequest(application, requestId);
  assert.equal(metadata.outcome, "success", JSON.stringify(metadata));
  assert.deepEqual(await (await output(application, requestId)).json(), { total: 16, empty: 10 });
  const reducerRuns = (metadata.function_runs ?? []).filter(
    (run) => run.name === "typescript_verification_reduce_sum",
  );
  assert.equal(reducerRuns.length, 3, `expected one durable call per reduce item: ${JSON.stringify(metadata)}`);
  assert(reducerRuns.every((run) => run.outcome === "success"));

  const failureRequestId = await invoke(
    application,
    JSON.stringify({ initial: 10, values: [1, -1, 3] }),
    "application/json",
  );
  const failureMetadata = await waitForRequest(application, failureRequestId);
  assert.equal(failureMetadata.failure_reason, "function_error", JSON.stringify(failureMetadata));
  const failedReducerRuns = (failureMetadata.function_runs ?? []).filter(
    (run) => run.name === "typescript_verification_reduce_sum",
  );
  assert(
    failedReducerRuns.length >= 2 && failedReducerRuns.length <= 3,
    `reduce must run through the failing item and cancel any dependent step: ${JSON.stringify(failureMetadata)}`,
  );
  assert.equal(failedReducerRuns.filter((run) => run.outcome === "success").length, 1);
  assert.equal(
    failedReducerRuns.filter((run) => run.failure_reason === "function_error").length,
    1,
  );
  assert.equal(
    failedReducerRuns.filter((run) =>
      run.outcome === "success"
      || run.failure_reason === "function_error"
      || run.failure_reason === "function_run_cancelled"
    ).length,
    failedReducerRuns.length,
    `dependent reduce steps must be cancelled after failure: ${JSON.stringify(failureMetadata)}`,
  );
}

try {
  await verifyRuntime();
  await verifyNestedCalls();
  await verifyTerminalFailures();
  await verifyRetryExhaustion();
  await verifyReduce();
  await verifyTailCall();
  await verifyFile();
  await verifyRequestError();
  process.stdout.write("\nAll TypeScript server verification checks passed.\n");
} finally {
  if (cleanup) {
    await Promise.all(applications.map((application) =>
      checkedFetch(applicationURL(application), { method: "DELETE" }).catch((error) => {
        process.stderr.write(`Failed to delete ${application}: ${error.message}\n`);
      }),
    ));
  }
}
