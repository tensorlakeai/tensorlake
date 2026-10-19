# Sandbox metadata read resilience

Sandbox lookups, readiness polling, sandbox/snapshot/pool listings and lookups,
process listings/status, directory listings, and daemon info/health use the same
Rust read transport in Python, TypeScript, Rust and the CLI. When one load
balancer address accepts a connection but stops answering, a read can recover
through another address without the application issuing another SDK call.

## Request contract

The defaults are three attempts, at most three seconds per attempt and ten
seconds for the entire read. A shorter caller-supplied request timeout caps the
entire sequence, including resolution, connection establishment (at most two
seconds), response body reads and retry waits. Readiness polling retains its
existing retry/deadline contract and uses address selection for each individual
poll, without nesting another retry loop.
These are metadata-read limits: increasing the ordinary request timeout does not
increase the default ten-second read budget.

Only explicitly designated, bodyless GETs use `Client::execute_read_json`.
Timeouts, connection resets/broken pipes, incomplete response bodies, and HTTP
429/502/503/504 can be retried. A recognized proxy error that confirms the
request was not forwarded is also retryable, including legacy HTTP 500
responses with `SANDBOX_UPSTREAM_ERROR`. Other HTTP 500 responses remain
terminal. Authentication, authorization, not-found,
certificate verification and JSON/schema errors stop immediately. The full
response is received before returning any data. `Retry-After` accepts both
seconds and HTTP dates; a delay beyond the remaining budget ends the operation
instead of retrying early. Other retry waits use randomized exponential backoff.

The policy does not change sandbox creation, process start/restart, writes,
files, output streams or long polls. Those retain their existing request and
retry contracts. This change does not introduce server-side deduplication.

Rust callers can override the read limits with typed configuration:

```rust
use std::time::Duration;
use tensorlake::client::{ClientBuilder, ReadRetryConfig};

let client = ClientBuilder::new("https://sandbox.tensorlake.ai")
    .read_retry(ReadRetryConfig {
        max_attempts: 3,
        attempt_timeout: Duration::from_secs(3),
        total_timeout: Duration::from_secs(10),
    })
    .build()?;
```

## Address and connection handling

The resolver uses the machine's configured DNS servers and hosts file, with a
64-entry TTL-respecting cache per address family. A and AAAA queries run
independently: once one family answers, the other has at most 50 milliseconds
to join it. DNS gets at most half the remaining attempt budget, capped at one
second; half of that DNS budget is reserved for platform resolution if the
configured resolver fails or stalls. Available answers are retained when the
other family reaches that deadline.

Each attempt selects from the current records and prefers an untried address.
A connection failure before HTTP delivery can try one address from the other
family within the same attempt and deadline, without spending an HTTP retry.
If that family's DNS answer missed the initial grace period, connection failure
allows a bounded lookup for that family before giving up on the attempt.
TLS policy failures are terminal. Once an HTTP request is sent, any replay
consumes another attempt. Pools are scoped to the URL origin and selected
address, so an HTTP/2 retry cannot silently reuse the failed peer's multiplexed connection.
The URL hostname, TLS SNI, certificate verification, credentials, scope headers,
redirect policy and middleware are preserved.

Transport failure retires that pool from future use and deprioritizes the
address for ten seconds. Existing in-flight requests retain their own handles
and deadlines. If every address is suspect, the client still makes a bounded
attempt; it never disables every endpoint. Successful addresses continue to
reuse their pools. Each shared client retains at most 32 pools and removes
pools unused for 60 seconds.

System HTTP proxy settings remain in force. A forwarding proxy controls its
upstream DNS and connections, so selecting an origin address cannot guarantee
failover beyond that proxy. Redirects similarly retain their existing routing
semantics. In diagnostics, `selected_address` describes the requested direct
endpoint, not a verified peer beyond a proxy or redirect.

A budget shared by cloned clients allows a burst of 20 retries, replenished by
one token per successful read and one token per second. Initial requests do not
consume retry tokens. Cancellation drops the operation without launching
background retries.

`SdkError::ReadRetryExhausted` marks a completed retry sequence, preventing the
existing language-binding retry loops from multiplying attempts. The bindings
unwrap it before translating errors, preserving their error categories and HTTP
statuses. Rust consumers can use `into_original()` to inspect the final error.
Debug tracing records a read ID, individual trace IDs, attempt number, selected
address, failure stage and elapsed time. It does not log credentials or request
URLs/bodies.

## Validation

Run `just test-sdk-resilience` for the full shared SDK Rust suite. Its local
TLS/HTTP fault servers cover HTTP/1.1 and HTTP/2 stalls and truncated responses,
established HTTP/2 connection loss, deadline/cancellation behavior, DNS TTL
expiry, stalled A/AAAA queries, connection-family fallback, pool reuse,
middleware/auth propagation, retry budget exhaustion, undelivered proxy errors,
mutation timeout safety and nested retry limits. No cloud credentials are
required. The HTTP transport CI workflow runs this recipe as well.

Run `just bench-sdk-resilience` for a local comparison of healthy pooled HTTP/2
reads and resolution overhead. Results and limitations are recorded in
[`crates/cloud-sdk/BENCHMARK.md`](../crates/cloud-sdk/BENCHMARK.md).
