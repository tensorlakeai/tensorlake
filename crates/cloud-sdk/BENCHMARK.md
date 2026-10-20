# Shared SDK performance evidence

## Sandbox metadata read failover — 2026-10-19

- Measured implementation: `fbecd2c3b303c168c27c150c84d974ddd7f1e0e9`.
- Branch base: `f0cc24601ff8e1bda4ce26332a68b50d7e3d694b`.
- Machine: Mac17,6, Apple M5 Max, 18 logical CPUs, 48 GiB RAM;
  Darwin 25.5.0 arm64.
- Toolchain: Rust 1.98.1 (`48a229cea`, 2026-09-01), unoptimized test profile
  with debug information.
- Command: `just bench-sdk-resilience`.

The harness compares the unchanged `execute_json` path with
`execute_read_json` in the same process against a local TLS/HTTP/2 server.
Each path warms its own connection, then runs 1,000 sequential reads per
round. Five rounds alternate the baseline and resilient paths. Both paths use
the same JSON response, certificate and fixed test address. DNS is excluded
from these request measurements and measured separately below.

| Round | Baseline, 1,000 reads (ms) | Resilient, 1,000 reads (ms) |
| --- | ---: | ---: |
| 0 | 142.642 | 148.777 |
| 1 | 131.957 | 145.863 |
| 2 | 130.555 | 146.350 |
| 3 | 129.094 | 146.170 |
| 4 | 128.905 | 146.154 |
| Mean | 132.631 | 146.663 |

Mean latency is 132.631 microseconds/read for the baseline and 146.663
microseconds/read for the resilient path, an additional 14.032 microseconds
(10.6%) in this synthetic loopback workload, excluding DNS. Corresponding
sequential throughput is about 7,540 and 6,818 reads/second.

Separate resolution measurements for 1,000 `localhost` resolutions:

| Resolver | Total (ms) | Mean (microseconds) |
| --- | ---: | ---: |
| Platform `tokio::net::lookup_host` | 118.322 | 118.322 |
| Warm SDK resolver/hosts path | 9.700 | 9.700 |

The warm SDK resolution cost is additional to the request timing above.
`localhost` exercises local resolution; it does not measure uncached DNS
latency or production DNS behavior. TTL refresh is verified separately by a
UDP DNS fault test that changes a record after its one-second TTL expires.

The HTTP server observed exactly two TCP connections across all 10,020 reads
(including warmup), one per transport, confirming healthy pool reuse. The
benchmark body completed in 1.56 seconds; compilation took 3.18 seconds and
is excluded. CPU utilization and peak memory were not measured.

These results establish local overhead, not production throughput or WAN
latency. Fault tests separately cover bounded recovery from stalled and
broken connections over HTTP/1.1 and HTTP/2. No production load test was run.

## DNS and address-family review fixes — 2026-10-19

Measured implementation: `1dd20609127992c1c251229b2c6b9139cf81b331`, with the
same machine, toolchain, debug profile and `just bench-sdk-resilience` command
as above. The first run was more variable, so a second run checked repeatability;
both are retained here. Request timings still exclude DNS.

| Run / round | Baseline, 1,000 reads (ms) | Resilient, 1,000 reads (ms) |
| --- | ---: | ---: |
| First / 0 | 160.755 | 174.302 |
| First / 1 | 132.984 | 182.224 |
| First / 2 | 158.306 | 165.378 |
| First / 3 | 134.136 | 154.045 |
| First / 4 | 132.590 | 167.737 |
| Repeat / 0 | 137.912 | 148.921 |
| Repeat / 1 | 130.471 | 145.897 |
| Repeat / 2 | 128.430 | 145.996 |
| Repeat / 3 | 129.285 | 145.094 |
| Repeat / 4 | 130.724 | 147.043 |

The first run averaged 143.754 → 168.737 microseconds/read (+24.983
microseconds, 17.4%). The repeat averaged 131.364 → 146.590 microseconds/read
(+15.226 microseconds, 11.6%). Warm local resolution, measured separately,
averaged 17.520 and 15.077 microseconds per lookup; platform resolution averaged
123.389 and 113.352 microseconds. Independent family resolution adds some local
lookup overhead while allowing an available family to proceed when the other
query stalls. These samples do not establish a production latency percentage.

Each run retained exactly two pooled TCP connections across 10,020 reads.
Benchmark body wall times were 1.72 and 1.53 seconds. CPU utilization and peak
memory were not measured. The new fault tests additionally verify stalled
A/AAAA queries, late family answers, and recovery after a two-second connection
timeout within one three-second HTTP attempt.

## Pool retirement and retry quota fixes — 2026-10-19

Measured implementation: `d12277a45149d510d92ff85cead5cc2525ed4515`, using
`just bench-sdk-resilience` on the same Mac17,6 / Apple M5 Max, 18 logical CPUs,
48 GiB RAM, Darwin 25.5.0 arm64, Rust 1.98.1 and unoptimized test profile.
Request timings exclude DNS, as in the earlier measurements.

| Round | Baseline, 1,000 reads (ms) | Resilient, 1,000 reads (ms) |
| --- | ---: | ---: |
| 0 | 135.404 | 147.570 |
| 1 | 129.518 | 146.284 |
| 2 | 128.534 | 148.154 |
| 3 | 129.008 | 147.825 |
| 4 | 130.728 | 149.142 |
| Mean | 130.638 | 147.795 |

Mean latency was 130.638 → 147.795 microseconds/read (+17.157 microseconds,
13.1%), corresponding to about 7,655 and 6,766 sequential reads/second.
The resilient mean remains within the 146.590–168.737 microseconds/read range
of the preceding two runs. Warm local resolution averaged another 15.243
microseconds per lookup; platform resolution averaged 114.835 microseconds.
This is a local debug-profile comparison, not a production latency estimate.

Exactly two TCP connections served all 10,020 reads. The benchmark body took
1.53 seconds; Cargo preparation took 0.13 seconds and is excluded. CPU
utilization and peak memory were not measured. The concurrency regression
also verifies that two failed streams on an old HTTP/2 connection leave its
healthy replacement available for reuse: four requests use two TCP connections,
instead of the three connections observed in the original review probe. The
quota regression verifies that 20 responses whose `Retry-After` exceeds the
deadline consume zero retry tokens and leave a subsequent read able to recover.
