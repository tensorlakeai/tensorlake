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
