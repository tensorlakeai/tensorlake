//! A tiny lock-free latency histogram for backend-operation timing.
//!
//! Fixed buckets with per-bucket atomic counters, plus a running sum + count. Lives in the shared
//! base crate so both the object store and the FDB client can record into one without a new
//! dependency edge; the server exports the snapshot through OpenTelemetry and the compatibility
//! metrics endpoint.

use std::sync::atomic::{AtomicU64, Ordering};

/// Upper bucket bounds in **seconds** (the implicit `+Inf` bucket is added on render). Spans
/// sub-millisecond cache/NVMe ops through multi-second cold S3 reads.
pub const LATENCY_BUCKETS_SECS: [f64; 9] = [0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0];

/// Same bounds in microseconds (the unit `observe` takes), to avoid float comparison on the hot path.
const BUCKETS_MICROS: [u64; 9] = [
    500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000,
];

/// A bucketed latency histogram with atomic counters (cheap to share via `Arc`).
#[derive(Default)]
pub struct LatencyHist {
    /// One counter per bucket in [`LATENCY_BUCKETS_SECS`]; index `len` would be `+Inf`.
    buckets: [AtomicU64; 9],
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl LatencyHist {
    /// Record one observation of `micros` microseconds.
    pub fn observe(&self, micros: u64) {
        let idx = BUCKETS_MICROS.iter().position(|&b| micros <= b);
        if let Some(i) = idx {
            self.buckets[i].fetch_add(1, Ordering::Relaxed);
        }
        // (over the last bound → only counted in +Inf, derived at render from `count`)
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// A consistent-enough snapshot for rendering.
    pub fn snapshot(&self) -> HistSnapshot {
        let mut buckets = [0u64; 9];
        for (i, b) in self.buckets.iter().enumerate() {
            buckets[i] = b.load(Ordering::Relaxed);
        }
        HistSnapshot {
            buckets,
            sum_secs: self.sum_micros.load(Ordering::Relaxed) as f64 / 1e6,
            count: self.count.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time copy of a [`LatencyHist`].
#[derive(Clone, Copy, Debug, Default)]
pub struct HistSnapshot {
    /// Per-bucket counts aligned with [`LATENCY_BUCKETS_SECS`] (non-cumulative).
    pub buckets: [u64; 9],
    pub sum_secs: f64,
    pub count: u64,
}

impl HistSnapshot {
    /// Render Prometheus-compatible histogram lines for `name` with the given label set
    /// (e.g. `op=\"read\"`), emitting cumulative `_bucket` series plus `_sum` and `_count`.
    pub fn render(&self, out: &mut String, name: &str, labels: &str) {
        let sep = if labels.is_empty() { "" } else { "," };
        let mut cumulative = 0u64;
        for (i, edge) in LATENCY_BUCKETS_SECS.iter().enumerate() {
            cumulative += self.buckets[i];
            out.push_str(&format!(
                "{name}_bucket{{{labels}{sep}le=\"{edge}\"}} {cumulative}\n"
            ));
        }
        out.push_str(&format!(
            "{name}_bucket{{{labels}{sep}le=\"+Inf\"}} {}\n",
            self.count
        ));
        out.push_str(&format!("{name}_sum{{{labels}}} {}\n", self.sum_secs));
        out.push_str(&format!("{name}_count{{{labels}}} {}\n", self.count));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_and_render_cumulative() {
        let h = LatencyHist::default();
        h.observe(300); // ≤ 0.0005s bucket
        h.observe(2_000); // ≤ 0.005s
        h.observe(9_000_000); // over the last bound → +Inf only
        let snap = h.snapshot();
        assert_eq!(snap.count, 3);
        let mut out = String::new();
        snap.render(&mut out, "test_seconds", "op=\"x\"");
        // First bucket has the 300µs sample.
        assert!(
            out.contains("test_seconds_bucket{op=\"x\",le=\"0.0005\"} 1"),
            "{out}"
        );
        // +Inf is cumulative over everything.
        assert!(
            out.contains("test_seconds_bucket{op=\"x\",le=\"+Inf\"} 3"),
            "{out}"
        );
        assert!(out.contains("test_seconds_count{op=\"x\"} 3"), "{out}");
    }
}
