//! Retry policy shared by every binding.
//!
//! Two questions decide whether a failed request is replayed, and they are
//! deliberately separate:
//!
//! 1. **Did the request reach the server that would have executed it?** When
//!    it provably did not — a connect failure, or the Sandbox Proxy answering
//!    that it could not complete the lookup before forwarding — replaying it
//!    cannot duplicate a side effect, so even non-idempotent operations
//!    (starting a process, creating a sandbox) are replayed. These failures
//!    are what a control-plane rollout looks like from the outside, so they
//!    get a time budget that spans one: [`UNDELIVERED_REPLAY_BUDGET`].
//! 2. **Is the failure transient?** A 502/503/504 or a dropped event stream
//!    is worth a handful of retries — but only for idempotent operations,
//!    because the server may have executed the request before failing.
//!
//! Timeouts are in neither class. A timed-out request was on the wire and may
//! be executing; nothing here replays it.

use std::{future::Future, time::Duration};

use crate::error::SdkError;

/// How long a request that never reached the server keeps being replayed.
///
/// Sized to outlast an Indexify server rollout: the pod is replaced in
/// 7–30 s and the Sandbox Proxy reports lookups as unavailable for a few
/// seconds after that. Replays stop as soon as one attempt succeeds.
/// No undelivered replay starts at or after this deadline, including when a
/// retry sleep wakes late. An attempt already in flight is allowed to finish.
pub const UNDELIVERED_REPLAY_BUDGET: Duration = Duration::from_secs(30);

/// Cap on the doubling wait between transient retries of an idempotent
/// operation. This is the schedule the bindings have always used; a caller
/// asking for ten retries gets ~53 s of patience, as before.
pub const MAX_TRANSIENT_BACKOFF: Duration = Duration::from_secs(15);

/// Cap on the wait between replays of an undelivered request. Lower than the
/// transient cap so the replays keep probing a recovering control plane every
/// few seconds instead of sleeping through most of the budget.
pub const MAX_UNDELIVERED_BACKOFF: Duration = Duration::from_secs(5);

/// The exponential schedule shared by both caps: `min(0.1 s × 2^attempt, cap)
/// × 0.75`, `attempt` 1-based. The 0.75 is applied after the cap, so the
/// longest wait is three quarters of `cap`.
fn backoff(attempt: usize, cap: Duration) -> Duration {
    let base = Duration::from_millis(100).saturating_mul(1u32 << attempt.min(31));
    base.min(cap).mul_f64(0.75)
}

/// Wait before the `attempt`-th transient retry: 0.15, 0.3, 0.6, 1.2, 2.4,
/// 4.8, 9.6, 11.25, 11.25 …
pub fn transient_backoff(attempt: usize) -> Duration {
    backoff(attempt, MAX_TRANSIENT_BACKOFF)
}

/// Wait before the `attempt`-th undelivered replay: 0.15, 0.3, 0.6, 1.2, 2.4,
/// 3.75, 3.75 …
pub fn undelivered_backoff(attempt: usize) -> Duration {
    backoff(attempt, MAX_UNDELIVERED_BACKOFF)
}

/// Whether the failure is worth retrying for an operation that can safely run
/// twice. Excludes timeouts on purpose; see the module docs.
pub fn is_transient(error: &SdkError) -> bool {
    match error {
        SdkError::ServerError { status, .. } => {
            *status == reqwest::StatusCode::BAD_GATEWAY
                || *status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                || *status == reqwest::StatusCode::GATEWAY_TIMEOUT
        }
        SdkError::EventSourceError(_) => true,
        _ => false,
    }
}

/// What a caller is willing to replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// The operation can run twice without harm, so transient failures are
    /// retried up to `max_transient_retries` times.
    pub idempotent: bool,
    /// Retry cap for transient failures of idempotent operations. Failures
    /// that never reached the server are governed by
    /// [`UNDELIVERED_REPLAY_BUDGET`] instead, whatever this is.
    pub max_transient_retries: usize,
}

impl RetryPolicy {
    /// Retry transient failures up to `max_transient_retries` times, and
    /// replay undelivered requests for the full budget.
    pub const fn idempotent(max_transient_retries: usize) -> Self {
        Self {
            idempotent: true,
            max_transient_retries,
        }
    }

    /// Replay only requests that provably never reached the server. Safe to
    /// wrap starting a process or creating a sandbox.
    pub const fn non_idempotent() -> Self {
        Self {
            idempotent: false,
            max_transient_retries: 0,
        }
    }
}

/// The outcome of asking whether to try again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// Wait this long, then try again.
    Retry(Duration),
    /// Give up and return the error.
    Stop,
}

/// Bookkeeping for one retried operation. Pure — the caller supplies elapsed
/// time and performs the wait — so the policy can be tested without a clock.
#[derive(Debug, Clone)]
pub struct RetryState {
    policy: RetryPolicy,
    transient_retries: usize,
    undelivered_replays: usize,
}

impl RetryState {
    pub fn new(policy: RetryPolicy) -> Self {
        Self {
            policy,
            transient_retries: 0,
            undelivered_replays: 0,
        }
    }

    /// Decide what to do about `error`, given how long the whole operation has
    /// been running.
    pub fn decide(&mut self, error: &SdkError, elapsed: Duration) -> RetryDecision {
        if error.never_reached_server() {
            if elapsed >= UNDELIVERED_REPLAY_BUDGET {
                return RetryDecision::Stop;
            }
            self.undelivered_replays += 1;
            let wait = undelivered_backoff(self.undelivered_replays);
            // Cap the wait at the budget. Callers must recheck after sleeping,
            // since a clipped wait reaches the deadline and any wait can wake late.
            return RetryDecision::Retry(wait.min(UNDELIVERED_REPLAY_BUDGET - elapsed));
        }
        if self.policy.idempotent
            && is_transient(error)
            && self.transient_retries < self.policy.max_transient_retries
        {
            self.transient_retries += 1;
            return RetryDecision::Retry(transient_backoff(self.transient_retries));
        }
        RetryDecision::Stop
    }

    /// Attempts made so far beyond the first.
    pub fn retries(&self) -> usize {
        self.transient_retries + self.undelivered_replays
    }
}

/// Run `op` under `policy`, sleeping on the tokio timer between attempts.
pub async fn retry<C, T, F, Fut>(client: C, policy: RetryPolicy, op: F) -> Result<T, SdkError>
where
    C: Clone,
    F: Fn(C) -> Fut,
    Fut: Future<Output = Result<T, SdkError>>,
{
    let started = tokio::time::Instant::now();
    let mut state = RetryState::new(policy);
    loop {
        match op(client.clone()).await {
            Ok(value) => return Ok(value),
            Err(error) => match state.decide(&error, started.elapsed()) {
                RetryDecision::Stop => return Err(error),
                RetryDecision::Retry(wait) => {
                    tokio::time::sleep(wait).await;
                    if started.elapsed() >= UNDELIVERED_REPLAY_BUDGET
                        && error.never_reached_server()
                    {
                        return Err(error);
                    }
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    fn server_error(status: u16, body: &str) -> SdkError {
        SdkError::ServerError {
            status: reqwest::StatusCode::from_u16(status).unwrap(),
            message: body.to_string(),
        }
    }

    fn proxy_unavailable() -> SdkError {
        server_error(
            503,
            r#"{"error":"Sandbox routing is temporarily unavailable: the control plane could not be reached. Retry the request.","code":"SANDBOX_UPSTREAM_ERROR"}"#,
        )
    }

    #[test]
    fn undelivered_backoff_grows_then_caps_low() {
        assert_eq!(undelivered_backoff(1), Duration::from_millis(150));
        assert_eq!(undelivered_backoff(2), Duration::from_millis(300));
        assert_eq!(undelivered_backoff(5), Duration::from_millis(2_400));
        assert_eq!(undelivered_backoff(6), Duration::from_millis(3_750));
        assert_eq!(undelivered_backoff(40), Duration::from_millis(3_750));
    }

    #[test]
    fn transient_backoff_keeps_the_original_schedule() {
        // The schedule the bindings shipped with before this module existed:
        // 0.1 s × 2^n, capped at 15 s, × 0.75.
        assert_eq!(transient_backoff(1), Duration::from_millis(150));
        assert_eq!(transient_backoff(6), Duration::from_millis(4_800));
        assert_eq!(transient_backoff(7), Duration::from_millis(9_600));
        assert_eq!(transient_backoff(8), Duration::from_millis(11_250));
        assert_eq!(transient_backoff(40), Duration::from_millis(11_250));
        let ten_retries: Duration = (1..=10).map(transient_backoff).sum();
        assert_eq!(ten_retries, Duration::from_millis(52_800));
    }

    #[test]
    fn a_proxy_lookup_failure_is_replayed_even_for_non_idempotent_operations() {
        let mut state = RetryState::new(RetryPolicy::non_idempotent());
        let error = proxy_unavailable();

        assert_eq!(
            state.decide(&error, Duration::ZERO),
            RetryDecision::Retry(Duration::from_millis(150))
        );
        assert_eq!(
            state.decide(&error, Duration::from_secs(1)),
            RetryDecision::Retry(Duration::from_millis(300))
        );
    }

    #[test]
    fn undelivered_replays_stop_at_the_budget_and_never_wait_past_it() {
        let mut state = RetryState::new(RetryPolicy::non_idempotent());
        let error = proxy_unavailable();

        // Deep into the budget the capped wait would overshoot; it is clipped.
        for _ in 0..6 {
            state.decide(&error, Duration::from_secs(1));
        }
        assert_eq!(
            state.decide(&error, Duration::from_secs(28)),
            RetryDecision::Retry(Duration::from_secs(2))
        );
        assert_eq!(
            state.decide(&error, UNDELIVERED_REPLAY_BUDGET),
            RetryDecision::Stop
        );
    }

    #[test]
    fn a_plain_503_is_not_replayed_for_non_idempotent_operations() {
        // No proxy code: this came from something that may have run the request.
        let error = server_error(503, r#"{"error":"daemon overloaded"}"#);
        let mut state = RetryState::new(RetryPolicy::non_idempotent());
        assert_eq!(state.decide(&error, Duration::ZERO), RetryDecision::Stop);
    }

    #[test]
    fn idempotent_operations_retry_transient_failures_a_bounded_number_of_times() {
        let error = server_error(502, "bad gateway");
        let mut state = RetryState::new(RetryPolicy::idempotent(2));
        assert!(matches!(
            state.decide(&error, Duration::ZERO),
            RetryDecision::Retry(_)
        ));
        assert!(matches!(
            state.decide(&error, Duration::ZERO),
            RetryDecision::Retry(_)
        ));
        assert_eq!(state.decide(&error, Duration::ZERO), RetryDecision::Stop);

        // 4xx is never transient.
        let mut state = RetryState::new(RetryPolicy::idempotent(5));
        assert_eq!(
            state.decide(&server_error(404, "gone"), Duration::ZERO),
            RetryDecision::Stop
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_replays_a_non_idempotent_operation_across_a_control_plane_gap() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&attempts);
        let result = retry((), RetryPolicy::non_idempotent(), move |()| {
            let counter = Arc::clone(&counter);
            async move {
                // Unavailable for the first six attempts, then the rollout is over.
                if counter.fetch_add(1, Ordering::SeqCst) < 6 {
                    Err(proxy_unavailable())
                } else {
                    Ok("started")
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), "started");
        assert_eq!(attempts.load(Ordering::SeqCst), 7);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_gives_up_once_the_budget_is_spent() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&attempts);
        let started = tokio::time::Instant::now();
        let result: Result<(), SdkError> = retry((), RetryPolicy::non_idempotent(), move |()| {
            assert!(
                started.elapsed() < UNDELIVERED_REPLAY_BUDGET,
                "a replay must not start after the budget is spent"
            );
            counter.fetch_add(1, Ordering::SeqCst);
            async move { Err(proxy_unavailable()) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(started.elapsed(), UNDELIVERED_REPLAY_BUDGET);
        // 0.15+0.3+0.6+1.2+2.4 = 4.65 s, then 3.75 s steps: ~7 more before 30 s.
        let made = attempts.load(Ordering::SeqCst);
        assert!((10..=14).contains(&made), "attempts before budget: {made}");
    }

    #[tokio::test(start_paused = true)]
    async fn retry_returns_the_last_error_if_the_timer_wakes_after_the_deadline() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&attempts);
        let task = tokio::spawn(retry((), RetryPolicy::non_idempotent(), move |()| {
            counter.fetch_add(1, Ordering::SeqCst);
            async move { Err::<(), _>(proxy_unavailable()) }
        }));

        // Let the first failure schedule its 150 ms wait, then simulate a
        // stalled executor that cannot poll that timer until the budget is gone.
        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        tokio::time::advance(UNDELIVERED_REPLAY_BUDGET + Duration::from_secs(1)).await;

        let error = task.await.unwrap().unwrap_err();
        assert_eq!(error.to_string(), proxy_unavailable().to_string());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn transient_retries_can_continue_after_the_undelivered_budget() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&attempts);
        let started = tokio::time::Instant::now();
        let result = retry((), RetryPolicy::idempotent(10), move |()| {
            let attempt = counter.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt < 10 {
                    Err(server_error(503, "service unavailable"))
                } else {
                    Ok(())
                }
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 11);
        assert_eq!(started.elapsed(), Duration::from_millis(52_800));
    }
}
