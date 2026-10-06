//! Server-sent events (SSE) response bodies.
//!
//! Every SSE stream in the SDK, its bindings and the CLI is read through
//! [`sse_events`], so they share one timeout rule and one error model: a
//! timeout bounds each wait for more of the stream, never the whole stream,
//! and connection failures keep their cause.

use std::{pin::Pin, time::Duration};

use eventsource_stream::{Event, EventStreamError, Eventsource};
use futures::{Stream, StreamExt};
use reqwest::Response;

use crate::error::SdkError;

const WAITING_FOR_STREAM_DATA: &str = "more data on the stream";

/// The events of a server-sent events response body.
pub type SseEvents = Pin<Box<dyn Stream<Item = Result<Event, SdkError>> + Send>>;

/// Decode a server-sent events response body; see [`sse_events`].
pub fn event_stream(response: Response, idle_timeout: Option<Duration>) -> SseEvents {
    sse_events(
        response
            .bytes_stream()
            .map(|chunk| chunk.map_err(SdkError::EventStreamTransport)),
        idle_timeout,
    )
}

/// Decode server-sent events from a response body's bytes. When
/// `idle_timeout` is set, each wait for more of the body must end within it;
/// keep-alive comments count as data. The timer runs only while the stream is
/// waiting for bytes. Body errors pass through unchanged; malformed stream
/// data becomes [`SdkError::EventSourceError`].
pub fn sse_events<S, B>(body: S, idle_timeout: Option<Duration>) -> SseEvents
where
    S: Stream<Item = Result<B, SdkError>> + Send + 'static,
    B: AsRef<[u8]> + Send + 'static,
{
    let body = match idle_timeout {
        Some(timeout) => idle_bounded(body, timeout).left_stream(),
        None => body.right_stream(),
    };
    Box::pin(body.eventsource().map(|event| {
        event.map_err(|error| match error {
            EventStreamError::Transport(error) => error,
            error => SdkError::EventSourceError(error.to_string()),
        })
    }))
}

/// End `stream` with [`SdkError::StreamTimeout`] when it yields nothing for
/// `timeout`.
fn idle_bounded<S, T>(stream: S, timeout: Duration) -> impl Stream<Item = Result<T, SdkError>>
where
    S: Stream<Item = Result<T, SdkError>> + Send + 'static,
{
    futures::stream::unfold(Some(Box::pin(stream)), move |stream| async move {
        let mut stream = stream?;
        match tokio::time::timeout(timeout, stream.next()).await {
            Ok(Some(item)) => Some((item, Some(stream))),
            Ok(None) => None,
            Err(_) => Some((
                Err(SdkError::StreamTimeout {
                    waiting_for: WAITING_FOR_STREAM_DATA,
                    timeout,
                }),
                None,
            )),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::sse_events;
    use crate::error::SdkError;
    use futures::{StreamExt, stream};
    use std::time::Duration;

    /// Yield each part after its delay.
    fn delayed(
        parts: Vec<(Duration, Result<&'static str, SdkError>)>,
    ) -> impl futures::Stream<Item = Result<&'static str, SdkError>> + Send + 'static {
        stream::iter(parts).then(|(delay, part)| async move {
            tokio::time::sleep(delay).await;
            part
        })
    }

    #[tokio::test(start_paused = true)]
    async fn keep_alive_comments_reset_the_idle_timeout() {
        let mut parts: Vec<_> = (0..10)
            .map(|_| (Duration::from_millis(50), Ok(":\n\n")))
            .collect();
        parts.push((Duration::from_millis(50), Ok("data: done\n\n")));
        let mut events = sse_events(delayed(parts), Some(Duration::from_millis(100)));

        let event = events.next().await.expect("an event").expect("no error");
        assert_eq!(event.data, "done");
        assert!(events.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_quiet_stream_ends_with_a_stream_timeout() {
        let parts = vec![
            (Duration::ZERO, Ok("data: first\n\n")),
            (Duration::from_secs(60), Ok("data: late\n\n")),
        ];
        let mut events = sse_events(delayed(parts), Some(Duration::from_millis(100)));

        assert_eq!(events.next().await.unwrap().unwrap().data, "first");
        let error = events.next().await.unwrap().unwrap_err();
        assert!(
            matches!(error, SdkError::StreamTimeout { timeout, .. } if timeout == Duration::from_millis(100)),
            "unexpected error: {error:?}"
        );
        assert!(
            events.next().await.is_none(),
            "the stream ends after a timeout"
        );
    }

    #[tokio::test]
    async fn body_errors_pass_through_unchanged() {
        let parts = vec![(
            Duration::ZERO,
            Err(SdkError::Io(std::io::Error::other("reset"))),
        )];
        let mut events = sse_events(delayed(parts), None);

        let error = events.next().await.unwrap().unwrap_err();
        assert!(
            matches!(error, SdkError::Io(_)),
            "unexpected error: {error:?}"
        );
    }
}
