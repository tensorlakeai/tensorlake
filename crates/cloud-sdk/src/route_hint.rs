//! Routing hints that follow the sandbox as it moves.
//!
//! The Sandbox Proxy stamps `x-tensorlake-route-hint` on a response whenever
//! it resolved the route through a live control-plane lookup, which is
//! exactly what happens after the hint a client presented went stale (the
//! sandbox resumed on another host, or the lane that issued the hint was
//! replaced). A client that keeps sending the old hint pays a fast-path miss
//! and a server lookup on every first request after each move; a client that
//! adopts the refreshed hint pays once.
//!
//! [`SharedRoutingHint`] is the one cell every clone of a proxy client reads
//! before sending and the [`RouteHintAdoption`] middleware writes after a
//! response. The middleware only acts on requests that carry a
//! [`RouteHintSink`] extension, so ordinary control-plane requests are
//! untouched.

use std::sync::{Arc, RwLock};

use http::Extensions;
use reqwest::header::HeaderMap;
use reqwest_middleware::{Middleware, Next};

/// Response (and request) header carrying the proxy routing hint.
pub const ROUTE_HINT_HEADER: &str = "x-tensorlake-route-hint";

/// The routing hint a proxy client presents, shared by every clone of that
/// client and refreshed from responses.
#[derive(Clone, Debug, Default)]
pub struct SharedRoutingHint(Arc<RwLock<Option<String>>>);

impl SharedRoutingHint {
    pub fn new(hint: Option<String>) -> Self {
        Self(Arc::new(RwLock::new(hint.filter(|h| !h.is_empty()))))
    }

    /// The hint to present on the next request.
    pub fn get(&self) -> Option<String> {
        self.0.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Replace the hint. An empty value clears it.
    pub fn set(&self, hint: Option<String>) {
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = hint.filter(|h| !h.is_empty());
    }

    /// Adopt the hint a response carries, if any. Returns whether the stored
    /// hint changed.
    pub fn adopt_from(&self, headers: &HeaderMap) -> bool {
        let Some(hint) = headers
            .get(ROUTE_HINT_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|h| !h.is_empty())
        else {
            return false;
        };
        let mut guard = self.0.write().unwrap_or_else(|e| e.into_inner());
        if guard.as_deref() == Some(hint) {
            return false;
        }
        *guard = Some(hint.to_string());
        true
    }
}

/// Middleware extension naming the cell a response's hint should be written
/// to. A proxy client attaches it through `Client::with_request_extension`.
#[derive(Clone, Debug)]
pub struct RouteHintSink(pub SharedRoutingHint);

/// Middleware that copies a response's `x-tensorlake-route-hint` into the
/// request's [`RouteHintSink`], when one is attached.
#[derive(Debug, Default)]
pub struct RouteHintAdoption;

#[async_trait::async_trait]
impl Middleware for RouteHintAdoption {
    async fn handle(
        &self,
        req: reqwest::Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let sink = extensions.get::<RouteHintSink>().cloned();
        let response = next.run(req, extensions).await?;
        if let Some(RouteHintSink(cell)) = sink {
            cell.adopt_from(response.headers());
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn adopts_a_new_hint_and_ignores_repeats_and_blanks() {
        let cell = SharedRoutingHint::new(Some("old".into()));
        let mut headers = HeaderMap::new();
        assert!(!cell.adopt_from(&headers));
        headers.insert(ROUTE_HINT_HEADER, HeaderValue::from_static("  "));
        assert!(!cell.adopt_from(&headers));
        assert_eq!(cell.get().as_deref(), Some("old"));
        headers.insert(ROUTE_HINT_HEADER, HeaderValue::from_static("new"));
        assert!(cell.adopt_from(&headers));
        assert!(!cell.adopt_from(&headers));
        assert_eq!(cell.get().as_deref(), Some("new"));
    }

    #[test]
    fn clones_share_one_cell() {
        let a = SharedRoutingHint::new(None);
        let b = a.clone();
        b.set(Some("h".into()));
        assert_eq!(a.get().as_deref(), Some("h"));
        a.set(Some(String::new()));
        assert_eq!(b.get(), None);
    }
}
