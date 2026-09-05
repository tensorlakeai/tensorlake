//! HTTP trust policy for shipped SDKs and CLIs.
//!
//! HTTPS uses Mozilla roots bundled with `webpki-root-certs`, independently of the
//! host/guest CA store. Root changes ship with dependency updates. System-storage
//! sockets use plaintext HTTP and an empty trust set, so HTTPS fails closed.
//! Configure request policy on these builders; never construct a bare client.

use std::sync::Once;

fn ensure_rustls_provider() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// HTTP(S) transport with bundled Mozilla roots and normal certificate verification.
#[allow(clippy::disallowed_methods)] // The only place that selects HTTPS trust.
pub fn https_builder() -> reqwest::ClientBuilder {
    ensure_rustls_provider();
    // Use full certificates (the sibling of webpki-roots' compact trust anchors), so
    // reqwest still owns TLS/ALPN setup and respects http1_only/http2 configuration.
    reqwest::Client::builder().tls_certs_only(webpki_root_certs::TLS_SERVER_ROOT_CERTS.iter().map(
        |cert| {
            reqwest::tls::Certificate::from_der(cert.as_ref())
                .expect("bundled Mozilla root is a valid X.509 certificate")
        },
    ))
}

/// Plaintext transport through the authenticated host's Unix socket.
/// No system roots, DNS/TCP fallback, or trusted HTTPS peers.
#[cfg(unix)]
#[allow(clippy::disallowed_methods)] // The only place that selects socket trust.
pub fn unix_socket_builder(socket: &std::path::Path) -> reqwest::ClientBuilder {
    ensure_rustls_provider();
    reqwest::Client::builder()
        .tls_certs_only(Vec::<reqwest::tls::Certificate>::new())
        .unix_socket(socket)
}
