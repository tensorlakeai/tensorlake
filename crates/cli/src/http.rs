use std::sync::Once;

/// Build a reqwest client builder after ensuring rustls has a crypto provider.
pub(crate) fn client_builder() -> reqwest::ClientBuilder {
    ensure_rustls_provider();
    reqwest::Client::builder()
}

fn ensure_rustls_provider() {
    static INSTALL_PROVIDER: Once = Once::new();
    INSTALL_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[cfg(test)]
mod tests {
    use super::client_builder;

    #[test]
    fn installs_rustls_provider() {
        let _ = client_builder();
        assert!(
            rustls::crypto::CryptoProvider::get_default().is_some(),
            "rustls crypto provider should be installed"
        );
    }
}
