/// Use the SDK's bundled-root HTTPS policy for every CLI request.
pub(crate) fn client_builder() -> reqwest::ClientBuilder {
    tensorlake::http_transport::https_builder()
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
