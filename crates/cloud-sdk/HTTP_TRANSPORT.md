# HTTP transport policy

The shipped SDK and CLI construct HTTP clients through `http_transport`, never directly through
reqwest. HTTPS uses Rustls with Mozilla trust anchors from `webpki-roots`; it does not consult the
host's CA store. Certificate and hostname verification stay enabled. Root rotation ships through
dependency updates and binary releases. OS-only enterprise/private roots are not implicitly trusted.

The Unix-socket factory uses plaintext HTTP with an empty TLS trust set. It routes all requests
through the supplied socket, without DNS/TCP fallback; an HTTPS peer cannot validate. The caller
must establish ownership and permissions of the socket (TLFS uses the root-owned host proxy).

`ArtifactStorageClient::with_http_client` lets a caller supply a prebuilt transport and share it
with wrappers using `http_client().clone()`. Clones reuse the connection pool and trust policy.
Passing no Platform API client skips that unused transport and makes token minting fail locally;
callers must supply scoped credentials or use the authenticated host proxy. Ordinary constructors
retain their Platform API client and existing authentication behavior.

The Rustls config is passed through reqwest's preconfigured backend because reqwest 0.13 accepts
full certificates in `tls_certs_only`, while `webpki-roots` supplies trust anchors. Keep Rustls's
version aligned with reqwest; the certless construction test detects an incompatible backend.

`clippy.toml` rejects bare client constructors and `reqwest::get`; only the transport factories
have narrowly scoped exceptions. CI runs `just lint-http-transports` and `just test-artifact-storage`.
The Artifact Storage companion CI also launches the real musl TLFS daemon in a certless child and
observes its first authenticated Unix-socket request for writable and read-only mounts.
