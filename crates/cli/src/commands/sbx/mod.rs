pub mod cp;
pub mod create;
pub mod exec;
pub mod ls;
pub mod run;
pub mod snapshot;
pub mod ssh;

use crate::auth::context::CliContext;
use crate::error::Result;

/// Build the lifecycle API base URL for sandbox CRUD operations.
///
/// Cloud mode: `{api_url}/sandboxes`, `{api_url}/snapshots/...`
/// Localhost mode: `{api_url}/v1/namespaces/{namespace}/sandboxes`
pub fn sandbox_endpoint(ctx: &CliContext, endpoint: &str) -> String {
    if is_localhost(&ctx.api_url) {
        format!(
            "{}/v1/namespaces/{}/{}",
            ctx.api_url, ctx.namespace, endpoint
        )
    } else {
        format!("{}/{}", ctx.api_url, endpoint)
    }
}

/// Build the proxy base URL for a specific sandbox (process/file/PTY operations).
///
/// The proxy URL uses subdomain-based routing: `{sandbox_id}.sandbox.example.com`
/// For localhost, it uses `localhost:9443` with a Host header override.
pub fn sandbox_proxy_base(ctx: &CliContext, sandbox_id: &str) -> (String, Option<String>) {
    let proxy_url = resolve_proxy_url(&ctx.api_url);

    if let Ok(parsed) = url::Url::parse(&proxy_url) {
        let host = parsed.host_str().unwrap_or("");
        if host == "localhost" || host == "127.0.0.1" {
            // Localhost: keep URL as-is, set Host header to {sandbox_id}.local
            let host_header = format!("{}.local", sandbox_id);
            return (proxy_url, Some(host_header));
        }
        // Cloud: prefix sandbox_id as subdomain
        let port_part = parsed.port().map(|p| format!(":{}", p)).unwrap_or_default();
        let base_url = format!("{}://{}.{}{}", parsed.scheme(), sandbox_id, host, port_part);
        return (base_url, None);
    }

    // Fallback
    (format!("{}/{}", proxy_url, sandbox_id), None)
}

/// Resolve the sandbox proxy URL from env or api_url.
fn resolve_proxy_url(api_url: &str) -> String {
    if let Ok(url) = std::env::var("TENSORLAKE_SANDBOX_PROXY_URL") {
        return url;
    }
    if is_localhost(api_url) {
        return "http://localhost:9443".to_string();
    }
    if let Ok(parsed) = url::Url::parse(api_url) {
        let host = parsed.host_str().unwrap_or("");
        if let Some(rest) = host.strip_prefix("api.") {
            let proxy_host = format!("sandbox.{}", rest);
            return format!("{}://{}", parsed.scheme(), proxy_host);
        }
    }
    "https://sandbox.tensorlake.ai".to_string()
}

fn is_localhost(url: &str) -> bool {
    if let Ok(parsed) = url::Url::parse(url) {
        let host = parsed.host_str().unwrap_or("");
        return host == "localhost" || host == "127.0.0.1";
    }
    false
}

/// Parse `sandbox_id:/remote/path` syntax.
pub fn parse_sandbox_path(path: &str) -> (Option<&str>, &str) {
    if let Some(colon_pos) = path.find(':') {
        let prefix = &path[..colon_pos];
        // Avoid matching single-letter drive letters (Windows)
        if prefix.len() > 1 {
            return (Some(prefix), &path[colon_pos + 1..]);
        }
    }
    (None, path)
}

/// Parse KEY=VALUE environment variable pairs.
pub fn parse_env_vars(env: &[String]) -> Result<Option<serde_json::Value>> {
    if env.is_empty() {
        return Ok(None);
    }
    let mut map = serde_json::Map::new();
    for item in env {
        let eq_pos = item.find('=').ok_or_else(|| {
            crate::error::CliError::usage(format!("Invalid env format: {}. Use KEY=VALUE.", item))
        })?;
        let key = &item[..eq_pos];
        let value = &item[eq_pos + 1..];
        map.insert(
            key.to_string(),
            serde_json::Value::String(value.to_string()),
        );
    }
    Ok(Some(serde_json::Value::Object(map)))
}
