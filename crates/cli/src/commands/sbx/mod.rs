pub mod clone;
pub mod cp;
pub mod create;
pub mod exec;
pub mod image;
pub mod ls;
pub mod name;
pub mod port;
pub mod resume;
pub mod run;
pub mod snapshot;
pub mod snapshot_ls;
pub mod snapshot_rm;
pub mod ssh;
pub mod suspend;
pub mod terminate;
pub mod tunnel;

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use chrono::{DateTime, Local, TimeZone, Utc};
use tokio::time::{Duration, Instant};

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

pub fn apply_proxy_access_settings(
    body: &mut serde_json::Value,
    ports: &[u16],
    allow_unauthenticated_access: bool,
) {
    if !ports.is_empty() {
        body["exposed_ports"] = serde_json::json!(ports);
    }

    if allow_unauthenticated_access || !ports.is_empty() {
        body["allow_unauthenticated_access"] = serde_json::Value::Bool(true);
    }
}

pub const DEFAULT_SANDBOX_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
const SANDBOX_WAIT_POLL_INTERVAL: Duration = Duration::from_secs(1);

pub fn new_spinner(message: &str) -> indicatif::ProgressBar {
    let spinner = indicatif::ProgressBar::new_spinner();
    spinner.set_style(
        indicatif::ProgressStyle::default_spinner()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
            .template("{spinner} {msg}")
            .unwrap(),
    );
    spinner.set_message(message.to_string());
    spinner.enable_steady_tick(std::time::Duration::from_millis(80));
    spinner
}

pub async fn wait_for_sandbox_status(
    ctx: &CliContext,
    sandbox_id: &str,
    waiting_message: &str,
    target_status: &str,
    timeout: Duration,
) -> Result<String> {
    let client = ctx.client()?;
    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());

    let spinner = if is_tty {
        Some(new_spinner(&format!("{}...", waiting_message)))
    } else {
        None
    };

    let deadline = Instant::now() + timeout;
    loop {
        if Instant::now() > deadline {
            if let Some(ref s) = spinner {
                s.finish_and_clear();
            }
            return Err(CliError::Other(anyhow::anyhow!(
                "Sandbox {} did not reach '{}' within {}s",
                sandbox_id,
                target_status,
                timeout.as_secs()
            )));
        }

        let info_resp = client
            .get(sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}")))
            .send()
            .await
            .map_err(CliError::Http)?;

        if info_resp.status().is_success() {
            let info: serde_json::Value = info_resp.json().await.map_err(CliError::Http)?;
            let current_status = info
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();

            if current_status == target_status {
                if let Some(ref s) = spinner {
                    s.finish_and_clear();
                }
                return Ok(current_status);
            }

            if current_status == "terminated" && target_status != "terminated" {
                if let Some(ref s) = spinner {
                    s.finish_and_clear();
                }
                let message =
                    format_sandbox_wait_termination_message("Sandbox", target_status, &info);
                return Err(CliError::Other(anyhow::anyhow!(message)));
            }
        }

        tokio::time::sleep(SANDBOX_WAIT_POLL_INTERVAL).await;
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

fn sandbox_termination_detail(info: &serde_json::Value) -> Option<String> {
    let reason = info
        .get("termination_reason")
        .and_then(|value| value.as_str())?;
    match reason {
        "ImageNotFound" => Some(
            info.get("image")
                .and_then(|value| value.as_str())
                .filter(|image| !image.is_empty())
                .map(|image| format!("image not found: {image}"))
                .unwrap_or_else(|| "image not found".to_string()),
        ),
        _ => Some(format!("termination reason: {reason}")),
    }
}

fn format_sandbox_wait_termination_message(
    subject: &str,
    target_status: &str,
    info: &serde_json::Value,
) -> String {
    if let Some(detail) = sandbox_termination_detail(info) {
        format!("{subject} failed to reach '{target_status}': {detail}")
    } else {
        format!("{subject} terminated while waiting to reach '{target_status}'")
    }
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

#[cfg(test)]
mod proxy_access_tests {
    use super::apply_proxy_access_settings;

    #[test]
    fn exposed_ports_enable_unauthenticated_access() {
        let mut body = serde_json::json!({});

        apply_proxy_access_settings(&mut body, &[8000], false);

        assert_eq!(body["exposed_ports"], serde_json::json!([8000]));
        assert_eq!(
            body["allow_unauthenticated_access"],
            serde_json::Value::Bool(true)
        );
    }

    #[test]
    fn explicit_unauthenticated_access_without_ports_is_preserved() {
        let mut body = serde_json::json!({});

        apply_proxy_access_settings(&mut body, &[], true);

        assert_eq!(
            body["allow_unauthenticated_access"],
            serde_json::Value::Bool(true)
        );
        assert!(body.get("exposed_ports").is_none());
    }

    #[test]
    fn no_ports_and_no_unauthenticated_access_leave_body_unchanged() {
        let mut body = serde_json::json!({});

        apply_proxy_access_settings(&mut body, &[], false);

        assert!(body.get("allow_unauthenticated_access").is_none());
        assert!(body.get("exposed_ports").is_none());
    }
}

pub fn format_created_at(value: Option<&serde_json::Value>) -> String {
    let Some(timestamp) = created_at_sort_key(value) else {
        return "-".to_string();
    };

    let now = Utc::now();
    let age = now.signed_duration_since(timestamp);

    if age < chrono::TimeDelta::zero() || age > chrono::TimeDelta::days(5) {
        return timestamp
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M")
            .to_string();
    }

    if age < chrono::TimeDelta::minutes(1) {
        let seconds = age.num_seconds().max(0);
        return format_relative(seconds, "sec");
    }

    if age < chrono::TimeDelta::hours(1) {
        return format_relative(age.num_minutes(), "min");
    }

    if age < chrono::TimeDelta::days(1) {
        return format_relative(age.num_hours(), "hr");
    }

    format_relative(age.num_days(), "day")
}

fn format_relative(value: i64, unit: &str) -> String {
    let suffix = if unit == "day" && value != 1 { "s" } else { "" };
    format!("{value} {unit}{suffix} ago")
}

pub fn created_at_sort_key(value: Option<&serde_json::Value>) -> Option<DateTime<Utc>> {
    match value? {
        serde_json::Value::String(timestamp) => DateTime::parse_from_rfc3339(timestamp)
            .ok()
            .map(|dt| dt.with_timezone(&Utc)),
        serde_json::Value::Number(timestamp) => parse_numeric_timestamp(timestamp.as_f64()?),
        _ => None,
    }
}

fn parse_numeric_timestamp(timestamp: f64) -> Option<DateTime<Utc>> {
    let seconds = if timestamp > 1e15 {
        timestamp / 1_000_000.0
    } else if timestamp > 1e12 {
        timestamp / 1_000.0
    } else {
        timestamp
    };

    let whole_seconds = seconds.trunc() as i64;
    let nanos = ((seconds.fract() * 1_000_000_000.0).round() as u32).min(999_999_999);
    Utc.timestamp_opt(whole_seconds, nanos).single()
}

#[cfg(test)]
mod tests {
    use super::{
        format_created_at, format_sandbox_wait_termination_message, sandbox_termination_detail,
    };
    use chrono::{Duration, Utc};

    #[test]
    fn format_created_at_uses_relative_time_for_recent_timestamps() {
        let timestamp = serde_json::Value::String((Utc::now() - Duration::minutes(3)).to_rfc3339());
        let formatted = format_created_at(Some(&timestamp));

        assert_eq!(formatted, "3 min ago");
    }

    #[test]
    fn format_created_at_uses_absolute_time_after_five_days() {
        let timestamp = serde_json::Value::String((Utc::now() - Duration::days(6)).to_rfc3339());
        let formatted = format_created_at(Some(&timestamp));

        assert!(!formatted.ends_with("ago"));
        assert!(formatted.contains('-'));
        assert!(formatted.contains(':'));
    }

    #[test]
    fn sandbox_termination_detail_formats_image_not_found() {
        let info = serde_json::json!({
            "termination_reason": "ImageNotFound",
            "image": "foo",
        });

        let detail = sandbox_termination_detail(&info);

        assert_eq!(detail.as_deref(), Some("image not found: foo"));
    }

    #[test]
    fn sandbox_wait_termination_message_includes_image_not_found_detail() {
        let info = serde_json::json!({
            "termination_reason": "ImageNotFound",
            "image": "foo",
        });

        let message = format_sandbox_wait_termination_message("Sandbox", "running", &info);

        assert_eq!(
            message,
            "Sandbox failed to reach 'running': image not found: foo"
        );
    }

    #[test]
    fn sandbox_wait_termination_message_falls_back_without_reason() {
        let info = serde_json::json!({
            "status": "terminated",
        });

        let message = format_sandbox_wait_termination_message("Sandbox", "running", &info);

        assert_eq!(
            message,
            "Sandbox terminated while waiting to reach 'running'"
        );
    }

    #[test]
    fn sandbox_wait_termination_message_includes_generic_reason() {
        let info = serde_json::json!({
            "termination_reason": "StartupFailedInternalError",
        });

        let message = format_sandbox_wait_termination_message("Sandbox", "running", &info);

        assert_eq!(
            message,
            "Sandbox failed to reach 'running': termination reason: StartupFailedInternalError"
        );
    }
}
