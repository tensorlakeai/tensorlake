use crate::auth::context::CliContext;
use crate::commands::sbx::{
    DEFAULT_SANDBOX_IMAGE_DISPLAY_NAME, format_created_at, sandbox_endpoint,
};
use crate::error::{CliError, Result};

const MAX_SANDBOX_LIST_PAGES: usize = 10_000;

pub async fn run(
    ctx: &CliContext,
    running_only: bool,
    suspended_only: bool,
    include_terminated: bool,
    quiet: bool,
    archived: bool,
) -> Result<()> {
    if archived {
        return run_archived(ctx, quiet).await;
    }

    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, "sandboxes");

    let mut count = 0usize;
    let mut terminated_hidden = 0usize;
    let mut printed_header = false;

    list_sandboxes_by_page(ctx, &client, &url, "sandboxes", |sandbox| {
        if !include_terminated && is_terminated_sandbox(sandbox) {
            terminated_hidden += 1;
            return;
        }

        if running_only && !is_running_sandbox(sandbox) {
            return;
        }

        if suspended_only && !is_suspended_sandbox(sandbox) {
            return;
        }

        count += 1;
        print_live_sandbox(sandbox, quiet, &mut printed_header);
    })
    .await?;

    if count == 0 && !quiet {
        println!("No sandboxes found.");
        return Ok(());
    }

    if !quiet {
        println!(
            "{} sandbox{}, {} terminated hidden (use --all to show)",
            count,
            if count != 1 { "es" } else { "" },
            terminated_hidden
        );
    }

    Ok(())
}

async fn run_archived(ctx: &CliContext, quiet: bool) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, "archived-sandboxes");

    let mut count = 0usize;
    let mut printed_header = false;

    list_sandboxes_by_page(ctx, &client, &url, "archived sandboxes", |sandbox| {
        count += 1;
        print_archived_sandbox(sandbox, quiet, &mut printed_header);
    })
    .await?;

    if count == 0 && !quiet {
        println!("No archived sandboxes found.");
        return Ok(());
    }

    if !quiet {
        println!(
            "{} archived sandbox{}",
            count,
            if count != 1 { "es" } else { "" },
        );
    }

    Ok(())
}

async fn list_sandboxes_by_page(
    ctx: &CliContext,
    client: &reqwest::Client,
    initial_url: &str,
    label: &str,
    mut on_sandbox: impl FnMut(&serde_json::Value),
) -> Result<()> {
    let mut url = initial_url.to_string();

    for _ in 0..MAX_SANDBOX_LIST_PAGES {
        if ctx.debug {
            eprintln!("DEBUG sbx ls: GET {}", url);
        }

        let resp = client.get(&url).send().await.map_err(CliError::Http)?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(CliError::Other(anyhow::anyhow!(
                "failed to list {} (HTTP {}): {}",
                label,
                status,
                body
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(CliError::Http)?;

        if let Some(items) = body
            .get("sandboxes")
            .or_else(|| body.get("items"))
            .and_then(|v| v.as_array())
        {
            for sandbox in items {
                on_sandbox(sandbox);
            }
        }

        let Some(next_url) = next_sandbox_list_url(&url, &body) else {
            return Ok(());
        };
        url = next_url;
    }

    Err(CliError::Other(anyhow::anyhow!(
        "failed to list {}: exceeded pagination limit of {} pages",
        label,
        MAX_SANDBOX_LIST_PAGES
    )))
}

fn print_live_sandbox(sandbox: &serde_json::Value, quiet: bool, printed_header: &mut bool) {
    let id = sandbox_id(sandbox);
    if quiet {
        println!("{}", id);
        return;
    }

    if !*printed_header {
        print_live_header();
        *printed_header = true;
    }

    let resources = sandbox.get("resources");
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        id,
        sandbox.get("name").and_then(|v| v.as_str()).unwrap_or(""),
        sandbox
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("-"),
        sandbox_image(sandbox),
        format_cpus(resources),
        format_memory(resources),
        format_disk(resources, false),
        format_created_at(sandbox.get("created_at")),
    );
}

fn print_archived_sandbox(sandbox: &serde_json::Value, quiet: bool, printed_header: &mut bool) {
    let id = sandbox_id(sandbox);
    if quiet {
        println!("{}", id);
        return;
    }

    if !*printed_header {
        print_archived_header();
        *printed_header = true;
    }

    let resources = sandbox.get("resources");
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}",
        id,
        sandbox.get("name").and_then(|v| v.as_str()).unwrap_or(""),
        sandbox_image(sandbox),
        format_cpus(resources),
        format_memory(resources),
        format_disk(resources, true),
        format_created_at(sandbox.get("archived_at")),
    );
}

fn print_live_header() {
    println!("ID\tName\tStatus\tImage\tCPUs\tMemory\tDisk\tCreated At");
}

fn print_archived_header() {
    println!("ID\tName\tImage\tCPUs\tMemory\tDisk\tArchived At");
}

fn sandbox_id(sandbox: &serde_json::Value) -> &str {
    sandbox
        .get("sandbox_id")
        .or_else(|| sandbox.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("-")
}

fn sandbox_image(sandbox: &serde_json::Value) -> &str {
    sandbox
        .get("image")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_SANDBOX_IMAGE_DISPLAY_NAME)
}

fn format_cpus(resources: Option<&serde_json::Value>) -> String {
    resources
        .and_then(|r| r.get("cpus"))
        .and_then(|v| v.as_f64())
        .map(|v| format!("{}", v))
        .unwrap_or_else(|| "-".to_string())
}

fn format_memory(resources: Option<&serde_json::Value>) -> String {
    resources
        .and_then(|r| r.get("memory_mb"))
        .and_then(|v| v.as_i64())
        .map(|v| format!("{} MB", v))
        .unwrap_or_else(|| "-".to_string())
}

fn format_disk(resources: Option<&serde_json::Value>, include_ephemeral_fallback: bool) -> String {
    resources
        .and_then(|r| {
            if include_ephemeral_fallback {
                r.get("disk_mb").or_else(|| r.get("ephemeral_disk_mb"))
            } else {
                r.get("disk_mb")
            }
        })
        .and_then(|v| v.as_i64())
        .map(|v| format!("{} MB", v))
        .unwrap_or_else(|| "-".to_string())
}

fn next_sandbox_list_url(current_url: &str, body: &serde_json::Value) -> Option<String> {
    if let Some(next) = body
        .get("pagination")
        .and_then(|v| v.get("next"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        return Some(resolve_next_url_or_token(current_url, "next", next));
    }

    if let Some(next) = body
        .get("paginator")
        .and_then(|v| v.get("next"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        return Some(resolve_next_url_or_token(current_url, "next", next));
    }

    for (container, field, query_name) in [
        ("pagination", "next_token", "nextToken"),
        ("pagination", "nextToken", "nextToken"),
        ("pagination", "next_cursor", "cursor"),
        ("pagination", "nextCursor", "cursor"),
        ("paginator", "next_token", "nextToken"),
        ("paginator", "nextToken", "nextToken"),
        ("paginator", "next_cursor", "cursor"),
        ("paginator", "nextCursor", "cursor"),
    ] {
        if let Some(token) = body
            .get(container)
            .and_then(|v| v.get(field))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            return Some(url_with_query_param(current_url, query_name, token));
        }
    }

    for (field, query_name) in [
        ("next_cursor", "cursor"),
        ("nextCursor", "cursor"),
        ("next_token", "nextToken"),
        ("nextToken", "nextToken"),
    ] {
        if let Some(token) = body
            .get(field)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            return Some(url_with_query_param(current_url, query_name, token));
        }
    }

    None
}

fn resolve_next_url_or_token(current_url: &str, query_name: &str, next: &str) -> String {
    if next.starts_with("http://") || next.starts_with("https://") {
        return next.to_string();
    }

    if next.starts_with('/')
        && let Ok(current) = url::Url::parse(current_url)
        && let Ok(resolved) = current.join(next)
    {
        return resolved.to_string();
    }

    url_with_query_param(current_url, query_name, next)
}

fn url_with_query_param(current_url: &str, name: &str, value: &str) -> String {
    let mut parsed =
        url::Url::parse(current_url).expect("sandbox list URL should be absolute and valid");
    let existing_pairs: Vec<(String, String)> = parsed
        .query_pairs()
        .filter(|(key, _)| !is_pagination_query_param(key))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();

    parsed.set_query(None);
    {
        let mut query_pairs = parsed.query_pairs_mut();
        for (key, value) in existing_pairs {
            query_pairs.append_pair(&key, &value);
        }
        query_pairs.append_pair(name, value);
    }
    parsed.to_string()
}

fn is_pagination_query_param(key: &str) -> bool {
    matches!(
        key,
        "next" | "cursor" | "nextToken" | "next_token" | "nextCursor" | "next_cursor"
    )
}

fn is_running_sandbox(sandbox: &serde_json::Value) -> bool {
    sandbox
        .get("status")
        .and_then(|v| v.as_str())
        .is_some_and(|status| status.eq_ignore_ascii_case("running"))
}

fn is_suspended_sandbox(sandbox: &serde_json::Value) -> bool {
    sandbox
        .get("status")
        .and_then(|v| v.as_str())
        .is_some_and(|status| status.eq_ignore_ascii_case("suspended"))
}

fn is_non_terminated_sandbox(sandbox: &serde_json::Value) -> bool {
    !is_terminated_sandbox(sandbox)
}

fn is_terminated_sandbox(sandbox: &serde_json::Value) -> bool {
    sandbox
        .get("status")
        .and_then(|v| v.as_str())
        .is_some_and(|status| status.eq_ignore_ascii_case("terminated"))
}

#[cfg(test)]
mod tests {
    use super::{
        is_non_terminated_sandbox, is_running_sandbox, is_suspended_sandbox, is_terminated_sandbox,
        next_sandbox_list_url,
    };
    use serde_json::json;

    #[test]
    fn running_filter_matches_only_running_status() {
        let running = serde_json::json!({ "status": "running" });
        let terminated = serde_json::json!({ "status": "terminated" });
        let pending = serde_json::json!({ "status": "pending" });

        assert!(is_running_sandbox(&running));
        assert!(!is_running_sandbox(&terminated));
        assert!(!is_running_sandbox(&pending));
    }

    #[test]
    fn suspended_filter_matches_only_suspended_status() {
        let suspended = serde_json::json!({ "status": "suspended" });
        let running = serde_json::json!({ "status": "running" });
        let suspending = serde_json::json!({ "status": "suspending" });

        assert!(is_suspended_sandbox(&suspended));
        assert!(!is_suspended_sandbox(&running));
        assert!(!is_suspended_sandbox(&suspending));
    }

    #[test]
    fn default_filter_hides_terminated_sandboxes() {
        let running = serde_json::json!({ "status": "running" });
        let terminated = serde_json::json!({ "status": "terminated" });
        let pending = serde_json::json!({ "status": "pending" });

        assert!(is_non_terminated_sandbox(&running));
        assert!(!is_non_terminated_sandbox(&terminated));
        assert!(is_non_terminated_sandbox(&pending));
    }

    #[test]
    fn terminated_filter_matches_only_terminated_status() {
        let running = serde_json::json!({ "status": "running" });
        let terminated = serde_json::json!({ "status": "terminated" });
        let pending = serde_json::json!({ "status": "pending" });

        assert!(!is_terminated_sandbox(&running));
        assert!(is_terminated_sandbox(&terminated));
        assert!(!is_terminated_sandbox(&pending));
    }

    #[test]
    fn pagination_next_link_resolves_against_current_host() {
        let body = json!({
            "sandboxes": [],
            "pagination": {
                "next": "/sandboxes?next=abc"
            }
        });

        assert_eq!(
            next_sandbox_list_url("https://sandbox.tensorlake.ai/sandboxes", &body),
            Some("https://sandbox.tensorlake.ai/sandboxes?next=abc".to_string())
        );
    }

    #[test]
    fn pagination_next_token_is_added_as_next_query_param() {
        let body = json!({
            "sandboxes": [],
            "pagination": {
                "next": "abc"
            }
        });

        assert_eq!(
            next_sandbox_list_url("https://sandbox.tensorlake.ai/sandboxes", &body),
            Some("https://sandbox.tensorlake.ai/sandboxes?next=abc".to_string())
        );
    }

    #[test]
    fn top_level_next_cursor_is_added_as_cursor_query_param() {
        let body = json!({
            "sandboxes": [],
            "next_cursor": "abc"
        });

        assert_eq!(
            next_sandbox_list_url(
                "https://sandbox.tensorlake.ai/archived-sandboxes?limit=100",
                &body
            ),
            Some(
                "https://sandbox.tensorlake.ai/archived-sandboxes?limit=100&cursor=abc".to_string()
            )
        );
    }

    #[test]
    fn pagination_query_param_replaces_previous_value() {
        let body = json!({
            "sandboxes": [],
            "pagination": {
                "next": "def"
            }
        });

        assert_eq!(
            next_sandbox_list_url("https://sandbox.tensorlake.ai/sandboxes?next=abc", &body),
            Some("https://sandbox.tensorlake.ai/sandboxes?next=def".to_string())
        );
    }
}
