use crate::auth::context::CliContext;
use crate::commands::sbx::{
    DEFAULT_SANDBOX_WAIT_TIMEOUT, sandbox_endpoint, sandbox_proxy_base, wait_for_sandbox_status,
};
use crate::error::{CliError, Result};

pub async fn create_with_request(
    ctx: &CliContext,
    body: serde_json::Value,
    wait: bool,
) -> Result<String> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, "sandboxes");

    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to create sandbox (HTTP {}): {}",
            status,
            body
        )));
    }

    let result: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let sandbox_id = result
        .get("sandbox_id")
        .or_else(|| result.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let status = result
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    if wait && status != "running" {
        wait_for_sandbox_status(
            ctx,
            &sandbox_id,
            "Waiting for sandbox to start",
            "running",
            DEFAULT_SANDBOX_WAIT_TIMEOUT,
        )
        .await?;
    }

    Ok(sandbox_id)
}

/// Resolve an image name to its snapshot ID by querying the Platform API.
async fn resolve_image(ctx: &CliContext, image_name: &str) -> Result<String> {
    let org_id = ctx
        .effective_organization_id()
        .ok_or_else(|| CliError::auth("Organization ID is required for --image"))?;
    let proj_id = ctx
        .effective_project_id()
        .ok_or_else(|| CliError::auth("Project ID is required for --image"))?;

    let client = ctx.client()?;
    let url = format!(
        "{}/platform/v1/organizations/{}/projects/{}/sandbox-templates",
        ctx.api_url, org_id, proj_id
    );

    let resp = client.get(&url).send().await.map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to list images (HTTP {}): {}",
            status,
            body
        )));
    }

    let result: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let items = result
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CliError::Other(anyhow::anyhow!("unexpected image list response")))?;

    for item in items {
        let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
        if name == image_name {
            let snapshot_id = item
                .get("snapshotId")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    CliError::Other(anyhow::anyhow!("image '{}' has no snapshotId", image_name))
                })?;
            eprintln!("Resolved image '{}' → snapshot {}", image_name, snapshot_id);
            return Ok(snapshot_id.to_string());
        }
    }

    Err(CliError::Other(anyhow::anyhow!(
        "image '{}' not found",
        image_name
    )))
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    ctx: &CliContext,
    cpus: f64,
    memory: i64,
    timeout: Option<i64>,
    entrypoint: &[String],
    snapshot_id: Option<&str>,
    image_name: Option<&str>,
    wait: bool,
    ports: &[u16],
    allow_unauthenticated_access: bool,
    no_internet: bool,
    network_allow: &[String],
    network_deny: &[String],
) -> Result<()> {
    // Resolve --image to a snapshot ID if provided.
    let resolved_snapshot = match image_name {
        Some(name) => Some(resolve_image(ctx, name).await?),
        None => None,
    };
    let effective_snapshot = snapshot_id.or(resolved_snapshot.as_deref());

    let mut body = serde_json::json!({
        "resources": {
            "cpus": cpus,
            "memory_mb": memory,
        },
    });

    if let Some(t) = timeout {
        body["timeout_secs"] = serde_json::Value::Number(t.into());
    }
    if !entrypoint.is_empty() {
        body["entrypoint"] = serde_json::json!(entrypoint);
    }
    if let Some(snap) = effective_snapshot {
        body["snapshot_id"] = serde_json::Value::String(snap.to_string());
    }
    if !ports.is_empty() {
        body["exposed_ports"] = serde_json::json!(ports);
    }
    if allow_unauthenticated_access {
        body["allow_unauthenticated_access"] = serde_json::Value::Bool(true);
    }

    let has_network = no_internet || !network_allow.is_empty() || !network_deny.is_empty();
    if has_network {
        let mut network = serde_json::json!({});
        if no_internet {
            network["allow_internet_access"] = serde_json::Value::Bool(false);
        }
        if !network_allow.is_empty() {
            network["allow_out"] = serde_json::json!(network_allow);
        }
        if !network_deny.is_empty() {
            network["deny_out"] = serde_json::json!(network_deny);
        }
        body["network"] = network;
    }

    let sandbox_id = create_with_request(ctx, body, wait).await?;
    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    if is_tty {
        eprintln!("Sandbox {} is ready.", sandbox_id);
    }
    if !is_tty {
        println!("{}", sandbox_id);
    }
    if is_tty {
        print_post_create_tip(ctx, &sandbox_id);
    }
    Ok(())
}

fn print_post_create_tip(ctx: &CliContext, sandbox_id: &str) {
    let (proxy_url, host_header) = sandbox_proxy_base(ctx, sandbox_id);
    let host_flag = host_header
        .as_deref()
        .map(|h| format!(" \\\n     -H \"Host: {}\"", h))
        .unwrap_or_default();

    eprintln!();
    eprintln!("Get started:");
    eprintln!("  tl sbx ssh {sandbox_id}");
    eprintln!("  tl sbx exec {sandbox_id} -- bash -c \"echo Hello, World!\"");

    let tips: Vec<(&str, String)> = vec![
        (
            "copy files into your sandbox?",
            format!("  tl sbx cp ./myfile.py {sandbox_id}:/tmp/myfile.py"),
        ),
        (
            "run a process via the HTTP API?",
            format!(
                "  curl -X POST {proxy_url}/api/v1/processes{host_flag} \\\n     -H \"Content-Type: application/json\" \\\n     -d '{{\"command\": \"echo\", \"args\": [\"Hello, World!\"]}}'"
            ),
        ),
        (
            "run a bash script via the HTTP API?",
            format!(
                "  curl -X POST {proxy_url}/api/v1/processes{host_flag} \\\n     -H \"Content-Type: application/json\" \\\n     -d '{{\"command\": \"bash\", \"args\": [\"-c\", \"for i in 1 2 3; do echo Line $i; sleep 1; done\"]}}'"
            ),
        ),
        (
            "follow process output in real-time?",
            format!(
                "  # Start a process:\n  curl -X POST {proxy_url}/api/v1/processes{host_flag} \\\n     -H \"Content-Type: application/json\" \\\n     -d '{{\"command\": \"bash\", \"args\": [\"-c\", \"for i in 1 2 3; do echo Line $i; sleep 1; done\"]}}'\n\n  # Then stream its output (replace <pid> with the returned pid):\n  curl {proxy_url}/api/v1/processes/<pid>/output/follow{host_flag}"
            ),
        ),
        (
            "write files into your sandbox via the HTTP API?",
            format!(
                "  curl -X PUT \"{proxy_url}/api/v1/files?path=/tmp/hello.txt\"{host_flag} \\\n     -H \"Content-Type: application/octet-stream\" \\\n     -d \"Hello from sandbox!\""
            ),
        ),
        (
            "read files from your sandbox via the HTTP API?",
            format!("  curl \"{proxy_url}/api/v1/files?path=/tmp/hello.txt\"{host_flag}"),
        ),
    ];

    let tip_index = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as usize)
        .unwrap_or(0)
        % tips.len();

    let (title, body) = &tips[tip_index];
    eprintln!();
    eprintln!("Did you know that you can {title}");
    eprintln!();
    eprintln!("{body}");
    eprintln!();
    eprintln!("Docs: https://docs.tensorlake.ai/sandboxes");
}
