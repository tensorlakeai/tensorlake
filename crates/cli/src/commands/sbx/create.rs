use crate::auth::context::CliContext;
use crate::commands::sbx::{
    DEFAULT_SANDBOX_WAIT_TIMEOUT, apply_proxy_access_settings, sandbox_endpoint,
    sandbox_proxy_base, wait_for_sandbox_status,
};
use crate::error::{CliError, Result};
use serde::Deserialize;

const DEFAULT_SANDBOX_CPUS: f64 = 1.0;
const DEFAULT_SANDBOX_MEMORY_MB: i64 = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GpuRequest<'a> {
    pub count: u32,
    pub model: &'a str,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CreateSandboxResult {
    #[serde(alias = "sandboxId", alias = "id")]
    pub sandbox_id: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default, alias = "sandboxUrl")]
    pub sandbox_url: Option<String>,
    #[serde(default, alias = "ingressEndpoint")]
    pub ingress_endpoint: Option<String>,
}

pub async fn create_with_request(
    ctx: &CliContext,
    body: serde_json::Value,
    wait: bool,
) -> Result<CreateSandboxResult> {
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

    let create_result: CreateSandboxResult = resp.json().await.map_err(CliError::Http)?;
    let is_running = create_result.status.as_deref() == Some("running");

    if wait && !is_running {
        wait_for_sandbox_status(
            ctx,
            &create_result.sandbox_id,
            "Waiting for sandbox to start",
            "running",
            DEFAULT_SANDBOX_WAIT_TIMEOUT,
        )
        .await?;
    }

    Ok(create_result)
}

pub struct CreateArgs<'a> {
    pub name: Option<&'a str>,
    pub cpus: Option<f64>,
    pub memory: Option<i64>,
    pub disk_mb: Option<u64>,
    pub gpu_count: Option<u32>,
    pub gpu_model: Option<&'a str>,
    pub timeout: Option<i64>,
    pub entrypoint: &'a [String],
    pub snapshot_id: Option<&'a str>,
    pub image_name: Option<&'a str>,
    pub wait: bool,
    pub ports: &'a [u16],
    pub allow_unauthenticated_access: bool,
    pub no_internet: bool,
    pub network_allow: &'a [String],
    pub network_deny: &'a [String],
}

pub async fn run(ctx: &CliContext, args: CreateArgs<'_>) -> Result<()> {
    let CreateArgs {
        name,
        cpus,
        memory,
        disk_mb,
        gpu_count,
        gpu_model,
        timeout,
        entrypoint,
        snapshot_id,
        image_name,
        wait,
        ports,
        allow_unauthenticated_access,
        no_internet,
        network_allow,
        network_deny,
    } = args;

    let gpu = match gpu_count {
        Some(count) => Some(GpuRequest {
            count,
            model: gpu_model.unwrap_or("A10"),
        }),
        None => None,
    };

    let mut body = build_create_request_body(
        cpus,
        memory,
        disk_mb,
        gpu,
        timeout,
        entrypoint,
        snapshot_id,
        image_name,
    );
    if let Some(n) = name {
        body["name"] = serde_json::Value::String(n.to_string());
    }

    apply_proxy_access_settings(&mut body, ports, allow_unauthenticated_access);

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

    let create_result = create_with_request(ctx, body, wait).await?;
    let sandbox_id = create_result.sandbox_id.clone();
    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    let display_id = name.unwrap_or(&sandbox_id);
    if is_tty {
        eprint!("{}", format_ready_message(name, &sandbox_id));
    }
    if !is_tty {
        println!("{}", sandbox_id);
    }
    if is_tty {
        print_post_create_tip(ctx, &create_result, display_id, name.is_none());
    }
    Ok(())
}

fn format_ready_message(name: Option<&str>, sandbox_id: &str) -> String {
    match name.filter(|name| !name.is_empty()) {
        Some(name) => format!("Sandbox {name} is ready.\nID: {sandbox_id}\n"),
        None => format!("Sandbox {sandbox_id} is ready.\n"),
    }
}

fn print_post_create_tip(
    ctx: &CliContext,
    create_result: &CreateSandboxResult,
    display_id: &str,
    is_ephemeral: bool,
) {
    let (proxy_url, host_header) = post_create_proxy_base(ctx, create_result, display_id);
    let host_flag = host_header
        .as_deref()
        .map(|h| format!(" \\\n     -H \"Host: {}\"", h))
        .unwrap_or_default();

    eprintln!();
    eprintln!("Get started:");
    eprintln!("  tl sbx ssh {display_id}");
    eprintln!("  tl sbx exec {display_id} -- bash -c \"echo Hello, World!\"");
    if is_ephemeral {
        eprintln!("  tl sbx name {display_id} <name>  # make persistent (enables suspend/resume)");
    }

    let tips: Vec<(&str, String)> = vec![
        (
            "copy files into your sandbox?",
            format!("  tl sbx cp ./myfile.py {display_id}:/tmp/myfile.py"),
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

fn post_create_proxy_base(
    ctx: &CliContext,
    create_result: &CreateSandboxResult,
    display_id: &str,
) -> (String, Option<String>) {
    if let Some(sandbox_url) = create_result.sandbox_url.clone() {
        return (sandbox_url, None);
    }

    if let Some(sandbox_url) = create_result
        .ingress_endpoint
        .as_deref()
        .and_then(|endpoint| sandbox_url_from_ingress_endpoint(endpoint, &create_result.sandbox_id))
    {
        return (sandbox_url, None);
    }

    let proxy_key = if create_result.sandbox_id == display_id {
        create_result.sandbox_id.as_str()
    } else {
        display_id
    };
    sandbox_proxy_base(ctx, proxy_key)
}

fn sandbox_url_from_ingress_endpoint(ingress_endpoint: &str, sandbox_id: &str) -> Option<String> {
    let parsed = url::Url::parse(ingress_endpoint).ok()?;
    let host = parsed.host_str()?;
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    let port = parsed
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    Some(format!(
        "{}://{}.{}{}",
        parsed.scheme(),
        sandbox_id,
        host,
        port
    ))
}

fn build_create_request_body(
    cpus: Option<f64>,
    memory: Option<i64>,
    disk_mb: Option<u64>,
    gpu: Option<GpuRequest<'_>>,
    timeout: Option<i64>,
    entrypoint: &[String],
    snapshot_id: Option<&str>,
    image_name: Option<&str>,
) -> serde_json::Value {
    let mut body = serde_json::json!({});

    if let Some(snapshot_id) = snapshot_id {
        let mut resources = serde_json::Map::new();
        if let Some(cpus) = cpus {
            resources.insert("cpus".to_string(), serde_json::json!(cpus));
        }
        if let Some(memory) = memory {
            resources.insert("memory_mb".to_string(), serde_json::json!(memory));
        }
        if let Some(disk_mb) = disk_mb {
            resources.insert("disk_mb".to_string(), serde_json::json!(disk_mb));
        }
        if let Some(gpu) = gpu {
            resources.insert(
                "gpus".to_string(),
                serde_json::json!([{ "count": gpu.count, "model": gpu.model }]),
            );
        }
        if !resources.is_empty() {
            body["resources"] = serde_json::Value::Object(resources);
        }
        body["snapshot_id"] = serde_json::Value::String(snapshot_id.to_string());
    } else {
        body["resources"] = serde_json::json!({
            "cpus": cpus.unwrap_or(DEFAULT_SANDBOX_CPUS),
            "memory_mb": memory.unwrap_or(DEFAULT_SANDBOX_MEMORY_MB),
        });
        if let Some(disk_mb) = disk_mb {
            body["resources"]["disk_mb"] = serde_json::json!(disk_mb);
        }
        if let Some(gpu) = gpu {
            body["resources"]["gpus"] =
                serde_json::json!([{ "count": gpu.count, "model": gpu.model }]);
        }
    }

    if let Some(t) = timeout {
        body["timeout_secs"] = serde_json::Value::Number(t.into());
    }
    if let Some(image_name) = image_name {
        body["image"] = serde_json::Value::String(image_name.to_string());
    }
    if !entrypoint.is_empty() {
        body["entrypoint"] = serde_json::json!(entrypoint);
    }
    body
}

#[cfg(test)]
mod tests {
    use super::{
        CreateSandboxResult, GpuRequest, build_create_request_body, format_ready_message,
        post_create_proxy_base, sandbox_url_from_ingress_endpoint,
    };
    use crate::auth::context::CliContext;
    use crate::config::resolver::ResolvedConfig;

    fn test_ctx() -> CliContext {
        CliContext::from_resolved(ResolvedConfig {
            api_url: "https://api.tensorlake.ai".to_string(),
            cloud_url: "https://cloud.tensorlake.ai".to_string(),
            namespace: "default".to_string(),
            api_key: None,
            personal_access_token: None,
            organization_id: None,
            project_id: None,
            debug: false,
        })
    }

    #[test]
    fn create_body_uses_defaults_without_snapshot() {
        let body = build_create_request_body(None, None, None, None, None, &[], None, None);

        assert_eq!(body["resources"]["cpus"], 1.0);
        assert_eq!(body["resources"]["memory_mb"], 1024);
        assert!(body["resources"].get("disk_mb").is_none());
        assert!(body.get("snapshot_id").is_none());
    }

    #[test]
    fn create_body_omits_resources_for_snapshot_without_overrides() {
        let body =
            build_create_request_body(None, None, None, None, None, &[], Some("snap-1"), None);

        assert_eq!(body["snapshot_id"], "snap-1");
        assert!(body.get("resources").is_none());
    }

    #[test]
    fn create_body_includes_only_explicit_snapshot_overrides() {
        let body =
            build_create_request_body(Some(2.5), None, None, None, None, &[], Some("snap-1"), None);

        assert_eq!(body["snapshot_id"], "snap-1");
        assert_eq!(body["resources"]["cpus"], 2.5);
        assert!(body["resources"].get("memory_mb").is_none());
    }

    #[test]
    fn create_body_includes_disk_override_without_snapshot() {
        let body =
            build_create_request_body(None, None, Some(25 * 1024), None, None, &[], None, None);

        assert_eq!(body["resources"]["cpus"], 1.0);
        assert_eq!(body["resources"]["memory_mb"], 1024);
        assert_eq!(body["resources"]["disk_mb"], 25 * 1024);
    }

    #[test]
    fn create_body_includes_disk_override_for_snapshot_restore() {
        let body = build_create_request_body(
            None,
            None,
            Some(25 * 1024),
            None,
            None,
            &[],
            Some("snap-1"),
            None,
        );

        assert_eq!(body["snapshot_id"], "snap-1");
        assert_eq!(body["resources"]["disk_mb"], 25 * 1024);
        assert!(body["resources"].get("cpus").is_none());
        assert!(body["resources"].get("memory_mb").is_none());
    }

    #[test]
    fn create_body_passes_image_name_through_to_server() {
        let body = build_create_request_body(
            None,
            None,
            Some(25 * 1024),
            None,
            None,
            &[],
            None,
            Some("tensorlake/ubuntu-minimal"),
        );

        assert_eq!(body["image"], "tensorlake/ubuntu-minimal");
        assert_eq!(body["resources"]["disk_mb"], 25 * 1024);
        assert!(body.get("snapshot_id").is_none());
    }

    #[test]
    fn create_body_includes_gpu_request_without_snapshot() {
        let body = build_create_request_body(
            None,
            None,
            None,
            Some(GpuRequest {
                count: 1,
                model: "A10",
            }),
            None,
            &[],
            None,
            Some("tensorlake/ubuntu-minimal"),
        );

        assert_eq!(body["resources"]["gpus"][0]["count"], 1);
        assert_eq!(body["resources"]["gpus"][0]["model"], "A10");
    }

    #[test]
    fn create_body_includes_gpu_request_for_snapshot_restore() {
        let body = build_create_request_body(
            None,
            None,
            None,
            Some(GpuRequest {
                count: 1,
                model: "A10",
            }),
            None,
            &[],
            Some("snap-1"),
            None,
        );

        assert_eq!(body["snapshot_id"], "snap-1");
        assert_eq!(body["resources"]["gpus"][0]["count"], 1);
        assert_eq!(body["resources"]["gpus"][0]["model"], "A10");
        assert!(body["resources"].get("cpus").is_none());
    }

    #[test]
    fn ready_message_includes_labeled_id_for_named_sandbox() {
        let output = format_ready_message(Some("stable-name"), "sbx-123");

        assert_eq!(output, "Sandbox stable-name is ready.\nID: sbx-123\n");
    }

    #[test]
    fn ready_message_falls_back_to_id_for_unnamed_sandbox() {
        let output = format_ready_message(None, "sbx-123");

        assert_eq!(output, "Sandbox sbx-123 is ready.\n");
    }

    #[test]
    fn create_result_reads_endpoint_fields_from_typed_create_response() {
        let response: CreateSandboxResult = serde_json::from_value(serde_json::json!({
            "sandbox_id": "sbx-123",
            "status": "running",
            "sandbox_url": "https://sbx-123.sandbox.us-east-1.aws.tensorlake.ai/",
            "ingress_endpoint": "https://sandbox.us-east-1.aws.tensorlake.ai/"
        }))
        .unwrap();

        assert_eq!(
            response,
            CreateSandboxResult {
                sandbox_id: "sbx-123".to_string(),
                status: Some("running".to_string()),
                sandbox_url: Some(
                    "https://sbx-123.sandbox.us-east-1.aws.tensorlake.ai/".to_string()
                ),
                ingress_endpoint: Some("https://sandbox.us-east-1.aws.tensorlake.ai/".to_string()),
            }
        );
    }

    #[test]
    fn sandbox_url_is_derived_from_returned_ingress_endpoint() {
        assert_eq!(
            sandbox_url_from_ingress_endpoint(
                "https://sandbox.us-east-1.aws.tensorlake.ai",
                "sbx-123"
            ),
            Some("https://sbx-123.sandbox.us-east-1.aws.tensorlake.ai".to_string())
        );
    }

    #[test]
    fn post_create_tip_prefers_sandbox_url_from_create_response() {
        let ctx = test_ctx();
        let create_result = CreateSandboxResult {
            sandbox_id: "sbx-123".to_string(),
            status: Some("running".to_string()),
            sandbox_url: Some("https://returned.example.com".to_string()),
            ingress_endpoint: Some("https://ingress.example.com".to_string()),
        };

        assert_eq!(
            post_create_proxy_base(&ctx, &create_result, "sbx-123"),
            ("https://returned.example.com".to_string(), None)
        );
    }

    #[test]
    fn post_create_tip_uses_ingress_endpoint_from_create_response() {
        let ctx = test_ctx();
        let create_result = CreateSandboxResult {
            sandbox_id: "sbx-123".to_string(),
            status: Some("running".to_string()),
            sandbox_url: None,
            ingress_endpoint: Some("https://sandbox.us-east-1.aws.tensorlake.ai".to_string()),
        };

        assert_eq!(
            post_create_proxy_base(&ctx, &create_result, "sbx-123"),
            (
                "https://sbx-123.sandbox.us-east-1.aws.tensorlake.ai".to_string(),
                None
            )
        );
    }
}
