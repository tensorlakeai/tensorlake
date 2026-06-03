use crate::auth::context::CliContext;
use crate::commands::sbx::{
    DEFAULT_SANDBOX_WAIT_TIMEOUT, apply_proxy_access_settings, sandbox_endpoint,
    sandbox_proxy_base, wait_for_sandbox_status,
};
use crate::error::{CliError, Result};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};

const DEFAULT_SANDBOX_CPUS: f64 = 1.0;
const DEFAULT_SANDBOX_MEMORY_MB: i64 = 1024;
const MAX_CLOUD_INIT_USER_DATA_BYTES: usize = 16 * 1024;

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

pub struct CreateArgs<'a> {
    pub name: Option<&'a str>,
    pub cpus: Option<f64>,
    pub memory: Option<i64>,
    pub disk_mb: Option<u64>,
    pub timeout: Option<i64>,
    pub entrypoint: &'a [String],
    pub snapshot_id: Option<&'a str>,
    pub image_name: Option<&'a str>,
    pub cloud_init: Option<&'a str>,
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
        timeout,
        entrypoint,
        snapshot_id,
        image_name,
        cloud_init,
        wait,
        ports,
        allow_unauthenticated_access,
        no_internet,
        network_allow,
        network_deny,
    } = args;

    let cloud_init_base64 = cloud_init.map(encode_cloud_init_source).transpose()?;

    let mut body = build_create_request_body(
        cpus,
        memory,
        disk_mb,
        timeout,
        entrypoint,
        snapshot_id,
        image_name,
        cloud_init_base64.as_deref(),
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

    let sandbox_id = create_with_request(ctx, body, wait).await?;
    let is_tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    let display_id = name.unwrap_or(&sandbox_id);
    if is_tty {
        eprint!("{}", format_ready_message(name, &sandbox_id));
    }
    if !is_tty {
        println!("{}", sandbox_id);
    }
    if is_tty {
        print_post_create_tip(ctx, &sandbox_id, display_id, name.is_none());
    }
    Ok(())
}

fn format_ready_message(name: Option<&str>, sandbox_id: &str) -> String {
    match name.filter(|name| !name.is_empty()) {
        Some(name) => format!("Sandbox {name} is ready.\nID: {sandbox_id}\n"),
        None => format!("Sandbox {sandbox_id} is ready.\n"),
    }
}

fn print_post_create_tip(ctx: &CliContext, sandbox_id: &str, display_id: &str, is_ephemeral: bool) {
    // Use the name as the proxy subdomain when available; it's a stable human-readable identifier.
    let proxy_key = if is_ephemeral { sandbox_id } else { display_id };
    let (proxy_url, host_header) = sandbox_proxy_base(ctx, proxy_key);
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

fn build_create_request_body(
    cpus: Option<f64>,
    memory: Option<i64>,
    disk_mb: Option<u64>,
    timeout: Option<i64>,
    entrypoint: &[String],
    snapshot_id: Option<&str>,
    image_name: Option<&str>,
    cloud_init_base64: Option<&str>,
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
    if let Some(user_data) = cloud_init_base64 {
        body["cloud_init_base64"] = serde_json::Value::String(user_data.to_string());
    }

    body
}

fn cloud_init_include_data(source: &str) -> Result<Option<Vec<u8>>> {
    match url::Url::parse(source) {
        Ok(url) if matches!(url.scheme(), "http" | "https") && url.host_str().is_some() => {
            Ok(Some(format!("#include\n{source}\n").into_bytes()))
        }
        Ok(_) => Err(CliError::usage(
            "cloud-init URL must be an absolute HTTP(S) URL with a host",
        )),
        Err(_) if source.contains("://") => Err(CliError::usage(
            "cloud-init URL must be an absolute HTTP(S) URL with a host",
        )),
        Err(_) => Ok(None),
    }
}

fn encode_cloud_init_source(source: &str) -> Result<String> {
    let data = if let Some(data) = cloud_init_include_data(source)? {
        data
    } else {
        std::fs::read(source).map_err(|error| {
            CliError::Other(anyhow::anyhow!(
                "failed to read cloud-init file {}: {}",
                source,
                error
            ))
        })?
    };

    if data.is_empty() {
        return Err(CliError::usage("cloud-init user data must not be empty"));
    }
    if data.len() > MAX_CLOUD_INIT_USER_DATA_BYTES {
        return Err(CliError::usage(format!(
            "cloud-init user data exceeds {} byte limit",
            MAX_CLOUD_INIT_USER_DATA_BYTES
        )));
    }
    Ok(BASE64_STANDARD.encode(data))
}

#[cfg(test)]
mod tests {
    use super::{build_create_request_body, encode_cloud_init_source, format_ready_message};

    #[test]
    fn create_body_uses_defaults_without_snapshot() {
        let body = build_create_request_body(None, None, None, None, &[], None, None, None);

        assert_eq!(body["resources"]["cpus"], 1.0);
        assert_eq!(body["resources"]["memory_mb"], 1024);
        assert!(body["resources"].get("disk_mb").is_none());
        assert!(body.get("snapshot_id").is_none());
    }

    #[test]
    fn create_body_omits_resources_for_snapshot_without_overrides() {
        let body =
            build_create_request_body(None, None, None, None, &[], Some("snap-1"), None, None);

        assert_eq!(body["snapshot_id"], "snap-1");
        assert!(body.get("resources").is_none());
    }

    #[test]
    fn create_body_includes_only_explicit_snapshot_overrides() {
        let body =
            build_create_request_body(Some(2.5), None, None, None, &[], Some("snap-1"), None, None);

        assert_eq!(body["snapshot_id"], "snap-1");
        assert_eq!(body["resources"]["cpus"], 2.5);
        assert!(body["resources"].get("memory_mb").is_none());
    }

    #[test]
    fn create_body_includes_disk_override_without_snapshot() {
        let body =
            build_create_request_body(None, None, Some(25 * 1024), None, &[], None, None, None);

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
            &[],
            Some("snap-1"),
            None,
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
            &[],
            None,
            Some("tensorlake/ubuntu-minimal"),
            None,
        );

        assert_eq!(body["image"], "tensorlake/ubuntu-minimal");
        assert_eq!(body["resources"]["disk_mb"], 25 * 1024);
        assert!(body.get("snapshot_id").is_none());
    }

    #[test]
    fn create_body_includes_cloud_init_user_data_base64() {
        let body = build_create_request_body(
            None,
            None,
            None,
            None,
            &[],
            None,
            Some("tensorlake/cloud-init"),
            Some("I2Nsb3VkLWNvbmZpZwo="),
        );

        assert_eq!(body["cloud_init_base64"], "I2Nsb3VkLWNvbmZpZwo=");
    }

    #[test]
    fn cloud_init_rejects_invalid_url() {
        let err = encode_cloud_init_source("ftp://example.com/cloud-init.yaml")
            .expect_err("unsupported URL scheme should fail");

        assert!(err.to_string().contains("HTTP(S) URL"));
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
}
