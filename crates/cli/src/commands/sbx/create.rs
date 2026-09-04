use crate::auth::context::CliContext;
use crate::commands::sbx::{
    DEFAULT_SANDBOX_WAIT_TIMEOUT, apply_proxy_access_settings, build_network_config,
    sandbox_endpoint, wait_for_sandbox_status,
};
use crate::commands::sbx::fs::is_canonical_source_path;
use crate::error::{CliError, Result};
use serde::Deserialize;
use tensorlake::sandboxes::resolve_sandbox_proxy_target;

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
            "{}",
            format_create_error(status, &body)
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

fn format_create_error(status: reqwest::StatusCode, body: &str) -> String {
    #[derive(Deserialize)]
    struct ServerError<'a> {
        #[serde(default)]
        code: Option<&'a str>,
        message: &'a str,
    }

    if let Ok(error) = serde_json::from_str::<ServerError<'_>>(body)
        && error.code == Some("GPU_REQUIRES_CAS_SNAPSHOT")
    {
        return format!(
            "{}\n\nTo see compatible images, run `tl sbx image ls --cas`.",
            error.message
        );
    }
    if let Ok(error) = serde_json::from_str::<ServerError<'_>>(body) {
        return format!(
            "failed to create sandbox (HTTP {status}): {}",
            error.message
        );
    }
    let body = body.trim();
    if body.is_empty() {
        format!("failed to create sandbox (HTTP {status})")
    } else {
        format!("failed to create sandbox (HTTP {status}): {body}")
    }
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
    /// Boot-time file system mounts, each as
    /// `<name>[@<snapshot_id>]:<mount_path>[:<opts>]`.
    pub file_systems: &'a [String],
}

const FILESYSTEM_FLAG_USAGE: &str = "--filesystem must be <name>[@<snapshot_id>]:<mount_path>[:<opts>] where <opts> \
     is a comma-separated list of `ro`, `prefetch`, and/or `source=<absolute_path>`, and a `@<snapshot_id>` pin requires \
     `ro`";

/// Parse `--filesystem <name>[@<snapshot>]:<path>[:<opts>]` flags into the
/// request `file_systems` array. Everything before the first `:` names the
/// file system, optionally pinned to a snapshot after `@` (`@` cannot appear
/// in file system names and snapshot ids are hex-ish, so the split is
/// unambiguous); the mount path ends at the next `:` (file system names and
/// absolute mount paths never contain one); the optional trailing segment is
/// a comma-separated option list drawn from `ro` and `prefetch`. A snapshot
/// pin requires `ro`: pinned mounts are read-only, and rejecting the combo
/// here mirrors the server's 400 so users fail fast offline. The option and
/// pin keys are added to the wire object only when set: older servers reject
/// unknown mount fields, so an explicit `false` (or a null pin) must never
/// be sent.
fn parse_file_system_mounts(raw: &[String]) -> Result<Vec<serde_json::Value>> {
    raw.iter()
        .map(|entry| {
            let (mount_source, rest) = entry.split_once(':').ok_or_else(|| {
                CliError::usage(format!("{FILESYSTEM_FLAG_USAGE}, got {entry:?}"))
            })?;
            let (file_system_id, snapshot_id) = match mount_source.split_once('@') {
                Some((name, snapshot)) => {
                    if name.is_empty() || snapshot.is_empty() || snapshot.contains('@') {
                        return Err(CliError::usage(format!(
                            "{FILESYSTEM_FLAG_USAGE}, got {entry:?}"
                        )));
                    }
                    (name, Some(snapshot))
                }
                None => (mount_source, None),
            };
            let (mount_path, opts) = match rest.split_once(':') {
                Some((mount_path, opts)) => (mount_path, Some(opts)),
                None => (rest, None),
            };
            if file_system_id.is_empty() || mount_path.is_empty() {
                return Err(CliError::usage(format!(
                    "{FILESYSTEM_FLAG_USAGE}, got {entry:?}"
                )));
            }
            let mut mount = serde_json::json!({
                "file_system_id": file_system_id,
                "mount_path": mount_path,
            });
            let mut read_only = false;
            let mut source_path = None;
            if let Some(opts) = opts {
                for opt in opts.split(',') {
                    match opt {
                        "ro" => {
                            read_only = true;
                            mount["read_only"] = serde_json::Value::Bool(true);
                        }
                        "prefetch" => mount["prefetch"] = serde_json::Value::Bool(true),
                        _ if opt.starts_with("source=") => {
                            let source = &opt["source=".len()..];
                            if source_path.is_some() || !is_canonical_source_path(source) {
                                return Err(CliError::usage(format!(
                                    "invalid or duplicate --filesystem source path in {entry:?}: \
                                     source must be a canonical absolute path"
                                )));
                            }
                            source_path = Some(source);
                            if source != "/" {
                                mount["source_path"] =
                                    serde_json::Value::String(source.to_string());
                            }
                        }
                        _ => {
                            return Err(CliError::usage(format!(
                                "unknown --filesystem option {opt:?} in {entry:?}: \
                                 valid options are `ro`, `prefetch`, and `source=<absolute_path>`"
                            )));
                        }
                    }
                }
            }
            if let Some(snapshot_id) = snapshot_id {
                if !read_only {
                    return Err(CliError::usage(format!(
                        "snapshot pin @{snapshot_id} in {entry:?} requires `ro`: \
                         snapshot-pinned mounts are read-only"
                    )));
                }
                mount["snapshot_id"] = serde_json::Value::String(snapshot_id.to_string());
            }
            Ok(mount)
        })
        .collect()
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
        file_systems,
    } = args;

    let gpu = gpu_count.map(|count| GpuRequest {
        count,
        model: gpu_model.unwrap_or("A10"),
    });

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

    if let Some(network) = build_network_config(no_internet, network_allow, network_deny)? {
        body["network"] = serde_json::to_value(network)?;
    }

    let file_system_mounts = parse_file_system_mounts(file_systems)?;
    if !file_system_mounts.is_empty() {
        body["file_systems"] = serde_json::Value::Array(file_system_mounts);
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
        print_post_create_tip(&create_result, display_id, name.is_none());
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
    create_result: &CreateSandboxResult,
    display_id: &str,
    is_ephemeral: bool,
) {
    eprintln!();
    eprintln!("Get started:");
    eprintln!("  tl sbx ssh {display_id}");
    eprintln!("  tl sbx exec {display_id} -- bash -c 'echo Hello, World!'");
    if is_ephemeral {
        eprintln!("  tl sbx name {display_id} <name>  # make persistent (enables suspend/resume)");
    }

    let Some(proxy_target) = post_create_proxy_base(create_result, display_id) else {
        eprintln!();
        eprintln!("Docs: https://docs.tensorlake.ai/sandboxes");
        return;
    };
    let header_flags = proxy_target.header_flags();
    let proxy_url = proxy_target.proxy_url;

    let tips: Vec<(&str, String)> = vec![
        (
            "copy files into your sandbox?",
            format!("  tl sbx cp ./myfile.py {display_id}:/tmp/myfile.py"),
        ),
        (
            "run a process via the HTTP API?",
            format!(
                "  curl -X POST {proxy_url}/api/v1/processes{header_flags} \\\n     -H \"Content-Type: application/json\" \\\n     -d '{{\"command\": \"echo\", \"args\": [\"Hello, World!\"]}}'"
            ),
        ),
        (
            "run a bash script via the HTTP API?",
            format!(
                "  curl -X POST {proxy_url}/api/v1/processes{header_flags} \\\n     -H \"Content-Type: application/json\" \\\n     -d '{{\"command\": \"bash\", \"args\": [\"-c\", \"for i in 1 2 3; do echo Line $i; sleep 1; done\"]}}'"
            ),
        ),
        (
            "follow process output in real-time?",
            format!(
                "  # Start a process:\n  curl -X POST {proxy_url}/api/v1/processes{header_flags} \\\n     -H \"Content-Type: application/json\" \\\n     -d '{{\"command\": \"bash\", \"args\": [\"-c\", \"for i in 1 2 3; do echo Line $i; sleep 1; done\"]}}'\n\n  # Then stream its output (replace <pid> with the returned pid):\n  curl {proxy_url}/api/v1/processes/<pid>/output/follow{header_flags}"
            ),
        ),
        (
            "write files into your sandbox via the HTTP API?",
            format!(
                "  curl -X PUT \"{proxy_url}/api/v1/files?path=/tmp/hello.txt\"{header_flags} \\\n     -H \"Content-Type: application/octet-stream\" \\\n     -d 'Hello from sandbox!'"
            ),
        ),
        (
            "read files from your sandbox via the HTTP API?",
            format!("  curl \"{proxy_url}/api/v1/files?path=/tmp/hello.txt\"{header_flags}"),
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct PostCreateProxyTarget {
    proxy_url: String,
    host_header: Option<String>,
    sandbox_id_header: Option<String>,
}

impl PostCreateProxyTarget {
    fn header_flags(&self) -> String {
        let mut flags = String::new();
        if let Some(host) = self.host_header.as_deref() {
            flags.push_str(&format!(" \\\n     -H \"Host: {host}\""));
        }
        if let Some(sandbox_id) = self.sandbox_id_header.as_deref() {
            flags.push_str(&format!(
                " \\\n     -H \"X-Tensorlake-Sandbox-Id: {sandbox_id}\""
            ));
        }
        flags
    }
}

fn post_create_proxy_base(
    create_result: &CreateSandboxResult,
    display_id: &str,
) -> Option<PostCreateProxyTarget> {
    let explicit_proxy_url = super::explicit_proxy_url_override();
    post_create_proxy_base_with_explicit(create_result, display_id, explicit_proxy_url.as_deref())
}

fn post_create_proxy_base_with_explicit(
    create_result: &CreateSandboxResult,
    display_id: &str,
    explicit_proxy_url: Option<&str>,
) -> Option<PostCreateProxyTarget> {
    let proxy_url = create_result
        .sandbox_url
        .as_deref()
        .or(explicit_proxy_url)?;
    let has_server_sandbox_url = create_result.sandbox_url.is_some();
    let proxy_key = if has_server_sandbox_url || create_result.sandbox_id == display_id {
        create_result.sandbox_id.as_str()
    } else {
        display_id
    };
    let target = resolve_sandbox_proxy_target(proxy_url, proxy_key).ok()?;
    Some(PostCreateProxyTarget {
        proxy_url: target.base_url,
        host_header: target.host_override,
        sandbox_id_header: target.sandbox_id_header,
    })
}

#[allow(clippy::too_many_arguments)]
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
        CreateSandboxResult, GpuRequest, PostCreateProxyTarget, build_create_request_body,
        format_create_error, format_ready_message, parse_file_system_mounts,
        post_create_proxy_base_with_explicit,
    };

    #[test]
    fn gpu_cas_error_adds_catalog_guidance() {
        let message = format_create_error(
            reqwest::StatusCode::BAD_REQUEST,
            r#"{"code":"GPU_REQUIRES_CAS_SNAPSHOT","message":"Image 'base' uses TLSnap."}"#,
        );
        assert!(message.contains("Image 'base' uses TLSnap."));
        assert!(message.contains("tl sbx image ls --cas"));
    }

    #[test]
    fn ordinary_server_error_keeps_status_and_message() {
        assert_eq!(
            format_create_error(
                reqwest::StatusCode::BAD_REQUEST,
                r#"{"code":"OTHER","message":"bad request"}"#,
            ),
            "failed to create sandbox (HTTP 400 Bad Request): bad request"
        );
    }

    #[test]
    fn parse_file_system_mounts_builds_wire_objects() {
        let raw = vec![
            "file_system_abc:/mnt/skills".to_string(),
            "file_system_def:/data".to_string(),
        ];
        let mounts = parse_file_system_mounts(&raw).unwrap();
        assert_eq!(mounts.len(), 2);
        assert_eq!(mounts[0]["file_system_id"], "file_system_abc");
        assert_eq!(mounts[0]["mount_path"], "/mnt/skills");
        assert_eq!(mounts[1]["mount_path"], "/data");
    }

    #[test]
    fn parse_file_system_mounts_rejects_missing_colon() {
        assert!(parse_file_system_mounts(&["file_system_abc".to_string()]).is_err());
    }

    #[test]
    fn parse_file_system_mounts_rejects_empty_sides() {
        assert!(parse_file_system_mounts(&[":/mnt".to_string()]).is_err());
        assert!(parse_file_system_mounts(&["file_system_abc:".to_string()]).is_err());
    }

    #[test]
    fn parse_file_system_mounts_without_opts_omits_mount_modes() {
        let mounts = parse_file_system_mounts(&["skills:/mnt/skills".to_string()]).unwrap();
        assert_eq!(
            mounts[0],
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills",
            })
        );
    }

    #[test]
    fn parse_file_system_mounts_parses_ro_opt() {
        let mounts = parse_file_system_mounts(&["skills:/mnt/skills:ro".to_string()]).unwrap();
        assert_eq!(
            mounts[0],
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills",
                "read_only": true,
            })
        );
    }

    #[test]
    fn parse_file_system_mounts_parses_prefetch_opt() {
        let mounts =
            parse_file_system_mounts(&["skills:/mnt/skills:prefetch".to_string()]).unwrap();
        assert_eq!(
            mounts[0],
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills",
                "prefetch": true,
            })
        );
    }

    #[test]
    fn parse_file_system_mounts_parses_combined_opts() {
        let mounts =
            parse_file_system_mounts(&["skills:/mnt/skills:ro,prefetch".to_string()]).unwrap();
        assert_eq!(
            mounts[0],
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills",
                "read_only": true,
                "prefetch": true,
            })
        );
    }

    #[test]
    fn parse_file_system_mounts_parses_source_path() {
        let mounts = parse_file_system_mounts(&[
            "skills:/mnt/models:source=/shared/models,ro".to_string(),
        ])
        .unwrap();
        assert_eq!(
            mounts[0],
            serde_json::json!({
                "file_system_id": "skills",
                "source_path": "/shared/models",
                "mount_path": "/mnt/models",
                "read_only": true,
            })
        );
    }

    #[test]
    fn parse_file_system_mounts_rejects_relative_or_duplicate_source_path() {
        assert!(
            parse_file_system_mounts(&[
                "skills:/mnt/models:source=shared/models".to_string()
            ])
            .is_err()
        );
        assert!(
            parse_file_system_mounts(&[
                "skills:/mnt/models:source=/shared/../secrets".to_string()
            ])
            .is_err()
        );
        assert!(
            parse_file_system_mounts(&[
                "skills:/mnt/models:source=/one,source=/two".to_string()
            ])
            .is_err()
        );
    }

    #[test]
    fn parse_file_system_mounts_rejects_unknown_opt() {
        let error = parse_file_system_mounts(&["skills:/mnt/skills:rw".to_string()]).unwrap_err();
        assert!(error.to_string().contains("unknown --filesystem option"));
    }

    #[test]
    fn parse_file_system_mounts_rejects_empty_opt() {
        assert!(parse_file_system_mounts(&["skills:/mnt/skills:".to_string()]).is_err());
        assert!(parse_file_system_mounts(&["skills:/mnt/skills:ro,".to_string()]).is_err());
    }

    #[test]
    fn parse_file_system_mounts_parses_snapshot_pin_with_ro() {
        let mounts =
            parse_file_system_mounts(&["skills@0abc123def:/mnt/skills:ro".to_string()]).unwrap();
        assert_eq!(
            mounts[0],
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills",
                "read_only": true,
                "snapshot_id": "0abc123def",
            })
        );
    }

    #[test]
    fn parse_file_system_mounts_parses_snapshot_pin_with_combined_opts() {
        let mounts =
            parse_file_system_mounts(&["skills@0abc123def:/mnt/skills:ro,prefetch".to_string()])
                .unwrap();
        assert_eq!(
            mounts[0],
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills",
                "read_only": true,
                "prefetch": true,
                "snapshot_id": "0abc123def",
            })
        );
    }

    #[test]
    fn parse_file_system_mounts_rejects_snapshot_pin_without_ro() {
        let error =
            parse_file_system_mounts(&["skills@0abc123def:/mnt/skills".to_string()]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("snapshot-pinned mounts are read-only"),
            "unexpected error: {error}"
        );
        let error =
            parse_file_system_mounts(&["skills@0abc123def:/mnt/skills:prefetch".to_string()])
                .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("snapshot-pinned mounts are read-only"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_file_system_mounts_rejects_malformed_snapshot_pin() {
        // A bare or one-sided `@` never names a valid mount source.
        assert!(parse_file_system_mounts(&["@:/mnt/skills:ro".to_string()]).is_err());
        assert!(parse_file_system_mounts(&["@0abc123def:/mnt/skills:ro".to_string()]).is_err());
        assert!(parse_file_system_mounts(&["skills@:/mnt/skills:ro".to_string()]).is_err());
        // `@` cannot appear in file system names or snapshot ids.
        assert!(parse_file_system_mounts(&["skills@0abc@def:/mnt/skills:ro".to_string()]).is_err());
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
    fn post_create_tip_prefers_sandbox_url_from_create_response() {
        let create_result = CreateSandboxResult {
            sandbox_id: "sbx-123".to_string(),
            status: Some("running".to_string()),
            sandbox_url: Some("https://returned.example.com".to_string()),
            ingress_endpoint: Some("https://ingress.example.com".to_string()),
        };

        assert_eq!(
            post_create_proxy_base_with_explicit(&create_result, "sbx-123", None),
            Some(PostCreateProxyTarget {
                proxy_url: "https://returned.example.com".to_string(),
                host_header: None,
                sandbox_id_header: Some("sbx-123".to_string()),
            })
        );
    }

    #[test]
    fn post_create_tip_uses_canonical_id_with_server_sandbox_url_for_named_sandbox() {
        let create_result = CreateSandboxResult {
            sandbox_id: "sbx-123".to_string(),
            status: Some("running".to_string()),
            sandbox_url: Some("https://sbx-123.sandbox.us-east-1.aws.tensorlake.ai".to_string()),
            ingress_endpoint: Some("https://sandbox.us-east-1.aws.tensorlake.ai".to_string()),
        };

        assert_eq!(
            post_create_proxy_base_with_explicit(&create_result, "stable-name", None),
            Some(PostCreateProxyTarget {
                proxy_url: "https://sbx-123.sandbox.us-east-1.aws.tensorlake.ai".to_string(),
                host_header: None,
                sandbox_id_header: Some("sbx-123".to_string()),
            })
        );
    }

    #[test]
    fn post_create_tip_does_not_derive_from_ingress_endpoint() {
        let create_result = CreateSandboxResult {
            sandbox_id: "sbx-123".to_string(),
            status: Some("running".to_string()),
            sandbox_url: None,
            ingress_endpoint: Some("https://sandbox.us-east-1.aws.tensorlake.ai".to_string()),
        };

        assert_eq!(
            post_create_proxy_base_with_explicit(&create_result, "sbx-123", None),
            None
        );
    }

    #[test]
    fn post_create_tip_uses_explicit_override_when_server_url_missing() {
        let create_result = CreateSandboxResult {
            sandbox_id: "sbx-123".to_string(),
            status: Some("running".to_string()),
            sandbox_url: None,
            ingress_endpoint: Some("https://sandbox.us-east-1.aws.tensorlake.ai".to_string()),
        };

        assert_eq!(
            post_create_proxy_base_with_explicit(
                &create_result,
                "sbx-123",
                Some("http://localhost:9443")
            ),
            Some(PostCreateProxyTarget {
                proxy_url: "http://localhost:9443".to_string(),
                host_header: Some("sbx-123.local".to_string()),
                sandbox_id_header: None,
            })
        );
    }
}
