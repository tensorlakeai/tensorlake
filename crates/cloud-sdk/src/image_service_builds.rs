//! CAS image builds through the Image Service build plane.
//!
//! The SDK owns Dockerfile parsing and reference resolution: the build
//! plan's external references are resolved against the Image Service
//! catalog here, client-side, and submitted already resolved with the build
//! request. The Image Service validates each pin against the caller's
//! project and its reconciler drives the builder sandbox, mounting one
//! read-only parent volume per pin beside the writable target volume and
//! snapshotting the target through the CAS filesystem daemon. The client
//! only creates the build, uploads and seals the context, and polls the
//! build to completion; it never talks to the builder sandbox.

use std::{path::Path, time::Duration};

use reqwest::{Method, StatusCode, header::CONTENT_LENGTH};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    Client,
    sandbox_images::{
        CommonBuildOptions, DockerfileBuildPlan, SandboxImageBuildError, SandboxImageBuildEvent,
        client_builder, collect_dir_files, resolve_build_context, resolved_docker_config_json,
    },
};

type Result<T> = std::result::Result<T, SandboxImageBuildError>;

/// Base URL of the Image Service. Until the platform gateway routes to the
/// service, CAS builds require this to be set explicitly.
pub const IMAGE_SERVICE_URL_ENV: &str = "TENSORLAKE_IMAGE_SERVICE_URL";

/// Ingress route that fronts the Image Service. The ingress authenticates,
/// replaces any client-supplied identity headers with verified ones, and maps
/// this prefix onto the service's own routes -- so the paths below are
/// prefix-relative (`/builds`, not `/v1/builds`).
const IMAGE_SERVICE_INGRESS_PATH: &str = "/images/v4";

/// Immutable image reference prefix (`cas-v1:<64 hex sha256>`). A Dockerfile
/// reference written in this form pins the image id directly, with no
/// catalog name lookup.
const CAS_IMAGE_REF_PREFIX: &str = "cas-v1:";

/// Where the Dockerfile text is injected into the context tar when the
/// Dockerfile does not live inside the context directory (`-f` outside the
/// context, or inline Dockerfile text).
const INJECTED_DOCKERFILE_PATH: &str = ".tensorlake/Dockerfile";

/// The Image Service caps pinned parents so they always fit the sandbox
/// volume budget beside the target volume. Enforced client-side too for a
/// clearer error than a rejected build request.
const MAX_PINNED_PARENTS: usize = 3;

const BUILD_POLL_INTERVAL: Duration = Duration::from_secs(3);
const BUILD_POLL_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Run a build plan through the Image Service. `dockerfile_path` is the
/// local Dockerfile path for Dockerfile builds (used only to decide whether
/// the file already lives inside the context directory); import plans carry
/// no Dockerfile.
pub(crate) async fn run_image_service_build<F>(
    plan: DockerfileBuildPlan,
    dockerfile_path: Option<&Path>,
    build_args: Vec<(String, String)>,
    options: CommonBuildOptions,
    mut emit: F,
) -> Result<Value>
where
    F: FnMut(SandboxImageBuildEvent),
{
    warn_ignored_options(&options, &mut emit);
    // Forward the local `docker login` state for the guest's registry pulls,
    // the same source the legacy path ships into its builder. The service
    // stages it write-only and the reconciler hands the guest a short-lived
    // fetch URL; the document never enters the catalog.
    let registry_credentials_json = resolved_docker_config_json().await?;
    if registry_credentials_json.is_some() {
        emit(SandboxImageBuildEvent::Status(
            "Forwarding local registry credentials to the build".to_string(),
        ));
    }
    let ctx = resolve_build_context(options).await?;
    let project = ctx.project_id.clone();
    let image_service_url = image_service_url(&ctx.api_url);
    let client = client_builder(
        &image_service_url,
        &ctx.bearer_token,
        ctx.use_scope_headers,
        Some(&ctx.organization_id),
        Some(&project),
        ctx.user_agent.as_deref(),
    )
    .build()?;

    if let Some(import_reference) = plan.import_image_reference.clone() {
        let created = create_build(
            &client,
            with_registry_credentials(
                json!({
                    "kind": "import",
                    "image_ref": import_reference,
                    "name": plan.registered_name,
                    "project": project,
                }),
                registry_credentials_json,
            ),
            &mut emit,
        )
        .await?;
        return wait_for_publication(&client, &project, &created, &plan.registered_name, emit)
            .await;
    }

    let parents = resolve_parents(&client, &project, &plan, &mut emit).await?;
    let (dockerfile_in_context, injected_dockerfile) = context_dockerfile(&plan, dockerfile_path)?;
    let created = create_build(
        &client,
        with_registry_credentials(
            json!({
                "kind": "dockerfile",
                "dockerfile_path": dockerfile_in_context,
                "parents": parents,
                "build_args": build_args
                    .iter()
                    .cloned()
                    .collect::<std::collections::BTreeMap<String, String>>(),
                "name": plan.registered_name,
                "project": project,
            }),
            registry_credentials_json,
        ),
        &mut emit,
    )
    .await?;
    let build_id = build_id_of(&created)?;

    let upload = created.get("context_upload").cloned().ok_or_else(|| {
        SandboxImageBuildError::other(
            "Image Service build creation returned no context upload capability",
        )
    })?;
    upload_and_seal_context(
        &client,
        &project,
        &build_id,
        &upload,
        &plan,
        injected_dockerfile.as_deref(),
        &mut emit,
    )
    .await?;

    wait_for_publication(&client, &project, &created, &plan.registered_name, emit).await
}

/// Base URL for the Image Service.
///
/// Defaults to the ingress route on the configured API host, so a normal
/// client needs no extra configuration: `https://api.tensorlake.ai` yields
/// `https://api.tensorlake.ai/images/v4`, and the ingress authenticates the
/// request, injects verified identity headers, and strips the prefix before
/// the Image Service sees it. The service performs no authentication of its
/// own, so reaching it any other way in a deployed environment would bypass
/// the only auth boundary there is.
///
/// `TENSORLAKE_IMAGE_SERVICE_URL` overrides it for pointing at a local
/// Image Service directly during development.
fn image_service_url(api_url: &str) -> String {
    resolve_image_service_url(api_url, std::env::var(IMAGE_SERVICE_URL_ENV).ok())
}

/// Pure form, so the defaulting rule is testable without mutating process
/// environment (which races across parallel tests).
fn resolve_image_service_url(api_url: &str, override_url: Option<String>) -> String {
    if let Some(override_url) = override_url
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
    {
        return override_url;
    }
    format!(
        "{}{IMAGE_SERVICE_INGRESS_PATH}",
        api_url.trim_end_matches('/')
    )
}

/// Attach the forwarded docker config to a build-creation body. The field is
/// write-only on the service side; it is never echoed back.
fn with_registry_credentials(mut body: Value, registry_credentials_json: Option<String>) -> Value {
    if let (Some(credentials), Some(object)) = (registry_credentials_json, body.as_object_mut()) {
        object.insert(
            "registry_credentials_json".to_string(),
            Value::String(credentials),
        );
    }
    body
}

/// Builder resource and visibility knobs belong to the legacy platform-api
/// rootfs builder; the Image Service reconciler sizes builder sandboxes from
/// service configuration and the catalog has no public/private bit yet.
fn warn_ignored_options(
    options: &CommonBuildOptions,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) {
    let mut ignored = Vec::new();
    if options.disk_mb.is_some() {
        ignored.push("disk_mb");
    }
    if options.builder_disk_mb.is_some() {
        ignored.push("builder_disk_mb");
    }
    if options.cpus.is_some() {
        ignored.push("cpus");
    }
    if options.memory_mb.is_some() {
        ignored.push("memory_mb");
    }
    if options.is_public {
        ignored.push("public");
    }
    if options.docker_compat {
        ignored.push("docker_compat");
    }
    if !ignored.is_empty() {
        emit(SandboxImageBuildEvent::Warning(format!(
            "Image Service builds ignore: {}. Builder resources come from service configuration.",
            ignored.join(", ")
        )));
    }
}

/// Resolve the plan's external references against the Image Service catalog
/// and return the resolved parents the build request submits. A
/// `cas-v1:<64 hex>` reference pins the image id directly; every other
/// plain reference is looked up as a catalog name, and a miss means the
/// guest pulls the reference from a registry. References the plan already
/// marked unresolvable (`$` expansions, `@` digest pins) never reach here.
async fn resolve_parents(
    client: &Client,
    project: &str,
    plan: &DockerfileBuildPlan,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<Vec<Value>> {
    let mut candidates: Vec<&str> = Vec::new();
    if !plan.base_image_is_internal_stage
        && !plan.base_image.eq_ignore_ascii_case("scratch")
        && !plan.base_image.contains('$')
        && !plan.base_image.contains('@')
    {
        candidates.push(plan.base_image.as_str());
    }
    candidates.extend(plan.additional_image_references.iter().map(String::as_str));

    let mut parents = Vec::new();
    let mut pinned_references = Vec::new();
    for reference in candidates {
        let image_id = match direct_pin_image_id(reference)? {
            Some(image_id) => Some(image_id),
            None => lookup_catalog_name(client, project, reference).await?,
        };
        let Some(image_id) = image_id else {
            continue; // external registry reference, pulled by the guest
        };
        emit(SandboxImageBuildEvent::Status(format!(
            "Resolved '{reference}' to registered image {CAS_IMAGE_REF_PREFIX}{image_id}"
        )));
        pinned_references.push(reference.to_string());
        parents.push(json!({ "reference": reference, "image_id": image_id }));
    }
    if parents.len() > MAX_PINNED_PARENTS {
        return Err(SandboxImageBuildError::usage(format!(
            "the Dockerfile references {} registered images ({}) but a build may pin at most \
             {MAX_PINNED_PARENTS}",
            parents.len(),
            pinned_references.join(", "),
        )));
    }
    Ok(parents)
}

/// `cas-v1:<64 lowercase hex>` pins an image id directly. The prefix with a
/// malformed id is an error rather than a registry fallthrough: no registry
/// image can live under the reserved scheme.
fn direct_pin_image_id(reference: &str) -> Result<Option<String>> {
    let Some(image_id) = reference.strip_prefix(CAS_IMAGE_REF_PREFIX) else {
        return Ok(None);
    };
    if image_id.len() != 64
        || !image_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(SandboxImageBuildError::usage(format!(
            "reference '{reference}' uses the reserved {CAS_IMAGE_REF_PREFIX} scheme but is not \
             a 64-lowercase-hex image id"
        )));
    }
    Ok(Some(image_id.to_string()))
}

/// Look up a reference as a catalog name in the caller's project. `404` and
/// `400` both mean "not a registered image" (absent, or a reference shape
/// the name grammar rejects) and fall through to a registry pull, matching
/// how the legacy path treats template-registry misses.
async fn lookup_catalog_name(
    client: &Client,
    project: &str,
    reference: &str,
) -> Result<Option<String>> {
    let path = format!("/names/{reference}?project={}", urlencoding_encode(project));
    let request = client.request(Method::GET, &path).build()?;
    let response = client.execute_raw(request).await?;
    match response.status() {
        StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST => Ok(None),
        status if status.is_success() => {
            let body: Value = response.json().await?;
            body.get("image_id")
                .and_then(Value::as_str)
                .map(|image_id| Some(image_id.to_string()))
                .ok_or_else(|| {
                    SandboxImageBuildError::other(format!(
                        "Image Service name lookup for '{reference}' returned no image_id"
                    ))
                })
        }
        status => {
            let body = response.text().await.unwrap_or_default();
            Err(SandboxImageBuildError::other(format!(
                "Image Service name lookup for '{reference}' failed (HTTP {status}): {body}"
            )))
        }
    }
}

/// Minimal query-value encoding; project identifiers are constrained but
/// encode defensively.
fn urlencoding_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// Decide where the Dockerfile lives in the uploaded context: its own
/// relative path when the file sits inside the context directory, or an
/// injected `.tensorlake/Dockerfile` entry carrying the plan's Dockerfile
/// text otherwise (`-f` outside the context, inline text).
fn context_dockerfile(
    plan: &DockerfileBuildPlan,
    dockerfile_path: Option<&Path>,
) -> Result<(String, Option<String>)> {
    if let Some(dockerfile_path) = dockerfile_path
        && let (Ok(dockerfile), Ok(context_dir)) = (
            dockerfile_path.canonicalize(),
            plan.context_dir.canonicalize(),
        )
        && let Ok(relative) = dockerfile.strip_prefix(&context_dir)
    {
        let relative = relative
            .components()
            .map(|component| component.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        if !relative.is_empty() {
            return Ok((relative, None));
        }
    }
    Ok((
        INJECTED_DOCKERFILE_PATH.to_string(),
        Some(plan.dockerfile_text.clone()),
    ))
}

fn build_id_of(build: &Value) -> Result<String> {
    build
        .get("build_id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            SandboxImageBuildError::other("Image Service build response carries no build_id")
        })
}

async fn create_build(
    client: &Client,
    body: Value,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<Value> {
    emit(SandboxImageBuildEvent::Status(
        "Creating Image Service build...".to_string(),
    ));
    let request = client
        .request(Method::POST, "/builds")
        .json(&body)
        .build()?;
    let response = client.execute_raw(request).await?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(SandboxImageBuildError::other(format!(
            "Image Service rejected the build (HTTP {status}): {body}"
        )));
    }
    let created: Value = response.json().await?;
    emit(SandboxImageBuildEvent::Status(format!(
        "Build {} accepted",
        created
            .get("build_id")
            .and_then(Value::as_str)
            .unwrap_or("-")
    )));
    Ok(created)
}

/// Create the plain (uncompressed) context tar, honoring `.dockerignore`,
/// upload it through the sealed capability, and seal the build with the
/// tar's sha256. The capability URL is presigned (or the service's direct
/// dev route); the upload deliberately uses a bare HTTP client so no
/// Authorization header corrupts a presigned request.
async fn upload_and_seal_context(
    client: &Client,
    project: &str,
    build_id: &str,
    upload: &Value,
    plan: &DockerfileBuildPlan,
    injected_dockerfile: Option<&str>,
    emit: &mut impl FnMut(SandboxImageBuildEvent),
) -> Result<()> {
    emit(SandboxImageBuildEvent::Status(
        "Creating build context archive...".to_string(),
    ));
    let tar_file = tempfile::Builder::new()
        .prefix("tensorlake-image-context-")
        .suffix(".tar")
        .tempfile()?;
    let (tar_bytes, digest) =
        create_context_tar(&plan.context_dir, injected_dockerfile, tar_file.path())?;

    let max_bytes = upload.get("max_bytes").and_then(Value::as_u64);
    if let Some(max_bytes) = max_bytes
        && tar_bytes > max_bytes
    {
        return Err(SandboxImageBuildError::usage(format!(
            "build context is {tar_bytes} bytes, above the Image Service cap of {max_bytes}"
        )));
    }

    let url = upload
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| SandboxImageBuildError::other("context upload capability carries no url"))?;
    let method = upload
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("PUT")
        .parse::<Method>()
        .map_err(|_| {
            SandboxImageBuildError::other("context upload capability method is invalid")
        })?;

    emit(SandboxImageBuildEvent::Status(format!(
        "Uploading build context ({tar_bytes} bytes)..."
    )));
    let file = tokio::fs::File::open(tar_file.path()).await?;
    let stream = tokio_util::io::ReaderStream::new(file);
    let response = reqwest::Client::new()
        .request(method, url)
        .header(CONTENT_LENGTH, tar_bytes)
        .body(reqwest::Body::wrap_stream(stream))
        .send()
        .await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(SandboxImageBuildError::other(format!(
            "context upload failed (HTTP {status}): {body}"
        )));
    }

    emit(SandboxImageBuildEvent::Status(
        "Sealing build context...".to_string(),
    ));
    let request = client
        .request(Method::POST, &format!("/builds/{build_id}/context/seal"))
        .json(&json!({ "project": project, "digest": digest }))
        .build()?;
    let response = client.execute_raw(request).await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(SandboxImageBuildError::other(format!(
            "context seal failed (HTTP {status}): {body}"
        )));
    }
    Ok(())
}

/// Write the plain context tar to `tar_path` and return its byte length and
/// sha256. Entries honor `.dockerignore` via the shared context walker; the
/// optional injected Dockerfile is appended last and collides with nothing
/// because its reserved path is rejected if present in the context.
fn create_context_tar(
    context_dir: &Path,
    injected_dockerfile: Option<&str>,
    tar_path: &Path,
) -> Result<(u64, String)> {
    let mut tar = tar::Builder::new(std::fs::File::create(tar_path)?);
    if context_dir.is_dir() {
        for (full_path, relative_path) in collect_dir_files(context_dir, context_dir)? {
            if injected_dockerfile.is_some() && relative_path == INJECTED_DOCKERFILE_PATH {
                return Err(SandboxImageBuildError::usage(format!(
                    "the build context already contains {INJECTED_DOCKERFILE_PATH}, which is \
                     reserved for the injected Dockerfile"
                )));
            }
            let mut file = std::fs::File::open(&full_path)?;
            tar.append_file(&relative_path, &mut file)?;
        }
    }
    if let Some(dockerfile_text) = injected_dockerfile {
        let mut header = tar::Header::new_gnu();
        header.set_size(dockerfile_text.len() as u64);
        header.set_mode(0o644);
        header.set_mtime(0);
        header.set_cksum();
        tar.append_data(
            &mut header,
            INJECTED_DOCKERFILE_PATH,
            dockerfile_text.as_bytes(),
        )?;
    }
    tar.finish()?;
    drop(tar);

    let mut file = std::fs::File::open(tar_path)?;
    let mut hasher = Sha256::new();
    let bytes = std::io::copy(&mut file, &mut hasher)?;
    Ok((bytes, format!("{:x}", hasher.finalize())))
}

/// Poll the build to a terminal status, then fetch and return the published
/// image record.
async fn wait_for_publication(
    client: &Client,
    project: &str,
    created: &Value,
    registered_name: &str,
    mut emit: impl FnMut(SandboxImageBuildEvent),
) -> Result<Value> {
    let build_id = build_id_of(created)?;
    let path = format!("/builds/{build_id}?project={}", urlencoding_encode(project));
    let deadline = tokio::time::Instant::now() + BUILD_POLL_TIMEOUT;
    let mut last_reported = String::new();
    let image_id = loop {
        if tokio::time::Instant::now() > deadline {
            return Err(SandboxImageBuildError::other(format!(
                "build {build_id} did not finish within {}s",
                BUILD_POLL_TIMEOUT.as_secs()
            )));
        }
        let request = client.request(Method::GET, &path).build()?;
        let response = client.execute_raw(request).await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(SandboxImageBuildError::other(format!(
                "build status poll failed (HTTP {status}): {body}"
            )));
        }
        let build: Value = response.json().await?;
        let status = build
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let attempt = build.get("attempt_no").and_then(Value::as_u64).unwrap_or(0);
        let progress = format!("{status}/{attempt}");
        if progress != last_reported {
            emit(SandboxImageBuildEvent::Status(format!(
                "Build {build_id}: {status} (attempt {attempt})"
            )));
            last_reported = progress;
        }
        match status.as_str() {
            "succeeded" => {
                break build
                    .get("image_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        SandboxImageBuildError::other(format!(
                            "build {build_id} succeeded but reports no image_id"
                        ))
                    })?;
            }
            "failed" => {
                let error = build
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("no error recorded");
                return Err(SandboxImageBuildError::other(format!(
                    "build {build_id} failed: {error}"
                )));
            }
            _ => tokio::time::sleep(BUILD_POLL_INTERVAL).await,
        }
    };

    let request = client
        .request(
            Method::GET,
            &format!(
                "/v1/images/{image_id}?project={}",
                urlencoding_encode(project)
            ),
        )
        .build()?;
    let response = client.execute_raw(request).await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(SandboxImageBuildError::other(format!(
            "published image lookup failed (HTTP {status}): {body}"
        )));
    }
    let image: Value = response.json().await?;
    emit(SandboxImageBuildEvent::Status(format!(
        "Image '{registered_name}' published ({CAS_IMAGE_REF_PREFIX}{image_id})"
    )));
    Ok(image)
}

#[cfg(test)]
mod url_tests {
    use super::*;

    /// A normal client needs no configuration: the Image Service is reached
    /// through the API host's ingress path, which is the only place requests
    /// are authenticated.
    #[test]
    fn defaults_to_the_ingress_path_on_the_api_host() {
        assert_eq!(
            resolve_image_service_url("https://api.tensorlake.ai", None),
            "https://api.tensorlake.ai/images/v4"
        );
        // A trailing slash on the API URL must not double up.
        assert_eq!(
            resolve_image_service_url("https://api.tensorlake.ai/", None),
            "https://api.tensorlake.ai/images/v4"
        );
    }

    #[test]
    fn the_override_wins_and_is_trimmed() {
        assert_eq!(
            resolve_image_service_url(
                "https://api.tensorlake.ai",
                Some("  http://127.0.0.1:8843/  ".to_string())
            ),
            "http://127.0.0.1:8843"
        );
    }

    /// An empty or whitespace override is ignored rather than producing a
    /// client with an empty base URL.
    #[test]
    fn a_blank_override_falls_back_to_the_default() {
        for blank in ["", "   "] {
            assert_eq!(
                resolve_image_service_url("https://api.tensorlake.ai", Some(blank.to_string())),
                "https://api.tensorlake.ai/images/v4"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;

    use super::*;

    #[test]
    fn registry_credentials_attach_only_when_present() {
        let body = with_registry_credentials(
            serde_json::json!({ "kind": "import" }),
            Some(r#"{"auths":{}}"#.to_string()),
        );
        assert_eq!(body["registry_credentials_json"], r#"{"auths":{}}"#);
        let body = with_registry_credentials(serde_json::json!({ "kind": "import" }), None);
        assert!(body.get("registry_credentials_json").is_none());
    }

    #[test]
    fn direct_pins_accept_only_lowercase_hex_ids() {
        let hex = "a".repeat(64);
        assert_eq!(
            direct_pin_image_id(&format!("cas-v1:{hex}")).unwrap(),
            Some(hex)
        );
        assert_eq!(direct_pin_image_id("alpine:3.20").unwrap(), None);
        assert_eq!(direct_pin_image_id("team/base").unwrap(), None);
        assert!(direct_pin_image_id("cas-v1:NOT-HEX").is_err());
        assert!(direct_pin_image_id(&format!("cas-v1:{}", "a".repeat(63))).is_err());
    }

    #[test]
    fn context_dockerfile_uses_the_relative_path_inside_the_context() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("build");
        std::fs::create_dir(&nested).unwrap();
        let dockerfile = nested.join("prod.Dockerfile");
        std::fs::write(&dockerfile, "FROM scratch\n").unwrap();

        let plan = plan_with_context(dir.path().to_path_buf());
        let (path, injected) = context_dockerfile(&plan, Some(&dockerfile)).unwrap();
        assert_eq!(path, "build/prod.Dockerfile");
        assert!(injected.is_none());
    }

    #[test]
    fn context_dockerfile_injects_text_when_outside_the_context() {
        let context = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let dockerfile = elsewhere.path().join("Dockerfile");
        std::fs::write(&dockerfile, "FROM scratch\n").unwrap();

        let plan = plan_with_context(context.path().to_path_buf());
        let (path, injected) = context_dockerfile(&plan, Some(&dockerfile)).unwrap();
        assert_eq!(path, INJECTED_DOCKERFILE_PATH);
        assert_eq!(injected.as_deref(), Some("FROM scratch\n"));
    }

    #[test]
    fn context_tar_is_plain_and_carries_the_injected_dockerfile() {
        let dir = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("app.py"), "print('hi')\n").unwrap();

        let tar_path = scratch.path().join("out.tar");
        let (bytes, digest) =
            create_context_tar(dir.path(), Some("FROM scratch\n"), &tar_path).unwrap();
        assert_eq!(bytes, std::fs::metadata(&tar_path).unwrap().len());
        assert_eq!(digest.len(), 64);

        let mut names = Vec::new();
        let mut archive = tar::Archive::new(std::fs::File::open(&tar_path).unwrap());
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let name = entry.path().unwrap().to_string_lossy().to_string();
            if name == INJECTED_DOCKERFILE_PATH {
                let mut text = String::new();
                entry.read_to_string(&mut text).unwrap();
                assert_eq!(text, "FROM scratch\n");
            }
            names.push(name);
        }
        names.sort();
        assert_eq!(
            names,
            vec![INJECTED_DOCKERFILE_PATH.to_string(), "app.py".to_string()]
        );
    }

    #[test]
    fn context_tar_rejects_a_colliding_reserved_dockerfile_entry() {
        let dir = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join(".tensorlake")).unwrap();
        std::fs::write(dir.path().join(INJECTED_DOCKERFILE_PATH), "FROM x\n").unwrap();

        let tar_path = scratch.path().join("out.tar");
        let error = create_context_tar(dir.path(), Some("FROM scratch\n"), &tar_path)
            .expect_err("reserved path must collide");
        assert!(
            error.to_string().contains(".tensorlake/Dockerfile"),
            "{error}"
        );
    }

    fn plan_with_context(context_dir: std::path::PathBuf) -> DockerfileBuildPlan {
        DockerfileBuildPlan {
            context_dir,
            registered_name: "img".to_string(),
            dockerfile_text: "FROM scratch\n".to_string(),
            base_image: "scratch".to_string(),
            base_image_is_internal_stage: false,
            additional_image_references: Vec::new(),
            unresolvable_image_references: Vec::new(),
            ignored_instructions: Vec::new(),
            import_image_reference: None,
        }
    }
}
