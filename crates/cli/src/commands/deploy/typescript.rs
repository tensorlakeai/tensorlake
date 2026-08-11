use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::distr::{Alphanumeric, SampleString};
use rolldown::plugin::{
    HookLoadArgs, HookLoadOutput, HookLoadReturn, HookNoopReturn, HookResolveIdArgs,
    HookResolveIdOutput, HookResolveIdReturn, HookUsage, Plugin, PluginContext,
    SharedLoadPluginContext,
};
use rolldown::{
    Bundler, BundlerOptions, BundlerTransformOptions, ChunkFilenamesOutputOption, Either,
    InputItem, OutputFormat, Platform,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tensorlake::sandbox_images::{
    CommonBuildOptions, SandboxImageBuildOptions, SandboxImageContextFile, build_sandbox_image,
};
use tokio::process::Command;
use url::Url;
use zip::CompressionMethod;
use zip::write::FileOptions;

use crate::auth::context::CliContext;
use crate::commands::sbx::image::ImageBuildEventRenderer;
use crate::error::{CliError, Result};

const VIRTUAL_ENTRY: &str = "tensorlake:application-entry";
const RESOLVED_VIRTUAL_ENTRY: &str = "\0tensorlake:application-entry";
const RUNTIME_MODULE: &str = "runtime.mjs";
const CODE_MANIFEST_FILE: &str = ".tensorlake_code_manifest.json";
const MAX_CODE_SIZE: u64 = 5 * 1024 * 1024;
const DEFAULT_NODE_IMAGE: &str = "node:24-bookworm-slim";
const CAS_IMAGE_REF_PREFIX: &str = "cas-v1:";
const FUNCTION_RUNNER_CAPSULE_CONTEXT_PATH: &str =
    ".tensorlake/typescript-function-runner-runtime.tgz";
const FUNCTION_RUNNER_LINUX_X64_BINDING: &str =
    "package/dist/native/linux-x64/tensorlake-node.node";
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(30);
const UNAUTHENTICATED_REQUESTS: &str = "unauthenticated_requests";
const DISCOVERY_SCRIPT: &str = r#"
import { writeFile } from "node:fs/promises";

const runtimeUrl = process.argv[1];
const outputPath = process.argv[2];
const runtime = await import(runtimeUrl);
if (typeof runtime.__tensorlakeDeployment !== "function") {
  throw new Error("Tensorlake runtime does not export __tensorlakeDeployment");
}
const deployment = await runtime.__tensorlakeDeployment();
await writeFile(outputPath, JSON.stringify(deployment));
// Discovery is complete once the metadata is durable. Application modules may
// intentionally own long-lived timers, sockets, or client pools that belong in
// the function runner process but must not keep this short-lived CLI helper alive.
process.exit(0);
"#;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeploymentDiscovery {
    applications: Vec<Value>,
    code_manifest: Value,
    images: BTreeMap<String, Option<SerializedImageDefinition>>,
    sdk_version: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct SerializedImageDefinition {
    #[allow(dead_code)]
    id: String,
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    tag: String,
    base_image: Option<String>,
    operations: Vec<SerializedImageOperation>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SerializedImageOperation {
    #[serde(rename = "type")]
    operation_type: String,
    args: Vec<String>,
    options: BTreeMap<String, String>,
}

#[derive(Debug)]
struct ApplicationBundle {
    _temp: TempDir,
    code_zip: Vec<u8>,
    discovery: DeploymentDiscovery,
    function_runner_capsule: FunctionRunnerCapsule,
}

#[derive(Debug)]
struct FunctionRunnerCapsule {
    runtime_id: String,
    tgz: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct FunctionRunnerCapsuleManifest {
    format_version: u64,
    sdk_version: String,
    minimum_node_major: u64,
    package_name: String,
    runtime_id: String,
    files: BTreeMap<String, FunctionRunnerCapsuleFile>,
}

#[derive(Debug, Deserialize)]
struct FunctionRunnerCapsuleFile {
    sha256: String,
    size: u64,
    mode: u32,
}

#[derive(Debug)]
struct TensorlakeEntryPlugin {
    source: String,
    sdk_package_root: Arc<Mutex<Option<PathBuf>>>,
}

impl Plugin for TensorlakeEntryPlugin {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        "tensorlake-application-entry".into()
    }

    async fn resolve_id(
        &self,
        _ctx: &PluginContext,
        args: &HookResolveIdArgs<'_>,
    ) -> HookResolveIdReturn {
        if args.specifier != VIRTUAL_ENTRY {
            return Ok(None);
        }
        Ok(Some(HookResolveIdOutput {
            id: RESOLVED_VIRTUAL_ENTRY.into(),
            external: Some(false.into()),
            ..Default::default()
        }))
    }

    async fn load(&self, _ctx: SharedLoadPluginContext, args: &HookLoadArgs<'_>) -> HookLoadReturn {
        if args.id != RESOLVED_VIRTUAL_ENTRY {
            return Ok(None);
        }
        Ok(Some(HookLoadOutput {
            code: self.source.clone().into(),
            ..Default::default()
        }))
    }

    async fn module_parsed(
        &self,
        _ctx: &PluginContext,
        module_info: Arc<rolldown_common::ModuleInfo>,
        _normal_module: &rolldown_common::NormalModule,
    ) -> HookNoopReturn {
        let id = module_info.id.as_str();
        if let Some(package_root) = tensorlake_package_root(id)? {
            let mut captured = self
                .sdk_package_root
                .lock()
                .map_err(|_| anyhow::anyhow!("Tensorlake SDK path lock was poisoned"))?;
            if let Some(previous) = captured.as_ref() {
                if previous != &package_root {
                    anyhow::bail!(
                        "Application resolved multiple Tensorlake SDK packages: {} and {}",
                        previous.display(),
                        package_root.display()
                    );
                }
            } else {
                *captured = Some(package_root);
            }
        }
        if id == RESOLVED_VIRTUAL_ENTRY || !is_local_source(id) {
            return Ok(());
        }
        let extension = Path::new(id)
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        if matches!(extension.as_str(), "cjs" | "cts") || module_info.input_format.is_commonjs() {
            anyhow::bail!(
                "CommonJS is not supported in Tensorlake applications: {id}. Use an ESM module instead."
            );
        }
        Ok(())
    }

    fn register_hook_usage(&self) -> HookUsage {
        HookUsage::ResolveId | HookUsage::Load | HookUsage::ModuleParsed
    }
}

fn tensorlake_package_root(id: &str) -> anyhow::Result<Option<PathBuf>> {
    let path = Path::new(id);
    if path.file_name().and_then(|value| value.to_str()) != Some("index.js") {
        return Ok(None);
    }
    let Some(applications_directory) = path.parent() else {
        return Ok(None);
    };
    if applications_directory
        .file_name()
        .and_then(|value| value.to_str())
        != Some("applications")
    {
        return Ok(None);
    }
    let Some(dist_directory) = applications_directory.parent() else {
        return Ok(None);
    };
    if dist_directory.file_name().and_then(|value| value.to_str()) != Some("dist") {
        return Ok(None);
    }
    let Some(package_root) = dist_directory.parent() else {
        return Ok(None);
    };
    let package_json_path = package_root.join("package.json");
    let package_json: Value = match std::fs::read(&package_json_path) {
        Ok(contents) => serde_json::from_slice(&contents)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if package_json.get("name").and_then(Value::as_str) != Some("tensorlake") {
        return Ok(None);
    }
    Ok(Some(package_root.canonicalize()?))
}

fn is_local_source(id: &str) -> bool {
    let path = Path::new(id);
    path.is_absolute()
        && !path
            .components()
            .any(|component| component == Component::Normal("node_modules".as_ref()))
}

pub async fn run(
    ctx: &CliContext,
    entry_file: &Path,
    upgrade_running_requests: bool,
) -> Result<()> {
    if upgrade_running_requests {
        return Err(CliError::usage(
            "--upgrade-running-requests is not supported by Function Service; existing requests stay pinned to their deployed application version",
        ));
    }
    let entry_file = canonical_entrypoint(entry_file)?;
    eprintln!(
        "⚙️  Bundling TypeScript application {}",
        entry_file.display()
    );
    let bundle = bundle_application(&entry_file).await?;

    let function_images = build_application_images(ctx, &entry_file, &bundle).await?;
    eprintln!("\n✅ All images built successfully");

    for application in &bundle.discovery.applications {
        let application = application_with_resolved_images(application, &function_images)?;
        upsert_application(
            ctx,
            &application,
            &bundle.code_zip,
            upgrade_running_requests,
        )
        .await?;
        let name = required_string(&application, "name", "application")?;
        eprintln!("🚀 Application `{name}` deployed successfully");
        eprintln!(
            "\n💡 To invoke it, you can use the following cURL command:\n\ncurl {}/applications/{} \\\n  -H \"Authorization: Bearer $TENSORLAKE_API_KEY\" \\\n  --json 'null'",
            ctx.api_url.trim_end_matches('/'),
            name
        );
    }

    eprintln!(
        "\n📚 Visit our documentation if you need more information about invoking applications: https://docs.tensorlake.ai/applications/quickstart\n"
    );
    Ok(())
}

fn canonical_entrypoint(entry_file: &Path) -> Result<PathBuf> {
    let extension = entry_file
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !matches!(extension.as_str(), "ts" | "mts" | "js" | "mjs") {
        return Err(CliError::usage(format!(
            "Tensorlake TypeScript applications must be ESM .ts, .mts, .js, or .mjs files: {}",
            entry_file.display()
        )));
    }
    entry_file.canonicalize().map_err(|error| {
        CliError::usage(format!(
            "Application entrypoint not found: {} ({error})",
            entry_file.display()
        ))
    })
}

async fn bundle_application(entry_file: &Path) -> Result<ApplicationBundle> {
    let entry_file = canonical_entrypoint(entry_file)?;
    validate_node_24().await?;
    let application_root = entry_file
        .parent()
        .ok_or_else(|| CliError::usage("Application entrypoint has no parent directory"))?
        .to_path_buf();
    let temp = tempfile::tempdir().map_err(CliError::Io)?;
    let output_directory = temp.path().join("bundle");
    let image_context_directory = temp.path().join("image-context");
    std::fs::create_dir_all(&output_directory).map_err(CliError::Io)?;
    std::fs::create_dir_all(&image_context_directory).map_err(CliError::Io)?;

    let quoted_entry = serde_json::to_string(&entry_file.to_string_lossy().as_ref())?;
    let source = format!(
        r#"
import {quoted_entry};
import {{
  createApplicationManifests,
  createCodeManifest,
  getFunction,
  getFunctions,
  SDK_VERSION,
}} from "tensorlake/applications";

export function __tensorlakeGetFunction(name) {{
  return getFunction(name);
}}

export function __tensorlakeDeployment() {{
  const images = {{}};
  for (const definition of getFunctions()) {{
    const image = definition.options.image;
    images[definition.name] = image == null ? null : {{
      id: image._id,
      name: image.name,
      tag: image.tag,
      baseImage: image.baseImage,
      operations: image.buildOperations,
    }};
  }}
  return {{
    applications: createApplicationManifests(),
    codeManifest: createCodeManifest("runtime.mjs"),
    images,
    sdkVersion: SDK_VERSION,
  }};
}}
"#
    );

    let options = BundlerOptions {
        input: Some(vec![InputItem {
            name: Some("runtime".to_string()),
            import: VIRTUAL_ENTRY.to_string(),
        }]),
        cwd: Some(application_root.clone()),
        platform: Some(Platform::Node),
        dir: Some(output_directory.to_string_lossy().into_owned()),
        format: Some(OutputFormat::Esm),
        entry_filenames: Some(ChunkFilenamesOutputOption::String("[name].mjs".to_string())),
        chunk_filenames: Some(ChunkFilenamesOutputOption::String(
            "chunks/[name]-[hash].mjs".to_string(),
        )),
        // The function runner does not enable Node source-map loading. Shipping maps
        // only embeds application and dependency sources in the code ZIP and
        // can make otherwise small third-party dependency graphs exceed the
        // 5 MB application-code limit.
        sourcemap: None,
        transform: Some(BundlerTransformOptions {
            target: Some(Either::Left("node24".to_string())),
            ..Default::default()
        }),
        virtual_dirname: Some(application_root.to_string_lossy().into_owned()),
        clean_dir: Some(true),
        // Application-owned modules are validated as ESM by the plugin above.
        // Some npm dependencies (notably Ajv) still publish CommonJS internals;
        // Rolldown converts those into this otherwise-ESM output.
        polyfill_require: Some(true),
        ..Default::default()
    };
    let sdk_package_root = Arc::new(Mutex::new(None));
    let plugin = TensorlakeEntryPlugin {
        source,
        sdk_package_root: Arc::clone(&sdk_package_root),
    };
    let mut bundler = Bundler::with_plugins(options, vec![Arc::new(plugin)])
        .map_err(|error| CliError::Other(anyhow::anyhow!(error.to_string())))?;
    let output = bundler
        .write()
        .await
        .map_err(|error| CliError::Other(anyhow::anyhow!(error.to_string())))?;
    for warning in output.warnings {
        eprintln!("⚠️  {warning}");
    }
    bundler
        .close()
        .await
        .map_err(|error| CliError::Other(anyhow::anyhow!(error.to_string())))?;

    let runtime_path = output_directory.join(RUNTIME_MODULE);
    if !runtime_path.is_file() {
        return Err(CliError::Other(anyhow::anyhow!(
            "Rolldown did not produce {RUNTIME_MODULE}"
        )));
    }
    let discovery = discover_deployment(&runtime_path, temp.path()).await?;
    validate_discovery(&discovery, &entry_file)?;
    let sdk_package_root = sdk_package_root
        .lock()
        .map_err(|_| CliError::Other(anyhow::anyhow!("Tensorlake SDK path lock was poisoned")))?
        .clone()
        .ok_or_else(|| {
            CliError::usage(
                "Rolldown did not resolve tensorlake/applications from a Tensorlake SDK package",
            )
        })?;
    let function_runner_capsule =
        load_function_runner_capsule(&sdk_package_root, &discovery.sdk_version)?;
    std::fs::write(
        output_directory.join(CODE_MANIFEST_FILE),
        serde_json::to_vec(&discovery.code_manifest)?,
    )
    .map_err(CliError::Io)?;
    let code_zip = create_code_zip(&output_directory)?;

    Ok(ApplicationBundle {
        _temp: temp,
        code_zip,
        discovery,
        function_runner_capsule,
    })
}

async fn validate_node_24() -> Result<()> {
    let output = Command::new("node")
        .arg("--version")
        .output()
        .await
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                CliError::usage("Tensorlake TypeScript deployment requires Node 24 or newer")
            } else {
                CliError::Io(error)
            }
        })?;
    if !output.status.success() {
        return Err(CliError::usage(
            "Tensorlake TypeScript deployment requires Node 24 or newer",
        ));
    }
    let version = String::from_utf8_lossy(&output.stdout);
    let major = version
        .trim()
        .trim_start_matches('v')
        .split('.')
        .next()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or_default();
    if major < 24 {
        return Err(CliError::usage(format!(
            "Tensorlake TypeScript deployment requires Node 24 or newer; got {}",
            version.trim()
        )));
    }
    Ok(())
}

async fn discover_deployment(runtime_path: &Path, temp_root: &Path) -> Result<DeploymentDiscovery> {
    discover_deployment_with_timeout(runtime_path, temp_root, DISCOVERY_TIMEOUT).await
}

async fn discover_deployment_with_timeout(
    runtime_path: &Path,
    temp_root: &Path,
    timeout: Duration,
) -> Result<DeploymentDiscovery> {
    let runtime_url = Url::from_file_path(runtime_path).map_err(|_| {
        CliError::Other(anyhow::anyhow!(
            "Could not convert runtime path to a file URL: {}",
            runtime_path.display()
        ))
    })?;
    let discovery_path = temp_root.join("deployment.json");
    let output = Command::new("node")
        .args(["--input-type=module", "--eval", DISCOVERY_SCRIPT, "--"])
        .arg(runtime_url.as_str())
        .arg(&discovery_path)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .output();
    let output = tokio::time::timeout(timeout, output)
        .await
        .map_err(|_| {
            CliError::Other(anyhow::anyhow!(
                "TypeScript application initialization timed out after {:.1}s",
                timeout.as_secs_f64()
            ))
        })?
        .map_err(CliError::Io)?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(CliError::Other(anyhow::anyhow!(
            "TypeScript application initialization failed{}{}",
            if stderr.trim().is_empty() { "" } else { ":\n" },
            stderr.trim()
        )));
    }
    let data = std::fs::read(&discovery_path).map_err(|error| {
        CliError::Other(anyhow::anyhow!(
            "Node did not write deployment metadata to {}: {error}",
            discovery_path.display()
        ))
    })?;
    serde_json::from_slice(&data).map_err(Into::into)
}

fn validate_discovery(discovery: &DeploymentDiscovery, entry_file: &Path) -> Result<()> {
    if discovery.applications.is_empty() {
        return Err(CliError::usage(format!(
            "No Tensorlake application was registered by {}",
            entry_file.display()
        )));
    }
    if discovery
        .code_manifest
        .get("format_version")
        .and_then(Value::as_u64)
        != Some(2)
        || discovery
            .code_manifest
            .get("runtime")
            .and_then(Value::as_str)
            != Some("typescript")
        || discovery
            .code_manifest
            .get("minimum_node_major")
            .and_then(Value::as_u64)
            != Some(24)
        || discovery
            .code_manifest
            .get("module")
            .and_then(Value::as_str)
            != Some(RUNTIME_MODULE)
    {
        return Err(CliError::usage(
            "Application produced an invalid Tensorlake TypeScript code manifest",
        ));
    }
    let code_functions = discovery
        .code_manifest
        .get("functions")
        .and_then(Value::as_object)
        .ok_or_else(|| CliError::usage("Code manifest is missing functions"))?;
    let mut application_names = BTreeSet::new();
    for application in &discovery.applications {
        let name = required_string(application, "name", "application")?;
        if !application_names.insert(name.to_string()) {
            return Err(CliError::usage(format!(
                "Multiple Tensorlake applications are named '{name}'"
            )));
        }
        let functions = application
            .get("functions")
            .and_then(Value::as_object)
            .ok_or_else(|| CliError::usage(format!("Application '{name}' is missing functions")))?;
        let entrypoint = application
            .pointer("/entrypoint/function_name")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::usage(format!("Application '{name}' has no entrypoint")))?;
        if !functions.contains_key(entrypoint) {
            return Err(CliError::usage(format!(
                "Application '{name}' entrypoint '{entrypoint}' is not present in its functions"
            )));
        }
        for function_name in functions.keys() {
            if !code_functions.contains_key(function_name) {
                return Err(CliError::usage(format!(
                    "Application '{name}' function '{function_name}' is missing from the code manifest"
                )));
            }
        }
    }
    Ok(())
}

fn load_function_runner_capsule(
    package_root: &Path,
    sdk_version: &str,
) -> Result<FunctionRunnerCapsule> {
    let capsule_root = package_root.join("runtime/typescript-function-runner");
    let manifest_path = capsule_root.join("manifest.json");
    let manifest_data = std::fs::read(&manifest_path).map_err(|error| {
        CliError::usage(format!(
            "The Tensorlake SDK resolved from {} does not contain a built TypeScript function runner capsule at {} ({error}). Reinstall the package or run its build:sdk script.",
            package_root.display(),
            manifest_path.display(),
        ))
    })?;
    let manifest: FunctionRunnerCapsuleManifest =
        serde_json::from_slice(&manifest_data).map_err(|error| {
            CliError::usage(format!(
                "The Tensorlake TypeScript function runner capsule manifest at {} is invalid: {error}",
                manifest_path.display()
            ))
        })?;
    if manifest.format_version != 1
        || manifest.minimum_node_major != 24
        || manifest.package_name != "@tensorlake/typescript-function-runner-runtime"
    {
        return Err(CliError::usage(format!(
            "The Tensorlake TypeScript function runner capsule at {} uses an unsupported format",
            capsule_root.display()
        )));
    }
    if manifest.sdk_version != sdk_version {
        return Err(CliError::usage(format!(
            "The application uses Tensorlake SDK {sdk_version}, but its function runner capsule reports SDK {}",
            manifest.sdk_version
        )));
    }
    if manifest.files.is_empty() {
        return Err(CliError::usage(
            "Tensorlake TypeScript function runner capsule is empty",
        ));
    }
    if !manifest
        .files
        .contains_key(FUNCTION_RUNNER_LINUX_X64_BINDING)
    {
        return Err(CliError::usage(format!(
            "The Tensorlake TypeScript function runner capsule does not contain the Linux x64 native agent core required by sandbox images ({FUNCTION_RUNNER_LINUX_X64_BINDING}). Reinstall the published package or build the linux-x64 native target before running build:runtime."
        )));
    }

    let package_directory = capsule_root.join("package");
    let actual_files = collect_capsule_files(&package_directory)?;
    let expected_files = manifest.files.keys().cloned().collect::<BTreeSet<_>>();
    if actual_files != expected_files {
        let missing = expected_files
            .difference(&actual_files)
            .cloned()
            .collect::<Vec<_>>();
        let unexpected = actual_files
            .difference(&expected_files)
            .cloned()
            .collect::<Vec<_>>();
        return Err(CliError::usage(format!(
            "Tensorlake TypeScript function runner capsule file list does not match its manifest (missing: {}; unexpected: {})",
            missing.join(", "),
            unexpected.join(", ")
        )));
    }

    let package_json: Value = serde_json::from_slice(
        &std::fs::read(package_directory.join("package.json")).map_err(CliError::Io)?,
    )?;
    if package_json.get("name").and_then(Value::as_str) != Some(manifest.package_name.as_str())
        || package_json.get("version").and_then(Value::as_str) != Some(sdk_version)
        || package_json.get("type").and_then(Value::as_str) != Some("module")
        || package_json
            .pointer("/bin/tensorlake-typescript-function-runner")
            .and_then(Value::as_str)
            != Some("./bin/tensorlake-typescript-function-runner.js")
        || !manifest.files.contains_key("package/npm-shrinkwrap.json")
    {
        return Err(CliError::usage(format!(
            "Tensorlake TypeScript function runner capsule package metadata at {} is invalid",
            package_directory.display()
        )));
    }

    let mut runtime_hash = Sha256::new();
    let mut archive_files = Vec::with_capacity(manifest.files.len());
    for (relative_path, metadata) in &manifest.files {
        let path = safe_capsule_file_path(&capsule_root, relative_path)?;
        let contents = std::fs::read(&path).map_err(CliError::Io)?;
        let digest = hex::encode(Sha256::digest(&contents));
        if contents.len() as u64 != metadata.size || digest != metadata.sha256 {
            return Err(CliError::usage(format!(
                "Tensorlake TypeScript function runner capsule file failed integrity validation: {relative_path}"
            )));
        }
        if !matches!(metadata.mode, 0o644 | 0o755) {
            return Err(CliError::usage(format!(
                "Tensorlake TypeScript function runner capsule file has unsupported mode {:o}: {relative_path}",
                metadata.mode
            )));
        }
        runtime_hash.update(format!(
            "{relative_path}\0{}\0{}\0{}\n",
            metadata.sha256, metadata.size, metadata.mode
        ));
        archive_files.push((relative_path, metadata, contents));
    }
    let runtime_id = format!("sha256:{}", hex::encode(runtime_hash.finalize()));
    if runtime_id != manifest.runtime_id {
        return Err(CliError::usage(format!(
            "Tensorlake TypeScript function runner capsule runtime ID is invalid: expected {}, calculated {runtime_id}",
            manifest.runtime_id
        )));
    }

    let encoder = flate2::GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), flate2::Compression::best());
    let mut archive = tar::Builder::new(encoder);
    for (relative_path, metadata, contents) in archive_files {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_size(contents.len() as u64);
        header.set_mode(metadata.mode);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_cksum();
        archive
            .append_data(&mut header, relative_path, Cursor::new(contents))
            .map_err(CliError::Io)?;
    }
    archive.finish().map_err(CliError::Io)?;
    let encoder = archive.into_inner().map_err(CliError::Io)?;
    let tgz = encoder.finish().map_err(CliError::Io)?;
    Ok(FunctionRunnerCapsule { runtime_id, tgz })
}

fn safe_capsule_file_path(capsule_root: &Path, relative_path: &str) -> Result<PathBuf> {
    if relative_path.contains('\\') {
        return Err(CliError::usage(format!(
            "Tensorlake TypeScript function runner capsule contains an unsafe path: {relative_path}"
        )));
    }
    let relative = Path::new(relative_path);
    let components = relative.components().collect::<Vec<_>>();
    if components.len() < 2
        || components[0] != Component::Normal("package".as_ref())
        || components
            .iter()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(CliError::usage(format!(
            "Tensorlake TypeScript function runner capsule contains an unsafe path: {relative_path}"
        )));
    }
    Ok(capsule_root.join(relative))
}

fn collect_capsule_files(package_directory: &Path) -> Result<BTreeSet<String>> {
    fn visit(root: &Path, directory: &Path, files: &mut BTreeSet<String>) -> Result<()> {
        for entry in std::fs::read_dir(directory).map_err(CliError::Io)? {
            let entry = entry.map_err(CliError::Io)?;
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path).map_err(CliError::Io)?;
            if metadata.file_type().is_symlink() {
                return Err(CliError::usage(format!(
                    "Tensorlake TypeScript function runner capsule cannot contain symlinks: {}",
                    path.display()
                )));
            }
            if metadata.is_dir() {
                visit(root, &path, files)?;
            } else if metadata.is_file() {
                let relative = path
                    .strip_prefix(root)
                    .map_err(|error| CliError::Other(error.into()))?
                    .components()
                    .map(|component| component.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                files.insert(format!("package/{relative}"));
            }
        }
        Ok(())
    }

    let mut files = BTreeSet::new();
    visit(package_directory, package_directory, &mut files)?;
    Ok(files)
}

fn create_code_zip(output_directory: &Path) -> Result<Vec<u8>> {
    let mut files = Vec::new();
    collect_bundle_files(output_directory, output_directory, &mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));
    let total_size: u64 = files
        .iter()
        .map(|(_, path)| path.metadata().map(|m| m.len()).unwrap_or(0))
        .sum();
    if total_size > MAX_CODE_SIZE {
        let details = files
            .iter()
            .map(|(name, path)| {
                format!(
                    "  {name}: {} bytes",
                    path.metadata()
                        .map(|metadata| metadata.len())
                        .unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        return Err(CliError::usage(format!(
            "Application bundle is {:.2} MB, exceeding the 5 MB limit:\n{details}",
            total_size as f64 / 1024.0 / 1024.0
        )));
    }

    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options = FileOptions::default()
        .compression_method(CompressionMethod::Deflated)
        .unix_permissions(0o644);
    for (name, path) in files {
        writer
            .start_file(name, options)
            .map_err(|error| CliError::Other(error.into()))?;
        let mut file = File::open(path).map_err(CliError::Io)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).map_err(CliError::Io)?;
        writer.write_all(&data).map_err(CliError::Io)?;
    }
    let cursor = writer
        .finish()
        .map_err(|error| CliError::Other(error.into()))?;
    Ok(cursor.into_inner())
}

fn collect_bundle_files(
    root: &Path,
    directory: &Path,
    output: &mut Vec<(String, PathBuf)>,
) -> Result<()> {
    for entry in std::fs::read_dir(directory).map_err(CliError::Io)? {
        let entry = entry.map_err(CliError::Io)?;
        let path = entry.path();
        if path.is_dir() {
            collect_bundle_files(root, &path, output)?;
        } else if path.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|error| CliError::Other(error.into()))?
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            output.push((relative, path));
        }
    }
    Ok(())
}

async fn build_application_images(
    ctx: &CliContext,
    entry_file: &Path,
    bundle: &ApplicationBundle,
) -> Result<BTreeMap<(String, String), String>> {
    let discovery = &bundle.discovery;
    let context_directory = entry_file.parent().unwrap_or_else(|| Path::new("."));
    let empty_context_directory = bundle._temp.path().join("image-context");
    let mut built = BTreeMap::<String, (String, String)>::new();
    let mut function_images = BTreeMap::new();
    for application in &discovery.applications {
        let application_name = required_string(application, "name", "application")?;
        let version = required_string(application, "version", application_name)?;
        let application_component = registered_image_component(application_name);
        let version_component = registered_image_component(version);
        let default_name =
            format!("applications/{application_component}/versions/{version_component}/default");
        let functions = application
            .get("functions")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                CliError::usage(format!(
                    "Application '{application_name}' is missing functions"
                ))
            })?;
        for (function_name, function_manifest) in functions {
            let image = discovery.images.get(function_name).and_then(Option::as_ref);
            let explicit_name = function_manifest.get("image").and_then(Value::as_str);
            let registered_name = explicit_name.map_or_else(
                || default_name.clone(),
                |image_name| {
                    let image_component = registered_image_component(image_name);
                    format!(
                        "applications/{application_component}/versions/{version_component}/images/{image_component}"
                    )
                },
            );
            let fingerprint = match image {
                Some(image) => format!(
                    "{}:{}",
                    bundle.function_runner_capsule.runtime_id,
                    serde_json::to_string(image)?
                ),
                None => format!(
                    "{}:default-node-24",
                    bundle.function_runner_capsule.runtime_id
                ),
            };
            if let Some((previous, immutable_ref)) = built.get(&registered_name) {
                if previous != &fingerprint {
                    return Err(CliError::usage(format!(
                        "Different Image definitions use the registered name '{registered_name}'"
                    )));
                }
                function_images.insert(
                    (application_name.to_string(), function_name.to_string()),
                    immutable_ref.clone(),
                );
                continue;
            }
            eprintln!("📦 Building `{registered_name}` image...");
            let dockerfile = application_dockerfile(image)?;
            let needs_context = image.is_some_and(|image| {
                image
                    .operations
                    .iter()
                    .any(|operation| matches!(operation.operation_type.as_str(), "ADD" | "COPY"))
            });
            let options = SandboxImageBuildOptions {
                common: CommonBuildOptions {
                    api_url: ctx.api_url.clone(),
                    bearer_token: ctx.bearer_token()?,
                    use_scope_headers: ctx.personal_access_token.is_some() && ctx.api_key.is_none(),
                    organization_id: ctx.effective_organization_id(),
                    project_id: ctx.effective_project_id(),
                    namespace: ctx.namespace.clone(),
                    registered_name: Some(registered_name.clone()),
                    disk_mb: None,
                    builder_disk_mb: None,
                    cpus: None,
                    memory_mb: None,
                    is_public: false,
                    cas: true,
                    user_agent: Some(format!(
                        "Tensorlake CLI (rust/{})",
                        env!("CARGO_PKG_VERSION")
                    )),
                    docker_compat: true,
                },
                dockerfile_path: context_directory.join(".tensorlake-application.Dockerfile"),
                dockerfile_text: Some(dockerfile),
                context_dir: Some(if needs_context {
                    context_directory.to_path_buf()
                } else {
                    empty_context_directory.clone()
                }),
                context_files: vec![SandboxImageContextFile {
                    path: PathBuf::from(FUNCTION_RUNNER_CAPSULE_CONTEXT_PATH),
                    contents: bundle.function_runner_capsule.tgz.clone(),
                    mode: 0o644,
                }],
                build_args: Vec::new(),
            };
            let mut renderer = ImageBuildEventRenderer::new();
            let published = build_sandbox_image(options, |event| renderer.render(event))
                .await
                .map_err(|error| CliError::Other(error.into()))?;
            let immutable_ref = published_image_reference(&published)?;
            built.insert(registered_name, (fingerprint, immutable_ref.clone()));
            function_images.insert(
                (application_name.to_string(), function_name.to_string()),
                immutable_ref,
            );
        }
    }
    Ok(function_images)
}

fn published_image_reference(image: &Value) -> Result<String> {
    let image_id = image
        .get("image_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            CliError::Other(anyhow::anyhow!(
                "Image Service publication returned no immutable image_id"
            ))
        })?;
    if image_id.len() != 64
        || !image_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CliError::Other(anyhow::anyhow!(
            "Image Service publication returned an invalid immutable image_id"
        )));
    }
    Ok(format!("{CAS_IMAGE_REF_PREFIX}{image_id}"))
}

fn registered_image_component(value: &str) -> String {
    if value.len() <= 64 && is_valid_registered_image_component(value) {
        return value.to_string();
    }
    let digest = hex::encode(Sha256::digest(value.as_bytes()));
    format!("id-{}", &digest[..32])
}

fn is_valid_registered_image_component(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }
    let mut previous_was_separator = true;
    for byte in value.bytes() {
        let is_separator = matches!(byte, b'.' | b'_' | b'-');
        if is_separator {
            if previous_was_separator {
                return false;
            }
        } else if !(byte.is_ascii_lowercase() || byte.is_ascii_digit()) {
            return false;
        }
        previous_was_separator = is_separator;
    }
    !previous_was_separator
}

fn application_with_resolved_images(
    application: &Value,
    function_images: &BTreeMap<(String, String), String>,
) -> Result<Value> {
    let mut application = application.clone();
    let application_name = required_string(&application, "name", "application")?.to_string();
    let functions = application
        .get_mut("functions")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| {
            CliError::usage(format!(
                "Application '{application_name}' is missing functions"
            ))
        })?;
    for (function_name, manifest) in functions {
        let registered_name = function_images
            .get(&(application_name.clone(), function_name.clone()))
            .ok_or_else(|| {
                CliError::usage(format!(
                    "Runtime image was not prepared for function '{function_name}'"
                ))
            })?;
        manifest
            .as_object_mut()
            .ok_or_else(|| {
                CliError::usage(format!(
                    "Application '{application_name}' has an invalid manifest for function '{function_name}'"
                ))
            })?
            .insert("image".to_string(), Value::String(registered_name.clone()));
    }
    Ok(application)
}

fn application_dockerfile(image: Option<&SerializedImageDefinition>) -> Result<String> {
    let base_image = image
        .and_then(|image| image.base_image.as_deref())
        .unwrap_or(DEFAULT_NODE_IMAGE);
    if base_image.trim().is_empty() {
        return Err(CliError::usage("Application images require a base image"));
    }
    let has_workdir = image.is_some_and(|image| {
        image
            .operations
            .iter()
            .any(|operation| operation.operation_type == "WORKDIR")
    });
    let mut lines = vec![format!("FROM {base_image}")];
    if !has_workdir {
        lines.push("WORKDIR /app".to_string());
    }
    if let Some(image) = image {
        for operation in &image.operations {
            lines.push(render_image_operation(operation)?);
        }
    }
    lines.push("USER root".to_string());
    lines.push(format!(
        "COPY {FUNCTION_RUNNER_CAPSULE_CONTEXT_PATH} /tmp/tensorlake-typescript-function-runner-runtime.tgz"
    ));
    lines.push(
        "RUN set -eu; npm install --global --force --omit=dev --no-bin-links /tmp/tensorlake-typescript-function-runner-runtime.tgz; runner_entry=\"$(npm root --global)/@tensorlake/typescript-function-runner-runtime/bin/tensorlake-typescript-function-runner.js\"; test -f \"$runner_entry\"; mkdir -p /usr/local/bin; printf '#!/bin/sh\\nexec node \"%s\" \"$@\"\\n' \"$runner_entry\" > /usr/local/bin/tensorlake-typescript-function-runner; chmod 0755 /usr/local/bin/tensorlake-typescript-function-runner; rm -f /tmp/tensorlake-typescript-function-runner-runtime.tgz; test -x /usr/local/bin/tensorlake-typescript-function-runner; test ! -L /usr/local/bin/tensorlake-typescript-function-runner; node -e \"if (Number(process.versions.node.split('.')[0]) < 24) process.exit(1)\""
            .to_string(),
    );
    Ok(lines.join("\n"))
}

fn render_image_operation(operation: &SerializedImageOperation) -> Result<String> {
    if !matches!(
        operation.operation_type.as_str(),
        "ADD" | "COPY" | "ENV" | "RUN" | "WORKDIR"
    ) {
        return Err(CliError::usage(format!(
            "Unsupported application image operation '{}'",
            operation.operation_type
        )));
    }
    let options = operation
        .options
        .iter()
        .map(|(key, value)| format!(" --{key}={value}"))
        .collect::<String>();
    if operation.operation_type == "ENV" {
        if operation.args.len() != 2 {
            return Err(CliError::usage(
                "ENV image operations require a name and value",
            ));
        }
        return Ok(format!(
            "ENV{options} {}={}",
            operation.args[0],
            serde_json::to_string(&operation.args[1])?
        ));
    }
    Ok(format!(
        "{}{options} {}",
        operation.operation_type,
        operation.args.join(" ")
    ))
}

async fn upsert_application(
    ctx: &CliContext,
    application: &Value,
    code_zip: &[u8],
    upgrade_running_requests: bool,
) -> Result<()> {
    let client = ctx.client()?;
    let mut application = application.clone();
    let function_service_url = function_service_url(&ctx.api_url);
    ensure_public_endpoint_id(
        &function_service_url,
        &ctx.namespace,
        &client,
        &mut application,
    )
    .await?;
    let manifest = serde_json::to_string(&application)?;
    let url = format!(
        "{}/v1/namespaces/{}/applications",
        function_service_url.trim_end_matches('/'),
        urlencoding::encode(&ctx.namespace)
    );
    let mut delay = Duration::from_millis(500);
    for attempt in 0..=3 {
        let form = reqwest::multipart::Form::new()
            .part(
                "code",
                reqwest::multipart::Part::bytes(code_zip.to_vec())
                    .file_name("code.zip")
                    .mime_str("application/zip")?,
            )
            .text("code_content_type", "application/zip")
            .text("application", manifest.clone())
            .text(
                "upgrade_requests_to_latest_code",
                upgrade_running_requests.to_string(),
            );
        let response = client
            .post(&url)
            .multipart(form)
            .send()
            .await
            .map_err(CliError::Http)?;
        if response.status().is_success() {
            return Ok(());
        }
        let status = response.status();
        let retryable = matches!(status.as_u16(), 429 | 502 | 503 | 504);
        let body = response.text().await.unwrap_or_default();
        if !retryable || attempt == 3 {
            return Err(CliError::Other(anyhow::anyhow!(
                "Application deployment failed (HTTP {status}): {body}"
            )));
        }
        tokio::time::sleep(delay).await;
        delay *= 2;
    }
    unreachable!()
}

fn function_service_url(api_url: &str) -> String {
    resolve_function_service_url(
        api_url,
        std::env::var("TENSORLAKE_FUNCTION_SERVICE_URL").ok(),
    )
}

fn resolve_function_service_url(api_url: &str, override_url: Option<String>) -> String {
    override_url
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| api_url.to_string())
}

fn allows_unauthenticated_requests(application: &Value) -> bool {
    application
        .get("allow")
        .and_then(Value::as_array)
        .is_some_and(|allow| {
            allow
                .iter()
                .any(|capability| capability.as_str() == Some(UNAUTHENTICATED_REQUESTS))
        })
}

fn generate_public_endpoint_id() -> String {
    format!(
        "endpoint_{}",
        Alphanumeric.sample_string(&mut rand::rng(), 21)
    )
}

async fn ensure_public_endpoint_id(
    api_url: &str,
    namespace: &str,
    client: &reqwest::Client,
    application: &mut Value,
) -> Result<()> {
    if !allows_unauthenticated_requests(application)
        || application
            .get("public_endpoint_id")
            .and_then(Value::as_str)
            .is_some_and(|endpoint_id| !endpoint_id.is_empty())
    {
        return Ok(());
    }

    let application_name = required_string(application, "name", "application")?;
    let url = format!(
        "{}/v1/namespaces/{}/applications/{}",
        api_url.trim_end_matches('/'),
        urlencoding::encode(namespace),
        urlencoding::encode(application_name),
    );
    let response = client.get(url).send().await.map_err(CliError::Http)?;
    let endpoint_id = if response.status().is_success() {
        response
            .json::<Value>()
            .await
            .map_err(CliError::Http)?
            .get("public_endpoint_id")
            .and_then(Value::as_str)
            .filter(|endpoint_id| !endpoint_id.is_empty())
            .map(ToOwned::to_owned)
    } else if response.status() == reqwest::StatusCode::NOT_FOUND {
        None
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "Could not resolve the public endpoint for application \
             '{application_name}' (HTTP {status}): {body}"
        )));
    };

    application
        .as_object_mut()
        .ok_or_else(|| CliError::usage("Tensorlake application manifest must be an object"))?
        .insert(
            "public_endpoint_id".to_string(),
            Value::String(endpoint_id.unwrap_or_else(generate_public_endpoint_id)),
        );
    Ok(())
}

fn required_string<'a>(value: &'a Value, key: &str, label: &str) -> Result<&'a str> {
    value.get(key).and_then(Value::as_str).ok_or_else(|| {
        CliError::usage(format!(
            "Tensorlake {label} is missing string field '{key}'"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::{
        FUNCTION_RUNNER_LINUX_X64_BINDING, SerializedImageOperation, application_dockerfile,
        application_with_resolved_images, bundle_application, canonical_entrypoint,
        discover_deployment_with_timeout, ensure_public_endpoint_id, load_function_runner_capsule,
        published_image_reference, registered_image_component, render_image_operation,
        resolve_function_service_url,
    };
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::Duration;

    fn repository_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    #[test]
    fn accepts_only_esm_entrypoint_extensions() {
        let directory = tempfile::tempdir().unwrap();
        for extension in ["ts", "mts", "js", "mjs"] {
            let path = directory.path().join(format!("application.{extension}"));
            std::fs::write(&path, "").unwrap();
            assert!(canonical_entrypoint(&path).is_ok());
        }
        for extension in ["cts", "cjs"] {
            let path = directory.path().join(format!("application.{extension}"));
            std::fs::write(&path, "").unwrap();
            assert!(canonical_entrypoint(&path).is_err());
        }
    }

    #[test]
    fn renders_node_24_application_dockerfile() {
        let dockerfile = application_dockerfile(None).unwrap();
        assert!(dockerfile.contains("FROM node:24-bookworm-slim"));
        assert!(dockerfile.contains("COPY .tensorlake/typescript-function-runner-runtime.tgz"));
        assert!(!dockerfile.contains("tensorlake@"));
        assert!(dockerfile.contains("--no-bin-links"));
        assert!(
            dockerfile.contains("test ! -L /usr/local/bin/tensorlake-typescript-function-runner")
        );
        assert!(dockerfile.contains("tensorlake-typescript-function-runner"));
    }

    #[test]
    fn validates_and_packages_built_function_runner_capsule() {
        let root = repository_root().join("typescript");
        let capsule = load_function_runner_capsule(&root, env!("CARGO_PKG_VERSION")).unwrap();
        assert!(capsule.runtime_id.starts_with("sha256:"));
        assert!(!capsule.tgz.is_empty());
        let decoder = flate2::read::GzDecoder::new(capsule.tgz.as_slice());
        let mut archive = tar::Archive::new(decoder);
        let paths = archive
            .entries()
            .unwrap()
            .map(|entry| {
                entry
                    .unwrap()
                    .path()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect::<Vec<_>>();
        assert!(paths.contains(&"package/package.json".to_string()));
        assert!(paths.contains(&"package/npm-shrinkwrap.json".to_string()));
        assert!(
            paths.contains(&"package/bin/tensorlake-typescript-function-runner.js".to_string())
        );
        assert!(paths.contains(&FUNCTION_RUNNER_LINUX_X64_BINDING.to_string()));
    }

    #[test]
    fn renders_image_operations() {
        let operation = SerializedImageOperation {
            operation_type: "ENV".to_string(),
            args: vec!["NAME".to_string(), "a value".to_string()],
            options: BTreeMap::new(),
        };
        assert_eq!(
            render_image_operation(&operation).unwrap(),
            "ENV NAME=\"a value\""
        );
    }

    #[test]
    fn writes_resolved_registered_images_into_application_manifest() {
        let application = json!({
            "name": "app",
            "runtime": "typescript",
            "functions": {
                "default_fn": {"name": "default_fn"},
                "custom_fn": {"name": "custom_fn", "image": "logical-image"}
            }
        });
        let images = BTreeMap::from([
            (
                ("app".to_string(), "default_fn".to_string()),
                format!("cas-v1:{}", "a".repeat(64)),
            ),
            (
                ("app".to_string(), "custom_fn".to_string()),
                format!("cas-v1:{}", "b".repeat(64)),
            ),
        ]);

        let resolved = application_with_resolved_images(&application, &images).unwrap();

        assert_eq!(
            resolved["functions"]["default_fn"]["image"],
            format!("cas-v1:{}", "a".repeat(64))
        );
        assert_eq!(
            resolved["functions"]["custom_fn"]["image"],
            format!("cas-v1:{}", "b".repeat(64))
        );
    }

    #[test]
    fn validates_published_image_ids_and_registered_name_components() {
        let image_id = "a".repeat(64);
        assert_eq!(
            published_image_reference(&json!({ "image_id": image_id })).unwrap(),
            format!("cas-v1:{}", "a".repeat(64))
        );
        assert!(published_image_reference(&json!({ "image_id": "mutable-name" })).is_err());
        assert_eq!(registered_image_component("app-v1"), "app-v1");
        let sanitized = registered_image_component("My Application");
        assert!(sanitized.starts_with("id-"));
        assert_eq!(sanitized.len(), 35);
    }

    #[test]
    fn function_service_url_defaults_to_api_origin() {
        assert_eq!(
            resolve_function_service_url("https://api.tensorlake.ai", None),
            "https://api.tensorlake.ai"
        );
        assert_eq!(
            resolve_function_service_url(
                "https://api.tensorlake.ai",
                Some("http://functions.test:8930".to_string())
            ),
            "http://functions.test:8930"
        );
    }

    #[tokio::test]
    async fn bundles_and_discovers_esm_application() {
        let entry = repository_root().join("typescript/examples/hello-world.ts");
        let bundle = bundle_application(&entry).await.unwrap();
        assert!(bundle._temp.path().join("bundle/runtime.mjs").is_file());
        assert!(!bundle._temp.path().join("bundle/runtime.mjs.map").exists());
        assert!(!bundle.code_zip.is_empty());
        assert_eq!(bundle.discovery.applications.len(), 1);
        assert_eq!(
            bundle.discovery.applications[0]
                .get("name")
                .and_then(serde_json::Value::as_str),
            Some("hello_world")
        );
    }

    #[tokio::test]
    async fn completes_application_discovery_with_live_node_handles() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = directory.path().join("runtime.mjs");
        std::fs::write(
            &runtime,
            r#"
setInterval(() => {}, 1000);
export function __tensorlakeDeployment() {
  return {
    applications: [],
    codeManifest: {},
    images: {},
    sdkVersion: "test",
  };
}
"#,
        )
        .unwrap();

        let discovery =
            discover_deployment_with_timeout(&runtime, directory.path(), Duration::from_secs(2))
                .await
                .unwrap();
        assert_eq!(discovery.sdk_version, "test");
    }

    #[tokio::test]
    async fn public_application_deployment_reuses_existing_endpoint_id() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 4096];
            let count = socket.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..count]);
            assert!(
                request.starts_with(
                    "GET /v1/namespaces/default/applications/public_webhook HTTP/1.1\r\n"
                ),
                "{request}",
            );
            let body = r#"{"public_endpoint_id":"endpoint_0123456789abcdefghijk"}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len(),
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let mut application = json!({
            "name": "public_webhook",
            "allow": ["unauthenticated_requests"],
        });

        ensure_public_endpoint_id(
            &format!("http://{address}"),
            "default",
            &crate::http::client_builder().build().unwrap(),
            &mut application,
        )
        .await
        .unwrap();
        server.await.unwrap();

        assert_eq!(
            application["public_endpoint_id"],
            "endpoint_0123456789abcdefghijk",
        );
    }

    #[tokio::test]
    async fn public_application_deployment_generates_endpoint_id_after_not_found() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            socket
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\
                      Connection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let mut application = json!({
            "name": "new_public_webhook",
            "allow": ["unauthenticated_requests"],
        });

        ensure_public_endpoint_id(
            &format!("http://{address}"),
            "default",
            &crate::http::client_builder().build().unwrap(),
            &mut application,
        )
        .await
        .unwrap();
        server.await.unwrap();

        let endpoint_id = application["public_endpoint_id"].as_str().unwrap();
        assert!(endpoint_id.starts_with("endpoint_"));
        assert_eq!(endpoint_id.len(), "endpoint_".len() + 21);
        assert!(
            endpoint_id["endpoint_".len()..]
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
        );
    }

    #[tokio::test]
    async fn rejects_imported_application_commonjs_modules() {
        let typescript_root = repository_root().join("typescript");
        let directory = tempfile::Builder::new()
            .prefix("tensorlake-commonjs-test-")
            .tempdir_in(typescript_root)
            .unwrap();
        let entry = directory.path().join("application.mjs");
        std::fs::write(
            &entry,
            r#"
import "./legacy.cjs";
import { registerApplication, schema } from "tensorlake/applications";

export const app = registerApplication(async () => null, {
  name: "commonjs_rejection",
  parameters: [],
  returns: schema.null(),
});
"#,
        )
        .unwrap();
        std::fs::write(directory.path().join("legacy.cjs"), "module.exports = 1;").unwrap();

        let error = bundle_application(&entry).await.unwrap_err().to_string();
        assert!(error.contains("CommonJS is not supported"), "{error}");
    }
}
