use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{self, Write};
use url;

/// Internal representation of build information from the API.
#[derive(Debug, Serialize, Deserialize)]
pub struct BuildInfo {
    pub id: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
    pub finished_at: Option<String>,
    pub error_message: Option<String>,
}

/// Response for build info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildInfoResponse {
    /// The build ID.
    pub id: String,
    /// The build status.
    pub status: BuildStatus,
    /// Error message if failed.
    pub error_message: Option<String>,
    /// Creation time.
    pub created_at: String,
    /// Updated time.
    pub updated_at: String,
    /// Finished time.
    pub finished_at: Option<String>,
    /// Image hash.
    pub image_hash: String,
    /// Image name.
    pub image_name: Option<String>,
}

/// Response for listing builds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildListResponse {
    /// The public ID of the build.
    pub public_id: String,
    /// The name of the image.
    pub name: String,
    /// Tags associated with the build.
    pub tags: Vec<String>,
    /// The creation time of the build.
    pub creation_time: String,
    /// The status of the build.
    pub status: BuildStatus,
}

/// The status of an image build.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum BuildStatus {
    /// The build is pending.
    Pending,
    /// The build is enqueued.
    Enqueued,
    /// The build is in progress.
    Building,
    /// The build completed successfully.
    Succeeded,
    /// The build failed.
    Failed,
    /// The build is being canceled.
    Canceling,
    /// The build was canceled.
    Canceled,
}

/// Response for canceling a build.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelBuildResponse {
    /// The status message.
    pub status: String,
}

/// Request parameters for building an image.
#[derive(Builder, Clone, Debug)]
pub struct ImageBuildRequest {
    /// The image definition.
    pub image: Image,
    /// The tag for the image.
    #[builder(setter(into))]
    pub image_tag: String,
    /// The name of the application this image belongs to.
    #[builder(setter(into))]
    pub application_name: String,
    /// The version of the application.
    #[builder(setter(into))]
    pub application_version: String,
    /// The name of the function in the application.
    #[builder(setter(into))]
    pub function_name: String,
    /// The SDK version for hashing.
    #[builder(setter(into))]
    pub sdk_version: String,
}

impl ImageBuildRequest {
    /// Creates a new `ImageBuildRequest` builder.
    pub fn builder() -> ImageBuildRequestBuilder {
        ImageBuildRequestBuilder::default()
    }
}

/// Result of an image build operation.
#[derive(Debug, Clone)]
pub struct ImageBuildResult {
    /// The unique ID of the build.
    pub id: String,
    /// The final status of the build.
    pub status: BuildStatus,
    /// When the build was created.
    pub created_at: String,
    /// When the build finished (if completed).
    pub finished_at: Option<String>,
    /// Error message if the build failed.
    pub error_message: Option<String>,
}

/// Response for pulling an image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImagePullResponse {
    /// The build ID.
    pub id: String,
    /// The image URI.
    pub image_uri: String,
    /// The image hash.
    pub image_hash: String,
    /// The image digest.
    pub image_digest: String,
    /// The image name.
    pub image_name: String,
    /// The registry type.
    pub registry: RegistryType,
    /// The build status.
    pub status: BuildStatus,
    /// Error message if failed.
    pub error: Option<String>,
    /// Creation time.
    pub created_at: String,
    /// Finished time.
    pub finished_at: Option<String>,
}

/// Log entry for streaming logs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// The build ID.
    pub build_id: String,
    /// The timestamp of the log entry.
    pub timestamp: String,
    /// The stream type.
    pub stream: String,
    /// The log message.
    pub message: String,
    /// The sequence number.
    pub sequence_number: i64,
    /// The build status at the time of the log.
    pub build_status: String,
}

/// Paginated page of build list responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Page<T> {
    /// The items in this page.
    pub items: Vec<T>,
    /// The total number of items.
    pub total_items: i64,
    /// The current page number.
    pub page: i32,
    /// The number of items per page.
    pub page_size: i32,
    /// The total number of pages.
    pub total_pages: i32,
}

/// Registry type for the image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RegistryType {
    /// ECR registry.
    ECR,
    /// Docker registry.
    Docker,
}

#[derive(Builder, Debug)]
pub struct CancelBuildRequest {
    #[builder(setter(into))]
    pub build_id: String,
}

impl CancelBuildRequest {
    pub fn builder() -> CancelBuildRequestBuilder {
        CancelBuildRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct GetBuildInfoRequest {
    #[builder(setter(into))]
    pub build_id: String,
}

impl GetBuildInfoRequest {
    pub fn builder() -> GetBuildInfoRequestBuilder {
        GetBuildInfoRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct ListBuildsRequest {
    #[builder(default, setter(strip_option))]
    pub page: Option<i32>,
    #[builder(default, setter(strip_option))]
    pub page_size: Option<i32>,
    #[builder(default, setter(strip_option))]
    pub status: Option<BuildStatus>,
    #[builder(default, setter(into, strip_option))]
    pub application_name: Option<String>,
    #[builder(default, setter(into, strip_option))]
    pub image_name: Option<String>,
    #[builder(default, setter(into, strip_option))]
    pub function_name: Option<String>,
}

impl ListBuildsRequest {
    pub fn builder() -> ListBuildsRequestBuilder {
        ListBuildsRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct StreamLogsRequest {
    #[builder(setter(into))]
    pub build_id: String,
}

impl StreamLogsRequest {
    pub fn builder() -> StreamLogsRequestBuilder {
        StreamLogsRequestBuilder::default()
    }
}

#[derive(Builder, Clone, Debug, Serialize, Deserialize)]
pub struct CreateApplicationBuildImageRequest {
    #[builder(setter(into))]
    pub key: String,
    #[builder(setter(into))]
    pub name: String,
    #[builder(setter(into))]
    pub context_tar_part_name: String,
    #[builder(setter(into))]
    pub context_sha256: String,
    #[builder(default, setter(into))]
    pub function_names: Vec<String>,
}

impl CreateApplicationBuildImageRequest {
    pub fn builder() -> CreateApplicationBuildImageRequestBuilder {
        CreateApplicationBuildImageRequestBuilder::default()
    }
}

#[derive(Builder, Clone, Debug, Serialize, Deserialize)]
pub struct CreateApplicationBuildRequest {
    #[builder(setter(into))]
    pub name: String,
    #[builder(setter(into))]
    pub version: String,
    #[builder(default, setter(into))]
    pub images: Vec<CreateApplicationBuildImageRequest>,
}

impl CreateApplicationBuildRequest {
    pub fn builder() -> CreateApplicationBuildRequestBuilder {
        CreateApplicationBuildRequestBuilder::default()
    }
}

#[derive(Clone, Debug)]
pub struct ApplicationBuildContext {
    pub context_tar_part_name: String,
    pub context_tar_gz: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplicationBuildImageResponse {
    pub id: String,
    pub app_version_id: Option<String>,
    pub key: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub status: String,
    pub error_message: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
    pub finished_at: Option<String>,
    pub function_names: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplicationBuildResponse {
    pub id: String,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub name: String,
    pub version: String,
    pub status: Option<String>,
    pub image_builds: Vec<ApplicationBuildImageResponse>,
}

/// Type of image build operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ImageBuildOperationType {
    /// Copy files from the build context.
    COPY,
    /// Run a command.
    RUN,
    /// Add files or URLs.
    ADD,
    /// Set environment variables.
    ENV,
}

/// Image build operation.
#[derive(Debug, Clone, Builder, Deserialize)]
pub struct ImageBuildOperation {
    /// The type of operation.
    #[serde(rename = "op")]
    pub operation_type: ImageBuildOperationType,
    /// Arguments for the operation.
    #[builder(setter(into))]
    pub args: Vec<String>,
    /// Options for the operation.
    #[builder(default, setter(into))]
    #[serde(default)]
    pub options: HashMap<String, String>,
}

impl ImageBuildOperation {
    pub fn builder() -> ImageBuildOperationBuilder {
        ImageBuildOperationBuilder::default()
    }
}

/// Image definition for building container images.
#[derive(Debug, Clone, Builder)]
pub struct Image {
    /// The name of the image.
    #[builder(setter(into))]
    pub name: String,
    /// The base image to use.
    #[builder(setter(into))]
    pub base_image: String,
    /// List of build operations.
    #[builder(default)]
    pub build_operations: Vec<ImageBuildOperation>,
}

impl Image {
    pub fn builder() -> ImageBuilder {
        ImageBuilder::default()
    }

    /// Calculate the hash for this image, matching the Python implementation.
    pub fn image_hash(&self, sdk_version: &str) -> io::Result<String> {
        let mut hasher = Sha256::new();
        hasher.update(self.name.as_bytes());
        hasher.update(self.base_image.as_bytes());
        for op in &self.build_operations {
            add_build_op_to_hasher(op, &mut hasher)?;
        }
        hasher.update(sdk_version.as_bytes());
        Ok(hex::encode(hasher.finalize()))
    }

    /// Generate the Dockerfile content for this image.
    ///
    /// If `stage` is `Some`, appends `AS <stage>` to the `FROM` line.
    pub fn dockerfile_content(&self, sdk_version: &str, stage: Option<&str>) -> String {
        let from_line = match stage {
            Some(s) => format!("FROM {} AS {}", self.base_image, s),
            None => format!("FROM {}", self.base_image),
        };
        let mut lines = vec![
            from_line,
            "WORKDIR /app".to_string(),
            // Handle externally-managed environments (PEP 668) on modern Linux distros.
            "ENV PIP_BREAK_SYSTEM_PACKAGES=1".to_string(),
        ];

        for op in &self.build_operations {
            lines.push(render_build_operation(op));
        }

        if sdk_version.starts_with("~=")
            || sdk_version.starts_with(">=")
            || sdk_version.starts_with("<=")
            || sdk_version.starts_with("!=")
            || sdk_version.starts_with("==")
        {
            lines.push(format!("RUN pip install tensorlake{}", sdk_version));
        } else {
            lines.push(format!("RUN pip install tensorlake=={}", sdk_version));
        }

        lines.join("\n")
    }

    /// Create a tar.gz archive containing the build context.
    ///
    /// Generates the Dockerfile from the image definition, optionally with a stage name.
    pub fn create_context_archive<W: Write>(
        &self,
        writer: W,
        sdk_version: &str,
        stage: Option<&str>,
    ) -> io::Result<()> {
        let dockerfile = self.dockerfile_content(sdk_version, stage);
        self.create_context_archive_with_dockerfile(writer, &dockerfile)
    }

    /// Create a tar.gz archive using the provided Dockerfile content.
    ///
    /// Useful when the Dockerfile has been post-processed (e.g. via a template).
    pub fn create_context_archive_with_dockerfile<W: Write>(
        &self,
        writer: W,
        dockerfile: &str,
    ) -> io::Result<()> {
        let gz_writer = flate2::write::GzEncoder::new(writer, flate2::Compression::default());
        let mut tar = tar::Builder::new(gz_writer);

        for op in &self.build_operations {
            match op.operation_type {
                ImageBuildOperationType::COPY => {
                    if let Some(src) = op.args.first() {
                        add_path_to_archive(&mut tar, src)?;
                    }
                }
                ImageBuildOperationType::ADD => {
                    if let Some(src) = op.args.first() {
                        if is_url(src) || is_git_repo_url(src) {
                            continue;
                        }
                        if !std::path::Path::new(src).exists() {
                            continue;
                        }
                        if is_inside_git_dir(src) {
                            continue;
                        }
                        add_path_to_archive(&mut tar, src)?;
                    }
                }
                _ => {}
            }
        }

        let df_bytes = dockerfile.as_bytes();
        let mut header = tar::Header::new_gnu();
        header.set_size(df_bytes.len() as u64);
        header.set_mode(0o644);
        tar.append_data(&mut header, "Dockerfile", df_bytes)?;

        tar.finish()?;
        Ok(())
    }
}

impl std::fmt::Display for ImageBuildOperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImageBuildOperationType::COPY => write!(f, "COPY"),
            ImageBuildOperationType::RUN => write!(f, "RUN"),
            ImageBuildOperationType::ADD => write!(f, "ADD"),
            ImageBuildOperationType::ENV => write!(f, "ENV"),
        }
    }
}

fn render_build_operation(op: &ImageBuildOperation) -> String {
    let options = if op.options.is_empty() {
        String::new()
    } else {
        let mut sorted_opts: Vec<_> = op.options.iter().collect();
        sorted_opts.sort_by_key(|(k, _)| (*k).clone());
        format!(
            " {}",
            sorted_opts
                .into_iter()
                .map(|(k, v)| format!("--{}={}", k, v))
                .collect::<Vec<_>>()
                .join(" ")
        )
    };

    match op.operation_type {
        // Each arg in a RUN list is a separate shell command → one RUN line each.
        ImageBuildOperationType::RUN => op
            .args
            .iter()
            .map(|cmd| format!("RUN{} {}", options, cmd))
            .collect::<Vec<_>>()
            .join("\n"),
        ImageBuildOperationType::ENV => {
            let body = if op.args.len() >= 2 {
                format!("{}=\"{}\"", op.args[0], op.args[1])
            } else {
                op.args.join(" ")
            };
            format!("ENV{} {}", options, body)
        }
        _ => format!("{}{} {}", op.operation_type, options, op.args.join(" ")),
    }
}

fn add_path_to_archive<W: Write>(tar: &mut tar::Builder<W>, src: &str) -> io::Result<()> {
    if std::path::Path::new(src).is_dir() {
        tar.append_dir_all(src, src)
    } else {
        tar.append_path(src)
    }
}

fn is_url(path: &str) -> bool {
    if let Ok(url) = url::Url::parse(path) {
        matches!(url.scheme(), "http" | "https")
    } else {
        false
    }
}

fn is_git_repo_url(path: &str) -> bool {
    if let Ok(url) = url::Url::parse(path) {
        if url.scheme() == "git" {
            return true;
        }
        if let Some(host) = url.host_str() {
            return host == "github.com" || host.ends_with(".github.com");
        }
    }
    false
}

fn is_inside_git_dir(path: &str) -> bool {
    std::path::Path::new(path)
        .components()
        .any(|c| c.as_os_str() == ".git")
}

fn add_build_op_to_hasher(op: &ImageBuildOperation, hasher: &mut Sha256) -> io::Result<()> {
    hasher.update(op.operation_type.to_string().as_bytes());

    match op.operation_type {
        ImageBuildOperationType::RUN
        | ImageBuildOperationType::ADD
        | ImageBuildOperationType::ENV => {
            for arg in &op.args {
                hasher.update(arg.as_bytes());
            }
        }
        ImageBuildOperationType::COPY => {
            if let Some(src) = op.args.first() {
                hash_directory(src, hasher)?;
            }
        }
    }
    Ok(())
}

fn hash_directory(path: &str, hasher: &mut Sha256) -> io::Result<()> {
    use std::fs;
    use std::io::Read;

    fn visit_dir(
        dir: &std::path::Path,
        base: &std::path::Path,
        hasher: &mut Sha256,
    ) -> io::Result<()> {
        if dir.is_dir() {
            let mut entries: Vec<_> = fs::read_dir(dir)?.collect::<Result<Vec<_>, _>>()?;
            entries.sort_by_key(|e| e.path());
            for entry in entries {
                let path = entry.path();
                if path.is_dir() {
                    visit_dir(&path, base, hasher)?;
                } else {
                    let rel = path.strip_prefix(base).unwrap_or(&path);
                    hasher.update(rel.to_string_lossy().as_bytes());
                    let mut file = fs::File::open(&path)?;
                    let mut buffer = [0u8; 1024];
                    loop {
                        let bytes_read = file.read(&mut buffer)?;
                        if bytes_read == 0 {
                            break;
                        }
                        hasher.update(&buffer[..bytes_read]);
                    }
                }
            }
        }
        Ok(())
    }

    let path = std::path::Path::new(path);
    if path.exists() {
        visit_dir(path, path, hasher)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_directory_is_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        // Create files in non-alphabetical order
        std::fs::write(dir.path().join("z.txt"), b"zzz").unwrap();
        std::fs::write(dir.path().join("a.txt"), b"aaa").unwrap();
        std::fs::write(dir.path().join("m.txt"), b"mmm").unwrap();

        let image = Image::builder()
            .name("test")
            .base_image("python:3.10")
            .build_operations(vec![
                ImageBuildOperation::builder()
                    .operation_type(ImageBuildOperationType::COPY)
                    .args(vec![dir.path().to_string_lossy().into_owned()])
                    .build()
                    .unwrap(),
            ])
            .build()
            .unwrap();

        let h1 = image.image_hash("1.0.0").unwrap();
        let h2 = image.image_hash("1.0.0").unwrap();
        assert_eq!(h1, h2, "hash must be deterministic across calls");
    }

    #[test]
    fn test_render_build_operation_options_are_sorted() {
        let op = ImageBuildOperation::builder()
            .operation_type(ImageBuildOperationType::COPY)
            .args(vec!["src/".into(), "/app/".into()])
            .options(HashMap::from([
                ("from".into(), "builder".into()),
                ("chown".into(), "1000:1000".into()),
                ("chmod".into(), "755".into()),
            ]))
            .build()
            .unwrap();

        let rendered = render_build_operation(&op);
        assert_eq!(
            rendered,
            "COPY --chmod=755 --chown=1000:1000 --from=builder src/ /app/"
        );
    }

    #[test]
    #[cfg(unix)]
    fn test_hash_directory_returns_error_on_io_failure() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("unreadable.txt");
        std::fs::write(&file_path, b"secret").unwrap();
        std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o000)).unwrap();

        let image = Image::builder()
            .name("test")
            .base_image("python:3.10")
            .build_operations(vec![
                ImageBuildOperation::builder()
                    .operation_type(ImageBuildOperationType::COPY)
                    .args(vec![dir.path().to_string_lossy().into_owned()])
                    .build()
                    .unwrap(),
            ])
            .build()
            .unwrap();

        let result = image.image_hash("1.0.0");
        assert!(result.is_err(), "should return error for unreadable file");

        // Restore permissions so tempdir cleanup succeeds
        std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    }
}
