//! The merged sandbox-image catalog: one client-side view over the platform
//! sandbox-template registry (legacy `.tlsnap` images) and the Image Service
//! catalog (CAS images), so `describe`, `ls`, `rm` and the SDK lookups behave
//! identically for both families.
//!
//! Dispatch is by reference shape: a `sandbox_template_` id is a platform
//! template, an `image_` id is an Image Service image, anything else is a
//! name. A name is looked up in the Image Service first (when it fits that
//! catalog's grammar) and only a clean 404 falls through to the platform
//! registry; any other failure is reported, never mistaken for a miss. Single
//! image operations never list and match. Listings concatenate both catalogs
//! and sort newest first, then by name, with CAS images ahead of legacy ones
//! on a tie.
//!
//! The Image Service answers the platform template shape (snake_case, RFC 3339
//! timestamps); this module maps it onto [`SandboxTemplate`] so callers see
//! one record kind for both families.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeZone, Utc};
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    Traced,
    client::Client,
    error::SdkError,
    sandbox_images::{cas_image_service_url, client_builder},
    sandbox_templates::{SandboxTemplatesClient, models::SandboxTemplate},
};

/// Prefix of an Image Service logical image id.
pub const IMAGE_SERVICE_ID_PREFIX: &str = "image_";
/// Prefix of a platform sandbox-template id.
pub const SANDBOX_TEMPLATE_ID_PREFIX: &str = "sandbox_template_";
/// The snapshot format version of a CAS (content-addressed streaming) image.
pub const CAS_SNAPSHOT_FORMAT_VERSION: &str = "content_addressed_streaming_v1";
/// The retired immutable reference scheme. Images are addressed by name or
/// `image_` id; a digest reference is an error, not a lookup.
const CAS_V1_REFERENCE_PREFIX: &str = "cas-v1:";
const IMAGE_SERVICE_LIST_PAGE_SIZE: &str = "100";

/// How a user-supplied image reference is dispatched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageReference {
    /// A platform sandbox-template id.
    TemplateId(String),
    /// An Image Service logical image id.
    ImageServiceId(String),
    /// A registered name, resolvable in either catalog.
    Name(String),
}

/// Classify `reference` by its shape. Content digests (`cas-v1:<digest>` or
/// a bare 64-hex digest) are rejected: they are not names and no catalog
/// resolves them.
pub fn classify_image_reference(reference: &str) -> Result<ImageReference, SdkError> {
    let reference = reference.trim();
    if reference.is_empty() {
        return Err(SdkError::ClientError(
            "image reference must not be empty".to_string(),
        ));
    }
    if reference.starts_with(CAS_V1_REFERENCE_PREFIX) || is_lower_sha256_hex(reference) {
        return Err(SdkError::ClientError(format!(
            "'{reference}' is a content digest, not an image reference; use the image name or \
             its image_ id"
        )));
    }
    if reference.starts_with(SANDBOX_TEMPLATE_ID_PREFIX) {
        return Ok(ImageReference::TemplateId(reference.to_string()));
    }
    if reference.starts_with(IMAGE_SERVICE_ID_PREFIX) {
        return Ok(ImageReference::ImageServiceId(reference.to_string()));
    }
    Ok(ImageReference::Name(reference.to_string()))
}

pub(crate) fn is_lower_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Whether `reference` is an Image Service id (`image_<lowercase alnum>`).
pub fn is_image_service_id(reference: &str) -> bool {
    reference
        .strip_prefix(IMAGE_SERVICE_ID_PREFIX)
        .is_some_and(|rest| {
            !rest.is_empty()
                && rest
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        })
}

/// A name a build, import or registration may claim: names carrying a
/// reserved id prefix could never be addressed, since clients dispatch an
/// identifier to a catalog by that prefix, and digest references are not
/// names at all.
pub fn validate_registered_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("image name must not be empty".to_string());
    }
    for reserved in [SANDBOX_TEMPLATE_ID_PREFIX, IMAGE_SERVICE_ID_PREFIX] {
        if name.starts_with(reserved) {
            return Err(format!(
                "image name must not start with the reserved id prefix {reserved:?}"
            ));
        }
    }
    if name.starts_with(CAS_V1_REFERENCE_PREFIX) {
        return Err("image name must not be a cas-v1 reference".to_string());
    }
    Ok(())
}

/// The Image Service name grammar, mirrored client-side so a name that
/// catalog could never hold (uppercase, reserved prefixes, odd separators) is
/// routed straight to the platform registry instead of producing a 400 that a
/// caller could mistake for a miss. Lowercase repository components joined by
/// `/`, each made of alphanumeric runs joined by single `.`, `_` or `-`
/// separators, with an optional `:tag`.
pub fn validate_image_service_name(name: &str) -> Result<(), String> {
    validate_registered_name(name)?;
    if name.len() > 255 {
        return Err("image name must be at most 255 characters".to_string());
    }
    let (repository, tag) = match name.split_once(':') {
        Some((repository, tag)) => (repository, Some(tag)),
        None => (name, None),
    };
    for component in repository.split('/') {
        if !is_valid_name_component(component) {
            return Err(format!(
                "name component {component:?} must be lowercase alphanumeric segments joined by \
                 single '.', '_' or '-' separators"
            ));
        }
    }
    if let Some(tag) = tag {
        let valid_tag = !tag.is_empty()
            && tag.len() <= 128
            && tag.bytes().all(|b| {
                b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'.' | b'-')
            })
            && !tag.starts_with('.')
            && !tag.starts_with('-');
        if !valid_tag {
            return Err(format!("invalid tag {tag:?}"));
        }
    }
    Ok(())
}

fn is_valid_name_component(component: &str) -> bool {
    if component.is_empty() {
        return false;
    }
    let mut previous_was_separator = true; // rejects a leading separator
    for byte in component.bytes() {
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

/// Whether a template record describes a CAS image.
pub fn is_cas_image(template: &SandboxTemplate) -> bool {
    template.snapshot_format_version.as_deref() == Some(CAS_SNAPSHOT_FORMAT_VERSION)
}

/// Parse a template's `createdAt` for ordering: RFC 3339, or an epoch number
/// in seconds, milliseconds or microseconds.
pub fn created_at_sort_key(value: Option<&str>) -> Option<DateTime<Utc>> {
    let value = value?.trim();
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Some(timestamp.with_timezone(&Utc));
    }
    let timestamp: f64 = value.parse().ok()?;
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

/// The merge order: newest first, then name ascending, then CAS before
/// legacy. Records without a parseable creation time sort last.
pub fn sort_merged_images(images: &mut [SandboxTemplate]) {
    images.sort_by(|left, right| {
        created_at_sort_key(right.created_at.as_deref())
            .cmp(&created_at_sort_key(left.created_at.as_deref()))
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| is_cas_image(right).cmp(&is_cas_image(left)))
    });
}

// ---------------------------------------------------------------------------
// Image Service wire types
// ---------------------------------------------------------------------------

/// One logical image as the Image Service answers it (ADR 0107 D18): the
/// platform template shape in snake_case plus the content digest and build
/// provenance legacy templates lack.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ImageServiceImage {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) snapshot_id: String,
    pub(crate) snapshot_uri: String,
    #[serde(default)]
    pub(crate) snapshot_sandbox_id: Option<String>,
    pub(crate) snapshot_format_version: String,
    pub(crate) snapshot_size_bytes: u64,
    pub(crate) rootfs_disk_bytes: u64,
    pub(crate) rootfs_node_kind: String,
    pub(crate) public: bool,
    #[serde(default)]
    pub(crate) dockerfile: String,
    #[serde(default)]
    pub(crate) created_by_user_id: Option<String>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    #[serde(default)]
    pub(crate) image_id: Option<String>,
    #[serde(default)]
    pub(crate) build_id: Option<String>,
}

impl From<ImageServiceImage> for SandboxTemplate {
    fn from(image: ImageServiceImage) -> Self {
        let mut extra = BTreeMap::new();
        if let Some(image_id) = image.image_id {
            extra.insert("imageId".to_string(), Value::String(image_id));
        }
        if let Some(build_id) = image.build_id {
            extra.insert("buildId".to_string(), Value::String(build_id));
        }
        SandboxTemplate {
            id: Some(image.id),
            name: Some(image.name),
            snapshot_id: Some(image.snapshot_id),
            snapshot_sandbox_id: image.snapshot_sandbox_id,
            snapshot_uri: Some(image.snapshot_uri),
            snapshot_format_version: Some(image.snapshot_format_version),
            snapshot_size_bytes: Some(image.snapshot_size_bytes),
            rootfs_disk_bytes: Some(image.rootfs_disk_bytes),
            rootfs_node_kind: Some(image.rootfs_node_kind),
            public: Some(image.public),
            dockerfile: Some(image.dockerfile),
            created_by_user_id: image.created_by_user_id,
            created_at: Some(image.created_at),
            updated_at: Some(image.updated_at),
            extra,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct ImageServiceListPage {
    #[serde(default)]
    items: Vec<ImageServiceImage>,
    #[serde(default)]
    pagination: Option<ImageServicePagination>,
}

#[derive(Debug, Default, Deserialize)]
struct ImageServicePagination {
    #[serde(default)]
    next: Option<String>,
}

#[derive(Debug, Serialize)]
struct AdoptSnapshotRequest<'a> {
    name: &'a str,
    snapshot_id: &'a str,
    public: bool,
}

/// The Image Service error body (`{"error": "..."}`), reduced to its message
/// so callers print the service's words rather than raw JSON.
#[derive(Debug, Deserialize)]
struct ImageServiceErrorBody {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

fn tidy_error_message(message: String) -> String {
    match serde_json::from_str::<ImageServiceErrorBody>(&message) {
        Ok(body) => body.error.or(body.message).unwrap_or(message),
        Err(_) => message,
    }
}

fn tidy_error(error: SdkError) -> SdkError {
    match error {
        SdkError::ServerError { status, message } => SdkError::ServerError {
            status,
            message: tidy_error_message(message),
        },
        SdkError::Authentication(message) => SdkError::Authentication(tidy_error_message(message)),
        SdkError::Authorization(message) => SdkError::Authorization(tidy_error_message(message)),
        other => other,
    }
}

/// The Image Service half of the catalog, reached through the authenticated
/// `/images/v4` ingress path of the API host (or `TENSORLAKE_IMAGE_SERVICE_URL`
/// for local development, which then includes the service's `/v1` prefix).
#[derive(Clone)]
pub struct ImageServiceCatalog {
    client: Client,
}

impl ImageServiceCatalog {
    /// Share `client`'s credentials and scope headers, rebased onto the Image
    /// Service URL derived from `api_url`.
    pub fn new(client: &Client, api_url: &str) -> Self {
        Self {
            client: client.with_base_url(&cas_image_service_url(api_url)),
        }
    }

    /// Every image visible to the project, following the opaque cursor to
    /// the end of the collection.
    pub async fn list_all(&self) -> Result<Vec<SandboxTemplate>, SdkError> {
        let mut cursor: Option<String> = None;
        let mut items = Vec::new();
        loop {
            let mut query: Vec<(&str, &str)> = vec![("page_size", IMAGE_SERVICE_LIST_PAGE_SIZE)];
            if let Some(cursor) = cursor.as_deref() {
                query.push(("cursor", cursor));
            }
            let request = self
                .client
                .build_get_json_request("/images", Some(&query))?;
            let page = self
                .client
                .execute_json::<ImageServiceListPage>(request)
                .await
                .map_err(tidy_error)?
                .into_inner();
            items.extend(page.items.into_iter().map(SandboxTemplate::from));
            match page
                .pagination
                .and_then(|pagination| pagination.next)
                .filter(|next| !next.is_empty())
            {
                Some(next) => cursor = Some(next),
                None => return Ok(items),
            }
        }
    }

    /// The image registered under `name` in the caller's project, or a global
    /// image of that name. `Ok(None)` only on a clean 404.
    pub async fn get_by_name(&self, name: &str) -> Result<Option<SandboxTemplate>, SdkError> {
        self.get_optional(&format!("/names/{name}")).await
    }

    /// The image with the `image_` id `image_id`, when it is the caller's or
    /// global. `Ok(None)` only on a clean 404.
    pub async fn get_by_id(&self, image_id: &str) -> Result<Option<SandboxTemplate>, SdkError> {
        self.get_optional(&format!("/images/{image_id}")).await
    }

    async fn get_optional(&self, path: &str) -> Result<Option<SandboxTemplate>, SdkError> {
        let request = self.client.build_get_json_request(path, None)?;
        match self.client.execute_json::<ImageServiceImage>(request).await {
            Ok(traced) => Ok(Some(traced.into_inner().into())),
            Err(SdkError::ServerError { status, .. }) if status == StatusCode::NOT_FOUND => {
                Ok(None)
            }
            Err(error) => Err(tidy_error(error)),
        }
    }

    /// Delete the project's image named `name`. `Ok(false)` only on a clean
    /// 404; a 403 (the member role cannot delete) is reported as such.
    pub async fn delete_by_name(&self, name: &str) -> Result<bool, SdkError> {
        self.delete_optional(&format!("/names/{name}")).await
    }

    /// Delete the project's image with the `image_` id `image_id`.
    pub async fn delete_by_id(&self, image_id: &str) -> Result<bool, SdkError> {
        self.delete_optional(&format!("/images/{image_id}")).await
    }

    async fn delete_optional(&self, path: &str) -> Result<bool, SdkError> {
        let request = self.client.request(Method::DELETE, path).build()?;
        match self.client.execute_traced(request).await {
            Ok(_) => Ok(true),
            Err(SdkError::ServerError { status, .. }) if status == StatusCode::NOT_FOUND => {
                Ok(false)
            }
            Err(error) => Err(tidy_error(error)),
        }
    }

    /// Register a completed CAS snapshot as the image `name` (ADR 0107
    /// D9-D12). `public` asks for global scope, which the service grants only
    /// to allowlisted publisher projects.
    pub async fn adopt_snapshot(
        &self,
        name: &str,
        snapshot_id: &str,
        public: bool,
    ) -> Result<SandboxTemplate, SdkError> {
        let request = self.client.build_post_json_request(
            Method::POST,
            "/images/from-snapshot",
            &AdoptSnapshotRequest {
                name,
                snapshot_id,
                public,
            },
        )?;
        let image = self
            .client
            .execute_json::<ImageServiceImage>(request)
            .await
            .map_err(tidy_error)?
            .into_inner();
        Ok(image.into())
    }
}

/// Both catalogs behind one lookup, delete and list.
#[derive(Clone)]
pub struct SandboxImageCatalog {
    templates: SandboxTemplatesClient,
    image_service: ImageServiceCatalog,
}

impl SandboxImageCatalog {
    /// `scope` is the explicit organization/project pair PAT callers address;
    /// API keys are self-scoping and pass `None`.
    pub fn new(client: Client, api_url: &str, scope: Option<(String, String)>) -> Self {
        let image_service = ImageServiceCatalog::new(&client, api_url);
        let templates = match scope {
            Some((organization_id, project_id)) => {
                SandboxTemplatesClient::new(client, organization_id, project_id)
            }
            None => SandboxTemplatesClient::new_api_key_scoped(client),
        };
        Self {
            templates,
            image_service,
        }
    }

    /// Build the catalog from raw credentials, the way the language bindings
    /// and the image build paths do. With `use_scope_headers` (PAT auth) the
    /// organization and project are required.
    pub fn from_credentials(
        api_url: &str,
        bearer_token: &str,
        use_scope_headers: bool,
        organization_id: Option<&str>,
        project_id: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<Self, SdkError> {
        let scope = if use_scope_headers {
            match (organization_id, project_id) {
                (Some(organization_id), Some(project_id)) => {
                    Some((organization_id.to_string(), project_id.to_string()))
                }
                _ => {
                    return Err(SdkError::ClientError(
                        "organization and project IDs are required with personal access token \
                         authentication"
                            .to_string(),
                    ));
                }
            }
        } else {
            None
        };
        let client = client_builder(
            api_url,
            bearer_token,
            use_scope_headers,
            organization_id,
            project_id,
            user_agent,
        )
        .build()?;
        Ok(Self::new(client, api_url, scope))
    }

    pub fn image_service(&self) -> &ImageServiceCatalog {
        &self.image_service
    }

    pub fn templates(&self) -> &SandboxTemplatesClient {
        &self.templates
    }

    /// Resolve one reference to its record, or `None` when neither catalog
    /// knows it. An id is tried in its own catalog first; a miss there falls
    /// through to the by-name order.
    pub async fn find(&self, reference: &str) -> Result<Option<SandboxTemplate>, SdkError> {
        match classify_image_reference(reference)? {
            ImageReference::TemplateId(id) => {
                if let Some(template) = self.templates.get_by_id(&id).await? {
                    return Ok(Some(template.into_inner()));
                }
                self.find_by_name(&id).await
            }
            ImageReference::ImageServiceId(id) => {
                if let Some(image) = self.image_service.get_by_id(&id).await? {
                    return Ok(Some(image));
                }
                self.find_by_name(&id).await
            }
            ImageReference::Name(name) => self.find_by_name(&name).await,
        }
    }

    async fn find_by_name(&self, name: &str) -> Result<Option<SandboxTemplate>, SdkError> {
        if validate_image_service_name(name).is_ok()
            && let Some(image) = self.image_service.get_by_name(name).await?
        {
            return Ok(Some(image));
        }
        Ok(self
            .templates
            .find_by_name(name)
            .await?
            .map(Traced::into_inner))
    }

    /// Delete one image. Not found (both catalogs miss) surfaces as a 404
    /// [`SdkError::ServerError`] so every binding reports it the same way.
    /// Never retried on a lost response: a replay could delete a same-named
    /// image in the other catalog.
    pub async fn delete(&self, reference: &str) -> Result<(), SdkError> {
        let deleted = match classify_image_reference(reference)? {
            ImageReference::TemplateId(id) => {
                self.templates.delete_by_id(&id).await? || self.delete_by_name(&id).await?
            }
            ImageReference::ImageServiceId(id) => {
                self.image_service.delete_by_id(&id).await? || self.delete_by_name(&id).await?
            }
            ImageReference::Name(name) => self.delete_by_name(&name).await?,
        };
        if deleted {
            Ok(())
        } else {
            Err(SdkError::ServerError {
                status: StatusCode::NOT_FOUND,
                message: format!("image '{}' not found", reference.trim()),
            })
        }
    }

    async fn delete_by_name(&self, name: &str) -> Result<bool, SdkError> {
        if validate_image_service_name(name).is_ok()
            && self.image_service.delete_by_name(name).await?
        {
            return Ok(true);
        }
        self.templates.delete_by_name(name).await
    }

    /// Every image of both families, merged and sorted (see
    /// [`sort_merged_images`]). Either catalog failing fails the listing.
    pub async fn list(&self) -> Result<Vec<SandboxTemplate>, SdkError> {
        let (templates, images) =
            tokio::try_join!(self.templates.list(), self.image_service.list_all())?;
        let mut all = templates.into_inner();
        all.extend(images);
        sort_merged_images(&mut all);
        Ok(all)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn template(name: &str, created_at: Option<&str>, format: &str) -> SandboxTemplate {
        SandboxTemplate {
            name: Some(name.to_string()),
            created_at: created_at.map(str::to_string),
            snapshot_format_version: Some(format.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn references_dispatch_by_prefix_and_digests_are_refused() {
        assert_eq!(
            classify_image_reference("sandbox_template_abc").unwrap(),
            ImageReference::TemplateId("sandbox_template_abc".to_string())
        );
        assert_eq!(
            classify_image_reference(" image_abc123 ").unwrap(),
            ImageReference::ImageServiceId("image_abc123".to_string())
        );
        assert_eq!(
            classify_image_reference("team/app:v1").unwrap(),
            ImageReference::Name("team/app:v1".to_string())
        );
        for digest in [format!("cas-v1:{}", "a".repeat(64)), "a".repeat(64)] {
            let error = classify_image_reference(&digest).unwrap_err().to_string();
            assert!(
                error.contains("use the image name or its image_ id"),
                "{error}"
            );
        }
        assert!(classify_image_reference("  ").is_err());
    }

    #[test]
    fn reserved_prefixes_are_not_names() {
        assert!(validate_registered_name("image_x").is_err());
        assert!(validate_registered_name("sandbox_template_x").is_err());
        assert!(validate_registered_name(&format!("cas-v1:{}", "a".repeat(64))).is_err());
        assert!(validate_registered_name("MyImage").is_ok());
        assert!(validate_registered_name("team/app:v1").is_ok());
    }

    #[test]
    fn image_service_grammar_routes_only_lowercase_names() {
        for name in [
            "team/app",
            "team/app:v1.2-rc",
            "python",
            "ghcr.io/org/app:v1",
        ] {
            assert!(validate_image_service_name(name).is_ok(), "{name}");
        }
        for name in [
            "MyImage",
            "team//app",
            "-leading",
            "team/app:.tag",
            "a b",
            "image_abc",
            "sandbox_template_abc",
        ] {
            assert!(validate_image_service_name(name).is_err(), "{name}");
        }
        assert!(is_image_service_id("image_abc123"));
        assert!(!is_image_service_id("image_"));
        assert!(!is_image_service_id("image_ABC"));
        assert!(!is_image_service_id("sandbox_template_x"));
    }

    #[test]
    fn created_at_parses_rfc3339_and_epoch_numbers() {
        let rfc = created_at_sort_key(Some("2026-10-15T00:00:00.000Z")).unwrap();
        assert_eq!(rfc.timestamp(), 1_792_022_400);
        assert_eq!(
            created_at_sort_key(Some("1792022400")).unwrap().timestamp(),
            1_792_022_400
        );
        assert_eq!(
            created_at_sort_key(Some("1792022400000"))
                .unwrap()
                .timestamp(),
            1_792_022_400
        );
        assert_eq!(created_at_sort_key(Some("not a time")), None);
        assert_eq!(created_at_sort_key(None), None);
    }

    #[test]
    fn merged_listing_sorts_newest_then_name_then_cas_first() {
        let mut images = vec![
            template("zeta", Some("2026-10-14T00:00:00Z"), "durable_archive_v1"),
            template("alpha", Some("2026-10-15T00:00:00Z"), "durable_archive_v1"),
            template(
                "alpha",
                Some("2026-10-15T00:00:00Z"),
                CAS_SNAPSHOT_FORMAT_VERSION,
            ),
            template("beta", None, "durable_archive_v1"),
            template("beta", Some("2026-10-15T00:00:00Z"), "durable_archive_v1"),
        ];
        sort_merged_images(&mut images);
        let order: Vec<(String, bool)> = images
            .iter()
            .map(|image| (image.name.clone().unwrap(), is_cas_image(image)))
            .collect();
        assert_eq!(
            order,
            vec![
                ("alpha".to_string(), true),
                ("alpha".to_string(), false),
                ("beta".to_string(), false),
                ("zeta".to_string(), false),
                ("beta".to_string(), false),
            ]
        );
    }

    #[test]
    fn image_service_records_map_onto_the_template_shape() {
        let image: ImageServiceImage = serde_json::from_value(serde_json::json!({
            "id": "image_abc",
            "name": "team/app",
            "snapshot_id": "snap123",
            "snapshot_uri": "snapshot://snap123",
            "snapshot_sandbox_id": null,
            "snapshot_format_version": CAS_SNAPSHOT_FORMAT_VERSION,
            "snapshot_size_bytes": 42,
            "rootfs_disk_bytes": 10_737_418_240_u64,
            "rootfs_node_kind": "base",
            "public": false,
            "dockerfile": "",
            "created_by_user_id": null,
            "created_at": "2026-10-15T00:00:00.000Z",
            "updated_at": "2026-10-15T00:00:00.000Z",
            "image_id": "c".repeat(64),
            "build_id": "build-1",
        }))
        .unwrap();
        let template: SandboxTemplate = image.into();
        let json = serde_json::to_value(&template).unwrap();
        assert_eq!(json["id"], "image_abc");
        assert_eq!(json["snapshotId"], "snap123");
        assert_eq!(json["snapshotUri"], "snapshot://snap123");
        assert_eq!(json["snapshotFormatVersion"], CAS_SNAPSHOT_FORMAT_VERSION);
        assert_eq!(json["rootfsDiskBytes"], 10_737_418_240_u64);
        assert_eq!(json["public"], false);
        assert_eq!(json["dockerfile"], "");
        assert_eq!(json["createdAt"], "2026-10-15T00:00:00.000Z");
        assert_eq!(json["imageId"], "c".repeat(64));
        assert_eq!(json["buildId"], "build-1");
        assert!(is_cas_image(&template));
    }

    #[test]
    fn image_service_error_bodies_reduce_to_their_message() {
        let error = tidy_error(SdkError::ServerError {
            status: StatusCode::CONFLICT,
            message: r#"{"error":"image is being deleted"}"#.to_string(),
        });
        assert!(error.to_string().contains("image is being deleted"));
        assert!(!error.to_string().contains('{'));
        let plain = tidy_error(SdkError::Authorization("Forbidden".to_string()));
        assert!(plain.to_string().contains("Forbidden"));
    }
}
