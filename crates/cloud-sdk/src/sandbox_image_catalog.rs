//! The merged sandbox-image catalog: one client-side view over the platform
//! sandbox-template registry (legacy `.tlsnap` images) and the CAS image registry
//! catalog (CAS images), so `describe`, `ls`, `rm` and the SDK lookups behave
//! identically for both families.
//!
//! Dispatch is by reference shape: a `sandbox_template_` id is a platform
//! template, an `image_` id is a CAS image, anything else is a
//! name. A name is looked up in the CAS image registry first (when it fits that
//! catalog's grammar) and only a clean 404 falls through to the platform
//! registry; any other failure is reported, never mistaken for a miss. Single
//! image operations never list and match. Listings concatenate both catalogs
//! and sort newest first, then by name, with CAS images ahead of legacy ones
//! on a tie.
//!
//! The CAS image registry answers the platform template shape (snake_case, RFC 3339
//! timestamps); this module maps it onto [`SandboxTemplate`] so callers see
//! one record kind for both families.

use std::{collections::BTreeMap, time::Duration};

use chrono::{DateTime, TimeZone, Utc};
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::{
    Traced,
    client::Client,
    error::SdkError,
    image_builds::{DEFAULT_WAIT_LIMIT, ImageBuildError, InterruptSignal, new_client_request_id},
    sandbox_images::{SandboxImageBuildEvent, cas_image_service_url, client_builder},
    sandbox_templates::{SandboxTemplatesClient, models::SandboxTemplate},
};

/// Prefix of a CAS image id.
pub const IMAGE_SERVICE_ID_PREFIX: &str = "image_";
/// Prefix of a platform sandbox-template id.
pub const SANDBOX_TEMPLATE_ID_PREFIX: &str = "sandbox_template_";
/// The snapshot format version of a CAS (content-addressed streaming) image.
pub const CAS_SNAPSHOT_FORMAT_VERSION: &str = "content_addressed_streaming_v1";
/// The retired immutable reference scheme. Images are addressed by name or
/// `image_` id; a digest reference is an error, not a lookup.
const CAS_V1_REFERENCE_PREFIX: &str = "cas-v1:";
const IMAGE_SERVICE_LIST_PAGE_SIZE: &str = "100";

/// What [`SandboxImageCatalog::delete_reporting`] deleted.
#[derive(Debug)]
pub struct ImageDeletion {
    /// The family of the image the reference named.
    pub family: DeletedImageFamily,
    /// Set when a name's CAS image was deleted and a legacy image of the
    /// same name existed.
    pub same_named_legacy: Option<LegacyImageDeletion>,
}

impl ImageDeletion {
    fn only(family: DeletedImageFamily) -> Self {
        Self {
            family,
            same_named_legacy: None,
        }
    }
}

/// The catalog an image was deleted from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletedImageFamily {
    /// A CAS image, deleted through the CAS image registry.
    Cas,
    /// A legacy image, deleted from the platform template registry.
    Legacy,
}

/// The legacy image a name's CAS delete left behind, which can no longer
/// start sandboxes. `id` is its `sandbox_template_` id when known.
#[derive(Debug)]
pub enum LegacyImageDeletion {
    Deleted {
        id: Option<String>,
    },
    /// Not deleted; deleting the name or id again finishes the job.
    Failed {
        id: Option<String>,
        error: SdkError,
    },
}

/// How a user-supplied image reference is dispatched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageReference {
    /// A platform sandbox-template id.
    TemplateId(String),
    /// A CAS image id.
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

/// Whether `reference` is a CAS image id (`image_<lowercase alnum>`).
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

/// The CAS image registry name grammar, mirrored client-side so a name that
/// catalog could never hold (uppercase, reserved prefixes, odd separators) is
/// routed straight to the legacy image registry instead of producing a 400 that a
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

/// One logical image as the CAS image registry answers it (ADR 0107 as amended
/// by the API trim): the identity clients key on, its visibility, its sizes
/// and its timestamps. The platform template shape callers of both image
/// families receive is derived locally by [`Self::into_template`]; the
/// service sends no digest, parent pins, publisher provenance or template
/// placeholders.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageServiceImage {
    /// The mutable `image_` id: names the current image under `name`.
    pub id: String,
    pub name: String,
    /// The engine row the image is registered against; a rebuild changes it.
    pub snapshot_id: String,
    /// Visible to every project.
    pub global: bool,
    /// Stored CAS bytes of the content.
    pub physical_size_bytes: u64,
    /// Guest-visible size of the rootfs block device.
    pub logical_size_bytes: u64,
    pub created_at: String,
    pub updated_at: String,
}

impl ImageServiceImage {
    /// The platform template shape callers of both image families receive.
    pub fn into_template(self) -> SandboxTemplate {
        self.into()
    }
}

impl From<ImageServiceImage> for SandboxTemplate {
    fn from(image: ImageServiceImage) -> Self {
        // Every Image Service record is content-addressed by construction;
        // the handle, format label, node kind and the legacy template
        // placeholders are supplied here so the merged catalog, `is_cas_image`
        // and the CAS-first sort keep one record shape. No `imageId` or
        // `buildId` extras: the service exposes neither.
        SandboxTemplate {
            id: Some(image.id),
            name: Some(image.name),
            snapshot_uri: Some(format!("snapshot://{}", image.snapshot_id)),
            snapshot_id: Some(image.snapshot_id),
            snapshot_sandbox_id: None,
            snapshot_format_version: Some(CAS_SNAPSHOT_FORMAT_VERSION.to_string()),
            snapshot_size_bytes: Some(image.physical_size_bytes),
            rootfs_disk_bytes: Some(image.logical_size_bytes),
            rootfs_node_kind: Some("base".to_string()),
            public: Some(image.global),
            dockerfile: Some(String::new()),
            created_by_user_id: None,
            created_at: Some(image.created_at),
            updated_at: Some(image.updated_at),
            extra: BTreeMap::new(),
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

/// The CAS image registry error body (`{"error": "..."}`), reduced to its message
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

/// The CAS image registry half of the catalog, reached through the authenticated
/// `/images/v4` ingress path of the API host (or `TENSORLAKE_IMAGE_SERVICE_URL`
/// for local development, which then includes the service's `/v1` prefix).
#[derive(Clone)]
pub struct ImageServiceCatalog {
    client: Client,
}

impl ImageServiceCatalog {
    /// Share `client`'s credentials and scope headers, rebased onto the CAS
    /// image registry URL derived from `api_url`.
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
        self.get_optional(&format!("/images/name/{name}")).await
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
    /// 404; a 403 (the member role cannot delete) is reported as such, and
    /// so is a 409 `image_build_in_progress` (the name has an unfinished
    /// build: cancel it with the build id first; deleting never cancels).
    /// A 202 means the deletion is still in progress and counts as done.
    /// Deleting removes the published image only; a name whose only state is
    /// an unfinished first build has no image and answers 404.
    pub async fn delete_by_name(&self, name: &str) -> Result<bool, SdkError> {
        self.delete_optional(&format!("/images/name/{name}")).await
    }

    /// Delete the project's image with the `image_` id `image_id`; the same
    /// answers as [`Self::delete_by_name`].
    pub async fn delete_by_id(&self, image_id: &str) -> Result<bool, SdkError> {
        self.delete_optional(&format!("/images/{image_id}")).await
    }

    /// `Ok(true)` on any success (204 done, 202 deletion in progress),
    /// `Ok(false)` only on a clean 404. 202, 409, 5xx and transport failures
    /// are never mistaken for absence, and a lost response is never
    /// replayed here: the merged catalog decides what a miss means.
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
    ///
    /// The request is admitted under a fresh client request id
    /// (`Idempotency-Key`), retained across its transport retries. The
    /// service answers 201 with the published image, or `503
    /// publication_pending` naming the build when its bounded inline wait
    /// expired; the caller then retries the identical request under the same
    /// key ([`Self::adopt_snapshot_and_wait`] does so up to its registration
    /// wait bound). A 409 `image_build_in_progress` names the active
    /// operation to cancel first. Transport acceptance is never reported as
    /// completion.
    pub async fn adopt_snapshot(
        &self,
        name: &str,
        snapshot_id: &str,
        public: bool,
    ) -> Result<SnapshotAdoption, ImageBuildError> {
        let request_id = new_client_request_id();
        let wait = RegistrationWait::new(RegistrationWaitPolicy::default());
        self.adopt_snapshot_with_request_id(
            name,
            snapshot_id,
            public,
            &request_id,
            &wait,
            &InterruptSignal::new(),
        )
        .await
    }

    /// [`Self::adopt_snapshot`], retrying a pending registration under the
    /// same key until it publishes or the registration wait bound expires.
    /// Returns the published image in the template shape, so registration
    /// flows keep their completed-image result. `interrupt` cancels a pending
    /// Direct operation through the bounded handshake.
    pub async fn adopt_snapshot_and_wait<F>(
        &self,
        name: &str,
        snapshot_id: &str,
        public: bool,
        interrupt: &InterruptSignal,
        emit: F,
    ) -> Result<SandboxTemplate, ImageBuildError>
    where
        F: FnMut(SandboxImageBuildEvent),
    {
        self.adopt_snapshot_and_wait_with(
            name,
            snapshot_id,
            public,
            RegistrationWaitPolicy::default(),
            interrupt,
            emit,
        )
        .await
    }

    /// [`Self::adopt_snapshot_and_wait`] under an explicit wait policy. A
    /// `publication_pending` answer is progress, not unavailability: it
    /// resets the per-request reconnect budget and the loop re-POSTs the
    /// identical request under the same key after `poll_interval`, every
    /// request and sleep clamped to the absolute registration deadline, so a
    /// request still in flight at the deadline is abandoned there. Expiry is
    /// [`ImageBuildError::Timeout`] naming the pending build (or the request
    /// id while no answer named one); the operation keeps running.
    pub(crate) async fn adopt_snapshot_and_wait_with<F>(
        &self,
        name: &str,
        snapshot_id: &str,
        public: bool,
        policy: RegistrationWaitPolicy,
        interrupt: &InterruptSignal,
        mut emit: F,
    ) -> Result<SandboxTemplate, ImageBuildError>
    where
        F: FnMut(SandboxImageBuildEvent),
    {
        let request_id = new_client_request_id();
        let mut wait = RegistrationWait::new(policy);
        loop {
            match self
                .adopt_snapshot_with_request_id(
                    name,
                    snapshot_id,
                    public,
                    &request_id,
                    &wait,
                    interrupt,
                )
                .await?
            {
                SnapshotAdoption::Published(image) => return Ok((*image).into_template()),
                SnapshotAdoption::Pending { build_id } => {
                    if wait.build_id.is_none() {
                        emit(SandboxImageBuildEvent::Accepted {
                            build_id: build_id.clone(),
                            client_request_id: Some(request_id.clone()),
                        });
                    }
                    wait.build_id = Some(build_id.clone());
                    emit(SandboxImageBuildEvent::Status(format!(
                        "Registration of '{name}' pending (build {build_id}); waiting for \
                         publication..."
                    )));
                    let now = Instant::now();
                    if now >= wait.deadline {
                        return Err(wait.timeout(&request_id));
                    }
                    let delay = wait.policy.poll_interval.min(wait.deadline - now);
                    tokio::select! {
                        biased;
                        _ = interrupt.triggered() => {
                            let report = crate::image_service_builds::interruption_for_build(
                                &self.client,
                                &build_id,
                                Some(&request_id),
                            )
                            .await;
                            emit(SandboxImageBuildEvent::Interrupted(report.clone()));
                            return Err(ImageBuildError::Interrupted(report));
                        }
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
    }

    async fn adopt_snapshot_with_request_id(
        &self,
        name: &str,
        snapshot_id: &str,
        public: bool,
        request_id: &str,
        wait: &RegistrationWait,
        interrupt: &InterruptSignal,
    ) -> Result<SnapshotAdoption, ImageBuildError> {
        let body = serde_json::to_value(AdoptSnapshotRequest {
            name,
            snapshot_id,
            public,
        })?;
        crate::image_service_builds::admit_direct(
            &self.client,
            "/images/from-snapshot",
            &body,
            request_id,
            wait,
            interrupt,
        )
        .await
    }
}

/// The answer to a direct publication: done, or admitted as an operation
/// that outlived the service's bounded inline wait.
#[derive(Debug, Clone, PartialEq)]
pub enum SnapshotAdoption {
    /// 201: the image is published.
    Published(Box<ImageServiceImage>),
    /// 503 `publication_pending`: the operation is admitted and still
    /// running. Retry the identical request under the same key to join it;
    /// `build_id` is the operation to inspect or cancel meanwhile.
    Pending { build_id: String },
}

/// How long a registration keeps retrying `publication_pending` answers
/// before giving up on waiting (the operation itself keeps running), and how
/// long it pauses between identical POSTs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistrationWaitPolicy {
    pub limit: Duration,
    pub poll_interval: Duration,
}

impl Default for RegistrationWaitPolicy {
    /// The same bound every build wait has without a service deadline or a
    /// caller timeout, and the build poll interval.
    fn default() -> Self {
        Self {
            limit: DEFAULT_WAIT_LIMIT,
            poll_interval: crate::image_service_builds::BUILD_POLL_INTERVAL,
        }
    }
}

/// One registration's absolute deadline and what it has learnt so far.
#[derive(Debug, Clone)]
pub(crate) struct RegistrationWait {
    pub(crate) policy: RegistrationWaitPolicy,
    pub(crate) started: Instant,
    pub(crate) deadline: Instant,
    /// The build a `publication_pending` answer named, once one did.
    pub(crate) build_id: Option<String>,
}

impl RegistrationWait {
    pub(crate) fn new(policy: RegistrationWaitPolicy) -> Self {
        let started = Instant::now();
        Self {
            policy,
            started,
            deadline: started + policy.limit,
            build_id: None,
        }
    }

    /// The timeout the wait reports: the pending build when an answer named
    /// one, otherwise the request id, so the caller can still find the
    /// operation with the cancel route's request-id selector.
    pub(crate) fn timeout(&self, request_id: &str) -> ImageBuildError {
        ImageBuildError::Timeout {
            build_id: self
                .build_id
                .clone()
                .unwrap_or_else(|| format!("(request {request_id})")),
            waited: self.started.elapsed(),
        }
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
    ///
    /// Accepted merged-catalog behavior for legacy images: a name that fits
    /// the CAS image registry's grammar is looked up there first, and only a
    /// clean 404 falls through to the legacy image registry. Any other CAS
    /// image registry answer (unavailability, a deploy 5xx) fails the lookup
    /// of a legacy image name, where the released SDKs touched only the
    /// legacy image registry. A `sandbox_template_` id never consults the CAS
    /// image registry.
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
    ///
    /// Accepted merged-catalog behavior for legacy images: deleting a legacy
    /// image removes it from the legacy image registry (the released SDKs
    /// only made it unusable and left it listed), and an unknown name is an
    /// error rather than a silent success. An untyped name probes the CAS
    /// image registry first and falls through to the legacy image registry
    /// only on a clean 404; a 202 (deletion in progress) is success, and a
    /// 409 `image_build_in_progress`, a 5xx or a transport failure is
    /// reported as is. Deleting removes published images only; an unfinished
    /// build is cancelled with its build id, never by deleting its name.
    ///
    /// A name is deleted from both registries: once its CAS image is
    /// deleted, a legacy image of the same name can no longer start
    /// sandboxes, so it is deleted too. Failing to delete that legacy image
    /// does not fail the call, since the CAS image is already gone;
    /// [`Self::delete_reporting`] reports it. An `image_` or
    /// `sandbox_template_` id deletes only the image it names.
    pub async fn delete(&self, reference: &str) -> Result<(), SdkError> {
        self.delete_reporting(reference).await.map(|_| ())
    }

    /// [`Self::delete`], also reporting the deleted image's family and what
    /// happened to a same-named legacy image.
    pub async fn delete_reporting(&self, reference: &str) -> Result<ImageDeletion, SdkError> {
        let deleted = match classify_image_reference(reference)? {
            ImageReference::TemplateId(id) => {
                if self.templates.delete_by_id(&id).await? {
                    Some(ImageDeletion::only(DeletedImageFamily::Legacy))
                } else {
                    self.delete_by_name(&id).await?
                }
            }
            ImageReference::ImageServiceId(id) => {
                if self.image_service.delete_by_id(&id).await? {
                    Some(ImageDeletion::only(DeletedImageFamily::Cas))
                } else {
                    self.delete_by_name(&id).await?
                }
            }
            ImageReference::Name(name) => self.delete_by_name(&name).await?,
        };
        deleted.ok_or_else(|| SdkError::ServerError {
            status: StatusCode::NOT_FOUND,
            message: format!("image '{}' not found", reference.trim()),
        })
    }

    async fn delete_by_name(&self, name: &str) -> Result<Option<ImageDeletion>, SdkError> {
        if validate_image_service_name(name).is_ok()
            && self.image_service.delete_by_name(name).await?
        {
            return Ok(Some(ImageDeletion {
                family: DeletedImageFamily::Cas,
                same_named_legacy: self.delete_same_named_legacy_image(name).await,
            }));
        }
        Ok(self
            .templates
            .delete_by_name(name)
            .await?
            .then(|| ImageDeletion::only(DeletedImageFamily::Legacy)))
    }

    /// Delete the legacy image named `name` after its CAS namesake was
    /// deleted. `None` when there is none. A failure is reported rather
    /// than raised, because the CAS delete already succeeded.
    async fn delete_same_named_legacy_image(&self, name: &str) -> Option<LegacyImageDeletion> {
        let template = match self.templates.find_by_name(name).await {
            Ok(template) => template?.into_inner(),
            Err(error) => return Some(LegacyImageDeletion::Failed { id: None, error }),
        };
        let deleted = match template.id.as_deref() {
            Some(id) => self.templates.delete_by_id(id).await,
            None => self.templates.delete_by_name(name).await,
        };
        match deleted {
            Ok(true) => Some(LegacyImageDeletion::Deleted { id: template.id }),
            Ok(false) => None,
            Err(error) => Some(LegacyImageDeletion::Failed {
                id: template.id,
                error,
            }),
        }
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
        // The service answers the trimmed eight fields; the template shape
        // (handle, format label, node kind, placeholders) is derived here.
        let image: ImageServiceImage = serde_json::from_value(serde_json::json!({
            "id": "image_abc",
            "name": "team/app",
            "snapshot_id": "snap123",
            "global": true,
            "physical_size_bytes": 42,
            "logical_size_bytes": 10_737_418_240_u64,
            "created_at": "2026-10-15T00:00:00.000Z",
            "updated_at": "2026-10-15T00:00:00.000Z",
        }))
        .unwrap();
        let template: SandboxTemplate = image.into();
        let json = serde_json::to_value(&template).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "id": "image_abc",
                "name": "team/app",
                "snapshotId": "snap123",
                "snapshotSandboxId": null,
                "snapshotUri": "snapshot://snap123",
                "snapshotFormatVersion": CAS_SNAPSHOT_FORMAT_VERSION,
                "snapshotSizeBytes": 42,
                "rootfsDiskBytes": 10_737_418_240_u64,
                "rootfsNodeKind": "base",
                "public": true,
                "dockerfile": "",
                "createdAt": "2026-10-15T00:00:00.000Z",
                "updatedAt": "2026-10-15T00:00:00.000Z",
            }),
            "no imageId or buildId extras"
        );
        assert!(is_cas_image(&template));
        // A pre-trim record is not a current record.
        assert!(
            serde_json::from_value::<ImageServiceImage>(serde_json::json!({
                "id": "image_abc",
                "name": "team/app",
                "snapshot_id": "snap123",
                "public": true,
                "rootfs_disk_bytes": 10_737_418_240_u64,
            }))
            .is_err()
        );
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
