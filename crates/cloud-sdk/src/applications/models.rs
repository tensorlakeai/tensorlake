use chrono::{DateTime, Utc};
use derive_builder::Builder;
use futures::Stream;
use reqwest::header::HeaderValue;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json;
use std::{collections::HashMap, fmt::Display, pin::Pin};
use uuid::Uuid;

use crate::error::SdkError;

/// A custom DateTime<Utc> type that handles RFC3339 timestamps with missing 'Z' timezone indicator.
/// When deserializing, if the timestamp doesn't end with 'Z', it's automatically appended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct Rfc3339DateTime(DateTime<Utc>);

impl Rfc3339DateTime {
    pub fn now() -> Self {
        Self(Utc::now())
    }
}

impl From<DateTime<Utc>> for Rfc3339DateTime {
    fn from(value: DateTime<Utc>) -> Self {
        Self(value)
    }
}

impl Display for Rfc3339DateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.to_rfc3339())
    }
}

impl<'de> Deserialize<'de> for Rfc3339DateTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut s = String::deserialize(deserializer)?;
        if !s.ends_with("Z") && !s.ends_with("+00:00") {
            s.push('Z');
        }

        DateTime::parse_from_rfc3339(&s)
            .map(|dt| Rfc3339DateTime(dt.with_timezone(&Utc)))
            .map_err(serde::de::Error::custom)
    }
}

impl std::ops::Deref for Rfc3339DateTime {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(feature = "openapi")]
impl utoipa::PartialSchema for Rfc3339DateTime {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::Schema> {
        utoipa::openapi::RefOr::T(utoipa::openapi::Schema::Object(
            utoipa::openapi::ObjectBuilder::new()
                .schema_type(utoipa::openapi::schema::SchemaType::Type(
                    utoipa::openapi::schema::Type::String,
                ))
                .format(Some(utoipa::openapi::SchemaFormat::KnownFormat(
                    utoipa::openapi::KnownFormat::DateTime,
                )))
                .description(Some("RFC 3339 datetime"))
                .build(),
        ))
    }
}

#[cfg(feature = "openapi")]
impl utoipa::ToSchema for Rfc3339DateTime {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Rfc3339DateTime")
    }
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize, Builder)]
pub struct ApplicationManifest {
    #[builder(setter(into))]
    pub name: String,
    #[builder(setter(into), default)]
    pub description: String,
    #[builder(setter(into), default)]
    pub tags: HashMap<String, String>,
    #[builder(setter(into))]
    pub version: String,
    pub functions: HashMap<String, FunctionManifest>,
    pub entrypoint: Entrypoint,
}

impl ApplicationManifest {
    pub fn builder() -> ApplicationManifestBuilder {
        ApplicationManifestBuilder::default()
    }
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize, Builder)]
pub struct Entrypoint {
    #[builder(setter(into))]
    pub function_name: String,
    #[builder(setter(into))]
    pub input_serializer: String,
    #[builder(setter(into))]
    pub output_serializer: String,
    #[builder(setter(into, strip_option), default)]
    pub output_type_hints_base64: Option<String>,
}

impl Entrypoint {
    pub fn builder() -> EntrypointBuilder {
        EntrypointBuilder::default()
    }
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize, Builder)]
pub struct FunctionManifest {
    #[builder(setter(into))]
    pub name: String,
    #[builder(setter(into), default)]
    pub description: String,
    #[builder(default)]
    pub is_api: bool,
    #[builder(setter(into, strip_option), default)]
    pub secret_names: Vec<String>,
    #[builder(default)]
    pub initialization_timeout_sec: i32,
    #[builder(default)]
    pub timeout_sec: i32,
    pub resources: Resources,
    #[builder(default)]
    pub retry_policy: RetryPolicy,
    #[builder(setter(into, strip_option), default)]
    pub cache_key: Option<String>,
    #[builder(setter(into), default)]
    pub parameters: Vec<Parameter>,
    pub return_type: serde_json::Value,
    #[builder(default)]
    pub placement_constraints: PlacementConstraintsManifest,
    #[builder(default)]
    pub max_concurrency: i32,
}

impl FunctionManifest {
    pub fn builder() -> FunctionManifestBuilder {
        FunctionManifestBuilder::default()
    }
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize, Builder)]
pub struct Resources {
    pub cpus: f64,
    pub memory_mb: i64,
    pub ephemeral_disk_mb: i64,
    #[builder(setter(into), default)]
    pub gpus: Vec<String>,
}

impl Resources {
    pub fn builder() -> ResourcesBuilder {
        ResourcesBuilder::default()
    }
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize, Builder)]
pub struct RetryPolicy {
    pub max_retries: i32,
    pub initial_delay_sec: f64,
    pub max_delay_sec: f64,
    pub delay_multiplier: f64,
}

impl RetryPolicy {
    pub fn builder() -> RetryPolicyBuilder {
        RetryPolicyBuilder::default()
    }
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize, Builder)]
pub struct PlacementConstraintsManifest {
    #[builder(setter(into), default)]
    pub filter_expressions: Vec<String>,
}

impl PlacementConstraintsManifest {
    pub fn builder() -> PlacementConstraintsManifestBuilder {
        PlacementConstraintsManifestBuilder::default()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Builder)]
pub struct DataType {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option), default)]
    pub typ: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option), default)]
    pub items: Option<Box<DataType>>,
    #[serde(
        rename = "additionalProperties",
        skip_serializing_if = "Option::is_none"
    )]
    #[builder(setter(into, strip_option), default)]
    pub additional_properties: Option<Box<DataType>>,
    #[serde(rename = "anyOf", skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option), default)]
    pub any_of: Option<Vec<DataType>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option), default)]
    pub description: Option<String>,
    #[serde(rename = "default", skip_serializing_if = "Option::is_none")]
    #[builder(setter(into, strip_option), default)]
    pub default_value: Option<serde_json::Value>,
}

impl DataType {
    pub fn builder() -> DataTypeBuilder {
        DataTypeBuilder::default()
    }

    pub fn to_json_value(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self)
    }

    pub fn to_json_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Builder)]
pub struct Parameter {
    #[builder(setter(into))]
    pub name: String,
    #[builder(setter(into, strip_option), default)]
    pub description: Option<String>,
    #[builder(setter(into), default = "true")]
    pub required: bool,
    #[builder(setter(into))]
    pub data_type: DataType,
}

impl Parameter {
    pub fn builder() -> ParameterBuilder {
        ParameterBuilder::default()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Allocation {
    pub attempt_number: i32,
    pub created_at: u128,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_duration_ms: Option<i64>,
    pub executor_id: String,
    pub container_id: String,
    pub function_name: String,
    pub id: String,
    pub outcome: FunctionRunOutcome,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct Application {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    pub description: String,
    pub entrypoint: EntryPointManifest,
    pub functions: HashMap<String, ApplicationFunction>,
    pub name: String,
    #[serde(skip_deserializing, default)]
    pub namespace: String,
    pub tags: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tombstoned: Option<bool>,
    #[serde(skip_serializing, default)]
    pub state: Option<ApplicationState>,
    pub version: String,
}

#[derive(Clone, Default, Debug, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApplicationState {
    #[default]
    Active,
    Disabled {
        reason: String,
    },
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApplicationFunction {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_key: Option<String>,
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initialization_timeout_sec: Option<i32>,
    pub max_concurrency: i32,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Vec<ParameterMetadata>>,
    pub placement_constraints: PlacementConstraints,
    pub resources: FunctionResources,
    pub retry_policy: NodeRetryPolicy,
    pub return_type: Option<serde_json::Value>,
    pub secret_names: Vec<String>,
    pub timeout_sec: i32,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApplicationRequests {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub requests: Vec<ShallowRequest>,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApplicationsList {
    pub applications: Vec<Application>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum CursorDirection {
    Forward,
    Backward,
}

impl std::fmt::Display for CursorDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CursorDirection::Forward => write!(f, "forward"),
            CursorDirection::Backward => write!(f, "backward"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DownloadOutput {
    pub content_length: Option<HeaderValue>,
    pub content_type: Option<HeaderValue>,
    pub content: bytes::Bytes,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct EntryPointManifest {
    pub function_name: String,
    pub input_serializer: String,
    pub output_serializer: String,
    pub output_type_hints_base64: String,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct FunctionResources {
    pub cpus: f64,
    pub gpus: Vec<GpuResources>,
    pub memory_mb: i64,
    pub ephemeral_disk_mb: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FunctionRun {
    pub created_at: u128,
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub application: String,
    pub application_version: String,
    pub allocations: Vec<Allocation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<FunctionRunOutcome>,
    pub status: FunctionRunStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum FunctionRunOutcome {
    #[serde(alias = "Unknown")]
    Unknown,
    #[serde(alias = "Undefined")]
    Undefined,
    #[serde(alias = "Success")]
    Success,
    #[serde(alias = "Failure")]
    Failure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FunctionRunStatus {
    #[serde(alias = "Pending")]
    Pending,
    #[serde(alias = "Enqueued")]
    Enqueued,
    #[serde(alias = "Running")]
    Running,
    #[serde(alias = "Completed")]
    Completed,
    #[serde(alias = "Failed")]
    Failed,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct GpuResources {
    pub count: u32,
    pub model: String,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeRetryPolicy {
    pub max_retries: i32,
    pub initial_delay_sec: f64,
    pub max_delay_sec: f64,
    pub delay_multiplier: f64,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct ParameterMetadata {
    pub data_type: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub name: String,
    pub required: bool,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlacementConstraints {
    /// List of label filter expressions in the format "key=value", "key!=value", etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locations: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<RequestOutcome>,
    #[serde(skip_serializing_if = "Option::is_none", alias = "failureReason")]
    pub failure_reason: Option<RequestFailureReason>,
    #[serde(alias = "applicationVersion")]
    pub application_version: String,
    #[serde(alias = "createdAt")]
    pub created_at: u128,
    #[serde(skip_serializing_if = "Option::is_none", alias = "requestError")]
    pub request_error: Option<RequestError>,
    #[serde(alias = "functionRuns")]
    pub function_runs: Vec<FunctionRun>,
    #[serde(
        skip_serializing_if = "Vec::is_empty",
        default,
        alias = "progressUpdates"
    )]
    pub progress_updates: Vec<RequestStateChangeEvent>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        default,
        alias = "updatesPaginationToken"
    )]
    pub updates_pagination_token: Option<String>,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct RequestError {
    pub function_name: String,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum RequestFailureReason {
    #[serde(alias = "unknown")]
    Unknown,
    #[serde(alias = "internalerror", alias = "internal_error")]
    InternalError,
    #[serde(alias = "functionerror", alias = "function_error")]
    FunctionError,
    #[serde(alias = "requesterror", alias = "request_error")]
    RequestError,
    #[serde(alias = "nextfunctionnotfound", alias = "next_function_not_found")]
    NextFunctionNotFound,
    #[serde(alias = "constraintunsatisfiable", alias = "constraint_unsatisfiable")]
    ConstraintUnsatisfiable,
    #[serde(alias = "functiontimeout", alias = "function_timeout")]
    FunctionTimeout,
    #[serde(alias = "cancelled")]
    Cancelled,
    #[serde(alias = "outofmemory", alias = "out_of_memory")]
    OutOfMemory,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum RequestOutcome {
    #[default]
    Unknown,
    Success,
    Failure(RequestFailureReason),
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct ShallowRequest {
    pub created_at: i64,
    #[serde(rename = "id")]
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LogSignal {
    pub timestamp: u64,
    pub uuid: Uuid,
    pub namespace: String,
    pub application: String,
    #[serde(rename = "resourceAttributes")]
    pub resource_attributes: Vec<(String, String)>,
    pub body: String,
    #[serde(rename = "logAttributes")]
    pub log_attributes: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventsResponse {
    pub logs: Vec<LogSignal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

pub trait RequestEventMetadata {
    fn namespace(&self) -> &str;
    fn application_name(&self) -> &str;
    fn application_version(&self) -> &str;
    fn request_id(&self) -> &str;
    fn created_at(&self) -> Option<&DateTime<Utc>>;
    fn set_created_at(&mut self, date: DateTime<Utc>);
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum RequestStateChangeEvent {
    RequestStarted(RequestStartedEvent),
    FunctionRunCreated(FunctionRunCreated),
    /// Event emitted when a function run reaches its final outcome (after all retries exhausted or success)
    FunctionRunCompleted(FunctionRunCompleted),
    FunctionRunMatchedCache(FunctionRunMatchedCache),
    /// Event emitted when an allocation (execution attempt) is created and assigned to an executor
    AllocationCreated(AllocationCreated),
    /// Event emitted when an allocation (execution attempt) completes with an outcome
    AllocationCompleted(AllocationCompleted),
    RequestProgressUpdated(RequestProgressUpdated),
    RequestFinished(RequestFinishedEvent),
    // Legacy variants for backward compatibility
    #[serde(alias = "FunctionRunAssigned")]
    #[deprecated(note = "Use AllocationCreated instead")]
    FunctionRunAssigned(AllocationCreated),
}

impl RequestStateChangeEvent {
    #[allow(deprecated)]
    pub fn as_str(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(_) => "RequestStarted",
            RequestStateChangeEvent::FunctionRunCreated(_) => "FunctionRunCreated",
            RequestStateChangeEvent::FunctionRunCompleted(_) => "FunctionRunCompleted",
            RequestStateChangeEvent::FunctionRunMatchedCache(_) => "FunctionRunMatchedCache",
            RequestStateChangeEvent::AllocationCreated(_) => "AllocationCreated",
            RequestStateChangeEvent::AllocationCompleted(_) => "AllocationCompleted",
            RequestStateChangeEvent::RequestProgressUpdated(_) => "RequestProgressUpdated",
            RequestStateChangeEvent::RequestFinished(_) => "RequestFinished",
            // Legacy - maps to new name
            RequestStateChangeEvent::FunctionRunAssigned(_) => "AllocationCreated",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, RequestStateChangeEvent::RequestFinished(_))
    }

    #[allow(deprecated)]
    pub fn namespace(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.namespace(),
            RequestStateChangeEvent::RequestFinished(event) => event.namespace(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.namespace(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.namespace(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.namespace(),
            RequestStateChangeEvent::AllocationCreated(event) => event.namespace(),
            RequestStateChangeEvent::AllocationCompleted(event) => event.namespace(),
            RequestStateChangeEvent::RequestProgressUpdated(event) => event.namespace(),
            RequestStateChangeEvent::FunctionRunAssigned(event) => event.namespace(),
        }
    }

    #[allow(deprecated)]
    pub fn application_name(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.application_name(),
            RequestStateChangeEvent::RequestFinished(event) => event.application_name(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.application_name(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.application_name(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.application_name(),
            RequestStateChangeEvent::AllocationCreated(event) => event.application_name(),
            RequestStateChangeEvent::AllocationCompleted(event) => event.application_name(),
            RequestStateChangeEvent::RequestProgressUpdated(event) => event.application_name(),
            RequestStateChangeEvent::FunctionRunAssigned(event) => event.application_name(),
        }
    }

    #[allow(deprecated)]
    pub fn application_version(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.application_version(),
            RequestStateChangeEvent::RequestFinished(event) => event.application_version(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.application_version(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.application_version(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.application_version(),
            RequestStateChangeEvent::AllocationCreated(event) => event.application_version(),
            RequestStateChangeEvent::AllocationCompleted(event) => event.application_version(),
            RequestStateChangeEvent::RequestProgressUpdated(event) => event.application_version(),
            RequestStateChangeEvent::FunctionRunAssigned(event) => event.application_version(),
        }
    }

    #[allow(deprecated)]
    pub fn request_id(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.request_id(),
            RequestStateChangeEvent::RequestFinished(event) => event.request_id(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.request_id(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.request_id(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.request_id(),
            RequestStateChangeEvent::AllocationCreated(event) => event.request_id(),
            RequestStateChangeEvent::AllocationCompleted(event) => event.request_id(),
            RequestStateChangeEvent::RequestProgressUpdated(event) => event.request_id(),
            RequestStateChangeEvent::FunctionRunAssigned(event) => event.request_id(),
        }
    }

    #[allow(deprecated)]
    pub fn created_at(&self) -> Option<&DateTime<Utc>> {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.created_at(),
            RequestStateChangeEvent::RequestFinished(event) => event.created_at(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.created_at(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.created_at(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.created_at(),
            RequestStateChangeEvent::AllocationCreated(event) => event.created_at(),
            RequestStateChangeEvent::AllocationCompleted(event) => event.created_at(),
            RequestStateChangeEvent::RequestProgressUpdated(event) => event.created_at(),
            RequestStateChangeEvent::FunctionRunAssigned(event) => event.created_at(),
        }
    }

    #[allow(deprecated)]
    pub fn set_created_at(&mut self, date: DateTime<Utc>) {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.set_created_at(date),
            RequestStateChangeEvent::RequestFinished(event) => event.set_created_at(date),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.set_created_at(date),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.set_created_at(date),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.set_created_at(date),
            RequestStateChangeEvent::AllocationCreated(event) => event.set_created_at(date),
            RequestStateChangeEvent::AllocationCompleted(event) => event.set_created_at(date),
            RequestStateChangeEvent::RequestProgressUpdated(event) => event.set_created_at(date),
            RequestStateChangeEvent::FunctionRunAssigned(event) => event.set_created_at(date),
        }
    }

    #[allow(deprecated)]
    pub fn message(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(_) => "Request Started",
            RequestStateChangeEvent::RequestFinished(_) => "Request Finished",
            RequestStateChangeEvent::FunctionRunCreated(_) => "Function Run Created",
            RequestStateChangeEvent::FunctionRunCompleted(_) => "Function Run Completed",
            RequestStateChangeEvent::FunctionRunMatchedCache(_) => {
                "Function Run Matched a Cached output"
            }
            RequestStateChangeEvent::AllocationCreated(_) => "Allocation Created",
            RequestStateChangeEvent::AllocationCompleted(_) => "Allocation Completed",
            RequestStateChangeEvent::RequestProgressUpdated(_) => "Request Progress Updated",
            // Legacy - maps to new message
            RequestStateChangeEvent::FunctionRunAssigned(_) => "Allocation Created",
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(untagged)]
pub enum StringKind {
    String(String),
    Unknown(serde_json::Value),
}

impl StringKind {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            StringKind::String(value) => Some(value),
            _ => None,
        }
    }
}

impl Default for StringKind {
    fn default() -> Self {
        StringKind::String(String::new())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(untagged)]
pub enum FloatKind {
    Float(f64),
    String(String),
    Unknown(serde_json::Value),
}

impl FloatKind {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FloatKind::Float(value) => Some(*value),
            FloatKind::String(value) => value.parse().ok(),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[non_exhaustive]
pub struct RequestProgressUpdated {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub namespace: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub application_name: String,
    #[serde(default)]
    pub application_version: String,
    pub request_id: String,
    #[serde(default)]
    pub function_name: String,
    #[serde(default)]
    pub function_run_id: String,
    #[serde(default)]
    pub allocation_id: String,
    #[serde(default)]
    pub message: StringKind,
    #[serde(default)]
    pub step: Option<FloatKind>,
    #[serde(default)]
    pub total: Option<FloatKind>,
    #[serde(default)]
    pub attributes: Option<serde_json::Value>,
    #[serde(default)]
    pub created_at: Option<Rfc3339DateTime>,
}

impl RequestEventMetadata for RequestProgressUpdated {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }

    fn created_at(&self) -> Option<&DateTime<Utc>> {
        self.created_at.as_ref().map(|rfc| &rfc.0)
    }

    fn set_created_at(&mut self, date: DateTime<Utc>) {
        self.created_at = Some(Rfc3339DateTime(date));
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RequestFinishedEvent {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    #[serde(default)]
    pub outcome: RequestOutcome,
    #[serde(default)]
    pub created_at: Option<Rfc3339DateTime>,
}

impl RequestEventMetadata for RequestFinishedEvent {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }

    fn created_at(&self) -> Option<&DateTime<Utc>> {
        self.created_at.as_ref().map(|rfc| &rfc.0)
    }

    fn set_created_at(&mut self, date: DateTime<Utc>) {
        self.created_at = Some(Rfc3339DateTime(date));
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RequestStartedEvent {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    #[serde(default)]
    pub created_at: Option<Rfc3339DateTime>,
}

impl RequestEventMetadata for RequestStartedEvent {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }

    fn created_at(&self) -> Option<&DateTime<Utc>> {
        self.created_at.as_ref().map(|rfc| &rfc.0)
    }

    fn set_created_at(&mut self, date: DateTime<Utc>) {
        self.created_at = Some(Rfc3339DateTime(date));
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FunctionRunCreated {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
    #[serde(default)]
    pub created_at: Option<Rfc3339DateTime>,
}

impl RequestEventMetadata for FunctionRunCreated {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }

    fn created_at(&self) -> Option<&DateTime<Utc>> {
        self.created_at.as_ref().map(|rfc| &rfc.0)
    }

    fn set_created_at(&mut self, date: DateTime<Utc>) {
        self.created_at = Some(Rfc3339DateTime(date));
    }
}

/// Event emitted when an allocation (execution attempt) is created and assigned to an executor
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AllocationCreated {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
    pub allocation_id: String,
    pub executor_id: String,
    #[serde(default)]
    pub created_at: Option<Rfc3339DateTime>,
}

impl RequestEventMetadata for AllocationCreated {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }

    fn created_at(&self) -> Option<&DateTime<Utc>> {
        self.created_at.as_ref().map(|rfc| &rfc.0)
    }

    fn set_created_at(&mut self, date: DateTime<Utc>) {
        self.created_at = Some(Rfc3339DateTime(date));
    }
}

/// @deprecated Use AllocationCreated instead
pub type FunctionRunAssigned = AllocationCreated;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum FunctionRunOutcomeSummary {
    Unknown,
    Success,
    Failure,
}

/// Event emitted when a function run reaches its final outcome (after all retries exhausted or success)
///
/// Note: In older server versions (before allocation/function-run lifecycle split),
/// this event included `allocation_id`. For backward compatibility, `allocation_id`
/// is kept as an optional field. New server versions will not include it.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FunctionRunCompleted {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
    /// Optional for backward compatibility with older servers.
    /// New servers (with allocation lifecycle) won't include this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocation_id: Option<String>,
    pub outcome: FunctionRunOutcomeSummary,
    #[serde(default)]
    pub created_at: Option<Rfc3339DateTime>,
}

impl RequestEventMetadata for FunctionRunCompleted {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }

    fn created_at(&self) -> Option<&DateTime<Utc>> {
        self.created_at.as_ref().map(|rfc| &rfc.0)
    }

    fn set_created_at(&mut self, date: DateTime<Utc>) {
        self.created_at = Some(Rfc3339DateTime(date));
    }
}

/// Event emitted when an allocation (execution attempt) completes with an outcome
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AllocationCompleted {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
    pub allocation_id: String,
    pub outcome: FunctionRunOutcomeSummary,
    #[serde(default)]
    pub created_at: Option<Rfc3339DateTime>,
}

impl RequestEventMetadata for AllocationCompleted {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }

    fn created_at(&self) -> Option<&DateTime<Utc>> {
        self.created_at.as_ref().map(|rfc| &rfc.0)
    }

    fn set_created_at(&mut self, date: DateTime<Utc>) {
        self.created_at = Some(Rfc3339DateTime(date));
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FunctionRunMatchedCache {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
    #[serde(default)]
    pub created_at: Option<Rfc3339DateTime>,
}

impl RequestEventMetadata for FunctionRunMatchedCache {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }

    fn created_at(&self) -> Option<&DateTime<Utc>> {
        self.created_at.as_ref().map(|rfc| &rfc.0)
    }

    fn set_created_at(&mut self, date: DateTime<Utc>) {
        self.created_at = Some(Rfc3339DateTime(date));
    }
}

#[derive(Builder, Debug)]
pub struct CheckFunctionOutputRequest {
    #[builder(setter(into))]
    pub namespace: String,
    #[builder(setter(into))]
    pub application: String,
    #[builder(setter(into))]
    pub request_id: String,
}

impl CheckFunctionOutputRequest {
    pub fn builder() -> CheckFunctionOutputRequestBuilder {
        CheckFunctionOutputRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct DeleteApplicationRequest {
    #[builder(setter(into))]
    pub namespace: String,
    #[builder(setter(into))]
    pub application: String,
}

impl DeleteApplicationRequest {
    pub fn builder() -> DeleteApplicationRequestBuilder {
        DeleteApplicationRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct DeleteFunctionRequest {
    #[builder(setter(into))]
    pub namespace: String,
    #[builder(setter(into))]
    pub application: String,
    #[builder(setter(into))]
    pub function_name: String,
}

impl DeleteFunctionRequest {
    pub fn builder() -> DeleteFunctionRequestBuilder {
        DeleteFunctionRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct DeleteRequestRequest {
    #[builder(setter(into))]
    pub namespace: String,
    #[builder(setter(into))]
    pub application: String,
    #[builder(setter(into))]
    pub request_id: String,
}

impl DeleteRequestRequest {
    pub fn builder() -> DeleteRequestRequestBuilder {
        DeleteRequestRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct DownloadFunctionOutputRequest {
    #[builder(setter(into))]
    pub namespace: String,
    #[builder(setter(into))]
    pub application: String,
    #[builder(setter(into))]
    pub request_id: String,
    #[builder(setter(into))]
    pub function_call_id: String,
}

impl DownloadFunctionOutputRequest {
    pub fn builder() -> DownloadFunctionOutputRequestBuilder {
        DownloadFunctionOutputRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct DownloadRequestOutputRequest {
    #[builder(setter(into))]
    pub namespace: String,
    #[builder(setter(into))]
    pub application: String,
    #[builder(setter(into))]
    pub request_id: String,
}

impl DownloadRequestOutputRequest {
    pub fn builder() -> DownloadRequestOutputRequestBuilder {
        DownloadRequestOutputRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct GetApplicationRequest {
    #[builder(setter(into))]
    pub namespace: String,
    #[builder(setter(into))]
    pub application: String,
}

impl GetApplicationRequest {
    pub fn builder() -> GetApplicationRequestBuilder {
        GetApplicationRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct GetRequestRequest {
    #[builder(setter(into))]
    pub namespace: String,
    #[builder(setter(into))]
    pub application: String,
    #[builder(setter(into))]
    pub request_id: String,
    #[builder(setter(into, strip_option), default)]
    pub updates_pagination_token: Option<String>,
}

impl GetRequestRequest {
    pub fn builder() -> GetRequestRequestBuilder {
        GetRequestRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct InvokeApplicationRequest {
    #[builder(setter(into))]
    pub namespace: String,
    #[builder(setter(into))]
    pub application: String,
    pub body: serde_json::Value,
}

impl InvokeApplicationRequest {
    pub fn builder() -> InvokeApplicationRequestBuilder {
        InvokeApplicationRequestBuilder::default()
    }
}

/// Response from invoking an application
pub enum InvokeResponse {
    /// The request ID of the invocation
    RequestId(String),
    /// A stream of progress events
    Stream(Pin<Box<dyn Stream<Item = Result<RequestStateChangeEvent, SdkError>> + Send>>),
}

#[derive(Builder, Debug)]
pub struct ListApplicationsRequest {
    #[builder(setter(into))]
    pub namespace: String,
    #[builder(default, setter(strip_option))]
    pub limit: Option<i32>,
    #[builder(default, setter(into, strip_option))]
    pub cursor: Option<String>,
    #[builder(default, setter(strip_option))]
    pub direction: Option<CursorDirection>,
}

impl ListApplicationsRequest {
    pub fn builder() -> ListApplicationsRequestBuilder {
        ListApplicationsRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct ListRequestsRequest {
    #[builder(setter(into))]
    pub namespace: String,
    #[builder(setter(into))]
    pub application: String,
    #[builder(default, setter(strip_option))]
    pub limit: Option<i32>,
    #[builder(default, setter(into, strip_option))]
    pub cursor: Option<String>,
    #[builder(default, setter(strip_option))]
    pub direction: Option<CursorDirection>,
}

impl ListRequestsRequest {
    pub fn builder() -> ListRequestsRequestBuilder {
        ListRequestsRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct StreamProgressRequest {
    #[builder(setter(into))]
    pub namespace: String,
    #[builder(setter(into))]
    pub application: String,
    #[builder(setter(into))]
    pub request_id: String,
}

impl StreamProgressRequest {
    pub fn builder() -> StreamProgressRequestBuilder {
        StreamProgressRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct UpsertApplicationRequest {
    #[builder(setter(into))]
    pub namespace: String,
    pub application_manifest: ApplicationManifest,
    #[builder(setter(into))]
    pub code_zip: Vec<u8>,
}

impl UpsertApplicationRequest {
    pub fn builder() -> UpsertApplicationRequestBuilder {
        UpsertApplicationRequestBuilder::default()
    }
}

#[derive(Builder, Debug)]
pub struct GetLogsRequest {
    #[builder(setter(into))]
    pub namespace: String,
    #[builder(setter(into))]
    pub application: String,
    #[builder(default, setter(into, strip_option))]
    pub request_id: Option<String>,
    #[builder(default, setter(into, strip_option))]
    pub container_id: Option<String>,
    #[builder(default, setter(into, strip_option))]
    pub function: Option<String>,
    #[builder(default, setter(into, strip_option))]
    pub next_token: Option<String>,
    #[builder(default, setter(strip_option))]
    pub head: Option<usize>,
    #[builder(default, setter(strip_option))]
    pub tail: Option<usize>,
    #[builder(default, setter(into, strip_option))]
    pub ignore: Option<String>,
    #[builder(default, setter(into, strip_option))]
    pub function_executor: Option<String>,
}

impl GetLogsRequest {
    pub fn builder() -> GetLogsRequestBuilder {
        GetLogsRequestBuilder::default()
    }
}

#[derive(Builder, Clone, Debug)]
pub struct ProgressUpdatesRequest {
    #[builder(setter(into))]
    pub namespace: String,
    #[builder(setter(into))]
    pub application: String,
    #[builder(setter(into))]
    pub request_id: String,
    pub mode: ProgressUpdatesRequestMode,
}

#[derive(Clone, Debug)]
pub enum ProgressUpdatesRequestMode {
    Paginated(Option<String>),
    Stream,
}

impl ProgressUpdatesRequest {
    pub fn builder() -> ProgressUpdatesRequestBuilder {
        ProgressUpdatesRequestBuilder::default()
    }
}

type ProgressUpdatesStream =
    Pin<Box<dyn Stream<Item = Result<RequestStateChangeEvent, SdkError>> + Send>>;

pub enum ProgressUpdatesResponse {
    /// A JSON object containing progress updates
    Json(ProgressUpdatesJson),
    /// A stream of progress events
    Stream(ProgressUpdatesStream),
}

impl ProgressUpdatesResponse {
    /// Returns the JSON object containing progress updates.
    /// Use this function only if the `ProgressUpdatesRequestMode` was set to `ProgressUpdatesRequestMode::Paginated(_)`.
    ///
    /// This function panics if the response is a `ProgressUpdatesResponse::Stream`.
    pub fn json(&self) -> &ProgressUpdatesJson {
        match self {
            ProgressUpdatesResponse::Json(updates) => updates,
            _ => panic!(
                "Expected ProgressUpdatesResponse::Json, got ProgressUpdatesResponse::Stream"
            ),
        }
    }

    /// Returns the Stream containing progress updates.
    /// Use this function only if the `ProgressUpdatesRequestMode` was set to `ProgressUpdatesRequestMode::Stream`.
    ///
    /// This function panics if the response is a `ProgressUpdatesResponse::Json`.
    pub fn stream(&mut self) -> &mut ProgressUpdatesStream {
        match self {
            ProgressUpdatesResponse::Stream(stream) => stream,
            _ => panic!(
                "Expected ProgressUpdatesResponse::Stream, got ProgressUpdatesResponse::Json"
            ),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct ProgressUpdatesJson {
    pub updates: Vec<RequestStateChangeEvent>,
    pub next_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;
    use serde_json::json;

    #[test]
    fn test_rfc3339_datetime_with_z() {
        let json = json!("2024-01-15T10:30:45Z");
        let result: Result<Rfc3339DateTime, _> = serde_json::from_value(json);
        assert!(result.is_ok());
    }

    #[test]
    fn test_rfc3339_datetime_without_z() {
        let json = json!("2024-01-15T10:30:45");
        let result: Result<Rfc3339DateTime, _> = serde_json::from_value(json);
        assert!(result.is_ok());
        let dt = result.unwrap();
        // Verify it was parsed correctly as UTC
        assert_eq!(dt.0.year(), 2024);
        assert_eq!(dt.0.month(), 1);
        assert_eq!(dt.0.day(), 15);
    }

    #[test]
    fn test_rfc3339_datetime_with_timezone_offset() {
        let json = json!("2024-01-15T10:30:45+00:00");
        let result: Result<Rfc3339DateTime, _> = serde_json::from_value(json);
        assert!(result.is_ok());
    }

    #[test]
    fn test_request_started_event_deserialization() {
        let json = json!({
            "namespace": "test",
            "application_name": "app",
            "application_version": "1.0",
            "request_id": "req-123",
            "created_at": "2024-01-15T10:30:45"
        });
        let result: Result<RequestStartedEvent, _> = serde_json::from_value(json);
        assert!(result.is_ok());
        let event = result.unwrap();
        assert!(event.created_at.is_some());
    }

    #[test]
    fn test_rfc3339_datetime_serialization() {
        // Test that serializing Rfc3339DateTime produces a plain string, not a nested struct
        let now = chrono::Utc::now();
        let rfc_dt = Rfc3339DateTime(now);
        let serialized = serde_json::to_value(rfc_dt).unwrap();

        // Should be a string, not an object
        assert!(
            serialized.is_string(),
            "Expected serialized DateTime to be a string, got: {:?}",
            serialized
        );

        // Should contain 'Z' at the end
        let date_str = serialized.as_str().unwrap();
        assert!(
            date_str.ends_with('Z'),
            "Expected 'Z' at end of serialized DateTime"
        );
    }

    #[test]
    fn test_request_started_event_serialization() {
        // Test that serializing an event doesn't nest the created_at field
        let event = RequestStartedEvent {
            namespace: "test".to_string(),
            application_name: "app".to_string(),
            application_version: "1.0".to_string(),
            request_id: "req-123".to_string(),
            created_at: Some(Rfc3339DateTime(Utc::now())),
        };

        let serialized = serde_json::to_value(&event).unwrap();
        let obj = serialized.as_object().unwrap();

        // created_at should be a string directly, not an object
        let created_at = &obj["created_at"];
        assert!(
            created_at.is_string(),
            "Expected created_at to be a string, got: {:?}",
            created_at
        );

        let date_str = created_at.as_str().unwrap();
        assert!(
            date_str.ends_with('Z'),
            "Expected 'Z' at end of created_at value"
        );
    }

    // Backward compatibility tests for allocation events (PR #2042)

    #[test]
    fn test_old_server_function_run_completed_with_allocation_id() {
        // Old server sends FunctionRunCompleted WITH allocation_id
        let json = json!({
            "FunctionRunCompleted": {
                "namespace": "test-ns",
                "application_name": "test-app",
                "application_version": "1.0",
                "request_id": "req-123",
                "function_name": "my-func",
                "function_run_id": "run-456",
                "allocation_id": "alloc-789",
                "outcome": "success"
            }
        });

        let result: Result<RequestStateChangeEvent, _> = serde_json::from_value(json);
        assert!(
            result.is_ok(),
            "Failed to deserialize old server FunctionRunCompleted: {:?}",
            result.err()
        );

        let event = result.unwrap();
        match event {
            RequestStateChangeEvent::FunctionRunCompleted(e) => {
                assert_eq!(e.allocation_id, Some("alloc-789".to_string()));
                assert_eq!(e.function_run_id, "run-456");
            }
            _ => panic!("Expected FunctionRunCompleted variant"),
        }
    }

    #[test]
    fn test_new_server_function_run_completed_without_allocation_id() {
        // New server sends FunctionRunCompleted WITHOUT allocation_id
        let json = json!({
            "FunctionRunCompleted": {
                "namespace": "test-ns",
                "application_name": "test-app",
                "application_version": "1.0",
                "request_id": "req-123",
                "function_name": "my-func",
                "function_run_id": "run-456",
                "outcome": "success"
            }
        });

        let result: Result<RequestStateChangeEvent, _> = serde_json::from_value(json);
        assert!(
            result.is_ok(),
            "Failed to deserialize new server FunctionRunCompleted: {:?}",
            result.err()
        );

        let event = result.unwrap();
        match event {
            RequestStateChangeEvent::FunctionRunCompleted(e) => {
                assert_eq!(e.allocation_id, None);
                assert_eq!(e.function_run_id, "run-456");
            }
            _ => panic!("Expected FunctionRunCompleted variant"),
        }
    }

    #[test]
    fn test_old_server_function_run_assigned() {
        // Old server sends FunctionRunAssigned
        let json = json!({
            "FunctionRunAssigned": {
                "namespace": "test-ns",
                "application_name": "test-app",
                "application_version": "1.0",
                "request_id": "req-123",
                "function_name": "my-func",
                "function_run_id": "run-456",
                "allocation_id": "alloc-789",
                "executor_id": "exec-001"
            }
        });

        let result: Result<RequestStateChangeEvent, _> = serde_json::from_value(json);
        assert!(
            result.is_ok(),
            "Failed to deserialize old server FunctionRunAssigned: {:?}",
            result.err()
        );

        let event = result.unwrap();
        // Should deserialize to FunctionRunAssigned variant (backward compat)
        #[allow(deprecated)]
        match event {
            RequestStateChangeEvent::FunctionRunAssigned(e) => {
                assert_eq!(e.allocation_id, "alloc-789");
                assert_eq!(e.executor_id, "exec-001");
            }
            _ => panic!(
                "Expected FunctionRunAssigned variant, got {:?}",
                event.as_str()
            ),
        }
    }

    #[test]
    fn test_new_server_allocation_created() {
        // New server sends AllocationCreated
        let json = json!({
            "AllocationCreated": {
                "namespace": "test-ns",
                "application_name": "test-app",
                "application_version": "1.0",
                "request_id": "req-123",
                "function_name": "my-func",
                "function_run_id": "run-456",
                "allocation_id": "alloc-789",
                "executor_id": "exec-001"
            }
        });

        let result: Result<RequestStateChangeEvent, _> = serde_json::from_value(json);
        assert!(
            result.is_ok(),
            "Failed to deserialize new server AllocationCreated: {:?}",
            result.err()
        );

        let event = result.unwrap();
        match event {
            RequestStateChangeEvent::AllocationCreated(e) => {
                assert_eq!(e.allocation_id, "alloc-789");
                assert_eq!(e.executor_id, "exec-001");
            }
            _ => panic!("Expected AllocationCreated variant"),
        }
    }

    #[test]
    fn test_new_server_allocation_completed() {
        // New server sends AllocationCompleted
        let json = json!({
            "AllocationCompleted": {
                "namespace": "test-ns",
                "application_name": "test-app",
                "application_version": "1.0",
                "request_id": "req-123",
                "function_name": "my-func",
                "function_run_id": "run-456",
                "allocation_id": "alloc-789",
                "outcome": "failure"
            }
        });

        let result: Result<RequestStateChangeEvent, _> = serde_json::from_value(json);
        assert!(
            result.is_ok(),
            "Failed to deserialize new server AllocationCompleted: {:?}",
            result.err()
        );

        let event = result.unwrap();
        match event {
            RequestStateChangeEvent::AllocationCompleted(e) => {
                assert_eq!(e.allocation_id, "alloc-789");
                assert_eq!(e.outcome, FunctionRunOutcomeSummary::Failure);
            }
            _ => panic!("Expected AllocationCompleted variant"),
        }
    }
}
