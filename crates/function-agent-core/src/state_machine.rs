use serde::{Deserialize, Serialize};

use crate::model::{
    BlobReference, ExecutionHistoryEntry, FailureReason, GraphUpdates, RequestHeader,
};

pub use crate::model::RequestStateOperation as AgentRequestStateOperation;

pub const AGENT_PROTOCOL_VERSION: u32 = 1;
pub const AGENT_HEARTBEAT_BODY_LIMIT_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_REQUEST_STATE_OPERATION_ID_BYTES: usize = 512;
pub const MAX_REQUEST_STATE_KEY_BYTES: usize = 1_024;
pub const MAX_REQUEST_STATE_VALUE_BYTES: usize = 1024 * 1024;

fn default_agent_content_type() -> String {
    "application/octet-stream".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterAgentRequest {
    pub agent_id: String,
    pub incarnation: String,
    pub registration_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterAgentResponse {
    pub protocol_version: u32,
    pub agent_id: String,
    pub lease_expires_at_ms: u64,
    pub next_event_sequence: u64,
    pub sandbox_id: String,
    pub application: String,
    pub application_version: String,
    pub function: String,
    pub namespace: String,
    pub max_concurrency: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHeartbeatRequest {
    pub incarnation: String,
    pub available_slots: u32,
    #[serde(default)]
    pub active_attempts: Vec<AgentActiveAttempt>,
    #[serde(default)]
    pub events: Vec<AgentEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentHeartbeatResponse {
    pub lease_expires_at_ms: u64,
    pub acknowledged_event_sequence: u64,
    pub expected_event_sequence: u64,
    pub assignments: Vec<AgentAssignment>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub function_call_results: Vec<AgentFunctionCallResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub request_state_results: Vec<AgentRequestStateResult>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rejected_events: Vec<AgentEventRejection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct AgentActiveAttempt {
    pub attempt_id: String,
    pub fence_token: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAssignment {
    pub attempt_id: String,
    pub fence_token: u64,
    pub function_run_id: String,
    pub request_id: String,
    pub namespace: String,
    pub application: String,
    pub application_version: String,
    pub function: String,
    pub timeout_ms: u64,
    pub initialization_timeout_ms: u64,
    pub inputs: Vec<AgentInput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub request_headers: Vec<RequestHeader>,
    pub call_metadata_base64: String,
    pub application_code_base64: String,
    pub application_code_sha256: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub execution_history: Vec<ExecutionHistoryEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_function_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub data_base64: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<BlobReference>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read: Option<AgentReadPlan>,
    #[serde(default)]
    pub metadata_base64: String,
    #[serde(default = "default_agent_content_type")]
    pub content_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentReadPlan {
    pub url: String,
    pub method: String,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub headers: std::collections::BTreeMap<String, String>,
    pub expires_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentEvent {
    pub sequence: u64,
    #[serde(flatten)]
    pub payload: AgentEventPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEventPayload {
    Progress {
        attempt_id: String,
        fence_token: u64,
        message: String,
    },
    Result {
        result: AgentRunResult,
    },
    FunctionCall {
        attempt_id: String,
        fence_token: u64,
        history_sequence: u64,
        watcher_id: String,
        updates: GraphUpdates,
        #[serde(default)]
        timeout_ms: Option<u64>,
    },
    FunctionCallResultAck {
        attempt_id: String,
        fence_token: u64,
        watcher_id: String,
    },
    RequestState {
        attempt_id: String,
        fence_token: u64,
        history_sequence: u64,
        operation_id: String,
        #[serde(flatten)]
        operation: AgentRequestStateOperation,
    },
    RequestStateResultAck {
        attempt_id: String,
        fence_token: u64,
        operation_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentRequestStateResult {
    pub operation_id: String,
    pub attempt_id: String,
    pub fence_token: u64,
    #[serde(flatten)]
    pub result: AgentRequestStateResultPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum AgentRequestStateResultPayload {
    Get {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value_base64: Option<String>,
    },
    Set,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentFunctionCallResult {
    pub watcher_id: String,
    pub attempt_id: String,
    pub fence_token: u64,
    #[serde(flatten)]
    pub result: AgentFunctionCallResultPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum AgentFunctionCallResultPayload {
    Success {
        #[serde(default, skip_serializing_if = "String::is_empty")]
        output_base64: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        blob: Option<BlobReference>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        read: Option<Box<AgentReadPlan>>,
        #[serde(default)]
        metadata_base64: String,
        #[serde(default = "default_agent_content_type")]
        content_type: String,
    },
    Failure {
        reason: FailureReason,
    },
    TimedOut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentEventRejection {
    pub sequence: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum AgentRunResult {
    Success {
        attempt_id: String,
        fence_token: u64,
        result: AgentSuccessResult,
    },
    Failure {
        attempt_id: String,
        fence_token: u64,
        reason: FailureReason,
        #[serde(default)]
        message: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentSuccessResult {
    Value {
        output_base64: String,
        #[serde(default)]
        metadata_base64: String,
        #[serde(default = "default_agent_content_type")]
        content_type: String,
    },
    UploadedValue {
        write_id: String,
        #[serde(default)]
        metadata_base64: String,
        #[serde(default = "default_agent_content_type")]
        content_type: String,
    },
    CallGraph {
        updates: GraphUpdates,
    },
}
