use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};

fn default_content_type() -> String {
    "application/octet-stream".to_string()
}

fn is_default_content_type(content_type: &String) -> bool {
    content_type == "application/octet-stream"
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestHeader {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlobReference {
    pub uri: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FunctionCall {
    pub function_call_id: String,
    pub inputs: Vec<FunctionArgument>,
    pub function_name: String,
    #[serde(default, with = "base64_bytes")]
    pub call_metadata: Vec<u8>,
    #[serde(default)]
    pub parent_function_call_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum FunctionArgument {
    FunctionRunOutput {
        function_call_id: String,
    },
    Data {
        #[serde(with = "base64_bytes")]
        data: Vec<u8>,
        #[serde(default, with = "base64_bytes", skip_serializing_if = "Vec::is_empty")]
        metadata: Vec<u8>,
        #[serde(
            default = "default_content_type",
            skip_serializing_if = "is_default_content_type"
        )]
        content_type: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReduceOperation {
    pub function_call_id: String,
    pub collection: Vec<FunctionArgument>,
    pub function_name: String,
    #[serde(default, with = "base64_bytes")]
    pub call_metadata: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ComputeOperation {
    FunctionCall { call: FunctionCall },
    Reduce { operation: ReduceOperation },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphUpdates {
    pub operations: Vec<ComputeOperation>,
    pub output_function_call_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionHistoryCommand {
    FunctionCall {
        updates: GraphUpdates,
        #[serde(default)]
        timeout_ms: Option<u64>,
    },
    RequestState {
        operation: RequestStateOperation,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum FunctionCallResult {
    Success {
        #[serde(with = "base64_bytes")]
        output: Vec<u8>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        blob: Option<BlobReference>,
        #[serde(default, with = "base64_bytes", skip_serializing_if = "Vec::is_empty")]
        metadata: Vec<u8>,
        #[serde(
            default = "default_content_type",
            skip_serializing_if = "is_default_content_type"
        )]
        content_type: String,
    },
    Failure {
        reason: FailureReason,
    },
    TimedOut,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecutionHistoryResult {
    FunctionCall { result: FunctionCallResult },
    RequestState { result: RequestStateOperationResult },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionHistoryEntry {
    pub sequence: u64,
    pub command: ExecutionHistoryCommand,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<ExecutionHistoryResult>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum RequestStateOperation {
    Get { key: String },
    Set { key: String, value_base64: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestStateOperationResult {
    Get {
        #[serde(default, with = "optional_base64_bytes")]
        value: Option<Vec<u8>>,
    },
    Set,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureReason {
    InternalError,
    FunctionError,
    FunctionTimeout,
    RequestError,
    Cancelled,
    OutOfMemory,
    AgentLost,
    RuntimeLost,
    SandboxLost,
    ReplayEventHistoryMismatch,
}

impl FailureReason {
    pub fn is_retriable(&self) -> bool {
        !matches!(
            self,
            Self::RequestError | Self::Cancelled | Self::ReplayEventHistoryMismatch
        )
    }

    pub fn counts_against_user_retries(&self) -> bool {
        !matches!(
            self,
            Self::AgentLost | Self::RuntimeLost | Self::SandboxLost
        )
    }
}

mod base64_bytes {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    use super::BASE64;
    use base64::Engine as _;

    pub fn serialize<S>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&BASE64.encode(value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        BASE64.decode(value).map_err(D::Error::custom)
    }
}

mod optional_base64_bytes {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

    use super::BASE64;
    use base64::Engine as _;

    pub fn serialize<S>(value: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        value
            .as_ref()
            .map(|value| BASE64.encode(value))
            .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<String>::deserialize(deserializer)?;
        value
            .map(|value| BASE64.decode(value).map_err(D::Error::custom))
            .transpose()
    }
}
