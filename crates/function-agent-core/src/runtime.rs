//! Rust execution core between the Function Service agent and a language runner.
//!
//! The core owns attempt-local call-graph state, watcher identities, protocol
//! validation, and replay matching. Language runners only load customer code,
//! translate SDK hooks, and serialize values.

use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};

use crate::{
    model::{
        ComputeOperation, ExecutionHistoryCommand, ExecutionHistoryEntry, FailureReason,
        FunctionArgument, FunctionCall, GraphUpdates,
    },
    state_machine::{
        AgentAssignment, AgentFunctionCallResult, AgentFunctionCallResultPayload,
        AgentRequestStateOperation, AgentRequestStateResult, AgentSuccessResult,
        MAX_REQUEST_STATE_KEY_BYTES, MAX_REQUEST_STATE_OPERATION_ID_BYTES,
        MAX_REQUEST_STATE_VALUE_BYTES,
    },
};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CoreAgentInput {
    Assignment { assignment: Box<AgentAssignment> },
    FunctionCallResult { result: AgentFunctionCallResult },
    RequestStateResult { result: AgentRequestStateResult },
    Cancel { attempt_id: String },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CoreAgentOutput {
    Initialized,
    Suspend {
        attempt_id: String,
    },
    Resume {
        attempt_id: String,
    },
    Progress {
        attempt_id: String,
        message: String,
    },
    FunctionCall {
        attempt_id: String,
        history_sequence: u64,
        watcher_id: String,
        updates: GraphUpdates,
        #[serde(skip_serializing_if = "Option::is_none")]
        timeout_ms: Option<u64>,
    },
    RequestState {
        attempt_id: String,
        history_sequence: u64,
        operation_id: String,
        #[serde(flatten)]
        operation: AgentRequestStateOperation,
    },
    Success {
        attempt_id: String,
        result: AgentSuccessResult,
    },
    Failure {
        attempt_id: String,
        reason: FailureReason,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SdkOutput {
    Initialized,
    Suspend {
        attempt_id: String,
    },
    Resume {
        attempt_id: String,
    },
    Progress {
        attempt_id: String,
        message: String,
    },
    CallBatch {
        attempt_id: String,
        calls: Vec<SdkFunctionCall>,
    },
    Watch {
        attempt_id: String,
        function_call_id: String,
        #[serde(default)]
        timeout_ms: Option<u64>,
    },
    RequestState {
        attempt_id: String,
        operation_id: String,
        operation: AgentRequestStateOperation,
    },
    Success {
        attempt_id: String,
        result: SdkSuccessResult,
    },
    Failure {
        attempt_id: String,
        reason: FailureReason,
        #[serde(default)]
        message: Option<String>,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum SdkSuccessResult {
    Value {
        output_base64: String,
        #[serde(default)]
        metadata_base64: String,
        content_type: String,
    },
    CallGraph {
        output_function_call_id: String,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SdkFunctionCall {
    pub function_call_id: String,
    pub function_name: String,
    pub inputs: Vec<SdkFunctionArgument>,
    #[serde(default)]
    pub call_metadata_base64: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum SdkFunctionArgument {
    FunctionRunOutput {
        function_call_id: String,
    },
    Data {
        data_base64: String,
        #[serde(default)]
        metadata_base64: String,
        content_type: String,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SdkInput {
    Assignment {
        assignment: Box<AgentAssignment>,
    },
    CallResult {
        attempt_id: String,
        function_call_id: String,
        #[serde(flatten)]
        result: AgentFunctionCallResultPayload,
    },
    RequestStateResult {
        result: AgentRequestStateResult,
    },
    Cancel {
        attempt_id: String,
    },
}

#[derive(Debug)]
struct AttemptState {
    assignment: AgentAssignment,
    operations: Vec<ComputeOperation>,
    call_indexes: HashMap<String, usize>,
    watchers: HashMap<String, WatcherState>,
    watched_calls: HashSet<String>,
    next_watcher_sequence: u64,
    execution_history: Vec<ExecutionHistoryEntry>,
    next_history_sequence: u64,
    request_state_operations: HashMap<String, bool>,
}

#[derive(Debug)]
struct WatcherState {
    function_call_id: String,
    delivered: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct ReplayEventHistoryMismatch(String);

#[derive(Default)]
pub struct RuntimeCore {
    initialized: bool,
    attempts: HashMap<String, AttemptState>,
    closed_attempts: HashSet<String>,
    closed_attempt_order: VecDeque<String>,
}

impl RuntimeCore {
    pub fn handle_agent_input(&mut self, input: CoreAgentInput) -> Result<Option<SdkInput>> {
        match input {
            CoreAgentInput::Assignment { assignment } => {
                let assignment = *assignment;
                let attempt_id = assignment.attempt_id.clone();
                if self.attempts.contains_key(&attempt_id)
                    || self.closed_attempts.contains(&attempt_id)
                {
                    return Ok(None);
                }
                let execution_history = assignment.execution_history.clone();
                let sdk_input = SdkInput::Assignment {
                    assignment: Box::new(assignment.clone()),
                };
                self.attempts.insert(
                    attempt_id,
                    AttemptState {
                        assignment,
                        operations: Vec::new(),
                        call_indexes: HashMap::new(),
                        watchers: HashMap::new(),
                        watched_calls: HashSet::new(),
                        next_watcher_sequence: 1,
                        execution_history,
                        next_history_sequence: 1,
                        request_state_operations: HashMap::new(),
                    },
                );
                Ok(Some(sdk_input))
            }
            CoreAgentInput::FunctionCallResult { result } => {
                let Some(attempt) = self.attempts.get_mut(&result.attempt_id) else {
                    return Ok(None);
                };
                if attempt.assignment.fence_token != result.fence_token {
                    bail!(
                        "function-call result fence token {} does not match attempt {} fence token {}",
                        result.fence_token,
                        result.attempt_id,
                        attempt.assignment.fence_token
                    );
                }
                let Some(watcher) = attempt.watchers.get_mut(&result.watcher_id) else {
                    bail!(
                        "function-call result references unknown watcher {}",
                        result.watcher_id
                    );
                };
                if watcher.delivered {
                    return Ok(None);
                }
                let sdk_input = SdkInput::CallResult {
                    attempt_id: result.attempt_id,
                    function_call_id: watcher.function_call_id.clone(),
                    result: result.result,
                };
                watcher.delivered = true;
                Ok(Some(sdk_input))
            }
            CoreAgentInput::RequestStateResult { result } => {
                let Some(attempt) = self.attempts.get_mut(&result.attempt_id) else {
                    return Ok(None);
                };
                if attempt.assignment.fence_token != result.fence_token {
                    bail!(
                        "request-state result fence token {} does not match attempt {} fence token {}",
                        result.fence_token,
                        result.attempt_id,
                        attempt.assignment.fence_token
                    );
                }
                let Some(delivered) = attempt
                    .request_state_operations
                    .get_mut(&result.operation_id)
                else {
                    bail!(
                        "request-state result references unknown operation {}",
                        result.operation_id
                    );
                };
                if *delivered {
                    return Ok(None);
                }
                *delivered = true;
                Ok(Some(SdkInput::RequestStateResult { result }))
            }
            CoreAgentInput::Cancel { attempt_id } => {
                if self.attempts.remove(&attempt_id).is_some() {
                    self.close_attempt(attempt_id.clone());
                    Ok(Some(SdkInput::Cancel { attempt_id }))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub fn handle_sdk_output(&mut self, output: SdkOutput) -> Result<Vec<CoreAgentOutput>> {
        match output {
            SdkOutput::Initialized => {
                if self.initialized {
                    bail!("language runner emitted initialized more than once");
                }
                self.initialized = true;
                Ok(vec![CoreAgentOutput::Initialized])
            }
            output if !self.initialized => {
                let _ = output;
                bail!("language runner emitted execution output before initialization")
            }
            SdkOutput::Suspend { attempt_id } => {
                if self.closed_attempts.contains(&attempt_id) {
                    return Ok(Vec::new());
                }
                self.require_attempt(&attempt_id)?;
                Ok(vec![CoreAgentOutput::Suspend { attempt_id }])
            }
            SdkOutput::Resume { attempt_id } => {
                if self.closed_attempts.contains(&attempt_id) {
                    return Ok(Vec::new());
                }
                self.require_attempt(&attempt_id)?;
                Ok(vec![CoreAgentOutput::Resume { attempt_id }])
            }
            SdkOutput::Progress {
                attempt_id,
                message,
            } => {
                if self.closed_attempts.contains(&attempt_id) {
                    return Ok(Vec::new());
                }
                self.require_attempt(&attempt_id)?;
                Ok(vec![CoreAgentOutput::Progress {
                    attempt_id,
                    message,
                }])
            }
            SdkOutput::CallBatch { attempt_id, calls } => {
                if self.closed_attempts.contains(&attempt_id) {
                    return Ok(Vec::new());
                }
                if calls.is_empty() {
                    bail!("language runner emitted an empty call batch for attempt {attempt_id}");
                }
                let attempt = self.require_attempt_mut(&attempt_id)?;
                for call in calls {
                    let call = decode_call(call, &attempt.assignment.function_run_id)?;
                    let call_id = call.function_call_id.clone();
                    let operation = ComputeOperation::FunctionCall { call };
                    if let Some(index) = attempt.call_indexes.get(&call_id).copied() {
                        if attempt.operations[index] != operation {
                            bail!("language runner redefined function call {call_id}");
                        }
                        continue;
                    }
                    attempt
                        .call_indexes
                        .insert(call_id, attempt.operations.len());
                    attempt.operations.push(operation);
                }
                Ok(Vec::new())
            }
            SdkOutput::Watch {
                attempt_id,
                function_call_id,
                timeout_ms,
            } => {
                if self.closed_attempts.contains(&attempt_id) {
                    return Ok(Vec::new());
                }
                let prepared = (|| -> Result<_> {
                    let attempt = self.require_attempt_mut(&attempt_id)?;
                    if !attempt.call_indexes.contains_key(&function_call_id) {
                        bail!("language runner watched unknown function call {function_call_id}");
                    }
                    if !attempt.watched_calls.insert(function_call_id.clone()) {
                        bail!(
                            "language runner watched function call {function_call_id} more than once"
                        );
                    }
                    let updates = GraphUpdates {
                        operations: attempt.operations.clone(),
                        output_function_call_id: function_call_id.clone(),
                    };
                    let command = ExecutionHistoryCommand::FunctionCall {
                        updates: updates.clone(),
                        timeout_ms,
                    };
                    let history_sequence = match_execution_history(attempt, &command)?;
                    let watcher_id = format!(
                        "{}:{}",
                        attempt.assignment.attempt_id, attempt.next_watcher_sequence
                    );
                    attempt.next_watcher_sequence = attempt.next_watcher_sequence.saturating_add(1);
                    attempt.watchers.insert(
                        watcher_id.clone(),
                        WatcherState {
                            function_call_id: function_call_id.clone(),
                            delivered: false,
                        },
                    );
                    Ok((history_sequence, watcher_id, updates))
                })();
                let (history_sequence, watcher_id, updates) = match prepared {
                    Ok(prepared) => prepared,
                    Err(error) if error.downcast_ref::<ReplayEventHistoryMismatch>().is_some() => {
                        return Ok(self.fail_replay_mismatch(attempt_id, error.to_string()));
                    }
                    Err(error) => return Err(error),
                };
                Ok(vec![CoreAgentOutput::FunctionCall {
                    attempt_id,
                    history_sequence,
                    watcher_id,
                    updates,
                    timeout_ms,
                }])
            }
            SdkOutput::RequestState {
                attempt_id,
                operation_id,
                operation,
            } => {
                if self.closed_attempts.contains(&attempt_id) {
                    return Ok(Vec::new());
                }
                validate_request_state_operation(&operation_id, &operation)?;
                let history_sequence = {
                    let attempt = self.require_attempt_mut(&attempt_id)?;
                    match_execution_history(
                        attempt,
                        &ExecutionHistoryCommand::RequestState {
                            operation: operation.clone(),
                        },
                    )
                };
                let history_sequence = match history_sequence {
                    Ok(sequence) => sequence,
                    Err(error) if error.downcast_ref::<ReplayEventHistoryMismatch>().is_some() => {
                        return Ok(self.fail_replay_mismatch(attempt_id, error.to_string()));
                    }
                    Err(error) => return Err(error),
                };
                let attempt = self.require_attempt_mut(&attempt_id)?;
                if attempt
                    .request_state_operations
                    .insert(operation_id.clone(), false)
                    .is_some()
                {
                    bail!("language runner reused request-state operation id {operation_id}");
                }
                Ok(vec![CoreAgentOutput::RequestState {
                    attempt_id,
                    history_sequence,
                    operation_id,
                    operation,
                }])
            }
            SdkOutput::Success { attempt_id, result } => {
                if self.closed_attempts.contains(&attempt_id) {
                    return Ok(Vec::new());
                }
                if let Some(message) = self.unconsumed_history_message(&attempt_id)? {
                    return Ok(self.fail_replay_mismatch(attempt_id, message));
                }
                let attempt = self.attempts.remove(&attempt_id).ok_or_else(|| {
                    anyhow!("language runner completed unknown attempt {attempt_id}")
                })?;
                let result = match result {
                    SdkSuccessResult::Value {
                        output_base64,
                        metadata_base64,
                        content_type,
                    } => {
                        validate_base64("function output", &output_base64)?;
                        validate_base64("function output metadata", &metadata_base64)?;
                        if content_type.trim().is_empty() {
                            bail!("language runner emitted an empty function output content type");
                        }
                        AgentSuccessResult::Value {
                            output_base64,
                            metadata_base64,
                            content_type,
                        }
                    }
                    SdkSuccessResult::CallGraph {
                        output_function_call_id,
                    } => {
                        if !attempt.call_indexes.contains_key(&output_function_call_id) {
                            bail!(
                                "language runner returned unknown tail call {output_function_call_id}"
                            );
                        }
                        AgentSuccessResult::CallGraph {
                            updates: GraphUpdates {
                                operations: attempt.operations,
                                output_function_call_id,
                            },
                        }
                    }
                };
                self.close_attempt(attempt_id.clone());
                Ok(vec![CoreAgentOutput::Success { attempt_id, result }])
            }
            SdkOutput::Failure {
                attempt_id,
                reason,
                message,
            } => {
                if self.closed_attempts.contains(&attempt_id) {
                    return Ok(Vec::new());
                }
                if let Some(message) = self.unconsumed_history_message(&attempt_id)? {
                    return Ok(self.fail_replay_mismatch(attempt_id, message));
                }
                if self.attempts.remove(&attempt_id).is_none() {
                    bail!("language runner failed unknown attempt {attempt_id}");
                }
                self.close_attempt(attempt_id.clone());
                Ok(vec![CoreAgentOutput::Failure {
                    attempt_id,
                    reason,
                    message,
                }])
            }
        }
    }

    fn require_attempt(&self, attempt_id: &str) -> Result<&AttemptState> {
        self.attempts
            .get(attempt_id)
            .ok_or_else(|| anyhow!("language runner referenced unknown attempt {attempt_id}"))
    }

    fn require_attempt_mut(&mut self, attempt_id: &str) -> Result<&mut AttemptState> {
        self.attempts
            .get_mut(attempt_id)
            .ok_or_else(|| anyhow!("language runner referenced unknown attempt {attempt_id}"))
    }

    fn close_attempt(&mut self, attempt_id: String) {
        const MAX_CLOSED_ATTEMPTS: usize = 4_096;
        if !self.closed_attempts.insert(attempt_id.clone()) {
            return;
        }
        self.closed_attempt_order.push_back(attempt_id);
        while self.closed_attempt_order.len() > MAX_CLOSED_ATTEMPTS {
            if let Some(expired) = self.closed_attempt_order.pop_front() {
                self.closed_attempts.remove(&expired);
            }
        }
    }

    fn fail_replay_mismatch(
        &mut self,
        attempt_id: String,
        message: String,
    ) -> Vec<CoreAgentOutput> {
        self.attempts.remove(&attempt_id);
        self.close_attempt(attempt_id.clone());
        vec![CoreAgentOutput::Failure {
            attempt_id,
            reason: FailureReason::ReplayEventHistoryMismatch,
            message: Some(message),
        }]
    }

    fn unconsumed_history_message(&self, attempt_id: &str) -> Result<Option<String>> {
        let attempt = self.require_attempt(attempt_id)?;
        let consumed = attempt.next_history_sequence.saturating_sub(1);
        let expected = u64::try_from(attempt.execution_history.len())
            .context("execution-history length overflow")?;
        Ok((consumed < expected).then(|| {
            format!(
                "deterministic replay ended after {consumed} commands but durable history contains {expected}"
            )
        }))
    }
}

fn match_execution_history(
    attempt: &mut AttemptState,
    command: &ExecutionHistoryCommand,
) -> Result<u64> {
    let sequence = attempt.next_history_sequence;
    let index = usize::try_from(sequence - 1).context("execution-history sequence overflow")?;
    if let Some(expected) = attempt.execution_history.get(index)
        && &expected.command != command
    {
        return Err(anyhow!(ReplayEventHistoryMismatch(format!(
            "deterministic replay mismatch at execution-history sequence {sequence}: expected {:?}, observed {:?}",
            expected.command, command,
        ))));
    }
    attempt.next_history_sequence = sequence.saturating_add(1);
    Ok(sequence)
}

fn decode_call(call: SdkFunctionCall, parent_function_call_id: &str) -> Result<FunctionCall> {
    if call.function_call_id.is_empty() || call.function_name.is_empty() {
        bail!("language runner function call identity and function name must not be empty");
    }
    let inputs = call
        .inputs
        .into_iter()
        .map(|input| match input {
            SdkFunctionArgument::FunctionRunOutput { function_call_id } => {
                if function_call_id.is_empty() {
                    bail!("language runner emitted an empty function-call input reference");
                }
                Ok(FunctionArgument::FunctionRunOutput { function_call_id })
            }
            SdkFunctionArgument::Data {
                data_base64,
                metadata_base64,
                content_type,
            } => {
                if content_type.trim().is_empty() {
                    bail!("language runner emitted an empty function-call input content type");
                }
                Ok(FunctionArgument::Data {
                    data: BASE64
                        .decode(data_base64)
                        .context("decode language runner function-call input")?,
                    metadata: BASE64
                        .decode(metadata_base64)
                        .context("decode language runner function-call input metadata")?,
                    content_type,
                })
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(FunctionCall {
        function_call_id: call.function_call_id,
        inputs,
        function_name: call.function_name,
        call_metadata: BASE64
            .decode(call.call_metadata_base64)
            .context("decode language runner function-call metadata")?,
        parent_function_call_id: Some(parent_function_call_id.to_string()),
    })
}

fn validate_base64(label: &str, encoded: &str) -> Result<()> {
    BASE64
        .decode(encoded)
        .with_context(|| format!("decode language runner {label}"))?;
    Ok(())
}

fn validate_request_state_operation(
    operation_id: &str,
    operation: &AgentRequestStateOperation,
) -> Result<()> {
    if operation_id.is_empty() || operation_id.len() > MAX_REQUEST_STATE_OPERATION_ID_BYTES {
        bail!(
            "language runner request-state operation id must contain 1 to {MAX_REQUEST_STATE_OPERATION_ID_BYTES} bytes"
        );
    }
    let key = match operation {
        AgentRequestStateOperation::Get { key } | AgentRequestStateOperation::Set { key, .. } => {
            key
        }
    };
    if key.len() > MAX_REQUEST_STATE_KEY_BYTES {
        bail!(
            "language runner request-state key contains {} bytes; maximum is {MAX_REQUEST_STATE_KEY_BYTES}",
            key.len()
        );
    }
    if let AgentRequestStateOperation::Set { value_base64, .. } = operation {
        let value = BASE64
            .decode(value_base64)
            .context("decode language runner request-state value")?;
        if value.len() > MAX_REQUEST_STATE_VALUE_BYTES {
            bail!(
                "language runner request-state value contains {} bytes; maximum is {MAX_REQUEST_STATE_VALUE_BYTES}",
                value.len()
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::RequestHeader,
        state_machine::{AgentFunctionCallResultPayload, AgentInput},
    };

    fn assignment() -> AgentAssignment {
        AgentAssignment {
            attempt_id: "attempt-1".into(),
            fence_token: 7,
            function_run_id: "parent-call".into(),
            request_id: "request-1".into(),
            namespace: "default".into(),
            application: "app".into(),
            application_version: "v1".into(),
            function: "parent".into(),
            timeout_ms: 1_000,
            initialization_timeout_ms: 1_000,
            inputs: vec![AgentInput {
                source_function_call_id: None,
                data_base64: BASE64.encode(b"input"),
                blob: None,
                read: None,
                metadata_base64: String::new(),
                content_type: "application/octet-stream".into(),
            }],
            request_headers: vec![RequestHeader {
                name: "x-test".into(),
                value: "value".into(),
            }],
            call_metadata_base64: String::new(),
            application_code_base64: BASE64.encode(b"code"),
            application_code_sha256: "digest".into(),
            execution_history: Vec::new(),
        }
    }

    fn core_with_attempt() -> RuntimeCore {
        RuntimeCore {
            initialized: true,
            attempts: HashMap::from([(
                "attempt-1".into(),
                AttemptState {
                    assignment: assignment(),
                    operations: Vec::new(),
                    call_indexes: HashMap::new(),
                    watchers: HashMap::new(),
                    watched_calls: HashSet::new(),
                    next_watcher_sequence: 1,
                    execution_history: Vec::new(),
                    next_history_sequence: 1,
                    request_state_operations: HashMap::new(),
                },
            )]),
            closed_attempts: HashSet::new(),
            closed_attempt_order: VecDeque::new(),
        }
    }

    #[test]
    fn rust_core_builds_graph_and_allocates_watcher_identity() {
        let mut core = core_with_attempt();
        assert!(
            core.handle_sdk_output(SdkOutput::CallBatch {
                attempt_id: "attempt-1".into(),
                calls: vec![SdkFunctionCall {
                    function_call_id: "child-call".into(),
                    function_name: "child".into(),
                    inputs: vec![SdkFunctionArgument::Data {
                        data_base64: BASE64.encode(b"argument"),
                        metadata_base64: BASE64.encode(b"metadata"),
                        content_type: "application/x-test".into(),
                    }],
                    call_metadata_base64: BASE64.encode(b"call-metadata"),
                }],
            })
            .unwrap()
            .is_empty()
        );

        let output = core
            .handle_sdk_output(SdkOutput::Watch {
                attempt_id: "attempt-1".into(),
                function_call_id: "child-call".into(),
                timeout_ms: Some(250),
            })
            .unwrap();
        let CoreAgentOutput::FunctionCall {
            watcher_id,
            updates,
            timeout_ms,
            ..
        } = &output[0]
        else {
            panic!("expected function-call output")
        };
        assert_eq!(watcher_id, "attempt-1:1");
        assert_eq!(*timeout_ms, Some(250));
        assert_eq!(updates.output_function_call_id, "child-call");
        assert_eq!(updates.operations.len(), 1);
        let ComputeOperation::FunctionCall { call } = &updates.operations[0] else {
            panic!("expected function call")
        };
        assert_eq!(call.parent_function_call_id.as_deref(), Some("parent-call"));
        assert_eq!(call.call_metadata, b"call-metadata");
    }

    #[test]
    fn rust_core_replays_an_exact_durable_command() {
        let mut core = core_with_attempt();
        core.handle_sdk_output(SdkOutput::CallBatch {
            attempt_id: "attempt-1".into(),
            calls: vec![SdkFunctionCall {
                function_call_id: "child-call".into(),
                function_name: "child".into(),
                inputs: Vec::new(),
                call_metadata_base64: String::new(),
            }],
        })
        .unwrap();
        let updates = GraphUpdates {
            operations: core.attempts["attempt-1"].operations.clone(),
            output_function_call_id: "child-call".into(),
        };
        core.attempts
            .get_mut("attempt-1")
            .unwrap()
            .execution_history = vec![ExecutionHistoryEntry {
            sequence: 1,
            command: ExecutionHistoryCommand::FunctionCall {
                updates,
                timeout_ms: Some(250),
            },
            result: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        }];

        let output = core
            .handle_sdk_output(SdkOutput::Watch {
                attempt_id: "attempt-1".into(),
                function_call_id: "child-call".into(),
                timeout_ms: Some(250),
            })
            .unwrap();
        assert!(matches!(
            output.as_slice(),
            [CoreAgentOutput::FunctionCall {
                history_sequence: 1,
                ..
            }]
        ));
    }

    #[test]
    fn rust_core_fences_a_replay_command_mismatch() {
        let mut core = core_with_attempt();
        core.handle_sdk_output(SdkOutput::CallBatch {
            attempt_id: "attempt-1".into(),
            calls: vec![SdkFunctionCall {
                function_call_id: "child-call".into(),
                function_name: "child".into(),
                inputs: Vec::new(),
                call_metadata_base64: String::new(),
            }],
        })
        .unwrap();
        core.attempts
            .get_mut("attempt-1")
            .unwrap()
            .execution_history = vec![ExecutionHistoryEntry {
            sequence: 1,
            command: ExecutionHistoryCommand::RequestState {
                operation: AgentRequestStateOperation::Get {
                    key: "different-command".into(),
                },
            },
            result: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        }];

        let output = core
            .handle_sdk_output(SdkOutput::Watch {
                attempt_id: "attempt-1".into(),
                function_call_id: "child-call".into(),
                timeout_ms: None,
            })
            .unwrap();
        assert!(matches!(
            output.as_slice(),
            [CoreAgentOutput::Failure {
                reason: FailureReason::ReplayEventHistoryMismatch,
                ..
            }]
        ));
        assert!(!core.attempts.contains_key("attempt-1"));
    }

    #[test]
    fn rust_core_fences_replay_that_ends_before_the_history_frontier() {
        let mut core = core_with_attempt();
        core.attempts
            .get_mut("attempt-1")
            .unwrap()
            .execution_history = vec![ExecutionHistoryEntry {
            sequence: 1,
            command: ExecutionHistoryCommand::RequestState {
                operation: AgentRequestStateOperation::Get {
                    key: "expected".into(),
                },
            },
            result: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        }];
        let output = core
            .handle_sdk_output(SdkOutput::Success {
                attempt_id: "attempt-1".into(),
                result: SdkSuccessResult::Value {
                    output_base64: BASE64.encode(b"output"),
                    metadata_base64: String::new(),
                    content_type: "application/octet-stream".into(),
                },
            })
            .unwrap();
        assert!(matches!(
            output.as_slice(),
            [CoreAgentOutput::Failure {
                reason: FailureReason::ReplayEventHistoryMismatch,
                ..
            }]
        ));
    }

    #[test]
    fn rust_core_forwards_execution_suspension_boundaries() {
        let mut core = core_with_attempt();
        let suspended = core
            .handle_sdk_output(SdkOutput::Suspend {
                attempt_id: "attempt-1".into(),
            })
            .unwrap();
        assert!(matches!(
            suspended.as_slice(),
            [CoreAgentOutput::Suspend { attempt_id }] if attempt_id == "attempt-1"
        ));

        let resumed = core
            .handle_sdk_output(SdkOutput::Resume {
                attempt_id: "attempt-1".into(),
            })
            .unwrap();
        assert!(matches!(
            resumed.as_slice(),
            [CoreAgentOutput::Resume { attempt_id }] if attempt_id == "attempt-1"
        ));
    }

    #[test]
    fn rust_core_returns_all_buffered_operations_for_tail_call() {
        let mut core = core_with_attempt();
        core.handle_sdk_output(SdkOutput::CallBatch {
            attempt_id: "attempt-1".into(),
            calls: vec![SdkFunctionCall {
                function_call_id: "tail-call".into(),
                function_name: "child".into(),
                inputs: Vec::new(),
                call_metadata_base64: String::new(),
            }],
        })
        .unwrap();
        let output = core
            .handle_sdk_output(SdkOutput::Success {
                attempt_id: "attempt-1".into(),
                result: SdkSuccessResult::CallGraph {
                    output_function_call_id: "tail-call".into(),
                },
            })
            .unwrap();
        let CoreAgentOutput::Success {
            result: AgentSuccessResult::CallGraph { updates },
            ..
        } = &output[0]
        else {
            panic!("expected call-graph success")
        };
        assert_eq!(updates.output_function_call_id, "tail-call");
        assert_eq!(updates.operations.len(), 1);
    }

    #[test]
    fn rust_core_rejects_conflicting_call_redefinition() {
        let mut core = core_with_attempt();
        let call = || SdkFunctionCall {
            function_call_id: "child-call".into(),
            function_name: "child".into(),
            inputs: Vec::new(),
            call_metadata_base64: String::new(),
        };
        core.handle_sdk_output(SdkOutput::CallBatch {
            attempt_id: "attempt-1".into(),
            calls: vec![call()],
        })
        .unwrap();
        let error = core
            .handle_sdk_output(SdkOutput::CallBatch {
                attempt_id: "attempt-1".into(),
                calls: vec![SdkFunctionCall {
                    function_name: "different".into(),
                    ..call()
                }],
            })
            .unwrap_err();
        assert!(error.to_string().contains("redefined function call"));
    }

    #[test]
    fn rust_core_validates_and_routes_request_state_operations() {
        let decoded: SdkOutput = serde_json::from_value(serde_json::json!({
            "type": "request_state",
            "attempt_id": "attempt-1",
            "operation_id": "attempt-1:state:1",
            "operation": {
                "operation": "set",
                "key": "saved",
                "value_base64": BASE64.encode(b"serialized-value"),
            },
        }))
        .unwrap();
        let mut core = core_with_attempt();
        let output = core.handle_sdk_output(decoded).unwrap();
        let CoreAgentOutput::RequestState {
            attempt_id,
            history_sequence,
            operation_id,
            operation,
        } = &output[0]
        else {
            panic!("expected request-state output")
        };
        assert_eq!(attempt_id, "attempt-1");
        assert_eq!(*history_sequence, 1);
        assert_eq!(operation_id, "attempt-1:state:1");
        assert_eq!(
            operation,
            &AgentRequestStateOperation::Set {
                key: "saved".into(),
                value_base64: BASE64.encode(b"serialized-value"),
            }
        );

        let duplicate = core
            .handle_sdk_output(SdkOutput::RequestState {
                attempt_id: "attempt-1".into(),
                operation_id: "attempt-1:state:1".into(),
                operation: AgentRequestStateOperation::Get {
                    key: "saved".into(),
                },
            })
            .unwrap_err();
        assert!(duplicate.to_string().contains("reused request-state"));
    }

    #[test]
    fn child_result_payload_serializes_for_the_sdk_runtime() {
        let payload = AgentFunctionCallResultPayload::Success {
            output_base64: BASE64.encode(b"output"),
            blob: None,
            read: None,
            metadata_base64: BASE64.encode(b"metadata"),
            content_type: "application/x-test".into(),
        };
        let input = SdkInput::CallResult {
            attempt_id: "attempt-1".into(),
            function_call_id: "child-call".into(),
            result: payload,
        };
        let value = serde_json::to_value(input).unwrap();
        assert_eq!(value["type"], "call_result");
        assert_eq!(value["outcome"], "success");
        assert_eq!(value["function_call_id"], "child-call");
    }
}
