//! Protocol-v1 function agent core.
//!
//! The core owns the disposable side of the Function Service protocol and is
//! embedded directly in the production Python and TypeScript runners.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use reqwest::{Client, StatusCode, header::ETAG};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinSet,
};
use tracing::{error, info, warn};
use url::Url;

use crate::{
    io::{AgentUploadPlan, CommitWriteRequest, PrepareWriteRequest, PrepareWriteResponse},
    model::FailureReason,
    runtime::{CoreAgentInput, CoreAgentOutput, RuntimeCore, SdkInput, SdkOutput},
    state_machine::{
        AGENT_HEARTBEAT_BODY_LIMIT_BYTES, AGENT_PROTOCOL_VERSION, AgentActiveAttempt,
        AgentAssignment, AgentEvent, AgentEventPayload, AgentFunctionCallResult,
        AgentFunctionCallResultPayload, AgentHeartbeatRequest, AgentHeartbeatResponse,
        AgentRequestStateResult, AgentRunResult, AgentSuccessResult, RegisterAgentRequest,
        RegisterAgentResponse,
    },
};

#[derive(Debug, Clone)]
pub struct FunctionAgentConfig {
    pub function_service_url: Url,
    pub registration_token: String,
    pub agent_id: String,
    pub incarnation: String,
    pub heartbeat_interval: Duration,
    pub request_timeout: Duration,
    pub io_transfer_timeout: Duration,
    pub registration_attempts: usize,
    pub registration_retry_interval: Duration,
    pub max_events_per_heartbeat: usize,
    pub max_outbox_events: usize,
    pub shutdown_timeout: Duration,
}

pub struct FunctionAgent {
    config: FunctionAgentConfig,
    client: Client,
    transfer_client: Client,
    next_event_sequence: u64,
    max_concurrency: u32,
    outbox: VecDeque<AgentEvent>,
    attempts: HashMap<String, ActiveAttempt>,
    pending_assignments: HashSet<String>,
    pending_function_call_results: HashSet<(String, String)>,
    runtime: Option<RuntimeConnection>,
    runtime_tx: mpsc::Sender<AgentMessage>,
    runtime_rx: mpsc::Receiver<AgentMessage>,
    io_tasks: JoinSet<()>,
}

struct RuntimeConnection {
    input_tx: mpsc::Sender<CoreAgentInput>,
    task: tokio::task::JoinHandle<()>,
    initialization_deadline: Option<tokio::time::Instant>,
    initialized: bool,
}

impl RuntimeConnection {
    async fn send(&mut self, input: CoreAgentInput, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, self.input_tx.send(input))
            .await
            .map_err(|_| anyhow!("embedded runtime input delivery timed out"))?
            .map_err(|_| anyhow!("embedded runtime stopped"))?;
        Ok(())
    }

    fn failure(&mut self) -> Option<(FailureReason, String)> {
        self.task.is_finished().then(|| {
            (
                FailureReason::RuntimeLost,
                "embedded function runtime stopped".to_string(),
            )
        })
    }

    async fn shutdown(mut self, timeout: Duration) {
        self.task.abort();
        if tokio::time::timeout(timeout, &mut self.task).await.is_err() {
            warn!("embedded function runtime did not stop before shutdown deadline");
        }
    }
}

/// Language-facing half of an embedded function agent.
///
/// N-API and PyO3 runners await inputs and submit outputs through this handle;
/// the HTTP agent and deterministic execution core remain entirely in Rust.
pub struct EmbeddedSdkRuntime {
    input_rx: Mutex<mpsc::Receiver<SdkInput>>,
    output_tx: mpsc::Sender<SdkOutput>,
}

struct ActiveAttempt {
    assignment: AgentAssignment,
    execution_deadline: Option<tokio::time::Instant>,
    execution_suspended: bool,
    output_uploading: bool,
}

impl ActiveAttempt {
    fn reset_execution_deadline(&mut self, now: tokio::time::Instant) {
        if !self.execution_suspended {
            self.execution_deadline = Some(now + Duration::from_millis(self.assignment.timeout_ms));
        }
    }

    fn suspend_execution(&mut self) {
        self.execution_suspended = true;
        self.execution_deadline = None;
    }

    fn resume_execution(&mut self, now: tokio::time::Instant) {
        self.execution_suspended = false;
        self.reset_execution_deadline(now);
    }
}

#[derive(Debug, thiserror::Error)]
#[error("function agent was terminally fenced: {0}")]
struct AgentFenced(String);

#[derive(Debug)]
enum AgentMessage {
    Runtime(CoreAgentOutput),
    RuntimeFailed(String),
    AssignmentPrepared {
        attempt_id: String,
        fence_token: u64,
        result: Result<AgentAssignment, String>,
    },
    FunctionCallResultPrepared {
        attempt_id: String,
        watcher_id: String,
        result: Result<AgentFunctionCallResult, String>,
    },
    UploadFinished {
        attempt_id: String,
        result: Result<AgentSuccessResult, String>,
    },
}

impl FunctionAgent {
    fn new_validated(config: FunctionAgentConfig) -> Result<Self> {
        install_default_crypto_provider();
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .context("build Function Service agent HTTP client")?;
        let transfer_client = Client::builder()
            .timeout(config.io_transfer_timeout)
            .build()
            .context("build function-agent object transfer HTTP client")?;
        let (runtime_tx, runtime_rx) = mpsc::channel(config.max_outbox_events);
        Ok(Self {
            config,
            client,
            transfer_client,
            next_event_sequence: 1,
            max_concurrency: 0,
            outbox: VecDeque::new(),
            attempts: HashMap::new(),
            pending_assignments: HashSet::new(),
            pending_function_call_results: HashSet::new(),
            runtime: None,
            runtime_tx,
            runtime_rx,
            io_tasks: JoinSet::new(),
        })
    }

    pub fn new_embedded(
        config: FunctionAgentConfig,
    ) -> Result<(Self, std::sync::Arc<EmbeddedSdkRuntime>)> {
        validate_config(&config)?;
        let mut agent = Self::new_validated(config)?;
        let capacity = agent.config.max_outbox_events;
        let (core_input_tx, mut core_input_rx) = mpsc::channel(capacity);
        let (sdk_input_tx, sdk_input_rx) = mpsc::channel(capacity);
        let (sdk_output_tx, mut sdk_output_rx) = mpsc::channel(capacity);
        let agent_tx = agent.runtime_tx.clone();
        let task = tokio::spawn(async move {
            let mut core = RuntimeCore::default();
            loop {
                let result = tokio::select! {
                    input = core_input_rx.recv() => {
                        let Some(input) = input else { return; };
                        match core.handle_agent_input(input) {
                            Ok(Some(input)) => sdk_input_tx
                                .send(input)
                                .await
                                .map_err(|_| anyhow!("language runner dropped its SDK input channel")),
                            Ok(None) => Ok(()),
                            Err(error) => Err(error.context("apply embedded agent input")),
                        }
                    }
                    output = sdk_output_rx.recv() => {
                        let Some(output) = output else { return; };
                        match core.handle_sdk_output(output) {
                            Ok(outputs) => {
                                for output in outputs {
                                    if agent_tx.send(AgentMessage::Runtime(output)).await.is_err() {
                                        return;
                                    }
                                }
                                Ok(())
                            }
                            Err(error) => Err(error.context("apply embedded SDK output")),
                        }
                    }
                };
                if let Err(error) = result {
                    let _ = agent_tx
                        .send(AgentMessage::RuntimeFailed(format!("{error:#}")))
                        .await;
                    return;
                }
            }
        });
        agent.runtime = Some(RuntimeConnection {
            input_tx: core_input_tx,
            task,
            initialization_deadline: None,
            initialized: false,
        });
        let runtime = std::sync::Arc::new(EmbeddedSdkRuntime {
            input_rx: Mutex::new(sdk_input_rx),
            output_tx: sdk_output_tx,
        });
        Ok((agent, runtime))
    }

    pub async fn run(mut self) -> Result<()> {
        let registration = self.register().await?;
        if registration.protocol_version != AGENT_PROTOCOL_VERSION {
            bail!(
                "Function Service returned protocol version {}; agent requires {}",
                registration.protocol_version,
                AGENT_PROTOCOL_VERSION
            );
        }
        if usize::try_from(registration.max_concurrency).unwrap_or(usize::MAX)
            >= self.config.max_outbox_events
        {
            bail!(
                "agent max_outbox_events {} must exceed function max_concurrency {}",
                self.config.max_outbox_events,
                registration.max_concurrency
            );
        }
        self.next_event_sequence = registration.next_event_sequence;
        self.max_concurrency = registration.max_concurrency;
        info!(
            agent_id = %self.config.agent_id,
            incarnation = %self.config.incarnation,
            sandbox_id = %registration.sandbox_id,
            namespace = %registration.namespace,
            application = %registration.application,
            function = %registration.function,
            max_concurrency = registration.max_concurrency,
            "function agent registered"
        );

        let mut heartbeat = tokio::time::interval(self.config.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    self.reconcile_runtime_messages().await;
                    self.reconcile_exited_runtimes().await;
                    if let Err(error) = self.heartbeat().await {
                        if error.downcast_ref::<AgentFenced>().is_some() {
                            self.io_tasks.abort_all();
                            self.shutdown_runtimes().await;
                            return Err(error);
                        }
                        warn!(%error, "function-agent heartbeat failed; retaining outbox");
                    }
                }
                Some(message) = self.runtime_rx.recv(), if self.has_runtime_message_capacity() => {
                    self.apply_agent_message(message).await;
                }
                Some(joined) = self.io_tasks.join_next(), if !self.io_tasks.is_empty() => {
                    if let Err(error) = joined {
                        warn!(%error, "function-agent I/O task failed");
                    }
                }
                _ = shutdown_signal() => {
                    info!("function-agent shutdown requested");
                    self.io_tasks.abort_all();
                    self.shutdown_runtimes().await;
                    return Ok(());
                }
            }
        }
    }

    async fn register(&self) -> Result<RegisterAgentResponse> {
        let request = RegisterAgentRequest {
            agent_id: self.config.agent_id.clone(),
            incarnation: self.config.incarnation.clone(),
            registration_token: self.config.registration_token.clone(),
        };
        let mut last_error = None;
        for attempt in 1..=self.config.registration_attempts {
            match self
                .post_json("internal/v1/agents/register", &request)
                .await
            {
                Ok(response) => return decode_response(response).await,
                Err(error) => {
                    if error.downcast_ref::<AgentFenced>().is_some() {
                        return Err(error);
                    }
                    warn!(
                        %error,
                        attempt,
                        max_attempts = self.config.registration_attempts,
                        "function-agent registration attempt failed"
                    );
                    last_error = Some(error);
                }
            }
            if attempt < self.config.registration_attempts {
                tokio::time::sleep(self.config.registration_retry_interval).await;
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("function-agent registration failed")))
    }

    async fn heartbeat(&mut self) -> Result<()> {
        let active_attempts = self
            .attempts
            .values()
            .map(|runtime| AgentActiveAttempt {
                attempt_id: runtime.assignment.attempt_id.clone(),
                fence_token: runtime.assignment.fence_token,
            })
            .collect::<Vec<_>>();
        let active_count = u32::try_from(
            active_attempts
                .len()
                .saturating_add(self.pending_assignments.len()),
        )
        .unwrap_or(u32::MAX);
        let event_capacity = self
            .config
            .max_outbox_events
            .saturating_sub(self.outbox.len())
            .saturating_sub(self.config.max_events_per_heartbeat);
        let available_slots = self
            .max_concurrency
            .saturating_sub(active_count)
            .min(u32::try_from(event_capacity).unwrap_or(u32::MAX));
        let mut request = AgentHeartbeatRequest {
            incarnation: self.config.incarnation.clone(),
            available_slots,
            active_attempts,
            events: Vec::new(),
        };
        let empty_body_len = serde_json::to_vec(&request)
            .context("encode empty function-agent heartbeat")?
            .len();
        request.events = bounded_heartbeat_events(
            &self.outbox,
            self.config.max_events_per_heartbeat,
            empty_body_len,
        )?;
        let path = format!(
            "internal/v1/agents/{}/heartbeat",
            urlencoding::encode(&self.config.agent_id)
        );
        let response: AgentHeartbeatResponse =
            decode_response(self.post_json(&path, &request).await?).await?;
        self.acknowledge_events(response.acknowledged_event_sequence);
        for rejection in response.rejected_events {
            warn!(
                sequence = rejection.sequence,
                reason = %rejection.reason,
                "Function Service rejected function-agent event"
            );
        }
        for result in response.function_call_results {
            self.prepare_function_call_result(result);
        }
        for result in response.request_state_results {
            self.deliver_request_state_result(result).await;
        }
        for assignment in response.assignments {
            if self.attempts.contains_key(&assignment.attempt_id)
                || self.pending_assignments.contains(&assignment.attempt_id)
            {
                continue;
            }
            self.prepare_assignment(assignment);
        }
        Ok(())
    }

    fn prepare_assignment(&mut self, mut assignment: AgentAssignment) {
        let attempt_id = assignment.attempt_id.clone();
        let fence_token = assignment.fence_token;
        self.pending_assignments.insert(attempt_id.clone());
        let client = self.transfer_client.clone();
        let sender = self.runtime_tx.clone();
        self.io_tasks.spawn(async move {
            let result = materialize_assignment_inputs(&client, &mut assignment)
                .await
                .map(|()| assignment)
                .map_err(|error| format!("download function inputs: {error:#}"));
            let _ = sender
                .send(AgentMessage::AssignmentPrepared {
                    attempt_id,
                    fence_token,
                    result,
                })
                .await;
        });
    }

    async fn start_runtime(&mut self, assignment: AgentAssignment) -> Result<()> {
        let runtime = self
            .runtime
            .as_mut()
            .expect("embedded runtime is installed at construction");
        runtime.initialization_deadline.get_or_insert_with(|| {
            tokio::time::Instant::now()
                + Duration::from_millis(assignment.initialization_timeout_ms)
        });
        let write_timeout = if runtime.initialized {
            self.config.request_timeout
        } else {
            runtime
                .initialization_deadline
                .expect("initialization deadline was installed above")
                .saturating_duration_since(tokio::time::Instant::now())
        };
        let write_result = runtime
            .send(
                CoreAgentInput::Assignment {
                    assignment: Box::new(assignment.clone()),
                },
                write_timeout,
            )
            .await;
        if let Err(error) = write_result {
            self.fail_runtime(
                FailureReason::RuntimeLost,
                format!("write assignment to persistent function runtime: {error}"),
            )
            .await;
            return Err(error);
        }
        let execution_deadline = runtime
            .initialized
            .then(|| tokio::time::Instant::now() + Duration::from_millis(assignment.timeout_ms));
        info!(
            attempt_id = %assignment.attempt_id,
            function = %assignment.function,
            "accepted assignment into function runtime"
        );
        self.attempts.insert(
            assignment.attempt_id.clone(),
            ActiveAttempt {
                assignment,
                execution_deadline,
                execution_suspended: false,
                output_uploading: false,
            },
        );
        Ok(())
    }

    fn prepare_function_call_result(&mut self, mut result: AgentFunctionCallResult) {
        if self.has_pending_result_ack(&result)
            || self.outbox.len() >= self.config.max_outbox_events
        {
            return;
        }
        if !self.attempts.contains_key(&result.attempt_id) {
            warn!(
                attempt_id = %result.attempt_id,
                watcher_id = %result.watcher_id,
                "cannot deliver function-call result without an active attempt"
            );
            return;
        }
        let key = (result.attempt_id.clone(), result.watcher_id.clone());
        if !self.pending_function_call_results.insert(key.clone()) {
            return;
        }
        let client = self.transfer_client.clone();
        let sender = self.runtime_tx.clone();
        self.io_tasks.spawn(async move {
            let result = materialize_function_call_result(&client, &mut result)
                .await
                .map(|()| result)
                .map_err(|error| format!("download function-call result: {error:#}"));
            let _ = sender
                .send(AgentMessage::FunctionCallResultPrepared {
                    attempt_id: key.0,
                    watcher_id: key.1,
                    result,
                })
                .await;
        });
    }

    async fn deliver_function_call_result(&mut self, result: AgentFunctionCallResult) {
        let write_timeout = self.config.request_timeout;
        let Some(runtime) = self.runtime.as_mut() else {
            warn!(
                attempt_id = %result.attempt_id,
                watcher_id = %result.watcher_id,
                "cannot deliver function-call result without a function runtime"
            );
            return;
        };
        if let Err(error) = runtime
            .send(
                CoreAgentInput::FunctionCallResult {
                    result: result.clone(),
                },
                write_timeout,
            )
            .await
        {
            warn!(
                %error,
                attempt_id = %result.attempt_id,
                watcher_id = %result.watcher_id,
                "failed to deliver function-call result; result will be replayed"
            );
            return;
        }
        self.enqueue(AgentEventPayload::FunctionCallResultAck {
            attempt_id: result.attempt_id,
            fence_token: result.fence_token,
            watcher_id: result.watcher_id,
        });
    }

    fn has_pending_result_ack(&self, result: &AgentFunctionCallResult) -> bool {
        self.outbox.iter().any(|event| {
            matches!(
                &event.payload,
                AgentEventPayload::FunctionCallResultAck {
                    attempt_id,
                    fence_token,
                    watcher_id,
                } if attempt_id == &result.attempt_id
                    && fence_token == &result.fence_token
                    && watcher_id == &result.watcher_id
            )
        })
    }

    async fn deliver_request_state_result(&mut self, result: AgentRequestStateResult) {
        if self.has_pending_request_state_result_ack(&result)
            || self.outbox.len() >= self.config.max_outbox_events
        {
            return;
        }
        if !self.attempts.contains_key(&result.attempt_id) {
            warn!(
                attempt_id = %result.attempt_id,
                operation_id = %result.operation_id,
                "cannot deliver request-state result without an active attempt"
            );
            return;
        }
        let Some(runtime) = self.runtime.as_mut() else {
            warn!(
                attempt_id = %result.attempt_id,
                operation_id = %result.operation_id,
                "cannot deliver request-state result without a function runtime"
            );
            return;
        };
        if let Err(error) = runtime
            .send(
                CoreAgentInput::RequestStateResult {
                    result: result.clone(),
                },
                self.config.request_timeout,
            )
            .await
        {
            warn!(
                %error,
                attempt_id = %result.attempt_id,
                operation_id = %result.operation_id,
                "failed to deliver request-state result; result will be replayed"
            );
            return;
        }
        self.enqueue(AgentEventPayload::RequestStateResultAck {
            attempt_id: result.attempt_id,
            fence_token: result.fence_token,
            operation_id: result.operation_id,
        });
    }

    fn has_pending_request_state_result_ack(&self, result: &AgentRequestStateResult) -> bool {
        self.outbox.iter().any(|event| {
            matches!(
                &event.payload,
                AgentEventPayload::RequestStateResultAck {
                    attempt_id,
                    fence_token,
                    operation_id,
                } if attempt_id == &result.attempt_id
                    && fence_token == &result.fence_token
                    && operation_id == &result.operation_id
            )
        })
    }

    async fn reconcile_runtime_messages(&mut self) {
        while self.has_runtime_message_capacity() {
            let Ok(message) = self.runtime_rx.try_recv() else {
                break;
            };
            self.apply_agent_message(message).await;
        }
    }

    fn has_runtime_message_capacity(&self) -> bool {
        self.outbox
            .len()
            .saturating_add(self.attempts.len())
            .saturating_add(self.pending_assignments.len())
            .saturating_add(self.pending_function_call_results.len())
            .max(1)
            <= self.config.max_outbox_events
    }

    async fn apply_agent_message(&mut self, message: AgentMessage) {
        match message {
            AgentMessage::Runtime(output) => self.apply_runtime_message(output).await,
            AgentMessage::RuntimeFailed(message) => {
                self.fail_runtime(FailureReason::RuntimeLost, message).await;
            }
            AgentMessage::AssignmentPrepared {
                attempt_id,
                fence_token,
                result,
            } => {
                self.pending_assignments.remove(&attempt_id);
                match result {
                    Ok(assignment) => {
                        if let Err(error) = self.start_runtime(assignment).await {
                            error!(%error, %attempt_id, "failed to start assigned function runtime");
                            self.enqueue(AgentEventPayload::Result {
                                result: AgentRunResult::Failure {
                                    attempt_id,
                                    fence_token,
                                    reason: FailureReason::RuntimeLost,
                                    message: Some(error.to_string()),
                                },
                            });
                        }
                    }
                    Err(message) => {
                        warn!(%attempt_id, %message, "failed to materialize function assignment");
                        self.enqueue(AgentEventPayload::Result {
                            result: AgentRunResult::Failure {
                                attempt_id,
                                fence_token,
                                reason: FailureReason::InternalError,
                                message: Some(message),
                            },
                        });
                    }
                }
            }
            AgentMessage::FunctionCallResultPrepared {
                attempt_id,
                watcher_id,
                result,
            } => {
                self.pending_function_call_results
                    .remove(&(attempt_id.clone(), watcher_id.clone()));
                if !self.attempts.contains_key(&attempt_id) {
                    return;
                }
                match result {
                    Ok(result) => self.deliver_function_call_result(result).await,
                    Err(message) => warn!(
                        %attempt_id,
                        %watcher_id,
                        %message,
                        "failed to materialize function-call result; result will be replayed"
                    ),
                }
            }
            AgentMessage::UploadFinished { attempt_id, result } => {
                let Some(attempt) = self.attempts.remove(&attempt_id) else {
                    return;
                };
                match result {
                    Ok(result) => self.enqueue(AgentEventPayload::Result {
                        result: AgentRunResult::Success {
                            attempt_id,
                            fence_token: attempt.assignment.fence_token,
                            result,
                        },
                    }),
                    Err(message) => self.enqueue(AgentEventPayload::Result {
                        result: AgentRunResult::Failure {
                            attempt_id,
                            fence_token: attempt.assignment.fence_token,
                            reason: FailureReason::InternalError,
                            message: Some(message),
                        },
                    }),
                }
            }
        }
    }

    async fn apply_runtime_message(&mut self, output: CoreAgentOutput) {
        let output = match output {
            CoreAgentOutput::Initialized => {
                let Some(runtime) = self.runtime.as_mut() else {
                    return;
                };
                if runtime.initialized {
                    warn!("function runtime repeated its initialized message");
                    return;
                }
                runtime.initialized = true;
                runtime.initialization_deadline = None;
                let now = tokio::time::Instant::now();
                for attempt in self.attempts.values_mut() {
                    attempt.reset_execution_deadline(now);
                }
                info!("persistent function runtime initialized");
                return;
            }
            output => output,
        };
        if !self
            .runtime
            .as_ref()
            .is_some_and(|runtime| runtime.initialized)
        {
            self.fail_runtime(
                FailureReason::RuntimeLost,
                "function runtime emitted output before initialization completed".into(),
            )
            .await;
            return;
        }
        match output {
            CoreAgentOutput::Initialized => unreachable!("handled above"),
            CoreAgentOutput::Suspend { attempt_id } => {
                if let Some(attempt) = self.attempts.get_mut(&attempt_id) {
                    attempt.suspend_execution();
                }
            }
            CoreAgentOutput::Resume { attempt_id } => {
                if let Some(attempt) = self.attempts.get_mut(&attempt_id) {
                    attempt.resume_execution(tokio::time::Instant::now());
                }
            }
            CoreAgentOutput::Progress {
                attempt_id,
                message,
            } => {
                if let Some(attempt) = self.attempts.get_mut(&attempt_id) {
                    attempt.reset_execution_deadline(tokio::time::Instant::now());
                    let fence_token = attempt.assignment.fence_token;
                    self.enqueue(AgentEventPayload::Progress {
                        attempt_id,
                        fence_token,
                        message,
                    });
                }
            }
            CoreAgentOutput::FunctionCall {
                attempt_id,
                history_sequence,
                watcher_id,
                updates,
                timeout_ms,
            } => {
                if let Some(fence_token) = self.attempt_fence_token(&attempt_id) {
                    self.enqueue(AgentEventPayload::FunctionCall {
                        attempt_id,
                        fence_token,
                        history_sequence,
                        watcher_id,
                        updates,
                        timeout_ms,
                    });
                }
            }
            CoreAgentOutput::RequestState {
                attempt_id,
                history_sequence,
                operation_id,
                operation,
            } => {
                if let Some(fence_token) = self.attempt_fence_token(&attempt_id) {
                    self.enqueue(AgentEventPayload::RequestState {
                        attempt_id,
                        fence_token,
                        history_sequence,
                        operation_id,
                        operation,
                    });
                }
            }
            CoreAgentOutput::Success { attempt_id, result } => match result {
                AgentSuccessResult::CallGraph { .. } => {
                    if let Some(attempt) = self.attempts.remove(&attempt_id) {
                        self.enqueue(AgentEventPayload::Result {
                            result: AgentRunResult::Success {
                                attempt_id,
                                fence_token: attempt.assignment.fence_token,
                                result,
                            },
                        });
                    }
                }
                AgentSuccessResult::Value {
                    output_base64,
                    metadata_base64,
                    content_type,
                } => {
                    let Some(attempt) = self.attempts.get_mut(&attempt_id) else {
                        return;
                    };
                    if attempt.output_uploading {
                        warn!(%attempt_id, "function runtime repeated a completed value");
                        return;
                    }
                    attempt.output_uploading = true;
                    attempt.suspend_execution();
                    let fence_token = attempt.assignment.fence_token;
                    let client = self.client.clone();
                    let transfer_client = self.transfer_client.clone();
                    let config = self.config.clone();
                    let sender = self.runtime_tx.clone();
                    let upload_attempt_id = attempt_id.clone();
                    self.io_tasks.spawn(async move {
                        let result = upload_function_output(
                            &client,
                            &transfer_client,
                            &config,
                            &upload_attempt_id,
                            fence_token,
                            output_base64,
                            metadata_base64,
                            content_type,
                        )
                        .await
                        .map_err(|error| format!("persist function output: {error:#}"));
                        let _ = sender
                            .send(AgentMessage::UploadFinished {
                                attempt_id: upload_attempt_id,
                                result,
                            })
                            .await;
                    });
                }
                AgentSuccessResult::UploadedValue { .. } => {
                    if let Some(attempt) = self.attempts.remove(&attempt_id) {
                        self.enqueue(AgentEventPayload::Result {
                            result: AgentRunResult::Failure {
                                attempt_id,
                                fence_token: attempt.assignment.fence_token,
                                reason: FailureReason::RuntimeLost,
                                message: Some(
                                    "function runtime emitted a service-only uploaded value"
                                        .to_string(),
                                ),
                            },
                        });
                    }
                }
            },
            CoreAgentOutput::Failure {
                attempt_id,
                reason,
                message,
            } => {
                if let Some(attempt) = self.attempts.remove(&attempt_id) {
                    self.enqueue(AgentEventPayload::Result {
                        result: AgentRunResult::Failure {
                            attempt_id,
                            fence_token: attempt.assignment.fence_token,
                            reason,
                            message,
                        },
                    });
                }
            }
        }
    }

    fn attempt_fence_token(&self, attempt_id: &str) -> Option<u64> {
        self.attempts
            .get(attempt_id)
            .map(|attempt| attempt.assignment.fence_token)
    }

    async fn reconcile_exited_runtimes(&mut self) {
        let capacity = self
            .config
            .max_outbox_events
            .saturating_sub(self.outbox.len());
        let Some(runtime) = self.runtime.as_mut() else {
            return;
        };
        let now = tokio::time::Instant::now();
        let runtime_failure = if !runtime.initialized
            && runtime
                .initialization_deadline
                .is_some_and(|deadline| deadline <= now)
        {
            Some((
                FailureReason::FunctionTimeout,
                "function runtime initialization timed out".to_string(),
            ))
        } else {
            runtime.failure()
        };
        if let Some((reason, message)) = runtime_failure {
            if capacity >= self.attempts.len() {
                self.fail_runtime(reason, message).await;
            }
            return;
        }

        let expired = self
            .attempts
            .iter()
            .filter(|(_, attempt)| {
                attempt
                    .execution_deadline
                    .is_some_and(|deadline| deadline <= now)
            })
            .take(capacity)
            .map(|(attempt_id, _)| attempt_id.clone())
            .collect::<Vec<_>>();
        for attempt_id in expired {
            self.cancel_runtime_attempt(&attempt_id).await;
            let attempt = self
                .attempts
                .remove(&attempt_id)
                .expect("expired attempt remains before removal");
            self.enqueue(AgentEventPayload::Result {
                result: AgentRunResult::Failure {
                    attempt_id,
                    fence_token: attempt.assignment.fence_token,
                    reason: FailureReason::FunctionTimeout,
                    message: Some("function execution timed out".into()),
                },
            });
        }
    }

    async fn cancel_runtime_attempt(&mut self, attempt_id: &str) {
        let Some(runtime) = self.runtime.as_mut() else {
            return;
        };
        if let Err(error) = runtime
            .send(
                CoreAgentInput::Cancel {
                    attempt_id: attempt_id.to_string(),
                },
                self.config.request_timeout,
            )
            .await
        {
            warn!(%error, %attempt_id, "cancel timed-out function execution");
        }
    }

    async fn fail_runtime(&mut self, reason: FailureReason, message: String) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown(self.config.shutdown_timeout).await;
        }
        let attempts = self
            .attempts
            .drain()
            .map(|(_, attempt)| attempt)
            .collect::<Vec<_>>();
        for attempt in attempts {
            self.enqueue(AgentEventPayload::Result {
                result: AgentRunResult::Failure {
                    attempt_id: attempt.assignment.attempt_id,
                    fence_token: attempt.assignment.fence_token,
                    reason: reason.clone(),
                    message: Some(message.clone()),
                },
            });
        }
    }

    fn enqueue(&mut self, payload: AgentEventPayload) {
        assert!(
            self.outbox.len() < self.config.max_outbox_events,
            "function-agent event outbox capacity invariant violated"
        );
        let sequence = self.next_event_sequence;
        self.next_event_sequence = sequence.saturating_add(1);
        self.outbox.push_back(AgentEvent { sequence, payload });
    }

    fn acknowledge_events(&mut self, acknowledged_sequence: u64) {
        while self
            .outbox
            .front()
            .is_some_and(|event| event.sequence <= acknowledged_sequence)
        {
            self.outbox.pop_front();
        }
    }

    async fn post_json<T: Serialize + ?Sized>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<reqwest::Response> {
        let url = self
            .config
            .function_service_url
            .join(path)
            .context("join Function Service agent path")?;
        let response = self
            .client
            .post(url)
            .bearer_auth(&self.config.registration_token)
            .json(body)
            .send()
            .await
            .context("send request to Function Service")?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            if status == StatusCode::GONE {
                return Err(anyhow!(AgentFenced(format!("{status}: {body}"))));
            }
            bail!("Function Service returned {status}: {body}");
        }
        Ok(response)
    }

    async fn shutdown_runtimes(&mut self) {
        let count = self.attempts.len();
        if count > 0 {
            info!(count, "stopping active function executions");
        }
        self.attempts.clear();
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        runtime.shutdown(self.config.shutdown_timeout).await;
    }
}

fn install_default_crypto_provider() {
    #[cfg(all(feature = "tls-aws-lc", feature = "tls-ring"))]
    compile_error!("enable exactly one function-agent TLS crypto provider");

    #[cfg(not(any(feature = "tls-aws-lc", feature = "tls-ring")))]
    compile_error!("enable one function-agent TLS crypto provider");

    #[cfg(feature = "tls-aws-lc")]
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    #[cfg(feature = "tls-ring")]
    let _ = rustls::crypto::ring::default_provider().install_default();
}

impl EmbeddedSdkRuntime {
    pub async fn next_input(&self) -> Result<SdkInput> {
        self.input_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| anyhow!("embedded function-agent core stopped"))
    }

    pub async fn submit_output(&self, output: SdkOutput) -> Result<()> {
        self.output_tx
            .send(output)
            .await
            .map_err(|_| anyhow!("embedded function-agent core stopped"))
    }
}

fn validate_config(config: &FunctionAgentConfig) -> Result<()> {
    if !matches!(config.function_service_url.scheme(), "http" | "https")
        || config.function_service_url.host_str().is_none()
        || config.function_service_url.cannot_be_a_base()
    {
        bail!("function-service URL must be an absolute HTTP origin");
    }
    if config.function_service_url.path() != "/"
        || config.function_service_url.query().is_some()
        || config.function_service_url.fragment().is_some()
    {
        bail!("function-service URL must not contain a path, query, or fragment");
    }
    if config.registration_token.is_empty()
        || config.agent_id.is_empty()
        || config.incarnation.is_empty()
    {
        bail!("agent identity and registration token must not be empty");
    }
    if config.heartbeat_interval.is_zero()
        || config.request_timeout.is_zero()
        || config.io_transfer_timeout.is_zero()
        || config.registration_attempts == 0
        || config.registration_retry_interval.is_zero()
        || config.max_events_per_heartbeat == 0
        || config.max_outbox_events == 0
        || config.shutdown_timeout.is_zero()
    {
        bail!("function-agent intervals and work bounds must be non-zero");
    }
    if config.max_outbox_events <= config.max_events_per_heartbeat {
        bail!("function-agent outbox must exceed one heartbeat event batch");
    }
    Ok(())
}

fn bounded_heartbeat_events(
    outbox: &VecDeque<AgentEvent>,
    max_events: usize,
    empty_body_len: usize,
) -> Result<Vec<AgentEvent>> {
    if empty_body_len > AGENT_HEARTBEAT_BODY_LIMIT_BYTES {
        bail!(
            "function-agent heartbeat metadata contains {empty_body_len} bytes; maximum body size is {AGENT_HEARTBEAT_BODY_LIMIT_BYTES}"
        );
    }
    let mut body_len = empty_body_len;
    let mut events = Vec::new();
    for event in outbox.iter().take(max_events) {
        let encoded_len = serde_json::to_vec(event)
            .context("encode function-agent heartbeat event")?
            .len();
        let separator_len = usize::from(!events.is_empty());
        if body_len
            .saturating_add(separator_len)
            .saturating_add(encoded_len)
            > AGENT_HEARTBEAT_BODY_LIMIT_BYTES
        {
            break;
        }
        body_len += separator_len + encoded_len;
        events.push(event.clone());
    }
    if events.is_empty() && !outbox.is_empty() {
        bail!(
            "oldest function-agent event cannot fit in the {AGENT_HEARTBEAT_BODY_LIMIT_BYTES}-byte heartbeat body limit"
        );
    }
    Ok(events)
}

#[allow(clippy::too_many_arguments)]
async fn upload_function_output(
    client: &Client,
    transfer_client: &Client,
    config: &FunctionAgentConfig,
    attempt_id: &str,
    fence_token: u64,
    output_base64: String,
    metadata_base64: String,
    content_type: String,
) -> Result<AgentSuccessResult> {
    let output = BASE64
        .decode(output_base64)
        .context("decode runtime function output")?;
    let output_size = output.len() as u64;
    let sha256 = hex::encode(Sha256::digest(&output));
    let prepare_path = format!(
        "internal/v1/agents/{}/attempts/{attempt_id}/io/writes",
        config.agent_id
    );
    let prepare: PrepareWriteResponse = decode_response(
        post_agent_json(
            client,
            config,
            &prepare_path,
            &PrepareWriteRequest {
                incarnation: config.incarnation.clone(),
                fence_token,
                size_bytes: output_size,
                sha256: sha256.clone(),
            },
        )
        .await?,
    )
    .await?;

    let part_etags = match prepare.upload {
        AgentUploadPlan::Committed => Vec::new(),
        AgentUploadPlan::SinglePut {
            url,
            method,
            headers,
            ..
        } => {
            if method != "PUT" {
                bail!("Function Service returned unsupported single upload method {method}");
            }
            upload_single(transfer_client, &url, &headers, output).await?;
            Vec::new()
        }
        AgentUploadPlan::Multipart {
            part_size_bytes,
            parts,
            ..
        } => {
            let part_size = usize::try_from(part_size_bytes)
                .context("multipart part size does not fit this agent")?;
            if part_size == 0 || parts.is_empty() {
                bail!("Function Service returned an empty multipart upload plan");
            }
            let expected_parts = output.len().div_ceil(part_size).max(1);
            if parts.len() != expected_parts {
                bail!(
                    "Function Service returned {} upload parts; output requires {expected_parts}",
                    parts.len()
                );
            }
            let mut etags = Vec::with_capacity(parts.len());
            for (index, part) in parts.into_iter().enumerate() {
                if part.part_number as usize != index + 1 {
                    bail!("Function Service returned non-contiguous multipart part numbers");
                }
                let start = index.saturating_mul(part_size).min(output.len());
                let end = start.saturating_add(part_size).min(output.len());
                let response = transfer_client
                    .put(&part.url)
                    .body(output[start..end].to_vec())
                    .send()
                    .await
                    .with_context(|| format!("upload function output part {}", index + 1))?;
                if !response.status().is_success() {
                    bail!(
                        "object store rejected function output part {} with {}",
                        index + 1,
                        response.status()
                    );
                }
                let etag = response
                    .headers()
                    .get(ETAG)
                    .and_then(|value| value.to_str().ok())
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| anyhow!("object store omitted multipart ETag"))?;
                etags.push(etag.to_string());
            }
            etags
        }
    };

    let commit_path = format!(
        "internal/v1/agents/{}/attempts/{attempt_id}/io/writes/{}/commit",
        config.agent_id, prepare.write_id
    );
    let committed: crate::model::BlobReference = decode_response(
        post_agent_json(
            client,
            config,
            &commit_path,
            &CommitWriteRequest {
                incarnation: config.incarnation.clone(),
                fence_token,
                part_etags,
            },
        )
        .await?,
    )
    .await?;
    if committed.sha256 != sha256 || committed.size_bytes != output_size {
        bail!("Function Service committed a different function output blob");
    }
    Ok(AgentSuccessResult::UploadedValue {
        write_id: prepare.write_id,
        metadata_base64,
        content_type,
    })
}

async fn upload_single(
    client: &Client,
    url: &str,
    headers: &std::collections::BTreeMap<String, String>,
    output: Vec<u8>,
) -> Result<()> {
    if url.starts_with("file://") {
        let path = Url::parse(url)
            .context("parse local function output URL")?
            .to_file_path()
            .map_err(|_| anyhow!("local function output URL is not a file path"))?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .context("create local function output directory")?;
        }
        tokio::fs::write(&path, output)
            .await
            .context("write local function output")?;
        return Ok(());
    }
    let mut request = client.put(url).body(output);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = request.send().await.context("upload function output")?;
    if !response.status().is_success() {
        bail!(
            "object store rejected function output with {}",
            response.status()
        );
    }
    Ok(())
}

async fn materialize_assignment_inputs(
    client: &Client,
    assignment: &mut AgentAssignment,
) -> Result<()> {
    for input in &mut assignment.inputs {
        let (Some(blob), Some(read)) = (input.blob.as_ref(), input.read.take()) else {
            continue;
        };
        let bytes = download_blob(client, blob, read).await?;
        input.data_base64 = BASE64.encode(bytes);
    }
    Ok(())
}

async fn materialize_function_call_result(
    client: &Client,
    result: &mut AgentFunctionCallResult,
) -> Result<()> {
    let AgentFunctionCallResultPayload::Success {
        output_base64,
        blob: Some(blob),
        read,
        ..
    } = &mut result.result
    else {
        return Ok(());
    };
    let read = read
        .take()
        .ok_or_else(|| anyhow!("blob function-call result has no read capability"))?;
    let bytes = download_blob(client, blob, *read).await?;
    *output_base64 = BASE64.encode(bytes);
    Ok(())
}

async fn download_blob(
    client: &Client,
    blob: &crate::model::BlobReference,
    read: crate::state_machine::AgentReadPlan,
) -> Result<Vec<u8>> {
    if read.method != "GET" {
        bail!(
            "Function Service returned unsupported blob read method {}",
            read.method
        );
    }
    let bytes = if read.url.starts_with("file://") {
        let path = Url::parse(&read.url)
            .context("parse local function input URL")?
            .to_file_path()
            .map_err(|_| anyhow!("local function input URL is not a file path"))?;
        tokio::fs::read(path)
            .await
            .context("read local function input")?
    } else {
        let mut request = client.get(&read.url);
        for (name, value) in &read.headers {
            request = request.header(name, value);
        }
        let response = request.send().await.context("download function input")?;
        if !response.status().is_success() {
            bail!(
                "object store rejected function input download with {}",
                response.status()
            );
        }
        response
            .bytes()
            .await
            .context("read function input response")?
            .to_vec()
    };
    ensure_blob_bytes(blob, &bytes)?;
    Ok(bytes)
}

fn ensure_blob_bytes(blob: &crate::model::BlobReference, bytes: &[u8]) -> Result<()> {
    ensure!(
        bytes.len() as u64 == blob.size_bytes,
        "downloaded blob size does not match durable reference"
    );
    ensure!(
        hex::encode(Sha256::digest(bytes)) == blob.sha256,
        "downloaded blob digest does not match durable reference"
    );
    Ok(())
}

async fn post_agent_json<T: Serialize + ?Sized>(
    client: &Client,
    config: &FunctionAgentConfig,
    path: &str,
    body: &T,
) -> Result<reqwest::Response> {
    let url = config
        .function_service_url
        .join(path)
        .context("join Function Service agent path")?;
    let response = client
        .post(url)
        .bearer_auth(&config.registration_token)
        .json(body)
        .send()
        .await
        .context("send request to Function Service")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if status == StatusCode::GONE {
            return Err(anyhow!(AgentFenced(format!("{status}: {body}"))));
        }
        bail!("Function Service returned {status}: {body}");
    }
    Ok(response)
}

async fn decode_response<T: for<'de> Deserialize<'de>>(response: reqwest::Response) -> Result<T> {
    response
        .json()
        .await
        .context("decode Function Service agent response")
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "install function-agent Ctrl-C handler");
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                warn!(%error, "install function-agent SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_machine::AgentRequestStateOperation;

    fn config() -> FunctionAgentConfig {
        FunctionAgentConfig {
            function_service_url: Url::parse("http://functions.test/").unwrap(),
            registration_token: "token".into(),
            agent_id: "agent".into(),
            incarnation: "incarnation".into(),
            heartbeat_interval: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            io_transfer_timeout: Duration::from_secs(30),
            registration_attempts: 3,
            registration_retry_interval: Duration::from_millis(1),
            max_events_per_heartbeat: 10,
            max_outbox_events: 100,
            shutdown_timeout: Duration::from_secs(1),
        }
    }

    #[test]
    fn config_rejects_unbounded_or_ambiguous_values() {
        let mut invalid = config();
        invalid.registration_attempts = 0;
        assert!(validate_config(&invalid).is_err());
        invalid = config();
        invalid.function_service_url = Url::parse("http://functions.test/a").unwrap();
        assert!(validate_config(&invalid).is_err());
    }

    #[tokio::test]
    async fn acknowledged_prefix_is_removed_without_losing_later_events() {
        let (mut agent, _runtime) = FunctionAgent::new_embedded(config()).unwrap();
        agent.next_event_sequence = 7;
        for message in ["one", "two", "three"] {
            agent.enqueue(AgentEventPayload::Progress {
                attempt_id: "attempt".into(),
                fence_token: 1,
                message: message.into(),
            });
        }
        agent.acknowledge_events(8);
        assert_eq!(agent.outbox.len(), 1);
        assert_eq!(agent.outbox[0].sequence, 9);
    }

    #[test]
    fn heartbeat_batches_large_state_events_by_encoded_body_size() {
        let mut outbox = VecDeque::new();
        for sequence in 1..=2 {
            outbox.push_back(AgentEvent {
                sequence,
                payload: AgentEventPayload::RequestState {
                    attempt_id: format!("attempt-{sequence}"),
                    fence_token: sequence,
                    history_sequence: 1,
                    operation_id: format!("attempt-{sequence}:state:1"),
                    operation: AgentRequestStateOperation::Set {
                        key: "large".into(),
                        value_base64: "a".repeat(1_398_104),
                    },
                },
            });
        }
        let request = AgentHeartbeatRequest {
            incarnation: "incarnation".into(),
            available_slots: 0,
            active_attempts: vec![],
            events: vec![],
        };
        let empty_body_len = serde_json::to_vec(&request).unwrap().len();
        let events = bounded_heartbeat_events(&outbox, 2, empty_body_len).unwrap();
        assert_eq!(events.len(), 1);

        let encoded = serde_json::to_vec(&AgentHeartbeatRequest { events, ..request }).unwrap();
        assert!(encoded.len() <= AGENT_HEARTBEAT_BODY_LIMIT_BYTES);
    }

    #[test]
    fn request_state_messages_preserve_the_runtime_protocol_shape() {
        let output: CoreAgentOutput = serde_json::from_value(serde_json::json!({
            "type": "request_state",
            "attempt_id": "attempt",
            "history_sequence": 1,
            "operation_id": "attempt:state:1",
            "operation": "set",
            "key": "saved",
            "value_base64": "dmFsdWU=",
        }))
        .unwrap();
        assert!(matches!(
            output,
            CoreAgentOutput::RequestState {
                attempt_id,
                history_sequence,
                operation_id,
                operation: AgentRequestStateOperation::Set { key, value_base64 },
            } if attempt_id == "attempt"
                && history_sequence == 1
                && operation_id == "attempt:state:1"
                && key == "saved"
                && value_base64 == "dmFsdWU="
        ));

        let result = AgentRequestStateResult {
            operation_id: "attempt:state:1".into(),
            attempt_id: "attempt".into(),
            fence_token: 7,
            result: crate::state_machine::AgentRequestStateResultPayload::Set,
        };
        let input = serde_json::to_value(CoreAgentInput::RequestStateResult { result }).unwrap();
        assert_eq!(input["type"], "request_state_result");
        assert_eq!(input["result"]["operation_id"], "attempt:state:1");
        assert_eq!(input["result"]["result"], "set");
    }

    #[test]
    fn suspended_execution_has_no_deadline_until_user_code_resumes() {
        let assignment = AgentAssignment {
            attempt_id: "attempt".into(),
            fence_token: 42,
            function_run_id: "call".into(),
            request_id: "request".into(),
            namespace: "ns".into(),
            application: "app".into(),
            application_version: "v1".into(),
            function: "function".into(),
            timeout_ms: 250,
            initialization_timeout_ms: 1_000,
            inputs: Vec::new(),
            request_headers: Vec::new(),
            call_metadata_base64: String::new(),
            application_code_base64: String::new(),
            application_code_sha256: String::new(),
            execution_history: Vec::new(),
        };
        let mut attempt = ActiveAttempt {
            assignment,
            execution_deadline: None,
            execution_suspended: false,
            output_uploading: false,
        };
        let started = tokio::time::Instant::now();
        attempt.reset_execution_deadline(started);
        assert_eq!(
            attempt.execution_deadline,
            Some(started + Duration::from_millis(250))
        );

        attempt.suspend_execution();
        assert_eq!(attempt.execution_deadline, None);
        attempt.reset_execution_deadline(started + Duration::from_secs(10));
        assert_eq!(attempt.execution_deadline, None);

        let resumed = started + Duration::from_secs(20);
        attempt.resume_execution(resumed);
        assert_eq!(
            attempt.execution_deadline,
            Some(resumed + Duration::from_millis(250))
        );
    }

    #[tokio::test]
    async fn embedded_runtime_handles_multiple_fenced_assignments() {
        let (mut agent, runtime) = FunctionAgent::new_embedded(config()).unwrap();
        runtime.submit_output(SdkOutput::Initialized).await.unwrap();
        let initialized = agent.runtime_rx.recv().await.unwrap();
        agent.apply_agent_message(initialized).await;
        let assignment = AgentAssignment {
            attempt_id: "attempt".into(),
            fence_token: 42,
            function_run_id: "call".into(),
            request_id: "request".into(),
            namespace: "ns".into(),
            application: "app".into(),
            application_version: "v1".into(),
            function: "function".into(),
            timeout_ms: 1_000,
            initialization_timeout_ms: 1_000,
            inputs: Vec::new(),
            request_headers: Vec::new(),
            call_metadata_base64: String::new(),
            application_code_base64: String::new(),
            application_code_sha256: String::new(),
            execution_history: Vec::new(),
        };
        agent.start_runtime(assignment.clone()).await.unwrap();
        assert!(matches!(
            runtime.next_input().await.unwrap(),
            SdkInput::Assignment { .. }
        ));
        let mut second = assignment;
        second.attempt_id = "attempt-2".into();
        second.fence_token = 43;
        agent.start_runtime(second).await.unwrap();
        assert!(matches!(
            runtime.next_input().await.unwrap(),
            SdkInput::Assignment { .. }
        ));
        for attempt_id in ["attempt", "attempt-2"] {
            runtime
                .submit_output(SdkOutput::Failure {
                    attempt_id: attempt_id.to_string(),
                    reason: FailureReason::FunctionError,
                    message: Some("expected test failure".into()),
                })
                .await
                .unwrap();
            let message = tokio::time::timeout(Duration::from_secs(1), agent.runtime_rx.recv())
                .await
                .unwrap()
                .unwrap();
            agent.apply_agent_message(message).await;
        }
        assert!(agent.attempts.is_empty());
        assert_eq!(agent.outbox.len(), 2);
        assert!(matches!(
            &agent.outbox[0].payload,
            AgentEventPayload::Result {
                result: AgentRunResult::Failure {
                    attempt_id,
                    fence_token: 42,
                    reason: FailureReason::FunctionError,
                    ..
                },
            } if attempt_id == "attempt"
        ));
        assert!(matches!(
            &agent.outbox[1].payload,
            AgentEventPayload::Result {
                result: AgentRunResult::Failure {
                    attempt_id,
                    fence_token: 43,
                    reason: FailureReason::FunctionError,
                    ..
                },
            } if attempt_id == "attempt-2"
        ));
    }
}
