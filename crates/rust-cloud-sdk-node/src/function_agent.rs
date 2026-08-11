//! N-API surface for embedding the disposable Function Service agent in the
//! TypeScript application runner.

#![deny(clippy::all)]

use std::{sync::Arc, time::Duration};

use napi::{Error, Result};
use napi_derive::napi;
use tensorlake_function_agent_core::{
    agent::{EmbeddedSdkRuntime, FunctionAgent, FunctionAgentConfig},
    runtime::SdkOutput,
};
use url::Url;
use uuid::Uuid;

#[napi(object)]
pub struct FunctionAgentOptions {
    pub function_service_url: String,
    pub registration_token: String,
    pub agent_id: Option<String>,
    pub incarnation: Option<String>,
    pub heartbeat_interval_ms: Option<u32>,
    pub request_timeout_ms: Option<u32>,
    pub io_transfer_timeout_ms: Option<i64>,
    pub registration_attempts: Option<u32>,
    pub registration_retry_ms: Option<u32>,
    pub max_events_per_heartbeat: Option<u32>,
    pub max_outbox_events: Option<u32>,
    pub shutdown_timeout_ms: Option<u32>,
}

#[napi]
pub struct FunctionAgentCore {
    runtime: Arc<EmbeddedSdkRuntime>,
}

#[napi]
impl FunctionAgentCore {
    #[napi(constructor)]
    pub fn new(options: FunctionAgentOptions) -> Result<Self> {
        let config = config(options).map_err(native_error)?;
        let (agent, runtime) = FunctionAgent::new_embedded(config).map_err(native_error)?;
        tokio::spawn(async move {
            if let Err(error) = agent.run().await {
                tracing::error!(%error, "embedded TypeScript function agent stopped");
            }
        });
        Ok(Self { runtime })
    }

    #[napi]
    pub async fn next_input(&self) -> Result<String> {
        let input = self.runtime.next_input().await.map_err(native_error)?;
        serde_json::to_string(&input).map_err(native_error)
    }

    #[napi]
    pub async fn submit_output(&self, output_json: String) -> Result<()> {
        let output = serde_json::from_str::<SdkOutput>(&output_json).map_err(native_error)?;
        self.runtime
            .submit_output(output)
            .await
            .map_err(native_error)
    }
}

fn config(options: FunctionAgentOptions) -> anyhow::Result<FunctionAgentConfig> {
    let function_service_url = options.function_service_url.parse::<Url>()?;
    let agent_id = options
        .agent_id
        .unwrap_or_else(|| format!("agent-{}", Uuid::new_v4()));
    Ok(FunctionAgentConfig {
        function_service_url,
        registration_token: options.registration_token,
        agent_id,
        incarnation: options
            .incarnation
            .unwrap_or_else(|| Uuid::new_v4().to_string()),
        heartbeat_interval: Duration::from_millis(u64::from(
            options.heartbeat_interval_ms.unwrap_or(1_000),
        )),
        request_timeout: Duration::from_millis(u64::from(
            options.request_timeout_ms.unwrap_or(10_000),
        )),
        io_transfer_timeout: Duration::from_millis(
            u64::try_from(options.io_transfer_timeout_ms.unwrap_or(3_600_000))
                .map_err(|_| anyhow::anyhow!("io_transfer_timeout_ms must not be negative"))?,
        ),
        registration_attempts: usize::try_from(options.registration_attempts.unwrap_or(120))?,
        registration_retry_interval: Duration::from_millis(u64::from(
            options.registration_retry_ms.unwrap_or(500),
        )),
        max_events_per_heartbeat: usize::try_from(options.max_events_per_heartbeat.unwrap_or(256))?,
        max_outbox_events: usize::try_from(options.max_outbox_events.unwrap_or(4_096))?,
        shutdown_timeout: Duration::from_millis(u64::from(
            options.shutdown_timeout_ms.unwrap_or(10_000),
        )),
    })
}

fn native_error(error: impl std::fmt::Display) -> Error {
    Error::from_reason(error.to_string())
}
