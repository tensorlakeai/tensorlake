use std::{sync::Arc, time::Duration};

use pyo3::{exceptions::PyRuntimeError, prelude::*};
use pyo3_async_runtimes::tokio::future_into_py;
use tensorlake_function_agent_core::{
    agent::{EmbeddedSdkRuntime, FunctionAgent, FunctionAgentConfig},
    runtime::SdkOutput,
};
use url::Url;
use uuid::Uuid;

#[pyclass]
pub struct FunctionAgentCore {
    runtime: Arc<EmbeddedSdkRuntime>,
}

#[pymethods]
impl FunctionAgentCore {
    #[new]
    #[pyo3(signature = (
        function_service_url,
        registration_token,
        agent_id=None,
        incarnation=None,
        heartbeat_interval_ms=1_000,
        request_timeout_ms=10_000,
        io_transfer_timeout_ms=3_600_000,
        registration_attempts=120,
        registration_retry_ms=500,
        max_events_per_heartbeat=256,
        max_outbox_events=4_096,
        shutdown_timeout_ms=10_000,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        function_service_url: String,
        registration_token: String,
        agent_id: Option<String>,
        incarnation: Option<String>,
        heartbeat_interval_ms: u64,
        request_timeout_ms: u64,
        io_transfer_timeout_ms: u64,
        registration_attempts: usize,
        registration_retry_ms: u64,
        max_events_per_heartbeat: usize,
        max_outbox_events: usize,
        shutdown_timeout_ms: u64,
    ) -> PyResult<Self> {
        let function_service_url = function_service_url.parse::<Url>().map_err(python_error)?;
        let config = FunctionAgentConfig {
            function_service_url,
            registration_token,
            agent_id: agent_id.unwrap_or_else(|| format!("agent-{}", Uuid::new_v4())),
            incarnation: incarnation.unwrap_or_else(|| Uuid::new_v4().to_string()),
            heartbeat_interval: Duration::from_millis(heartbeat_interval_ms),
            request_timeout: Duration::from_millis(request_timeout_ms),
            io_transfer_timeout: Duration::from_millis(io_transfer_timeout_ms),
            registration_attempts,
            registration_retry_interval: Duration::from_millis(registration_retry_ms),
            max_events_per_heartbeat,
            max_outbox_events,
            shutdown_timeout: Duration::from_millis(shutdown_timeout_ms),
        };
        let executor = pyo3_async_runtimes::tokio::get_runtime();
        let (agent, runtime) = {
            let _guard = executor.enter();
            FunctionAgent::new_embedded(config).map_err(python_error)?
        };
        executor.spawn(async move {
            if let Err(error) = agent.run().await {
                tracing::error!(%error, "embedded Python function agent stopped");
            }
        });
        Ok(Self { runtime })
    }

    fn next_input<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let runtime = self.runtime.clone();
        future_into_py(py, async move {
            let input = runtime.next_input().await.map_err(python_error)?;
            serde_json::to_string(&input).map_err(python_error)
        })
    }

    fn submit_output<'py>(
        &self,
        py: Python<'py>,
        output_json: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let runtime = self.runtime.clone();
        future_into_py(py, async move {
            let output = serde_json::from_str::<SdkOutput>(&output_json).map_err(python_error)?;
            runtime.submit_output(output).await.map_err(python_error)
        })
    }
}

fn python_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}
