//! Thin synchronous and asyncio handles to the shared Rust TCP tunnel.
use std::{collections::HashMap, sync::Arc, time::Duration};

use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tensorlake::{
    error::SdkError,
    sandboxes::tunnel::{TcpTunnel, TcpTunnelOptions},
};

use super::{into_sandbox_py_error, shared_runtime};

fn options(
    base_url: String,
    headers: HashMap<String, String>,
    remote_port: u16,
    local_host: String,
    local_port: u16,
    timeout: f64,
) -> PyResult<TcpTunnelOptions> {
    let headers = headers
        .into_iter()
        .map(|(name, value)| {
            Ok((
                HeaderName::from_bytes(name.as_bytes())
                    .map_err(|e| SdkError::InvalidHeaderValue(e.to_string()))?,
                HeaderValue::from_str(&value)
                    .map_err(|e| SdkError::InvalidHeaderValue(e.to_string()))?,
            ))
        })
        .collect::<Result<HeaderMap, SdkError>>()
        .map_err(into_sandbox_py_error)?;
    let connect_timeout = Duration::try_from_secs_f64(timeout).map_err(|_| {
        into_sandbox_py_error(SdkError::ClientError(
            "invalid tunnel connect timeout".into(),
        ))
    })?;
    Ok(TcpTunnelOptions {
        base_url,
        headers,
        remote_port,
        local_host,
        local_port,
        connect_timeout,
    })
}

#[pyclass]
pub struct CloudTcpTunnel {
    tunnel: Arc<TcpTunnel>,
}

#[pymethods]
impl CloudTcpTunnel {
    #[new]
    fn new(
        py: Python<'_>,
        base_url: String,
        headers: HashMap<String, String>,
        remote_port: u16,
        local_host: String,
        local_port: u16,
        timeout: f64,
    ) -> PyResult<Self> {
        let options = options(
            base_url,
            headers,
            remote_port,
            local_host,
            local_port,
            timeout,
        )?;
        py.detach(|| shared_runtime().block_on(TcpTunnel::listen(options)))
            .map(|tunnel| Self {
                tunnel: Arc::new(tunnel),
            })
            .map_err(into_sandbox_py_error)
    }

    #[staticmethod]
    fn listen_async(
        py: Python<'_>,
        base_url: String,
        headers: HashMap<String, String>,
        remote_port: u16,
        local_host: String,
        local_port: u16,
        timeout: f64,
    ) -> PyResult<Bound<'_, PyAny>> {
        let options = options(
            base_url,
            headers,
            remote_port,
            local_host,
            local_port,
            timeout,
        )?;
        future_into_py(py, async move {
            TcpTunnel::listen(options)
                .await
                .map(|tunnel| Self {
                    tunnel: Arc::new(tunnel),
                })
                .map_err(into_sandbox_py_error)
        })
    }

    #[getter]
    fn local_port(&self) -> u16 {
        self.tunnel.local_addr().port()
    }

    #[getter]
    fn closed(&self) -> bool {
        self.tunnel.is_closed()
    }

    fn close(&self, py: Python<'_>) {
        py.detach(|| shared_runtime().block_on(self.tunnel.close()));
    }

    fn close_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let tunnel = self.tunnel.clone();
        // Start cancellation before constructing the awaitable, so cancelling the
        // Python task cannot leave its listener running.
        tunnel.cancel();
        future_into_py(py, async move {
            tunnel.closed().await;
            Ok(())
        })
    }
}
