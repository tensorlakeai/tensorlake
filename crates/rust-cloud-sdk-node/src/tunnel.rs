//! A worker-owned handle; all socket and TLS operations run in the Rust core.
use std::{collections::HashMap, sync::Arc, time::Duration};

use napi_derive::napi;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tensorlake::{
    error::SdkError,
    sandboxes::tunnel::{TcpTunnel, TcpTunnelOptions},
};
use tokio::sync::Mutex;

use crate::sandbox::into_napi_error;

#[napi]
pub struct NativeTcpTunnel {
    tunnel: Mutex<Option<Arc<TcpTunnel>>>,
}

#[napi]
impl NativeTcpTunnel {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            tunnel: Mutex::new(None),
        }
    }

    #[napi]
    pub async fn listen(
        &self,
        base_url: String,
        headers: HashMap<String, String>,
        remote_port: u16,
        local_host: String,
        local_port: u16,
        timeout: f64,
    ) -> napi::Result<u16> {
        let mut slot = self.tunnel.lock().await;
        if slot.is_some() {
            return Err(napi::Error::from_reason("tunnel already started"));
        }
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
            .map_err(into_napi_error)?;
        let connect_timeout = Duration::try_from_secs_f64(timeout).map_err(|_| {
            into_napi_error(SdkError::ClientError(
                "invalid tunnel connect timeout".into(),
            ))
        })?;
        let tunnel = TcpTunnel::listen(TcpTunnelOptions {
            base_url,
            headers,
            remote_port,
            local_host,
            local_port,
            connect_timeout,
        })
        .await
        .map_err(into_napi_error)?;
        let port = tunnel.local_addr().port();
        *slot = Some(Arc::new(tunnel));
        Ok(port)
    }

    #[napi]
    pub async fn close(&self) {
        let tunnel = self.tunnel.lock().await.clone();
        if let Some(tunnel) = tunnel {
            tunnel.close().await;
        }
    }

    #[napi]
    pub async fn wait_closed(&self) {
        let tunnel = self.tunnel.lock().await.clone();
        if let Some(tunnel) = tunnel {
            tunnel.closed().await;
        }
    }
}

impl Default for NativeTcpTunnel {
    fn default() -> Self {
        Self::new()
    }
}
