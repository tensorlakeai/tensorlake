//! napi-rs bindings for an organization's sandbox usage and cost.

use napi_derive::napi;
use tensorlake::ClientBuilder;
use tensorlake::usage::{UsageClient, UsageWindow};

use crate::sandbox::{TracedJson, duration_from_seconds, into_napi_error, usage_error, with_retry};

fn usage_window(last: Option<&str>) -> napi::Result<UsageWindow> {
    match last {
        None => Ok(UsageWindow::CurrentPeriod),
        Some(value) => value
            .parse()
            .map_err(|error: tensorlake::error::SdkError| usage_error(error.to_string())),
    }
}

#[napi]
pub struct NativeUsageClient {
    client: UsageClient,
}

#[napi]
impl NativeUsageClient {
    #[napi(constructor)]
    pub fn new(
        api_url: String,
        api_key: Option<String>,
        organization_id: String,
        user_agent: Option<String>,
        request_timeout_sec: Option<f64>,
    ) -> napi::Result<Self> {
        let mut builder = ClientBuilder::new(&api_url);
        if let Some(token) = api_key.as_deref() {
            builder = builder.bearer_token(token);
        }
        if let Some(ua) = user_agent.as_deref() {
            builder = builder.user_agent(ua);
        }
        if let Some(seconds) = request_timeout_sec {
            builder = builder.timeout(duration_from_seconds("request_timeout_sec", seconds)?);
        }
        let client = builder.build().map_err(into_napi_error)?;
        Ok(Self {
            client: UsageClient::new(client, organization_id),
        })
    }

    /// Organization usage totals for the current billing period, or for the
    /// last `1d`, `7d` or `30d`.
    #[napi]
    pub async fn summary(&self, last: Option<String>) -> napi::Result<TracedJson> {
        let window = usage_window(last.as_deref())?;
        with_retry(self.client.clone(), 5, move |client| async move {
            let traced = client.summary(window).await?;
            Ok(TracedJson {
                json: serde_json::to_string(&*traced)?,
                trace_id: traced.trace_id,
            })
        })
        .await
    }

    /// Every sandbox of the window with its usage and attributed charge.
    #[napi]
    pub async fn sandboxes(&self, last: Option<String>) -> napi::Result<TracedJson> {
        let window = usage_window(last.as_deref())?;
        with_retry(self.client.clone(), 5, move |client| async move {
            let traced = client.sandboxes(window).await?;
            Ok(TracedJson {
                json: serde_json::to_string(&*traced)?,
                trace_id: traced.trace_id,
            })
        })
        .await
    }
}
