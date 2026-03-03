use std::collections::HashMap;

use eventsource_stream::Eventsource;
use futures::StreamExt;
use reqwest::{
    Method,
    header::ACCEPT,
    multipart::{Form, Part},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{client::Client, error::SdkError};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DocumentAiResponse {
    pub status_code: u16,
    pub headers: HashMap<String, String>,
    pub body: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DocumentAiEvent {
    pub event: String,
    pub data: String,
}

#[derive(Clone)]
pub struct DocumentAiClient {
    client: Client,
}

impl DocumentAiClient {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    fn normalize_path(path: &str) -> String {
        path.trim_start_matches('/').to_string()
    }

    async fn response_to_payload(
        response: reqwest::Response,
    ) -> Result<DocumentAiResponse, SdkError> {
        let status_code = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|text| (name.as_str().to_ascii_lowercase(), text.to_string()))
            })
            .collect::<HashMap<_, _>>();
        let body = response.text().await?;

        Ok(DocumentAiResponse {
            status_code,
            headers,
            body,
        })
    }

    pub async fn request(
        &self,
        method: Method,
        path: &str,
        body_json: Option<&Value>,
    ) -> Result<DocumentAiResponse, SdkError> {
        let normalized = Self::normalize_path(path);
        let mut req_builder = self.client.request(method, &normalized);
        if let Some(body_json) = body_json {
            req_builder = req_builder.json(body_json);
        }
        let req = req_builder.build()?;
        let response = self.client.execute_raw(req).await?;
        Self::response_to_payload(response).await
    }

    pub async fn upload_file(
        &self,
        file_name: &str,
        file_contents: Vec<u8>,
    ) -> Result<DocumentAiResponse, SdkError> {
        let form = Form::new().part(
            "file",
            Part::bytes(file_contents).file_name(file_name.to_string()),
        );
        let req = self
            .client
            .build_multipart_request(Method::PUT, "files", form)?;
        let response = self.client.execute_raw(req).await?;
        Self::response_to_payload(response).await
    }

    pub async fn parse_events(&self, parse_id: &str) -> Result<Vec<DocumentAiEvent>, SdkError> {
        let req = self
            .client
            .request(Method::GET, &format!("parse/{parse_id}"))
            .header(ACCEPT, "text/event-stream")
            .build()?;
        let response = self.client.execute_raw(req).await?;
        let status = response.status();

        if !status.is_success() {
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "request failed".to_string());
            return Err(SdkError::ServerError { status, message });
        }

        let stream = response
            .bytes_stream()
            .eventsource()
            .filter_map(move |event| async move {
                match event {
                    Ok(msg) => Some(Ok(DocumentAiEvent {
                        event: msg.event,
                        data: msg.data,
                    })),
                    Err(error) => Some(Err(SdkError::EventSourceError(error.to_string()))),
                }
            });

        futures::pin_mut!(stream);
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            events.push(event?);
        }
        Ok(events)
    }
}
