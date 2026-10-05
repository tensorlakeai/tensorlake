//! Persisted host observations. Connections are not requests; missing counters
//! remain unknown. Read capture status and gap rows before interpreting totals.
use super::SandboxesClient;
use crate::{client::Traced, error::SdkError};
use reqwest::Method;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NetworkQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkEvent {
    pub namespace: String,
    pub sandbox_id: String,
    pub executor_id: String,
    pub allocation_id: String,
    pub capture_session: String,
    pub sequence: u64,
    pub observed_at_ms: i64,
    /// Connection, DNS, policy, gap or collector-status events. Destruction is a kernel
    /// tracking-entry event, not application completion or TCP close latency.
    pub event_kind: String,
    pub connection_id: String,
    /// One for a detailed connection; cumulative opens for an aggregate bucket.
    pub observed_connections: u64,
    pub source_ip: String,
    pub source_port: u16,
    pub destination_ip: String,
    pub destination_port: u16,
    pub transport: String,
    pub original_bytes: Option<u64>,
    pub reply_bytes: Option<u64>,
    pub original_packets: Option<u64>,
    pub reply_packets: Option<u64>,
    pub kernel_started_at_ns: Option<u64>,
    pub kernel_destroyed_at_ns: Option<u64>,
    pub conntrack_status: u32,
    pub tcp_state: Option<u8>,
    /// Bounded capture-status code; never a customer payload or packet body.
    pub detail: String,
    /// conntrack, dns_packet, policy, egress_proxy or collector.
    pub observation_source: String,
    pub dns_name: Option<String>,
    pub dns_query_type: Option<u16>,
    pub dns_response_code: Option<u16>,
    /// Only bounded A/AAAA answer addresses. TXT and other RDATA are discarded.
    pub dns_answers: Vec<String>,
    pub policy_action: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkEventsResponse {
    pub events: Vec<NetworkEvent>,
    pub next_cursor: Option<String>,
    pub from_ms: i64,
    pub to_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkDestination {
    pub destination_ip: String,
    pub destination_port: u16,
    pub transport: String,
    pub observed_connections: u64,
    /// Cumulative kernel counters for observed connections, not bytes inside
    /// the requested wall-clock window. Unknown counters remain explicit.
    pub original_bytes: Option<u64>,
    pub reply_bytes: Option<u64>,
    pub unknown_byte_connections: u64,
    pub degraded_connections: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkDestinationsResponse {
    pub destinations: Vec<NetworkDestination>,
    pub truncated: bool,
    /// Summary buckets cover whole UTC minutes, inclusive endpoints.
    pub from_ms: i64,
    pub to_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkCaptureStatus {
    /// recent, stale, or unknown. This is observed collector health, separate
    /// from project consent; an empty connection query cannot prove silence.
    pub state: String,
    pub last_observed_at_ms: Option<i64>,
    pub allocation_id: Option<String>,
    pub coverage: Vec<String>,
    pub limitations: Vec<String>,
}

impl SandboxesClient {
    /// Read project-scoped network events from the telemetry API. It works
    /// after sandbox termination and does not contact the guest or sandbox proxy.
    pub async fn network_events(
        &self,
        sandbox_id: &str,
        query: &NetworkQuery,
    ) -> Result<Traced<NetworkEventsResponse>, SdkError> {
        let uri = self.log_endpoint(&format!(
            "sandboxes/{}/network/events",
            urlencoding::encode(sandbox_id)
        ));
        let request = self
            .log_client
            .request(Method::GET, &uri)
            .query(query)
            .build()?;
        self.log_client.execute_json(request).await
    }
    /// Read project-scoped network destinations from the telemetry API. It works
    /// after sandbox termination and does not contact the guest or sandbox proxy.
    pub async fn network_destinations(
        &self,
        sandbox_id: &str,
        query: &NetworkQuery,
    ) -> Result<Traced<NetworkDestinationsResponse>, SdkError> {
        let uri = self.log_endpoint(&format!(
            "sandboxes/{}/network/destinations",
            urlencoding::encode(sandbox_id)
        ));
        let request = self
            .log_client
            .request(Method::GET, &uri)
            .query(query)
            .build()?;
        self.log_client.execute_json(request).await
    }
    /// Read project-scoped network status from the telemetry API. It works
    /// after sandbox termination and does not contact the guest or sandbox proxy.
    pub async fn network_status(
        &self,
        sandbox_id: &str,
    ) -> Result<Traced<NetworkCaptureStatus>, SdkError> {
        let uri = self.log_endpoint(&format!(
            "sandboxes/{}/network/status",
            urlencoding::encode(sandbox_id)
        ));
        let request = self.log_client.request(Method::GET, &uri).build()?;
        self.log_client.execute_json(request).await
    }
}
