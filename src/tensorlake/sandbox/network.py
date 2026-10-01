"""Host-observed network metadata, distinct from guest network-policy configuration."""

from pydantic import BaseModel


class NetworkEvent(BaseModel):
    namespace: str
    sandbox_id: str
    executor_id: str
    allocation_id: str
    capture_session: str
    sequence: int
    observed_at_ms: int
    event_kind: str
    connection_id: str
    observed_connections: int
    source_ip: str
    source_port: int
    destination_ip: str
    destination_port: int
    transport: str
    original_bytes: int | None = None
    reply_bytes: int | None = None
    original_packets: int | None = None
    reply_packets: int | None = None
    kernel_started_at_ns: int | None = None
    kernel_destroyed_at_ns: int | None = None
    conntrack_status: int
    tcp_state: int | None = None
    detail: str
    observation_source: str
    dns_name: str | None = None
    dns_query_type: int | None = None
    dns_response_code: int | None = None
    dns_answers: list[str]
    policy_action: str | None = None


class NetworkEventsResponse(BaseModel):
    events: list[NetworkEvent]
    next_cursor: str | None = None
    from_ms: int
    to_ms: int


class NetworkDestination(BaseModel):
    destination_ip: str
    destination_port: int
    transport: str
    observed_connections: int
    original_bytes: int | None = None
    reply_bytes: int | None = None
    unknown_byte_connections: int
    degraded_connections: int


class NetworkDestinationsResponse(BaseModel):
    destinations: list[NetworkDestination]
    truncated: bool
    from_ms: int
    to_ms: int


class NetworkCaptureStatus(BaseModel):
    state: str
    last_observed_at_ms: int | None = None
    allocation_id: str | None = None
    coverage: list[str]
    limitations: list[str]
