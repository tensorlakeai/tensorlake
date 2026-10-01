/** Persisted host observations. Connections are not HTTP requests. */
export interface NetworkEvent {
  namespace: string;
  sandbox_id: string;
  executor_id: string;
  allocation_id: string;
  capture_session: string;
  sequence: number;
  observed_at_ms: number;
  event_kind: string;
  connection_id: string;
  observed_connections: number;
  source_ip: string;
  source_port: number;
  destination_ip: string;
  destination_port: number;
  transport: string;
  original_bytes: number | null;
  reply_bytes: number | null;
  original_packets: number | null;
  reply_packets: number | null;
  kernel_started_at_ns: number | null;
  kernel_destroyed_at_ns: number | null;
  conntrack_status: number;
  tcp_state: number | null;
  detail: string;
  observation_source: string;
  dns_name: string | null;
  dns_query_type: number | null;
  dns_response_code: number | null;
  dns_answers: string[];
  policy_action: string | null;
}

export interface NetworkEventsResponse {
  events: NetworkEvent[];
  next_cursor: string | null;
  from_ms: number;
  to_ms: number;
}

export interface NetworkDestination {
  destination_ip: string;
  destination_port: number;
  transport: string;
  observed_connections: number;
  original_bytes: number | null;
  reply_bytes: number | null;
  unknown_byte_connections: number;
  degraded_connections: number;
}

export interface NetworkDestinationsResponse {
  destinations: NetworkDestination[];
  truncated: boolean;
  from_ms: number;
  to_ms: number;
}

export interface NetworkCaptureStatus {
  state: string;
  last_observed_at_ms: number | null;
  allocation_id: string | null;
  coverage: string[];
  limitations: string[];
}

export interface NetworkQuery {
  fromMs?: number;
  toMs?: number;
  limit?: number;
  cursor?: string;
}
