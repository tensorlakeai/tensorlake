export interface NativeFunctionAgentOptions {
  functionServiceUrl: string;
  registrationToken: string;
  agentId?: string;
  incarnation?: string;
  heartbeatIntervalMs?: number;
  requestTimeoutMs?: number;
  ioTransferTimeoutMs?: number;
  registrationAttempts?: number;
  registrationRetryMs?: number;
  maxEventsPerHeartbeat?: number;
  maxOutboxEvents?: number;
  shutdownTimeoutMs?: number;
  secretServiceWorkloadUrl?: string;
  credentialRequestTimeoutMs?: number;
}

export interface NativeFunctionAgentCore {
  nextInput(): Promise<string>;
  submitOutput(outputJson: string): Promise<void>;
}

export interface NativeFunctionAgentBinding {
  FunctionAgentCore: new (options: NativeFunctionAgentOptions) => NativeFunctionAgentCore;
}

export interface AgentInputValue {
  source_function_call_id?: string;
  data_base64: string;
  metadata_base64?: string;
  content_type: string;
}

export interface Assignment {
  attempt_id: string;
  fence_token: number;
  function_run_id: string;
  request_id: string;
  namespace: string;
  application: string;
  application_version: string;
  function: string;
  timeout_ms: number;
  initialization_timeout_ms: number;
  inputs: AgentInputValue[];
  request_headers?: Array<{ name: string; value: string }>;
  call_metadata_base64: string;
  application_code_base64: string;
  application_code_sha256: string;
  resolved_environment?: Array<{ target: string; value: string }>;
}

export type AgentInput =
  | { type: "assignment"; assignment: Assignment }
  | ({
      type: "call_result";
      attempt_id: string;
      function_call_id: string;
    } & CallResult)
  | {
      type: "request_state_result";
      result: RequestStateResult;
    }
  | { type: "cancel"; attempt_id: string }
  | { type: "shutdown" };

export type CallResult =
  | {
      outcome: "success";
      output_base64: string;
      metadata_base64?: string;
      content_type: string;
    }
  | { outcome: "failure"; reason: string }
  | { outcome: "timed_out" };

export type RequestStateResult = {
  operation_id: string;
  attempt_id: string;
  fence_token: number;
} & (
  | { result: "get"; value_base64?: string }
  | { result: "set" }
);
