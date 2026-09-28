import type { SandboxClient } from "./client.js";
import type {
  PendingSandboxRecord,
  ReadyOptions,
  SandboxInfo,
  SandboxStatus,
} from "./models.js";
import type { Sandbox } from "./sandbox.js";
import type { Traced } from "./traced.js";

/** @internal How a client binds a handle it issued. */
export interface PendingSandboxBinding {
  client: SandboxClient;
  proxyUrl?: string;
  requestTimeout?: number;
  requestedName?: string | null;
  ownsSandbox: boolean;
  traceId: string;
}

/**
 * A sandbox requested with `wait: false`: the handle returned by
 * `Sandbox.create({ wait: false })` / `SandboxClient.create({ wait: false })`.
 *
 * The sandbox is durable and starts whenever capacity allows. Collect it
 * with {@link PendingSandbox.ready}, look at it with
 * {@link PendingSandbox.status}, or from any other process with
 * `Sandbox.connect({ sandboxId })` (which waits while it is pending) or
 * `client.list()`. `pendingReason` is `scheduling` at create time; the
 * capacity reasons appear after the first scheduler pass.
 */
export class PendingSandbox implements PendingSandboxRecord {
  readonly sandboxId: string;
  readonly name?: string;
  readonly state: SandboxStatus;
  readonly pendingReason?: string;
  readonly routingHint?: string;
  readonly ingressEndpoint?: string;
  readonly sandboxUrl?: string;
  readonly reason?: string;
  readonly terminationReason?: string;
  readonly errorDetails?: unknown;
  /** W3C trace id of the create request. */
  readonly traceId: string;

  /** @internal */
  constructor(
    record: PendingSandboxRecord,
    private readonly binding: PendingSandboxBinding,
  ) {
    this.sandboxId = record.sandboxId;
    this.name = record.name;
    this.state = record.state;
    this.pendingReason = record.pendingReason;
    this.routingHint = record.routingHint;
    this.ingressEndpoint = record.ingressEndpoint;
    this.sandboxUrl = record.sandboxUrl;
    this.reason = record.reason;
    this.terminationReason = record.terminationReason;
    this.errorDetails = record.errorDetails;
    this.traceId = binding.traceId;
  }

  /** Fetch the sandbox's current state with one `GET`. */
  async status(): Promise<Traced<SandboxInfo>> {
    return this.binding.client.get(this.sandboxId);
  }

  /**
   * Wait for the sandbox to be running and return a connected `Sandbox`.
   *
   * Polls `GET /sandboxes/{id}` every `pollInterval` seconds (default 2).
   * Call it again after a `SandboxPending` to keep waiting.
   *
   * @throws {SandboxPending} `timeout` elapsed while the sandbox was still
   *   pending. It keeps its place in the queue; the error carries
   *   `sandboxId` and `pendingReason`.
   * @throws {SandboxError} The sandbox settled somewhere else: `terminated`
   *   or `failed` (`reason` is the server's, such as `no_capacity` or
   *   `cancelled`), or `suspended`.
   */
  async ready(options?: ReadyOptions): Promise<Sandbox> {
    return this.binding.client._ready(this.sandboxId, this.binding, options);
  }
}
