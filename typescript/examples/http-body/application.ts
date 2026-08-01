import { createHash } from "node:crypto";
import {
  HttpBody,
  type Infer,
  RequestContext,
  registerApplication,
  registerFunction,
  schema,
} from "tensorlake/applications";

const webhookEventSchema = schema.object({
  event: schema.string(),
  id: schema.string(),
});

type WebhookEvent = Infer<typeof webhookEventSchema>;

const normalizeWebhookEvent = registerFunction(
  async (event: WebhookEvent) => ({
    eventId: event.id,
    eventType: event.event.toLowerCase(),
  }),
  {
    name: "normalize_webhook_event",
    description: "Normalizes a validated webhook event for downstream processing.",
    parameters: [
      schema.parameter("event", webhookEventSchema),
    ] as const,
    returns: schema.object({
      eventId: schema.string(),
      eventType: schema.string(),
    }),
  },
);

export const ingestWebhook = registerApplication(
  async (body: HttpBody) => {
    const event = body.json<WebhookEvent>();
    const normalized = await normalizeWebhookEvent(event);
    const deliveryId = RequestContext.get().headers.get("x-webhook-id") ?? null;

    return {
      ...normalized,
      contentType: body.contentType ?? null,
      deliveryId,
      payloadSha256: createHash("sha256").update(body.content).digest("hex"),
      rawByteLength: body.content.byteLength,
    };
  },
  {
    name: "typescript_http_body_webhook",
    description: "Ingests a webhook without changing its raw request bytes.",
    tags: {
      example: "http-body",
      useCase: "webhook-ingestion",
    },
    allow: ["unauthenticated_requests"],
    parameters: [
      schema.parameter("body", schema.httpBody(), {
        description: "The exact webhook request body and its Content-Type.",
      }),
    ] as const,
    returns: schema.object({
      eventId: schema.string(),
      eventType: schema.string(),
      contentType: schema.nullable(schema.string()),
      deliveryId: schema.nullable(schema.string()),
      payloadSha256: schema.string(),
      rawByteLength: schema.integer(),
    }),
  },
);
