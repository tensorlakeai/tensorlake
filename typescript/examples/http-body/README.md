# Raw webhook body with TypeScript

This example receives a webhook as `HttpBody`, preserving the exact bytes and
`Content-Type` sent by the caller. It parses the JSON payload, normalizes the
event in a durable Tensorlake function, and returns a SHA-256 digest that can
be stored for auditing or idempotency. It also reads the sanitized
`X-Webhook-Id` request header from `RequestContext`.

The application sets `allow: ["unauthenticated_requests"]`, making the
webhook ingestion endpoint callable without an API key. Retrieving the
request output still uses an authenticated Tensorlake API request.

`schema.httpBody()` is intentionally used only on the application entrypoint.
After parsing the body, the application passes ordinary JSON data to
`normalize_webhook_event`.

## Install and type-check

From the repository root:

```bash
npm --prefix typescript install
npm --prefix typescript run build:sdk
npm --prefix typescript/examples/http-body install
npm --prefix typescript/examples/http-body run typecheck
```

The repository example uses `"tensorlake": "file:../.."` to resolve the SDK
from this checkout. In an independent project, install the published
`tensorlake` package instead.

## Deploy

```bash
tl deploy typescript/examples/http-body/application.ts
```

## Invoke

Send the body with `--data-binary` so curl does not rewrite it:

```bash
REQUEST_ID=$(curl -fsS \
  https://api.tensorlake.ai/v1/namespaces/default/applications/typescript_http_body_webhook \
  -H "Content-Type: application/cloudevents+json" \
  -H "X-Webhook-Id: delivery_123" \
  --data-binary '{"event":"invoice.created","id":"evt_123"}' \
  | jq -r .request_id)

curl -fsS \
  "https://api.tensorlake.ai/applications/typescript_http_body_webhook/requests/$REQUEST_ID/output" \
  -H "Authorization: Bearer $TENSORLAKE_API_KEY"
```

The output includes the normalized event fields, delivery ID, original content
type, raw byte length, and digest of the unchanged request bytes.
