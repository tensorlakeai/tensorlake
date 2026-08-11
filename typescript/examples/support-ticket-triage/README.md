# TypeScript support-ticket triage

This example deploys a customer-support workflow that combines durable
Tensorlake functions with a Vercel AI SDK agent. It accepts up to 100 support
tickets, fans them out for parallel triage, applies a deterministic escalation
policy, and returns structured results suitable for a support queue.

The example uses the expanded `registerFunction` and `registerApplication`
syntax. Each registration declares its stable name, parameter schemas, return
schema, resource configuration, retries, and operational metadata explicitly.
Tensorlake uses those schemas for runtime validation and application manifests.

## Project contents

- `application.ts` defines the ticket schemas, AI-backed triage function, and
  batch application.
- `package.json` declares `ai`, `@ai-sdk/openai`, and `zod` as external
  dependencies.
- `tsconfig.json` enables strict checking without emitting JavaScript.

The repository example uses `"tensorlake": "file:../.."` so it resolves the
SDK from this checkout. In an independent project, install the published
`tensorlake` package instead.

## Prerequisites

- Node.js 24 or newer
- the `tl` CLI
- a Tensorlake account and API key
- an OpenAI API key

## Install

From the repository root, build the local Tensorlake package and install the
example's dependencies:

```bash
npm --prefix typescript install
npm --prefix typescript run build:sdk
npm --prefix typescript/examples/support-ticket-triage install
```

After the first install, use `npm ci` for reproducible clean installs:

```bash
npm --prefix typescript/examples/support-ticket-triage ci
```

## Type-check

```bash
npm --prefix typescript/examples/support-ticket-triage run typecheck
```

## Configure secrets

```bash
export TENSORLAKE_API_KEY="your-tensorlake-api-key"
tl login
tl secrets set OPENAI_API_KEY "your-openai-api-key"
```

Only the AI-backed `typescript_triage_support_ticket` function declares the
OpenAI secret. Tensorlake injects it into that function's runtime.

## Deploy

From the repository root:

```bash
tl deploy typescript/examples/support-ticket-triage/application.ts
```

## Invoke

The application input is a JSON array of tickets.

```bash
REQUEST_ID=$(curl -fsS \
  https://api.tensorlake.ai/applications/typescript_support_ticket_triage \
  -H "Authorization: Bearer $TENSORLAKE_API_KEY" \
  --json '[
    {
      "id": "ticket-1042",
      "customerTier": "enterprise",
      "subject": "Production API requests are timing out",
      "message": "All requests in us-east have timed out for the last ten minutes.",
      "previousContactCount": 1
    },
    {
      "id": "ticket-1043",
      "customerTier": "free",
      "subject": "How do I rotate an API key?",
      "message": "I cannot find the key rotation instructions in the dashboard.",
      "previousContactCount": 0
    }
  ]' \
  | jq -r .request_id)

curl -fsS \
  "https://api.tensorlake.ai/applications/typescript_support_ticket_triage/requests/$REQUEST_ID/output" \
  -H "Authorization: Bearer $TENSORLAKE_API_KEY"
```

Each result includes a priority, risk classification, category, response
target, short summary, recommended action, draft response, and human-review
flag. The application also reports aggregate urgent and human-review counts.
