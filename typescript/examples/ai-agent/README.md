# TypeScript AI agent

This example deploys a Tensorlake application that uses third-party npm
packages. It accepts a string prompt, runs a Vercel AI SDK `ToolLoopAgent` with
OpenAI, and returns the agent's final text response.

The agent also has an `analyzeText` tool. For example, a prompt can ask it to
count the words in a passage before explaining the result.

## Project contents

- `application.ts` defines the Tensorlake application and AI agent.
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

Install the CLI if necessary:

```bash
curl -fsSL https://tensorlake.ai/install | sh
```

## Install

From the repository root, build the local Tensorlake package and install this
example's locked dependencies:

```bash
npm --prefix typescript install
npm --prefix typescript run build:sdk
npm --prefix typescript/examples/ai-agent install
```

No separate application build step is required. `tl deploy` transpiles and
bundles the TypeScript application and its npm dependencies into the uploaded
application runtime.

After the first install, use `npm ci` for a reproducible clean install:

```bash
npm --prefix typescript/examples/ai-agent ci
```

## Optional type-check

`tl deploy` transpiles TypeScript but does not perform full static type-checking.
During development, run the following command to catch type errors before
deploying:

```bash
npm --prefix typescript/examples/ai-agent run typecheck
```

This check is optional and is not required for deployment.

## Configure secrets

Authenticate the CLI and store the OpenAI key as a Tensorlake secret:

```bash
export TENSORLAKE_API_KEY="your-tensorlake-api-key"
tl login
tl secrets set OPENAI_API_KEY "your-openai-api-key"
```

`application.ts` declares `OPENAI_API_KEY` in the application's `secrets`
option. Tensorlake injects it into the function at runtime; it is not included
in the application bundle.

## Deploy

Deploy from the repository root:

```bash
tl deploy typescript/examples/ai-agent/application.ts
```

Or deploy from this directory:

```bash
cd typescript/examples/ai-agent
tl deploy application.ts
```

## Invoke the agent

The application input is a JSON string containing the prompt. The output is a
JSON string containing the agent's final response.

```bash
REQUEST_ID=$(curl -fsS \
  https://api.tensorlake.ai/applications/typescript_ai_agent \
  -H "Authorization: Bearer $TENSORLAKE_API_KEY" \
  --json '"Count the words in: Tensorlake runs TypeScript agents in isolated sandboxes."' \
  | jq -r .request_id)

curl -fsS \
  "https://api.tensorlake.ai/applications/typescript_ai_agent/requests/$REQUEST_ID/output" \
  -H "Authorization: Bearer $TENSORLAKE_API_KEY"
```

The request remains active while the agent makes model and tool calls. Poll the
output endpoint again if the first response indicates that execution is still
in progress.

## Use a different model

Change the model passed to `openai()` in `application.ts`, then deploy again:

```ts
model: openai("gpt-4o-mini"),
```

The selected model must support the agent features and tools used by the
application.
