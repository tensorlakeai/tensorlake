#!/usr/bin/env node

import process from "node:process";

const image = process.argv[2] ?? "tensorlake/ubuntu-minimal";
const apiUrl = process.env.TENSORLAKE_API_URL ?? "https://api.tensorlake.ai";
const apiKey = process.env.TENSORLAKE_API_KEY;
const organizationId = process.env.TENSORLAKE_ORGANIZATION_ID;
const projectId = process.env.TENSORLAKE_PROJECT_ID;
const name = `codex-smoke-ts-${Date.now()}`;

if (!apiKey) {
  console.error("TENSORLAKE_API_KEY must be set.");
  process.exit(2);
}

const { SandboxClient } = await import("../../typescript/dist/index.js");

const client = SandboxClient.forCloud({
  apiUrl,
  apiKey,
  organizationId,
  projectId,
});

let sandbox;
try {
  console.log(`Creating sandbox from image ${JSON.stringify(image)}...`);
  sandbox = await client.createAndConnect({
    image,
    name,
    startupTimeout: 60,
  });

  const info = await client.get(sandbox.sandboxId);
  const health = await sandbox.health();
  const run = await sandbox.run("sh", {
    args: ["-lc", "printf 'typescript smoke ok\\n'"],
  });

  console.log(`Sandbox ID: ${sandbox.sandboxId}`);
  console.log(`Create path ingress endpoint: ${info.ingressEndpoint}`);
  console.log(`Create path healthy: ${health.healthy}`);
  console.log(`Create path command stdout: ${run.stdout.trim()}`);

  await sandbox.terminate();
  sandbox = undefined;
  console.log("TypeScript createAndConnect smoke test passed.");
} finally {
  if (sandbox) {
    try {
      await sandbox.terminate();
    } catch {
      sandbox.close();
    }
  }
  client.close();
}
