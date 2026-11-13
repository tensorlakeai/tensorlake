import { Sandbox, SandboxClient } from "@tensorlakeai/tensorlake";

export const runtime = "nodejs";

export async function GET() {
  const apiUrl = process.env.TENSORLAKE_REPRO_API_URL;
  const client = new SandboxClient({ apiUrl, apiKey: "repro-only" }, true);
  const sandbox = new Sandbox({ sandboxId: "next-repro", proxyUrl: apiUrl });
  try {
    return Response.json({
      sandboxes: Array.from(await client.list()),
      processes: Array.from(await sandbox.listProcesses()),
    });
  } finally {
    sandbox.close();
    client.close();
  }
}
