import { remoteOptions, runRemote } from "@tensorlakeai/tensorlake/applications";

export const runtime = "nodejs";

export async function GET() {
  const request = await runRemote("next-repro", remoteOptions({
    apiUrl: process.env.TENSORLAKE_REPRO_API_URL,
    maxRetries: 0,
  }));
  return Response.json({ requestId: request.id });
}
