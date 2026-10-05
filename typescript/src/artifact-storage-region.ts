/** Production Artifact Storage regions. Repositories and filesystems are region-local. */
export type ArtifactStorageRegion = "us-east-1" | "eu-central-1";

const API_URLS: Record<ArtifactStorageRegion, string> = {
  "us-east-1": "https://api.tensorlake.ai",
  "eu-central-1": "https://api.eu-central-1.tensorlake.ai",
};

/** Resolve credential and data routing together, without cross-region fallback. */
export function resolveArtifactStorageApiUrl(apiUrl: string, region?: ArtifactStorageRegion): string {
  if (region === undefined) return apiUrl;
  if (!Object.hasOwn(API_URLS, region)) {
    throw new Error(`Unsupported Artifact Storage region: ${region}; expected us-east-1 or eu-central-1`);
  }
  if (!Object.values(API_URLS).includes(apiUrl.replace(/\/+$/, ""))) {
    throw new Error("Artifact Storage region cannot be combined with a custom or development API URL");
  }
  return API_URLS[region];
}
