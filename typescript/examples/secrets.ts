import { createHash } from "node:crypto";
import { Image, registerApplication } from "tensorlake/applications";

// Function Agent resolves declared secrets before importing this module. Reading
// here verifies that import-time installation works, not only handler-time access.
const secretAtImport = process.env.E2E_SECRET;

const image = new Image({
  baseImage: "node:24-trixie",
});

export const secretsE2E = registerApplication(
  "typescript_secrets_e2e",
  async (expectedSha256: string) => {
    if (secretAtImport === undefined) {
      return {
        present: false,
        resolvedAtImport: false,
        matchesExpectedValue: false,
      };
    }

    const actualSha256 = createHash("sha256")
      .update(secretAtImport, "utf8")
      .digest("hex");

    return {
      present: true,
      resolvedAtImport: true,
      matchesExpectedValue: actualSha256 === expectedSha256,
    };
  },
  {
    secrets: ["E2E_SECRET"],
    image,
  },
);
