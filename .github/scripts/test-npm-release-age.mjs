import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { checkReleaseAge } from "./check-npm-release-age.mjs";

const now = Date.parse("2026-10-09T12:00:00Z");
const mature = "2026-10-01T12:00:00Z";
const young = "2026-10-09T11:00:00Z";

async function fixture(run) {
  const directory = await mkdtemp(join(tmpdir(), "npm-age-test-"));
  const lockfile = join(directory, "package-lock.json");
  const entries = {
    "node_modules/example": {
      version: "1.0.0", integrity: "sha512-fixture",
      resolved: "https://registry.npmjs.org/example/-/example-1.0.0.tgz",
    },
  };
  let timestamp = mature;
  const options = {
    now,
    metadata: async () => ({
      time: timestamp ? { "1.0.0": timestamp } : {},
      versions: { "1.0.0": { dist: { integrity: "sha512-fixture" } } },
    }),
  };
  async function save() {
    await writeFile(lockfile, JSON.stringify({ lockfileVersion: 3, packages: { "": {}, ...entries } }));
  }
  try {
    await run({ lockfile, entries, options, save, setTime: (value) => { timestamp = value; } });
  } finally {
    await rm(directory, { recursive: true });
  }
}

test("mature locked versions pass", () => fixture(async ({ lockfile, options, save }) => {
  await save();
  await checkReleaseAge(lockfile, options);
}));

test("a young pinned lockfile entry fails", () => fixture(async (f) => {
  f.setTime(young); await f.save();
  await assert.rejects(checkReleaseAge(f.lockfile, f.options), /younger/);
}));

test("missing timestamps fail closed", () => fixture(async (f) => {
  f.setTime(null); await f.save();
  await assert.rejects(checkReleaseAge(f.lockfile, f.options), /Missing publication/);
}));

test("fresh exceptions still require trustworthy metadata", () => fixture(async (f) => {
  f.setTime(null); await f.save();
  await assert.rejects(checkReleaseAge(f.lockfile, {
    ...f.options, allowedVersions: ["example@1.0.0"],
  }), /Missing publication/);
}));

test("an exact fresh-release exception passes", () => fixture(async (f) => {
  f.setTime(young); await f.save();
  await checkReleaseAge(f.lockfile, { ...f.options, allowedVersions: ["example@1.0.0"] });
}));

test("a fresh-release exception does not exempt transitive dependencies", () => fixture(async (f) => {
  f.entries["node_modules/dependency"] = {
    ...f.entries["node_modules/example"],
    resolved: "https://registry.npmjs.org/dependency/-/dependency-1.0.0.tgz",
  };
  f.setTime(young); await f.save();
  await assert.rejects(checkReleaseAge(f.lockfile, {
    ...f.options, allowedVersions: ["example@1.0.0"],
  }), /dependency@1.0.0/);
}));

test("alias identities cannot disguise an artifact", () => fixture(async (f) => {
  f.entries["node_modules/example"].name = "different"; await f.save();
  await assert.rejects(checkReleaseAge(f.lockfile, f.options), /identity does not match/);
}));

test("changed artifact integrity is rejected", () => fixture(async (f) => {
  f.entries["node_modules/example"].integrity = "sha512-changed"; await f.save();
  await assert.rejects(checkReleaseAge(f.lockfile, f.options), /integrity does not match/);
}));

test("local tarballs require an explicit artifact exception", () => fixture(async (f) => {
  f.entries["node_modules/example"].resolved = "file:./artifact.tgz"; await f.save();
  await assert.rejects(checkReleaseAge(f.lockfile, f.options), /Local artifact requires/);
  await checkReleaseAge(f.lockfile, { ...f.options, allowLocalArtifacts: true });
}));

test("known compromised releases cannot use fresh-release exceptions", () => fixture(async (f) => {
  delete f.entries["node_modules/example"];
  f.entries["node_modules/tensorlake"] = {
    version: "0.5.144", integrity: "sha512-fixture",
    resolved: "https://registry.npmjs.org/tensorlake/-/tensorlake-0.5.144.tgz",
  };
  await f.save();
  await assert.rejects(checkReleaseAge(f.lockfile, {
    ...f.options, allowedVersions: ["tensorlake@0.5.144"],
  }), /Known compromised/);
}));
