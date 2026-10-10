import { appendFileSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";

// Staged npm packages only become public after a maintainer approves them, so
// a release run that just stages them looks finished while nothing is public.
//
//   announce  lists the staged packages in the job summary, warns on the run
//             page, and exposes the list to the Slack notification.
//   wait      polls the public registry until every package is public, then
//             fails if that takes longer than APPROVAL_TIMEOUT_MINUTES or if a
//             package was approved before the packages it depends on.
//
// INCLUDE_WRAPPER=true adds the `tensorlake` wrapper, which is staged on a
// best-effort basis. This polls the anonymous registry because trusted
// publishing tokens cannot run `npm stage list`.
const [command] = process.argv.slice(2);
if (command !== "announce" && command !== "wait") {
  throw new Error("usage: await-npm-approval.mjs <announce|wait>");
}

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const readManifest = (file) => JSON.parse(readFileSync(path.join(root, file), "utf8"));
const sdk = readManifest("package.json");
const version = sdk.version;

// Approve in this order so no public package depends on a staged version.
const groups = [
  { title: "Native packages", names: Object.keys(sdk.optionalDependencies ?? {}).sort() },
  { title: "SDK", names: [sdk.name] },
];
if (process.env.INCLUDE_WRAPPER === "true") {
  groups.push({ title: "Wrapper", names: [readManifest("tensorlake-wrapper/package.json").name] });
}
const packages = groups.flatMap((group, order) => group.names.map((name) => ({ name, order })));
const timeoutMinutes = Number(process.env.APPROVAL_TIMEOUT_MINUTES ?? "120");
const pollSeconds = 60;

function writeSummary(markdown) {
  if (process.env.GITHUB_STEP_SUMMARY) {
    appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${markdown}\n`);
  }
}

function writeOutput(name, value) {
  if (process.env.GITHUB_OUTPUT) {
    appendFileSync(process.env.GITHUB_OUTPUT, `${name}<<EOF\n${value}\nEOF\n`);
  }
}

function announce() {
  const steps = groups
    .map((group, index) => `${index + 1}. ${group.title}: ${group.names.map((name) => `\`${name}@${version}\``).join(", ")}`)
    .join("\n");
  writeSummary(
    [
      "## npm packages awaiting approval",
      "",
      `Version \`${version}\` is staged but **not public**. A maintainer must approve every package with 2FA, in this order:`,
      "",
      steps,
      "",
      "Approve on npmjs.com, or from a terminal:",
      "",
      "```sh",
      `npm stage list ${sdk.name}@${version}   # prints the stage ID`,
      "npm stage approve <stage-id>",
      "```",
      "",
      `This run waits up to ${timeoutMinutes} minutes for every package to become public and fails otherwise.`,
    ].join("\n"),
  );
  process.stdout.write(
    `::warning title=npm approval required::${packages.length} packages at ${version} are staged and must be approved on npm before they are public. See the job summary.\n`,
  );
  writeOutput("version", version);
  writeOutput("packages", packages.map(({ name }) => `\`${name}@${version}\``).join("\n"));
  process.stdout.write(`${packages.map(({ name }) => `${name}@${version}`).join("\n")}\n`);
}

// A staged version is invisible to the public registry until it is approved.
async function isPublic(name) {
  try {
    const response = await fetch(`https://registry.npmjs.org/${name}/${version}`, {
      headers: { accept: "application/json" },
      signal: AbortSignal.timeout(30_000),
    });
    if (response.ok) return true;
    if (response.status !== 404) {
      process.stdout.write(`${name}: registry answered ${response.status}, retrying\n`);
    }
  } catch (error) {
    process.stdout.write(`${name}: ${error.message}, retrying\n`);
  }
  return false;
}

async function wait() {
  const deadline = Date.now() + timeoutMinutes * 60_000;
  const published = new Set();
  const outOfOrder = [];

  while (true) {
    const checks = await Promise.all(packages.map(({ name }) => isPublic(name)));
    const publicNow = new Set(packages.filter((_, index) => checks[index]).map(({ name }) => name));
    for (const pkg of packages) {
      if (!publicNow.has(pkg.name) || published.has(pkg.name)) continue;
      published.add(pkg.name);
      const stillStaged = packages.filter((other) => other.order < pkg.order && !publicNow.has(other.name));
      process.stdout.write(`${pkg.name}@${version} is public\n`);
      if (stillStaged.length > 0) {
        const message = `${pkg.name}@${version} was approved before ${stillStaged.map(({ name }) => name).join(", ")}`;
        outOfOrder.push(message);
        process.stdout.write(`::error title=npm approved out of order::${message}. Approve the remaining packages now.\n`);
      }
    }

    const pending = packages.filter(({ name }) => !published.has(name)).map(({ name }) => name);
    if (pending.length === 0) break;
    if (Date.now() >= deadline) {
      const message = `Still staged after ${timeoutMinutes} minutes: ${pending.join(", ")}`;
      writeOutput("failure", `Still staged after ${timeoutMinutes} minutes:\n${pending.map((name) => `\`${name}@${version}\``).join("\n")}`);
      writeSummary(`\n:x: ${message}. Approve them on npm; nothing needs to be re-run.`);
      process.stdout.write(`::error title=npm approval missing::${message}\n`);
      process.exit(1);
    }
    process.stdout.write(`Waiting for approval: ${pending.join(", ")}\n`);
    await sleep(pollSeconds * 1000);
  }

  if (outOfOrder.length > 0) {
    writeOutput("failure", `Every package is public, but approved out of order:\n${outOfOrder.join("\n")}`);
    writeSummary(`\n:warning: Every package is public, but approved out of order:\n\n${outOfOrder.map((m) => `- ${m}`).join("\n")}`);
    process.exit(1);
  }
  writeSummary(`\n:white_check_mark: Every package at \`${version}\` is public.`);
}

if (command === "announce") {
  announce();
} else {
  await wait();
}
