import { execFile } from "node:child_process";
import { accessSync, constants } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { CliNotFoundError, MountError } from "./filesystem-models.js";

const execFileAsync = promisify(execFile);

/** Shared command runner for the separately installed local mount daemon. */
export class MountCli {
  private binary: string | null = null;

  constructor(
    private readonly command: "fs" | "git",
    private readonly envOverrides: Record<string, string>,
  ) {}

  private async findCli(): Promise<string> {
    const candidates: string[] = [];
    if (process.env.TENSORLAKE_CLI) candidates.push(process.env.TENSORLAKE_CLI);
    candidates.push("tl");
    const installed = join(homedir(), ".tensorlake", "bin", "tl");
    try {
      // Probe present-but-broken installs too, so they report an upgrade error.
      accessSync(installed, constants.F_OK);
      candidates.push(installed);
    } catch {
      // Not installed at the default path.
    }
    const support = this.command === "git" ? "git mount" : "fs";
    const probe = this.command === "git" ? ["git", "mount", "--help"] : ["fs", "--help"];
    let unsupported: string | null = null;
    for (const candidate of candidates) {
      try {
        await execFileAsync(candidate, probe, { timeout: 15_000 });
        return candidate;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") unsupported = candidate;
      }
    }
    throw new CliNotFoundError(
      unsupported
        ? `\`tl\` at ${unsupported} does not support \`tl ${support}\` (upgrade required)`
        : "`tl` was not found on PATH",
      support,
    );
  }

  protected async run(args: string[], timeoutMs = 300_000): Promise<string> {
    if (this.binary === null) this.binary = await this.findCli();
    try {
      const { stdout } = await execFileAsync(this.binary, [this.command, ...args], {
        timeout: timeoutMs,
        env: { ...process.env, ...this.envOverrides },
        maxBuffer: 16 * 1024 * 1024,
      });
      return stdout;
    } catch (error) {
      const failure = error as { stderr?: string; stdout?: string; killed?: boolean };
      if (failure.killed) {
        throw new MountError(`\`tl ${this.command} ${args[0]}\` timed out after ${timeoutMs}ms`);
      }
      const detail = (failure.stderr || failure.stdout || String(error)).trim();
      throw new MountError(`\`tl ${this.command} ${args.join(" ")}\` failed: ${detail}`);
    }
  }
}
