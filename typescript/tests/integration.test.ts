/**
 * Integration tests for the TensorLake TypeScript SDK.
 *
 * Runs against cloud.tensorlake.ai (or the URL in TENSORLAKE_API_URL).
 * Requires TENSORLAKE_API_KEY to be set.
 *
 * Usage:
 *   TENSORLAKE_API_KEY=... npm run test:integration
 */

import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  PoolInUseError,
  PoolNotFoundError,
  SandboxClient,
  SandboxError,
  SandboxNotFoundError,
  SandboxStatus,
  type SandboxInfo,
  type SandboxPoolInfo,
  type PoolContainerInfo,
} from "../src/index.js";
import { Sandbox } from "../src/sandbox.js";

const SANDBOX_IMAGE = "ubuntu-minimal";
const SANDBOX_CPUS = 1.0;
const SANDBOX_MEMORY_MB = 1024;
const SANDBOX_DISK_MB = 1024;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function pollSandboxStatus(
  client: SandboxClient,
  sandboxId: string,
  target: SandboxStatus | SandboxStatus[],
  timeoutSec = 60,
  intervalMs = 1000,
): Promise<SandboxStatus> {
  const targets = Array.isArray(target) ? target : [target];
  const deadline = Date.now() + timeoutSec * 1000;
  let status: SandboxStatus | undefined;
  while (Date.now() < deadline) {
    const info = await client.get(sandboxId);
    status = info.status;
    if (targets.includes(status)) return status;
    await sleep(intervalMs);
  }
  throw new Error(
    `Sandbox ${sandboxId} did not reach ${targets.join(" or ")} within ${timeoutSec}s (last: ${status})`,
  );
}

async function createAndConnectWithStartupRetry(
  client: SandboxClient,
  options: Parameters<SandboxClient["createAndConnect"]>[0],
  attempts = 3,
): Promise<Sandbox> {
  let lastError: unknown;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      return await client.createAndConnect(options);
    } catch (error) {
      lastError = error;
      if (
        !(error instanceof SandboxError) ||
        !error.message.includes("terminated during startup") ||
        attempt === attempts
      ) {
        throw error;
      }
      await sleep(1000);
    }
  }

  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

async function pollPoolContainers(
  client: SandboxClient,
  poolId: string,
  minCount: number,
  timeoutSec = 60,
  intervalMs = 1000,
): Promise<PoolContainerInfo[]> {
  const deadline = Date.now() + timeoutSec * 1000;
  let containers: PoolContainerInfo[] = [];
  while (Date.now() < deadline) {
    const detail = await client.getPool(poolId);
    containers = detail.containers ?? [];
    if (containers.length >= minCount) return containers;
    await sleep(intervalMs);
  }
  throw new Error(
    `Pool ${poolId} did not reach ${minCount} containers within ${timeoutSec}s (last: ${containers.length})`,
  );
}

function warmContainers(containers: PoolContainerInfo[]): PoolContainerInfo[] {
  return containers.filter((c) => c.sandboxId == null);
}

function claimedContainers(containers: PoolContainerInfo[]): PoolContainerInfo[] {
  return containers.filter((c) => c.sandboxId != null);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe(
  "Sandbox Lifecycle",
  () => {
    let client: SandboxClient;
    let sandboxId: string;

    beforeAll(() => {
      client = new SandboxClient({
        apiUrl: process.env.TENSORLAKE_API_URL ?? "https://api.tensorlake.ai",
        apiKey: process.env.TENSORLAKE_API_KEY,
      });
    });

    afterAll(async () => {
      if (sandboxId) {
        try {
          await client.delete(sandboxId);
        } catch {}
      }
      client.close();
    });

    it("creates a sandbox", async () => {
      const resp = await client.create({
        image: SANDBOX_IMAGE,
        cpus: SANDBOX_CPUS,
        memoryMb: SANDBOX_MEMORY_MB,
        ephemeralDiskMb: SANDBOX_DISK_MB,
        entrypoint: ["sleep", "300"],
      });
      expect(resp.sandboxId).toBeTruthy();
      expect([
        SandboxStatus.PENDING,
        SandboxStatus.RUNNING,
        SandboxStatus.TERMINATED,
      ]).toContain(resp.status);
      sandboxId = resp.sandboxId;
    });

    it("gets a sandbox", async () => {
      const info = await client.get(sandboxId);
      expect(info.sandboxId).toBe(sandboxId);
      expect([
        SandboxStatus.PENDING,
        SandboxStatus.RUNNING,
        SandboxStatus.TERMINATED,
      ]).toContain(info.status);
    });

    it("lists sandboxes", async () => {
      const list = await client.list();
      const ids = list.map((s) => s.sandboxId);
      expect(ids).toContain(sandboxId);
    });

    it("transitions out of pending", async () => {
      const status = await pollSandboxStatus(
        client,
        sandboxId,
        [SandboxStatus.RUNNING, SandboxStatus.TERMINATED],
      );
      expect([SandboxStatus.RUNNING, SandboxStatus.TERMINATED]).toContain(status);
    });

    it("deletes a sandbox", async () => {
      await client.delete(sandboxId);
    });

    it("transitions to terminated", async () => {
      const status = await pollSandboxStatus(
        client,
        sandboxId,
        SandboxStatus.TERMINATED,
        30,
      );
      expect(status).toBe(SandboxStatus.TERMINATED);
      sandboxId = undefined!;
    });

    it("throws SandboxNotFoundError for non-existent sandbox", async () => {
      await expect(
        client.delete("nonexistent-sandbox-id-000"),
      ).rejects.toThrow(SandboxNotFoundError);
    });
  },
  { timeout: 120_000 },
);

describe(
  "Sandbox Commands",
  () => {
    let client: SandboxClient;
    let sandbox: Sandbox;
    let poolId: string;

    beforeAll(async () => {
      client = new SandboxClient({
        apiUrl: process.env.TENSORLAKE_API_URL ?? "https://api.tensorlake.ai",
        apiKey: process.env.TENSORLAKE_API_KEY,
      });
      const pool = await client.createPool({
        image: SANDBOX_IMAGE,
        cpus: SANDBOX_CPUS,
        memoryMb: SANDBOX_MEMORY_MB,
        ephemeralDiskMb: SANDBOX_DISK_MB,
        entrypoint: ["sleep", "300"],
        warmContainers: 1,
      });
      poolId = pool.poolId;
      await pollPoolContainers(client, poolId, 1);
      sandbox = await createAndConnectWithStartupRetry(client, {
        poolId,
        startupTimeout: 120,
      }, 5);
    });

    afterAll(async () => {
      if (sandbox) {
        try {
          const sandboxId = sandbox.sandboxId;
          await sandbox.terminate();
          await pollSandboxStatus(client, sandboxId, SandboxStatus.TERMINATED, 30);
        } catch {}
      }
      if (poolId) {
        try {
          await sleep(2000);
          await client.deletePool(poolId);
        } catch {}
      }
      client.close();
    });

    it("runs a command and captures stdout", async () => {
      const result = await sandbox.run("echo", { args: ["hello world"] });
      expect(result.exitCode).toBe(0);
      expect(result.stdout).toContain("hello world");
    });

    it("captures stderr", async () => {
      const result = await sandbox.run("sh", {
        args: ["-c", "echo error >&2"],
      });
      expect(result.exitCode).toBe(0);
      expect(result.stderr).toContain("error");
    });

    it("returns non-zero exit code", async () => {
      const result = await sandbox.run("sh", { args: ["-c", "exit 42"] });
      expect(result.exitCode).toBe(42);
    });

    it("runs command with env vars", async () => {
      const result = await sandbox.run("sh", {
        args: ["-c", "echo $MY_VAR"],
        env: { MY_VAR: "test-value" },
      });
      expect(result.exitCode).toBe(0);
      expect(result.stdout).toContain("test-value");
    });

    it("runs command with working directory", async () => {
      const result = await sandbox.run("pwd", { workingDir: "/tmp" });
      expect(result.exitCode).toBe(0);
      expect(result.stdout).toContain("/tmp");
    });

    it("writes and reads a file", async () => {
      const content = new TextEncoder().encode("hello from typescript sdk");
      await sandbox.writeFile("/tmp/test-ts-sdk.txt", content);

      const data = await sandbox.readFile("/tmp/test-ts-sdk.txt");
      const text = new TextDecoder().decode(data);
      expect(text).toBe("hello from typescript sdk");
    });

    it("lists a directory", async () => {
      // Write a file first so we know at least one entry exists
      await sandbox.writeFile(
        "/tmp/list-test.txt",
        new TextEncoder().encode("x"),
      );

      const listing = await sandbox.listDirectory("/tmp");
      expect(listing.path).toBe("/tmp");
      expect(listing.entries.length).toBeGreaterThan(0);
      const names = listing.entries.map((e) => e.name);
      expect(names).toContain("list-test.txt");
    });

    it("deletes a file", async () => {
      await sandbox.writeFile(
        "/tmp/to-delete.txt",
        new TextEncoder().encode("delete me"),
      );
      await sandbox.deleteFile("/tmp/to-delete.txt");

      // Verify the file is gone by listing the directory
      const listing = await sandbox.listDirectory("/tmp");
      const names = listing.entries.map((e) => e.name);
      expect(names).not.toContain("to-delete.txt");
    });

    it("starts and manages processes", async () => {
      const proc = await sandbox.startProcess("sleep", { args: ["10"] });
      expect(proc.pid).toBeGreaterThan(0);
      expect(proc.status).toBe("running");

      const processes = await sandbox.listProcesses();
      const pids = processes.map((p) => p.pid);
      expect(pids).toContain(proc.pid);

      await sandbox.killProcess(proc.pid);

      // Wait for process to exit
      await sleep(500);
      const info = await sandbox.getProcess(proc.pid);
      expect(info.status).not.toBe("running");
    });

    it("checks health", async () => {
      const health = await sandbox.health();
      expect(health.healthy).toBe(true);
    });

    it("gets daemon info", async () => {
      const info = await sandbox.info();
      expect(info.version).toBeTruthy();
      expect(info.uptimeSecs).toBeGreaterThanOrEqual(0);
    });
  },
  { timeout: 180_000 },
);

describe(
  "Pool Lifecycle",
  () => {
    let client: SandboxClient;
    let poolId: string;

    beforeAll(() => {
      client = new SandboxClient({
        apiUrl: process.env.TENSORLAKE_API_URL ?? "https://api.tensorlake.ai",
        apiKey: process.env.TENSORLAKE_API_KEY,
      });
    });

    afterAll(async () => {
      if (poolId) {
        try {
          await client.deletePool(poolId);
        } catch {}
      }
      client.close();
    });

    it("creates a pool", async () => {
      const resp = await client.createPool({
        image: SANDBOX_IMAGE,
        cpus: SANDBOX_CPUS,
        memoryMb: SANDBOX_MEMORY_MB,
        ephemeralDiskMb: SANDBOX_DISK_MB,
        entrypoint: ["sleep", "300"],
      });
      expect(resp.poolId).toBeTruthy();
      poolId = resp.poolId;
    });

    it("gets pool details", async () => {
      const info = await client.getPool(poolId);
      expect(info.poolId).toBe(poolId);
      expect(info.image).toBe(SANDBOX_IMAGE);
      expect(info.resources.memoryMb).toBe(SANDBOX_MEMORY_MB);
    });

    it("lists pools", async () => {
      const pools = await client.listPools();
      const ids = pools.map((p) => p.poolId);
      expect(ids).toContain(poolId);
    });

    it("updates a pool", async () => {
      const updated = await client.updatePool(poolId, {
        image: SANDBOX_IMAGE,
        cpus: SANDBOX_CPUS,
        memoryMb: 2048,
        ephemeralDiskMb: SANDBOX_DISK_MB,
        warmContainers: 1,
      });
      expect(updated.resources.memoryMb).toBe(2048);
      expect(updated.warmContainers).toBe(1);
    });

    it("deletes a pool", async () => {
      await client.deletePool(poolId);
      poolId = undefined!;
    });

    it("throws PoolNotFoundError for non-existent pool", async () => {
      await expect(
        client.deletePool("nonexistent-pool-id-000"),
      ).rejects.toThrow(PoolNotFoundError);
    });
  },
  { timeout: 120_000 },
);

describe(
  "Pool with Sandboxes",
  () => {
    let client: SandboxClient;
    let poolId: string;
    let sandboxId: string;

    beforeAll(() => {
      client = new SandboxClient({
        apiUrl: process.env.TENSORLAKE_API_URL ?? "https://api.tensorlake.ai",
        apiKey: process.env.TENSORLAKE_API_KEY,
      });
    });

    afterAll(async () => {
      if (sandboxId) {
        try {
          await client.delete(sandboxId);
        } catch {}
      }
      if (poolId) {
        try {
          await sleep(3000);
          await client.deletePool(poolId);
        } catch {}
      }
      client.close();
    });

    it("creates a pool with warm containers", async () => {
      const resp = await client.createPool({
        image: SANDBOX_IMAGE,
        cpus: SANDBOX_CPUS,
        memoryMb: SANDBOX_MEMORY_MB,
        ephemeralDiskMb: SANDBOX_DISK_MB,
        entrypoint: ["sleep", "300"],
        warmContainers: 1,
      });
      poolId = resp.poolId;
    });

    it("claims a sandbox from pool", async () => {
      const resp = await client.claim(poolId);
      expect(resp.sandboxId).toBeTruthy();
      expect([SandboxStatus.PENDING, SandboxStatus.RUNNING]).toContain(resp.status);
      sandboxId = resp.sandboxId;
    });

    it("sandbox from pool reaches running", async () => {
      const status = await pollSandboxStatus(client, sandboxId, SandboxStatus.RUNNING);
      expect(status).toBe(SandboxStatus.RUNNING);
    });

    it("cannot delete pool with active sandbox", async () => {
      await expect(client.deletePool(poolId)).rejects.toThrow(PoolInUseError);
    });

    it("deletes sandbox then pool", async () => {
      await client.delete(sandboxId);
      await pollSandboxStatus(client, sandboxId, SandboxStatus.TERMINATED, 30);
      sandboxId = undefined!;

      await client.deletePool(poolId);
      poolId = undefined!;
    });
  },
  { timeout: 120_000 },
);

describe(
  "Warm Containers",
  () => {
    let client: SandboxClient;
    let poolId: string;
    let sandboxId: string;
    let warmContainerId: string;

    beforeAll(() => {
      client = new SandboxClient({
        apiUrl: process.env.TENSORLAKE_API_URL ?? "https://api.tensorlake.ai",
        apiKey: process.env.TENSORLAKE_API_KEY,
      });
    });

    afterAll(async () => {
      if (sandboxId) {
        try {
          await client.delete(sandboxId);
        } catch {}
      }
      if (poolId) {
        try {
          await sleep(2000);
          await client.deletePool(poolId);
        } catch {}
      }
      client.close();
    });

    it("creates pool with one warm container", async () => {
      const resp = await client.createPool({
        image: SANDBOX_IMAGE,
        cpus: SANDBOX_CPUS,
        memoryMb: SANDBOX_MEMORY_MB,
        ephemeralDiskMb: SANDBOX_DISK_MB,
        entrypoint: ["sleep", "300"],
        warmContainers: 1,
      });
      poolId = resp.poolId;
    });

    it("warm container is created", async () => {
      const containers = await pollPoolContainers(client, poolId, 1);
      expect(containers).toHaveLength(1);
      expect(containers[0].sandboxId).toBeUndefined();
      warmContainerId = containers[0].id;
    });

    it("sandbox claims warm container", async () => {
      const resp = await client.claim(poolId);
      sandboxId = resp.sandboxId;

      await pollSandboxStatus(client, sandboxId, SandboxStatus.RUNNING);

      const detail = await client.getPool(poolId);
      const claimed = (detail.containers ?? []).filter(
        (c) => c.id === warmContainerId,
      );
      expect(claimed).toHaveLength(1);
      expect(claimed[0].sandboxId).toBe(sandboxId);
    });

    it("replacement warm container is created", async () => {
      const containers = await pollPoolContainers(client, poolId, 2);
      const warm = warmContainers(containers);
      expect(warm.length).toBeGreaterThanOrEqual(1);
      expect(warm[0].id).not.toBe(warmContainerId);
    });

    it("deleting sandbox removes claimed container", async () => {
      await client.delete(sandboxId);
      await pollSandboxStatus(client, sandboxId, SandboxStatus.TERMINATED, 30);
      sandboxId = undefined!;

      // Wait for pool to converge back to 1 warm container
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
        const detail = await client.getPool(poolId);
        const containers = detail.containers ?? [];
        if (
          claimedContainers(containers).length === 0 &&
          warmContainers(containers).length === 1
        ) {
          break;
        }
        await sleep(1000);
      }

      const detail = await client.getPool(poolId);
      const containers = detail.containers ?? [];
      expect(claimedContainers(containers)).toHaveLength(0);
      expect(warmContainers(containers)).toHaveLength(1);
    });

    it("cleanup", async () => {
      if (poolId) {
        await sleep(2000);
        await client.deletePool(poolId);
        poolId = undefined!;
      }
    });
  },
  { timeout: 180_000 },
);

describe(
  "Sandbox Timeout",
  () => {
    let client: SandboxClient;
    let poolId: string;
    let sandboxId: string;

    beforeAll(() => {
      client = new SandboxClient({
        apiUrl: process.env.TENSORLAKE_API_URL ?? "https://api.tensorlake.ai",
        apiKey: process.env.TENSORLAKE_API_KEY,
      });
    });

    afterAll(async () => {
      if (sandboxId) {
        try {
          await client.delete(sandboxId);
        } catch {}
      }
      if (poolId) {
        try {
          await sleep(2000);
          await client.deletePool(poolId);
        } catch {}
      }
      client.close();
    });

    it("creates pool with timeout", async () => {
      const resp = await client.createPool({
        image: SANDBOX_IMAGE,
        cpus: SANDBOX_CPUS,
        memoryMb: SANDBOX_MEMORY_MB,
        ephemeralDiskMb: SANDBOX_DISK_MB,
        entrypoint: ["sleep", "300"],
        warmContainers: 1,
        timeoutSecs: 30,
      });
      poolId = resp.poolId;
    });

    it("claims sandbox from pool", async () => {
      await pollPoolContainers(client, poolId, 1);
      const resp = await client.claim(poolId);
      sandboxId = resp.sandboxId;
    });

    it("sandbox reaches running", async () => {
      const status = await pollSandboxStatus(client, sandboxId, SandboxStatus.RUNNING);
      expect(status).toBe(SandboxStatus.RUNNING);
    });

    it("sandbox suspended after timeout", async () => {
      // Wait beyond the 30s timeout
      await sleep(35_000);

      // On timeout the server suspends sandboxes regardless of whether they are
      // named or ephemeral. Suspended sandboxes do not have a terminal outcome.
      const status = await pollSandboxStatus(
        client,
        sandboxId,
        [SandboxStatus.TERMINATED, SandboxStatus.SUSPENDED],
        30,
      );
      expect([SandboxStatus.TERMINATED, SandboxStatus.SUSPENDED]).toContain(status);

      const info = await client.get(sandboxId);
      if (status === SandboxStatus.TERMINATED) {
        expect(info.outcome).toBe("Success(Timeout)");
        sandboxId = undefined!;
      } else {
        expect(info.outcome).toBeUndefined();
      }
    });

    it("cleanup", async () => {
      if (sandboxId) {
        try {
          await client.delete(sandboxId);
          await pollSandboxStatus(client, sandboxId, SandboxStatus.TERMINATED, 30);
        } catch {}
        sandboxId = undefined!;
      }
      if (poolId) {
        await sleep(2000);
        await client.deletePool(poolId);
        poolId = undefined!;
      }
    });
  },
  { timeout: 180_000 },
);
