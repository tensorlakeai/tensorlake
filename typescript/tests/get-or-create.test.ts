import { afterEach, describe, expect, it, vi } from "vitest";
import { Sandbox } from "../src/sandbox.js";
import { SandboxStatus } from "../src/models.js";
import {
  RemoteAPIError,
  SandboxError,
  SandboxNotFoundError,
} from "../src/errors.js";

// Sandbox.getOrCreate composes the static create/connect factories and the
// status/resume instance methods; spy on those, so no native binding or
// server is needed.

const NAME = "agent-session-123";

function sandboxMock(status: SandboxStatus): Sandbox {
  return {
    sandboxId: "sbx-uuid-1",
    status: vi.fn(async () => status),
    resume: vi.fn(async () => undefined),
  } as unknown as Sandbox;
}

describe("Sandbox.getOrCreate", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("returns an existing running sandbox without create or resume", async () => {
    const existing = sandboxMock(SandboxStatus.RUNNING);
    const connect = vi.spyOn(Sandbox, "connect").mockResolvedValue(existing);
    const create = vi.spyOn(Sandbox, "create");

    const result = await Sandbox.getOrCreate(NAME);

    expect(result).toBe(existing);
    expect(connect).toHaveBeenCalledTimes(1);
    expect(connect).toHaveBeenCalledWith({ sandboxId: NAME });
    expect(create).not.toHaveBeenCalled();
    expect(existing.resume).not.toHaveBeenCalled();
  });

  it("resumes an existing suspended sandbox", async () => {
    const existing = sandboxMock(SandboxStatus.SUSPENDED);
    vi.spyOn(Sandbox, "connect").mockResolvedValue(existing);

    const result = await Sandbox.getOrCreate(NAME);

    expect(result).toBe(existing);
    expect(existing.resume).toHaveBeenCalledTimes(1);
  });

  it("skips resume but still checks existence when resume is false", async () => {
    const existing = sandboxMock(SandboxStatus.SUSPENDED);
    vi.spyOn(Sandbox, "connect").mockResolvedValue(existing);

    const result = await Sandbox.getOrCreate(NAME, { resume: false });

    expect(result).toBe(existing);
    // The status fetch stays: connect returns a lazy handle, so it is the
    // only proof the name exists.
    expect(existing.status).toHaveBeenCalledTimes(1);
    expect(existing.resume).not.toHaveBeenCalled();
  });

  it("creates the sandbox when the lazy connect handle finds nothing", async () => {
    // The real Sandbox.connect never throws for a missing name: it returns
    // a lazy handle, and the 404 surfaces on the first request. getOrCreate
    // must treat that late SandboxNotFoundError as "name is free".
    const dead = {
      status: vi.fn(async () => {
        throw new SandboxNotFoundError(NAME);
      }),
      resume: vi.fn(),
    } as unknown as Sandbox;
    const created = sandboxMock(SandboxStatus.RUNNING);
    vi.spyOn(Sandbox, "connect").mockResolvedValue(dead);
    const create = vi.spyOn(Sandbox, "create").mockResolvedValue(created);

    const result = await Sandbox.getOrCreate(NAME, { resume: false });

    expect(result).toBe(created);
    expect(create).toHaveBeenCalledWith({ name: NAME });
  });

  it("creates the sandbox on first use", async () => {
    const created = sandboxMock(SandboxStatus.RUNNING);
    vi.spyOn(Sandbox, "connect").mockRejectedValue(
      new SandboxNotFoundError(NAME),
    );
    const create = vi.spyOn(Sandbox, "create").mockResolvedValue(created);

    const result = await Sandbox.getOrCreate(NAME, { image: "my-agent" });

    expect(result).toBe(created);
    expect(create).toHaveBeenCalledWith({ image: "my-agent", name: NAME });
    // A fresh create is already running; no status/resume round-trip.
    expect(created.status).not.toHaveBeenCalled();
    expect(created.resume).not.toHaveBeenCalled();
  });

  it("attaches to the winner after losing a create race", async () => {
    vi.useFakeTimers();
    const winner = sandboxMock(SandboxStatus.RUNNING);
    const connect = vi
      .spyOn(Sandbox, "connect")
      .mockRejectedValueOnce(new SandboxNotFoundError(NAME))
      .mockResolvedValueOnce(winner);
    vi.spyOn(Sandbox, "create").mockRejectedValue(
      new RemoteAPIError(409, "name is currently claimed"),
    );

    const pending = Sandbox.getOrCreate(NAME);
    await vi.runAllTimersAsync();
    const result = await pending;

    expect(result).toBe(winner);
    expect(connect).toHaveBeenCalledTimes(2);
  });

  it("resumes the race winner when it is suspended", async () => {
    vi.useFakeTimers();
    const winner = sandboxMock(SandboxStatus.SUSPENDED);
    vi.spyOn(Sandbox, "connect")
      .mockRejectedValueOnce(new SandboxNotFoundError(NAME))
      .mockResolvedValueOnce(winner);
    vi.spyOn(Sandbox, "create").mockRejectedValue(
      new RemoteAPIError(409, "name is currently claimed"),
    );

    const pending = Sandbox.getOrCreate(NAME);
    await vi.runAllTimersAsync();
    const result = await pending;

    expect(result).toBe(winner);
    expect(winner.resume).toHaveBeenCalledTimes(1);
  });

  it("waits for a suspending sandbox to settle before resume", async () => {
    // The server rejects resume while a suspend is still in progress, so
    // getOrCreate must wait until the sandbox is Suspended before resume().
    const settle = vi.fn(async () => SandboxStatus.SUSPENDED);
    const winner = {
      sandboxId: "sbx-uuid-1",
      status: vi.fn(async () => SandboxStatus.SUSPENDING),
      resume: vi.fn(async () => undefined),
      waitOutSuspending: settle,
    } as unknown as Sandbox;
    vi.spyOn(Sandbox, "connect").mockResolvedValue(winner);
    const create = vi.spyOn(Sandbox, "create");

    const result = await Sandbox.getOrCreate(NAME);

    expect(result).toBe(winner);
    expect(settle).toHaveBeenCalledTimes(1);
    expect(winner.resume).toHaveBeenCalledTimes(1);
    expect(create).not.toHaveBeenCalled();
  });

  it("waits for a pending sandbox to become running", async () => {
    // Another caller claimed the name but its create is still starting:
    // getOrCreate must block until Running and refresh proxy routing
    // instead of returning an unusable pending handle.
    const refresh = vi.fn(async () => undefined);
    const pendingSandbox = {
      sandboxId: "sbx-uuid-1",
      status: vi.fn(async () => SandboxStatus.PENDING),
      resume: vi.fn(),
      refreshRoutingWhenRunning: refresh,
    } as unknown as Sandbox;
    vi.spyOn(Sandbox, "connect").mockResolvedValue(pendingSandbox);
    const create = vi.spyOn(Sandbox, "create");

    const result = await Sandbox.getOrCreate(NAME);

    expect(result).toBe(pendingSandbox);
    expect(create).not.toHaveBeenCalled();
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(pendingSandbox.resume).not.toHaveBeenCalled();
  });

  it("rethrows non-409 create errors", async () => {
    vi.spyOn(Sandbox, "connect").mockRejectedValue(
      new SandboxNotFoundError(NAME),
    );
    vi.spyOn(Sandbox, "create").mockRejectedValue(
      new RemoteAPIError(400, "Image 'nope' is not registered"),
    );

    await expect(Sandbox.getOrCreate(NAME, { image: "nope" })).rejects.toThrow(
      /status 400/,
    );
  });

  it("rethrows unexpected connect errors", async () => {
    vi.spyOn(Sandbox, "connect").mockRejectedValue(
      new RemoteAPIError(403, "Invalid API key."),
    );
    const create = vi.spyOn(Sandbox, "create");

    await expect(Sandbox.getOrCreate(NAME)).rejects.toThrow(/status 403/);
    expect(create).not.toHaveBeenCalled();
  });

  it("gives up when the name stays unattachable", async () => {
    // Every connect says "not found", every create says "name claimed":
    // the previous holder is stuck terminating. The loop must end with a
    // SandboxError instead of spinning forever.
    vi.useFakeTimers();
    const conflict = new RemoteAPIError(409, "name is currently claimed");
    const connect = vi
      .spyOn(Sandbox, "connect")
      .mockRejectedValue(new SandboxNotFoundError(NAME));
    const create = vi.spyOn(Sandbox, "create").mockRejectedValue(conflict);

    const pending = Sandbox.getOrCreate(NAME);
    const outcome = expect(pending).rejects.toMatchObject({
      name: "SandboxError",
      message: expect.stringContaining(NAME),
      cause: conflict,
    });
    await vi.runAllTimersAsync();
    await outcome;

    expect(connect.mock.calls.length).toBeGreaterThan(1);
    expect(connect).toHaveBeenCalledTimes(create.mock.calls.length);
  });

  it("rejects poolId before touching the server", async () => {
    // A pool claim cannot carry a name on the wire, so a claimed sandbox
    // would be unnamed and every later getOrCreate would claim another one.
    const connect = vi.spyOn(Sandbox, "connect");
    const create = vi.spyOn(Sandbox, "create");

    await expect(
      Sandbox.getOrCreate(NAME, {
        poolId: "pool-1",
      } as Parameters<typeof Sandbox.getOrCreate>[1]),
    ).rejects.toThrow(/poolId/);
    expect(connect).not.toHaveBeenCalled();
    expect(create).not.toHaveBeenCalled();
  });

  it("forwards connection options to connect", async () => {
    const existing = sandboxMock(SandboxStatus.RUNNING);
    const connect = vi.spyOn(Sandbox, "connect").mockResolvedValue(existing);

    await Sandbox.getOrCreate(NAME, {
      apiUrl: "https://api.example.test",
      requestTimeout: 17,
    });

    expect(connect).toHaveBeenCalledWith({
      sandboxId: NAME,
      apiUrl: "https://api.example.test",
      requestTimeout: 17,
    });
  });
});
