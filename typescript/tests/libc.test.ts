import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { detectLinuxLibc } from "../lib/libc.cjs";

const fs = createRequire(import.meta.url)("node:fs") as typeof import("node:fs");

function executable(interpreter: string): Buffer {
  const name = Buffer.from(`${interpreter}\0`);
  const elf = Buffer.alloc(64 + 56 + name.length);
  elf.writeUInt32BE(0x7f454c46);
  elf[4] = 2;
  elf[5] = 1;
  elf.writeBigUInt64LE(64n, 32);
  elf.writeUInt16LE(56, 54);
  elf.writeUInt16LE(1, 56);
  elf.writeUInt32LE(3, 64);
  elf.writeBigUInt64LE(120n, 72);
  elf.writeBigUInt64LE(BigInt(name.length), 96);
  name.copy(elf, 120);
  return elf;
}

describe("Linux executable libc detection", () => {
  let directory: string;
  let filename: string;
  beforeEach(() => {
    vi.stubEnv("TENSORLAKE_NODE_LIBC", undefined);
    directory = mkdtempSync(path.join(tmpdir(), "tensorlake-libc-"));
    filename = path.join(directory, "node");
  });
  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    rmSync(directory, { recursive: true, force: true });
  });

  it.each([
    ["/lib64/ld-linux-x86-64.so.2", "gnu"],
    ["/lib/ld-linux-aarch64.so.1", "gnu"],
    ["/nix/store/runtime/lib/ld-linux-x86-64.so.2", "gnu"],
    ["/lib/ld-musl-x86_64.so.1", "musl"],
    ["/lib/ld-musl-aarch64.so.1", "musl"],
  ])("detects %s from bounded executable metadata", (interpreter, libc) => {
    const elf = executable(interpreter);
    writeFileSync(filename, elf);
    const read = vi.spyOn(fs, "readSync");
    const close = vi.spyOn(fs, "closeSync");
    expect(detectLinuxLibc(fs, filename)).toBe(libc);
    expect(read.mock.results.reduce((sum, result) => sum + Number(result.value), 0)).toBe(elf.length);
    expect(close).toHaveBeenCalledTimes(1);
  });

  it.each(["truncated", "oversized", "static", "unknown"]) (
    "fails clearly and closes the executable for %s metadata",
    (kind) => {
      let elf = executable("/lib/unknown-loader.so");
      if (kind === "truncated") elf = elf.subarray(0, 20);
      if (kind === "oversized") elf.writeUInt16LE(65535, 56);
      if (kind === "static") elf.writeUInt32LE(1, 64);
      writeFileSync(filename, elf);
      const close = vi.spyOn(fs, "closeSync");
      expect(() => detectLinuxLibc(fs, filename)).toThrow("TENSORLAKE_NODE_LIBC");
      expect(close).toHaveBeenCalledTimes(1);
    },
  );

  it("allows an explicit libc for a nonstandard executable", () => {
    vi.stubEnv("TENSORLAKE_NODE_LIBC", "musl");
    expect(detectLinuxLibc(fs, filename)).toBe("musl");
  });
});
