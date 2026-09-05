// Read the ELF interpreter, rather than generating a process-wide diagnostic
// report. Both supported Linux architectures use little-endian ELF64. Reads
// are bounded to the header, program headers, and interpreter pathname.
function detectLinuxLibc(fs, executable = process.execPath) {
  const override = process.env.TENSORLAKE_NODE_LIBC;
  if (override != null) {
    if (override === "gnu" || override === "musl") return override;
    throw new Error("TENSORLAKE_NODE_LIBC must be 'gnu' or 'musl'.");
  }

  let fd;
  try {
    fd = fs.openSync(executable, "r");
    function read(size, offset) {
      if (!Number.isSafeInteger(offset) || offset < 0) {
        throw new Error("Invalid ELF offset");
      }
      const buffer = Buffer.alloc(size);
      let count = 0;
      while (count < size) {
        const bytes = fs.readSync(fd, buffer, count, size - count, offset + count);
        if (bytes === 0) throw new Error("Truncated ELF executable");
        count += bytes;
      }
      return buffer;
    }

    const header = read(64, 0);
    if (header.readUInt32BE(0) !== 0x7f454c46 || header[4] !== 2 || header[5] !== 1) {
      throw new Error("Expected a little-endian ELF64 executable");
    }
    const entrySize = header.readUInt16LE(54);
    const count = header.readUInt16LE(56);
    if (entrySize !== 56 || count === 0 || count > 128) {
      throw new Error("Unsupported ELF program headers");
    }
    const entries = read(entrySize * count, Number(header.readBigUInt64LE(32)));
    for (let i = 0; i < count; i++) {
      const offset = i * entrySize;
      if (entries.readUInt32LE(offset) !== 3) continue; // PT_INTERP
      const size = Number(entries.readBigUInt64LE(offset + 32));
      if (size < 2 || size > 4096) throw new Error("Invalid ELF interpreter size");
      const interpreter = read(size, Number(entries.readBigUInt64LE(offset + 8)));
      if (interpreter[size - 1] !== 0) throw new Error("Unterminated ELF interpreter");
      const name = interpreter.subarray(0, -1).toString("utf8").split("/").at(-1);
      if (name.startsWith("ld-musl-")) return "musl";
      if (name.startsWith("ld-linux-")) return "gnu";
      throw new Error("Unrecognized ELF interpreter");
    }
    throw new Error("No ELF interpreter (statically linked executable)");
  } catch (cause) {
    throw new Error(
      "Unable to detect Linux libc from the Node executable. Set TENSORLAKE_NODE_LIBC to 'gnu' or 'musl' for a nonstandard runtime.",
      { cause },
    );
  } finally {
    if (fd != null) fs.closeSync(fd);
  }
}

module.exports = { detectLinuxLibc };
