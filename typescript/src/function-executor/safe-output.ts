type OutputDestination = "stdout" | "stderr";

interface CapturedStream {
  readonly write: (
    chunk: string,
    callback: (error?: Error | null) => void,
  ) => boolean;
}

interface ExecutorOutput {
  readonly stdout: CapturedStream;
  readonly stderr: CapturedStream;
}

const nativeJSONStringify = JSON.stringify;

function captureStream(stream: NodeJS.WriteStream): CapturedStream {
  // A closed descriptor reports its failure through the stream's error event.
  // The executor owns these output streams, so logging failures must not become
  // uncaught exceptions in an allocation or initialization path.
  stream.on("error", () => undefined);
  return {
    // Deployed code runs in this process and can replace stream.write. Capture
    // the executor's writer before application modules are imported.
    write: stream.write.bind(stream) as CapturedStream["write"],
  };
}

const executorOutput: ExecutorOutput = {
  stdout: captureStream(process.stdout),
  stderr: captureStream(process.stderr),
};

/**
 * Writes one JSON log record without allowing serialization or stream failures
 * to affect executor control flow.
 */
export function writeStructuredOutput(
  destination: OutputDestination,
  createRecord: () => Record<string, unknown>,
): void {
  try {
    const serialized = nativeJSONStringify(createRecord());
    if (serialized == null) return;
    executorOutput[destination].write(`${serialized}\n`, () => undefined);
  } catch {
    // Logging is best-effort. In particular, terminal allocation and
    // initialization callbacks must still run after user code damages stdio.
  }
}
