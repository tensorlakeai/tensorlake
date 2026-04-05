export interface SSEMessage {
  data: string;
  event?: string;
  id?: string;
}

/**
 * Parse a Server-Sent Events stream into raw SSE messages.
 */
export async function* parseSSEMessages(
  stream: ReadableStream<Uint8Array>,
  signal?: AbortSignal,
): AsyncIterable<SSEMessage> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      if (signal?.aborted) break;

      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const parts = buffer.split(/\r?\n\r?\n/);
      buffer = parts.pop() ?? "";

      for (const part of parts) {
        const lines = part.split(/\r?\n/);
        const dataLines: string[] = [];
        let event: string | undefined;
        let id: string | undefined;

        for (const line of lines) {
          if (!line || line.startsWith(":")) continue;

          const separator = line.indexOf(":");
          const field = separator === -1 ? line : line.slice(0, separator);
          let value = separator === -1 ? "" : line.slice(separator + 1);
          if (value.startsWith(" ")) {
            value = value.slice(1);
          }

          if (field === "data") {
            dataLines.push(value);
          } else if (field === "event") {
            event = value;
          } else if (field === "id") {
            id = value;
          }
        }

        if (dataLines.length > 0 || event || id) {
          yield { data: dataLines.join("\n"), event, id };
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}

/**
 * Parse a Server-Sent Events stream from a ReadableStream<Uint8Array>.
 *
 * Yields parsed JSON objects for each `data:` line. Handles reconnection
 * by simply yielding events as they arrive.
 */
export async function* parseSSEStream<T>(
  stream: ReadableStream<Uint8Array>,
  signal?: AbortSignal,
): AsyncIterable<T> {
  for await (const message of parseSSEMessages(stream, signal)) {
    if (!message.data) continue;
    try {
      yield JSON.parse(message.data) as T;
    } catch {
      // Skip malformed JSON
    }
  }
}
