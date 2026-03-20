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
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      if (signal?.aborted) break;

      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });

      // SSE events are separated by double newlines
      const parts = buffer.split("\n\n");
      // Last part may be incomplete
      buffer = parts.pop() ?? "";

      for (const part of parts) {
        const lines = part.split("\n");
        for (const line of lines) {
          if (line.startsWith("data:")) {
            const data = line.slice(5).trim();
            if (data) {
              try {
                yield JSON.parse(data) as T;
              } catch {
                // Skip malformed JSON
              }
            }
          }
          // Ignore `event:`, `id:`, `retry:`, and comment lines
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}
