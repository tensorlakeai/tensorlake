import { describe, expect, it } from "vitest";
import { parseSSEMessages, parseSSEStream } from "../src/sse.js";

function createStream(text: string): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode(text));
      controller.close();
    },
  });
}

function createChunkedStream(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(encoder.encode(chunk));
      }
      controller.close();
    },
  });
}

describe("parseSSEStream", () => {
  it("parses a single event", async () => {
    const stream = createStream('data: {"line":"hello","timestamp":1700000000}\n\n');
    const events: unknown[] = [];
    for await (const event of parseSSEStream(stream)) {
      events.push(event);
    }
    expect(events).toEqual([{ line: "hello", timestamp: 1700000000 }]);
  });

  it("parses multiple events", async () => {
    const stream = createStream(
      'data: {"line":"one","timestamp":1}\n\n' +
        'data: {"line":"two","timestamp":2}\n\n',
    );
    const events: unknown[] = [];
    for await (const event of parseSSEStream(stream)) {
      events.push(event);
    }
    expect(events).toHaveLength(2);
    expect(events[0]).toEqual({ line: "one", timestamp: 1 });
    expect(events[1]).toEqual({ line: "two", timestamp: 2 });
  });

  it("handles chunked delivery", async () => {
    const stream = createChunkedStream([
      'data: {"line":"he',
      'llo","timestamp":1}\n\ndata: {"line"',
      ':"world","timestamp":2}\n\n',
    ]);
    const events: unknown[] = [];
    for await (const event of parseSSEStream(stream)) {
      events.push(event);
    }
    expect(events).toHaveLength(2);
  });

  it("ignores non-data lines", async () => {
    const stream = createStream(
      'event: message\ndata: {"value":1}\nid: 123\n\n',
    );
    const events: unknown[] = [];
    for await (const event of parseSSEStream(stream)) {
      events.push(event);
    }
    expect(events).toEqual([{ value: 1 }]);
  });

  it("skips malformed JSON", async () => {
    const stream = createStream(
      'data: not json\n\ndata: {"ok":true}\n\n',
    );
    const events: unknown[] = [];
    for await (const event of parseSSEStream(stream)) {
      events.push(event);
    }
    expect(events).toEqual([{ ok: true }]);
  });

  it("handles empty stream", async () => {
    const stream = createStream("");
    const events: unknown[] = [];
    for await (const event of parseSSEStream(stream)) {
      events.push(event);
    }
    expect(events).toEqual([]);
  });

  it("respects abort signal", async () => {
    const controller = new AbortController();
    controller.abort();

    const stream = createStream('data: {"line":"test","timestamp":1}\n\n');
    const events: unknown[] = [];
    for await (const event of parseSSEStream(stream, controller.signal)) {
      events.push(event);
    }
    expect(events).toEqual([]);
  });

  it("parses raw SSE messages with event metadata", async () => {
    const stream = createStream(
      "event: progress\nid: 42\ndata: hello\ndata: world\n\n",
    );

    const messages: unknown[] = [];
    for await (const message of parseSSEMessages(stream)) {
      messages.push(message);
    }

    expect(messages).toEqual([
      { event: "progress", id: "42", data: "hello\nworld" },
    ]);
  });
});
