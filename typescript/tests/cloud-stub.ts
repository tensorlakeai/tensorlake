import { __setNativeSandboxBindingForTest, type NativeSandboxBinding } from "../src/native-sandbox.js";
import type { NativeCloudRequest } from "../src/native-cloud.js";

/** Drive the native boundary without an addon; keep existing API wire assertions. */
export function installCloudStub(handler: (url: string, init?: RequestInit) => Response | Promise<Response>): void {
  __setNativeSandboxBindingForTest({
    NativeCloudClient: class {
      private options: Record<string, string>;
      constructor(optionsJson: string) { this.options = JSON.parse(optionsJson); }

      private async response(request: NativeCloudRequest): Promise<Response> {
        const headers = JSON.parse(request.headersJson) as Record<string, string>;
        if (this.options.apiKey) headers.Authorization = `Bearer ${this.options.apiKey}`;
        if (this.options.organizationId) headers["X-Forwarded-Organization-Id"] = this.options.organizationId;
        if (this.options.projectId) headers["X-Forwarded-Project-Id"] = this.options.projectId;
        let body: BodyInit | undefined = request.body && Uint8Array.from(request.body).buffer;
        if (request.parts) {
          const form = new FormData();
          for (const part of request.parts) {
            if (part.filename === undefined) form.append(part.name, part.data.toString());
            else form.append(part.name, new Blob([Uint8Array.from(part.data).buffer], { type: part.contentType }), part.filename);
          }
          body = form;
        }
        const response = await handler(this.options.baseUrl + request.path, { method: request.method, headers, body });
        if (!response.ok && !request.statusOnlyCodes.includes(response.status)) {
          throw new Error(JSON.stringify({ category: "remote_api", status: response.status, message: await response.text() }));
        }
        return response;
      }

      async request(request: NativeCloudRequest) {
        const response = await this.response(request);
        const data = request.statusOnlyCodes.includes(response.status)
          ? new Uint8Array() : new Uint8Array(await response.arrayBuffer());
        return { status: response.status, headersJson: JSON.stringify(Object.fromEntries(response.headers)), data, traceId: "cloud-trace" };
      }

      async stream(request: NativeCloudRequest, emit: (event: string) => Promise<void>) {
        const response = await this.response(request);
        // Fixtures contain finite, single-line JSON events. Real SSE parsing is
        // exercised against the Rust transport in native-cloud.test.ts.
        for (const line of (await response.text()).split("\n")) {
          if (line.startsWith("data: ")) await emit(line.slice(6));
        }
        return "cloud-trace";
      }
    },
  } as unknown as NativeSandboxBinding);
}

export function clearCloudStub(): void { __setNativeSandboxBindingForTest(undefined); }
