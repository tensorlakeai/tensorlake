/** Internal protocol. Native objects never cross an isolate boundary. */
export type NativeClientKind =
  | "NativeSandboxClient"
  | "NativeSandboxProxyClient"
  | "NativeRepositoryClient";

export interface NativeHandle {
  id: number;
  kind: NativeClientKind;
  args: unknown[];
  parent?: NativeHandle;
}

export type WorkerRequest =
  | { type: "create"; handle: Omit<NativeHandle, "parent">; parentId?: number }
  | {
      type: "call";
      id: number;
      handleId?: number;
      method: string;
      args: unknown[];
      stream: boolean;
    }
  | { type: "release"; handleId: number }
  | { type: "ack" | "cancel"; id: number };

export type WorkerResponse =
  | { type: "result"; id: number; value: unknown }
  | {
      type: "error";
      id: number;
      error: { name: string; message: string; stack?: string };
    }
  | { type: "event"; id: number; value: string };

export const cancelNativeCall = Symbol.for("tensorlake.native.cancel.v1");
export const closeNativeHandle = Symbol.for("tensorlake.native.close.v1");
export type NativeCall<T> = Promise<T> & { [cancelNativeCall]?: () => void };
