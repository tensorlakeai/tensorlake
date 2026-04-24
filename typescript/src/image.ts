import { randomUUID } from "node:crypto";

export const ImageBuildOperationType = {
  ADD: "ADD",
  COPY: "COPY",
  ENV: "ENV",
  RUN: "RUN",
  WORKDIR: "WORKDIR",
} as const;

export type ImageBuildOperationType =
  (typeof ImageBuildOperationType)[keyof typeof ImageBuildOperationType];

export interface ImageBuildOperation {
  type: ImageBuildOperationType;
  args: string[];
  options: Record<string, string>;
}

export interface ImageOptions {
  name?: string;
  tag?: string;
  baseImage?: string | null;
}

function cloneOperation(op: ImageBuildOperation): ImageBuildOperation {
  return {
    type: op.type,
    args: [...op.args],
    options: { ...op.options },
  };
}

export class Image {
  readonly _id: string;
  readonly _name: string;
  readonly _tag: string;
  readonly _baseImage: string | null;
  readonly _buildOperations: ImageBuildOperation[];

  constructor();
  constructor(name: string, tag?: string, baseImage?: string | null);
  constructor(options: ImageOptions);
  constructor(
    nameOrOptions: string | ImageOptions = {},
    tag = "latest",
    baseImage: string | null = null,
  ) {
    this._id = randomUUID();
    this._buildOperations = [];

    if (typeof nameOrOptions === "string") {
      this._name = nameOrOptions;
      this._tag = tag;
      this._baseImage = baseImage;
      return;
    }

    this._name = nameOrOptions.name ?? "default";
    this._tag = nameOrOptions.tag ?? "latest";
    this._baseImage = nameOrOptions.baseImage ?? null;
  }

  get name(): string {
    return this._name;
  }

  get tag(): string {
    return this._tag;
  }

  get baseImage(): string | null {
    return this._baseImage;
  }

  get buildOperations(): ImageBuildOperation[] {
    return this._buildOperations.map(cloneOperation);
  }

  add(
    src: string,
    dest: string,
    options: Record<string, string> | undefined = undefined,
  ): this {
    return this._addOperation({
      type: ImageBuildOperationType.ADD,
      args: [src, dest],
      options: options == null ? {} : { ...options },
    });
  }

  copy(
    src: string,
    dest: string,
    options: Record<string, string> | undefined = undefined,
  ): this {
    return this._addOperation({
      type: ImageBuildOperationType.COPY,
      args: [src, dest],
      options: options == null ? {} : { ...options },
    });
  }

  env(key: string, value: string): this {
    return this._addOperation({
      type: ImageBuildOperationType.ENV,
      args: [key, value],
      options: {},
    });
  }

  run(
    commands: string | string[],
    options: Record<string, string> | undefined = undefined,
  ): this {
    return this._addOperation({
      type: ImageBuildOperationType.RUN,
      args: Array.isArray(commands) ? [...commands] : [commands],
      options: options == null ? {} : { ...options },
    });
  }

  workdir(directory: string): this {
    return this._addOperation({
      type: ImageBuildOperationType.WORKDIR,
      args: [directory],
      options: {},
    });
  }

  /**
   * Build this image as a sandbox template and register it.
   *
   * Materializes the image in a build sandbox, snapshots the filesystem,
   * and registers the snapshot as a named sandbox template.
   */
  async build(
    options: import("./sandbox-image.js").CreateSandboxImageOptions = {},
  ): Promise<Record<string, unknown>> {
    const { createSandboxImage } = await import("./sandbox-image.js");
    return createSandboxImage(this, options);
  }

  private _addOperation(op: ImageBuildOperation): this {
    this._buildOperations.push(op);
    return this;
  }
}

function renderOptions(options: Record<string, string>): string {
  const entries = Object.entries(options);
  if (entries.length === 0) {
    return "";
  }
  return ` ${entries.map(([key, value]) => `--${key}=${value}`).join(" ")}`;
}

function renderBuildOp(op: ImageBuildOperation): string {
  const options = renderOptions(op.options);
  if (op.type === ImageBuildOperationType.ENV) {
    return `ENV${options} ${op.args[0]}=${JSON.stringify(op.args[1])}`;
  }
  return `${op.type}${options} ${op.args.join(" ")}`;
}

export function dockerfileContent(image: Image): string {
  const lines = image.baseImage == null ? [] : [`FROM ${image.baseImage}`];
  lines.push(...image.buildOperations.map((op) => renderBuildOp(op)));
  return lines.join("\n");
}
