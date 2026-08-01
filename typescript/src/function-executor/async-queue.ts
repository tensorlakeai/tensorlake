export class AsyncQueue<T> {
  private readonly values: T[] = [];
  private readonly readers: Array<(value: T | undefined) => void> = [];
  private closed = false;

  push(value: T): void {
    if (this.closed) return;
    const reader = this.readers.shift();
    if (reader == null) this.values.push(value);
    else reader(value);
  }

  async next(): Promise<T | undefined> {
    const value = this.values.shift();
    if (value != null) return value;
    if (this.closed) return undefined;
    return new Promise((resolve) => this.readers.push(resolve));
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const reader of this.readers.splice(0)) reader(undefined);
  }
}

export interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

export function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((ok, fail) => {
    resolve = ok;
    reject = fail;
  });
  return { promise, resolve, reject };
}
