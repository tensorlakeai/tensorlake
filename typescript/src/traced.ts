/** An SDK result paired with its server-side W3C trace ID. */
export type Traced<T> = T & { readonly traceId: string };
