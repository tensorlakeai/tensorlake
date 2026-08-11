import Ajv, { type ValidateFunction } from "ajv";
import Ajv2020 from "ajv/dist/2020.js";
import { File, isFile } from "./file.js";
import { HttpBody, isHttpBody } from "./http-body.js";
import { SDKUsageError } from "./errors.js";

export type JSONSchema = Record<string, unknown>;

const TYPE = Symbol("tensorlake.schema.type");
// SDK-generated schemas use JSON Schema 2020-12 features such as prefixItems.
// Validate with the same dialect that we emit into application manifests so
// local/executor checks cannot silently accept values the public schema rejects.
const ajvDraft7 = new Ajv({ allErrors: true, strict: false });
const ajv2020 = new Ajv2020({ allErrors: true, strict: false });

export interface Schema<T> {
  readonly jsonSchema: JSONSchema;
  readonly [TYPE]?: T;
  readonly _file?: boolean;
  readonly _httpBody?: boolean;
}

export type Infer<S extends Schema<unknown>> = S extends Schema<infer T> ? T : never;

export interface ParameterOptions<T> {
  description?: string;
  optional?: boolean;
  default?: T;
}

export interface Parameter<T> {
  readonly name: string;
  readonly schema: Schema<T>;
  readonly description?: string;
  readonly required: boolean;
  readonly hasDefault: boolean;
  readonly defaultValue?: T;
}

export type ParameterValues<P extends readonly Parameter<unknown>[]> = {
  [K in keyof P]: P[K] extends Parameter<infer T> ? T : never;
};

function snapshotJSONValue(
  value: unknown,
  label: string,
  ancestors: Set<object>,
  freeze: boolean,
): unknown {
  if (
    value == null
    || typeof value === "string"
    || typeof value === "boolean"
  ) {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new SDKUsageError(`${label} contains a non-finite number`);
    }
    return value;
  }
  if (typeof value !== "object") {
    throw new SDKUsageError(`${label} contains unsupported ${typeof value}`);
  }
  if (ancestors.has(value)) {
    throw new SDKUsageError(`${label} contains a cycle`);
  }
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype) {
    throw new SDKUsageError(
      `${label} contains unsupported ${value.constructor?.name ?? "object"}`,
    );
  }

  ancestors.add(value);
  let result: unknown[] | Record<string, unknown>;
  if (Array.isArray(value)) {
    result = [];
    for (let index = 0; index < value.length; index += 1) {
      if (!Object.prototype.hasOwnProperty.call(value, index)) {
        throw new SDKUsageError(`${label} contains a sparse array entry at index ${index}`);
      }
      result.push(snapshotJSONValue(
        value[index],
        `${label}[${index}]`,
        ancestors,
        freeze,
      ));
    }
  } else {
    result = Object.fromEntries(
      Object.entries(value).map(([key, item]) => [
        key,
        snapshotJSONValue(item, `${label}.${key}`, ancestors, freeze),
      ]),
    );
  }
  ancestors.delete(value);
  return freeze ? Object.freeze(result) : result;
}

function snapshotJSONSchema(jsonSchema: JSONSchema): JSONSchema {
  return snapshotJSONValue(
    jsonSchema,
    "JSON schema",
    new Set(),
    true,
  ) as JSONSchema;
}

function snapshotDefault<T>(valueSchema: Schema<T>, value: T): T {
  if (valueSchema._file) {
    if (!isFile(value)) {
      throw new SDKUsageError("parameter default must be a Tensorlake File");
    }
    return new File(new Uint8Array(value.content), value.contentType) as T;
  }
  if (valueSchema._httpBody) {
    if (!isHttpBody(value)) {
      throw new SDKUsageError("parameter default must be a Tensorlake HttpBody");
    }
    return new HttpBody(new Uint8Array(value.content), value.contentType) as T;
  }
  return snapshotJSONValue(
    value,
    "parameter default",
    new Set(),
    false,
  ) as T;
}

function makeSchema<T>(
  jsonSchema: JSONSchema,
  rawBody?: "file" | "httpBody",
): Schema<T> {
  return Object.freeze({
    jsonSchema: snapshotJSONSchema(jsonSchema),
    _file: rawBody === "file",
    _httpBody: rawBody === "httpBody",
  });
}

function rejectRawBodyComposition(
  operation: string,
  values: readonly Schema<unknown>[],
): void {
  const rawBody = values.find((value) => value._file || value._httpBody);
  if (rawBody != null) {
    const factory = rawBody._httpBody ? "httpBody" : "file";
    const type = rawBody._httpBody ? "HttpBody" : "File";
    const supportedPosition = rawBody._httpBody
      ? "as a direct application parameter"
      : "as a direct parameter or return value";
    throw new SDKUsageError(
      `schema.${operation} cannot contain schema.${factory}(); ${type} is only supported`
      + ` ${supportedPosition}`,
    );
  }
}

function parameter<T>(
  name: string,
  valueSchema: Schema<T>,
  options: ParameterOptions<T> = {},
): Parameter<T> {
  if (!/^[A-Za-z_$][\w$]*$/.test(name)) {
    throw new SDKUsageError(`Invalid parameter name '${name}'`);
  }
  const hasDefault = Object.prototype.hasOwnProperty.call(options, "default");
  if (hasDefault) validateWithSchema(valueSchema, options.default, `default for '${name}'`);
  const defaultSnapshot = hasDefault
    ? snapshotDefault(valueSchema, options.default as T)
    : undefined;
  return Object.freeze({
    name,
    schema: valueSchema,
    description: options.description,
    required: options.optional !== true && !hasDefault,
    hasDefault,
    // Defaults cross two public boundaries: callers can inspect the registered
    // definition, and every invocation receives the default when an argument is
    // omitted. Return a fresh boundary copy so neither path can mutate the
    // registration snapshot, later invocations, or generated manifests.
    get defaultValue(): T | undefined {
      return hasDefault
        ? snapshotDefault(valueSchema, defaultSnapshot as T)
        : undefined;
    },
  });
}

export const schema = {
  /** Any JSON-serializable value. Useful when strict runtime validation is not needed. */
  json: <T = unknown>() => makeSchema<T>({}),
  string: (options: Omit<JSONSchema, "type"> = {}) =>
    makeSchema<string>({ ...options, type: "string" }),
  number: (options: Omit<JSONSchema, "type"> = {}) =>
    makeSchema<number>({ ...options, type: "number" }),
  integer: (options: Omit<JSONSchema, "type"> = {}) =>
    makeSchema<number>({ ...options, type: "integer" }),
  boolean: () => makeSchema<boolean>({ type: "boolean" }),
  null: () => makeSchema<null>({ type: "null" }),
  literal: <T extends string | number | boolean | null>(value: T) =>
    makeSchema<T>({ const: value }),
  enum: <const T extends readonly (string | number)[]>(values: T) =>
    makeSchema<T[number]>({ enum: [...values] }),
  array: <S extends Schema<unknown>>(item: S, options: Omit<JSONSchema, "type" | "items"> = {}) => {
    rejectRawBodyComposition("array", [item]);
    return makeSchema<Infer<S>[]>({ ...options, type: "array", items: item.jsonSchema });
  },
  tuple: <const S extends readonly Schema<unknown>[]>(items: S) => {
    rejectRawBodyComposition("tuple", items);
    return makeSchema<{ [K in keyof S]: S[K] extends Schema<infer T> ? T : never }>({
      type: "array",
      prefixItems: items.map((item) => item.jsonSchema),
      minItems: items.length,
      maxItems: items.length,
    });
  },
  record: <S extends Schema<unknown>>(value: S) => {
    rejectRawBodyComposition("record", [value]);
    return makeSchema<Record<string, Infer<S>>>({
      type: "object",
      additionalProperties: value.jsonSchema,
    });
  },
  object: <const P extends Record<string, Schema<unknown>>>(properties: P) => {
    rejectRawBodyComposition("object", Object.values(properties));
    return makeSchema<{ [K in keyof P]: P[K] extends Schema<infer T> ? T : never }>({
      type: "object",
      properties: Object.fromEntries(
        Object.entries(properties).map(([key, item]) => [key, item.jsonSchema]),
      ),
      required: Object.keys(properties),
      additionalProperties: false,
    });
  },
  union: <const S extends readonly Schema<unknown>[]>(...choices: S) => {
    rejectRawBodyComposition("union", choices);
    return makeSchema<Infer<S[number]>>({ anyOf: choices.map((choice) => choice.jsonSchema) });
  },
  nullable: <S extends Schema<unknown>>(value: S) => {
    rejectRawBodyComposition("nullable", [value]);
    return makeSchema<Infer<S> | null>({ anyOf: [value.jsonSchema, { type: "null" }] });
  },
  file: () => makeSchema<File>({ type: "tensorlake_file" }, "file"),
  httpBody: () =>
    makeSchema<HttpBody>({ type: "tensorlake_http_body" }, "httpBody"),
  custom: <T>(jsonSchema: JSONSchema) => makeSchema<T>({ ...jsonSchema }),
  parameter,
};

interface CachedValidator {
  readonly validate: ValidateFunction;
  readonly errorsText: (errors: ValidateFunction["errors"]) => string;
}

const validatorCache = new WeakMap<object, CachedValidator>();

function compileValidator(jsonSchema: JSONSchema): CachedValidator {
  // Earlier SDK versions validated custom schemas with Ajv's draft-07
  // default. Preserve that behavior for explicitly declared draft-07 schemas
  // while using 2020-12 for SDK-generated schemas and custom schemas without a
  // dialect declaration.
  const dialect = jsonSchema.$schema;
  const engine = typeof dialect === "string" && /\/draft-07\/schema#?$/.test(dialect)
    ? ajvDraft7
    : ajv2020;
  const validate = engine.compile(jsonSchema);
  return {
    validate,
    errorsText: (errors) => engine.errorsText(errors, { separator: "; " }),
  };
}

export function validateWithSchema<T>(valueSchema: Schema<T>, value: unknown, label: string): T {
  if (valueSchema._file) {
    if (!isFile(value)) {
      throw new SDKUsageError(`${label} must be a Tensorlake File`);
    }
    return value as T;
  }
  if (valueSchema._httpBody) {
    if (!isHttpBody(value)) {
      throw new SDKUsageError(`${label} must be a Tensorlake HttpBody`);
    }
    return value as T;
  }
  if (isFile(value)) {
    throw new SDKUsageError(`${label} must use schema.file() for a Tensorlake File`);
  }
  if (isHttpBody(value)) {
    throw new SDKUsageError(
      `${label} must use schema.httpBody() for a Tensorlake HttpBody`,
    );
  }
  let cached = validatorCache.get(valueSchema as object);
  if (cached == null) {
    try {
      cached = compileValidator(valueSchema.jsonSchema);
    } catch (error) {
      const details = error instanceof Error ? error.message : String(error);
      throw new SDKUsageError(`${label} uses an invalid JSON schema: ${details}`);
    }
    validatorCache.set(valueSchema as object, cached);
  }
  if (!cached.validate(value)) {
    const details = cached.errorsText(cached.validate.errors);
    throw new SDKUsageError(`${label} does not match its schema: ${details}`);
  }
  return value as T;
}

export function parameterJSONSchema(parameter: Parameter<unknown>): JSONSchema {
  const result: JSONSchema = {
    ...parameter.schema.jsonSchema,
    title: parameter.name,
    parameter_kind: "POSITIONAL_OR_KEYWORD",
  };
  if (parameter.description != null) result.description = parameter.description;
  if (parameter.hasDefault) {
    result.default = parameter.schema._file || parameter.schema._httpBody
      ? true
      : parameter.defaultValue;
  }
  return result;
}
