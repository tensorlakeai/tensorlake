import Ajv, { type ValidateFunction } from "ajv";
import { isFile, type File } from "./file.js";
import { SDKUsageError } from "./errors.js";

export type JSONSchema = Record<string, unknown>;

const TYPE = Symbol("tensorlake.schema.type");
const ajv = new Ajv({ allErrors: true, strict: false });

export interface Schema<T> {
  readonly jsonSchema: JSONSchema;
  readonly [TYPE]?: T;
  readonly _file?: boolean;
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

function makeSchema<T>(jsonSchema: JSONSchema, file = false): Schema<T> {
  return Object.freeze({ jsonSchema: Object.freeze(jsonSchema), _file: file });
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
  return Object.freeze({
    name,
    schema: valueSchema,
    description: options.description,
    required: options.optional !== true && !hasDefault,
    hasDefault,
    defaultValue: options.default,
  });
}

export const schema = {
  /** Any JSON-serializable value. Useful when strict runtime validation is not needed. */
  json: <T = unknown>() => makeSchema<T>({}),
  string: (options: Omit<JSONSchema, "type"> = {}) =>
    makeSchema<string>({ type: "string", ...options }),
  number: (options: Omit<JSONSchema, "type"> = {}) =>
    makeSchema<number>({ type: "number", ...options }),
  integer: (options: Omit<JSONSchema, "type"> = {}) =>
    makeSchema<number>({ type: "integer", ...options }),
  boolean: () => makeSchema<boolean>({ type: "boolean" }),
  null: () => makeSchema<null>({ type: "null" }),
  literal: <T extends string | number | boolean | null>(value: T) =>
    makeSchema<T>({ const: value }),
  enum: <const T extends readonly (string | number)[]>(values: T) =>
    makeSchema<T[number]>({ enum: [...values] }),
  array: <S extends Schema<unknown>>(item: S, options: Omit<JSONSchema, "type" | "items"> = {}) =>
    makeSchema<Infer<S>[]>({ type: "array", items: item.jsonSchema, ...options }),
  tuple: <const S extends readonly Schema<unknown>[]>(items: S) =>
    makeSchema<{ [K in keyof S]: S[K] extends Schema<infer T> ? T : never }>({
      type: "array",
      prefixItems: items.map((item) => item.jsonSchema),
      minItems: items.length,
      maxItems: items.length,
    }),
  record: <S extends Schema<unknown>>(value: S) =>
    makeSchema<Record<string, Infer<S>>>({
      type: "object",
      additionalProperties: value.jsonSchema,
    }),
  object: <const P extends Record<string, Schema<unknown>>>(properties: P) =>
    makeSchema<{ [K in keyof P]: P[K] extends Schema<infer T> ? T : never }>({
      type: "object",
      properties: Object.fromEntries(
        Object.entries(properties).map(([key, item]) => [key, item.jsonSchema]),
      ),
      required: Object.keys(properties),
      additionalProperties: false,
    }),
  union: <const S extends readonly Schema<unknown>[]>(...choices: S) =>
    makeSchema<Infer<S[number]>>({ anyOf: choices.map((choice) => choice.jsonSchema) }),
  nullable: <S extends Schema<unknown>>(value: S) =>
    makeSchema<Infer<S> | null>({ anyOf: [value.jsonSchema, { type: "null" }] }),
  file: () => makeSchema<File>({ type: "tensorlake_file" }, true),
  custom: <T>(jsonSchema: JSONSchema) => makeSchema<T>({ ...jsonSchema }),
  parameter,
};

const validatorCache = new WeakMap<object, ValidateFunction>();

export function validateWithSchema<T>(valueSchema: Schema<T>, value: unknown, label: string): T {
  if (valueSchema._file) {
    if (!isFile(value)) {
      throw new SDKUsageError(`${label} must be a Tensorlake File`);
    }
    return value as T;
  }
  let validate = validatorCache.get(valueSchema as object);
  if (validate == null) {
    validate = ajv.compile(valueSchema.jsonSchema);
    validatorCache.set(valueSchema as object, validate);
  }
  if (!validate(value)) {
    const details = ajv.errorsText(validate.errors, { separator: "; " });
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
  if (parameter.hasDefault) result.default = parameter.schema._file ? true : parameter.defaultValue;
  return result;
}
