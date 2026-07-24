import type { RegisteredDefinition } from "./function.js";
import { SDKUsageError } from "./errors.js";

interface RegistryState {
  functions: Map<string, RegisteredDefinition>;
  applications: Map<string, RegisteredDefinition>;
}

export interface RegistrySnapshot {
  functions: Map<string, RegisteredDefinition>;
  applications: Map<string, RegisteredDefinition>;
}

const REGISTRY_KEY = Symbol.for("tensorlake.applications.registry.v1");

function registry(): RegistryState {
  const target = globalThis as typeof globalThis & { [REGISTRY_KEY]?: RegistryState };
  target[REGISTRY_KEY] ??= { functions: new Map(), applications: new Map() };
  return target[REGISTRY_KEY];
}

export function registerDefinition(definition: RegisteredDefinition, isApplication: boolean): void {
  const state = registry();
  const existing = state.functions.get(definition.name);
  if (existing != null && existing !== definition) {
    throw new SDKUsageError(`Multiple Tensorlake functions are named '${definition.name}'`);
  }
  state.functions.set(definition.name, definition);
  if (isApplication) {
    const app = state.applications.get(definition.name);
    if (app != null && app !== definition) {
      throw new SDKUsageError(`Multiple Tensorlake applications are named '${definition.name}'`);
    }
    state.applications.set(definition.name, definition);
  }
}

export function getFunction(name: string): RegisteredDefinition {
  const result = registry().functions.get(name);
  if (result == null) throw new SDKUsageError(`Tensorlake function '${name}' is not registered`);
  return result;
}

export function getFunctions(): RegisteredDefinition[] {
  return [...registry().functions.values()];
}

export function getApplications(): RegisteredDefinition[] {
  return [...registry().applications.values()];
}

export function snapshotRegistry(): RegistrySnapshot {
  const state = registry();
  return {
    functions: new Map(state.functions),
    applications: new Map(state.applications),
  };
}

export function restoreRegistry(snapshot: RegistrySnapshot): void {
  const state = registry();
  state.functions.clear();
  state.applications.clear();
  for (const [name, definition] of snapshot.functions) state.functions.set(name, definition);
  for (const [name, definition] of snapshot.applications) state.applications.set(name, definition);
}

export function clearRegistryForTest(): void {
  registry().functions.clear();
  registry().applications.clear();
}
