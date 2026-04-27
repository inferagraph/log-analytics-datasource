import type { LogOperation, LogQueryContext } from '../types.js';

/**
 * Decouples the SDK path (`@azure/monitor-query` + `@azure/identity`) from
 * the APIM path (HTTP via `globalThis.fetch`). Implementations return rows
 * as plain `Record<string, unknown>[]` so the datasource mapping layer can
 * stay agnostic of transport.
 */
export interface QueryExecutor {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  isConnected(): boolean;
  /**
   * Execute KQL against the workspace and return the first result table's
   * rows as `{ columnName: value }[]`. Empty results MUST return `[]`.
   */
  run(
    op: LogOperation,
    kql: string,
    ctx: LogQueryContext,
  ): Promise<Record<string, unknown>[]>;
}
