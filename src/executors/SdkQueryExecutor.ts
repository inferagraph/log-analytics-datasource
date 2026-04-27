import {
  LogsQueryClient,
  type LogsQueryResult,
  type LogsTable,
} from '@azure/monitor-query';
import {
  ClientSecretCredential,
  ManagedIdentityCredential,
  type TokenCredential,
} from '@azure/identity';
import type { QueryExecutor } from './QueryExecutor.js';
import type {
  LogAnalyticsAuth,
  LogOperation,
  LogQueryContext,
} from '../types.js';

/** Default timespan if the caller didn't configure one. */
const DEFAULT_TIMESPAN = { duration: 'P1D' } as const;

/**
 * `QueryExecutor` that talks to Log Analytics via the official
 * `@azure/monitor-query` SDK. Builds a credential from `auth.kind`:
 *
 * - `app-registration` → `ClientSecretCredential`
 * - `managed-identity` → `ManagedIdentityCredential`
 *
 * Any other `auth.kind` is rejected at construction time — that's an
 * `ApimQueryExecutor` job.
 */
export class SdkQueryExecutor implements QueryExecutor {
  private client: LogsQueryClient | null = null;
  private readonly auth: Extract<
    LogAnalyticsAuth,
    { kind: 'app-registration' } | { kind: 'managed-identity' }
  >;
  private readonly timespan: { duration: string };

  constructor(
    auth: LogAnalyticsAuth,
    timespan?: { duration: string },
  ) {
    if (auth.kind !== 'app-registration' && auth.kind !== 'managed-identity') {
      throw new Error(
        `SdkQueryExecutor cannot handle auth.kind='${auth.kind}'. Use ApimQueryExecutor.`,
      );
    }
    this.auth = auth;
    this.timespan = timespan ?? DEFAULT_TIMESPAN;
  }

  async connect(): Promise<void> {
    const credential = this.buildCredential();
    this.client = new LogsQueryClient(credential);
  }

  async disconnect(): Promise<void> {
    this.client = null;
  }

  isConnected(): boolean {
    return this.client !== null;
  }

  async run(
    _op: LogOperation,
    kql: string,
    ctx: LogQueryContext,
  ): Promise<Record<string, unknown>[]> {
    if (!this.client) {
      throw new Error(
        'SdkQueryExecutor is not connected. Call connect() first.',
      );
    }

    const result: LogsQueryResult = await this.client.queryWorkspace(
      ctx.workspaceId,
      kql,
      this.timespan,
    );

    const tables = (result as { tables?: LogsTable[] }).tables ?? [];
    if (tables.length === 0) return [];

    const table = tables[0];
    const cols = table.columnDescriptors ?? [];
    const rows = table.rows ?? [];
    return rows.map((row) => {
      const obj: Record<string, unknown> = {};
      for (let i = 0; i < cols.length; i++) {
        const name = (cols[i] as { name: string }).name;
        obj[name] = (row as unknown[])[i];
      }
      return obj;
    });
  }

  private buildCredential(): TokenCredential {
    if (this.auth.kind === 'app-registration') {
      return new ClientSecretCredential(
        this.auth.tenantId,
        this.auth.clientId,
        this.auth.clientSecret,
      );
    }
    // managed-identity
    return new ManagedIdentityCredential();
  }
}
