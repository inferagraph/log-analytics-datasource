import type { QueryExecutor } from './QueryExecutor.js';
import type {
  LogAnalyticsAuth,
  LogOperation,
  LogQueryContext,
} from '../types.js';

type ApimAuth = Extract<LogAnalyticsAuth, { kind: 'apim' }>;

/**
 * `QueryExecutor` that calls a configured APIM endpoint over HTTP using
 * `globalThis.fetch` (Node 20+ has it natively).
 *
 * Default request shape: `POST {endpoint}` with JSON body
 * `{ workspaceId, query, op }`. Override either piece via `auth.buildRequest`.
 *
 * Response shapes accepted (both common Log Analytics shapes):
 *   - `{ rows: Record<string, unknown>[] }`
 *   - `{ tables: [{ columns: [{ name, ... }], rows: any[][] }, ...] }`
 *
 * Non-2xx responses throw with the status code and best-effort body text.
 */
export class ApimQueryExecutor implements QueryExecutor {
  private readonly auth: ApimAuth;
  private connected = false;

  constructor(auth: LogAnalyticsAuth) {
    if (auth.kind !== 'apim') {
      throw new Error(
        `ApimQueryExecutor only supports auth.kind='apim'. Got '${auth.kind}'.`,
      );
    }
    this.auth = auth;
  }

  async connect(): Promise<void> {
    // Sanity-check the configured endpoint; defer real network use to run().
    try {
      // eslint-disable-next-line no-new
      new URL(this.auth.endpoint);
    } catch {
      throw new Error(
        `ApimQueryExecutor: invalid endpoint URL '${this.auth.endpoint}'.`,
      );
    }
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    this.connected = false;
  }

  isConnected(): boolean {
    return this.connected;
  }

  async run(
    op: LogOperation,
    kql: string,
    ctx: LogQueryContext,
  ): Promise<Record<string, unknown>[]> {
    if (!this.connected) {
      throw new Error(
        'ApimQueryExecutor is not connected. Call connect() first.',
      );
    }

    let url = this.auth.endpoint;
    let body: unknown = {
      workspaceId: ctx.workspaceId,
      query: kql,
      op,
    };

    if (this.auth.buildRequest) {
      const overrides = this.auth.buildRequest(op, kql, ctx);
      if (overrides.url !== undefined) url = overrides.url;
      if (overrides.body !== undefined) body = overrides.body;
    }

    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      ...(this.auth.headers ?? {}),
    };

    const res = await globalThis.fetch(url, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      let detail = '';
      try {
        detail = await res.text();
      } catch {
        // ignore
      }
      throw new Error(
        `ApimQueryExecutor: ${res.status} ${res.statusText}${detail ? ` — ${detail}` : ''}`,
      );
    }

    const json = (await res.json()) as unknown;
    return ApimQueryExecutor.flatten(json);
  }

  /** Flatten either response shape into row objects. Exposed for testing. */
  static flatten(json: unknown): Record<string, unknown>[] {
    if (json && typeof json === 'object') {
      const obj = json as Record<string, unknown>;
      if (Array.isArray(obj.rows)) {
        return obj.rows as Record<string, unknown>[];
      }
      if (Array.isArray(obj.tables) && obj.tables.length > 0) {
        const table = obj.tables[0] as {
          columns?: Array<{ name: string }>;
          rows?: unknown[][];
        };
        const cols = table.columns ?? [];
        const rows = table.rows ?? [];
        return rows.map((row) => {
          const out: Record<string, unknown> = {};
          for (let i = 0; i < cols.length; i++) {
            out[cols[i].name] = row[i];
          }
          return out;
        });
      }
    }
    return [];
  }
}
