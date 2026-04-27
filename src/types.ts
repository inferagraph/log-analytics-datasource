/**
 * Logical operation key passed to query resolvers and to the executor.
 * Lets callers branch their KQL or APIM routing per-operation.
 */
export type LogOperation =
  | 'initial'
  | 'node'
  | 'neighbors'
  | 'search'
  | 'filter'
  | 'content';

/**
 * Authentication mode for the underlying executor.
 *
 * - `app-registration`: server-side app registration with client secret
 *   (uses `ClientSecretCredential` from `@azure/identity`).
 * - `managed-identity`: Azure-hosted managed identity
 *   (uses `ManagedIdentityCredential`).
 * - `apim`: route all requests through an Azure API Management endpoint
 *   (no Azure SDK; uses `globalThis.fetch`).
 *
 * NOTE: `app-registration` and `managed-identity` are server-only —
 * the client secret / managed identity must never reach the browser.
 */
export type LogAnalyticsAuth =
  | {
      kind: 'app-registration';
      tenantId: string;
      clientId: string;
      clientSecret: string;
    }
  | { kind: 'managed-identity' }
  | {
      kind: 'apim';
      /** APIM URL the app calls instead of monitor-query directly. */
      endpoint: string;
      /** Any APIM keys/headers (e.g. `Ocp-Apim-Subscription-Key`). */
      headers?: Record<string, string>;
      /**
       * Optional override for request shape. Default request is
       * `POST {endpoint}` with body `{ workspaceId, query, op }`.
       * Returning `url` overrides the URL; returning `body` overrides the JSON body.
       */
      buildRequest?: (
        op: LogOperation,
        kql: string,
        ctx: LogQueryContext,
      ) => { url?: string; body?: unknown };
    };

/**
 * Context passed to KQL resolvers (when `queries.x` is a function) and to
 * `buildRequest` overrides for the APIM executor.
 */
export interface LogQueryContext {
  workspaceId: string;
  /** Operation-specific parameters (e.g. `{ id }`, `{ query }`, `{ filter }`). */
  params: Record<string, unknown>;
}

/**
 * KQL query resolvers for each supported operation. Each entry can be a
 * static KQL string, or a function that receives a `LogQueryContext` and
 * returns a KQL string.
 *
 * `nodes` and `edges` are required; the rest are optional with documented
 * fallbacks (see `LogAnalyticsDatasource`).
 */
export interface LogAnalyticsQueryConfig {
  nodes: string | ((ctx: LogQueryContext) => string);
  edges: string | ((ctx: LogQueryContext) => string);
  node?: string | ((ctx: LogQueryContext) => string);
  neighbors?: string | ((ctx: LogQueryContext) => string);
  search?: string | ((ctx: LogQueryContext) => string);
  filter?: string | ((ctx: LogQueryContext) => string);
  content?: string | ((ctx: LogQueryContext) => string);
}

/**
 * Column-name mapping that tells the datasource how to turn KQL result rows
 * into `NodeData` / `EdgeData` / `ContentData`.
 */
export interface LogAnalyticsMapping {
  nodes: { idColumn: string; typeColumn?: string };
  edges: {
    idColumn: string;
    sourceColumn: string;
    targetColumn: string;
    typeColumn: string;
  };
  content?: {
    idColumn: string;
    bodyColumn: string;
    contentTypeColumn?: string;
  };
}

/**
 * Configuration for `LogAnalyticsDatasource`.
 */
export interface LogAnalyticsDatasourceConfig {
  /** Azure Log Analytics workspace ID (GUID). */
  workspaceId: string;
  /** Human-readable workspace name (used in error messages and logging). */
  workspaceName: string;
  auth: LogAnalyticsAuth;
  queries: LogAnalyticsQueryConfig;
  mapping: LogAnalyticsMapping;
  /**
   * ISO 8601 duration. Forwarded to the monitor-query SDK as the query timespan.
   * Defaults to `'P1D'` (one day) when omitted, to keep tests deterministic.
   */
  timespan?: { duration: string };
}
