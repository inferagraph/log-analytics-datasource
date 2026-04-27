# @inferagraph/log-analytics-datasource

Azure Log Analytics datasource plugin for [@inferagraph/core](https://github.com/inferagraph/core). Reads graph nodes, edges, and content from a Log Analytics workspace via KQL.

Three auth modes are supported:

- `app-registration` — server-side app registration with client secret
- `managed-identity` — Azure-hosted managed identity
- `apim` — route through an Azure API Management endpoint (no Azure SDK)

The first two use [`@azure/monitor-query`](https://www.npmjs.com/package/@azure/monitor-query) and [`@azure/identity`](https://www.npmjs.com/package/@azure/identity). The third uses `globalThis.fetch` (Node 20+).

> **SSR caveat:** `app-registration` and `managed-identity` are server-side only. The client secret / managed identity must NEVER reach the browser. Use Next.js Server Components, Route Handlers, or other server-side code paths. For browser-callable scenarios, use the `apim` mode and put your APIM in front of the workspace.

## Installation

```bash
pnpm add @inferagraph/log-analytics-datasource @inferagraph/core
```

`@azure/monitor-query` and `@azure/identity` are bundled as direct dependencies.

## Usage

The datasource is configured with three things:

1. `auth` — how to talk to Log Analytics
2. `queries` — KQL strings (or builder functions) per operation
3. `mapping` — which result columns become `id`, `sourceId`, `targetId`, `type`, etc.

### App registration (server-side)

```typescript
import { LogAnalyticsDatasource } from '@inferagraph/log-analytics-datasource';

const datasource = new LogAnalyticsDatasource({
  workspaceId: '00000000-0000-0000-0000-000000000000',
  workspaceName: 'graph-prod',
  auth: {
    kind: 'app-registration',
    tenantId: process.env.AZURE_TENANT_ID!,
    clientId: process.env.AZURE_CLIENT_ID!,
    clientSecret: process.env.AZURE_CLIENT_SECRET!,
  },
  queries: {
    nodes: 'GraphNodes_CL | project id=node_id, type=node_type, name',
    edges:
      'GraphEdges_CL | project edge_id, source=source_id, target=target_id, rel=rel_type',
    search: (ctx) =>
      `GraphNodes_CL | where name contains '${ctx.params.query}' | project id=node_id, type=node_type, name`,
  },
  mapping: {
    nodes: { idColumn: 'id', typeColumn: 'type' },
    edges: {
      idColumn: 'edge_id',
      sourceColumn: 'source',
      targetColumn: 'target',
      typeColumn: 'rel',
    },
  },
  timespan: { duration: 'P30D' },
});

await datasource.connect();
const view = await datasource.getInitialView();
await datasource.disconnect();
```

> Server-side only. Do not import this from a `'use client'` component — secrets must not ship to the browser.

### Managed identity (Azure-hosted)

```typescript
const datasource = new LogAnalyticsDatasource({
  workspaceId: process.env.AZURE_LA_WORKSPACE_ID!,
  workspaceName: 'graph-prod',
  auth: { kind: 'managed-identity' },
  queries: { /* …same as above… */ },
  mapping: { /* …same as above… */ },
});
```

> Server-side only. The managed identity is bound to the Azure host and cannot be used from a browser.

### APIM (HTTP via fetch)

```typescript
const datasource = new LogAnalyticsDatasource({
  workspaceId: process.env.AZURE_LA_WORKSPACE_ID!,
  workspaceName: 'graph-prod',
  auth: {
    kind: 'apim',
    endpoint: 'https://api.example.com/log-analytics',
    headers: { 'Ocp-Apim-Subscription-Key': process.env.APIM_KEY! },
    // Optional: customize per-op routing or body shape
    buildRequest: (op, kql, ctx) => ({
      url: `https://api.example.com/log-analytics/${op}`,
      body: { workspaceId: ctx.workspaceId, query: kql },
    }),
  },
  queries: { /* …same as above… */ },
  mapping: { /* …same as above… */ },
});
```

The default APIM request is `POST {endpoint}` with body `{ workspaceId, query, op }`. Override `body` and/or `url` via `buildRequest`. The executor accepts both response shapes:

- `{ rows: [{ ... }] }` (already row-objects)
- `{ tables: [{ columns: [{name, ...}], rows: [[...], ...] }] }` (Log Analytics REST shape)

## Configuration reference

### `LogAnalyticsDatasourceConfig`

| Field | Type | Description |
|---|---|---|
| `workspaceId` | `string` | Log Analytics workspace GUID |
| `workspaceName` | `string` | Human label (used in error messages) |
| `auth` | `LogAnalyticsAuth` | Discriminated union — see auth modes above |
| `queries` | `LogAnalyticsQueryConfig` | Per-op KQL or `(ctx) => kql` |
| `mapping` | `LogAnalyticsMapping` | Column-name → `id`/`source`/`target`/`type` mapping |
| `timespan` | `{ duration: string }` | ISO 8601 duration. Default `'P1D'`. |

### `LogAnalyticsQueryConfig`

| Field | Required | Fallback if omitted |
|---|---|---|
| `nodes` | yes | — |
| `edges` | yes | — |
| `node` | no | filter `nodes` results in memory by id |
| `neighbors` | no | run `nodes` + `edges`, BFS in memory |
| `search` | no | `search()` throws |
| `filter` | no | `filter()` throws |
| `content` | no | `getContent()` returns `undefined` |

### `LogAnalyticsMapping`

| Section | Required | Notes |
|---|---|---|
| `nodes.idColumn` | yes | Column whose value becomes `NodeData.id` |
| `nodes.typeColumn` | no | If set, value is exposed as `attributes.type` |
| `edges.{idColumn, sourceColumn, targetColumn, typeColumn}` | yes | All four required |
| `content.{idColumn, bodyColumn}` | yes (when `queries.content` set) | — |
| `content.contentTypeColumn` | no | Default content type is `'text'` |

## Behavior notes

- `getInitialView` runs the `nodes` and `edges` queries, slices nodes to `limit`, then keeps only edges whose source AND target are in the returned node set.
- `getNeighbors` uses `queries.neighbors` if configured; otherwise falls back to in-memory BFS over `queries.nodes` + `queries.edges`. The `depth` parameter limits traversal.
- `findPath` is always an in-memory BFS over `queries.nodes` + `queries.edges` (KQL has no practical native path-finding for arbitrary graphs).
- All row attributes are preserved on `NodeData.attributes` / `EdgeData.attributes` (mirrors the CosmosDB datasource — the host application decides what's relevant).
- Pagination is applied in memory after row mapping.

## License

MIT
