import { Datasource } from '@inferagraph/core';
import type {
  DataAdapterConfig,
  GraphData,
  NodeId,
  NodeData,
  EdgeData,
  ContentData,
  PaginationOptions,
  PaginatedResult,
  DataFilter,
} from '@inferagraph/core';
import type {
  LogAnalyticsDatasourceConfig,
  LogAnalyticsQueryConfig,
  LogOperation,
  LogQueryContext,
} from './types.js';
import type { QueryExecutor } from './executors/QueryExecutor.js';
import { SdkQueryExecutor } from './executors/SdkQueryExecutor.js';
import { ApimQueryExecutor } from './executors/ApimQueryExecutor.js';
import { rowToNode, rowToEdge, rowToContent } from './mapping.js';

/**
 * Datasource that reads graph data from an Azure Log Analytics workspace
 * using KQL. Supports three auth modes via the `auth` discriminator:
 * `app-registration`, `managed-identity` (both via `@azure/monitor-query`),
 * and `apim` (HTTP via `fetch`).
 *
 * Domain-agnostic: row→graph mapping is driven entirely by `mapping`
 * column names; nothing in here knows about the hosting application's
 * schema.
 */
export class LogAnalyticsDatasource extends Datasource {
  readonly name = 'log-analytics';
  private readonly config: LogAnalyticsDatasourceConfig;
  private readonly executor: QueryExecutor;

  constructor(config: LogAnalyticsDatasourceConfig) {
    super();
    this.config = config;
    this.executor = LogAnalyticsDatasource.createExecutor(config);
  }

  static createExecutor(config: LogAnalyticsDatasourceConfig): QueryExecutor {
    switch (config.auth.kind) {
      case 'app-registration':
      case 'managed-identity':
        return new SdkQueryExecutor(config.auth, config.timespan);
      case 'apim':
        return new ApimQueryExecutor(config.auth);
      default: {
        // Exhaustiveness guard
        const exhaustive: never = config.auth;
        throw new Error(
          `Unsupported auth.kind: ${(exhaustive as { kind: string }).kind}`,
        );
      }
    }
  }

  async connect(): Promise<void> {
    await this.executor.connect();
  }

  async disconnect(): Promise<void> {
    await this.executor.disconnect();
  }

  isConnected(): boolean {
    return this.executor.isConnected();
  }

  async getInitialView(config?: DataAdapterConfig): Promise<GraphData> {
    this.ensureConnected();
    const limit = (config?.limit as number | undefined) ?? 100;

    const params = { limit };
    const nodeRows = await this.runOp('initial', 'nodes', params);
    const edgeRows = await this.runOp('initial', 'edges', params);

    const allNodes = nodeRows.map((row) => rowToNode(row, this.config.mapping.nodes));
    const nodes = allNodes.slice(0, limit);
    const nodeIds = new Set(nodes.map((n) => n.id));

    const edges = edgeRows
      .map((row) => rowToEdge(row, this.config.mapping.edges))
      .filter((e) => nodeIds.has(e.sourceId) && nodeIds.has(e.targetId));

    return { nodes, edges };
  }

  async getNode(id: NodeId): Promise<NodeData | undefined> {
    this.ensureConnected();
    if (this.config.queries.node) {
      const rows = await this.runOp('node', 'node', { id });
      if (rows.length === 0) return undefined;
      return rowToNode(rows[0], this.config.mapping.nodes);
    }
    // Fallback: filter `nodes` results in memory
    const rows = await this.runOp('node', 'nodes', { id });
    const idCol = this.config.mapping.nodes.idColumn;
    const match = rows.find((r) => String(r[idCol]) === id);
    if (!match) return undefined;
    return rowToNode(match, this.config.mapping.nodes);
  }

  async getNeighbors(nodeId: NodeId, depth: number = 1): Promise<GraphData> {
    this.ensureConnected();

    if (this.config.queries.neighbors) {
      const params = { id: nodeId, depth };
      const nodeRows = await this.runOp('neighbors', 'nodes', params);
      const edgeRows = await this.runOp('neighbors', 'edges', params);
      const nodes = nodeRows.map((row) =>
        rowToNode(row, this.config.mapping.nodes),
      );
      const edges = edgeRows.map((row) =>
        rowToEdge(row, this.config.mapping.edges),
      );
      return { nodes, edges };
    }

    // Fallback: in-memory BFS over nodes + edges
    const params = { id: nodeId, depth };
    const allNodeRows = await this.runOp('neighbors', 'nodes', params);
    const allEdgeRows = await this.runOp('neighbors', 'edges', params);
    const allNodes = allNodeRows.map((row) =>
      rowToNode(row, this.config.mapping.nodes),
    );
    const allEdges = allEdgeRows.map((row) =>
      rowToEdge(row, this.config.mapping.edges),
    );

    return LogAnalyticsDatasource.bfsNeighbors(allNodes, allEdges, nodeId, depth);
  }

  async findPath(fromId: NodeId, toId: NodeId): Promise<GraphData> {
    this.ensureConnected();
    const params = { fromId, toId };
    const allNodeRows = await this.runOp('initial', 'nodes', params);
    const allEdgeRows = await this.runOp('initial', 'edges', params);
    const allNodes = allNodeRows.map((row) =>
      rowToNode(row, this.config.mapping.nodes),
    );
    const allEdges = allEdgeRows.map((row) =>
      rowToEdge(row, this.config.mapping.edges),
    );

    return LogAnalyticsDatasource.bfsPath(allNodes, allEdges, fromId, toId);
  }

  async search(
    query: string,
    pagination?: PaginationOptions,
  ): Promise<PaginatedResult<NodeData>> {
    this.ensureConnected();
    if (!this.config.queries.search) {
      throw new Error(
        'LogAnalyticsDatasource.search requires queries.search to be configured.',
      );
    }
    const rows = await this.runOp('search', 'search', { query });
    const items = rows.map((row) => rowToNode(row, this.config.mapping.nodes));
    return LogAnalyticsDatasource.paginate(items, pagination);
  }

  async filter(
    filter: DataFilter,
    pagination?: PaginationOptions,
  ): Promise<PaginatedResult<NodeData>> {
    this.ensureConnected();
    if (!this.config.queries.filter) {
      throw new Error(
        'LogAnalyticsDatasource.filter requires queries.filter to be configured.',
      );
    }
    const rows = await this.runOp('filter', 'filter', { filter });
    const items = rows.map((row) => rowToNode(row, this.config.mapping.nodes));
    return LogAnalyticsDatasource.paginate(items, pagination);
  }

  async getContent(nodeId: NodeId): Promise<ContentData | undefined> {
    this.ensureConnected();
    if (!this.config.queries.content || !this.config.mapping.content) {
      return undefined;
    }
    const rows = await this.runOp('content', 'content', { id: nodeId });
    if (rows.length === 0) return undefined;
    return rowToContent(rows[0], this.config.mapping.content);
  }

  // --- Private Helpers ---

  private ensureConnected(): void {
    if (!this.executor.isConnected()) {
      throw new Error(
        `LogAnalyticsDatasource (workspace='${this.config.workspaceName}') is not connected. Call connect() first.`,
      );
    }
  }

  private resolveKql(
    queryKey: keyof LogAnalyticsQueryConfig,
    ctx: LogQueryContext,
  ): string {
    const q = this.config.queries[queryKey];
    if (q === undefined) {
      throw new Error(
        `LogAnalyticsDatasource: no query configured for '${String(queryKey)}'.`,
      );
    }
    return typeof q === 'function' ? q(ctx) : q;
  }

  private async runOp(
    op: LogOperation,
    queryKey: keyof LogAnalyticsQueryConfig,
    params: Record<string, unknown>,
  ): Promise<Record<string, unknown>[]> {
    const ctx: LogQueryContext = {
      workspaceId: this.config.workspaceId,
      params,
    };
    const kql = this.resolveKql(queryKey, ctx);
    return this.executor.run(op, kql, ctx);
  }

  static paginate<T>(
    items: T[],
    pagination?: PaginationOptions,
  ): PaginatedResult<T> {
    const total = items.length;
    if (!pagination) return { items, total, hasMore: false };
    const { offset, limit } = pagination;
    const sliced = items.slice(offset, offset + limit);
    return { items: sliced, total, hasMore: offset + limit < total };
  }

  static bfsNeighbors(
    allNodes: NodeData[],
    allEdges: EdgeData[],
    startId: NodeId,
    depth: number,
  ): GraphData {
    if (depth < 1) return { nodes: [], edges: [] };

    const nodeById = new Map(allNodes.map((n) => [n.id, n]));
    const visited = new Set<string>([startId]);
    let frontier = [startId];
    const collectedEdges: EdgeData[] = [];

    for (let d = 0; d < depth; d++) {
      const next: string[] = [];
      for (const currentId of frontier) {
        for (const edge of allEdges) {
          if (edge.sourceId === currentId || edge.targetId === currentId) {
            const neighbor =
              edge.sourceId === currentId ? edge.targetId : edge.sourceId;
            if (!visited.has(neighbor)) {
              visited.add(neighbor);
              next.push(neighbor);
            }
            // Collect edge once
            if (!collectedEdges.some((e) => e.id === edge.id)) {
              collectedEdges.push(edge);
            }
          }
        }
      }
      frontier = next;
      if (frontier.length === 0) break;
    }

    const nodes: NodeData[] = [];
    for (const id of visited) {
      const n = nodeById.get(id);
      if (n) nodes.push(n);
    }
    return { nodes, edges: collectedEdges };
  }

  static bfsPath(
    allNodes: NodeData[],
    allEdges: EdgeData[],
    fromId: NodeId,
    toId: NodeId,
  ): GraphData {
    if (fromId === toId) {
      const n = allNodes.find((x) => x.id === fromId);
      return { nodes: n ? [n] : [], edges: [] };
    }

    const visited = new Set<string>([fromId]);
    const parent = new Map<string, { nodeId: string; edge: EdgeData }>();
    let frontier = [fromId];
    let found = false;
    const maxDepth = 20;
    let depth = 0;

    while (frontier.length > 0 && !found && depth < maxDepth) {
      const next: string[] = [];
      for (const currentId of frontier) {
        for (const edge of allEdges) {
          if (edge.sourceId !== currentId && edge.targetId !== currentId) {
            continue;
          }
          const neighborId =
            edge.sourceId === currentId ? edge.targetId : edge.sourceId;
          if (visited.has(neighborId)) continue;
          visited.add(neighborId);
          parent.set(neighborId, { nodeId: currentId, edge });
          next.push(neighborId);
          if (neighborId === toId) {
            found = true;
            break;
          }
        }
        if (found) break;
      }
      frontier = next;
      depth++;
    }

    if (!found) return { nodes: [], edges: [] };

    const pathIds: string[] = [toId];
    const pathEdges: EdgeData[] = [];
    let cursor = toId;
    while (parent.has(cursor)) {
      const p = parent.get(cursor)!;
      pathIds.push(p.nodeId);
      pathEdges.push(p.edge);
      cursor = p.nodeId;
    }

    const nodeById = new Map(allNodes.map((n) => [n.id, n]));
    const nodes: NodeData[] = [];
    for (const id of pathIds) {
      const n = nodeById.get(id);
      if (n) nodes.push(n);
    }
    return { nodes, edges: pathEdges };
  }
}
