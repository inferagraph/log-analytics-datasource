import type { NodeData, EdgeData, ContentData } from '@inferagraph/core';
import type { LogAnalyticsMapping } from './types.js';

/**
 * Turn a KQL result row into a `NodeData`. The id-column value becomes
 * `id`; ALL row columns (including the id column itself) are preserved on
 * `attributes`, mirroring the `CosmosDbDatasource` "preserve everything"
 * behavior. If `typeColumn` is configured, its value is also exposed as
 * `attributes.type`.
 */
export function rowToNode(
  row: Record<string, unknown>,
  mapping: LogAnalyticsMapping['nodes'],
): NodeData {
  const id = String(row[mapping.idColumn]);
  const attributes: Record<string, unknown> = { ...row };
  if (mapping.typeColumn) {
    attributes.type = row[mapping.typeColumn];
  }
  return { id, attributes };
}

/**
 * Turn a KQL result row into an `EdgeData`. id/source/target/type are
 * pulled from named columns; everything else (including the named columns
 * themselves) is preserved on `attributes`.
 */
export function rowToEdge(
  row: Record<string, unknown>,
  mapping: LogAnalyticsMapping['edges'],
): EdgeData {
  const id = String(row[mapping.idColumn]);
  const sourceId = String(row[mapping.sourceColumn]);
  const targetId = String(row[mapping.targetColumn]);
  const type = String(row[mapping.typeColumn] ?? '');
  return {
    id,
    sourceId,
    targetId,
    attributes: { ...row, type },
  };
}

/**
 * Turn a KQL result row into a `ContentData`. The body is coerced to a
 * string via `String(...)`; the content type defaults to `'text'` if
 * `contentTypeColumn` is omitted or its value is missing.
 */
export function rowToContent(
  row: Record<string, unknown>,
  mapping: NonNullable<LogAnalyticsMapping['content']>,
): ContentData {
  const nodeId = String(row[mapping.idColumn]);
  const content = String(row[mapping.bodyColumn]);
  const ct =
    mapping.contentTypeColumn !== undefined
      ? row[mapping.contentTypeColumn]
      : undefined;
  return {
    nodeId,
    content,
    contentType: typeof ct === 'string' && ct.length > 0 ? ct : 'text',
    metadata: { ...row },
  };
}
