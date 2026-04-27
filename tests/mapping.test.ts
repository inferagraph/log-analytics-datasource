import { describe, it, expect } from 'vitest';
import { rowToNode, rowToEdge, rowToContent } from '../src/mapping.js';

describe('rowToNode', () => {
  it('builds a NodeData from row + idColumn (no typeColumn)', () => {
    const row = { node_id: 'n1', name: 'Adam', age: 930 };
    const node = rowToNode(row, { idColumn: 'node_id' });
    expect(node.id).toBe('n1');
    // attributes preserve everything (mirrors CosmosDB behavior)
    expect(node.attributes.node_id).toBe('n1');
    expect(node.attributes.name).toBe('Adam');
    expect(node.attributes.age).toBe(930);
    // No `type` attribute injected when typeColumn is absent
    expect(node.attributes.type).toBeUndefined();
  });

  it('injects type attribute when typeColumn is set', () => {
    const row = { node_id: 'n1', kind: 'person', name: 'Adam' };
    const node = rowToNode(row, { idColumn: 'node_id', typeColumn: 'kind' });
    expect(node.id).toBe('n1');
    expect(node.attributes.type).toBe('person');
    // The original `kind` column is still preserved
    expect(node.attributes.kind).toBe('person');
  });

  it('coerces non-string ids via String(...)', () => {
    const row = { node_id: 42, name: 'forty-two' };
    const node = rowToNode(row, { idColumn: 'node_id' });
    expect(node.id).toBe('42');
  });
});

describe('rowToEdge', () => {
  it('builds an EdgeData from id/source/target/type columns', () => {
    const row = {
      edge_id: 'e1',
      from: 'n1',
      to: 'n2',
      rel: 'father_of',
      weight: 0.9,
    };
    const edge = rowToEdge(row, {
      idColumn: 'edge_id',
      sourceColumn: 'from',
      targetColumn: 'to',
      typeColumn: 'rel',
    });
    expect(edge.id).toBe('e1');
    expect(edge.sourceId).toBe('n1');
    expect(edge.targetId).toBe('n2');
    expect(edge.attributes.type).toBe('father_of');
    expect(edge.attributes.weight).toBe(0.9);
  });

  it('coerces missing type to empty string', () => {
    const row = { edge_id: 'e1', from: 'n1', to: 'n2', rel: undefined };
    const edge = rowToEdge(row, {
      idColumn: 'edge_id',
      sourceColumn: 'from',
      targetColumn: 'to',
      typeColumn: 'rel',
    });
    expect(edge.attributes.type).toBe('');
  });
});

describe('rowToContent', () => {
  it('uses default contentType "text" when no contentTypeColumn is set', () => {
    const row = { node_id: 'n1', body: 'Hello world.' };
    const content = rowToContent(row, {
      idColumn: 'node_id',
      bodyColumn: 'body',
    });
    expect(content.nodeId).toBe('n1');
    expect(content.content).toBe('Hello world.');
    expect(content.contentType).toBe('text');
    expect(content.metadata).toEqual(row);
  });

  it('uses explicit contentTypeColumn when set', () => {
    const row = { node_id: 'n1', body: '# hi', mime: 'markdown' };
    const content = rowToContent(row, {
      idColumn: 'node_id',
      bodyColumn: 'body',
      contentTypeColumn: 'mime',
    });
    expect(content.contentType).toBe('markdown');
  });

  it('falls back to "text" when contentTypeColumn value is empty', () => {
    const row = { node_id: 'n1', body: 'x', mime: '' };
    const content = rowToContent(row, {
      idColumn: 'node_id',
      bodyColumn: 'body',
      contentTypeColumn: 'mime',
    });
    expect(content.contentType).toBe('text');
  });

  it('coerces non-string body to string', () => {
    const row = { node_id: 'n1', body: 42 };
    const content = rowToContent(row, {
      idColumn: 'node_id',
      bodyColumn: 'body',
    });
    expect(content.content).toBe('42');
  });
});
