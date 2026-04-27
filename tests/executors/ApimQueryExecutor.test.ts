import { describe, it, expect, beforeEach, vi } from 'vitest';
import { ApimQueryExecutor } from '../../src/executors/ApimQueryExecutor.js';

function jsonResponse(body: unknown, init: { status?: number; statusText?: string } = {}) {
  const status = init.status ?? 200;
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: init.statusText ?? 'OK',
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(typeof body === 'string' ? body : JSON.stringify(body)),
  } as unknown as Response;
}

describe('ApimQueryExecutor', () => {
  let fetchMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    fetchMock = vi.fn();
    globalThis.fetch = fetchMock as unknown as typeof globalThis.fetch;
  });

  it('rejects non-apim auth at construction', () => {
    expect(
      () =>
        new ApimQueryExecutor({ kind: 'managed-identity' }),
    ).toThrow(/only supports auth.kind='apim'/);
  });

  it('connect() validates the endpoint URL', async () => {
    const exec = new ApimQueryExecutor({
      kind: 'apim',
      endpoint: 'not-a-url',
    });
    await expect(exec.connect()).rejects.toThrow(/invalid endpoint URL/);
  });

  it('isConnected reflects state', async () => {
    const exec = new ApimQueryExecutor({
      kind: 'apim',
      endpoint: 'https://apim.example.com/la',
    });
    expect(exec.isConnected()).toBe(false);
    await exec.connect();
    expect(exec.isConnected()).toBe(true);
    await exec.disconnect();
    expect(exec.isConnected()).toBe(false);
  });

  it('run() throws when not connected', async () => {
    const exec = new ApimQueryExecutor({
      kind: 'apim',
      endpoint: 'https://apim.example.com/la',
    });
    await expect(
      exec.run('initial', 'Nodes', { workspaceId: 'w', params: {} }),
    ).rejects.toThrow(/not connected/);
  });

  it('default request: POST endpoint with body { workspaceId, query, op } and Content-Type JSON', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse({ rows: [] }));
    const exec = new ApimQueryExecutor({
      kind: 'apim',
      endpoint: 'https://apim.example.com/la',
      headers: { 'Ocp-Apim-Subscription-Key': 'k1' },
    });
    await exec.connect();
    await exec.run('initial', 'Nodes | take 5', {
      workspaceId: 'wkspace-1',
      params: {},
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe('https://apim.example.com/la');
    expect(init.method).toBe('POST');
    expect(init.headers['Content-Type']).toBe('application/json');
    expect(init.headers['Ocp-Apim-Subscription-Key']).toBe('k1');
    expect(JSON.parse(init.body)).toEqual({
      workspaceId: 'wkspace-1',
      query: 'Nodes | take 5',
      op: 'initial',
    });
  });

  it('buildRequest can override body', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse({ rows: [] }));
    const exec = new ApimQueryExecutor({
      kind: 'apim',
      endpoint: 'https://apim.example.com/la',
      buildRequest: (op, kql, ctx) => ({
        body: { customOp: op, customKql: kql, customWorkspace: ctx.workspaceId },
      }),
    });
    await exec.connect();
    await exec.run('search', 'Nodes', { workspaceId: 'w1', params: {} });
    const [, init] = fetchMock.mock.calls[0];
    expect(JSON.parse(init.body)).toEqual({
      customOp: 'search',
      customKql: 'Nodes',
      customWorkspace: 'w1',
    });
  });

  it('buildRequest can override url', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse({ rows: [] }));
    const exec = new ApimQueryExecutor({
      kind: 'apim',
      endpoint: 'https://apim.example.com/la',
      buildRequest: (op) => ({
        url: `https://apim.example.com/la/${op}`,
      }),
    });
    await exec.connect();
    await exec.run('node', 'Nodes', { workspaceId: 'w', params: {} });
    const [url] = fetchMock.mock.calls[0];
    expect(url).toBe('https://apim.example.com/la/node');
  });

  it('flattens response shape { rows: [...] }', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse({
        rows: [
          { id: 'n1', name: 'Adam' },
          { id: 'n2', name: 'Eve' },
        ],
      }),
    );
    const exec = new ApimQueryExecutor({
      kind: 'apim',
      endpoint: 'https://apim.example.com/la',
    });
    await exec.connect();
    const rows = await exec.run('initial', 'Nodes', {
      workspaceId: 'w',
      params: {},
    });
    expect(rows).toEqual([
      { id: 'n1', name: 'Adam' },
      { id: 'n2', name: 'Eve' },
    ]);
  });

  it('flattens response shape { tables: [{ columns, rows }] }', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse({
        tables: [
          {
            columns: [{ name: 'id' }, { name: 'name' }],
            rows: [
              ['n1', 'Adam'],
              ['n2', 'Eve'],
            ],
          },
        ],
      }),
    );
    const exec = new ApimQueryExecutor({
      kind: 'apim',
      endpoint: 'https://apim.example.com/la',
    });
    await exec.connect();
    const rows = await exec.run('initial', 'Nodes', {
      workspaceId: 'w',
      params: {},
    });
    expect(rows).toEqual([
      { id: 'n1', name: 'Adam' },
      { id: 'n2', name: 'Eve' },
    ]);
  });

  it('returns [] for unrecognized response shape', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse({ unexpected: true }));
    const exec = new ApimQueryExecutor({
      kind: 'apim',
      endpoint: 'https://apim.example.com/la',
    });
    await exec.connect();
    const rows = await exec.run('initial', 'Nodes', {
      workspaceId: 'w',
      params: {},
    });
    expect(rows).toEqual([]);
  });

  it('non-2xx response throws with status info', async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse('Bad query', { status: 400, statusText: 'Bad Request' }),
    );
    const exec = new ApimQueryExecutor({
      kind: 'apim',
      endpoint: 'https://apim.example.com/la',
    });
    await exec.connect();
    await expect(
      exec.run('initial', 'broken', { workspaceId: 'w', params: {} }),
    ).rejects.toThrow(/400 Bad Request/);
  });
});
