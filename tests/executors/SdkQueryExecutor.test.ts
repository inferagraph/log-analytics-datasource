import { describe, it, expect, vi, beforeEach } from 'vitest';

// --- Mocks ---
// Track constructor calls to the credential classes and the LogsQueryClient.
const clientSecretCtor = vi.fn();
const managedIdentityCtor = vi.fn();
const queryWorkspace = vi.fn();
const logsQueryClientCtor = vi.fn();

vi.mock('@azure/identity', () => {
  return {
    ClientSecretCredential: class {
      constructor(tenantId: string, clientId: string, clientSecret: string) {
        clientSecretCtor(tenantId, clientId, clientSecret);
      }
    },
    ManagedIdentityCredential: class {
      constructor() {
        managedIdentityCtor();
      }
    },
  };
});

vi.mock('@azure/monitor-query', () => {
  return {
    LogsQueryClient: class {
      constructor(credential: unknown) {
        logsQueryClientCtor(credential);
      }
      queryWorkspace = (...args: unknown[]) => queryWorkspace(...args);
    },
  };
});

import { SdkQueryExecutor } from '../../src/executors/SdkQueryExecutor.js';

describe('SdkQueryExecutor', () => {
  beforeEach(() => {
    clientSecretCtor.mockClear();
    managedIdentityCtor.mockClear();
    logsQueryClientCtor.mockClear();
    queryWorkspace.mockReset();
  });

  it('rejects unsupported auth.kind at construction', () => {
    expect(
      () =>
        new SdkQueryExecutor({
          kind: 'apim',
          endpoint: 'https://example.com',
        }),
    ).toThrow(/SdkQueryExecutor cannot handle auth.kind='apim'/);
  });

  it('connect() builds ClientSecretCredential for app-registration', async () => {
    const exec = new SdkQueryExecutor({
      kind: 'app-registration',
      tenantId: 't1',
      clientId: 'c1',
      clientSecret: 'secret',
    });
    expect(exec.isConnected()).toBe(false);
    await exec.connect();
    expect(clientSecretCtor).toHaveBeenCalledWith('t1', 'c1', 'secret');
    expect(managedIdentityCtor).not.toHaveBeenCalled();
    expect(logsQueryClientCtor).toHaveBeenCalledTimes(1);
    expect(exec.isConnected()).toBe(true);
  });

  it('connect() builds ManagedIdentityCredential for managed-identity', async () => {
    const exec = new SdkQueryExecutor({ kind: 'managed-identity' });
    await exec.connect();
    expect(managedIdentityCtor).toHaveBeenCalledTimes(1);
    expect(clientSecretCtor).not.toHaveBeenCalled();
    expect(logsQueryClientCtor).toHaveBeenCalledTimes(1);
  });

  it('run() throws when not connected', async () => {
    const exec = new SdkQueryExecutor({ kind: 'managed-identity' });
    await expect(
      exec.run('initial', 'Nodes | take 10', {
        workspaceId: 'w1',
        params: {},
      }),
    ).rejects.toThrow(/not connected/);
  });

  it('run() calls queryWorkspace with workspaceId, kql, and timespan and flattens first table', async () => {
    queryWorkspace.mockResolvedValueOnce({
      tables: [
        {
          columnDescriptors: [{ name: 'id' }, { name: 'name' }],
          rows: [
            ['n1', 'Adam'],
            ['n2', 'Eve'],
          ],
        },
      ],
    });

    const exec = new SdkQueryExecutor(
      { kind: 'managed-identity' },
      { duration: 'PT1H' },
    );
    await exec.connect();
    const rows = await exec.run('initial', 'Nodes | take 2', {
      workspaceId: 'wkspace-1',
      params: {},
    });

    expect(queryWorkspace).toHaveBeenCalledWith(
      'wkspace-1',
      'Nodes | take 2',
      { duration: 'PT1H' },
    );
    expect(rows).toEqual([
      { id: 'n1', name: 'Adam' },
      { id: 'n2', name: 'Eve' },
    ]);
  });

  it('run() defaults timespan to P1D when not configured', async () => {
    queryWorkspace.mockResolvedValueOnce({
      tables: [{ columnDescriptors: [], rows: [] }],
    });
    const exec = new SdkQueryExecutor({ kind: 'managed-identity' });
    await exec.connect();
    await exec.run('initial', 'Nodes', { workspaceId: 'w', params: {} });
    expect(queryWorkspace).toHaveBeenCalledWith('w', 'Nodes', {
      duration: 'P1D',
    });
  });

  it('run() returns [] when result has no tables', async () => {
    queryWorkspace.mockResolvedValueOnce({});
    const exec = new SdkQueryExecutor({ kind: 'managed-identity' });
    await exec.connect();
    const rows = await exec.run('initial', 'Nodes', {
      workspaceId: 'w',
      params: {},
    });
    expect(rows).toEqual([]);
  });

  it('run() returns [] when first table has empty rows', async () => {
    queryWorkspace.mockResolvedValueOnce({
      tables: [{ columnDescriptors: [{ name: 'id' }], rows: [] }],
    });
    const exec = new SdkQueryExecutor({ kind: 'managed-identity' });
    await exec.connect();
    const rows = await exec.run('initial', 'Nodes', {
      workspaceId: 'w',
      params: {},
    });
    expect(rows).toEqual([]);
  });

  it('disconnect() clears the client and isConnected reflects state', async () => {
    const exec = new SdkQueryExecutor({ kind: 'managed-identity' });
    await exec.connect();
    expect(exec.isConnected()).toBe(true);
    await exec.disconnect();
    expect(exec.isConnected()).toBe(false);
  });
});
