import { describe, expect, it } from 'vitest';
import { MemoryClientsRepository } from './memory-clients.repository.js';

describe('MemoryClientsRepository', () => {
  const repository = new MemoryClientsRepository();

  it('seeds at least 6 clients', () => {
    expect(repository.getAll().length).toBeGreaterThanOrEqual(6);
  });

  it('seeds clients distributed across the three markets', () => {
    const markets = new Set(repository.getAll().map((client) => client.market));
    expect(markets).toEqual(new Set(['MX', 'CO', 'PE']));
  });

  it('finds a client by id', () => {
    const client = repository.getById('CLI-0001');
    expect(client).toMatchObject({
      id: 'CLI-0001',
      market: 'MX',
      name: 'Distribuidora Central',
    });
  });

  it('returns undefined for an unknown client id', () => {
    expect(repository.getById('CLI-404')).toBeUndefined();
  });
});
