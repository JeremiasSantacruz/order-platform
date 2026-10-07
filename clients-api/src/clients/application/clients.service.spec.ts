import { describe, expect, it } from 'vitest';
import { ClientsService } from './clients.service.js';
import { MemoryClientsRepository } from '../infrastructure/memory-clients.repository.js';
import {
  ClientNotFoundError,
  InvalidClientIdError,
} from '../domain/clients.error.js';

describe('ClientsService', () => {
  const service = new ClientsService(new MemoryClientsRepository());

  describe('getById', () => {
    it('normalizes and returns an existing client', () => {
      const client = service.getById('  cli-0001  ');
      expect(client).toEqual({
        clientId: 'CLI-0001',
        name: 'Distribuidora Central',
        market: 'MX',
        status: 'ACTIVE',
        segment: 'WHOLESALE',
        taxRegime: 'GENERAL',
      });
    });

    it('throws InvalidClientIdError for an empty client id', () => {
      expect(() => service.getById('   ')).toThrow(InvalidClientIdError);
    });

    it('throws ClientNotFoundError for an unknown client id', () => {
      expect(() => service.getById('CLI-9999')).toThrow(ClientNotFoundError);
    });
  });

  describe('getAll', () => {
    it('returns all seeded clients', () => {
      expect(service.getAll()).toHaveLength(10);
    });

    it('returns copies so callers cannot mutate the store', () => {
      const all = service.getAll();
      all.pop();
      expect(service.getAll()).toHaveLength(10);
    });
  });
});
