import { Client } from '../types/client.interface.js';
import { ClientsRepository } from '../domain/clients.repository.js';

export class MemoryClientsRepository implements ClientsRepository {
  private readonly seed: Client[] = [
    // Mercado MX (México)
    {
      id: 'CLI-0001',
      name: 'Distribuidora Central',
      market: 'MX',
      status: 'ACTIVE',
      segment: 'WHOLESALE',
      taxRegime: 'GENERAL',
      createdAt: new Date('2024-01-15T10:00:00Z'),
    },
    {
      id: 'CLI-0002',
      name: 'Comercializadora Norte',
      market: 'MX',
      status: 'ACTIVE',
      segment: 'RETAIL',
      taxRegime: 'SIMPLIFIED',
      createdAt: new Date('2024-03-22T10:00:00Z'),
    },
    {
      id: 'CLI-0003',
      name: 'Mercados Regionales',
      market: 'MX',
      status: 'BLOCKED',
      segment: 'WHOLESALE',
      taxRegime: 'GENERAL',
      createdAt: new Date('2024-06-10T10:00:00Z'),
    },

    // Mercado CO (Colombia)
    {
      id: 'CLI-0004',
      name: 'Alimentos Andinos',
      market: 'CO',
      status: 'ACTIVE',
      segment: 'WHOLESALE',
      taxRegime: 'SIMPLIFIED',
      createdAt: new Date('2024-02-08T10:00:00Z'),
    },
    {
      id: 'CLI-0005',
      name: 'Tiendas del Valle',
      market: 'CO',
      status: 'ACTIVE',
      segment: 'RETAIL',
      taxRegime: 'GENERAL',
      createdAt: new Date('2024-05-19T10:00:00Z'),
    },
    {
      id: 'CLI-0006',
      name: 'Exportadora Caribe',
      market: 'CO',
      status: 'BLOCKED',
      segment: 'WHOLESALE',
      taxRegime: 'EXEMPT',
      createdAt: new Date('2024-08-30T10:00:00Z'),
    },

    // Mercado PE (Perú)
    {
      id: 'CLI-0007',
      name: 'Distribuciones Lima',
      market: 'PE',
      status: 'ACTIVE',
      segment: 'WHOLESALE',
      taxRegime: 'GENERAL',
      createdAt: new Date('2024-04-12T10:00:00Z'),
    },
    {
      id: 'CLI-0008',
      name: 'Inversiones Cusco',
      market: 'PE',
      status: 'ACTIVE',
      segment: 'RETAIL',
      taxRegime: 'SIMPLIFIED',
      createdAt: new Date('2024-07-07T10:00:00Z'),
    },
    {
      id: 'CLI-0009',
      name: 'Redes del Pacífico',
      market: 'PE',
      status: 'ACTIVE',
      segment: 'WHOLESALE',
      taxRegime: 'EXEMPT',
      createdAt: new Date('2024-09-25T10:00:00Z'),
    },
  ];

  getAll(): Client[] {
    return [...this.seed];
  }

  getById(id: string): Client | undefined {
    return this.seed.find((client) => client.id === id);
  }
}
