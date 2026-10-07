import { Client } from '../types/client.interface.js';
import { ClientsRepository } from '../domain/clients.repository.js';

export class MemoryClientsRepository implements ClientsRepository {
  private readonly seed: Client[] = [
    // Mercado MX (México)
    {
      clientId: 'CLI-0001',
      name: 'Distribuidora Central',
      market: 'MX',
      status: 'ACTIVE',
      segment: 'WHOLESALE',
      taxRegime: 'GENERAL',
    },
    {
      clientId: 'CLI-0002',
      name: 'Comercializadora Norte',
      market: 'MX',
      status: 'ACTIVE',
      segment: 'RETAIL',
      taxRegime: 'SIMPLIFIED',
    },
    {
      clientId: 'CLI-0003',
      name: 'Mercados Regionales',
      market: 'MX',
      status: 'BLOCKED',
      segment: 'WHOLESALE',
      taxRegime: 'GENERAL',
    },
    // Cliente del ejemplo de la sección 5.A (evento orders.created.v1).
    {
      clientId: 'CLI-99821',
      name: 'Distribuidora Confiable',
      market: 'MX',
      status: 'ACTIVE',
      segment: 'WHOLESALE',
      taxRegime: 'GENERAL',
    },

    // Mercado CO (Colombia)
    {
      clientId: 'CLI-0004',
      name: 'Alimentos Andinos',
      market: 'CO',
      status: 'ACTIVE',
      segment: 'WHOLESALE',
      taxRegime: 'SIMPLIFIED',
    },
    {
      clientId: 'CLI-0005',
      name: 'Tiendas del Valle',
      market: 'CO',
      status: 'ACTIVE',
      segment: 'RETAIL',
      taxRegime: 'GENERAL',
    },
    {
      clientId: 'CLI-0006',
      name: 'Exportadora Caribe',
      market: 'CO',
      status: 'BLOCKED',
      segment: 'WHOLESALE',
      taxRegime: 'EXEMPT',
    },

    // Mercado PE (Perú)
    {
      clientId: 'CLI-0007',
      name: 'Distribuciones Lima',
      market: 'PE',
      status: 'ACTIVE',
      segment: 'WHOLESALE',
      taxRegime: 'GENERAL',
    },
    {
      clientId: 'CLI-0008',
      name: 'Inversiones Cusco',
      market: 'PE',
      status: 'ACTIVE',
      segment: 'RETAIL',
      taxRegime: 'SIMPLIFIED',
    },
    {
      clientId: 'CLI-0009',
      name: 'Redes del Pacífico',
      market: 'PE',
      status: 'ACTIVE',
      segment: 'WHOLESALE',
      taxRegime: 'EXEMPT',
    },
  ];

  getAll(): Client[] {
    return [...this.seed];
  }

  getById(id: string): Client | undefined {
    return this.seed.find((client) => client.clientId === id);
  }
}
