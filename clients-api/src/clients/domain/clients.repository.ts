import { Client } from '../types/client.interface.js';

export const CLIENTS_REPOSITORY = Symbol('CLIENTS_REPOSITORY');

export interface ClientsRepository {
  getAll(): Client[];
  getById(id: string): Client | undefined;
}
