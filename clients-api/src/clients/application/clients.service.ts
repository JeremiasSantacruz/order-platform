import { Inject, Injectable } from '@nestjs/common';
import { Client } from '../types/client.interface.js';
import {
  CLIENTS_REPOSITORY,
  type ClientsRepository,
} from '../domain/clients.repository.js';
import {
  ClientNotFoundError,
  InvalidClientIdError,
} from '../domain/clients.error.js';

@Injectable()
export class ClientsService {
  constructor(
    @Inject(CLIENTS_REPOSITORY) private readonly repository: ClientsRepository,
  ) {}

  getAll(): Client[] {
    return this.repository.getAll();
  }

  getById(rawClientId: string): Client {
    const id = rawClientId.trim().toUpperCase();

    if (id === '') {
      throw new InvalidClientIdError();
    }

    const client = this.repository.getById(id);
    if (client === undefined) {
      throw new ClientNotFoundError();
    }

    return client;
  }
}
