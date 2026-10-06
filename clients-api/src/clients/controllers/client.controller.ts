import { Controller, Get, Param } from '@nestjs/common';
import type { Client } from '../types/client.interface.js';
import { ClientsService } from '../application/clients.service.js';

@Controller('clients')
export class ClientsController {
  constructor(private readonly clientsService: ClientsService) {}

  @Get()
  getClientes(): Client[] {
    return this.clientsService.getAll();
  }

  @Get(':clientId')
  getCliente(@Param('clientId') clientId: string): Client {
    return this.clientsService.getById(clientId);
  }
}
