import { Module } from '@nestjs/common';
import { APP_FILTER } from '@nestjs/core';
import { ClientsService } from './application/clients.service.js';
import { ClientsController } from './controllers/client.controller.js';
import { CLIENTS_REPOSITORY } from './domain/clients.repository.js';
import { MemoryClientsRepository } from './infrastructure/memory-clients.repository.js';
import { DomainErrorFilter } from '../common/filters/domain-error.filter.js';

@Module({
  controllers: [ClientsController],
  providers: [
    ClientsService,
    { provide: CLIENTS_REPOSITORY, useClass: MemoryClientsRepository },
    { provide: APP_FILTER, useClass: DomainErrorFilter },
  ],
  exports: [ClientsService],
})
export class ClientsModule {}
