import { describe, it } from 'vitest';
import { Test } from '@nestjs/testing';
import request from 'supertest';
import { INestApplication } from '@nestjs/common';
import { App } from 'supertest/types.js';
import { AppModule } from '../src/app.module.js';

describe('Clients API (e2e)', () => {
  let app: INestApplication<App>;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.init();
  });

  afterAll(async () => {
    await app.close();
  });

  it('GET /clients/:clientId returns an existing client', () => {
    return request(app.getHttpServer())
      .get('/clients/cli-0001')
      .expect(200)
      .expect({
        id: 'CLI-0001',
        name: 'Distribuidora Central',
        market: 'MX',
        status: 'ACTIVE',
        segment: 'WHOLESALE',
        taxRegime: 'GENERAL',
        createdAt: '2024-01-15T10:00:00.000Z',
      });
  });

  it('GET /clients/:clientId returns the error contract on 404', () => {
    return request(app.getHttpServer())
      .get('/clients/CLI-9999')
      .expect(404)
      .expect({ code: 404, message: 'client not found' });
  });

  it('GET /health returns { status: UP }', () => {
    return request(app.getHttpServer())
      .get('/health')
      .expect(200)
      .expect({ status: 'UP' });
  });
});
