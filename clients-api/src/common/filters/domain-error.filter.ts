import { ArgumentsHost, Catch, ExceptionFilter } from '@nestjs/common';
import type { Response } from 'express';
import { DomainError } from '../../clients/domain/clients.error.js';
import type { ErrorResponse } from '../../clients/types/error.interface.js';

@Catch(DomainError)
export class DomainErrorFilter implements ExceptionFilter {
  catch(error: DomainError, host: ArgumentsHost) {
    const response = host.switchToHttp().getResponse<Response>();
    const body: ErrorResponse = { code: error.status, message: error.message };
    response.status(error.status).json(body);
  }
}
