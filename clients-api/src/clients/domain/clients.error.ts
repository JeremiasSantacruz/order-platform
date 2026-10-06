export abstract class DomainError extends Error {
  abstract readonly status: number;

  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class InvalidClientIdError extends DomainError {
  readonly status = 400;

  constructor() {
    super('client id is required');
  }
}

export class ClientNotFoundError extends DomainError {
  readonly status = 404;

  constructor() {
    super('client not found');
  }
}
