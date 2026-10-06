import { Controller, Get } from '@nestjs/common';

@Controller('health')
export class HealthController {
  @Get()
  getStatus(): { status: string } {
    return { status: 'UP' };
  }
}
