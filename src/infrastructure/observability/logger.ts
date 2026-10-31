import pino from 'pino';
import type { LoggerService } from '@nestjs/common';
import type { AppLogger } from '../../application/ports';

/**
 * JSON logs on stdout. Redaction is a safety net: callers already avoid logging payloads,
 * but money and idempotency material is censored if it ever slips into a log call.
 */
export function createLogger(instanceId: string, level = 'info'): pino.Logger & AppLogger {
  return pino({
    level,
    base: { service: 'wagering-processor', instanceId },
    timestamp: pino.stdTimeFunctions.isoTime,
    formatters: { level: (label) => ({ level: label }) },
    redact: {
      paths: ['money', '*.money', 'balance', '*.balance', 'payload', '*.payload', 'body', 'idempotencyKey', 'authorization', 'headers.authorization'],
      censor: '[redacted]',
    },
  });
}

/** Routes NestJS framework logs through the same JSON logger. */
export class NestPinoLogger implements LoggerService {
  constructor(private readonly logger: pino.Logger) {}

  log(message: unknown, context?: string) {
    this.logger.info({ context }, String(message));
  }
  error(message: unknown, trace?: string, context?: string) {
    this.logger.error({ context, trace }, String(message));
  }
  warn(message: unknown, context?: string) {
    this.logger.warn({ context }, String(message));
  }
  debug(message: unknown, context?: string) {
    this.logger.debug({ context }, String(message));
  }
  verbose(message: unknown, context?: string) {
    this.logger.trace({ context }, String(message));
  }
}
