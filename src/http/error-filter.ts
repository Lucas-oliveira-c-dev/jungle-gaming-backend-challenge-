import { Catch, HttpException, type ArgumentsHost, type ExceptionFilter } from '@nestjs/common';
import type { Request, Response } from 'express';
import {
  ApplicationError,
  IdempotencyConflictError,
  NotFoundError,
  TransientInfrastructureError,
  ValidationError,
  WalletAlreadyExistsError,
  WalletNotFoundError,
} from '../application/errors';
import type { AppLogger } from '../application/ports';

/**
 * One status per situation, identical on every endpoint:
 *   400 invalid payload · 404 not found · 409 idempotency/uniqueness conflict
 *   422 business rejection · 503 transient infrastructure failure (Retry-After) · 500 bug
 * (202 "accepted, pending" and 422 for persisted REJECTED transactions are produced by the
 *  controller from the transaction status.)
 */
const STATUS = new Map<Function, number>([
  [ValidationError, 400],
  [NotFoundError, 404],
  [IdempotencyConflictError, 409],
  [WalletAlreadyExistsError, 409],
  [WalletNotFoundError, 422],
  [TransientInfrastructureError, 503],
]);

@Catch()
export class ErrorFilter implements ExceptionFilter {
  constructor(private readonly logger: AppLogger) {}

  catch(error: unknown, host: ArgumentsHost): void {
    const http = host.switchToHttp();
    const response = http.getResponse<Response>();
    const request = http.getRequest<Request & { correlationId?: string }>();

    if (error instanceof ApplicationError) {
      const status = STATUS.get(error.constructor) ?? 500;
      if (status === 503) response.setHeader('Retry-After', '1');
      response.status(status).json({
        error: { code: error.code, message: error.message, retryable: status === 503, ...(error.details ? { details: error.details } : {}) },
        correlationId: request.correlationId,
      });
      return;
    }
    if (error instanceof HttpException) {
      const status = error.getStatus();
      response.status(status).json({
        error: { code: status === 404 ? 'NOT_FOUND' : 'INVALID_PAYLOAD', message: error.message, retryable: false },
        correlationId: request.correlationId,
      });
      return;
    }
    this.logger.error({ correlationId: request.correlationId, err: (error as Error)?.message, stack: (error as Error)?.stack }, 'unhandled error');
    response.status(500).json({
      error: { code: 'INTERNAL_ERROR', message: 'Unexpected error', retryable: true },
      correlationId: request.correlationId,
    });
  }
}
