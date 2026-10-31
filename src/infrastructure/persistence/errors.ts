import {
  ConnectionException,
  DeadlockException,
  LockWaitTimeoutException,
  UniqueConstraintViolationException,
} from '@mikro-orm/core';

/** Errors worth retrying as-is: the same input may succeed a moment later. */
const TRANSIENT_PG_CODES = new Set([
  '40001', // serialization_failure
  '40P01', // deadlock_detected
  '55P03', // lock_not_available (lock_timeout)
  '57014', // query_canceled (statement_timeout)
  '57P01', // admin_shutdown
  '57P02', // crash_shutdown
  '57P03', // cannot_connect_now
  '53300', // too_many_connections
]);

const TRANSIENT_NODE_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN']);

function codeOf(error: unknown): string | undefined {
  const e = error as { code?: unknown; cause?: unknown };
  if (typeof e?.code === 'string') return e.code;
  return e?.cause ? codeOf(e.cause) : undefined;
}

export function isUniqueViolation(error: unknown): boolean {
  return error instanceof UniqueConstraintViolationException || codeOf(error) === '23505';
}

export function isLockConflict(error: unknown): boolean {
  const code = codeOf(error);
  return error instanceof LockWaitTimeoutException || error instanceof DeadlockException || code === '55P03' || code === '40P01';
}

export function isTransientInfrastructureError(error: unknown): boolean {
  if (error instanceof ConnectionException || isLockConflict(error)) return true;
  const code = codeOf(error);
  if (!code) {
    const message = String((error as Error)?.message ?? '');
    return /timeout|connection terminated|ECONNREFUSED|Connection terminated/i.test(message);
  }
  return TRANSIENT_PG_CODES.has(code) || TRANSIENT_NODE_CODES.has(code) || code.startsWith('08');
}
