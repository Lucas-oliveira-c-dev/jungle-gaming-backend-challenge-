/**
 * Application-level outcomes that are not a persisted transaction result.
 * Each maps to exactly one HTTP status and one SQS handling strategy.
 */
export abstract class ApplicationError extends Error {
  abstract readonly code: string;

  constructor(
    message: string,
    readonly details?: unknown,
  ) {
    super(message);
    this.name = new.target.name;
  }
}

/** Malformed or semantically invalid input (400 / DLQ). */
export class ValidationError extends ApplicationError {
  readonly code = 'INVALID_PAYLOAD';
}

/** Same idempotency key (or provider transaction id) with a different payload (409 / DLQ). */
export class IdempotencyConflictError extends ApplicationError {
  readonly code = 'IDEMPOTENCY_CONFLICT';
}

/** Wallet already exists for player + currency (409). */
export class WalletAlreadyExistsError extends ApplicationError {
  readonly code = 'WALLET_ALREADY_EXISTS';
}

/** Resource lookup miss (404). */
export class NotFoundError extends ApplicationError {
  readonly code = 'NOT_FOUND';
}

/**
 * Submission against an unknown wallet. Not persisted (there is no wallet to own the
 * audit row), so it is a business rejection outside the transaction table (422 / ack).
 */
export class WalletNotFoundError extends ApplicationError {
  readonly code = 'WALLET_NOT_FOUND';
}

/** Database/broker temporarily unavailable, lock timeout, deadlock (503 / retry). */
export class TransientInfrastructureError extends ApplicationError {
  readonly code = 'TEMPORARILY_UNAVAILABLE';
}
