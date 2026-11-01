import { createHash } from 'node:crypto';
import { z } from 'zod';
import { InvalidMoneyError } from '../domain/errors';
import { Money } from '../domain/money';
import { EXTERNAL_KINDS, WagerTransactionKind } from '../domain/wager-transaction';
import { ValidationError } from './errors';

const identifier = z.string().trim().min(1).max(128);

const moneySchema = z.object({ amount: z.string(), currency: z.string() }).strict();

export const wagerRequestSchema = z
  .object({
    providerId: identifier,
    externalTransactionId: identifier,
    playerId: z.string().uuid(),
    walletId: z.string().uuid(),
    roundId: identifier,
    gameId: identifier,
    kind: z.enum(EXTERNAL_KINDS.map(String) as [string, ...string[]], {
      errorMap: () => ({ message: `kind must be one of ${EXTERNAL_KINDS.join(', ')} (OPENING is internal)` }),
    }),
    money: moneySchema,
    referenceExternalTransactionId: identifier.optional(),
  })
  .strict();

export interface WagerRequest {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
}

export interface ValidatedWagerSubmission {
  request: WagerRequest;
  idempotencyKey: string;
  payloadHash: string;
}

/** Shared by HTTP and SQS entry points so both validate and hash identically. */
export function parseWagerSubmission(body: unknown, idempotencyKey: unknown): ValidatedWagerSubmission {
  if (typeof idempotencyKey !== 'string' || idempotencyKey.trim().length === 0 || idempotencyKey.length > 256) {
    throw new ValidationError('Idempotency-Key is required (1-256 chars)');
  }
  const parsed = wagerRequestSchema.safeParse(body);
  if (!parsed.success) {
    throw new ValidationError('Invalid wager transaction payload', parsed.error.flatten());
  }
  const data = parsed.data;
  let money: Money;
  try {
    money = Money.from(data.money);
  } catch (error) {
    if (error instanceof InvalidMoneyError) throw new ValidationError(error.message);
    throw error;
  }
  const kind = data.kind as WagerTransactionKind;
  if (kind !== WagerTransactionKind.Loss && money.isZero()) {
    throw new ValidationError(`${kind} amount must be greater than zero`);
  }
  if ((kind === WagerTransactionKind.Refund || kind === WagerTransactionKind.Rollback) && !data.referenceExternalTransactionId) {
    throw new ValidationError(`${kind} requires referenceExternalTransactionId`);
  }
  if (kind === WagerTransactionKind.Bet && data.referenceExternalTransactionId) {
    throw new ValidationError('BET cannot reference another transaction');
  }
  if (data.referenceExternalTransactionId === data.externalTransactionId) {
    throw new ValidationError('A transaction cannot reference itself');
  }

  // UUIDs are case-insensitive; Postgres returns them lowercase, so normalize once here
  // or comparisons in the domain would reject valid uppercase ids.
  const request: WagerRequest = {
    ...data,
    playerId: data.playerId.toLowerCase(),
    walletId: data.walletId.toLowerCase(),
    kind,
    money,
  };
  return { request, idempotencyKey: idempotencyKey.trim(), payloadHash: payloadHash(request) };
}

/**
 * sha256 (hex) of the canonical JSON of the business fields: keys sorted recursively,
 * no whitespace, amount normalized to scale 2, absent optional fields omitted.
 * Transport metadata (Idempotency-Key header, messageId, occurredAt) is excluded.
 */
export function payloadHash(request: WagerRequest): string {
  const business = {
    providerId: request.providerId,
    externalTransactionId: request.externalTransactionId,
    playerId: request.playerId,
    walletId: request.walletId,
    roundId: request.roundId,
    gameId: request.gameId,
    kind: request.kind,
    money: request.money.toJSON(),
    referenceExternalTransactionId: request.referenceExternalTransactionId,
  };
  return createHash('sha256').update(canonicalJson(business)).digest('hex');
}

export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`).join(',')}}`;
}

export function sha256(text: string): string {
  return createHash('sha256').update(text).digest('hex');
}
