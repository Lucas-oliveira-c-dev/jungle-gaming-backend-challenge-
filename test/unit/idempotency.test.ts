import { beforeEach, describe, expect, test } from 'bun:test';
import { IdempotencyConflictError, ValidationError } from '../../src/application/errors';
import { systemClock, uuidV7 } from '../../src/application/ports';
import { SubmitWagerTransaction } from '../../src/application/submit-wager-transaction';
import { canonicalJson, parseWagerSubmission } from '../../src/application/wager-request';
import { Money } from '../../src/domain/money';
import { Wallet } from '../../src/domain/wallet';
import { InMemoryStore, noopMetrics, silentLogger } from '../support/in-memory-store';

const playerId = '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1';
const walletId = '0192f291-27dd-7d3f-8071-5f8685deef37';

const body = (overrides: Record<string, unknown> = {}) => ({
  providerId: 'provider-a',
  externalTransactionId: 'transaction-123',
  playerId,
  walletId,
  roundId: 'round-987',
  gameId: 'fortune-chimp',
  kind: 'BET',
  money: { amount: '25.00', currency: 'BRL' },
  ...overrides,
});

describe('payload hash', () => {
  test('is independent of key order and amount formatting', () => {
    const a = parseWagerSubmission(body(), 'k');
    const reordered = Object.fromEntries(Object.entries(body({ money: { currency: 'BRL', amount: '25' } })).reverse());
    const b = parseWagerSubmission(reordered, 'other-key');
    expect(a.payloadHash).toBe(b.payloadHash); // header is not part of the hash
  });

  test('changes when any business field changes', () => {
    const base = parseWagerSubmission(body(), 'k').payloadHash;
    expect(parseWagerSubmission(body({ money: { amount: '25.01', currency: 'BRL' } }), 'k').payloadHash).not.toBe(base);
    expect(parseWagerSubmission(body({ roundId: 'round-988' }), 'k').payloadHash).not.toBe(base);
  });

  test('canonical JSON sorts keys recursively and drops undefined', () => {
    expect(canonicalJson({ b: 1, a: { d: undefined, c: [2, { z: 1, y: 2 }] } })).toBe('{"a":{"c":[2,{"y":2,"z":1}]},"b":1}');
  });
});

describe('request validation', () => {
  test.each([
    ['missing idempotency key', body(), undefined],
    ['OPENING is internal', body({ kind: 'OPENING' }), 'k'],
    ['number amount', body({ money: { amount: 25, currency: 'BRL' } }), 'k'],
    ['too many decimals', body({ money: { amount: '1.001', currency: 'BRL' } }), 'k'],
    ['zero bet', body({ money: { amount: '0.00', currency: 'BRL' } }), 'k'],
    ['refund without reference', body({ kind: 'REFUND' }), 'k'],
    ['unknown field', body({ extra: true }), 'k'],
    ['invalid uuid', body({ walletId: 'nope' }), 'k'],
    ['self reference', body({ kind: 'REFUND', referenceExternalTransactionId: 'transaction-123' }), 'k'],
  ])('%s', (_name, payload, key) => {
    expect(() => parseWagerSubmission(payload, key)).toThrow(ValidationError);
  });
});

describe('SubmitWagerTransaction idempotency', () => {
  let store: InMemoryStore;
  let submit: SubmitWagerTransaction;

  beforeEach(() => {
    store = new InMemoryStore();
    const { wallet } = Wallet.open({
      id: walletId,
      playerId,
      initialBalance: Money.from({ amount: '1000.00', currency: 'BRL' }),
      opening: { transactionId: uuidV7.next(), entryId: uuidV7.next() },
      at: new Date(),
    });
    store.wallets.set(walletId, wallet);
    submit = new SubmitWagerTransaction(store, uuidV7, systemClock, noopMetrics, silentLogger);
  });

  const ctx = { source: 'http' as const, correlationId: 'c' };

  test('identical request replays the original result', async () => {
    const first = await submit.execute(parseWagerSubmission(body(), 'provider-a:transaction-123'), ctx);
    expect(first).toMatchObject({ status: 'PROCESSED', balance: { amount: '975.00' }, idempotentReplay: false });

    // A later bet changes the balance; the replay must still report the original one.
    await submit.execute(parseWagerSubmission(body({ externalTransactionId: 't-2' }), 'provider-a:t-2'), ctx);
    const replay = await submit.execute(parseWagerSubmission(body(), 'provider-a:transaction-123'), ctx);
    expect(replay).toEqual({ ...first, idempotentReplay: true });
    expect(store.ledger).toHaveLength(2);
  });

  test('same key with a different payload is a conflict, not a replay', async () => {
    await submit.execute(parseWagerSubmission(body(), 'provider-a:transaction-123'), ctx);
    const divergent = parseWagerSubmission(body({ money: { amount: '30.00', currency: 'BRL' } }), 'provider-a:transaction-123');
    await expect(submit.execute(divergent, ctx)).rejects.toBeInstanceOf(IdempotencyConflictError);
    expect(store.ledger).toHaveLength(1);
  });

  test('same provider transaction id under another key is a conflict', async () => {
    await submit.execute(parseWagerSubmission(body(), 'provider-a:transaction-123'), ctx);
    await expect(submit.execute(parseWagerSubmission(body(), 'another-key'), ctx)).rejects.toBeInstanceOf(IdempotencyConflictError);
  });

  test('rejected transactions are replayed as rejected', async () => {
    const big = parseWagerSubmission(body({ money: { amount: '5000.00', currency: 'BRL' } }), 'k-big');
    const first = await submit.execute(big, ctx);
    expect(first).toMatchObject({ status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS' });
    expect(await submit.execute(big, ctx)).toMatchObject({ status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS', idempotentReplay: true });
  });

  test('uppercase UUIDs are the same ids (no false PLAYER_WALLET_MISMATCH)', async () => {
    const upper = body({ playerId: playerId.toUpperCase(), walletId: walletId.toUpperCase() });
    const result = await submit.execute(parseWagerSubmission(upper, 'k-upper'), ctx);
    expect(result).toMatchObject({ status: 'PROCESSED', balance: { amount: '975.00' } });
    // same business payload regardless of casing
    expect(parseWagerSubmission(upper, 'x').payloadHash).toBe(parseWagerSubmission(body(), 'x').payloadHash);
  });

  test('SQS redelivery of the same messageId is deduplicated by the inbox', async () => {
    const sub = parseWagerSubmission(body(), 'provider-a:transaction-123');
    const inbox = { consumerName: 'c', messageId: 'msg-1', payloadHash: 'x' };
    await submit.execute(sub, { ...ctx, source: 'sqs', inbox });
    const again = await submit.execute(sub, { ...ctx, source: 'sqs', inbox });
    expect(again.idempotentReplay).toBe(true);
    expect(store.ledger).toHaveLength(1);
  });
});
