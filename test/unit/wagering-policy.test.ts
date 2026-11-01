import { describe, expect, test } from 'bun:test';
import { InvalidTransactionStateError, InvariantViolationError } from '../../src/domain/errors';
import { FailureCode } from '../../src/domain/failure-codes';
import { LedgerDirection } from '../../src/domain/ledger-entry';
import { Money } from '../../src/domain/money';
import { WagerTransaction, WagerTransactionKind as K, WagerTransactionStatus as S } from '../../src/domain/wager-transaction';
import { applyWager } from '../../src/domain/wagering-policy';
import { Wallet } from '../../src/domain/wallet';

const at = new Date('2026-10-31T12:00:00Z');
const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
let seq = 0;

function wallet(balance = '100.00') {
  return Wallet.open({ id: 'w1', playerId: 'p1', initialBalance: brl(balance), opening: { transactionId: 'o', entryId: 'oe' }, at }).wallet;
}

function tx(kind: K, amount: string, extra: Partial<Parameters<typeof WagerTransaction.create>[0]> = {}) {
  const id = `tx-${++seq}`;
  return WagerTransaction.create({
    id,
    providerId: 'provider-a',
    externalTransactionId: `ext-${seq}`,
    idempotencyKey: `provider-a:ext-${seq}`,
    payloadHash: 'h'.repeat(64),
    walletId: 'w1',
    playerId: 'p1',
    roundId: 'round-1',
    gameId: 'game',
    kind,
    money: brl(amount),
    createdAt: at,
    ...extra,
  });
}

function processed(kind: K, amount: string, w: Wallet, extra = {}) {
  const t = tx(kind, amount, extra);
  applyWager(t, w, { alreadyReversed: false }, { ledgerEntryId: `le-${seq}` }, at);
  expect(t.status).toBe(S.Processed);
  return t;
}

const run = (t: WagerTransaction, w: Wallet, reference?: WagerTransaction, alreadyReversed = false) =>
  applyWager(t, w, { reference, alreadyReversed }, { ledgerEntryId: `le-${++seq}` }, at);

describe('BET', () => {
  test('debits the wallet with one DEBIT entry', () => {
    const w = wallet();
    const outcome = run(tx(K.Bet, '25.00'), w);
    expect(outcome.type).toBe('processed');
    expect(outcome.type === 'processed' && outcome.entry?.direction).toBe(LedgerDirection.Debit);
    expect(w.balance.toString()).toBe('75.00');
  });

  test('rejects with INSUFFICIENT_FUNDS without touching the wallet', () => {
    const w = wallet('10.00');
    const t = tx(K.Bet, '10.01');
    expect(run(t, w)).toEqual({ type: 'rejected', code: FailureCode.InsufficientFunds });
    expect(t.status).toBe(S.Rejected);
    expect(w.balance.toString()).toBe('10.00');
    expect(w.version).toBe(1);
  });

  test('cannot reference another transaction', () => {
    expect(() => tx(K.Bet, '1.00', { referenceExternalTransactionId: 'x' })).toThrow(InvariantViolationError);
  });
});

describe('WIN', () => {
  test('credits, optionally linked to the BET of the same round', () => {
    const w = wallet();
    const bet = processed(K.Bet, '10.00', w);
    const win = tx(K.Win, '30.00', { referenceExternalTransactionId: bet.externalTransactionId });
    const outcome = run(win, w, bet);
    expect(outcome.type).toBe('processed');
    expect(win.referenceTransactionId).toBe(bet.id);
    expect(w.balance.toString()).toBe('120.00');
  });

  test('referencing a BET from another round is a mismatch', () => {
    const w = wallet();
    const bet = processed(K.Bet, '10.00', w);
    const win = tx(K.Win, '30.00', { referenceExternalTransactionId: bet.externalTransactionId, roundId: 'round-2' });
    expect(run(win, w, bet)).toEqual({ type: 'rejected', code: FailureCode.ReferenceMismatch });
  });
});

describe('LOSS', () => {
  test('is processed without moving money or producing a ledger entry', () => {
    const w = wallet();
    const outcome = run(tx(K.Loss, '0.00'), w);
    expect(outcome).toEqual({ type: 'processed', reference: undefined });
    expect(w.version).toBe(1);
    expect(w.balance.toString()).toBe('100.00');
  });
});

describe('REFUND', () => {
  test('credits back a PROCESSED BET exactly once', () => {
    const w = wallet();
    const bet = processed(K.Bet, '40.00', w);
    const refund = tx(K.Refund, '40.00', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(run(refund, w, bet).type).toBe('processed');
    expect(w.balance.toString()).toBe('100.00');

    const again = tx(K.Refund, '40.00', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(run(again, w, bet, true)).toEqual({ type: 'rejected', code: FailureCode.ReferenceAlreadyReversed });
    expect(w.balance.toString()).toBe('100.00');
  });

  test('requires a reference', () => {
    expect(() => tx(K.Refund, '1.00')).toThrow(InvariantViolationError);
  });

  test('only BETs can be refunded', () => {
    const w = wallet();
    const win = processed(K.Win, '5.00', w);
    const refund = tx(K.Refund, '5.00', { referenceExternalTransactionId: win.externalTransactionId });
    expect(run(refund, w, win)).toEqual({ type: 'rejected', code: FailureCode.ReferenceKindNotAllowed });
  });

  test('partial refunds are rejected', () => {
    const w = wallet();
    const bet = processed(K.Bet, '40.00', w);
    const refund = tx(K.Refund, '39.99', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(run(refund, w, bet)).toEqual({ type: 'rejected', code: FailureCode.ReversalAmountMismatch });
  });

  test('a rejected BET cannot be refunded', () => {
    const w = wallet('1.00');
    const bet = tx(K.Bet, '40.00');
    run(bet, w);
    const refund = tx(K.Refund, '40.00', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(run(refund, w, bet)).toEqual({ type: 'rejected', code: FailureCode.ReferenceNotProcessed });
  });

  test('waits (PENDING_REFERENCE) while the BET has not arrived', () => {
    const w = wallet();
    const refund = tx(K.Refund, '10.00', { referenceExternalTransactionId: 'not-yet' });
    expect(run(refund, w).type).toBe('pending_reference');
    expect(refund.status).toBe(S.PendingReference);
    expect(w.version).toBe(1);
  });
});

describe('ROLLBACK', () => {
  test('of a BET credits; of a WIN debits', () => {
    const w = wallet();
    const bet = processed(K.Bet, '10.00', w);
    const rbBet = tx(K.Rollback, '10.00', { referenceExternalTransactionId: bet.externalTransactionId });
    const o1 = run(rbBet, w, bet);
    expect(o1.type === 'processed' && o1.entry?.direction).toBe(LedgerDirection.Credit);

    const win = processed(K.Win, '50.00', w);
    const rbWin = tx(K.Rollback, '50.00', { referenceExternalTransactionId: win.externalTransactionId });
    const o2 = run(rbWin, w, win);
    expect(o2.type === 'processed' && o2.entry?.direction).toBe(LedgerDirection.Debit);
    expect(w.balance.toString()).toBe('100.00');
  });

  test('rollback that would make the balance negative uses a distinct failure code', () => {
    const w = wallet('0.00');
    const win = processed(K.Win, '50.00', w);
    processed(K.Bet, '45.00', w);
    const rb = tx(K.Rollback, '50.00', { referenceExternalTransactionId: win.externalTransactionId });
    expect(run(rb, w, win)).toEqual({ type: 'rejected', code: FailureCode.ReversalInsufficientFunds });
    expect(w.balance.toString()).toBe('5.00');
  });

  test('rollback of a REFUND debits; rollback of a LOSS or ROLLBACK is not allowed', () => {
    const w = wallet();
    const bet = processed(K.Bet, '10.00', w);
    const refund = tx(K.Refund, '10.00', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(run(refund, w, bet).type).toBe('processed');
    const rbRefund = tx(K.Rollback, '10.00', { referenceExternalTransactionId: refund.externalTransactionId });
    const outcome = run(rbRefund, w, refund);
    expect(outcome.type === 'processed' && outcome.entry?.direction).toBe(LedgerDirection.Debit);
    expect(w.balance.toString()).toBe('90.00');

    const loss = processed(K.Loss, '0.00', w);
    const rbLoss = tx(K.Rollback, '1.00', { referenceExternalTransactionId: loss.externalTransactionId });
    expect(run(rbLoss, w, loss)).toEqual({ type: 'rejected', code: FailureCode.ReferenceKindNotAllowed });
  });

  test('cannot reverse the same transaction twice', () => {
    const w = wallet();
    const bet = processed(K.Bet, '10.00', w);
    const rb = tx(K.Rollback, '10.00', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(run(rb, w, bet, true)).toEqual({ type: 'rejected', code: FailureCode.ReferenceAlreadyReversed });
  });
});

describe('settling a reversed BET', () => {
  test('WIN/LOSS against a refunded BET is rejected (no prize on a cancelled bet)', () => {
    const w = wallet();
    const bet = processed(K.Bet, '10.00', w);
    const win = tx(K.Win, '50.00', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(run(win, w, bet, true)).toEqual({ type: 'rejected', code: FailureCode.ReferenceAlreadyReversed });
    const loss = tx(K.Loss, '0.00', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(run(loss, w, bet, true)).toEqual({ type: 'rejected', code: FailureCode.ReferenceAlreadyReversed });
    expect(w.balance.toString()).toBe('90.00');
  });

  test('pending outcome tells whether the reference exists but is itself pending', () => {
    const w = wallet();
    const refund = tx(K.Refund, '10.00', { referenceExternalTransactionId: 'not-yet' });
    expect(run(refund, w)).toEqual({ type: 'pending_reference', referenceFound: false });
    const pendingBet = WagerTransaction.rehydrate({ ...tx(K.Refund, '10.00', { referenceExternalTransactionId: 'x' }), status: S.PendingReference } as never);
    const rollback = tx(K.Rollback, '10.00', { referenceExternalTransactionId: pendingBet.externalTransactionId });
    expect(run(rollback, w, pendingBet)).toEqual({ type: 'pending_reference', referenceFound: true });
  });
});

describe('cross-cutting rules', () => {
  test('currency conflict is rejected and auditable', () => {
    const w = wallet();
    const t = tx(K.Bet, '1.00', { money: Money.from({ amount: '1.00', currency: 'USD' }) });
    expect(run(t, w)).toEqual({ type: 'rejected', code: FailureCode.CurrencyMismatch });
    expect(t.failureCode).toBe(FailureCode.CurrencyMismatch);
  });

  test('wallet of another player is rejected', () => {
    const w = wallet();
    expect(run(tx(K.Bet, '1.00', { playerId: 'p2' }), w)).toEqual({ type: 'rejected', code: FailureCode.PlayerWalletMismatch });
  });

  test('reference must belong to the same player', () => {
    const w = wallet();
    const bet = processed(K.Bet, '10.00', w);
    const foreign = WagerTransaction.rehydrate({ ...bet, money: bet.money, status: S.Processed, playerId: 'p2' } as never);
    const refund = tx(K.Refund, '10.00', { referenceExternalTransactionId: bet.externalTransactionId });
    expect(run(refund, w, foreign)).toEqual({ type: 'rejected', code: FailureCode.ReferenceMismatch });
  });

  test('terminal states cannot transition', () => {
    const t = tx(K.Bet, '1.00');
    t.markProcessed(undefined, at);
    expect(t.isTerminal()).toBe(true);
    expect(() => t.reject(FailureCode.InsufficientFunds, at)).toThrow(InvalidTransactionStateError);
    expect(() => t.markPendingReference()).toThrow(InvalidTransactionStateError);
    expect(() => t.markProcessed(undefined, at)).toThrow(InvalidTransactionStateError);
  });

  test('PENDING_REFERENCE can still be resolved or rejected', () => {
    const t = tx(K.Refund, '1.00', { referenceExternalTransactionId: 'x' });
    t.markPendingReference();
    t.reject(FailureCode.ReferenceNotFound, at);
    expect(t.status).toBe(S.Rejected);
  });
});
