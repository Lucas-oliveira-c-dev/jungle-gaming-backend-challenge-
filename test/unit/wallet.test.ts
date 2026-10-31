import { describe, expect, test } from 'bun:test';
import { CurrencyMismatchError, InsufficientFundsError, InvariantViolationError } from '../../src/domain/errors';
import { LedgerDirection, WalletLedgerEntry } from '../../src/domain/ledger-entry';
import { Money } from '../../src/domain/money';
import { Wallet } from '../../src/domain/wallet';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const at = new Date('2026-10-31T12:00:00Z');
let n = 0;
const movement = (amount: string) => ({ entryId: `e-${++n}`, transactionId: `t-${n}`, money: brl(amount), at });

function open(initial: string) {
  return Wallet.open({ id: 'w1', playerId: 'p1', initialBalance: brl(initial), opening: { transactionId: 'open', entryId: 'e0' }, at });
}

describe('Wallet', () => {
  test('opens at version 1 with an OPENING credit in the ledger', () => {
    const { wallet, openingEntry } = open('100.00');
    expect(wallet.version).toBe(1);
    expect(wallet.balance.toString()).toBe('100.00');
    expect(openingEntry?.direction).toBe(LedgerDirection.Credit);
    expect(openingEntry?.sequence).toBe(1);
    expect(openingEntry?.balanceBefore.toString()).toBe('0.00');
  });

  test('zero initial balance: version 1 and no ledger entry', () => {
    const { wallet, openingEntry } = open('0.00');
    expect(wallet.version).toBe(1);
    expect(openingEntry).toBeUndefined();
  });

  test('every balance change returns a balanced ledger entry and bumps the version', () => {
    const { wallet } = open('100.00');
    const debit = wallet.debit(movement('30.00'));
    expect(debit.balanceBefore.toString()).toBe('100.00');
    expect(debit.balanceAfter.toString()).toBe('70.00');
    expect(debit.sequence).toBe(2);
    expect(debit.isBalanced()).toBe(true);
    const credit = wallet.credit(movement('5.50'));
    expect(credit.balanceAfter.toString()).toBe('75.50');
    expect(wallet.version).toBe(3);
  });

  test('balance never goes negative', () => {
    const { wallet } = open('10.00');
    expect(() => wallet.debit(movement('10.01'))).toThrow(InsufficientFundsError);
    expect(wallet.balance.toString()).toBe('10.00');
    expect(wallet.version).toBe(1);
    wallet.debit(movement('10.00'));
    expect(wallet.balance.isZero()).toBe(true);
  });

  test('rejects movements in another currency', () => {
    const { wallet } = open('10.00');
    const usd = { ...movement('1.00'), money: Money.from({ amount: '1.00', currency: 'USD' }) };
    expect(() => wallet.credit(usd)).toThrow(CurrencyMismatchError);
    expect(() => wallet.debit(usd)).toThrow(CurrencyMismatchError);
  });

  test('rejects zero movements (they would not change the balance)', () => {
    const { wallet } = open('10.00');
    expect(() => wallet.credit(movement('0.00'))).toThrow(InvariantViolationError);
  });

  test('replaying the ledger rebuilds the balance', () => {
    const { wallet, openingEntry } = open('100.00');
    const entries = [openingEntry!, wallet.debit(movement('20.00')), wallet.credit(movement('45.00')), wallet.debit(movement('0.01'))];
    const rebuilt = entries.reduce((sum, e) => sum.add(e.signedAmount()), Money.zero('BRL'));
    expect(rebuilt.equals(wallet.balance)).toBe(true);
  });
});

describe('WalletLedgerEntry', () => {
  test('factory refuses unbalanced entries', () => {
    expect(() =>
      WalletLedgerEntry.create({
        id: 'x',
        walletId: 'w',
        transactionId: 't',
        sequence: 2,
        direction: LedgerDirection.Debit,
        money: brl('10.00'),
        balanceBefore: brl('50.00'),
        balanceAfter: brl('45.00'),
        createdAt: at,
      }),
    ).toThrow(InvariantViolationError);
  });

  test('exposes no mutators', () => {
    const proto = Object.getOwnPropertyNames(WalletLedgerEntry.prototype);
    expect(proto.sort()).toEqual(['constructor', 'isBalanced', 'signedAmount']);
  });
});
