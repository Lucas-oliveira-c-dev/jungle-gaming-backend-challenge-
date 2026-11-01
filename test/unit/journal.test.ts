import { describe, expect, test } from 'bun:test';
import { InvariantViolationError } from '../../src/domain/errors';
import { Accounts, AccountType, JournalEntry } from '../../src/domain/journal';
import { LedgerDirection } from '../../src/domain/ledger-entry';
import { Money } from '../../src/domain/money';
import { Wallet } from '../../src/domain/wallet';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });
const at = new Date('2026-11-01T12:00:00Z');

describe('JournalEntry (double entry)', () => {
  const { wallet, openingEntry } = Wallet.open({
    id: 'w1',
    playerId: 'p1',
    initialBalance: brl('100.00'),
    opening: { transactionId: 'open', entryId: 'e0' },
    at,
  });

  test('opening: debit house funding (asset), credit the wallet (liability)', () => {
    const journal = JournalEntry.forWalletMovement({ id: 'j1', entry: openingEntry!, counterparty: Accounts.houseFunding('BRL') });
    expect(journal.postings.map((p) => [p.account.code, p.account.type, p.direction, p.money.toString()])).toEqual([
      ['wallet:w1', AccountType.Liability, LedgerDirection.Credit, '100.00'],
      ['house:funding:BRL', AccountType.Asset, LedgerDirection.Debit, '100.00'],
    ]);
    expect(journal.isBalanced()).toBe(true);
  });

  test('bet: debit the wallet, credit the provider settlement account', () => {
    const bet = wallet.debit({ entryId: 'e1', transactionId: 't1', money: brl('25.00'), at });
    const journal = JournalEntry.forWalletMovement({ id: 'j2', entry: bet, counterparty: Accounts.provider('provider-a', 'BRL') });
    expect(journal.postings.map((p) => [p.account.code, p.direction])).toEqual([
      ['wallet:w1', LedgerDirection.Debit],
      ['provider:provider-a:BRL', LedgerDirection.Credit],
    ]);
    expect(journal.transactionId).toBe('t1');
  });

  test('rejects unbalanced, single-posting, zero and mixed-currency entries', () => {
    const wallet = Accounts.wallet('w1', 'BRL');
    const provider = Accounts.provider('p', 'BRL');
    const make = (postings: Parameters<typeof JournalEntry.create>[0]['postings']) => () =>
      JournalEntry.create({ id: 'j', transactionId: 't', createdAt: at, postings });

    expect(make([{ account: wallet, direction: LedgerDirection.Debit, money: brl('10.00') }, { account: provider, direction: LedgerDirection.Credit, money: brl('9.99') }])).toThrow(InvariantViolationError);
    expect(make([{ account: wallet, direction: LedgerDirection.Debit, money: brl('10.00') }])).toThrow(InvariantViolationError);
    expect(make([{ account: wallet, direction: LedgerDirection.Debit, money: brl('0.00') }, { account: provider, direction: LedgerDirection.Credit, money: brl('0.00') }])).toThrow(InvariantViolationError);
    const usd = Money.from({ amount: '10.00', currency: 'USD' });
    expect(make([{ account: wallet, direction: LedgerDirection.Debit, money: brl('10.00') }, { account: Accounts.provider('p', 'USD'), direction: LedgerDirection.Credit, money: usd }])).toThrow(InvariantViolationError);
  });

  test('postings are frozen', () => {
    const journal = JournalEntry.forWalletMovement({ id: 'j3', entry: openingEntry!, counterparty: Accounts.houseFunding('BRL') });
    expect(Object.isFrozen(journal.postings)).toBe(true);
  });
});
