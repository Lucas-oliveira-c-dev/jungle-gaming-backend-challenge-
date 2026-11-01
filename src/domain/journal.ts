import { InvariantViolationError } from './errors';
import { LedgerDirection, type WalletLedgerEntry } from './ledger-entry';
import { Money } from './money';

/**
 * Chart of accounts (house perspective):
 *  - wallet:<walletId>                 LIABILITY  money the house owes the player
 *  - provider:<providerId>:<currency>  LIABILITY  settlement position with the game provider
 *  - house:funding:<currency>          ASSET      cash received when wallets are funded (OPENING)
 * Liabilities grow with credits, assets with debits.
 */
export enum AccountType {
  Asset = 'ASSET',
  Liability = 'LIABILITY',
}

export interface AccountRef {
  code: string;
  type: AccountType;
  currency: string;
}

export const Accounts = {
  wallet: (walletId: string, currency: string): AccountRef => ({ code: `wallet:${walletId}`, type: AccountType.Liability, currency }),
  provider: (providerId: string, currency: string): AccountRef => ({
    code: `provider:${providerId}:${currency}`,
    type: AccountType.Liability,
    currency,
  }),
  houseFunding: (currency: string): AccountRef => ({ code: `house:funding:${currency}`, type: AccountType.Asset, currency }),
};

export interface JournalPosting {
  account: AccountRef;
  direction: LedgerDirection;
  money: Money;
}

export interface CreateJournalEntryProps {
  id: string;
  transactionId: string;
  postings: JournalPosting[];
  createdAt: Date;
}

/**
 * Immutable double-entry journal entry: at least two postings, a single currency, and
 * total debits equal to total credits (verified in the factory and again by the schema).
 */
export class JournalEntry {
  private constructor(
    public readonly id: string,
    public readonly transactionId: string,
    public readonly postings: readonly JournalPosting[],
    public readonly createdAt: Date,
  ) {}

  static create(props: CreateJournalEntryProps): JournalEntry {
    const entry = new JournalEntry(props.id, props.transactionId, Object.freeze([...props.postings]), props.createdAt);
    if (entry.postings.length < 2) throw new InvariantViolationError('A journal entry needs at least two postings');
    const currencies = new Set(entry.postings.map((p) => p.money.currency));
    if (currencies.size !== 1) throw new InvariantViolationError('A journal entry must use a single currency');
    for (const p of entry.postings) {
      if (!p.money.isPositive()) throw new InvariantViolationError('Postings must have a positive amount');
      if (p.account.currency !== p.money.currency) throw new InvariantViolationError(`Account ${p.account.code} is not in ${p.money.currency}`);
    }
    if (!entry.isBalanced()) throw new InvariantViolationError(`Unbalanced journal entry for transaction ${entry.transactionId}`);
    return entry;
  }

  isBalanced(): boolean {
    const currency = this.postings[0]!.money.currency;
    const total = (direction: LedgerDirection) =>
      this.postings.filter((p) => p.direction === direction).reduce((sum, p) => sum.add(p.money), Money.zero(currency));
    return total(LedgerDirection.Debit).equals(total(LedgerDirection.Credit));
  }

  /**
   * Mirrors a wallet sub-ledger entry into the journal. The wallet account is a liability:
   * a wallet CREDIT (balance up) credits the wallet account and debits the counterparty;
   * a wallet DEBIT does the opposite.
   */
  static forWalletMovement(props: { id: string; entry: WalletLedgerEntry; counterparty: AccountRef }): JournalEntry {
    const { entry, counterparty } = props;
    const opposite = entry.direction === LedgerDirection.Credit ? LedgerDirection.Debit : LedgerDirection.Credit;
    return JournalEntry.create({
      id: props.id,
      transactionId: entry.transactionId,
      createdAt: entry.createdAt,
      postings: [
        { account: Accounts.wallet(entry.walletId, entry.money.currency), direction: entry.direction, money: entry.money },
        { account: counterparty, direction: opposite, money: entry.money },
      ],
    });
  }
}
