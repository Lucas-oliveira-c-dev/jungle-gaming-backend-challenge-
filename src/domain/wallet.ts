import { CurrencyMismatchError, InsufficientFundsError, InvariantViolationError } from './errors';
import { LedgerDirection, WalletLedgerEntry } from './ledger-entry';
import { Money } from './money';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface WalletMovement {
  entryId: string;
  transactionId: string;
  money: Money;
  at: Date;
}

/**
 * Aggregate root. The only way to change the balance is through debit/credit, which
 * always return the ledger entry describing the change — so balance and ledger can
 * never drift apart inside the domain.
 */
export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  /**
   * Opens a wallet at version 1. A positive initial balance is booked as a regular
   * credit (sequence 1) against the internal OPENING transaction, so the ledger alone
   * can always rebuild the balance.
   */
  static open(props: {
    id: string;
    playerId: string;
    initialBalance: Money;
    opening: { transactionId: string; entryId: string };
    at: Date;
  }): { wallet: Wallet; openingEntry?: WalletLedgerEntry } {
    if (props.initialBalance.isNegative()) {
      throw new InvariantViolationError('Initial balance cannot be negative');
    }
    const currency = props.initialBalance.currency;
    const wallet = new Wallet(props.id, props.playerId, currency, Money.zero(currency), 0, props.at, props.at);
    if (props.initialBalance.isZero()) {
      wallet._version = 1;
      return { wallet };
    }
    const openingEntry = wallet.credit({
      entryId: props.opening.entryId,
      transactionId: props.opening.transactionId,
      money: props.initialBalance,
      at: props.at,
    });
    return { wallet, openingEntry };
  }

  /** Rebuilds from persistence — does not revalidate transitions. */
  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  canDebit(money: Money): boolean {
    this.assertSameCurrency(money);
    return !this._balance.isLessThan(money);
  }

  debit(movement: WalletMovement): WalletLedgerEntry {
    this.assertSameCurrency(movement.money);
    if (!this.canDebit(movement.money)) {
      throw new InsufficientFundsError(`Balance ${this._balance} is lower than ${movement.money}`);
    }
    return this.apply(LedgerDirection.Debit, movement);
  }

  credit(movement: WalletMovement): WalletLedgerEntry {
    this.assertSameCurrency(movement.money);
    return this.apply(LedgerDirection.Credit, movement);
  }

  /**
   * Version starts at 1 and increments only when the balance changes, so the version
   * after a movement doubles as the ledger sequence number for that entry.
   */
  private apply(direction: LedgerDirection, movement: WalletMovement): WalletLedgerEntry {
    if (!movement.money.isPositive()) {
      throw new InvariantViolationError('Wallet movements must have a positive amount');
    }
    const before = this._balance;
    const after = direction === LedgerDirection.Credit ? before.add(movement.money) : before.subtract(movement.money);
    const entry = WalletLedgerEntry.create({
      id: movement.entryId,
      walletId: this.id,
      transactionId: movement.transactionId,
      sequence: this._version + 1,
      direction,
      money: movement.money,
      balanceBefore: before,
      balanceAfter: after,
      createdAt: movement.at,
    });
    this._balance = after;
    this._version += 1;
    this._updatedAt = movement.at;
    return entry;
  }

  private assertSameCurrency(money: Money): void {
    if (money.currency !== this.currency) {
      throw new CurrencyMismatchError(`Wallet ${this.id} holds ${this.currency}, got ${money.currency}`);
    }
  }
}
