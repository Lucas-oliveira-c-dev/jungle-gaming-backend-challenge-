import { CurrencyMismatchError, InvalidMoneyError } from './errors';

export interface MoneyProps {
  amount: string; // decimal string, e.g. "25.00"
  currency: string; // ISO-4217
}

const SCALE = 2;
const FACTOR = 10n ** BigInt(SCALE);
/** Matches numeric(20,2) in PostgreSQL: at most 18 integer digits. */
const MAX_INTEGER_DIGITS = 18;
const AMOUNT_PATTERN = /^(0|[1-9]\d*)(\.\d{1,2})?$/;
const CURRENCY_PATTERN = /^[A-Z]{3}$/;

/**
 * Immutable monetary value. Internally stored as an integer number of minor units
 * (bigint), so arithmetic is exact and never touches floating point.
 */
export class Money {
  private constructor(
    private readonly minorUnits: bigint,
    public readonly currency: string,
  ) {}

  /**
   * Parses a contract value. Accepts only plain non-negative decimal strings with up to
   * two fractional digits: rejects NaN, Infinity, exponent notation, signs, blanks,
   * leading zeros and anything that is not a string.
   */
  static from(props: MoneyProps): Money {
    const { amount, currency } = props ?? ({} as MoneyProps);
    Money.assertCurrency(currency);
    if (typeof amount !== 'string' || !AMOUNT_PATTERN.test(amount)) {
      throw new InvalidMoneyError(`Invalid amount "${String(amount)}": expected a non-negative decimal string with at most 2 decimal places`);
    }
    const [integer, fraction = ''] = amount.split('.');
    if (integer!.length > MAX_INTEGER_DIGITS) {
      throw new InvalidMoneyError(`Amount exceeds ${MAX_INTEGER_DIGITS} integer digits`);
    }
    return new Money(BigInt(integer!) * FACTOR + BigInt(fraction.padEnd(SCALE, '0')), currency);
  }

  /**
   * Rebuilds a value read from persistence, where negative values may legitimately
   * appear (e.g. a signed reconciliation difference).
   */
  static fromSigned(props: MoneyProps): Money {
    const negative = typeof props.amount === 'string' && props.amount.startsWith('-');
    const money = Money.from({ amount: negative ? props.amount.slice(1) : props.amount, currency: props.currency });
    return negative ? money.negate() : money;
  }

  static zero(currency: string): Money {
    Money.assertCurrency(currency);
    return new Money(0n, currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.minorUnits + other.minorUnits, this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.minorUnits - other.minorUnits, this.currency);
  }

  negate(): Money {
    return new Money(-this.minorUnits, this.currency);
  }

  isZero(): boolean {
    return this.minorUnits === 0n;
  }

  isPositive(): boolean {
    return this.minorUnits > 0n;
  }

  isNegative(): boolean {
    return this.minorUnits < 0n;
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minorUnits < other.minorUnits;
  }

  equals(other: Money): boolean {
    return this.currency === other.currency && this.minorUnits === other.minorUnits;
  }

  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }

  /** Fixed scale decimal string, e.g. "25.00" or "-3.10". */
  toString(): string {
    const negative = this.minorUnits < 0n;
    const abs = negative ? -this.minorUnits : this.minorUnits;
    const integer = abs / FACTOR;
    const fraction = (abs % FACTOR).toString().padStart(SCALE, '0');
    return `${negative ? '-' : ''}${integer}.${fraction}`;
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(`Cannot combine ${this.currency} with ${other.currency}`);
    }
  }

  private static assertCurrency(currency: unknown): asserts currency is string {
    if (typeof currency !== 'string' || !CURRENCY_PATTERN.test(currency)) {
      throw new InvalidMoneyError(`Invalid currency "${String(currency)}": expected an ISO-4217 code`);
    }
  }
}
