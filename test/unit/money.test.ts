import { describe, expect, test } from 'bun:test';
import { CurrencyMismatchError, InvalidMoneyError } from '../../src/domain/errors';
import { Money } from '../../src/domain/money';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });

describe('Money', () => {
  test('serializes with a fixed scale of 2', () => {
    expect(brl('25').toString()).toBe('25.00');
    expect(brl('25.5').toString()).toBe('25.50');
    expect(brl('0.01').toJSON()).toEqual({ amount: '0.01', currency: 'BRL' });
  });

  test('arithmetic is exact (no floating point drift)', () => {
    expect(brl('0.10').add(brl('0.20')).toString()).toBe('0.30');
    let total = Money.zero('BRL');
    for (let i = 0; i < 1000; i++) total = total.add(brl('0.01'));
    expect(total.toString()).toBe('10.00');
    expect(brl('100.00').subtract(brl('80.00')).toString()).toBe('20.00');
  });

  test('handles amounts beyond Number.MAX_SAFE_INTEGER', () => {
    const big = brl('999999999999999999.99');
    expect(big.toString()).toBe('999999999999999999.99');
    expect(big.subtract(brl('0.01')).toString()).toBe('999999999999999999.98');
  });

  test('is immutable', () => {
    const a = brl('10.00');
    a.add(brl('5.00'));
    expect(a.toString()).toBe('10.00');
  });

  test('negative results and comparisons', () => {
    const diff = brl('1.00').subtract(brl('3.10'));
    expect(diff.isNegative()).toBe(true);
    expect(diff.toString()).toBe('-2.10');
    expect(diff.negate().toString()).toBe('2.10');
    expect(Money.fromSigned({ amount: '-2.10', currency: 'BRL' }).equals(diff)).toBe(true);
    expect(brl('1.00').isLessThan(brl('1.01'))).toBe(true);
    expect(brl('0.00').isZero()).toBe(true);
    expect(brl('0.01').isPositive()).toBe(true);
  });

  test.each([
    ['NaN'],
    ['Infinity'],
    ['-Infinity'],
    ['1e3'],
    ['1E-2'],
    [''],
    [' 1.00'],
    ['1.001'],
    ['-1.00'],
    ['+1.00'],
    ['01.00'],
    ['1.'],
    ['.5'],
    ['1,00'],
    ['0x10'],
    ['1234567890123456789.00'],
  ])('rejects invalid amount %p', (amount) => {
    expect(() => brl(amount)).toThrow(InvalidMoneyError);
  });

  test('rejects non-string amounts', () => {
    expect(() => Money.from({ amount: 25 as unknown as string, currency: 'BRL' })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: null as unknown as string, currency: 'BRL' })).toThrow(InvalidMoneyError);
  });

  test.each([['brl'], ['BR'], ['BRLL'], [''], ['12A']])('rejects invalid currency %p', (currency) => {
    expect(() => Money.from({ amount: '1.00', currency })).toThrow(InvalidMoneyError);
  });

  test('operations across currencies fail', () => {
    const usd = Money.from({ amount: '1.00', currency: 'USD' });
    expect(() => brl('1.00').add(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl('1.00').subtract(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl('1.00').isLessThan(usd)).toThrow(CurrencyMismatchError);
    expect(brl('1.00').equals(usd)).toBe(false);
  });
});
