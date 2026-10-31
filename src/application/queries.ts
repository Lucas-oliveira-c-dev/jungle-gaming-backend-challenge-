import type { WalletLedgerEntry } from '../domain/ledger-entry';
import { Money, type MoneyProps } from '../domain/money';
import type { Wallet } from '../domain/wallet';
import { walletView, type WalletView } from './create-wallet';
import { NotFoundError, ValidationError } from './errors';
import type { AppLogger, StoredTransaction, WageringMetrics } from './ports';

export interface WageringReadModel {
  findWallet(walletId: string): Promise<Wallet | null>;
  /** Entries with sequence > afterSequence, ascending. */
  ledgerPage(walletId: string, afterSequence: number, limit: number): Promise<WalletLedgerEntry[]>;
  findTransactionById(id: string): Promise<StoredTransaction | null>;
  findTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<StoredTransaction | null>;
  /**
   * Stored balance and ledger aggregates read from one snapshot (REPEATABLE READ), so
   * concurrent writes cannot produce a false divergence.
   */
  reconciliationSnapshot(walletId: string): Promise<{
    wallet: Wallet;
    signedSum: string;
    entries: number;
    lastBalanceAfter?: string;
  } | null>;
}

export interface TransactionView {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  status: string;
  referenceExternalTransactionId?: string;
  referenceTransactionId?: string;
  failureCode?: string;
  balance?: MoneyProps;
  createdAt: string;
  processedAt?: string;
}

export function transactionView({ transaction: tx, resultBalance }: StoredTransaction): TransactionView {
  return {
    id: tx.id,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    idempotencyKey: tx.idempotencyKey,
    walletId: tx.walletId,
    playerId: tx.playerId,
    roundId: tx.roundId,
    gameId: tx.gameId,
    kind: tx.kind,
    money: tx.money.toJSON(),
    status: tx.status,
    ...(tx.referenceExternalTransactionId ? { referenceExternalTransactionId: tx.referenceExternalTransactionId } : {}),
    ...(tx.referenceTransactionId ? { referenceTransactionId: tx.referenceTransactionId } : {}),
    ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
    ...(resultBalance ? { balance: resultBalance.toJSON() } : {}),
    createdAt: tx.createdAt.toISOString(),
    ...(tx.processedAt ? { processedAt: tx.processedAt.toISOString() } : {}),
  };
}

const CURSOR_VERSION = 1;

/** Opaque cursor: base64url of {v, s} where s is the last ledger sequence returned. */
export function encodeCursor(sequence: number): string {
  return Buffer.from(JSON.stringify({ v: CURSOR_VERSION, s: sequence })).toString('base64url');
}

export function decodeCursor(cursor: string | undefined): number {
  if (!cursor) return 0;
  try {
    const parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
    if (parsed?.v === CURSOR_VERSION && Number.isInteger(parsed.s) && parsed.s >= 0) return parsed.s;
  } catch {
    // fall through
  }
  throw new ValidationError('Invalid cursor');
}

export class WageringQueries {
  constructor(
    private readonly read: WageringReadModel,
    private readonly metrics: WageringMetrics,
    private readonly logger: AppLogger,
  ) {}

  async wallet(walletId: string): Promise<WalletView> {
    const wallet = await this.read.findWallet(walletId);
    if (!wallet) throw new NotFoundError(`Wallet ${walletId} not found`);
    return walletView(wallet);
  }

  async ledger(walletId: string, cursor: string | undefined, rawLimit: string | undefined) {
    const limit = rawLimit === undefined ? 50 : Number(rawLimit);
    if (!Number.isInteger(limit) || limit < 1 || limit > 500) throw new ValidationError('limit must be an integer between 1 and 500');
    const after = decodeCursor(cursor);
    if (!(await this.read.findWallet(walletId))) throw new NotFoundError(`Wallet ${walletId} not found`);
    const entries = await this.read.ledgerPage(walletId, after, limit + 1);
    const page = entries.slice(0, limit);
    const last = page.at(-1);
    return {
      items: page.map((e) => ({
        id: e.id,
        transactionId: e.transactionId,
        sequence: e.sequence,
        direction: e.direction,
        money: e.money.toJSON(),
        balanceBefore: e.balanceBefore.toJSON(),
        balanceAfter: e.balanceAfter.toJSON(),
        createdAt: e.createdAt.toISOString(),
      })),
      nextCursor: entries.length > limit && last ? encodeCursor(last.sequence) : null,
    };
  }

  async transaction(id: string): Promise<TransactionView> {
    const stored = await this.read.findTransactionById(id);
    if (!stored) throw new NotFoundError(`Transaction ${id} not found`);
    return transactionView(stored);
  }

  async transactionByExternalId(providerId: string, externalTransactionId: string): Promise<TransactionView> {
    const stored = await this.read.findTransactionByExternalId(providerId, externalTransactionId);
    if (!stored) throw new NotFoundError(`Transaction ${providerId}/${externalTransactionId} not found`);
    return transactionView(stored);
  }

  /**
   * Compares the materialized balance with the balance rebuilt from the ledger. Never
   * fixes anything: divergences are logged, counted and reported.
   */
  async reconcile(walletId: string, ctx: { correlationId: string }) {
    const snapshot = await this.read.reconciliationSnapshot(walletId);
    if (!snapshot) throw new NotFoundError(`Wallet ${walletId} not found`);
    const { wallet } = snapshot;
    const stored = wallet.balance;
    const calculated = Money.fromSigned({ amount: snapshot.signedSum, currency: wallet.currency });
    const difference = stored.subtract(calculated);
    const chainOk =
      snapshot.lastBalanceAfter === undefined
        ? stored.isZero()
        : Money.from({ amount: snapshot.lastBalanceAfter, currency: wallet.currency }).equals(stored);
    const consistent = difference.isZero() && chainOk;
    if (!consistent) {
      this.metrics.reconciliationDivergence(walletId);
      this.logger.error(
        { correlationId: ctx.correlationId, walletId, difference: difference.toString(), chainOk },
        'wallet reconciliation divergence',
      );
    }
    return {
      walletId,
      storedBalance: stored.toJSON(),
      calculatedBalance: calculated.toJSON(),
      difference: difference.toJSON(),
      consistent,
      checkedEntries: snapshot.entries,
    };
  }
}
