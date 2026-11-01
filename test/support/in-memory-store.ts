import type {
  AppLogger,
  PendingReferenceSchedule,
  StoredTransaction,
  WageringMetrics,
  WageringStore,
  WageringUnitOfWork,
} from '../../src/application/ports';
import type { IntegrationEvent } from '../../src/domain/events';
import type { JournalEntry } from '../../src/domain/journal';
import type { WalletLedgerEntry } from '../../src/domain/ledger-entry';
import type { InboxMessage } from '../../src/domain/messaging';
import type { Money } from '../../src/domain/money';
import { WagerTransaction, WagerTransactionStatus } from '../../src/domain/wager-transaction';
import { Wallet } from '../../src/domain/wallet';

/**
 * Unit-test double. Sequential only — it does NOT model concurrency or atomicity; those
 * are covered by the integration/concurrency suites against real PostgreSQL.
 */
export class InMemoryStore implements WageringStore {
  wallets = new Map<string, Wallet>();
  transactions = new Map<string, StoredTransaction>();
  ledger: WalletLedgerEntry[] = [];
  journal: JournalEntry[] = [];
  outbox: IntegrationEvent<unknown>[] = [];
  inbox = new Set<string>();

  isUniqueViolation = () => false;
  isTransient = () => false;
  isLockConflict = () => false;

  async transactional<T>(work: (uow: WageringUnitOfWork) => Promise<T>): Promise<T> {
    const store = this;
    const clone = (s: StoredTransaction): StoredTransaction => ({
      ...s,
      transaction: WagerTransaction.rehydrate({
        ...s.transaction,
        status: s.transaction.status,
        referenceTransactionId: s.transaction.referenceTransactionId,
        failureCode: s.transaction.failureCode,
        processedAt: s.transaction.processedAt,
      }),
    });
    const find = (pred: (t: WagerTransaction) => boolean) => {
      for (const s of store.transactions.values()) if (pred(s.transaction)) return clone(s);
      return null;
    };
    const uow: WageringUnitOfWork = {
      async lockWallet(id) {
        const w = store.wallets.get(id);
        return w ? Wallet.rehydrate({ ...w, balance: w.balance, version: w.version, updatedAt: w.updatedAt }) : null;
      },
      async insertWallet(w) {
        store.wallets.set(w.id, w);
      },
      async saveWallet(w) {
        store.wallets.set(w.id, w);
      },
      findTransactionByIdempotencyKey: async (key) => find((t) => t.idempotencyKey === key),
      findTransactionByExternalId: async (p, e) => find((t) => t.providerId === p && t.externalTransactionId === e),
      findTransactionById: async (id) => find((t) => t.id === id),
      async hasProcessedReversal(refId) {
        return !!find((t) => t.referenceTransactionId === refId && t.isReversal() && t.status === WagerTransactionStatus.Processed);
      },
      async insertTransaction(tx: WagerTransaction, resultBalance?: Money, schedule?: PendingReferenceSchedule) {
        store.transactions.set(tx.id, { transaction: tx, resultBalance, referenceAttempts: schedule?.attempts ?? 0 });
      },
      async updateTransaction(tx: WagerTransaction, resultBalance?: Money, schedule?: PendingReferenceSchedule) {
        store.transactions.set(tx.id, { transaction: tx, resultBalance, referenceAttempts: schedule?.attempts ?? 0 });
      },
      wakePendingDependents: async () => 0,
      async recordMovement(e, journal) {
        store.ledger.push(e);
        store.journal.push(journal);
      },
      async enqueue(events) {
        store.outbox.push(...events);
      },
      async registerInbox(m: InboxMessage) {
        const key = `${m.consumerName}:${m.messageId}`;
        if (store.inbox.has(key)) return false;
        store.inbox.add(key);
        return true;
      },
    };
    return work(uow);
  }

  async findTransactionByIdempotencyKey(key: string) {
    return this.transactional((uow) => uow.findTransactionByIdempotencyKey(key));
  }

  async findDuePendingReferences() {
    return [];
  }
}

export const silentLogger: AppLogger = { info() {}, warn() {}, error() {}, debug() {} };

export const noopMetrics: WageringMetrics = {
  transactionRecorded() {},
  duplicateDetected() {},
  idempotencyConflict() {},
  lockConflict() {},
  processingDuration() {},
  reconciliationDivergence() {},
};
