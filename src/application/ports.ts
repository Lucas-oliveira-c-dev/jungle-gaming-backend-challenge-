import type { IntegrationEvent } from '../domain/events';
import type { JournalEntry } from '../domain/journal';
import type { WalletLedgerEntry } from '../domain/ledger-entry';
import type { InboxMessage, OutboxMessage } from '../domain/messaging';
import type { Money } from '../domain/money';
import type { WagerTransaction } from '../domain/wager-transaction';
import type { Wallet } from '../domain/wallet';

export interface StoredTransaction {
  transaction: WagerTransaction;
  /** Wallet balance observed when the transaction reached its current status. */
  resultBalance?: Money;
  referenceAttempts: number;
}

export interface PendingReferenceSchedule {
  attempts: number;
  nextCheckAt: Date;
}

/**
 * Everything that happens inside one SQL transaction. Implementations must guarantee that
 * all writes commit or roll back together.
 */
export interface WageringUnitOfWork {
  /**
   * Database time at the start of this transaction. Every timestamp and schedule written in
   * the transaction uses it, so all instances share one clock (app clocks may be skewed).
   */
  now(): Date;
  /** SELECT ... FOR UPDATE on the wallet row: the per-wallet serialization point. */
  lockWallet(walletId: string): Promise<Wallet | null>;
  insertWallet(wallet: Wallet): Promise<void>;
  saveWallet(wallet: Wallet): Promise<void>;

  findTransactionByIdempotencyKey(key: string): Promise<StoredTransaction | null>;
  findTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<StoredTransaction | null>;
  findTransactionById(id: string): Promise<StoredTransaction | null>;
  hasProcessedReversal(referenceTransactionId: string): Promise<boolean>;
  insertTransaction(tx: WagerTransaction, resultBalance: Money | undefined, schedule?: PendingReferenceSchedule): Promise<void>;
  updateTransaction(tx: WagerTransaction, resultBalance: Money | undefined, schedule?: PendingReferenceSchedule): Promise<void>;
  /** Makes transactions waiting for (providerId, externalTransactionId) due immediately. */
  wakePendingDependents(providerId: string, externalTransactionId: string): Promise<number>;

  /**
   * Writes a wallet sub-ledger entry together with its double-entry journal entry —
   * there is no way to record one without the other.
   */
  recordMovement(entry: WalletLedgerEntry, journal: JournalEntry): Promise<void>;
  enqueue(events: IntegrationEvent<unknown>[]): Promise<void>;
  /** Returns false when (consumer, messageId) was already recorded — i.e. a redelivery. */
  registerInbox(message: InboxMessage): Promise<boolean>;
}

export interface WageringStore {
  transactional<T>(work: (uow: WageringUnitOfWork) => Promise<T>): Promise<T>;
  isUniqueViolation(error: unknown): boolean;
  isTransient(error: unknown): boolean;
  isLockConflict(error: unknown): boolean;
  /** Non-locking read used for the idempotency fast path. */
  findTransactionByIdempotencyKey(key: string): Promise<StoredTransaction | null>;
  /** Due according to the database clock (shared by all instances, immune to app clock skew). */
  findDuePendingReferences(limit: number): Promise<{ id: string; walletId: string }[]>;
}

export interface OutboxStore {
  /** Leases due messages so concurrent publishers never work on the same row. */
  claim(limit: number, leaseSeconds: number): Promise<OutboxMessage[]>;
  /** One UPDATE for the whole batch. */
  markPublished(messages: OutboxMessage[]): Promise<void>;
  scheduleRetry(message: OutboxMessage, error: string): Promise<void>;
  oldestPendingAgeSeconds(): Promise<number>;
}

export interface EventPublisher {
  publish(message: OutboxMessage): Promise<void>;
  /**
   * Optional bulk path (e.g. SQS SendMessageBatch, up to 10 per call). Returns the error
   * per message id that was NOT accepted; every other message counts as published.
   */
  publishBatch?(messages: OutboxMessage[]): Promise<Map<string, string>>;
}

export type EntryPoint = 'http' | 'sqs' | 'worker';

export interface WageringMetrics {
  transactionRecorded(status: string, kind: string, source: EntryPoint): void;
  duplicateDetected(kind: 'idempotent_replay' | 'inbox_redelivery'): void;
  idempotencyConflict(): void;
  lockConflict(): void;
  processingDuration(seconds: number, source: EntryPoint, outcome: string): void;
  reconciliationDivergence(walletId: string): void;
}

/** Structured logger (pino-compatible signature). Never pass full financial payloads. */
export interface AppLogger {
  info(fields: Record<string, unknown>, message: string): void;
  warn(fields: Record<string, unknown>, message: string): void;
  error(fields: Record<string, unknown>, message: string): void;
  debug(fields: Record<string, unknown>, message: string): void;
}

export interface Clock {
  now(): Date;
}

export interface IdGenerator {
  next(): string;
}

export const systemClock: Clock = { now: () => new Date() };
/** UUIDv7: time-ordered, index friendly. */
export const uuidV7: IdGenerator = { next: () => Bun.randomUUIDv7() };
