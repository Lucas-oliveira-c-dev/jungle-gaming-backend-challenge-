import { LockMode, type EntityManager, type MikroORM } from '@mikro-orm/postgresql';
import type {
  PendingReferenceSchedule,
  StoredTransaction,
  WageringStore,
  WageringUnitOfWork,
} from '../../application/ports';
import type { IntegrationEvent } from '../../domain/events';
import type { WalletLedgerEntry } from '../../domain/ledger-entry';
import type { Money } from '../../domain/money';
import { OutboxMessage, type InboxMessage } from '../../domain/messaging';
import { WagerTransactionStatus, type WagerTransaction } from '../../domain/wager-transaction';
import type { Wallet } from '../../domain/wallet';
import { isLockConflict, isTransientInfrastructureError, isUniqueViolation } from './errors';
import {
  ledgerEntryToRecordData,
  transactionStateData,
  transactionToDomain,
  transactionToRecordData,
  walletToDomain,
  walletToRecordData,
  toStoredTransaction,
} from './mappers';
import { LedgerEntryRecord, OutboxRecord, WagerTransactionRecord, WalletRecord } from './records';


/**
 * Unit of Work bound to one forked EntityManager inside `em.transactional()`.
 * Inserts go through the identity map and are flushed on commit; FKs are DEFERRABLE so
 * MikroORM's insert ordering never matters.
 */
class MikroOrmWageringUnitOfWork implements WageringUnitOfWork {
  constructor(private readonly em: EntityManager) {}

  async lockWallet(walletId: string): Promise<Wallet | null> {
    const record = await this.em.findOne(WalletRecord, { id: walletId }, { lockMode: LockMode.PESSIMISTIC_WRITE, refresh: true });
    return record ? walletToDomain(record) : null;
  }

  async insertWallet(wallet: Wallet): Promise<void> {
    this.em.persist(this.em.create(WalletRecord, walletToRecordData(wallet)));
  }

  async saveWallet(wallet: Wallet): Promise<void> {
    // Already managed (loaded by lockWallet) — the UoW computes the change set.
    const record = this.em.getReference(WalletRecord, wallet.id);
    record.balance = wallet.balance.toString();
    record.version = wallet.version;
    record.updatedAt = wallet.updatedAt;
  }

  async findTransactionByIdempotencyKey(key: string): Promise<StoredTransaction | null> {
    const record = await this.em.findOne(WagerTransactionRecord, { idempotencyKey: key }, { refresh: true });
    return record ? toStoredTransaction(record) : null;
  }

  async findTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<StoredTransaction | null> {
    const record = await this.em.findOne(WagerTransactionRecord, { providerId, externalTransactionId }, { refresh: true });
    return record ? toStoredTransaction(record) : null;
  }

  async findTransactionById(id: string): Promise<StoredTransaction | null> {
    const record = await this.em.findOne(WagerTransactionRecord, { id }, { refresh: true });
    return record ? toStoredTransaction(record) : null;
  }

  async hasProcessedReversal(referenceTransactionId: string): Promise<boolean> {
    const count = await this.em.count(WagerTransactionRecord, {
      referenceTransactionId,
      kind: { $in: ['REFUND', 'ROLLBACK'] },
      status: WagerTransactionStatus.Processed,
    });
    return count > 0;
  }

  async insertTransaction(tx: WagerTransaction, resultBalance: Money | undefined, schedule?: PendingReferenceSchedule): Promise<void> {
    this.em.persist(
      this.em.create(WagerTransactionRecord, {
        ...transactionToRecordData(tx),
        resultBalance: resultBalance?.toString() ?? null,
        referenceAttempts: schedule?.attempts ?? 0,
        nextReferenceCheckAt: schedule?.nextCheckAt ?? null,
      }),
    );
  }

  async updateTransaction(tx: WagerTransaction, resultBalance: Money | undefined, schedule?: PendingReferenceSchedule): Promise<void> {
    const record = await this.em.findOneOrFail(WagerTransactionRecord, { id: tx.id });
    this.em.assign(record, {
      ...transactionStateData(tx),
      resultBalance: resultBalance?.toString() ?? record.resultBalance ?? null,
      referenceAttempts: schedule?.attempts ?? record.referenceAttempts,
      nextReferenceCheckAt: tx.status === WagerTransactionStatus.PendingReference ? (schedule?.nextCheckAt ?? null) : null,
    });
  }

  async wakePendingDependents(providerId: string, externalTransactionId: string): Promise<number> {
    return this.em.nativeUpdate(
      WagerTransactionRecord,
      { providerId, referenceExternalTransactionId: externalTransactionId, status: WagerTransactionStatus.PendingReference },
      { nextReferenceCheckAt: new Date() },
    );
  }

  async insertLedgerEntry(entry: WalletLedgerEntry): Promise<void> {
    this.em.persist(this.em.create(LedgerEntryRecord, ledgerEntryToRecordData(entry)));
  }

  async enqueue(events: IntegrationEvent<unknown>[]): Promise<void> {
    for (const event of events) {
      const message = OutboxMessage.enqueue(event);
      this.em.persist(
        this.em.create(OutboxRecord, {
          id: message.id,
          aggregateId: message.aggregateId,
          eventType: message.eventType,
          payload: { ...message.payload },
          occurredAt: message.occurredAt,
          attempts: message.attempts,
          nextAttemptAt: message.nextAttemptAt ?? message.occurredAt,
          lockedUntil: null,
          publishedAt: null,
          lastError: null,
        }),
      );
    }
  }

  async registerInbox(message: InboxMessage): Promise<boolean> {
    // ON CONFLICT DO NOTHING keeps the surrounding transaction alive on a redelivery;
    // a concurrent insert of the same key blocks here until the other tx commits.
    const rows = await this.em.execute(
      `insert into inbox_messages (consumer_name, message_id, payload_hash, received_at, processed_at)
       values (?, ?, ?, ?, ?)
       on conflict (consumer_name, message_id) do nothing
       returning message_id`,
      [message.consumerName, message.messageId, message.payloadHash, message.receivedAt, message.processedAt ?? null],
    );
    return rows.length === 1;
  }
}

export class MikroOrmWageringStore implements WageringStore {
  constructor(
    private readonly orm: MikroORM,
    private readonly lockTimeoutMs: number,
  ) {}

  async transactional<T>(work: (uow: WageringUnitOfWork) => Promise<T>): Promise<T> {
    return this.orm.em.fork().transactional(async (em) => {
      // Bound the wait on a hot wallet: a timeout surfaces as a transient (retryable) error.
      await em.execute(`set local lock_timeout = '${Math.max(1, Math.trunc(this.lockTimeoutMs))}ms'`);
      return work(new MikroOrmWageringUnitOfWork(em));
    });
  }

  isUniqueViolation(error: unknown): boolean {
    return isUniqueViolation(error);
  }

  isTransient(error: unknown): boolean {
    return isTransientInfrastructureError(error);
  }

  isLockConflict(error: unknown): boolean {
    return isLockConflict(error);
  }

  async findTransactionByIdempotencyKey(key: string): Promise<StoredTransaction | null> {
    const record = await this.orm.em.fork().findOne(WagerTransactionRecord, { idempotencyKey: key });
    return record ? toStoredTransaction(record) : null;
  }

  async findDuePendingReferences(now: Date, limit: number): Promise<{ id: string; walletId: string }[]> {
    const records = await this.orm.em.fork().find(
      WagerTransactionRecord,
      { status: WagerTransactionStatus.PendingReference, nextReferenceCheckAt: { $lte: now } },
      { orderBy: { nextReferenceCheckAt: 'asc' }, limit, fields: ['id', 'walletId'] },
    );
    return records.map((r) => ({ id: r.id, walletId: r.walletId }));
  }
}
