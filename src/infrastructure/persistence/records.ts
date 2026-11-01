import { EntitySchema } from '@mikro-orm/core';

/**
 * Persistence records. The domain stays free of ORM types/decorators: records are plain
 * data classes mapped with EntitySchema, and `mappers.ts` converts to/from the domain.
 * Money columns are `numeric(20,2)` mapped as decimal strings — exact, never `number`.
 */

export class WalletRecord {
  id!: string;
  playerId!: string;
  currency!: string;
  balance!: string;
  version!: number;
  createdAt!: Date;
  updatedAt!: Date;
}

export const WalletSchema = new EntitySchema<WalletRecord>({
  class: WalletRecord,
  tableName: 'wallets',
  properties: {
    id: { type: 'uuid', primary: true },
    playerId: { type: 'uuid' },
    currency: { type: 'string', columnType: 'char(3)' },
    balance: { type: 'decimal', columnType: 'numeric(20,2)' },
    version: { type: 'integer' },
    createdAt: { type: 'datetime', columnType: 'timestamptz' },
    updatedAt: { type: 'datetime', columnType: 'timestamptz' },
  },
});

export class WagerTransactionRecord {
  id!: string;
  providerId!: string;
  externalTransactionId!: string;
  idempotencyKey!: string;
  payloadHash!: string;
  walletId!: string;
  playerId!: string;
  roundId!: string;
  gameId!: string;
  kind!: string;
  amount!: string;
  currency!: string;
  referenceExternalTransactionId?: string | null;
  referenceTransactionId?: string | null;
  status!: string;
  failureCode?: string | null;
  resultBalance?: string | null;
  referenceAttempts!: number;
  nextReferenceCheckAt?: Date | null;
  createdAt!: Date;
  processedAt?: Date | null;
}

export const WagerTransactionSchema = new EntitySchema<WagerTransactionRecord>({
  class: WagerTransactionRecord,
  tableName: 'wager_transactions',
  properties: {
    id: { type: 'uuid', primary: true },
    providerId: { type: 'text' },
    externalTransactionId: { type: 'text' },
    idempotencyKey: { type: 'text' },
    payloadHash: { type: 'string', columnType: 'char(64)' },
    walletId: { type: 'uuid' },
    playerId: { type: 'uuid' },
    roundId: { type: 'text' },
    gameId: { type: 'text' },
    kind: { type: 'text' },
    amount: { type: 'decimal', columnType: 'numeric(20,2)' },
    currency: { type: 'string', columnType: 'char(3)' },
    referenceExternalTransactionId: { type: 'text', nullable: true },
    referenceTransactionId: { type: 'uuid', nullable: true },
    status: { type: 'text' },
    failureCode: { type: 'text', nullable: true },
    resultBalance: { type: 'decimal', columnType: 'numeric(20,2)', nullable: true },
    referenceAttempts: { type: 'integer' },
    nextReferenceCheckAt: { type: 'datetime', columnType: 'timestamptz', nullable: true },
    createdAt: { type: 'datetime', columnType: 'timestamptz' },
    processedAt: { type: 'datetime', columnType: 'timestamptz', nullable: true },
  },
});

export class LedgerEntryRecord {
  id!: string;
  walletId!: string;
  transactionId!: string;
  sequence!: number;
  direction!: string;
  amount!: string;
  currency!: string;
  balanceBefore!: string;
  balanceAfter!: string;
  createdAt!: Date;
}

export const LedgerEntrySchema = new EntitySchema<LedgerEntryRecord>({
  class: LedgerEntryRecord,
  tableName: 'wallet_ledger_entries',
  properties: {
    id: { type: 'uuid', primary: true },
    walletId: { type: 'uuid' },
    transactionId: { type: 'uuid' },
    sequence: { type: 'integer' },
    direction: { type: 'text' },
    amount: { type: 'decimal', columnType: 'numeric(20,2)' },
    currency: { type: 'string', columnType: 'char(3)' },
    balanceBefore: { type: 'decimal', columnType: 'numeric(20,2)' },
    balanceAfter: { type: 'decimal', columnType: 'numeric(20,2)' },
    createdAt: { type: 'datetime', columnType: 'timestamptz' },
  },
});

export class OutboxRecord {
  id!: string;
  aggregateId!: string;
  eventType!: string;
  payload!: Record<string, unknown>;
  occurredAt!: Date;
  attempts!: number;
  nextAttemptAt!: Date;
  lockedUntil?: Date | null;
  publishedAt?: Date | null;
  lastError?: string | null;
}

export const OutboxSchema = new EntitySchema<OutboxRecord>({
  class: OutboxRecord,
  tableName: 'outbox_messages',
  properties: {
    id: { type: 'uuid', primary: true },
    aggregateId: { type: 'uuid' },
    eventType: { type: 'text' },
    payload: { type: 'json', columnType: 'jsonb' },
    occurredAt: { type: 'datetime', columnType: 'timestamptz' },
    attempts: { type: 'integer' },
    nextAttemptAt: { type: 'datetime', columnType: 'timestamptz' },
    lockedUntil: { type: 'datetime', columnType: 'timestamptz', nullable: true },
    publishedAt: { type: 'datetime', columnType: 'timestamptz', nullable: true },
    lastError: { type: 'text', nullable: true },
  },
});


export class JournalEntryRecord {
  id!: string;
  transactionId!: string;
  createdAt!: Date;
}

export const JournalEntrySchema = new EntitySchema<JournalEntryRecord>({
  class: JournalEntryRecord,
  tableName: 'journal_entries',
  properties: {
    id: { type: 'uuid', primary: true },
    transactionId: { type: 'uuid' },
    createdAt: { type: 'datetime', columnType: 'timestamptz' },
  },
});

export class JournalPostingRecord {
  id!: string;
  journalEntryId!: string;
  accountCode!: string;
  direction!: string;
  amount!: string;
  currency!: string;
  createdAt!: Date;
}

export const JournalPostingSchema = new EntitySchema<JournalPostingRecord>({
  class: JournalPostingRecord,
  tableName: 'journal_postings',
  properties: {
    id: { type: 'uuid', primary: true },
    journalEntryId: { type: 'uuid' },
    accountCode: { type: 'text' },
    direction: { type: 'text' },
    amount: { type: 'decimal', columnType: 'numeric(20,2)' },
    currency: { type: 'string', columnType: 'char(3)' },
    createdAt: { type: 'datetime', columnType: 'timestamptz' },
  },
});

export const ENTITY_SCHEMAS = [WalletSchema, WagerTransactionSchema, LedgerEntrySchema, OutboxSchema, JournalEntrySchema, JournalPostingSchema];
