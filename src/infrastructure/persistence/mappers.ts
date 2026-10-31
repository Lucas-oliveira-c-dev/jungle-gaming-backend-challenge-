import type { FailureCode } from '../../domain/failure-codes';
import { LedgerDirection, WalletLedgerEntry } from '../../domain/ledger-entry';
import { Money } from '../../domain/money';
import { WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from '../../domain/wager-transaction';
import { Wallet } from '../../domain/wallet';
import type { StoredTransaction } from '../../application/ports';
import type { LedgerEntryRecord, WagerTransactionRecord, WalletRecord } from './records';

/** numeric(20,2) comes back from pg as a string; currency columns are char(3). */
const money = (amount: string, currency: string): Money => Money.from({ amount, currency: currency.trim() });

export function walletToDomain(r: WalletRecord): Wallet {
  return Wallet.rehydrate({
    id: r.id,
    playerId: r.playerId,
    currency: r.currency.trim(),
    balance: money(r.balance, r.currency),
    version: r.version,
    createdAt: r.createdAt,
    updatedAt: r.updatedAt,
  });
}

export function walletToRecordData(w: Wallet): WalletRecord {
  return {
    id: w.id,
    playerId: w.playerId,
    currency: w.currency,
    balance: w.balance.toString(),
    version: w.version,
    createdAt: w.createdAt,
    updatedAt: w.updatedAt,
  };
}

export function transactionToDomain(r: WagerTransactionRecord): WagerTransaction {
  return WagerTransaction.rehydrate({
    id: r.id,
    providerId: r.providerId,
    externalTransactionId: r.externalTransactionId,
    idempotencyKey: r.idempotencyKey,
    payloadHash: r.payloadHash,
    walletId: r.walletId,
    playerId: r.playerId,
    roundId: r.roundId,
    gameId: r.gameId,
    kind: r.kind as WagerTransactionKind,
    money: money(r.amount, r.currency),
    referenceExternalTransactionId: r.referenceExternalTransactionId ?? undefined,
    createdAt: r.createdAt,
    status: r.status as WagerTransactionStatus,
    referenceTransactionId: r.referenceTransactionId ?? undefined,
    failureCode: (r.failureCode ?? undefined) as FailureCode | undefined,
    processedAt: r.processedAt ?? undefined,
  });
}

/** Mutable state of a transaction (the columns allowed to change after insert). */
export function transactionStateData(tx: WagerTransaction) {
  return {
    status: tx.status,
    referenceTransactionId: tx.referenceTransactionId ?? null,
    failureCode: tx.failureCode ?? null,
    processedAt: tx.processedAt ?? null,
  };
}

export function transactionToRecordData(tx: WagerTransaction): Omit<WagerTransactionRecord, 'resultBalance' | 'referenceAttempts' | 'nextReferenceCheckAt'> {
  return {
    id: tx.id,
    providerId: tx.providerId,
    externalTransactionId: tx.externalTransactionId,
    idempotencyKey: tx.idempotencyKey,
    payloadHash: tx.payloadHash,
    walletId: tx.walletId,
    playerId: tx.playerId,
    roundId: tx.roundId,
    gameId: tx.gameId,
    kind: tx.kind,
    amount: tx.money.toString(),
    currency: tx.money.currency,
    referenceExternalTransactionId: tx.referenceExternalTransactionId ?? null,
    createdAt: tx.createdAt,
    ...transactionStateData(tx),
  };
}

export function ledgerEntryToDomain(r: LedgerEntryRecord): WalletLedgerEntry {
  return WalletLedgerEntry.rehydrate({
    id: r.id,
    walletId: r.walletId,
    transactionId: r.transactionId,
    sequence: r.sequence,
    direction: r.direction as LedgerDirection,
    money: money(r.amount, r.currency),
    balanceBefore: money(r.balanceBefore, r.currency),
    balanceAfter: money(r.balanceAfter, r.currency),
    createdAt: r.createdAt,
  });
}

export function ledgerEntryToRecordData(e: WalletLedgerEntry): LedgerEntryRecord {
  return {
    id: e.id,
    walletId: e.walletId,
    transactionId: e.transactionId,
    sequence: e.sequence,
    direction: e.direction,
    amount: e.money.toString(),
    currency: e.money.currency,
    balanceBefore: e.balanceBefore.toString(),
    balanceAfter: e.balanceAfter.toString(),
    createdAt: e.createdAt,
  };
}

export function toStoredTransaction(record: WagerTransactionRecord): StoredTransaction {
  return {
    transaction: transactionToDomain(record),
    resultBalance: record.resultBalance ? money(record.resultBalance, record.currency) : undefined,
    referenceAttempts: record.referenceAttempts,
  };
}
