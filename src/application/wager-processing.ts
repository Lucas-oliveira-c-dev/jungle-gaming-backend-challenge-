import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
  type EventContext,
  type IntegrationEvent,
} from '../domain/events';
import { applyWager, type WagerOutcome } from '../domain/wagering-policy';
import type { WagerTransaction } from '../domain/wager-transaction';
import type { Wallet } from '../domain/wallet';
import type { IdGenerator, PendingReferenceSchedule, WageringUnitOfWork } from './ports';

export const PENDING_REFERENCE_BACKOFF = { baseDelayMs: 1_000, maxDelayMs: 5 * 60_000 } as const;

/** Exponential backoff for PENDING_REFERENCE re-checks: 1s, 2s, 4s ... capped at 5 min. */
export function nextReferenceCheck(attempts: number, now: Date): Date {
  const delay = Math.min(PENDING_REFERENCE_BACKOFF.maxDelayMs, PENDING_REFERENCE_BACKOFF.baseDelayMs * 2 ** attempts);
  return new Date(now.getTime() + delay);
}

export interface ProcessingContext {
  correlationId: string;
  causationId?: string;
  now: Date;
}

/**
 * Resolves the reference, applies the domain policy and writes every consequence —
 * transaction state, wallet, ledger entry and outbox events — through the same unit of
 * work. Must be called with the wallet row locked. Shared by the submission use case and
 * the PENDING_REFERENCE worker so both follow exactly the same rules.
 */
export async function applyAndRecord(
  uow: WageringUnitOfWork,
  tx: WagerTransaction,
  wallet: Wallet,
  ctx: ProcessingContext,
  ids: IdGenerator,
  mode: { isNew: boolean; referenceAttempts: number },
): Promise<WagerOutcome> {
  const reference = tx.referenceExternalTransactionId
    ? (await uow.findTransactionByExternalId(tx.providerId, tx.referenceExternalTransactionId))?.transaction
    : undefined;
  const alreadyReversed = reference && tx.isReversal() ? await uow.hasProcessedReversal(reference.id) : false;

  const outcome = applyWager(tx, wallet, { reference, alreadyReversed }, { ledgerEntryId: ids.next() }, ctx.now);

  const eventCtx = (): EventContext => ({
    eventId: ids.next(),
    correlationId: ctx.correlationId,
    causationId: ctx.causationId,
    occurredAt: ctx.now,
  });
  const events: IntegrationEvent<unknown>[] = [];
  let schedule: PendingReferenceSchedule | undefined;

  switch (outcome.type) {
    case 'processed':
      if (outcome.entry) {
        await uow.saveWallet(wallet);
        await uow.insertLedgerEntry(outcome.entry);
        events.push(WalletBalanceChanged.from(wallet, outcome.entry, eventCtx()));
      }
      events.push(WagerTransactionProcessed.from(tx, wallet, eventCtx()));
      await uow.wakePendingDependents(tx.providerId, tx.externalTransactionId);
      break;
    case 'rejected':
      events.push(WagerTransactionRejected.from(tx, wallet, eventCtx()));
      // A rejected reference is final too: dependents can now be rejected right away.
      await uow.wakePendingDependents(tx.providerId, tx.externalTransactionId);
      break;
    case 'pending_reference':
      schedule = { attempts: mode.referenceAttempts, nextCheckAt: nextReferenceCheck(mode.referenceAttempts, ctx.now) };
      if (mode.isNew) events.push(WagerTransactionPendingReference.from(tx, eventCtx()));
      break;
  }

  if (mode.isNew) await uow.insertTransaction(tx, wallet.balance, schedule);
  else await uow.updateTransaction(tx, wallet.balance, schedule);
  await uow.enqueue(events);
  return outcome;
}
