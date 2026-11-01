import { FailureCode } from './failure-codes';
import { LedgerDirection, WalletLedgerEntry } from './ledger-entry';
import {
  ALLOWED_REFERENCE_KINDS,
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from './wager-transaction';
import { Wallet } from './wallet';

export interface ReferenceContext {
  /** Resolved by (providerId, referenceExternalTransactionId); undefined if not received yet. */
  reference?: WagerTransaction;
  /** Whether a PROCESSED REFUND/ROLLBACK already targets the reference. */
  alreadyReversed: boolean;
}

export type WagerOutcome =
  | { type: 'processed'; entry?: WalletLedgerEntry; reference?: WagerTransaction }
  | { type: 'rejected'; code: FailureCode }
  /** referenceFound: the reference exists but is itself still pending (not "missing"). */
  | { type: 'pending_reference'; referenceFound: boolean };

/**
 * Applies a wager transaction to its (already locked) wallet. Pure domain logic: it
 * mutates the transaction and the wallet in memory and tells the caller what happened;
 * persistence is the caller's job.
 */
export function applyWager(
  tx: WagerTransaction,
  wallet: Wallet,
  context: ReferenceContext,
  ids: { ledgerEntryId: string },
  now: Date,
): WagerOutcome {
  const reject = (code: FailureCode, referenceId?: string): WagerOutcome => {
    tx.reject(code, now, referenceId);
    return { type: 'rejected', code };
  };

  if (tx.playerId !== wallet.playerId) return reject(FailureCode.PlayerWalletMismatch);
  if (tx.money.currency !== wallet.currency) return reject(FailureCode.CurrencyMismatch);

  const { reference } = context;
  if (tx.referenceExternalTransactionId) {
    if (
      !reference ||
      reference.status === WagerTransactionStatus.Pending ||
      reference.status === WagerTransactionStatus.PendingReference
    ) {
      if (tx.status === WagerTransactionStatus.Pending) tx.markPendingReference();
      return { type: 'pending_reference', referenceFound: reference !== undefined };
    }
    const code = validateReference(tx, reference, context.alreadyReversed);
    if (code) return reject(code, reference.id);
  }

  if (!tx.affectsBalance()) {
    tx.markProcessed(reference?.id, now);
    return { type: 'processed', reference };
  }

  const direction = tx.ledgerDirectionFor(reference);
  const movement = { entryId: ids.ledgerEntryId, transactionId: tx.id, money: tx.money, at: now };
  if (direction === LedgerDirection.Debit && !wallet.canDebit(tx.money)) {
    return reject(tx.isReversal() ? FailureCode.ReversalInsufficientFunds : FailureCode.InsufficientFunds, reference?.id);
  }
  const entry = direction === LedgerDirection.Debit ? wallet.debit(movement) : wallet.credit(movement);
  tx.markProcessed(reference?.id, now);
  return { type: 'processed', entry, reference };
}

function validateReference(
  tx: WagerTransaction,
  reference: WagerTransaction,
  alreadyReversed: boolean,
): FailureCode | undefined {
  if (!ALLOWED_REFERENCE_KINDS[tx.kind]?.includes(reference.kind)) return FailureCode.ReferenceKindNotAllowed;
  if (
    reference.providerId !== tx.providerId ||
    reference.playerId !== tx.playerId ||
    reference.walletId !== tx.walletId ||
    reference.money.currency !== tx.money.currency ||
    reference.roundId !== tx.roundId
  ) {
    return FailureCode.ReferenceMismatch;
  }
  if (reference.status !== WagerTransactionStatus.Processed) return FailureCode.ReferenceNotProcessed;
  if (tx.isReversal() && !reference.money.equals(tx.money)) return FailureCode.ReversalAmountMismatch;
  // A reversed BET is cancelled: it can't be reversed again, nor settled by a WIN/LOSS.
  if (alreadyReversed) return FailureCode.ReferenceAlreadyReversed;
  return undefined;
}

export function isReversalKind(kind: WagerTransactionKind): boolean {
  return kind === WagerTransactionKind.Refund || kind === WagerTransactionKind.Rollback;
}
