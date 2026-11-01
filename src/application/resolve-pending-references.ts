import { FailureCode } from '../domain/failure-codes';
import { WagerTransactionRejected } from '../domain/events';
import { WagerTransactionStatus } from '../domain/wager-transaction';
import type { AppLogger, Clock, IdGenerator, WageringMetrics, WageringStore } from './ports';
import { applyAndRecord } from './wager-processing';

/**
 * Re-processes PENDING_REFERENCE transactions whose next check is due. Candidates are read
 * without locks; each one is then re-read under its wallet lock, so concurrent workers on
 * several instances can never apply the same transaction twice.
 */
export class ResolvePendingReferences {
  constructor(
    private readonly store: WageringStore,
    private readonly ids: IdGenerator,
    private readonly clock: Clock,
    private readonly metrics: WageringMetrics,
    private readonly logger: AppLogger,
    private readonly maxAttempts: number,
  ) {}

  /** Returns how many transactions left PENDING_REFERENCE in this run. */
  async runOnce(batchSize = 50): Promise<number> {
    const due = await this.store.findDuePendingReferences(batchSize);
    let resolved = 0;
    for (const candidate of due) {
      try {
        if (await this.resolve(candidate.id, candidate.walletId)) resolved += 1;
      } catch (error) {
        if (this.store.isLockConflict(error)) this.metrics.lockConflict();
        if (this.store.isTransient(error)) {
          this.logger.warn(
            { transactionId: candidate.id, walletId: candidate.walletId, err: (error as Error).message },
            'pending reference resolution failed transiently, will retry',
          );
        } else {
          await this.markFailed(candidate.id, candidate.walletId, error);
        }
      }
    }
    return resolved;
  }

  /** A non-transient error would loop forever: park the transaction as FAILED (auditable). */
  private async markFailed(transactionId: string, walletId: string, cause: unknown): Promise<void> {
    this.logger.error({ transactionId, walletId, err: (cause as Error).message }, 'pending reference permanently failed');
    await this.store
      .transactional(async (uow) => {
        const wallet = await uow.lockWallet(walletId);
        const stored = await uow.findTransactionById(transactionId);
        if (!wallet || stored?.transaction.status !== WagerTransactionStatus.PendingReference) return;
        stored.transaction.fail(FailureCode.ProcessingFailed, uow.now());
        await uow.updateTransaction(stored.transaction, wallet.balance);
        await uow.wakePendingDependents(stored.transaction.walletId, stored.transaction.providerId, stored.transaction.externalTransactionId);
        this.metrics.transactionRecorded(stored.transaction.status, stored.transaction.kind, 'worker');
      })
      .catch((error) => this.logger.error({ transactionId, err: (error as Error).message }, 'could not mark transaction as FAILED'));
  }

  private async resolve(transactionId: string, walletId: string): Promise<boolean> {
    const resolved = await this.store.transactional(async (uow) => {
      const wallet = await uow.lockWallet(walletId);
      const stored = await uow.findTransactionById(transactionId);
      if (!wallet || !stored) return undefined;
      const { transaction: tx } = stored;
      const now = uow.now();
      if (tx.status !== WagerTransactionStatus.PendingReference) return undefined; // someone else got it

      const correlationId = `pending-reference:${tx.id}`;
      const outcome = await applyAndRecord(uow, tx, wallet, { correlationId, causationId: tx.id, now }, this.ids, {
        isNew: false,
        previousAttempts: stored.referenceAttempts,
      });
      const attempts = outcome.referenceAttempts;

      if (outcome.type === 'pending_reference') {
        if (attempts < this.maxAttempts) return undefined;
        // Retry budget exhausted: the reference never arrived.
        tx.reject(FailureCode.ReferenceNotFound, now);
        await uow.updateTransaction(tx, wallet.balance);
        await uow.enqueue([
          WagerTransactionRejected.from(tx, wallet, { eventId: this.ids.next(), correlationId, causationId: tx.id, occurredAt: now }),
        ]);
        // Transactions waiting on this one can now be rejected right away.
        await uow.wakePendingDependents(tx.walletId, tx.providerId, tx.externalTransactionId);
      }

      return { tx, attempts };
    });
    if (!resolved) return false;
    // After COMMIT only.
    const { tx, attempts } = resolved;
    this.metrics.transactionRecorded(tx.status, tx.kind, 'worker');
    this.logger.info(
      { correlationId: `pending-reference:${tx.id}`, transactionId: tx.id, walletId, providerId: tx.providerId, status: tx.status, failureCode: tx.failureCode, attempts },
      'pending reference resolved',
    );
    return true;
  }
}
