import { InboxMessage } from '../domain/messaging';
import type { MoneyProps } from '../domain/money';
import type { FailureCode } from '../domain/failure-codes';
import { WagerTransaction, WagerTransactionStatus } from '../domain/wager-transaction';
import { IdempotencyConflictError, TransientInfrastructureError, WalletNotFoundError } from './errors';
import type {
  AppLogger,
  Clock,
  EntryPoint,
  IdGenerator,
  StoredTransaction,
  WageringMetrics,
  WageringStore,
} from './ports';
import { applyAndRecord } from './wager-processing';
import type { ValidatedWagerSubmission } from './wager-request';

export interface SubmitContext {
  correlationId: string;
  source: EntryPoint;
  causationId?: string;
  /** Present when the submission comes from SQS: deduplicated in the same SQL transaction. */
  inbox?: { consumerName: string; messageId: string; payloadHash: string };
}

export interface SubmitResult {
  transactionId: string;
  status: WagerTransactionStatus;
  balance?: MoneyProps;
  failureCode?: FailureCode;
  idempotentReplay: boolean;
}

/**
 * The single entry point for wager transactions (HTTP and SQS).
 *
 * Correctness does not depend on the fast-path read: the authoritative checks happen
 * under the wallet row lock, and the unique constraints are the last line of defence
 * (a violation rolls everything back and turns into a replay or a conflict).
 */
export class SubmitWagerTransaction {
  constructor(
    private readonly store: WageringStore,
    private readonly ids: IdGenerator,
    private readonly clock: Clock,
    private readonly metrics: WageringMetrics,
    private readonly logger: AppLogger,
  ) {}

  async execute(submission: ValidatedWagerSubmission, ctx: SubmitContext): Promise<SubmitResult> {
    const started = performance.now();
    const { request, idempotencyKey, payloadHash } = submission;
    const logFields = {
      correlationId: ctx.correlationId,
      messageId: ctx.inbox?.messageId,
      walletId: request.walletId,
      providerId: request.providerId,
      externalTransactionId: request.externalTransactionId,
      source: ctx.source,
    };

    try {
      // Fast path (no lock): most retries end here.
      const known = await this.store.findTransactionByIdempotencyKey(idempotencyKey);
      if (known && !ctx.inbox) return this.replay(known, payloadHash, logFields);

      const result = await this.store.transactional(async (uow) => {
        const wallet = await uow.lockWallet(request.walletId);
        if (!wallet) throw new WalletNotFoundError(`Wallet ${request.walletId} not found`);

        if (ctx.inbox) {
          const inbox = InboxMessage.receive({ ...ctx.inbox, receivedAt: uow.now() });
          inbox.markProcessed(uow.now());
          if (!(await uow.registerInbox(inbox))) {
            this.metrics.duplicateDetected('inbox_redelivery');
            const previous = await uow.findTransactionByIdempotencyKey(idempotencyKey);
            if (!previous) throw new IdempotencyConflictError(`messageId ${ctx.inbox.messageId} was already used for another payload`);
            return this.replay(previous, payloadHash, logFields);
          }
        }

        // Authoritative idempotency check, serialized by the wallet lock.
        const existing =
          (await uow.findTransactionByIdempotencyKey(idempotencyKey)) ??
          (await uow.findTransactionByExternalId(request.providerId, request.externalTransactionId));
        if (existing) return this.replay(existing, payloadHash, logFields, idempotencyKey);

        const now = uow.now();
        const tx = WagerTransaction.create({
          id: this.ids.next(),
          ...request,
          idempotencyKey,
          payloadHash,
          createdAt: now,
        });
        await applyAndRecord(uow, tx, wallet, { correlationId: ctx.correlationId, causationId: ctx.causationId, now }, this.ids, {
          isNew: true,
          referenceAttempts: 0,
        });
        this.metrics.transactionRecorded(tx.status, tx.kind, ctx.source);
        this.logger.info(
          { ...logFields, transactionId: tx.id, kind: tx.kind, status: tx.status, failureCode: tx.failureCode },
          'wager transaction recorded',
        );
        return {
          transactionId: tx.id,
          status: tx.status,
          balance: wallet.balance.toJSON(),
          ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
          idempotentReplay: false,
        } satisfies SubmitResult;
      });
      this.metrics.processingDuration((performance.now() - started) / 1000, ctx.source, result.status);
      return result;
    } catch (error) {
      if (this.store.isUniqueViolation(error)) {
        // Lost a race on a unique constraint (e.g. same key on another wallet): everything
        // was rolled back; answer from what the winner committed.
        const winner = await this.store.findTransactionByIdempotencyKey(idempotencyKey);
        if (winner) return this.replay(winner, payloadHash, logFields);
        throw new IdempotencyConflictError('Provider transaction id already used with another idempotency key');
      }
      if (this.store.isLockConflict(error)) this.metrics.lockConflict();
      if (this.store.isTransient(error)) {
        this.logger.warn({ ...logFields, err: (error as Error).message }, 'transient failure while processing wager');
        throw new TransientInfrastructureError('Temporarily unable to process the transaction, retry later');
      }
      throw error;
    }
  }

  private replay(
    stored: StoredTransaction,
    payloadHash: string,
    logFields: Record<string, unknown>,
    idempotencyKey?: string,
  ): SubmitResult {
    const { transaction } = stored;
    if (!transaction.matchesPayload(payloadHash) || (idempotencyKey && transaction.idempotencyKey !== idempotencyKey)) {
      this.metrics.idempotencyConflict();
      this.logger.warn({ ...logFields, transactionId: transaction.id }, 'idempotency conflict');
      throw new IdempotencyConflictError(
        'Idempotency key (or provider transaction id) already used with a different payload',
        { transactionId: transaction.id },
      );
    }
    this.metrics.duplicateDetected('idempotent_replay');
    return {
      transactionId: transaction.id,
      status: transaction.status,
      ...(stored.resultBalance ? { balance: stored.resultBalance.toJSON() } : {}),
      ...(transaction.failureCode ? { failureCode: transaction.failureCode } : {}),
      idempotentReplay: true,
    };
  }
}
