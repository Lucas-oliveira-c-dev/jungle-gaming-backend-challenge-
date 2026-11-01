import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { z } from 'zod';
import { IdempotencyConflictError, ValidationError, WalletNotFoundError } from '../../application/errors';
import { DomainError } from '../../domain/errors';
import type { AppLogger } from '../../application/ports';
import type { SubmitWagerTransaction } from '../../application/submit-wager-transaction';
import { canonicalJson, parseWagerSubmission, sha256 } from '../../application/wager-request';
import { WagerTransactionStatus } from '../../domain/wager-transaction';
import type { ConsumerMetrics } from '../observability/metrics';

const envelopeSchema = z.object({
  messageId: z.string().min(1).max(128),
  type: z.literal('WagerTransactionRequested'),
  occurredAt: z.string().datetime({ offset: true }),
  data: z.object({ idempotencyKey: z.string() }).passthrough(),
});

export interface ConsumerOptions {
  consumerName: string;
  queueUrl: string;
  dlqUrl: string;
  concurrency: number;
  maxReceiveCount: number;
  visibilityTimeoutSeconds: number;
  waitTimeSeconds: number;
  /** Fault injection for crash tests: called after commit, before the ack. */
  afterCommit?: () => void;
}

type Disposition = 'processed' | 'replay' | 'business_rejection' | 'retry' | 'dlq';

/**
 * Error policy:
 *  - business outcome (REJECTED, unknown wallet)       -> ack (delete)
 *  - permanent (malformed, idempotency conflict)        -> DLQ + delete
 *  - transient / unexpected                             -> leave on the queue with a
 *    backoff visibility; after maxReceiveCount attempts -> DLQ + delete
 * The ack is only sent after the SQL transaction committed (execute() has returned).
 */
export class WagerTransactionConsumer {
  private stopping = false;
  private readonly abort = new AbortController();
  private loops: Promise<void>[] = [];
  private inFlight = 0;

  constructor(
    private readonly sqs: SQSClient,
    private readonly submit: SubmitWagerTransaction,
    private readonly metrics: ConsumerMetrics,
    private readonly logger: AppLogger,
    private readonly options: ConsumerOptions,
  ) {}

  start(): void {
    for (let i = 0; i < this.options.concurrency; i++) this.loops.push(this.loop(i));
    this.logger.info({ concurrency: this.options.concurrency, consumer: this.options.consumerName }, 'sqs consumer started');
  }

  /**
   * Graceful shutdown (SIGTERM): stop polling, finish the message being processed by each
   * loop and give back the visibility of the ones not started yet.
   */
  async stop(): Promise<void> {
    this.stopping = true;
    this.abort.abort();
    await Promise.allSettled(this.loops);
    this.logger.info({ consumer: this.options.consumerName }, 'sqs consumer stopped');
  }

  get busy(): number {
    return this.inFlight;
  }

  private async loop(index: number): Promise<void> {
    while (!this.stopping) {
      let messages: Message[] = [];
      try {
        const response = await this.sqs.send(
          new ReceiveMessageCommand({
            QueueUrl: this.options.queueUrl,
            MaxNumberOfMessages: 10,
            WaitTimeSeconds: this.options.waitTimeSeconds,
            VisibilityTimeout: this.options.visibilityTimeoutSeconds,
            MessageSystemAttributeNames: ['ApproximateReceiveCount', 'MessageGroupId'],
            MessageAttributeNames: ['All'],
          }),
          { abortSignal: this.abort.signal },
        );
        messages = response.Messages ?? [];
      } catch (error) {
        if (this.stopping) break;
        this.logger.warn({ loop: index, err: (error as Error).message }, 'sqs receive failed');
        await Bun.sleep(1_000);
        continue;
      }

      await this.processBatch(index, messages);
    }
  }

  /**
   * Messages of one FIFO group arrive in order and are processed sequentially. If one is
   * left for retry, the rest of its group in this batch is deferred with it, so order holds.
   * Visibility is extended when a slow batch (hot wallet) approaches the timeout.
   */
  private async processBatch(loopIndex: number, messages: Message[]): Promise<void> {
    const deferredGroups = new Map<string, number>(); // group -> retry delay (s)
    let leaseStartedAt = Date.now();
    for (const [position, message] of messages.entries()) {
      if (this.stopping) {
        await this.changeVisibility(messages.slice(position), 0);
        return;
      }
      const group = message.Attributes?.MessageGroupId ?? '';
      const deferral = deferredGroups.get(group);
      if (deferral !== undefined) {
        await this.changeVisibility([message], deferral);
        continue;
      }
      if (Date.now() - leaseStartedAt > this.options.visibilityTimeoutSeconds * 600) {
        await this.changeVisibility(messages.slice(position), this.options.visibilityTimeoutSeconds);
        leaseStartedAt = Date.now();
      }
      this.inFlight += 1;
      try {
        const outcome = await this.handle(message);
        if (outcome.retryInSeconds !== undefined) deferredGroups.set(group, outcome.retryInSeconds);
      } catch (error) {
        // Never let one message kill the loop; it will be redelivered after its visibility.
        this.logger.error({ loop: loopIndex, sqsMessageId: message.MessageId, err: (error as Error).message }, 'unexpected consumer error');
      } finally {
        this.inFlight -= 1;
      }
    }
  }

  private async handle(message: Message): Promise<{ retryInSeconds?: number }> {
    const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? '1');
    const fields: Record<string, unknown> = { sqsMessageId: message.MessageId, receiveCount };
    let disposition: Disposition;
    try {
      const body = JSON.parse(message.Body ?? '');
      const envelope = envelopeSchema.safeParse(body);
      if (!envelope.success) throw new ValidationError('Invalid message envelope', envelope.error.flatten());
      const { idempotencyKey, ...data } = envelope.data.data;
      const submission = parseWagerSubmission(data, idempotencyKey);
      const correlationId = message.MessageAttributes?.correlationId?.StringValue ?? envelope.data.messageId;
      Object.assign(fields, {
        messageId: envelope.data.messageId,
        correlationId,
        walletId: submission.request.walletId,
        providerId: submission.request.providerId,
      });

      const result = await this.submit.execute(submission, {
        source: 'sqs',
        correlationId,
        causationId: envelope.data.messageId,
        inbox: { consumerName: this.options.consumerName, messageId: envelope.data.messageId, payloadHash: sha256(canonicalJson(body)) },
      });
      this.options.afterCommit?.();
      fields.transactionId = result.transactionId;
      disposition = result.idempotentReplay
        ? 'replay'
        : result.status === WagerTransactionStatus.Rejected
          ? 'business_rejection'
          : 'processed';
    } catch (error) {
      const failure = await this.handleFailure(message, error, receiveCount, fields);
      this.metrics.messageHandled(failure.disposition);
      return failure;
    }
    // Committed. A failed ack is harmless: the redelivery is absorbed by the inbox, so it
    // must not be treated as a processing failure (which could send it to the DLQ).
    await this.ack(message).catch((error) =>
      this.logger.warn({ ...fields, err: (error as Error).message }, 'ack failed after commit; redelivery will be deduplicated'),
    );
    this.metrics.messageHandled(disposition);
    return {};
  }

  private async handleFailure(
    message: Message,
    error: unknown,
    receiveCount: number,
    fields: Record<string, unknown>,
  ): Promise<{ disposition: Disposition; retryInSeconds?: number }> {
    const permanent =
      error instanceof SyntaxError || error instanceof ValidationError || error instanceof IdempotencyConflictError || error instanceof DomainError;
    if (permanent) {
      const reason =
        error instanceof IdempotencyConflictError ? error.code : error instanceof DomainError ? 'DOMAIN_INVARIANT' : 'INVALID_PAYLOAD';
      this.logger.warn({ ...fields, reason, err: (error as Error).message }, 'permanent message failure, moving to DLQ');
      return this.moveToDlqOrRetry(message, reason, fields);
    }
    if (error instanceof WalletNotFoundError) {
      this.logger.warn({ ...fields, reason: error.code }, 'business rejection, acking');
      await this.ack(message).catch(() => undefined); // if it fails, the redelivery is rejected again
      return { disposition: 'business_rejection' };
    }
    if (receiveCount >= this.options.maxReceiveCount) {
      this.logger.error({ ...fields, err: (error as Error).message }, 'retry budget exhausted, moving to DLQ');
      return this.moveToDlqOrRetry(message, 'MAX_RECEIVE_COUNT_EXCEEDED', fields);
    }
    // Transient: let it come back after an exponential backoff (2s, 4s, 8s ... max 60s).
    const delay = Math.min(60, 2 ** receiveCount);
    this.metrics.retry('consumer');
    this.logger.warn({ ...fields, retryInSeconds: delay, err: (error as Error).message }, 'transient failure, message will be retried');
    await this.changeVisibility([message], delay);
    return { disposition: 'retry', retryInSeconds: delay };
  }

  /** If the broker itself fails while dead-lettering, leave the message for redelivery. */
  private async moveToDlqOrRetry(message: Message, reason: string, fields: Record<string, unknown>) {
    try {
      await this.moveToDlq(message, reason);
      return { disposition: 'dlq' as const };
    } catch (error) {
      this.logger.warn({ ...fields, reason, err: (error as Error).message }, 'could not move message to DLQ, will retry');
      this.metrics.retry('consumer');
      return { disposition: 'retry' as const, retryInSeconds: this.options.visibilityTimeoutSeconds };
    }
  }

  private async ack(message: Message): Promise<void> {
    await this.sqs.send(new DeleteMessageCommand({ QueueUrl: this.options.queueUrl, ReceiptHandle: message.ReceiptHandle! }));
  }

  private async moveToDlq(message: Message, reason: string): Promise<void> {
    await this.sqs.send(
      new SendMessageCommand({
        QueueUrl: this.options.dlqUrl,
        MessageBody: message.Body ?? '',
        MessageGroupId: message.Attributes?.MessageGroupId ?? 'dlq',
        MessageDeduplicationId: message.MessageId!,
        MessageAttributes: {
          failureReason: { DataType: 'String', StringValue: reason },
          sourceMessageId: { DataType: 'String', StringValue: message.MessageId! },
        },
      }),
    );
    this.metrics.movedToDlq(reason);
    await this.ack(message);
  }

  private async changeVisibility(messages: Message[], seconds: number): Promise<void> {
    await Promise.allSettled(
      messages.map((m) =>
        this.sqs.send(
          new ChangeMessageVisibilityCommand({ QueueUrl: this.options.queueUrl, ReceiptHandle: m.ReceiptHandle!, VisibilityTimeout: seconds }),
        ),
      ),
    );
  }
}
