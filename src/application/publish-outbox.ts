import type { OutboxMessage } from '../domain/messaging';
import type { AppLogger, Clock, EventPublisher, OutboxStore } from './ports';

export interface OutboxMetrics {
  published(eventType: string): void;
  publishFailed(eventType: string): void;
  retry(component: 'outbox' | 'consumer'): void;
}

/**
 * Publishes leased outbox rows. A crash between "published to SQS" and "marked as
 * published" re-publishes after the lease expires: delivery is at-least-once and every
 * event carries a stable eventId for consumer-side dedup (also used as the SQS FIFO
 * deduplication id).
 */
export class PublishOutbox {
  constructor(
    private readonly outbox: OutboxStore,
    private readonly publisher: EventPublisher,
    private readonly clock: Clock,
    private readonly metrics: OutboxMetrics,
    private readonly logger: AppLogger,
    private readonly options: { batchSize: number; leaseSeconds: number },
  ) {}

  async runOnce(): Promise<number> {
    const messages = await this.outbox.claim(this.options.batchSize, this.options.leaseSeconds);
    if (messages.length === 0) return 0;

    const failures = await this.send(messages);
    const now = this.clock.now();
    const published: OutboxMessage[] = [];
    for (const message of messages) {
      const error = failures.get(message.id);
      if (error === undefined) {
        message.markPublished(now);
        published.push(message);
        this.metrics.published(message.eventType);
        continue;
      }
      message.scheduleRetry(now);
      this.metrics.publishFailed(message.eventType);
      this.metrics.retry('outbox');
      this.logger.warn(
        { eventId: message.id, walletId: message.aggregateId, eventType: message.eventType, attempts: message.attempts, err: error },
        'outbox publish failed, retry scheduled',
      );
      await this.outbox.scheduleRetry(message, error).catch(() => undefined);
    }
    await this.outbox.markPublished(published);
    return published.length;
  }

  /** Returns the error per message id that was not published. */
  private async send(messages: OutboxMessage[]): Promise<Map<string, string>> {
    if (this.publisher.publishBatch) {
      try {
        return await this.publisher.publishBatch(messages);
      } catch (error) {
        return new Map(messages.map((m) => [m.id, (error as Error).message]));
      }
    }
    const failures = new Map<string, string>();
    for (const message of messages) {
      await this.publisher.publish(message).catch((error) => failures.set(message.id, (error as Error).message));
    }
    return failures;
  }
}
