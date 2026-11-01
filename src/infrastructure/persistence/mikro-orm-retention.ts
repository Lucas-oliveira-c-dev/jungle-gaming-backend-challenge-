import type { MikroORM } from '@mikro-orm/postgresql';

export interface RetentionPolicy {
  /** Published outbox rows are only needed for audit/debug after this. */
  outboxDays: number;
  /**
   * Must exceed the SQS message retention (max 14 days): a redelivery older than the
   * inbox window would otherwise be processed again. Idempotency keys still protect it.
   */
  inboxDays: number;
  batchSize: number;
}

/**
 * Deletes old *published* outbox rows and old processed inbox rows in small batches, so
 * the job never holds long locks. Never touches unpublished events, wallets, transactions
 * or the ledger (those are append-only and protected by triggers).
 */
export class MikroOrmRetention {
  constructor(
    private readonly orm: MikroORM,
    private readonly policy: RetentionPolicy,
  ) {}

  /** Returns how many rows were deleted in this run (0 when nothing is due). */
  async runOnce(): Promise<number> {
    const em = this.orm.em.fork();
    const outbox = await em.execute(
      `delete from outbox_messages where id in (
         select id from outbox_messages
          where published_at is not null and published_at < now() - make_interval(days => ?)
          limit ?)
       returning id`,
      [this.policy.outboxDays, this.policy.batchSize],
    );
    const inbox = await em.execute(
      `delete from inbox_messages where (consumer_name, message_id) in (
         select consumer_name, message_id from inbox_messages
          where processed_at < now() - make_interval(days => ?)
          limit ?)
       returning message_id`,
      [this.policy.inboxDays, this.policy.batchSize],
    );
    return outbox.length + inbox.length;
  }
}
