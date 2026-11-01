import type { MikroORM } from '@mikro-orm/postgresql';
import type { OutboxStore } from '../../application/ports';
import { OutboxMessage } from '../../domain/messaging';

interface OutboxRow {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown> | string;
  occurred_at: Date;
  attempts: number;
  next_attempt_at: Date;
  published_at: Date | null;
}

/**
 * Lease-based claiming: `FOR UPDATE SKIP LOCKED` picks rows nobody else is claiming, and
 * `locked_until` keeps them reserved while we talk to SQS *outside* any DB transaction.
 * If the publisher dies, the lease expires and another instance takes over — so delivery
 * is at-least-once and consumers dedupe by eventId.
 */
export class MikroOrmOutboxStore implements OutboxStore {
  constructor(private readonly orm: MikroORM) {}

  async claim(limit: number, leaseSeconds: number): Promise<OutboxMessage[]> {
    const rows = await this.orm.em.fork().execute<OutboxRow[]>(
      `update outbox_messages o
          set locked_until = now() + make_interval(secs => ?)
        where o.id in (
          select c.id from outbox_messages c
           where c.published_at is null
             and c.next_attempt_at <= now()
             and (c.locked_until is null or c.locked_until < now())
             -- per-aggregate order: never overtake an older event of the same wallet that
             -- is waiting for a retry or is being published by someone else
             and not exists (
               select 1 from outbox_messages older
                where older.aggregate_id = c.aggregate_id
                  and older.published_at is null
                  and older.occurred_at < c.occurred_at
                  and (older.next_attempt_at > now() or older.locked_until >= now()))
           order by c.occurred_at, c.id
           limit ?
           for update skip locked)
       returning o.id, o.aggregate_id, o.event_type, o.payload, o.occurred_at, o.attempts, o.next_attempt_at, o.published_at`,
      [leaseSeconds, limit],
    );
    return rows
      // occurred_at ties (events of one transaction) are broken by the time-ordered UUIDv7 id
      .sort((a, b) => new Date(a.occurred_at).getTime() - new Date(b.occurred_at).getTime() || (a.id < b.id ? -1 : 1))
      .map((row) =>
        OutboxMessage.rehydrate({
          id: row.id,
          aggregateId: row.aggregate_id,
          eventType: row.event_type,
          payload: typeof row.payload === 'string' ? JSON.parse(row.payload) : row.payload,
          occurredAt: new Date(row.occurred_at),
          attempts: row.attempts,
          nextAttemptAt: new Date(row.next_attempt_at),
          publishedAt: row.published_at ? new Date(row.published_at) : undefined,
        }),
      );
  }

  async markPublished(messages: OutboxMessage[]): Promise<void> {
    if (messages.length === 0) return;
    await this.orm.em.fork().execute(
      `update outbox_messages set published_at = ?, locked_until = null where id = any(?::uuid[]) and published_at is null`,
      [messages[0]!.publishedAt, `{${messages.map((m) => m.id).join(',')}}`],
    );
  }

  async scheduleRetry(message: OutboxMessage, error: string): Promise<void> {
    await this.orm.em.fork().execute(
      `update outbox_messages set attempts = ?, next_attempt_at = ?, locked_until = null, last_error = ?
        where id = ? and published_at is null`,
      [message.attempts, message.nextAttemptAt, error.slice(0, 1000), message.id],
    );
  }

  async oldestPendingAgeSeconds(): Promise<number> {
    const [row] = await this.orm.em
      .fork()
      .execute<{ age: string | null }[]>(
        `select extract(epoch from now() - min(occurred_at)) as age from outbox_messages where published_at is null`,
      );
    return row?.age ? Number(row.age) : 0;
  }
}
