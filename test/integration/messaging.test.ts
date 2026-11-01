import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { TransientInfrastructureError } from '../../src/application/errors';
import { systemClock, uuidV7, type WageringStore } from '../../src/application/ports';
import { PublishOutbox } from '../../src/application/publish-outbox';
import { SubmitWagerTransaction } from '../../src/application/submit-wager-transaction';
import { parseWagerSubmission } from '../../src/application/wager-request';
import { ensureQueues } from '../../src/infrastructure/messaging/sqs';
import { WagerTransactionConsumer } from '../../src/infrastructure/messaging/wager-transaction-consumer';
import { PrometheusMetrics } from '../../src/infrastructure/observability/metrics';
import { MikroOrmOutboxStore } from '../../src/infrastructure/persistence/mikro-orm-outbox-store';
import { MikroOrmWageringStore } from '../../src/infrastructure/persistence/mikro-orm-wagering-store';
import { silentLogger } from '../support/in-memory-store';
import {
  createWallet,
  drain,
  eventually,
  expectLedgerConsistent,
  keyOf,
  purge,
  sendWagerMessage,
  sql,
  startTestApp,
  testConfig,
  wager,
  type TestApp,
} from '../support/harness';

describe('atomicity, SQS consumer and transactional outbox', () => {
  let t: TestApp;

  beforeAll(async () => {
    t = await startTestApp('messaging', { WORKER_OUTBOX: 'false', SQS_MAX_RECEIVE_COUNT: '3' });
  });
  beforeEach(async () => purge(t.app.sqs, t.app.queues.transactions, t.app.queues.transactionsDlq, t.app.queues.events));
  afterAll(async () => {
    if (t) {
      await expectLedgerConsistent(t.app.orm);
      await t.close();
    }
  });

  const countRows = async (table: string, column: string, value: string) =>
    Number((await sql<{ n: string }>(t.app.orm, `select count(*)::text as n from ${table} where ${column} = ?`, [value]))[0]?.n);

  test('wallet, transaction, ledger, inbox and outbox commit or roll back together', async () => {
    const w = await createWallet(baseUrl(), '100.00');
    const real = new MikroOrmWageringStore(t.app.orm, 2_000);
    // Same store, but the outbox write fails after everything else was staged.
    const failing: WageringStore = Object.assign(Object.create(real), {
      transactional: (work: Parameters<WageringStore['transactional']>[0]) =>
        real.transactional((uow) =>
          work(
            Object.assign(Object.create(uow), {
              recordMovement: uow.recordMovement.bind(uow),
              enqueue: async () => {
                throw new Error('boom: outbox unavailable');
              },
            }),
          ),
        ),
    });
    const submit = new SubmitWagerTransaction(failing, uuidV7, systemClock, new PrometheusMetrics(), silentLogger);
    const body = wager(w, 'BET', '30.00');
    const inbox = { consumerName: 'atomicity', messageId: crypto.randomUUID(), payloadHash: 'x' };
    await expect(submit.execute(parseWagerSubmission(body, keyOf(body)), { source: 'sqs', correlationId: 'c', inbox })).rejects.toThrow('boom');

    expect(await countRows('wager_transactions', 'external_transaction_id', body.externalTransactionId)).toBe(0);
    expect(await countRows('wallet_ledger_entries', 'wallet_id', w.walletId)).toBe(1); // opening only
    expect(await countRows('inbox_messages', 'message_id', inbox.messageId)).toBe(0);
    const [wallet] = await sql<{ balance: string; version: number }>(t.app.orm, `select balance::text, version from wallets where id = ?`, [w.walletId]);
    expect(wallet).toEqual({ balance: '100.00', version: 1 });
  });

  test('consumer processes a message end to end (ack-after-commit is proven by concurrency scenario 5)', async () => {
    const w = await createWallet(baseUrl(), '100.00');
    const body = wager(w, 'BET', '10.00');
    await sendWagerMessage(t.app.sqs, t.app.queues.transactions, body);
    const tx = await eventually(
      () => sql<{ status: string }>(t.app.orm, `select status from wager_transactions where external_transaction_id = ?`, [body.externalTransactionId]),
      (rows) => rows.length === 1,
    );
    expect(tx[0]?.status).toBe('PROCESSED');
    await eventually(() => countRows('inbox_messages', 'consumer_name', 'wager-transactions-consumer'), (n) => n >= 1);
  });

  test('redelivery of the same messageId and resend under a new messageId never duplicate effects', async () => {
    const w = await createWallet(baseUrl(), '100.00');
    const body = wager(w, 'BET', '10.00');
    const messageId = crypto.randomUUID();
    // Different SQS dedup ids so the broker does not hide the duplicates from us.
    await sendWagerMessage(t.app.sqs, t.app.queues.transactions, body, messageId, `${messageId}-1`);
    await sendWagerMessage(t.app.sqs, t.app.queues.transactions, body, messageId, `${messageId}-2`);
    await sendWagerMessage(t.app.sqs, t.app.queues.transactions, body, crypto.randomUUID());

    await eventually(() => countRows('inbox_messages', 'message_id', messageId), (n) => n === 1);
    await Bun.sleep(1_500);
    expect(await countRows('wager_transactions', 'external_transaction_id', body.externalTransactionId)).toBe(1);
    expect(await countRows('wallet_ledger_entries', 'wallet_id', w.walletId)).toBe(2);
    expect(await drain(t.app.sqs, t.app.queues.transactionsDlq, 1_500)).toHaveLength(0);
  });

  test('malformed messages and idempotency conflicts go straight to the DLQ', async () => {
    const w = await createWallet(baseUrl(), '100.00');
    const good = wager(w, 'BET', '1.00');
    await sendWagerMessage(t.app.sqs, t.app.queues.transactions, good);
    await sendWagerMessage(t.app.sqs, t.app.queues.transactions, { ...good, money: { amount: '2.00', currency: 'BRL' } }); // conflict
    await sendWagerMessage(t.app.sqs, t.app.queues.transactions, { ...wager(w, 'BET', '1.001') }); // invalid amount

    const dlq = await eventually(() => drain(t.app.sqs, t.app.queues.transactionsDlq, 2_000), (m) => m.length >= 2);
    const reasons = dlq.map((m) => m.MessageAttributes?.failureReason?.StringValue).sort();
    expect(reasons).toEqual(['IDEMPOTENCY_CONFLICT', 'INVALID_PAYLOAD']);
  });

  test('transient failures are retried with backoff and end in the DLQ after the attempt limit', async () => {
    const metrics = new PrometheusMetrics();
    let attempts = 0;
    const alwaysTransient = {
      execute: async () => {
        attempts += 1;
        throw new TransientInfrastructureError('db down');
      },
    } as unknown as SubmitWagerTransaction;
    // Dedicated queues so the application's own consumer does not pick the message up.
    const config = testConfig(`transient-${Date.now()}`);
    const queues = await ensureQueues(t.app.sqs, config.sqs);
    const consumer = new WagerTransactionConsumer(t.app.sqs, alwaysTransient, metrics, silentLogger, {
      consumerName: 'transient-test',
      queueUrl: queues.transactions,
      dlqUrl: queues.transactionsDlq,
      concurrency: 1,
      maxReceiveCount: 3,
      visibilityTimeoutSeconds: 5,
      waitTimeSeconds: 1,
    });
    const w = await createWallet(baseUrl(), '100.00');
    await sendWagerMessage(t.app.sqs, queues.transactions, wager(w, 'BET', '1.00'));
    consumer.start();
    const dlq = await eventually(() => drain(t.app.sqs, queues.transactionsDlq, 2_000), (m) => m.length === 1, 60_000);
    await consumer.stop();
    // Attempt 1 and 2 are retried (visibility 2s, 4s); attempt 3 hits the limit.
    expect(attempts).toBe(3);
    expect(dlq[0]?.MessageAttributes?.failureReason?.StringValue).toBe('MAX_RECEIVE_COUNT_EXCEEDED');
    expect(await metrics.registry.getSingleMetricAsString('wagering_retries_total')).toContain('component="consumer"} 2');
  }, 90_000);

  test('concurrent publishers never lose an event; duplicates are bounded and carry the same eventId', async () => {
    await sql(t.app.orm, `update outbox_messages set published_at = now() where published_at is null`);
    const w = await createWallet(baseUrl(), '1000.00');
    for (let i = 0; i < 20; i++) {
      await t.app.submit.execute(parseWagerSubmission(wager(w, 'BET', '1.00'), crypto.randomUUID()), { source: 'http', correlationId: 'c' });
    }
    const pending = await sql<{ id: string }>(t.app.orm, `select id from outbox_messages where published_at is null`);
    expect(pending.length).toBe(42); // 2 opening + 2 per bet

    const outboxStore = new MikroOrmOutboxStore(t.app.orm);
    const published: string[] = [];
    const publisher = { publish: async (m: { id: string }) => void published.push(m.id) };
    const publishers = Array.from({ length: 4 }, () => new PublishOutbox(outboxStore, publisher, systemClock, new PrometheusMetrics(), silentLogger, { batchSize: 5, leaseSeconds: 30 }));
    await Promise.all(
      publishers.map(async (p) => {
        while ((await p.runOnce()) > 0) {
          /* drain */
        }
      }),
    );
    expect(new Set(published).size).toBe(42);
    expect(published.length).toBe(42); // leases prevent concurrent double publication
    expect(await sql(t.app.orm, `select id from outbox_messages where published_at is null`)).toHaveLength(0);
  });

  test('publisher crash after commit: another instance publishes once the lease expires', async () => {
    await sql(t.app.orm, `update outbox_messages set published_at = now() where published_at is null`);
    const w = await createWallet(baseUrl(), '10.00'); // commits 2 events
    const store = new MikroOrmOutboxStore(t.app.orm);

    // Instance A claims (lease 1s) and "dies" before publishing.
    const claimed = await store.claim(10, 1);
    expect(claimed).toHaveLength(2);
    expect(await store.claim(10, 1)).toHaveLength(0); // leased: nobody else may take them

    await Bun.sleep(1_200);
    const published: string[] = [];
    const b = new PublishOutbox(store, { publish: async (m) => void published.push(m.id) }, systemClock, new PrometheusMetrics(), silentLogger, {
      batchSize: 10,
      leaseSeconds: 30,
    });
    expect(await b.runOnce()).toBe(2);
    expect(published.sort()).toEqual(claimed.map((m) => m.id).sort());
    expect(w.walletId).toBeDefined();
  });

  test('failed publication is retried with backoff and later succeeds', async () => {
    await sql(t.app.orm, `update outbox_messages set published_at = now() where published_at is null`);
    await createWallet(baseUrl(), '10.00');
    const store = new MikroOrmOutboxStore(t.app.orm);
    let fail = true;
    const publisher = new PublishOutbox(
      store,
      {
        publish: async () => {
          if (fail) throw new Error('sqs down');
        },
      },
      systemClock,
      new PrometheusMetrics(),
      silentLogger,
      { batchSize: 10, leaseSeconds: 30 },
    );
    expect(await publisher.runOnce()).toBe(0);
    const [row] = await sql<{ attempts: number; last_error: string }>(t.app.orm, `select attempts, last_error from outbox_messages where published_at is null limit 1`);
    expect(row).toMatchObject({ attempts: 1, last_error: 'sqs down' });
    fail = false;
    await sql(t.app.orm, `update outbox_messages set next_attempt_at = now() where published_at is null`);
    expect(await publisher.runOnce()).toBe(2);
  });

  test('real SQS publication: events land on the events queue with eventId as dedup id', async () => {
    await sql(t.app.orm, `update outbox_messages set published_at = now() where published_at is null`);
    const w = await createWallet(baseUrl(), '10.00');
    while ((await t.app.outboxPublisher.runOnce()) > 0) {
      /* drain */
    }
    const messages = await eventually(() => drain(t.app.sqs, t.app.queues.events, 2_000), (m) => m.length >= 2);
    const envelopes = messages.map((m) => JSON.parse(m.Body!));
    expect(envelopes.every((e) => e.aggregateId === w.walletId && e.version === 1 && typeof e.eventId === 'string')).toBe(true);
    const changed = envelopes.find((e) => e.eventType === 'WalletBalanceChanged');
    expect(changed.data).toMatchObject({ walletId: w.walletId, direction: 'CREDIT', money: { amount: '10.00', currency: 'BRL' }, walletVersion: 1 });
  });

  function baseUrl() {
    return `http://127.0.0.1:${(t.app.http.getHttpServer().address() as { port: number }).port}`;
  }
});
