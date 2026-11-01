import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { DeleteMessageCommand, ReceiveMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { createSqsClient, ensureQueues, type QueueUrls } from '../../src/infrastructure/messaging/sqs';
import { initOrm } from '../../src/infrastructure/persistence/orm';
import {
  TEST_DATABASE_URL,
  api,
  createWallet,
  eventually,
  expectLedgerConsistent,
  migrateFresh,
  purge,
  sendWagerMessage,
  sql,
  submit,
  testConfig,
  wager,
} from '../support/harness';
import { spawnInstance, type Instance } from '../support/processes';

/**
 * Real parallelism against real PostgreSQL + SQS, with up to three separate OS processes.
 * Every scenario ends by checking wallet.balance == balance rebuilt from the ledger.
 */
describe('concurrency', () => {
  const suite = `conc-${Date.now()}`;
  let orm: MikroORM;
  let sqs: SQSClient;
  let queues: QueueUrls;
  let instances: Instance[] = [];

  beforeAll(async () => {
    await migrateFresh();
    orm = await initOrm({ databaseUrl: TEST_DATABASE_URL, dbPoolMax: 4 });
    const config = testConfig(suite);
    sqs = createSqsClient(config.sqs);
    queues = await ensureQueues(sqs, config.sqs);
    await purge(sqs, queues.transactions, queues.transactionsDlq, queues.events);
    instances = await Promise.all(['inst-1', 'inst-2', 'inst-3'].map((name) => spawnInstance(suite, name)));
  }, 60_000);

  afterAll(async () => {
    await Promise.all(instances.map((i) => i.stop().catch(() => undefined)));
    if (orm) {
      await expectLedgerConsistent(orm);
      await orm.close();
    }
    sqs?.destroy();
  });

  const at = (i: number) => instances[i % instances.length]!.baseUrl;
  const metricSum = async (baseUrl: string, metric: string) =>
    (await fetch(`${baseUrl}/metrics`).then((r) => r.text()))
      .split('\n')
      .filter((line) => line.startsWith(`${metric}{`))
      .reduce((sum, line) => sum + Number(line.split(' ').pop()), 0);
  const ledgerCount = async (walletId: string, direction?: string) =>
    Number(
      (
        await sql<{ n: string }>(
          orm,
          `select count(*)::text as n from wallet_ledger_entries where wallet_id = ? ${direction ? 'and direction = ?' : ''}`,
          direction ? [walletId, direction] : [walletId],
        )
      )[0]?.n,
    );

  test('1. the same bet sent 50 times in parallel (across 3 instances) debits once', async () => {
    const w = await createWallet(at(0), '100.00');
    const bet = wager(w, 'BET', '10.00');
    const responses = await Promise.all(Array.from({ length: 50 }, (_, i) => submit(at(i), bet)));

    const statuses = responses.map((r) => r.status);
    expect(statuses.filter((s) => s === 201)).toHaveLength(1);
    expect(statuses.filter((s) => s === 200)).toHaveLength(49);
    expect(new Set(responses.map((r) => r.body.transactionId)).size).toBe(1);
    expect(responses.every((r) => r.body.balance.amount === '90.00')).toBe(true);
    expect(await ledgerCount(w.walletId, 'DEBIT')).toBe(1);
    await expectLedgerConsistent(orm, [w.walletId]);
  });

  test('2. two 80.00 bets racing for 100.00: one PROCESSED, one INSUFFICIENT_FUNDS, balance 20.00', async () => {
    for (let round = 0; round < 5; round++) {
      const w = await createWallet(at(round), '100.00');
      const betA = wager(w, 'BET', '80.00');
      const betB = wager(w, 'BET', '80.00');
      const [a, b] = await Promise.all([submit(at(0), betA), submit(at(1), betB)]);
      const results = [a!.body, b!.body];
      expect(results.filter((r) => r.status === 'PROCESSED')).toHaveLength(1);
      expect(results.filter((r) => r.status === 'REJECTED' && r.failureCode === 'INSUFFICIENT_FUNDS')).toHaveLength(1);
      expect((await api(at(2), 'GET', `/wallets/${w.walletId}`)).body.balance.amount).toBe('20.00');
      expect(await ledgerCount(w.walletId, 'DEBIT')).toBe(1);

      // Retrying the same two requests (same keys), concurrently, on other instances:
      // both are replays with their original outcome, and the debit is never duplicated.
      const [ra, rb] = await Promise.all([submit(at(2), betA), submit(at(0), betB)]);
      expect([ra!.body, rb!.body]).toEqual([
        { ...a!.body, idempotentReplay: true },
        { ...b!.body, idempotentReplay: true },
      ]);
      expect(await ledgerCount(w.walletId, 'DEBIT')).toBe(1);
      await expectLedgerConsistent(orm, [w.walletId]);
    }
  });

  test('2b. hot wallet: 200 concurrent bets never overdraw', async () => {
    const w = await createWallet(at(0), '50.00');
    const responses = await Promise.all(Array.from({ length: 200 }, (_, i) => submit(at(i), wager(w, 'BET', '1.00'))));
    const processed = responses.filter((r) => r.body?.status === 'PROCESSED').length;
    const rejected = responses.filter((r) => r.body?.failureCode === 'INSUFFICIENT_FUNDS').length;
    const transient = responses.filter((r) => r.status === 503).length;
    expect(processed).toBe(50);
    expect(processed + rejected + transient).toBe(200);
    expect((await api(at(0), 'GET', `/wallets/${w.walletId}`)).body).toMatchObject({ balance: { amount: '0.00' }, version: 51 });
    await expectLedgerConsistent(orm, [w.walletId]);
  });

  test('3. different wallets are processed in parallel: a lock on one wallet never blocks another', async () => {
    const [a, b] = await Promise.all([createWallet(at(0), '100.00'), createWallet(at(1), '100.00')]);
    // Hold wallet A's row lock from outside the application.
    let betOnA: Promise<Awaited<ReturnType<typeof submit>>> | undefined;
    await orm.em.fork().transactional(async (em) => {
      await em.execute(`select 1 from wallets where id = ? for update`, [a.walletId]);
      const startedB = performance.now();
      const onB = await submit(at(1), wager(b, 'BET', '5.00'));
      expect(onB.status).toBe(201); // B is not blocked by A's lock
      expect(performance.now() - startedB).toBeLessThan(1_000);

      betOnA = submit(at(2), wager(a, 'BET', '5.00'));
      const winner = await Promise.race([betOnA.then(() => 'finished'), Bun.sleep(1_500).then(() => 'waiting')]);
      expect(winner).toBe('waiting'); // A waits for its own lock only
    });
    expect((await betOnA!).status).toBe(201); // released at commit, then processed

    // And many wallets in parallel all converge.
    const wallets = await Promise.all(Array.from({ length: 20 }, (_, i) => createWallet(at(i), '100.00')));
    await Promise.all(wallets.flatMap((w, i) => Array.from({ length: 10 }, (_, j) => submit(at(i + j), wager(w, 'BET', '5.00')))));
    for (const w of wallets) expect((await api(at(0), 'GET', `/wallets/${w.walletId}`)).body.balance.amount).toBe('50.00');
    await expectLedgerConsistent(orm, [a.walletId, b.walletId, ...wallets.map((w) => w.walletId)]);
  });

  test('4. three instances consume the same queue concurrently without double effects', async () => {
    const wallets = await Promise.all(Array.from({ length: 10 }, (_, i) => createWallet(at(i), '100.00')));
    const bodies = wallets.flatMap((w) => Array.from({ length: 10 }, () => wager(w, 'BET', '3.00')));
    // Each message is sent twice (distinct broker dedup ids) to force cross-instance races.
    await Promise.all(
      bodies.flatMap((b) => {
        const id = crypto.randomUUID();
        return [sendWagerMessage(sqs, queues.transactions, b, id, `${id}-a`), sendWagerMessage(sqs, queues.transactions, b, crypto.randomUUID())];
      }),
    );
    // Plus HTTP on the side, same idempotency keys.
    await Promise.all(bodies.slice(0, 30).map((b, i) => submit(at(i), b)));

    await eventually(
      () => sql<{ n: string }>(orm, `select count(*)::text as n from wager_transactions where external_transaction_id = any(?::text[])`, [`{${bodies.map((b) => b.externalTransactionId).join(',')}}`]),
      (rows) => Number(rows[0]?.n) === bodies.length,
      60_000,
    );
    await Bun.sleep(2_000); // let duplicates drain
    // The queue was really shared: more than one process consumed from it.
    const consumedBy = await Promise.all(instances.map((i) => metricSum(i.baseUrl, 'wagering_consumer_messages_total')));
    expect(consumedBy.filter((n) => n > 0).length).toBeGreaterThanOrEqual(2);
    for (const w of wallets) {
      expect(await ledgerCount(w.walletId, 'DEBIT')).toBe(10);
      expect((await api(at(1), 'GET', `/wallets/${w.walletId}`)).body.balance.amount).toBe('70.00');
    }
    await expectLedgerConsistent(orm, wallets.map((w) => w.walletId));
  }, 90_000);

  test('5. worker killed after commit and before ack: redelivery is absorbed by the inbox', async () => {
    const crashSuite = `${suite}-crash`;
    const config = testConfig(crashSuite);
    const crashQueues = await ensureQueues(sqs, config.sqs);
    const w = await createWallet(at(0), '100.00');
    const body = wager(w, 'BET', '25.00');
    const messageId = crypto.randomUUID();

    const doomed = await spawnInstance(crashSuite, 'doomed', { FAULT_CRASH_AFTER_COMMIT: 'true', WORKER_OUTBOX: 'false', SQS_VISIBILITY_TIMEOUT_SECONDS: '3' });
    await sendWagerMessage(sqs, crashQueues.transactions, body, messageId);
    expect(await doomed.exited).not.toBe(0); // SIGKILLed itself after COMMIT, before DeleteMessage

    const committed = await sql<{ status: string }>(orm, `select status from wager_transactions where external_transaction_id = ?`, [body.externalTransactionId]);
    expect(committed).toEqual([{ status: 'PROCESSED' }]);

    const survivor = await spawnInstance(crashSuite, 'survivor', { SQS_VISIBILITY_TIMEOUT_SECONDS: '3' });
    try {
      // The message reappears after the visibility timeout; the survivor acks it as a duplicate.
      await eventually(
        () => fetch(`${survivor.baseUrl}/metrics`).then((r) => r.text()),
        (text) => /wagering_duplicates_total\{kind="inbox_redelivery"\} [1-9]/.test(text),
        30_000,
      );
      expect(await ledgerCount(w.walletId, 'DEBIT')).toBe(1);
      expect((await api(survivor.baseUrl, 'GET', `/wallets/${w.walletId}`)).body.balance.amount).toBe('75.00');
    } finally {
      await survivor.stop();
    }
    await expectLedgerConsistent(orm, [w.walletId]);
  }, 90_000);

  test('6. three concurrent publishers on the same outbox: every event published, per-wallet order kept', async () => {
    const wallets = await Promise.all(Array.from({ length: 3 }, (_, i) => createWallet(at(i), '100.00')));
    for (let i = 0; i < 5; i++) await Promise.all(wallets.map((w, j) => submit(at(i + j), wager(w, 'BET', '1.00'))));
    const ids = wallets.map((w) => w.walletId);
    const expected = await sql<{ id: string }>(orm, `select id from outbox_messages where aggregate_id = any(?::uuid[])`, [`{${ids.join(',')}}`]);
    expect(expected).toHaveLength(3 * (2 + 5 * 2));

    // Read the events queue (deleting as we go, as a real consumer would) until all arrived.
    const received: { eventId: string; aggregateId: string; eventType: string; data: { walletVersion?: number } }[] = [];
    await eventually(
      async () => {
        const res = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queues.events, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }));
        for (const m of res.Messages ?? []) {
          received.push(JSON.parse(m.Body!));
          await sqs.send(new DeleteMessageCommand({ QueueUrl: queues.events, ReceiptHandle: m.ReceiptHandle! }));
        }
        return new Set(received.filter((e) => ids.includes(e.aggregateId)).map((e) => e.eventId));
      },
      (seen) => expected.every((e) => seen.has(e.id)),
      60_000,
      0,
    );
    for (const id of ids) {
      const versions = received.filter((e) => e.aggregateId === id && e.eventType === 'WalletBalanceChanged').map((e) => e.data.walletVersion!);
      expect([...new Set(versions)]).toEqual([...new Set(versions)].sort((x, y) => x - y)); // per-wallet order on the wire
    }
  }, 90_000);

  test('7. ROLLBACK/REFUND delivered before the reference converge through the queue', async () => {
    const w = await createWallet(at(0), '100.00');
    const bet = wager(w, 'BET', '30.00');
    const refund = wager(w, 'REFUND', '30.00', { referenceExternalTransactionId: bet.externalTransactionId });
    // REFUND through the queue, BET later through HTTP: broker ordering cannot help here.
    await sendWagerMessage(sqs, queues.transactions, refund);
    // Only send the BET once the REFUND is provably waiting for it.
    await eventually(
      () => api(at(2), 'GET', `/providers/provider-a/wagering/transactions/${refund.externalTransactionId}`),
      (r) => r.status === 200 && r.body.status === 'PENDING_REFERENCE',
      30_000,
    );
    await submit(at(1), bet);
    const done = await eventually(
      () => api(at(2), 'GET', `/providers/provider-a/wagering/transactions/${refund.externalTransactionId}`),
      (r) => r.status === 200 && r.body.status === 'PROCESSED',
      30_000,
    );
    expect(done.body.referenceTransactionId).toBeDefined();
    expect((await api(at(0), 'GET', `/wallets/${w.walletId}`)).body.balance.amount).toBe('100.00');
    await expectLedgerConsistent(orm, [w.walletId]);
  });

  test('8. hard restart mid-load converges to a consistent state', async () => {
    const restartSuite = `${suite}-restart`;
    const config = testConfig(restartSuite);
    const restartQueues = await ensureQueues(sqs, config.sqs);
    const wallets = await Promise.all(Array.from({ length: 5 }, (_, i) => createWallet(at(i), '100.00')));
    const bodies = wallets.flatMap((w) => Array.from({ length: 60 }, () => wager(w, 'BET', '1.00')));
    const countDone = async () =>
      Number(
        (
          await sql<{ n: string }>(orm, `select count(*)::text as n from wager_transactions where external_transaction_id = any(?::text[])`, [
            `{${bodies.map((b) => b.externalTransactionId).join(',')}}`,
          ])
        )[0]?.n,
      );

    const first = await spawnInstance(restartSuite, 'restart-a', { SQS_VISIBILITY_TIMEOUT_SECONDS: '3' });
    await Promise.all(bodies.map((b) => sendWagerMessage(sqs, restartQueues.transactions, b)));
    await eventually(countDone, (n) => n >= 1, 30_000, 20);
    await first.kill(); // crash in the middle of processing...
    const atCrash = await countDone();
    expect(atCrash).toBeGreaterThan(0); // ...really in the middle: some committed,
    expect(atCrash).toBeLessThan(bodies.length); // some not

    const second = await spawnInstance(restartSuite, 'restart-b', { SQS_VISIBILITY_TIMEOUT_SECONDS: '3' });
    try {
      await eventually(
        () => sql<{ n: string }>(orm, `select count(*)::text as n from wager_transactions where external_transaction_id = any(?::text[])`, [`{${bodies.map((b) => b.externalTransactionId).join(',')}}`]),
        (rows) => Number(rows[0]?.n) === bodies.length,
        60_000,
      );
      for (const w of wallets) {
        expect(await ledgerCount(w.walletId, 'DEBIT')).toBe(60);
        const rec = await api(second.baseUrl, 'POST', `/wallets/${w.walletId}/reconciliation`);
        expect(rec.body).toMatchObject({ consistent: true, storedBalance: { amount: '40.00' }, checkedEntries: 61 });
      }
      // Outbox drains after the restart too.
      await eventually(
        () => sql<{ n: string }>(orm, `select count(*)::text as n from outbox_messages where published_at is null`),
        (rows) => Number(rows[0]?.n) === 0,
        30_000,
      );
    } finally {
      await second.stop();
    }
    await expectLedgerConsistent(orm, wallets.map((w) => w.walletId));
  }, 120_000);
});
