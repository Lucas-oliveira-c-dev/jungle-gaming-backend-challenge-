import { PurgeQueueCommand, ReceiveMessageCommand, SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import { expect } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { createApplication, type Application } from '../../src/app';
import { loadConfig, type AppConfig } from '../../src/config';
import { initOrm } from '../../src/infrastructure/persistence/orm';

export const TEST_DATABASE_URL = process.env.TEST_DATABASE_URL ?? 'postgres://wagering:wagering@localhost:5432/wagering_test';
export const SQS_ENDPOINT = process.env.SQS_ENDPOINT ?? 'http://localhost:4566';

/** Env for a test instance. Queue names are unique per suite so suites never interfere. */
export function testEnv(suite: string, overrides: Record<string, string> = {}): Record<string, string> {
  return {
    ...(process.env as Record<string, string>),
    DATABASE_URL: TEST_DATABASE_URL,
    SQS_ENDPOINT,
    PORT: '0',
    LOG_LEVEL: process.env.TEST_LOG_LEVEL ?? 'silent',
    SQS_TRANSACTIONS_QUEUE: `wt-${suite}.fifo`,
    SQS_TRANSACTIONS_DLQ: `wt-${suite}-dlq.fifo`,
    SQS_EVENTS_QUEUE: `we-${suite}.fifo`,
    SQS_WAIT_TIME_SECONDS: '1',
    SQS_VISIBILITY_TIMEOUT_SECONDS: '5',
    OUTBOX_POLL_MS: '100',
    PENDING_REFERENCE_POLL_MS: '100',
    DB_POOL_MAX: '30',
    ...overrides,
  };
}

export function testConfig(suite: string, overrides: Record<string, string> = {}): AppConfig {
  return loadConfig(testEnv(suite, overrides));
}

let migrated = false;

/** Runs every migration down and up again once per test process (also proves reversibility). */
export async function migrateFresh(): Promise<void> {
  if (migrated) return;
  const orm = await initOrm({ databaseUrl: TEST_DATABASE_URL, dbPoolMax: 2 });
  try {
    const migrator = orm.getMigrator();
    await migrator.down({ to: 0 });
    await migrator.up();
    migrated = true;
  } finally {
    await orm.close();
  }
}

export interface TestApp {
  app: Application;
  baseUrl: string;
  close(): Promise<void>;
}

export async function startTestApp(suite: string, overrides: Record<string, string> = {}): Promise<TestApp> {
  await migrateFresh();
  const app = await createApplication(testConfig(suite, overrides));
  await app.start();
  const address = app.http.getHttpServer().address() as { port: number };
  return { app, baseUrl: `http://127.0.0.1:${address.port}`, close: () => app.stop() };
}

export async function purge(sqs: SQSClient, ...queueUrls: string[]): Promise<void> {
  for (const url of queueUrls) await sqs.send(new PurgeQueueCommand({ QueueUrl: url })).catch(() => undefined);
}

// ---------- HTTP helpers

export async function api(baseUrl: string, method: string, path: string, body?: unknown, headers: Record<string, string> = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: { 'content-type': 'application/json', ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  return { status: response.status, body: text ? JSON.parse(text) : undefined };
}

export async function createWallet(baseUrl: string, amount = '100.00', currency = 'BRL') {
  const playerId = crypto.randomUUID();
  const res = await api(baseUrl, 'POST', '/wallets', { playerId, initialBalance: { amount, currency } });
  expect(res.status).toBe(201);
  return { walletId: res.body.id as string, playerId };
}

let externalSeq = 0;
export function wager(
  wallet: { walletId: string; playerId: string },
  kind: string,
  amount: string,
  extra: Record<string, unknown> = {},
) {
  return {
    providerId: 'provider-a',
    externalTransactionId: `ext-${Date.now()}-${++externalSeq}-${Math.random().toString(36).slice(2, 8)}`,
    playerId: wallet.playerId,
    walletId: wallet.walletId,
    roundId: 'round-1',
    gameId: 'fortune-chimp',
    kind,
    money: { amount, currency: 'BRL' },
    ...extra,
  };
}

export const keyOf = (body: { providerId: string; externalTransactionId: string }) => `${body.providerId}:${body.externalTransactionId}`;

export function submit(baseUrl: string, body: ReturnType<typeof wager>, key = keyOf(body)) {
  return api(baseUrl, 'POST', '/wagering/transactions', body, { 'idempotency-key': key });
}

// ---------- SQS helpers

export function sendWagerMessage(sqs: SQSClient, queueUrl: string, body: ReturnType<typeof wager>, messageId = crypto.randomUUID(), dedupId = messageId) {
  return sqs.send(
    new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageGroupId: body.walletId,
      MessageDeduplicationId: dedupId,
      MessageBody: JSON.stringify({
        messageId,
        type: 'WagerTransactionRequested',
        occurredAt: new Date().toISOString(),
        data: { ...body, idempotencyKey: keyOf(body) },
      }),
    }),
  );
}

export async function drain(sqs: SQSClient, queueUrl: string, waitMs = 3_000) {
  const messages = [];
  const deadline = Date.now() + waitMs;
  while (Date.now() < deadline) {
    const res = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, MessageAttributeNames: ['All'] }));
    messages.push(...(res.Messages ?? []));
    if (messages.length && !(res.Messages ?? []).length) break;
  }
  return messages;
}

// ---------- assertions

export async function eventually<T>(fn: () => Promise<T>, check: (value: T) => boolean, timeoutMs = 20_000, intervalMs = 200): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: T = await fn();
  while (!check(last)) {
    if (Date.now() > deadline) throw new Error(`Condition not met within ${timeoutMs}ms; last value: ${JSON.stringify(last)}`);
    await Bun.sleep(intervalMs);
    last = await fn();
  }
  return last;
}

export async function sql<T = Record<string, unknown>>(orm: MikroORM, query: string, params: unknown[] = []): Promise<T[]> {
  return orm.em.fork().execute<T[]>(query, params);
}

/**
 * The invariant every test ends with: wallet.balance == balance rebuilt from the wallet
 * ledger == balance of the wallet account in the double-entry general ledger, and the
 * general ledger as a whole nets to zero.
 */
export async function expectLedgerConsistent(orm: MikroORM, walletIds?: string[]): Promise<void> {
  const rows = await sql<{ id: string; balance: string; rebuilt: string; general: string }>(
    orm,
    `select w.id, w.balance::text as balance,
            (select coalesce(sum(case l.direction when 'CREDIT' then l.amount else -l.amount end), 0)::numeric(20,2)::text
               from wallet_ledger_entries l where l.wallet_id = w.id) as rebuilt,
            (select coalesce(sum(case p.direction when 'CREDIT' then p.amount else -p.amount end), 0)::numeric(20,2)::text
               from journal_postings p where p.account_code = 'wallet:' || w.id) as general
       from wallets w
      ${walletIds ? 'where w.id = any(?::uuid[])' : ''}`,
    walletIds ? [`{${walletIds.join(',')}}`] : [],
  );
  for (const row of rows) {
    expect({ wallet: row.id, ledger: row.rebuilt, generalLedger: row.general }).toEqual({
      wallet: row.id,
      ledger: row.balance,
      generalLedger: row.balance,
    });
  }
  const [net] = await sql<{ net: string }>(
    orm,
    `select coalesce(sum(case direction when 'DEBIT' then amount else -amount end), 0)::numeric(20,2)::text as net from journal_postings`,
  );
  expect(net?.net).toBe('0.00');
}
