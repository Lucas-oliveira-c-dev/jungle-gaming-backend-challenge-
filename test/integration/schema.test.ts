import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import type { MikroORM } from '@mikro-orm/postgresql';
import { initOrm } from '../../src/infrastructure/persistence/orm';
import { TEST_DATABASE_URL, migrateFresh, sql } from '../support/harness';

/**
 * Invariants enforced by the schema itself, exercised with raw SQL (bypassing all
 * application code).
 */
describe('migrations and constraints', () => {
  let orm: MikroORM;
  const playerId = crypto.randomUUID();
  const walletId = crypto.randomUUID();
  const txId = crypto.randomUUID();

  const expectSqlError = async (statements: string[], pattern: RegExp) => {
    const em = orm.em.fork();
    let error: unknown;
    try {
      await em.transactional(async (tem) => {
        for (const s of statements) await tem.execute(s);
      });
    } catch (e) {
      error = e;
    }
    expect(String((error as Error)?.message)).toMatch(pattern);
  };

  beforeAll(async () => {
    await migrateFresh();
    orm = await initOrm({ databaseUrl: TEST_DATABASE_URL, dbPoolMax: 4 });
    // A consistent wallet: OPENING credit of 100.00 (sequence 1), then one BET of 25.00 (sequence 2).
    const openingId = crypto.randomUUID();
    await orm.em.fork().transactional(async (em) => {
      await em.execute(`insert into wallets values ('${walletId}', '${playerId}', 'BRL', 75.00, 2, now(), now())`);
      await em.execute(`insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
        round_id, game_id, kind, amount, currency, status, created_at, processed_at, result_balance)
        values ('${openingId}', 'internal', 'opening:${walletId}', 'internal:opening:${walletId}', '${'0'.repeat(64)}', '${walletId}', '${playerId}',
        'opening', 'opening', 'OPENING', 100.00, 'BRL', 'PROCESSED', now(), now(), 100.00)`);
      await em.execute(`insert into wallet_ledger_entries values ('${crypto.randomUUID()}', '${walletId}', '${openingId}', 1, 'CREDIT', 100.00, 'BRL', 0.00, 100.00, now())`);
      await em.execute(`insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
        round_id, game_id, kind, amount, currency, status, created_at, processed_at, result_balance)
        values ('${txId}', 'p', 'e1', 'p:e1', '${'a'.repeat(64)}', '${walletId}', '${playerId}', 'r', 'g', 'BET', 25.00, 'BRL', 'PROCESSED', now(), now(), 75.00)`);
      await em.execute(`insert into wallet_ledger_entries values ('${crypto.randomUUID()}', '${walletId}', '${txId}', 2, 'DEBIT', 25.00, 'BRL', 100.00, 75.00, now())`);
    });
  });

  afterAll(async () => orm?.close());

  test('migrations are reversible (down to zero and up again)', async () => {
    const tmp = await initOrm({ databaseUrl: TEST_DATABASE_URL, dbPoolMax: 1 });
    try {
      const executed = await tmp.getMigrator().getExecutedMigrations();
      expect(executed.map((m) => m.name)).toContain('Migration20261031000001_initial');
    } finally {
      await tmp.close();
    }
  });

  test('balance cannot be negative', async () => {
    await expectSqlError([`update wallets set balance = -1 where id = '${walletId}'`], /check constraint/);
  });

  test('balance cannot change without the matching ledger entry (checked at commit)', async () => {
    await expectSqlError([`update wallets set balance = 1000, version = 3 where id = '${walletId}'`], /has no ledger entry/);
    await expectSqlError([`update wallets set balance = 1000 where id = '${walletId}'`], /differs from ledger/);
  });

  test('ledger is append-only', async () => {
    await expectSqlError([`update wallet_ledger_entries set amount = 1 where wallet_id = '${walletId}'`], /append-only/);
    await expectSqlError([`delete from wallet_ledger_entries where wallet_id = '${walletId}'`], /append-only/);
    await expectSqlError([`truncate wallet_ledger_entries cascade`], /append-only/);
  });

  test('ledger arithmetic must balance', async () => {
    await expectSqlError(
      [`insert into wallet_ledger_entries values ('${crypto.randomUUID()}', '${walletId}', '${txId}', 9, 'DEBIT', 10, 'BRL', 75, 70, now())`],
      /ledger_balanced|ledger_one_entry_per_tx/,
    );
  });

  test('one ledger entry per transaction and per sequence', async () => {
    await expectSqlError(
      [`insert into wallet_ledger_entries values ('${crypto.randomUUID()}', '${walletId}', '${txId}', 3, 'DEBIT', 5, 'BRL', 75, 70, now())`],
      /ledger_one_entry_per_tx_uq/,
    );
  });

  test('one wallet per player and currency', async () => {
    await expectSqlError([`insert into wallets values ('${crypto.randomUUID()}', '${playerId}', 'BRL', 0, 1, now(), now())`], /wallets_player_currency_uq/);
  });

  test('idempotency key and provider transaction id are unique', async () => {
    const insert = (key: string, ext: string) =>
      `insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
        round_id, game_id, kind, amount, currency, status, created_at) values ('${crypto.randomUUID()}', 'p', '${ext}', '${key}', '${'b'.repeat(64)}',
        '${walletId}', '${playerId}', 'r', 'g', 'BET', 1, 'BRL', 'PENDING', now())`;
    await expectSqlError([insert('p:e1', 'other')], /wager_tx_idempotency_key_uq/);
    await expectSqlError([insert('other', 'e1')], /wager_tx_provider_external_uq/);
  });

  test('terminal transactions are immutable and rows cannot be deleted', async () => {
    await expectSqlError([`update wager_transactions set status = 'REJECTED', failure_code = 'X' where id = '${txId}'`], /is terminal/);
    await expectSqlError([`delete from wager_transactions where id = '${txId}'`], /cannot be deleted/);
    await expectSqlError([`delete from wallets where id = '${walletId}'`], /cannot be deleted/);
  });

  test('REFUND/ROLLBACK require a reference and at most one PROCESSED reversal per reference', async () => {
    const reversal = (kind: string, ref: string | null) =>
      `insert into wager_transactions (id, provider_id, external_transaction_id, idempotency_key, payload_hash, wallet_id, player_id,
        round_id, game_id, kind, amount, currency, reference_external_transaction_id, reference_transaction_id, status, created_at, processed_at)
        values ('${crypto.randomUUID()}', 'p', '${crypto.randomUUID()}', '${crypto.randomUUID()}', '${'c'.repeat(64)}', '${walletId}', '${playerId}',
        'r', 'g', '${kind}', 25, 'BRL', ${ref ? `'e1'` : 'null'}, ${ref ? `'${ref}'` : 'null'}, 'PROCESSED', now(), now())`;
    await expectSqlError([reversal('REFUND', null)], /wager_tx_reversal_needs_reference/);
    await expectSqlError([reversal('REFUND', txId), reversal('ROLLBACK', txId)], /wager_tx_one_reversal_per_reference/);
  });

  test('money columns keep exact decimals', async () => {
    const [row] = await sql<{ v: string }>(orm, `select (0.1::numeric(20,2) + 0.2::numeric(20,2))::text as v`);
    expect(row?.v).toBe('0.30');
  });
});
