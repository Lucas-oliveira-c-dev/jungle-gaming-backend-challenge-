import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { api, createWallet, eventually, expectLedgerConsistent, sql, startTestApp, submit, wager, type TestApp } from '../support/harness';

describe('HTTP API', () => {
  let t: TestApp;

  beforeAll(async () => {
    t = await startTestApp('api', { WORKER_CONSUMER: 'false' });
  });
  afterAll(async () => {
    if (t) {
      await expectLedgerConsistent(t.app.orm);
      await t.close();
    }
  });

  test('creates a wallet with an OPENING transaction, ledger entry and events in one transaction', async () => {
    const playerId = crypto.randomUUID();
    const res = await api(t.baseUrl, 'POST', '/wallets', { playerId, initialBalance: { amount: '1000.00', currency: 'BRL' } });
    expect(res.status).toBe(201);
    expect(res.body).toMatchObject({ playerId, balance: { amount: '1000.00', currency: 'BRL' }, version: 1 });

    const [opening] = await sql<{ kind: string; status: string }>(t.app.orm, `select kind, status from wager_transactions where wallet_id = ?`, [res.body.id]);
    expect(opening).toEqual({ kind: 'OPENING', status: 'PROCESSED' });
    const ledger = await api(t.baseUrl, 'GET', `/wallets/${res.body.id}/ledger`);
    expect(ledger.body.items).toHaveLength(1);
    expect(ledger.body.items[0]).toMatchObject({ direction: 'CREDIT', sequence: 1, balanceAfter: { amount: '1000.00' } });
    const events = await sql<{ event_type: string }>(t.app.orm, `select event_type from outbox_messages where aggregate_id = ? order by event_type`, [res.body.id]);
    expect(events.map((e) => e.event_type)).toEqual(['WagerTransactionProcessed', 'WalletBalanceChanged']);

    const duplicate = await api(t.baseUrl, 'POST', '/wallets', { playerId, initialBalance: { amount: '1.00', currency: 'BRL' } });
    expect(duplicate.status).toBe(409);
    const otherCurrency = await api(t.baseUrl, 'POST', '/wallets', { playerId, initialBalance: { amount: '0.00', currency: 'USD' } });
    expect(otherCurrency.status).toBe(201);
    expect(otherCurrency.body.version).toBe(1);
  });

  test('status codes distinguish every situation', async () => {
    const w = await createWallet(t.baseUrl, '100.00');
    const bet = wager(w, 'BET', '25.00');

    const created = await submit(t.baseUrl, bet);
    expect(created.status).toBe(201);
    expect(created.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '75.00', currency: 'BRL' }, idempotentReplay: false });

    const replay = await submit(t.baseUrl, bet);
    expect(replay.status).toBe(200);
    expect(replay.body).toEqual({ ...created.body, idempotentReplay: true });

    const conflict = await submit(t.baseUrl, { ...bet, money: { amount: '26.00', currency: 'BRL' } });
    expect(conflict.status).toBe(409);
    expect(conflict.body.error.code).toBe('IDEMPOTENCY_CONFLICT');

    const invalid = await submit(t.baseUrl, { ...wager(w, 'BET', '1.001') });
    expect(invalid.status).toBe(400);
    const noKey = await api(t.baseUrl, 'POST', '/wagering/transactions', wager(w, 'BET', '1.00'));
    expect(noKey.status).toBe(400);
    const opening = await submit(t.baseUrl, wager(w, 'OPENING', '1.00'));
    expect(opening.status).toBe(400);

    const rejected = await submit(t.baseUrl, wager(w, 'BET', '1000.00'));
    expect(rejected.status).toBe(422);
    expect(rejected.body).toMatchObject({ status: 'REJECTED', failureCode: 'INSUFFICIENT_FUNDS', balance: { amount: '75.00' } });

    const pending = await submit(t.baseUrl, wager(w, 'REFUND', '5.00', { referenceExternalTransactionId: 'does-not-exist-yet' }));
    expect(pending.status).toBe(202);
    expect(pending.body.status).toBe('PENDING_REFERENCE');

    const unknownWallet = await submit(t.baseUrl, wager({ walletId: crypto.randomUUID(), playerId: w.playerId }, 'BET', '1.00'));
    expect(unknownWallet.status).toBe(422);
    expect(unknownWallet.body.error.code).toBe('WALLET_NOT_FOUND');

    expect((await api(t.baseUrl, 'GET', `/wallets/${crypto.randomUUID()}`)).status).toBe(404);
    expect((await api(t.baseUrl, 'GET', `/wallets/not-a-uuid`)).status).toBe(400);
  });

  test('full round: BET, WIN, LOSS, REFUND, ROLLBACK and queries', async () => {
    const w = await createWallet(t.baseUrl, '100.00');
    const bet = wager(w, 'BET', '10.00');
    await submit(t.baseUrl, bet);
    const win = wager(w, 'WIN', '30.00', { referenceExternalTransactionId: bet.externalTransactionId });
    expect((await submit(t.baseUrl, win)).body.balance.amount).toBe('120.00');
    const loss = await submit(t.baseUrl, wager(w, 'LOSS', '0.00', { roundId: 'round-2' }));
    expect(loss.status).toBe(201);
    expect(loss.body.balance.amount).toBe('120.00');

    const bet2 = wager(w, 'BET', '20.00', { roundId: 'round-3' });
    await submit(t.baseUrl, bet2);
    const refund = wager(w, 'REFUND', '20.00', { roundId: 'round-3', referenceExternalTransactionId: bet2.externalTransactionId });
    expect((await submit(t.baseUrl, refund)).body.balance.amount).toBe('120.00');
    const secondRefund = await submit(t.baseUrl, { ...refund, externalTransactionId: `${refund.externalTransactionId}-b` });
    expect(secondRefund.body).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_ALREADY_REVERSED' });

    const rollbackWin = await submit(t.baseUrl, wager(w, 'ROLLBACK', '30.00', { referenceExternalTransactionId: win.externalTransactionId }));
    expect(rollbackWin.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '90.00' } });

    const wallet = await api(t.baseUrl, 'GET', `/wallets/${w.walletId}`);
    expect(wallet.body).toMatchObject({ balance: { amount: '90.00' }, version: 6 });

    const byExternal = await api(t.baseUrl, 'GET', `/providers/provider-a/wagering/transactions/${win.externalTransactionId}`);
    expect(byExternal.body).toMatchObject({ kind: 'WIN', status: 'PROCESSED', money: { amount: '30.00', currency: 'BRL' } });
    const byId = await api(t.baseUrl, 'GET', `/wagering/transactions/${byExternal.body.id}`);
    expect(byId.body.externalTransactionId).toBe(win.externalTransactionId);

    // Paginate the ledger 2 by 2 with the opaque cursor.
    const seen: number[] = [];
    let cursor: string | null = null;
    do {
      const page: { body: { items: { sequence: number }[]; nextCursor: string | null } } = await api(
        t.baseUrl,
        'GET',
        `/wallets/${w.walletId}/ledger?limit=2${cursor ? `&cursor=${cursor}` : ''}`,
      );
      seen.push(...page.body.items.map((i) => i.sequence));
      cursor = page.body.nextCursor;
    } while (cursor);
    expect(seen).toEqual([1, 2, 3, 4, 5, 6]);
    expect((await api(t.baseUrl, 'GET', `/wallets/${w.walletId}/ledger?cursor=garbage`)).status).toBe(400);

    const reconciliation = await api(t.baseUrl, 'POST', `/wallets/${w.walletId}/reconciliation`);
    expect(reconciliation.body).toEqual({
      walletId: w.walletId,
      storedBalance: { amount: '90.00', currency: 'BRL' },
      calculatedBalance: { amount: '90.00', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 6,
      generalLedgerBalance: { amount: '90.00', currency: 'BRL' },
    });

    // Double entry: every movement of this round has its journal entry, and they all balance.
    const journal = await sql<{ entries: string; net: string }>(
      t.app.orm,
      `select count(distinct j.id)::text as entries, sum(case p.direction when 'DEBIT' then p.amount else -p.amount end)::text as net
         from journal_entries j join journal_postings p on p.journal_entry_id = j.id
         join wager_transactions tx on tx.id = j.transaction_id where tx.wallet_id = ?`,
      [w.walletId],
    );
    expect(journal[0]).toEqual({ entries: '6', net: '0.00' });
  });

  test('trial balance: total debits equal total credits, per account group', async () => {
    const w = await createWallet(t.baseUrl, '50.00');
    await submit(t.baseUrl, wager(w, 'BET', '20.00', { providerId: 'provider-tb' }));
    await submit(t.baseUrl, wager(w, 'WIN', '35.00', { providerId: 'provider-tb' }));

    const res = await api(t.baseUrl, 'GET', '/ledger/trial-balance?currency=BRL');
    expect(res.status).toBe(200);
    expect(res.body.balanced).toBe(true);
    expect(res.body.totalDebits).toEqual(res.body.totalCredits);
    const provider = res.body.accounts.find((a: { account: string }) => a.account === 'provider:provider-tb:BRL');
    // Provider settlement: credited by the bet (20), debited by the win (35) -> owes us 15.
    expect(provider).toMatchObject({ type: 'LIABILITY', debits: { amount: '35.00' }, credits: { amount: '20.00' }, balance: { amount: '-15.00' } });
    const wallets = res.body.accounts.find((a: { account: string }) => a.account === 'wallet:*');
    const funding = res.body.accounts.find((a: { account: string }) => a.account === 'house:funding:BRL');
    expect(wallets.type).toBe('LIABILITY');
    expect(funding.type).toBe('ASSET');
    expect((await api(t.baseUrl, 'GET', '/ledger/trial-balance?currency=brl1')).status).toBe(400);
  });

  test('idempotency keys are scoped per provider: one provider cannot block another', async () => {
    const w = await createWallet(t.baseUrl, '100.00');
    const sharedKey = 'provider-b:win-123'; // predictable key of provider-b
    // provider-a claims provider-b's key first...
    const squatter = await submit(t.baseUrl, wager(w, 'LOSS', '0.00', { providerId: 'provider-a' }), sharedKey);
    expect(squatter.status).toBe(201);
    // ...and provider-b's real transaction with that key is still processed, not a 409.
    const legit = await submit(t.baseUrl, wager(w, 'WIN', '10.00', { providerId: 'provider-b' }), sharedKey);
    expect(legit.status).toBe(201);
    expect(legit.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '110.00' }, idempotentReplay: false });
    expect(legit.body.transactionId).not.toBe(squatter.body.transactionId);
  });

  test('currency conflict is rejected and auditable', async () => {
    const w = await createWallet(t.baseUrl, '100.00');
    const res = await submit(t.baseUrl, { ...wager(w, 'BET', '1.00'), money: { amount: '1.00', currency: 'USD' } });
    expect(res.status).toBe(422);
    expect(res.body.failureCode).toBe('CURRENCY_MISMATCH');
    const tx = await api(t.baseUrl, 'GET', `/wagering/transactions/${res.body.transactionId}`);
    expect(tx.body).toMatchObject({ status: 'REJECTED', failureCode: 'CURRENCY_MISMATCH' });
  });

  test('ROLLBACK delivered before its reference is resolved by the worker', async () => {
    const w = await createWallet(t.baseUrl, '100.00');
    const bet = wager(w, 'BET', '40.00');
    const rollback = wager(w, 'ROLLBACK', '40.00', { referenceExternalTransactionId: bet.externalTransactionId });

    const early = await submit(t.baseUrl, rollback);
    expect(early.status).toBe(202);
    expect((await submit(t.baseUrl, bet)).body.balance.amount).toBe('60.00');

    const resolved = await eventually(
      () => api(t.baseUrl, 'GET', `/wagering/transactions/${early.body.transactionId}`),
      (r) => r.body.status !== 'PENDING_REFERENCE',
    );
    expect(resolved.body).toMatchObject({ status: 'PROCESSED', balance: { amount: '100.00' } });
    // A replay now returns the final result.
    expect((await submit(t.baseUrl, rollback)).body).toMatchObject({ status: 'PROCESSED', idempotentReplay: true });
    const events = await sql<{ event_type: string }>(
      t.app.orm,
      `select event_type from outbox_messages where payload->'data'->>'transactionId' = ? order by occurred_at, event_type`,
      [early.body.transactionId],
    );
    expect(events.map((e) => e.event_type)).toEqual(['WagerTransactionPendingReference', 'WagerTransactionProcessed', 'WalletBalanceChanged']);
  });

  test('reference that never arrives is rejected with REFERENCE_NOT_FOUND after the retry budget', async () => {
    const w = await createWallet(t.baseUrl, '100.00');
    const refund = await submit(t.baseUrl, wager(w, 'REFUND', '5.00', { referenceExternalTransactionId: 'ghost' }));
    // Fast-forward the backoff instead of waiting minutes.
    for (let i = 0; i < 12; i++) {
      await sql(t.app.orm, `update wager_transactions set next_reference_check_at = now() where id = ? and status = 'PENDING_REFERENCE'`, [refund.body.transactionId]);
      await t.app.pendingReferences.runOnce();
    }
    const tx = await api(t.baseUrl, 'GET', `/wagering/transactions/${refund.body.transactionId}`);
    expect(tx.body).toMatchObject({ status: 'REJECTED', failureCode: 'REFERENCE_NOT_FOUND' });
    const [rejected] = await sql(t.app.orm, `select 1 from outbox_messages where event_type = 'WagerTransactionRejected' and payload->'data'->>'transactionId' = ?`, [
      refund.body.transactionId,
    ]);
    expect(rejected).toBeDefined();
  });

  test('health endpoints are open; readiness checks postgres and sqs', async () => {
    expect((await api(t.baseUrl, 'GET', '/health/live')).status).toBe(200);
    const ready = await api(t.baseUrl, 'GET', '/health/ready');
    expect(ready).toEqual({ status: 200, body: { status: 'ok', checks: { postgres: true, sqs: true } } });
    const metrics = await fetch(`${t.baseUrl}/metrics`).then((r) => r.text());
    expect(metrics).toContain('wagering_transactions_total');
    expect(metrics).toContain('wagering_outbox_lag_seconds');
  });
});
