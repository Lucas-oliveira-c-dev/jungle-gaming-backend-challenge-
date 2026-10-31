/**
 * bun run test:load — HTTP load test against running instances (docker compose up).
 *
 * Env: TARGETS (comma separated, default the 3 compose instances), DURATION_S (30),
 * CONCURRENCY (64), WALLETS (200), HOT_WALLET_SHARE (0.2 — share of requests on one wallet).
 */
const targets = (process.env.TARGETS ?? 'http://localhost:3001,http://localhost:3002,http://localhost:3003').split(',');
const durationMs = Number(process.env.DURATION_S ?? 30) * 1000;
const concurrency = Number(process.env.CONCURRENCY ?? 64);
const walletCount = Number(process.env.WALLETS ?? 200);
const hotShare = Number(process.env.HOT_WALLET_SHARE ?? 0.2);

const post = (base: string, path: string, body: unknown, headers: Record<string, string> = {}) =>
  fetch(`${base}${path}`, { method: 'POST', headers: { 'content-type': 'application/json', ...headers }, body: JSON.stringify(body) });

/** Counters are per instance (sum); DB-derived gauges are identical everywhere (max). */
async function scrape(metric: string, aggregate: 'sum' | 'max' = 'sum'): Promise<number> {
  const values: number[] = [];
  for (const base of targets) {
    const text = await fetch(`${base}/metrics`).then((r) => r.text());
    for (const line of text.split('\n')) if (line.startsWith(metric)) values.push(Number(line.split(' ').pop()));
  }
  return aggregate === 'sum' ? values.reduce((a, b) => a + b, 0) : Math.max(0, ...values);
}

console.log(`setting up ${walletCount} wallets...`);
const wallets: { walletId: string; playerId: string }[] = [];
for (let i = 0; i < walletCount; i++) {
  const playerId = crypto.randomUUID();
  const res = await post(targets[i % targets.length]!, '/wallets', { playerId, initialBalance: { amount: '1000000.00', currency: 'BRL' } });
  wallets.push({ walletId: ((await res.json()) as { id: string }).id, playerId });
}

const lockConflictsBefore = await scrape('wagering_lock_conflicts_total');
const latencies: number[] = [];
const statuses = new Map<number, number>();
let sent = 0;
const deadline = Date.now() + durationMs;

async function worker(id: number) {
  while (Date.now() < deadline) {
    const wallet = Math.random() < hotShare ? wallets[0]! : wallets[Math.floor(Math.random() * wallets.length)]!;
    const external = `load-${id}-${sent++}-${Math.random().toString(36).slice(2)}`;
    const kind = Math.random() < 0.7 ? 'BET' : 'WIN';
    const body = {
      providerId: 'load',
      externalTransactionId: external,
      playerId: wallet.playerId,
      walletId: wallet.walletId,
      roundId: `r-${external}`,
      gameId: 'load',
      kind,
      money: { amount: '1.00', currency: 'BRL' },
    };
    const started = performance.now();
    const res = await post(targets[sent % targets.length]!, '/wagering/transactions', body, { 'idempotency-key': `load:${external}` }).catch(() => null);
    latencies.push(performance.now() - started);
    const status = res?.status ?? 0;
    statuses.set(status, (statuses.get(status) ?? 0) + 1);
    await res?.arrayBuffer();
  }
}

console.log(`running ${durationMs / 1000}s with concurrency ${concurrency} against ${targets.length} instances...`);
const started = performance.now();
await Promise.all(Array.from({ length: concurrency }, (_, i) => worker(i)));
const elapsedS = (performance.now() - started) / 1000;

latencies.sort((a, b) => a - b);
const pct = (p: number) => latencies[Math.min(latencies.length - 1, Math.floor((p / 100) * latencies.length))]!.toFixed(1);
const errors = [...statuses].filter(([s]) => s === 0 || s >= 500).reduce((n, [, c]) => n + c, 0);

console.log(
  JSON.stringify(
    {
      requests: latencies.length,
      throughputRps: Math.round(latencies.length / elapsedS),
      latencyMs: { p50: pct(50), p95: pct(95), p99: pct(99), max: latencies.at(-1)?.toFixed(1) },
      statuses: Object.fromEntries(statuses),
      errorRate: `${((errors / latencies.length) * 100).toFixed(2)}%`,
      lockConflicts: (await scrape('wagering_lock_conflicts_total')) - lockConflictsBefore,
      outboxLagSecondsNow: await scrape('wagering_outbox_lag_seconds', 'max'),
    },
    null,
    2,
  ),
);

// Verify the invariant on the hot wallet after the run.
const rec = await post(targets[0]!, `/wallets/${wallets[0]!.walletId}/reconciliation`, {});
console.log('hot wallet reconciliation:', await rec.json());
