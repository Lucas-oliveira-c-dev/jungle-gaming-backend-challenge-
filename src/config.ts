export interface AppConfig {
  instanceId: string;
  logLevel: string;
  port: number;
  databaseUrl: string;
  dbPoolMax: number;
  lockTimeoutMs: number;
  sqs: {
    endpoint: string;
    region: string;
    accessKeyId: string;
    secretAccessKey: string;
    transactionsQueue: string;
    transactionsDlq: string;
    eventsQueue: string;
    maxReceiveCount: number;
    visibilityTimeoutSeconds: number;
    waitTimeSeconds: number;
  };
  workers: {
    consumer: boolean;
    consumerConcurrency: number;
    outboxPublisher: boolean;
    outboxBatchSize: number;
    outboxPublishLanes: number;
    outboxPollMs: number;
    outboxLeaseSeconds: number;
    pendingReference: boolean;
    pendingReferencePollMs: number;
    pendingReferenceMaxAttempts: number;
  };
  /** Test-only fault injection: SIGKILL self right after commit, before the SQS ack. */
  crashAfterCommit: boolean;
}

const bool = (value: string | undefined, fallback: boolean) => (value === undefined ? fallback : value === 'true');
const int = (value: string | undefined, fallback: number) => (value === undefined ? fallback : Number.parseInt(value, 10));

export function loadConfig(env: Record<string, string | undefined> = process.env): AppConfig {
  return {
    instanceId: env.INSTANCE_ID ?? `instance-${process.pid}`,
    logLevel: env.LOG_LEVEL ?? 'info',
    port: int(env.PORT, 3000),
    databaseUrl: env.DATABASE_URL ?? 'postgres://wagering:wagering@localhost:5432/wagering',
    dbPoolMax: int(env.DB_POOL_MAX, 20),
    lockTimeoutMs: int(env.LOCK_TIMEOUT_MS, 5_000),
    sqs: {
      endpoint: env.SQS_ENDPOINT ?? 'http://localhost:4566',
      region: env.AWS_REGION ?? 'us-east-1',
      accessKeyId: env.AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: env.AWS_SECRET_ACCESS_KEY ?? 'test',
      transactionsQueue: env.SQS_TRANSACTIONS_QUEUE ?? 'wager-transactions.fifo',
      transactionsDlq: env.SQS_TRANSACTIONS_DLQ ?? 'wager-transactions-dlq.fifo',
      eventsQueue: env.SQS_EVENTS_QUEUE ?? 'wagering-events.fifo',
      maxReceiveCount: int(env.SQS_MAX_RECEIVE_COUNT, 5),
      visibilityTimeoutSeconds: int(env.SQS_VISIBILITY_TIMEOUT_SECONDS, 30),
      waitTimeSeconds: int(env.SQS_WAIT_TIME_SECONDS, 5),
    },
    workers: {
      consumer: bool(env.WORKER_CONSUMER, true),
      consumerConcurrency: int(env.CONSUMER_CONCURRENCY, 4),
      outboxPublisher: bool(env.WORKER_OUTBOX, true),
      outboxBatchSize: int(env.OUTBOX_BATCH_SIZE, 200),
      outboxPublishLanes: int(env.OUTBOX_PUBLISH_LANES, 8),
      outboxPollMs: int(env.OUTBOX_POLL_MS, 500),
      outboxLeaseSeconds: int(env.OUTBOX_LEASE_SECONDS, 30),
      pendingReference: bool(env.WORKER_PENDING_REFERENCE, true),
      pendingReferencePollMs: int(env.PENDING_REFERENCE_POLL_MS, 500),
      pendingReferenceMaxAttempts: int(env.PENDING_REFERENCE_MAX_ATTEMPTS, 10),
    },
    crashAfterCommit: bool(env.FAULT_CRASH_AFTER_COMMIT, false),
  };
}
