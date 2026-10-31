import { Counter, Gauge, Histogram, Registry } from 'prom-client';
import type { EntryPoint, OutboxStore, WageringMetrics } from '../../application/ports';
import type { OutboxMetrics } from '../../application/publish-outbox';

export interface ConsumerMetrics {
  messageHandled(outcome: 'processed' | 'replay' | 'business_rejection' | 'retry' | 'dlq'): void;
  movedToDlq(reason: string): void;
  retry(component: 'outbox' | 'consumer'): void;
}

/** Prometheus metrics exposed on GET /metrics. One registry per process. */
export class PrometheusMetrics implements WageringMetrics, OutboxMetrics, ConsumerMetrics {
  readonly registry = new Registry();

  private readonly transactions = new Counter({
    name: 'wagering_transactions_total',
    help: 'Wager transactions by resulting status',
    labelNames: ['status', 'kind', 'source'],
    registers: [this.registry],
  });
  private readonly duplicates = new Counter({
    name: 'wagering_duplicates_total',
    help: 'Duplicates detected (idempotent replays and inbox redeliveries)',
    labelNames: ['kind'],
    registers: [this.registry],
  });
  private readonly conflicts = new Counter({
    name: 'wagering_idempotency_conflicts_total',
    help: 'Same idempotency key with a different payload',
    registers: [this.registry],
  });
  private readonly lockConflicts = new Counter({
    name: 'wagering_lock_conflicts_total',
    help: 'Lock timeouts and deadlocks on wallet rows',
    registers: [this.registry],
  });
  private readonly retries = new Counter({
    name: 'wagering_retries_total',
    help: 'Retries scheduled by component',
    labelNames: ['component'],
    registers: [this.registry],
  });
  private readonly dlq = new Counter({
    name: 'wagering_dlq_messages_total',
    help: 'Messages sent to the DLQ by this instance (explicit permanent failures)',
    labelNames: ['reason'],
    registers: [this.registry],
  });
  private readonly consumerMessages = new Counter({
    name: 'wagering_consumer_messages_total',
    help: 'SQS messages handled by outcome',
    labelNames: ['outcome'],
    registers: [this.registry],
  });
  private readonly publishedEvents = new Counter({
    name: 'wagering_outbox_published_total',
    help: 'Integration events published',
    labelNames: ['event_type'],
    registers: [this.registry],
  });
  private readonly publishFailures = new Counter({
    name: 'wagering_outbox_publish_failures_total',
    help: 'Integration event publish failures',
    labelNames: ['event_type'],
    registers: [this.registry],
  });
  private readonly divergences = new Counter({
    name: 'wagering_reconciliation_divergences_total',
    help: 'Wallets whose stored balance differs from the ledger',
    registers: [this.registry],
  });
  private readonly latency = new Histogram({
    name: 'wagering_processing_duration_seconds',
    help: 'End-to-end processing latency of a wager submission',
    labelNames: ['source', 'outcome'],
    buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
    registers: [this.registry],
  });

  /** Outbox lag is measured on scrape from the database, so it is correct across instances. */
  attachOutbox(outbox: OutboxStore): void {
    new Gauge({
      name: 'wagering_outbox_lag_seconds',
      help: 'Age of the oldest unpublished outbox message',
      registers: [this.registry],
      async collect() {
        this.set(await outbox.oldestPendingAgeSeconds().catch(() => -1));
      },
    });
  }

  transactionRecorded(status: string, kind: string, source: EntryPoint) {
    this.transactions.inc({ status, kind, source });
  }
  duplicateDetected(kind: 'idempotent_replay' | 'inbox_redelivery') {
    this.duplicates.inc({ kind });
  }
  idempotencyConflict() {
    this.conflicts.inc();
  }
  lockConflict() {
    this.lockConflicts.inc();
  }
  processingDuration(seconds: number, source: EntryPoint, outcome: string) {
    this.latency.observe({ source, outcome }, seconds);
  }
  reconciliationDivergence() {
    this.divergences.inc();
  }
  published(eventType: string) {
    this.publishedEvents.inc({ event_type: eventType });
  }
  publishFailed(eventType: string) {
    this.publishFailures.inc({ event_type: eventType });
  }
  retry(component: 'outbox' | 'consumer') {
    this.retries.inc({ component });
  }
  messageHandled(outcome: 'processed' | 'replay' | 'business_rejection' | 'retry' | 'dlq') {
    this.consumerMessages.inc({ outcome });
  }
  movedToDlq(reason: string) {
    this.dlq.inc({ reason });
  }
}
