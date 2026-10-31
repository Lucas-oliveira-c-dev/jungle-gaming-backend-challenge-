import 'reflect-metadata';
import { Module, type INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { MikroORM } from '@mikro-orm/postgresql';
import type { SQSClient } from '@aws-sdk/client-sqs';
import type { NextFunction, Request, Response } from 'express';
import { CreateWallet } from './application/create-wallet';
import { systemClock, uuidV7 } from './application/ports';
import { PublishOutbox } from './application/publish-outbox';
import { WageringQueries } from './application/queries';
import { ResolvePendingReferences } from './application/resolve-pending-references';
import { SubmitWagerTransaction } from './application/submit-wager-transaction';
import type { AppConfig } from './config';
import { ErrorFilter } from './http/error-filter';
import { OpsController, TOKENS, WageringController, WalletsController, type HealthProbe } from './http/controllers';
import { createSqsClient, ensureQueues, queueReachable, type QueueUrls } from './infrastructure/messaging/sqs';
import { SqsEventPublisher } from './infrastructure/messaging/sqs-event-publisher';
import { WagerTransactionConsumer } from './infrastructure/messaging/wager-transaction-consumer';
import { createLogger, NestPinoLogger } from './infrastructure/observability/logger';
import { PrometheusMetrics } from './infrastructure/observability/metrics';
import { MikroOrmOutboxStore } from './infrastructure/persistence/mikro-orm-outbox-store';
import { MikroOrmReadModel } from './infrastructure/persistence/mikro-orm-read-model';
import { MikroOrmWageringStore } from './infrastructure/persistence/mikro-orm-wagering-store';
import { initOrm } from './infrastructure/persistence/orm';
import { PollingWorker } from './workers/polling-worker';

export const CONSUMER_NAME = 'wager-transactions-consumer';

export interface Application {
  config: AppConfig;
  http: INestApplication;
  orm: MikroORM;
  sqs: SQSClient;
  queues: QueueUrls;
  submit: SubmitWagerTransaction;
  outboxPublisher: PublishOutbox;
  pendingReferences: ResolvePendingReferences;
  start(): Promise<void>;
  stop(): Promise<void>;
}

/** Composition root: wires domain, application and infrastructure without hidden magic. */
export async function createApplication(config: AppConfig): Promise<Application> {
  const logger = createLogger(config.instanceId, config.logLevel);
  const metrics = new PrometheusMetrics();
  const orm = await initOrm(config);
  const sqs = createSqsClient(config.sqs);
  const queues = await ensureQueues(sqs, config.sqs);

  const store = new MikroOrmWageringStore(orm, config.lockTimeoutMs);
  const outboxStore = new MikroOrmOutboxStore(orm);
  metrics.attachOutbox(outboxStore);

  const submit = new SubmitWagerTransaction(store, uuidV7, systemClock, metrics, logger);
  const createWallet = new CreateWallet(store, uuidV7, systemClock, logger);
  const queries = new WageringQueries(new MikroOrmReadModel(orm), metrics, logger);
  const outboxPublisher = new PublishOutbox(outboxStore, new SqsEventPublisher(sqs, queues.events, config.workers.outboxPublishLanes), systemClock, metrics, logger, {
    batchSize: config.workers.outboxBatchSize,
    leaseSeconds: config.workers.outboxLeaseSeconds,
  });
  const pendingReferences = new ResolvePendingReferences(
    store,
    uuidV7,
    systemClock,
    metrics,
    logger,
    config.workers.pendingReferenceMaxAttempts,
  );

  const health: HealthProbe = {
    database: () => orm.isConnected(),
    queue: () => queueReachable(sqs, config.sqs.transactionsQueue),
  };

  @Module({
    controllers: [WalletsController, WageringController, OpsController],
    providers: [
      { provide: TOKENS.createWallet, useValue: createWallet },
      { provide: TOKENS.submitWager, useValue: submit },
      { provide: TOKENS.queries, useValue: queries },
      { provide: TOKENS.health, useValue: health },
      { provide: TOKENS.metrics, useValue: metrics.registry },
    ],
  })
  class AppModule {}

  const http = await NestFactory.create(AppModule, { logger: new NestPinoLogger(logger), bodyParser: true });
  http.useGlobalFilters(new ErrorFilter(logger));
  http.use((req: Request & { correlationId?: string }, res: Response, next: NextFunction) => {
    const header = req.header('x-correlation-id');
    req.correlationId = header && header.length <= 128 ? header : Bun.randomUUIDv7();
    res.setHeader('x-correlation-id', req.correlationId);
    next();
  });

  const consumer = new WagerTransactionConsumer(sqs, submit, metrics, logger, {
    consumerName: CONSUMER_NAME,
    queueUrl: queues.transactions,
    dlqUrl: queues.transactionsDlq,
    concurrency: config.workers.consumerConcurrency,
    maxReceiveCount: config.sqs.maxReceiveCount,
    visibilityTimeoutSeconds: config.sqs.visibilityTimeoutSeconds,
    waitTimeSeconds: config.sqs.waitTimeSeconds,
    afterCommit: config.crashAfterCommit
      ? () => {
          logger.warn({}, 'FAULT_CRASH_AFTER_COMMIT: killing process after commit, before ack');
          process.kill(process.pid, 'SIGKILL');
        }
      : undefined,
  });
  const outboxWorker = new PollingWorker('outbox-publisher', () => outboxPublisher.runOnce(), config.workers.outboxPollMs, logger);
  const pendingWorker = new PollingWorker(
    'pending-reference',
    () => pendingReferences.runOnce(),
    config.workers.pendingReferencePollMs,
    logger,
  );

  return {
    config,
    http,
    orm,
    sqs,
    queues,
    submit,
    outboxPublisher,
    pendingReferences,
    async start() {
      await http.listen(config.port);
      if (config.workers.consumer) consumer.start();
      if (config.workers.outboxPublisher) outboxWorker.start();
      if (config.workers.pendingReference) pendingWorker.start();
      logger.info({ port: config.port }, 'wagering processor started');
    },
    async stop() {
      // Order matters: stop intake first, drain in-flight work, then close resources.
      await consumer.stop();
      await Promise.all([outboxWorker.stop(), pendingWorker.stop()]);
      await http.close();
      await orm.close();
      sqs.destroy();
      logger.info({}, 'wagering processor stopped');
    },
  };
}
