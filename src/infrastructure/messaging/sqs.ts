import { CreateQueueCommand, GetQueueAttributesCommand, GetQueueUrlCommand, SQSClient } from '@aws-sdk/client-sqs';
import type { AppConfig } from '../../config';

export interface QueueUrls {
  transactions: string;
  transactionsDlq: string;
  events: string;
}

export function createSqsClient(config: AppConfig['sqs']): SQSClient {
  return new SQSClient({
    endpoint: config.endpoint,
    region: config.region,
    credentials: { accessKeyId: config.accessKeyId, secretAccessKey: config.secretAccessKey },
  });
}

/**
 * Idempotent queue provisioning (CreateQueue returns the existing URL). FIFO everywhere:
 * MessageGroupId = walletId gives per-wallet ordering and avoids needless lock contention,
 * but it is an optimisation only — correctness lives in the database.
 */
export async function ensureQueues(sqs: SQSClient, config: AppConfig['sqs']): Promise<QueueUrls> {
  const create = async (name: string, extra: Record<string, string> = {}) => {
    const attributes = {
      FifoQueue: 'true',
      ContentBasedDeduplication: 'false',
      VisibilityTimeout: String(config.visibilityTimeoutSeconds),
      MessageRetentionPeriod: '1209600',
      ...extra,
    };
    try {
      const { QueueUrl } = await sqs.send(new CreateQueueCommand({ QueueName: name, Attributes: attributes }));
      if (!QueueUrl) throw new Error(`Could not create queue ${name}`);
      return QueueUrl;
    } catch (error) {
      // Already provisioned with other attributes (e.g. a config change between deploys,
      // or another instance): reuse it. The consumer sets VisibilityTimeout per receive.
      if ((error as { name?: string }).name !== 'QueueNameExists') throw error;
      const { QueueUrl } = await sqs.send(new GetQueueUrlCommand({ QueueName: name }));
      if (!QueueUrl) throw error;
      return QueueUrl;
    }
  };

  const transactionsDlq = await create(config.transactionsDlq);
  const { Attributes } = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: transactionsDlq, AttributeNames: ['QueueArn'] }));
  const dlqArn = Attributes?.QueueArn ?? `arn:aws:sqs:${config.region}:000000000000:${config.transactionsDlq}`;
  // Redrive is a backstop: the consumer moves a message to the DLQ itself once the limit
  // is reached, so behaviour does not depend on the emulator's redrive implementation.
  const transactions = await create(config.transactionsQueue, {
    RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqArn, maxReceiveCount: config.maxReceiveCount + 1 }),
  });
  const events = await create(config.eventsQueue);
  return { transactions, transactionsDlq, events };
}

export async function queueReachable(sqs: SQSClient, queueName: string): Promise<boolean> {
  try {
    await sqs.send(new GetQueueUrlCommand({ QueueName: queueName }));
    return true;
  } catch {
    return false;
  }
}
