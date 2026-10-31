import { SendMessageBatchCommand, SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { EventPublisher } from '../../application/ports';
import type { OutboxMessage } from '../../domain/messaging';

/** SQS hard limit for SendMessageBatch. */
export const SQS_BATCH_LIMIT = 10;

/** Stable lane for an aggregate, so one wallet's events always go through the same lane. */
function laneOf(aggregateId: string, lanes: number): number {
  let hash = 0;
  for (let i = 0; i < aggregateId.length; i++) hash = (hash * 31 + aggregateId.charCodeAt(i)) | 0;
  return Math.abs(hash) % lanes;
}

export class SqsEventPublisher implements EventPublisher {
  constructor(
    private readonly sqs: SQSClient,
    private readonly queueUrl: string,
    /** Parallel SendMessageBatch calls per publisher run. */
    private readonly lanes = 8,
  ) {}

  async publish(message: OutboxMessage): Promise<void> {
    await this.sqs.send(new SendMessageCommand({ QueueUrl: this.queueUrl, ...this.entry(message) }));
  }

  /**
   * Partitions messages into lanes by aggregate (wallet) and sends the lanes in parallel.
   * Inside a lane, order is preserved: chunks are sent sequentially, and FIFO keeps entry
   * order within a batch for the same MessageGroupId.
   */
  async publishBatch(messages: OutboxMessage[]): Promise<Map<string, string>> {
    const lanes = Array.from({ length: this.lanes }, () => [] as OutboxMessage[]);
    for (const message of messages) lanes[laneOf(message.aggregateId, this.lanes)]!.push(message);
    const results = await Promise.all(lanes.filter((lane) => lane.length > 0).map((lane) => this.sendLane(lane)));
    return new Map(results.flatMap((failed) => [...failed]));
  }

  private async sendLane(messages: OutboxMessage[]): Promise<Map<string, string>> {
    const failed = new Map<string, string>();
    for (let i = 0; i < messages.length; i += SQS_BATCH_LIMIT) {
      const chunk = messages.slice(i, i + SQS_BATCH_LIMIT);
      // A lane that fails stops: later events of the same wallets must not overtake.
      if (failed.size > 0) {
        for (const m of chunk) failed.set(m.id, 'skipped: earlier batch in lane failed');
        continue;
      }
      // Batch entry ids must be unique and alphanumeric: use the position in the chunk.
      let response;
      try {
        response = await this.sqs.send(
          new SendMessageBatchCommand({
            QueueUrl: this.queueUrl,
            Entries: chunk.map((m, index) => ({ Id: String(index), ...this.entry(m) })),
          }),
        );
      } catch (error) {
        // Only this lane is affected; other lanes' successes still count as published.
        for (const m of chunk) failed.set(m.id, (error as Error).message);
        continue;
      }
      for (const failure of response.Failed ?? []) {
        const message = chunk[Number(failure.Id)];
        if (message) failed.set(message.id, `${failure.Code}: ${failure.Message ?? ''}`);
      }
    }
    return failed;
  }

  private entry(message: OutboxMessage) {
    return {
      MessageBody: JSON.stringify(message.payload),
      MessageGroupId: message.aggregateId,
      MessageDeduplicationId: message.id,
      MessageAttributes: {
        eventType: { DataType: 'String', StringValue: message.eventType },
        eventId: { DataType: 'String', StringValue: message.id },
      },
    };
  }
}
