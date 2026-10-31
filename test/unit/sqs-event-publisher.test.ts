import { describe, expect, test } from 'bun:test';
import type { SQSClient } from '@aws-sdk/client-sqs';
import { OutboxMessage } from '../../src/domain/messaging';
import { SqsEventPublisher } from '../../src/infrastructure/messaging/sqs-event-publisher';

type Entry = { Id: string; MessageGroupId: string; MessageDeduplicationId: string };

function fakeSqs(failGroup?: string) {
  const sent: Entry[] = [];
  const client = {
    async send(command: { input: { Entries: Entry[] } }) {
      await Bun.sleep(Math.random() * 5); // lanes interleave
      if (failGroup && command.input.Entries.some((e) => e.MessageGroupId === failGroup)) throw new Error('throttled');
      sent.push(...command.input.Entries);
      return { Failed: [] };
    },
  };
  return { client: client as unknown as SQSClient, sent };
}

const message = (aggregateId: string, seq: number) =>
  OutboxMessage.rehydrate({
    id: `${aggregateId}-${String(seq).padStart(3, '0')}`,
    aggregateId,
    eventType: 'WalletBalanceChanged',
    payload: {},
    occurredAt: new Date(),
    attempts: 0,
  });

describe('SqsEventPublisher lanes', () => {
  const wallets = Array.from({ length: 12 }, (_, i) => `wallet-${i}`);
  const messages = Array.from({ length: 25 }, (_, seq) => wallets.map((w) => message(w, seq))).flat();

  test('keeps per-wallet order while sending lanes in parallel', async () => {
    const { client, sent } = fakeSqs();
    const failed = await new SqsEventPublisher(client, 'q', 4).publishBatch(messages);
    expect(failed.size).toBe(0);
    expect(sent).toHaveLength(messages.length);
    for (const w of wallets) {
      const ids = sent.filter((e) => e.MessageGroupId === w).map((e) => e.MessageDeduplicationId);
      expect(ids).toEqual([...ids].sort());
    }
  });

  test('a failing lane stops (no overtaking) without affecting other lanes', async () => {
    const { client, sent } = fakeSqs('wallet-3');
    const failed = await new SqsEventPublisher(client, 'q', 4).publishBatch(messages);
    // every wallet-3 event failed or was skipped, so none can be published out of order
    expect(messages.filter((m) => m.aggregateId === 'wallet-3').every((m) => failed.has(m.id))).toBe(true);
    expect(sent.some((e) => e.MessageGroupId === 'wallet-3')).toBe(false);
    // what was not reported failed was really sent
    expect(sent.length + failed.size).toBe(messages.length);
  });
});
