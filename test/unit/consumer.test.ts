import { describe, expect, test } from 'bun:test';
import type { Message, SQSClient } from '@aws-sdk/client-sqs';
import { TransientInfrastructureError } from '../../src/application/errors';
import type { SubmitWagerTransaction } from '../../src/application/submit-wager-transaction';
import { WagerTransactionConsumer } from '../../src/infrastructure/messaging/wager-transaction-consumer';
import { PrometheusMetrics } from '../../src/infrastructure/observability/metrics';
import { silentLogger } from '../support/in-memory-store';

const walletId = '0192f291-27dd-7d3f-8071-5f8685deef37';

function message(id: string, group: string, data: Record<string, unknown> = {}): Message {
  const externalTransactionId = `ext-${id}`;
  return {
    MessageId: id,
    ReceiptHandle: `rh-${id}`,
    Attributes: { ApproximateReceiveCount: '1', MessageGroupId: group },
    Body: JSON.stringify({
      messageId: id,
      type: 'WagerTransactionRequested',
      occurredAt: new Date().toISOString(),
      data: {
        providerId: 'p',
        externalTransactionId,
        idempotencyKey: `p:${externalTransactionId}`,
        playerId: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1',
        walletId,
        roundId: 'r',
        gameId: 'g',
        kind: 'BET',
        money: { amount: '1.00', currency: 'BRL' },
        ...data,
      },
    }),
  };
}

/** Fake SQS: serves one batch, records calls, fails where told to. */
function fakeSqs(batch: Message[], failures: { dlq?: boolean; deleteOf?: string } = {}) {
  const calls: { op: string; handle?: string; visibility?: number }[] = [];
  let served = false;
  const client = {
    async send(command: { constructor: { name: string }; input: Record<string, unknown> }) {
      const op = command.constructor.name.replace('Command', '');
      const handle = command.input.ReceiptHandle as string | undefined;
      calls.push({ op, handle, visibility: command.input.VisibilityTimeout as number | undefined });
      if (op === 'ReceiveMessage') {
        if (served) {
          await Bun.sleep(20);
          return { Messages: [] };
        }
        served = true;
        return { Messages: batch };
      }
      if (op === 'SendMessage' && failures.dlq) throw new Error('DLQ unavailable');
      if (op === 'DeleteMessage' && failures.deleteOf && handle === `rh-${failures.deleteOf}`) throw new Error('throttled');
      return {};
    },
  };
  return { client: client as unknown as SQSClient, calls };
}

function consumer(sqs: SQSClient, execute: (externalId: string) => Promise<unknown>) {
  const submit = { execute: (s: { request: { externalTransactionId: string } }) => execute(s.request.externalTransactionId) };
  return new WagerTransactionConsumer(sqs, submit as unknown as SubmitWagerTransaction, new PrometheusMetrics(), silentLogger, {
    consumerName: 'test',
    queueUrl: 'q',
    dlqUrl: 'dlq',
    concurrency: 1,
    maxReceiveCount: 5,
    visibilityTimeoutSeconds: 30,
    waitTimeSeconds: 0,
  });
}

const processed = { transactionId: 't', status: 'PROCESSED', idempotentReplay: false };

async function runOnce(c: WagerTransactionConsumer) {
  c.start();
  await Bun.sleep(100);
  await c.stop();
}

describe('WagerTransactionConsumer resilience', () => {
  test('a broker failure while dead-lettering does not kill the loop', async () => {
    const bad = { ...message('bad', 'g1'), Body: '{not json' };
    const { client, calls } = fakeSqs([bad, message('good', 'g2')], { dlq: true });
    const seen: string[] = [];
    await runOnce(consumer(client, async (id) => (seen.push(id), processed)));

    expect(seen).toEqual(['ext-good']); // the loop went on after the failed DLQ move
    expect(calls).toContainEqual({ op: 'DeleteMessage', handle: 'rh-good', visibility: undefined });
    expect(calls.some((c) => c.op === 'DeleteMessage' && c.handle === 'rh-bad')).toBe(false); // left for redelivery
    expect(calls.filter((c) => c.op === 'ReceiveMessage').length).toBeGreaterThan(1); // still polling
  });

  test('a transient failure defers the rest of its FIFO group, other groups proceed', async () => {
    const { client, calls } = fakeSqs([message('a1', 'A'), message('a2', 'A'), message('b1', 'B')]);
    const seen: string[] = [];
    await runOnce(
      consumer(client, async (id) => {
        seen.push(id);
        if (id === 'ext-a1') throw new TransientInfrastructureError('db down');
        return processed;
      }),
    );
    expect(seen).toEqual(['ext-a1', 'ext-b1']); // a2 must not overtake a1
    const visibility = (h: string) => calls.find((c) => c.op === 'ChangeMessageVisibility' && c.handle === h)?.visibility;
    expect(visibility('rh-a1')).toBe(2);
    expect(visibility('rh-a2')).toBe(2); // deferred together with a1
    expect(calls.some((c) => c.op === 'DeleteMessage' && c.handle === 'rh-b1')).toBe(true);
  });

  test('an ack failure after commit is not treated as a processing failure (no DLQ)', async () => {
    const { client, calls } = fakeSqs([message('m1', 'A')], { deleteOf: 'm1' });
    await runOnce(consumer(client, async () => processed));
    expect(calls.some((c) => c.op === 'SendMessage')).toBe(false);
    expect(calls.some((c) => c.op === 'ChangeMessageVisibility' && c.handle === 'rh-m1')).toBe(false);
  });
});
