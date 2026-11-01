import { z } from 'zod';
import { InvalidMoneyError } from '../domain/errors';
import { WagerTransactionProcessed, WalletBalanceChanged, type IntegrationEvent } from '../domain/events';
import { Accounts, JournalEntry } from '../domain/journal';
import { Money } from '../domain/money';
import { WagerTransaction, WagerTransactionKind } from '../domain/wager-transaction';
import { Wallet } from '../domain/wallet';
import { TransientInfrastructureError, ValidationError, WalletAlreadyExistsError } from './errors';
import type { AppLogger, Clock, IdGenerator, WageringStore } from './ports';
import { payloadHash } from './wager-request';

const createWalletSchema = z
  .object({
    playerId: z.string().uuid(),
    initialBalance: z.object({ amount: z.string(), currency: z.string() }).strict(),
  })
  .strict();

export const OPENING_PROVIDER = 'internal';

export interface WalletView {
  id: string;
  playerId: string;
  balance: { amount: string; currency: string };
  version: number;
}

export function walletView(wallet: Wallet): WalletView {
  return { id: wallet.id, playerId: wallet.playerId, balance: wallet.balance.toJSON(), version: wallet.version };
}

/**
 * Opens a wallet. A positive initial balance is booked in the same SQL transaction as an
 * internal OPENING transaction + CREDIT ledger entry + outbox events.
 */
export class CreateWallet {
  constructor(
    private readonly store: WageringStore,
    private readonly ids: IdGenerator,
    private readonly clock: Clock,
    private readonly logger: AppLogger,
  ) {}

  async execute(body: unknown, ctx: { correlationId: string }): Promise<WalletView> {
    const parsed = createWalletSchema.safeParse(body);
    if (!parsed.success) throw new ValidationError('Invalid wallet payload', parsed.error.flatten());
    let initialBalance: Money;
    try {
      initialBalance = Money.from(parsed.data.initialBalance);
    } catch (error) {
      if (error instanceof InvalidMoneyError) throw new ValidationError(error.message);
      throw error;
    }

    const now = this.clock.now();
    const walletId = this.ids.next();
    const openingId = this.ids.next();
    const { wallet, openingEntry } = Wallet.open({
      id: walletId,
      playerId: parsed.data.playerId.toLowerCase(),
      initialBalance,
      opening: { transactionId: openingId, entryId: this.ids.next() },
      at: now,
    });

    try {
      await this.store.transactional(async (uow) => {
        await uow.insertWallet(wallet);
        if (!openingEntry) return;
        const request = {
          providerId: OPENING_PROVIDER,
          externalTransactionId: `opening:${walletId}`,
          playerId: wallet.playerId,
          walletId,
          roundId: 'opening',
          gameId: 'opening',
          kind: WagerTransactionKind.Opening,
          money: initialBalance,
        };
        const opening = WagerTransaction.create({
          id: openingId,
          ...request,
          idempotencyKey: `${OPENING_PROVIDER}:opening:${walletId}`,
          payloadHash: payloadHash(request),
          createdAt: now,
        });
        opening.markProcessed(undefined, now);
        await uow.insertTransaction(opening, wallet.balance);
        await uow.recordMovement(
          openingEntry,
          JournalEntry.forWalletMovement({ id: this.ids.next(), entry: openingEntry, counterparty: Accounts.houseFunding(initialBalance.currency) }),
        );
        const eventCtx = () => ({ eventId: this.ids.next(), correlationId: ctx.correlationId, occurredAt: now });
        const events: IntegrationEvent<unknown>[] = [
          WalletBalanceChanged.from(wallet, openingEntry, eventCtx()),
          WagerTransactionProcessed.from(opening, wallet, eventCtx()),
        ];
        await uow.enqueue(events);
      });
    } catch (error) {
      if (this.store.isUniqueViolation(error)) {
        throw new WalletAlreadyExistsError(`Player already has a ${initialBalance.currency} wallet`);
      }
      if (this.store.isTransient(error)) throw new TransientInfrastructureError('Temporarily unable to create the wallet');
      throw error;
    }

    this.logger.info({ correlationId: ctx.correlationId, walletId }, 'wallet opened');
    return walletView(wallet);
  }
}
