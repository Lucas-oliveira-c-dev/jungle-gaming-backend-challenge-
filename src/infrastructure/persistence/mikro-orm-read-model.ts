import { IsolationLevel, type MikroORM } from '@mikro-orm/postgresql';
import type { StoredTransaction } from '../../application/ports';
import type { WageringReadModel } from '../../application/queries';
import type { WalletLedgerEntry } from '../../domain/ledger-entry';
import type { Wallet } from '../../domain/wallet';
import { ledgerEntryToDomain, toStoredTransaction, walletToDomain } from './mappers';
import { LedgerEntryRecord, WagerTransactionRecord, WalletRecord } from './records';


export class MikroOrmReadModel implements WageringReadModel {
  constructor(private readonly orm: MikroORM) {}

  async findWallet(walletId: string): Promise<Wallet | null> {
    const record = await this.orm.em.fork().findOne(WalletRecord, { id: walletId });
    return record ? walletToDomain(record) : null;
  }

  async ledgerPage(walletId: string, afterSequence: number, limit: number): Promise<WalletLedgerEntry[]> {
    const records = await this.orm.em
      .fork()
      .find(LedgerEntryRecord, { walletId, sequence: { $gt: afterSequence } }, { orderBy: { sequence: 'asc' }, limit });
    return records.map(ledgerEntryToDomain);
  }

  async findTransactionById(id: string): Promise<StoredTransaction | null> {
    const record = await this.orm.em.fork().findOne(WagerTransactionRecord, { id });
    return record ? toStoredTransaction(record) : null;
  }

  async findTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<StoredTransaction | null> {
    const record = await this.orm.em.fork().findOne(WagerTransactionRecord, { providerId, externalTransactionId });
    return record ? toStoredTransaction(record) : null;
  }

  async reconciliationSnapshot(walletId: string) {
    return this.orm.em.fork().transactional(
      async (em) => {
        const record = await em.findOne(WalletRecord, { id: walletId });
        if (!record) return null;
        const [aggregate] = await em.execute<{ signed_sum: string; entries: string; last_balance_after: string | null }[]>(
          `select coalesce(sum(case direction when 'CREDIT' then amount else -amount end), 0)::text as signed_sum,
                  count(*)::text as entries,
                  (select balance_after::text from wallet_ledger_entries
                    where wallet_id = ? order by sequence desc limit 1) as last_balance_after
             from wallet_ledger_entries where wallet_id = ?`,
          [walletId, walletId],
        );
        const signed = aggregate?.signed_sum ?? '0';
        return {
          wallet: walletToDomain(record),
          signedSum: signed.includes('.') ? signed : `${signed}.00`,
          entries: Number(aggregate?.entries ?? 0),
          lastBalanceAfter: aggregate?.last_balance_after ?? undefined,
        };
      },
      { isolationLevel: IsolationLevel.REPEATABLE_READ, readOnly: true },
    );
  }
}
