import { IsolationLevel, type MikroORM } from '@mikro-orm/postgresql';
import type { StoredTransaction } from '../../application/ports';
import type { TrialBalanceRow, WageringReadModel } from '../../application/queries';
import type { WalletLedgerEntry } from '../../domain/ledger-entry';
import { Accounts } from '../../domain/journal';
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
        // The wallet account is a liability: its balance is credits minus debits.
        const [general] = await em.execute<{ balance: string }[]>(
          `select coalesce(sum(case direction when 'CREDIT' then amount else -amount end), 0)::numeric(20,2)::text as balance
             from journal_postings where account_code = ?`,
          [Accounts.wallet(walletId, record.currency.trim()).code],
        );
        const signed = aggregate?.signed_sum ?? '0';
        return {
          wallet: walletToDomain(record),
          signedSum: signed.includes('.') ? signed : `${signed}.00`,
          entries: Number(aggregate?.entries ?? 0),
          lastBalanceAfter: aggregate?.last_balance_after ?? undefined,
          generalLedgerBalance: general?.balance ?? '0.00',
        };
      },
      { isolationLevel: IsolationLevel.REPEATABLE_READ, readOnly: true },
    );
  }

  async trialBalance(currency: string): Promise<TrialBalanceRow[]> {
    // Wallet accounts are summed into one line: there is one per player.
    const rows = await this.orm.em.fork().execute<{ account: string; type: string; accounts: string; debits: string; credits: string }[]>(
      `select case when a.code like 'wallet:%' then 'wallet:*' else a.code end as account,
              a.type,
              count(distinct a.code)::text as accounts,
              coalesce(sum(p.amount) filter (where p.direction = 'DEBIT'), 0)::numeric(20,2)::text as debits,
              coalesce(sum(p.amount) filter (where p.direction = 'CREDIT'), 0)::numeric(20,2)::text as credits
         from accounts a join journal_postings p on p.account_code = a.code
        where a.currency = ?
        group by 1, 2
        order by 1`,
      [currency],
    );
    return rows.map((r) => ({ account: r.account, type: r.type, accounts: Number(r.accounts), debits: r.debits, credits: r.credits }));
  }
}
