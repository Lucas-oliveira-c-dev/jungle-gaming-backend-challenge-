import { Migration } from '@mikro-orm/migrations';

/**
 * Double-entry general ledger on top of the per-wallet sub-ledger.
 *
 * Enforced by the schema:
 *  - every journal entry balances (sum of debits = sum of credits), has >= 2 postings and
 *    a single currency matching its accounts — checked at commit (deferred trigger);
 *  - one journal entry per wager transaction;
 *  - every wallet sub-ledger entry has the matching posting on its wallet account
 *    (same transaction, direction and amount) — checked at commit;
 *  - accounts, journal entries and postings are append-only.
 *
 * Existing wallet ledger rows are backfilled so the migration works on populated databases.
 */
export class Migration20261101000001_double_entry extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      create table accounts (
        code       text primary key check (code ~ '^(wallet|provider|house):'),
        type       text not null check (type in ('ASSET','LIABILITY')),
        currency   char(3) not null check (currency ~ '^[A-Z]{3}$'),
        created_at timestamptz not null
      );

      create table journal_entries (
        id             uuid primary key,
        transaction_id uuid not null references wager_transactions(id) deferrable initially deferred,
        created_at     timestamptz not null,
        constraint journal_one_entry_per_tx_uq unique (transaction_id)
      );

      create table journal_postings (
        id               uuid primary key,
        journal_entry_id uuid not null references journal_entries(id) deferrable initially deferred,
        account_code     text not null references accounts(code) deferrable initially deferred,
        direction        text not null check (direction in ('DEBIT','CREDIT')),
        amount           numeric(20,2) not null check (amount > 0),
        currency         char(3) not null,
        created_at       timestamptz not null
      );

      create index journal_postings_entry on journal_postings (journal_entry_id);
      create index journal_postings_account on journal_postings (account_code);
    `);

    // ---- backfill from the existing wallet sub-ledger. The lock blocks concurrent ledger
    // writes until this migration commits, so no movement can slip between the backfill
    // snapshot and the trigger below. (Deploy note: run migrations before new instances.)
    this.addSql(`
      lock table wallet_ledger_entries in share row exclusive mode;
      insert into accounts (code, type, currency, created_at)
      select distinct 'wallet:' || l.wallet_id, 'LIABILITY', l.currency, now() from wallet_ledger_entries l
      union
      select distinct
             case when t.kind = 'OPENING' then 'house:funding:' || l.currency else 'provider:' || t.provider_id || ':' || l.currency end,
             case when t.kind = 'OPENING' then 'ASSET' else 'LIABILITY' end,
             l.currency, now()
        from wallet_ledger_entries l join wager_transactions t on t.id = l.transaction_id
      on conflict (code) do nothing;

      create temporary table _backfill on commit drop as
        select gen_random_uuid() as journal_id, l.*, t.kind, t.provider_id
          from wallet_ledger_entries l join wager_transactions t on t.id = l.transaction_id;

      insert into journal_entries (id, transaction_id, created_at)
      select journal_id, transaction_id, created_at from _backfill;

      insert into journal_postings (id, journal_entry_id, account_code, direction, amount, currency, created_at)
      select gen_random_uuid(), journal_id, 'wallet:' || wallet_id, direction, amount, currency, created_at from _backfill
      union all
      select gen_random_uuid(), journal_id,
             case when kind = 'OPENING' then 'house:funding:' || currency else 'provider:' || provider_id || ':' || currency end,
             case direction when 'CREDIT' then 'DEBIT' else 'CREDIT' end,
             amount, currency, created_at
        from _backfill;
    `);

    // ---- append-only
    this.addSql(`
      create function forbid_append_only_mutation() returns trigger language plpgsql as $$
      begin
        raise exception '% is append-only (% blocked)', tg_table_name, tg_op using errcode = 'restrict_violation';
      end $$;

      create trigger accounts_append_only before update or delete on accounts
        for each row execute function forbid_append_only_mutation();
      create trigger journal_entries_append_only before update or delete on journal_entries
        for each row execute function forbid_append_only_mutation();
      create trigger journal_postings_append_only before update or delete on journal_postings
        for each row execute function forbid_append_only_mutation();
      create trigger journal_no_truncate before truncate on journal_postings
        for each statement execute function forbid_append_only_mutation();
      create trigger journal_entries_no_truncate before truncate on journal_entries
        for each statement execute function forbid_append_only_mutation();
    `);

    // ---- every journal entry balances (checked at commit)
    this.addSql(`
      create function check_journal_entry_balanced() returns trigger language plpgsql as $$
      declare
        debits numeric; credits numeric; postings int; currencies int; mismatched int;
      begin
        select coalesce(sum(amount) filter (where direction = 'DEBIT'), 0),
               coalesce(sum(amount) filter (where direction = 'CREDIT'), 0),
               count(*), count(distinct p.currency),
               count(*) filter (where a.currency <> p.currency)
          into debits, credits, postings, currencies, mismatched
          from journal_postings p join accounts a on a.code = p.account_code
         where p.journal_entry_id = new.journal_entry_id;
        if postings < 2 or debits <> credits then
          raise exception 'journal entry % is unbalanced (debits %, credits %, postings %)', new.journal_entry_id, debits, credits, postings
            using errcode = 'check_violation';
        end if;
        if currencies <> 1 or mismatched > 0 then
          raise exception 'journal entry % mixes currencies', new.journal_entry_id using errcode = 'check_violation';
        end if;
        return null;
      end $$;

      create constraint trigger journal_entry_balanced
        after insert on journal_postings
        deferrable initially deferred
        for each row execute function check_journal_entry_balanced();
    `);

    // ---- wallet sub-ledger <-> general ledger (checked at commit)
    this.addSql(`
      create function check_wallet_entry_journaled() returns trigger language plpgsql as $$
      begin
        if not exists (
          select 1 from journal_postings p join journal_entries j on j.id = p.journal_entry_id
           where j.transaction_id = new.transaction_id
             and p.account_code = 'wallet:' || new.wallet_id
             and p.direction = new.direction
             and p.amount = new.amount) then
          raise exception 'wallet ledger entry % has no matching journal posting', new.id using errcode = 'check_violation';
        end if;
        return null;
      end $$;

      create constraint trigger wallet_entry_journaled
        after insert on wallet_ledger_entries
        deferrable initially deferred
        for each row execute function check_wallet_entry_journaled();
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`
      drop trigger if exists wallet_entry_journaled on wallet_ledger_entries;
      drop table if exists journal_postings;
      drop table if exists journal_entries;
      drop table if exists accounts;
      drop function if exists check_wallet_entry_journaled();
      drop function if exists check_journal_entry_balanced();
      drop function if exists forbid_append_only_mutation();
    `);
  }
}
