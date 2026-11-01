import { Migration } from '@mikro-orm/migrations';

/**
 * Closes the remaining gaps between what the schema promised and what it enforced:
 *  - wallet sub-ledger is a gap-free chain: each entry continues the previous one
 *    (sequence + 1, balance_before = previous balance_after), and the wallet's version
 *    matches its last entry — so "ledger without balance" also fails at commit;
 *  - a posting on a wallet account requires the matching wallet sub-ledger entry
 *    (the reverse direction of wallet_entry_journaled);
 *  - the journal balance check reports a missing account instead of "unbalanced";
 *  - index used by the outbox claim to keep per-aggregate order after failed publishes.
 */
export class Migration20261101000002_ledger_integrity extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      create function check_wallet_ledger_chain() returns trigger language plpgsql as $$
      declare
        previous wallet_ledger_entries%rowtype;
        wallet_version integer;
        last_sequence integer;
      begin
        select * into previous from wallet_ledger_entries
         where wallet_id = new.wallet_id and sequence < new.sequence
         order by sequence desc limit 1;
        if found then
          if new.sequence <> previous.sequence + 1 or new.balance_before <> previous.balance_after then
            raise exception 'wallet ledger entry % breaks the chain (sequence % after %, balance_before % after %)',
              new.id, new.sequence, previous.sequence, new.balance_before, previous.balance_after
              using errcode = 'check_violation';
          end if;
        elsif new.balance_before <> 0 or new.sequence not in (1, 2) then
          -- first entry: sequence 1 (OPENING) or 2 (first movement of a zero-balance wallet)
          raise exception 'first ledger entry % of wallet % must start from zero at sequence 1 or 2', new.id, new.wallet_id
            using errcode = 'check_violation';
        end if;

        select version into wallet_version from wallets where id = new.wallet_id;
        select max(sequence) into last_sequence from wallet_ledger_entries where wallet_id = new.wallet_id;
        if wallet_version is distinct from last_sequence then
          raise exception 'wallet % is at version % but its ledger ends at sequence %', new.wallet_id, wallet_version, last_sequence
            using errcode = 'check_violation';
        end if;
        return null;
      end $$;

      create constraint trigger wallet_ledger_chain
        after insert on wallet_ledger_entries
        deferrable initially deferred
        for each row execute function check_wallet_ledger_chain();
    `);

    this.addSql(`
      create function check_wallet_posting_has_entry() returns trigger language plpgsql as $$
      begin
        if new.account_code like 'wallet:%' and not exists (
          select 1 from wallet_ledger_entries l join journal_entries j on j.transaction_id = l.transaction_id
           where j.id = new.journal_entry_id
             and 'wallet:' || l.wallet_id = new.account_code
             and l.direction = new.direction
             and l.amount = new.amount) then
          raise exception 'posting % on % has no matching wallet ledger entry', new.id, new.account_code
            using errcode = 'check_violation';
        end if;
        return null;
      end $$;

      create constraint trigger wallet_posting_has_entry
        after insert on journal_postings
        deferrable initially deferred
        for each row execute function check_wallet_posting_has_entry();
    `);

    this.addSql(`
      create or replace function check_journal_entry_balanced() returns trigger language plpgsql as $$
      declare
        debits numeric; credits numeric; postings int; currencies int; mismatched int; missing int;
      begin
        select coalesce(sum(p.amount) filter (where p.direction = 'DEBIT'), 0),
               coalesce(sum(p.amount) filter (where p.direction = 'CREDIT'), 0),
               count(*), count(distinct p.currency),
               count(*) filter (where a.currency <> p.currency),
               count(*) filter (where a.code is null)
          into debits, credits, postings, currencies, mismatched, missing
          from journal_postings p left join accounts a on a.code = p.account_code
         where p.journal_entry_id = new.journal_entry_id;
        if missing > 0 then
          raise exception 'journal entry % posts to an unknown account', new.journal_entry_id using errcode = 'foreign_key_violation';
        end if;
        if postings < 2 or debits <> credits then
          raise exception 'journal entry % is unbalanced (debits %, credits %, postings %)', new.journal_entry_id, debits, credits, postings
            using errcode = 'check_violation';
        end if;
        if currencies <> 1 or mismatched > 0 then
          raise exception 'journal entry % mixes currencies', new.journal_entry_id using errcode = 'check_violation';
        end if;
        return null;
      end $$;
    `);

    this.addSql(`
      create index outbox_pending_by_aggregate on outbox_messages (aggregate_id, occurred_at) where published_at is null;
      create index outbox_pending_by_time on outbox_messages (occurred_at, id) where published_at is null;
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`
      drop index if exists outbox_pending_by_aggregate;
      drop index if exists outbox_pending_by_time;
      drop trigger if exists wallet_posting_has_entry on journal_postings;
      drop trigger if exists wallet_ledger_chain on wallet_ledger_entries;
      drop function if exists check_wallet_posting_has_entry();
      drop function if exists check_wallet_ledger_chain();
      create or replace function check_journal_entry_balanced() returns trigger language plpgsql as $$
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
    `);
  }
}
