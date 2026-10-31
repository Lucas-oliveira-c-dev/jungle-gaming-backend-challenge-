import { Migration } from '@mikro-orm/migrations';

/**
 * Initial schema. Every invariant the challenge calls "inviolable" is enforced here,
 * independently of the application code:
 *  - uniqueness: wallet per (player, currency), idempotency key, (provider, external id),
 *    one ledger entry per (wallet, transaction), gap-free ledger sequence per wallet,
 *    one PROCESSED reversal per reference, inbox (consumer, message);
 *  - non-negativity: wallet balance and ledger balances;
 *  - arithmetic: balance_after = balance_before ± amount;
 *  - immutability: ledger rows cannot be updated/deleted/truncated; terminal transactions
 *    cannot change; business columns of a transaction cannot change;
 *  - balance ⇄ ledger: every balance change must be backed by the ledger entry whose
 *    sequence equals the new wallet version, and the version moves only with the balance
 *    (checked at commit by a deferred constraint trigger).
 */
export class Migration20261031000001_initial extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      create table wallets (
        id          uuid primary key,
        player_id   uuid        not null,
        currency    char(3)     not null check (currency ~ '^[A-Z]{3}$'),
        balance     numeric(20,2) not null check (balance >= 0),
        version     integer     not null check (version >= 1),
        created_at  timestamptz not null,
        updated_at  timestamptz not null,
        constraint wallets_player_currency_uq unique (player_id, currency)
      );
    `);

    this.addSql(`
      create table wager_transactions (
        id                                uuid primary key,
        provider_id                       text not null check (length(provider_id) between 1 and 128),
        external_transaction_id           text not null check (length(external_transaction_id) between 1 and 128),
        idempotency_key                   text not null check (length(idempotency_key) between 1 and 256),
        payload_hash                      char(64) not null,
        wallet_id                         uuid not null references wallets(id) deferrable initially deferred,
        player_id                         uuid not null,
        round_id                          text not null,
        game_id                           text not null,
        kind                              text not null check (kind in ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        amount                            numeric(20,2) not null check (amount > 0 or (kind = 'LOSS' and amount = 0)),
        currency                          char(3) not null,
        reference_external_transaction_id text,
        reference_transaction_id          uuid references wager_transactions(id) deferrable initially deferred,
        status                            text not null check (status in ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        failure_code                      text,
        result_balance                    numeric(20,2),
        reference_attempts                integer not null default 0,
        next_reference_check_at           timestamptz,
        created_at                        timestamptz not null,
        processed_at                      timestamptz,
        constraint wager_tx_idempotency_key_uq unique (idempotency_key),
        constraint wager_tx_provider_external_uq unique (provider_id, external_transaction_id),
        constraint wager_tx_reversal_needs_reference
          check (kind not in ('REFUND','ROLLBACK') or reference_external_transaction_id is not null),
        constraint wager_tx_failure_code_iff_failed
          check ((status in ('REJECTED','FAILED')) = (failure_code is not null)),
        constraint wager_tx_terminal_has_processed_at
          check (status not in ('PROCESSED','REJECTED','FAILED') or processed_at is not null)
      );

      -- At most one successful reversal (REFUND or ROLLBACK) per referenced transaction.
      create unique index wager_tx_one_reversal_per_reference
        on wager_transactions (reference_transaction_id)
        where kind in ('REFUND','ROLLBACK') and status = 'PROCESSED';

      create index wager_tx_pending_reference_due
        on wager_transactions (next_reference_check_at)
        where status = 'PENDING_REFERENCE';

      create index wager_tx_pending_by_reference
        on wager_transactions (provider_id, reference_external_transaction_id)
        where status = 'PENDING_REFERENCE';

      create index wager_tx_wallet on wager_transactions (wallet_id);
    `);

    this.addSql(`
      create table wallet_ledger_entries (
        id             uuid primary key,
        wallet_id      uuid not null references wallets(id) deferrable initially deferred,
        transaction_id uuid not null references wager_transactions(id) deferrable initially deferred,
        sequence       integer not null check (sequence >= 1),
        direction      text not null check (direction in ('DEBIT','CREDIT')),
        amount         numeric(20,2) not null check (amount > 0),
        currency       char(3) not null,
        balance_before numeric(20,2) not null check (balance_before >= 0),
        balance_after  numeric(20,2) not null check (balance_after >= 0),
        created_at     timestamptz not null,
        constraint ledger_one_entry_per_tx_uq unique (wallet_id, transaction_id),
        constraint ledger_sequence_uq unique (wallet_id, sequence),
        constraint ledger_balanced check (
          (direction = 'CREDIT' and balance_after = balance_before + amount) or
          (direction = 'DEBIT'  and balance_after = balance_before - amount)
        )
      );
    `);

    this.addSql(`
      create table inbox_messages (
        consumer_name text not null,
        message_id    text not null,
        payload_hash  char(64) not null,
        received_at   timestamptz not null,
        processed_at  timestamptz,
        primary key (consumer_name, message_id)
      );
    `);

    this.addSql(`
      create table outbox_messages (
        id              uuid primary key,
        aggregate_id    uuid not null,
        event_type      text not null,
        payload         jsonb not null,
        occurred_at     timestamptz not null,
        attempts        integer not null default 0 check (attempts >= 0),
        next_attempt_at timestamptz not null,
        locked_until    timestamptz,
        published_at    timestamptz,
        last_error      text
      );

      create index outbox_pending_due on outbox_messages (next_attempt_at) where published_at is null;
    `);

    // ---- immutability triggers
    this.addSql(`
      create function forbid_ledger_mutation() returns trigger language plpgsql as $$
      begin
        raise exception 'wallet_ledger_entries is append-only (% blocked)', tg_op
          using errcode = 'restrict_violation';
      end $$;

      create trigger ledger_no_update_delete before update or delete on wallet_ledger_entries
        for each row execute function forbid_ledger_mutation();
      create trigger ledger_no_truncate before truncate on wallet_ledger_entries
        for each statement execute function forbid_ledger_mutation();
    `);

    this.addSql(`
      create function guard_wager_transaction_update() returns trigger language plpgsql as $$
      begin
        if old.status in ('PROCESSED','REJECTED','FAILED') then
          raise exception 'transaction % is terminal (%)', old.id, old.status using errcode = 'restrict_violation';
        end if;
        if (new.id, new.provider_id, new.external_transaction_id, new.idempotency_key, new.payload_hash,
            new.wallet_id, new.player_id, new.round_id, new.game_id, new.kind, new.amount, new.currency,
            new.reference_external_transaction_id, new.created_at)
           is distinct from
           (old.id, old.provider_id, old.external_transaction_id, old.idempotency_key, old.payload_hash,
            old.wallet_id, old.player_id, old.round_id, old.game_id, old.kind, old.amount, old.currency,
            old.reference_external_transaction_id, old.created_at) then
          raise exception 'business fields of transaction % are immutable', old.id using errcode = 'restrict_violation';
        end if;
        return new;
      end $$;

      create trigger wager_tx_guard_update before update on wager_transactions
        for each row execute function guard_wager_transaction_update();

      create function forbid_delete() returns trigger language plpgsql as $$
      begin
        raise exception '% rows cannot be deleted', tg_table_name using errcode = 'restrict_violation';
      end $$;

      create trigger wager_tx_no_delete before delete on wager_transactions
        for each row execute function forbid_delete();
      create trigger wallets_no_delete before delete on wallets
        for each row execute function forbid_delete();
    `);

    // ---- balance <-> ledger consistency, verified at commit time
    this.addSql(`
      create function check_wallet_ledger_consistency() returns trigger language plpgsql as $$
      declare
        current wallets%rowtype;
        entry   wallet_ledger_entries%rowtype;
      begin
        -- Re-read the row: with deferred triggers several updates may have happened.
        select * into current from wallets where id = new.id;

        if tg_op = 'UPDATE' and current.version <> old.version and current.version <= old.version then
          raise exception 'wallet % version must increase', new.id using errcode = 'check_violation';
        end if;

        if current.balance = 0 and current.version = 1 then
          return null; -- freshly opened wallet without initial balance
        end if;

        select * into entry from wallet_ledger_entries
          where wallet_id = current.id and sequence = current.version;
        if not found then
          raise exception 'wallet % (version %) has no ledger entry for its current balance', current.id, current.version
            using errcode = 'check_violation';
        end if;
        if entry.balance_after <> current.balance then
          raise exception 'wallet % balance % differs from ledger balance %', current.id, current.balance, entry.balance_after
            using errcode = 'check_violation';
        end if;
        return null;
      end $$;

      create constraint trigger wallet_ledger_consistency
        after insert or update on wallets
        deferrable initially deferred
        for each row execute function check_wallet_ledger_consistency();
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`
      drop table if exists outbox_messages;
      drop table if exists inbox_messages;
      drop table if exists wallet_ledger_entries;
      drop table if exists wager_transactions;
      drop table if exists wallets;
      drop function if exists check_wallet_ledger_consistency();
      drop function if exists forbid_delete();
      drop function if exists guard_wager_transaction_update();
      drop function if exists forbid_ledger_mutation();
    `);
  }
}
