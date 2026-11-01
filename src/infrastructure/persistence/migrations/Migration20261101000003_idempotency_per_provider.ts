import { Migration } from '@mikro-orm/migrations';

/**
 * - Idempotency keys are unique per provider, not globally. With a global key, a provider
 *   could pre-claim another provider's predictable key ("providerB:win-123") and make that
 *   provider's real transaction fail as an idempotency conflict (a cross-tenant block that
 *   would survive authentication, which only binds the body providerId to the token).
 * - Indexes for the retention job (published outbox rows, processed inbox rows).
 */
export class Migration20261101000003_idempotency_per_provider extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      alter table wager_transactions drop constraint wager_tx_idempotency_key_uq;
      alter table wager_transactions add constraint wager_tx_provider_idempotency_key_uq unique (provider_id, idempotency_key);

      create index outbox_published_at on outbox_messages (published_at) where published_at is not null;
      create index inbox_processed_at on inbox_messages (processed_at);
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`
      drop index if exists inbox_processed_at;
      drop index if exists outbox_published_at;
      alter table wager_transactions drop constraint wager_tx_provider_idempotency_key_uq;
      alter table wager_transactions add constraint wager_tx_idempotency_key_uq unique (idempotency_key);
    `);
  }
}
