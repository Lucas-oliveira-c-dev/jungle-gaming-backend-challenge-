import { Migrator } from '@mikro-orm/migrations';
import { MikroORM, defineConfig } from '@mikro-orm/postgresql';
import type { AppConfig } from '../../config';
import { Migration20261031000001_initial } from './migrations/Migration20261031000001_initial';
import { Migration20261101000001_double_entry } from './migrations/Migration20261101000001_double_entry';
import { ENTITY_SCHEMAS } from './records';

export function ormConfig(config: Pick<AppConfig, 'databaseUrl' | 'dbPoolMax'>) {
  return defineConfig({
    clientUrl: config.databaseUrl,
    entities: ENTITY_SCHEMAS,
    discovery: { warnWhenNoEntities: false },
    pool: { min: 0, max: config.dbPoolMax },
    // Explicit list: works the same under Bun, in Docker and in tests (no glob discovery).
    migrations: {
      migrationsList: [
        { name: 'Migration20261031000001_initial', class: Migration20261031000001_initial },
        { name: 'Migration20261101000001_double_entry', class: Migration20261101000001_double_entry },
      ],
      transactional: true,
      allOrNothing: true,
      snapshot: false,
    },
    extensions: [Migrator],
    // Each request/message works on its own fork; never share the global identity map.
    allowGlobalContext: false,
    debug: false,
    logger: () => undefined, // framework chatter off; app logs are structured via pino
  });
}

export async function initOrm(config: Pick<AppConfig, 'databaseUrl' | 'dbPoolMax'>): Promise<MikroORM> {
  return MikroORM.init(ormConfig(config));
}
