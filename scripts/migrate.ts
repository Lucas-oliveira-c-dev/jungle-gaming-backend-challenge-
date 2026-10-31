import { loadConfig } from '../src/config';
import { initOrm } from '../src/infrastructure/persistence/orm';

/** Usage: bun scripts/migrate.ts up|down */
const direction = process.argv[2] ?? 'up';
const orm = await initOrm(loadConfig());
try {
  const migrator = orm.getMigrator();
  const applied = direction === 'down' ? await migrator.down() : await migrator.up();
  console.log(JSON.stringify({ direction, migrations: applied.map((m) => m.name) }));
} finally {
  await orm.close();
}
