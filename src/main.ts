import { createApplication } from './app';
import { loadConfig } from './config';

const app = await createApplication(loadConfig());
await app.start();

let shuttingDown = false;
for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  process.on(signal, async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    const watchdog = setTimeout(() => process.exit(1), 25_000);
    try {
      await app.stop();
      process.exit(0);
    } finally {
      clearTimeout(watchdog);
    }
  });
}
