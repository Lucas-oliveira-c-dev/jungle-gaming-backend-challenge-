import { createApplication } from './app';
import { loadConfig } from './config';

const app = await createApplication(loadConfig());
await app.start();

let shuttingDown = false;
for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  process.on(signal, async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    // Bounded shutdown: exit 1 if draining hangs or fails, 0 only after a clean stop.
    setTimeout(() => process.exit(1), 25_000).unref();
    try {
      await app.stop();
      process.exit(0);
    } catch (error) {
      console.error(JSON.stringify({ level: 'error', msg: 'shutdown failed', err: (error as Error).message }));
      process.exit(1);
    }
  });
}
