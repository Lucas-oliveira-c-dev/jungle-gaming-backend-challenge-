import { join } from 'node:path';
import type { Subprocess } from 'bun';
import { testEnv } from './harness';

const ROOT = join(import.meta.dir, '..', '..');
let nextPort = 4100 + Math.floor(Math.random() * 500);

export interface Instance {
  name: string;
  baseUrl: string;
  process: Subprocess;
  /** Graceful stop (SIGTERM) */
  stop(): Promise<number>;
  /** Hard crash (SIGKILL) */
  kill(): Promise<number>;
  exited: Promise<number>;
}

/** Starts a real `bun src/main.ts` process — a separate instance sharing DB and queues. */
export async function spawnInstance(suite: string, name: string, env: Record<string, string> = {}): Promise<Instance> {
  const port = nextPort++;
  const child = Bun.spawn([process.execPath, 'src/main.ts'], {
    cwd: ROOT,
    env: testEnv(suite, { PORT: String(port), INSTANCE_ID: name, LOG_LEVEL: process.env.TEST_LOG_LEVEL ?? 'warn', ...env }),
    stdout: 'inherit',
    stderr: 'inherit',
  });
  const baseUrl = `http://127.0.0.1:${port}`;
  const deadline = Date.now() + 30_000;
  for (;;) {
    if (child.exitCode !== null) throw new Error(`${name} exited with ${child.exitCode} during startup`);
    try {
      const res = await fetch(`${baseUrl}/health/ready`);
      if (res.ok) break;
    } catch {
      // not listening yet
    }
    if (Date.now() > deadline) {
      child.kill('SIGKILL');
      throw new Error(`${name} did not become ready`);
    }
    await Bun.sleep(200);
  }
  return {
    name,
    baseUrl,
    process: child,
    exited: child.exited,
    stop: async () => {
      child.kill('SIGTERM');
      return child.exited;
    },
    kill: async () => {
      child.kill('SIGKILL');
      return child.exited;
    },
  };
}
