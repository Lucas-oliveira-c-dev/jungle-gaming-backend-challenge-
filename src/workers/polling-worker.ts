import type { AppLogger } from '../application/ports';

/**
 * Runs `tick` repeatedly. When a tick did work it loops immediately (drain mode);
 * otherwise it sleeps `intervalMs`. stop() lets the current tick finish.
 */
export class PollingWorker {
  private running = false;
  private current: Promise<void> = Promise.resolve();
  private wake?: () => void;

  constructor(
    private readonly name: string,
    private readonly tick: () => Promise<number>,
    private readonly intervalMs: number,
    private readonly logger: AppLogger,
  ) {}

  start(): void {
    this.running = true;
    this.current = this.run();
    this.logger.info({ worker: this.name }, 'worker started');
  }

  async stop(): Promise<void> {
    this.running = false;
    this.wake?.();
    await this.current;
    this.logger.info({ worker: this.name }, 'worker stopped');
  }

  private async run(): Promise<void> {
    while (this.running) {
      let didWork = 0;
      try {
        didWork = await this.tick();
      } catch (error) {
        this.logger.warn({ worker: this.name, err: (error as Error).message }, 'worker tick failed');
      }
      if (!this.running) break;
      if (didWork === 0) {
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, this.intervalMs);
          this.wake = () => {
            clearTimeout(timer);
            resolve();
          };
        });
      }
    }
  }
}
