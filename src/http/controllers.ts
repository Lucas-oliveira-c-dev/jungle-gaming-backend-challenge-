import {
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  Inject,
  Injectable,
  Param,
  ParseUUIDPipe,
  Post,
  Query,
  Req,
  Res,
  UseGuards,
  type CanActivate,
  type ExecutionContext,
} from '@nestjs/common';
import type { Request, Response } from 'express';
import type { CreateWallet } from '../application/create-wallet';
import type { WageringQueries } from '../application/queries';
import type { SubmitWagerTransaction } from '../application/submit-wager-transaction';
import { parseWagerSubmission } from '../application/wager-request';
import { WagerTransactionStatus } from '../domain/wager-transaction';

export const TOKENS = {
  createWallet: Symbol('CreateWallet'),
  submitWager: Symbol('SubmitWagerTransaction'),
  queries: Symbol('WageringQueries'),
  health: Symbol('HealthProbe'),
  metrics: Symbol('MetricsRegistry'),
} as const;

type CorrelatedRequest = Request & { correlationId: string };

/**
 * Authentication extension point (see ARCHITECTURE.md §Autenticação). Intentionally a
 * no-op: the target design validates an OIDC client-credentials token issued by an
 * external IdP (Keycloak/Zitadel) and binds `providerId` to the token's client id.
 */
@Injectable()
export class ProviderAuthGuard implements CanActivate {
  canActivate(_context: ExecutionContext): boolean {
    return true;
  }
}

const UUID = new ParseUUIDPipe({ version: undefined });

/** HTTP status from the persisted transaction status (consistent for new and replayed). */
function statusFor(status: WagerTransactionStatus, replay: boolean): number {
  switch (status) {
    case WagerTransactionStatus.Processed:
      return replay ? 200 : 201;
    case WagerTransactionStatus.Pending:
    case WagerTransactionStatus.PendingReference:
      return 202;
    case WagerTransactionStatus.Rejected:
      return 422;
    case WagerTransactionStatus.Failed:
      return 500;
  }
}

@Controller('wallets')
@UseGuards(ProviderAuthGuard)
export class WalletsController {
  constructor(
    @Inject(TOKENS.createWallet) private readonly createWallet: CreateWallet,
    @Inject(TOKENS.queries) private readonly queries: WageringQueries,
  ) {}

  @Post()
  @HttpCode(201)
  create(@Body() body: unknown, @Req() req: CorrelatedRequest) {
    return this.createWallet.execute(body, { correlationId: req.correlationId });
  }

  @Get(':walletId')
  get(@Param('walletId', UUID) walletId: string) {
    return this.queries.wallet(walletId);
  }

  @Get(':walletId/ledger')
  ledger(@Param('walletId', UUID) walletId: string, @Query('cursor') cursor?: string, @Query('limit') limit?: string) {
    return this.queries.ledger(walletId, cursor, limit);
  }

  @Post(':walletId/reconciliation')
  @HttpCode(200)
  reconcile(@Param('walletId', UUID) walletId: string, @Req() req: CorrelatedRequest) {
    return this.queries.reconcile(walletId, { correlationId: req.correlationId });
  }
}

@Controller()
@UseGuards(ProviderAuthGuard)
export class WageringController {
  constructor(
    @Inject(TOKENS.submitWager) private readonly submitWager: SubmitWagerTransaction,
    @Inject(TOKENS.queries) private readonly queries: WageringQueries,
  ) {}

  @Post('wagering/transactions')
  async submit(
    @Body() body: unknown,
    @Headers('idempotency-key') idempotencyKey: string | undefined,
    @Req() req: CorrelatedRequest,
    @Res({ passthrough: true }) res: Response,
  ) {
    const submission = parseWagerSubmission(body, idempotencyKey);
    const result = await this.submitWager.execute(submission, { source: 'http', correlationId: req.correlationId });
    res.status(statusFor(result.status, result.idempotentReplay));
    return result;
  }

  @Get('wagering/transactions/:transactionId')
  byId(@Param('transactionId', UUID) transactionId: string) {
    return this.queries.transaction(transactionId);
  }

  @Get('ledger/trial-balance')
  trialBalance(@Query('currency') currency?: string) {
    return this.queries.trialBalance(currency);
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  byExternalId(@Param('providerId') providerId: string, @Param('externalTransactionId') externalTransactionId: string) {
    return this.queries.transactionByExternalId(providerId, externalTransactionId);
  }
}

export interface HealthProbe {
  database(): Promise<boolean>;
  queue(): Promise<boolean>;
  /** True during graceful shutdown. */
  draining(): boolean;
}

/** Health endpoints are public (no guard). */
@Controller()
export class OpsController {
  constructor(
    @Inject(TOKENS.health) private readonly health: HealthProbe,
    @Inject(TOKENS.metrics) private readonly registry: { contentType: string; metrics(): Promise<string> },
  ) {}

  @Get('health/live')
  live() {
    return { status: 'ok' };
  }

  @Get('health/ready')
  async ready(@Res() res: Response) {
    if (this.health.draining()) {
      res.status(503).json({ status: 'draining', checks: {} });
      return;
    }
    const [database, queue] = await Promise.all([this.health.database(), this.health.queue()]);
    const ok = database && queue;
    res.status(ok ? 200 : 503).json({ status: ok ? 'ok' : 'unavailable', checks: { postgres: database, sqs: queue } });
  }

  @Get('metrics')
  async metrics(@Res() res: Response) {
    res.setHeader('Content-Type', this.registry.contentType);
    res.send(await this.registry.metrics());
  }
}
