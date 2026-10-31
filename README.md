# Distributed Wagering Processor

Serviço financeiro que processa transações de apostas (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) vindas de vários provedores, por HTTP e por SQS, mantendo-se correto com mensagens duplicadas, fora de ordem e processadas por várias instâncias ao mesmo tempo.

**Stack:** Bun 1.4 · TypeScript (strict) · NestJS 11 · MikroORM 6 · PostgreSQL 17 · AWS SQS (MiniStack) · Docker Compose.

As decisões de projeto, trade-offs e limitações estão em [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Pré-requisitos

- Docker (Docker Desktop ou OrbStack) com Compose v2
- Bun ≥ 1.3 (só para rodar os testes e scripts fora do container)

## Subindo tudo

```bash
docker compose up --build -d
```

Isso sobe:

| Serviço | Porta | Função |
|---|---|---|
| `postgres` | 5432 | banco (`wagering`) + banco de testes (`wagering_test`) |
| `sqs` | 4566 | MiniStack (emulador SQS) |
| `migrate` | — | aplica as migrations e termina |
| `app-1`, `app-2`, `app-3` | 3001, 3002, 3003 | **três instâncias** da aplicação (HTTP + consumidor SQS + publisher da outbox + worker de referências pendentes) |

As filas (`wager-transactions.fifo`, `wager-transactions-dlq.fifo`, `wagering-events.fifo`) são criadas de forma idempotente pela própria aplicação ao iniciar.

> **Por que MiniStack e não LocalStack?** Desde a versão 2026.03 a imagem do LocalStack exige auth token. O MiniStack é MIT, não exige conta e expõe a mesma API na porta 4566. Para usar o LocalStack, basta trocar a imagem do serviço `sqs` (por exemplo `localstack/localstack:4.14`, a última sem token) — nada no código muda.

### Rodando local (sem containers para a app)

```bash
bun install
docker compose up -d postgres sqs
bun run migrate            # aplica migrations
bun run dev                # http://localhost:3000
```

Variáveis de ambiente: veja [.env.example](.env.example) e [src/config.ts](src/config.ts).

## Exemplos

```bash
# criar wallet
curl -s localhost:3001/wallets -H 'content-type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'

# apostar (troque WALLET_ID)
curl -s localhost:3002/wagering/transactions -H 'content-type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
       "walletId":"WALLET_ID","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'

# enviar pela fila
aws --endpoint-url http://localhost:4566 sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id WALLET_ID --message-deduplication-id msg-123 \
  --message-body '{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-07-29T15:00:00.000Z","data":{...,"idempotencyKey":"provider-a:transaction-124"}}'

# consultas
curl -s localhost:3003/wallets/WALLET_ID
curl -s 'localhost:3003/wallets/WALLET_ID/ledger?limit=50'
curl -s -X POST localhost:3003/wallets/WALLET_ID/reconciliation
curl -s localhost:3001/providers/provider-a/wagering/transactions/transaction-123
curl -s localhost:3001/health/ready
curl -s localhost:3001/metrics
```

## Endpoints e status HTTP

| Endpoint | |
|---|---|
| `POST /wallets` | cria wallet (+ transação `OPENING` e lançamento `CREDIT` se saldo > 0) |
| `GET /wallets/:walletId` | wallet |
| `GET /wallets/:walletId/ledger?cursor=&limit=` | ledger paginado por cursor opaco e estável |
| `POST /wallets/:walletId/reconciliation` | saldo materializado × saldo reconstruído pelo ledger |
| `POST /wagering/transactions` | submete transação (header `Idempotency-Key` obrigatório) |
| `GET /wagering/transactions/:transactionId` | por id interno |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | por id do provedor |
| `GET /health/live`, `GET /health/ready` | liveness / readiness (Postgres + SQS), sem autenticação |
| `GET /metrics` | Prometheus |

O mesmo status para a mesma situação em todos os endpoints:

| Status | Significado | O provedor deve… |
|---|---|---|
| `201` | transação criada e `PROCESSED` | — |
| `200` | replay idempotente de uma transação `PROCESSED` (`idempotentReplay: true`) | — |
| `202` | aceita, aguardando referência (`PENDING_REFERENCE`) | consultar depois / aguardar evento |
| `400` | payload inválido (`INVALID_PAYLOAD`) | corrigir o payload |
| `404` | recurso inexistente | — |
| `409` | conflito de idempotência (`IDEMPOTENCY_CONFLICT`) ou wallet duplicada | **não** reenviar; investigar |
| `422` | rejeição por regra de negócio — corpo traz `status: REJECTED` + `failureCode` (ou `WALLET_NOT_FOUND`) | desistir ou corrigir |
| `503` | falha transitória de infraestrutura (`retryable: true`, `Retry-After`) | **reenviar a mesma requisição** |
| `500` | erro inesperado | reenviar com backoff |

Erros sempre têm o formato `{ "error": { "code", "message", "retryable" }, "correlationId" }`. Os `failureCode` estão documentados em [ARCHITECTURE.md](ARCHITECTURE.md#7-códigos-de-falha).

## Testes

Com `postgres` e `sqs` de pé (`docker compose up -d postgres sqs`):

```bash
bun run test:unit           # domínio e idempotência — sem dependências externas
bun run test:integration    # Postgres e SQS reais: schema, API, inbox, outbox, DLQ, atomicidade
bun run test:concurrency    # paralelismo real + 3 processos + crash/restart
bun run test                # tudo
bun run typecheck
```

A suíte de concorrência sobe **processos `bun src/main.ts` reais** (não mocks) e cobre: a mesma aposta 50× em paralelo, a disputa 100/80/80, wallets distintas em paralelo, 3 instâncias simultâneas, `SIGKILL` depois do commit e antes do ack, publishers concorrentes, `REFUND` antes da `BET` e reinício no meio da carga. Todo teste termina verificando `wallet.balance == saldo reconstruído pelo ledger`.

### Teste de carga (opcional)

```bash
docker compose up --build -d
bun run test:load          # DURATION_S, CONCURRENCY, WALLETS, HOT_WALLET_SHARE configuráveis
```

Reporta throughput, p50/p95/p99, taxa de erro, conflitos de lock, lag da outbox e reconcilia a wallet "quente" ao final. Metodologia em [ARCHITECTURE.md](ARCHITECTURE.md#13-teste-de-carga).

## Estrutura

```
src/
  domain/            Money, Wallet, WagerTransaction, WalletLedgerEntry, Inbox/Outbox, eventos, política de apostas
  application/       casos de uso (submit, create wallet, queries, reconciliação, outbox, referências pendentes) + portas
  infrastructure/
    persistence/     MikroORM (EntitySchema), migrations, repositórios/unit of work, outbox store
    messaging/       SQS: provisionamento de filas, consumidor, publisher de eventos
    observability/   logs JSON (pino), métricas Prometheus
  http/              controllers NestJS, filtro de erros, guard de autenticação (ponto de extensão)
  workers/           loop de polling dos workers
test/
  unit/ integration/ concurrency/ support/
scripts/             migrate, setup de filas, teste de carga
```
