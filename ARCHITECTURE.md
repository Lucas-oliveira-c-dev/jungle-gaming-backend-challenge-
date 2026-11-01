# Arquitetura e decisões

Este documento explica **como** o serviço garante as invariantes pedidas e **por que** cada decisão foi tomada. A regra geral: *o banco é a fonte da verdade*. Broker, caches e ordenação são otimizações; nenhuma garantia depende só deles.

## 1. Visão geral

```
            HTTP (NestJS)                         SQS wager-transactions.fifo
                 │                                          │
                 ▼                                          ▼
      parseWagerSubmission ◄──── mesma validação e hash ───► WagerTransactionConsumer
                 │                                          │  (inbox em (consumer, messageId))
                 └──────────────► SubmitWagerTransaction ◄──┘
                                       │
                 ┌──────── uma transação SQL (em.transactional) ─────────┐
                 │ SELECT wallet FOR UPDATE (lock por wallet)             │
                 │ inbox  · wager_transactions · wallets · ledger · outbox│
                 └────────────────────────────────────────────────────────┘
                                       │ commit
               ┌───────────────────────┼────────────────────────┐
               ▼                       ▼                        ▼
       ack da mensagem SQS     PublishOutbox (lease +     ResolvePendingReferences
                               SKIP LOCKED) → SQS         (PENDING_REFERENCE, backoff)
                               wagering-events.fifo
```

Cada instância roda HTTP, consumidor, publisher da outbox e worker de referências pendentes. Os workers podem ser desligados por variável de ambiente (`WORKER_CONSUMER`, `WORKER_OUTBOX`, `WORKER_PENDING_REFERENCE`) para escalar cada papel separadamente.

## 2. Camadas

| Camada | Conteúdo | Depende de |
|---|---|---|
| `domain/` | `Money`, `Wallet` (aggregate root), `WagerTransaction` (máquina de estados), `WalletLedgerEntry`, `InboxMessage`, `OutboxMessage`, eventos de integração, `applyWager` (política) | nada (nem ORM, nem Nest) |
| `application/` | casos de uso e **portas** (`WageringStore`, `WageringUnitOfWork`, `OutboxStore`, `EventPublisher`, `Clock`, `IdGenerator`, `WageringMetrics`, `AppLogger`) | domínio |
| `infrastructure/` | adaptadores: MikroORM, SQS, pino, Prometheus | aplicação + domínio |
| `http/` | controllers finos, filtro de erros, guard | aplicação |

Todas as entidades seguem o padrão pedido: construtor `private`, factories `create`/`open`/`receive`/`enqueue` (que validam) e `rehydrate` (que **não** revalida transições — só reconstrói o estado persistido). `WalletLedgerEntry` só tem campos `readonly` e nenhum método de transição. Um teste verifica isso olhando o protótipo.

A regra de negócio central está numa função pura de domínio (`applyWager`), que recebe a transação, a wallet já travada e o contexto da referência, e devolve `processed | rejected | pending_reference`. HTTP, SQS e o worker de pendências passam todos pelo mesmo `applyAndRecord`. Os únicos outros caminhos de escrita são estreitos e não movem saldo por regra de negócio: a abertura de wallet (`OPENING`) e as transições terminais do worker (`REFERENCE_NOT_FOUND`, `FAILED`). Todos gravam pela mesma porta transacional, `WageringUnitOfWork`.

## 3. Schema: invariantes aplicadas no banco

Migration: [`Migration20261031000001_initial.ts`](src/infrastructure/persistence/migrations/Migration20261031000001_initial.ts) (versionada e reversível, com `up` e `down`; os testes rodam `down` até zero e `up` de novo).

| Garantia | Como |
|---|---|
| uma wallet por `playerId + currency` | `unique (player_id, currency)` |
| saldo nunca negativo | `check (balance >= 0)` em `wallets` e em `balance_before/after` do ledger |
| idempotência persistente, isolada por provedor | `unique (provider_id, idempotency_key)` e `unique (provider_id, external_transaction_id)` |
| no máximo um lançamento por transação/wallet | `unique (wallet_id, transaction_id)` |
| ledger encadeado e sem buracos por wallet | `sequence` = versão da wallet após o lançamento; `unique (wallet_id, sequence)`; *constraint trigger* no commit: cada lançamento continua o anterior (`sequence + 1` e `balance_before` = `balance_after` anterior), e o primeiro parte de zero |
| aritmética do lançamento | `check (balance_after = balance_before ± amount)` |
| ledger imutável | triggers `BEFORE UPDATE/DELETE` (por linha) e `BEFORE TRUNCATE` que levantam erro |
| transação terminal imutável | trigger: `UPDATE` em linha `PROCESSED/REJECTED/FAILED` falha; campos de negócio nunca mudam |
| nada é apagado | triggers `BEFORE DELETE` em `wallets` e `wager_transactions` |
| reversão única por referência | índice único parcial em `reference_transaction_id` onde `kind in (REFUND, ROLLBACK) and status = PROCESSED` |
| `REFUND`/`ROLLBACK` exigem referência | `check` |
| `failure_code` existe se e somente se o status for `REJECTED/FAILED` | `check` |
| **saldo ⇄ ledger** | *constraint trigger* `DEFERRABLE INITIALLY DEFERRED` em `wallets`: no **commit**, o saldo atual precisa ser igual ao `balance_after` do lançamento cuja `sequence = version` (saldo sem ledger falha). No sentido inverso, um trigger no ledger exige que a versão da wallet seja a do último lançamento (ledger sem saldo também falha) |
| inbox | `primary key (consumer_name, message_id)` |
| dinheiro exato | `numeric(20,2)`. Nunca `float` |

As FKs são `DEFERRABLE INITIALLY DEFERRED`. Assim, a ordem dos INSERTs gerada pelo Unit of Work do MikroORM não importa: tudo é verificado no commit.

## 3.1 Ledger de partidas dobradas

Além do ledger por wallet (o sub-razão, com `balance_before`/`balance_after` e sequência sem buracos), existe um **razão geral de partidas dobradas**. Cada movimento de saldo gera, na **mesma transação SQL**, um lançamento contábil com débito e crédito de mesmo valor.

**Plano de contas** (do ponto de vista da casa):

| Conta | Tipo | Significado |
|---|---|---|
| `wallet:<walletId>` | passivo | dinheiro que a casa deve ao jogador |
| `provider:<providerId>:<moeda>` | passivo | posição de liquidação com o provedor do jogo |
| `house:funding:<moeda>` | ativo | caixa recebido quando a wallet é aberta com saldo (`OPENING`) |

**Lançamentos:**

| Operação | Débito | Crédito |
|---|---|---|
| `OPENING` | `house:funding` | `wallet` |
| `BET`, `ROLLBACK` de crédito | `wallet` | `provider` |
| `WIN`, `REFUND`, `ROLLBACK` de `BET` | `provider` | `wallet` |
| `LOSS` / rejeitadas | — (sem movimento, sem lançamento) | |

Como a conta da wallet é um passivo, o saldo dela no razão geral (créditos menos débitos) é exatamente o saldo da wallet. A conta do provedor fica com o resultado líquido da casa contra aquele provedor.

**Garantias no schema** (migration `Migration20261101000001_double_entry`):
- todo lançamento fecha (débitos = créditos), tem pelo menos 2 partidas e uma única moeda, igual à das contas. Isso é verificado no commit por um *constraint trigger* diferido;
- todo movimento do sub-razão da wallet precisa ter, no commit, a partida correspondente na conta `wallet:<id>` (mesma transação, direção e valor). Também é diferido;
- um lançamento por transação; contas, lançamentos e partidas são *append-only* (triggers bloqueiam `UPDATE`, `DELETE` e `TRUNCATE`).

No código, a porta `recordMovement(entry, journal)` grava os dois juntos: não existe caminho para registrar um movimento de wallet sem o lançamento contábil.

**Contas quentes:** os saldos das contas contábeis **não** são materializados, só as partidas. Assim, a conta de um provedor, que recebe todas as apostas daquele provedor, não vira uma linha disputada por lock; a única serialização continua sendo a da wallet. Contas são criadas sob demanda com `INSERT … ON CONFLICT DO NOTHING`, que não bloqueia quando a conta já existe.

**Backfill:** a migration gera os lançamentos dos movimentos já existentes. No banco de desenvolvimento, com os dados dos testes de carga, foram 93.761 lançamentos e 187.522 partidas em ~3 s. O balancete fechou em 0,00 e nenhuma wallet divergiu. A migration é reversível.

**Consultas:** a reconciliação (`POST /wallets/:id/reconciliation`) confere também o saldo da wallet no razão geral (`generalLedgerBalance`). `GET /ledger/trial-balance?currency=BRL` devolve o **balancete**: débitos, créditos e saldo natural por grupo de contas (as wallets somadas numa linha), com `balanced: true` quando o total de débitos é igual ao de créditos.

## 4. Money

- Internamente é um **`bigint` em centavos** (escala fixa 2). A aritmética é exata e não há dependência de biblioteca decimal. `numeric(20,2)` ↔ string decimal ↔ `bigint`, sem passar por `number`.
- `Money.from` aceita apenas `^(0|[1-9]\d*)(\.\d{1,2})?$`. Rejeita `NaN`, `Infinity`, notação científica, sinais, string vazia, espaços, zeros à esquerda, mais de 2 casas, valores não-string e mais de 18 dígitos inteiros (limite do `numeric(20,2)`).
- Serializa sempre com 2 casas (`"25.00"`). Operações entre moedas diferentes lançam `CurrencyMismatchError`.
- O modelo é multi-moeda (wallet por `playerId + currency`). Os testes cobrem conflito de moeda no domínio e na API.

## 5. ORM: MikroORM

**Por que MikroORM:** Unit of Work explícito, `em.transactional()` com fork isolado por requisição, `LockMode.PESSIMISTIC_WRITE` e acesso direto a SQL quando necessário (`em.execute`) dentro da mesma transação.

**Mapeamento:** o domínio não conhece o ORM. As tabelas são mapeadas com `EntitySchema` para *records* simples ([records.ts](src/infrastructure/persistence/records.ts)) e convertidas por [mappers.ts](src/infrastructure/persistence/mappers.ts). `Money` vira duas colunas (`amount numeric(20,2)` lido como string, mais `currency char(3)`) e é reidratado com `Money.from`.

**Estratégia transacional:** cada submissão roda em um `em.fork().transactional(...)`. Dentro dela:
1. `set local lock_timeout` (padrão 5 s);
2. `SELECT … FOR UPDATE` na wallet;
3. inbox (se veio do SQS) via `INSERT … ON CONFLICT DO NOTHING`;
4. checagem de idempotência sob o lock;
5. `applyWager`;
6. persist de transação, ledger, wallet (o change set do UoW) e outbox;
7. flush + commit. O trigger diferido valida saldo ⇄ ledger.

Usei SQL direto onde o UoW não ajuda: `ON CONFLICT DO NOTHING` (inbox e contas contábeis), o `now()` do banco no início da transação, `wakePendingDependents`, a busca de pendências vencidas, o claim/marcação/retry da outbox (`SKIP LOCKED`, lease) e os agregados de reconciliação e balancete.

## 6. Concorrência

**Unidade de concorrência: a wallet.** Estratégia: **lock pessimista por linha** (`SELECT … FOR UPDATE` na wallet), mais as constraints como última linha de defesa.

Por que pessimista e não otimista:
- o caso típico de iGaming é a **wallet quente** (um jogador com muitas rodadas por segundo). Com locking otimista, sob contenção, quase toda tentativa falha e precisa refazer leitura, cálculo e escrita, gastando CPU e conexões. Com o lock pessimista as escritas entram em fila e cada uma roda uma única vez;
- o lock é **por linha**. Wallets diferentes nunca se bloqueiam, e não existe lock global;
- todo caminho trava primeiro a wallet e só depois toca outras linhas, então não há ordem de lock que gere deadlock entre os fluxos;
- `lock_timeout` limita a espera. Se estourar, a operação vira um erro transitório (HTTP 503 ou retry no SQS) e incrementa `wagering_lock_conflicts_total`.

`version` continua existindo. Ela incrementa só quando o saldo muda e funciona como número de sequência do ledger. Isso dá uma checagem otimista implícita no schema: dois débitos concorrentes que por algum motivo escapassem do lock colidiriam em `unique (wallet_id, sequence)`.

**Idempotência sob concorrência** (50 requisições iguais em paralelo):
1. *fast path* sem lock: se a idempotency key já existe, a resposta é um replay;
2. caso contrário, todas as requisições entram na fila do lock da wallet; a primeira insere a transação, e as demais, já com o lock, encontram a key e devolvem replay;
3. se a mesma key chegar para **wallets diferentes** (e portanto com locks diferentes), o `unique (provider_id, idempotency_key)` faz a perdedora dar rollback; ela relê o que a vencedora gravou e responde com replay ou conflito.

FIFO do SQS (`MessageGroupId = walletId`) reduz contenção e preserva a ordem por wallet, mas é **só otimização**: os testes enviam mensagens duplicadas com `MessageDeduplicationId` diferentes e misturam HTTP e fila de propósito.

## 7. Códigos de falha

[`failure-codes.ts`](src/domain/failure-codes.ts). Nenhum é *retryable* com o mesmo payload: quando a falha é transitória, a transação não é persistida e a resposta é HTTP 503 (ou retry na fila).

| `failureCode` | Status | Quando |
|---|---|---|
| `INSUFFICIENT_FUNDS` | REJECTED | `BET` maior que o saldo |
| `REVERSAL_INSUFFICIENT_FUNDS` | REJECTED | `ROLLBACK` de um crédito (WIN/REFUND) deixaria o saldo negativo. Código **distinto** de uma aposta sem saldo, porque operacionalmente é outra situação: dinheiro já creditado e possivelmente sacado |
| `CURRENCY_MISMATCH` | REJECTED | moeda da operação diferente da moeda da wallet |
| `PLAYER_WALLET_MISMATCH` | REJECTED | a wallet não é do `playerId` informado |
| `REFERENCE_NOT_FOUND` | REJECTED | nenhuma transação `(providerId, referenceExternalTransactionId)` chegou dentro do limite de tentativas. A busca é sempre dentro do mesmo provider, como pede o enunciado, então uma referência a outro provider também termina aqui |
| `REFERENCE_MISMATCH` | REJECTED | a referência é de outro player, wallet, moeda ou rodada |
| `REFERENCE_KIND_NOT_ALLOWED` | REJECTED | ex.: `REFUND` de um `WIN`, `ROLLBACK` de um `LOSS` |
| `REFERENCE_NOT_PROCESSED` | REJECTED | a referência foi rejeitada ou falhou |
| `REFERENCE_ALREADY_REVERSED` | REJECTED | a referência já foi revertida, seja por um novo `REFUND`/`ROLLBACK` ou por um `WIN`/`LOSS` tentando liquidar uma aposta cancelada |
| `REVERSAL_AMOUNT_MISMATCH` | REJECTED | valor diferente do da referência (reversão parcial está fora de escopo) |
| `PROCESSING_FAILED` | FAILED | erro permanente (não transitório) ao reprocessar uma pendência. Fica para auditoria |

`WALLET_NOT_FOUND` (HTTP 422) **não** é um `failureCode` persistido: sem wallet, não há agregado que seja dono da linha (a FK exige a wallet). Ver §15.

## 8. Idempotência

- A fonte da verdade é o header `Idempotency-Key` (no SQS, `data.idempotencyKey`). O default recomendado é `providerId:externalTransactionId`.
- **A chave é isolada por provedor** (`unique (provider_id, idempotency_key)`, e toda busca usa os dois). Com uma chave global, um provedor poderia ocupar antes a chave previsível de outro (`providerB:win-123`) e fazer a transação legítima dele falhar como conflito. A autenticação planejada (§14) não evitaria isso, porque ela só vincula o `providerId` do corpo ao token. Esse risco foi apontado na revisão de segurança e corrigido.
- **`payloadHash`** = `sha256` em hex do **JSON canônico** dos campos de negócio: `providerId`, `externalTransactionId`, `playerId`, `walletId` (UUIDs em minúsculas), `roundId`, `gameId`, `kind`, `money` (`amount` normalizado para 2 casas) e `referenceExternalTransactionId` (omitido quando ausente). JSON canônico aqui significa: chaves ordenadas recursivamente, sem espaços e sem campos `undefined`. Header, `messageId`, `occurredAt` e outros metadados de transporte **não** entram no hash. Assim, `"25"` e `"25.00"` geram o mesmo hash, e a ordem das chaves não importa.
- Requisição idêntica: a resposta é a original, com `idempotentReplay: true` e **o saldo observado naquele momento** (coluna `result_balance`).
- Mesma key com payload diferente: `409 IDEMPOTENCY_CONFLICT`. A mesma `(providerId, externalTransactionId)` sob outra key também dá 409.
- No SQS a deduplicação acontece em dois níveis: a **inbox** `(consumerName, messageId)`, gravada na mesma transação SQL, e a idempotency key (cobre o mesmo pedido reenviado com outro `messageId`).
- Nenhum cache em memória participa da garantia.

## 9. Referências fora de ordem

- Se a referência ainda não existe (ou está ela mesma `PENDING`/`PENDING_REFERENCE`), a transação é persistida como `PENDING_REFERENCE` e o evento `WagerTransactionPendingReference` é publicado. A resposta HTTP é `202`.
- O worker `ResolvePendingReferences` busca as pendências vencidas (`next_reference_check_at <= now()`), relê cada uma **sob o lock da wallet** (vários workers em várias instâncias não aplicam a mesma pendência duas vezes) e roda o mesmo `applyAndRecord`.
- **Relógio:** a decisão de "venceu?" usa o `now()` do **PostgreSQL**, o único relógio comum a todas as instâncias. Instâncias com relógios levemente diferentes (o teste pegou uma diferença de 185 ms entre o container do banco e o host) não deixam pendências vencidas sem processar. O backoff em si é calculado no app, e um desvio de relógio só o desloca alguns milissegundos. A outbox segue a mesma regra (`next_attempt_at <= now()`, `locked_until < now()`).
- **Backoff exponencial:** 1 s, 2 s, 4 s… com teto de 5 min por espera. **Limite: 10 verificações sem encontrar a referência** (configurável). A 10ª acontece ~811 s (cerca de **13,5 min**) depois da chegada. Se a referência **existe mas ainda está ela mesma pendente**, a verificação não consome o orçamento: a dependente espera a referência se resolver ou ser rejeitada no prazo dela. Isso cobre atrasos normais de entrega entre provedores sem deixar a pendência viva indefinidamente. Esgotado o limite, a transação vira `REJECTED` com `REFERENCE_NOT_FOUND`, e o evento `WagerTransactionRejected` é publicado.
- **Atalho:** quando uma transação é processada (ou rejeitada), as pendências que dependem dela são "acordadas" na mesma transação SQL (`next_reference_check_at = now()`). Na prática, a reversão é aplicada milissegundos depois de a referência chegar.
- Um erro não transitório ao reprocessar faz a pendência ir para `FAILED` / `PROCESSING_FAILED`, para não ficar em loop infinito.

## 10. Consumidor SQS

- Usa o **mesmo caso de uso** e a mesma validação da entrada HTTP.
- O `ack` (`DeleteMessage`) só acontece depois do commit: `execute()` só retorna depois do commit.
- Classificação de erros:

| Tipo | Exemplos | Ação |
|---|---|---|
| negócio | `REJECTED`, `WALLET_NOT_FOUND` | ack (terminal) |
| permanente | JSON inválido, payload inválido, `IDEMPOTENCY_CONFLICT`, violação de invariante de domínio | envia para a DLQ com `failureReason` e faz ack |
| transitório | Postgres fora do ar, `lock_timeout`, deadlock, conexão | devolve à fila com `ChangeMessageVisibility` em backoff (2, 4, 8… até 60 s) |
| limite atingido | `ApproximateReceiveCount >= SQS_MAX_RECEIVE_COUNT` (5) | envia para a DLQ (`MAX_RECEIVE_COUNT_EXCEEDED`) |

  O consumidor move a mensagem para a DLQ por conta própria, para não depender de como cada emulador implementa a redrive policy. A `RedrivePolicy` da fila também está configurada (com `maxReceiveCount + 1`) como rede de segurança.
- **Ordem e visibilidade no lote:** as mensagens de um mesmo grupo FIFO são processadas em sequência. Se uma volta para retry, as seguintes do mesmo grupo naquele lote são adiadas junto (não passam na frente). Se o lote demora (wallet quente), a visibilidade das restantes é estendida antes de expirar.
- **Robustez:** uma falha do próprio SQS (por exemplo, ao mover para a DLQ) nunca derruba o loop do consumidor: a mensagem fica para redelivery. Uma falha no `ack` **depois** do commit também não é tratada como erro de processamento; a redelivery é absorvida pela inbox, em vez de ir para a DLQ por engano.
- **SIGTERM:** para de buscar mensagens (aborta o long poll), termina a mensagem em andamento de cada loop, devolve a visibilidade (`VisibilityTimeout=0`) das que ainda não começaram, e só então fecha HTTP e banco. O readiness passa a responder 503 (`draining`) logo no início. Um watchdog de 25 s garante a saída (código 1 se o shutdown travar ou falhar).
- **Crash depois do commit e antes do ack:** a mensagem volta após o visibility timeout. A inbox detecta o `messageId` e o consumidor responde com replay, sem efeito novo. Há um teste com `SIGKILL` real (`FAULT_CRASH_AFTER_COMMIT`).

## 11. Transactional Outbox

- Os eventos são gravados em `outbox_messages` **na mesma transação** que o saldo, o ledger e a inbox. Nada é publicado antes do commit.
- **Publisher concorrente:** `UPDATE … SET locked_until = now() + lease WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED LIMIT n) RETURNING *`. A linha fica reservada por um *lease* e a publicação no SQS acontece **fora** de qualquer transação SQL, sem segurar lock durante I/O de rede.
- **Crash do publisher:** se a instância morre depois de reservar e antes de marcar `published_at`, o lease expira (30 s) e outra instância publica. A entrega é **at-least-once**: o `eventId` é estável, é usado como `MessageDeduplicationId` no SQS FIFO e permite deduplicação no consumidor.
- **Vazão:** cada execução reserva até 200 eventos e os distribui em 8 *faixas* pelo hash do `aggregateId`. As faixas enviam em paralelo com `SendMessageBatch` (10 por chamada); dentro de uma faixa o envio é sequencial, então a ordem por wallet é preservada. Se um lote falha, o resto da faixa é adiado, sem afetar as outras faixas. Num lote com falha *parcial* (`Failed` em algumas entradas), as entradas seguintes do mesmo lote já foram aceitas pelo broker; é o único caso em que um evento pode chegar antes de um anterior, e o `walletVersion` resolve. A marcação como publicado é um único `UPDATE … WHERE id = any(…)`.
- **Falha ao publicar:** `attempts++` e `next_attempt_at` com backoff exponencial e jitter (1 s até 5 min). O último erro fica gravado.
- **Ordem depois de falhas:** o claim nunca reserva um evento se houver um evento **mais antigo da mesma wallet** aguardando retry ou em publicação por outra instância (`NOT EXISTS` com índice por agregado). Um evento novo não passa na frente de um atrasado.
- **Ordem entre instâncias:** é *best effort* na janela em que dois claims concorrentes ainda não confirmaram seus leases. Para os consumidores, a referência de ordem é o `walletVersion` de `WalletBalanceChanged` (monotônico por wallet), não a ordem de chegada.
- Eventos: `WagerTransactionProcessed` (inclusive para `LOSS` e `OPENING`), `WagerTransactionRejected`, `WalletBalanceChanged` (somente quando o saldo muda) e `WagerTransactionPendingReference`. São publicados em `wagering-events.fifo` com `MessageGroupId = walletId`, preservando a ordem por wallet.
- O envelope é uma classe abstrata `IntegrationEvent<T>`, com uma subclasse por evento; `eventType` e `version` ficam no tipo. `data` só carrega `MoneyProps` (strings), nunca instâncias de `Money`.

## 12. Observabilidade

- **Logs JSON** (pino) com `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId`, `instanceId`. Nenhum payload financeiro é logado. Como rede de segurança, o pino tem `redact` para `money`, `balance`, `payload`, `body`, `idempotencyKey` e `authorization`.
- `x-correlation-id` é aceito na requisição HTTP e devolvido na resposta. No SQS vem do atributo `correlationId` ou, na falta dele, do `messageId`.
- **Métricas** (`GET /metrics`, Prometheus):

| Métrica | |
|---|---|
| `wagering_transactions_total{status,kind,source}` | transações por status |
| `wagering_duplicates_total{kind}` | replays idempotentes e redeliveries da inbox |
| `wagering_idempotency_conflicts_total` | mesma key com payload diferente |
| `wagering_retries_total{component}` | retries do consumidor e da outbox |
| `wagering_dlq_messages_total{reason}` | mensagens enviadas para a DLQ |
| `wagering_lock_conflicts_total` | `lock_timeout` e deadlocks |
| `wagering_outbox_lag_seconds` | idade da mensagem mais antiga não publicada (lida do banco a cada scrape, portanto correta com várias instâncias) |
| `wagering_processing_duration_seconds{source,outcome}` | latência do processamento |
| `wagering_reconciliation_divergences_total` | divergências entre saldo e ledger |
| `wagering_consumer_messages_total{outcome}`, `wagering_outbox_published_total`, `wagering_outbox_publish_failures_total` | |

- **Health:** `/health/live` só indica que o processo está vivo; `/health/ready` checa Postgres e SQS, cada um com timeout de 2 s, e responde 503 se algum falhar ou se a instância estiver em shutdown.
- **Métricas depois do commit:** `wagering_transactions_total` e o log "wager transaction recorded" só são emitidos depois do commit, então um commit rejeitado por um trigger diferido não é contado.
- **DLQ:** `wagering_dlq_messages_total` conta as mensagens que o consumidor moveu para a DLQ (todos os casos de limite e de erro permanente). As que a `RedrivePolicy` mover como rede de segurança aparecem na profundidade da DLQ, que deve ser monitorada no broker (CloudWatch `ApproximateNumberOfMessagesVisible`).
- **Reconciliação:** lê saldo e agregados do ledger num mesmo snapshot `REPEATABLE READ` (escritas concorrentes não geram falsa divergência). Verifica a soma do sub-razão, o `balance_after` do último lançamento e o saldo da conta da wallet no razão geral (§3.1). **Nunca corrige nada**: loga em `error`, incrementa a métrica e devolve `consistent: false`.

## 12.1 Retenção

Um worker (de hora em hora, `RETENTION_INTERVAL_MS`) apaga em lotes de 5.000:
- eventos da outbox **já publicados** há mais de 7 dias (`OUTBOX_RETENTION_DAYS`). Eventos ainda não publicados **nunca** são apagados;
- registros da inbox com mais de **15 dias** (`INBOX_RETENTION_DAYS`). O prazo é maior que a retenção máxima da SQS (14 dias): enquanto uma mensagem ainda puder ser reentregue, o registro dela continua na inbox. Depois disso, a idempotency key ainda protege contra efeito duplicado.

Carteiras, transações, ledger e razão geral nunca são apagados (são *append-only*, protegidos por triggers).

## 13. Teste de carga

`bun run test:load` gera tráfego HTTP contra as 3 instâncias do compose. Por padrão: 64 clientes concorrentes, 200 wallets, **20% do tráfego numa única wallet quente**, 70% BET e 30% WIN. Reporta throughput, p50/p95/p99, distribuição de status, taxa de erro, conflitos de lock e lag da outbox, e no fim reconcilia a wallet quente.

### Ambiente

Apple M5 (10 núcleos, 16 GB), macOS 26.6, OrbStack (Docker 29.8). Tudo na mesma máquina: 3 instâncias da aplicação, PostgreSQL 17.11, MiniStack e o gerador de carga. Configuração padrão do compose (pool de 20 conexões por instância, `lock_timeout` de 5 s). Execução de 30 s, depois de um aquecimento implícito (criação das 200 wallets).

### Resultados (31/10/2026)

| Métrica | Valor |
|---|---|
| Requisições | 28.796 em 30 s |
| Throughput | **~950–1.100 req/s** (3 execuções: 1.026, 1.107, 953) |
| Latência p50 / p95 / p99 / máx. | 13,9 / 386,9 / 613,9 / 1.533 ms |
| Status | 100% `201` |
| Taxa de erro | **0,00%** |
| Conflitos de lock (`lock_timeout`) | **0** |
| Reconciliação da wallet quente | `consistent: true`, 6.355 lançamentos, diferença `0.00` |
| Lag da outbox ao fim da carga | ~29 s (ver análise) |

### Análise

- **A distribuição de latência é bimodal, e é isso que se espera.** O p50 de cerca de 14 ms é o caminho normal: lock da wallet, 4 a 5 escritas e commit. O p95/p99 vem da wallet quente: 20% do tráfego disputa uma única linha, e o lock pessimista coloca essas requisições em fila. Elas esperam, mas **nenhuma falha**: zero `lock_timeout`, zero 503, e o saldo bate com o ledger no fim. Com locking otimista, esse mesmo cenário viraria uma tempestade de retries. A contrapartida está em §16: a vazão de uma wallet isolada é limitada (1 transação por vez).
- **Throughput:** a vazão é limitada pela máquina compartilhada (gerador de carga, 3 apps, Postgres e emulador disputando os mesmos 10 núcleos) e pela wallet quente. Não há meta de RPS; o número serve de referência.
- **Lag da outbox: o gargalo foi o emulador, não o serviço.** Durante a carga são gerados cerca de 2 eventos por transação (~2.000 eventos/s). Investigando o lag:
  1. a query de claim (`SKIP LOCKED`) leva **0,4 ms** (`EXPLAIN ANALYZE`);
  2. durante o escoamento, as 3 instâncias ficam em ~1% de CPU e o Postgres em ~0,6%, enquanto o **container do MiniStack fica em 99%** (um núcleo saturado);
  3. num benchmark isolado, o MiniStack aceita ~6 msg/s com `SendMessage` sequencial, ~56 msg/s com `SendMessageBatch` sequencial e ~790 msg/s com 16 lotes em paralelo. É exatamente por isso que o publisher usa lotes e faixas paralelas (§11). Essa mudança, feita a partir dessa medição, é o que deixou o publisher no limite do emulador.

  Na AWS real esse teto não existe nessa ordem de grandeza. O que importa para a correção: **nenhum evento se perde** (todos ficam na outbox até o commit da publicação), o lag é observável (`wagering_outbox_lag_seconds`) e o backlog drena sozinho depois do pico. A outbox desacopla a vazão de escrita da vazão do broker, que é justamente a função dela.

## 14. Autenticação

**Decisão: não implementada**, porque não vale pontos e competiria com correção financeira, concorrência e idempotência. O ponto de extensão está no código: `ProviderAuthGuard` ([controllers.ts](src/http/controllers.ts)) é um guard que hoje não faz nada e está aplicado nos controllers de negócio. `/health/*` e `/metrics` ficam abertos.

**Desenho que eu adotaria:**
- Keycloak ou Zitadel no compose; cada provedor é um *client* OIDC com **client credentials**;
- o guard valida o JWT (assinatura via JWKS em cache, `iss`, `aud`, `exp`) e extrai `client_id`;
- **vínculo de identidade:** o `providerId` do body tem que ser igual ao provedor do token. Caso contrário, 403. Isso impede que um provedor submeta ou reverta transações de outro;
- mensagens SQS continuam tratadas como canal interno confiável (IAM na AWS real), mas o `providerId` delas passa pelas mesmas validações de domínio (a referência precisa ser do mesmo provider etc.).

## 15. Interpretações adotadas

- **"Não pode ser revertida duas vezes pelo mesmo tipo de operação":** apliquei uma regra mais forte, de **uma reversão no total por referência**. Uma `BET` com `REFUND` não aceita também um `ROLLBACK`, porque isso creditaria a aposta duas vezes. Ainda é possível encadear: `BET → REFUND → ROLLBACK(do REFUND)`, já que o `ROLLBACK` reverte outra transação. A regra é aplicada sob o lock e garantida por índice único parcial.
- **`WIN`/`LOSS` com referência:** opcional. Se informada, precisa ser uma `BET` `PROCESSED` e **não revertida** da mesma rodada, player, wallet e moeda; se ainda não chegou, a transação fica `PENDING_REFERENCE`. Vários `WIN` para a mesma `BET` são aceitos (jogos com várias linhas de prêmio), e um `ROLLBACK` de `BET` depois de um `WIN` também (o provedor é a fonte da verdade sobre a rodada).
- **UUIDs** são normalizados para minúsculas na entrada (maiúsculas são válidas e representam o mesmo id). Uma transação que referencia a si mesma é payload inválido (400).
- **`LOSS`** aceita valor `0.00` (só registra o resultado). As demais operações exigem valor maior que zero.
- **`ROLLBACK`** é aceito para `BET`, `WIN` e `REFUND`, com direção inversa à da referência. `ROLLBACK` de `LOSS` ou de outro `ROLLBACK` é `REFERENCE_KIND_NOT_ALLOWED`.
- **Wallet inexistente** na submissão: `422 WALLET_NOT_FOUND`, sem persistir (sem wallet, não existe agregado dono do registro). No SQS conta como erro de negócio (ack).
- **Versão da wallet:** `version = 1` ao criar. Com saldo inicial maior que zero, o lançamento `OPENING` é a sequência 1, então a resposta da criação mostra `version: 1`, como no exemplo do enunciado. Wallets criadas com saldo zero começam em `version 1` sem lançamento; o primeiro movimento é a sequência 2.
- **Valores de entrada** aceitam de 0 a 2 casas decimais (`"25"`, `"25.5"`, `"25.00"`), sempre normalizados para 2 casas na saída e no hash. **Arredondamento:** mais de 2 casas é **rejeitado**, nunca arredondado. Num contrato financeiro, arredondar em silêncio um valor enviado pelo provedor criaria divergência entre o nosso ledger e o dele. Como o `Money` é exato e só soma e subtrai, nenhuma operação interna produz frações que exijam arredondamento.
- **Conflito de idempotência no SQS** vai para a DLQ, não recebe ack silencioso: é um erro do produtor que precisa de investigação humana.

## 16. Limitações e próximos passos

- A wallet quente é serializada pelo lock (correto e previsível, mas o throughput fica limitado a 1 transação por vez naquela wallet). Se isso virar gargalo: *sub-wallets* ou *batching* por wallet.
- O publisher de outbox usa polling (100–500 ms). `LISTEN/NOTIFY` reduziria a latência.
- Retenção por `DELETE` em lotes é suficiente para o volume do desafio. Em volume de produção, particionar `outbox_messages`/`inbox_messages` por data e descartar partições inteiras seria mais barato.
- **Pool:** obter uma conexão tem timeout de 5 s (`DB_ACQUIRE_TIMEOUT_MS`). Com o pool esgotado (muitas requisições esperando a mesma wallet quente), as demais falham rápido com 503 em vez de esperar 60 s.
- **Custos aceitos no caminho quente:** cada movimento faz um `INSERT … ON CONFLICT DO NOTHING` das contas contábeis, e a verificação de balanço do lançamento roda uma vez por partida. São idas extras pequenas, trocadas por garantias no banco. O balancete agrega todas as partidas a cada chamada; em produção, viraria uma visão materializada ou rodaria numa réplica de leitura.
- O `lock_timeout` é global, por configuração. Em produção valeria ter também um `statement_timeout` e um limite de concorrência por wallet no consumidor.
- Sem OpenTelemetry. O `correlationId` está pronto para virar `traceId`.
