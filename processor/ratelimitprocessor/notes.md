# Rate-limit processor — priority-aware refactor

## Problema
O `ratelimitprocessor` original era all-or-nothing: quando a cota estourava,
o batch inteiro era descartado (ou tudo passava, em modo monitoring). Dois
problemas críticos de produção:

1. **Perda de sinal importante**: spans de erro e logs com severity ERROR+
   são justamente o que importa durante incidentes, e eram descartados junto
   com telemetria de baixo valor.
2. **Noisy-neighbor**: um único serviço podia consumir 100% do budget,
   silenciando os outros. A única defesa era configurar `specific_limits`
   para todos os serviços — inviável em ambiente multi-tenant.

## Solução

### 1. Priority-aware dropping
- Novo flag `preserve_errors` (default `true`).
- Em traces: spans com `Status.Code == Error` sempre passam, sem consumir
  budget. O rationale é que durante um incidente o sinal de erro é o mais
  valioso e não faz sentido rate-limitar justamente ele.
- Em logs: `SeverityNumber >= ERROR` recebe o mesmo tratamento.
- Métricas **não** têm conceito de "crítico" — mantêm semântica all-or-nothing.
- Implementação faz *partial drop*: ao invés de derrubar o batch inteiro,
  usa `AllowUpToN` para consumir o que cabe e só descarta o excedente
  via `RemoveIf` (preservando os spans/logs críticos). Capabilities mudam
  para `MutatesData: true`.

### 2. Fair-share (global ceiling + max share ratio)
- Novos campos `global_requests_per_second` / `global_requests_per_minute`:
  teto total compartilhado entre todos os keys.
- Novo `max_share_ratio` (0..1]: fração máxima do global que uma única
  key pode ocupar. Ex.: global=1000 rps, ratio=0.2 → cada serviço é
  capado em 200 rps, mesmo se o `specific_limits` pedir mais.
- Arquitetura: dois níveis de token bucket. `AllowUpToN` sempre tenta
  tirar do global primeiro, depois do per-key, com `Refund` para evitar
  over-charging quando o per-key granta menos que o global pré-consumiu.
- Garantia: um serviço barulhento atinge no máximo seu teto, e mesmo
  se todos os keys pedirem no máximo, o total nunca excede o global.
  Comprovado por `TestFairShare_NoisyNeighborCannotStarveOthers`.

### 3. Partial-allow semantics (`AllowUpToN`)
API nova no `TokenBucket` e `RateLimiter` que consome **até** N tokens
e retorna quanto foi granteado. Fundamental para o partial drop — sem
isso seria só boolean allow/deny, que é exatamente o que a gente
queria evitar.

## Mudanças no código

| Arquivo | O quê |
|---|---|
| `config.go` | Novos campos (`GlobalRequests*`, `MaxShareRatio`, `PreserveErrors`), `GetLimit` faz capping via `capToShare`, nova validação |
| `ratelimiter.go` | `AllowUpToN`, `Refund`, `refillLocked` compartilhado, `RateLimiter.global` |
| `processor.go` | Split critical/normal em traces e logs, `dropNormalSpans`/`dropNormalLogs` via `RemoveIf`, `MutatesData: true`, atributo `priority` na telemetria |
| `telemetry.go` | Novo counter `otelcol_processor_ratelimit_preserved_items` |
| `priority_test.go` | Suite nova (8 testes) cobrindo preserve, fair-share, validação, integração |

## Testes

Todos passam com `-race`:
```
go test -race -count=1 ./...
ok  github.com/nochaosio/.../ratelimitprocessor   1.016s
```

Cobertura do novo comportamento:
- `TestTraces_PreserveErrors_DropsOnlyNormal`: 10 normal + 5 error, budget=3 → 5 err preservados + 3 normal allowed = 8 forwarded, 7 dropped. Verifica quais spans sobreviveram (checa status code).
- `TestTraces_PreserveErrors_Disabled`: com flag off, volta ao comportamento old-school (any span é descartável).
- `TestLogs_PreserveErrors_DropsOnlyNormal`: mesmo conceito para logs via SeverityNumber.
- `TestFairShare_NoisyNeighborCannotStarveOthers`: serviço pede 200, é capado em 20 (20% de 100), outros 4 serviços ainda conseguem 20 cada.
- `TestFairShare_SpecificLimitAlsoCapped`: mesmo quando o usuário declara `specific_limits: {greedy: 50}`, o fair-share sobrescreve para 10 se ratio=0.1.
- `TestFairShare_GlobalCeilingRespected`: per-key=100, global=50 → só 50 passam, próximo request retorna 0.
- `TestFairShare_Validation`: ratio > 1 e ratio sem global são rejeitados.
- `TestIntegration_PriorityAndFairShare`: cenário completo com 3 serviços. Confirma que mesmo com global exaurido, o 3º serviço consegue forwardar seu 1 error via bypass (1 span, não 0).

Testes antigos continuam passando — compatibilidade mantida quando as
novas flags ficam no default.

## Exemplo de config para produção

```yaml
processors:
  ratelimit:
    limit_type: service_name
    # Teto global: 10k spans/s somados entre todos os serviços
    global_requests_per_second: 10000
    # Nenhum serviço pode ocupar mais de 15% do global = 1500 rps
    max_share_ratio: 0.15
    # Error spans sempre passam — não queremos perder sinal de incidente
    preserve_errors: true
    drop_on_limit: true
    # Overrides específicos ainda respeitam o max_share_ratio
    specific_limits:
      payments-api:
        requests_per_second: 2000  # será capado em 1500
```

## Decisões / tradeoffs

- **Errors não consomem budget**: testei a alternativa (força-consumir com
  `ForceConsume` fazendo o bucket ir negativo), mas isso causa starvation
  do normal quando há muitos errors. Optei por deixar errors passarem
  livres — o rationale é que se um serviço está com flood de erros, o
  objetivo do rate limit (controlar custo) é menos importante que o
  objetivo do observability (entender o incidente). Se isso se tornar
  problema no futuro, dá pra adicionar um `error_burst_limit` separado.
- **MutatesData**: agora é `true` para traces e logs. Custo: o pipeline
  não pode mais compartilhar ptrace.Traces entre fanouts sem cópia. Foi
  necessário para o partial drop ser real; alternativa (construir um
  novo pdata) seria mais caro.
- **Métricas sem priority**: gauges/counters não têm conceito de "erro",
  então mantive semântica antiga (all-or-nothing). Se alguém quiser
  preservar métricas de latência p99, teria que ser por atributo/nome,
  o que é um feature separado.

## Métrica nova exposta

- `otelcol_processor_ratelimit_preserved_items{priority="critical",signal="traces|logs",key=...}`
  — items forwardados via priority bypass. Útil para alertar quando um
  serviço está exclusivamente em modo crítico (sinal de incidente ativo).

## O que NÃO foi feito (e por quê)

- Autenticação/autorização por key: fora do escopo.
- Burst allowance separado: o token bucket já tem burst implícito (maxTokens).
- Rate limit adaptativo baseado em backpressure: seria feature nova,
  não melhoria do existente.
