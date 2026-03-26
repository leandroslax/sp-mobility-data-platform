# Architectural Decisions

Este diretório registra decisões importantes de desenho e operação do projeto.

## ADR-001: Arquitetura Medallion no Databricks

### Decisão

Adotar camadas Bronze, Silver e Gold em Delta Lake sobre ADLS Gen2.

### Motivo

- separar claramente ingestão, tratamento e consumo
- melhorar rastreabilidade
- permitir evolução por camada

## ADR-002: Orquestração por Databricks Jobs

### Decisão

Executar o pipeline como job multi-task com dependências explícitas.

### Motivo

- clareza operacional
- fácil troubleshooting
- execução reproduzível no workspace

## ADR-003: Uso de cluster existente em vez de job cluster efêmero

### Decisão

Configurar o job para usar o cluster `sp-mobility` em vez de um `job_cluster` efêmero.

### Motivo

- o cluster efêmero apresentou falhas de provisionamento
- o cluster existente foi validado nos testes manuais
- a mudança estabilizou a execução do job ponta a ponta

## ADR-004: Paths de notebooks no Workspace Users

### Decisão

Usar paths em `/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/...`

### Motivo

- foi o path efetivamente disponível no ambiente validado
- eliminou falhas de acesso em jobs e no quality runner

## ADR-005: Fallback local para GTFS

### Decisão

Usar um arquivo GTFS local versionado no workspace quando a origem remota retornar erro.

### Motivo

- a origem remota apresentou `403 Forbidden`
- o fallback permitiu manter o pipeline operacional e demonstrável

## Próximas decisões a formalizar

- estratégia de backend remoto do Terraform
- política de promoção entre ambientes
- desenho de observabilidade e alertas
- estratégia de testes de integração com Spark / Databricks
