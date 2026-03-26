# sp-mobility-data-platform

Plataforma de engenharia de dados para mobilidade urbana em Databricks, usando arquitetura Lakehouse com camadas Bronze, Silver e Gold sobre ADLS Gen2.

## Arquitetura

- Cloud: Azure
- Storage: ADLS Gen2
- Processamento: Databricks
- Formato: Delta Lake
- Arquitetura: Medallion

Fluxo principal:

```text
Setup -> Governance -> Ingestion -> Bronze -> Silver -> Gold -> Quality
```

## Status atual

O pipeline foi validado com sucesso no Databricks em março de 2026.

Validações concluídas:

- notebooks de setup executando com sucesso
- governança executando com sucesso
- ingestão GTFS executando com fallback local quando o download externo retorna 403
- ingestão SPTrans executando com sucesso
- camadas Bronze, Silver e Gold executando com sucesso
- `05_quality_runner` executando com sucesso
- job Databricks `847346803592537` executando com sucesso

## Estrutura do projeto

```text
notebooks/
  00_setup/
  05_governance/
  10_ingestion/
  20_bronze/
  30_silver/
  35_quality/
  40_gold/
  45_observability/
jobs/
workflows/
observability/
terraform/
```

## Pré-requisitos

Antes de rodar o pipeline, o ambiente Databricks precisa ter:

- acesso ao ADLS Gen2
- acesso ao workspace com os notebooks deste repositório
- secret scope `kv-sp-mobility`
- cluster existente `sp-mobility`

Cluster validado:

- nome: `sp-mobility`
- cluster id: `0323-121133-n0dnzyjm`

## Secrets necessários

No scope `kv-sp-mobility`, os seguintes secrets devem existir:

- `databricks-sp-client-id`
- `databricks-sp-secret`
- `databricks-sp-tenant-id`
- `sptrans-api-token`

## Configuração do job

Job validado:

- nome: `sp-mobility-pipeline`
- job id: `847346803592537`

Arquivos de definição:

- [jobs/sp_mobility_job.json](/Users/leandrosantos/Downloads/sp-mobility-data-platform/jobs/sp_mobility_job.json)
- [jobs/sp_mobility_job_update.json](/Users/leandrosantos/Downloads/sp-mobility-data-platform/jobs/sp_mobility_job_update.json)
- [workflows/jobs/sp_mobility_lakehouse_pipeline.yml](/Users/leandrosantos/Downloads/sp-mobility-data-platform/workflows/jobs/sp_mobility_lakehouse_pipeline.yml)

Os jobs estão configurados para usar o cluster existente `sp-mobility`, evitando falhas de provisionamento do `job_cluster` efêmero.

## Execução manual no Databricks

Ordem validada de execução manual:

1. `notebooks/00_setup/00_adls_gen2_oauth_connection`
2. `notebooks/00_setup/01_create_lakehouse_structure`
3. `notebooks/05_governance/00_governance_catalog_registration`
4. `notebooks/05_governance/01_create_delta_tables_bronze`
5. `notebooks/05_governance/02_register_silver_tables`
6. `notebooks/05_governance/03_register_gold_tables`
7. `notebooks/10_ingestion/02_ingest_gtfs_static_data`
8. `notebooks/20_bronze/03_bronze_gtfs`
9. `notebooks/30_silver/04_silver_gtfs`
10. `notebooks/10_ingestion/09_ingest_sptrans_vehicle_positions`
11. `notebooks/20_bronze/10_bronze_sptrans_vehicle_positions`
12. `notebooks/30_silver/11_silver_sptrans_vehicle_positions`
13. `notebooks/40_gold/13_gold_sptrans_mobility_intelligence`
14. `notebooks/40_gold/22_gold_route_performance`
15. `notebooks/40_gold/23_gold_city_activity`
16. `notebooks/40_gold/24_gold_city_heatmap`
17. `notebooks/40_gold/25_gold_mobility_kpis`
18. `notebooks/35_quality/05_quality_runner`

## Execução via CLI

Atualizar o job:

```bash
databricks jobs reset --json @jobs/sp_mobility_job_update.json
```

Disparar uma execução:

```bash
databricks jobs run-now 847346803592537
```

Consultar a configuração atual do job:

```bash
databricks jobs get 847346803592537
```

Consultar um run:

```bash
databricks jobs get-run <run_id>
```

## Observações operacionais

### GTFS

O notebook `02_ingest_gtfs_static_data` tenta baixar o GTFS remoto. Quando a fonte externa responde `403 Forbidden`, o pipeline usa fallback local:

- `/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/data/raw/gtfs/cittamobi_gtfs.zip`

Isso foi validado em execução real.

### Quality runner

O notebook `05_quality_runner` usa paths absolutos de Workspace sem extensão `.py`, porque esse foi o formato aceito pelo `dbutils.notebook.run()` no workspace validado.

### Workspace paths

Os notebooks operacionais do job estão configurados em:

- `/Workspace/Users/slaxdataengineer@outlook.com/sp-mobility-data-platform/...`

Se o repositório for montado em outro path de workspace, os notebooks de quality e os artefatos de job podem precisar de ajuste.

## Troubleshooting

### Erro de `%run` no Databricks

Os notebooks principais foram ajustados para evitar dependência frágil de `%run` relativo. Se um notebook antigo ainda falhar por parsing, padronize-o para o mesmo modelo usado nos notebooks já validados.

### Erro de secret inexistente

Confirme o scope e as chaves:

```bash
databricks secrets list-secrets kv-sp-mobility
```

### Erro de notebook não encontrado no job

Confirme que o job aponta para paths em `/Workspace/Users/...` e não em `/Workspace/Repos/...`.

### Job travado em `Waiting for cluster`

O pipeline foi estabilizado trocando o `job_cluster` efêmero pelo cluster existente `sp-mobility`.

## Próximos passos recomendados

- adicionar schedule no Databricks quando a operação sair do modo manual
- documentar política de acesso ao cluster e ao secret scope
- evoluir observabilidade e alertas
- adicionar validações automatizadas antes do deploy
