# 🚍 sp-mobility-data-platform

Plataforma de engenharia de dados para mobilidade urbana em tempo real,
construída com arquitetura **Lakehouse + Medallion** no **Databricks
(Azure)**.

------------------------------------------------------------------------

## 🧠 Arquitetura

-   Cloud: Azure\
-   Storage: ADLS Gen2\
-   Processamento: Databricks (Spark)\
-   Formato: Delta Lake\
-   Arquitetura: Medallion (Bronze, Silver, Gold)

------------------------------------------------------------------------

## 🔄 Pipeline de Dados

    Ingestion → Bronze → Silver → Gold → Consumption

------------------------------------------------------------------------

## ✅ O que já foi implementado

-   Estrutura de projeto organizada (setup, ingestion, bronze, silver,
    gold)
-   Conexão com ADLS Gen2 via OAuth
-   Ingestão de dados GTFS (batch)
-   Ingestão de dados de veículos (streaming simulado)
-   Criação de tabelas Delta
-   Pipeline orquestrado via Databricks Jobs
-   Camadas Bronze, Silver e Gold implementadas
-   Versionamento por data de ingestão
-   Estrutura inicial de observabilidade
-   Organização de notebooks por responsabilidade

------------------------------------------------------------------------

## 🧪 Boas práticas aplicadas

-   Arquitetura Medallion\
-   Separação por camadas\
-   Uso de Delta Lake\
-   Versionamento por ingestion_date\
-   Orquestração com dependência entre tasks\
-   Estrutura preparada para escala

------------------------------------------------------------------------

## 🚀 Próximas etapas (Roadmap)

### 🔁 Versionamento

-   Time Travel com Delta Lake\
-   Estratégia SCD

### 👁️ Observabilidade

-   Logs estruturados\
-   Métricas de pipeline\
-   Alertas automáticos

### ✅ Qualidade de Dados

-   Validação de schema\
-   Testes automatizados\
-   Great Expectations / dbt

### 🔄 CI/CD

-   Pipeline de deploy automatizado\
-   Validação de código\
-   Promoção entre ambientes

### 🔐 Governança

-   Catálogo de dados\
-   Controle de acesso\
-   Data lineage

### ⚙️ Performance

-   OPTIMIZE e VACUUM\
-   Particionamento\
-   Processamento incremental

------------------------------------------------------------------------

## 🎯 Objetivo

Evoluir o projeto para um padrão de engenharia de dados de nível
produção, com foco em escalabilidade, governança e confiabilidade.
