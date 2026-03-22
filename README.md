🚍 sp-mobility-data-platform

Plataforma de engenharia de dados para mobilidade urbana em tempo real, construída com arquitetura Lakehouse + Medallion no Databricks (Azure).

O projeto simula ingestão e processamento de dados de transporte público (GTFS + veículos em tempo real), transformando-os em datasets analíticos para tomada de decisão.

🧠 Arquitetura
Cloud: Azure
Storage: ADLS Gen2
Processamento: Databricks (Spark)
Orquestração: Databricks Jobs
Formato: Delta Lake
Arquitetura: Medallion (Bronze, Silver, Gold)
🔄 Pipeline de Dados
Ingestion → Bronze → Silver → Gold → Consumption
📥 Ingestion
Coleta de dados GTFS (static)
Ingestão de dados em tempo real (SPTrans - veículos)
Escrita em camada Bronze (raw)
🥉 Bronze (Raw Layer)
Dados brutos, sem transformação
Versionamento por data (ingestion_date)
Histórico completo
🥈 Silver (Trusted Layer)
Limpeza e padronização
Tratamento de nulos e inconsistências
Enriquecimento e joins
Estrutura pronta para análise
🥇 Gold (Business Layer)
Tabelas analíticas
KPIs e métricas de mobilidade
Otimizado para consumo

Exemplos:

mobility_kpis
route_performance
city_heatmap
mobility_intelligence
⚙️ Orquestração

Pipeline organizado em Databricks Jobs com dependências entre tarefas:

Setup
OAuth ADLS
Criação do Lakehouse
Governance
Criação de tabelas Delta
Registro no catálogo
Ingestion
GTFS Static
Vehicle Positions (streaming)
Transformações
Bronze → Silver
Silver → Gold
🧪 Boas Práticas Aplicadas
✔️ Arquitetura Medallion
✔️ Versionamento por data
✔️ Processamento incremental
✔️ Uso de Delta Lake
✔️ Separação por camadas
✔️ Orquestração com dependência de tasks
✔️ Governança de dados
✔️ Estrutura escalável
📊 Casos de Uso
Monitoramento de transporte público
Análise de performance de rotas
Heatmap de mobilidade urbana
KPIs de operação em tempo real
🚀 Próximos Passos
🔄 Implementar CDC / ingest incremental avançado
📡 Evoluir streaming com Structured Streaming
📊 Criar dashboards (Power BI / Databricks SQL)
🧪 Adicionar testes de qualidade (Great Expectations / dbt)
🧬 Implementar data lineage
👨‍💻 Autor

Leandro Santos
Data Engineer
