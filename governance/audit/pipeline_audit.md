# Pipeline Audit

## Objetivo
Garantir rastreabilidade das execuções do pipeline de dados.

## Campos recomendados para auditoria

pipeline_name
task_name
start_time
end_time
status
records_read
records_written
error_message

## Uso

Cada execução do pipeline deve registrar eventos de auditoria
para permitir monitoramento e troubleshooting.

## Exemplo

pipeline_name: sp_mobility_pipeline
task_name: silver_vehicle_positions
status: success
records_written: 921
