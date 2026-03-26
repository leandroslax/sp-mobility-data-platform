# Data Quality Rules

## Objetivo
Definir regras mínimas de qualidade para datasets da plataforma.

## Regras Bronze
- arquivos devem existir no path esperado
- schema básico deve ser carregado corretamente
- colunas obrigatórias não podem estar ausentes

## Regras Silver
- event_date não pode ser nulo
- event_hour deve estar entre 0 e 23
- latitude e longitude devem ser válidas
- registros duplicados devem ser tratados
- timestamps devem estar em formato válido

## Regras Gold
- tabelas não podem estar vazias
- indicadores percentuais devem estar entre 0 e 100
- agregações devem respeitar granularidade esperada
- datasets analíticos devem possuir data de processamento

## Observações
Essas regras devem ser implementadas em notebooks de validação e auditoria.
