# Data Access Policy

## Objetivo
Definir o acesso por camada da plataforma de dados de mobilidade.

## Regras por camada

### Raw
- Acesso: Data Engineering
- Finalidade: armazenamento dos dados ingeridos diretamente da fonte

### Bronze
- Acesso: Data Engineering
- Finalidade: estruturação inicial e persistência dos dados brutos

### Silver
- Acesso: Data Engineering, Analytics Engineering
- Finalidade: limpeza, padronização e enriquecimento

### Gold
- Acesso: Data Engineering, Analytics Engineering, Data Analysts, BI
- Finalidade: consumo analítico, dashboards e indicadores

## Princípios
- Menor privilégio possível
- Separação entre camadas operacionais e analíticas
- Acesso somente conforme necessidade de negócio
