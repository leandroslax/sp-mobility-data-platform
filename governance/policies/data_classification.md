# Data Classification Policy

## Objetivo
Classificar os dados da plataforma de mobilidade segundo sensibilidade e uso.

## Classificações

### Public
Dados públicos ou abertos, sem informação sensível.
Exemplos:
- GTFS
- posições agregadas de mobilidade
- métricas urbanas públicas

### Internal
Dados internos de uso operacional do projeto.
Exemplos:
- logs técnicos
- métricas de execução de pipeline
- tabelas de auditoria

### Restricted
Dados que exigem acesso controlado.
Exemplos:
- segredos
- credenciais
- informações de conexão

## Observações
Credenciais nunca devem ser armazenadas em repositório.
Segredos devem permanecer no Azure Key Vault.
