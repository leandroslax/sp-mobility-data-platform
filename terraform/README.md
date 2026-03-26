# Terraform

Esta pasta centraliza a infraestrutura como código da plataforma.

## Objetivo

Provisionar de forma reprodutível os recursos base do ambiente:

- Resource Group
- Storage Account e containers do Lakehouse
- Key Vault
- Databricks Workspace

## Estrutura

```text
terraform/
  environments/
    dev/
    prod/
  modules/
    resource_group/
    storage/
    keyvault/
    databricks/
```

## Ambientes

### `environments/dev`

Ambiente de desenvolvimento com composição dos módulos principais.

### `environments/prod`

Reservado para a estrutura de produção. Ainda precisa ser expandido para refletir o mesmo nível de maturidade do ambiente `dev`.

## Módulos

### `resource_group`

Cria o resource group base do ambiente.

### `storage`

Cria a storage account e os containers usados na arquitetura Medallion:

- `landing`
- `bronze`
- `silver`
- `gold`
- `checkpoint`

### `keyvault`

Cria o Key Vault usado para armazenamento de credenciais e segredos operacionais.

### `databricks`

Cria o workspace Databricks do ambiente.

## Boas práticas adotadas

- separação por ambiente
- reutilização por módulos
- versionamento da infraestrutura
- uso de `terraform fmt` e `terraform validate`
- exclusão de `tfstate` e `.terraform` do versionamento

## Boas práticas recomendadas para próxima fase

- usar backend remoto para `terraform state`
- remover defaults sensíveis ou específicos demais do código
- parametrizar melhor `prod`
- adicionar pipeline de CI para `fmt`, `validate` e `plan`
- revisar nomes de recursos para aderência ao ambiente real

## Execução local

Formatar:

```bash
make terraform-fmt
```

Inicializar ambiente dev:

```bash
make terraform-dev-init
```

Validar ambiente dev:

```bash
make terraform-dev-validate
```

Gerar plano em dev:

```bash
make terraform-dev-plan
```

Aplicar em dev:

```bash
make terraform-dev-apply
```

Inicializar ambiente prod:

```bash
make terraform-prod-init
```

Validar ambiente prod:

```bash
make terraform-prod-validate
```

Gerar plano em prod:

```bash
make terraform-prod-plan
```

## Observações

- o provider `azurerm` foi deixado sem `subscription_id` hardcoded para seguir uma configuração mais portável
- a autenticação deve ser resolvida via Azure CLI, variáveis de ambiente ou credenciais padronizadas do ambiente de execução
- antes de aplicar em produção, revise cuidadosamente os valores padrão dos ambientes
