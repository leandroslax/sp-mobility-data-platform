# Contributing

## Objetivo

Este projeto foi estruturado para demonstrar boas práticas de engenharia de dados aplicadas a um pipeline Lakehouse em Databricks. Contribuições devem priorizar clareza, confiabilidade e consistência operacional.

## Fluxo de trabalho

1. Crie ou atualize uma branch de trabalho.
2. Faça mudanças pequenas e objetivas.
3. Valide localmente o que foi alterado.
4. Abra Pull Request com contexto suficiente para revisão.

## Padrões esperados

- evitar credenciais e segredos no repositório
- preferir configurações por ambiente
- manter notebooks operacionais coerentes com os jobs do Databricks
- não introduzir paths frágeis sem justificativa
- atualizar documentação quando o comportamento operacional mudar

## Validações recomendadas

Instale primeiro as dependências de desenvolvimento:

```bash
.venv/bin/pip install -r requirements-dev.txt
```

### Python

```bash
python -m py_compile $(find notebooks observability -name "*.py")
```

### Testes unitários

```bash
pytest tests/unit -q
```

### Terraform

```bash
make terraform-fmt
make terraform-dev-validate
```

### Databricks job definitions

```bash
python -m json.tool jobs/sp_mobility_job.json > /dev/null
python -m json.tool jobs/sp_mobility_job_update.json > /dev/null
```

## Pull Requests

Uma boa PR deve incluir:

- objetivo da mudança
- impacto esperado
- evidência de validação
- riscos ou pontos de atenção

## Escopo futuro

As próximas contribuições prioritárias devem focar em:

- testes unitários e de integração
- observabilidade mais forte
- evolução da qualidade de dados
- CI/CD mais robusto
