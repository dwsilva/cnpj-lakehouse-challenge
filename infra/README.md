# Policy tags (BigQuery)

Uso em produção; o avaliador **não** precisa aplicar Terraform. Execução local: ver README na raiz.

Taxonomia PII criada em `policy_tags.tf`. Depois do `terraform apply`, copie os outputs para o profile de prod do dbt:

```yaml
vars:
  policy_tag_cnpj_cpf: "projects/.../policyTags/..."
  policy_tag_nome: "projects/.../policyTags/..."
  policy_tag_endereco: "projects/.../policyTags/..."
```

Colunas marcadas nos `.yml` do dbt (`policy_tags:`) — só gold + staging de socios. DuckDB local ignora esses campos.

Mapeamento:

| Tag | Colunas |
|-----|---------|
| `cnpj_cpf` | `cnpj`, `cnpj_basico`, `cnpj_cpf_socio` |
| `nome` | `razao_social`, `nome_fantasia`, `nome_socio` |
| `endereco` | `cep` |
