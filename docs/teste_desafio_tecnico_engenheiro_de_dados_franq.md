# Teste Desafio Técnico — Engenheiro de Dados (Franq)

> Transcrição do PDF [`teste_desafio_tcnico_-_engenheiro_de_dados_franq.pdf`](teste_desafio_tcnico_-_engenheiro_de_dados_franq.pdf).

Olá! Este teste avalia sua expertise em dbt + Prefect + BigQuery para nosso Data Lakehouse. Não há a necessidade de executar na GCP, podes executar localmente com DuckDB. Mas se quiseres fazer em um ambiente cloud, fique à vontade.

Considere os seguintes dados:

| Dados | Link Direto | Tamanho | Formato |
|-------|-------------|---------|---------|
| ESTABELECIMENTOS | Download [dados-abertos-rf-cnpj.casadosdados.com.br](https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/) | 1.2GB | ZIP→CSV |
| EMPRESAS | Download [dados-abertos-rf-cnpj.casadosdados.com.br](https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/) | 300MB | ZIP→CSV |
| SOCIOS | Download [dados-abertos-rf-cnpj.casadosdados.com.br](https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/) | 400MB | ZIP→CSV |
| Simples Nacional | Download [dados-abertos-rf-cnpj.casadosdados.com.br](https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/) | 50MB | ZIP→CSV |
| CNAE | Download CSV gov | 2MB | CSV |
| ReceitaWS API | https://receitaws.com.br/v1/cnpj/{CNPJ} | - | JSON |

Mirror oficial: [dados.gov.br/cnpj](https://dados.gov.br/dataset/cadastro-nacional-da-pessoa-juridica-cnpj)

## Exercício

### 1. dbt Core Funcional

Estruture um projeto dbt seguindo as melhores práticas de modelagem (ex: arquitetura em camadas).

**Requisitos:**

- Materializações adequadas (configure o uso de incremental onde fizer sentido prático para simular um ambiente de produção).
- Crie modelos de staging para padronização básica dos dados brutos.
- Crie uma tabela fato consolidando métricas de empresas ativas e tabelas dimensão relevantes (ex: hierarquia de CNAEs).
- Implemente um Snapshot (SCD Type 2) para rastrear o histórico de alterações de um campo crítico (ex: Capital Social).

### 2. Macros Jinja

Demonstre sua capacidade de criar código SQL modular e dinâmico.

**Requisitos:**

- Desenvolva e aplique no mínimo 3 macros que resolvam problemas reais de negócio ou engenharia (ex: formatação/parse de campos complexos, padronização, auditoria).

### 3. Testes dbt

Garanta a confiabilidade do seu pipeline de dados.

**Requisitos:**

- Implemente no mínimo 8 testes, que devem abranger obrigatoriamente:
  - Testes genéricos nativos (unicidade, não-nulos, integridade referencial entre tabelas, valores aceitos).
  - Pelo menos um teste customizado via pacote externo (ex: dbt_utils) ou macro própria que valide uma regra de negócio específica (ex: quantidade de sócios maior que zero).

### 4. Prefect Flow

Crie um fluxo automatizado que integre extração e transformação. Desenvolva um Flow contendo Tasks que:

- Faça a extração e carga de uma amostra dos dados (recomendamos ~10.000 linhas por arquivo para otimizar o tempo de execução local).
- Orquestre a execução do pipeline de transformação (dbt run) e a validação de qualidade (dbt test).

### 5. FinOps e Otimização para BigQuery

Gere uma documentação com uma proposta de projeto do modelo desenvolvido, mas dentro de um ambiente cloud.

**Requisitos:**

- Descreva a estratégia de otimização de custos e performance caso esse pipeline rodasse no BigQuery real.
- Defina e justifique suas escolhas de Partitioning e Clustering. Estime (conceitualmente) como essas escolhas impactariam na redução de slots lidos e nos custos de query.
- Estruture o documento de forma clara pensando no onboarding de novos colaboradores. O documento deve ter entre 4 e 7 páginas.
- Inclua no documento o link do projeto desenvolvido, ou então, envie em anexo os artefatos construídos. Caso o link seja privado, envie os dados para acesso no email de resposta.

## 📊 Critérios de Avaliação (100pts)

| Critério | Pts | Checklist |
|----------|-----|-----------|
| dbt Mastery | 30 | Macros, incremental, snapshots, 8+ testes |
| Arquitetura | 25 | Prefect→dbt→BigQuery, resilience |
| Data Quality | 20 | Testes + tratamento nulos/duplicatas |
| BigQuery FinOps | 15 | Partitioning, clustering, custo |
| Código/Execução | 10 | Funciona local, Git limpo |
