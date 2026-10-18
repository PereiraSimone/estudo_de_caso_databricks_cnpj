# Governança de Contratos de Dados (Data Contracts) na Arquitetura Medallion: Caso CNPJ Alfanumérico

Repositório prático para monografia acadêmica explorando a governança *shift-left* com Databricks e Alice Data Contract CLI, utilizando os dados abertos do CNPJ da Receita Federal.

## Arquitetura Medallion e Ambientes

O pipeline adota segregação lógica por schemas dentro do catálogo `workspace`:
- **`dev`** (`dev_raw`, `dev_silver`, `dev_gold`): Amostra de 1.000 registros para desenvolvimento rápido.
- **`homolog`** (`homolog_raw`, `homolog_silver`, `homolog_gold`): Amostra de 10.000 registros para validação de esteira CI/CD e testes de quebra de contrato.
- **`prod`** (`prod_raw`, `prod_silver`, `prod_gold`): Base integral da Receita Federal.

## Estrutura do Projeto

- `.github/workflows/`: Esteiras de CI (Alice Lint e Test) e CD (Deploy Databricks).
- `contracts/`: Especificações declarativas de Data Contracts no padrão Alice.
- `notebooks/`: Códigos de ingestão, higienização (LGPD) e consolidação dos Data Products.

## Como Executar o Teste Prático de Breaking Change

1. **Estado Normal (v1.0.0)**:
   - A tabela `workspace.homolog_gold.gold_dp_cnpj_estabelecimentos_360` possui a coluna `nr_cnpj` tipada como `BIGINT`.
   - O contrato `contracts/gold_dp_cnpj_estabelecimentos_360.json` declara `nr_cnpj` como `long`.
   - O GitHub Actions roda `datacontract test` e aprova a esteira (**PASS**).

2. **Simulação de Quebra (Breaking Change)**:
   - Altere a Silver para tipar `nr_cnpj` como `STRING` (CNPJ com caracteres alfanuméricos).
   - Abra um Pull Request sem atualizar o contrato JSON.
   - O GitHub Actions falha (**FAIL**) acusando incompatibilidade de esquema (`expected long, found string`).

3. **Evolução de Contrato (v2.0.0)**:
   - Atualize a especificação JSON para `type: string` com o regex `^[0-9A-Za-z]{14}$`.
   - A esteira valida com sucesso e autoriza o merge para `homolog` e `main`.

## Configuração de Secrets no GitHub

Cadastre os seguintes segredos em **Settings > Secrets and variables > Actions**:
- `DATABRICKS_HOST`: URL do seu workspace Databricks.
- `DATABRICKS_TOKEN`: Personal Access Token (PAT) gerado no Databricks.