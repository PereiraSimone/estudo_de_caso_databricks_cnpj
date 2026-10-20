# Governança de Contratos de Dados (Data Contracts) na Arquitetura Medallion: Caso Prático CNPJ Alfanumérico

Este repositório apresenta a implementação ponta a ponta de uma arquitetura analítica moderna orientada a **Data Mesh** e **Governança Shift-Left**, utilizando **Databricks**, **Delta Lake** e a esteira de validação declarativa **Alice Data Contract CLI (`datacontract-cli`)**. O caso de estudo baseia-se nos dados públicos cadastrais do CNPJ disponibilizados pela Receita Federal do Brasil, simulando o impacto arquitetural e a mitigação da transição para o novo formato de CNPJ alfanumérico.

---

## 1. Visão Geral da Arquitetura e Estratégia Multi-Ambiente

A ingestão e o processamento seguem o padrão Medallion (Raw $\rightarrow$ Silver $\rightarrow$ Gold). Para viabilizar testes contínuos sem sobreposição de dados nem custos desnecessários no Databricks, adotou-se o isolamento lógico por esquemas vinculados ao catálogo `workspace`:

* **`dev` (`workspace.dev_*`)**: Ambiente de desenvolvimento para prototipação e validação de scripts.
* **`homolog` (`workspace.homolog_*`)**: Ambiente de homologação e validação contínua (CI/CD), onde os testes de contratos de dados são executados contra as tabelas Gold materializadas.
* **`prod` (`workspace.prod_*`)**: Ambiente produtivo consolidado com os dados integrados em escala real.

A alternância entre ambientes é realizada dinamicamente através de **Databricks Widgets** (`dbutils.widgets.dropdown("env", "homolog", ...)`), permitindo que o mesmo código versionado no Git seja promovido entre branches (`develop`, `homolog`, `main`) sem alterações manuais de caminho ou catálogo.

[ Receita Federal (WebDAV / Parquet) ]
                         │
                         ▼
          [ Landing Zone / Volume DBFS ]
         /Volumes/workspace/default/...
                         │
   ┌─────────────────────┼─────────────────────┐
   ▼                     ▼                     ▼
[ DEV TIER ]          [ HOMOLOG TIER ]       [ PROD TIER ]
workspace.dev_*       workspace.homolog_*    workspace.prod_*
├── raw_*             ├── raw_*              ├── raw_*
├── silver_*          ├── silver_*           ├── silver_*
└── gold_dp_*         └── gold_dp_*          └── gold_dp_*
│
▼
[ Alice Data Contract CI ]
GitHub Actions Pull Request


## 2. Camadas da Arquitetura Medallion

### 2.1. Landing Zone & Raw (Bronze)
* **Origem**: Ficheiros brutos descarregados mensalmente via WebDAV no volume `/Volumes/workspace/default/cnpj_ingestion/bronze`.
* **Raw**: Ingestão no formato Delta Lake preservando a estrutura original, com acréscimo de metadados técnicos de auditoria (`_ingested_at`, `_source_path`):
  * `raw_empresas`
  * `raw_estabelecimentos`
  * `raw_socios`
  * `raw_tabelas_apoio`

### 2.2. Silver (Trusted & Conformidade LGPD)
A camada Silver realiza a tipagem estrita de datas (`yyyy-MM-dd`), conversão monetária de capital social, enriquecimento semântico de status cadastrais e deduplicação de chaves de negócio:
* **Conformidade com a LGPD**: Os documentos e nomes dos sócios são anonimizados/mascarados via criptografia hash determinística (`SHA-256`) na criação da chave `id_socio_hash`, eliminando exposição de dados sensíveis (PII).
* **Particionamento**: Otimização física por Unidade Federativa (`sg_uf`) para consultas regionais eficientes.
* **Tabelas Silver**: `silver_empresas`, `silver_estabelecimentos`, `silver_socios`, `silver_simples_nacional` e domínios auxiliares (`cnaes`, `municipios`, `naturezas_juridicas`, `paises`, `qualificacoes`).

---

## 3. Catálogo de Produtos de Dados (Camada Gold)

A camada Gold é composta por 7 Produtos de Dados analíticos modelados para atender a casos de uso específicos de negócio, crédito, compliance e inteligência mercadológica:

### Produto 1: Visão Cadastral 360 (`gold_dp_cnpj_estabelecimentos_360`)
* **Finalidade**: Visão consolidada unificada de matrizes e filiais com razão social, porte, natureza jurídica, situação cadastral e localização geográfica.
* **Campos Chave**: `nr_cnpj`, `nr_cnpj_basico`, `nm_razao_social`, `nm_fantasia`, `tp_estabelecimento`, `ds_situacao_cadastral`, `fl_simples_nacional_ativo`, `fl_mei_ativo`, `sg_uf`, `nm_municipio`.
* **Papel no Estudo**: É a tabela central sob **Contrato de Dados**, utilizada para a demonstração da quebra de compatibilidade de esquema (CNPJ numérico vs. alfanumérico).

### Produto 2: Rede Societária e QSA (`gold_dp_rede_societaria_qsa`)
* **Finalidade**: Mapeamento do Quadro de Sócios e Administradores (QSA) com enriquecimento da qualificação funcional e segmentação etária.
* **Campos Chave**: `id_socio_hash`, `nr_cnpj_basico`, `nm_razao_social`, `tp_socio`, `nm_socio`, `ds_qualificacao_socio`, `dt_entrada_sociedade`, `ds_faixa_etaria`.
* **Segurança**: Conformidade total com a LGPD através do identificador pseudoanonimizado.

### Produto 3: Inteligência Tributária Simples / MEI (`gold_dp_regimes_tributarios_simples_mei`)
* **Finalidade**: Análise de histórico de adesão, permanência e exclusão dos regimes simplificados de tributação (Simples Nacional e Microempreendedor Individual).
* **Campos Chave**: `nr_cnpj_basico`, `nm_razao_social`, `fl_optante_simples`, `dt_opcao_simples`, `dt_exclusao_simples`, `fl_optante_mei`, `nu_dias_permanencia_simples`, `vl_capital_social`.

### Produto 4: Inteligência Geoeconômica e CNAE (`gold_dp_inteligencia_mercadologica_cnae`)
* **Finalidade**: Produto agregado focado em densidade econômica regional, detalhando concentração de empresas ativas, matrizes e ramos de atividade (CNAE) por município e UF.
* **Campos Chave**: `sg_uf`, `nm_municipio`, `cd_cnae`, `ds_cnae`, `qt_total_estabelecimentos`, `qt_estabelecimentos_ativos`, `qt_matrizes`.

### Produto 5: Risco e Longevidade Empresarial (`gold_dp_risco_longevidade_empresarial`)
* **Finalidade**: Indicadores de solvência, estabilidade temporal e tempo de sobrevivência corporativa, calculando a idade operacional da empresa e sinalizando alterações cadastrais recentes.
* **Campos Chave**: `nr_cnpj`, `nr_cnpj_basico`, `nm_razao_social`, `nu_idade_empresa_anos`, `fl_alteracao_situacao_recente`, `cd_situacao_cadastral`, `vl_capital_social`, `sg_uf`.

### Produto 6: Administração Pública e B2G (`gold_dp_governo_entes_publicos`)
* **Finalidade**: Segmentação exclusiva de órgãos públicos, autarquias, fundações e entes federativos da administração direta e indireta (naturezas jurídicas iniciadas em `1`).
* **Campos Chave**: `nr_cnpj`, `nr_cnpj_basico`, `nm_razao_social`, `cd_natureza_juridica`, `ds_natureza_juridica`, `ds_ente_federativo_responsavel`, `sg_uf`.

### Produto 7: Score de Saúde e Propensão PJ (`gold_dp_score_saude_e_propensao_pj`)
* **Finalidade**: Produto analítico voltado a esteiras de crédito e propensão de vendas B2B, categorizando empresas em segmentos operacionais (`MEI`, `PYME`, `CORPORATE`) e alertando sobre risco cadastral elevado.
* **Campos Chave**: `nr_cnpj`, `nr_cnpj_basico`, `nm_razao_social`, `fl_risco_cadastral_elevado`, `ds_segmento_operacional`.

---

## 4. Governança Shift-Left com Alice Data Contracts

A governança tradicional de dados identifica erros em produção após a execução de relatórios ou quebra de dashboards. Neste projeto, a abordagem **Shift-Left** move a garantia da qualidade para o início do ciclo de entrega (no momento da abertura de Pull Requests).

### Especificação Declarativa
Os contratos de dados residem no diretório `contracts/` em formato JSON compatível com o ecossistema Alice / Open Data Contract Standard:
* `gold_dp_cnpj_estabelecimentos_360.json`
* `gold_dp_rede_societaria_qsa.json`
* `gold_dp_score_saude_e_propensao_pj.json`

### O Ciclo Prático do Experimento: Quebra e Evolução

O estudo de caso da monografia simula o cenário regulatório da Receita Federal com o formato do CNPJ:

1. **Cenário 1 — Baseline Numérico (Status: PASS)**:
   * A tabela `workspace.homolog_gold.gold_dp_cnpj_estabelecimentos_360` possui a coluna `nr_cnpj` tipada como `BIGINT`.
   * O contrato v1.0.0 declara o campo como `type: long`.
   * Ao abrir o Pull Request, a GitHub Action executa `datacontract test`. A validação é bem-sucedida (esteira verde).

2. **Cenário 2 — Quebra de Contrato / Breaking Change (Status: FAIL)**:
   * A ingestão altera o tipo da coluna para `STRING` para comportar caracteres alfanuméricos.
   * Um Pull Request é submetido sem a atualização formal do contrato v1.0.0.
   * A Action acusa incompatibilidade estrutural (`Schema Mismatch: expected long, got string`) e falha os testes, bloqueando a integração do código antes de atingir o ambiente de produção.

3. **Cenário 3 — Evolução Governada do Contrato (Status: PASS)**:
   * O contrato é versionado para `2.0.0`, declarando `nr_cnpj` como `type: string` e aplicando validação semântica com expressão regular alfanumérica (`^[0-9A-Z]{14}$`).
   * A esteira de CI reavalia o esquema, aprova a mudança e autoriza o merge.

---

## 5. Estrutura do Repositório

```text
├── .github/
│   └── workflows/
│       ├── alice-contract-check.yml   # CI: Lint e Teste de Contratos de Dados no PR
│       └── databricks-deploy.yml      # CD: Sincronização nos Git Folders do Databricks
├── contracts/
│   ├── gold_dp_cnpj_estabelecimentos_360.json
│   ├── gold_dp_rede_societaria_qsa.json
│   └── gold_dp_score_saude_e_propensao_pj.json
├── databricks.yml                      # Configuração do Databricks Asset Bundle
├── notebooks/
│   ├── 00_master_runner.py            # Orquestrador sequencial da arquitetura Medallion
│   ├── landing_zone/
│   │   └── 00_webdav_replication_receita_federal.py
│   ├── soft_raw/
│   │   └── 1_ingestao_delta_soft_raw.py
│   ├── silver_trusted/
│   │   └── 2_tratamento_higienizacao_silver.py
│   └── gold_refined/
│       ├── 1_gold_dp_cnpj_estabelecimentos_360.py
│       ├── 2_gold_dp_rede_societaria_qsa.py
│       ├── 3_gold_dp_regimes_tributarios_simples_mei.py
│       ├── 4_gold_dp_inteligencia_mercadologica_cnae.py
│       ├── 5_gold_dp_risco_longevidade_empresarial.py
│       ├── 6_gold_dp_governo_entes_publicos.py
│       └── 7_gold_dp_score_saude_e_propensao_pj.py
├── .gitignore
└── README.md

## 6. Configuração e Execução 
Pré-requisitosWorkspace no Databricks com Unity Catalog ativo.Serverless SQL Warehouse ativo.Volume populado com os ficheiros de origem (/Volumes/workspace/default/cnpj_ingestion/bronze).Configuração de Secrets no GitHubNo repositório do GitHub, navegue até Settings > Secrets and variables > Actions e adicione as seguintes variáveis:DATABRICKS_HOST: URL base do workspace (ex: https://dbc-xxxx.cloud.databricks.com).DATABRICKS_TOKEN: Personal Access Token (PAT) gerado no Databricks.DATABRICKS_HTTP_PATH: Caminho HTTP de conexão do SQL Warehouse Serverless.Execução no DatabricksPara reprocessar todas as camadas da esteira no ambiente desejado:Abra o arquivo notebooks/00_master_runner.py no Databricks.No widget no topo da página, selecione o ambiente (dev, homolog ou prod).Clique em Run All. O pipeline executará de forma encadeada as camadas Raw $\rightarrow$ Silver $\rightarrow$ Gold.