# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Definição e captura do ambiente de execução
dbutils.widgets.dropdown("env", "homolog", ["dev", "homolog", "prod"], "Ambiente Alvo")
ENV = dbutils.widgets.get("env")

# Timeout de 2 horas para garantir a carga integral (Full)
timeout = 7200

print("=" * 70)
print(f"  EXECUTANDO PIPELINE FULL MEDALLION - AMBIENTE: [{ENV.upper()}]")
print("=" * 70)

# COMMAND ----------

# 1. Camada Raw
print("\n>>> [1/3] A executar Camada Raw (Soft Raw)...")
dbutils.notebook.run("soft_raw/1_ingestao_delta_soft_raw", timeout, {"env": ENV})

# COMMAND ----------

# DBTITLE 1,2. Camada Silver
# 2. Camada Silver
# Reestabelece o estado da Célula 1 caso o kernel tenha sido reiniciado entre as etapas
try:
    timeout, ENV
except NameError:
    ENV = dbutils.widgets.get("env")
    timeout = 7200

print("\n>>> [2/3] A executar Camada Silver (Trusted Tier)...")
dbutils.notebook.run("silver_trusted/2_tratamento_higienizacao_silver", timeout, {"env": ENV})

# COMMAND ----------

# DBTITLE 1,3. Camada Gold
# 3. Camada Gold (7 Produtos de Dados)
# Reestabelece o estado da Célula 1 caso o kernel tenha sido reiniciado entre as etapas
try:
    timeout, ENV
except NameError:
    ENV = dbutils.widgets.get("env")
    timeout = 7200

print("\n>>> [3/3] A consolidar os 7 Produtos de Dados Gold...")
dbutils.notebook.run("gold_refined/1_gold_dp_cnpj_estabelecimentos_360", timeout, {"env": ENV})
dbutils.notebook.run("gold_refined/2_gold_dp_rede_societaria_qsa", timeout, {"env": ENV})
dbutils.notebook.run("gold_refined/3_gold_dp_regimes_tributarios_simples_mei", timeout, {"env": ENV})
dbutils.notebook.run("gold_refined/4_gold_dp_inteligencia_mercadologica_cnae", timeout, {"env": ENV})
dbutils.notebook.run("gold_refined/5_gold_dp_risco_longevidade_empresarial", timeout, {"env": ENV})
dbutils.notebook.run("gold_refined/6_gold_dp_governo_entes_publicos", timeout, {"env": ENV})
dbutils.notebook.run("gold_refined/7_gold_dp_score_saude_e_propensao_pj", timeout, {"env": ENV})

print(f"\n Pipeline completo concluído com êxito no ambiente [{ENV.upper()}]!")