# Databricks notebook source
# COMMAND ----------
dbutils.widgets.dropdown("env", "homolog", ["dev", "homolog", "prod"], "Ambiente Alvo")
ENV = dbutils.widgets.get("env")

# Tempo limite de 2 horas para processamento completo dos Parquets
timeout = 7200

print("=" * 65)
print(f"  EXECUTANDO ESTEIRA FULL MEDALLION - AMBIENTE: [{ENV.upper()}]")
print("=" * 65)

# COMMAND ----------
# 1. Soft Raw Ingestion
print("\n>>> [1/3] A executar Camada Raw (Full)...")
dbutils.notebook.run("soft_raw/1. Ingestao Delta Soft Raw", timeout, {"env": ENV})

# COMMAND ----------
# 2. Silver Trusted Tier
print("\n>>> [2/3] A executar Camada Silver (Full)...")
dbutils.notebook.run("silver_trusted/2. Tratamento e Higienizacao Silver", timeout, {"env": ENV})

# COMMAND ----------
# 3. Gold Refined Tier
print("\n>>> [3/3] A consolidar Produtos de Dados Gold (Full)...")
dbutils.notebook.run("gold_refined/1_gold_dp_cnpj_estabelecimentos_360", timeout, {"env": ENV})
dbutils.notebook.run("gold_refined/2_gold_dp_rede_societaria_qsa", timeout, {"env": ENV})
dbutils.notebook.run("gold_refined/3_gold_dp_regimes_tributarios_simples_mei", timeout, {"env": ENV})
dbutils.notebook.run("gold_refined/4_gold_dp_inteligencia_mercadologica_cnae", timeout, {"env": ENV})
dbutils.notebook.run("gold_refined/5_gold_dp_risco_longevidade_empresarial", timeout, {"env": ENV})
dbutils.notebook.run("gold_refined/6_gold_dp_governo_entes_publicos", timeout, {"env": ENV})
dbutils.notebook.run("gold_refined/7_gold_dp_score_saude_e_propensao_pj", timeout, {"env": ENV})

print(f"\n Pipeline Full concluído com êxito no ambiente [{ENV.upper()}]!")