# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.dropdown("env", "homolog", ["dev", "homolog", "prod"], "Ambiente Alvo")
ENV = dbutils.widgets.get("env")

SCHEMA_GOLD = f"workspace.{ENV}_gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}")

print(f"=== [GOLD] PRODUTO 7: SCORE DE SAÚDE E PROPENSÃO PJ [{ENV.upper()}] ===")

df_base = spark.table(f"{SCHEMA_GOLD}.gold_dp_cnpj_estabelecimentos_360")

df_score = (
    df_base.alias("b")
    .select(
        F.col("b.nr_cnpj"),
        F.col("b.nr_cnpj_basico"),
        F.col("b.nm_razao_social"),
        F.when(F.col("b.ds_situacao_cadastral") != "ATIVA", True).otherwise(False).alias("fl_risco_cadastral_elevado"),
        F.when(F.col("b.fl_mei_ativo") == True, "MEI")
         .when(F.col("b.vl_capital_social") > 500000, "CORPORATE")
         .otherwise("PYME").alias("ds_segmento_operacional"),
        F.current_timestamp().alias("dt_carga_gold")
    )
)

(
    df_score.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_GOLD}.gold_dp_score_saude_e_propensao_pj")
)
print(f"-> {SCHEMA_GOLD}.gold_dp_score_saude_e_propensao_pj criada.")