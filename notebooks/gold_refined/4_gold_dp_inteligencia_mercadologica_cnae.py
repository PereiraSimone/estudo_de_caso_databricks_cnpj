# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.dropdown("env", "homolog", ["dev", "homolog", "prod"], "Ambiente Alvo")
ENV = dbutils.widgets.get("env")

SCHEMA_SILVER = f"workspace.{ENV}_silver"
SCHEMA_GOLD = f"workspace.{ENV}_gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}")

print(f"=== [GOLD] PRODUTO 4: INTELIGÊNCIA CNAE [{ENV.upper()}] ===")

df_est = spark.table(f"{SCHEMA_SILVER}.silver_estabelecimentos")
df_cna = spark.table(f"{SCHEMA_SILVER}.silver_dominio_cnaes")
df_mun = spark.table(f"{SCHEMA_SILVER}.silver_dominio_municipios")

df_gold_cnae = (
    df_est.alias("est")
    .join(df_cna.alias("cna"), F.col("est.cd_cnae_fiscal_principal") == F.col("cna.cd_cnae"), "left")
    .join(df_mun.alias("mun"), F.col("est.cd_municipio") == F.col("mun.cd_municipio"), "left")
    .groupBy(
        F.col("est.sg_uf"),
        F.col("mun.ds_municipio").alias("nm_municipio"),
        F.col("est.cd_cnae_fiscal_principal").alias("cd_cnae"),
        F.col("cna.ds_cnae")
    )
    .agg(
        F.count("est.nr_cnpj_completo").alias("qt_total_estabelecimentos"),
        F.count(F.when(F.col("est.cd_situacao_cadastral") == "02", 1)).alias("qt_estabelecimentos_ativos"),
        F.count(F.when(F.col("est.cd_identificador_matriz_filial") == 1, 1)).alias("qt_matrizes")
    )
    .withColumn("dt_carga_gold", F.current_timestamp())
)

(
    df_gold_cnae.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("sg_uf")
    .saveAsTable(f"{SCHEMA_GOLD}.gold_dp_inteligencia_mercadologica_cnae")
)
print(f"-> {SCHEMA_GOLD}.gold_dp_inteligencia_mercadologica_cnae criada.")