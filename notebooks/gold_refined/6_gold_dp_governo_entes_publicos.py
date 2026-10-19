# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.dropdown("env", "homolog", ["dev", "homolog", "prod"], "Ambiente Alvo")
ENV = dbutils.widgets.get("env")

SCHEMA_SILVER = f"workspace.{ENV}_silver"
SCHEMA_GOLD = f"workspace.{ENV}_gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}")

print(f"=== [GOLD] PRODUTO 6: GOVERNO E ENTES PÚBLICOS [{ENV.upper()}] ===")

df_est = spark.table(f"{SCHEMA_SILVER}.silver_estabelecimentos")
df_emp = spark.table(f"{SCHEMA_SILVER}.silver_empresas")
df_nat = spark.table(f"{SCHEMA_SILVER}.silver_dominio_naturezas_juridicas")

df_governo = (
    df_est.alias("est")
    .join(df_emp.alias("emp"), "nr_cnpj_basico", "inner")
    .join(df_nat.alias("nat"), F.col("emp.cd_natureza_juridica") == F.col("nat.cd_natureza_juridica"), "inner")
    .filter(F.col("emp.cd_natureza_juridica").cast("string").startswith("1"))
    .select(
        F.col("est.nr_cnpj_completo").alias("nr_cnpj"),
        F.col("est.nr_cnpj_basico"),
        F.col("emp.nm_razao_social"),
        F.col("est.nm_fantasia"),
        F.col("emp.cd_natureza_juridica"),
        F.col("nat.ds_natureza_juridica"),
        F.col("emp.ds_ente_federativo_responsavel"),
        F.col("est.sg_uf"),
        F.col("est.cd_municipio"),
        F.current_timestamp().alias("dt_carga_gold")
    )
)

(
    df_governo.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("sg_uf")
    .saveAsTable(f"{SCHEMA_GOLD}.gold_dp_governo_entes_publicos")
)
print(f"-> {SCHEMA_GOLD}.gold_dp_governo_entes_publicos criada.")