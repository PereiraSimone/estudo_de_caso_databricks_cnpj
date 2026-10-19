# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.dropdown("env", "homolog", ["dev", "homolog", "prod"], "Ambiente Alvo")
ENV = dbutils.widgets.get("env")

SCHEMA_SILVER = f"workspace.{ENV}_silver"
SCHEMA_GOLD = f"workspace.{ENV}_gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}")

print(f"=== [GOLD] PRODUTO 5: RISCO E LONGEVIDADE [{ENV.upper()}] ===")

df_est = spark.table(f"{SCHEMA_SILVER}.silver_estabelecimentos")
df_emp = spark.table(f"{SCHEMA_SILVER}.silver_empresas")

df_longevidade = (
    df_est.alias("est")
    .join(df_emp.alias("emp"), "nr_cnpj_basico", "inner")
    .select(
        F.col("est.nr_cnpj_completo").alias("nr_cnpj"),
        F.col("est.nr_cnpj_basico"),
        F.col("emp.nm_razao_social"),
        F.col("est.dt_inicio_atividade"),
        F.col("est.dt_situacao_cadastral"),
        F.col("est.cd_situacao_cadastral"),
        F.col("est.cd_motivo_situacao_cadastral"),
        F.when(
            F.col("est.cd_situacao_cadastral") == "02",
            F.round(F.datediff(F.current_date(), F.col("est.dt_inicio_atividade")) / 365.25, 2)
        ).otherwise(
            F.round(F.datediff(F.col("est.dt_situacao_cadastral"), F.col("est.dt_inicio_atividade")) / 365.25, 2)
        ).alias("nu_idade_empresa_anos"),
        F.when(
            F.datediff(F.current_date(), F.col("est.dt_situacao_cadastral")) <= 365,
            True
        ).otherwise(False).alias("fl_alteracao_situacao_recente"),
        F.col("emp.vl_capital_social"),
        F.col("est.sg_uf"),
        F.current_timestamp().alias("dt_carga_gold")
    )
)

(
    df_longevidade.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("sg_uf")
    .saveAsTable(f"{SCHEMA_GOLD}.gold_dp_risco_longevidade_empresarial")
)
print(f"-> {SCHEMA_GOLD}.gold_dp_risco_longevidade_empresarial criada.")