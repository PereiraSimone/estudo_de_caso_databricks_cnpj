# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.dropdown("env", "homolog", ["dev", "homolog", "prod"], "Ambiente Alvo")
ENV = dbutils.widgets.get("env")

SCHEMA_SILVER = f"workspace.{ENV}_silver"
SCHEMA_GOLD = f"workspace.{ENV}_gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}")

print(f"=== [GOLD] PRODUTO 3: REGIMES TRIBUTÁRIOS [{ENV.upper()}] ===")

df_smp = spark.table(f"{SCHEMA_SILVER}.silver_simples_nacional")
df_emp = spark.table(f"{SCHEMA_SILVER}.silver_empresas")

df_gold_simples = (
    df_smp.alias("smp")
    .join(df_emp.alias("emp"), "nr_cnpj_basico", "inner")
    .select(
        F.col("smp.nr_cnpj_basico"),
        F.col("emp.nm_razao_social"),
        F.when(F.col("smp.fl_opcao_simples") == "S", True).otherwise(False).alias("fl_optante_simples"),
        F.col("smp.dt_opcao_simples"),
        F.col("smp.dt_exclusao_simples"),
        F.when(F.col("smp.fl_opcao_mei") == "S", True).otherwise(False).alias("fl_optante_mei"),
        F.col("smp.dt_opcao_mei"),
        F.col("smp.dt_exclusao_mei"),
        F.when(
            F.col("smp.dt_exclusao_simples").isNotNull(),
            F.datediff(F.col("smp.dt_exclusao_simples"), F.col("smp.dt_opcao_simples"))
        ).when(
            F.col("smp.fl_opcao_simples") == "S",
            F.datediff(F.current_date(), F.col("smp.dt_opcao_simples"))
        ).otherwise(0).alias("nu_dias_permanencia_simples"),
        F.col("emp.vl_capital_social"),
        F.current_timestamp().alias("dt_carga_gold")
    )
)

(
    df_gold_simples.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_GOLD}.gold_dp_regimes_tributarios_simples_mei")
)
print(f"-> {SCHEMA_GOLD}.gold_dp_regimes_tributarios_simples_mei criada.")