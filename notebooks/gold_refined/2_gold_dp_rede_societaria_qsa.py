# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.dropdown("env", "homolog", ["dev", "homolog", "prod"], "Ambiente Alvo")
ENV = dbutils.widgets.get("env")

SCHEMA_SILVER = f"workspace.{ENV}_silver"
SCHEMA_GOLD = f"workspace.{ENV}_gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}")

print(f"=== [GOLD] PRODUTO 2: REDE SOCIETÁRIA QSA [{ENV.upper()}] ===")

df_soc = spark.table(f"{SCHEMA_SILVER}.silver_socios")
df_emp = spark.table(f"{SCHEMA_SILVER}.silver_empresas")
df_qlf = spark.table(f"{SCHEMA_SILVER}.silver_dominio_qualificacoes")

df_gold_qsa = (
    df_soc.alias("soc")
    .join(df_emp.alias("emp"), "nr_cnpj_basico", "inner")
    .join(df_qlf.alias("qlf"), F.col("soc.cd_qualificacao_socio") == F.col("qlf.cd_qualificacao"), "left")
    .select(
        F.col("soc.id_socio_hash"),
        F.col("soc.nr_cnpj_basico"),
        F.col("emp.nm_razao_social"),
        F.when(F.col("soc.cd_identificador_socio") == 1, "PESSOA JURIDICA")
         .when(F.col("soc.cd_identificador_socio") == 2, "PESSOA FISICA")
         .when(F.col("soc.cd_identificador_socio") == 3, "ESTRANGEIRO")
         .otherwise("OUTROS").alias("tp_socio"),
        F.col("soc.nm_socio"),
        F.col("soc.nr_documento_socio_mascarado"),
        F.col("qlf.ds_qualificacao").alias("ds_qualificacao_socio"),
        F.col("soc.dt_entrada_sociedade"),
        F.when(F.col("soc.cd_faixa_etaria") == 1, "0 a 12 anos")
         .when(F.col("soc.cd_faixa_etaria") == 2, "13 a 20 anos")
         .when(F.col("soc.cd_faixa_etaria") == 3, "21 a 30 anos")
         .when(F.col("soc.cd_faixa_etaria") == 4, "31 a 40 anos")
         .when(F.col("soc.cd_faixa_etaria") == 5, "41 a 50 anos")
         .when(F.col("soc.cd_faixa_etaria") == 6, "51 a 60 anos")
         .when(F.col("soc.cd_faixa_etaria") == 7, "61 a 70 anos")
         .when(F.col("soc.cd_faixa_etaria") == 8, "71 a 80 anos")
         .when(F.col("soc.cd_faixa_etaria") == 9, "Mais de 80 anos")
         .otherwise("Nao se aplica").alias("ds_faixa_etaria"),
        F.current_timestamp().alias("dt_carga_gold")
    )
)

(
    df_gold_qsa.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_GOLD}.gold_dp_rede_societaria_qsa")
)
print(f"-> {SCHEMA_GOLD}.gold_dp_rede_societaria_qsa criada.")