# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.dropdown("env", "homolog", ["dev", "homolog", "prod"], "Ambiente Alvo")
ENV = dbutils.widgets.get("env")

SCHEMA_SILVER = f"workspace.{ENV}_silver"
SCHEMA_GOLD = f"workspace.{ENV}_gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}")

print(f"=== [GOLD] PRODUTO 1: ESTABELECIMENTOS 360 [{ENV.upper()}] ===")

df_est = spark.table(f"{SCHEMA_SILVER}.silver_estabelecimentos")
df_emp = spark.table(f"{SCHEMA_SILVER}.silver_empresas")
df_smp = spark.table(f"{SCHEMA_SILVER}.silver_simples_nacional")
df_mun = spark.table(f"{SCHEMA_SILVER}.silver_dominio_municipios")
df_nat = spark.table(f"{SCHEMA_SILVER}.silver_dominio_naturezas_juridicas")
df_cna = spark.table(f"{SCHEMA_SILVER}.silver_dominio_cnaes")

df_gold_360 = (
    df_est.alias("est")
    .join(df_emp.alias("emp"), "nr_cnpj_basico", "inner")
    .join(df_smp.alias("smp"), "nr_cnpj_basico", "left")
    .join(df_mun.alias("mun"), F.col("est.cd_municipio") == F.col("mun.cd_municipio"), "left")
    .join(df_nat.alias("nat"), F.col("emp.cd_natureza_juridica") == F.col("nat.cd_natureza_juridica"), "left")
    .join(df_cna.alias("cna"), F.col("est.cd_cnae_fiscal_principal") == F.col("cna.cd_cnae"), "left")
    .select(
        F.col("est.nr_cnpj_completo").alias("nr_cnpj"),
        F.col("est.nr_cnpj_basico"),
        F.col("emp.nm_razao_social"),
        F.col("est.nm_fantasia"),
        F.when(F.col("est.cd_identificador_matriz_filial") == 1, "MATRIZ").otherwise("FILIAL").alias("tp_estabelecimento"),
        F.when(F.col("est.cd_situacao_cadastral") == "02", "ATIVA").otherwise("INATIVA").alias("ds_situacao_cadastral"),
        F.col("est.dt_inicio_atividade"),
        F.col("emp.vl_capital_social"),
        F.col("nat.ds_natureza_juridica"),
        F.when(F.col("smp.fl_opcao_simples") == "S", True).otherwise(False).alias("fl_simples_nacional_ativo"),
        F.when(F.col("smp.fl_opcao_mei") == "S", True).otherwise(False).alias("fl_mei_ativo"),
        F.col("est.sg_uf"),
        F.col("mun.ds_municipio").alias("nm_municipio"),
        F.current_timestamp().alias("dt_carga_gold")
    )
)

# Antes da escrita final na tabela gold_dp_cnpj_estabelecimentos_360:
df_gold_estabelecimentos = df_gold_estabelecimentos.filter(
    F.col("nr_cnpj").isNotNull() & (F.trim(F.col("nr_cnpj")) != "")
)
(
    df_gold_estabelecimentos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("sg_uf")
    .saveAsTable(f"{SCHEMA_GOLD}.workspace.homolog_gold.gold_dp_cnpj_estabelecimentos_360")
)
print(f"-> {SCHEMA_GOLD}.gold_dp_cnpj_estabelecimentos_360 criada.")