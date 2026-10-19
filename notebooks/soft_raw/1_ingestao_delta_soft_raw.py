# Databricks notebook source
from pyspark.sql import functions as F

# 1. Parâmetro do ambiente de execução
dbutils.widgets.dropdown("env", "homolog", ["dev", "homolog", "prod"], "Ambiente Alvo")
ENV = dbutils.widgets.get("env")

# 2. Resolução dinâmica do esquema
SCHEMA_RAW = f"workspace.{ENV}_raw"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RAW}")

print(f"=== [RAW] INICIANDO INGESTÃO INTEGRAL (FULL) NO ESQUEMA: {SCHEMA_RAW} ===")

BASE_VOLUME = "/Volumes/workspace/default/cnpj_ingestion/bronze"

# COMMAND ----------

# 3. Ingestão Full das tabelas estruturais
entidades = ["empresas", "estabelecimentos", "socios"]

for entidade in entidades:
    caminho = f"{BASE_VOLUME}/{entidade}"
    print(f"A carregar partição Raw (FULL): {entidade}...")
    
    (
        spark.read.parquet(caminho)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_path", F.col("_metadata.file_path"))
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SCHEMA_RAW}.raw_{entidade}")
    )
    print(f"-> Tabela criada: {SCHEMA_RAW}.raw_{entidade}")

# COMMAND ----------

# 4. Ingestão Full das tabelas de apoio
print("A carregar tabelas de apoio auxiliares (FULL)...")
caminho_apoio = f"{BASE_VOLUME}/tabelas_apoio"

(
    spark.read.option("mergeSchema", "true").parquet(caminho_apoio)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_path", F.col("_metadata.file_path"))
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_RAW}.raw_tabelas_apoio")
)

print(f"-> Tabela criada: {SCHEMA_RAW}.raw_tabelas_apoio")
print(f"=== [RAW] CONCLUÍDO COM SUCESSO: {SCHEMA_RAW} ===")