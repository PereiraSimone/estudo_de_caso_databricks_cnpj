# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.dropdown("env", "homolog", ["dev", "homolog", "prod"], "Ambiente Alvo")
ENV = dbutils.widgets.get("env")

SCHEMA_RAW = f"workspace.{ENV}_raw"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RAW}")

print(f"=== [RAW] INICIANDO INGESTÃO FULL NO SCHEMA: {SCHEMA_RAW} ===")

BASE_VOLUME = "/Volumes/workspace/default/cnpj_ingestion/bronze"

# COMMAND ----------
# 1. Tabelas Principais (empresas, estabelecimentos, socios)
entidades = ["empresas", "estabelecimentos", "socios"]

for entidade in entidades:
    caminho = f"{BASE_VOLUME}/{entidade}"
    print(f"Ingerindo Raw: {entidade}...")
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
    print(f"-> {SCHEMA_RAW}.raw_{entidade} OK.")

# COMMAND ----------
# 2. Ingestão Robusta de Tabelas de Apoio via dbutils.fs.ls (sem depender de glob sintático)
caminho_apoio = f"{BASE_VOLUME}/tabelas_apoio"
arquivos_apoio = [f.path for f in dbutils.fs.ls(caminho_apoio)]

mapeamento_apoio = {
    "raw_apoio_simples": "_07",
    "raw_apoio_cnaes": "_01",
    "raw_apoio_paises": "_02",
    "raw_apoio_municipios": "_03",
    "raw_apoio_qualificacoes": "_04",
    "raw_apoio_naturezas": "_05"
}

for tabela_raw, sufixo in mapeamento_apoio.items():
    arquivos_alvo = [f for f in arquivos_apoio if sufixo in f]
    if arquivos_alvo:
        print(f"Ingerindo {tabela_raw} ({len(arquivos_alvo)} arquivos encontrados)...")
        (
            spark.read.parquet(*arquivos_alvo)
            .withColumn("_ingested_at", F.current_timestamp())
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{SCHEMA_RAW}.{tabela_raw}")
        )
        print(f"-> {SCHEMA_RAW}.{tabela_raw} OK.")
    else:
        print(f"[AVISO] Nenhum arquivo contendo '{sufixo}' encontrado em {caminho_apoio}.")

print(f"=== [RAW] ETAPA CONCLUÍDA NO SCHEMA {SCHEMA_RAW} ===")