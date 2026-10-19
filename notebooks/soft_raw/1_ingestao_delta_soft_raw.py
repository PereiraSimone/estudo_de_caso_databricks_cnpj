# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.dropdown("env", "homolog", ["dev", "homolog", "prod"], "Ambiente Alvo")
ENV = dbutils.widgets.get("env")

SCHEMA_RAW = f"workspace.{ENV}_raw"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RAW}")

print(f"=== [RAW] INGESTÃO INTEGRAL (FULL) NO ESQUEMA: {SCHEMA_RAW} ===")

BASE_VOLUME = "/Volumes/workspace/default/cnpj_ingestion/bronze"

# COMMAND ----------
# 1. Tabelas Principais (empresas, estabelecimentos, socios)
entidades_principais = ["empresas", "estabelecimentos", "socios"]

for entidade in entidades_principais:
    caminho = f"{BASE_VOLUME}/{entidade}"
    print(f"Ingerindo {entidade} de {caminho}...")
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
# 2. Tabelas de Apoio (Varredura dinâmica com proteção contra erro de schema)
caminho_apoio = f"{BASE_VOLUME}/tabelas_apoio"
lista_ficheiros = [f.path for f in dbutils.fs.ls(caminho_apoio)]

mapeamento_apoio = {
    "raw_apoio_simples": "07",
    "raw_apoio_cnaes": "01",
    "raw_apoio_paises": "02",
    "raw_apoio_municipios": "03",
    "raw_apoio_qualificacoes": "04",
    "raw_apoio_naturezas": "05"
}

for tabela_destino, padrao in mapeamento_apoio.items():
    arquivos_alvo = [f for f in lista_ficheiros if padrao in f]
    if arquivos_alvo:
        print(f"A carregar {tabela_destino} ({len(arquivos_alvo)} ficheiro(s))...")
        (
            spark.read.parquet(*arquivos_alvo)
            .withColumn("_ingested_at", F.current_timestamp())
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{SCHEMA_RAW}.{tabela_destino}")
        )
        print(f"-> Tabela criada: {SCHEMA_RAW}.{tabela_destino}")
    else:
        print(f"[AVISO] Nenhum ficheiro com identificador '{padrao}' encontrado.")

print(f"=== [RAW] CARGA CONCLUÍDA COM SUCESSO: {SCHEMA_RAW} ===")