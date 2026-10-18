# Databricks notebook source
from pyspark.sql import functions as F

# 1. Configuração do Widget de Ambiente
dbutils.widgets.dropdown("env", "homolog", ["dev", "homolog", "prod"], "Ambiente Alvo")
ENV = dbutils.widgets.get("env")

# 2. Resolução dinâmica dos schemas
SCHEMA_RAW = f"workspace.{ENV}_raw"
SCHEMA_SILVER = f"workspace.{ENV}_silver"
SCHEMA_GOLD = f"workspace.{ENV}_gold"

print(f"=== INICIANDO PROCESSAMENTO SILVER (TABELAS DE APOIO & SIMPLES) [{ENV.upper()}] ===")
print(f"Schema Silver Destino: {SCHEMA_SILVER}")

# Assegura a criação do schema de destino no Unity Catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_SILVER}")

# 3. Caminho base dos arquivos brutos
BASE_DIR = "/Volumes/workspace/default/cnpj_ingestion/bronze/tabelas_apoio"

# Controlo de volumetria para tabelas densas (como Simples Nacional)
LIMITES_AMOSTRA = {
    "dev": 1000,
    "homolog": 10000,
    "prod": None
}
limite_linhas = LIMITES_AMOSTRA.get(ENV)

# -------------------------------------------------------------------------
# 1. SILVER: SIMPLES NACIONAL E MEI (Sufixo _07)
# -------------------------------------------------------------------------
df_simples_raw = spark.read.parquet(f"{BASE_DIR}/*_07.parquet")

if limite_linhas is not None:
    df_simples_raw = df_simples_raw.limit(limite_linhas)

df_silver_simples = (
    df_simples_raw
    .select(
        # CNPJ Básico mantido como BIGINT para posterior teste de quebra de contrato Alice
        F.col("col_0").cast("bigint").alias("nr_cnpj_basico"),
        F.upper(F.trim(F.col("col_1"))).alias("fl_opcao_simples"),
        F.to_date(F.try_to_date(F.col("col_2").cast("string"), "yyyyMMdd"), "yyyy-MM-dd").alias("dt_opcao_simples"),
        F.to_date(F.try_to_date(F.col("col_3").cast("string"), "yyyyMMdd"), "yyyy-MM-dd").alias("dt_exclusao_simples"),
        F.upper(F.trim(F.col("col_4"))).alias("fl_opcao_mei"),
        F.to_date(F.try_to_date(F.col("col_5").cast("string"), "yyyyMMdd"), "yyyy-MM-dd").alias("dt_opcao_mei"),
        F.to_date(F.try_to_date(F.col("col_6").cast("string"), "yyyyMMdd"), "yyyy-MM-dd").alias("dt_exclusao_mei"),
        F.current_timestamp().alias("dt_silver_update")
    )
    .dropDuplicates(["nr_cnpj_basico"])
)

(
    df_silver_simples.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_SILVER}.silver_simples_nacional")
)
print(f"-> {SCHEMA_SILVER}.silver_simples_nacional criada com sucesso.")

# -------------------------------------------------------------------------
# 2. SILVER: DOMÍNIO DE MUNICÍPIOS (Sufixo _03)
# -------------------------------------------------------------------------
(
    spark.read.parquet(f"{BASE_DIR}/*_03.parquet")
    .select(
        F.col("col_0").cast("int").alias("cd_municipio"),
        F.upper(F.trim(F.col("col_1"))).alias("ds_municipio"),
        F.current_timestamp().alias("dt_silver_update")
    )
    .dropDuplicates(["cd_municipio"])
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_SILVER}.silver_dominio_municipios")
)
print(f"-> {SCHEMA_SILVER}.silver_dominio_municipios criada com sucesso.")

# -------------------------------------------------------------------------
# 3. SILVER: DOMÍNIO DE CNAES (Sufixo _01)
# -------------------------------------------------------------------------
(
    spark.read.parquet(f"{BASE_DIR}/*_01.parquet")
    .select(
        F.lpad(F.col("col_0").cast("string"), 7, "0").alias("cd_cnae"),
        F.upper(F.trim(F.col("col_1"))).alias("ds_cnae"),
        F.current_timestamp().alias("dt_silver_update")
    )
    .dropDuplicates(["cd_cnae"])
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_SILVER}.silver_dominio_cnaes")
)
print(f"-> {SCHEMA_SILVER}.silver_dominio_cnaes criada com sucesso.")

# -------------------------------------------------------------------------
# 4. SILVER: DOMÍNIO DE NATUREZAS JURÍDICAS (Sufixo _05)
# -------------------------------------------------------------------------
(
    spark.read.parquet(f"{BASE_DIR}/*_05.parquet")
    .select(
        F.col("col_0").cast("int").alias("cd_natureza_juridica"),
        F.upper(F.trim(F.col("col_1"))).alias("ds_natureza_juridica"),
        F.current_timestamp().alias("dt_silver_update")
    )
    .dropDuplicates(["cd_natureza_juridica"])
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_SILVER}.silver_dominio_naturezas_juridicas")
)
print(f"-> {SCHEMA_SILVER}.silver_dominio_naturezas_juridicas criada com sucesso.")

# -------------------------------------------------------------------------
# 5. SILVER: DOMÍNIO DE QUALIFICAÇÕES DE SÓCIOS (Sufixo _04)
# -------------------------------------------------------------------------
(
    spark.read.parquet(f"{BASE_DIR}/*_04.parquet")
    .select(
        F.col("col_0").cast("int").alias("cd_qualificacao"),
        F.upper(F.trim(F.col("col_1"))).alias("ds_qualificacao"),
        F.current_timestamp().alias("dt_silver_update")
    )
    .dropDuplicates(["cd_qualificacao"])
    .write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_SILVER}.silver_dominio_qualificacoes_socios")
)
print(f"-> {SCHEMA_SILVER}.silver_dominio_qualificacoes_socios criada com sucesso.")