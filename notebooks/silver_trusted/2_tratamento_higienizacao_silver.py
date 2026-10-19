# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.dropdown("env", "homolog", ["dev", "homolog", "prod"], "Ambiente Alvo")
ENV = dbutils.widgets.get("env")

SCHEMA_RAW = f"workspace.{ENV}_raw"
SCHEMA_SILVER = f"workspace.{ENV}_silver"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_SILVER}")

print(f"=== [SILVER] PROCESSAMENTO FULL: {SCHEMA_RAW} -> {SCHEMA_SILVER} ===")

# COMMAND ----------
# 1. Simples Nacional e MEI
print("Processando Silver: Simples Nacional...")
df_simples_raw = spark.table(f"{SCHEMA_RAW}.raw_apoio_simples")

df_silver_simples = (
    df_simples_raw
    .select(
        F.col("col_0").cast("bigint").alias("nr_cnpj_basico"),
        F.upper(F.trim(F.col("col_1"))).alias("fl_opcao_simples"),
        F.try_to_date(F.col("col_2").cast("string"), "yyyyMMdd").alias("dt_opcao_simples"),
        F.try_to_date(F.col("col_3").cast("string"), "yyyyMMdd").alias("dt_exclusao_simples"),
        F.upper(F.trim(F.col("col_4"))).alias("fl_opcao_mei"),
        F.try_to_date(F.col("col_5").cast("string"), "yyyyMMdd").alias("dt_opcao_mei"),
        F.try_to_date(F.col("col_6").cast("string"), "yyyyMMdd").alias("dt_exclusao_mei"),
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
print(f"-> {SCHEMA_SILVER}.silver_simples_nacional OK.")

# COMMAND ----------
# 2. Tabelas de Domínio (Lendo direto das Deltas Raw dedicadas)
print("Processando Silver: Domínios...")

# Municípios
(
    spark.table(f"{SCHEMA_RAW}.raw_apoio_municipios")
    .select(
        F.col("col_0").cast("int").alias("cd_municipio"),
        F.upper(F.trim(F.col("col_1"))).alias("ds_municipio"),
        F.current_timestamp().alias("dt_silver_update")
    )
    .dropDuplicates(["cd_municipio"])
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_SILVER}.silver_dominio_municipios")
)

# Naturezas Jurídicas
(
    spark.table(f"{SCHEMA_RAW}.raw_apoio_naturezas")
    .select(
        F.col("col_0").cast("int").alias("cd_natureza_juridica"),
        F.upper(F.trim(F.col("col_1"))).alias("ds_natureza_juridica"),
        F.current_timestamp().alias("dt_silver_update")
    )
    .dropDuplicates(["cd_natureza_juridica"])
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_SILVER}.silver_dominio_naturezas_juridicas")
)

# Qualificações de Sócios
(
    spark.table(f"{SCHEMA_RAW}.raw_apoio_qualificacoes")
    .select(
        F.col("col_0").cast("int").alias("cd_qualificacao"),
        F.upper(F.trim(F.col("col_1"))).alias("ds_qualificacao"),
        F.current_timestamp().alias("dt_silver_update")
    )
    .dropDuplicates(["cd_qualificacao"])
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_SILVER}.silver_dominio_qualificacoes")
)

# Países
(
    spark.table(f"{SCHEMA_RAW}.raw_apoio_paises")
    .select(
        F.col("col_0").cast("int").alias("cd_pais"),
        F.upper(F.trim(F.col("col_1"))).alias("ds_pais"),
        F.current_timestamp().alias("dt_silver_update")
    )
    .dropDuplicates(["cd_pais"])
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_SILVER}.silver_dominio_paises")
)

# CNAEs
(
    spark.table(f"{SCHEMA_RAW}.raw_apoio_cnaes")
    .select(
        F.lpad(F.col("col_0").cast("string"), 7, "0").alias("cd_cnae"),
        F.upper(F.trim(F.col("col_1"))).alias("ds_cnae"),
        F.current_timestamp().alias("dt_silver_update")
    )
    .dropDuplicates(["cd_cnae"])
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_SILVER}.silver_dominio_cnaes")
)
print("-> Tabelas de Domínio OK.")

# COMMAND ----------
# 3. Empresas
print("Processando Silver: Empresas...")
(
    spark.table(f"{SCHEMA_RAW}.raw_empresas")
    .select(
        F.col("col_0").cast("bigint").alias("nr_cnpj_basico"),
        F.upper(F.trim(F.col("col_1"))).alias("nm_razao_social"),
        F.col("col_2").cast("int").alias("cd_natureza_juridica"),
        F.col("col_3").cast("int").alias("cd_qualificacao_responsavel"),
        F.regexp_replace(F.col("col_4"), ",", ".").cast("decimal(18,2)").alias("vl_capital_social"),
        F.col("col_5").cast("string").alias("cd_porte_empresa"),
        F.col("col_6").cast("string").alias("ds_ente_federativo_responsavel"),
        F.current_timestamp().alias("dt_silver_update")
    )
    .dropDuplicates(["nr_cnpj_basico"])
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_SILVER}.silver_empresas")
)
print(f"-> {SCHEMA_SILVER}.silver_empresas OK.")

# COMMAND ----------
# 4. Estabelecimentos
print("Processando Silver: Estabelecimentos...")
(
    spark.table(f"{SCHEMA_RAW}.raw_estabelecimentos")
    .select(
        F.col("col_0").cast("bigint").alias("nr_cnpj_basico"),
        F.concat(
            F.lpad(F.col("col_0").cast("string"), 8, "0"),
            F.lpad(F.col("col_1").cast("string"), 4, "0"),
            F.lpad(F.col("col_2").cast("string"), 2, "0")
        ).cast("bigint").alias("nr_cnpj_completo"),
        F.col("col_3").cast("int").alias("cd_identificador_matriz_filial"),
        F.upper(F.trim(F.col("col_4"))).alias("nm_fantasia"),
        F.col("col_5").cast("string").alias("cd_situacao_cadastral"),
        F.try_to_date(F.col("col_6").cast("string"), "yyyyMMdd").alias("dt_situacao_cadastral"),
        F.col("col_7").cast("int").alias("cd_motivo_situacao_cadastral"),
        F.try_to_date(F.col("col_10").cast("string"), "yyyyMMdd").alias("dt_inicio_atividade"),
        F.lpad(F.col("col_11").cast("string"), 7, "0").alias("cd_cnae_fiscal_principal"),
        F.col("col_12").cast("string").alias("ds_cnae_fiscal_secundaria"),
        F.col("col_13").cast("string").alias("ds_tipo_logradouro"),
        F.col("col_14").cast("string").alias("nm_logradouro"),
        F.col("col_15").cast("string").alias("nr_logradouro"),
        F.col("col_18").cast("string").alias("nm_bairro"),
        F.col("col_19").cast("string").alias("nr_cep"),
        F.col("col_20").cast("string").alias("sg_uf"),
        F.col("col_21").cast("int").alias("cd_municipio"),
        F.current_timestamp().alias("dt_silver_update")
    )
    .dropDuplicates(["nr_cnpj_completo"])
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .partitionBy("sg_uf")
    .saveAsTable(f"{SCHEMA_SILVER}.silver_estabelecimentos")
)
print(f"-> {SCHEMA_SILVER}.silver_estabelecimentos OK.")

# COMMAND ----------
# 5. Sócios (Tratamento LGPD)
print("Processando Silver: Sócios...")
(
    spark.table(f"{SCHEMA_RAW}.raw_socios")
    .select(
        F.sha2(F.concat_ws("||", F.col("col_0"), F.col("col_1"), F.col("col_3")), 256).alias("id_socio_hash"),
        F.col("col_0").cast("bigint").alias("nr_cnpj_basico"),
        F.col("col_1").cast("int").alias("cd_identificador_socio"),
        F.upper(F.trim(F.col("col_2"))).alias("nm_socio"),
        F.col("col_3").cast("string").alias("nr_documento_socio_mascarado"),
        F.col("col_4").cast("int").alias("cd_qualificacao_socio"),
        F.try_to_date(F.col("col_5").cast("string"), "yyyyMMdd").alias("dt_entrada_sociedade"),
        F.col("col_6").cast("int").alias("cd_pais"),
        F.col("col_10").cast("int").alias("cd_faixa_etaria"),
        F.current_timestamp().alias("dt_silver_update")
    )
    .dropDuplicates(["id_socio_hash"])
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{SCHEMA_SILVER}.silver_socios")
)
print(f"-> {SCHEMA_SILVER}.silver_socios OK.")
print(f"=== [SILVER] ETAPA CONCLUÍDA NO SCHEMA {SCHEMA_SILVER} ===")