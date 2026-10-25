{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1792281689694,
     "inputWidgets": {},
     "nuid": "0490f009-3063-4c47-9618-3182c90e3ec4",
     "showTitle": false,
     "startTime": 1792281482472,
     "submitTime": 1792281481589,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-> workspace.default.gold_dp_cnpj_estabelecimentos_360 gravada.\n"
     ]
    }
   ],
   "source": [
    "# Databricks notebook source\n",
    "# COMMAND ----------\n",
    "from pyspark.sql import functions as F\n",
    "\n",
    "dbutils.widgets.dropdown(\"env\", \"homolog\", [\"dev\", \"homolog\", \"prod\"], \"Ambiente Alvo\")\n",
    "ENV = dbutils.widgets.get(\"env\")\n",
    "\n",
    "SCHEMA_SILVER = f\"workspace.{ENV}_silver\"\n",
    "SCHEMA_GOLD = f\"workspace.{ENV}_gold\"\n",
    "spark.sql(f\"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}\")\n",
    "\n",
    "print(f\"=== [GOLD] PRODUTO 1: ESTABELECIMENTOS 360 FULL [{ENV.upper()}] ===\")\n",
    "\n",
    "df_est = spark.table(f\"{SCHEMA_SILVER}.silver_estabelecimentos\")\n",
    "df_emp = spark.table(f\"{SCHEMA_SILVER}.silver_empresas\")\n",
    "df_smp = spark.table(f\"{SCHEMA_SILVER}.silver_simples_nacional\")\n",
    "df_mun = spark.table(f\"{SCHEMA_SILVER}.silver_dominio_municipios\")\n",
    "df_nat = spark.table(f\"{SCHEMA_SILVER}.silver_dominio_naturezas_juridicas\")\n",
    "df_cna = spark.table(f\"{SCHEMA_SILVER}.silver_dominio_cnaes\")\n",
    "\n",
    "df_gold_360 = (\n",
    "    df_est.alias(\"est\")\n",
    "    .join(df_emp.alias(\"emp\"), \"nr_cnpj_basico\", \"inner\")\n",
    "    .join(df_smp.alias(\"smp\"), \"nr_cnpj_basico\", \"left\")\n",
    "    .join(df_mun.alias(\"mun\"), F.col(\"est.cd_municipio\") == F.col(\"mun.cd_municipio\"), \"left\")\n",
    "    .join(df_nat.alias(\"nat\"), F.col(\"emp.cd_natureza_juridica\") == F.col(\"nat.cd_natureza_juridica\"), \"left\")\n",
    "    .join(df_cna.alias(\"cna\"), F.col(\"est.cd_cnae_fiscal_principal\") == F.col(\"cna.cd_cnae\"), \"left\")\n",
    "    .select(\n",
    "        F.col(\"est.nr_cnpj_completo\").alias(\"nr_cnpj\"),\n",
    "        F.col(\"est.nr_cnpj_basico\"),\n",
    "        F.col(\"emp.nm_razao_social\"),\n",
    "        F.col(\"est.nm_fantasia\"),\n",
    "        F.when(F.col(\"est.cd_identificador_matriz_filial\") == 1, \"MATRIZ\").otherwise(\"FILIAL\").alias(\"tp_estabelecimento\"),\n",
    "        F.when(F.col(\"est.cd_situacao_cadastral\") == \"02\", \"ATIVA\").otherwise(\"INATIVA\").alias(\"ds_situacao_cadastral\"),\n",
    "        F.col(\"est.dt_inicio_atividade\"),\n",
    "        F.col(\"emp.vl_capital_social\"),\n",
    "        F.col(\"nat.ds_natureza_juridica\"),\n",
    "        F.when(F.col(\"smp.fl_opcao_simples\") == \"S\", True).otherwise(False).alias(\"fl_simples_nacional_ativo\"),\n",
    "        F.when(F.col(\"smp.fl_opcao_mei\") == \"S\", True).otherwise(False).alias(\"fl_mei_ativo\"),\n",
    "        F.col(\"est.sg_uf\"),\n",
    "        F.col(\"mun.ds_municipio\").alias(\"nm_municipio\"),\n",
    "        F.current_timestamp().alias(\"dt_carga_gold\")\n",
    "    )\n",
    ")\n",
    "\n",
    "(\n",
    "    df_gold_360.write\n",
    "    .format(\"delta\")\n",
    "    .mode(\"overwrite\")\n",
    "    .option(\"overwriteSchema\", \"true\")\n",
    "    .partitionBy(\"sg_uf\")\n",
    "    .saveAsTable(f\"{SCHEMA_GOLD}.gold_dp_cnpj_estabelecimentos_360\")\n",
    ")\n",
    "print(f\"-> Tabela criada: {SCHEMA_GOLD}.gold_dp_cnpj_estabelecimentos_360\")"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "6"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "3.Gold Produto 1: gold_dp_cnpj_estabelecimentos_360 (Visão Cadastral 360°)",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
