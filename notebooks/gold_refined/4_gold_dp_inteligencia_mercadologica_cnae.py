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
     "finishTime": 1792292225251,
     "inputWidgets": {},
     "nuid": "2ff1c31a-8e4a-4f02-b8a2-7e59a44566cc",
     "showTitle": false,
     "startTime": 1792292143514,
     "submitTime": 1792292141219,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-> workspace.default.gold_dp_inteligencia_mercadologica_cnae gravada com sucesso.\n"
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
    "print(f\"=== [GOLD] PRODUTO 4: INTELIGÊNCIA MERCADOLÓGICA CNAE FULL [{ENV.upper()}] ===\")\n",
    "\n",
    "df_est = spark.table(f\"{SCHEMA_SILVER}.silver_estabelecimentos\")\n",
    "df_cna = spark.table(f\"{SCHEMA_SILVER}.silver_dominio_cnaes\")\n",
    "df_mun = spark.table(f\"{SCHEMA_SILVER}.silver_dominio_municipios\")\n",
    "\n",
    "df_gold_cnae = (\n",
    "    df_est.alias(\"est\")\n",
    "    .join(df_cna.alias(\"cna\"), F.col(\"est.cd_cnae_fiscal_principal\") == F.col(\"cna.cd_cnae\"), \"left\")\n",
    "    .join(df_mun.alias(\"mun\"), F.col(\"est.cd_municipio\") == F.col(\"mun.cd_municipio\"), \"left\")\n",
    "    .groupBy(\n",
    "        F.col(\"est.sg_uf\"),\n",
    "        F.col(\"mun.ds_municipio\").alias(\"nm_municipio\"),\n",
    "        F.col(\"est.cd_cnae_fiscal_principal\").alias(\"cd_cnae\"),\n",
    "        F.col(\"cna.ds_cnae\")\n",
    "    )\n",
    "    .agg(\n",
    "        F.count(\"est.nr_cnpj_completo\").alias(\"qt_total_estabelecimentos\"),\n",
    "        F.count(F.when(F.col(\"est.cd_situacao_cadastral\") == \"02\", 1)).alias(\"qt_estabelecimentos_ativos\"),\n",
    "        F.count(F.when(F.col(\"est.cd_identificador_matriz_filial\") == 1, 1)).alias(\"qt_matrizes\")\n",
    "    )\n",
    "    .withColumn(\"dt_carga_gold\", F.current_timestamp())\n",
    ")\n",
    "\n",
    "(\n",
    "    df_gold_cnae.write\n",
    "    .format(\"delta\")\n",
    "    .mode(\"overwrite\")\n",
    "    .option(\"overwriteSchema\", \"true\")\n",
    "    .partitionBy(\"sg_uf\")\n",
    "    .saveAsTable(f\"{SCHEMA_GOLD}.gold_dp_inteligencia_mercadologica_cnae\")\n",
    ")\n",
    "print(f\"-> Tabela criada: {SCHEMA_GOLD}.gold_dp_inteligencia_mercadologica_cnae\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
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
   "notebookName": "3 Produto 4: 1. gold_dp_inteligencia_mercadologica_cnae (Inteligência Geoeconómica)",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
