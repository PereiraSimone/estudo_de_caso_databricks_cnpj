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
     "finishTime": 1792324700963,
     "inputWidgets": {},
     "nuid": "d445f90e-94d1-4768-9799-43c7ea20d0a3",
     "showTitle": false,
     "startTime": 1792324109643,
     "submitTime": 1792324107716,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "=== INICIANDO INGESTÃO RAW PARA O AMBIENTE: [PROD] ===\n",
      "Target Schema Raw: workspace.prod_raw\n",
      "Processando entidade: empresas a partir de /Volumes/workspace/default/cnpj_ingestion/bronze/empresas...\n",
      "-> Tabela workspace.prod_raw.raw_empresas criada com sucesso.\n",
      "Processando entidade: estabelecimentos a partir de /Volumes/workspace/default/cnpj_ingestion/bronze/estabelecimentos...\n",
      "-> Tabela workspace.prod_raw.raw_estabelecimentos criada com sucesso.\n",
      "Processando entidade: socios a partir de /Volumes/workspace/default/cnpj_ingestion/bronze/socios...\n",
      "-> Tabela workspace.prod_raw.raw_socios criada com sucesso.\n",
      "Processando tabelas_apoio...\n",
      "-> Tabela workspace.prod_raw.raw_tabelas_apoio criada com sucesso.\n",
      "=== CAMADA RAW CONCLUÍDA NO SCHEMA workspace.prod_raw ===\n"
     ]
    }
   ],
   "source": [
    "# Databricks notebook source\n",
    "# COMMAND ----------\n",
    "from pyspark.sql import functions as F\n",
    "\n",
    "# 1. Parâmetro do ambiente de execução\n",
    "dbutils.widgets.dropdown(\"env\", \"homolog\", [\"dev\", \"homolog\", \"prod\"], \"Ambiente Alvo\")\n",
    "ENV = dbutils.widgets.get(\"env\")\n",
    "\n",
    "# 2. Resolução dinâmica do esquema\n",
    "SCHEMA_RAW = f\"workspace.{ENV}_raw\"\n",
    "spark.sql(f\"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RAW}\")\n",
    "\n",
    "print(f\"=== [RAW] INICIANDO INGESTÃO INTEGRAL (FULL) NO ESQUEMA: {SCHEMA_RAW} ===\")\n",
    "\n",
    "BASE_VOLUME = \"/Volumes/workspace/default/cnpj_ingestion/bronze\"\n",
    "\n",
    "# COMMAND ----------\n",
    "# 3. Ingestão Full das tabelas estruturais\n",
    "entidades = [\"empresas\", \"estabelecimentos\", \"socios\"]\n",
    "\n",
    "for entidade in entidades:\n",
    "    caminho = f\"{BASE_VOLUME}/{entidade}\"\n",
    "    print(f\"A carregar partição Raw (FULL): {entidade}...\")\n",
    "    \n",
    "    (\n",
    "        spark.read.parquet(caminho)\n",
    "        .withColumn(\"_ingested_at\", F.current_timestamp())\n",
    "        .withColumn(\"_source_path\", F.col(\"_metadata.file_path\"))\n",
    "        .write\n",
    "        .format(\"delta\")\n",
    "        .mode(\"overwrite\")\n",
    "        .option(\"overwriteSchema\", \"true\")\n",
    "        .saveAsTable(f\"{SCHEMA_RAW}.raw_{entidade}\")\n",
    "    )\n",
    "    print(f\"-> Tabela criada: {SCHEMA_RAW}.raw_{entidade}\")\n",
    "\n",
    "# COMMAND ----------\n",
    "# 4. Ingestão Full das tabelas de apoio\n",
    "print(\"A carregar tabelas de apoio auxiliares (FULL)...\")\n",
    "caminho_apoio = f\"{BASE_VOLUME}/tabelas_apoio\"\n",
    "\n",
    "(\n",
    "    spark.read.parquet(caminho_apoio)\n",
    "    .withColumn(\"_ingested_at\", F.current_timestamp())\n",
    "    .withColumn(\"_source_path\", F.col(\"_metadata.file_path\"))\n",
    "    .write\n",
    "    .format(\"delta\")\n",
    "    .mode(\"overwrite\")\n",
    "    .option(\"overwriteSchema\", \"true\")\n",
    "    .saveAsTable(f\"{SCHEMA_RAW}.raw_tabelas_apoio\")\n",
    ")\n",
    "\n",
    "print(f\"-> Tabela criada: {SCHEMA_RAW}.raw_tabelas_apoio\")\n",
    "print(f\"=== [RAW] CONCLUÍDO COM SUCESSO: {SCHEMA_RAW} ===\")"
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
   "notebookName": "1. Ingestão Delta Soft Raw",
   "widgets": {
    "env": {
     "currentValue": "prod",
     "nuid": "472738b9-54d6-41cd-bf7c-8e35249a296e",
     "typedWidgetInfo": {
      "autoCreated": false,
      "defaultValue": "homolog",
      "dynamic": false,
      "label": "Ambiente",
      "name": "env",
      "options": {
       "choices": [
        "dev",
        "homolog",
        "prod"
       ],
       "fixedDomain": true,
       "multiselect": false,
       "widgetDisplayType": "Dropdown"
      },
      "parameterDataType": "String"
     },
     "widgetInfo": {
      "defaultValue": "homolog",
      "label": "Ambiente",
      "name": "env",
      "options": {
       "autoCreated": null,
       "choices": [
        "dev",
        "homolog",
        "prod"
       ],
       "widgetType": "dropdown"
      },
      "widgetType": "dropdown"
     }
    }
   }
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
