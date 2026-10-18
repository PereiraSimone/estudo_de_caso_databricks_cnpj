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
     "finishTime": 1792283979977,
     "inputWidgets": {},
     "nuid": "963e1e68-c533-4f48-a421-1d07715ff3be",
     "showTitle": false,
     "startTime": 1792283974338,
     "submitTime": 1792283974271,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-> workspace.default.gold_dp_governo_entes_publicos gravada com sucesso.\n"
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
    "print(f\"=== [GOLD] PRODUTO 6: GOVERNO E ENTES PÚBLICOS FULL [{ENV.upper()}] ===\")\n",
    "\n",
    "df_est = spark.table(f\"{SCHEMA_SILVER}.silver_estabelecimentos\")\n",
    "df_emp = spark.table(f\"{SCHEMA_SILVER}.silver_empresas\")\n",
    "df_nat = spark.table(f\"{SCHEMA_SILVER}.silver_dominio_naturezas_juridicas\")\n",
    "\n",
    "df_governo = (\n",
    "    df_est.alias(\"est\")\n",
    "    .join(df_emp.alias(\"emp\"), \"nr_cnpj_basico\", \"inner\")\n",
    "    .join(df_nat.alias(\"nat\"), F.col(\"emp.cd_natureza_juridica\") == F.col(\"nat.cd_natureza_juridica\"), \"inner\")\n",
    "    .filter(F.col(\"emp.cd_natureza_juridica\").cast(\"string\").startswith(\"1\"))\n",
    "    .select(\n",
    "        F.col(\"est.nr_cnpj_completo\").alias(\"nr_cnpj\"),\n",
    "        F.col(\"est.nr_cnpj_basico\"),\n",
    "        F.col(\"emp.nm_razao_social\"),\n",
    "        F.col(\"est.nm_fantasia\"),\n",
    "        F.col(\"emp.cd_natureza_juridica\"),\n",
    "        F.col(\"nat.ds_natureza_juridica\"),\n",
    "        F.col(\"emp.ds_ente_federativo_responsavel\"),\n",
    "        F.col(\"est.sg_uf\"),\n",
    "        F.col(\"est.cd_municipio\"),\n",
    "        F.current_timestamp().alias(\"dt_carga_gold\")\n",
    "    )\n",
    ")\n",
    "\n",
    "(\n",
    "    df_governo.write\n",
    "    .format(\"delta\")\n",
    "    .mode(\"overwrite\")\n",
    "    .option(\"overwriteSchema\", \"true\")\n",
    "    .partitionBy(\"sg_uf\")\n",
    "    .saveAsTable(f\"{SCHEMA_GOLD}.gold_dp_governo_entes_publicos\")\n",
    ")\n",
    "print(f\"-> Tabela criada: {SCHEMA_GOLD}.gold_dp_governo_entes_publicos\")"
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
   "notebookName": "3. gold_dp_governo_entes_publicos (Administração Pública e B2G)",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
