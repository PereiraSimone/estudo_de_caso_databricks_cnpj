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
     "finishTime": 1792289053574,
     "inputWidgets": {},
     "nuid": "d35e5ea5-160d-4765-94cc-8ffe838c8648",
     "showTitle": false,
     "startTime": 1792288961838,
     "submitTime": 1792288961568,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-> workspace.default.gold_dp_score_saude_e_propensao_pj gravada com sucesso.\n"
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
    "SCHEMA_GOLD = f\"workspace.{ENV}_gold\"\n",
    "spark.sql(f\"CREATE SCHEMA IF NOT EXISTS {SCHEMA_GOLD}\")\n",
    "\n",
    "print(f\"=== [GOLD] PRODUTO 7: SCORE DE SAÚDE E PROPENSÃO PJ FULL [{ENV.upper()}] ===\")\n",
    "\n",
    "df_base = spark.table(f\"{SCHEMA_GOLD}.gold_dp_cnpj_estabelecimentos_360\")\n",
    "\n",
    "df_score = (\n",
    "    df_base.alias(\"b\")\n",
    "    .select(\n",
    "        F.col(\"b.nr_cnpj\"),\n",
    "        F.col(\"b.nr_cnpj_basico\"),\n",
    "        F.col(\"b.nm_razao_social\"),\n",
    "        F.when(F.col(\"b.ds_situacao_cadastral\") != \"ATIVA\", True).otherwise(False).alias(\"fl_risco_cadastral_elevado\"),\n",
    "        F.when(F.col(\"b.fl_mei_ativo\") == True, \"MEI\")\n",
    "         .when(F.col(\"b.vl_capital_social\") > 500000, \"CORPORATE\")\n",
    "         .otherwise(\"PYME\").alias(\"ds_segmento_operacional\"),\n",
    "        F.current_timestamp().alias(\"dt_carga_gold\")\n",
    "    )\n",
    ")\n",
    "\n",
    "(\n",
    "    df_score.write\n",
    "    .format(\"delta\")\n",
    "    .mode(\"overwrite\")\n",
    "    .option(\"overwriteSchema\", \"true\")\n",
    "    .saveAsTable(f\"{SCHEMA_GOLD}.gold_dp_score_saude_e_propensao_pj\")\n",
    ")\n",
    "print(f\"-> Tabela criada: {SCHEMA_GOLD}.gold_dp_score_saude_e_propensao_pj\")"
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
   "notebookName": "4. gold_dp_score_saude_e_propensao_pj (Produto Analítico Composto)",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
