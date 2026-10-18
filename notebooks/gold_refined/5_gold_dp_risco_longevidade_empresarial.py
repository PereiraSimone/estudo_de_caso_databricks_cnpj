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
     "finishTime": 1792331905538,
     "inputWidgets": {},
     "nuid": "4e685561-042e-461d-a0b4-ddaae1f4a485",
     "showTitle": false,
     "startTime": 1792331835463,
     "submitTime": 1792331835394,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-> workspace.prod_gold.gold_dp_risco_longevidade_empresarial gravada com sucesso.\n"
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
    "print(f\"=== [GOLD] PRODUTO 5: RISCO E LONGEVIDADE FULL [{ENV.upper()}] ===\")\n",
    "\n",
    "df_est = spark.table(f\"{SCHEMA_SILVER}.silver_estabelecimentos\")\n",
    "df_emp = spark.table(f\"{SCHEMA_SILVER}.silver_empresas\")\n",
    "\n",
    "df_longevidade = (\n",
    "    df_est.alias(\"est\")\n",
    "    .join(df_emp.alias(\"emp\"), \"nr_cnpj_basico\", \"inner\")\n",
    "    .select(\n",
    "        F.col(\"est.nr_cnpj_completo\").alias(\"nr_cnpj\"),\n",
    "        F.col(\"est.nr_cnpj_basico\"),\n",
    "        F.col(\"emp.nm_razao_social\"),\n",
    "        F.col(\"est.dt_inicio_atividade\"),\n",
    "        F.col(\"est.dt_situacao_cadastral\"),\n",
    "        F.col(\"est.cd_situacao_cadastral\"),\n",
    "        F.col(\"est.cd_motivo_situacao_cadastral\"),\n",
    "        F.when(\n",
    "            F.col(\"est.cd_situacao_cadastral\") == \"02\",\n",
    "            F.round(F.datediff(F.current_date(), F.col(\"est.dt_inicio_atividade\")) / 365.25, 2)\n",
    "        ).otherwise(\n",
    "            F.round(F.datediff(F.col(\"est.dt_situacao_cadastral\"), F.col(\"est.dt_inicio_atividade\")) / 365.25, 2)\n",
    "        ).alias(\"nu_idade_empresa_anos\"),\n",
    "        F.when(\n",
    "            F.datediff(F.current_date(), F.col(\"est.dt_situacao_cadastral\")) <= 365,\n",
    "            True\n",
    "        ).otherwise(False).alias(\"fl_alteracao_situacao_recente\"),\n",
    "        F.col(\"emp.vl_capital_social\"),\n",
    "        F.col(\"est.sg_uf\"),\n",
    "        F.current_timestamp().alias(\"dt_carga_gold\")\n",
    "    )\n",
    ")\n",
    "\n",
    "(\n",
    "    df_longevidade.write\n",
    "    .format(\"delta\")\n",
    "    .mode(\"overwrite\")\n",
    "    .option(\"overwriteSchema\", \"true\")\n",
    "    .partitionBy(\"sg_uf\")\n",
    "    .saveAsTable(f\"{SCHEMA_GOLD}.gold_dp_risco_longevidade_empresarial\")\n",
    ")\n",
    "print(f\"-> Tabela criada: {SCHEMA_GOLD}.gold_dp_risco_longevidade_empresarial\")"
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
   "notebookName": "3.Produto 5: gold_dp_risco_longevidade_empresarial (Métricas de Sobrevivência e Solvência)",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
