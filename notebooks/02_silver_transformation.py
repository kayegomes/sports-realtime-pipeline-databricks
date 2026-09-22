# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Camada Silver: limpeza, padronizacao e qualidade
# MAGIC
# MAGIC Le a Bronze em streaming, aplica limpeza e regras de qualidade e grava
# MAGIC em duas tabelas:
# MAGIC
# MAGIC | Tabela | Conteudo |
# MAGIC |---|---|
# MAGIC | `silver/events_clean` | eventos aprovados em todas as regras |
# MAGIC | `silver/events_quarantine` | eventos rejeitados + motivo (`quality_errors`) |
# MAGIC
# MAGIC Nada e descartado silenciosamente: todo registro rejeitado fica auditavel.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

dbutils.widgets.text("delta_root", "dbfs:/delta", "Raiz das tabelas Delta")
dbutils.widgets.dropdown("trigger_mode", "continuous", ["continuous", "once"], "Modo de execucao")

os.environ["SPORTS_DELTA_ROOT"] = dbutils.widgets.get("delta_root")
RUN_ONCE = dbutils.widgets.get("trigger_mode") == "once"

# COMMAND ----------

from pyspark.sql import functions as F

from src import config
from src.silver.transform_silver import read_bronze_stream, silver_expectations, transform_silver, write_silver
from src.utils.quality_checks import quality_summary
from src.utils.spark_session import configure_logging

configure_logging()

print(f"Origem     : {config.BRONZE_PATH}")
print(f"Destino    : {config.SILVER_PATH}")
print(f"Quarentena : {config.SILVER_QUARANTINE_PATH}")

# COMMAND ----------

# MAGIC %md ## Ensaio em batch
# MAGIC Antes de ligar o streaming, rodamos a mesma transformacao sobre um
# MAGIC snapshot batch da Bronze. Como `transform_silver` e uma funcao pura, o
# MAGIC resultado e identico ao que a query de streaming vai produzir.

# COMMAND ----------

bronze_snapshot = spark.read.format("delta").load(config.BRONZE_PATH).limit(5000)
clean_preview, quarantine_preview = transform_silver(bronze_snapshot)

print(f"Aprovados  : {clean_preview.count():,}")
print(f"Rejeitados : {quarantine_preview.count():,}")
display(clean_preview.limit(20))

# COMMAND ----------

# MAGIC %md ### Motivos de rejeicao

# COMMAND ----------

display(
    quarantine_preview.select(F.explode("quality_errors").alias("regra"))
    .groupBy("regra")
    .count()
    .orderBy(F.col("count").desc())
)

# COMMAND ----------

# MAGIC %md ### Placar das expectativas

# COMMAND ----------

from src.silver.transform_silver import parse_event_timestamp

display(quality_summary(parse_event_timestamp(bronze_snapshot), silver_expectations()))

# COMMAND ----------

# MAGIC %md ## Streaming

# COMMAND ----------

bronze_stream = read_bronze_stream(spark, config.BRONZE_PATH)
query = write_silver(bronze_stream, config.SILVER_CHECKPOINT, once=RUN_ONCE)

if RUN_ONCE:
    query.awaitTermination()
    print("Backlog processado.")
else:
    print(f"Query `{query.name}` em execucao.")

# COMMAND ----------

# MAGIC %md ## Validacao

# COMMAND ----------

silver_table = spark.read.format("delta").load(config.SILVER_PATH)
print(f"Linhas na Silver: {silver_table.count():,}")
display(silver_table.orderBy("event_timestamp", ascending=False).limit(20))

# COMMAND ----------

# MAGIC %md ### Padronizacao de times
# MAGIC A lista abaixo deve conter apenas nomes canonicos.

# COMMAND ----------

display(silver_table.groupBy("team").count().orderBy(F.col("count").desc()))

# COMMAND ----------

# MAGIC %md ## Manutencao da tabela
# MAGIC Em streaming, cada micro-batch gera arquivos pequenos. `OPTIMIZE` com
# MAGIC `ZORDER` compacta e melhora o pruning das consultas por partida.

# COMMAND ----------

spark.sql(f"OPTIMIZE delta.`{config.SILVER_PATH}` ZORDER BY (match_id, event_timestamp)")
