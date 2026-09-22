# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Camada Bronze: ingestao dos eventos crus
# MAGIC
# MAGIC Le os arquivos JSON produzidos pelo gerador de eventos e grava na tabela
# MAGIC Delta `bronze.events`, sem nenhuma regra de negocio.
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Origem | `${data_root}/streaming` (arquivos JSON Lines) |
# MAGIC | Destino | `${delta_root}/bronze/events` |
# MAGIC | Checkpoint | `${delta_root}/checkpoints/bronze` |
# MAGIC | Modo | `append` |
# MAGIC
# MAGIC > **Producao:** troque `.format("json")` por `.format("cloudFiles")`
# MAGIC > (Auto Loader) para usar notificacao de eventos em vez de listagem de
# MAGIC > diretorio. O restante do codigo nao muda.

# COMMAND ----------

# MAGIC %md ## Setup
# MAGIC Torna o pacote `src` importavel quando o notebook roda dentro de um Databricks Repo.

# COMMAND ----------

import os
import sys

# Em Databricks Repos, o notebook fica em `<repo>/notebooks/`; subimos um nivel.
REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

# MAGIC %md ## Parametros
# MAGIC Os widgets permitem reaproveitar o notebook em diferentes ambientes e
# MAGIC sao os mesmos parametros aceitos pelo job do Databricks Workflows.

# COMMAND ----------

dbutils.widgets.text("data_root", "dbfs:/FileStore/sports_pipeline", "Raiz dos dados de origem")
dbutils.widgets.text("delta_root", "dbfs:/delta", "Raiz das tabelas Delta")
dbutils.widgets.dropdown("trigger_mode", "continuous", ["continuous", "once"], "Modo de execucao")

# As variaveis de ambiente sao lidas por `src.config` no momento do import.
os.environ["SPORTS_DATA_ROOT"] = dbutils.widgets.get("data_root")
os.environ["SPORTS_DELTA_ROOT"] = dbutils.widgets.get("delta_root")
RUN_ONCE = dbutils.widgets.get("trigger_mode") == "once"

# COMMAND ----------

from src import config
from src.bronze.ingest_bronze import add_ingestion_metadata, read_raw_stream, write_bronze
from src.utils.spark_session import configure_logging

configure_logging()

print(f"Origem     : {config.STREAMING_PATH}")
print(f"Destino    : {config.BRONZE_PATH}")
print(f"Checkpoint : {config.BRONZE_CHECKPOINT}")

# COMMAND ----------

# MAGIC %md ## Ingestao
# MAGIC No Databricks a SparkSession (`spark`) ja existe, entao usamos direto.

# COMMAND ----------

raw_df = read_raw_stream(spark, config.STREAMING_PATH)
bronze_df = add_ingestion_metadata(raw_df)

display(bronze_df.printSchema())

# COMMAND ----------

query = write_bronze(bronze_df, config.BRONZE_PATH, config.BRONZE_CHECKPOINT, once=RUN_ONCE)

if RUN_ONCE:
    # `availableNow`: processa o backlog e encerra - ideal para jobs agendados.
    query.awaitTermination()
    print("Backlog processado.")
else:
    print(f"Query `{query.name}` em execucao. Use o painel de streaming para acompanhar.")

# COMMAND ----------

# MAGIC %md ## Validacao rapida

# COMMAND ----------

bronze_table = spark.read.format("delta").load(config.BRONZE_PATH)
print(f"Linhas na Bronze: {bronze_table.count():,}")
display(bronze_table.orderBy("ingestion_timestamp", ascending=False).limit(20))

# COMMAND ----------

# MAGIC %md ### Historico da tabela Delta (time travel e auditoria)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY delta.`{config.BRONZE_PATH}`").limit(10))
