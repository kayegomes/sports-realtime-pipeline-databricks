# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Camada Gold: KPIs em tempo real
# MAGIC
# MAGIC Agrega a Silver em janelas de 5 minutos e materializa tres tabelas
# MAGIC prontas para o Databricks SQL:
# MAGIC
# MAGIC | Tabela | KPI |
# MAGIC |---|---|
# MAGIC | `gold/kpis_realtime/audience_by_window` | audiencia media, pico e minimo |
# MAGIC | `gold/kpis_realtime/top_players` | top 3 jogadores por eventos |
# MAGIC | `gold/kpis_realtime/goals_by_team` | gols por time |
# MAGIC
# MAGIC As queries rodam em modo `update` com `foreachBatch` + `MERGE`: cada
# MAGIC janela e sobrescrita a cada micro-batch, entao o painel reflete o estado
# MAGIC atual sem esperar a janela fechar.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# COMMAND ----------

dbutils.widgets.text("delta_root", "dbfs:/delta", "Raiz das tabelas Delta")
dbutils.widgets.text("window_duration", "5 minutes", "Tamanho da janela")
dbutils.widgets.text("top_n_players", "3", "Jogadores no ranking")
dbutils.widgets.dropdown("trigger_mode", "continuous", ["continuous", "once"], "Modo de execucao")

os.environ["SPORTS_DELTA_ROOT"] = dbutils.widgets.get("delta_root")
os.environ["SPORTS_WINDOW_DURATION"] = dbutils.widgets.get("window_duration")
os.environ["SPORTS_TOP_N_PLAYERS"] = dbutils.widgets.get("top_n_players")
RUN_ONCE = dbutils.widgets.get("trigger_mode") == "once"

# COMMAND ----------

from pyspark.sql import functions as F

from src import config
from src.gold.aggregate_gold import (
    audience_kpis,
    player_event_kpis,
    rank_top_players,
    read_silver_stream,
    run,
    team_goal_kpis,
)
from src.utils.spark_session import configure_logging

configure_logging()

print(f"Origem : {config.SILVER_PATH}")
print(f"Destino: {config.GOLD_ROOT}")
print(f"Janela : {config.WINDOW_DURATION} | Top N: {config.TOP_N_PLAYERS}")

# COMMAND ----------

# MAGIC %md ## Ensaio em batch
# MAGIC As funcoes de KPI funcionam igual em batch e em streaming, o que
# MAGIC permite conferir o resultado antes de ligar as queries.

# COMMAND ----------

silver_snapshot = spark.read.format("delta").load(config.SILVER_PATH)

display(audience_kpis(silver_snapshot).orderBy(F.col("window_start").desc()).limit(20))

# COMMAND ----------

display(
    rank_top_players(player_event_kpis(silver_snapshot))
    .orderBy(F.col("window_start").desc(), "match_id", "rank_position")
    .limit(30)
)

# COMMAND ----------

display(team_goal_kpis(silver_snapshot).orderBy(F.col("goals").desc()).limit(20))

# COMMAND ----------

# MAGIC %md ## Streaming
# MAGIC `run()` inicia as tres queries, cada uma com seu proprio checkpoint.

# COMMAND ----------

queries = run(once=RUN_ONCE, await_termination=RUN_ONCE)

for query in queries:
    print(f"{query.name}: {'concluida' if RUN_ONCE else 'em execucao'}")

# COMMAND ----------

# MAGIC %md ## Registro no metastore
# MAGIC Com as tabelas registradas, o Databricks SQL as enxerga e o dashboard
# MAGIC pode consulta-las por nome.

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS sports_gold")

for table_name, path in {
    "audience_by_window": config.GOLD_AUDIENCE_PATH,
    "top_players": config.GOLD_TOP_PLAYERS_PATH,
    "goals_by_team": config.GOLD_TEAM_GOALS_PATH,
    "player_events": config.GOLD_PLAYER_EVENTS_PATH,
}.items():
    spark.sql(f"CREATE TABLE IF NOT EXISTS sports_gold.{table_name} USING DELTA LOCATION '{path}'")

display(spark.sql("SHOW TABLES IN sports_gold"))

# COMMAND ----------

# MAGIC %md ## Consultas do dashboard
# MAGIC Cole estas queries no Databricks SQL para montar o painel.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Audiencia media por janela (grafico de linha)
# MAGIC SELECT window_start, match_id, avg_audience_count, peak_audience_count
# MAGIC FROM sports_gold.audience_by_window
# MAGIC WHERE window_start >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC ORDER BY window_start DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 3 jogadores da janela mais recente (tabela)
# MAGIC WITH ultima_janela AS (
# MAGIC   SELECT max(window_start) AS ws FROM sports_gold.top_players
# MAGIC )
# MAGIC SELECT match_id, rank_position, player_name, team, total_events, goals
# MAGIC FROM sports_gold.top_players
# MAGIC WHERE window_start = (SELECT ws FROM ultima_janela)
# MAGIC ORDER BY match_id, rank_position;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gols por time na ultima hora (grafico de barras)
# MAGIC SELECT team, sum(goals) AS gols
# MAGIC FROM sports_gold.goals_by_team
# MAGIC WHERE window_start >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC GROUP BY team
# MAGIC ORDER BY gols DESC;
