"""
Camada Gold - KPIs em tempo real sobre janelas de 5 minutos.

Tres KPIs sao materializados:

* ``audience_by_window`` - audiencia media, pico e minimo por partida.
  Como cada evento carrega a audiencia daquele instante, a media dentro da
  janela e exatamente a "audiencia media por minuto" do periodo.
* ``top_players`` - os N jogadores com mais eventos em cada janela.
* ``goals_by_team`` - gols por time em cada janela.

**Por que `foreachBatch` + MERGE e nao um `append` direto?**
Uma agregacao com janela em modo `append` so emite a linha quando a janela
fecha (isto e, apos o watermark) - o painel ficaria minutos atrasado. Em modo
`update` o Spark reemite a janela a cada micro-batch, e o MERGE sobrescreve a
versao anterior daquela janela. O resultado e uma tabela Delta sempre
atualizada, consultavel pelo Databricks SQL a qualquer momento.

Execucao::

    python -m src.gold.aggregate_gold
    python -m src.gold.aggregate_gold --once
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable, Sequence

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from src import config
from src.utils.spark_session import configure_logging, get_spark

LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def with_event_time_watermark(df: DataFrame, delay: str = config.WATERMARK_DELAY) -> DataFrame:
    """Aplica o watermark apenas quando o DataFrame e de streaming.

    Em batch (nos testes, por exemplo) o watermark nao tem efeito algum, e
    manter a chamada condicional deixa as funcoes de agregacao utilizaveis nos
    dois modos sem nenhuma ramificacao extra.
    """
    return df.withWatermark("event_timestamp", delay) if df.isStreaming else df


def _flatten_window(df: DataFrame) -> DataFrame:
    """Converte a struct `window` em duas colunas planas.

    Colunas planas sao muito mais praticas para o Databricks SQL e para as
    chaves do MERGE do que um struct aninhado.
    """
    return (
        df.withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )


def _event_flag(event_type: str) -> Column:
    """1 quando o evento e do tipo informado, 0 caso contrario."""
    return F.when(F.col("event_type") == event_type, F.lit(1)).otherwise(F.lit(0))


# ---------------------------------------------------------------------------
# KPIs (funcoes puras: DataFrame -> DataFrame)
# ---------------------------------------------------------------------------


def audience_kpis(df: DataFrame, window_duration: str = config.WINDOW_DURATION) -> DataFrame:
    """Audiencia media, pico e minimo por partida em cada janela."""
    return _flatten_window(
        with_event_time_watermark(df)
        .groupBy(F.window(F.col("event_timestamp"), window_duration), F.col("match_id"))
        .agg(
            F.round(F.avg("audience_count"), 2).alias("avg_audience_count"),
            F.max("audience_count").alias("peak_audience_count"),
            F.min("audience_count").alias("min_audience_count"),
            F.count(F.lit(1)).alias("total_events"),
            # `countDistinct` nao e suportado em agregacoes de streaming;
            # `approx_count_distinct` e a alternativa valida (HyperLogLog++).
            F.approx_count_distinct("minute").alias("match_minutes_covered"),
            F.sum(_event_flag("gol")).alias("goals"),
        )
    ).select(
        "window_start",
        "window_end",
        "match_id",
        "avg_audience_count",
        "peak_audience_count",
        "min_audience_count",
        "total_events",
        "match_minutes_covered",
        "goals",
    )


def player_event_kpis(df: DataFrame, window_duration: str = config.WINDOW_DURATION) -> DataFrame:
    """Contagem de eventos por jogador em cada janela.

    Tabela de apoio: o ranking top N e derivado dela em
    :func:`rank_top_players`.
    """
    return _flatten_window(
        with_event_time_watermark(df)
        .groupBy(
            F.window(F.col("event_timestamp"), window_duration),
            F.col("match_id"),
            F.col("team"),
            F.col("player_name"),
        )
        .agg(
            F.count(F.lit(1)).alias("total_events"),
            F.sum(_event_flag("gol")).alias("goals"),
            F.sum(_event_flag("cartao")).alias("cards"),
            F.sum(_event_flag("finalizacao")).alias("shots"),
        )
    ).select(
        "window_start",
        "window_end",
        "match_id",
        "team",
        "player_name",
        "total_events",
        "goals",
        "cards",
        "shots",
    )


def rank_top_players(player_stats: DataFrame, top_n: int = config.TOP_N_PLAYERS) -> DataFrame:
    """Seleciona os N jogadores com mais eventos por janela e partida.

    Usamos `row_number` (e nao `rank`) para garantir exatamente N linhas
    mesmo com empates, e desempatamos por gols e depois pelo nome - assim o
    resultado e deterministico e os testes sao estaveis.
    """
    ranking_window = Window.partitionBy("window_start", "match_id").orderBy(
        F.col("total_events").desc(), F.col("goals").desc(), F.col("player_name").asc()
    )
    return (
        player_stats.withColumn("rank_position", F.row_number().over(ranking_window))
        .filter(F.col("rank_position") <= top_n)
        .select(
            "window_start",
            "window_end",
            "match_id",
            "rank_position",
            "player_name",
            "team",
            "total_events",
            "goals",
            "cards",
            "shots",
        )
    )


def team_goal_kpis(df: DataFrame, window_duration: str = config.WINDOW_DURATION) -> DataFrame:
    """Gols (e demais eventos) por time em cada janela."""
    return _flatten_window(
        with_event_time_watermark(df)
        .groupBy(
            F.window(F.col("event_timestamp"), window_duration),
            F.col("match_id"),
            F.col("team"),
        )
        .agg(
            F.sum(_event_flag("gol")).alias("goals"),
            F.sum(_event_flag("cartao")).alias("cards"),
            F.sum(_event_flag("substituicao")).alias("substitutions"),
            F.sum(_event_flag("finalizacao")).alias("shots"),
            F.count(F.lit(1)).alias("total_events"),
        )
    ).select(
        "window_start",
        "window_end",
        "match_id",
        "team",
        "goals",
        "cards",
        "substitutions",
        "shots",
        "total_events",
    )


# ---------------------------------------------------------------------------
# Escrita idempotente (upsert) em Delta
# ---------------------------------------------------------------------------


def upsert_to_delta(
    batch_df: DataFrame,
    target_path: str,
    keys: Sequence[str],
    partition_by: Sequence[str] | None = None,
) -> None:
    """Faz MERGE do micro-batch na tabela Delta de destino.

    Se a tabela ainda nao existe, cria-a com um append simples. A partir dai,
    cada janela reprocessada sobrescreve a versao anterior de si mesma.
    """
    from delta.tables import DeltaTable  # import tardio: so e necessario em runtime

    spark = batch_df.sparkSession

    if not DeltaTable.isDeltaTable(spark, target_path):
        LOGGER.info("Criando a tabela Gold %s", target_path)
        writer = batch_df.write.format("delta").mode("append")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(target_path)
        return

    condition = " AND ".join(f"target.{key} <=> source.{key}" for key in keys)
    (
        DeltaTable.forPath(spark, target_path)
        .alias("target")
        .merge(batch_df.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _audience_batch_writer(batch_df: DataFrame, batch_id: int) -> None:
    """Persiste o KPI de audiencia da janela."""
    upsert_to_delta(batch_df, config.GOLD_AUDIENCE_PATH, keys=("window_start", "match_id"))


def _team_goals_batch_writer(batch_df: DataFrame, batch_id: int) -> None:
    """Persiste o KPI de gols por time."""
    upsert_to_delta(batch_df, config.GOLD_TEAM_GOALS_PATH, keys=("window_start", "match_id", "team"))


def _players_batch_writer(batch_df: DataFrame, batch_id: int) -> None:
    """Persiste as estatisticas por jogador e recalcula o top N.

    O ranking **nao** pode ser calculado apenas sobre o micro-batch: ele
    contem somente os jogadores que tiveram eventos agora, e um jogador que
    lidera mas ficou parado neste intervalo sairia indevidamente do top N.

    Por isso o fluxo tem dois passos:
        1. o micro-batch e mesclado na tabela de apoio `player_events`;
        2. o ranking e recalculado lendo essa tabela, restrita as janelas
           tocadas pelo batch (semi-join, sem `collect()` no driver).
    """
    spark = batch_df.sparkSession
    batch_df.persist()

    try:
        # `team` faz parte da chave: dois jogadores homonimos em times
        # diferentes gerariam duas linhas de origem para a mesma chave, e o
        # MERGE do Delta falha quando isso acontece.
        upsert_to_delta(
            batch_df,
            config.GOLD_PLAYER_EVENTS_PATH,
            keys=("window_start", "match_id", "team", "player_name"),
        )

        affected_windows = batch_df.select("window_start", "match_id").distinct()
        # `left_semi` filtra a tabela completa pelas janelas afetadas sem
        # trazer nada para o driver e sem duplicar linhas.
        touched_stats = (
            spark.read.format("delta")
            .load(config.GOLD_PLAYER_EVENTS_PATH)
            .join(affected_windows, on=["window_start", "match_id"], how="left_semi")
        )

        top_players = rank_top_players(touched_stats)
        upsert_to_delta(
            top_players,
            config.GOLD_TOP_PLAYERS_PATH,
            keys=("window_start", "match_id", "rank_position"),
        )
    finally:
        batch_df.unpersist()


# ---------------------------------------------------------------------------
# Orquestracao das queries
# ---------------------------------------------------------------------------


def read_silver_stream(spark: SparkSession, source_path: str = config.SILVER_PATH) -> DataFrame:
    """Le a tabela Delta da camada Silver como stream."""
    LOGGER.info("Lendo stream da camada Silver em %s", source_path)
    return spark.readStream.format("delta").option("ignoreChanges", "true").load(source_path)


def _start_query(
    df: DataFrame,
    writer_fn: Callable[[DataFrame, int], None],
    checkpoint_path: str,
    query_name: str,
    once: bool,
    trigger_interval: str = config.TRIGGER_INTERVAL,
) -> StreamingQuery:
    """Inicia uma query de agregacao em modo `update` com `foreachBatch`."""
    writer = (
        df.writeStream.foreachBatch(writer_fn)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .queryName(query_name)
    )
    writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime=trigger_interval)
    LOGGER.info("Iniciando a query Gold `%s` (checkpoint: %s)", query_name, checkpoint_path)
    return writer.start()


def run(once: bool = False, await_termination: bool = True) -> list[StreamingQuery]:
    """Inicia as tres queries de KPI da camada Gold.

    As tres leem a mesma tabela Silver mas mantem checkpoints separados: uma
    falha (ou uma mudanca de regra) em um KPI nao obriga a reprocessar os outros.
    """
    spark = get_spark("gold-aggregation")
    silver_stream = read_silver_stream(spark)

    queries = [
        _start_query(
            audience_kpis(silver_stream),
            _audience_batch_writer,
            config.GOLD_AUDIENCE_CHECKPOINT,
            "gold_audience_kpis",
            once,
        ),
        _start_query(
            player_event_kpis(silver_stream),
            _players_batch_writer,
            config.GOLD_PLAYERS_CHECKPOINT,
            "gold_top_players",
            once,
        ),
        _start_query(
            team_goal_kpis(silver_stream),
            _team_goals_batch_writer,
            config.GOLD_TEAM_GOALS_CHECKPOINT,
            "gold_team_goals",
            once,
        ),
    ]

    if await_termination:
        try:
            for query in queries:
                query.awaitTermination()
        except KeyboardInterrupt:  # pragma: no cover - interacao manual
            LOGGER.info("Interrompido pelo usuario - parando as queries com seguranca.")
            for query in queries:
                query.stop()

    return queries


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Agregacao de KPIs da camada Gold.")
    parser.add_argument("--once", action="store_true", help="Processa o backlog disponivel e encerra.")
    args = parser.parse_args(argv)

    try:
        run(once=args.once)
    except Exception:
        LOGGER.exception("Falha na agregacao da camada Gold.")
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
