"""
Camada Silver - limpeza, padronizacao, deduplicacao e validacao.

O que acontece aqui:

1. ``timestamp`` (string ISO) vira ``event_timestamp`` (tipo timestamp), que e
   a coluna de *event time* usada pelas janelas da camada Gold.
2. Textos sao normalizados: acentos removidos, espacos colapsados, caixa
   padronizada. Nomes de time passam por um mapa de-para (``Fla`` -> ``Flamengo``).
3. Duplicatas sao removidas por ``event_id`` dentro do watermark - o gerador
   reenvia eventos de proposito para simular entrega *at-least-once*.
4. As regras de qualidade sao avaliadas. Quem passa vai para
   ``events_clean``; quem falha vai para ``events_quarantine`` com o motivo
   da rejeicao. Nada e descartado silenciosamente.

Todas as transformacoes sao funcoes puras ``DataFrame -> DataFrame``, o que
permite testa-las com DataFrames estaticos, sem subir uma query de streaming.

Execucao::

    python -m src.silver.transform_silver
    python -m src.silver.transform_silver --once
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from src import config
from src.utils.quality_checks import Expectation, annotate_violations, split_by_quality
from src.utils.spark_session import configure_logging, get_spark

LOGGER = logging.getLogger(__name__)

#: Caracteres acentuados e seus equivalentes ASCII, para uso com `translate`.
_ACCENTED = "áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ"
_UNACCENTED = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"

#: Colunas finais da tabela Silver, na ordem.
SILVER_COLUMNS: tuple[str, ...] = (
    "event_id",
    "event_timestamp",
    "match_id",
    "minute",
    "event_type",
    "player_name",
    "team",
    "audience_count",
    "ingestion_timestamp",
    "processed_timestamp",
    "event_date",
)


# ---------------------------------------------------------------------------
# Normalizacao
# ---------------------------------------------------------------------------


def normalize_text(column: Column) -> Column:
    """Minusculas, sem acento, sem pontuacao e com espacos colapsados.

    Implementado so com funcoes nativas do Spark (sem UDF Python), portanto
    roda inteiramente na JVM e nao paga o custo de serializacao.
    """
    lowered = F.lower(F.translate(column, _ACCENTED, _UNACCENTED))
    without_punctuation = F.regexp_replace(lowered, r"[^a-z0-9 ]", " ")
    return F.trim(F.regexp_replace(without_punctuation, r"\s+", " "))


#: Formato ISO-8601 com offset, que e o que o gerador produz
#: (`2026-09-22T21:44:18.914286+00:00`). Os colchetes marcam secoes opcionais,
#: entao a mesma expressao cobre tambem `2026-09-22T21:44:18`.
_ISO_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS][XXX][X]"


def parse_event_timestamp(df: DataFrame) -> DataFrame:
    """Converte a string ISO em timestamp e deriva a data do evento.

    Tentamos primeiro o parser padrao e, se ele falhar, o formato ISO
    explicito - assim tanto `2026-09-22 15:00:00` quanto
    `2026-09-22T15:00:00.123456+00:00` sao aceitos. Strings realmente
    invalidas viram NULL, capturado depois pela expectativa
    `timestamp_valido`.
    """
    parsed = F.coalesce(
        F.to_timestamp(F.col("timestamp")),
        F.to_timestamp(F.col("timestamp"), _ISO_TIMESTAMP_FORMAT),
    )
    return df.withColumn("event_timestamp", parsed).withColumn(
        "event_date", F.to_date(F.col("event_timestamp"))
    )


def standardize_team_names(df: DataFrame, column: str = "team") -> DataFrame:
    """Aplica o mapa de-para de nomes de time.

    O mapa vira um literal `map<string,string>` embutido no plano de execucao
    (broadcast implicito), evitando qualquer join ou UDF. Nomes desconhecidos
    caem no fallback `initcap`, preservando o dado em vez de descarta-lo.
    """
    normalized = normalize_text(F.col(column))
    lookup = F.create_map([F.lit(item) for pair in config.TEAM_NAME_MAP.items() for item in pair])
    return df.withColumn(column, F.coalesce(F.element_at(lookup, normalized), F.initcap(normalized)))


def standardize_event_types(df: DataFrame, column: str = "event_type") -> DataFrame:
    """Padroniza o tipo de evento (`GOL!!` -> `gol`).

    A validacao contra a lista permitida acontece depois, nas expectativas.
    """
    return df.withColumn(column, normalize_text(F.col(column)))


def clean_player_names(df: DataFrame, column: str = "player_name") -> DataFrame:
    """Colapsa espacos e transforma strings em branco em NULL."""
    trimmed = F.trim(F.regexp_replace(F.col(column), r"\s+", " "))
    return df.withColumn(column, F.when(trimmed == "", None).otherwise(trimmed))


def deduplicate(df: DataFrame, keys: tuple[str, ...] = ("event_id",)) -> DataFrame:
    """Remove duplicatas por `event_id` dentro do watermark.

    O watermark e obrigatorio em streaming: sem ele, o Spark manteria todos os
    ids ja vistos em estado, para sempre. Com ele, o estado e descartado apos
    `WATERMARK_DELAY`. Em DataFrames batch o watermark nao se aplica e a
    deduplicacao e global.
    """
    if df.isStreaming:
        return df.withWatermark("event_timestamp", config.WATERMARK_DELAY).dropDuplicates(list(keys))
    return df.dropDuplicates(list(keys))


def add_processing_metadata(df: DataFrame) -> DataFrame:
    """Marca o instante em que a linha foi processada pela camada Silver."""
    return df.withColumn("processed_timestamp", F.current_timestamp())


# ---------------------------------------------------------------------------
# Regras de qualidade
# ---------------------------------------------------------------------------


def silver_expectations() -> list[Expectation]:
    """Regras de qualidade da camada Silver.

    Cada regra corresponde a um defeito que o gerador injeta de proposito.
    """
    return [
        Expectation(
            name="event_id_presente",
            condition=F.col("event_id").isNotNull() & (F.length(F.col("event_id")) > 0),
            description="Todo evento precisa de um identificador unico.",
            critical=True,
        ),
        Expectation(
            name="timestamp_valido",
            condition=F.col("event_timestamp").isNotNull(),
            description="O timestamp precisa ser uma data/hora ISO valida.",
            critical=True,
        ),
        Expectation(
            name="minuto_valido",
            condition=F.col("minute").between(config.MIN_MATCH_MINUTE, config.MAX_MATCH_MINUTE),
            description=f"O minuto deve estar entre {config.MIN_MATCH_MINUTE} e {config.MAX_MATCH_MINUTE}.",
        ),
        Expectation(
            name="tipo_evento_conhecido",
            condition=F.col("event_type").isin(list(config.VALID_EVENT_TYPES)),
            description=f"event_type deve ser um de {config.VALID_EVENT_TYPES}.",
        ),
        Expectation(
            name="audiencia_nao_negativa",
            condition=F.col("audience_count") >= 0,
            description="A audiencia nunca pode ser negativa.",
        ),
        Expectation(
            name="audiencia_plausivel",
            condition=F.col("audience_count") <= config.MAX_AUDIENCE_COUNT,
            description="Audiencia acima do teto indica erro de telemetria.",
        ),
        Expectation(
            name="jogador_informado",
            condition=F.col("player_name").isNotNull(),
            description="O evento precisa estar associado a um jogador.",
        ),
        Expectation(
            name="time_informado",
            condition=F.col("team").isNotNull() & (F.length(F.col("team")) > 0),
            description="O evento precisa estar associado a um time.",
        ),
    ]


# ---------------------------------------------------------------------------
# Pipeline completo da camada
# ---------------------------------------------------------------------------


def transform_silver(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Aplica toda a cadeia de transformacao da camada Silver.

    Args:
        bronze_df: DataFrame (batch ou streaming) no formato da camada Bronze.

    Returns:
        Tupla ``(clean_df, quarantine_df)``. O primeiro segue para a Gold; o
        segundo guarda os registros rejeitados junto com o motivo.
    """
    parsed = parse_event_timestamp(bronze_df)
    standardized = clean_player_names(standardize_event_types(standardize_team_names(parsed)))
    deduplicated = deduplicate(standardized)
    validated = annotate_violations(deduplicated, silver_expectations())
    enriched = add_processing_metadata(validated)

    clean_df, quarantine_df = split_by_quality(enriched)

    # A ordem fixa de colunas mantem o contrato da tabela estavel.
    clean_df = clean_df.select(*SILVER_COLUMNS)
    return clean_df, quarantine_df


# ---------------------------------------------------------------------------
# Leitura e escrita
# ---------------------------------------------------------------------------


def read_bronze_stream(spark, source_path: str = config.BRONZE_PATH) -> DataFrame:
    """Le a tabela Delta da camada Bronze como stream.

    `ignoreChanges` evita que a query quebre quando a Bronze sofre compactacao
    (OPTIMIZE reescreve arquivos sem alterar o conteudo logico).
    """
    LOGGER.info("Lendo stream da camada Bronze em %s", source_path)
    return spark.readStream.format("delta").option("ignoreChanges", "true").load(source_path)


def _make_batch_writer(app_id: str):
    """Cria a funcao `foreachBatch` que grava as duas saidas da camada.

    Usamos um unico `writeStream` com `foreachBatch` em vez de duas queries
    independentes: assim a camada Bronze e lida uma unica vez.

    `txnAppId` + `txnVersion` tornam a escrita idempotente no Delta: se o
    micro-batch for reprocessado apos uma falha, o Delta reconhece a transacao
    ja aplicada e nao duplica as linhas (foreachBatch, por si so, oferece
    apenas garantia *at-least-once*).
    """

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        clean_df, quarantine_df = transform_silver(batch_df)

        # O DataFrame de origem e consumido duas vezes (clean + quarentena);
        # o cache evita recalcular todo o plano na segunda acao.
        batch_df.persist()
        try:
            (
                clean_df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .option("txnAppId", f"{app_id}_clean")
                .option("txnVersion", batch_id)
                .partitionBy("event_date")
                .save(config.SILVER_PATH)
            )
            (
                quarantine_df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .option("txnAppId", f"{app_id}_quarantine")
                .option("txnVersion", batch_id)
                .save(config.SILVER_QUARANTINE_PATH)
            )
        finally:
            batch_df.unpersist()

    return write_batch


def write_silver(
    bronze_stream: DataFrame,
    checkpoint_path: str = config.SILVER_CHECKPOINT,
    trigger_interval: str | None = config.TRIGGER_INTERVAL,
    once: bool = False,
) -> StreamingQuery:
    """Inicia a query de streaming da camada Silver."""
    writer = (
        bronze_stream.writeStream.foreachBatch(_make_batch_writer("silver"))
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .queryName("silver_transformation")
    )
    writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime=trigger_interval)

    LOGGER.info(
        "Escrevendo Silver em %s (quarentena: %s, checkpoint: %s)",
        config.SILVER_PATH,
        config.SILVER_QUARANTINE_PATH,
        checkpoint_path,
    )
    return writer.start()


def run(once: bool = False, await_termination: bool = True) -> StreamingQuery:
    """Monta e inicia a transformacao da camada Silver."""
    spark = get_spark("silver-transformation")
    bronze_stream = read_bronze_stream(spark)
    query = write_silver(bronze_stream, once=once)

    if await_termination:
        try:
            query.awaitTermination()
        except KeyboardInterrupt:  # pragma: no cover - interacao manual
            LOGGER.info("Interrompido pelo usuario - parando a query com seguranca.")
            query.stop()

    return query


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Transformacao da camada Silver.")
    parser.add_argument("--once", action="store_true", help="Processa o backlog disponivel e encerra.")
    args = parser.parse_args(argv)

    try:
        run(once=args.once)
    except Exception:
        LOGGER.exception("Falha na transformacao da camada Silver.")
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
