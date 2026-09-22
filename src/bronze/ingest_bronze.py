"""
Camada Bronze - ingestao dos eventos crus em Delta Lake.

Principios da camada Bronze neste projeto:

* **Fidelidade a origem**: nenhuma regra de negocio e aplicada aqui. Tipos sao
  lidos conforme o schema declarado e nada e descartado - inclusive o dado
  ruim, que so sera tratado na camada Silver.
* **Schema explicito**: em Structured Streaming a inferencia de schema e
  desligada por padrao (e deve continuar assim). Um schema fixo evita que um
  arquivo atipico mude o contrato da tabela no meio da execucao.
* **Rastreabilidade**: gravamos `ingestion_timestamp` e `source_file`, o que
  permite reprocessar ou auditar a origem de qualquer linha.

Execucao::

    python -m src.bronze.ingest_bronze            # streaming continuo
    python -m src.bronze.ingest_bronze --once     # processa o backlog e sai
"""

from __future__ import annotations

import argparse
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from src import config
from src.utils.spark_session import configure_logging, get_spark

LOGGER = logging.getLogger(__name__)


def raw_event_schema() -> StructType:
    """Schema dos arquivos JSON produzidos pelo gerador.

    Tudo e nullable de proposito: a Bronze aceita o dado como ele chega. Note
    que `minute` e `audience_count` sao numericos - se vier lixo nao numerico,
    o Spark grava NULL e a Silver trata como violacao de qualidade.
    """
    return StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("match_id", StringType(), True),
            StructField("minute", IntegerType(), True),
            StructField("event_type", StringType(), True),
            StructField("player_name", StringType(), True),
            StructField("team", StringType(), True),
            StructField("audience_count", LongType(), True),
        ]
    )


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Adiciona as colunas de auditoria da ingestao.

    Funcao pura (DataFrame -> DataFrame), identica em batch e em streaming, o
    que permite testa-la com um DataFrame estatico.
    """
    return df.withColumn("ingestion_timestamp", F.current_timestamp()).withColumn(
        "ingestion_date", F.to_date(F.current_timestamp())
    )


def read_raw_stream(
    spark: SparkSession,
    source_path: str = config.STREAMING_PATH,
    max_files_per_trigger: int = config.MAX_FILES_PER_TRIGGER,
) -> DataFrame:
    """Le em streaming os arquivos JSON da pasta monitorada.

    `maxFilesPerTrigger` limita o tamanho do micro-batch: sem ele, a primeira
    execucao tentaria ler todo o backlog de uma vez.

    No Databricks, a alternativa recomendada em producao e o Auto Loader
    (``.format("cloudFiles")``), que usa notificacao de eventos em vez de
    listar o diretorio. O formato ``json`` foi mantido para que o projeto
    rode tambem localmente e no CI.
    """
    LOGGER.info("Lendo stream de arquivos JSON em %s", source_path)
    return (
        spark.readStream.format("json")
        .schema(raw_event_schema())
        .option("maxFilesPerTrigger", max_files_per_trigger)
        # Registros malformados ficam em `_corrupt_record` em vez de derrubar a query.
        .option("mode", "PERMISSIVE")
        .load(source_path)
        # `_metadata` e uma coluna oculta do file source (Spark 3.4+).
        .withColumn("source_file", F.col("_metadata.file_path"))
    )


def write_bronze(
    df: DataFrame,
    target_path: str = config.BRONZE_PATH,
    checkpoint_path: str = config.BRONZE_CHECKPOINT,
    trigger_interval: str | None = config.TRIGGER_INTERVAL,
    once: bool = False,
) -> StreamingQuery:
    """Grava o stream na tabela Delta da camada Bronze.

    A tabela e particionada por `ingestion_date`: e a chave usada tanto para
    limpeza (VACUUM/retencao) quanto para reprocessamento por dia.

    Args:
        once: se True, usa `availableNow` - processa todo o backlog disponivel
            e encerra. E o modo usado por jobs agendados e pelo smoke test.
    """
    writer = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .partitionBy("ingestion_date")
        .queryName("bronze_ingestion")
    )

    writer = writer.trigger(availableNow=True) if once else writer.trigger(processingTime=trigger_interval)

    LOGGER.info("Escrevendo Bronze em %s (checkpoint: %s)", target_path, checkpoint_path)
    return writer.start(target_path)


def run(once: bool = False, await_termination: bool = True) -> StreamingQuery:
    """Monta e inicia a query de ingestao da camada Bronze."""
    spark = get_spark("bronze-ingestion")

    raw_df = read_raw_stream(spark)
    bronze_df = add_ingestion_metadata(raw_df)
    query = write_bronze(bronze_df, once=once)

    if await_termination:
        try:
            query.awaitTermination()
        except KeyboardInterrupt:  # pragma: no cover - interacao manual
            LOGGER.info("Interrompido pelo usuario - parando a query com seguranca.")
            query.stop()

    return query


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Ingestao da camada Bronze.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Processa o backlog disponivel (trigger availableNow) e encerra.",
    )
    args = parser.parse_args(argv)

    try:
        run(once=args.once)
    except Exception:
        LOGGER.exception("Falha na ingestao da camada Bronze.")
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
