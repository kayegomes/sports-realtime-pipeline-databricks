"""
Fixtures compartilhadas pelos testes.

A SparkSession e criada uma unica vez por sessao de testes: subir a JVM custa
alguns segundos e nao ha motivo para pagar esse custo por arquivo.
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator
from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.utils.spark_session import get_spark


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """SparkSession local com Delta Lake habilitado."""
    warehouse = tempfile.mkdtemp(prefix="spark-warehouse-")
    session = get_spark(
        app_name="sports-pipeline-tests",
        extra_configs={
            "spark.sql.warehouse.dir": warehouse,
            # Particoes minimas: os DataFrames de teste tem poucas linhas e
            # shuffles grandes so deixariam a suite lenta.
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",
        },
    )
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture(scope="session")
def bronze_schema() -> StructType:
    """Schema da camada Bronze (saida de `add_ingestion_metadata`)."""
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
            StructField("ingestion_timestamp", TimestampType(), True),
        ]
    )


@pytest.fixture()
def make_bronze_df(spark: SparkSession, bronze_schema: StructType):
    """Fabrica de DataFrames no formato Bronze.

    Cada chamada recebe uma lista de dicionarios parciais; os campos ausentes
    recebem um valor padrao valido. Isso mantem os testes focados no atributo
    que esta sendo verificado.
    """
    default = {
        "event_id": "evt-000",
        "timestamp": "2026-09-22T15:00:00",
        "match_id": "MATCH-001",
        "minute": 10,
        "event_type": "gol",
        "player_name": "Joao Silva",
        "team": "Flamengo",
        "audience_count": 1_000_000,
        "ingestion_timestamp": datetime(2026, 9, 22, 15, 0, 5),
    }

    def _factory(rows: list[dict]) -> DataFrame:
        materialized = [{**default, **row} for row in rows]
        return spark.createDataFrame(materialized, schema=bronze_schema)

    return _factory


@pytest.fixture()
def make_silver_df(spark: SparkSession):
    """Fabrica de DataFrames no formato Silver (entrada da camada Gold)."""
    silver_schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("match_id", StringType(), True),
            StructField("minute", IntegerType(), True),
            StructField("event_type", StringType(), True),
            StructField("player_name", StringType(), True),
            StructField("team", StringType(), True),
            StructField("audience_count", LongType(), True),
        ]
    )
    default = {
        "event_id": "evt-000",
        "event_timestamp": datetime(2026, 9, 22, 15, 0, 0),
        "match_id": "MATCH-001",
        "minute": 10,
        "event_type": "finalizacao",
        "player_name": "Joao Silva",
        "team": "Flamengo",
        "audience_count": 1_000_000,
    }

    def _factory(rows: list[dict]) -> DataFrame:
        materialized = [{**default, **row} for row in rows]
        return spark.createDataFrame(materialized, schema=silver_schema)

    return _factory
