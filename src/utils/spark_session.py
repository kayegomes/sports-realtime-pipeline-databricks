"""
Criacao e configuracao da SparkSession com suporte a Delta Lake.

O mesmo helper funciona em dois cenarios:

* **Databricks** - a SparkSession ja existe e ja vem com o Delta Lake
  habilitado; apenas reaproveitamos a sessao ativa.
* **Local / CI** - criamos a sessao com `delta-spark`, que baixa o jar do
  Delta e injeta as extensoes necessarias.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from pyspark.sql import SparkSession

from src.config import is_databricks

LOGGER = logging.getLogger(__name__)

#: Configuracoes aplicadas em qualquer ambiente.
_BASE_CONFIGS: dict[str, str] = {
    # Extensoes obrigatorias do Delta Lake.
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # Evita que o schema de tabelas Delta precise de ALTER manual quando
    # adicionamos colunas novas nas transformacoes.
    "spark.databricks.delta.schema.autoMerge.enabled": "true",
    # Compactacao automatica de arquivos pequenos: essencial em streaming,
    # onde cada micro-batch tende a gerar arquivos minusculos.
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    # Timestamps consistentes independente do fuso da maquina.
    "spark.sql.session.timeZone": "UTC",
}

#: Configuracoes usadas somente fora do Databricks (cluster local de 1 no).
_LOCAL_CONFIGS: dict[str, str] = {
    # 200 particoes (padrao) e um exagero absurdo em uma maquina local e
    # deixa os testes lentissimos.
    "spark.sql.shuffle.partitions": "4",
    "spark.sql.streaming.schemaInference": "false",
    "spark.ui.showConsoleProgress": "false",
}


def get_spark(
    app_name: str = "sports-realtime-pipeline",
    extra_configs: dict[str, str] | None = None,
) -> SparkSession:
    """Retorna uma SparkSession pronta para ler e escrever Delta Lake.

    Args:
        app_name: nome da aplicacao Spark (ignorado no Databricks).
        extra_configs: configuracoes adicionais que sobrescrevem os padroes.

    Returns:
        A SparkSession ativa.
    """
    configs: dict[str, str] = dict(_BASE_CONFIGS)

    if is_databricks():
        # No Databricks a sessao ja existe: criar outra causa erro. Apenas
        # aplicamos as configuracoes que fazem sentido no cluster.
        LOGGER.info("Ambiente Databricks detectado - reaproveitando a SparkSession ativa.")
        spark = SparkSession.builder.getOrCreate()
        for key, value in (extra_configs or {}).items():
            spark.conf.set(key, value)
        return spark

    configs.update(_LOCAL_CONFIGS)
    configs.update(extra_configs or {})

    builder: Any = SparkSession.builder.appName(app_name).master(os.getenv("SPARK_MASTER", "local[*]"))
    for key, value in configs.items():
        builder = builder.config(key, value)

    try:
        # `configure_spark_with_delta_pip` resolve a versao correta do jar do
        # Delta a partir da versao instalada do pacote `delta-spark`.
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:  # pragma: no cover - ambiente sem delta-spark
        LOGGER.warning(
            "Pacote `delta-spark` nao encontrado. A sessao sera criada sem o "
            "resolvedor automatico de jars do Delta Lake."
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    LOGGER.info("SparkSession criada (versao %s, master %s).", spark.version, spark.sparkContext.master)
    return spark


def configure_logging(level: int = logging.INFO) -> None:
    """Configura o logging padrao dos entrypoints do pipeline."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
