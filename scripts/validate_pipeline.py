"""
Validacao pos-execucao do pipeline (usada pelo smoke test do CI).

Confere que as tres camadas produziram dados coerentes:

* a Bronze recebeu os eventos gerados;
* a Silver tem apenas eventos validos e padronizados;
* a Gold materializou os tres KPIs.

Encerra com codigo 1 na primeira falha, o que reprova o job do GitHub Actions.
"""

from __future__ import annotations

import sys

from pyspark.sql import functions as F

from src import config
from src.utils.spark_session import configure_logging, get_spark

FAILURES: list[str] = []


def check(description: str, condition: bool, detail: str = "") -> None:
    """Registra o resultado de uma verificacao."""
    status = "OK  " if condition else "FALHA"
    print(f"[{status}] {description}{f' -> {detail}' if detail else ''}")
    if not condition:
        FAILURES.append(description)


def main() -> int:
    configure_logging()
    spark = get_spark("pipeline-validation")

    # --- Bronze -----------------------------------------------------------
    bronze = spark.read.format("delta").load(config.BRONZE_PATH)
    bronze_count = bronze.count()
    check("Bronze contem eventos", bronze_count > 0, f"{bronze_count} linhas")
    check(
        "Bronze registrou os metadados de ingestao",
        bronze.filter(F.col("ingestion_timestamp").isNull()).count() == 0,
    )

    # --- Silver -----------------------------------------------------------
    silver = spark.read.format("delta").load(config.SILVER_PATH)
    silver_count = silver.count()
    check("Silver contem eventos", silver_count > 0, f"{silver_count} linhas")
    check("Silver nao perdeu volume demais", silver_count <= bronze_count)
    check(
        "Silver nao tem minuto invalido",
        silver.filter(
            ~F.col("minute").between(config.MIN_MATCH_MINUTE, config.MAX_MATCH_MINUTE)
        ).count()
        == 0,
    )
    check(
        "Silver nao tem audiencia negativa",
        silver.filter(F.col("audience_count") < 0).count() == 0,
    )
    check(
        "Silver so tem tipos de evento conhecidos",
        silver.filter(~F.col("event_type").isin(list(config.VALID_EVENT_TYPES))).count() == 0,
    )
    check(
        "Silver nao tem event_id duplicado",
        silver.count() == silver.select("event_id").distinct().count(),
    )
    times = sorted(row["team"] for row in silver.select("team").distinct().collect())
    check(
        "Silver padronizou os nomes de time",
        all(team in config.CANONICAL_TEAMS for team in times),
        ", ".join(times),
    )

    # --- Gold -------------------------------------------------------------
    for label, path in {
        "audiencia por janela": config.GOLD_AUDIENCE_PATH,
        "top jogadores": config.GOLD_TOP_PLAYERS_PATH,
        "gols por time": config.GOLD_TEAM_GOALS_PATH,
    }.items():
        rows = spark.read.format("delta").load(path).count()
        check(f"Gold materializou {label}", rows > 0, f"{rows} linhas")

    top_players = spark.read.format("delta").load(config.GOLD_TOP_PLAYERS_PATH)
    check(
        "Ranking respeita o limite de top N",
        top_players.filter(F.col("rank_position") > config.TOP_N_PLAYERS).count() == 0,
    )
    check(
        "Ranking nao tem posicao duplicada por janela/partida",
        top_players.count()
        == top_players.select("window_start", "match_id", "rank_position").distinct().count(),
    )

    # --- Quarentena (informativo) ----------------------------------------
    quarantine = spark.read.format("delta").load(config.SILVER_QUARANTINE_PATH)
    print(f"\nQuarentena: {quarantine.count()} eventos rejeitados")
    quarantine.select(F.explode("quality_errors").alias("regra")).groupBy("regra").count().orderBy(
        F.col("count").desc()
    ).show(truncate=False)

    spark.stop()

    if FAILURES:
        print(f"\n{len(FAILURES)} verificacao(oes) falharam:")
        for failure in FAILURES:
            print(f"  - {failure}")
        return 1

    print("\nTodas as verificacoes passaram.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
