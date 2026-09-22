"""
Validacao pos-execucao do pipeline (usada pelo smoke test do CI).

Confere que as tres camadas produziram dados coerentes:

* a Bronze recebeu os eventos gerados;
* a Silver tem apenas eventos validos e padronizados;
* a Gold materializou os tres KPIs.

Tudo e lido das tabelas Delta em disco: este script nao acessa nenhuma API
externa, nao precisa de credencial e roda offline.

Cada verificacao carrega os numeros que a embasam. Dentro do GitHub Actions,
as falhas viram *annotations* (`::error::`) e um resumo em Markdown e escrito
no `$GITHUB_STEP_SUMMARY`, entao da para diagnosticar a falha direto na tela
do run, sem abrir o log bruto.

Encerra com codigo 1 se qualquer verificacao falhar.
"""

from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path

# Executar `python scripts/validate_pipeline.py` coloca `scripts/` no
# sys.path, e nao a raiz do repositorio - sem esta linha, `import src` falha.
# As camadas nao sofrem disso porque rodam como modulo (`python -m src...`),
# o que ja poe o diretorio corrente no caminho de busca.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import config
from src.bronze.ingest_bronze import raw_event_schema
from src.utils.spark_session import configure_logging, get_spark

#: True quando rodando dentro do GitHub Actions.
IN_CI = os.getenv("GITHUB_ACTIONS") == "true"

#: Resultado de cada verificacao: (descricao, passou, detalhe).
RESULTS: list[tuple[str, bool, str]] = []


def _escape(text: str) -> str:
    """Escapa o texto para uma annotation do GitHub Actions."""
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def check(description: str, condition: bool, detail: str = "") -> None:
    """Registra o resultado de uma verificacao.

    O `detail` deve trazer os numeros observados: quando a verificacao falha,
    e ele que explica o porque sem precisar reproduzir a execucao.
    """
    passed = bool(condition)
    RESULTS.append((description, passed, detail))

    status = "OK   " if passed else "FALHA"
    print(f"[{status}] {description}{f' -> {detail}' if detail else ''}", flush=True)

    if not passed and IN_CI:
        print(f"::error title={_escape(description)}::{_escape(detail or 'condicao falsa')}", flush=True)


def _count_delta(spark: SparkSession, path: str) -> int:
    """Conta as linhas de uma tabela Delta, ou -1 se ela nao existir."""
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:  # noqa: BLE001 - tabela ausente e um resultado valido aqui
        return -1


def _count_source_files(spark: SparkSession) -> int:
    """Conta os eventos na origem (arquivos JSON), ou -1 se a pasta nao existir."""
    try:
        return spark.read.schema(raw_event_schema()).json(config.STREAMING_PATH).count()
    except Exception:  # noqa: BLE001
        return -1


def report_layer_counts(spark: SparkSession) -> dict[str, int]:
    """Conta e imprime o volume de registros de cada camada.

    E a primeira coisa que se quer olhar quando algo da errado: mostra em que
    ponto do pipeline o dado parou de fluir.
    """
    counts = {
        "Origem (JSON)": _count_source_files(spark),
        "Bronze / events": _count_delta(spark, config.BRONZE_PATH),
        "Silver / events_clean": _count_delta(spark, config.SILVER_PATH),
        "Silver / events_quarantine": _count_delta(spark, config.SILVER_QUARANTINE_PATH),
        "Gold / audience_by_window": _count_delta(spark, config.GOLD_AUDIENCE_PATH),
        "Gold / player_events": _count_delta(spark, config.GOLD_PLAYER_EVENTS_PATH),
        "Gold / top_players": _count_delta(spark, config.GOLD_TOP_PLAYERS_PATH),
        "Gold / goals_by_team": _count_delta(spark, config.GOLD_TEAM_GOALS_PATH),
    }

    largura = max(len(nome) for nome in counts)
    print("\n" + "=" * (largura + 14))
    print("REGISTROS POR CAMADA")
    print("=" * (largura + 14))
    for nome, total in counts.items():
        valor = "ausente" if total < 0 else f"{total:,}".replace(",", ".")
        print(f"  {nome.ljust(largura)}  {valor:>10}")
    print("=" * (largura + 14) + "\n", flush=True)

    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_path:
        linhas = ["## Registros por camada", "", "| Camada | Registros |", "|---|---:|"]
        linhas += [f"| {nome} | {'ausente' if total < 0 else total} |" for nome, total in counts.items()]
        with open(summary_path, "a", encoding="utf-8") as handle:
            handle.write("\n".join(linhas) + "\n\n")

    return counts


def write_summary() -> None:
    """Escreve o resumo em Markdown no painel do run (se houver)."""
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    lines = ["## Validacao do pipeline", "", "| | Verificacao | Observado |", "|---|---|---|"]
    lines += [
        f"| {'PASS' if passed else 'FAIL'} | {description} | {detail or '-'} |"
        for description, passed, detail in RESULTS
    ]
    with open(summary_path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> int:
    configure_logging()
    spark = get_spark("pipeline-validation")

    # Panorama antes das assercoes: mostra de imediato em que ponto do
    # pipeline o dado eventualmente parou de fluir.
    counts = report_layer_counts(spark)

    origem = counts["Origem (JSON)"]
    if origem >= 0:
        check(
            "Bronze ingeriu todos os eventos da origem",
            counts["Bronze / events"] >= origem,
            f"origem={origem} bronze={counts['Bronze / events']}",
        )

    clean, quarentena, bronze_total = (
        counts["Silver / events_clean"],
        counts["Silver / events_quarantine"],
        counts["Bronze / events"],
    )
    if min(clean, quarentena, bronze_total) >= 0:
        # A Silver so pode encolher em relacao a Bronze, e apenas por
        # deduplicacao: todo evento rejeitado vai para a quarentena, nenhum
        # e descartado em silencio.
        check(
            "Silver nao descarta eventos fora da deduplicacao",
            clean + quarentena <= bronze_total,
            f"clean={clean} + quarentena={quarentena} <= bronze={bronze_total} "
            f"(duplicatas removidas: {bronze_total - clean - quarentena})",
        )

    # --- Bronze -----------------------------------------------------------
    bronze = spark.read.format("delta").load(config.BRONZE_PATH)
    bronze_count = counts["Bronze / events"]
    check("Bronze contem eventos", bronze_count > 0, f"{bronze_count} linhas")

    sem_metadado = bronze.filter(F.col("ingestion_timestamp").isNull()).count()
    check(
        "Bronze registrou os metadados de ingestao",
        sem_metadado == 0,
        f"{sem_metadado} linhas sem ingestion_timestamp",
    )

    # --- Silver -----------------------------------------------------------
    silver = spark.read.format("delta").load(config.SILVER_PATH)
    silver_count = silver.count()
    check("Silver contem eventos", silver_count > 0, f"{silver_count} linhas")
    check(
        "Silver nao tem mais linhas que a Bronze",
        silver_count <= bronze_count,
        f"silver={silver_count} bronze={bronze_count}",
    )

    minuto_invalido = silver.filter(
        ~F.col("minute").between(config.MIN_MATCH_MINUTE, config.MAX_MATCH_MINUTE)
    ).count()
    check("Silver nao tem minuto invalido", minuto_invalido == 0, f"{minuto_invalido} linhas fora da faixa")

    audiencia_negativa = silver.filter(F.col("audience_count") < 0).count()
    check(
        "Silver nao tem audiencia negativa",
        audiencia_negativa == 0,
        f"{audiencia_negativa} linhas negativas",
    )

    tipo_desconhecido = silver.filter(~F.col("event_type").isin(list(config.VALID_EVENT_TYPES))).count()
    check(
        "Silver so tem tipos de evento conhecidos",
        tipo_desconhecido == 0,
        f"{tipo_desconhecido} linhas com tipo fora de {list(config.VALID_EVENT_TYPES)}",
    )

    ids_distintos = silver.select("event_id").distinct().count()
    check(
        "Silver nao tem event_id duplicado",
        silver_count == ids_distintos,
        f"{silver_count} linhas para {ids_distintos} ids distintos",
    )

    times = sorted(row["team"] for row in silver.select("team").distinct().collect())
    fora_do_padrao = [team for team in times if team not in config.CANONICAL_TEAMS]
    check(
        "Silver padronizou os nomes de time",
        not fora_do_padrao,
        f"fora do padrao: {fora_do_padrao} | encontrados: {times}",
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
    acima_do_limite = top_players.filter(F.col("rank_position") > config.TOP_N_PLAYERS).count()
    check(
        "Ranking respeita o limite de top N",
        acima_do_limite == 0,
        f"{acima_do_limite} linhas com rank_position > {config.TOP_N_PLAYERS}",
    )

    top_count = top_players.count()
    top_distinct = top_players.select("window_start", "match_id", "rank_position").distinct().count()
    check(
        "Ranking nao tem posicao duplicada por janela/partida",
        top_count == top_distinct,
        f"{top_count} linhas para {top_distinct} chaves distintas",
    )

    # --- Quarentena (informativo) ----------------------------------------
    quarantine = spark.read.format("delta").load(config.SILVER_QUARANTINE_PATH)
    print(f"\nQuarentena: {quarantine.count()} eventos rejeitados", flush=True)
    quarantine.select(F.explode("quality_errors").alias("regra")).groupBy("regra").count().orderBy(
        F.col("count").desc()
    ).show(truncate=False)

    write_summary()
    spark.stop()

    failures = [description for description, passed, _ in RESULTS if not passed]
    if failures:
        print(f"\n{len(failures)} verificacao(oes) falharam:")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print(f"\nTodas as {len(RESULTS)} verificacoes passaram.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        # Sem isto, uma excecao (tabela ausente, schema inesperado) aparece no
        # CI apenas como "exit code 1". Emitir o traceback como annotation faz
        # a causa raiz aparecer na tela do run.
        detalhe = traceback.format_exc()
        print(detalhe, file=sys.stderr, flush=True)
        if IN_CI:
            print(f"::error title=Excecao na validacao::{_escape(detalhe)}", flush=True)
        sys.exit(1)
