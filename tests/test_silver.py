"""
Testes da camada Silver.

A logica da camada e feita de funcoes puras ``DataFrame -> DataFrame``, entao
todos os cenarios sao verificados com DataFrames estaticos - sem subir query
de streaming e sem depender de arquivos.
"""

from __future__ import annotations

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F

from src import config
from src.silver.transform_silver import (
    SILVER_COLUMNS,
    clean_player_names,
    deduplicate,
    normalize_text,
    parse_event_timestamp,
    silver_expectations,
    standardize_event_types,
    standardize_team_names,
    transform_silver,
)
from src.utils.quality_checks import annotate_violations, split_by_quality


def _errors_of(quarantine_df) -> set[str]:
    """Conjunto dos nomes de regra violados na quarentena."""
    rows = quarantine_df.select(F.explode("quality_errors").alias("regra")).distinct().collect()
    return {row["regra"] for row in rows}


# ---------------------------------------------------------------------------
# Normalizacao e padronizacao
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("entrada", "esperado"),
    [
        ("  FLAMENGO ", "Flamengo"),
        ("fla", "Flamengo"),
        ("CR Flamengo", "Flamengo"),
        ("Timao", "Corinthians"),
        ("SPFC", "Sao Paulo"),
        ("Sao  Paulo", "Sao Paulo"),
        ("Gremio", "Gremio"),
        ("atletico-mg", "Atletico Mineiro"),
    ],
)
def test_padroniza_nomes_de_time(make_bronze_df, entrada, esperado):
    df = make_bronze_df([{"team": entrada}])

    resultado = standardize_team_names(df).select("team").first()

    assert resultado["team"] == esperado


def test_time_desconhecido_e_preservado_em_formato_canonico(make_bronze_df):
    """Um time fora do mapa nao pode ser perdido: viramos `initcap` e seguimos."""
    df = make_bronze_df([{"team": "  bangu atletico  clube "}])

    assert standardize_team_names(df).first()["team"] == "Bangu Atletico Clube"


@pytest.mark.parametrize(
    ("entrada", "esperado"),
    [("GOL!!", "gol"), ("  Cartao ", "cartao"), ("SUBSTITUICAO", "substituicao"), ("penalti", "penalti")],
)
def test_padroniza_tipos_de_evento(make_bronze_df, entrada, esperado):
    df = make_bronze_df([{"event_type": entrada}])

    assert standardize_event_types(df).first()["event_type"] == esperado


def test_normalize_text_remove_acentos_e_pontuacao(spark):
    df = spark.createDataFrame([("São  Paulo - F.C.",)], ["valor"])

    assert df.select(normalize_text(F.col("valor")).alias("v")).first()["v"] == "sao paulo f c"


def test_nome_de_jogador_em_branco_vira_nulo(make_bronze_df):
    df = make_bronze_df([{"player_name": "   "}, {"player_name": " Joao   Silva "}])

    resultado = clean_player_names(df)

    assert resultado.filter(F.col("player_name").isNull()).count() == 1
    assert resultado.filter(F.col("player_name") == "Joao Silva").count() == 1


def test_parse_timestamp_invalido_resulta_em_nulo(make_bronze_df):
    df = make_bronze_df([{"timestamp": "nao-e-uma-data"}])

    assert parse_event_timestamp(df).first()["event_timestamp"] is None


@pytest.mark.parametrize(
    "entrada",
    [
        "2026-09-22T15:00:00",
        "2026-09-22 15:00:00",
        # Formato real emitido pelo gerador (`datetime.isoformat()` em UTC).
        "2026-09-22T15:00:00.914286+00:00",
        "2026-09-22T15:00:00+00:00",
    ],
)
def test_parse_timestamp_aceita_os_formatos_iso_da_origem(make_bronze_df, entrada):
    df = make_bronze_df([{"timestamp": entrada}])

    resultado = parse_event_timestamp(df).select(
        F.date_format("event_timestamp", "yyyy-MM-dd HH:mm:ss").alias("ts"),
        F.date_format("event_date", "yyyy-MM-dd").alias("dia"),
    )

    linha = resultado.first()
    assert linha["ts"] == "2026-09-22 15:00:00"
    assert linha["dia"] == "2026-09-22"


# ---------------------------------------------------------------------------
# Deduplicacao
# ---------------------------------------------------------------------------


def test_remove_duplicatas_por_event_id(make_bronze_df):
    df = parse_event_timestamp(
        make_bronze_df([{"event_id": "evt-1"}, {"event_id": "evt-1"}, {"event_id": "evt-2"}])
    )

    assert deduplicate(df).count() == 2


# ---------------------------------------------------------------------------
# Regras de qualidade
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("linha", "regra_esperada"),
    [
        ({"minute": 95}, "minuto_valido"),
        ({"minute": -1}, "minuto_valido"),
        ({"minute": None}, "minuto_valido"),
        ({"audience_count": -500}, "audiencia_nao_negativa"),
        ({"audience_count": None}, "audiencia_nao_negativa"),
        ({"event_type": "penalti"}, "tipo_evento_conhecido"),
        ({"player_name": "   "}, "jogador_informado"),
        ({"timestamp": "invalido"}, "timestamp_valido"),
        ({"audience_count": config.MAX_AUDIENCE_COUNT + 1}, "audiencia_plausivel"),
    ],
)
def test_evento_invalido_vai_para_quarentena(make_bronze_df, linha, regra_esperada):
    df = make_bronze_df([linha])

    clean_df, quarantine_df = transform_silver(df)

    assert clean_df.count() == 0
    assert quarantine_df.count() == 1
    assert regra_esperada in _errors_of(quarantine_df)


def test_evento_valido_passa_intacto(make_bronze_df):
    df = make_bronze_df([{"event_id": "evt-ok", "minute": 44, "event_type": "gol", "team": "  fla "}])

    clean_df, quarantine_df = transform_silver(df)

    assert quarantine_df.count() == 0
    # `date_format` resolve o timestamp no fuso da sessao (UTC), evitando que o
    # teste dependa do fuso da maquina que o executa.
    linha = clean_df.withColumn(
        "ts_formatado", F.date_format("event_timestamp", "yyyy-MM-dd HH:mm:ss")
    ).first()
    assert linha["event_id"] == "evt-ok"
    assert linha["minute"] == 44
    assert linha["event_type"] == "gol"
    assert linha["team"] == "Flamengo"
    assert linha["ts_formatado"] == "2026-09-22 15:00:00"


def test_nulo_e_tratado_como_violacao_e_nao_como_aprovacao(make_bronze_df):
    """`NULL >= 0` retorna NULL em SQL; a linha nao pode escapar do filtro."""
    df = parse_event_timestamp(make_bronze_df([{"audience_count": None}]))

    anotado = annotate_violations(df, silver_expectations())
    validos, invalidos = split_by_quality(anotado)

    assert validos.count() == 0
    assert invalidos.count() == 1


def test_uma_linha_pode_acumular_varias_violacoes(make_bronze_df):
    df = make_bronze_df([{"minute": 120, "audience_count": -1, "event_type": "penalti"}])

    _, quarantine_df = transform_silver(df)

    assert _errors_of(quarantine_df) == {
        "minuto_valido",
        "audiencia_nao_negativa",
        "tipo_evento_conhecido",
    }


# ---------------------------------------------------------------------------
# Contrato da camada
# ---------------------------------------------------------------------------


def test_silver_expoe_exatamente_as_colunas_contratadas(make_bronze_df):
    clean_df, _ = transform_silver(make_bronze_df([{"event_id": "evt-1"}]))

    assert tuple(clean_df.columns) == SILVER_COLUMNS


def test_colunas_tecnicas_nao_vazam_para_a_silver(make_bronze_df):
    clean_df, quarantine_df = transform_silver(make_bronze_df([{"event_id": "evt-1"}]))

    assert "quality_errors" not in clean_df.columns
    assert "is_valid" not in clean_df.columns
    # Na quarentena elas sao justamente a informacao util.
    assert "quality_errors" in quarantine_df.columns


def test_processed_timestamp_e_preenchido(make_bronze_df):
    clean_df, _ = transform_silver(make_bronze_df([{"event_id": "evt-1"}]))

    assert clean_df.filter(F.col("processed_timestamp").isNull()).count() == 0


def test_lote_misto_separa_validos_e_invalidos(make_bronze_df):
    df = make_bronze_df(
        [
            {"event_id": "ok-1", "minute": 10},
            {"event_id": "ok-2", "minute": 90},
            {"event_id": "ok-2", "minute": 90},  # duplicata
            {"event_id": "ruim-1", "minute": 91},
            {"event_id": "ruim-2", "audience_count": -10},
        ]
    )

    clean_df, quarantine_df = transform_silver(df)

    assert clean_df.count() == 2
    assert quarantine_df.count() == 2
    assert {row["event_id"] for row in clean_df.collect()} == {"ok-1", "ok-2"}


def test_transformacao_e_estavel_entre_execucoes(make_bronze_df):
    """Duas execucoes sobre a mesma entrada produzem o mesmo resultado."""
    linhas = [{"event_id": f"evt-{i}", "minute": i, "team": "timao"} for i in range(10)]

    primeira, _ = transform_silver(make_bronze_df(linhas))
    segunda, _ = transform_silver(make_bronze_df(linhas))

    colunas = ["event_id", "minute", "team", "event_type", "audience_count"]
    assert_df_equality(
        primeira.select(*colunas).orderBy("event_id"),
        segunda.select(*colunas).orderBy("event_id"),
        ignore_nullable=True,
    )
