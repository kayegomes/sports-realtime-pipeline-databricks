"""
Testes do gerador de eventos e da camada Bronze.

Cobertura:
    * formato e tipos dos eventos gerados;
    * determinismo com semente fixa;
    * escrita atomica dos arquivos JSON;
    * leitura dos arquivos com o schema declarado pela Bronze;
    * metadados de ingestao.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timezone

from faker import Faker
from pyspark.sql import functions as F

from src import config
from src.bronze.ingest_bronze import add_ingestion_metadata, raw_event_schema
from src.generator.event_generator import (
    EVENT_FIELDS,
    build_matches,
    generate_batch,
    generate_event,
    run,
    write_batch,
)

# ---------------------------------------------------------------------------
# Gerador de eventos
# ---------------------------------------------------------------------------


def _fixture_generator(seed: int = 7) -> tuple[list, Faker, random.Random]:
    rng = random.Random(seed)
    fake = Faker("pt_BR")
    Faker.seed(seed)
    matches = build_matches(fake, rng, quantity=2)
    return matches, fake, rng


def test_evento_tem_todos_os_campos_obrigatorios():
    matches, fake, rng = _fixture_generator()

    event = generate_event(matches[0], fake, rng)

    assert set(event) == set(EVENT_FIELDS)


def test_evento_tem_tipos_corretos():
    matches, fake, rng = _fixture_generator()

    event = generate_event(matches[0], fake, rng, now=datetime(2026, 9, 22, 15, 0, tzinfo=timezone.utc))

    assert isinstance(event["event_id"], str)
    assert isinstance(event["minute"], int)
    assert isinstance(event["audience_count"], int)
    assert isinstance(event["player_name"], str)
    assert event["timestamp"].startswith("2026-09-22T15:00:00")
    assert event["match_id"] == matches[0].match_id


def test_evento_limpo_respeita_as_regras_de_negocio():
    """Sem injecao de defeito, todo evento deve ser valido."""
    matches, fake, rng = _fixture_generator()

    for _ in range(200):
        matches[0].advance(rng)
        event = generate_event(matches[0], fake, rng, dirty_rate=0.0)

        assert event["event_type"] in config.VALID_EVENT_TYPES
        assert config.MIN_MATCH_MINUTE <= event["minute"] <= config.MAX_MATCH_MINUTE
        assert event["audience_count"] >= 0
        assert event["team"] in (matches[0].home_team, matches[0].away_team)


def test_defeitos_sao_injetados_quando_dirty_rate_e_maximo():
    """Com `dirty_rate=1.0`, todo evento sai corrompido de alguma forma."""
    matches, fake, rng = _fixture_generator()

    defects = 0
    for _ in range(100):
        matches[0].advance(rng)
        event = generate_event(matches[0], fake, rng, dirty_rate=1.0)
        is_clean = (
            event["event_type"] in config.VALID_EVENT_TYPES
            and event["minute"] <= config.MAX_MATCH_MINUTE
            and event["audience_count"] >= 0
            and event["player_name"] not in ("", "   ", None)
            and event["team"] in config.CANONICAL_TEAMS
        )
        defects += 0 if is_clean else 1

    assert defects == 100


#: Campos que nao podem ser deterministicos: `event_id` e um uuid4 por
#: definicao e `timestamp` vem do relogio de parede.
_NON_DETERMINISTIC = {"event_id", "timestamp"}


def test_geracao_e_deterministica_com_a_mesma_semente():
    """Mesma semente -> mesma sequencia de partidas, jogadores e eventos."""

    def first_events() -> list[dict]:
        matches, fake, rng = _fixture_generator(seed=123)
        batch = generate_batch(matches, fake, rng, events_per_match=2, dirty_rate=0.0, duplicate_rate=0.0)
        return [{k: v for k, v in event.items() if k not in _NON_DETERMINISTIC} for event in batch]

    assert first_events() == first_events()


def test_sementes_diferentes_produzem_eventos_diferentes():
    def events_for(seed: int) -> list[dict]:
        matches, fake, rng = _fixture_generator(seed=seed)
        batch = generate_batch(matches, fake, rng, events_per_match=3, dirty_rate=0.0, duplicate_rate=0.0)
        return [{k: v for k, v in event.items() if k not in _NON_DETERMINISTIC} for event in batch]

    assert events_for(1) != events_for(2)


def test_lote_gera_a_quantidade_esperada_de_eventos():
    matches, fake, rng = _fixture_generator()

    batch = generate_batch(matches, fake, rng, events_per_match=3, duplicate_rate=0.0)

    assert len(batch) == len(matches) * 3


def test_lote_pode_conter_duplicatas_para_simular_at_least_once():
    matches, fake, rng = _fixture_generator()

    batch = generate_batch(matches, fake, rng, events_per_match=5, duplicate_rate=1.0)

    ids = [event["event_id"] for event in batch]
    assert len(ids) > len(set(ids))


# ---------------------------------------------------------------------------
# Escrita dos arquivos
# ---------------------------------------------------------------------------


def test_write_batch_grava_json_lines_e_nao_deixa_arquivo_temporario(tmp_path):
    matches, fake, rng = _fixture_generator()
    batch = generate_batch(matches, fake, rng, events_per_match=2)

    path = write_batch(batch, tmp_path)

    assert path is not None and path.exists()
    # Nenhum `_tmp_` remanescente: o rename foi concluido.
    assert not list(tmp_path.glob("_tmp_*"))

    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == len(batch)
    assert json.loads(lines[0])["match_id"] == batch[0]["match_id"]


def test_write_batch_ignora_lote_vazio(tmp_path):
    assert write_batch([], tmp_path) is None
    assert not list(tmp_path.iterdir())


def test_run_com_max_ticks_encerra_sozinho(tmp_path):
    total = run(
        output_dir=tmp_path,
        interval_seconds=0.0,
        matches_count=1,
        events_per_tick=2,
        dirty_rate=0.0,
        max_ticks=3,
        seed=99,
    )

    assert total >= 3 * 2  # 3 ciclos x 2 eventos (duplicatas podem somar)
    assert len(list(tmp_path.glob("events_*.json"))) == 3


# ---------------------------------------------------------------------------
# Camada Bronze
# ---------------------------------------------------------------------------


def test_schema_da_bronze_cobre_exatamente_os_campos_do_gerador():
    assert tuple(field.name for field in raw_event_schema().fields) == EVENT_FIELDS


def test_bronze_le_os_arquivos_do_gerador_com_o_schema_declarado(spark, tmp_path):
    """Contrato de ponta a ponta entre gerador e Bronze."""
    matches, fake, rng = _fixture_generator()
    batch = generate_batch(matches, fake, rng, events_per_match=4, dirty_rate=0.0)
    write_batch(batch, tmp_path)

    df = spark.read.schema(raw_event_schema()).json(str(tmp_path))

    assert df.count() == len(batch)
    # Nenhuma coluna virou NULL por incompatibilidade de tipo.
    nulls = df.select(
        F.sum(F.col("minute").isNull().cast("int")).alias("minute"),
        F.sum(F.col("audience_count").isNull().cast("int")).alias("audience"),
        F.sum(F.col("event_id").isNull().cast("int")).alias("event_id"),
    ).first()
    assert (nulls["minute"], nulls["audience"], nulls["event_id"]) == (0, 0, 0)


def test_add_ingestion_metadata_adiciona_colunas_de_auditoria(spark, tmp_path):
    matches, fake, rng = _fixture_generator()
    write_batch(generate_batch(matches, fake, rng, events_per_match=2), tmp_path)
    df = spark.read.schema(raw_event_schema()).json(str(tmp_path))

    result = add_ingestion_metadata(df)

    assert "ingestion_timestamp" in result.columns
    assert "ingestion_date" in result.columns
    # As colunas originais continuam intactas.
    assert set(EVENT_FIELDS).issubset(set(result.columns))
    assert result.filter(F.col("ingestion_timestamp").isNull()).count() == 0


def test_add_ingestion_metadata_nao_altera_a_quantidade_de_linhas(make_bronze_df):
    df = make_bronze_df([{"event_id": "a"}, {"event_id": "b"}, {"event_id": "c"}]).drop("ingestion_timestamp")

    assert add_ingestion_metadata(df).count() == 3
