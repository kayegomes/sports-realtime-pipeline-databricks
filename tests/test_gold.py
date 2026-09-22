"""
Testes da camada Gold.

As funcoes de KPI sao puras e funcionam igual em batch e em streaming (o
watermark so e aplicado quando o DataFrame e de streaming), entao aqui elas
sao exercitadas com DataFrames estaticos.

Os testes evitam comparar `window_start` com um `datetime` literal: isso
tornaria o resultado dependente do fuso da maquina. Verificamos a *quantidade*
de janelas e os valores agregados, que sao o que realmente importa.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from src.gold.aggregate_gold import (
    audience_kpis,
    player_event_kpis,
    rank_top_players,
    team_goal_kpis,
)

BASE = datetime(2026, 9, 22, 15, 0, 0)


def _at(minutes: float) -> datetime:
    """Instante deslocado em N minutos a partir da base."""
    return BASE + timedelta(minutes=minutes)


# ---------------------------------------------------------------------------
# Audiencia
# ---------------------------------------------------------------------------


def test_audiencia_media_pico_e_minimo_na_janela(make_silver_df):
    df = make_silver_df(
        [
            {"event_id": "1", "event_timestamp": _at(0), "audience_count": 1_000_000},
            {"event_id": "2", "event_timestamp": _at(1), "audience_count": 2_000_000},
            {"event_id": "3", "event_timestamp": _at(2), "audience_count": 3_000_000},
        ]
    )

    resultado = audience_kpis(df).first()

    assert resultado["avg_audience_count"] == 2_000_000.0
    assert resultado["peak_audience_count"] == 3_000_000
    assert resultado["min_audience_count"] == 1_000_000
    assert resultado["total_events"] == 3


def test_eventos_distantes_caem_em_janelas_diferentes(make_silver_df):
    """Janela de 5 minutos: 15:00 e 15:07 nao podem ficar juntos."""
    df = make_silver_df(
        [
            {"event_id": "1", "event_timestamp": _at(0)},
            {"event_id": "2", "event_timestamp": _at(1)},
            {"event_id": "3", "event_timestamp": _at(7)},
        ]
    )

    resultado = audience_kpis(df)

    assert resultado.count() == 2
    assert resultado.select("window_start").distinct().count() == 2
    # As janelas tem exatamente 5 minutos de duracao.
    linha = resultado.first()
    assert (linha["window_end"] - linha["window_start"]) == timedelta(minutes=5)


def test_partidas_sao_agregadas_separadamente(make_silver_df):
    df = make_silver_df(
        [
            {"event_id": "1", "match_id": "MATCH-001", "audience_count": 100},
            {"event_id": "2", "match_id": "MATCH-002", "audience_count": 300},
        ]
    )

    resultado = {row["match_id"]: row["avg_audience_count"] for row in audience_kpis(df).collect()}

    assert resultado == {"MATCH-001": 100.0, "MATCH-002": 300.0}


def test_audiencia_conta_gols_da_janela(make_silver_df):
    df = make_silver_df(
        [
            {"event_id": "1", "event_type": "gol"},
            {"event_id": "2", "event_type": "gol"},
            {"event_id": "3", "event_type": "finalizacao"},
        ]
    )

    assert audience_kpis(df).first()["goals"] == 2


# ---------------------------------------------------------------------------
# Gols por time
# ---------------------------------------------------------------------------


def test_conta_gols_por_time(make_silver_df):
    df = make_silver_df(
        [
            {"event_id": "1", "team": "Flamengo", "event_type": "gol"},
            {"event_id": "2", "team": "Flamengo", "event_type": "gol"},
            {"event_id": "3", "team": "Flamengo", "event_type": "cartao"},
            {"event_id": "4", "team": "Palmeiras", "event_type": "gol"},
            {"event_id": "5", "team": "Palmeiras", "event_type": "finalizacao"},
        ]
    )

    resultado = {row["team"]: row for row in team_goal_kpis(df).collect()}

    assert resultado["Flamengo"]["goals"] == 2
    assert resultado["Flamengo"]["cards"] == 1
    assert resultado["Flamengo"]["total_events"] == 3
    assert resultado["Palmeiras"]["goals"] == 1
    assert resultado["Palmeiras"]["shots"] == 1


def test_time_sem_gol_aparece_com_zero(make_silver_df):
    df = make_silver_df([{"event_id": "1", "team": "Gremio", "event_type": "cartao"}])

    linha = team_goal_kpis(df).first()

    assert linha["team"] == "Gremio"
    assert linha["goals"] == 0
    assert linha["cards"] == 1


# ---------------------------------------------------------------------------
# Estatisticas e ranking de jogadores
# ---------------------------------------------------------------------------


def test_conta_eventos_por_jogador(make_silver_df):
    df = make_silver_df(
        [
            {"event_id": "1", "player_name": "Ana", "event_type": "gol"},
            {"event_id": "2", "player_name": "Ana", "event_type": "finalizacao"},
            {"event_id": "3", "player_name": "Bruno", "event_type": "cartao"},
        ]
    )

    resultado = {row["player_name"]: row for row in player_event_kpis(df).collect()}

    assert resultado["Ana"]["total_events"] == 2
    assert resultado["Ana"]["goals"] == 1
    assert resultado["Ana"]["shots"] == 1
    assert resultado["Bruno"]["cards"] == 1


def _rows_for(players: dict[str, int], match_id: str = "MATCH-001", offset: float = 0.0) -> list[dict]:
    """Gera N eventos por jogador dentro da mesma janela."""
    rows: list[dict] = []
    for player, quantity in players.items():
        for index in range(quantity):
            rows.append(
                {
                    "event_id": f"{match_id}-{player}-{index}",
                    "player_name": player,
                    "match_id": match_id,
                    "event_timestamp": _at(offset),
                }
            )
    return rows


def test_top_3_jogadores_por_janela(make_silver_df):
    df = make_silver_df(_rows_for({"Ana": 5, "Bruno": 4, "Carla": 3, "Diego": 2, "Elis": 1}))

    ranking = rank_top_players(player_event_kpis(df), top_n=3).orderBy("rank_position").collect()

    assert [row["player_name"] for row in ranking] == ["Ana", "Bruno", "Carla"]
    assert [row["rank_position"] for row in ranking] == [1, 2, 3]
    assert ranking[0]["total_events"] == 5


def test_ranking_respeita_o_parametro_top_n(make_silver_df):
    df = make_silver_df(_rows_for({"Ana": 4, "Bruno": 3, "Carla": 2, "Diego": 1}))

    assert rank_top_players(player_event_kpis(df), top_n=2).count() == 2
    assert rank_top_players(player_event_kpis(df), top_n=10).count() == 4


def test_empate_e_desempatado_de_forma_deterministica(make_silver_df):
    """Com o mesmo numero de eventos, o criterio e gols e depois o nome."""
    df = make_silver_df(_rows_for({"Zeca": 2, "Ana": 2, "Bruno": 2}))

    ranking = rank_top_players(player_event_kpis(df), top_n=3).orderBy("rank_position").collect()

    assert [row["player_name"] for row in ranking] == ["Ana", "Bruno", "Zeca"]


def test_ranking_e_independente_por_partida(make_silver_df):
    df = make_silver_df(
        _rows_for({"Ana": 3, "Bruno": 1}, match_id="MATCH-001")
        + _rows_for({"Carla": 2, "Diego": 5}, match_id="MATCH-002")
    )

    ranking = rank_top_players(player_event_kpis(df), top_n=1).collect()
    lideres = {row["match_id"]: row["player_name"] for row in ranking}

    assert lideres == {"MATCH-001": "Ana", "MATCH-002": "Diego"}


def test_ranking_e_independente_por_janela(make_silver_df):
    df = make_silver_df(
        _rows_for({"Ana": 3, "Bruno": 1}, offset=0)
        + _rows_for({"Ana": 1, "Bruno": 4}, offset=7)  # janela seguinte
    )

    ranking = rank_top_players(player_event_kpis(df), top_n=1).collect()

    assert len(ranking) == 2
    assert {row["player_name"] for row in ranking} == {"Ana", "Bruno"}


def test_ranking_expoe_o_contrato_de_colunas(make_silver_df):
    df = make_silver_df(_rows_for({"Ana": 1}))

    colunas = rank_top_players(player_event_kpis(df)).columns

    assert colunas == [
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
    ]
