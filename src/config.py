"""
Configuracoes centrais do pipeline `sports-realtime-pipeline-databricks`.

Este modulo e propositalmente livre de dependencias do PySpark: o gerador de
eventos precisa rodar em qualquer maquina (inclusive sem Java/Spark instalado).
Todos os caminhos podem ser sobrescritos por variaveis de ambiente, o que
permite usar exatamente o mesmo codigo localmente e no Databricks (DBFS).
"""

from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Deteccao de ambiente
# ---------------------------------------------------------------------------

#: Raiz do repositorio (usada apenas para os caminhos padrao locais).
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def is_databricks() -> bool:
    """Retorna True quando o codigo esta rodando dentro de um cluster Databricks.

    O Databricks Runtime sempre injeta a variavel ``DATABRICKS_RUNTIME_VERSION``
    nos executores e no driver, entao ela e um marcador confiavel.
    """
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _default_root(dbfs_path: str, local_dir: str) -> str:
    """Escolhe o caminho padrao conforme o ambiente.

    No Databricks usamos DBFS; localmente usamos pastas dentro do repositorio.
    O ``as_posix()`` garante barras normais tambem no Windows (o Spark aceita
    ``C:/Users/...`` mas nao lida bem com barras invertidas).
    """
    if is_databricks():
        return dbfs_path
    return (PROJECT_ROOT / local_dir).as_posix()


#: Pasta onde o gerador escreve os arquivos JSON (o nosso "topico Kafka").
DATA_ROOT: str = os.getenv("SPORTS_DATA_ROOT", _default_root("dbfs:/FileStore/sports_pipeline", "data"))

#: Raiz de todas as tabelas Delta e checkpoints.
DELTA_ROOT: str = os.getenv("SPORTS_DELTA_ROOT", _default_root("dbfs:/delta", "delta"))


# ---------------------------------------------------------------------------
# Caminhos das camadas (Medallion)
# ---------------------------------------------------------------------------

#: Pasta monitorada pelo `readStream` da camada Bronze.
STREAMING_PATH: str = f"{DATA_ROOT}/streaming"

#: Bronze: eventos crus, sem nenhuma transformacao de negocio.
BRONZE_PATH: str = f"{DELTA_ROOT}/bronze/events"

#: Silver: eventos limpos, validados e padronizados.
SILVER_PATH: str = f"{DELTA_ROOT}/silver/events_clean"

#: Silver: registros reprovados nas regras de qualidade (quarentena).
SILVER_QUARANTINE_PATH: str = f"{DELTA_ROOT}/silver/events_quarantine"

#: Gold: raiz dos KPIs em tempo real.
GOLD_ROOT: str = f"{DELTA_ROOT}/gold/kpis_realtime"

#: Gold: audiencia agregada por janela de tempo.
GOLD_AUDIENCE_PATH: str = f"{GOLD_ROOT}/audience_by_window"

#: Gold: contagem de eventos por jogador (tabela de apoio do ranking).
GOLD_PLAYER_EVENTS_PATH: str = f"{GOLD_ROOT}/player_events"

#: Gold: top N jogadores por janela de tempo.
GOLD_TOP_PLAYERS_PATH: str = f"{GOLD_ROOT}/top_players"

#: Gold: gols por time por janela de tempo.
GOLD_TEAM_GOALS_PATH: str = f"{GOLD_ROOT}/goals_by_team"


# ---------------------------------------------------------------------------
# Checkpoints (um por query de streaming)
# ---------------------------------------------------------------------------

CHECKPOINT_ROOT: str = f"{DELTA_ROOT}/checkpoints"

BRONZE_CHECKPOINT: str = f"{CHECKPOINT_ROOT}/bronze"
SILVER_CHECKPOINT: str = f"{CHECKPOINT_ROOT}/silver"
SILVER_QUARANTINE_CHECKPOINT: str = f"{CHECKPOINT_ROOT}/silver_quarantine"
GOLD_CHECKPOINT: str = f"{CHECKPOINT_ROOT}/gold"
GOLD_AUDIENCE_CHECKPOINT: str = f"{GOLD_CHECKPOINT}/audience"
GOLD_PLAYERS_CHECKPOINT: str = f"{GOLD_CHECKPOINT}/players"
GOLD_TEAM_GOALS_CHECKPOINT: str = f"{GOLD_CHECKPOINT}/team_goals"


# ---------------------------------------------------------------------------
# Regras de negocio
# ---------------------------------------------------------------------------

#: Tipos de evento aceitos na camada Silver (qualquer outro vai para quarentena).
VALID_EVENT_TYPES: tuple[str, ...] = ("gol", "cartao", "substituicao", "finalizacao")

#: Ultimo minuto valido de uma partida (acrescimos sao descartados por decisao de negocio).
MAX_MATCH_MINUTE: int = 90

#: Primeiro minuto valido.
MIN_MATCH_MINUTE: int = 0

#: Teto sanitario para a audiencia (valores acima disso sao considerados erro de telemetria).
MAX_AUDIENCE_COUNT: int = 100_000_000

#: Times canonicos usados pelo gerador.
CANONICAL_TEAMS: tuple[str, ...] = (
    "Flamengo",
    "Palmeiras",
    "Sao Paulo",
    "Corinthians",
    "Gremio",
    "Internacional",
    "Fluminense",
    "Atletico Mineiro",
)

#: Mapa de padronizacao de nomes de time.
#: A chave e o nome ja NORMALIZADO (minusculo, sem acento, sem pontuacao e com
#: espacos colapsados) e o valor e o nome canonico. A camada Silver normaliza a
#: string de entrada antes de consultar este mapa.
TEAM_NAME_MAP: dict[str, str] = {
    "flamengo": "Flamengo",
    "fla": "Flamengo",
    "cr flamengo": "Flamengo",
    "palmeiras": "Palmeiras",
    "verdao": "Palmeiras",
    "se palmeiras": "Palmeiras",
    "sao paulo": "Sao Paulo",
    "spfc": "Sao Paulo",
    "sao paulo fc": "Sao Paulo",
    "corinthians": "Corinthians",
    "timao": "Corinthians",
    "sc corinthians": "Corinthians",
    "gremio": "Gremio",
    "gremio fbpa": "Gremio",
    "internacional": "Internacional",
    "inter": "Internacional",
    "sc internacional": "Internacional",
    "fluminense": "Fluminense",
    "flu": "Fluminense",
    "fluminense fc": "Fluminense",
    "atletico mineiro": "Atletico Mineiro",
    "atletico mg": "Atletico Mineiro",
    "galo": "Atletico Mineiro",
}


# ---------------------------------------------------------------------------
# Parametros de streaming
# ---------------------------------------------------------------------------

#: Tamanho da janela deslizante usada nos KPIs da camada Gold.
WINDOW_DURATION: str = os.getenv("SPORTS_WINDOW_DURATION", "5 minutes")

#: Atraso maximo tolerado para eventos fora de ordem (late data).
WATERMARK_DELAY: str = os.getenv("SPORTS_WATERMARK_DELAY", "10 minutes")

#: Quantidade de jogadores no ranking da camada Gold.
TOP_N_PLAYERS: int = int(os.getenv("SPORTS_TOP_N_PLAYERS", "3"))

#: Intervalo de disparo das queries de streaming.
TRIGGER_INTERVAL: str = os.getenv("SPORTS_TRIGGER_INTERVAL", "10 seconds")

#: Limite de arquivos lidos por micro-batch na Bronze (controla o tamanho do batch).
MAX_FILES_PER_TRIGGER: int = int(os.getenv("SPORTS_MAX_FILES_PER_TRIGGER", "100"))


# ---------------------------------------------------------------------------
# Parametros do gerador de eventos
# ---------------------------------------------------------------------------

#: Intervalo entre lotes de eventos, em segundos.
GENERATOR_INTERVAL_SECONDS: float = float(os.getenv("SPORTS_GENERATOR_INTERVAL", "5"))

#: Quantidade de partidas simuladas simultaneamente.
GENERATOR_MATCHES: int = int(os.getenv("SPORTS_GENERATOR_MATCHES", "3"))

#: Eventos gerados por partida a cada ciclo.
GENERATOR_EVENTS_PER_TICK: int = int(os.getenv("SPORTS_GENERATOR_EVENTS_PER_TICK", "2"))

#: Fracao de eventos propositalmente "sujos", para exercitar a camada Silver.
GENERATOR_DIRTY_RATE: float = float(os.getenv("SPORTS_GENERATOR_DIRTY_RATE", "0.08"))

#: Distribuicao de tipos de evento (peso relativo).
EVENT_TYPE_WEIGHTS: dict[str, float] = {
    "finalizacao": 0.55,
    "cartao": 0.20,
    "substituicao": 0.15,
    "gol": 0.10,
}
