"""
Gerador de eventos de partidas de futebol (simulacao de um topico Kafka).

Cada ciclo escreve um arquivo JSON Lines em ``data/streaming/``, que e a pasta
monitorada pelo `readStream` da camada Bronze. O arquivo e escrito primeiro com
o prefixo ``_tmp_`` e depois renomeado: o Spark ignora arquivos iniciados por
``_`` ou ``.``, e o rename dentro do mesmo diretorio e atomico, o que impede a
camada Bronze de ler um arquivo pela metade.

Execucao::

    python -m src.generator.event_generator                  # loop infinito
    python -m src.generator.event_generator --max-ticks 10   # 10 ciclos
    python -m src.generator.event_generator --seed 42        # deterministico

Interrompa com Ctrl+C: o encerramento e tratado e o ultimo arquivo nunca fica
pela metade.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import FrameType

from faker import Faker

from src import config

LOGGER = logging.getLogger(__name__)

#: Campos obrigatorios de um evento (usado nos testes e pela camada Bronze).
EVENT_FIELDS: tuple[str, ...] = (
    "event_id",
    "timestamp",
    "match_id",
    "minute",
    "event_type",
    "player_name",
    "team",
    "audience_count",
)


# ---------------------------------------------------------------------------
# Estado de uma partida
# ---------------------------------------------------------------------------


@dataclass
class Match:
    """Estado mutavel de uma partida simulada.

    Guardar estado (minuto corrente, elenco, audiencia base) faz os eventos
    gerados terem coerencia temporal em vez de serem ruido aleatorio puro.
    """

    match_id: str
    home_team: str
    away_team: str
    squads: dict[str, list[str]]
    base_audience: int
    minute: int = 0
    goals: dict[str, int] = field(default_factory=dict)

    def advance(self, rng: random.Random) -> None:
        """Avanca o relogio da partida em 1 a 3 minutos e reinicia no fim."""
        self.minute += rng.randint(1, 3)
        if self.minute > config.MAX_MATCH_MINUTE:
            # Fim de jogo: uma nova partida comeca com o mesmo confronto.
            self.minute = 0
            self.goals = {}

    def pick_team(self, rng: random.Random) -> str:
        return rng.choice([self.home_team, self.away_team])

    def pick_player(self, rng: random.Random, team: str) -> str:
        return rng.choice(self.squads[team])


def build_matches(
    fake: Faker,
    rng: random.Random,
    quantity: int,
    squad_size: int = 11,
) -> list[Match]:
    """Cria N partidas com confrontos distintos e elencos fixos.

    Os elencos sao gerados uma unica vez para que o mesmo jogador reapareca ao
    longo da partida - condicao necessaria para o ranking de jogadores da
    camada Gold fazer sentido.
    """
    if quantity < 1:
        raise ValueError("A quantidade de partidas deve ser >= 1.")

    available = list(config.CANONICAL_TEAMS)
    rng.shuffle(available)
    matches: list[Match] = []

    for index in range(quantity):
        # Reaproveita a lista de times ciclicamente se forem pedidas muitas partidas.
        home = available[(index * 2) % len(available)]
        away = available[(index * 2 + 1) % len(available)]
        squads = {
            home: [fake.name() for _ in range(squad_size)],
            away: [fake.name() for _ in range(squad_size)],
        }
        matches.append(
            Match(
                match_id=f"MATCH-{index + 1:03d}",
                home_team=home,
                away_team=away,
                squads=squads,
                base_audience=rng.randint(150_000, 3_000_000),
            )
        )

    return matches


# ---------------------------------------------------------------------------
# Geracao de eventos
# ---------------------------------------------------------------------------


def _weighted_event_type(rng: random.Random) -> str:
    """Sorteia um tipo de evento respeitando a distribuicao de `config`."""
    types = list(config.EVENT_TYPE_WEIGHTS)
    weights = [config.EVENT_TYPE_WEIGHTS[t] for t in types]
    return rng.choices(types, weights=weights, k=1)[0]


def _dirty_team_name(team: str, rng: random.Random) -> str:
    """Bagunca o nome do time para exercitar a padronizacao da camada Silver."""
    variants = [team.upper(), team.lower(), f"  {team} "]
    # Apelidos cadastrados no mapa de-para (`Fla`, `Timao`, `SPFC`, ...).
    variants.extend(alias for alias, canonical in config.TEAM_NAME_MAP.items() if canonical == team)
    return rng.choice(variants)


def generate_event(
    match: Match,
    fake: Faker,
    rng: random.Random,
    dirty_rate: float = 0.0,
    now: datetime | None = None,
) -> dict:
    """Gera um unico evento da partida informada.

    Funcao pura em relacao a I/O (nao escreve nada em disco), o que a torna
    facilmente testavel. Com `rng` semeado, o resultado e deterministico.

    Args:
        match: partida de origem (seu estado e atualizado: gols e minuto).
        fake: instancia do Faker (usada apenas para substitutos entrando em campo).
        rng: gerador de numeros aleatorios.
        dirty_rate: probabilidade de injetar um defeito proposital no evento.
        now: timestamp do evento; por padrao, o instante atual em UTC.

    Returns:
        Dicionario com as chaves de :data:`EVENT_FIELDS`.
    """
    event_time = now or datetime.now(timezone.utc)
    event_type = _weighted_event_type(rng)
    team = match.pick_team(rng)
    player = match.pick_player(rng, team)

    if event_type == "substituicao":
        # Um substituto novo entra e passa a fazer parte do elenco.
        player = fake.name()
        match.squads[team].append(player)
    if event_type == "gol":
        match.goals[team] = match.goals.get(team, 0) + 1

    # A audiencia cresce ao longo do jogo e dispara a cada gol marcado.
    goal_bonus = sum(match.goals.values()) * 0.05
    progress_bonus = (match.minute / config.MAX_MATCH_MINUTE) * 0.30
    noise = rng.uniform(-0.05, 0.05)
    audience = int(match.base_audience * (1 + goal_bonus + progress_bonus + noise))

    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": event_time.isoformat(),
        "match_id": match.match_id,
        "minute": match.minute,
        "event_type": event_type,
        "player_name": player,
        "team": team,
        "audience_count": audience,
    }

    if dirty_rate > 0 and rng.random() < dirty_rate:
        event = _inject_defect(event, rng)

    return event


def _inject_defect(event: dict, rng: random.Random) -> dict:
    """Corrompe um evento de forma controlada.

    Sem dado sujo na origem, as regras de qualidade da camada Silver seriam
    apenas decorativas. Cada defeito aqui tem uma regra correspondente la.
    """
    defect = rng.choice(
        ["minuto_invalido", "audiencia_negativa", "tipo_desconhecido", "time_sujo", "jogador_vazio"]
    )
    corrupted = dict(event)

    if defect == "minuto_invalido":
        corrupted["minute"] = rng.randint(config.MAX_MATCH_MINUTE + 1, 130)
    elif defect == "audiencia_negativa":
        corrupted["audience_count"] = -abs(corrupted["audience_count"])
    elif defect == "tipo_desconhecido":
        corrupted["event_type"] = rng.choice(["GOL!!", "penalti", "", "unknown"])
    elif defect == "time_sujo":
        corrupted["team"] = _dirty_team_name(corrupted["team"], rng)
    elif defect == "jogador_vazio":
        corrupted["player_name"] = rng.choice(["", "   ", None])

    return corrupted


def generate_batch(
    matches: list[Match],
    fake: Faker,
    rng: random.Random,
    events_per_match: int,
    dirty_rate: float = 0.0,
    duplicate_rate: float = 0.03,
) -> list[dict]:
    """Gera um lote de eventos avancando o relogio de todas as partidas.

    Um percentual dos eventos e reenviado identico (mesmo ``event_id``) para
    simular a entrega *at-least-once* tipica de um broker como o Kafka - e
    justificar a deduplicacao da camada Silver.
    """
    batch: list[dict] = []

    for match in matches:
        match.advance(rng)
        for _ in range(events_per_match):
            event = generate_event(match, fake, rng, dirty_rate=dirty_rate)
            batch.append(event)
            if duplicate_rate > 0 and rng.random() < duplicate_rate:
                batch.append(dict(event))

    return batch


# ---------------------------------------------------------------------------
# Escrita em disco
# ---------------------------------------------------------------------------


def write_batch(events: list[dict], output_dir: str | Path) -> Path | None:
    """Escreve o lote como JSON Lines de forma atomica.

    Retorna o caminho final do arquivo, ou None se o lote estiver vazio.
    """
    if not events:
        return None

    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    # O sufixo aleatorio evita colisao entre ciclos muito proximos.
    final_path = directory / f"events_{stamp}_{uuid.uuid4().hex[:8]}.json"
    # Prefixo `_` faz o Spark ignorar o arquivo enquanto ele esta sendo escrito.
    temp_path = directory / f"_tmp_{final_path.name}"

    payload = "\n".join(json.dumps(event, ensure_ascii=False) for event in events)
    temp_path.write_text(payload + "\n", encoding="utf-8")
    os.replace(temp_path, final_path)  # atomico no mesmo filesystem

    return final_path


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------


class _GracefulExit:
    """Captura SIGINT/SIGTERM para encerrar o loop sem stack trace."""

    def __init__(self) -> None:
        self.should_stop = False
        signal.signal(signal.SIGINT, self._handle)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum: int, frame: FrameType | None) -> None:
        LOGGER.info("Sinal %s recebido - encerrando apos o ciclo atual.", signum)
        self.should_stop = True


def run(
    output_dir: str | Path = config.STREAMING_PATH,
    interval_seconds: float = config.GENERATOR_INTERVAL_SECONDS,
    matches_count: int = config.GENERATOR_MATCHES,
    events_per_tick: int = config.GENERATOR_EVENTS_PER_TICK,
    dirty_rate: float = config.GENERATOR_DIRTY_RATE,
    max_ticks: int | None = None,
    seed: int | None = None,
) -> int:
    """Executa o loop de geracao ate ser interrompido ou atingir `max_ticks`.

    Returns:
        Numero total de eventos gerados.
    """
    rng = random.Random(seed)
    fake = Faker("pt_BR")
    if seed is not None:
        Faker.seed(seed)

    matches = build_matches(fake, rng, matches_count)
    LOGGER.info(
        "Gerando eventos de %s partida(s) a cada %.1fs em %s",
        len(matches),
        interval_seconds,
        output_dir,
    )
    for match in matches:
        LOGGER.info("  %s: %s x %s", match.match_id, match.home_team, match.away_team)

    guard = _GracefulExit()
    total_events = 0
    tick = 0

    while not guard.should_stop and (max_ticks is None or tick < max_ticks):
        tick += 1
        try:
            batch = generate_batch(matches, fake, rng, events_per_tick, dirty_rate=dirty_rate)
            path = write_batch(batch, output_dir)
            total_events += len(batch)
            LOGGER.info("Ciclo %s: %s eventos -> %s", tick, len(batch), path.name if path else "-")
        except OSError as exc:
            # Falha de escrita nao deve matar o gerador: registramos e seguimos.
            LOGGER.error("Falha ao escrever o lote %s: %s", tick, exc)

        if guard.should_stop or (max_ticks is not None and tick >= max_ticks):
            break
        time.sleep(interval_seconds)

    LOGGER.info("Gerador encerrado: %s ciclos, %s eventos.", tick, total_events)
    return total_events


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gerador de eventos de futebol em tempo real.")
    parser.add_argument("--output", default=config.STREAMING_PATH, help="Pasta de saida dos arquivos JSON.")
    parser.add_argument(
        "--interval", type=float, default=config.GENERATOR_INTERVAL_SECONDS, help="Segundos entre ciclos."
    )
    parser.add_argument(
        "--matches", type=int, default=config.GENERATOR_MATCHES, help="Partidas simuladas em paralelo."
    )
    parser.add_argument(
        "--events-per-tick",
        type=int,
        default=config.GENERATOR_EVENTS_PER_TICK,
        help="Eventos por partida em cada ciclo.",
    )
    parser.add_argument(
        "--dirty-rate",
        type=float,
        default=config.GENERATOR_DIRTY_RATE,
        help="Fracao de eventos com defeito proposital (0 a 1).",
    )
    parser.add_argument(
        "--max-ticks", type=int, default=None, help="Encerra apos N ciclos (padrao: infinito)."
    )
    parser.add_argument("--seed", type=int, default=None, help="Semente para geracao deterministica.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )
    args = _parse_args(argv)
    run(
        output_dir=args.output,
        interval_seconds=args.interval,
        matches_count=args.matches,
        events_per_tick=args.events_per_tick,
        dirty_rate=args.dirty_rate,
        max_ticks=args.max_ticks,
        seed=args.seed,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
