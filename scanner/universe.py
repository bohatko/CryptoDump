"""Бутстрап из REST: вселенная инструментов и история 1m-свечей."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable

from .config import MarketConfig
from .instrument import InstrumentState
from .market import OkxRest, OkxRestError
from .models import Instrument
from .parsers import parse_candle, parse_instrument

log = logging.getLogger(__name__)


async def build_universe(rest: OkxRest, market: MarketConfig) -> list[tuple[Instrument, float]]:
    """Вселенная: SWAP + FUTURES по quote-валюте, отранжированные по обороту.

    Возвращает пары ``(инструмент, оборот_24ч_usdt)``: оборот нужен и для приоритета
    слотов стакана, и для выбора топ-N в режиме ``depth.mode == "top"``.

    ``top_n == 0`` → берём весь рынок (после отсечения мёртвых по обороту).
    Поток ``trades`` это позволяет: 511 инструментов дают ~400 сообщ/с.
    """
    wanted = {s.upper().strip() for s in market.symbols if s.strip()}
    candidates: list[tuple[Instrument, float]] = []

    for inst_type in market.inst_types:
        try:
            instruments = await rest.instruments(inst_type)
            turnovers = await rest.tickers(inst_type)
        except OkxRestError as exc:
            log.error("REST %s недоступен: %s", inst_type, exc)
            continue
        candidates.extend(select_candidates(instruments, inst_type, turnovers, market, wanted))

    # мусорные инструменты (6-20k оборота в сутки) слушать бессмысленно:
    # там нет ни глубины, ни движения, только трафик
    dropped = [c for c in candidates if c[1] < market.min_turnover_usdt]
    candidates = [c for c in candidates if c[1] >= market.min_turnover_usdt]
    if dropped and not wanted:
        log.info(
            "отсечено по обороту (< %.0fk/24ч): %d инструментов",
            market.min_turnover_usdt / 1e3,
            len(dropped),
        )

    # один базовый актив может дать несколько инструментов (перп + квартальный) —
    # это ожидаемо, берём все, но ранжируем по обороту
    candidates.sort(key=lambda item: item[1], reverse=True)
    if wanted or market.top_n <= 0:
        selected = candidates  # явный список баз или «весь рынок»
    else:
        selected = candidates[: market.top_n]

    for inst, turnover in selected[:10]:
        log.info(
            "  + %s (%s, 24h %.1fM, контракт=%.8g %s)",
            inst.inst_id,
            inst.inst_type,
            turnover / 1_000_000,
            inst.contract_value,
            inst.base,
        )
    if len(selected) > 10:
        log.info("  ... и ещё %d инструментов", len(selected) - 10)
    return selected


def select_candidates(
    rows: Iterable[dict],
    inst_type: str,
    turnovers: dict[str, float],
    market: MarketConfig,
    wanted: set[str],
) -> list[tuple[Instrument, float]]:
    """Отфильтровать сырые инструменты одного ``instType``: quote, исключения, список баз."""
    out: list[tuple[Instrument, float]] = []
    for row in rows:
        inst = parse_instrument(row, inst_type)
        if inst is None:
            continue
        if inst.settle_ccy.upper() != market.quote_ccy.upper():
            continue
        if any(pat in inst.inst_id for pat in market.exclude_patterns):
            continue
        if wanted and inst.base not in wanted:
            continue
        out.append((inst, turnovers.get(inst.inst_id, 0.0)))
    return out


async def bootstrap_candles(
    rest: OkxRest,
    states: Iterable[InstrumentState],
    *,
    limit: int,
    concurrency: int,
) -> None:
    """Подтянуть историю 1m-свечей, чтобы длинные окна работали сразу после старта."""
    sem = asyncio.Semaphore(max(1, concurrency))

    async def one(state: InstrumentState) -> None:
        async with sem:
            try:
                rows = await rest.candles(state.inst_id, bar="1m", limit=limit)
            except OkxRestError as exc:
                log.warning("свечи %s не получены: %s", state.inst_id, exc)
                return
            state.candles.seed([c for c in (parse_candle(r) for r in rows) if c is not None])

    await asyncio.gather(*(one(state) for state in states))
    log.info("история свечей загружена (по %d на инструмент)", limit)
