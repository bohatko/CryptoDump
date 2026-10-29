"""Оценка одного инструмента: просадка → фильтр ликвидности → сигнатуры → ``Alert``.

Чистая функция над ``InstrumentState``: без сети и без побочных эффектов,
кроме подтверждения стены внутри ``OrderBook.summary()``.

Отличие для широкой вселенной (``depth.mode != "all"``): если стакан для инструмента
ещё не подключён, оценка **не блокируется** — возвращается просадка без сигнатур
(``DropSeen``), и вызывающий код запрашивает слот стакана. Так детекция работает по
всем 511 инструментам, а дорогой канал ``books`` поднимается только под дампы.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .config import Config
from .detection import detect_signals, passed_liquidity, score_ok
from .instrument import InstrumentState
from .models import Alert, DropHit

log = logging.getLogger(__name__)


@dataclass(slots=True)
class DropSeen:
    """Просадка найдена, но стакана ещё нет — нужен слот ``books``."""

    drop: DropHit


def evaluate_instrument(
    state: InstrumentState,
    cfg: Config,
    *,
    now: float,
    depth_available: bool = True,
) -> Alert | DropSeen | None:
    price = state.price(now=now, stale_after_s=cfg.engine.trade_stale_after_s)
    if not price:
        return None

    drops = state.drops.all_drops(cfg.window_thresholds(), price=price, now=now)
    if not drops:
        return None

    if not state.trades.has_data(now=now, window_s=cfg.cvd.window_s):
        # без сделок в окне CVD-сигнатуры ничего не значат
        return None

    # из сработавших окон берём самое глубокое — оно информативнее
    deep = min(drops, key=lambda d: d.drop_pct)

    if not depth_available:
        # стакан не подключён: сообщаем о просадке, чтобы код запросил слот,
        # но алерта не строим — без стакана сигнатуры неполные
        return DropSeen(drop=deep)

    book = state.book.summary(cfg.depth, now=now)
    if not passed_liquidity(cfg, book):
        log.debug("%s: просадка есть, стакан не прошёл фильтр (%.0f USDT)", state.inst_id, book.total_notional)
        return None

    cvd = state.trades.snapshot(window_s=cfg.cvd.window_s, burst_s=cfg.cvd.burst_s, now=now)
    liq = state.liq.snapshot(window_s=cfg.liquidation.window_s, now=now)
    reversal = state.trades.reversal_ratio(window_s=cfg.cvd.window_s, now=now)
    signals = detect_signals(cfg, book=book, cvd=cvd, liq=liq, reversal_ratio=reversal)

    if not score_ok(cfg, signals):
        return None

    return Alert(
        ts=now,
        inst_id=state.inst_id,
        inst_type=state.inst_type,
        window_label=deep.window_label,
        window_s=deep.window_s,
        drop_pct=deep.drop_pct,
        price=price,
        high=deep.high,
        book=book,
        cvd=cvd,
        liq=liq,
        signals=signals,
        sibling_windows=[d.window_label for d in drops if d.window_s != deep.window_s],
    )
