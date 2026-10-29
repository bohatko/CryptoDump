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
from typing import Callable

from .config import Config
from .detection import detect_signals, passed_liquidity, score_ok
from .instrument import InstrumentState
from .models import Alert, BookSummary, DropHit, Signals

log = logging.getLogger(__name__)

# Чем закончилась оценка просадки — для дашборда: видно, на каком фильтре дампы
# не доходят до алерта, и какой порог крутить.
STATUS_NO_FLOW = "нет сделок"
STATUS_NO_BOOK = "ждёт стакан"
STATUS_THIN_BOOK = "тонкий стакан"
STATUS_LOW_SCORE = "мало сигнатур"
STATUS_ALERT = "алерт"

# (самое глубокое окно, статус, подробность)
DropObserver = Callable[[DropHit, str, str], None]


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
    observe: DropObserver | None = None,
) -> Alert | DropSeen | None:
    price = state.price(now=now, stale_after_s=cfg.engine.trade_stale_after_s)
    if not price:
        return None

    drops = state.drops.all_drops(cfg.window_thresholds(), price=price, now=now)
    if not drops:
        return None

    # из сработавших окон берём самое глубокое — оно информативнее
    deep = min(drops, key=lambda d: d.drop_pct)

    def report(status: str, detail: str = "") -> None:
        if observe is not None:
            observe(deep, status, detail)

    if not state.trades.has_data(now=now, window_s=cfg.cvd.window_s):
        # без сделок в окне CVD-сигнатуры ничего не значат
        report(STATUS_NO_FLOW)
        return None

    if not depth_available:
        # стакан не подключён: сообщаем о просадке, чтобы код запросил слот,
        # но алерта не строим — без стакана сигнатуры неполные
        report(STATUS_NO_BOOK)
        return DropSeen(drop=deep)

    book = state.book.summary(cfg.depth, now=now)
    if not passed_liquidity(cfg, book):
        log.debug("%s: просадка есть, стакан не прошёл фильтр (%.0f USDT)", state.inst_id, book.total_notional)
        report(STATUS_THIN_BOOK, _book_detail(book))
        return None

    cvd = state.trades.snapshot(window_s=cfg.cvd.window_s, burst_s=cfg.cvd.burst_s, now=now)
    liq = state.liq.snapshot(window_s=cfg.liquidation.window_s, now=now)
    reversal = state.trades.reversal_ratio(window_s=cfg.cvd.window_s, now=now)
    signals = detect_signals(cfg, book=book, cvd=cvd, liq=liq, reversal_ratio=reversal)

    if not score_ok(cfg, signals):
        report(STATUS_LOW_SCORE, _signals_detail(signals))
        return None

    report(STATUS_ALERT, _signals_detail(signals))
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


def _book_detail(book: BookSummary) -> str:
    parts = [f"{book.total_notional / 1e6:.2f}M USDT"]
    if not book.fresh:
        parts.append("протух")
    if not book.full_depth:
        parts.append("неполный")
    return ", ".join(parts)


def _signals_detail(signals: Signals) -> str:
    return f"{signals.score}/3: {', '.join(signals.hits) or '—'}"
