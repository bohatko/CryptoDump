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
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Callable

from .config import Config
from .detection import book_ready, detect_signals, passed_liquidity, score_ok
from .instrument import InstrumentState
from .models import Alert, BookSummary, DropHit, Signals
from .plan import build_plan

log = logging.getLogger(__name__)

# Чем закончилась оценка просадки — для дашборда: видно, на каком фильтре дампы
# не доходят до алерта, и какой порог крутить.
STATUS_NO_FLOW = "нет сделок"
STATUS_NO_BOOK = "ждёт стакан"
STATUS_THIN_BOOK = "тонкий стакан"
STATUS_LOW_SCORE = "мало сигнатур"
STATUS_MARKET_DUMP = "общий пролив"
STATUS_ALERT = "алерт"

# Поводыри крипты и фондового рынка: когда они падают, остальные инструменты
# тянет за рынком, а не выкупают после одиночного дампа.
MARKET_LEADERS = ("BTC", "ETH", "SPY", "QQQ")

# (самое глубокое окно, статус, подробность)
DropObserver = Callable[[DropHit, str, str], None]


@dataclass(slots=True)
class MarketDropContext:
    """Снимок широты пролива и состояния поводырей на один проход оценки."""

    active_coins: int = 0
    dropping_coins: int = 0
    breadth_reason: str | None = None
    leaders: dict[str, tuple[str, float]] | None = None  # inst_id -> (base, drop_pct)

    def reason_for(self, inst_id: str) -> str | None:
        if self.breadth_reason is not None:
            return self.breadth_reason
        if self.leaders:
            for leader_id, (base, drop_pct) in self.leaders.items():
                if leader_id != inst_id:
                    return f"поводырь {base} {drop_pct:.1f}%"
        return None

    @property
    def summary(self) -> str | None:
        if self.breadth_reason is not None:
            return self.breadth_reason
        if self.leaders:
            base, drop_pct = next(iter(self.leaders.values()))
            return f"поводырь {base} {drop_pct:.1f}%"
        return None


def analyze_market_drop(
    states: Mapping[str, InstrumentState], cfg: Config, *, now: float
) -> MarketDropContext:
    """Проверить, не падает ли весь рынок разом или поводыри (BTC/ETH/SPY/QQQ)."""
    scfg = cfg.signals
    min_coins = max(1, scfg.market_drop_min_coins)
    if (scfg.max_market_drop_share <= 0 and scfg.leader_max_drop_pct <= 0) or len(states) < min_coins:
        return MarketDropContext()

    win_s = max(60, scfg.market_drop_window_s)
    thresholds = cfg.window_thresholds()
    short_thresholds = [(s, p) for s, p in thresholds if s <= win_s] or thresholds
    stale_s = cfg.engine.trade_stale_after_s

    active = 0
    dropping = 0
    leaders: dict[str, tuple[str, float]] = {}

    for state in states.values():
        price = state.price(now=now, stale_after_s=stale_s)
        if not price:
            continue
        active += 1
        in_drop = bool(state.drops.all_drops(short_thresholds, price=price, now=now))
        if not in_drop and scfg.leader_max_drop_pct > 0:
            in_drop = state.drops.drop(win_s, threshold_pct=scfg.leader_max_drop_pct, price=price, now=now) is not None
        if in_drop:
            dropping += 1
        if scfg.leader_max_drop_pct > 0:
            base = state.inst_id.split("-")[0].upper()
            if base in MARKET_LEADERS:
                hit = state.drops.drop(win_s, threshold_pct=scfg.leader_max_drop_pct, price=price, now=now)
                if hit is not None:
                    leaders[state.inst_id] = (base, hit.drop_pct)

    if active < min_coins:
        return MarketDropContext(active_coins=active, dropping_coins=dropping)

    breadth_reason = None
    if scfg.max_market_drop_share > 0 and dropping >= min_coins:
        share = dropping / active
        if share >= scfg.max_market_drop_share:
            breadth_reason = f"падает {dropping}/{active} рынка ({share * 100:.0f}%)"

    return MarketDropContext(
        active_coins=active,
        dropping_coins=dropping,
        breadth_reason=breadth_reason,
        leaders=leaders or None,
    )


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
    market_drop: str | None = None,
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
        if market_drop:
            # в общий пролив не просим новые слоты стакана, чтобы не забивать канал
            report(STATUS_MARKET_DUMP, market_drop)
            return None
        # стакан не подключён: сообщаем о просадке, чтобы код запросил слот,
        # но алерта не строим — без стакана сигнатуры неполные
        report(STATUS_NO_BOOK)
        return DropSeen(drop=deep)

    book = state.book.summary(cfg.depth, now=now)
    if not book_ready(cfg, book):
        # слот есть, но snapshot ещё не пришёл или стакан протух (его чинит DepthGuard):
        # это не «тонкий стакан» — иначе ликвидную монету отпустили бы на паузу
        report(STATUS_NO_BOOK, "стакан не готов")
        return DropSeen(drop=deep)
    if not passed_liquidity(cfg, book):
        log.debug("%s: просадка есть, стакан не прошёл фильтр (%.0f USDT)", state.inst_id, book.liquidity_notional)
        report(STATUS_THIN_BOOK, _book_detail(book))
        return None

    cvd = state.trades.snapshot(window_s=cfg.cvd.window_s, burst_s=cfg.cvd.burst_s, now=now)
    liq = state.liq.snapshot(window_s=cfg.liquidation.window_s, now=now)
    reversal = state.trades.reversal_ratio(window_s=cfg.cvd.window_s, now=now)
    signals = detect_signals(cfg, book=book, cvd=cvd, liq=liq, reversal_ratio=reversal)

    if not score_ok(cfg, signals):
        report(STATUS_LOW_SCORE, _signals_detail(signals))
        return None

    if market_drop:
        log.debug("%s: алерт отложен — общий пролив (%s)", state.inst_id, market_drop)
        report(STATUS_MARKET_DUMP, market_drop)
        return None

    report(STATUS_ALERT, _signals_detail(signals))
    plan = build_plan(
        cfg.trade,
        price=price,
        high=deep.high,
        low=state.drops.low_since(deep.high_ts, now=now),
        book=book,
        ct_val=state.ct_val,
        tick_sz=state.tick_sz,
        now=now,
    )
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
        plan=plan,
    )


def _book_detail(book: BookSummary) -> str:
    parts = [f"{book.liquidity_notional / 1e6:.2f}M USDT"]
    if book.band_pct > 0:
        parts[0] += f" в ±{book.band_pct:g}%"
    if not book.fresh:
        parts.append("протух")
    if not book.full_depth:
        parts.append("неполный")
    return ", ".join(parts)


def _signals_detail(signals: Signals) -> str:
    return f"{signals.score}/3: {', '.join(signals.hits) or '—'}"
