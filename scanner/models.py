"""Доменные модели: сделки, свечи, стакан, снимки метрик, алерт."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

from .config import WINDOW_LABELS

CANDLE_INTERVAL_S = 60.0  # 1m-свечи — база для всех окон


@dataclass(slots=True)
class Trade:
    """Одна сделка из канала ``trades``.

    ``side`` — сторона **тейкера**. ``sz`` уже переведён в базовую валюту
    (контракты × ``ctVal``), поэтому ``notional`` — это честные USDT.
    """

    inst_id: str
    px: float
    sz: float
    side: str  # "buy" | "sell"
    ts: float  # epoch seconds

    @property
    def notional(self) -> float:
        return self.px * self.sz

    @property
    def signed(self) -> float:
        return self.notional if self.side == "buy" else -self.notional


@dataclass(slots=True)
class Instrument:
    """Метаданные инструмента из ``/public/instruments``."""

    inst_id: str
    inst_type: str
    base: str
    settle_ccy: str
    ct_val: float = 1.0  # размер контракта в базовой валюте
    ct_mult: float = 1.0  # множитель контракта (у большинства инструментов 1)
    tick_sz: float = 0.0

    @property
    def contract_value(self) -> float:
        """Сколько базовой валюты в одном контракте: ``ctVal * ctMult``."""
        return self.ct_val * self.ct_mult


@dataclass(slots=True)
class Candle:
    """1m-свеча. ``ts`` — время открытия в epoch seconds."""

    ts: float
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    confirm: bool = False

    @property
    def end_ts(self) -> float:
        return self.ts + CANDLE_INTERVAL_S


@dataclass(slots=True)
class Level:
    """Уровень стакана. ``sz`` — в базовой валюте (контракты уже умножены на ``ctVal``)."""

    px: float
    sz: float
    ts: float

    @property
    def notional(self) -> float:
        return self.px * self.sz


@dataclass(slots=True)
class BookSummary:
    """Сжатое состояние стакана — то, что попадает в детекцию и в алерт."""

    inst_id: str
    ts: float
    best_bid: float | None = None
    best_ask: float | None = None
    bid_notional: float = 0.0
    ask_notional: float = 0.0
    levels: int = 0
    wall: Level | None = None
    wall_ratio: float = 0.0
    fresh: bool = True
    full_depth: bool = False  # снимок содержит все запрошенные уровни, а не только 5 из books5

    @property
    def total_notional(self) -> float:
        return self.bid_notional + self.ask_notional

    @property
    def imbalance(self) -> float:
        if self.ask_notional <= 0:
            return float("inf") if self.bid_notional > 0 else 1.0
        return self.bid_notional / self.ask_notional

    @property
    def mid(self) -> float | None:
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / 2.0
        return None


@dataclass(slots=True)
class DropHit:
    """Сработавшее окно просадки."""

    window_s: int
    drop_pct: float  # отрицательное число
    high: float
    price: float
    high_ts: float
    threshold_pct: float

    @property
    def window_label(self) -> str:
        return WINDOW_LABELS.get(self.window_s, f"{self.window_s}s")


@dataclass(slots=True)
class CvdSnapshot:
    """Поток агрессора за окно."""

    window_s: int
    cvd: float = 0.0
    buy_notional: float = 0.0
    sell_notional: float = 0.0
    burst_buy_notional: float = 0.0
    burst_sell_notional: float = 0.0

    @property
    def taker_ratio(self) -> float:
        if self.burst_sell_notional <= 0:
            return float("inf") if self.burst_buy_notional > 0 else 1.0
        return self.burst_buy_notional / self.burst_sell_notional


@dataclass(slots=True)
class LiquidationSnapshot:
    """Ликвидации: было/стало и признак затухания."""

    window_s: int
    earlier_notional: float = 0.0
    recent_notional: float = 0.0
    earlier_rate: float = 0.0
    recent_rate: float = 0.0

    @property
    def decay_ratio(self) -> float:
        if self.earlier_rate <= 0:
            return 1.0
        return self.recent_rate / self.earlier_rate


@dataclass(slots=True)
class Signals:
    """Результат работы детектора: какие сигнатуры откупа сработали."""

    bid_wall: bool = False
    cvd_fade: bool = False
    liq_decay: bool = False
    hits: list[str] = field(default_factory=list)

    @property
    def score(self) -> int:
        return sum((self.bid_wall, self.cvd_fade, self.liq_decay))


@dataclass(slots=True)
class Alert:
    """Готовый алерт. ``to_dict()`` — то, что уходит в JSONL и вебхук."""

    ts: float
    inst_id: str
    inst_type: str
    window_label: str
    window_s: int
    drop_pct: float
    price: float
    high: float
    book: BookSummary
    cvd: CvdSnapshot
    liq: LiquidationSnapshot
    signals: Signals
    # остальные окна, которые сработали в этом же дампе (гасятся кулдауном)
    sibling_windows: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": self.ts,
            "instId": self.inst_id,
            "instType": self.inst_type,
            "window": self.window_label,
            "windowSeconds": self.window_s,
            "dropPct": _round(self.drop_pct, 4),
            "price": self.price,
            "windowHigh": self.high,
            "book": {
                "bidNotional": _round(self.book.bid_notional, 2),
                "askNotional": _round(self.book.ask_notional, 2),
                "totalNotional": _round(self.book.total_notional, 2),
                "imbalance": _round(self.book.imbalance, 3),
                "levels": self.book.levels,
                "fullDepth": self.book.full_depth,
                "bestBid": self.book.best_bid,
                "bestAsk": self.book.best_ask,
                "wall": (
                    {
                        "px": self.book.wall.px,
                        "sz": self.book.wall.sz,
                        "notional": _round(self.book.wall.notional, 2),
                        "ratio": _round(self.book.wall_ratio, 2),
                    }
                    if self.book.wall
                    else None
                ),
            },
            "cvd": {
                "windowSeconds": self.cvd.window_s,
                "cvd": _round(self.cvd.cvd, 2),
                "takerRatio": _round(self.cvd.taker_ratio, 3),
                "buyNotional": _round(self.cvd.buy_notional, 2),
                "sellNotional": _round(self.cvd.sell_notional, 2),
            },
            "liquidations": {
                "windowSeconds": self.liq.window_s,
                "earlierNotional": _round(self.liq.earlier_notional, 2),
                "recentNotional": _round(self.liq.recent_notional, 2),
                "decayRatio": _round(self.liq.decay_ratio, 3),
            },
            "signals": {
                "score": self.signals.score,
                "hits": list(self.signals.hits),
            },
            "siblingWindows": list(self.sibling_windows),
        }

    def console_text(self) -> str:
        from datetime import datetime, timezone

        stamp = datetime.fromtimestamp(self.ts, tz=timezone.utc).strftime("%H:%M:%S")
        wall = self.book.wall
        wall_txt = (
            f"стена bid {_fmt_px(wall.px)} × {_fmt_money(wall.notional)} ({self.book.wall_ratio:.1f}× среднего)"
            if wall
            else "стены нет"
        )
        lines: Sequence[str] = (
            f"[{stamp}] ДАМП {self.inst_id}  окно {self.window_label}  {self.drop_pct:.2f}%  px {_fmt_px(self.price)}",
            f"  стакан: bid {_fmt_money(self.book.bid_notional)} / ask {_fmt_money(self.book.ask_notional)}"
            f"  imb {_fmt_ratio(self.book.imbalance)}  {wall_txt}",
            f"  CVD {self.cvd.window_s}s: {_fmt_money(self.cvd.cvd)}  taker {_fmt_ratio(self.cvd.taker_ratio)}"
            f"  ({'покупка' if self.cvd.burst_buy_notional > self.cvd.burst_sell_notional else 'продажа'})",
            f"  ликвидации {self.liq.window_s}s: {_fmt_money(self.liq.earlier_notional)} →"
            f" {_fmt_money(self.liq.recent_notional)}  затухание {_fmt_ratio(self.liq.decay_ratio)}",
            f"  score {self.signals.score}/3: {', '.join(self.signals.hits) or '—'}",
        )
        return "\n".join(lines)


def _round(value: float, digits: int) -> float | None:
    """Не даём ``inf``/``nan`` просочиться в JSONL и вебхук."""
    if value is None:
        return None
    if value != value or value in (float("inf"), float("-inf")):
        return None
    return round(value, digits)


def _fmt_ratio(value: float) -> str:
    if value != value:
        return "n/a"
    if value in (float("inf"), float("-inf")):
        return "∞"
    return f"{value:.2f}"


def _fmt_px(px: float) -> str:
    if px >= 1000:
        return f"{px:,.1f}"
    if px >= 1:
        return f"{px:.4f}".rstrip("0").rstrip(".")
    return f"{px:.8f}".rstrip("0").rstrip(".")


def _fmt_money(usdt: float) -> str:
    if abs(usdt) >= 1_000_000:
        return f"{usdt / 1_000_000:.2f}M"
    if abs(usdt) >= 1_000:
        return f"{usdt / 1_000:.0f}k"
    return f"{usdt:.0f}"
