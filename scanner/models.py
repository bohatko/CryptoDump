"""Доменные модели: сделки, свечи, стакан, снимки метрик, алерт."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

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
    band_pct: float = 0.0  # 0 — ликвидность считается по levels уровням
    band_notional: float = 0.0  # заявки обеих сторон в ±band_pct% от середины спреда
    # хранимые уровни дотянулись до края полосы с обеих сторон; иначе band_notional —
    # оценка снизу (у плотного стакана 400 уровней OKX бывают уже ±1%)
    band_covered: bool = True

    @property
    def total_notional(self) -> float:
        return self.bid_notional + self.ask_notional

    @property
    def liquidity_notional(self) -> float:
        """Номинал, по которому работает фильтр ликвидности."""
        return self.band_notional if self.band_pct > 0 else self.total_notional

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
class TradePlan:
    """Что делать по алерту: лонг на откуп, вход, стоп, цели.

    Механический расчёт по уровням дампа (дно, максимум окна, стена покупателя),
    а не прогноз: решение и размер позиции остаются за человеком.
    """

    entry: float
    entry_max: float  # выше — не догонять
    stop: float
    stop_basis: str  # под чем стоит стоп: «стеной 0.136» / «дном дампа 0.1340»
    targets: list[float]
    rr: list[float]  # прибыль/риск до каждой цели
    low: float  # дно дампа
    valid_until: float
    qty: float | None = None  # в монетах, если задан риск в USDT
    contracts: float | None = None
    notional: float | None = None
    risk_usdt: float | None = None
    warnings: list[str] = field(default_factory=list)

    @property
    def stop_pct(self) -> float:
        return (self.stop / self.entry - 1.0) * 100.0

    def target_pcts(self) -> list[float]:
        return [(t / self.entry - 1.0) * 100.0 for t in self.targets]

    def to_dict(self) -> dict[str, Any]:
        return {
            "side": "long",
            "entry": self.entry,
            "entryMax": self.entry_max,
            "stop": self.stop,
            "stopPct": _round(self.stop_pct, 3),
            "stopBasis": self.stop_basis,
            "targets": list(self.targets),
            "targetPcts": [_round(p, 3) for p in self.target_pcts()],
            "rr": [_round(r, 2) for r in self.rr],
            "low": self.low,
            "validUntil": self.valid_until,
            "qty": self.qty,
            "contracts": self.contracts,
            "notional": _round(self.notional, 2) if self.notional is not None else None,
            "riskUsdt": self.risk_usdt,
            "warnings": list(self.warnings),
        }


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
    plan: TradePlan | None = None

    @property
    def base(self) -> str:
        return self.inst_id.split("-")[0]

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
                "bandPct": self.book.band_pct,
                "bandNotional": _round(self.book.band_notional, 2),
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
            "plan": self.plan.to_dict() if self.plan is not None else None,
        }

    def console_text(self) -> str:
        from datetime import datetime

        stamp = datetime.fromtimestamp(self.ts).strftime("%H:%M:%S")
        lines: list[str] = [
            f"[{stamp}] ПОКУПКА {self.base} ({self.inst_id}) — откуп после дампа {self.drop_pct:.2f}%"
            f" за {self.window_label}",
        ]
        plan = self.plan
        if plan is not None:
            targets = " · ".join(f"{_fmt_px(t)} ({p:+.1f}%)" for t, p in zip(plan.targets, plan.target_pcts()))
            lines += [
                f"  вход {_fmt_px(plan.entry)} по рынку, не выше {_fmt_px(plan.entry_max)}",
                f"  стоп {_fmt_px(plan.stop)} ({plan.stop_pct:.1f}%) — под {plan.stop_basis}",
                f"  цели {targets}",
                f"  прибыль/риск {' · '.join(f'{r:.1f}' for r in plan.rr)}"
                f"  до {datetime.fromtimestamp(plan.valid_until).strftime('%H:%M')}",
            ]
            if plan.qty is not None:
                lines.append(
                    f"  размер {_fmt_qty(plan.qty)} {self.base} ≈ {_fmt_money(plan.notional or 0.0)} USDT"
                    f" ({_fmt_qty(plan.contracts or 0.0)} контр.) при риске {plan.risk_usdt:g} USDT"
                )
            lines += [f"  ⚠ {w}" for w in plan.warnings]
        else:
            lines.append(f"  цена {_fmt_px(self.price)}, максимум окна {_fmt_px(self.high)}")
        lines.append(f"  сигналов {self.signals.score}/3: {', '.join(self.signals.hits) or '—'}")
        lines += [f"  · {reason}" for reason in self.reasons()]
        lines.append(
            f"  стакан {self.book.levels} ур.: покупатели {_fmt_money(self.book.bid_notional)}"
            f" / продавцы {_fmt_money(self.book.ask_notional)}"
        )
        return "\n".join(lines)

    def reasons(self) -> list[str]:
        """Почему это откуп, человеческими словами — по сработавшим сигнатурам."""
        out: list[str] = []
        wall = self.book.wall
        if self.signals.bid_wall and wall is not None:
            out.append(
                f"стена покупателя {_fmt_px(wall.px)} на {_fmt_money(wall.notional)} USDT"
                f" ({self.book.wall_ratio:.1f}× обычного уровня) держится и не снимается"
            )
        if self.signals.cvd_fade:
            buy, sell = self.cvd.burst_buy_notional, self.cvd.burst_sell_notional
            if sell > 0 and buy > sell:
                out.append(
                    f"покупатели перехватили инициативу: за минуту купили {_fmt_money(buy)}"
                    f" против {_fmt_money(sell)} продаж ({buy / sell:.1f}×)"
                )
            else:
                out.append("продавец выдыхается: в конце окна покупки отыграли большую часть продаж")
        if self.signals.liq_decay:
            out.append(
                f"каскад ликвидаций затух: {_fmt_money(self.liq.earlier_notional)} →"
                f" {_fmt_money(self.liq.recent_notional)} USDT"
            )
        return out


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


def _fmt_qty(value: float) -> str:
    if value >= 100:
        return f"{value:,.0f}".replace(",", " ")
    if value >= 1:
        return f"{value:.2f}".rstrip("0").rstrip(".")
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _fmt_money(usdt: float) -> str:
    if abs(usdt) >= 1_000_000:
        return f"{usdt / 1_000_000:.2f}M"
    if abs(usdt) >= 1_000:
        return f"{usdt / 1_000:.0f}k"
    return f"{usdt:.0f}"
