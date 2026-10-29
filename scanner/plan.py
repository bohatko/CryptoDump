"""План сделки по алерту: вход, стоп, цели, размер позиции.

Сканер ловит откуп после дампа, поэтому сделка одна — лонг. Уровни берутся из
самого дампа, а не из индикаторов:

* **вход** — лучший ask (покупка по рынку), догонять выше ``entry_slippage_pct`` не надо;
* **стоп** — под опорой: стеной покупателя, если она есть ниже входа (идея сделки —
  «стена держит»), иначе под дном дампа. Слишком близкий стоп отодвигается до
  ``min_stop_pct``: иначе его выбьет обычный шум спреда;
* **цели** — откат дампа от дна к максимуму окна на доли ``targets_retrace``
  (0.382 / 0.618 / 1.0 — классические уровни отката);
* **размер** — сколько монет купить, чтобы на стопе потерять ровно ``risk_usdt``.

Функция чистая: всё нужное приходит аргументами, время — через ``now``.
"""

from __future__ import annotations

import math

from .config import TradeConfig
from .models import BookSummary, TradePlan

# цель, до которой меньше 0.1%, — это уже не цель, а текущая цена
MIN_TARGET_GAP = 0.001
# отскок уже прошёл больше этой доли дампа — основная часть движения упущена
LATE_RECOVERY = 0.618


def build_plan(
    cfg: TradeConfig,
    *,
    price: float,
    high: float,
    low: float | None,
    book: BookSummary,
    ct_val: float = 1.0,
    tick_sz: float = 0.0,
    now: float,
) -> TradePlan | None:
    if not cfg.enabled or price <= 0 or high <= 0:
        return None

    entry = book.best_ask if book.best_ask and book.best_ask > 0 else price
    bottom = min(low, price) if low and low > 0 else price
    warnings: list[str] = []

    wall = book.wall
    if wall is not None and 0 < wall.px < entry:
        basis_px, basis = wall.px, f"стеной {_px(wall.px)}"
    else:
        basis_px, basis = bottom, f"дном дампа {_px(bottom)}"

    stop = basis_px * (1.0 - cfg.stop_buffer_pct / 100.0)
    closest = entry * (1.0 - cfg.min_stop_pct / 100.0)
    if stop > closest:
        stop = closest
    stop = _floor(stop, tick_sz)
    risk = entry - stop
    if risk <= 0:
        return None
    stop_pct = risk / entry * 100.0
    if stop_pct > cfg.max_stop_pct:
        warnings.append(f"стоп далеко ({stop_pct:.1f}%) — возьмите позицию меньше")

    span = high - bottom
    targets: list[float] = []
    for share in sorted(cfg.targets_retrace):
        target = _floor(bottom + span * share, tick_sz)
        if target > entry * (1.0 + MIN_TARGET_GAP) and target not in targets:
            targets.append(target)
    if not targets:
        return None
    recovered = (entry - bottom) / span if span > 0 else 0.0
    if recovered >= LATE_RECOVERY:
        warnings.append(f"цена уже отыграла {recovered:.0%} дампа — вход поздний")

    rr = [(t - entry) / risk for t in targets]
    key_rr = rr[1] if len(rr) > 1 else rr[0]
    if key_rr < cfg.min_rr:
        warnings.append(f"прибыль/риск {key_rr:.1f} ниже {cfg.min_rr:g} — слабая сделка")

    plan = TradePlan(
        entry=entry,
        entry_max=max(entry, _floor(entry * (1.0 + cfg.entry_slippage_pct / 100.0), tick_sz)),
        stop=stop,
        stop_basis=basis,
        targets=targets,
        rr=rr,
        low=bottom,
        valid_until=now + cfg.valid_s,
        warnings=warnings,
    )
    if cfg.risk_usdt > 0:
        plan.risk_usdt = cfg.risk_usdt
        plan.qty = cfg.risk_usdt / risk
        plan.notional = plan.qty * entry
        # OKX принимает размер ордера в контрактах: монеты / ctVal
        plan.contracts = plan.qty / ct_val if ct_val > 0 else None
    return plan


def _floor(px: float, tick: float) -> float:
    """Вниз до шага цены: стоп и цели лонга безопаснее чуть ниже, а биржа примет только кратное шагу."""
    if px <= 0:
        return px
    if tick <= 0:
        # шаг неизвестен — пяти значащих цифр хватает, чтобы не печатать шум вроде 0.1381131
        tick = 10.0 ** (math.floor(math.log10(px)) - 4)
    decimals = max(0, -math.floor(math.log10(tick)) + 1)
    # round до деления: 94.715 / 0.001 в двоичной арифметике даёт 94714.999…
    return round(math.floor(round(px / tick, 6)) * tick, decimals)


def _px(px: float) -> str:
    if px >= 1000:
        return f"{px:,.1f}"
    if px >= 1:
        return f"{px:.4f}".rstrip("0").rstrip(".")
    return f"{px:.8f}".rstrip("0").rstrip(".")
