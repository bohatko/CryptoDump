"""Разбор сырых ответов OKX в доменные модели.

Здесь же живёт перевод контрактов в базовую валюту: OKX отдаёт ``sz`` у сделок,
стакана и ликвидаций в **контрактах**, и без ``ctVal`` все суммы получаются
в контрактах, а не в USDT.
"""

from __future__ import annotations

import time
from typing import Any

from .models import Candle, Instrument, Trade


def parse_instrument(row: dict[str, Any], inst_type: str) -> Instrument | None:
    inst_id = str(row.get("instId") or "")
    if not inst_id:
        return None
    return Instrument(
        inst_id=inst_id,
        inst_type=str(row.get("instType") or inst_type),
        base=inst_id.split("-")[0].upper(),
        settle_ccy=str(row.get("settleCcy") or row.get("quoteCcy") or ""),
        ct_val=_positive_float(row.get("ctVal"), 1.0),
        ct_mult=_positive_float(row.get("ctMult"), 1.0),
        tick_sz=_positive_float(row.get("tickSz"), 0.0),
    )


def parse_trade(inst_id: str, row: dict[str, Any], *, ct_val: float = 1.0) -> Trade | None:
    """``sz`` из канала ``trades`` — в контрактах, переводим в базовую валюту."""
    try:
        px = float(row.get("px") or 0.0)
        contracts = float(row.get("sz") or 0.0)
    except (TypeError, ValueError):
        return None
    if px <= 0 or contracts <= 0:
        return None
    side = str(row.get("side") or "").lower()
    if side not in ("buy", "sell"):
        return None
    return Trade(
        inst_id=inst_id,
        px=px,
        sz=contracts * ct_val,
        side=side,
        ts=_ms_to_s(row.get("ts")) or time.time(),
    )


def parse_candle(row: object) -> Candle | None:
    """1m-свеча из WS или REST: ``[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]``."""
    if not isinstance(row, (list, tuple)) or len(row) < 6:
        return None
    try:
        ts = float(row[0]) / 1000.0
        o = float(row[1])
        h = float(row[2])
        low = float(row[3])
        c = float(row[4])
        vol = float(row[5])
    except (TypeError, ValueError):
        return None
    confirm = len(row) > 8 and str(row[8]) == "1"
    return Candle(ts=ts, open=o, high=h, low=low, close=c, volume=vol, confirm=confirm)


def _ms_to_s(value: object) -> float:
    try:
        ms = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return ms / 1000.0 if ms > 0 else 0.0


def _positive_float(value: object, default: float) -> float:
    try:
        parsed = float(value or 0.0)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default
