"""Общие фабрики для тестов: синтетические снимки без сети."""

from __future__ import annotations

import pytest

from scanner.models import BookSummary, Candle, Level

NOW = 1_700_000_000.0


def make_candles(start: float, highs: list[float], last_close: float) -> list[Candle]:
    """Синтетические 1m-свечи: важны только ``high`` (по ним считается просадка)."""
    return [
        Candle(ts=start + i * 60, open=high, high=high, low=high * 0.99, close=last_close, confirm=True)
        for i, high in enumerate(highs)
    ]


def make_book_summary(inst_id: str = "TEST-USDT-SWAP", *, now: float = NOW) -> BookSummary:
    """Свежий полный стакан на 2.3M USDT с подтверждённой стеной 6×."""
    return BookSummary(
        inst_id=inst_id,
        ts=now,
        best_bid=99.9,
        best_ask=100.1,
        bid_notional=1_400_000.0,
        ask_notional=900_000.0,
        levels=20,
        wall=Level(px=99.9, sz=6000.0, ts=now),
        wall_ratio=6.0,
        fresh=True,
        full_depth=True,
    )


@pytest.fixture
def now() -> float:
    return NOW
