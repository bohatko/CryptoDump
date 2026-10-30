"""План сделки: вход, стоп под опорой, цели по откату дампа, размер позиции."""

from __future__ import annotations

from conftest import make_book_summary

from scanner.config import TradeConfig
from scanner.models import Level, Trade
from scanner.plan import build_plan
from scanner.windows import CandleBook, DropWindows, TradeWindow


def _book(*, ask: float, wall_px: float | None, now: float):
    book = make_book_summary(now=now)
    book.best_ask = ask
    book.best_bid = ask - 0.0001
    book.wall = Level(px=wall_px, sz=1000.0, ts=now) if wall_px else None
    return book


def test_stop_under_wall_targets_by_retrace(now: float) -> None:
    """Пример из живого алерта CT: дамп 0.1426 → 0.1370, стена 0.136, шаг цены 0.0001."""
    book = _book(ask=0.1378, wall_px=0.136, now=now)
    plan = build_plan(TradeConfig(), price=0.1377, high=0.1426, low=0.1370, book=book, tick_sz=0.0001, now=now)
    assert plan is not None
    assert plan.entry == 0.1378
    assert plan.entry_max == 0.1382  # +0.3% и вниз до шага
    assert plan.stop_basis == "стеной 0.136"
    assert plan.stop == 0.1355  # 0.136 − 0.3% = 0.135592 → вниз до шага
    # 38.2% / 61.8% / 100% отката от дна 0.1370 к максимуму 0.1426, вниз до шага
    assert plan.targets == [0.1391, 0.1404, 0.1426]
    assert plan.rr[0] < plan.rr[1] < plan.rr[2]
    assert plan.valid_until == now + 900
    assert plan.qty is None  # риск в USDT не задан — размер не считаем


def test_levels_without_tick_keep_five_significant_digits(now: float) -> None:
    book = _book(ask=0.1378, wall_px=0.136, now=now)
    plan = build_plan(TradeConfig(), price=0.1377, high=0.1426, low=0.1370, book=book, now=now)
    assert plan is not None
    assert plan.stop == 0.13559
    assert plan.targets == [0.13913, 0.14046, 0.1426]


def test_stop_under_dump_bottom_without_wall(now: float) -> None:
    plan = build_plan(
        TradeConfig(), price=96.0, high=100.0, low=95.0, book=_book(ask=96.1, wall_px=None, now=now), now=now
    )
    assert plan is not None
    assert plan.stop_basis == "дном дампа 95"
    assert plan.stop == 94.715  # 95 − 0.3%


def test_wall_above_dump_bottom_does_not_place_stop_in_midair(now: float) -> None:
    """Цена уже ходила на 64.72 и отскочила к 65.41, стена встала на 65.25 — стоп ставим под дно 64.72."""
    plan = build_plan(
        TradeConfig(),
        price=65.40,
        high=69.95,
        low=64.72,
        book=_book(ask=65.41, wall_px=65.25, now=now),
        tick_sz=0.01,
        now=now,
    )
    assert plan is not None
    assert plan.stop_basis == "дном дампа 64.72"
    assert plan.stop == 64.52


def test_too_close_stop_is_widened(now: float) -> None:
    """Стена прямо под ценой: стоп 0.1% выбьет шум, отодвигаем до min_stop_pct."""
    plan = build_plan(
        TradeConfig(), price=100.0, high=104.0, low=99.9, book=_book(ask=100.0, wall_px=99.95, now=now), now=now
    )
    assert plan is not None
    assert abs(plan.stop_pct - (-0.6)) < 1e-9


def test_late_entry_and_far_stop_are_flagged(now: float) -> None:
    cfg = TradeConfig()
    late = build_plan(cfg, price=99.5, high=100.0, low=90.0, book=_book(ask=99.6, wall_px=None, now=now), now=now)
    assert late is not None
    assert late.targets == [100.0]
    assert any("поздний" in w for w in late.warnings)
    assert any("стоп далеко" in w for w in late.warnings)

    done = build_plan(cfg, price=100.0, high=100.0, low=90.0, book=_book(ask=100.05, wall_px=None, now=now), now=now)
    assert done is None  # целей выше входа нет — план не строим


def test_position_size_from_risk(now: float) -> None:
    cfg = TradeConfig(risk_usdt=10.0)
    plan = build_plan(
        cfg, price=96.0, high=100.0, low=95.0, book=_book(ask=96.0, wall_px=None, now=now), ct_val=0.1, now=now
    )
    assert plan is not None and plan.qty is not None
    assert abs(plan.qty * (plan.entry - plan.stop) - 10.0) < 1e-9  # на стопе теряем ровно риск
    assert abs(plan.contracts - plan.qty / 0.1) < 1e-9
    assert abs(plan.notional - plan.qty * 96.0) < 1e-9


def test_low_since_peak_uses_trades(now: float) -> None:
    trades = TradeWindow("X")
    for dt, px in ((-300, 99.0), (-200, 100.0), (-100, 94.0), (-10, 96.0)):
        trades.add(Trade("X", px, 1.0, "sell", now + dt), now=now)
    drops = DropWindows(trades, CandleBook("X"))
    assert drops.low_since(now - 200, now=now) == 94.0
    assert drops.low_since(now - 50, now=now) == 96.0
