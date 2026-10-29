"""Эпизоды просадки: открытие, глубина, закрытие по паузе."""

from __future__ import annotations

from scanner.evaluate import STATUS_ALERT, STATUS_LOW_SCORE, STATUS_NO_BOOK
from scanner.models import DropHit
from scanner.monitor import DropMonitor


def _drop(pct: float, window_s: int = 300) -> DropHit:
    price = 100.0 * (1 + pct / 100)
    return DropHit(window_s=window_s, drop_pct=pct, high=100.0, price=price, high_ts=0.0, threshold_pct=1.0)


def test_episode_tracks_deepest_drop_and_alert(now: float) -> None:
    mon = DropMonitor(gap_s=60)
    mon.observe("BTC", _drop(-2.0, 60), STATUS_NO_BOOK, now=now)
    mon.observe("BTC", _drop(-4.0, 300), STATUS_ALERT, "3/3", now=now + 2)
    mon.observe("BTC", _drop(-3.0, 300), STATUS_LOW_SCORE, "1/3", now=now + 4)

    [ep] = mon.active()
    assert ep.started_at == now and ep.last_seen == now + 4
    assert ep.max_drop_pct == -4.0 and ep.max_window == "5m"
    assert ep.drop_pct == -3.0 and ep.status == STATUS_LOW_SCORE
    assert ep.alerted  # алерт был внутри эпизода, даже если последний итог другой


def test_episode_closes_after_gap(now: float) -> None:
    mon = DropMonitor(gap_s=60)
    mon.observe("BTC", _drop(-2.0), STATUS_NO_BOOK, now=now)
    mon.observe("ETH", _drop(-5.0), STATUS_NO_BOOK, now=now + 50)

    assert mon.sweep(now=now + 60) == []
    closed = mon.sweep(now=now + 61)
    assert [ep.inst_id for ep in closed] == ["BTC"]
    assert [ep.inst_id for ep in mon.active()] == ["ETH"]
    assert [ep.inst_id for ep in mon.drain()] == ["ETH"] and mon.active() == []
