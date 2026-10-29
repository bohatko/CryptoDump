"""Оркестратор: горячая перезагрузка конфига и режимы покрытия стакана."""

from __future__ import annotations

import asyncio

from test_evaluate import _dumped_state

from scanner.config import Config, config_from_dict
from scanner.engine import BOOK_GRACE_S, Engine
from scanner.history import History


def _engine(**depth: object) -> Engine:
    cfg = Config()
    cfg.alerts.sinks = ["console"]
    cfg.engine.warmup_s = 0.0
    for key, value in depth.items():
        setattr(cfg.depth, key, value)
    return Engine(cfg, dry_run=True)


def test_apply_config_propagates_to_components() -> None:
    engine = _engine()
    fresh = config_from_dict({"windows": {"5m": 4.0}, "alerts": {"cooldown_s": 60, "sinks": ["console"]}})

    engine._apply_config(fresh)

    assert engine.cfg is fresh
    assert engine.router.cfg is fresh
    assert engine.depth.cfg is fresh
    assert engine.depth_scheduler is not None and engine.depth_scheduler.cfg is fresh
    assert engine.alerts.cooldown.cooldown_s == 60


def test_apply_config_keeps_restart_only_fields() -> None:
    engine = _engine()
    old_market = engine.cfg.market
    fresh = config_from_dict(
        {"market": {"top_n": 5}, "depth": {"levels": 50, "mode": "all", "min_notional_usdt": 2e6}}
    )

    engine._apply_config(fresh)

    assert engine.cfg.market is old_market
    assert engine.cfg.depth.levels == 20
    assert engine.cfg.depth.mode == "ondemand"  # планировщик создан под старый режим
    # пороги, которые можно менять на лету, применились
    assert engine.cfg.depth.min_notional_usdt == 2e6


def test_warmup_suppresses_evaluation() -> None:
    engine = _engine()
    engine.cfg.engine.warmup_s = 60.0
    engine._started_at = 1_000.0
    assert asyncio.run(engine._evaluate_all(now=1_000.0 + 59)) == []


def test_ondemand_dump_requests_book_slot(now: float) -> None:
    engine = _engine(mode="ondemand")
    state = _dumped_state(now)
    engine.states[state.inst_id] = state

    alerts = asyncio.run(engine._evaluate_all(now=now))

    assert alerts == []  # без подписанного стакана алерта нет
    assert engine.depth_scheduler is not None
    assert state.inst_id in engine.depth_scheduler._pending


def test_top_mode_uses_static_books(now: float) -> None:
    """top: стакан топа подписан сразу, а вне топа просадка не роняет оценку."""
    engine = _engine(mode="top", book_slots=1)
    assert engine.depth_scheduler is None

    inside = _dumped_state(now)
    outside = _dumped_state(now)
    outside.inst_id = "OUT-USDT-SWAP"
    inside.turnover, outside.turnover = 9e9, 1e6
    engine.states = {inside.inst_id: inside, outside.inst_id: outside}
    engine.router.states = engine.depth.states = engine.states
    engine._subscriptions()

    alerts = asyncio.run(engine._evaluate_all(now=now))
    assert [a.inst_id for a in alerts] == [inside.inst_id]


def test_rest_source_treats_every_book_as_available(now: float) -> None:
    engine = _engine(source="rest", mode="ondemand")
    assert engine.depth_scheduler is None
    state = _dumped_state(now)
    engine.states[state.inst_id] = state

    alerts = asyncio.run(engine._evaluate_all(now=now))
    assert [a.inst_id for a in alerts] == [state.inst_id]


def test_evaluation_feeds_history(now: float) -> None:
    """Опубликованный алерт и закрытый эпизод просадки попадают в историю."""
    engine = _engine(source="rest")
    engine.history = History(":memory:")
    state = _dumped_state(now)
    engine.states[state.inst_id] = state

    asyncio.run(engine._evaluate_all(now=now))
    assert engine.history.alerts()["total"] == 1
    assert [ep.inst_id for ep in engine.monitor.active()] == [state.inst_id]

    # цена протухла — просадки больше нет, через episode_gap_s эпизод закрывается
    asyncio.run(engine._evaluate_all(now=now + engine.cfg.dashboard.episode_gap_s + 1))
    episodes = engine.history.episodes()["items"]
    assert len(episodes) == 1 and episodes[0]["alerted"] is True
    assert engine.monitor.active() == []


def test_dashboard_fields_require_restart() -> None:
    engine = _engine()
    fresh = config_from_dict({"dashboard": {"port": 9999, "episode_gap_s": 120}})
    engine._apply_config(fresh)
    assert engine.cfg.dashboard.port == 8787
    assert engine.cfg.dashboard.episode_gap_s == 120 and engine.monitor.gap_s == 120


def test_guard_waits_for_first_snapshot_of_new_slot(now: float) -> None:
    engine = _engine(mode="ondemand")
    scheduler = engine.depth_scheduler
    assert scheduler is not None

    class Stream:
        async def subscribe(self, _args: list[dict[str, str]]) -> bool:
            return True

    scheduler.stream = Stream()  # type: ignore[assignment]
    engine.states["BTC-USDT-SWAP"] = _dumped_state(now)
    scheduler.want("BTC-USDT-SWAP", drop_pct=-4.0, now=now)
    asyncio.run(scheduler.tick(now=now))

    assert not engine._guard_owns("BTC-USDT-SWAP", now=now + 1)
    assert engine._guard_owns("BTC-USDT-SWAP", now=now + BOOK_GRACE_S)
    assert not engine._guard_owns("ETH-USDT-SWAP", now=now + BOOK_GRACE_S)
