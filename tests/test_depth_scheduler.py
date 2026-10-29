"""Планировщик стакана: подписка по требованию, TTL, вытеснение."""

from __future__ import annotations

import asyncio

from scanner.config import Config
from scanner.depth_scheduler import DepthScheduler
from scanner.instrument import InstrumentState
from scanner.models import Trade


class FakeStream:
    """Подменяет OkxPublicStream: журналирует вызовы, эмулирует успех."""

    def __init__(self) -> None:
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []
        self.fail = False

    async def subscribe(self, args: list[dict[str, str]]) -> bool:
        if self.fail:
            return False
        self.subscribed.extend(arg["instId"] for arg in args)
        return True

    async def unsubscribe(self, args: list[dict[str, str]]) -> bool:
        self.unsubscribed.extend(arg["instId"] for arg in args)
        return True


def make_state(inst_id: str, *, drop_pct: float, now: float) -> InstrumentState:
    """Состояние с просадкой заданной глубины: пик 30 c назад, цена ниже на ``drop_pct``."""
    state = InstrumentState(inst_id=inst_id, inst_type="SWAP", ct_val=1.0)
    peak = 100.0
    state.trades.add(Trade(inst_id, peak, 1.0, "buy", now - 30), now=now)
    state.trades.add(Trade(inst_id, peak * (1 + drop_pct / 100.0), 1.0, "sell", now - 1), now=now)
    return state


def _scheduler(
    states: dict[str, InstrumentState], *, slots: int, ttl: float = 600.0
) -> tuple[DepthScheduler, FakeStream]:
    cfg = Config()
    cfg.depth.mode = "ondemand"
    cfg.depth.book_slots = slots
    cfg.depth.book_ttl_s = ttl
    stream = FakeStream()
    return DepthScheduler(states, cfg, stream), stream  # type: ignore[arg-type]


def test_ondemand_subscribe(now: float) -> None:
    """Слот выдаётся только по запросу и живёт book_ttl_s после последнего want()."""
    states = {f"X{i}-USDT-SWAP": make_state(f"X{i}-USDT-SWAP", drop_pct=-4.0, now=now) for i in range(3)}
    sched, stream = _scheduler(states, slots=5, ttl=60.0)

    assert sched.active() == []
    asyncio.run(sched.tick(now=now))
    assert stream.subscribed == []  # без запроса ничего не подписывается

    sched.want("X0-USDT-SWAP", drop_pct=-4.0, now=now)
    asyncio.run(sched.tick(now=now))
    assert stream.subscribed == ["X0-USDT-SWAP"]
    assert sched.has_book("X0-USDT-SWAP")
    assert sched.subscribed_at("X0-USDT-SWAP") == now

    # повторный want не дублирует подписку, но продлевает жизнь
    sched.want("X0-USDT-SWAP", drop_pct=-5.0, now=now + 30)
    asyncio.run(sched.tick(now=now + 30))
    assert stream.subscribed == ["X0-USDT-SWAP"]

    asyncio.run(sched.tick(now=now + 80))
    assert sched.has_book("X0-USDT-SWAP")  # TTL от последнего want ещё не истёк

    asyncio.run(sched.tick(now=now + 200))
    assert not sched.has_book("X0-USDT-SWAP")
    assert stream.unsubscribed == ["X0-USDT-SWAP"]
    assert states["X0-USDT-SWAP"].book.authoritative is False  # отписанный стакан не выдаётся за живой


def test_slot_limit_and_eviction(now: float) -> None:
    """Слоты достаются самым глубоким просадкам; слабый ждёт освобождения."""
    depths = {"X0-USDT-SWAP": -3.0, "X1-USDT-SWAP": -6.0, "X2-USDT-SWAP": -9.0}
    states = {iid: make_state(iid, drop_pct=d, now=now) for iid, d in depths.items()}
    sched, stream = _scheduler(states, slots=2)

    for iid, d in depths.items():
        sched.want(iid, drop_pct=d, now=now)
    asyncio.run(sched.tick(now=now))

    assert set(sched.active()) == {"X1-USDT-SWAP", "X2-USDT-SWAP"}
    assert stream.subscribed == ["X2-USDT-SWAP", "X1-USDT-SWAP"]

    # TTL истёк, X0 всё ещё в очереди — он занимает освободившийся слот
    asyncio.run(sched.tick(now=now + 1000))
    assert "X0-USDT-SWAP" in sched.active()

    # очередь пуста — слоты действительно освобождаются
    asyncio.run(sched.tick(now=now + 3000))
    assert sched.active() == []


def test_deeper_drop_evicts_weaker_slot(now: float) -> None:
    states = {iid: make_state(iid, drop_pct=-3.0, now=now) for iid in ("A-USDT-SWAP", "B-USDT-SWAP")}
    sched, stream = _scheduler(states, slots=1)

    sched.want("A-USDT-SWAP", drop_pct=-3.0, now=now)
    asyncio.run(sched.tick(now=now))
    sched.want("B-USDT-SWAP", drop_pct=-8.0, now=now + 1)
    asyncio.run(sched.tick(now=now + 1))

    assert sched.active() == ["B-USDT-SWAP"]
    assert stream.unsubscribed == ["A-USDT-SWAP"]


def test_failed_subscribe_keeps_pending(now: float) -> None:
    """Неудачная подписка не создаёт слот: инструмент остаётся в очереди."""
    states = {"X0-USDT-SWAP": make_state("X0-USDT-SWAP", drop_pct=-5.0, now=now)}
    sched, stream = _scheduler(states, slots=3)
    stream.fail = True

    sched.want("X0-USDT-SWAP", drop_pct=-5.0, now=now)
    asyncio.run(sched.tick(now=now))
    assert not sched.has_book("X0-USDT-SWAP")

    stream.fail = False
    asyncio.run(sched.tick(now=now + 1))
    assert sched.has_book("X0-USDT-SWAP")
