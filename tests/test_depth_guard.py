"""Сторож стакана: переподписка books и кулдаун ресинка."""

from __future__ import annotations

import asyncio

from scanner.config import Config
from scanner.depth_guard import DepthGuard
from scanner.instrument import InstrumentState
from scanner.market import OkxRest
from scanner.okx_ws import Subscription


class FakeStream:
    def __init__(self, channels: tuple[str, ...]) -> None:
        self.subscriptions = [Subscription(channel=ch, inst_id="BTC-USDT-SWAP") for ch in channels]
        self.resubscribed: list[dict[str, str]] = []

    async def resubscribe(self, arg: dict[str, str]) -> bool:
        self.resubscribed.append(arg)
        return True


def _fresh_ws_book(state: InstrumentState, now: float) -> None:
    levels = [["100", "1", "0", "1"]]
    state.book.apply_ws_snapshot(
        {"bids": levels, "asks": levels, "seqId": 1, "prevSeqId": -1},
        max_levels=20,
        authoritative=True,
        now=now,
    )


def _guard(now: float) -> tuple[DepthGuard, InstrumentState, FakeStream]:
    state = InstrumentState("BTC-USDT-SWAP", "SWAP")
    _fresh_ws_book(state, now)
    guard = DepthGuard({state.inst_id: state}, Config(), OkxRest("https://example.invalid"), asyncio.Event())
    stream = FakeStream(("trades", "books"))
    guard.streams = [FakeStream(("candle1m",)), stream]  # type: ignore[list-item]
    return guard, state, stream


def test_resync_resubscribes_books_on_gap(now: float) -> None:
    guard, state, stream = _guard(now)

    asyncio.run(guard.resync_ws_books(now=now))
    assert stream.resubscribed == []  # свежий стакан без разрыва не трогаем

    guard.schedule_resync(state)
    asyncio.run(guard.resync_ws_books(now=now))
    assert stream.resubscribed == [{"channel": "books", "instId": "BTC-USDT-SWAP"}]
    assert guard.pending_resync() == set()
    # до нового снимка стакан не должен попадать в алерты
    assert state.book.authoritative is False


def test_resync_cooldown_prevents_storm(now: float) -> None:
    """Пауза между ресинками одного стакана растёт вдвое: 5 → 10 → 20 c (лимит OKX 480/ч)."""
    guard, state, stream = _guard(now)
    base = max(5.0, guard.cfg.depth.poll_interval_s)

    guard.schedule_resync(state)
    asyncio.run(guard.resync_ws_books(now=now))
    guard.schedule_resync(state)
    asyncio.run(guard.resync_ws_books(now=now + 1))
    assert len(stream.resubscribed) == 1

    asyncio.run(guard.resync_ws_books(now=now + base + 0.1))
    assert len(stream.resubscribed) == 1  # вторая попытка ждёт уже 2 × base
    asyncio.run(guard.resync_ws_books(now=now + 2 * base + 0.1))
    assert len(stream.resubscribed) == 2
    asyncio.run(guard.resync_ws_books(now=now + 2 * base + 3 * base))
    assert len(stream.resubscribed) == 2  # третья — через 4 × base после второй
    assert guard.cooldown(state.inst_id) == 4 * base


def test_backoff_resets_after_book_recovers(now: float) -> None:
    guard, state, stream = _guard(now)
    guard.schedule_resync(state)
    asyncio.run(guard.resync_ws_books(now=now))
    assert guard.cooldown(state.inst_id) > max(5.0, guard.cfg.depth.poll_interval_s)

    later = now + 120
    _fresh_ws_book(state, later)  # пришёл свежий снимок, стакан минуту исправен
    asyncio.run(guard.resync_ws_books(now=later))
    assert guard.cooldown(state.inst_id) == max(5.0, guard.cfg.depth.poll_interval_s)


def test_stale_book_triggers_resync(now: float) -> None:
    guard, _state, stream = _guard(now)
    asyncio.run(guard.resync_ws_books(now=now + guard.cfg.depth.stale_after_s + 1))
    assert len(stream.resubscribed) == 1


def test_books_without_slot_are_not_resynced(now: float) -> None:
    """ondemand: инструмент без слота не должен получать books в обход планировщика."""
    guard, state, stream = _guard(now)
    guard.has_book = lambda _inst_id: False

    guard.schedule_resync(state)
    asyncio.run(guard.resync_ws_books(now=now + guard.cfg.depth.stale_after_s + 1))
    assert stream.resubscribed == []
    assert guard.pending_resync() == set()  # разрыв без владельца забыт, а не копится
