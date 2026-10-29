"""Сторож стакана: ресинк протухших и разорванных стаканов.

При ``depth.source == "ws"`` стакан приходит из канала ``books`` (~100 мс), и сторож
только чинит проблемы: переподписывает канал, если стакан протух или порвалась
цепочка ``seqId``. Подписка на ``books`` отдаёт полный snapshot с актуальным
``seqId``, поэтому REST для этого не нужен — а накладывать REST-снимок на
WS-инкременты нельзя, у них разные точки отсчёта.

REST-поллинг работает только в резервном режиме ``depth.source == "rest"``.

В режимах ``ondemand``/``top`` стакан подписан не у всех: сторож чинит только те,
для которых ``has_book`` возвращает ``True``. Остальные «протухли» по определению,
и переподписка вернула бы им дорогой канал в обход планировщика.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable

from .aio import sleep_or_stop
from .config import Config
from .instrument import InstrumentState
from .market import OkxRest, OkxRestError
from .okx_ws import OkxPublicStream

log = logging.getLogger(__name__)


class DepthGuard:
    def __init__(
        self,
        states: dict[str, InstrumentState],
        cfg: Config,
        rest: OkxRest,
        stop: asyncio.Event,
        *,
        has_book: Callable[[str], bool] | None = None,
    ) -> None:
        self.states = states
        self.cfg = cfg
        self.rest = rest
        self._stop = stop
        self.has_book = has_book or (lambda _inst_id: True)
        # заполняется движком после создания соединений: нужен поток с каналом books
        self.streams: list[OkxPublicStream] = []
        self._semaphore = asyncio.Semaphore(max(1, cfg.depth.concurrency))
        self._resync_queue: set[str] = set()  # инструменты, которым нужен полный снимок
        self._last_resync: dict[str, float] = {}  # когда последний раз переподписывали books

    def schedule_resync(self, state: InstrumentState) -> None:
        """Пометить инструмент на перезапрос полного снимка стакана."""
        self._resync_queue.add(state.inst_id)

    def pending_resync(self) -> set[str]:
        return set(self._resync_queue)

    async def run(self) -> None:
        ws_mode = self.cfg.depth.source == "ws"
        if ws_mode:
            # первый снимок придёт по подписке; ждём его и не трогаем REST
            await sleep_or_stop(max(1.0, self.cfg.depth.poll_interval_s), self._stop)
        else:
            await self.refresh_stale_books(force=True)

        interval = 1.0 if ws_mode else max(1.0, self.cfg.depth.poll_interval_s)
        while not self._stop.is_set():
            await sleep_or_stop(interval, self._stop)
            if self._stop.is_set():
                return
            try:
                if ws_mode:
                    await self.resync_ws_books()
                else:
                    await self.refresh_stale_books()
            except Exception:  # noqa: BLE001 — сторож не должен падать
                log.exception("ошибка обновления стакана")

    async def resync_ws_books(self, *, now: float | None = None) -> None:
        """Переподписать ``books`` для инструментов с протухшим или разорванным стаканом."""
        now = time.time() if now is None else now
        # Кулдаун: переподписка может не дать снимок (биржа не ответила, канал занят),
        # и без паузы инструмент переподписывался бы каждую секунду — это шторм
        # запросов и риск бана по лимитам.
        cooldown = max(5.0, self.cfg.depth.poll_interval_s)
        pending = [
            state
            for state in list(self.states.values())
            if self.has_book(state.inst_id)
            and (state.inst_id in self._resync_queue or state.book.needs_resync(self.cfg.depth, now=now))
            and (now - self._last_resync.get(state.inst_id, 0.0)) >= cooldown
        ]
        # разрывы у инструментов без стакана (слот уже отдан) чинить незачем
        self._resync_queue = {iid for iid in self._resync_queue if self.has_book(iid)}
        if not pending:
            return

        stream = next((s for s in self.streams if "books" in {sub.channel for sub in s.subscriptions}), None)
        if stream is None:
            log.warning("канал books не подключён — ресинк стакана невозможен")
            return

        for state in pending:
            self._last_resync[state.inst_id] = now
            ok = await stream.resubscribe({"channel": "books", "instId": state.inst_id})
            if ok:
                self._resync_queue.discard(state.inst_id)
                # до прихода нового snapshot помечаем стакан неавторитетным,
                # чтобы устаревшие метрики не попали в алерт
                state.book.authoritative = False
                state.book.gaps += 1
                log.info("%s: переподписка books для ресинка стакана", state.inst_id)

    async def refresh_stale_books(self, *, force: bool = False, now: float | None = None) -> None:
        """REST-режим: опросить инструменты, которым нужен свежий снимок."""
        now = time.time() if now is None else now
        pending = [
            state
            for state in list(self.states.values())
            if force or state.book.needs_resync(self.cfg.depth, now=now) or state.inst_id in self._resync_queue
        ]
        if not pending:
            return
        await asyncio.gather(*(self._poll(state) for state in pending), return_exceptions=True)

    async def _poll(self, state: InstrumentState) -> None:
        async with self._semaphore:
            if self._stop.is_set():
                return
            try:
                raw = await self.rest.books(state.inst_id, depth=self.cfg.depth.levels)
            except OkxRestError as exc:
                log.debug("стакан %s: %s", state.inst_id, exc)
                return
            state.book.apply_snapshot(raw, max_levels=self.cfg.depth.store_levels)
            self._resync_queue.discard(state.inst_id)
