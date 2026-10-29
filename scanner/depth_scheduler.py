"""Стакан по требованию: подписка на ``books`` только там, где он нужен.

Зачем. Канал ``books`` (400 уровней, апдейты ~100 мс) стоит ~28 МБ/час на инструмент.
На 511 USDT-перпетуалах это ~13 ГБ/час — держать его на всём рынке нельзя.
А ``trades`` на том же рынке стоит ~300 МБ/час, поэтому просадку дешевле считать по
сделкам на всех инструментах, а стакан поднимать только когда дамп уже случился.

Схема:

* ``trades`` + ``candle1m`` — на все инструменты, всегда;
* дамп найден (просадка прошла порог окна) → инструменту выделяется слот и
  подписывается ``books``;
* стакан живёт ``book_ttl_s`` после последнего срабатывания, затем отписывается
  и слот возвращается;
* если дампов больше, чем слотов, приоритет у самых глубоких просадок: при
  переполнении вытесняется инструмент с наименьшей текущей просадкой.

Так детекция работает по всему рынку, а дорогие данные платятся только за те
инструменты, которые прямо сейчас интересны.

Каждая выдача и освобождение слота — запрос к OKX, а их 480 в час на соединение.
Поэтому слот не отбирается раньше ``book_min_hold_s`` (иначе две просадки забирают
его друг у друга каждую секунду), а освобождённый инструмент ``book_retry_s`` не
получает стакан снова — кроме случая, когда его просадка стала в 1.5 раза глубже.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

from .config import Config
from .instrument import InstrumentState
from .okx_ws import OkxPublicStream

log = logging.getLogger(__name__)

# во сколько раз должна углубиться просадка, чтобы стакан вернули раньше book_retry_s
RETRY_DEEPER = 1.5


@dataclass
class Slot:
    """Выделенный слот стакана."""

    inst_id: str
    subscribed_at: float
    last_used: float
    drop_pct: float = 0.0


@dataclass
class DepthScheduler:
    """Управляет горячим списком ``books`` при ``depth.mode != "all"``."""

    states: dict[str, InstrumentState]
    cfg: Config
    # Поток появляется позже: движок создаёт соединения в отдельной задаче,
    # а планировщик должен пережить этот момент (иначе его задача стартует
    # раньше, чем появится возможность подписываться).
    stream: OkxPublicStream | None = None
    _slots: dict[str, Slot] = field(default_factory=dict)
    _pending: dict[str, float] = field(default_factory=dict)  # inst_id -> последняя известная просадка
    _released: dict[str, tuple[float, float]] = field(default_factory=dict)  # inst_id -> (когда, просадка)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    # ---------- учёт потребности ----------

    def want(self, inst_id: str, *, drop_pct: float, now: float | None = None) -> None:
        """Отметить, что инструменту нужен стакан (вызывается при срабатывании окна).

        Если слот уже есть — продлеваем жизнь и обновляем глубину просадки
        (по ней решается, кого вытеснять при переполнении).
        """
        now = time.time() if now is None else now
        slot = self._slots.get(inst_id)
        if slot is not None:
            slot.last_used = now
            slot.drop_pct = min(slot.drop_pct, drop_pct)  # просадка отрицательная
            return
        # просадку запоминаем: по ней строится очередь, когда слотов не хватает
        self._pending[inst_id] = min(self._pending.get(inst_id, drop_pct), drop_pct)

    def has_book(self, inst_id: str) -> bool:
        return inst_id in self._slots

    def subscribed_at(self, inst_id: str) -> float | None:
        slot = self._slots.get(inst_id)
        return slot.subscribed_at if slot is not None else None

    def active(self) -> list[str]:
        return list(self._slots)

    def slots(self) -> list[Slot]:
        return list(self._slots.values())

    def pending(self) -> dict[str, float]:
        return dict(self._pending)

    def cooling(self) -> dict[str, float]:
        """Инструменты, которым стакан пока не выдаётся повторно: inst_id → секунд до конца паузы."""
        now = time.time()
        retry = self.cfg.depth.book_retry_s
        return {iid: retry - (now - at) for iid, (at, _drop) in self._released.items() if now - at < retry}

    async def release(self, inst_id: str) -> bool:
        """Освободить слот вручную (из дашборда)."""
        async with self._lock:
            self._pending.pop(inst_id, None)
            if inst_id not in self._slots:
                return False
            await self._release(inst_id, now=time.time())
            return True

    # ---------- обслуживание ----------

    async def run(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            if self.stream is None:
                # соединение ещё не создано — просто ждём, работу не теряем
                continue
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — планировщик не должен падать
                log.exception("ошибка планировщика стакана")

    async def tick(self, *, now: float | None = None) -> None:
        """Один проход: вытеснить протухшие слоты, выдать слоты ожидающим."""
        now = time.time() if now is None else now
        async with self._lock:
            await self._evict_expired(now)
            await self._grant_pending(now)

    async def _evict_expired(self, now: float) -> None:
        ttl = max(30.0, self.cfg.depth.book_ttl_s)
        expired = [inst_id for inst_id, slot in self._slots.items() if now - slot.last_used > ttl]
        for inst_id in expired:
            await self._release(inst_id, now=now)
        if expired:
            log.debug("освобождено слотов стакана: %d", len(expired))
        retry = self.cfg.depth.book_retry_s
        self._released = {iid: rel for iid, rel in self._released.items() if now - rel[0] < retry}

    def _cooling_down(self, inst_id: str, drop_pct: float, now: float) -> bool:
        released = self._released.get(inst_id)
        if released is None:
            return False
        at, was = released
        if now - at >= self.cfg.depth.book_retry_s:
            del self._released[inst_id]
            return False
        # просадки отрицательные: «в 1.5 раза глубже» = меньше в 1.5 раза
        return drop_pct > was * RETRY_DEEPER

    async def _grant_pending(self, now: float) -> None:
        if not self._pending:
            return

        slots_limit = max(1, self.cfg.depth.book_slots)
        # Самые глубокие просадки получают слот первыми. Берём просадку, сохранённую
        # в want(): она пришла из той же оценки, что и запрос, поэтому не зависит от
        # того, «свежее» ли сейчас состояние инструмента.
        queue = sorted(self._pending.items(), key=lambda item: item[1])

        for inst_id, candidate_drop in queue:
            if inst_id in self._slots:
                self._pending.pop(inst_id, None)
                continue
            if self._cooling_down(inst_id, candidate_drop, now):
                # стакан только что был и ничего не дал — очередь не занимаем,
                # want() вернёт инструмент сюда, если просадка продолжится
                self._pending.pop(inst_id, None)
                continue

            # Если слотов нет — вытесняем только того, кто СЛАБЕЕ кандидата.
            # Просадка отрицательна: глубже = меньше.
            while len(self._slots) >= slots_limit:
                victim = self._weakest_slot(now)
                if victim is None or victim.drop_pct <= candidate_drop:
                    log.debug(
                        "слоты стакана заняты, %s ждёт (%.2f%% против %.2f%%)",
                        inst_id,
                        candidate_drop,
                        victim.drop_pct if victim else 0.0,
                    )
                    return
                log.debug(
                    "вытесняю %s (%.2f%%) ради %s (%.2f%%)",
                    victim.inst_id,
                    victim.drop_pct,
                    inst_id,
                    candidate_drop,
                )
                await self._release(victim.inst_id, now=now)

            if await self._subscribe(inst_id):
                self._pending.pop(inst_id, None)
                self._slots[inst_id] = Slot(
                    inst_id=inst_id,
                    subscribed_at=now,
                    last_used=now,
                    drop_pct=candidate_drop,
                )

    def _weakest_slot(self, now: float) -> Slot | None:
        """Слот с наименее интересным инструментом (ближайшая к нулю просадка) среди тех,
        что прожили ``book_min_hold_s``: свежий слот ещё не успел дать стакан."""
        hold = self.cfg.depth.book_min_hold_s
        candidates = [s for s in self._slots.values() if now - s.subscribed_at >= hold]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.drop_pct)

    async def _subscribe(self, inst_id: str) -> bool:
        if self.stream is None:
            return False
        ok = await self.stream.subscribe([{"channel": "books", "instId": inst_id}])
        if ok:
            log.info("%s: стакан подключён по требованию", inst_id)
        return ok

    async def _release(self, inst_id: str, *, now: float) -> None:
        slot = self._slots.pop(inst_id, None)
        if slot is not None:
            self._released[inst_id] = (now, slot.drop_pct)
        if self.stream is not None:
            await self.stream.unsubscribe([{"channel": "books", "instId": inst_id}])
        state = self.states.get(inst_id)
        if state is not None:
            # старые данные больше не обновляются — не выдаём их за живые
            state.book.authoritative = False
        log.info("%s: стакан отключён (слот освобождён)", inst_id)
