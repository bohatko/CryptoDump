"""Эпизоды просадки: что падает прямо сейчас и чем это закончилось.

Алертов мало (их режут фильтр ликвидности и скоринг), а для настройки порогов важно
видеть и те дампы, которые до алерта не дошли, и причину. Эпизод начинается, когда
инструмент впервые попал в просадку, и закрывается, если он ``gap_s`` секунд в ней
не появлялся. Закрытые эпизоды движок пишет в историю (``history.py``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .evaluate import STATUS_ALERT
from .models import DropHit


@dataclass(slots=True)
class DropEpisode:
    inst_id: str
    started_at: float
    last_seen: float
    window: str  # окно текущей самой глубокой просадки
    drop_pct: float  # текущая просадка (отрицательная)
    max_drop_pct: float  # самая глубокая за эпизод
    max_window: str
    price: float
    status: str  # последний итог оценки (см. evaluate.STATUS_*)
    detail: str = ""
    alerted: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "instId": self.inst_id,
            "startedAt": self.started_at,
            "lastSeen": self.last_seen,
            "durationS": round(self.last_seen - self.started_at, 1),
            "window": self.window,
            "dropPct": round(self.drop_pct, 3),
            "maxDropPct": round(self.max_drop_pct, 3),
            "maxWindow": self.max_window,
            "price": self.price,
            "status": self.status,
            "detail": self.detail,
            "alerted": self.alerted,
        }


class DropMonitor:
    def __init__(self, *, gap_s: float = 60.0) -> None:
        self.gap_s = gap_s
        self._open: dict[str, DropEpisode] = {}

    def observe(self, inst_id: str, drop: DropHit, status: str, detail: str = "", *, now: float) -> None:
        episode = self._open.get(inst_id)
        if episode is None:
            episode = DropEpisode(
                inst_id=inst_id,
                started_at=now,
                last_seen=now,
                window=drop.window_label,
                drop_pct=drop.drop_pct,
                max_drop_pct=drop.drop_pct,
                max_window=drop.window_label,
                price=drop.price,
                status=status,
            )
            self._open[inst_id] = episode
        episode.last_seen = now
        episode.window = drop.window_label
        episode.drop_pct = drop.drop_pct
        episode.price = drop.price
        episode.status = status
        episode.detail = detail
        if drop.drop_pct < episode.max_drop_pct:
            episode.max_drop_pct = drop.drop_pct
            episode.max_window = drop.window_label
        if status == STATUS_ALERT:
            episode.alerted = True

    def sweep(self, *, now: float) -> list[DropEpisode]:
        """Закрыть эпизоды, которые ``gap_s`` не обновлялись, и вернуть их."""
        closed = [ep for ep in self._open.values() if now - ep.last_seen > self.gap_s]
        for ep in closed:
            del self._open[ep.inst_id]
        return closed

    def drain(self) -> list[DropEpisode]:
        """Забрать все открытые эпизоды (на остановке — чтобы не потерять их в истории)."""
        episodes = list(self._open.values())
        self._open.clear()
        return episodes

    def active(self) -> list[DropEpisode]:
        return sorted(self._open.values(), key=lambda ep: ep.drop_pct)

    def get(self, inst_id: str) -> DropEpisode | None:
        return self._open.get(inst_id)
