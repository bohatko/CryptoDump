"""Стакан: сборка из WS (snapshot + incremental) или REST-снимком.

Источник задаётся в ``Engine``: по умолчанию канал ``books`` (400 уровней,
апдейты ~100 мс), REST — резерв для ``depth.source == "rest"``.

Два правила, без которых инкрементальный стакан ломается:

**Свежесть только по стакану, не по уровню.** В канале ``books`` биржа присылает
в апдейте только изменившиеся уровни. Стоящая стена без изменения объёма не
попадает в апдейты, её ``ts`` не обновляется — и фильтр по ``stale_level_s``
выбрасывал бы ровно ту стену, которую мы ищем (и ровно на пороге
``wall_persist_s``). Поэтому в инкрементальном режиме свежесть проверяется
по ``updated_at`` всего стакана, а уровни не отбрасываются.

**Хранить глубже, чем считаем.** Стакан хранится целиком (до 400 уровней из
снимка), а срез до ``cfg.levels`` делается в ``summary()``. Иначе, когда верхние
уровни выедают, снизу нечего подтянуть и глубина временно падает ниже 20.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from .config import DepthConfig
from .models import BookSummary, Level

log = logging.getLogger(__name__)


@dataclass
class OrderBook:
    """Стакан инструмента.

    ``ct_val`` — размер одного контракта в базовой валюте (поле ``ctVal`` из
    ``/public/instruments``). OKX отдаёт ``sz`` в контрактах, поэтому без этого
    множителя номинал уровня был бы в контрактах, а не в USDT.
    """

    inst_id: str
    ct_val: float = 1.0
    bids: list[Level] = field(default_factory=list)
    asks: list[Level] = field(default_factory=list)
    updated_at: float = 0.0
    authoritative: bool = False  # снимок содержит всю запрошенную глубину, а не top-5
    exchange_ts: float | None = None  # время снимка по часам биржи (для диагностики)
    source: str = ""  # "ws" | "rest" — откуда пришли данные (диагностика)
    incremental: bool = False  # ведётся инкрементами WS: уровень может «молчать» и быть живым
    per_level_ts: bool = False  # у уровней есть собственное время (канал books с ts уровня)
    # цепочка sequence number для WS-инкрементов
    seq_id: int | None = None
    prev_seq_id: int | None = None
    gaps: int = 0  # сколько раз теряли целостность (для диагностики)
    # память о стене: время первого появления и последний подтверждённый ts
    _wall_px: float | None = None
    _wall_notional: float = 0.0
    _wall_since: float | None = None
    _wall_checked_at: float = 0.0

    # ---------- обновление ----------

    def apply_snapshot(self, raw: dict, *, max_levels: int, now: float | None = None) -> None:
        """Заменить стакан целиком из REST-ответа ``/market/books``.

        Уровни REST не содержат собственного времени, поэтому штампуем их локальным
        временем ответа: свежесть считается по нашим часам, и расхождение часов
        с биржей не приведёт к массовому отбрасыванию уровней.

        ``seq_id`` сбрасывается: REST-снимок — это отдельная ветка данных, и склеивать
        его с WS-инкрементами нельзя (у них своя нумерация). Если стакан ведётся
        инкрементами, такой снимок допустим только как полная замена.
        """
        now = time.time() if now is None else now
        self.bids = _parse_levels(raw.get("bids"), now, max_levels, ct_val=self.ct_val)
        self.asks = _parse_levels(
            raw.get("asks"), now, max_levels, ct_val=self.ct_val, descending=False
        )
        self.authoritative = True
        self.updated_at = now
        self.exchange_ts = _raw_ts(raw)
        self.source = "rest"
        self.incremental = False
        self.per_level_ts = False
        self.seq_id = self.prev_seq_id = None

    def apply_ws_snapshot(
        self,
        raw: dict,
        *,
        max_levels: int,
        level_ts_index: int | None = None,
        authoritative: bool = True,
        incremental: bool = True,
        now: float | None = None,
    ) -> None:
        """Полный снимок из WS-канала ``books``/``books5``."""
        now = time.time() if now is None else now
        self.bids = _parse_levels(
            raw.get("bids"),
            now,
            max_levels,
            ct_val=self.ct_val,
            level_ts_index=level_ts_index,
        )
        self.asks = _parse_levels(
            raw.get("asks"),
            now,
            max_levels,
            ct_val=self.ct_val,
            level_ts_index=level_ts_index,
            descending=False,
        )
        self.authoritative = authoritative
        self.updated_at = now
        self.source = "ws"
        self.incremental = incremental
        self.per_level_ts = level_ts_index is not None
        self.exchange_ts = _raw_ts(raw)
        self.seq_id = _as_int(raw.get("seqId"))
        self.prev_seq_id = _as_int(raw.get("prevSeqId"))

    def apply_ws_update(
        self,
        raw: dict,
        *,
        max_levels: int,
        level_ts_index: int | None = None,
        now: float | None = None,
    ) -> bool:
        """Применить инкрементальный апдейт канала ``books``.

        Возвращает ``False``, если применять нельзя: нет базы для сверки (стакан
        собран из REST, ``seqId`` неизвестен) или порвалась цепочка
        (``prevSeqId`` не совпал с последним ``seqId``). Вызывающий код в этом
        случае переподписывает канал и получает чистый WS-снимок.

        Накладывать WS-дельту на REST-снимок нельзя: у них разные точки отсчёта,
        и первый же апдейт дал бы расхождение с биржей.

        Формат строки уровня: ``[px, sz, liqOrders, orders]``; ``sz = "0"`` означает,
        что уровень удалён из стакана.
        """
        now = time.time() if now is None else now

        if self.seq_id is None:
            self.gaps += 1
            log.debug("%s: нет seqId (стакан из REST) — нужен WS-снимок", self.inst_id)
            return False

        incoming_prev = _as_int(raw.get("prevSeqId"))
        incoming_seq = _as_int(raw.get("seqId"))
        if incoming_prev is not None and incoming_prev != self.seq_id:
            self.gaps += 1
            log.debug(
                "%s: разрыв последовательности стакана (prev=%s, ожидался %s) — нужен ресинк",
                self.inst_id,
                incoming_prev,
                self.seq_id,
            )
            return False

        self.bids = _apply_side(
            self.bids, raw.get("bids"), now, max_levels, self.ct_val, level_ts_index, descending=True
        )
        self.asks = _apply_side(
            self.asks, raw.get("asks"), now, max_levels, self.ct_val, level_ts_index, descending=False
        )
        self.seq_id = incoming_seq if incoming_seq is not None else self.seq_id
        self.prev_seq_id = incoming_prev
        self.updated_at = now
        self.source = "ws"
        self.incremental = True
        self.exchange_ts = _raw_ts(raw) or self.exchange_ts
        return True

    # ---------- метрики ----------

    def summary(self, cfg: DepthConfig, *, now: float | None = None) -> BookSummary:
        now = time.time() if now is None else now

        # Свежесть. В инкрементальном режиме уровни не фильтруются по собственному ts:
        # биржа присылает только изменения, и неподвижная стена выглядела бы «мёртвой»
        # ровно через stale_level_s — то есть в момент подтверждения стены.
        # Живость определяется временем последнего апдейта стакана.
        fresh_overall = bool(self.updated_at) and (now - self.updated_at) <= cfg.stale_after_s
        if self.incremental or not self.per_level_ts:
            bids, asks = self.bids, self.asks
        else:
            bids = [lv for lv in self.bids if _fresh(lv, now, cfg.stale_level_s)]
            asks = [lv for lv in self.asks if _fresh(lv, now, cfg.stale_level_s)]

        band = band_notional(bids, asks, cfg.band_pct)

        # Срез до запрошенной глубины делается здесь, а не при хранении: когда верхние
        # уровни выедают, снизу должно что-то подтянуться.
        depth = max(1, cfg.levels)
        bids = bids[:depth]
        asks = asks[:depth]

        summary = BookSummary(
            inst_id=self.inst_id,
            ts=self.updated_at,
            best_bid=bids[0].px if bids else None,
            best_ask=asks[0].px if asks else None,
            bid_notional=sum(lv.notional for lv in bids),
            ask_notional=sum(lv.notional for lv in asks),
            levels=min(len(bids), len(asks)),
        )
        summary.fresh = fresh_overall
        summary.full_depth = self.authoritative and summary.levels > 0
        summary.band_pct = max(0.0, cfg.band_pct)
        summary.band_notional, summary.band_covered = band

        candidate, ratio = find_wall(bids, cfg)
        wall = self._confirm_wall(candidate, cfg, now)
        summary.wall = wall
        summary.wall_ratio = ratio if wall is not None else 0.0
        return summary

    def _confirm_wall(self, candidate: Level | None, cfg: DepthConfig, now: float) -> Level | None:
        """Считать стену настоящей только если она простояла ``wall_persist_s``.

        Проверка по времени, а не по числу опросов: с переходом на WS стакан
        обновляется ~10 раз в секунду, и «три снимка» означали бы 300 мс — спуф
        такой порог проходит.

        Уровень считается тем же, если цена совпадает (±0.2%) и номинал не изменился
        больше чем на 35% (стену могут частично исполнить, но не убрать).
        """
        if candidate is None:
            self._wall_px, self._wall_notional, self._wall_since = None, 0.0, None
            return None

        same_level = (
            self._wall_px is not None
            and _close(self._wall_px, candidate.px)
            and self._wall_notional > 0
            and abs(candidate.notional - self._wall_notional) / self._wall_notional <= 0.35
        )

        if not same_level:
            self._wall_since = now
        self._wall_px = candidate.px
        self._wall_notional = candidate.notional
        self._wall_checked_at = now

        held = now - (self._wall_since or now)
        if held < cfg.wall_persist_s:
            log.debug(
                "%s: стена %s × %.0f держится %.1f/%.0f c",
                self.inst_id,
                candidate.px,
                candidate.notional,
                held,
                cfg.wall_persist_s,
            )
            return None
        return candidate

    def needs_resync(self, cfg: DepthConfig, *, now: float | None = None) -> bool:
        """Пора ли перезапросить полный снимок (стакан протух или нет цепочки)."""
        now = time.time() if now is None else now
        if not self.updated_at:
            return True
        if self.incremental and self.seq_id is None:
            return True
        return (now - self.updated_at) > cfg.stale_after_s

    def liquid_enough(self, cfg: DepthConfig, *, now: float | None = None) -> bool:
        return self.summary(cfg, now=now).liquidity_notional >= cfg.min_notional_usdt

    @property
    def age(self) -> float:
        return time.time() - self.updated_at if self.updated_at else float("inf")


# ---------- внутреннее ----------


def _parse_levels(
    raw_levels: object,
    ts: float,
    max_levels: int,
    *,
    ct_val: float = 1.0,
    level_ts_index: int | None = None,
    descending: bool = True,
) -> list[Level]:
    if not isinstance(raw_levels, list):
        return []
    out: list[Level] = []
    for row in raw_levels[:max_levels]:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        try:
            px = float(row[0])
            contracts = float(row[1])
        except (TypeError, ValueError):
            continue
        if px <= 0 or contracts <= 0:
            continue
        level_ts = ts
        if level_ts_index is not None and len(row) > level_ts_index:
            try:
                level_ts_ms = float(row[level_ts_index])
                if level_ts_ms > 0:
                    level_ts = level_ts_ms / 1000.0
            except (TypeError, ValueError):
                level_ts = ts
        out.append(Level(px=px, sz=contracts * ct_val, ts=level_ts))
    out.sort(key=lambda lv: lv.px, reverse=descending)  # bids: сверху вниз, asks: снизу вверх
    return out


def _apply_side(
    levels: list[Level],
    raw_levels: object,
    ts: float,
    max_levels: int,
    ct_val: float,
    level_ts_index: int | None,
    *,
    descending: bool,
) -> list[Level]:
    """Применить инкременты к одной стороне стакана.

    ``sz == 0`` — удалить уровень. Хранится не больше ``max_levels`` уровней
    (запас глубины), а срез до ``cfg.levels`` делает ``summary()``: иначе при
    выедании верхних уровней снизу нечего подтянуть.
    """
    if not isinstance(raw_levels, list) or not raw_levels:
        return levels

    by_px: dict[float, Level] = {lv.px: lv for lv in levels}
    for row in raw_levels:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        try:
            px = float(row[0])
            contracts = float(row[1])
        except (TypeError, ValueError):
            continue
        if px <= 0:
            continue
        if contracts <= 0:
            by_px.pop(px, None)
            continue
        level_ts = ts
        if level_ts_index is not None and len(row) > level_ts_index:
            try:
                level_ts_ms = float(row[level_ts_index])
                if level_ts_ms > 0:
                    level_ts = level_ts_ms / 1000.0
            except (TypeError, ValueError):
                level_ts = ts
        by_px[px] = Level(px=px, sz=contracts * ct_val, ts=level_ts)

    ordered = list(by_px.values())
    ordered.sort(key=lambda lv: lv.px, reverse=descending)
    return ordered[:max_levels]


def _raw_ts(raw: dict) -> float | None:
    try:
        ts_ms = float(raw.get("ts") or 0.0)
    except (TypeError, ValueError):
        return None
    return ts_ms / 1000.0 if ts_ms > 0 else None


def _as_int(value: object) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def band_notional(bids: list[Level], asks: list[Level], band_pct: float) -> tuple[float, bool]:
    """Номинал обеих сторон в ±band_pct% от середины спреда и дотянулся ли стакан до краёв."""
    if band_pct <= 0 or not bids or not asks:
        return 0.0, True
    mid = (bids[0].px + asks[0].px) / 2
    lo, hi = mid * (1 - band_pct / 100), mid * (1 + band_pct / 100)
    notional = sum(lv.notional for lv in bids if lv.px >= lo) + sum(lv.notional for lv in asks if lv.px <= hi)
    return notional, bids[-1].px < lo and asks[-1].px > hi


def _fresh(level: Level, now: float, stale_s: float) -> bool:
    return (now - level.ts) <= stale_s


def _close(a: float, b: float, *, rel: float = 0.002) -> bool:
    """Совпадают ли цены с точностью 0.2% (уровень мог «дышать» на тик)."""
    if a <= 0 or b <= 0:
        return False
    return abs(a - b) / b <= rel


def find_wall(bids: list[Level], cfg: DepthConfig) -> tuple[Level | None, float]:
    """Найти стену поддержки на бидах.

    Две особенности реального стакана OKX, из-за которых «наивная» стена не работает:

    1. Самый верх стакана почти всегда держит основную массу объёма (у BTC-USDT-SWAP
       верхний бид — ~96% top-20), поэтому лучший бид стеной не считается
       (``wall_exclude_top_level``).
    2. База сравнения — **среднее** по остальным уровням, а не медиана: у BTC половина
       уровней top-20 это «пыль» по 16–500 USDT, и медиана даёт ratio в тысячи раз,
       из-за чего сигнатура срабатывала бы на любом мусоре. По среднему ratio
       получается 2–15× и порог в 4× имеет смысл.

    ``ratio`` — во сколько раз номинал уровня больше среднего уровня бидов.
    """
    if not bids:
        return None, 0.0

    # index 0 — лучший бид; при wall_exclude_top_level он не рассматривается
    start = 1 if cfg.wall_exclude_top_level else 0
    candidates = bids[start:]
    if not candidates:
        return None, 0.0

    base = sum(lv.notional for lv in candidates) / len(candidates)
    best = max(candidates, key=lambda lv: lv.notional)
    ratio = best.notional / base if base > 0 else 0.0
    if best.notional < cfg.wall_min_notional_usdt:
        return None, ratio
    if ratio < cfg.wall_multiplier:
        return None, ratio
    return best, ratio
