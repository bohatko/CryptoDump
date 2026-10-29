"""Скользящие окна: просадка (трейды + свечи), CVD (поток агрессора), ликвидации."""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass

from .models import CANDLE_INTERVAL_S, Candle, CvdSnapshot, DropHit, LiquidationSnapshot, Trade

log = logging.getLogger(__name__)


@dataclass(slots=True)
class _Second:
    """Секундный бакет сделок: нужен и для CVD, и для точного максимума коротких окон."""

    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    buy: float = 0.0
    sell: float = 0.0

    def add(self, trade: Trade) -> None:
        if trade.px > self.high or self.high == 0.0:
            self.high = trade.px
        if trade.px < self.low or self.low == 0.0:
            self.low = trade.px
        self.close = trade.px
        if trade.side == "buy":
            self.buy += trade.notional
        else:
            self.sell += trade.notional


class TradeWindow:
    """Секундные бакеты сделок: точные high/low и CVD для коротких окон.

    Зачем не 1m-свечи. ``CandleBook`` считает максимум по свечам, пересекающим
    границу окна, поэтому в окно 1m попадает максимум почти за 2 минуты, а в 5m —
    почти за 6. Это завышает просадку и даёт ложные срабатывания. Здесь максимум
    берётся по реальным сделкам внутри окна — точность до секунды.

    Бакеты — по аналогии с CVD: на активном инструменте за 2 часа приходят сотни
    тысяч сделок, хранить их все нельзя, а секундных бакетов максимум 7200.
    """

    def __init__(self, inst_id: str, *, max_window_s: int = 7200) -> None:
        self.inst_id = inst_id
        self.max_window_s = max_window_s
        self._seconds: dict[int, _Second] = {}

    def add(self, trade: Trade, *, now: float | None = None) -> None:
        second = int(trade.ts)
        bucket = self._seconds.get(second)
        if bucket is None:
            bucket = _Second()
            self._seconds[second] = bucket
        bucket.add(trade)
        self._prune(int(trade.ts if now is None else now) - self.max_window_s)

    def _prune(self, cutoff: int) -> None:
        if len(self._seconds) <= self.max_window_s + 1:
            return
        for second in [s for s in self._seconds if s < cutoff]:
            del self._seconds[second]

    def trim(self, *, now: float) -> None:
        self._prune(int(now) - self.max_window_s)

    # ---------- максимум и цена ----------

    def high(self, window_s: int, *, now: float) -> tuple[float, float] | None:
        """``(максимум, когда он был)`` по сделкам внутри окна."""
        start = now - window_s
        best = 0.0
        best_ts = 0.0
        for second, bucket in self._seconds.items():
            if second < start or second > now:
                continue
            if bucket.high > best:
                best = bucket.high
                best_ts = float(second)
        if best <= 0:
            return None
        return best, best_ts

    def last_price(self) -> float | None:
        if not self._seconds:
            return None
        newest = self._seconds[max(self._seconds)]
        return newest.close or None

    def last_ts(self) -> float | None:
        return float(max(self._seconds)) if self._seconds else None

    def has_data(self, *, now: float, window_s: int) -> bool:
        start = now - window_s
        return any(second >= start for second in self._seconds)

    # ---------- CVD ----------

    def snapshot(self, *, window_s: int, burst_s: int, now: float) -> CvdSnapshot:
        start = now - window_s
        burst_start = now - burst_s

        buy = 0.0
        sell = 0.0
        burst_buy = 0.0
        burst_sell = 0.0
        for second, bucket in self._seconds.items():
            if second < start:
                continue
            buy += bucket.buy
            sell += bucket.sell
            if second >= burst_start:
                burst_buy += bucket.buy
                burst_sell += bucket.sell

        return CvdSnapshot(
            window_s=window_s,
            cvd=buy - sell,
            buy_notional=buy,
            sell_notional=sell,
            burst_buy_notional=burst_buy,
            burst_sell_notional=burst_sell,
        )

    def reversal_ratio(self, *, window_s: int, now: float) -> float:
        """Насколько покупатель откупил из того, что продали, в последней трети окна.

        ``1.0`` — выкуп полностью перекрыл продажи первых двух третей, ``0.0`` — не было покупок.
        """
        if window_s < 3:
            return 0.0
        start = now - window_s
        third = window_s / 3.0
        last_cut = start + 2 * third

        early = 0.0
        late = 0.0
        for second, bucket in self._seconds.items():
            if second < start or second > now:
                continue
            signed = bucket.buy - bucket.sell
            if second < last_cut:
                early += signed
            else:
                late += signed

        if early >= 0:
            # Продаж в начале окна не было — «разворот» не о чем говорить.
            return 0.0
        return late / abs(early)


class CandleBook:
    """1m-свечи инструмента и расчёт просадки по длинным окнам.

    Для окон 30m/1h/2h секундные бакеты не нужны: погрешность в одну минуту на
    двухчасовом окне — это 0.8%, а память под 7200 бакетов на каждый инструмент
    тратится зря. Короткие окна (1m/5m) считаются по ``TradeWindow``.
    """

    def __init__(self, inst_id: str, *, max_window_s: int = 7200) -> None:
        self.inst_id = inst_id
        self.max_window_s = max_window_s
        self._candles: dict[float, Candle] = {}

    def __len__(self) -> int:
        return len(self._candles)

    # ---------- запись ----------

    def upsert(self, candle: Candle) -> None:
        self._candles[candle.ts] = candle
        self._prune(candle.ts)

    def seed(self, candles: list[Candle]) -> None:
        for candle in candles:
            self._candles[candle.ts] = candle
        if candles:
            self._prune(max(c.ts for c in candles))
        log.debug("%s: забутстраплено %d свечей", self.inst_id, len(self._candles))

    def _prune(self, newest_ts: float) -> None:
        cutoff = newest_ts - self.max_window_s - 2 * CANDLE_INTERVAL_S
        for ts in [ts for ts in self._candles if ts < cutoff]:
            del self._candles[ts]

    # ---------- чтение ----------

    def slice(self, window_s: int, *, now: float) -> list[Candle]:
        """Свечи, пересекающие окно ``[now - window_s, now]``, по возрастанию времени."""
        start = now - window_s
        end = now
        return [c for ts, c in sorted(self._candles.items()) if c.end_ts > start and c.ts <= end]

    def high(self, window_s: int, *, now: float) -> tuple[float, float] | None:
        """``(максимум, когда он был)`` за окно."""
        candles = self.slice(window_s, now=now)
        if not candles:
            return None
        peak = max(candles, key=lambda c: c.high)
        return peak.high, peak.ts

    def last_price(self, *, fallback: float | None = None) -> float | None:
        if not self._candles:
            return fallback
        newest = self._candles[max(self._candles)]
        return newest.close or fallback

    def last_ts(self) -> float | None:
        """Время открытия самой свежей свечи (``None``, если данных нет)."""
        if not self._candles:
            return None
        return max(self._candles)


class LiquidationTracker:
    """Ликвидации по инструментам (события приходят пачками по ``instType``)."""

    def __init__(self, *, max_window_s: int = 600) -> None:
        self.max_window_s = max_window_s
        self._events: deque[tuple[float, float]] = deque()  # (ts, notional)

    def add(self, ts: float, notional: float, *, now: float | None = None) -> None:
        self._events.append((ts, notional))
        cutoff = (time.time() if now is None else now) - self.max_window_s
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    def snapshot(self, *, window_s: int, now: float) -> LiquidationSnapshot:
        """Сравнить темп ликвидаций в первых 2/3 окна и в последней 1/3."""
        start = now - window_s
        third = max(window_s / 3.0, 1.0)
        early_cut = now - third

        earlier = 0.0
        recent = 0.0
        for ts, notional in self._events:
            if ts < start or ts > now:
                continue
            if ts < early_cut:
                earlier += notional
            else:
                recent += notional

        earlier_span = max((early_cut - start), 1.0)
        return LiquidationSnapshot(
            window_s=window_s,
            earlier_notional=earlier,
            recent_notional=recent,
            earlier_rate=earlier / earlier_span,
            recent_rate=recent / third,
        )

    def trim(self, *, now: float) -> None:
        cutoff = now - self.max_window_s
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    def __len__(self) -> int:
        return len(self._events)


def drop_from_high(
    *,
    high: float,
    high_ts: float,
    price: float,
    window_s: int,
    threshold_pct: float,
) -> DropHit | None:
    """Собрать ``DropHit``, если просадка от максимума достигла порога."""
    if high <= 0 or price <= 0:
        return None
    drop_pct = (price / high - 1.0) * 100.0
    if -drop_pct < threshold_pct:
        return None
    return DropHit(
        window_s=window_s,
        drop_pct=drop_pct,
        high=high,
        price=price,
        high_ts=high_ts,
        threshold_pct=threshold_pct,
    )


class DropWindows:
    """Просадка по окнам: короткие — по трейдам, длинные — по 1m-свечам.

    Порог переключения — ``precise_until_s``: окна не длиннее него считаются по
    секундным бакетам (точность до секунды), длиннее — по свечам (погрешность
    в одну минуту на окне 30m+ уже несущественна, а память экономится).

    Если по трейдам данных ещё нет (инструмент только что добавлен, WS не успел),
    короткое окно честно считается по свечам: лучше грубая оценка, чем пропуск дампа.
    """

    def __init__(
        self,
        trade_window: TradeWindow,
        candles: CandleBook,
        *,
        precise_until_s: int = 300,
    ) -> None:
        self.trades = trade_window
        self.candles = candles
        self.precise_until_s = precise_until_s

    def all_drops(
        self,
        thresholds: list[tuple[int, float]],
        *,
        price: float,
        now: float,
    ) -> list[DropHit]:
        hits: list[DropHit] = []
        for window_s, threshold_pct in thresholds:
            hit = self.drop(window_s, threshold_pct=threshold_pct, price=price, now=now)
            if hit is not None:
                hits.append(hit)
        return hits

    def drop(self, window_s: int, *, threshold_pct: float, price: float, now: float) -> DropHit | None:
        source = self._peak(window_s, now=now)
        if source is None:
            return None
        high, high_ts = source
        return drop_from_high(
            high=high,
            high_ts=high_ts,
            price=price,
            window_s=window_s,
            threshold_pct=threshold_pct,
        )

    def _peak(self, window_s: int, *, now: float) -> tuple[float, float] | None:
        if window_s <= self.precise_until_s and self.trades.has_data(now=now, window_s=window_s):
            found = self.trades.high(window_s, now=now)
            if found is not None:
                return found
        return self.candles.high(window_s, now=now)
