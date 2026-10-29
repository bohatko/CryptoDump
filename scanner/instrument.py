"""Состояние одного инструмента: стакан, свечи, сделки, окна просадки, ликвидации."""

from __future__ import annotations

from dataclasses import dataclass, field

from .models import CANDLE_INTERVAL_S, Trade
from .orderbook import OrderBook
from .windows import CandleBook, DropWindows, LiquidationTracker, TradeWindow


@dataclass
class InstrumentState:
    """Всё состояние одного инструмента в одном месте."""

    inst_id: str
    inst_type: str
    ct_val: float = 1.0
    tick_sz: float = 0.0  # шаг цены: уровни плана сделки округляются до него
    turnover: float = 0.0  # оборот за 24ч в USDT: приоритет для слотов стакана
    max_window_s: int = 7200
    cvd_window_s: int = 300
    liq_window_s: int = 300
    precise_windows_until_s: int = 300
    book: OrderBook = field(init=False)
    candles: CandleBook = field(init=False)
    trades: TradeWindow = field(init=False)
    drops: DropWindows = field(init=False)
    liq: LiquidationTracker = field(init=False)
    last_trade: Trade | None = None

    def __post_init__(self) -> None:
        self.book = OrderBook(inst_id=self.inst_id, ct_val=self.ct_val)
        self.candles = CandleBook(self.inst_id, max_window_s=self.max_window_s)
        # Секундные бакеты нужны только для коротких окон (точный high) и CVD —
        # держать их все 2 часа незачем: 30m/1h/2h считаются по свечам, а в state.json
        # иначе уезжали бы лишние 7000 бакетов на инструмент.
        self.trades = TradeWindow(
            self.inst_id,
            max_window_s=max(self.precise_windows_until_s, self.cvd_window_s * 2, 600),
        )
        self.drops = DropWindows(
            self.trades,
            self.candles,
            precise_until_s=self.precise_windows_until_s,
        )
        # ликвидации приходят общим каналом на instType, но раскладываем их по инструментам
        self.liq = LiquidationTracker(max_window_s=max(self.liq_window_s * 2, 600))

    def price(self, *, now: float, stale_after_s: float) -> float | None:
        """Цена для расчёта просадки: свежая сделка, иначе закрытие последней свечи.

        Обе ветки проверяются на свежесть: если и сделки, и свечи давно не обновлялись
        (например, инструмент выпал из потока), цена считается неизвестной, иначе
        просадка посчиталась бы от исторического максимума к устаревшей цене.
        """
        trade = self.last_trade
        if trade is not None and (now - trade.ts) <= stale_after_s:
            return trade.px
        last_candle_ts = self.candles.last_ts()
        if last_candle_ts is None or (now - last_candle_ts) > stale_after_s + CANDLE_INTERVAL_S:
            return None
        return self.candles.last_price()
