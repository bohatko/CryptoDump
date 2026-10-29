"""Разбор сообщений WS по каналам и раскладка данных в ``InstrumentState``.

Каналы ``trades``, ``candle1m``, ``books``, ``books5`` приходят по ``instId``;
``liquidation-orders`` — общим потоком на ``instType`` и раскладывается по
инструментам из ``details``.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from .config import Config
from .instrument import InstrumentState
from .parsers import parse_candle, parse_trade

log = logging.getLogger(__name__)

LIQUIDATION_CHANNELS = frozenset({"liquidation-orders", "liquidation-orders-filtered"})

ChannelHandler = Callable[[InstrumentState, dict, list[dict], float], None]


class WsRouter:
    """Обработчик ``OkxPublicStream``: канал → функция, без логики детекции.

    ``on_book_gap`` вызывается, когда инкремент ``books`` нельзя применить
    (нет ``seqId`` или порвалась цепочка) — стакану нужен чистый WS-снимок.
    """

    def __init__(
        self,
        states: dict[str, InstrumentState],
        cfg: Config,
        *,
        on_book_gap: Callable[[InstrumentState], None],
    ) -> None:
        self.states = states
        self.cfg = cfg
        self._on_book_gap = on_book_gap
        self._handlers: dict[str, ChannelHandler] = {
            "trades": self._trades,
            "candle1m": self._candles,
            "books": self._books,
            "books5": self._books5,
        }

    async def handle(self, payload: dict) -> None:
        """Сигнатура совпадает с ``MessageHandler`` из ``okx_ws``."""
        self.route(payload)

    def route(self, payload: dict, *, now: float | None = None) -> None:
        now = time.time() if now is None else now
        arg = payload.get("arg") or {}
        channel = arg.get("channel")
        data = payload.get("data")
        if not isinstance(data, list):
            return

        if channel in LIQUIDATION_CHANNELS:
            self._liquidations(data, now)
            return

        handler = self._handlers.get(channel)
        inst_id = arg.get("instId")
        state = self.states.get(inst_id) if inst_id else None
        if handler is None or state is None:
            return
        handler(state, payload, data, now)

    # ---------- каналы по instId ----------

    def _trades(self, state: InstrumentState, _payload: dict, data: list[dict], now: float) -> None:
        for row in data:
            trade = parse_trade(state.inst_id, row, ct_val=state.ct_val)
            if trade is None:
                continue
            # данные приходят пачкой и не гарантируют порядок — держим самую свежую сделку
            if state.last_trade is None or trade.ts >= state.last_trade.ts:
                state.last_trade = trade
            state.trades.add(trade, now=now)
        state.trades.trim(now=now)

    def _candles(self, state: InstrumentState, _payload: dict, data: list[dict], _now: float) -> None:
        for row in data:
            candle = parse_candle(row)
            if candle is not None:
                state.candles.upsert(candle)

    def _books(self, state: InstrumentState, payload: dict, data: list[dict], now: float) -> None:
        """Канал ``books``: ``snapshot`` полностью заменяет стакан, ``update`` — инкремент.

        Перед инкрементом сверяем ``prevSeqId`` с последним ``seqId``. При разрыве
        цепочки (потерянные сообщения после реконнекта, дроп в сети) локальный стакан
        расходится с биржей навсегда, поэтому канал переподписывается — это даёт
        чистый snapshot с актуальным ``seqId``.
        """
        if not data:
            return
        raw = data[0]
        # храним глубже, чем считаем: срез до cfg.levels делается в summary()
        store = max(self.cfg.depth.levels, self.cfg.depth.store_levels)

        if payload.get("action") == "snapshot":
            state.book.apply_ws_snapshot(
                raw,
                max_levels=store,
                # в books каждая строка уровня: [px, sz, liqOrders, orders] — времени нет
                level_ts_index=None,
                authoritative=True,
                incremental=True,
                now=now,
            )
            return

        applied = state.book.apply_ws_update(raw, max_levels=store, level_ts_index=None, now=now)
        if not applied:
            self._on_book_gap(state)

    def _books5(self, state: InstrumentState, _payload: dict, data: list[dict], now: float) -> None:
        """Заполнитель из ``books5`` до первого полного снимка.

        5 уровней не должны затирать полную глубину, поэтому как только стакан стал
        полным (``authoritative``), эти апдейты игнорируются.
        """
        if not data or state.book.authoritative:
            return
        state.book.apply_ws_snapshot(
            data[0],
            max_levels=min(self.cfg.depth.levels, 5),
            # в books5 третье поле строки уровня — liqOrders, а не timestamp
            level_ts_index=None,
            authoritative=False,
            incremental=False,
            now=now,
        )

    # ---------- ликвидации ----------

    def _liquidations(self, data: list[dict], now: float) -> None:
        for row in data:
            for det in row.get("details") or []:
                inst_id = det.get("instId") or row.get("instId")
                state = self.states.get(inst_id) if inst_id else None
                if state is None:
                    continue
                try:
                    px = float(det.get("bkPx") or 0.0)
                    contracts = float(det.get("sz") or 0.0)
                except (TypeError, ValueError):
                    continue
                ts_ms = det.get("ts") or row.get("ts")
                try:
                    ts = float(ts_ms) / 1000.0 if ts_ms else now
                except (TypeError, ValueError):
                    ts = now
                # sz в ликвидациях тоже в контрактах
                state.liq.add(ts, px * contracts * state.ct_val, now=now)
                state.liq.trim(now=now)
