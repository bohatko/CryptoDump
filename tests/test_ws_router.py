"""Маршрутизация WS-сообщений по каналам в InstrumentState."""

from __future__ import annotations

from scanner.config import Config
from scanner.instrument import InstrumentState
from scanner.ws_router import WsRouter


def _router(*states: InstrumentState) -> tuple[WsRouter, list[str]]:
    gaps: list[str] = []
    router = WsRouter({s.inst_id: s for s in states}, Config(), on_book_gap=lambda st: gaps.append(st.inst_id))
    return router, gaps


def _ms(ts: float) -> str:
    return str(int(ts * 1000))


def test_trades_use_ct_val_and_keep_latest(now: float) -> None:
    state = InstrumentState("BTC-USDT-SWAP", "SWAP", ct_val=0.01)
    router, _ = _router(state)

    # пачка не упорядочена по времени: последней сделкой должна стать самая свежая
    router.route(
        {
            "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
            "data": [
                {"px": "60000", "sz": "2", "side": "buy", "ts": _ms(now - 1)},
                {"px": "59000", "sz": "3", "side": "sell", "ts": _ms(now - 5)},
            ],
        },
        now=now,
    )
    assert state.last_trade is not None and state.last_trade.px == 60000.0
    snap = state.trades.snapshot(window_s=300, burst_s=60, now=now)
    assert abs(snap.buy_notional - 1200.0) < 1e-6  # 2 контракта × 0.01 × 60000
    assert abs(snap.sell_notional - 1770.0) < 1e-6


def test_candles_routed(now: float) -> None:
    state = InstrumentState("BTC-USDT-SWAP", "SWAP")
    router, _ = _router(state)
    router.route(
        {
            "arg": {"channel": "candle1m", "instId": "BTC-USDT-SWAP"},
            "data": [[_ms(now - 60), "100", "101", "99", "100.5", "10", "0", "0", "1"]],
        },
        now=now,
    )
    assert state.candles.last_price() == 100.5


def test_books_gap_requests_resync(now: float) -> None:
    state = InstrumentState("BTC-USDT-SWAP", "SWAP")
    router, gaps = _router(state)
    arg = {"channel": "books", "instId": "BTC-USDT-SWAP"}
    levels = [["100", "1", "0", "1"]]

    router.route(
        {"arg": arg, "action": "snapshot", "data": [{"bids": levels, "asks": levels, "seqId": 10, "prevSeqId": -1}]},
        now=now,
    )
    assert state.book.authoritative and state.book.seq_id == 10

    def update(seq: int, prev: int) -> dict:
        return {"arg": arg, "action": "update", "data": [{"bids": [], "asks": [], "seqId": seq, "prevSeqId": prev}]}

    router.route(update(11, 10), now=now)
    assert gaps == []

    # prevSeqId 15 при ожидаемом 11 — цепочка порвана
    router.route(update(16, 15), now=now)
    assert gaps == ["BTC-USDT-SWAP"]


def test_books5_does_not_overwrite_full_book(now: float) -> None:
    state = InstrumentState("BTC-USDT-SWAP", "SWAP")
    router, _ = _router(state)
    full = [[f"{100 - i}", "1", "0", "1"] for i in range(20)]
    router.route(
        {
            "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
            "action": "snapshot",
            "data": [{"bids": full, "asks": full, "seqId": 1, "prevSeqId": -1}],
        },
        now=now,
    )
    router.route(
        {"arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"}, "data": [{"bids": full[:5], "asks": full[:5]}]},
        now=now,
    )
    assert len(state.book.bids) == 20


def test_liquidations_split_by_instrument(now: float) -> None:
    btc = InstrumentState("BTC-USDT-SWAP", "SWAP", ct_val=0.01)
    eth = InstrumentState("ETH-USDT-SWAP", "SWAP", ct_val=0.1)
    router, _ = _router(btc, eth)
    router.route(
        {
            "arg": {"channel": "liquidation-orders", "instType": "SWAP"},
            "data": [
                {"instId": "BTC-USDT-SWAP", "details": [{"bkPx": "60000", "sz": "100", "ts": _ms(now - 10)}]},
                {"instId": "ETH-USDT-SWAP", "details": [{"bkPx": "3000", "sz": "50", "ts": _ms(now - 10)}]},
                {"instId": "XRP-USDT-SWAP", "details": [{"bkPx": "1", "sz": "1", "ts": _ms(now - 10)}]},
            ],
        },
        now=now,
    )
    assert len(btc.liq) == 1 and len(eth.liq) == 1
    btc_snap = btc.liq.snapshot(window_s=300, now=now)
    assert abs(btc_snap.recent_notional + btc_snap.earlier_notional - 60_000.0) < 1e-6


def test_unknown_channel_and_instrument_ignored(now: float) -> None:
    state = InstrumentState("BTC-USDT-SWAP", "SWAP")
    router, _ = _router(state)
    router.route({"arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"}, "data": [{}]}, now=now)
    router.route(
        {"arg": {"channel": "trades", "instId": "DOGE-USDT-SWAP"}, "data": [{"px": "1", "sz": "1", "side": "buy"}]},
        now=now,
    )
    router.route({"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": "not-a-list"}, now=now)
    assert state.last_trade is None
