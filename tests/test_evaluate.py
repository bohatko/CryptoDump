"""Полный путь оценки: WS-сообщения → InstrumentState → evaluate_instrument → Alert."""

from __future__ import annotations

from conftest import make_candles

from scanner.config import Config
from scanner.evaluate import (
    STATUS_ALERT,
    STATUS_MARKET_DUMP,
    STATUS_NO_BOOK,
    STATUS_THIN_BOOK,
    DropSeen,
    analyze_market_drop,
    evaluate_instrument,
)
from scanner.instrument import InstrumentState
from scanner.orderbook import OrderBook
from scanner.ws_router import WsRouter

WALL_BIDS = [["100.0", "1000"]] + [["99.95", "6000"]] + [["99.9", "1000"] for _ in range(18)]
ASKS = [["100.1", "1000"] for _ in range(20)]


def _dumped_state(
    now: float, *, bids: list[list[str]] = WALL_BIDS, asks: list[list[str]] = ASKS
) -> InstrumentState:
    """Дамп -4% за 5 минут, выкуп по CVD, затухшие ликвидации, стена на бидах."""
    cfg = Config()
    state = InstrumentState("TEST-USDT-SWAP", "SWAP")
    state.candles.seed(make_candles(now - 120 * 60, [100.0] * 120, 96.0))

    router = WsRouter({state.inst_id: state}, cfg, on_book_gap=lambda _s: None)
    router.route(
        {
            "arg": {"channel": "trades", "instId": state.inst_id},
            "data": [
                {"px": "100", "sz": "9000", "side": "sell", "ts": str(int((now - 200) * 1000))},
                {"px": "96", "sz": "8000", "side": "buy", "ts": str(int((now - 20) * 1000))},
            ],
        },
        now=now,
    )
    state.liq.add(now - 200, 500_000.0, now=now)
    state.liq.add(now - 10, 20_000.0, now=now)

    # стена должна простоять wall_persist_s: первое появление фиксируется вызовом summary()
    start = now - cfg.depth.wall_persist_s - 1
    state.book.apply_snapshot({"bids": bids, "asks": asks}, max_levels=20, now=start)
    state.book.summary(cfg.depth, now=start)
    state.book.apply_snapshot({"bids": bids, "asks": asks}, max_levels=20, now=now)
    return state


def test_dump_with_buyback_raises_alert(now: float) -> None:
    alert = evaluate_instrument(_dumped_state(now), Config(), now=now)
    assert alert is not None
    assert alert.window_label == "5m"
    assert abs(alert.drop_pct + 4.0) < 1e-9
    assert alert.price == 96.0 and alert.high == 100.0
    assert alert.signals.score == 3, alert.signals
    assert alert.sibling_windows == []


def test_thin_book_blocks_alert(now: float) -> None:
    thin = [["100.0", "10"] for _ in range(20)]
    assert evaluate_instrument(_dumped_state(now, bids=thin, asks=thin), Config(), now=now) is None


def test_no_drop_no_alert(now: float) -> None:
    state = _dumped_state(now)
    cfg = Config()
    cfg.windows = {"5m": 10.0}
    assert evaluate_instrument(state, cfg, now=now) is None


def _statuses(state: InstrumentState, now: float, **kwargs: object) -> list[tuple[str, str]]:
    seen: list[tuple[str, str]] = []
    evaluate_instrument(state, Config(), now=now, observe=lambda _d, s, detail: seen.append((s, detail)), **kwargs)
    return seen


def test_observer_reports_why_drop_did_not_alert(now: float) -> None:
    assert _statuses(_dumped_state(now), now) == [(STATUS_ALERT, "3/3: bid_wall, cvd_fade, liq_decay")]
    assert _statuses(_dumped_state(now), now, depth_available=False) == [(STATUS_NO_BOOK, "")]

    thin = [["100.0", "10"] for _ in range(20)]
    [(status, detail)] = _statuses(_dumped_state(now, bids=thin, asks=thin), now)
    assert status == STATUS_THIN_BOOK and detail.startswith("0.04M USDT")


def test_unready_book_is_not_thin(now: float) -> None:
    """Слот выдан, а snapshot ещё не пришёл: это «ждёт стакан» (слот продлевается), не «тонкий»."""
    state = _dumped_state(now)
    state.book = OrderBook(inst_id=state.inst_id, ct_val=state.ct_val)
    assert _statuses(state, now) == [(STATUS_NO_BOOK, "стакан не готов")]
    assert isinstance(evaluate_instrument(state, Config(), now=now), DropSeen)


def test_observer_silent_without_drop(now: float) -> None:
    state = _dumped_state(now)
    cfg = Config()
    cfg.windows = {"5m": 10.0}
    seen: list[str] = []
    evaluate_instrument(state, cfg, now=now, observe=lambda _d, s, _x: seen.append(s))
    assert seen == []


def test_stale_price_no_alert(now: float) -> None:
    # сделки и свечи старше trade_stale_after_s — цена неизвестна, просадку не считаем
    later = now + 3600
    assert evaluate_instrument(_dumped_state(now), Config(), now=later) is None


def test_market_dump_breadth_and_leader_block_alerts(now: float) -> None:
    cfg = Config()
    states: dict[str, InstrumentState] = {}
    for name in ("SOL", "DOGE", "XRP", "PEPE"):
        s = _dumped_state(now)
        s.inst_id = f"{name}-USDT-SWAP"
        states[s.inst_id] = s

    ctx = analyze_market_drop(states, cfg, now=now)
    assert ctx.breadth_reason is not None and "4/4" in ctx.breadth_reason
    seen: list[tuple[str, str]] = []
    res = evaluate_instrument(
        states["SOL-USDT-SWAP"],
        cfg,
        now=now,
        observe=lambda _d, st, detail: seen.append((st, detail)),
        market_drop=ctx.reason_for("SOL-USDT-SWAP"),
    )
    assert res is None
    assert seen and seen[0][0] == STATUS_MARKET_DUMP

    # одиночный дамп при падающем поводыре BTC тоже откладывается
    calm = {f"C{i}-USDT-SWAP": InstrumentState(f"C{i}-USDT-SWAP", "SWAP") for i in range(4)}
    for s in calm.values():
        s.candles.seed(make_candles(now - 600, [100.0] * 10, 100.0))
    btc = _dumped_state(now)
    btc.inst_id = "BTC-USDT-SWAP"
    calm[btc.inst_id] = btc
    leader_ctx = analyze_market_drop(calm, cfg, now=now)
    assert leader_ctx.breadth_reason is None
    assert leader_ctx.reason_for("SOL-USDT-SWAP") == "поводырь BTC -4.0%"
    assert leader_ctx.reason_for("BTC-USDT-SWAP") is None  # сам поводырь на себя не ссылается
