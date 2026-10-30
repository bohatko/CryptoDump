"""Виртуальный счёт: сопровождение сделки по барам, реинвест, пропуски, прогон по свечам."""

from __future__ import annotations

import asyncio

from scanner.config import Config, PaperConfig
from scanner.history import History
from scanner.instrument import InstrumentState
from scanner.models import Trade
from scanner.paper import REASON_BE, REASON_LIQ, REASON_SL, REASON_TIME, REASON_TP, PaperBook, open_trade, step


def _alert(now: float, *, inst: str = "X-USDT-SWAP", entry: float = 100.0, stop: float = 98.0,
           targets: tuple[float, ...] = (101.0, 103.0, 105.0)) -> dict:
    return {
        "ts": now,
        "instId": inst,
        "window": "5m",
        "windowSeconds": 300,
        "price": entry,
        "windowHigh": targets[-1],
        "plan": {"entry": entry, "stop": stop, "targets": list(targets)},
    }


def _cfg(**kw: object) -> PaperConfig:
    cfg = PaperConfig(fee_pct=0.0, slippage_pct=0.0)
    for key, value in kw.items():
        setattr(cfg, key, value)
    return cfg


def test_take_profit_on_second_target(now: float) -> None:
    cfg = _cfg()
    trade = open_trade(1, _alert(now), cfg)
    assert trade is not None and trade.target == 103.0 and abs(trade.rr - 1.5) < 1e-9
    assert not step(trade, cfg, low=99.5, high=102.0, close=101.5, ts=now + 60)
    assert step(trade, cfg, low=101.0, high=103.5, close=103.2, ts=now + 120)
    assert trade.reason == REASON_TP and trade.exit_px == 103.0
    assert abs(trade.margin_return() - 0.15) < 1e-9  # +3% цены × 5
    assert abs(trade.r_multiple() - 1.5) < 1e-9


def test_stop_wins_when_both_levels_inside_one_bar(now: float) -> None:
    """Порядок цен внутри минуты неизвестен — считаем худший вариант."""
    cfg = _cfg()
    trade = open_trade(1, _alert(now), cfg)
    assert step(trade, cfg, low=97.0, high=104.0, close=100.0, ts=now + 60)
    assert trade.reason == REASON_SL and trade.exit_px == 98.0
    assert abs(trade.margin_return() - (-0.10)) < 1e-9


def test_breakeven_after_first_target(now: float) -> None:
    cfg = PaperConfig(fee_pct=0.05, slippage_pct=0.0)
    trade = open_trade(1, _alert(now), cfg)
    step(trade, cfg, low=99.8, high=101.2, close=101.0, ts=now + 60)
    assert trade.be_armed and abs(trade.stop_now - 100.1) < 1e-9  # вход + две комиссии
    assert step(trade, cfg, low=100.0, high=100.5, close=100.0, ts=now + 120)
    assert trade.reason == REASON_BE and abs(trade.price_return()) < 1e-9  # в ноль, комиссии отбиты


def test_time_exit_and_liquidation(now: float) -> None:
    cfg = _cfg(max_hold_s=3600.0)
    trade = open_trade(1, _alert(now), cfg)
    assert not step(trade, cfg, low=99.0, high=100.5, close=100.2, ts=now + 1800)
    assert step(trade, cfg, low=99.0, high=100.5, close=100.4, ts=now + 3600)
    assert trade.reason == REASON_TIME and trade.exit_px == 100.4

    far = open_trade(2, _alert(now, stop=70.0, targets=(110.0, 150.0)), _cfg(leverage=5.0))
    assert far is not None and abs(far.liq_px - 80.5) < 1e-9  # −19.5% при ×5
    assert step(far, _cfg(leverage=5.0), low=80.0, high=100.0, close=81.0, ts=now + 60)
    assert far.reason == REASON_LIQ and far.margin_return() == -1.0


def test_account_reinvests_and_skips_while_busy(now: float) -> None:
    cfg = Config()
    cfg.paper = _cfg(start_balance=100.0, leverage=5.0)
    book = PaperBook(cfg)
    book.ready = True
    state = InstrumentState("X-USDT-SWAP", "SWAP")
    states = {state.inst_id: state}

    first = book.on_alert(1, _alert(now))
    assert first is not None and first.taken and first.margin == 100.0
    busy = book.on_alert(2, _alert(now + 10, inst="Y-USDT-SWAP"))
    assert busy is not None and not busy.taken and "капитал" in busy.skip_reason

    state.trades.add(Trade("X-USDT-SWAP", 103.5, 1.0, "buy", now + 30), now=now + 30)
    assert [t.alert_id for t in book.update(states, now=now + 30)] == [1]
    assert abs(book.balance() - 115.0) < 1e-9

    second = book.on_alert(3, _alert(now + 60))
    assert second.taken and abs(second.margin - 115.0) < 1e-9  # реинвест всего баланса
    weak = book.on_alert(4, _alert(now + 70, inst="Z-USDT-SWAP", targets=(100.5, 101.0)))
    assert not weak.taken  # капитал занят второй сделкой

    state.trades.add(Trade("X-USDT-SWAP", 97.0, 1.0, "sell", now + 90), now=now + 90)
    book.update(states, now=now + 90)
    assert abs(book.balance() - 115.0 * 0.9) < 1e-9

    summary = book.summary(states, now=now + 100)
    acc = summary["account"]
    assert acc["trades"] == 2 and acc["wins"] == 1 and acc["winRate"] == 50.0
    assert acc["expectancyPct"] == 2.5  # (+15% − 10%) / 2
    assert acc["maxDrawdownPct"] == 10.0
    assert summary["signals"]["trades"] == 2 and summary["signals"]["open"] == 2
    assert summary["signals"]["breakevenWinRate"] == 40.0  # 10 / (15 + 10)


def test_min_rr_filter(now: float) -> None:
    cfg = Config()
    cfg.paper = _cfg(min_rr=1.0)
    book = PaperBook(cfg)
    book.ready = True
    weak = book.on_alert(1, _alert(now, targets=(100.5, 101.0)))
    assert weak is not None and not weak.taken and weak.skip_reason.startswith("прибыль/риск")
    assert book.on_alert(2, {"ts": now, "instId": "X-USDT-SWAP"}) is None  # без плана сделки нет


def test_reentry_cooldown_and_loss_streak_pause(now: float) -> None:
    """После стопа на монете не перезаходим в неё час; после 2 стопов подряд счёт ждёт 30 мин."""
    cfg = Config()
    cfg.paper = _cfg(reentry_cooldown_s=3600.0, loss_streak_limit=2, loss_streak_pause_s=1800.0)
    book = PaperBook(cfg)
    book.ready = True
    states = {iid: InstrumentState(iid, "SWAP") for iid in ("A-USDT-SWAP", "B-USDT-SWAP", "C-USDT-SWAP")}

    t1 = book.on_alert(1, _alert(now, inst="A-USDT-SWAP"))
    assert t1 is not None and t1.taken
    states["A-USDT-SWAP"].trades.add(Trade("A-USDT-SWAP", 97.0, 1.0, "sell", now + 60), now=now + 60)
    book.update(states, now=now + 60)
    assert t1.reason == REASON_SL

    # повторный алерт по той же монете через 2 минуты блокируется
    same = book.on_alert(2, _alert(now + 180, inst="A-USDT-SWAP"))
    assert same is not None and not same.taken and "пауза после стопа A-USDT-SWAP" in same.skip_reason

    # другая монета открывается, но тоже ловит стоп -> серия 2 стопа подряд
    t2 = book.on_alert(3, _alert(now + 200, inst="B-USDT-SWAP"))
    assert t2 is not None and t2.taken
    states["B-USDT-SWAP"].trades.add(Trade("B-USDT-SWAP", 97.0, 1.0, "sell", now + 260), now=now + 260)
    book.update(states, now=now + 260)

    # третья монета во время 30-минутной паузы счёта пропускается
    paused = book.on_alert(4, _alert(now + 500, inst="C-USDT-SWAP"))
    assert paused is not None and not paused.taken and "2 стопов подряд" in paused.skip_reason

    # после окончания паузы счёта вход снова разрешён
    resumed = book.on_alert(5, _alert(now + 260 + 1801, inst="C-USDT-SWAP"))
    assert resumed is not None and resumed.taken


class FakeRest:
    def __init__(self, rows: list[list[str]]) -> None:
        self.rows = rows

    async def candles_between(self, inst_id: str, *, since: float, until: float) -> list[list[str]]:
        return [r for r in self.rows if since * 1000 <= int(r[0]) < until * 1000]


def _row(ts: float, o: float, h: float, low: float, c: float) -> list[str]:
    return [str(int(ts * 1000)), str(o), str(h), str(low), str(c), "1", "1", "1", "1"]


def test_replay_reconstructs_plan_for_old_alert_and_trades_it(now: float) -> None:
    """Алерт без плана (сохранён до планов): план собирается по стакану из алерта и свечам до него."""
    history = History(":memory:")
    alert_ts = now - 3000.0
    history.add_alert_dict({
        "ts": alert_ts,
        "instId": "CT-USDT-SWAP",
        "window": "2h",
        "windowSeconds": 7200,
        "price": 0.1377,
        "windowHigh": 0.1426,
        "book": {"bestBid": 0.1373, "bestAsk": 0.1374, "wall": {"px": 0.136, "sz": 373410.0}},
        "signals": {"score": 2, "hits": ["bid_wall", "cvd_fade"]},
    })
    t0 = (alert_ts // 60) * 60
    rows = [
        _row(t0 - 600, 0.1420, 0.1426, 0.1415, 0.1418),  # максимум окна
        _row(t0 - 300, 0.1390, 0.1392, 0.1365, 0.1370),  # дно дампа
        _row(t0 - 60, 0.1371, 0.1378, 0.1370, 0.1376),
        _row(t0, 0.1376, 0.1500, 0.1300, 0.1380),  # свеча алерта: пропускается
        _row(t0 + 60, 0.1380, 0.1395, 0.1378, 0.1392),  # 1-я цель → безубыток
        _row(t0 + 120, 0.1392, 0.1410, 0.1390, 0.1405),  # 2-я цель → тейк
    ]
    cfg = Config()
    book = PaperBook(cfg)
    book.load(history)
    asyncio.run(book.replay(FakeRest(rows), {}))  # type: ignore[arg-type]

    assert book.ready
    (trade,) = book.trades.values()
    assert trade.source == "replay" and trade.taken
    assert trade.entry == 0.1374 and trade.stop < 0.136  # стоп под стеной
    assert trade.reason == REASON_TP and trade.status == "closed"
    assert book.balance() > cfg.paper.start_balance

    # сделка сохранилась и переживает рестарт
    again = PaperBook(cfg)
    again.load(history)
    assert again.trades[trade.alert_id].reason == REASON_TP
    assert again.balance() == book.balance()

    asyncio.run(again.replay(FakeRest(rows), {}))  # type: ignore[arg-type]
    assert len(again.trades) == 1  # уже посчитанный алерт не прогоняется второй раз
