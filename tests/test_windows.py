"""Окна просадки, CVD и точный максимум коротких окон."""

from __future__ import annotations

from conftest import make_candles

from scanner.models import Candle, Trade
from scanner.windows import CandleBook, DropWindows, TradeWindow


def test_candle_windows(now: float) -> None:
    candles = CandleBook("TEST-USDT-SWAP")
    # 120 свечей: максимум 101 на первой десятке, потом 98, в конце падение до 87
    highs = [101.0] * 10 + [98.0] * 109 + [87.2]
    candles.seed(make_candles(now - 120 * 60, highs, 87.0))

    # без сделок DropWindows целиком опирается на свечи (как и задумано для 30m+)
    drops = DropWindows(TradeWindow("TEST-USDT-SWAP"), candles, precise_until_s=300)

    thresholds = [(60, 1.5), (300, 3.0), (1800, 6.0), (3600, 9.0), (7200, 12.0)]
    hits = drops.all_drops(thresholds, price=87.0, now=now)
    windows = {h.window_s for h in hits}

    # 87 от 98 = -11.2% (5m/30m/1h), от 101 = -13.9% (2h); в окне 1m максимум 87.2 — не срабатывает
    assert 60 not in windows, windows
    assert {300, 1800, 3600, 7200} <= windows, windows
    for hit in hits:
        assert hit.drop_pct < 0
        assert -hit.drop_pct >= hit.threshold_pct

    # просадки нет — нет и хитов
    assert drops.all_drops(thresholds, price=99.0, now=now) == []

    # максимум окна и его время доступны отдельно
    found = candles.high(7200, now=now)
    assert found is not None and found[0] == 101.0

    # граница окна: свеча, чей конец ровно на старте окна, уже не учитывается
    assert len(candles.slice(60, now=now)) == 1
    assert len(candles.slice(120, now=now)) == 2


def test_cvd(now: float) -> None:
    tracker = TradeWindow("TEST-USDT-SWAP", max_window_s=600)
    # первые 2/3 окна — продажи на 100k, последняя треть — покупки на 80k
    tracker.add(Trade("TEST-USDT-SWAP", 100.0, 1000.0, "sell", now - 240), now=now)
    tracker.add(Trade("TEST-USDT-SWAP", 100.0, 800.0, "buy", now - 30), now=now)

    snap = tracker.snapshot(window_s=300, burst_s=60, now=now)
    assert snap.cvd == -20_000.0, snap.cvd
    assert snap.buy_notional == 80_000.0
    assert snap.sell_notional == 100_000.0
    assert snap.burst_buy_notional == 80_000.0
    assert snap.burst_sell_notional == 0.0

    ratio = tracker.reversal_ratio(window_s=300, now=now)
    assert abs(ratio - 0.8) < 1e-9, ratio
    assert tracker.has_data(now=now, window_s=300)

    # после протухания окна данных нет
    assert not tracker.has_data(now=now + 400, window_s=300)


def test_trade_window_high(now: float) -> None:
    """Максимум коротких окон берётся по сделкам: свечи завышали его почти вдвое по времени."""
    window = TradeWindow("TEST-USDT-SWAP", max_window_s=7200)

    # пик случился 100 секунд назад: в окно 1m он НЕ должен попасть, в 5m — должен
    window.add(Trade("TEST-USDT-SWAP", 120.0, 1.0, "buy", now - 100), now=now)
    window.add(Trade("TEST-USDT-SWAP", 100.0, 1.0, "buy", now - 30), now=now)
    window.add(Trade("TEST-USDT-SWAP", 95.0, 1.0, "sell", now - 1), now=now)

    assert window.high(60, now=now)[0] == 100.0
    assert window.high(300, now=now)[0] == 120.0
    assert window.last_price() == 95.0

    # та же картина на 1m-свечах дала бы 120 в окне 1m — именно этот баг и чиним.
    # Свеча открылась 90 с назад и пересекает границу окна 1m, поэтому CandleBook
    # засчитывает её high, хотя пик был 90 секунд назад.
    candles = CandleBook("TEST-USDT-SWAP")
    candles.seed(
        [
            Candle(ts=now - 90, open=100, high=120, low=100, close=95, confirm=True),
            Candle(ts=now - 30, open=95, high=95, low=95, close=95, confirm=True),
        ]
    )
    assert candles.high(60, now=now)[0] == 120.0
    # а DropWindows для 1m переключается на трейды и даёт правильный 100
    drops = DropWindows(window, candles, precise_until_s=300)
    assert drops._peak(60, now=now)[0] == 100.0
    assert drops._peak(1800, now=now)[0] == 120.0  # длинное окно — по свечам
