"""Сохранение и восстановление состояния: CVD, ликвидации, кулдаун."""

from __future__ import annotations

from pathlib import Path

from scanner.alerts import Cooldown
from scanner.models import Trade
from scanner.state import load_state, save_state
from scanner.windows import LiquidationTracker, TradeWindow


def test_state_roundtrip(tmp_path: Path, now: float) -> None:
    trades = {"BTC-USDT-SWAP": TradeWindow("BTC-USDT-SWAP", max_window_s=600)}
    trades["BTC-USDT-SWAP"].add(Trade("BTC-USDT-SWAP", 100.0, 50.0, "sell", now - 100), now=now)
    trades["BTC-USDT-SWAP"].add(Trade("BTC-USDT-SWAP", 101.0, 30.0, "buy", now - 20), now=now)

    liq = {"BTC-USDT-SWAP": LiquidationTracker(max_window_s=600)}
    liq["BTC-USDT-SWAP"].add(now - 60, 250_000.0, now=now)

    cooldown = Cooldown(cooldown_s=300)
    assert cooldown.allow("BTC-USDT-SWAP", "5m", now=now - 10)

    path = str(tmp_path / "state.json")
    assert save_state(path, trades=trades, liq=liq, cooldown=cooldown, now=now)
    assert Path(path).is_file()

    # восстановление в чистые объекты
    trades2 = {"BTC-USDT-SWAP": TradeWindow("BTC-USDT-SWAP", max_window_s=600)}
    liq2 = {"BTC-USDT-SWAP": LiquidationTracker(max_window_s=600)}
    cooldown2 = Cooldown(cooldown_s=300)

    assert load_state(path, trades=trades2, liq=liq2, cooldown=cooldown2, max_age_s=900, now=now + 5)

    snap = trades2["BTC-USDT-SWAP"].snapshot(window_s=300, burst_s=60, now=now + 5)
    assert abs(snap.sell_notional - 5_000.0) < 1e-6, snap.sell_notional
    assert abs(snap.buy_notional - 3_030.0) < 1e-6, snap.buy_notional
    # максимум по трейдам восстановился — окно 1m снова работает точно
    assert trades2["BTC-USDT-SWAP"].high(300, now=now + 5)[0] == 101.0
    assert len(liq2["BTC-USDT-SWAP"]) == 1
    # кулдаун восстановлен: повторный алерт в те же 5 минут не пройдёт
    assert not cooldown2.allow("BTC-USDT-SWAP", "5m", now=now + 5)

    # устаревшее состояние игнорируется
    trades3 = {"BTC-USDT-SWAP": TradeWindow("BTC-USDT-SWAP", max_window_s=600)}
    assert not load_state(
        path,
        trades=trades3,
        liq={"BTC-USDT-SWAP": LiquidationTracker()},
        cooldown=Cooldown(cooldown_s=300),
        max_age_s=60,
        now=now + 10_000,
    )
    assert not trades3["BTC-USDT-SWAP"].has_data(now=now + 10_000, window_s=300)
