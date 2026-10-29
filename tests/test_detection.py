"""Три сигнатуры откупа, скоринг и формат алерта."""

from __future__ import annotations

import json
import time

from conftest import make_book_summary

from scanner.config import Config
from scanner.detection import detect_signals, score_ok
from scanner.models import Alert, Trade
from scanner.windows import LiquidationTracker, TradeWindow


def test_detection_and_alert() -> None:
    now = time.time()
    cfg = Config()

    liq = LiquidationTracker(max_window_s=600)
    liq.add(now - 200, 500_000.0, now=now)  # каскад был
    liq.add(now - 10, 20_000.0, now=now)  # и выдохся
    liq_snap = liq.snapshot(window_s=300, now=now)
    assert liq_snap.earlier_notional >= cfg.liquidation.min_notional_usdt

    book = make_book_summary(now=now)
    cvd = TradeWindow("TEST-USDT-SWAP", max_window_s=600)
    cvd.add(Trade("TEST-USDT-SWAP", 100.0, 9000.0, "sell", now - 200), now=now)
    cvd.add(Trade("TEST-USDT-SWAP", 100.0, 8000.0, "buy", now - 20), now=now)
    cvd_snap = cvd.snapshot(window_s=300, burst_s=60, now=now)
    ratio = cvd.reversal_ratio(window_s=300, now=now)

    signals = detect_signals(cfg, book=book, cvd=cvd_snap, liq=liq_snap, reversal_ratio=ratio)
    assert signals.bid_wall and signals.cvd_fade and signals.liq_decay, signals
    assert signals.score == 3
    assert score_ok(cfg, signals)

    alert = Alert(
        ts=now,
        inst_id="TEST-USDT-SWAP",
        inst_type="SWAP",
        window_label="5m",
        window_s=300,
        drop_pct=-3.42,
        price=96.58,
        high=100.0,
        book=book,
        cvd=cvd_snap,
        liq=liq_snap,
        signals=signals,
    )
    payload = alert.to_dict()
    text = json.dumps(payload, ensure_ascii=False)
    assert '"score": 3' in text
    assert abs(payload["book"]["wall"]["notional"] - 599_400.0) < 0.01
    assert "ПОКУПКА TEST (TEST-USDT-SWAP)" in alert.console_text()
    assert "сигналов 3/3" in alert.console_text()
