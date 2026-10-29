"""Кулдаун алертов и HTML-формат Telegram."""

from __future__ import annotations

from conftest import make_book_summary

from scanner.alerts import Cooldown
from scanner.models import Alert, CvdSnapshot, LiquidationSnapshot, Signals
from scanner.telegram import _split_message, format_alert_html


def test_alert_dedup(now: float) -> None:
    """Кулдаун и подавление «соседних» окон одного дампа."""
    cd = Cooldown(cooldown_s=300)

    assert cd.allow("BTC-USDT-SWAP", "5m", now=now)
    assert not cd.allow("BTC-USDT-SWAP", "5m", now=now + 100)
    assert cd.allow("BTC-USDT-SWAP", "1h", now=now + 100)  # другое окно — свой кулдаун
    assert cd.allow("BTC-USDT-SWAP", "5m", now=now + 301)  # кулдаун истёк

    # остальные окна того же дампа гасятся, чтобы не пришёл второй алерт
    cd.suppress("ETH-USDT-SWAP", ["5m", "30m", "1h"], now=now)
    assert not cd.allow("ETH-USDT-SWAP", "30m", now=now + 1)
    assert cd.allow("ETH-USDT-SWAP", "2h", now=now + 1)  # не входило в дамп — можно


def _alert(inst_id: str, *, now: float, window_label: str = "5m", drop_pct: float = -3.42) -> Alert:
    cvd = CvdSnapshot(window_s=300, cvd=1_200_000.0, buy_notional=1_500_000.0, sell_notional=300_000.0)
    liq = LiquidationSnapshot(window_s=300, earlier_notional=480_000.0, recent_notional=90_000.0)
    signals = Signals(bid_wall=True, cvd_fade=True, liq_decay=True, hits=["bid_wall", "cvd_fade", "liq_decay"])
    return Alert(
        ts=now,
        inst_id=inst_id,
        inst_type="SWAP",
        window_label=window_label,
        window_s=300,
        drop_pct=drop_pct,
        price=61_245.5,
        high=63_400.0,
        book=make_book_summary("BTC-USDT-SWAP", now=now),
        cvd=cvd,
        liq=liq,
        signals=signals,
    )


def test_telegram_format(now: float) -> None:
    """HTML-формат алерта: экранирование, лимит символов, нарезка сообщений."""
    text = format_alert_html(_alert("BTC-USDT-SWAP", now=now))
    assert "<b>BTC-USDT-SWAP</b>" in text
    assert "-3.42%" in text
    assert "1.40M" in text and "900k" in text
    assert "+1.20M" in text  # знак CVD в человекочитаемом виде
    assert "score" not in text.lower()  # подпись по-русски: «Сигналы»
    assert "Сигналы 3/3" in text
    assert len(text) < 4096

    # длинный текст режется на части, ни одна не превышает лимит
    chunks = _split_message("x" * 200 + "\n" + "y" * 5000, limit=100)
    assert all(len(c) <= 100 for c in chunks), [len(c) for c in chunks]
    assert "".join(chunks).count("y") == 5000

    # экранирование: инструмент с символами разметки не ломает HTML
    weird = _alert("A<B>&C", now=now, window_label="1m", drop_pct=-1.6)
    assert "A&lt;B&gt;&amp;C" in format_alert_html(weird)
