"""История в SQLite: алерты, эпизоды, фильтры, сводка, импорт alerts.jsonl."""

from __future__ import annotations

import json
from pathlib import Path

from test_alerts import _alert

from scanner.evaluate import STATUS_ALERT, STATUS_THIN_BOOK
from scanner.history import History
from scanner.monitor import DropEpisode


def _episode(inst_id: str, *, now: float, status: str, alerted: bool = False) -> DropEpisode:
    return DropEpisode(
        inst_id=inst_id,
        started_at=now,
        last_seen=now + 30,
        window="5m",
        drop_pct=-2.0,
        max_drop_pct=-3.5,
        max_window="5m",
        price=96.0,
        status=status,
        detail="0.40M USDT",
        alerted=alerted,
    )


def test_alerts_roundtrip_and_filters(now: float) -> None:
    h = History(":memory:")
    first = h.add_alert(_alert("BTC-USDT-SWAP", now=now - 7200, window_label="5m"))
    h.add_alert(_alert("ETH-USDT-SWAP", now=now, window_label="1h", drop_pct=-9.5))

    page = h.alerts()
    assert page["total"] == 2
    assert [a["instId"] for a in page["items"]] == ["ETH-USDT-SWAP", "BTC-USDT-SWAP"]  # новые сверху
    assert page["items"][0]["hits"] == ["bid_wall", "cvd_fade", "liq_decay"]

    assert h.alerts(inst="btc")["total"] == 1
    assert h.alerts(window="1h")["items"][0]["dropPct"] == -9.5
    assert h.alerts(since=now - 3600)["total"] == 1
    assert h.alerts(limit=1, offset=1)["items"][0]["instId"] == "BTC-USDT-SWAP"

    full = h.alert(first)
    assert full is not None and full["instId"] == "BTC-USDT-SWAP" and full["book"]["levels"] == 20
    assert h.alert(999) is None


def test_episodes_and_stats(now: float) -> None:
    h = History(":memory:")
    h.add_episode(_episode("BTC-USDT-SWAP", now=now, status=STATUS_ALERT, alerted=True))
    h.add_episode(_episode("ETH-USDT-SWAP", now=now, status=STATUS_THIN_BOOK))
    h.add_alert(_alert("BTC-USDT-SWAP", now=now))

    eps = h.episodes(alerted=False)
    assert eps["total"] == 1 and eps["items"][0]["status"] == STATUS_THIN_BOOK
    assert eps["items"][0]["durationS"] == 30
    assert h.episodes(status=STATUS_ALERT)["items"][0]["instId"] == "BTC-USDT-SWAP"

    stats = h.stats(since=now - 86400)
    assert stats["alerts"] == 1 and stats["episodes"] == 2 and stats["episodesAlerted"] == 1
    assert stats["statuses"] == {STATUS_ALERT: 1, STATUS_THIN_BOOK: 1}
    assert stats["hits"] == {"bid_wall": 1, "cvd_fade": 1, "liq_decay": 1}
    assert stats["top"][0]["instId"] == "BTC-USDT-SWAP"
    assert sum(d["episodes"] for d in stats["days"]) == 2


def test_import_jsonl_only_into_empty_db(tmp_path: Path, now: float) -> None:
    jsonl = tmp_path / "alerts.jsonl"
    lines = [json.dumps(_alert(s, now=now).to_dict()) for s in ("BTC-USDT-SWAP", "ETH-USDT-SWAP")]
    jsonl.write_text("\n".join(lines) + "\nбитая строка\n", encoding="utf-8")

    h = History(tmp_path / "history.sqlite3")
    assert h.import_jsonl(jsonl) == 2
    assert h.import_jsonl(jsonl) == 0  # повторный запуск не дублирует
    assert h.alerts()["total"] == 2
    h.close()
