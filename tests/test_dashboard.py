"""Веб-дашборд: защита, правка конфига, управление, выдача данных (без сети — тестовый сервер aiohttp)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from aiohttp.test_utils import TestClient, TestServer
from test_alerts import _alert
from test_engine import _engine
from test_evaluate import _dumped_state

from scanner.config import load_config
from scanner.dashboard import Dashboard
from scanner.engine import Engine
from scanner.evaluate import STATUS_NO_BOOK
from scanner.history import History
from scanner.models import DropHit


def _call(
    dash: Dashboard, method: str, path: str, *, body: Any = None, headers: dict[str, str] | None = None
) -> tuple[int, Any]:
    async def go() -> tuple[int, Any]:
        async with TestClient(TestServer(dash.build_app())) as client:
            kwargs: dict[str, Any] = {"headers": headers or {}}
            if body is not None:
                kwargs["json"] = body
            resp = await client.request(method, path, **kwargs)
            text = await resp.text()
            return resp.status, json.loads(text) if resp.content_type == "application/json" else text

    return asyncio.run(go())


def _file_engine(tmp_path: Path) -> tuple[Engine, Path]:
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"windows": {"5m": 3.0}, "market": {"symbols": ["BTC"]}}), encoding="utf-8")
    cfg = load_config(path)
    cfg.alerts.sinks = ["console"]
    return Engine(cfg, dry_run=True), path


def test_status_and_index() -> None:
    dash = Dashboard(_engine())
    status, data = _call(dash, "GET", "/api/status")
    assert status == 200
    assert data["phase"] == "старт" and data["depth"]["mode"] == "ondemand" and data["muted"] is False

    status, page = _call(dash, "GET", "/")
    assert status == 200 and "CryptoDump" in page


def test_guard_blocks_foreign_requests() -> None:
    dash = Dashboard(_engine())
    assert _call(dash, "GET", "/api/status", headers={"Host": "evil.example:8787"})[0] == 403
    assert _call(dash, "POST", "/api/control/mute", body={}, headers={"Origin": "http://evil.example"})[0] == 403
    # обычная HTML-форма с чужого сайта приходит не как JSON
    assert _call(dash, "POST", "/api/control/stop", headers={"Content-Type": "text/plain"})[0] == 415


def test_token_required_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DASHBOARD_TOKEN", "s3cret")
    dash = Dashboard(_engine())
    assert _call(dash, "GET", "/api/status")[0] == 401
    assert _call(dash, "GET", "/api/status", headers={"Authorization": "Bearer nope"})[0] == 401
    assert _call(dash, "GET", "/api/status", headers={"Authorization": "Bearer s3cret"})[0] == 200
    assert _call(dash, "GET", "/")[0] == 200  # страница без данных доступна, чтобы спросить токен


def test_refuses_network_bind_without_token() -> None:
    engine = _engine()
    engine.cfg.dashboard.host = "0.0.0.0"
    dash = Dashboard(engine)
    asyncio.run(asyncio.wait_for(dash.run(asyncio.Event()), timeout=2))  # вернулся сразу, не слушает


def test_config_patch_saves_file_and_applies(tmp_path: Path) -> None:
    engine, path = _file_engine(tmp_path)
    dash = Dashboard(engine)
    status, data = _call(
        dash,
        "PUT",
        "/api/config",
        body={"windows": {"5m": 2.0, "1m": 1.0}, "depth": {"mode": "top", "min_notional_usdt": 500000}},
    )
    assert status == 200, data
    assert data["saved"] and data["restartRequired"] == ["depth.mode"]

    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved["market"] == {"symbols": ["BTC"]}  # в файл не утекли дефолты и CLI-переопределения
    assert saved["windows"] == {"5m": 2.0, "1m": 1.0}
    assert saved["depth"] == {"mode": "top", "min_notional_usdt": 500000}

    assert engine.cfg.windows == {"5m": 2.0, "1m": 1.0}
    assert engine.cfg.depth.min_notional_usdt == 500000
    assert engine.cfg.depth.mode == "ondemand"  # до перезапуска остаётся прежний режим
    assert engine.watcher.poll() is None  # свою запись сторож не перечитывает
    assert _call(dash, "GET", "/api/status")[1]["pendingRestart"] == ["depth.mode"]


def test_cli_override_is_not_reported_as_restart(tmp_path: Path) -> None:
    engine, path = _file_engine(tmp_path)
    engine.cfg.market.symbols = ["ETH", "SOL"]  # как после `-s ETH,SOL`: в файле этого нет
    status, data = _call(Dashboard(engine), "PUT", "/api/config", body={"signals": {"min_score": 1}})
    assert status == 200 and data["restartRequired"] == []
    assert engine.cfg.signals.min_score == 1
    assert engine.cfg.market.symbols == ["ETH", "SOL"]  # вселенная не меняется на лету
    assert json.loads(path.read_text(encoding="utf-8"))["market"] == {"symbols": ["BTC"]}


def test_config_patch_validation(tmp_path: Path) -> None:
    engine, path = _file_engine(tmp_path)
    before = path.read_text(encoding="utf-8")
    status, data = _call(
        Dashboard(engine),
        "PUT",
        "/api/config",
        body={"depth": {"min_notional_usdt": "много", "mode": "fast"}, "windows": {"7m": 1}, "foo": 1},
    )
    assert status == 400
    assert len(data["errors"]) == 4, data["errors"]
    assert path.read_text(encoding="utf-8") == before


def test_config_patch_without_file_applies_in_memory() -> None:
    engine = _engine()
    status, data = _call(Dashboard(engine), "PUT", "/api/config", body={"alerts": {"cooldown_s": 42}})
    assert status == 200 and data["saved"] is False
    assert engine.alerts.cooldown.cooldown_s == 42


def test_controls(now: float) -> None:
    engine = _engine()
    state = _dumped_state(now)
    engine.states[state.inst_id] = state
    dash = Dashboard(engine)

    assert _call(dash, "POST", "/api/control/mute", body={"muted": True})[1] == {"ok": True, "muted": True}
    assert engine.alerts.muted

    assert _call(dash, "POST", "/api/control/book", body={"instId": state.inst_id})[0] == 200
    assert engine.depth_scheduler is not None and state.inst_id in engine.depth_scheduler.pending()

    engine.alerts.cooldown.restore(state.inst_id, "5m", now)
    assert _call(dash, "POST", "/api/control/cooldown-reset", body={"instId": state.inst_id})[0] == 200
    assert engine.alerts.cooldown.snapshot() == {}

    assert _call(dash, "POST", "/api/control/book", body={"instId": "NOPE"})[0] == 404
    assert _call(dash, "POST", "/api/control/telegram-test", body={})[0] == 400  # dry-run: Telegram нет


def test_history_endpoints(now: float) -> None:
    engine = _engine()
    dash = Dashboard(engine)
    assert _call(dash, "GET", "/api/alerts")[0] == 503  # история не открыта

    engine.history = History(":memory:")
    alert_id = engine.history.add_alert(_alert("BTC-USDT-SWAP", now=now))
    status, page = _call(dash, "GET", "/api/alerts?inst=btc")
    assert status == 200 and page["total"] == 1
    assert _call(dash, "GET", f"/api/alerts/{alert_id}")[1]["instId"] == "BTC-USDT-SWAP"
    assert _call(dash, "GET", "/api/alerts/999")[0] == 404
    assert _call(dash, "GET", "/api/stats?days=36500")[1]["alerts"] == 1


def test_drops_and_instrument(now: float) -> None:
    engine = _engine()
    state = _dumped_state(now)
    engine.states[state.inst_id] = state
    drop = DropHit(window_s=300, drop_pct=-4.0, high=100.0, price=96.0, high_ts=now - 200, threshold_pct=3.0)
    engine.monitor.observe(state.inst_id, drop, STATUS_NO_BOOK, now=now)
    dash = Dashboard(engine)

    status, data = _call(dash, "GET", "/api/drops")
    assert status == 200
    assert data["active"][0]["instId"] == state.inst_id and data["active"][0]["hasBook"] is False

    payload = dash.instrument_payload(state, now=now)
    five = next(w for w in payload["windows"] if w["window"] == "5m")
    assert abs(five["dropPct"] + 4.0) < 1e-9 and five["thresholdPct"] == 3.0
    assert payload["book"] is None  # слота нет — стакан не показываем как живой
    assert payload["episode"]["status"] == STATUS_NO_BOOK
    assert _call(dash, "GET", "/api/instrument/NOPE-USDT-SWAP")[0] == 404
