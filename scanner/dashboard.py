"""Веб-дашборд: состояние сканера, история алертов и дампов, пороги, управление.

Сервер — ``aiohttp.web`` (уже в зависимостях), фронтенд — один статический файл
``web/index.html`` без сборки. Данные берутся прямо из живого ``Engine``, история —
из SQLite (``history.py``).

Безопасность. Через дашборд можно менять пороги и останавливать сканер, поэтому:

* по умолчанию слушаем только 127.0.0.1; на другой адрес без токена не поднимаемся;
* токен — из переменной окружения ``dashboard.token_env`` (в конфиге только имя);
* изменяющие запросы принимаются только с ``Content-Type: application/json`` и
  своим ``Origin``, а на loopback ещё и проверяется ``Host`` — чужая страница в
  браузере (CSRF, DNS rebinding) не сможет нажать «Стоп» за пользователя.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import math
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from aiohttp import web

from . import __version__, logbuffer
from .config import (
    WINDOW_SECONDS,
    config_from_dict,
    dump_config,
    merge_config,
    read_config_file,
    write_config_file,
)
from .detection import detect_signals, passed_liquidity
from .instrument import InstrumentState
from .models import BookSummary

if TYPE_CHECKING:
    from .engine import Engine
    from .history import History

log = logging.getLogger(__name__)

WEB_DIR = Path(__file__).with_name("web")
LOOPBACK = {"127.0.0.1", "localhost", "::1"}
# приоритет ручного запроса стакана: глубже любой реальной просадки, чтобы слот выдали сразу
MANUAL_BOOK_DROP_PCT = -100.0
ENUMS = {("depth", "mode"): ("ondemand", "top", "all"), ("depth", "source"): ("ws", "rest")}

Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


class Dashboard:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        cfg = engine.cfg.dashboard
        self.host = cfg.host
        self.port = cfg.port
        self.token = os.environ.get(cfg.token_env, "").strip() if cfg.token_env else ""
        self.logs = logbuffer.install(cfg.log_buffer)
        # поля, сохранённые из дашборда, но требующие перезапуска
        self.pending_restart: set[str] = set()

    # ---------- жизненный цикл ----------

    async def run(self, stop: asyncio.Event) -> None:
        if self.host not in LOOPBACK and not self.token:
            log.error(
                "дашборд не запущен: адрес %s доступен из сети, а переменная %s с токеном не задана",
                self.host,
                self.engine.cfg.dashboard.token_env,
            )
            return
        runner = web.AppRunner(self.build_app(), access_log=None)
        await runner.setup()
        try:
            site = web.TCPSite(runner, self.host, self.port)
            try:
                await site.start()
            except OSError as exc:
                log.error("дашборд не запущен: порт %s:%s занят или недоступен (%s)", self.host, self.port, exc)
                return
            log.info("дашборд: http://%s:%s%s", self.host, self.port, " (нужен токен)" if self.token else "")
            await stop.wait()
        finally:
            await runner.cleanup()

    def build_app(self) -> web.Application:
        app = web.Application(middlewares=[self._guard], client_max_size=256 * 1024)
        app.router.add_get("/", self.index)
        get = {
            "/api/status": self.status,
            "/api/drops": self.drops,
            "/api/alerts": self.alerts,
            "/api/alerts/{alert_id}": self.alert,
            "/api/episodes": self.episodes,
            "/api/stats": self.stats,
            "/api/paper": self.paper,
            "/api/instruments": self.instruments,
            "/api/instrument/{inst_id}": self.instrument,
            "/api/logs": self.log_records,
            "/api/config": self.get_config,
        }
        for path, handler in get.items():
            app.router.add_get(path, handler)
        app.router.add_put("/api/config", self.put_config)
        app.router.add_post("/api/control/{action}", self.control)
        return app

    # ---------- защита ----------

    @web.middleware
    async def _guard(self, request: web.Request, handler: Handler) -> web.StreamResponse:
        if self.host in LOOPBACK and not _loopback_host(request.host):
            return _error(403, "чужой Host — запрос отклонён")
        if request.method not in ("GET", "HEAD"):
            origin = request.headers.get("Origin")
            if origin and origin not in (f"http://{request.host}", f"https://{request.host}"):
                return _error(403, "чужой Origin — запрос отклонён")
            if request.content_type != "application/json":
                return _error(415, "нужен Content-Type: application/json")
        if request.path.startswith("/api/") and self.token:
            supplied = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
            if not hmac.compare_digest(supplied.encode(), self.token.encode()):
                return _error(401, "нужен токен дашборда")
        try:
            return await handler(request)
        except web.HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001 — ошибка ручки не должна ронять сервер
            log.exception("дашборд: ошибка %s %s", request.method, request.path)
            return _error(500, f"внутренняя ошибка: {exc}")

    # ---------- страницы ----------

    async def index(self, _request: web.Request) -> web.StreamResponse:
        return web.FileResponse(WEB_DIR / "index.html", headers={"Cache-Control": "no-store"})

    async def status(self, _request: web.Request) -> web.Response:
        return _json(self.status_payload())

    def status_payload(self, *, now: float | None = None) -> dict[str, Any]:
        now = time.time() if now is None else now
        e = self.engine
        cfg = e.cfg
        started = e.started_at
        warmup_left = max(0.0, cfg.engine.warmup_s - (now - started)) if started else cfg.engine.warmup_s
        phase = e.phase
        if phase == "работает" and warmup_left > 0:
            phase = "прогрев"
        scheduler = e.depth_scheduler
        streams = []
        for stream in e.streams:
            streams.append(
                {
                    "endpoint": "business" if stream.url == cfg.market.ws_business_url else "public",
                    "url": stream.url,
                    "connected": stream.connected,
                    "subscriptions": len(stream.subscriptions),
                    "books": sum(1 for s in stream.subscriptions if s.channel == "books"),
                    "reconnects": max(0, stream.reconnects - 1),
                    "budget": stream.budget(now=now),
                }
            )
        slots_used = len(scheduler.slots()) if scheduler is not None else len(e.static_books)
        counts = self.logs.counts(since=now - 300)
        return {
            "version": __version__,
            "now": now,
            "phase": phase,
            "startedAt": started,
            "uptimeS": now - started if started else 0.0,
            "warmupLeftS": warmup_left,
            "instruments": len(e.states),
            "configPath": cfg.source_path,
            "dryRun": e.dry_run,
            "muted": e.alerts.muted,
            "sinks": e.alerts.sink_names,
            "alertsSent": e.alerts.sent,
            "lastEvalAt": e.last_eval_at,
            "lastEvalMs": round(e.last_eval_ms, 1),
            "feedLagS": e.feed_lag_s,
            "paper": {
                "enabled": cfg.paper.enabled,
                "start": cfg.paper.start_balance,
                "balance": round(e.paper.balance(), 2),
                "open": next((t.inst_id for t in e.paper.trades.values() if t.taken and t.status == "open"), None),
            },
            "streams": streams,
            "depth": {
                "mode": cfg.depth.mode,
                "source": cfg.depth.source,
                "slots": slots_used,
                "slotsLimit": scheduler.limit() if scheduler is not None else cfg.depth.book_slots,
                "slotsConfigured": cfg.depth.book_slots,
                "pending": len(scheduler.pending()) if scheduler is not None else 0,
                "cooling": len(scheduler.cooling()) if scheduler is not None else 0,
                "ttlS": cfg.depth.book_ttl_s,
            },
            "activeDrops": len(e.monitor.active()),
            "logs": {
                "errors5m": counts.get("ERROR", 0) + counts.get("CRITICAL", 0),
                "warnings5m": counts.get("WARNING", 0),
            },
            "history": e.history is not None,
            "pendingRestart": sorted(self.pending_restart),
            "telegram": e.alerts.telegram_client is not None,
            "authRequired": bool(self.token),
        }

    async def drops(self, _request: web.Request) -> web.Response:
        now = time.time()
        e = self.engine
        active = []
        for ep in e.monitor.active():
            item = ep.to_dict()
            state = e.states.get(ep.inst_id)
            item["hasBook"] = e.has_book(ep.inst_id)
            item["turnover"] = state.turnover if state else None
            active.append(item)

        slots = []
        scheduler = e.depth_scheduler
        ttl = max(30.0, e.cfg.depth.book_ttl_s)
        if scheduler is not None:
            for slot in sorted(scheduler.slots(), key=lambda s: s.drop_pct):
                slots.append(
                    {
                        "instId": slot.inst_id,
                        "dropPct": slot.drop_pct,
                        "ageS": now - slot.subscribed_at,
                        "ttlLeftS": max(0.0, ttl - (now - slot.last_used)),
                        "static": False,
                        "book": self._book_brief(e.states.get(slot.inst_id), now),
                    }
                )
        for inst_id in sorted(e.static_books):
            slots.append(
                {
                    "instId": inst_id,
                    "dropPct": None,
                    "ageS": None,
                    "ttlLeftS": None,
                    "static": True,
                    "book": self._book_brief(e.states.get(inst_id), now),
                }
            )
        pending = []
        if scheduler is not None:
            pending = [
                {"instId": iid, "dropPct": pct} for iid, pct in sorted(scheduler.pending().items(), key=lambda x: x[1])
            ]
        return _json({"active": active, "slots": slots, "pending": pending})

    def _book_brief(self, state: InstrumentState | None, now: float) -> dict[str, Any] | None:
        if state is None:
            return None
        book = state.book
        depth = max(1, self.engine.cfg.depth.levels)
        bids, asks = book.bids[:depth], book.asks[:depth]
        return {
            "ageS": now - book.updated_at if book.updated_at else None,
            "levels": min(len(bids), len(asks)),
            "totalNotional": sum(lv.notional for lv in bids) + sum(lv.notional for lv in asks),
            "authoritative": book.authoritative,
            "gaps": book.gaps,
        }

    # ---------- история ----------

    async def alerts(self, request: web.Request) -> web.Response:
        history = self._history()
        q = request.query
        return _json(
            history.alerts(
                inst=q.get("inst") or None,
                window=q.get("window") or None,
                since=_since(q.get("hours")),
                min_score=_int(q.get("min_score")),
                limit=_int(q.get("limit")) or 50,
                offset=_int(q.get("offset")) or 0,
            )
        )

    async def alert(self, request: web.Request) -> web.Response:
        alert_id = _int(request.match_info["alert_id"])
        data = self._history().alert(alert_id) if alert_id is not None else None
        if data is None:
            return _error(404, "алерт не найден")
        return _json(data)

    async def episodes(self, request: web.Request) -> web.Response:
        q = request.query
        alerted = {"1": True, "0": False}.get(q.get("alerted", ""))
        return _json(
            self._history().episodes(
                inst=q.get("inst") or None,
                since=_since(q.get("hours")),
                status=q.get("status") or None,
                alerted=alerted,
                limit=_int(q.get("limit")) or 50,
                offset=_int(q.get("offset")) or 0,
            )
        )

    async def paper(self, _request: web.Request) -> web.Response:
        e = self.engine
        return _json(e.paper.summary(e.states, now=time.time()))

    async def stats(self, request: web.Request) -> web.Response:
        days = _int(request.query.get("days")) or 7
        return _json(self._history().stats(since=time.time() - max(1, days) * 86400))

    def _history(self) -> History:
        if self.engine.history is None:
            raise web.HTTPServiceUnavailable(
                text=json.dumps({"error": "история отключена (dashboard.history_path)"}, ensure_ascii=False),
                content_type="application/json",
            )
        return self.engine.history

    # ---------- инструменты ----------

    async def instruments(self, request: web.Request) -> web.Response:
        query = (request.query.get("q") or "").strip().upper()
        limit = _int(request.query.get("limit")) or 50
        e = self.engine
        found = [s for s in e.states.values() if query in s.inst_id] if query else list(e.states.values())
        found.sort(key=lambda s: s.turnover, reverse=True)
        return _json(
            [
                {"instId": s.inst_id, "instType": s.inst_type, "turnover": s.turnover, "hasBook": e.has_book(s.inst_id)}
                for s in found[: max(1, min(limit, 1000))]
            ]
        )

    async def instrument(self, request: web.Request) -> web.Response:
        inst_id = request.match_info["inst_id"].upper()
        state = self.engine.states.get(inst_id)
        if state is None:
            return _error(404, f"инструмента {inst_id} нет в скане")
        return _json(self.instrument_payload(state))

    def instrument_payload(self, state: InstrumentState, *, now: float | None = None) -> dict[str, Any]:
        now = time.time() if now is None else now
        e = self.engine
        cfg = e.cfg
        price = state.price(now=now, stale_after_s=cfg.engine.trade_stale_after_s)

        windows = []
        for label, secs in WINDOW_SECONDS.items():
            hit = state.drops.drop(secs, threshold_pct=0.0, price=price, now=now) if price else None
            windows.append(
                {
                    "window": label,
                    "thresholdPct": cfg.windows.get(label),
                    "dropPct": hit.drop_pct if hit else 0.0,
                    "high": hit.high if hit else None,
                    "highTs": hit.high_ts if hit else None,
                }
            )

        cvd = state.trades.snapshot(window_s=cfg.cvd.window_s, burst_s=cfg.cvd.burst_s, now=now)
        liq = state.liq.snapshot(window_s=cfg.liquidation.window_s, now=now)
        reversal = state.trades.reversal_ratio(window_s=cfg.cvd.window_s, now=now)
        has_book = e.has_book(state.inst_id)
        book_data = None
        signals = None
        if has_book and state.book.updated_at:
            book = state.book.summary(cfg.depth, now=now)
            book_data = _book_dict(book)
            book_data["passed"] = passed_liquidity(cfg, book)
            found = detect_signals(cfg, book=book, cvd=cvd, liq=liq, reversal_ratio=reversal)
            signals = {"score": found.score, "hits": list(found.hits), "minScore": cfg.signals.min_score}

        candles = [
            [c.ts, c.open, c.high, c.low, c.close] for c in state.candles.slice(cfg.max_window_s(), now=now)
        ][-180:]
        episode = e.monitor.get(state.inst_id)
        recent = (
            e.history.alerts(inst=state.inst_id, limit=20)["items"] if e.history is not None else []
        )
        recent = [a for a in recent if a["instId"] == state.inst_id]
        cooldown = {
            label: ts + cfg.alerts.cooldown_s - now
            for (iid, label), ts in e.alerts.cooldown.snapshot().items()
            if iid == state.inst_id and ts + cfg.alerts.cooldown_s > now
        }
        last_trade = state.last_trade
        return {
            "instId": state.inst_id,
            "instType": state.inst_type,
            "ctVal": state.ct_val,
            "turnover": state.turnover,
            "price": price,
            "lastTradeAgeS": now - last_trade.ts if last_trade else None,
            "windows": windows,
            "hasBook": has_book,
            "canRequestBook": e.depth_scheduler is not None,
            "book": book_data,
            "cvd": {
                "windowS": cvd.window_s,
                "cvd": cvd.cvd,
                "buy": cvd.buy_notional,
                "sell": cvd.sell_notional,
                "takerRatio": cvd.taker_ratio,
                "reversalRatio": reversal,
            },
            "liq": {
                "windowS": liq.window_s,
                "earlier": liq.earlier_notional,
                "recent": liq.recent_notional,
                "decayRatio": liq.decay_ratio,
            },
            "signals": signals,
            "candles": candles,
            "episode": episode.to_dict() if episode else None,
            "alerts": recent,
            "cooldownLeftS": cooldown,
        }

    # ---------- журнал ----------

    async def log_records(self, request: web.Request) -> web.Response:
        q = request.query
        return _json(
            self.logs.records(after=_int(q.get("after")) or 0, level=q.get("level"), limit=_int(q.get("limit")) or 300)
        )

    # ---------- конфиг ----------

    async def get_config(self, _request: web.Request) -> web.Response:
        cfg = self.engine.cfg
        # Редактор JSON правит файл, а не действующий конфиг: в действующем есть
        # CLI-переопределения (-s, --no-telegram), которым в файле не место.
        file_data = None
        if cfg.source_path:
            try:
                file_data = read_config_file(cfg.source_path)
            except (OSError, ValueError) as exc:
                log.warning("дашборд: не удалось прочитать %s: %s", cfg.source_path, exc)
        return _json(
            {
                "config": json.loads(dump_config(cfg)),
                "file": file_data,
                "path": cfg.source_path,
                "pendingRestart": sorted(self.pending_restart),
            }
        )

    async def put_config(self, request: web.Request) -> web.Response:
        try:
            patch = await request.json()
        except json.JSONDecodeError:
            return _error(400, "тело запроса — не JSON")
        if not isinstance(patch, dict) or not patch:
            return _error(400, "нужен объект с изменениями конфига")
        result = self.apply_patch(patch)
        if "errors" in result:
            return _json(result, status=400)
        return _json(result)

    def apply_patch(self, patch: dict[str, Any]) -> dict[str, Any]:
        """Проверить правку, записать её в файл конфига и применить на лету."""
        e = self.engine
        current = json.loads(dump_config(e.cfg))
        errors = validate_patch(patch, current)
        if errors:
            return {"errors": errors}
        path = e.cfg.source_path
        # Правку накладываем на файл, а не на текущий конфиг: иначе в файл утекли бы
        # CLI-переопределения (-s, --no-telegram) и дефолты, которых там не было.
        try:
            base = read_config_file(path) if path else current
        except (OSError, ValueError) as exc:
            return {"errors": [f"не удалось прочитать {path}: {exc}"]}
        merged = merge_config(base, patch)
        fresh = config_from_dict(merged, source_path=path)
        # файл отличается от действующего конфига и CLI-переопределениями (-s → market);
        # о перезапуске говорим только про то, что меняет сама правка
        restart = [name for name in e.cfg.restart_required_changes(fresh) if _touches(patch, name)]
        if path:
            write_config_file(path, merged)
            e.watcher.mark_seen()
        e.apply_config(fresh)
        self.pending_restart.update(restart)
        log.info("дашборд: конфиг изменён (%s)", ", ".join(sorted(_leaf_paths(patch))))
        return {"ok": True, "saved": bool(path), "path": path, "restartRequired": sorted(self.pending_restart)}

    # ---------- управление ----------

    async def control(self, request: web.Request) -> web.Response:
        action = request.match_info["action"]
        try:
            body = await request.json()
        except json.JSONDecodeError:
            body = {}
        if not isinstance(body, dict):
            body = {}
        e = self.engine
        loop = asyncio.get_running_loop()

        if action == "mute":
            e.alerts.muted = bool(body.get("muted", not e.alerts.muted))
            log.info("дашборд: тихий режим %s", "включён" if e.alerts.muted else "выключен")
            return _json({"ok": True, "muted": e.alerts.muted})

        if action == "paper-reset":
            if e.history is None:
                return _error(400, "история выключена — счёту негде хранить сделки")
            if not e.replay_paper(reset=True):
                return _error(409, "пересчёт уже идёт")
            log.info("дашборд: виртуальный счёт пересчитывается с нуля")
            return _json({"ok": True})

        if action == "telegram-test":
            client = e.alerts.telegram_client
            if client is None:
                return _error(400, "Telegram не подключён (нет токена/чата, --dry-run или --no-telegram)")
            delivered = await client.send_text("<b>CryptoDump</b>: проверка связи из дашборда")
            return _json({"ok": delivered > 0, "delivered": delivered})

        if action in ("restart", "stop"):
            log.warning("дашборд: %s по команде пользователя", "перезапуск" if action == "restart" else "остановка")
            # ответ должен успеть уйти до того, как сервер начнёт гаснуть
            loop.call_later(0.3, e.request_restart if action == "restart" else e.stop)
            return _json({"ok": True})

        inst_id = str(body.get("instId") or "").upper()
        if inst_id not in e.states:
            return _error(404, f"инструмента {inst_id or '—'} нет в скане")

        if action == "book":
            scheduler = e.depth_scheduler
            if scheduler is None:
                return _error(400, f"стакан управляется вручную только в режиме ondemand (сейчас {e.cfg.depth.mode})")
            if body.get("op") == "release":
                return _json({"ok": await scheduler.release(inst_id)})
            scheduler.want(inst_id, drop_pct=MANUAL_BOOK_DROP_PCT)
            log.info("дашборд: запрошен стакан %s", inst_id)
            return _json({"ok": True})

        if action == "cooldown-reset":
            for label in WINDOW_SECONDS:
                e.alerts.cooldown.reset(inst_id, label)
            return _json({"ok": True})

        return _error(404, f"неизвестное действие {action}")


# ---------- проверка правок конфига ----------


def validate_patch(patch: dict[str, Any], current: dict[str, Any], prefix: tuple[str, ...] = ()) -> list[str]:
    """Типы берём из текущего конфига: сканер не должен получить строку вместо порога."""
    errors: list[str] = []
    for key, value in patch.items():
        where = ".".join((*prefix, key))
        if key not in current:
            errors.append(f"{where}: неизвестный ключ")
            continue
        ref = current[key]
        if not prefix and key == "windows":
            errors += _validate_windows(value)
            continue
        if isinstance(ref, dict):
            if not isinstance(value, dict):
                errors.append(f"{where}: ожидался объект")
            else:
                errors += validate_patch(value, ref, (*prefix, key))
            continue
        allowed = ENUMS.get((*prefix, key)) if len(prefix) == 1 else None
        if allowed is not None and value not in allowed:
            errors.append(f"{where}: допустимо {', '.join(allowed)}")
        elif isinstance(ref, bool):
            if not isinstance(value, bool):
                errors.append(f"{where}: ожидалось true/false")
        elif isinstance(ref, (int, float)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                errors.append(f"{where}: ожидалось число")
            elif value < 0:
                errors.append(f"{where}: не может быть отрицательным")
            elif isinstance(ref, int) and not float(value).is_integer():
                errors.append(f"{where}: ожидалось целое")
        elif isinstance(ref, str):
            if not isinstance(value, str):
                errors.append(f"{where}: ожидалась строка")
        elif isinstance(ref, list):
            if not isinstance(value, list):
                errors.append(f"{where}: ожидался список")
        elif ref is None and value is not None and not isinstance(value, str):
            errors.append(f"{where}: ожидалась строка или null")
    return errors


def _validate_windows(value: Any) -> list[str]:
    if not isinstance(value, dict) or not value:
        return ["windows: нужен объект {окно: порог в %}"]
    errors = []
    for label, pct in value.items():
        if label not in WINDOW_SECONDS:
            errors.append(f"windows.{label}: неизвестное окно (есть {', '.join(WINDOW_SECONDS)})")
        elif isinstance(pct, bool) or not isinstance(pct, (int, float)) or not math.isfinite(pct) or pct <= 0:
            errors.append(f"windows.{label}: нужен порог > 0")
    return errors


RESTART_KEYS = {
    "depth.poll": ("depth", {"levels", "poll_interval_s"}),
    "depth.mode": ("depth", {"mode", "source"}),
}


def _touches(patch: dict[str, Any], restart_name: str) -> bool:
    """Затрагивает ли правка поле из ``Config.restart_required_changes``."""
    if restart_name in RESTART_KEYS:
        section, keys = RESTART_KEYS[restart_name]
        return bool(keys & set(patch.get(section) or {}))
    if restart_name == "dashboard":
        return bool(set(patch.get("dashboard") or {}) - {"episode_gap_s"})
    return restart_name in patch


def _leaf_paths(patch: dict[str, Any], prefix: str = "") -> list[str]:
    paths: list[str] = []
    for key, value in patch.items():
        if isinstance(value, dict) and key != "windows":
            paths += _leaf_paths(value, f"{prefix}{key}.")
        else:
            paths.append(f"{prefix}{key}")
    return paths


# ---------- утилиты ----------


def _book_dict(book: BookSummary) -> dict[str, Any]:
    wall = book.wall
    return {
        "bestBid": book.best_bid,
        "bestAsk": book.best_ask,
        "bidNotional": book.bid_notional,
        "askNotional": book.ask_notional,
        "totalNotional": book.total_notional,
        "imbalance": book.imbalance,
        "levels": book.levels,
        "fresh": book.fresh,
        "fullDepth": book.full_depth,
        "ageS": time.time() - book.ts if book.ts else None,
        "wall": {"px": wall.px, "notional": wall.notional, "ratio": book.wall_ratio} if wall else None,
    }


def _loopback_host(host: str) -> bool:
    name = host.rsplit(":", 1)[0] if not host.endswith("]") else host
    return name.strip("[]") in LOOPBACK


def _clean(value: Any) -> Any:
    """``inf``/``nan`` ломают JSON.parse в браузере — заменяем на null."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    return value


def _json(data: Any, *, status: int = 200) -> web.Response:
    return web.json_response(
        _clean(data), status=status, dumps=lambda obj: json.dumps(obj, ensure_ascii=False, default=str)
    )


def _error(status: int, message: str) -> web.Response:
    return _json({"error": message}, status=status)


def _int(value: str | None) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except ValueError:
        return None


def _since(hours: str | None) -> float | None:
    value = _int(hours)
    return time.time() - value * 3600 if value else None
