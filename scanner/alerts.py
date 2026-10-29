"""Алерты: дедуп/кулдаун и синки (консоль, JSONL, вебхук, Telegram)."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Protocol

from .config import AlertsConfig
from .models import Alert
from .telegram import TelegramClient, build_client

log = logging.getLogger(__name__)


class Sink(Protocol):
    async def emit(self, alert: Alert) -> None: ...
    async def close(self) -> None: ...


class ConsoleSink:
    async def emit(self, alert: Alert) -> None:
        print("\n" + alert.console_text() + "\n", flush=True)

    async def close(self) -> None:
        return None


class JsonlSink:
    """Пишет алерты построчно — база для постфактум-разбора и тюнинга порогов."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

    async def emit(self, alert: Alert) -> None:
        line = json.dumps(alert.to_dict(), ensure_ascii=False)
        async with self._lock:
            await asyncio.to_thread(self._append, line)

    def _append(self, line: str) -> None:
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    async def close(self) -> None:
        return None


class WebhookSink:
    """POST JSON на внешний URL (n8n, свой сервис). Telegram — через ``TelegramSink``."""

    def __init__(self, url: str, *, timeout_s: float = 10.0) -> None:
        self.url = url
        self.timeout_s = timeout_s
        self._session = None

    async def emit(self, alert: Alert) -> None:
        import aiohttp

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout_s))
        payload = {"text": alert.console_text(), "alert": alert.to_dict()}
        try:
            async with self._session.post(self.url, json=payload) as resp:
                if resp.status >= 400:
                    log.warning("вебхук вернул %s на %s", resp.status, alert.inst_id)
        except Exception as exc:  # noqa: BLE001 — вебхук не должен ронять сканер
            log.warning("вебхук не доставлен (%s): %s", alert.inst_id, exc)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None


class TelegramSink:
    """Отправка алерта в Telegram. Токен и chat id — из окружения (``.env``)."""

    def __init__(self, client: TelegramClient) -> None:
        self.client = client

    async def emit(self, alert: Alert) -> None:
        try:
            delivered = await self.client.send_alert(alert)
        except Exception as exc:  # noqa: BLE001 — алерт не должен ронять сканер
            log.warning("telegram не доставлен (%s): %s", alert.inst_id, exc)
            return
        if delivered == 0:
            log.warning("telegram: %s не доставлен ни в один чат", alert.inst_id)

    async def close(self) -> None:
        await self.client.close()


class Cooldown:
    """Кулдаун на пару (инструмент, окно), чтобы не спамить, пока цена висит на дне."""

    def __init__(self, cooldown_s: float) -> None:
        self.cooldown_s = cooldown_s
        self._last: dict[tuple[str, str], float] = {}

    def allow(self, inst_id: str, window_label: str, *, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        key = (inst_id, window_label)
        last = self._last.get(key)
        if last is not None and now - last < self.cooldown_s:
            return False
        self._last[key] = now
        return True

    def reset(self, inst_id: str, window_label: str) -> None:
        self._last.pop((inst_id, window_label), None)

    def restore(self, inst_id: str, window_label: str, ts: float) -> None:
        """Восстановить отметку кулдауна после рестарта (см. ``state.load_state``)."""
        self._last[(inst_id, window_label)] = ts

    def snapshot(self) -> dict[tuple[str, str], float]:
        """Копия отметок — для сохранения состояния."""
        return dict(self._last)

    def suppress(self, inst_id: str, window_labels: list[str], *, now: float | None = None) -> None:
        """Заглушить окна, которые уже «озвучены» другим окном того же инструмента.

        Один дамп обычно пробивает несколько окон сразу. Алерт уходит один (по самому
        глубокому окну), но без этого остальные окна прислали бы свои алерты в
        следующие секунды, как только станут «самыми глубокими».
        """
        now = time.time() if now is None else now
        for label in window_labels:
            self._last[(inst_id, label)] = now


class AlertRouter:
    """Единая точка отправки: кулдаун → все синки параллельно."""

    def __init__(self, cfg: AlertsConfig, *, dry_run: bool = False) -> None:
        self.cfg = cfg
        self.dry_run = dry_run
        self.cooldown = Cooldown(cfg.cooldown_s)
        self.sinks: list[Sink] = []
        for name in cfg.sinks:
            sink = self._make_sink(name)
            if sink is not None:
                self.sinks.append(sink)
        self.sent = 0

    def _make_sink(self, name: str) -> Sink | None:
        if name == "console":
            return ConsoleSink()
        if name == "jsonl":
            return JsonlSink(self.cfg.jsonl_path)
        if name == "telegram":
            if self.dry_run:
                log.info("--dry-run: Telegram отключён")
                return None
            client = build_client(self.cfg.telegram)
            if client is None:
                return None
            return TelegramSink(client)
        if name == "webhook":
            if self.dry_run:
                log.info("--dry-run: вебхук отключён")
                return None
            if not self.cfg.webhook_url:
                log.warning("синк webhook включён, но alerts.webhook_url не задан — пропускаю")
                return None
            return WebhookSink(self.cfg.webhook_url)
        log.warning("неизвестный синк алертов: %r", name)
        return None

    async def publish(self, alert: Alert) -> bool:
        if not self.cooldown.allow(alert.inst_id, alert.window_label, now=alert.ts):
            log.debug("кулдаун: %s %s", alert.inst_id, alert.window_label)
            return False
        results = await asyncio.gather(*(s.emit(alert) for s in self.sinks), return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                log.warning("синк упал: %s", res)
        self.sent += 1
        return True

    async def close(self) -> None:
        await asyncio.gather(*(s.close() for s in self.sinks), return_exceptions=True)
