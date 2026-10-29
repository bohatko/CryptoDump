"""WebSocket OKX: подписки, ping/pong, реконнект с backoff.

Каналы: ``trades``, ``books``, ``candle1m``, ``liquidation-orders`` (по ``instType``).

Два эндпоинта, проверено на живом API:

* ``/ws/v5/public`` — сделки, стакан, ликвидации;
* ``/ws/v5/business`` — свечи (``candle1m`` и другие интервалы). На публичном
  эндпоинте подписка на свечи возвращает ``60018 channel doesn't exist``.

Порт 443 вместо 8443: OKX присылает ``notice 64010`` о выводе 8443 из эксплуатации.
Канал ``books`` (400 уровней, инкременты ~100 мс) работает без логина и является
основным источником стакана; ``resubscribe`` нужен для его ресинка.

Лимит OKX — 480 запросов ``subscribe``/``unsubscribe``/``login`` на соединение в час.
Сверх него биржа отвечает ``60014``, а после серии превышений перестаёт присылать
данные по соединению, хотя оно остаётся открытым (видели вживую: сделки встали по
всем 455 инструментам сразу). Поэтому ``subscribe``/``unsubscribe``/``resubscribe``
только меняют желаемый набор, а разницу с тем, что подписано на соединении,
отправляет ``flush``: одним запросом за тик, пачками до ``chunk_size`` аргументов
и не больше ``ops_per_hour`` запросов в час. Повторный ``subscribe`` на уже
подписанный ``books`` нового снимка не даёт (проверено), поэтому ресинк стоит
два запроса: ``unsubscribe`` и ``subscribe``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Iterable

import aiohttp

from .aio import sleep_or_stop

log = logging.getLogger(__name__)

CHANNELS = ("trades", "candle1m")

# Каналы разложены по эндпоинтам OKX: свечи отдаёт business-эндпоинт, остальное — public.
# На неверном эндпоинте OKX возвращает ошибку 60018 «channel doesn't exist».
PUBLIC_CHANNELS = ("trades", "books", "books5", "liquidation-orders")
BUSINESS_CHANNELS = ("candle1m", "candle1H", "candle1D", "mark-price", "index-tickers")

MessageHandler = Callable[[dict[str, Any]], Awaitable[None]]

SubKey = tuple[str, str | None, str | None]

# 480 в час минус запас: биржа считает окно по-своему, а часть уходит на ресинки
OPS_PER_HOUR = 400
FLUSH_INTERVAL_S = 0.4  # не больше одного запроса за тик: ~2.5 в секунду
RATE_LIMIT_CODE = "60014"
RATE_LIMIT_PAUSE_MAX_S = 60.0
INFLIGHT_TTL_S = 30.0
# Канал, подписанный на столько инструментов, не может молчать минуту: на всём
# рынке сделки идут сотнями в секунду. Молчание = биржа перестала присылать данные.
SILENT_CHANNEL_MIN_SUBS = 20
SILENT_CHANNEL_S = 60.0


@dataclass(frozen=True, slots=True)
class Subscription:
    """Аргумент подписки OKX: канал + либо ``instId``, либо ``instType``.

    ``liquidation-orders`` подписывается на тип инструмента (``SWAP``/``FUTURES``),
    остальные каналы — на конкретный ``instId``.
    """

    channel: str
    inst_id: str | None = None
    inst_type: str | None = None

    def to_arg(self) -> dict[str, str]:
        arg = {"channel": self.channel}
        if self.inst_id:
            arg["instId"] = self.inst_id
        elif self.inst_type:
            arg["instType"] = self.inst_type
        return arg

    @property
    def key(self) -> SubKey:
        return (self.channel, self.inst_id, self.inst_type)

    @classmethod
    def from_arg(cls, arg: dict[str, str]) -> Subscription:
        return cls(channel=arg["channel"], inst_id=arg.get("instId"), inst_type=arg.get("instType"))


def per_instrument(inst_ids: Iterable[str], channels: Iterable[str] = CHANNELS) -> list[Subscription]:
    return [Subscription(channel=ch, inst_id=inst_id) for inst_id in inst_ids for ch in channels]


def liquidations(inst_types: Iterable[str]) -> list[Subscription]:
    return [Subscription(channel="liquidation-orders", inst_type=t) for t in inst_types]


def split_by_endpoint(
    subs: list[Subscription],
) -> tuple[list[Subscription], list[Subscription], list[Subscription]]:
    """Разложить подписки на ``(public, business, неизвестные)``.

    Неизвестные не отправляются вовсе: OKX отклоняет всю пачку ``subscribe`` с
    несуществующим каналом, и тогда теряются и валидные подписки тоже.
    """
    public: list[Subscription] = []
    business: list[Subscription] = []
    unknown: list[Subscription] = []
    for sub in subs:
        if sub.channel in BUSINESS_CHANNELS:
            business.append(sub)
        elif sub.channel in PUBLIC_CHANNELS:
            public.append(sub)
        else:
            unknown.append(sub)
    return public, business, unknown


class OkxPublicStream:
    """Долгоживущее публичное соединение с автоматической переподпиской.

    ``subscriptions`` — желаемый набор, он переживает реконнект. Что из него уже
    отправлено бирже на текущем соединении, знает только ``flush``.
    """

    def __init__(
        self,
        url: str,
        subscriptions: list[Subscription],
        handler: MessageHandler,
        *,
        chunk_size: int = 64,
        max_backoff_s: float = 30.0,
        ping_interval_s: float = 20.0,
        on_reconnect: Callable[[float], Awaitable[None]] | None = None,
        ops_per_hour: int = OPS_PER_HOUR,
        flush_interval_s: float = FLUSH_INTERVAL_S,
    ) -> None:
        self.url = url
        self.subscriptions = subscriptions
        self.handler = handler
        self.chunk_size = max(1, chunk_size)
        self.max_backoff_s = max_backoff_s
        self.ping_interval_s = ping_interval_s
        self.on_reconnect = on_reconnect
        self.ops_per_hour = max(1, ops_per_hour)
        self.flush_interval_s = flush_interval_s
        self._connected_at: float = 0.0
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self.connected = False
        self.reconnects = 0
        self.rejected = 0  # сколько запросов OKX отклонил по лимиту (60014)
        self.silent_reconnects = 0
        self.forced_reconnects = 0
        self._reset_connection_state()

    def _reset_connection_state(self, now: float = 0.0) -> None:
        """Новое соединение начинает с нуля: ничего не подписано, лимит свой."""
        self._server: dict[SubKey, Subscription] = {}
        self._resync: set[SubKey] = set()
        self._failed: set[SubKey] = set()  # отклонены не по лимиту: до реконнекта не повторяем
        self._inflight: dict[str, tuple[str, list[Subscription], float]] = {}
        self._ops: deque[float] = deque()
        self._op_seq = 0
        self._paused_until = 0.0
        self._pause_s = 0.0
        self._budget_warned = False
        self._reconnect_reason: str | None = None
        self._channel_seen: dict[str, float] = {}
        self._session_started = now

    # ---------- жизненный цикл ----------

    async def run(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
            while not stop.is_set():
                started = time.time()
                try:
                    await self._session_loop(session, stop)
                    if stop.is_set():
                        break
                    # соединение закрыто штатно (биржа/сеть) — тоже нужен backoff
                    log.info("WS закрыт, реконнект через %.1f c", backoff)
                    await sleep_or_stop(backoff, stop)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 — WS рвётся по-разному
                    if stop.is_set():
                        break
                    log.warning("WS отключился: %s — реконнект через %.1f c", exc, backoff)
                    await sleep_or_stop(backoff, stop)
                finally:
                    self.connected = False

                # если соединение прожило достаточно долго — начинаем backoff заново
                backoff = 1.0 if (time.time() - started) > 30.0 else min(backoff * 1.8, self.max_backoff_s)

    async def _session_loop(self, session: aiohttp.ClientSession, stop: asyncio.Event) -> None:
        async with session.ws_connect(self.url, max_msg_size=0) as ws:
            log.info("WS подключён: %s (подписок: %d)", self.url, len(self.subscriptions))
            self.connected = True
            self._connected_at = time.time()
            self._ws = ws

            self._reset_connection_state(time.time())
            tasks = [
                asyncio.create_task(self._ping_loop(ws, stop), name="okx-ws-ping"),
                asyncio.create_task(self._flush_loop(ws, stop), name="okx-ws-flush"),
            ]
            if self.on_reconnect is not None and self.reconnects > 0:
                # Догрузка свечей по REST идёт десятки секунд. Ждать её до подписки нельзя:
                # OKX закрывает соединение, если за 30 c на нём не появилось подписок.
                tasks.append(asyncio.create_task(self._reconnect_hook(self._connected_at), name="okx-ws-reconnect"))
            self.reconnects += 1
            try:
                async for msg in ws:
                    if stop.is_set():
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            await self._dispatch(msg.data)
                        except Exception:  # noqa: BLE001 — одно битое сообщение не рвёт поток
                            log.exception("ошибка обработки сообщения WS")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            finally:
                self._ws = None
                for task in tasks:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

    async def _reconnect_hook(self, connected_at: float) -> None:
        assert self.on_reconnect is not None
        try:
            await self.on_reconnect(connected_at)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — сбой догрузки свечей не должен рвать соединение
            log.exception("ошибка обработчика реконнекта")

    # ---------- желаемый набор подписок ----------

    async def subscribe(self, args: list[dict[str, str]]) -> bool:
        """Добавить подписки (стакан по требованию). Отправит их ``flush``.

        ``False`` — соединения нет: вызывающий код повторит позже, набор не меняется.
        """
        if not self._is_open():
            return False
        known = {sub.key for sub in self.subscriptions}
        for arg in args:
            sub = Subscription.from_arg(arg)
            self._failed.discard(sub.key)
            if sub.key not in known:
                self.subscriptions.append(sub)
                known.add(sub.key)
        return True

    async def unsubscribe(self, args: list[dict[str, str]]) -> bool:
        """Снять подписки (стакан больше не нужен). Отправит отписку ``flush``.

        Из ``subscriptions`` подписка убирается даже без живого соединения: иначе
        после реконнекта стакан вернулся бы инструменту, которым уже никто не владеет.
        """
        if not args:
            return True
        drops = {Subscription.from_arg(arg).key for arg in args}
        self.subscriptions = [sub for sub in self.subscriptions if sub.key not in drops]
        self._resync -= drops
        return self._is_open()

    async def resubscribe(self, arg: dict[str, str]) -> bool:
        """Переподписать канал, чтобы получить свежий снимок (ресинк стакана).

        OKX на ``subscribe`` присылает полный snapshot с актуальным ``seqId``, и
        цепочка инкрементов начинается заново. Это дешевле и точнее, чем REST-снимок,
        который нельзя склеить с WS-инкрементами.
        """
        key = Subscription.from_arg(arg).key
        if not self._is_open() or key not in {sub.key for sub in self.subscriptions}:
            return False
        self._resync.add(key)
        return True

    def _is_open(self) -> bool:
        return self._ws is not None and not self._ws.closed

    # ---------- отправка с учётом лимита ----------

    async def _flush_loop(self, ws: aiohttp.ClientWebSocketResponse, stop: asyncio.Event) -> None:
        while not stop.is_set() and not ws.closed:
            now = time.time()
            if self._reconnect_reason is not None:
                log.warning("%s: пересоздаю соединение — %s", _endpoint(self.url), self._reconnect_reason)
                await ws.close()
                return
            if self._silent_channel(now):
                await ws.close()
                return
            try:
                await self.flush(ws, now=now)
            except Exception as exc:  # noqa: BLE001 — отправка упала: соединение рвётся, реконнект повторит всё
                log.debug("отправка подписок не удалась: %s", exc)
                return
            await sleep_or_stop(self.flush_interval_s, stop)

    async def flush(self, ws: Any, *, now: float | None = None) -> bool:
        """Отправить один запрос, приближающий соединение к желаемому набору.

        Сначала отписки (в том числе первая половина ресинка), потом подписки.
        ``False`` — отправлять нечего или лимит не позволяет.
        """
        now = time.time() if now is None else now
        self._expire_inflight(now)
        desired = {sub.key: sub for sub in self.subscriptions}
        self._resync &= set(desired)

        unsub = [sub for key, sub in self._server.items() if key not in desired or key in self._resync]
        sub = [s for key, s in desired.items() if key not in self._server and key not in self._failed]
        if not unsub and not sub:
            return False
        if not self._can_send(now, pending=len(unsub) + len(sub)):
            return False

        if unsub:
            chunk = unsub[: self.chunk_size]
            await self._send(ws, "unsubscribe", chunk, now)
            for item in chunk:
                self._server.pop(item.key, None)
            return True

        chunk = sub[: self.chunk_size]
        await self._send(ws, "subscribe", chunk, now)
        for item in chunk:
            self._server[item.key] = item
            self._resync.discard(item.key)
        return True

    def _can_send(self, now: float, *, pending: int) -> bool:
        if now < self._paused_until:
            return False
        if self._pause_s and now - self._paused_until > RATE_LIMIT_PAUSE_MAX_S:
            self._pause_s = 0.0  # давно не отказывали — следующая пауза снова короткая
        while self._ops and now - self._ops[0] >= 3600.0:
            self._ops.popleft()
        if len(self._ops) < self.ops_per_hour:
            self._budget_warned = False
            return True
        if not self._budget_warned:
            self._budget_warned = True
            wait = 3600.0 - (now - self._ops[0])
            log.warning(
                "%s: лимит подписок на час исчерпан (%d), %d изменений ждут ~%.0f c",
                _endpoint(self.url),
                self.ops_per_hour,
                pending,
                wait,
            )
        return False

    async def _send(self, ws: Any, op: str, subs: list[Subscription], now: float) -> None:
        self._op_seq += 1
        req_id = f"{op[0]}{self._op_seq}"
        await ws.send_json({"id": req_id, "op": op, "args": [s.to_arg() for s in subs]})
        self._inflight[req_id] = (op, list(subs), now)
        self._ops.append(now)
        log.debug("%s %s: %d каналов (%s…)", _endpoint(self.url), op, len(subs), subs[0].channel)

    def _expire_inflight(self, now: float) -> None:
        for req_id in [r for r, (_, _, at) in self._inflight.items() if now - at > INFLIGHT_TTL_S]:
            del self._inflight[req_id]

    def _on_error(self, payload: dict[str, Any], *, now: float) -> None:
        code = str(payload.get("code") or "")
        req = self._inflight.pop(str(payload.get("id") or ""), None)
        if code == RATE_LIMIT_CODE:
            self.rejected += 1
            self._pause_s = min(RATE_LIMIT_PAUSE_MAX_S, max(5.0, self._pause_s * 2))
            self._paused_until = now + self._pause_s
            if req is not None:
                self._revert(req[0], req[1])
            log.warning(
                "%s: OKX отклонил запрос подписки как слишком частый (60014) — пауза %.0f c, затем повтор",
                _endpoint(self.url),
                self._pause_s,
            )
            return
        log.error("WS ошибка: code=%s msg=%s arg=%s", code, payload.get("msg"), payload.get("arg"))
        if req is not None and req[0] == "subscribe":
            # не лимит, а отказ по существу: повторять бессмысленно до реконнекта
            for item in req[1]:
                self._server.pop(item.key, None)
                self._failed.add(item.key)

    def _revert(self, op: str, subs: list[Subscription]) -> None:
        """Отклонённый запрос не выполнен: вернуть учёт, чтобы ``flush`` его повторил."""
        desired = {sub.key for sub in self.subscriptions}
        for item in subs:
            if op == "subscribe":
                self._server.pop(item.key, None)
            else:
                self._server[item.key] = item
                if item.key in desired:
                    # отписка была первой половиной ресинка
                    self._resync.add(item.key)

    def request_reconnect(self, reason: str) -> None:
        """Пересоздать соединение на ближайшем тике ``flush``: подписки восстановятся сами."""
        if self._is_open():
            self.forced_reconnects += 1
            self._reconnect_reason = reason

    def _silent_channel(self, now: float) -> str | None:
        """Массовый канал молчит дольше ``SILENT_CHANNEL_S`` — биржа перестала слать данные."""
        if not self._session_started or now - self._session_started < SILENT_CHANNEL_S:
            return None
        counts: dict[str, int] = {}
        for key in self._server:
            counts[key[0]] = counts.get(key[0], 0) + 1
        for channel, count in counts.items():
            if count < SILENT_CHANNEL_MIN_SUBS:
                continue
            seen = self._channel_seen.get(channel, self._session_started)
            if now - seen > SILENT_CHANNEL_S:
                self.silent_reconnects += 1
                log.warning(
                    "%s: канал %s (%d инструментов) молчит %.0f c — пересоздаю соединение",
                    _endpoint(self.url),
                    channel,
                    count,
                    now - seen,
                )
                return channel
        return None

    def budget(self, *, now: float | None = None) -> dict[str, Any]:
        """Сколько запросов подписки потрачено за час и сколько изменений ждёт отправки."""
        now = time.time() if now is None else now
        used = sum(1 for at in self._ops if now - at < 3600.0)
        desired = {sub.key for sub in self.subscriptions}
        waiting = sum(1 for key in self._server if key not in desired) + len(self._resync)
        waiting += sum(1 for key in desired if key not in self._server and key not in self._failed)
        return {
            "opsHour": used,
            "opsLimit": self.ops_per_hour,
            "waiting": waiting,
            "rejected": self.rejected,
            "pausedS": max(0.0, self._paused_until - now),
            "silentReconnects": self.silent_reconnects,
            "forcedReconnects": self.forced_reconnects,
        }

    # ---------- сообщения ----------

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse, stop: asyncio.Event) -> None:
        """OKX требует текстовый ``ping`` не реже раза в 30 c, иначе закрывает соединение."""
        while not stop.is_set():
            await sleep_or_stop(self.ping_interval_s, stop)
            if stop.is_set() or ws.closed:
                return
            try:
                await ws.send_str("ping")
            except Exception as exc:  # noqa: BLE001
                log.debug("ping не отправлен: %s", exc)
                return

    async def _dispatch(self, raw: str) -> None:
        if raw == "pong":
            return
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            log.debug("нечитаемое сообщение WS: %.120s", raw)
            return

        event = payload.get("event")
        if event == "notice":
            # OKX так сообщает об изменениях API (например, о выводе порта из эксплуатации)
            log.warning("OKX notice %s: %s", payload.get("code"), payload.get("msg"))
            return
        if event == "error":
            self._on_error(payload, now=time.time())
            return
        if event in ("subscribe", "unsubscribe", "login"):
            log.debug("WS %s: %s", event, payload.get("arg"))
            return

        arg = payload.get("arg") or {}
        channel = arg.get("channel")
        if "data" in payload and channel:
            self._channel_seen[channel] = time.time()
            await self.handler(payload)


def _endpoint(url: str) -> str:
    return url.rstrip("/").rsplit("/", 1)[-1] or url
