"""WebSocket OKX: подписки, ping/pong, реконнект с backoff.

Каналы: ``trades``, ``books``, ``candle1m``, ``liquidation-orders`` (по ``instType``).

Два эндпоинта, проверено на живом API:

* ``/ws/v5/public`` — сделки, стакан, ликвидации;
* ``/ws/v5/business`` — свечи (``candle1m`` и другие интервалы). На публичном
  эндпоинте подписка на свечи возвращает ``60018 channel doesn't exist``.

Порт 443 вместо 8443: OKX присылает ``notice 64010`` о выводе 8443 из эксплуатации.
Канал ``books`` (400 уровней, инкременты ~100 мс) работает без логина и является
основным источником стакана; ``resubscribe`` нужен для его ресинка.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
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

    Один экземпляр держит одну подписку-группу. При разрыве переподключается
    с экспоненциальным backoff (кап ``max_backoff_s``) и заново подписывается
    на весь набор — вызывающему коду ничего делать не нужно.
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
    ) -> None:
        self.url = url
        self.subscriptions = subscriptions
        self.handler = handler
        self.chunk_size = max(1, chunk_size)
        self.max_backoff_s = max_backoff_s
        self.ping_interval_s = ping_interval_s
        self.on_reconnect = on_reconnect
        self._connected_at: float = 0.0
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self.connected = False
        self.reconnects = 0

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

            if self.on_reconnect is not None and self.reconnects > 0:
                await self.on_reconnect(self._connected_at)
            self.reconnects += 1

            await self._subscribe_all(ws)
            pinger = asyncio.create_task(self._ping_loop(ws, stop), name="okx-ws-ping")
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
                pinger.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await pinger

    async def resubscribe(self, arg: dict[str, str]) -> bool:
        """Переподписать канал, чтобы получить свежий снимок.

        Нужно для ресинка стакана: OKX на ``subscribe`` присылает полный snapshot
        с актуальным ``seqId``, и цепочка инкрементов начинается заново. Это дешевле
        и точнее, чем REST-снимок, который нельзя склеить с WS-инкрементами.
        """
        ws = self._ws
        if ws is None or ws.closed:
            return False
        try:
            await ws.send_json({"op": "unsubscribe", "args": [arg]})
            await asyncio.sleep(0.05)
            await ws.send_json({"op": "subscribe", "args": [arg]})
            log.debug("переподписка на %s", arg)
            return True
        except Exception as exc:  # noqa: BLE001 — при ошибке просто упадёт реконнект
            log.debug("переподписка не удалась (%s): %s", arg, exc)
            return False

    async def subscribe(self, args: list[dict[str, str]]) -> bool:
        """Добавить подписки на лету (стакан подключается по требованию).

        Подписки запоминаются в ``subscriptions`` — после реконнекта они восстановятся
        автоматически, и динамически добавленный стакан не потеряется.
        """
        ws = self._ws
        if ws is None or ws.closed:
            return False
        if not args:
            return True
        for chunk in _chunk_args(args, self.chunk_size):
            try:
                await ws.send_json({"op": "subscribe", "args": chunk})
            except Exception as exc:  # noqa: BLE001
                log.debug("подписка на лету не удалась: %s", exc)
                return False
            await asyncio.sleep(0.02)
        for arg in args:
            sub = Subscription(
                channel=arg["channel"], inst_id=arg.get("instId"), inst_type=arg.get("instType")
            )
            if sub not in self.subscriptions:
                self.subscriptions.append(sub)
        log.debug("подписано на лету: %d каналов", len(args))
        return True

    async def unsubscribe(self, args: list[dict[str, str]]) -> bool:
        """Снять подписки на лету (гасим стакан, когда он больше не нужен).

        Из ``subscriptions`` подписка убирается даже без живого соединения: иначе
        после реконнекта ``_subscribe_all`` вернул бы стакан, которым уже никто не владеет.
        """
        if not args:
            return True
        drops = {(arg["channel"], arg.get("instId"), arg.get("instType")) for arg in args}
        self.subscriptions = [
            sub for sub in self.subscriptions if (sub.channel, sub.inst_id, sub.inst_type) not in drops
        ]
        ws = self._ws
        if ws is None or ws.closed:
            return False
        for chunk in _chunk_args(args, self.chunk_size):
            try:
                await ws.send_json({"op": "unsubscribe", "args": chunk})
            except Exception as exc:  # noqa: BLE001
                log.debug("отписка не удалась: %s", exc)
                return False
            await asyncio.sleep(0.02)
        log.debug("отписано на лету: %d каналов", len(args))
        return True

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

    # ---------- сообщения ----------

    async def _subscribe_all(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        for chunk in _chunks(self.subscriptions, self.chunk_size):
            args = [sub.to_arg() for sub in chunk]
            await ws.send_json({"op": "subscribe", "args": args})
            log.debug("подписка на %d каналов: %s", len(args), chunk[0].channel)
            await asyncio.sleep(0.05)  # не бомбим биржу пачкой

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
            log.error("WS ошибка: code=%s msg=%s arg=%s", payload.get("code"), payload.get("msg"), payload.get("arg"))
            return
        if event in ("subscribe", "unsubscribe", "login"):
            log.debug("WS %s: %s", event, payload.get("arg"))
            return

        arg = payload.get("arg") or {}
        if "data" in payload and arg.get("channel"):
            await self.handler(payload)


def _chunks(items: list[Subscription], size: int) -> Iterable[list[Subscription]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _chunk_args(args: list[dict[str, str]], size: int) -> Iterable[list[dict[str, str]]]:
    for i in range(0, len(args), max(1, size)):
        yield args[i : i + max(1, size)]
