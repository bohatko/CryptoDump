"""Подписки OKX: раскладка по эндпоинтам и отправка в пределах лимита 480 запросов в час."""

from __future__ import annotations

import asyncio

from scanner.okx_ws import OkxPublicStream, Subscription, liquidations, per_instrument, split_by_endpoint

NOW = 1_700_000_000.0


async def _noop(_payload: dict) -> None:
    return None


class FakeWs:
    closed = False

    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send_json(self, data: dict) -> None:
        self.sent.append(data)


def _stream(subs: list[Subscription] | None = None, **kw) -> tuple[OkxPublicStream, FakeWs]:
    stream = OkxPublicStream("wss://example.invalid/ws/v5/public", list(subs or []), _noop, **kw)
    ws = FakeWs()
    stream._ws = ws  # type: ignore[assignment]  # соединение «открыто», сеть не нужна
    return stream, ws


def _books(*inst_ids: str) -> list[dict[str, str]]:
    return [{"channel": "books", "instId": i} for i in inst_ids]


def _flush_all(stream: OkxPublicStream, ws: FakeWs, *, now: float = NOW, limit: int = 20) -> None:
    for i in range(limit):
        if not asyncio.run(stream.flush(ws, now=now + i)):
            return


def test_unsubscribe_while_disconnected_forgets_subscription() -> None:
    """Иначе после реконнекта books вернулся бы инструменту, у которого уже нет слота."""
    books = Subscription(channel="books", inst_id="BTC-USDT-SWAP")
    trades = Subscription(channel="trades", inst_id="BTC-USDT-SWAP")
    stream = OkxPublicStream("wss://example.invalid", [trades, books], _noop)

    sent = asyncio.run(stream.unsubscribe([{"channel": "books", "instId": "BTC-USDT-SWAP"}]))

    assert sent is False  # соединения нет — на биржу ничего не ушло
    assert stream.subscriptions == [trades]


def test_changes_are_batched_into_one_request() -> None:
    stream, ws = _stream()
    asyncio.run(stream.subscribe(_books("A-USDT-SWAP", "B-USDT-SWAP", "C-USDT-SWAP")))
    assert ws.sent == []  # отправляет только flush

    _flush_all(stream, ws)
    assert len(ws.sent) == 1
    assert ws.sent[0]["op"] == "subscribe" and len(ws.sent[0]["args"]) == 3
    assert ws.sent[0]["id"]  # по id OKX вернёт ошибку именно этого запроса


def test_subscribe_then_unsubscribe_before_flush_costs_nothing() -> None:
    stream, ws = _stream()
    asyncio.run(stream.subscribe(_books("A-USDT-SWAP")))
    asyncio.run(stream.unsubscribe(_books("A-USDT-SWAP")))
    _flush_all(stream, ws)
    assert ws.sent == []


def test_resubscribe_is_unsubscribe_then_subscribe() -> None:
    """Повторный subscribe снимка не даёт (проверено на OKX) — нужна пара запросов."""
    stream, ws = _stream()
    asyncio.run(stream.subscribe(_books("A-USDT-SWAP")))
    _flush_all(stream, ws)
    ws.sent.clear()

    assert asyncio.run(stream.resubscribe({"channel": "books", "instId": "A-USDT-SWAP"}))
    assert asyncio.run(stream.resubscribe({"channel": "books", "instId": "A-USDT-SWAP"}))  # дубль схлопнется
    _flush_all(stream, ws)
    assert [m["op"] for m in ws.sent] == ["unsubscribe", "subscribe"]


def test_hourly_budget_is_never_exceeded() -> None:
    stream, ws = _stream(ops_per_hour=2)
    for i in range(5):
        asyncio.run(stream.subscribe(_books(f"X{i}-USDT-SWAP")))
        asyncio.run(stream.flush(ws, now=NOW + i))
    assert len(ws.sent) == 2
    assert stream.budget(now=NOW + 5)["waiting"] == 3

    # через час окно освободилось — ожидающие ушли одной пачкой
    asyncio.run(stream.flush(ws, now=NOW + 3601))
    assert len(ws.sent) == 3 and len(ws.sent[-1]["args"]) == 3


def test_rate_limit_error_pauses_and_retries_same_request() -> None:
    stream, ws = _stream()
    asyncio.run(stream.subscribe(_books("A-USDT-SWAP")))
    asyncio.run(stream.flush(ws, now=NOW))
    req_id = ws.sent[-1]["id"]

    asyncio.run(stream._dispatch(f'{{"event":"error","code":"60014","msg":"Requests too frequent","id":"{req_id}"}}'))
    stream._paused_until = NOW + 5.0  # время в тесте фиксированное
    assert stream.rejected == 1
    assert not asyncio.run(stream.flush(ws, now=NOW + 1))  # пауза
    assert asyncio.run(stream.flush(ws, now=NOW + 6))
    assert ws.sent[-1]["op"] == "subscribe" and ws.sent[-1]["args"] == _books("A-USDT-SWAP")


def test_non_rate_limit_error_is_not_retried() -> None:
    stream, ws = _stream()
    asyncio.run(stream.subscribe([{"channel": "books", "instId": "GONE-USDT-SWAP"}]))
    asyncio.run(stream.flush(ws, now=NOW))
    req_id = ws.sent[-1]["id"]
    asyncio.run(stream._dispatch(f'{{"event":"error","code":"60018","msg":"doesn\'t exist","id":"{req_id}"}}'))
    _flush_all(stream, ws, now=NOW + 1)
    assert len(ws.sent) == 1  # иначе каждый тик тратил бы запрос из лимита


def test_silent_mass_channel_triggers_reconnect() -> None:
    """Сделки по сотням инструментов не могут молчать минуту — это биржа перестала слать данные."""
    subs = per_instrument([f"X{i}-USDT-SWAP" for i in range(30)], ("trades",))
    stream, ws = _stream(subs)
    stream._reset_connection_state(NOW)
    stream._ws = ws  # type: ignore[assignment]
    _flush_all(stream, ws)

    stream._channel_seen["trades"] = NOW + 30
    assert stream._silent_channel(NOW + 80) is None
    assert stream._silent_channel(NOW + 95) == "trades"


def test_requested_reconnect_closes_connection() -> None:
    stream, ws = _stream()
    closed: list[bool] = []

    async def close() -> None:
        closed.append(True)
        ws.closed = True

    ws.close = close  # type: ignore[attr-defined]
    stream.request_reconnect("тест")
    asyncio.run(stream._flush_loop(ws, asyncio.Event()))  # type: ignore[arg-type]
    assert closed == [True] and stream.forced_reconnects == 1


def test_endpoint_split() -> None:
    """candle1m должен уходить на business-эндпоинт, trades — на public."""
    subs = per_instrument(["BTC-USDT-SWAP", "ETH-USDT-SWAP"], ("trades", "candle1m"))
    subs += liquidations(["SWAP"])
    subs += [Subscription(channel="bogus-channel", inst_id="X-USDT-SWAP")]

    public, business, unknown = split_by_endpoint(subs)
    assert {s.channel for s in public} == {"trades", "liquidation-orders"}
    assert {s.channel for s in business} == {"candle1m"}
    assert [s.channel for s in unknown] == ["bogus-channel"]
    assert len(public) == 2 + 1 and len(business) == 2
