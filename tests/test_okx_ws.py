"""Раскладка подписок по public/business-эндпоинтам OKX."""

from __future__ import annotations

import asyncio

from scanner.okx_ws import OkxPublicStream, Subscription, liquidations, per_instrument, split_by_endpoint


async def _noop(_payload: dict) -> None:
    return None


def test_unsubscribe_while_disconnected_forgets_subscription() -> None:
    """Иначе после реконнекта books вернулся бы инструменту, у которого уже нет слота."""
    books = Subscription(channel="books", inst_id="BTC-USDT-SWAP")
    trades = Subscription(channel="trades", inst_id="BTC-USDT-SWAP")
    stream = OkxPublicStream("wss://example.invalid", [trades, books], _noop)

    sent = asyncio.run(stream.unsubscribe([{"channel": "books", "instId": "BTC-USDT-SWAP"}]))

    assert sent is False  # соединения нет — на биржу ничего не ушло
    assert stream.subscriptions == [trades]


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
