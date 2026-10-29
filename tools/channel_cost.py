"""Сколько стоит WS-канал OKX по трафику: сообщений в секунду и МБ в час.

Запуск: ``python tools/channel_cost.py --seconds 60``.

Параллельно открывает отдельные соединения:

* ``trades`` — на все живые USDT-перпетуалы (так сканер слушает весь рынок);
* ``books5`` и ``books`` — на ``--sample`` инструментов (по умолчанию BTC и ETH).

По результату видно, сколько трафика даёт весь рынок по сделкам и сколько стоит
один слот стакана — из этого выбираются ``depth.mode`` и ``depth.book_slots``.
Считается размер текста сообщений (без служебных ``event`` и ``pong``).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time

import aiohttp

REST = "https://www.okx.com"
WS_PUBLIC = "wss://ws.okx.com:443/ws/v5/public"
CHUNK = 64


async def usdt_swaps(session: aiohttp.ClientSession) -> list[str]:
    async with session.get(f"{REST}/api/v5/public/instruments", params={"instType": "SWAP"}) as r:
        payload = await r.json()
    return [
        row["instId"]
        for row in payload.get("data") or []
        if row.get("state") == "live" and str(row.get("settleCcy") or "").upper() == "USDT"
    ]


async def measure(
    session: aiohttp.ClientSession, channel: str, inst_ids: list[str], seconds: float
) -> tuple[str, int, int, int, int]:
    """(канал, инструментов, сообщений, байт, ошибок подписки) за ``seconds``."""
    msgs = nbytes = errors = 0
    async with session.ws_connect(WS_PUBLIC, max_msg_size=0) as ws:
        args = [{"channel": channel, "instId": iid} for iid in inst_ids]
        for i in range(0, len(args), CHUNK):
            await ws.send_json({"op": "subscribe", "args": args[i : i + CHUNK]})
            await asyncio.sleep(0.05)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + seconds
        next_ping = loop.time() + 20.0
        while (left := deadline - loop.time()) > 0:
            if loop.time() >= next_ping:
                # OKX закрывает соединение без ping дольше 30 c
                await ws.send_str("ping")
                next_ping = loop.time() + 20.0
            try:
                msg = await ws.receive(timeout=min(left, 5.0))
            except asyncio.TimeoutError:
                continue
            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break
            if msg.type != aiohttp.WSMsgType.TEXT or msg.data == "pong":
                continue
            if msg.data.startswith('{"event"'):
                event = json.loads(msg.data)
                if event.get("event") == "error":
                    errors += 1
                    print(f"  {channel}: ошибка подписки {event.get('code')} {event.get('msg')}", flush=True)
                continue
            msgs += 1
            nbytes += len(msg.data.encode("utf-8"))
    return channel, len(inst_ids), msgs, nbytes, errors


async def main(args: argparse.Namespace) -> None:
    sample = [f"{base.strip().upper()}-USDT-SWAP" for base in args.sample.split(",") if base.strip()]
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
        market = await usdt_swaps(session)
        print(f"USDT-перпетуалов: {len(market)}; замер {args.seconds:.0f} c...", flush=True)
        started = time.time()
        results = await asyncio.gather(
            measure(session, "trades", market, args.seconds),
            measure(session, "books5", sample, args.seconds),
            measure(session, "books", sample, args.seconds),
        )
        elapsed = time.time() - started

    print(f"\n{'канал':8s} {'инстр':>6s} {'сообщ/с':>9s} {'МБ/час':>9s} {'МБ/час на 1':>12s} {'весь рынок':>11s}")
    for channel, n, msgs, nbytes, errors in results:
        mb_hour = nbytes / elapsed * 3600 / 1e6
        per_inst = mb_hour / max(n, 1)
        print(
            f"{channel:8s} {n:6d} {msgs / elapsed:9.1f} {mb_hour:9.1f} {per_inst:12.2f} "
            f"{per_inst * len(market) / 1000:9.2f} ГБ" + (f"  (ошибок подписки: {errors})" if errors else "")
        )
    print("\nобъём — по тексту сообщений; реальный трафик выше на заголовки WS/TLS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Трафик WS-каналов OKX: trades на весь рынок, books/books5 на выборке")
    parser.add_argument("--seconds", type=float, default=60.0)
    parser.add_argument("--sample", default="BTC,ETH", help="базы для замера books/books5")
    asyncio.run(main(parser.parse_args()))
