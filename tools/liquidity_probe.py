"""Сколько инструментов проходит фильтр ликвидности (глубина 20 уровней >= порога).

Запуск: ``python tools/liquidity_probe.py`` (по умолчанию порог 1 млн USDT, SWAP, USDT).
С ``--rounds N`` делает N замеров с паузой и делит инструменты на «всегда проходит»,
«мигает» и «никогда» — видно, насколько глубина колеблется вокруг порога.

Считает так же, как сканер: notional уровня = px * sz_контракты * ctVal,
сумма по 20 уровням с каждой стороны (bid + ask).
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time

import aiohttp

BASE = "https://www.okx.com"
SEM = asyncio.Semaphore(6)  # /market/books ~40 запросов за 2 c на IP


async def get(session: aiohttp.ClientSession, path: str, params: dict) -> list[dict]:
    async with SEM:
        for attempt in range(3):
            try:
                async with session.get(f"{BASE}{path}", params=params) as r:
                    payload = await r.json()
                if payload.get("code") == "0":
                    return payload.get("data") or []
                if payload.get("code") == "50011":  # rate limit
                    await asyncio.sleep(1.0 + attempt)
                    continue
                return []
            except Exception:  # noqa: BLE001 — разовый скрипт, сеть рвётся по-разному
                await asyncio.sleep(0.5 + attempt)
    return []


async def depth_for(session: aiohttp.ClientSession, inst_id: str, ct_val: float) -> tuple[float, float, int]:
    """(bid_notional, ask_notional, levels) по top-20."""
    data = await get(session, "/api/v5/market/books", {"instId": inst_id, "sz": "20"})
    if not data:
        return 0.0, 0.0, 0
    book = data[0]
    bids = book.get("bids") or []
    asks = book.get("asks") or []

    def total(side: list) -> float:
        return sum(float(px) * float(sz) * ct_val for px, sz, *_ in side)

    return total(bids), total(asks), min(len(bids), len(asks))


async def live_instruments(session: aiohttp.ClientSession, inst_type: str, quote: str) -> list[tuple[str, float]]:
    rows = []
    for row in await get(session, "/api/v5/public/instruments", {"instType": inst_type}):
        if row.get("state") != "live":
            continue
        if str(row.get("settleCcy") or "").upper() != quote.upper():
            continue
        inst_id = str(row.get("instId") or "")
        if any(pat in inst_id for pat in ("-USDC", "-USD-", "_UMC")):
            continue
        try:
            ct_val = float(row.get("ctVal") or 1.0) * float(row.get("ctMult") or 1.0)
        except (TypeError, ValueError):
            ct_val = 1.0
        rows.append((inst_id, ct_val))
    return rows


def report_single(stats: list[tuple[str, float, float, float, int]], args: argparse.Namespace) -> None:
    stats.sort(key=lambda x: -x[1])
    total = len(stats)
    passed = [s for s in stats if s[1] >= args.threshold and s[4] >= args.levels]
    failed = total - len(passed)

    print(f"\nпорог: глубина bid+ask >= {args.threshold / 1e6:.2f}M на {args.levels} уровнях\n")
    print(f"  ВСЕГО инструментов:     {total}")
    print(f"  ПРОШЛИ фильтр:          {len(passed)}  ({len(passed) / max(total, 1) * 100:.1f}%)")
    print(f"  НЕ прошли:              {failed}")

    if passed:
        vals = [s[1] for s in passed]
        print(
            f"\n  глубина прошедших: медиана {statistics.median(vals) / 1e6:.1f}M, "
            f"мин {min(vals) / 1e6:.2f}M, макс {max(vals) / 1e6:.1f}M"
        )
        print(f"\n  ТОП-{min(25, len(passed))} по глубине:")
        for inst_id, tot, bid, ask, levels in passed[:25]:
            print(f"    {inst_id:24s} {tot / 1e6:7.2f}M  (bid {bid / 1e6:5.2f}M / ask {ask / 1e6:5.2f}M, {levels} ур.)")

    # распределение по уровням глубины — видно, где ставить порог
    print("\n  Распределение глубины (bid+ask, top-20):")
    buckets = [(0, 0.25e6), (0.25e6, 0.5e6), (0.5e6, 1e6), (1e6, 2e6), (2e6, 5e6), (5e6, 1e10)]
    for lo, hi in buckets:
        cnt = sum(1 for s in stats if lo <= s[1] < hi)
        label = f"{lo / 1e6:.2f}M–{hi / 1e6:.0f}M" if hi < 1e10 else f">{lo / 1e6:.0f}M"
        print(f"    {label:14s} {cnt:4d} {'#' * min(40, cnt)}")

    if failed:
        print("\n  Примеры НЕ прошедших (самые глубокие из отсечённых):")
        for inst_id, tot, _bid, _ask, levels in [s for s in stats if s[1] < args.threshold][:8]:
            print(f"    {inst_id:24s} {tot / 1e6:7.2f}M  ({levels} ур.)")


def report_rounds(history: dict[str, list[float]], args: argparse.Namespace) -> None:
    always, sometimes, never = [], [], []
    for inst_id, vals in history.items():
        if not vals:
            continue
        hits = sum(1 for v in vals if v >= args.threshold)
        if hits == len(vals):
            always.append((inst_id, statistics.median(vals)))
        elif hits:
            sometimes.append((inst_id, statistics.median(vals), hits, len(vals)))
        else:
            never.append(inst_id)

    always.sort(key=lambda x: -x[1])
    sometimes.sort(key=lambda x: -x[1])
    print(f"\n=== порог {args.threshold / 1e6:.2f}M, {args.rounds} замеров ===")
    print(f"  ВСЕГДА проходит:  {len(always)}")
    print(f"  МИГАЕТ:           {len(sometimes)}")
    print(f"  НИКОГДА:          {len(never)}")
    print("\n  ВСЕГДА (медиана глубины):")
    for inst_id, med in always:
        print(f"    {inst_id:24s} {med / 1e6:6.2f}M")
    if sometimes:
        print("\n  МИГАЕТ:")
        for inst_id, med, hits, n in sometimes:
            print(f"    {inst_id:24s} {med / 1e6:6.2f}M  прошло {hits}/{n}")


async def main(args: argparse.Namespace) -> None:
    started = time.time()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        rows = await live_instruments(session, args.inst_type, args.quote)
        print(f"{args.inst_type}/{args.quote}: {len(rows)} живых инструментов, опрашиваю стакан...", flush=True)

        if args.rounds <= 1:
            results = await asyncio.gather(*(depth_for(session, iid, cv) for iid, cv in rows))
            stats = [(iid, bid + ask, bid, ask, lv) for (iid, _), (bid, ask, lv) in zip(rows, results)]
            report_single(stats, args)
        else:
            history: dict[str, list[float]] = {iid: [] for iid, _ in rows}
            for rnd in range(args.rounds):
                results = await asyncio.gather(*(depth_for(session, iid, cv) for iid, cv in rows))
                for (iid, _), (bid, ask, lv) in zip(rows, results):
                    if lv:
                        history[iid].append(bid + ask)
                print(f"  замер {rnd + 1}/{args.rounds} готов", flush=True)
                if rnd < args.rounds - 1:
                    await asyncio.sleep(args.pause)
            report_rounds(history, args)

    print(f"\nзаняло {time.time() - started:.0f} c")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Сколько инструментов OKX проходит фильтр глубины стакана")
    parser.add_argument("--inst-type", default="SWAP")
    parser.add_argument("--quote", default="USDT")
    parser.add_argument("--threshold", type=float, default=1_000_000.0)
    parser.add_argument("--levels", type=int, default=20)
    parser.add_argument("--rounds", type=int, default=1, help="сколько замеров (>1 — отчёт о стабильности)")
    parser.add_argument("--pause", type=float, default=3.0, help="пауза между замерами, c")
    asyncio.run(main(parser.parse_args()))
