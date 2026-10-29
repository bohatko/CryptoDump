"""Диагностика: показывает, что реально доходит до состояния инструментов.

Запуск: ``python -m scanner.diag --seconds 30 -s BTC,ETH``.

Печатает по каждому инструменту: уровни стакана и его возраст, seqId и число
разрывов, глубину, состояние стены, число секунд с трейдами, CVD и текущую
просадку по окнам. Нужен, чтобы отличить «нет дампов» от «данные не доходят».
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time

from .config import load_config
from .engine import Engine
from .env import load_dotenv


def _describe(engine: Engine) -> str:
    now = time.time()
    out: list[str] = []
    for state in engine.states.values():
        book = state.book.summary(engine.cfg.depth, now=now)
        price = state.price(now=now, stale_after_s=engine.cfg.engine.trade_stale_after_s)
        drops = state.drops.all_drops(engine.cfg.window_thresholds(), price=price or 0.0, now=now)
        cvd = state.trades.snapshot(
            window_s=engine.cfg.cvd.window_s,
            burst_s=engine.cfg.cvd.burst_s,
            now=now,
        )
        age_ms = (now - state.book.updated_at) * 1000 if state.book.updated_at else -1.0
        candles = state.candles.last_ts()
        candle_age = f"{now - candles:.0f}s" if candles else "—"
        out.append(
            f"{state.inst_id}\n"
            f"   стакан: уровней={book.levels} полный={book.full_depth} источник={state.book.source or '—'}"
            f" возраст={age_ms:.0f}мс seq={state.book.seq_id} разрывов={state.book.gaps}\n"
            f"   глубина: bid={book.bid_notional / 1e6:.2f}M ask={book.ask_notional / 1e6:.2f}M"
            f" в±{book.band_pct:g}%={book.band_notional / 1e6:.2f}M{'' if book.band_covered else '+'}"
            f" стена={'да' if book.wall is not None else 'нет'}\n"
            f"   трейды: {len(state.trades._seconds)}с данных, CVD={cvd.cvd / 1e3:.0f}k"
            f" (buy={cvd.buy_notional / 1e3:.0f}k sell={cvd.sell_notional / 1e3:.0f}k)\n"
            f"   свечи: последняя {candle_age} назад\n"
            f"   цена={price} просадки={[(d.window_label, round(d.drop_pct, 2)) for d in drops] or '—'}"
        )
    return "\n".join(out)


async def _run(args: argparse.Namespace) -> int:
    cfg = load_config(args.config)
    if args.symbols:
        cfg.market.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    cfg.alerts.sinks = ["console"]
    cfg.engine.warmup_s = 0.0
    cfg.engine.log_level = args.log_level
    # диагностика идёт рядом с рабочим сканером: порт дашборда и история заняты им
    cfg.dashboard.enabled = False

    engine = Engine(cfg, dry_run=True)

    async def reporter() -> None:
        while True:
            await asyncio.sleep(args.every)
            print(f"\n--- {time.strftime('%H:%M:%S')} ---\n{_describe(engine)}", flush=True)

    runner = asyncio.create_task(engine.run(duration_s=args.seconds))
    ticker = asyncio.create_task(reporter())
    try:
        await runner
    finally:
        ticker.cancel()
        await asyncio.gather(ticker, return_exceptions=True)
    print(f"\n=== итог ===\n{_describe(engine)}", flush=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scanner.diag", description="Что реально доходит до детекции")
    parser.add_argument("-c", "--config", default=None)
    parser.add_argument("-s", "--symbols", default="BTC,ETH")
    parser.add_argument("--seconds", type=float, default=30.0)
    parser.add_argument("--every", type=float, default=6.0, help="как часто печатать состояние")
    parser.add_argument("--log-level", default="WARNING")
    args = parser.parse_args(argv)
    load_dotenv()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.WARNING))
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
