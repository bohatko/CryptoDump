"""Бэктест «откупа после дампа» на 1m-свечах OKX за последние N часов.

Логика сканера переиспользуется целиком: пороги окон из конфига, ``build_plan`` для
входа/стопа/целей и ``PaperBook`` для сопровождения сделки и реинвеста — поэтому цифры
сравнимы с вкладкой «Счёт».

Чего здесь нет: стакана (стена покупателя), потока сделок (CVD) и ликвидаций — OKX не
отдаёт их историю. Поэтому сигнал = просадка по порогу окна без подтверждения
сигнатурами; это нижняя оценка того, что делает живой сканер. Вариант B добавляет
грубый заменитель подтверждения: свеча сигнала зелёная (покупатели уже выкупают).

    python tools/backtest.py --hours 24 --out tools/out/backtest_24h.csv
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import csv
import logging
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scanner.config import WINDOW_LABELS, Config, load_config  # noqa: E402
from scanner.market import OkxRest, OkxRestError  # noqa: E402
from scanner.models import BookSummary, Candle, Instrument  # noqa: E402
from scanner.paper import REASON_SL, REASON_TP, PaperBook, PaperTrade  # noqa: E402
from scanner.parsers import parse_candle  # noqa: E402
from scanner.plan import build_plan  # noqa: E402
from scanner.universe import build_universe  # noqa: E402

log = logging.getLogger("backtest")

# market/candles отдаёт свечи только за последние 1440 минут — старее догружаем history-candles
RECENT_WINDOW_S = 1440 * 60
# history-candles отдаёт 100 свечей за запрос; больше 4 параллельных запросов → 429
HISTORY_CONCURRENCY = 4


async def fetch_candles(
    rest: OkxRest, inst_id: str, *, since: float, until: float, history_sem: asyncio.Semaphore
) -> list[Candle]:
    """1m-свечи за ``[since, until)``: свежий хвост — market/candles, старый — history-candles.

    ``market/candles`` отдаёт 300 свечей за запрос и только последние 1440 минут, поэтому
    период длиннее суток целиком через него не поднять. ``history-candles`` достаёт глубже,
    но по 100 свечей за запрос: на 26 часах это десятки запросов, и на восьми инструментах
    параллельно OKX отвечает 429. Поэтому глубже 24 ч догружаем только недостающий хвост,
    под отдельным семафором ``history_sem`` (не больше ``HISTORY_CONCURRENCY`` сразу).
    """
    recent_since = max(since, until - RECENT_WINDOW_S)
    rows: dict[int, Any] = {}
    try:
        if recent_since > since:
            async with history_sem:
                for row in await rest.candles_between(inst_id, since=since, until=recent_since):
                    rows[int(row[0])] = row
        # перекрытие по ts снимается словарём: последний ответ главнее, дублей не будет
        for row in await rest.recent_candles(inst_id, since=recent_since, until=until):
            rows[int(row[0])] = row
    except OkxRestError as exc:
        # половина периода дала бы ложные просадки на стыке — инструмент пропускаем целиком
        log.warning("свечи %s: %s", inst_id, exc)
        return []
    candles = [c for c in (parse_candle(r) for r in rows.values()) if c is not None]
    return sorted(candles, key=lambda c: c.ts)


def find_signals(
    inst: Instrument, candles: list[Candle], cfg: Config, *, start: float, cooldown_s: float
) -> list[dict[str, Any]]:
    """Просадки по порогам окон на закрытиях свечей — как ``evaluate_instrument``, но по свечам.

    В окно берутся только свечи, целиком лежащие в нём (инвариант 6: свеча на границе
    завышала максимум). Из сработавших окон — самое глубокое, после сигнала — кулдаун.
    """
    thresholds = cfg.window_thresholds()
    opens = [c.ts for c in candles]
    out: list[dict[str, Any]] = []
    last = -1e18
    for i, candle in enumerate(candles):
        t = candle.end_ts
        if candle.ts < start or t - last < cooldown_s:
            continue
        price = candle.close
        hits = []
        for window_s, thr in thresholds:
            lo = bisect.bisect_left(opens, t - window_s)
            window = candles[lo : i + 1]
            if not window:
                continue
            peak = max(window, key=lambda c: c.high)
            drop = (price / peak.high - 1.0) * 100.0 if peak.high > 0 else 0.0
            if -drop >= thr:
                hits.append((drop, window_s, peak, window))
        if not hits:
            continue
        drop, window_s, peak, window = min(hits, key=lambda h: h[0])
        low = min(c.low for c in window if c.ts >= peak.ts and c.low > 0)
        last = t
        book = BookSummary(inst_id=inst.inst_id, ts=t)
        plan = build_plan(
            cfg.trade, price=price, high=peak.high, low=low, book=book,
            ct_val=inst.contract_value, tick_sz=inst.tick_sz, now=t,
        )
        out.append({
            "ts": t,
            "instId": inst.inst_id,
            "instType": inst.inst_type,
            "window": WINDOW_LABELS.get(window_s, str(window_s)),
            "windowSeconds": window_s,
            "dropPct": drop,
            "price": price,
            "windowHigh": peak.high,
            "plan": plan.to_dict() if plan else None,
            "green": candle.close > candle.open,
        })
    return out


def simulate(cfg: Config, signals: list[dict[str, Any]], candles: dict[str, list[Candle]], *, now: float) -> PaperBook:
    """Прогон как в ``PaperBook.replay``: сигналы по времени, каждая сделка до конца по свечам."""
    book = PaperBook(cfg)
    for alert_id, data in enumerate(signals, 1):
        trade = book.on_alert(alert_id, data, source="replay")
        if trade is not None:
            series = candles[data["instId"]]
            lo = bisect.bisect_left([c.ts for c in series], data["ts"])
            book._run_candles(trade, series[lo:], now=now)
    return book


def _pct(v: float | None, d: int = 2) -> str:
    return "—" if v is None else f"{v:+.{d}f}%"


def report(title: str, book: PaperBook, signals: list[dict[str, Any]], *, now: float) -> None:
    s = book.summary({}, now=now)
    a, g = s["account"], s["signals"]
    closed = [t for t in book.trades.values() if t.status == "closed"]
    print(f"\n=== {title}: сигналов {len(signals)}, с планом {sum(1 for x in signals if x['plan'])} ===")
    print(
        f"СЧЁТ  {a['start']:.0f} → {a['balance']:.2f} $ ({_pct(a['returnPct'])}), сделок {a['trades']}"
        f" ({a['wins']}+ / {a['losses']}−, {a['winRate']}%), макс. просадка {a['maxDrawdownPct']}%"
    )
    print(
        f"      ср. плюс {_pct(a['avgWinPct'])}, ср. минус {_pct(a['avgLossPct'])} маржи;"
        f" матожидание {_pct(a['expectancyPct'])} на сделку; PF {a['profitFactor']};"
        f" рост на сделку с реинвестом {_pct(a['growthPerTradePct'], 3)}"
    )
    print(
        f"СИГНАЛЫ (каждый отдельно) закрыто {g['trades']}, открыто {g['open']}: в плюс {g['winRate']}%"
        f" (нужно {g['breakevenWinRate']}% для нуля), ср. плюс {_pct(g['avgWinPct'])} / минус {_pct(g['avgLossPct'])},"
        f" матожидание {_pct(g['expectancyPct'])} маржи = {g['avgR']} R, PF {g['profitFactor']}"
    )
    print(f"      выходы: {g['reasons']}; пропущено счётом: {s['skipped']}")

    by_win: dict[str, list[PaperTrade]] = {}
    for t in closed:
        by_win.setdefault(t.window, []).append(t)
    for label in sorted(by_win, key=lambda w: next((s for s, lbl in WINDOW_LABELS.items() if lbl == w), 0)):
        ts = by_win[label]
        wins = sum(1 for t in ts if t.margin_return() > 0)
        mean = sum(t.margin_return() for t in ts) / len(ts) * 100
        print(
            f"      окно {label:>3}: сделок {len(ts):4}, в плюс {wins / len(ts) * 100:5.1f}%,"
            f" матожидание {mean:+.2f}%"
        )

    ranked = sorted(closed, key=lambda t: t.margin_return())
    fmt = lambda t: (  # noqa: E731
        f"{time.strftime('%d.%m %H:%M', time.localtime(t.opened_at))} {t.inst_id:18} {t.window:>3}"
        f" {t.entry:g} → {t.exit_px:g} {t.reason:9} {t.margin_return() * 100:+6.1f}%"
        f"{' [счёт]' if t.taken else ''}"
    )
    print("  худшие:")
    for t in ranked[:5]:
        print("    " + fmt(t))
    print("  лучшие:")
    for t in ranked[-5:][::-1]:
        print("    " + fmt(t))


def write_csv(path: Path, book: PaperBook) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [t.to_api() for t in sorted(book.trades.values(), key=lambda t: t.opened_at)]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]) if rows else ["alertId"])
        writer.writeheader()
        writer.writerows(rows)


def sanity(book_a: PaperBook, book_b: PaperBook, signals_a: list[dict[str, Any]]) -> bool:
    """Проверки на здравый смысл: нарушение значит баг расчёта, а не свойство рынка."""
    checks: list[tuple[str, bool, str]] = []
    trades = list(book_a.trades.values())

    bad_time = [t for t in trades if t.closed_at is not None and t.closed_at < t.opened_at]
    checks.append(("закрытие не раньше открытия", not bad_time, f"нарушений {len(bad_time)}"))

    # тейк исполняется по цене цели, поэтому его доходность положительна всегда: цель
    # отстоит от входа минимум на MIN_TARGET_GAP = 0.1%, а комиссии — 2 × 0.05%
    tp = [t for t in trades if t.reason == REASON_TP]
    sl = [t for t in trades if t.reason == REASON_SL]
    tp_bad = [t for t in tp if t.margin_return() < 0]
    sl_bad = [t for t in sl if t.margin_return() > 0]
    checks.append(("тейк в плюсе", not tp_bad, f"тейков {len(tp)}, нарушений {len(tp_bad)}"))
    checks.append(("стоп в минусе", not sl_bad, f"стопов {len(sl)}, нарушений {len(sl_bad)}"))

    account = sum(1 for t in trades if t.taken)
    checks.append(("сделок счёта ≤ сигналов", account <= len(trades), f"{account} сделок, {len(trades)} сигналов"))

    # вариант B — подмножество A: те же сигналы, отфильтрованные по зелёной свече.
    # Сверяем по (инструмент, время), а не по alert_id: simulate нумерует сигналы
    # заново в каждом прогоне, и одинаковые номера там относятся к разным сигналам.
    b_signals = [x for x in signals_a if x["green"]]
    checks.append(
        ("сигналы B ⊆ A", len(b_signals) <= len(signals_a), f"B {len(b_signals)} из A {len(signals_a)}")
    )
    key = lambda t: (t.inst_id, round(t.opened_at, 3))  # noqa: E731
    b_keys = {key(t) for t in book_b.trades.values()}
    a_keys = {key(t) for t in trades}
    checks.append(("сделки B ⊆ A", b_keys <= a_keys, f"лишних в B {len(b_keys - a_keys)}"))

    ok = all(c[1] for c in checks)
    print("\nПРОВЕРКИ")
    for title, passed, detail in checks:
        print(f"  {'OK ' if passed else 'БАГ'} {title}: {detail}")
    print(f"  итог: {'все пройдены' if ok else 'ЕСТЬ НАРУШЕНИЯ'}")
    return ok


async def run(args: argparse.Namespace) -> int:
    path = args.config or ("config.json" if Path("config.json").exists() else None)
    cfg = load_config(path) if path else Config()
    if args.symbols:
        cfg.market.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    # срез по обороту: --top задан явно и не мешает явному списку баз из -s
    if args.top > 0 and not cfg.market.symbols:
        cfg.market.top_n = args.top
    now = time.time()
    start = now - args.hours * 3600
    since = start - cfg.max_window_s() - 120

    async with OkxRest(cfg.market.rest_url, timeout_s=15.0) as rest:
        universe = await build_universe(rest, cfg.market)
        sem = asyncio.Semaphore(max(1, args.concurrency))
        # отдельный лимит: history-candles отдаёт по 100 свечей, и OKX быстро отвечает 429
        history_sem = asyncio.Semaphore(HISTORY_CONCURRENCY)
        done = 0

        async def one(inst: Instrument) -> tuple[Instrument, list[Candle]]:
            nonlocal done
            async with sem:
                candles = await fetch_candles(rest, inst.inst_id, since=since, until=now, history_sem=history_sem)
            done += 1
            if done % 50 == 0:
                log.info("свечи: %d/%d", done, len(universe))
            return inst, candles

        loaded = await asyncio.gather(*(one(inst) for inst, _ in universe))

    candles = {inst.inst_id: c for inst, c in loaded if c}
    signals = []
    for inst, series in loaded:
        if series:
            signals += find_signals(inst, series, cfg, start=start, cooldown_s=cfg.alerts.cooldown_s)
    signals.sort(key=lambda x: x["ts"])

    p = cfg.paper
    began = time.strftime("%d.%m %H:%M", time.localtime(start))
    ended = time.strftime("%d.%m %H:%M", time.localtime(now))
    print(f"Период {began} — {ended}, инструментов {len(candles)} из {len(universe)}")
    print(
        "Пороги: " + ", ".join(f"{WINDOW_LABELS.get(w, w)} {t}%" for w, t in cfg.window_thresholds())
        + f"; кулдаун {cfg.alerts.cooldown_s:g} c"
    )
    print(
        f"Счёт: {p.start_balance:g} $, ×{p.leverage:g}, тейк — цель #{p.target_index + 1}, R:R ≥ {p.min_rr:g},"
        f" безубыток {'да' if p.breakeven_after_first else 'нет'}, держать ≤ {p.max_hold_s / 3600:g} ч,"
        f" комиссия {p.fee_pct}%×2, проскальзывание {p.slippage_pct}%"
    )
    print("ВНИМАНИЕ: только свечи — без стакана, CVD и ликвидаций, которые требует живой сканер.")

    book_a = simulate(cfg, signals, candles, now=now)
    report("A: все просадки", book_a, signals, now=now)
    green = [x for x in signals if x["green"]]
    book_b = simulate(cfg, green, candles, now=now)
    report("B: только зелёная свеча сигнала", book_b, green, now=now)
    ok = sanity(book_a, book_b, signals)
    if args.out:
        write_csv(Path(args.out), book_a)
        print(f"\nсделки A: {args.out}")
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Бэктест откупа после дампа на 1m-свечах OKX")
    parser.add_argument("--hours", type=float, default=24.0, help="сколько часов истории (по умолчанию 24)")
    parser.add_argument("-c", "--config", help="конфиг (по умолчанию config.json, если есть)")
    parser.add_argument("-s", "--symbols", help="базы через запятую: BTC,ETH (по умолчанию весь рынок)")
    parser.add_argument("--top", type=int, default=0, help="сколько самых оборотных брать (0 — без среза)")
    parser.add_argument("--concurrency", type=int, default=8, help="параллельных запросов REST")
    parser.add_argument("--out", help="CSV со сделками варианта A")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S", stream=sys.stderr)
    logging.getLogger("scanner").setLevel(logging.WARNING)
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
