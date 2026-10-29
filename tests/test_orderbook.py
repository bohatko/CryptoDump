"""Стакан: ctVal, стена и её устойчивость, инкременты books, свежесть, запас глубины."""

from __future__ import annotations

from scanner.config import Config
from scanner.detection import passed_liquidity
from scanner.orderbook import OrderBook


def test_orderbook(now: float) -> None:
    cfg = Config().depth
    book = OrderBook("TEST-USDT-SWAP")

    # тонкий стакан: 20 уровней, но по 100 контрактов — ни стены, ни 1 млн USDT
    thin_bids = [["100.0", "100"] for _ in range(20)]
    thin_asks = [["100.1", "100"] for _ in range(20)]
    book.apply_snapshot(
        {"bids": thin_bids, "asks": thin_asks, "ts": str(int(now * 1000))},
        max_levels=20,
        now=now,
    )

    summary = book.summary(cfg, now=now)
    assert summary.full_depth
    assert summary.fresh
    assert summary.levels == 20
    assert summary.best_bid == 100.0 and summary.best_ask == 100.1
    # все уровни одинаковые: ratio = 1, стены нет
    assert summary.wall is None, summary
    assert not passed_liquidity(Config(), summary)

    # глубокий стакан со стеной: уровень на 99.95 держит 600k USDT, остальные — по 100k.
    # Стена должна простоять wall_persist_s (проверка по времени, а не по числу снимков).
    deep_bids = (
        [["100.0", "1000"]]  # лучший бид — не стена по определению
        + [["99.95", "6000"]]  # стена поддержки
        + [["99.9", "1000"] for _ in range(18)]
    )
    deep_asks = [["100.1", "1000"] for _ in range(20)]
    book.apply_snapshot({"bids": deep_bids, "asks": deep_asks}, max_levels=20, now=now)
    # сразу после появления стена ещё не подтверждена
    assert book.summary(cfg, now=now).wall is None

    t = now + cfg.wall_persist_s + 0.5
    book.apply_snapshot({"bids": deep_bids, "asks": deep_asks}, max_levels=20, now=t)
    summary = book.summary(cfg, now=t)
    assert summary.total_notional > 1_000_000, summary.total_notional
    assert passed_liquidity(Config(), summary)
    assert summary.wall is not None, summary
    assert summary.wall.px == 99.95
    assert summary.wall.notional == 99.95 * 6000.0
    # база — среднее по 19 уровням ниже лучшего бида: (599 700 + 18 × 99 900) / 19
    base = (599_700.0 + 18 * 99_900.0) / 19
    assert abs(summary.wall_ratio - 599_700.0 / base) < 1e-9, summary.wall_ratio
    # биды: 100k (лучший) + 599 700 (стена) + 1 798 200 (18 уровней)
    assert abs(summary.bid_notional - 2_497_900.0) < 1e-6, summary.bid_notional
    assert abs(summary.imbalance - 2_497_900.0 / 2_002_000.0) < 1e-9, summary.imbalance

    # протухший снимок в детекцию не проходит
    stale = book.summary(cfg, now=t + cfg.stale_after_s + 1)
    assert not stale.fresh
    assert not passed_liquidity(Config(), stale)

    # books5 не считается полной глубиной: 5 уровней вместо 20
    book.apply_ws_snapshot(
        {"bids": [["100.0", "5000"]] * 5, "asks": [["100.1", "5000"]] * 5},
        max_levels=5,
        level_ts_index=None,
        authoritative=False,
        now=now,
    )
    partial = book.summary(cfg, now=now)
    assert not partial.full_depth
    assert partial.total_notional > 1_000_000  # сумма-то большая, но глубина неполная
    assert not passed_liquidity(Config(), partial)


def test_contract_value(now: float) -> None:
    """sz у OKX в контрактах: без ctVal номинал занижается в 1/ctVal раз."""
    cfg = Config().depth

    # BTC-USDT-SWAP: ctVal = 0.01 BTC за контракт, 25 контрактов = 0.25 BTC
    book = OrderBook("BTC-USDT-SWAP", ct_val=0.01)
    # 20 уровней по 25 контрактов по 60 000 → 15k USDT на уровень, 300k на сторону
    levels = [["60000", "25"] for _ in range(20)]
    book.apply_snapshot({"bids": levels, "asks": levels}, max_levels=20, now=now)
    summary = book.summary(cfg, now=now)
    assert abs(summary.bid_notional - 300_000.0) < 1.0, summary.bid_notional
    assert abs(summary.total_notional - 600_000.0) < 1.0
    assert not passed_liquidity(Config(), summary)  # 600k < 1M

    # тот же стакан без ctVal дал бы 30M на сторону и ложный «зелёный свет»
    naive = OrderBook("BTC-USDT-SWAP", ct_val=1.0)
    naive.apply_snapshot({"bids": levels, "asks": levels}, max_levels=20, now=now)
    assert naive.summary(cfg, now=now).total_notional > 10_000_000


def test_wall_persistence(now: float) -> None:
    """Мигающая стена (спуф) не считается; устойчивая — после wall_persist_s."""
    cfg = Config().depth
    book = OrderBook("TEST-USDT-SWAP")

    deep_asks = [["100.1", "1000"] for _ in range(20)]
    wall_bids = [["100.0", "1000"]] + [["99.95", "6000"]] + [["99.9", "1000"] for _ in range(18)]

    # первый снимок: стена есть, но ещё не подтверждена
    book.apply_snapshot({"bids": wall_bids, "asks": deep_asks}, max_levels=20, now=now)
    assert book.summary(cfg, now=now).wall is None

    # через половину порога всё ещё не подтверждена
    half = now + cfg.wall_persist_s / 2
    book.apply_snapshot({"bids": wall_bids, "asks": deep_asks}, max_levels=20, now=half)
    assert book.summary(cfg, now=half).wall is None

    # прошло wall_persist_s с первого появления — подтверждена
    later = now + cfg.wall_persist_s + 0.5
    book.apply_snapshot({"bids": wall_bids, "asks": deep_asks}, max_levels=20, now=later)
    confirmed = book.summary(cfg, now=later)
    assert confirmed.wall is not None, confirmed
    assert confirmed.wall.px == 99.95
    base = (599_700.0 + 18 * 99_900.0) / 19
    assert abs(confirmed.wall_ratio - 599_700.0 / base) < 1e-9, confirmed.wall_ratio

    # повторный вызов summary() не сбрасывает подтверждение
    again = book.summary(cfg, now=later + 0.5)
    assert again.wall is not None

    # спуф исчез: уровня больше нет — стена сбрасывается
    small_bids = [["100.0", "1000"] for _ in range(20)]
    book.apply_snapshot({"bids": small_bids, "asks": deep_asks}, max_levels=20, now=later + 1)
    assert book.summary(cfg, now=later + 1).wall is None

    # и снова появился — отсчёт начинается заново
    book.apply_snapshot({"bids": wall_bids, "asks": deep_asks}, max_levels=20, now=later + 2)
    assert book.summary(cfg, now=later + 2).wall is None


def test_books_incremental(now: float) -> None:
    """Склейка инкрементов канала books и обнаружение разрыва seqId."""
    book = OrderBook("BTC-USDT-SWAP", ct_val=0.01)

    # снимок: 20 бидов и 20 асков (реальный формат [px, sz, liqOrders, orders])
    snapshot = {
        "bids": [[f"{100 - i}", "100", "0", "5"] for i in range(20)],
        "asks": [[f"{101 + i}", "100", "0", "5"] for i in range(20)],
        "seqId": 1000,
        "prevSeqId": -1,
        "ts": str(int(now * 1000)),
    }
    book.apply_ws_snapshot(snapshot, max_levels=20, authoritative=True, now=now)
    assert book.authoritative and book.source == "ws"
    assert book.seq_id == 1000
    assert book.bids[0].px == 100.0 and book.bids[-1].px == 81.0
    # аски отсортированы по возрастанию: лучший ask первым
    assert book.asks[0].px == 101.0 and book.asks[-1].px == 120.0
    assert book.bids[0].sz == 1.0  # 100 контрактов × ctVal 0.01

    # инкремент: меняем 100.0, добавляем 99.5, удаляем 81.0 (sz = 0)
    ok = book.apply_ws_update(
        {
            "bids": [["100", "300", "0", "5"], ["99.5", "50", "0", "3"], ["81", "0", "0", "0"]],
            "asks": [],
            "seqId": 1001,
            "prevSeqId": 1000,
        },
        max_levels=20,
        now=now + 0.1,
    )
    assert ok
    assert book.seq_id == 1001
    by_px = {lv.px: lv for lv in book.bids}
    assert by_px[100.0].sz == 3.0  # 300 контрактов × 0.01
    assert 99.5 in by_px
    assert 81.0 not in by_px
    assert len(book.bids) == 20  # удалили один, добавили один

    # разрыв цепочки: prevSeqId не совпадает — апдейт не применяется, нужен ресинк
    gap = book.apply_ws_update(
        {"bids": [["100", "999", "0", "5"]], "asks": [], "seqId": 1005, "prevSeqId": 1002},
        max_levels=20,
        now=now + 0.2,
    )
    assert gap is False
    assert book.gaps == 1
    assert {lv.px: lv for lv in book.bids}[100.0].sz == 3.0  # данные не испорчены
    assert book.seq_id == 1001


def test_wall_survives_stale_filter(now: float) -> None:
    """Стоячая стена без апдейтов не должна выбрасываться как «протухшая».

    Регресс: биржа присылает в books только изменившиеся уровни. Если стена стоит
    10 с без изменения объёма, её level.ts не обновляется, и старый фильтр по
    stale_level_s выбрасывал её ровно на пороге wall_persist_s.
    """
    cfg = Config().depth
    assert cfg.wall_persist_s == 10.0
    assert cfg.stale_level_s == 10.0, "тест проверяет именно совпадение порогов"

    book = OrderBook("BTC-USDT-SWAP", ct_val=0.01)
    # ct_val=0.01 и px≈100: номинал уровня в USDT ≈ число контрактов.
    # стена 400k контрактов против 1k на остальных уровнях → ratio ≈ 19×
    bids = (
        [["100", "1000", "0", "5"]]
        + [["99.95", "400000", "0", "5"]]
        + [[f"{99.9 - i * 0.1:.1f}", "1000", "0", "5"] for i in range(19)]
    )
    # асков должно быть столько же: levels в summary считается по минимуму сторон
    asks = [[f"{100.1 + i * 0.1:.1f}", "1000", "0", "5"] for i in range(20)]
    snapshot = {"bids": bids, "asks": asks, "seqId": 1, "prevSeqId": -1}
    book.apply_ws_snapshot(snapshot, max_levels=100, authoritative=True, incremental=True, now=now)

    # стена стоит без изменений: апдейты приходят только по ДРУГИМ уровням,
    # поэтому уровень 99.95 не получает нового ts.
    # summary() вызываем на каждой итерации — как это делает цикл оценки движка
    # (eval_interval_s), иначе отсчёт wall_persist_s не начнётся.
    t = now
    book.summary(cfg, now=t)
    for seq in range(2, 25):
        t = now + seq * 0.5  # суммарно > 10 c
        book.apply_ws_update(
            {
                "bids": [["100", str(1000 + seq), "0", "5"]],  # меняется только лучший бид
                "asks": [],
                "seqId": seq,
                "prevSeqId": seq - 1,
            },
            max_levels=100,
            now=t,
        )
        book.summary(cfg, now=t)

    assert t - now > cfg.wall_persist_s
    summary = book.summary(cfg, now=t)
    # стакан свежий, стена на месте и подтверждена
    assert summary.fresh is True
    assert summary.wall is not None, "стоячая стена выброшена фильтром свежести"
    assert summary.wall.px == 99.95

    # при этом по-настоящему мёртвый стакан (нет апдейтов вообще) не проходит
    stale = book.summary(cfg, now=t + cfg.stale_after_s + 1)
    assert stale.fresh is False
    assert not passed_liquidity(Config(), stale)


def test_depth_buffer(now: float) -> None:
    """Хранится глубже, чем считаем: после выедания верхних уровней глубина не падает."""
    cfg = Config().depth
    book = OrderBook("BTC-USDT-SWAP", ct_val=0.01)

    # 60 уровней в снимке, считаем по 20
    bids = [[f"{100 - i * 0.1:.1f}", "1000", "0", "5"] for i in range(60)]
    asks = [[f"{101 + i * 0.1:.1f}", "1000", "0", "5"] for i in range(60)]
    book.apply_ws_snapshot(
        {"bids": bids, "asks": asks, "seqId": 1, "prevSeqId": -1},
        max_levels=100,
        authoritative=True,
        now=now,
    )
    assert len(book.bids) == 60, "снимок обрезан при хранении"

    summary = book.summary(cfg, now=now)
    assert summary.levels == 20  # в метриках ровно запрошенная глубина

    # выедаем 5 верхних уровней — снизу должны подтянуться следующие
    eat = [["0", "0", "0", "0"]]
    updates = [[f"{100 - i * 0.1:.1f}", "0", "0", "0"] for i in range(5)]
    book.apply_ws_update(
        {"bids": updates, "asks": eat, "seqId": 2, "prevSeqId": 1},
        max_levels=100,
        now=now + 0.1,
    )
    after = book.summary(cfg, now=now + 0.1)
    assert after.levels == 20, f"глубина просела до {after.levels} — запас не сработал"
    assert after.best_bid < 100.0


def test_rest_snapshot_needs_ws_resync(now: float) -> None:
    """WS-дельта не накладывается на REST-снимок: у них разные точки отсчёта."""
    book = OrderBook("BTC-USDT-SWAP", ct_val=0.01)
    book.apply_snapshot({"bids": [["100", "1000"]], "asks": [["100.1", "1000"]]}, max_levels=20, now=now)
    assert book.source == "rest" and book.seq_id is None

    # первый же апдейт не применяется — иначе стакан разъедется с биржей
    applied = book.apply_ws_update(
        {"bids": [["100", "500", "0", "1"]], "asks": [], "seqId": 10, "prevSeqId": 9},
        max_levels=20,
        now=now + 0.1,
    )
    assert applied is False
    assert book.seq_id is None
    assert book.gaps == 1
    assert book.bids[0].sz == 10.0  # данные REST не испорчены

    # в WS-режиме такой стакан требует ресинка
    assert book.needs_resync(Config().depth, now=now + 0.1) is False  # свежий, но...
    book.incremental = True
    assert book.needs_resync(Config().depth, now=now + 0.1) is True  # ...без seqId нужен WS-снимок


def _ladder(start: float, step: float, n: int, sz: str) -> list[list[str]]:
    return [[f"{start + step * i:.10f}", sz] for i in range(n)]


def test_liquidity_band_dense_book_with_tiny_tick(now: float) -> None:
    """Как PEPE: шаг цены 0.002%, 20 уровней — копейки, а в ±1% от цены — миллионы."""
    cfg = Config()
    book = OrderBook("PEPE-USDT-SWAP")
    bids = _ladder(100.0, -0.002, 400, "100")  # 400 уровней по 10k USDT вниз на 0.8%
    asks = _ladder(100.002, 0.002, 400, "100")
    book.apply_snapshot({"bids": bids, "asks": asks}, max_levels=400, now=now)

    summary = book.summary(cfg.depth, now=now)
    assert summary.total_notional < 0.5e6  # 20 + 20 уровней × 10k
    assert summary.band_notional > 7e6 and not summary.band_covered  # 400 уровней не дотянулись до 1%
    assert passed_liquidity(cfg, summary)

    cfg.depth.band_pct = 0.0  # старое правило: только 20 уровней
    assert not passed_liquidity(cfg, book.summary(cfg.depth, now=now))


def test_liquidity_band_ignores_far_orders_of_sparse_book(now: float) -> None:
    """Как CASHCAT: 20 уровней растянуты на 10% — у цены пусто, а сумма набегает от дальних заявок."""
    cfg = Config()
    book = OrderBook("CASHCAT-USDT-SWAP")
    bids = [["99.9", "100"]] + _ladder(98.5, -0.5, 19, "1000")  # у цены 10k, дальше 1% — по 100k
    asks = [["100.1", "100"]] + _ladder(101.5, 0.5, 19, "1000")
    book.apply_snapshot({"bids": bids, "asks": asks}, max_levels=400, now=now)

    summary = book.summary(cfg.depth, now=now)
    assert summary.total_notional > 3e6
    assert summary.band_notional < 0.1e6 and summary.band_covered
    assert not passed_liquidity(cfg, summary)
