"""Детекция откупа: три сигнатуры + скоринг.

Сигнатуры:
1. ``bid_wall``  — крупная стена на бидах относительно среднего уровня;
2. ``cvd_fade``  — CVD выкуплен в последней трети окна и/или всплеск тейкер-покупок;
3. ``liq_decay`` — каскад ликвидаций был и затух (давление продавца выдохлось).

Стена засчитывается только если простояла ``depth.wall_persist_s`` секунд
(см. ``OrderBook._confirm_wall``) — одиночный крупный уровень обычно оказывается спуфом.
"""

from __future__ import annotations

import logging

from .config import Config
from .models import BookSummary, CvdSnapshot, LiquidationSnapshot, Signals

log = logging.getLogger(__name__)


def detect_signals(
    cfg: Config,
    *,
    book: BookSummary,
    cvd: CvdSnapshot,
    liq: LiquidationSnapshot,
    reversal_ratio: float,
) -> Signals:
    """Прогнать все сигнатуры и собрать ``Signals``."""
    signals = Signals()

    if _bid_wall(book, cfg):
        signals.bid_wall = True
        signals.hits.append("bid_wall")

    if _cvd_fade(cvd, reversal_ratio, cfg):
        signals.cvd_fade = True
        signals.hits.append("cvd_fade")

    if _liq_decay(liq, cfg):
        signals.liq_decay = True
        signals.hits.append("liq_decay")

    return signals


def _bid_wall(book: BookSummary, cfg: Config) -> bool:
    if cfg.depth.wall_side != "bid":
        return False
    return book.wall is not None


def _cvd_fade(cvd: CvdSnapshot, reversal_ratio: float, cfg: Config) -> bool:
    reversal = reversal_ratio >= cfg.cvd.reversal_ratio
    burst = cvd.taker_ratio >= cfg.cvd.taker_buy_ratio
    return reversal or burst


def _liq_decay(liq: LiquidationSnapshot, cfg: Config) -> bool:
    if liq.earlier_notional < cfg.liquidation.min_notional_usdt:
        return False
    return liq.decay_ratio <= cfg.liquidation.decay_ratio


def passed_liquidity(cfg: Config, book: BookSummary) -> bool:
    """Фильтр стакана: суммарный номинал top-N уровней и качество данных.

    ``require_full_depth`` важен: если снимок содержит только 5 уровней из ``books5``,
    сумма занижена, и порог в 1 млн USDT будет сравниваться с неполной глубиной.
    """
    return book_ready(cfg, book) and book.liquidity_notional >= cfg.depth.min_notional_usdt


def book_ready(cfg: Config, book: BookSummary) -> bool:
    """Стакану можно верить: свежий и полный. Неготовый ничего не говорит о ликвидности."""
    if cfg.depth.require_fresh_book and not book.fresh:
        return False
    return not (cfg.depth.require_full_depth and not book.full_depth)


def score_ok(cfg: Config, signals: Signals) -> bool:
    return signals.score >= cfg.signals.min_score
