"""Виртуальный счёт: что было бы, если входить по каждому алерту на весь баланс.

Правила (секция конфига ``paper``):

* вход — по цене плана (лучший ask в момент алерта), маржа — весь баланс, плечо ``leverage``;
* одна позиция за раз: пока она открыта, новые сигналы счёт пропускает — «весь баланс в
  каждую сделку» иначе не имеет смысла;
* тейк — цель плана ``target_index`` (по умолчанию 61.8% отката дампа), стоп — стоп плана;
  если ликвидация ближе стопа, позиция ликвидируется и маржа теряется целиком;
* после первой цели стоп переносится в безубыток (вход + комиссии);
* через ``max_hold_s`` позиция закрывается по рынку: откуп после дампа — идея на часы;
* комиссия тейкера на вход и выход, проскальзывание на выходах по рынку (стоп, время).

Каждый сигнал моделируется и отдельно от счёта: так видно качество самих сигналов
(матожидание в R и в % на сделку), а счёт показывает, что с ними делают плечо и реинвест.

Внутри одного бара порядок цен неизвестен — считаем, что сначала был стоп (пессимистично).
Бар — минутная свеча при прогоне прошлых алертов или сделки за 2 c вживую: логика одна,
поэтому прогон истории и живой счёт сравнимы. Свеча, в которую попал алерт, при прогоне
пропускается: её максимум мог быть до входа.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import asdict, dataclass, fields
from typing import TYPE_CHECKING, Any

from .config import Config, PaperConfig, TradeConfig
from .models import BookSummary, Candle, Level, TradePlan
from .parsers import parse_candle
from .plan import build_plan

if TYPE_CHECKING:
    from .history import History
    from .instrument import InstrumentState
    from .market import OkxRest

log = logging.getLogger(__name__)

# поддерживающая маржа OKX для небольших позиций: ликвидация чуть раньше, чем −1/плечо
MAINT_MARGIN = 0.005
# меньше сделок — матожидание ничего не значит, его разброс больше его самого
MIN_SAMPLE = 30

REASON_TP = "тейк"
REASON_SL = "стоп"
REASON_BE = "безубыток"
REASON_TIME = "время"
REASON_LIQ = "ликвидация"


@dataclass
class PaperTrade:
    alert_id: int
    inst_id: str
    window: str
    opened_at: float
    entry: float
    stop: float
    target: float
    first_target: float | None
    rr: float
    leverage: float
    fee_pct: float
    liq_px: float = 0.0
    stop_now: float = 0.0
    status: str = "open"  # open | closed
    reason: str = ""
    exit_px: float | None = None
    closed_at: float | None = None
    checked_to: float = 0.0
    be_armed: bool = False
    taken: bool = False
    skip_reason: str = ""
    margin: float = 0.0
    pnl_usdt: float = 0.0
    balance_after: float | None = None
    source: str = "live"  # live | replay

    def __post_init__(self) -> None:
        if not self.stop_now:
            self.stop_now = self.stop
        if not self.checked_to:
            self.checked_to = self.opened_at

    @property
    def risk_frac(self) -> float:
        return (self.entry - self.stop) / self.entry

    def price_return(self, px: float | None = None) -> float:
        """Доля движения цены за вычетом комиссий на вход и выход."""
        px = self.exit_px if px is None else px
        if px is None:
            return 0.0
        return px / self.entry - 1.0 - 2.0 * self.fee_pct / 100.0

    def margin_return(self, px: float | None = None) -> float:
        """Доля маржи: движение × плечо, не хуже −100% (ликвидация)."""
        if px is None and self.reason == REASON_LIQ:
            return -1.0
        return max(-1.0, self.leverage * self.price_return(px))

    def r_multiple(self) -> float:
        return self.price_return() / self.risk_frac if self.risk_frac > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PaperTrade:
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})

    def to_api(self, *, price: float | None = None) -> dict[str, Any]:
        closed = self.status == "closed"
        return {
            "alertId": self.alert_id,
            "instId": self.inst_id,
            "window": self.window,
            "openedAt": self.opened_at,
            "closedAt": self.closed_at,
            "status": self.status,
            "reason": self.reason,
            "entry": self.entry,
            "stop": self.stop,
            "stopNow": self.stop_now,
            "target": self.target,
            "firstTarget": self.first_target,
            "rr": round(self.rr, 2),
            "exit": self.exit_px,
            "price": price,
            "pricePct": round(self.price_return(None if closed else price) * 100, 3) if closed or price else None,
            "marginPct": round(self.margin_return(None if closed else price) * 100, 2) if closed or price else None,
            "r": round(self.r_multiple(), 2) if closed else None,
            "beArmed": self.be_armed,
            "taken": self.taken,
            "skipReason": self.skip_reason,
            "margin": round(self.margin, 2),
            "pnl": round(self.pnl_usdt, 2),
            "balanceAfter": None if self.balance_after is None else round(self.balance_after, 2),
            "source": self.source,
        }


def open_trade(alert_id: int, data: dict[str, Any], cfg: PaperConfig, *, source: str = "live") -> PaperTrade | None:
    """Сделка по плану алерта; ``None`` — плана нет или он не лонг (стоп выше входа, цель ниже)."""
    plan = data.get("plan")
    if not isinstance(plan, dict) or not plan.get("targets"):
        return None
    targets = [float(t) for t in plan["targets"]]
    idx = max(0, min(cfg.target_index, len(targets) - 1))
    entry, stop, target = float(plan["entry"]), float(plan["stop"]), targets[idx]
    if not (0 < stop < entry < target):
        return None
    leverage = max(1.0, cfg.leverage)
    return PaperTrade(
        alert_id=alert_id,
        inst_id=str(data.get("instId") or ""),
        window=str(data.get("window") or ""),
        opened_at=float(data.get("ts") or 0.0),
        entry=entry,
        stop=stop,
        target=target,
        first_target=targets[0] if idx > 0 else None,
        rr=(target - entry) / (entry - stop),
        leverage=leverage,
        fee_pct=cfg.fee_pct,
        liq_px=entry * (1.0 - 1.0 / leverage + MAINT_MARGIN) if leverage > 1 else 0.0,
        source=source,
    )


def step(
    trade: PaperTrade, cfg: PaperConfig, *, low: float, high: float, close: float, ts: float
) -> bool:
    """Провести сделку через бар цены. ``True`` — сделка закрылась на этом баре."""
    if trade.status != "open":
        return False
    slip = cfg.slippage_pct / 100.0
    if low <= max(trade.stop_now, trade.liq_px):
        if trade.liq_px >= trade.stop_now:
            _close(trade, REASON_LIQ, trade.liq_px, ts)
        else:
            _close(trade, REASON_BE if trade.be_armed else REASON_SL, trade.stop_now * (1.0 - slip), ts)
        return True
    if high >= trade.target:
        _close(trade, REASON_TP, trade.target, ts)
        return True
    if cfg.breakeven_after_first and not trade.be_armed and trade.first_target and high >= trade.first_target:
        trade.be_armed = True
        trade.stop_now = max(trade.stop_now, trade.entry * (1.0 + 2.0 * trade.fee_pct / 100.0))
    trade.checked_to = max(trade.checked_to, ts)
    if ts - trade.opened_at >= cfg.max_hold_s:
        _close(trade, REASON_TIME, close * (1.0 - slip), ts)
        return True
    return False


def _close(trade: PaperTrade, reason: str, px: float, ts: float) -> None:
    trade.status = "closed"
    trade.reason = reason
    trade.exit_px = px
    trade.closed_at = ts
    trade.checked_to = ts


def reconstruct_plan(
    cfg: TradeConfig, data: dict[str, Any], candles: list[Candle], *, ct_val: float = 1.0, tick_sz: float = 0.0
) -> TradePlan | None:
    """План для алерта, сохранённого до появления планов: по стакану из алерта и свечам до него."""
    ts = float(data.get("ts") or 0.0)
    price = float(data.get("price") or 0.0)
    high = float(data.get("windowHigh") or 0.0)
    window_s = float(data.get("windowSeconds") or 0.0)
    book = data.get("book") or {}
    # только свечи, закрывшиеся до алерта: в незакрытой уже есть цены после входа
    before = [c for c in candles if c.ts >= ts - window_s and c.end_ts <= ts]
    low = price
    if before:
        peak = max(before, key=lambda c: c.high)
        low = min([price] + [c.low for c in before if c.ts >= peak.ts and c.low > 0])
    wall = book.get("wall") or None
    summary = BookSummary(
        inst_id=str(data.get("instId") or ""),
        ts=ts,
        best_bid=book.get("bestBid"),
        best_ask=book.get("bestAsk"),
        wall=Level(px=float(wall["px"]), sz=float(wall.get("sz") or 0.0), ts=ts) if wall else None,
    )
    return build_plan(cfg, price=price, high=high, low=low, book=summary, ct_val=ct_val, tick_sz=tick_sz, now=ts)


class PaperBook:
    """Виртуальный счёт поверх истории алертов: открытие, сопровождение, реинвест, статистика."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.history: History | None = None
        self.trades: dict[int, PaperTrade] = {}
        self.ready = False
        self._seen: set[int] = set()

    @property
    def pcfg(self) -> PaperConfig:
        return self.cfg.paper

    def load(self, history: History | None) -> None:
        self.history = history
        if history is None:
            return
        for data in history.paper_trades():
            trade = PaperTrade.from_dict(data)
            self.trades[trade.alert_id] = trade
            self._seen.add(trade.alert_id)

    # ---------- счёт ----------

    def balance(self, *, at: float | None = None) -> float:
        """Баланс после всех сделок счёта, закрытых к моменту ``at`` (``None`` — ко всем)."""
        total = self.pcfg.start_balance
        for t in self.trades.values():
            if t.taken and t.status == "closed" and (at is None or (t.closed_at or 0.0) <= at):
                total += t.pnl_usdt
        return total

    def _busy(self, at: float) -> PaperTrade | None:
        """Позиция счёта, открытая в момент ``at`` — капитал занят."""
        for t in self.trades.values():
            if t.taken and t.opened_at <= at and (t.status == "open" or (t.closed_at or 0.0) > at):
                return t
        return None

    def _reentry_pause(self, inst_id: str, at: float) -> str | None:
        """Пауза на повторный вход в ту же монету после стопа (не ловим падающий нож дважды)."""
        cooldown = self.pcfg.reentry_cooldown_s
        if cooldown <= 0:
            return None
        closed = [
            t
            for t in self.trades.values()
            if t.taken and t.inst_id == inst_id and t.status == "closed" and (t.closed_at or 0.0) <= at
        ]
        if not closed:
            return None
        last = max(closed, key=lambda t: t.closed_at or 0.0)
        if last.reason not in (REASON_SL, REASON_LIQ):
            return None
        left = cooldown - (at - (last.closed_at or 0.0))
        if left <= 0:
            return None
        return f"пауза после стопа {inst_id} ({max(1, round(left / 60))} мин)"

    def pause_reason(self, at: float) -> str | None:
        """Пауза счёта после серии стопов подряд (защита от каскадного пролива рынка)."""
        limit = self.pcfg.loss_streak_limit
        pause = self.pcfg.loss_streak_pause_s
        if limit <= 0 or pause <= 0:
            return None
        closed = sorted(
            (t for t in self.trades.values() if t.taken and t.status == "closed" and (t.closed_at or 0.0) <= at),
            key=lambda t: t.closed_at or 0.0,
        )
        streak = 0
        last_stop_at = 0.0
        for t in closed:
            if streak >= limit and t.opened_at >= last_stop_at + pause:
                streak = 0
            if t.reason in (REASON_SL, REASON_LIQ):
                streak += 1
                last_stop_at = t.closed_at or t.opened_at
            else:
                streak = 0
        if streak < limit:
            return None
        left = pause - (at - last_stop_at)
        if left <= 0:
            return None
        return f"пауза после {streak} стопов подряд ({max(1, round(left / 60))} мин)"

    def on_alert(self, alert_id: int, data: dict[str, Any], *, source: str = "live") -> PaperTrade | None:
        if alert_id in self._seen or (source == "live" and not self.ready):
            # до конца прогона живые алерты подхватит он: порядок сделок на счёте важен
            return None
        self._seen.add(alert_id)
        trade = open_trade(alert_id, data, self.pcfg, source=source)
        if trade is None:
            return None
        busy = self._busy(trade.opened_at)
        streak_pause = self.pause_reason(trade.opened_at)
        inst_pause = self._reentry_pause(trade.inst_id, trade.opened_at)
        if not self.pcfg.enabled:
            trade.skip_reason = "счёт выключен"
        elif busy is not None:
            trade.skip_reason = f"капитал в сделке {busy.inst_id}"
        elif streak_pause is not None:
            trade.skip_reason = streak_pause
        elif inst_pause is not None:
            trade.skip_reason = inst_pause
        elif trade.rr < self.pcfg.min_rr:
            trade.skip_reason = f"прибыль/риск {trade.rr:.2f} < {self.pcfg.min_rr:g}"
        else:
            trade.taken = True
            trade.margin = self.balance(at=trade.opened_at)
        self.trades[alert_id] = trade
        self._save(trade)
        if trade.taken:
            log.info(
                "счёт: лонг %s на %.2f USDT ×%g, вход %s, стоп %s, тейк %s",
                trade.inst_id, trade.margin, trade.leverage, trade.entry, trade.stop, trade.target,
            )
        return trade

    def update(self, states: dict[str, InstrumentState], *, now: float) -> list[PaperTrade]:
        """Сопровождение открытых сделок по сделкам рынка с прошлой проверки (вызывается из оценки)."""
        if not self.ready:
            return []
        closed = []
        for trade in [t for t in self.trades.values() if t.status == "open"]:
            state = states.get(trade.inst_id)
            if state is None:
                continue
            rng = state.trades.range_since(trade.checked_to, now=now)
            if rng is None:
                px = state.price(now=now, stale_after_s=120.0)
                if px is None:
                    continue
                rng = (px, px, px)
            low, high, close = rng
            armed = trade.be_armed
            if step(trade, self.pcfg, low=low, high=high, close=close, ts=now):
                self._settle(trade)
                closed.append(trade)
            elif trade.be_armed != armed:
                self._save(trade)
        return closed

    def _settle(self, trade: PaperTrade) -> None:
        if trade.taken:
            trade.pnl_usdt = trade.margin * trade.margin_return()
            trade.balance_after = self.balance(at=trade.closed_at)
            log.info(
                "счёт: %s закрыт (%s) %+.2f USDT (%+.1f%% маржи), баланс %.2f",
                trade.inst_id, trade.reason, trade.pnl_usdt, trade.margin_return() * 100, trade.balance_after,
            )
        self._save(trade)

    def _save(self, trade: PaperTrade) -> None:
        if self.history is None:
            return
        try:
            self.history.save_paper(trade.alert_id, trade.opened_at, trade.status, trade.to_dict())
        except Exception:  # noqa: BLE001 — запись истории не важнее сопровождения
            log.exception("счёт: не удалось сохранить сделку %s", trade.alert_id)

    def reset(self) -> None:
        """Начать счёт заново: сделки пересчитаются прогоном по истории алертов."""
        self.trades.clear()
        self._seen.clear()
        self.ready = False
        if self.history is not None:
            self.history.clear_paper()

    # ---------- прогон прошлых алертов ----------

    async def replay(self, rest: OkxRest, states: dict[str, InstrumentState]) -> None:
        """Догнать счёт по истории: алерты, которых в нём нет, и сделки, открытые до рестарта."""
        try:
            for trade in sorted((t for t in self.trades.values() if t.status == "open"), key=lambda t: t.opened_at):
                await self._catch_up(rest, trade)
            while self.history is not None:
                pending = self.history.alert_payloads(exclude=self._seen)
                if not pending:
                    break
                for alert_id, data in pending:
                    try:
                        await self._replay_alert(rest, states, alert_id, data)
                    except Exception:  # noqa: BLE001 — один алерт без свечей не должен остановить прогон
                        log.exception("счёт: не удалось прогнать алерт %s", alert_id)
                        self._seen.add(alert_id)
        finally:
            # между последней выборкой и этим флагом нет await: живой алерт не потеряется
            self.ready = True
        log.info("счёт: %d сделок, баланс %.2f USDT", sum(t.taken for t in self.trades.values()), self.balance())

    async def _replay_alert(
        self, rest: OkxRest, states: dict[str, InstrumentState], alert_id: int, data: dict[str, Any]
    ) -> None:
        ts = float(data.get("ts") or 0.0)
        inst_id = str(data.get("instId") or "")
        now = time.time()
        has_plan = isinstance(data.get("plan"), dict)
        since = ts if has_plan else ts - float(data.get("windowSeconds") or 0.0) - 60.0
        until = min(now, ts + self.pcfg.max_hold_s + 120.0)
        rows = await rest.candles_between(inst_id, since=since, until=until)
        candles = [c for c in (parse_candle(r) for r in rows) if c is not None]
        if not has_plan:
            state = states.get(inst_id)
            plan = reconstruct_plan(
                self.cfg.trade,
                data,
                candles,
                ct_val=state.ct_val if state else 1.0,
                tick_sz=state.tick_sz if state else 0.0,
            )
            data = {**data, "plan": plan.to_dict() if plan else None}
        trade = self.on_alert(alert_id, data, source="replay")
        if trade is None:
            return
        self._run_candles(trade, candles, now=now)
        await asyncio.sleep(0)

    async def _catch_up(self, rest: OkxRest, trade: PaperTrade) -> None:
        """Сделка была открыта до рестарта: проводим её через свечи за время простоя."""
        now = time.time()
        if now - trade.checked_to < 60.0:
            return
        try:
            rows = await rest.candles_between(trade.inst_id, since=trade.checked_to, until=now)
        except Exception:  # noqa: BLE001 — без свечей сделка продолжит вживую
            log.warning("счёт: нет свечей %s для догоняющего прогона", trade.inst_id)
            return
        candles = [c for c in (parse_candle(r) for r in rows) if c is not None]
        self._run_candles(trade, candles, now=now)

    def _run_candles(self, trade: PaperTrade, candles: list[Candle], *, now: float) -> None:
        armed = trade.be_armed
        for candle in candles:
            if candle.ts < trade.checked_to:
                continue
            if step(trade, self.pcfg, low=candle.low, high=candle.high, close=candle.close, ts=min(candle.end_ts, now)):
                self._settle(trade)
                return
        if trade.be_armed != armed or trade.source == "replay":
            self._save(trade)

    # ---------- статистика ----------

    def summary(self, states: dict[str, InstrumentState], *, now: float) -> dict[str, Any]:
        pc = self.pcfg
        ordered = sorted(self.trades.values(), key=lambda t: t.opened_at)
        closed = [t for t in ordered if t.status == "closed"]
        account = sorted((t for t in closed if t.taken), key=lambda t: t.closed_at or 0.0)

        open_pos = next((t for t in ordered if t.taken and t.status == "open"), None)
        unrealized = 0.0
        open_api = None
        if open_pos is not None:
            state = states.get(open_pos.inst_id)
            px = state.price(now=now, stale_after_s=300.0) if state else None
            if px:
                unrealized = open_pos.margin * open_pos.margin_return(px)
            open_api = open_pos.to_api(price=px)

        balance = self.balance()
        curve = [{"ts": account[0].opened_at if account else now, "balance": pc.start_balance}]
        curve += [{"ts": t.closed_at, "balance": round(t.balance_after or 0.0, 4)} for t in account]
        peak, max_dd = pc.start_balance, 0.0
        for point in curve:
            peak = max(peak, point["balance"])
            max_dd = max(max_dd, (peak - point["balance"]) / peak if peak > 0 else 0.0)

        acc = _stats([t.margin_return() for t in account])
        growth = (balance / pc.start_balance) ** (1.0 / len(account)) - 1.0 if account and balance > 0 else None
        sig = _stats([t.margin_return() for t in closed])
        r_values = [t.r_multiple() for t in closed]
        return {
            "enabled": pc.enabled,
            "ready": self.ready,
            "rules": {
                "startBalance": pc.start_balance,
                "leverage": pc.leverage,
                "feePct": pc.fee_pct,
                "slippagePct": pc.slippage_pct,
                "targetIndex": pc.target_index,
                "minRr": pc.min_rr,
                "breakeven": pc.breakeven_after_first,
                "maxHoldS": pc.max_hold_s,
                "reentryCooldownS": pc.reentry_cooldown_s,
                "lossStreakLimit": pc.loss_streak_limit,
                "lossStreakPauseS": pc.loss_streak_pause_s,
            },
            "account": {
                "start": pc.start_balance,
                "balance": round(balance, 2),
                "equity": round(balance + unrealized, 2),
                "returnPct": round((balance / pc.start_balance - 1.0) * 100, 2) if pc.start_balance else None,
                "maxDrawdownPct": round(max_dd * 100, 2),
                "growthPerTradePct": None if growth is None else round(growth * 100, 3),
                "open": open_api,
                "pause": self.pause_reason(now),
                "unrealized": round(unrealized, 2),
                **acc,
            },
            "signals": {
                **sig,
                "open": sum(1 for t in ordered if t.status == "open"),
                "avgR": round(sum(r_values) / len(r_values), 3) if r_values else None,
                "reasons": _count(t.reason for t in closed),
            },
            "skipped": _count(t.skip_reason.split(" ")[0] for t in ordered if not t.taken),
            "minSample": MIN_SAMPLE,
            "curve": curve,
            "trades": [t.to_api() for t in reversed(ordered)][:300],
        }


def _stats(returns: list[float]) -> dict[str, Any]:
    """Сводка по доходностям сделок (доли маржи): частота, средние, матожидание."""
    n = len(returns)
    wins = [r for r in returns if r > 0]
    losses = [r for r in returns if r <= 0]
    avg_win = sum(wins) / len(wins) if wins else None
    avg_loss = sum(losses) / len(losses) if losses else None
    gross_loss = -sum(losses)
    breakeven = None
    if avg_win is not None and avg_loss is not None and avg_win - avg_loss > 0:
        breakeven = -avg_loss / (avg_win - avg_loss)
    return {
        "trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "winRate": round(len(wins) / n * 100, 1) if n else None,
        "avgWinPct": None if avg_win is None else round(avg_win * 100, 2),
        "avgLossPct": None if avg_loss is None else round(avg_loss * 100, 2),
        "expectancyPct": round(sum(returns) / n * 100, 2) if n else None,
        "profitFactor": round(sum(wins) / gross_loss, 2) if gross_loss > 0 else None,
        "breakevenWinRate": None if breakeven is None else round(breakeven * 100, 1),
    }


def _count(items: Any) -> dict[str, int]:
    out: dict[str, int] = {}
    for item in items:
        if item:
            out[item] = out.get(item, 0) + 1
    return out
