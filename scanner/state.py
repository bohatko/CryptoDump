"""Сохранение состояния между перезапусками: CVD, ликвидации, кулдаун алертов.

Зачем. После рестарта секундные бакеты трейдов и история ликвидаций пусты, поэтому
сигнатуры ``cvd_fade`` и ``liq_decay`` не работают, пока не наберётся окно
(5 минут по умолчанию), а кулдаун обнуляется и может продублировать только что
отправленный алерт. Снимок состояния убирает этот «слепой» период.

Что сохраняем — только агрегаты, не сырые сделки: секундные бакеты CVD, события
ликвидаций и отметки кулдауна. Файл небольшой (десятки КБ на инструмент) и
перезаписывается атомарно, чтобы падение в момент записи не оставило мусор.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

from .alerts import Cooldown
from .windows import LiquidationTracker, TradeWindow

log = logging.getLogger(__name__)

STATE_VERSION = 1

# Запись состояния идёт из фонового цикла (в отдельном потоке) и из финального
# сохранения при остановке. Без лока две записи конкурируют за файл — на Windows
# это даёт Permission denied на переименовании.
_SAVE_LOCK = threading.Lock()


def save_state(
    path: str | Path,
    *,
    trades: dict[str, TradeWindow],
    liq: dict[str, LiquidationTracker],
    cooldown: Cooldown,
    now: float | None = None,
) -> bool:
    """Записать состояние на диск. Возвращает ``False``, если записать не удалось."""
    now = time.time() if now is None else now
    return write_state(path, state_payload(trades=trades, liq=liq, cooldown=cooldown, now=now), now=now)


def state_payload(
    *,
    trades: dict[str, TradeWindow],
    liq: dict[str, LiquidationTracker],
    cooldown: Cooldown,
    now: float,
) -> dict[str, Any]:
    """Снимок состояния. Собирать в потоке event loop: обход словарей, которые этот
    же loop меняет, из другого потока падает с «dictionary changed size during iteration»."""
    return {
        "version": STATE_VERSION,
        "savedAt": now,
        "trades": {
            inst_id: {
                str(second): [round(b.buy, 4), round(b.sell, 4), b.high, b.low, b.close]
                for second, b in window._seconds.items()
            }
            for inst_id, window in trades.items()
        },
        "liquidations": {
            inst_id: [[round(ts, 3), round(notional, 4)] for ts, notional in tracker._events]
            for inst_id, tracker in liq.items()
            if len(tracker)
        },
        "cooldown": {
            f"{inst_id}|{label}": ts for (inst_id, label), ts in cooldown.snapshot().items()
        },
    }


def write_state(path: str | Path, payload: dict[str, Any], *, now: float) -> bool:
    """Записать готовый снимок — эту часть можно отдавать в поток."""
    with _SAVE_LOCK:
        return _write_payload(payload, Path(path), now)


def _write_payload(payload: dict[str, Any], target: Path, now: float) -> bool:
    # Уникальное имя временного файла: даже если лок не удержит (например, запись
    # из другого процесса), два писателя не будут биться за один и тот же .tmp.
    tmp = target.with_name(f"{target.name}.{os.getpid()}.{int(now * 1000) % 1_000_000}.tmp")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, separators=(",", ":"))
        _replace_with_retry(tmp, target)
        log.debug("состояние сохранено в %s (%.1f КБ)", target, target.stat().st_size / 1024)
        return True
    except OSError as exc:
        # Антивирус/индексатор Windows могут кратко держать файл — это не повод падать
        log.warning("не удалось сохранить состояние в %s: %s", target, exc)
        return False
    finally:
        tmp.unlink(missing_ok=True)


def _replace_with_retry(tmp: Path, target: Path, *, attempts: int = 5) -> None:
    """``os.replace`` с повторами: на Windows файл может быть кратко занят."""
    last: OSError | None = None
    for attempt in range(attempts):
        try:
            os.replace(tmp, target)
            return
        except OSError as exc:
            last = exc
            time.sleep(0.05 * (attempt + 1))
    if last is not None:
        raise last


def load_state(
    path: str | Path,
    *,
    trades: dict[str, TradeWindow],
    liq: dict[str, LiquidationTracker],
    cooldown: Cooldown,
    max_age_s: float,
    now: float | None = None,
) -> bool:
    """Восстановить состояние из файла.

    Состояние старше ``max_age_s`` игнорируется: протухшие бакеты только запутают
    детекцию (в них уже нет актуального потока). Возвращает ``True``, если данные
    применились.
    """
    now = time.time() if now is None else now
    target = Path(path)
    if not target.is_file():
        return False

    try:
        with target.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("состояние не прочитано (%s): %s — стартую с чистого листа", target, exc)
        return False

    if not isinstance(payload, dict) or payload.get("version") != STATE_VERSION:
        log.warning("состояние в %s несовместимо — игнорирую", target)
        return False

    saved_at = float(payload.get("savedAt") or 0.0)
    age = now - saved_at
    if age > max_age_s:
        log.info("состояние устарело (%.0f c > %.0f c) — стартую с чистого листа", age, max_age_s)
        return False
    if age < -60:
        # время ушло назад (сменили часы/таймзону) — доверять бакетам нельзя
        log.warning("состояние из будущего (%.0f c) — игнорирую", -age)
        return False

    restored_trades = 0
    restored_liq = 0

    raw_trades = payload.get("trades")
    if isinstance(raw_trades, dict):
        for inst_id, buckets in raw_trades.items():
            window = trades.get(inst_id)
            if window is None or not isinstance(buckets, dict):
                continue
            for second, values in buckets.items():
                if not isinstance(values, list) or len(values) < 5:
                    continue
                try:
                    ts = int(second)
                    buy, sell, high, low, close = (float(v) for v in values[:5])
                except (TypeError, ValueError):
                    continue
                if ts < now - window.max_window_s:
                    continue
                bucket = window._seconds.get(ts)
                if bucket is None:
                    from .windows import _Second

                    bucket = _Second()
                    window._seconds[ts] = bucket
                bucket.buy += buy
                bucket.sell += sell
                bucket.high = max(bucket.high, high)
                bucket.low = min(bucket.low, low) if bucket.low else low
                bucket.close = close or bucket.close
                restored_trades += 1

    raw_liq = payload.get("liquidations")
    if isinstance(raw_liq, dict):
        for inst_id, events in raw_liq.items():
            tracker = liq.get(inst_id)
            if tracker is None or not isinstance(events, list):
                continue
            for pair in events:
                if not isinstance(pair, list) or len(pair) < 2:
                    continue
                try:
                    ts, notional = float(pair[0]), float(pair[1])
                except (TypeError, ValueError):
                    continue
                if ts < now - tracker.max_window_s:
                    continue
                tracker.add(ts, notional, now=now)
                restored_liq += 1

    raw_cd = payload.get("cooldown")
    if isinstance(raw_cd, dict):
        for key, ts in raw_cd.items():
            if "|" not in key:
                continue
            inst_id, label = key.split("|", 1)
            try:
                stamp = float(ts)
            except (TypeError, ValueError):
                continue
            if now - stamp <= cooldown.cooldown_s:
                cooldown.restore(inst_id, label, stamp)

    log.info(
        "состояние восстановлено из %s (возраст %.0f c): бакетов %d, ликвидаций %d",
        target,
        age,
        restored_trades,
        restored_liq,
    )
    return True
