"""Последние записи лога в памяти — для вкладки «Журнал» в дашборде."""

from __future__ import annotations

import logging
import threading
from collections import deque
from typing import Any

_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}


class LogBuffer(logging.Handler):
    def __init__(self, maxlen: int = 1000) -> None:
        super().__init__()
        self._records: deque[dict[str, Any]] = deque(maxlen=max(10, maxlen))
        self._seq = 0
        self._seq_lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = record.getMessage()
            if record.exc_info and record.exc_info[1] is not None:
                message += f" | {type(record.exc_info[1]).__name__}: {record.exc_info[1]}"
            with self._seq_lock:
                self._seq += 1
                self._records.append(
                    {
                        "seq": self._seq,
                        "ts": record.created,
                        "level": record.levelname,
                        "name": record.name,
                        "msg": message,
                    }
                )
        except Exception:  # noqa: BLE001 — логирование не должно ронять вызывающий код
            self.handleError(record)

    def records(self, *, after: int = 0, level: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
        floor = _LEVELS.get((level or "").upper(), 0)
        with self._seq_lock:
            items = list(self._records)
        picked = [r for r in items if r["seq"] > after and _LEVELS.get(r["level"], 0) >= floor]
        return picked[-max(1, limit) :]

    def counts(self, *, since: float) -> dict[str, int]:
        with self._seq_lock:
            items = list(self._records)
        result: dict[str, int] = {}
        for rec in items:
            if rec["ts"] >= since:
                result[rec["level"]] = result.get(rec["level"], 0) + 1
        return result


_installed: LogBuffer | None = None


def install(maxlen: int = 1000) -> LogBuffer:
    """Подключить буфер к корневому логгеру один раз на процесс.

    Движок пересоздаётся при перезапуске из дашборда, а журнал должен пережить это.
    """
    global _installed
    if _installed is None:
        _installed = LogBuffer(maxlen)
        logging.getLogger().addHandler(_installed)
    return _installed
