"""История алертов и эпизодов просадки в SQLite (стандартная библиотека, без зависимостей).

Запросы синхронные: строки маленькие, выборки ограничены ``limit``, а запись идёт
раз в несколько секунд — это доли миллисекунды, отдельный поток не нужен.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

from .models import Alert
from .monitor import DropEpisode

log = logging.getLogger(__name__)

# «window» — ключевое слово SQLite, поэтому колонка называется win
SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    inst_id TEXT NOT NULL,
    inst_type TEXT,
    win TEXT,
    drop_pct REAL,
    price REAL,
    score INTEGER,
    hits TEXT,
    total_notional REAL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS alerts_ts ON alerts(ts);
CREATE INDEX IF NOT EXISTS alerts_inst ON alerts(inst_id, ts);

CREATE TABLE IF NOT EXISTS episodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    inst_id TEXT NOT NULL,
    started_at REAL NOT NULL,
    ended_at REAL NOT NULL,
    win TEXT,
    max_drop_pct REAL,
    price REAL,
    status TEXT,
    detail TEXT,
    alerted INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS episodes_started ON episodes(started_at);
CREATE INDEX IF NOT EXISTS episodes_inst ON episodes(inst_id, started_at);
"""

MAX_LIMIT = 500


class History:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        if self.path != ":memory:":
            # WAL: чтение из дашборда не блокирует запись из цикла оценки
            self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(SCHEMA)
        self._db.commit()

    def close(self) -> None:
        self._db.close()

    # ---------- запись ----------

    def add_alert(self, alert: Alert) -> int:
        return self.add_alert_dict(alert.to_dict())

    def add_alert_dict(self, data: dict[str, Any]) -> int:
        signals = data.get("signals") or {}
        book = data.get("book") or {}
        cur = self._db.execute(
            "INSERT INTO alerts (ts, inst_id, inst_type, win, drop_pct, price, score, hits, total_notional, payload)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                float(data.get("ts") or 0.0),
                str(data.get("instId") or ""),
                data.get("instType"),
                data.get("window"),
                data.get("dropPct"),
                data.get("price"),
                int(signals.get("score") or 0),
                ",".join(signals.get("hits") or []),
                book.get("totalNotional"),
                json.dumps(data, ensure_ascii=False),
            ),
        )
        self._db.commit()
        return int(cur.lastrowid or 0)

    def add_episode(self, episode: DropEpisode) -> None:
        self._db.execute(
            "INSERT INTO episodes (inst_id, started_at, ended_at, win, max_drop_pct, price, status, detail, alerted)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                episode.inst_id,
                episode.started_at,
                episode.last_seen,
                episode.max_window,
                episode.max_drop_pct,
                episode.price,
                episode.status,
                episode.detail,
                int(episode.alerted),
            ),
        )
        self._db.commit()

    def import_jsonl(self, path: str | Path) -> int:
        """Подтянуть алерты из ``alerts.jsonl``, если база ещё пустая (первый запуск дашборда)."""
        file_path = Path(path)
        if not file_path.exists() or self._count("SELECT COUNT(*) FROM alerts", ()) > 0:
            return 0
        imported = 0
        with file_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(data, dict) and data.get("instId"):
                    self.add_alert_dict(data)
                    imported += 1
        if imported:
            log.info("история: импортировано %d алертов из %s", imported, file_path)
        return imported

    # ---------- чтение ----------

    def alerts(
        self,
        *,
        inst: str | None = None,
        window: str | None = None,
        since: float | None = None,
        until: float | None = None,
        min_score: int | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        where, params = [], []
        if inst:
            where.append("inst_id LIKE ?")
            params.append(f"%{inst.strip().upper()}%")
        if window:
            where.append("win = ?")
            params.append(window)
        if since is not None:
            where.append("ts >= ?")
            params.append(since)
        if until is not None:
            where.append("ts < ?")
            params.append(until)
        if min_score is not None:
            where.append("score >= ?")
            params.append(min_score)
        clause = f" WHERE {' AND '.join(where)}" if where else ""
        total = self._count(f"SELECT COUNT(*) FROM alerts{clause}", params)
        rows = self._db.execute(
            "SELECT id, ts, inst_id, inst_type, win, drop_pct, price, score, hits, total_notional"
            f" FROM alerts{clause} ORDER BY ts DESC, id DESC LIMIT ? OFFSET ?",
            (*params, _limit(limit), max(0, offset)),
        ).fetchall()
        items = [
            {
                "id": r["id"],
                "ts": r["ts"],
                "instId": r["inst_id"],
                "instType": r["inst_type"],
                "window": r["win"],
                "dropPct": r["drop_pct"],
                "price": r["price"],
                "score": r["score"],
                "hits": [h for h in (r["hits"] or "").split(",") if h],
                "totalNotional": r["total_notional"],
            }
            for r in rows
        ]
        return {"items": items, "total": total}

    def alert(self, alert_id: int) -> dict[str, Any] | None:
        row = self._db.execute("SELECT id, payload FROM alerts WHERE id = ?", (alert_id,)).fetchone()
        if row is None:
            return None
        data = json.loads(row["payload"])
        data["id"] = row["id"]
        return data

    def episodes(
        self,
        *,
        inst: str | None = None,
        since: float | None = None,
        status: str | None = None,
        alerted: bool | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        where, params = [], []
        if inst:
            where.append("inst_id LIKE ?")
            params.append(f"%{inst.strip().upper()}%")
        if since is not None:
            where.append("started_at >= ?")
            params.append(since)
        if status:
            where.append("status = ?")
            params.append(status)
        if alerted is not None:
            where.append("alerted = ?")
            params.append(int(alerted))
        clause = f" WHERE {' AND '.join(where)}" if where else ""
        total = self._count(f"SELECT COUNT(*) FROM episodes{clause}", params)
        rows = self._db.execute(
            f"SELECT * FROM episodes{clause} ORDER BY started_at DESC, id DESC LIMIT ? OFFSET ?",
            (*params, _limit(limit), max(0, offset)),
        ).fetchall()
        items = [
            {
                "id": r["id"],
                "instId": r["inst_id"],
                "startedAt": r["started_at"],
                "endedAt": r["ended_at"],
                "durationS": round(r["ended_at"] - r["started_at"], 1),
                "window": r["win"],
                "maxDropPct": r["max_drop_pct"],
                "price": r["price"],
                "status": r["status"],
                "detail": r["detail"],
                "alerted": bool(r["alerted"]),
            }
            for r in rows
        ]
        return {"items": items, "total": total}

    def stats(self, *, since: float) -> dict[str, Any]:
        """Сводка за период: по дням, топ инструментов, окна, сигнатуры, причины отсева."""
        days: dict[str, dict[str, Any]] = {}
        for r in self._db.execute(
            "SELECT date(ts, 'unixepoch', 'localtime') d, COUNT(*) n FROM alerts WHERE ts >= ? GROUP BY d", (since,)
        ):
            days.setdefault(r["d"], {"day": r["d"], "alerts": 0, "episodes": 0})["alerts"] = r["n"]
        for r in self._db.execute(
            "SELECT date(started_at, 'unixepoch', 'localtime') d, COUNT(*) n FROM episodes"
            " WHERE started_at >= ? GROUP BY d",
            (since,),
        ):
            days.setdefault(r["d"], {"day": r["d"], "alerts": 0, "episodes": 0})["episodes"] = r["n"]

        top = [
            {"instId": r["inst_id"], "alerts": r["n"], "maxDropPct": r["m"]}
            for r in self._db.execute(
                "SELECT inst_id, COUNT(*) n, MIN(drop_pct) m FROM alerts WHERE ts >= ?"
                " GROUP BY inst_id ORDER BY n DESC, m ASC LIMIT 10",
                (since,),
            )
        ]
        windows = {
            r["win"]: r["n"]
            for r in self._db.execute("SELECT win, COUNT(*) n FROM alerts WHERE ts >= ? GROUP BY win", (since,))
        }
        hits: dict[str, int] = {}
        for r in self._db.execute("SELECT hits FROM alerts WHERE ts >= ?", (since,)):
            for hit in (r["hits"] or "").split(","):
                if hit:
                    hits[hit] = hits.get(hit, 0) + 1
        statuses = {
            r["status"]: r["n"]
            for r in self._db.execute(
                "SELECT status, COUNT(*) n FROM episodes WHERE started_at >= ? GROUP BY status", (since,)
            )
        }
        return {
            "since": since,
            "alerts": self._count("SELECT COUNT(*) FROM alerts WHERE ts >= ?", (since,)),
            "episodes": self._count("SELECT COUNT(*) FROM episodes WHERE started_at >= ?", (since,)),
            "episodesAlerted": self._count(
                "SELECT COUNT(*) FROM episodes WHERE started_at >= ? AND alerted = 1", (since,)
            ),
            "days": [days[d] for d in sorted(days)],
            "top": top,
            "windows": windows,
            "hits": hits,
            "statuses": statuses,
        }

    def _count(self, sql: str, params: Any) -> int:
        return int(self._db.execute(sql, tuple(params)).fetchone()[0])


def _limit(limit: int) -> int:
    return max(1, min(int(limit), MAX_LIMIT))
