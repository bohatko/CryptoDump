"""REST-клиент OKX: вселенная инструментов (+ ``ctVal``), обороты, свечи, стакан.

Стакан в штатном режиме приходит из WS-канала ``books``; ``/api/v5/market/books``
используется только при ``depth.source == "rest"``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

log = logging.getLogger(__name__)

REST_PATHS = {
    "instruments": "/api/v5/public/instruments",
    "tickers": "/api/v5/market/tickers",
    "books": "/api/v5/market/books",
    "candles": "/api/v5/market/candles",
}


class OkxRestError(RuntimeError):
    pass


class OkxRest:
    """Тонкая обёртка над публичным REST. Никакой авторизации не требует."""

    def __init__(self, base_url: str, *, timeout_s: float = 10.0, retries: int = 2) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.retries = retries
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "OkxRest":
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout_s),
                headers={"User-Agent": "CryptoDump/0.1"},
            )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    # ---------- низкий уровень ----------

    async def _get(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if self._session is None or self._session.closed:
            await self.start()
        assert self._session is not None

        url = f"{self.base_url}{path}"
        last_exc: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                async with self._session.get(url, params=params) as resp:
                    if resp.status == 429:
                        wait = 0.5 * (attempt + 1)
                        log.warning("REST 429 на %s — пауза %.1f c", path, wait)
                        await asyncio.sleep(wait)
                        continue
                    if resp.status >= 500:
                        last_exc = OkxRestError(f"{path}: HTTP {resp.status}")
                        await asyncio.sleep(0.4 * (attempt + 1))
                        continue
                    payload = await resp.json(content_type=None)
                    if payload.get("code") not in (None, "0"):
                        raise OkxRestError(f"{path}: {payload.get('code')} {payload.get('msg')}")
                    data = payload.get("data") or []
                    if not isinstance(data, list):
                        raise OkxRestError(f"{path}: неожиданный формат data")
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                log.debug("REST %s попытка %d/%d: %s", path, attempt + 1, self.retries + 1, exc)
                await asyncio.sleep(0.4 * (attempt + 1))
        raise OkxRestError(f"{path}: запрос не удался: {last_exc}")

    # ---------- доменные методы ----------

    async def instruments(self, inst_type: str) -> list[dict[str, Any]]:
        data = await self._get(REST_PATHS["instruments"], {"instType": inst_type})
        return [row for row in data if row.get("state") == "live"]

    async def tickers(self, inst_type: str) -> dict[str, float]:
        """``instId -> приблизительный оборот за 24h в USDT``.

        ``volCcy24h`` у деривативов OKX отдаётся в базовой валюте, поэтому умножаем
        на ``last``. Величина нужна только для ранжирования top-N, а не для расчётов,
        так что приблизительности достаточно.
        """
        data = await self._get(REST_PATHS["tickers"], {"instType": inst_type})
        out: dict[str, float] = {}
        for row in data:
            inst_id = row.get("instId")
            if not inst_id:
                continue
            try:
                vol = float(row.get("volCcy24h") or row.get("vol24h") or 0.0)
            except (TypeError, ValueError):
                continue
            try:
                last = float(row.get("last") or 0.0)
            except (TypeError, ValueError):
                last = 0.0
            out[inst_id] = vol * last if last > 0 else vol
        return out

    async def books(self, inst_id: str, depth: int = 20) -> dict[str, Any]:
        """Сырой стакан: ``{"bids": [[px, sz, ts, ...]], "asks": [...], "ts": ...}``.

        ``sz`` в ответе — **контракты**, не базовая валюта. Перевод в базовую валюту
        делает ``OrderBook`` по ``ctVal`` инструмента.
        """
        data = await self._get(REST_PATHS["books"], {"instId": inst_id, "sz": str(depth)})
        if not data:
            raise OkxRestError(f"books {inst_id}: пустой ответ")
        return data[0]

    async def candles(self, inst_id: str, *, bar: str = "1m", limit: int = 125) -> list[dict[str, Any]]:
        """Свечи в порядке от старых к новым (OKX отдаёт новые первыми)."""
        limit = max(1, min(limit, 300))
        data = await self._get(REST_PATHS["candles"], {"instId": inst_id, "bar": bar, "limit": str(limit)})
        return list(reversed(data))
