"""Общие asyncio-утилиты движка и WS-клиента."""

from __future__ import annotations

import asyncio


async def sleep_or_stop(seconds: float, stop: asyncio.Event) -> None:
    """Подождать ``seconds`` или выйти раньше, если выставлен ``stop``."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return
