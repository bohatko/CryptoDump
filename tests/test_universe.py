"""Сборка вселенной инструментов из REST."""

from __future__ import annotations

import asyncio

from scanner.config import MarketConfig
from scanner.universe import build_universe, select_candidates

ROWS = [
    {"instId": "BTC-USDT-SWAP", "settleCcy": "USDT", "ctVal": "0.01"},
    {"instId": "ETH-USDT-SWAP", "settleCcy": "USDT", "ctVal": "0.1"},
    {"instId": "SOL-USDT-SWAP", "settleCcy": "USDT", "ctVal": "1"},
    {"instId": "BTC-USDC-SWAP", "settleCcy": "USDC", "ctVal": "0.01"},
    {"instId": "BTC-USD-SWAP", "settleCcy": "BTC", "ctVal": "100"},
]
TURNOVER = {"BTC-USDT-SWAP": 9e9, "ETH-USDT-SWAP": 5e9, "SOL-USDT-SWAP": 1e9}


class FakeRest:
    async def instruments(self, inst_type: str) -> list[dict]:
        return ROWS if inst_type == "SWAP" else []

    async def tickers(self, inst_type: str) -> dict[str, float]:
        return TURNOVER


def test_select_filters_quote_and_patterns() -> None:
    market = MarketConfig()
    picked = select_candidates(ROWS, "SWAP", TURNOVER, market, wanted=set())
    assert [inst.inst_id for inst, _ in picked] == ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"]
    assert picked[0][0].contract_value == 0.01


def _ids(market: MarketConfig) -> list[str]:
    universe = asyncio.run(build_universe(FakeRest(), market))  # type: ignore[arg-type]
    return [inst.inst_id for inst, _turnover in universe]


def test_build_universe_ranks_and_limits() -> None:
    assert _ids(MarketConfig(inst_types=["SWAP"], top_n=2)) == ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]


def test_top_n_zero_means_whole_market() -> None:
    assert _ids(MarketConfig(inst_types=["SWAP"], top_n=0)) == ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"]


def test_min_turnover_drops_dead_instruments() -> None:
    market = MarketConfig(inst_types=["SWAP"], top_n=0, min_turnover_usdt=2e9)
    assert _ids(market) == ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]


def test_explicit_symbols_ignore_top_n() -> None:
    market = MarketConfig(inst_types=["SWAP"], top_n=1, symbols=["sol", "eth"])
    assert _ids(market) == ["ETH-USDT-SWAP", "SOL-USDT-SWAP"]


def test_turnover_returned_with_instrument() -> None:
    universe = asyncio.run(build_universe(FakeRest(), MarketConfig(inst_types=["SWAP"], top_n=1)))  # type: ignore[arg-type]
    assert universe[0][1] == 9e9
